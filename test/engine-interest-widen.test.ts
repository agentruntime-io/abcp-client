// 14 [CLI-72], 09 [READ-28] (V14 MUST-5): a widening installs "the load's entries for the added prefixes and the ancestor tombstones that it returns,
// and nothing else", and never takes the state of a key back (05 [STATE-38]'s guard); and 14 [CLI-70] (V14 S5): the app can stop following a keyspace.

import { describe, expect, it } from "vitest";
import { LocalRefusal, set } from "../src/engine/index.js";
import { kc, KS, rig, type Rig } from "./helpers/engine.js";

const setK = (key: string, v: unknown = 1): { keyspaceId: string; opType: string; changes: ReturnType<typeof set>[] } => ({ keyspaceId: KS, opType: "Set", changes: [set(key, v)] });

/** The `seen_version` of the first change of an action: the version of the key when the action was authored. */
function seenOf(r: Rig, id: string): bigint | undefined {
  const sent = r.server.calls.filter((c) => c.method === "mutate").flatMap((c) => (c.request as { mutations: { mutationId: string; changes: { seenVersion: bigint }[] }[] }).mutations);
  return sent.find((m) => m.mutationId === id)?.changes[0]?.seenVersion;
}

/** Two issues; the engine follows `issues/i1` and is started; pages of one entry, so that a write can land in the middle of a load. */
async function narrow(): Promise<Rig> {
  const r = await rig({ keyspaces: [], start: false, heartbeatMs: 0, serverOptions: { pageSize: 1 } });
  r.server.createKeyspaceNow(KS);
  r.server.write(KS, [kc("issues/i1", "set", { n: 1 }), kc("issues/i1/title", "set", "one")]);
  r.server.write(KS, [kc("issues/i2", "set", { n: 2 }), kc("issues/i2/title", "set", "two")]);
  await r.engine.addKeyspace(KS, { interest: ["issues/i1"] });
  r.engine.start();
  await r.engine.settle();
  return r;
}

describe("14 [CLI-72], 09 [READ-28]: a widening whose added prefix covers the old interest installs nothing of the old interest (V14 MUST-5)", () => {
  it("a key of the old interest is not taken back to the widening load's checkpoint: at `interest-changed` it has the value of the stream", async () => {
    const r = await narrow();
    r.server.inject("loadState", { kind: "after", fn: () => void r.server.write(KS, [kc("issues/i1/title", "set", "during")]) }, { times: 1 });
    const seen: { atEvent: unknown; cursor: bigint | undefined }[] = [];
    r.engine.on((e) => {
      if (e.type === "interest-changed") seen.push({ atEvent: r.engine.confirmed(KS).json("issues/i1/title"), cursor: r.engine.cursor(KS)?.seq });
    });
    await r.engine.setInterest(KS, ["issues"]);
    await r.engine.settle();
    expect(seen).toHaveLength(1);
    expect(seen[0]).toEqual({ atEvent: "during", cursor: 3n });
    expect(r.engine.confirmed(KS).json("issues/i1/title")).toBe("during");
    // the added keys came with the load: the whole of `issues` is held now
    expect(r.engine.confirmed(KS).json("issues/i2/title")).toBe("two");
  });

  it("a key of the old interest that was deleted while the load ran does not come back", async () => {
    const r = await narrow();
    r.server.inject("loadState", { kind: "after", fn: () => void r.server.write(KS, [kc("issues/i1/title", "delete")]) }, { times: 1 });
    const seen: boolean[] = [];
    r.engine.on((e) => {
      if (e.type === "interest-changed") seen.push(r.engine.confirmed(KS).has("issues/i1/title"));
    });
    await r.engine.setInterest(KS, ["issues"]);
    await r.engine.settle();
    expect(seen).toEqual([false]); // not even for the moment between the install and the re-add
    expect(r.engine.confirmed(KS).has("issues/i1/title")).toBe(false);
    expect(r.engine.confirmed(KS).json("issues/i2/title")).toBe("two");
  });

  it("an action authored at the moment of the switch carries the version of the stream, not the load's older one", async () => {
    const r = await narrow();
    r.server.inject("loadState", { kind: "after", fn: () => void r.server.write(KS, [kc("issues/i1/title", "set", "during")]) }, { times: 1 });
    let authored: Promise<{ mutationId: string }> | undefined;
    r.engine.on((e) => {
      if (e.type === "interest-changed" && authored === undefined) authored = r.engine.submit(setK("issues/i1/title", "mine"));
    });
    await r.engine.setInterest(KS, ["issues"]);
    await r.engine.settle();
    const id = (await authored)?.mutationId as string;
    await r.engine.settle();
    // the action saw the title as the stream left it (seq 3): the load's version (2) would have been "unknown newer" to the server
    expect(seenOf(r, id)).toBe(3n);
  });
});

describe("14 [CLI-70]: the app can stop following a keyspace (V14 S5)", () => {
  it("the keyspace leaves the session, the copy and the stored replica go, and a restart does not follow it again; the app asks for it again", async () => {
    const r = await rig({ heartbeatMs: 0 });
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    expect(r.storage.replica(KS)).toBeDefined();
    expect(await r.engine.removeKeyspace(KS)).toBe(true);
    await r.engine.settle();
    expect(r.engine.cursor(KS)).toBeUndefined();
    expect(() => r.engine.view(KS)).toThrow(LocalRefusal);
    expect(r.storage.replica(KS)).toBeUndefined();
    const up = r.server.calls.filter((c) => c.method === "updateSubscription").map((c) => c.request as { remove: string[] });
    expect(up.flatMap((u) => u.remove)).toEqual([KS]);
    expect(await r.engine.removeKeyspace(KS)).toBe(false);
    await r.engine.stop();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 3, keyspaces: [] });
    await r2.engine.settle();
    expect(r2.engine.cursor(KS)).toBeUndefined(); // not followed again at a restart
    expect(r.server.calls.filter((c) => c.method === "subscribe").at(-1)?.request).toMatchObject({ keyspaces: [] });
    await r2.engine.addKeyspace(KS);
    await r2.engine.settle();
    expect(r2.engine.view(KS).json("settings/a")).toBe(1);
  });

  it("it is refused while the outbox holds an action of the keyspace, which the engine still has to send", async () => {
    const r = await rig({ heartbeatMs: 0 });
    r.server.faults.paused = true;
    r.server.disconnect();
    await r.engine.settle();
    await r.engine.submit(setK("settings/p"));
    await expect(r.engine.removeKeyspace(KS)).rejects.toMatchObject({ code: "REFUSED" });
    expect(r.engine.cursor(KS)).toBeDefined();
  });
});
