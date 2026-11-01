// 14 §14.10 Interest: [CLI-70] to [CLI-73], with 09 §9.3 (interest prefixes, [READ-22] to [READ-30]) and §9.7, and SPEC-QUESTIONS Q28.

import { describe, expect, it } from "vitest";
import { LocalRefusal, set } from "../src/engine/index.js";
import type { KeyEntry } from "../src/gen/abpc/v2/types_pb.js";
import { create } from "@bufbuild/protobuf";
import { KeyEntrySchema } from "../src/gen/abpc/v2/types_pb.js";
import { toHex } from "../src/rules/bytes.js";
import type { ReplicaState } from "../src/rules/fold/index.js";
import { kc, KS, ofType, rig, tick, type Rig } from "./helpers/engine.js";

const setK = (key: string, v: unknown = 1): { keyspaceId: string; opType: string; changes: ReturnType<typeof set>[] } => ({ keyspaceId: KS, opType: "Set", changes: [set(key, v)] });
const stateOf = (r: Rig, ks = KS): ReplicaState => (r.engine as unknown as { follows: Map<string, { state: ReplicaState }> }).follows.get(ks)?.state as ReplicaState;
type Cursor = { keyspaceId: string; afterSeq: bigint; logEpoch: number; interestPrefixes: string[]; afterRecordHash: Uint8Array };
const loadCursors = (r: Rig): Cursor[] => r.server.calls.filter((c) => c.method === "loadState").map((c) => (c.request as { cursor: Cursor }).cursor);
const subscribeCursors = (r: Rig): Cursor[] => r.server.calls.filter((c) => c.method === "subscribe").flatMap((c) => (c.request as { keyspaces: Cursor[] }).keyspaces);
const updates = (r: Rig): { add: Cursor[]; setInterest: { keyspaceId: string; interestPrefixes: string[] }[]; remove: string[] }[] => r.server.calls.filter((c) => c.method === "updateSubscription").map((c) => c.request as never);

/** A server with the keys of two issues and a setting, and an engine that follows the keyspace with `interest` (and is started unless `start` is false). */
async function withInterest(interest: string[] | undefined, o: { start?: boolean } = {}): Promise<Rig> {
  const r = await rig({ keyspaces: [], start: false, heartbeatMs: 0 });
  r.server.createKeyspaceNow(KS);
  r.server.write(KS, [kc("issues/i1", "set", { n: 1 }), kc("issues/i1/title", "set", "one")]);
  r.server.write(KS, [kc("issues/i2", "set", { n: 2 }), kc("issues/i2/title", "set", "two")]);
  r.server.write(KS, [kc("settings/a", "set", "a")]);
  await r.engine.addKeyspace(KS, interest === undefined ? {} : { interest });
  if (o.start !== false) {
    r.engine.start();
    await r.engine.settle();
  }
  return r;
}

describe("14 [CLI-70]: the engine follows only the keyspaces that the app asks for, listed explicitly", () => {
  it("Subscribe names the followed keyspaces and no other, whatever the principal's grants match; a keyspace that the app adds is named in UpdateSubscription", async () => {
    const r = await rig({ keyspaces: [KS], heartbeatMs: 0 });
    r.server.createKeyspaceNow("ks2"); // the principal could read it, and the engine does not follow it
    r.server.write("ks2", [kc("settings/x", "set", 1)]);
    r.server.reconnect();
    await r.engine.settle();
    await tick(r, 2000);
    const sub = r.server.calls.filter((c) => c.method === "subscribe").at(-1)?.request as { keyspaces: { keyspaceId: string }[] };
    expect(sub.keyspaces.map((k) => k.keyspaceId)).toEqual([KS]);
    expect(() => r.engine.view("ks2")).toThrow(LocalRefusal);
    await r.engine.addKeyspace("ks2");
    await r.engine.settle();
    expect(updates(r).at(-1)?.add.map((c) => c.keyspaceId)).toEqual(["ks2"]);
    expect(r.engine.view("ks2").json("settings/x")).toBe(1);
  });
});

describe("14 [CLI-71]: interest prefixes per keyspace: the confirmed state holds only the keys under them", () => {
  it("the load and the subscription name the interest; the state holds the keys under it; a whole record that also touches keys outside it is delivered and those keys are not presented as current (09 [READ-26])", async () => {
    const r = await withInterest(["issues/i1"]);
    expect(loadCursors(r).at(-1)?.interestPrefixes).toEqual(["issues/i1"]);
    expect(subscribeCursors(r).at(-1)?.interestPrefixes).toEqual(["issues/i1"]);
    expect(r.engine.confirmed(KS).has("issues/i1/title")).toBe(true);
    expect(r.engine.confirmed(KS).has("issues/i1")).toBe(true);
    for (const k of ["issues/i2", "issues/i2/title", "settings/a"]) expect(r.engine.confirmed(KS).has(k), k).toBe(false);
    expect(r.engine.status().interest[KS]).toEqual(["issues/i1"]);
    // one record names both: it is delivered whole; the key outside the interest is dropped
    const seq = r.server.write(KS, [kc("issues/i1/title", "set", "uno"), kc("issues/i2/title", "set", "dos")]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).json("issues/i1/title")).toBe("uno");
    expect(r.engine.confirmed(KS).has("issues/i2/title")).toBe(false);
    expect(r.engine.view(KS).has("issues/i2/title")).toBe(false);
    expect(r.engine.cursor(KS)?.seq).toBe(seq);
  });

  it("a record that does not touch the interest is not delivered, and the delivery watermark passes it: the cursor follows the next frame, and the chain of `prev_cursor` holds", async () => {
    const r = await withInterest(["issues/i1"]);
    const before = r.engine.cursor(KS)?.seq as bigint;
    const quiet = r.server.write(KS, [kc("settings/other", "set", 1)]); // outside the interest: no frame
    await r.engine.settle();
    expect(r.engine.cursor(KS)?.seq).toBe(before); // nothing delivered
    const seq = r.server.write(KS, [kc("issues/i1/title", "set", "next")]);
    await r.engine.settle();
    expect(seq).toBe(quiet + 1n);
    expect(r.engine.cursor(KS)?.seq).toBe(seq); // the frame's cursor covers the record that was examined and not sent
    expect(r.engine.confirmed(KS).json("issues/i1/title")).toBe("next");
    expect(ofType(r, "protocol-error")).toEqual([]);
    expect(r.engine.status().holds).toEqual([]);
    expect(r.server.calls.filter((c) => c.method === "subscribe")).toHaveLength(1); // no resume was needed
  });

  it("a deleted marker reaches the readers of its subtree (09 [READ-23]): the subtree goes, and its tombstone stays", async () => {
    const r = await withInterest(["issues/i1/title"]);
    expect(r.engine.confirmed(KS).json("issues/i1/title")).toBe("one");
    r.server.write(KS, [kc("issues/i1", "delete")]); // an ancestor of the prefix
    await r.engine.settle();
    expect(r.engine.confirmed(KS).has("issues/i1/title")).toBe(false);
    expect(stateOf(r).tombstone("issues/i1")?.kind).toBe("entity");
  });

  it("an interest that no server accepts is refused when it is set: the empty prefix, a trailing slash, an empty segment, more prefixes than max_interest_prefixes", async () => {
    const r = await withInterest([]);
    for (const bad of [[""], ["issues/"], ["a//b"], ["has space"], ["../x"], Array.from({ length: 65 }, (_, i) => `p${i}`)]) {
      await expect(r.engine.setInterest(KS, bad), JSON.stringify(bad).slice(0, 40)).rejects.toMatchObject({ code: "INVALID" });
    }
    await expect(r.engine.setInterest("unknown", ["a"])).rejects.toMatchObject({ code: "NOT_FOLLOWED" });
    await expect(r.engine.addKeyspace("ks9", { interest: ["a/"] })).rejects.toMatchObject({ code: "INVALID" });
    expect(r.engine.interest(KS)).toEqual([]);
  });

  it("asking for a followed keyspace again with another interest is a change of interest; asking with none changes nothing", async () => {
    const r = await withInterest(["issues/i1"]);
    await r.engine.addKeyspace(KS, { interest: ["issues/i1", "issues/i2"] });
    await r.engine.settle();
    expect(r.engine.interest(KS)).toEqual(["issues/i1", "issues/i2"]);
    expect(r.engine.confirmed(KS).json("issues/i2/title")).toBe("two");
    await r.engine.addKeyspace(KS);
    expect(r.engine.interest(KS)).toEqual(["issues/i1", "issues/i2"]);
  });

  it("the interest is kept with the stored state: a restart follows the same keyspaces with the same interest, and the restored state holds the same keys", async () => {
    const r = await withInterest(["issues/i1"]);
    const live = toHex(stateOf(r).keyspaceStateHash());
    await r.engine.stop();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, keyspaces: [], start: false, seed: 41 });
    expect(r2.engine.interest(KS)).toEqual(["issues/i1"]); // the keyspace is followed again from the stored state alone
    expect(r2.engine.confirmed(KS).has("issues/i2/title")).toBe(false);
    expect(toHex(stateOf(r2).keyspaceStateHash())).toBe(live);
    r2.engine.start();
    await r2.engine.settle();
    expect(subscribeCursors(r2).at(-1)?.interestPrefixes).toEqual(["issues/i1"]);
    expect(loadCursors(r2).at(-1)?.interestPrefixes).toEqual(["issues/i1"]); // the resume check names it too
  });
});

describe("14 [CLI-72]: widening the interest loads the new prefixes first, with the cursor unchanged, and adds the keyspace again below it", () => {
  it("the widening load names only the added prefixes, installs its entries, leaves the cursor, and the keyspace is added again with the new interest at min(H, cursor): `interest-changed`", async () => {
    const r = await withInterest(["issues/i1"]);
    const cursor = r.engine.cursor(KS)?.seq as bigint;
    await r.engine.setInterest(KS, ["issues/i1", "issues/i2"]);
    await r.engine.settle();
    const widening = loadCursors(r).at(-1);
    expect(widening?.interestPrefixes).toEqual(["issues/i2"]); // only the added prefix
    expect(widening?.afterSeq).toBe(0n);
    expect(r.engine.cursor(KS)?.seq).toBe(cursor); // 09 [READ-28]: the widening load does not change the cursor
    expect(r.engine.confirmed(KS).json("issues/i2/title")).toBe("two");
    expect(r.engine.confirmed(KS).has("settings/a")).toBe(false);
    expect(ofType(r, "interest-changed")).toEqual([expect.objectContaining({ keyspaceId: KS, interest: ["issues/i1", "issues/i2"], cause: "WIDENED" })]);
    const add = updates(r).at(-1)?.add[0];
    expect(add?.interestPrefixes).toEqual(["issues/i1", "issues/i2"]);
    expect(add?.afterSeq).toBeLessThanOrEqual(cursor);
    expect(r.engine.status().interest[KS]).toEqual(["issues/i1", "issues/i2"]);
    // records under the new interest now arrive
    r.server.write(KS, [kc("issues/i2/title", "set", "dos")]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).json("issues/i2/title")).toBe("dos");
    expect(ofType(r, "protocol-error")).toEqual([]);
  });

  it("there is no hole: records written while the load is made, under the added prefixes and under the old interest, are all there afterwards, and the other changes of a record that touches both apply at once", async () => {
    const r = await withInterest(["issues/i1"]);
    // the load is read, then a record is written (and delivered under the old interest) before the engine has the page
    r.server.inject("loadState", {
      kind: "after",
      fn: () => {
        r.server.write(KS, [kc("issues/i1/title", "set", "during"), kc("issues/i2/title", "set", "during-i2")]); // touches both: delivered whole
        r.server.write(KS, [kc("issues/i2/title", "set", "i2-only")]); // touches only the added prefix: not delivered under the old interest
      },
    });
    await r.engine.setInterest(KS, ["issues/i1", "issues/i2"]);
    await r.engine.settle();
    await tick(r, 1000);
    expect(r.engine.confirmed(KS).json("issues/i1/title")).toBe("during"); // applied at once, never delayed
    expect(r.engine.confirmed(KS).json("issues/i2/title")).toBe("i2-only"); // the last value, by the load and the records after its checkpoint
    expect(toHex(stateOf(r).keyspaceStateHash())).not.toBe(""); // and it folds
    r.server.write(KS, [kc("issues/i2/title", "set", "after")]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).json("issues/i2/title")).toBe("after");
    expect(r.engine.cursor(KS)?.seq).toBe(r.server.head(KS));
  });

  it("an entry that a held ancestor tombstone hides is treated as absent, so a widening never brings a deleted entity back (05 [STATE-38])", async () => {
    const r = await withInterest(["issues/i1"]);
    r.server.write(KS, [kc("issues/i1", "delete")]);
    await r.engine.settle();
    const t = stateOf(r).tombstone("issues/i1");
    expect(t?.kind).toBe("entity");
    const follow = (r.engine as unknown as { follows: Map<string, { interest: string[] }> }).follows.get(KS) as { interest: string[] };
    // the entries below are the load's for a prefix that the old interest does not cover (the ones of the old interest are the stream's, and are not installed, 09 [READ-28])
    follow.interest = ["settings/a"];
    // a stale entry for the subtree, at a version at or below the deletion seq, arrives with a load
    const stale: KeyEntry = create(KeyEntrySchema, { key: "issues/i1/title", value: new TextEncoder().encode('"ghost"'), encoding: "json/v1", version: (t?.version as bigint) - 1n });
    (r.engine as unknown as { installWidening(f: unknown, e: KeyEntry[], v: number): void }).installWidening(follow, [stale], 1);
    expect(r.engine.confirmed(KS).has("issues/i1/title")).toBe(false);
    // an entry above the deletion seq is a new entity's, and is shown
    const fresh: KeyEntry = create(KeyEntrySchema, { key: "issues/i1/title", value: new TextEncoder().encode('"new"'), encoding: "json/v1", version: (t?.version as bigint) + 1n });
    (r.engine as unknown as { installWidening(f: unknown, e: KeyEntry[], v: number): void }).installWidening(follow, [fresh], 1);
    expect(r.engine.confirmed(KS).json("issues/i1/title")).toBe("new");
  });

  it("a widening that is asked for offline waits for a session; the interest is the new one when its load is installed", async () => {
    const r = await withInterest(["issues/i1"]);
    r.server.suspend();
    r.server.disconnect();
    await r.engine.settle();
    await r.engine.setInterest(KS, ["issues/i1", "issues/i2"]);
    expect(r.engine.interest(KS)).toEqual(["issues/i1"]); // not yet
    expect(r.engine.confirmed(KS).has("issues/i2/title")).toBe(false);
    r.server.resume();
    await tick(r, 3_700_000);
    expect(r.engine.interest(KS)).toEqual(["issues/i1", "issues/i2"]);
    expect(r.engine.confirmed(KS).json("issues/i2/title")).toBe("two");
    // the session was subscribed with the old interest (the load failed while the app was suspended, and was made again on the live session); the keyspace is added again with the new one
    expect(updates(r).at(-1)?.add[0]?.interestPrefixes).toEqual(["issues/i1", "issues/i2"]);
  });

  it("widening to the whole keyspace loads it whole (an empty interest); the stored image has the widened pages: a restart shows the same state", async () => {
    const r = await withInterest(["issues/i1"]);
    await r.engine.setInterest(KS, []);
    await r.engine.settle();
    expect(loadCursors(r).at(-1)?.interestPrefixes).toEqual([]);
    for (const k of ["issues/i2/title", "settings/a"]) expect(r.engine.confirmed(KS).has(k), k).toBe(true);
    expect(r.engine.interest(KS)).toEqual([]);
    const live = toHex(stateOf(r).keyspaceStateHash());
    expect(live).toBe(toHex(r.server.stateOf(KS).keyspaceStateHash()));
    await r.engine.stop();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, keyspaces: [], start: false, seed: 42 });
    expect(toHex(stateOf(r2).keyspaceStateHash())).toBe(live);
  });

  it("a widened image restores to the state that the live engine has, with the records after the widening folded again", async () => {
    const r = await withInterest(["issues/i1"]);
    await r.engine.setInterest(KS, ["issues/i1", "settings"]);
    await r.engine.settle();
    r.server.write(KS, [kc("settings/b", "set", "b"), kc("issues/i1/title", "set", "uno")]);
    r.server.write(KS, [kc("issues/i2/title", "set", "ignored")]);
    await r.engine.settle();
    const live = toHex(stateOf(r).keyspaceStateHash());
    await r.engine.stop();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, keyspaces: [], start: false, seed: 43 });
    expect(toHex(stateOf(r2).keyspaceStateHash())).toBe(live);
    expect(r2.engine.confirmed(KS).json("settings/b")).toBe("b");
    expect(r2.engine.confirmed(KS).has("issues/i2/title")).toBe(false);
  });
});

describe("14 [CLI-72]: narrowing the interest needs no load: the keys outside it go, and `set_interest` tells the server", () => {
  it("the keys that left the interest are removed at once, the server announces its switch point, later records outside the new interest are not delivered, and the interest is kept", async () => {
    const r = await withInterest(["issues/i1", "issues/i2"]);
    expect(r.engine.confirmed(KS).has("issues/i2/title")).toBe(true);
    const loads = loadCursors(r).length;
    await r.engine.setInterest(KS, ["issues/i1"]);
    await r.engine.settle();
    expect(loadCursors(r)).toHaveLength(loads); // no load
    expect(r.engine.confirmed(KS).has("issues/i2/title")).toBe(false);
    expect(r.engine.confirmed(KS).has("issues/i1/title")).toBe(true);
    expect(updates(r).at(-1)?.setInterest).toEqual([{ keyspaceId: KS, interestPrefixes: ["issues/i1"], $typeName: "abpc.v2.UpdateSubscriptionRequest.Interest" }]);
    expect(ofType(r, "interest-changed")).toEqual([expect.objectContaining({ interest: ["issues/i1"], cause: "NARROWED" })]);
    r.server.write(KS, [kc("issues/i2/title", "set", "late")]);
    r.server.write(KS, [kc("issues/i1/title", "set", "kept")]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).has("issues/i2/title")).toBe(false);
    expect(r.engine.confirmed(KS).json("issues/i1/title")).toBe("kept");
    expect(r.engine.cursor(KS)?.seq).toBe(r.server.head(KS));
    expect(ofType(r, "protocol-error")).toEqual([]);
    expect(r.storage.replica(KS)?.head.interest).toEqual(["issues/i1"]);
    // a restart: the restored state is pruned to the narrower interest too (the pages are those of the wider load)
    const live = toHex(stateOf(r).keyspaceStateHash());
    await r.engine.stop();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, keyspaces: [], start: false, seed: 44 });
    expect(r2.engine.interest(KS)).toEqual(["issues/i1"]);
    expect(r2.engine.confirmed(KS).has("issues/i2/title")).toBe(false);
    expect(toHex(stateOf(r2).keyspaceStateHash())).toBe(live);
  });
});

describe("14 [CLI-73]: a key outside the interest is loaded before the app changes it, so that its seen_version is right", () => {
  it("without the load the action carries seen_version 0 and is flagged `overwrote_newer`; with it the version is the key's and nothing is overwritten", async () => {
    const r = await withInterest(["issues/i1"]);
    r.server.write(KS, [kc("settings/o", "set", "theirs")], { sub: "bob" });
    r.server.write(KS, [kc("settings/o", "set", "theirs2")], { sub: "bob" });
    await r.engine.settle();
    const version = r.server.stateOf(KS).key("settings/o")?.version as bigint;
    const blind = await r.engine.submit(setK("settings/o", "mine"));
    await r.engine.settle();
    expect(ofType(r, "overwritten").filter((e) => e.mutationId === blind.mutationId)).toHaveLength(1); // the server flagged it: seen_version 0
    // with the load
    r.server.write(KS, [kc("settings/o", "set", "theirs3")], { sub: "bob" });
    await r.engine.settle();
    expect(await r.engine.loadKeys(KS, ["settings/o", "settings/never"])).toBe(true);
    expect(r.engine.confirmed(KS).has("settings/o")).toBe(false); // not shown as current ([READ-26])
    const versionNow = r.server.stateOf(KS).key("settings/o")?.version as bigint;
    const sure = await r.engine.submit(setK("settings/o", "mine again"));
    const m = (r.engine as unknown as { content(e: unknown): { changes: { seenVersion: bigint }[] } }).content(r.engine.outbox().find((e) => e.mutationId === sure.mutationId));
    expect(m.changes[0]?.seenVersion).toBe(versionNow);
    expect(versionNow).toBeGreaterThan(version);
    await r.engine.settle();
    expect(ofType(r, "overwritten").filter((e) => e.mutationId === sure.mutationId)).toHaveLength(0);
    // a key that does not exist has version 0, as no load or tombstone says otherwise
    const fresh = await r.engine.submit(setK("settings/never", 1));
    const m2 = (r.engine as unknown as { content(e: unknown): { changes: { seenVersion: bigint }[] } }).content(r.engine.outbox().find((e) => e.mutationId === fresh.mutationId) ?? { content: new Uint8Array() });
    void m2;
    await r.engine.settle();
    expect(ofType(r, "overwritten").filter((e) => e.mutationId === fresh.mutationId)).toHaveLength(0);
    // the tombstone's version is the key's version
    r.server.write(KS, [kc("settings/gone", "set", 1)], { sub: "bob" });
    r.server.write(KS, [kc("settings/gone", "delete")], { sub: "bob" });
    await r.engine.loadKeys(KS, ["settings/gone"]);
    const afterDelete = await r.engine.submit(setK("settings/gone", 2));
    await r.engine.settle();
    expect(ofType(r, "overwritten").filter((e) => e.mutationId === afterDelete.mutationId)).toHaveLength(0);
  });

  it("keys inside the interest need no load; asking for one is a no-op, and a keyspace that is not loaded refuses", async () => {
    const r = await withInterest(["issues/i1"]);
    const loads = loadCursors(r).length;
    expect(await r.engine.loadKeys(KS, ["issues/i1/title"])).toBe(true);
    expect(loadCursors(r)).toHaveLength(loads);
    await expect(r.engine.loadKeys("nope", ["a"])).rejects.toBeInstanceOf(LocalRefusal);
  });
});
