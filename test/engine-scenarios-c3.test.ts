// Scenarios of C3 (14 §14.7 to §14.10) that put the pieces together: a restart that resumes from storage and finds the world changed; the next
// tab taking over a flusher's queue; a revocation that quarantines a working outbox and the app's way back; an interest that follows what
// the user opens.

import { describe, expect, it } from "vitest";
import { cas, set, type ExportRecord } from "../src/engine/index.js";
import { kc, kcCas, KS, ofType, rig, tick, types } from "./helpers/engine.js";
import { settleAll, tabsRig, typesOf } from "./helpers/tabs.js";
import type { FakeTransport } from "../src/fake/index.js";

describe("scenario: a restart that resumes from storage and finds the world changed (14 [CLI-2], [CLI-8], [CLI-18], [CLI-47])", () => {
  it("the app is closed with a note; Bob edits it while Ana is away; Ana reopens offline, edits against the persisted copy, and connects: a conflict, in which the handler sees Bob's value, and her merge is a new action", async () => {
    const seen: string[] = [];
    const r = await rig({ heartbeatMs: 0 });
    r.server.write(KS, [kcCas("notes/n1", "base")]);
    await r.engine.settle();
    await r.engine.stop();
    r.server.write(KS, [kcCas("notes/n1", "bobs")], { sub: "bob" }); // while Ana is away
    const r2 = await rig({
      storage: r.storage,
      server: r.server,
      clock: r.clock,
      start: false,
      seed: 61,
      heartbeatMs: 0,
      cfg: {
        conflictHandler: async (c) => {
          seen.push(`theirs=${String(c.confirmed.json("notes/n1"))}`);
          await c.submit({ keyspaceId: KS, opType: "CasSet", changes: [cas("notes/n1", `${String(c.confirmed.json("notes/n1"))}+ana`)] });
        },
      },
    });
    // offline, from the persisted copy: version 1 of the note, at seq 1
    expect(r2.engine.view(KS).json("notes/n1")).toBe("base");
    const mine = await r2.engine.submit({ keyspaceId: KS, opType: "CasSet", changes: [cas("notes/n1", "ana")] });
    expect(r2.engine.view(KS).json("notes/n1")).toBe("ana");
    r2.engine.start();
    await r2.engine.settle();
    await tick(r2, 3000);
    expect(seen).toEqual(["theirs=bobs"]); // the conflict waited for the cursor to reach Stale.current_seq ([CLI-47])
    expect(types(r2, mine.mutationId).filter((t) => t !== "held")).toEqual(["conflict", "rejected"]);
    expect(r2.engine.confirmed(KS).json("notes/n1")).toBe("bobs+ana");
    expect(r2.engine.outbox()).toEqual([]);
    expect(ofType(r2, "reloaded")).toEqual([]); // it resumed: no cold load
  });
});

describe("scenario: two tabs, a flusher that closes with work queued, and the next tab that takes it over (14 [CLI-61], [CLI-63], [CLI-64])", () => {
  it("both tabs author while the engine is offline; the flusher tab closes; the other becomes the flusher, re-stamps both tabs' actions in authoring order, and every action is applied once", async () => {
    const t = tabsRig({ heartbeatMs: 0 });
    const a = await t.open();
    const b = await t.open();
    t.server.disconnect();
    t.server.suspend(); // offline
    await settleAll(a.engine, b.engine);
    const ids: string[] = [];
    ids.push((await a.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/one", 1)] })).mutationId);
    ids.push((await b.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/two", 2)] })).mutationId); // carried to the flusher
    ids.push((await a.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/three", 3)] })).mutationId);
    await settleAll(a.engine, b.engine); // the follower reads the flusher's stores when the flusher has told it (14 [CLI-61])
    expect(b.engine.outbox().map((e) => e.mutationId)).toEqual(ids); // one shared outbox
    await a.engine.stop();
    await settleAll(b.engine);
    expect(b.engine.status().role).toBe("flusher");
    t.server.resume();
    await t.clock.advance(3_700_000);
    await settleAll(b.engine);
    for (const id of ids) expect(typesOf(b, id), id).toEqual(["committed"]);
    const sent = t.server.calls.filter((c) => c.method === "mutate").flatMap((c) => (c.request as { mutations: { mutationId: string; clientSeq: bigint; clientId: string }[] }).mutations);
    expect(sent.map((m) => m.mutationId)).toEqual(ids);
    expect(sent.map((m) => Number(m.clientSeq))).toEqual([1, 2, 3]);
    expect(new Set(sent.map((m) => m.clientId))).toEqual(new Set([b.engine.clientId]));
    expect(t.server.records(KS).filter((rec) => ids.includes(rec.mutationId))).toHaveLength(3);
  });
});

describe("scenario: a revocation that quarantines a working outbox, and the way back (14 [CLI-66], [CLI-79], [CLI-77], 12 [ADM-90])", () => {
  it("a subject is revoked while it works offline in two keyspaces: both copies are purged, every queued action reaches the app's export hook, nothing is sent, and after a new token and a new add the app re-submits them as new actions", async () => {
    const hook: ExportRecord[] = [];
    const r = await rig({ keyspaces: [KS, "ks2"], heartbeatMs: 0, cfg: { exportHook: (x) => void hook.push(x), tokens: { refresh: () => Promise.resolve(true) } } });
    r.random.fixed = 0.5;
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    r.server.write("ks2", [kc("settings/s", "set", 2)]);
    await r.engine.settle();
    r.server.disconnect();
    await r.engine.settle();
    const queued = [
      (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] })).mutationId,
      (await r.engine.submit({ keyspaceId: "ks2", opType: "Set", changes: [set("settings/b", 1)] })).mutationId,
      (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/c", 1)] })).mutationId,
    ];
    r.server.revokeSubject("ana");
    await tick(r, 2000); // the device is online again: its first call carries the notice
    expect(ofType(r, "quarantined").map((e) => e.keyspaceId).sort()).toEqual([KS, "ks2"]);
    expect(ofType(r, "quarantined").flatMap((e) => e.mutationIds).sort()).toEqual([...queued].sort());
    expect(hook.map((h) => h.mutationId).sort()).toEqual([...queued].sort());
    expect(r.engine.outbox()).toEqual([]);
    expect(r.server.calls.filter((c) => c.method === "mutate")).toHaveLength(0);
    expect(r.storage.replicaKeyspaces()).toEqual([]);
    // the user signs in again: a new token, after the revocation's leeway; the app follows its keyspaces again and re-submits what it chooses
    await tick(r, 70_000);
    (r.config.transport as FakeTransport).renewToken();
    await tick(r, 200_000);
    await r.engine.addKeyspace(KS);
    await r.engine.addKeyspace("ks2");
    await r.engine.settle();
    expect(r.engine.status().holds).toEqual([]);
    expect(r.engine.view("ks2").json("settings/s")).toBe(2);
    const again = await r.engine.resubmitExport(queued[0] as string);
    await r.engine.settle();
    expect(types(r, again.mutationId)).toEqual(["committed"]);
    expect(r.engine.exportEntries().map((x) => x.mutationId).sort()).toEqual([queued[1], queued[2]].sort());
    await r.engine.ackExport(queued[1] as string);
    await r.engine.ackExport(queued[2] as string);
    expect(r.engine.status().exports).toBe(0);
  });
});

describe("scenario: an interest that follows what the user opens (14 [CLI-70] to [CLI-73])", () => {
  it("a list shows the first project; the user opens the second: the interest widens, the new project is loaded and live; closing the first narrows it, and the restart keeps the interest", async () => {
    const r = await rig({ keyspaces: [], start: false, heartbeatMs: 0 });
    r.server.createKeyspaceNow(KS);
    for (const p of ["p1", "p2", "p3"]) r.server.write(KS, [kc(`issues/${p}`, "set", { p }), kc(`issues/${p}/title`, "set", `title ${p}`)]);
    await r.engine.addKeyspace(KS, { interest: ["issues/p1"] });
    r.engine.start();
    await r.engine.settle();
    expect(r.engine.confirmed(KS).json("issues/p1/title")).toBe("title p1");
    expect(r.engine.confirmed(KS).has("issues/p2/title")).toBe(false);
    await r.engine.setInterest(KS, ["issues/p1", "issues/p2"]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).json("issues/p2/title")).toBe("title p2");
    r.server.write(KS, [kc("issues/p2/title", "set", "renamed")]);
    r.server.write(KS, [kc("issues/p3/title", "set", "never seen")]);
    await r.engine.settle();
    expect(r.engine.view(KS).json("issues/p2/title")).toBe("renamed");
    expect(r.engine.view(KS).has("issues/p3/title")).toBe(false);
    // the user edits project 2 with an action; it retires on the record that the interest delivers
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("issues/p2/title", "mine")] });
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["committed"]);
    await r.engine.setInterest(KS, ["issues/p2"]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).has("issues/p1/title")).toBe(false);
    await r.engine.stop();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, keyspaces: [], seed: 62, heartbeatMs: 0 });
    expect(r2.engine.interest(KS)).toEqual(["issues/p2"]);
    expect(r2.engine.view(KS).json("issues/p2/title")).toBe("mine");
    expect(r2.engine.view(KS).has("issues/p1/title")).toBe(false);
  });
});
