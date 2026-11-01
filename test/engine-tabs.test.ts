// 14 §14.8 Principals, tabs and devices: [CLI-59] to [CLI-65], with 10 §10.3 ([AUTH-19] to [AUTH-25]) and 03 [ID-35].

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { jsonBytes, LocalRefusal, set, type Engine, type EngineEvent, type ExportRecord } from "../src/engine/index.js";
import type { MutateRequestSchema } from "../src/gen/abpc/v2/sync_pb.js";
import { ChangeKind, Code, KeyChangeSchema, RetryReason } from "../src/gen/abpc/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { goOffline, goOnline, KS, rejectResult, retryResult, rig, types } from "./helpers/engine.js";
import { ORIGIN, settleAll, tabsRig, typesOf, type TabsRig } from "./helpers/tabs.js";

const ana = { issuer: "https://issuer.test", sub: "ana", deviceId: "ana-device" };
const subscribes = (server: { calls: { method: string; request: unknown }[] }): { clientId: string; keyspaces: { keyspaceId: string }[] }[] =>
  server.calls.filter((c) => c.method === "subscribe").map((c) => c.request as never);
const setA = (key = "settings/a", v: unknown = 1): { keyspaceId: string; opType: string; changes: ReturnType<typeof set>[] } => ({ keyspaceId: KS, opType: "Set", changes: [set(key, v)] });

describe("14 [CLI-59], [CLI-74]: a separate outbox, dead-letter store and replica per principal, keyed by origin, app and partition; every entry tagged with its author", () => {
  it("two principals on one device have two partitions, and an engine reads only its own; two origins are two partitions too", async () => {
    const r = tabsRig();
    const a = await r.open({ sub: "ana" });
    const b = await r.open({ sub: "bob" });
    expect(a.engine.partitionKey()).toEqual({ origin: ORIGIN, appId: "app", partition: "https://issuer.test|ana" });
    expect(b.engine.partitionKey()).toEqual({ origin: ORIGIN, appId: "app", partition: "https://issuer.test|bob" });
    await goOfflineAll(r, a.engine, b.engine);
    await a.engine.submit(setA());
    expect(a.engine.outbox()).toHaveLength(1);
    expect(b.engine.outbox()).toHaveLength(0);
    const o2 = await r.open({ sub: "ana", origin: "https://other.test" });
    expect(o2.engine.outbox()).toHaveLength(0); // the same principal on another server origin shares nothing
    const keys = await a.engine.partitions();
    expect(keys.map((k) => `${k.origin}|${k.partition}`).sort()).toEqual([`${ORIGIN}|https://issuer.test|ana`, `${ORIGIN}|https://issuer.test|bob`].sort());
    expect((await r.device.storage.list("https://other.test", "app")).map((k) => k.partition)).toEqual(["https://issuer.test|ana"]);
  });

  it("every outbox, dead-letter, journal and export-hook entry is tagged with its author's subject and device", async () => {
    const hook: ExportRecord[] = [];
    const r = tabsRig();
    const a = await r.open({ sub: "ana", cfg: { exportHook: (x) => void hook.push(x) } });
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.changes[0]?.key === "settings/dead" ? rejectResult(m, mk(Code.SCHEMA_INVALID)) : undefined) });
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.changes[0]?.key === "settings/old" ? rejectResult(m, mk(Code.OFFLINE_TOO_LONG, { details: { cause: "HORIZON" } })) : undefined) });
    await goOfflineAll(r, a.engine);
    const pending = await a.engine.submit(setA("settings/pending"));
    expect(a.engine.outbox()[0]?.author).toEqual(ana);
    await a.engine.submit(setA("settings/ok"));
    await a.engine.submit(setA("settings/dead"));
    await a.engine.submit(setA("settings/old"));
    r.server.reconnect();
    await tick2(r, a.engine, 3000);
    expect(a.engine.journalEntries().find((j) => j.opType === "Set")?.author).toEqual(ana);
    expect(a.engine.deadLetterEntries()[0]?.author).toEqual(ana);
    expect(a.engine.exportEntries()[0]?.author).toEqual(ana);
    expect(pending.mutationId).toBeTruthy();
    expect(hook).toHaveLength(1);
  });

  it("shared-device mode: the partition is the device, and the engine's principal is the device's (10 [AUTH-28])", async () => {
    const r = tabsRig();
    const till = await r.open({ sub: "till-7", deviceId: "till-7-device", cfg: { mode: "shared-device" } });
    expect(till.engine.partitionKey().partition).toBe("till-7-device");
    expect(till.engine.status().role).toBe("flusher");
  });
});

describe("14 [CLI-60]: only the entries whose author is the current principal are sent; the others stay queued and locked", () => {
  it("a partition that holds another author's entries keeps them in place, never shows or sends them, and sends them again when their author is current", async () => {
    const r = tabsRig();
    const till1 = await r.open({ sub: "till-1", deviceId: "till", cfg: { mode: "shared-device" } });
    await goOfflineAll(r, till1.engine);
    const x = await till1.engine.submit(setA("settings/x"));
    await till1.engine.stop();
    // another person on the same device: the partition is the device's, its entries are till-1's
    const till2 = await r.open({ sub: "till-2", deviceId: "till", cfg: { mode: "shared-device" } });
    expect(till2.engine.status()).toMatchObject({ role: "flusher", lockedEntries: 1, pending: 0 });
    expect(till2.engine.outbox()).toHaveLength(0);
    expect(till2.engine.locked().map((e) => e.mutationId)).toEqual([x.mutationId]);
    await till2.engine.submit(setA("settings/y"));
    await tick2(r, till2.engine, 3000);
    expect(r.server.calls.filter((c) => c.method === "mutate" && c.sub === "till-2")).toHaveLength(1);
    const sentIds = mutateRequests2(r).flatMap((q) => q.mutations.map((m) => m.mutationId));
    expect(sentIds).not.toContain(x.mutationId); // never sent under another identity
    const stored = r.device.storage.storageOf(till2.engine.partitionKey());
    expect(stored.entry(x.mutationId)?.author.sub).toBe("till-1"); // kept in place
    await till2.engine.stop();
    const again = await r.open({ sub: "till-1", deviceId: "till", cfg: { mode: "shared-device" }, start: false });
    expect(again.engine.status().lockedEntries).toBe(0);
    expect(again.engine.outbox().map((e) => e.mutationId)).toEqual([x.mutationId]);
    again.engine.start();
    await tick2(r, again.engine, 3000);
    expect(typesOf(again, x.mutationId)).toEqual(["committed"]);
  });
});

describe("14 [CLI-61]: the tabs of one engine share one outbox through a single flusher elected by a cross-tab lock", () => {
  it("the first tab is the flusher and opens the session; the second is a follower, opens none, and is told so by `role`", async () => {
    const r = tabsRig();
    const a = await r.open();
    const b = await r.open();
    expect(a.engine.status().role).toBe("flusher");
    expect(b.engine.status().role).toBe("follower");
    expect(typesOf(a)).toContain("role");
    expect(a.events.find((e) => e.type === "role")).toMatchObject({ role: "flusher", cause: "ELECTED" });
    expect(b.events.find((e) => e.type === "role")).toMatchObject({ role: "follower", cause: "FOLLOWING" });
    expect(subscribes(r.server)).toHaveLength(1); // one session for the engine (09 §9.4)
    expect(subscribes(r.server)[0]?.clientId).toBe(a.engine.clientId);
    expect(b.engine.clientId).toBe(""); // a follower has no client instance
    expect(b.engine.status().connection).toBe("live"); // the flusher's runtime, read through it
    expect(b.events.filter((e) => e.type === "role")).toHaveLength(1); // a tab's role is its own: the flusher's `role` is not relayed
  });

  it("a follower reads through the flusher: the confirmed state, the pending actions, the events and the status are the flusher's", async () => {
    const r = tabsRig();
    const a = await r.open();
    const b = await r.open();
    r.server.write(KS, [{ ...kcSet("settings/s", 5) }]);
    await settleAll(a.engine, b.engine);
    expect(b.engine.view(KS).json("settings/s")).toBe(5);
    expect(b.engine.cursor(KS)?.seq).toBe(1n);
    await goOfflineAll(r, a.engine);
    await settleAll(a.engine, b.engine);
    expect(b.engine.status().connection).toBe("offline");
    const x = await a.engine.submit(setA("settings/p", 9));
    await settleAll(a.engine, b.engine);
    expect(b.engine.status().pending).toBe(1);
    expect(b.engine.view(KS).json("settings/p")).toBe(9); // the prediction of the shared outbox
    expect(b.engine.confirmed(KS).has("settings/p")).toBe(false);
    r.server.reconnect();
    await r.clock.advance(2000);
    await settleAll(a.engine, b.engine);
    expect(typesOf(b, x.mutationId)).toEqual(["committed"]); // the flusher's event, relayed
    expect(b.engine.status().pending).toBe(0);
    expect(b.engine.view(KS).json("settings/p")).toBe(9);
  });

  it("a follower's action is the flusher's to author and store: it is sent once, under the flusher's client_id, and the follower's call resolves when it is in the durable outbox", async () => {
    const r = tabsRig();
    const a = await r.open();
    const b = await r.open();
    await goOfflineAll(r, a.engine);
    const x = await b.engine.submit(setA("settings/f", 3));
    expect(b.engine.outbox().map((e) => e.mutationId)).toEqual([x.mutationId]); // in the shared store, and shown at once
    expect(r.device.storage.storageOf(a.engine.partitionKey()).entry(x.mutationId)?.author.sub).toBe("ana");
    expect(a.engine.outbox().map((e) => e.mutationId)).toEqual([x.mutationId]);
    r.server.reconnect();
    await r.clock.advance(2000);
    await settleAll(a.engine, b.engine);
    const reqs = mutateRequests2(r).filter((q) => q.mutations.some((m) => m.mutationId === x.mutationId));
    expect(reqs).toHaveLength(1);
    expect(reqs[0]?.mutations[0]?.clientId).toBe(a.engine.clientId);
    expect(typesOf(b, x.mutationId)).toEqual(["committed"]);
    // cancel and the app's acknowledgements go the same way
    await goOfflineAll(r, a.engine);
    const y = await b.engine.submit(setA("settings/g", 1));
    expect(await b.engine.cancel(y.mutationId)).toBe(true);
    await settleAll(a.engine, b.engine);
    expect(b.engine.outbox()).toHaveLength(0);
    expect(typesOf(b, y.mutationId)).toEqual(["rejected"]);
    expect(b.engine.rejectionEntries()).toHaveLength(1);
    await b.engine.ackRejection(y.mutationId);
    expect(a.engine.rejectionEntries()).toHaveLength(0);
  });

  it("a follower that gets no answer from a flusher refuses the call with the typed local error NO_FLUSHER; nothing was accepted", async () => {
    const r = tabsRig();
    const a = await r.open();
    const b = await r.open();
    a.engine.kill(); // the flusher is gone, and the platform has not freed its lock (yet)
    const p = b.engine.submit(setA("settings/lost", 1));
    const settled = p.then(
      () => "accepted",
      (e: unknown) => (e instanceof LocalRefusal ? e.code : "other"),
    );
    await r.clock.advance(5000);
    expect(await settled).toBe("NO_FLUSHER");
    expect(b.engine.outbox()).toHaveLength(0);
  });

  it("a follower has no view document of a CRDT key: it is the flusher tab's", async () => {
    const r = tabsRig();
    await r.open();
    const b = await r.open();
    expect(() => b.engine.crdt(KS, "docs/d1")).toThrow(LocalRefusal);
  });

  it("where no reliable cross-tab lock exists each tab is an engine of its own: its own outbox, session and client_id", async () => {
    const r = tabsRig();
    const a = await r.open({ tab: { reliableLocks: false } });
    const b = await r.open({ tab: { reliableLocks: false } });
    expect(a.engine.status().role).toBe("flusher");
    expect(b.engine.status().role).toBe("flusher");
    expect(a.engine.partitionKey().tab).toBe(a.tab.tabId);
    expect(b.engine.partitionKey().tab).toBe(b.tab.tabId);
    expect(a.engine.clientId).not.toBe(b.engine.clientId);
    expect(subscribes(r.server).map((s) => s.clientId).sort()).toEqual([a.engine.clientId, b.engine.clientId].sort());
    await goOfflineAll(r, a.engine, b.engine);
    const x = await a.engine.submit(setA("settings/a"));
    expect(b.engine.outbox()).toHaveLength(0);
    expect(a.engine.outbox().map((e) => e.mutationId)).toEqual([x.mutationId]);
  });

  it("two flushers never drain one outbox: with one lock, only the engine that holds it sends", async () => {
    const r = tabsRig();
    const a = await r.open();
    const b = await r.open();
    const c = await r.open();
    await goOfflineAll(r, a.engine);
    const ids: string[] = [];
    for (const t of [a, b, c]) ids.push((await t.engine.submit(setA(`settings/${t.tab.tabId}`))).mutationId);
    r.server.reconnect();
    await r.clock.advance(2000);
    await settleAll(a.engine, b.engine, c.engine);
    const senders = new Set(mutateRequests2(r).flatMap((q) => q.mutations.map((m) => m.clientId)));
    expect([...senders]).toEqual([a.engine.clientId]);
    ids.forEach((id) => expect(typesOf(a, id)).toEqual(["committed"]));
    expect(r.device.lockManager.waiting(`abpc.flusher\u0000${ORIGIN}\u0000app\u0000https://issuer.test|ana`)).toBe(2);
  });

  it("a hidden flusher hands its role over to a visible tab, when there is one (and keeps it when there is none)", async () => {
    const r = tabsRig();
    const a = await r.open({ cfg: { handOverWhenHidden: true } });
    a.tab.setVisible(false);
    await settleAll(a.engine);
    expect(a.engine.status().role).toBe("flusher"); // no visible tab to hand it to
    const b = await r.open({ cfg: { handOverWhenHidden: true } });
    expect(a.engine.peerTabs()).toEqual([{ tabId: b.tab.tabId, visible: true }]);
    a.tab.setVisible(true);
    a.tab.setVisible(false); // hidden now, with a visible follower
    await settleAll(a.engine, b.engine);
    expect(a.engine.status().role).toBe("stopped");
    expect(a.events.at(-1)).toMatchObject({ type: "role", role: "stopped", cause: "HANDED_OVER" });
    expect(b.engine.status().role).toBe("flusher");
  });
});

describe("14 [CLI-63], [CLI-64], 03 [ID-35], 10 [AUTH-19], [AUTH-21]: a new client_id at every start and every flusher handover, re-stamping, and nothing applied twice", () => {
  it("the next tab in line becomes the flusher when the flusher closes: a new client_id, the queued actions re-stamped from 1 in authoring order with the same mutation ids and content", async () => {
    const r = tabsRig();
    const a = await r.open();
    const b = await r.open({ start: false });
    await goOfflineAll(r, a.engine);
    const ids: string[] = [];
    for (const k of ["a", "b", "c"]) ids.push((await a.engine.submit(setA(`settings/${k}`))).mutationId);
    const c1 = a.engine.clientId;
    const contents = a.engine.outbox().map((e) => e.content);
    await a.engine.stop();
    await settleAll(b.engine);
    expect(b.engine.status().role).toBe("flusher");
    expect(b.events.find((e) => e.type === "role" && e.role === "flusher")).toMatchObject({ cause: "ELECTED" });
    expect(b.engine.clientId).not.toBe("");
    expect(b.engine.clientId).not.toBe(c1);
    expect(b.engine.outbox().map((e) => e.mutationId)).toEqual(ids);
    expect(b.engine.outbox().map((e) => e.content)).toEqual(contents); // 14 [CLI-22]: the content is never rewritten
    b.engine.start();
    await r.clock.advance(2000);
    await settleAll(b.engine);
    const last = mutateRequests2(r).at(-1);
    expect(last?.mutations.map((m) => m.mutationId)).toEqual(ids);
    expect(last?.mutations.map((m) => Number(m.clientSeq))).toEqual([1, 2, 3]);
    expect(new Set(last?.mutations.map((m) => m.clientId))).toEqual(new Set([b.engine.clientId]));
    ids.forEach((id) => expect(typesOf(b, id)).toContain("committed"));
    expect(subscribes(r.server).at(-1)?.clientId).toBe(b.engine.clientId); // 14 [CLI-78]
  });

  it("a handover in the middle of a send: the flusher's tab crashes after its request was applied and before it heard; the next tab resends under a new client_id and the server answers from its index, so nothing is applied twice", async () => {
    const r = tabsRig();
    const a = await r.open();
    const b = await r.open();
    r.server.inject("mutate", { kind: "lose" }); // applied, and the response is lost: an unknown outcome
    r.server.faults.paused = true; // and its record does not reach the flusher either
    const x = await a.engine.submit(setA("settings/x", 1));
    await a.engine.settle();
    expect(a.engine.outbox()[0]).toMatchObject({ everSent: true });
    expect(r.server.records(KS).filter((rec) => rec.mutationId === x.mutationId)).toHaveLength(1);
    await goOfflineAll(r, a.engine);
    // three actions are queued behind it, never sent (the example of 14 §14.8)
    const queued: string[] = [];
    for (const k of ["q1", "q2", "q3"]) queued.push((await a.engine.submit(setA(`settings/${k}`))).mutationId);
    const c1 = a.engine.clientId;
    a.tab.crash(); // the tab is killed: its lock is freed, nothing is released by its engine
    a.engine.kill();
    r.server.faults.paused = false;
    await settleAll(b.engine);
    await r.clock.advance(2000);
    await settleAll(b.engine);
    expect(b.engine.status().role).toBe("flusher");
    expect(b.engine.clientId).not.toBe(c1);
    const resend = mutateRequests2(r).at(-1);
    expect(resend?.mutations.map((m) => m.mutationId)).toEqual([x.mutationId, ...queued]);
    expect(resend?.mutations.map((m) => Number(m.clientSeq))).toEqual([1, 2, 3, 4]); // client_seq 1 to 4 of the new instance
    expect(r.server.records(KS).filter((rec) => rec.mutationId === x.mutationId)).toHaveLength(1); // applied once
    for (const id of [x.mutationId, ...queued]) expect(typesOf(b, id)).toContain("committed");
    expect(b.engine.outbox()).toHaveLength(0);
    expect(r.server.value(KS, "settings/q3")).toBeDefined();
  });

  it("a GATED action that the server registered survives a handover: the next tab resends it under its own client_id, the server binds the new slot to the same intent (07 [MUT-39]), and the decision reaches the new flusher", async () => {
    const r = tabsRig();
    const a = await r.open();
    const b = await r.open();
    const g = await a.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("claims/c1", 1)] });
    await settleAll(a.engine, b.engine);
    expect(r.server.openItems(KS).pendingIntents).toBe(1); // registered, pending: the gate has not decided
    const c1 = a.engine.clientId;
    a.tab.crash();
    a.engine.kill();
    await settleAll(b.engine);
    await r.clock.advance(3000);
    await settleAll(b.engine);
    expect(b.engine.clientId).not.toBe(c1);
    expect(r.server.openItems(KS).pendingIntents).toBe(1); // one intent, never two: the resend is a duplicate of the registered one
    const resent = mutateRequests2(r).filter((q) => q.mutations.some((m) => m.mutationId === g.mutationId));
    expect(resent.map((q) => q.mutations[0]?.clientId)).toEqual([c1, b.engine.clientId]);
    r.server.decide(KS, g.mutationId, { status: "COMMITTED" });
    await settleAll(b.engine);
    expect(typesOf(b, g.mutationId)).toEqual(["committed"]);
    expect(b.engine.outbox()).toEqual([]);
  });

  it("an engine start mints a new client_id, and it is never persisted: no stored entry, head or meta carries it", async () => {
    const r = await rig();
    const x = await r.engine.submit(setA("settings/a"));
    await r.engine.settle();
    expect(x.mutationId).toBeTruthy();
    const c1 = r.engine.clientId;
    expect(c1).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/); // 03 [ID-35]
    await goOffline(r);
    const y = await r.engine.submit(setA("settings/b"));
    expect(r.storage.entry(y.mutationId)?.clientId).toBe("");
    expect(JSON.stringify(r.storage.log, (_k, v: unknown) => (typeof v === "bigint" ? v.toString() : v))).not.toContain(c1);
    await r.engine.stop();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 77 });
    expect(r2.engine.clientId).not.toBe(c1);
  });

  it("a handover re-stamps only what has no definitive result: an action with an outcome keeps it and retires on the cursor", async () => {
    const r = tabsRig();
    const a = await r.open();
    const b = await r.open();
    r.server.faults.paused = true; // the record is held back: the action has its Appended and not its record
    const x = await a.engine.submit(setA("settings/x"));
    await a.engine.settle();
    expect(a.engine.outbox()[0]?.outcome).toMatchObject({ kind: "appended" });
    await a.engine.stop();
    r.server.resumeDelivery();
    await settleAll(b.engine);
    await r.clock.advance(2000);
    await settleAll(b.engine);
    expect(typesOf(b, x.mutationId)).toContain("committed");
    expect(mutateRequests2(r).filter((q) => q.mutations.some((m) => m.mutationId === x.mutationId))).toHaveLength(1); // never sent again
  });
});

describe("14 [CLI-62]: the tabs and devices of one user are concurrent writers", () => {
  it("there is no read-your-writes across devices until the write's confirmed seq is observed", async () => {
    const a = await rig({ seed: 1 });
    const b = await rig({ server: a.server, clock: a.clock, seed: 2, start: false }); // device b is offline
    const x = await a.engine.submit(setA("settings/shared", 1));
    await a.engine.settle();
    expect(types(a, x.mutationId)).toEqual(["committed"]);
    expect(b.engine.view(KS).has("settings/shared")).toBe(false); // device b has not observed the seq
    expect(b.engine.cursor(KS)?.seq).toBe(0n);
    b.engine.start();
    await b.engine.settle();
    expect(b.engine.view(KS).json("settings/shared")).toBe(1);
    expect(b.engine.cursor(KS)?.seq).toBe(1n);
  });
});

describe("14 [CLI-65]: sign-out flushes and then purges (personal mode) or locks (shared-device mode), and never purges unsynced entries silently", () => {
  it("online with nothing unsynced: the partition is purged and the engine stops", async () => {
    const r = tabsRig();
    const a = await r.open();
    await a.engine.submit(setA("settings/a"));
    await settleAll(a.engine); // online: the action is sent and retires
    expect(await a.engine.flush()).toBe(0);
    const res = await a.engine.signOut();
    expect(res).toEqual({ done: true, outcome: "purged", unsynced: 0 });
    expect(a.engine.status().role).toBe("stopped");
    expect(await r.device.storage.list(ORIGIN, "app")).toEqual([]);
  });

  it("with unsynced entries it offers a choice and purges nothing; the user keeps them locked, and their author finds them again", async () => {
    const r = tabsRig();
    const a = await r.open();
    await goOfflineAll(r, a.engine);
    const x = await a.engine.submit(setA("settings/a"));
    const ask = await a.engine.signOut();
    expect(ask).toEqual({ done: false, unsynced: 1, options: ["keep-locked", "discard"] });
    expect(a.engine.status().role).toBe("flusher");
    expect(a.engine.outbox()).toHaveLength(1);
    expect(await a.engine.signOut({ unsynced: "keep-locked" })).toEqual({ done: true, outcome: "locked", unsynced: 1 });
    expect(await r.device.storage.list(ORIGIN, "app")).toHaveLength(1);
    const again = await r.open({ start: false });
    expect(again.engine.outbox().map((e) => e.mutationId)).toEqual([x.mutationId]);
  });

  it("a discard reports every entry to the app's export hook first, and refuses when the engine has no hook, since it would be silent", async () => {
    const hook: ExportRecord[] = [];
    const r = tabsRig();
    const noHook = await r.open();
    await goOfflineAll(r, noHook.engine);
    await noHook.engine.submit(setA("settings/a"));
    await expect(noHook.engine.signOut({ unsynced: "discard" })).rejects.toBeInstanceOf(LocalRefusal);
    expect(noHook.engine.outbox()).toHaveLength(1);
    await noHook.engine.stop();
    const r2 = tabsRig();
    const a = await r2.open({ cfg: { exportHook: (x) => void hook.push(x) } });
    await goOfflineAll(r2, a.engine);
    const x = await a.engine.submit(setA("settings/a"));
    const res = await a.engine.signOut({ unsynced: "discard" });
    expect(res).toEqual({ done: true, outcome: "discarded", unsynced: 1 });
    expect(hook.map((h) => [h.mutationId, h.reason])).toEqual([[x.mutationId, "SIGN_OUT"]]);
    expect(a.events.filter((e: EngineEvent) => e.type === "exported")).toHaveLength(1);
    expect(await r2.device.storage.list(ORIGIN, "app")).toEqual([]);
  });

  it("shared-device mode locks the partition instead of purging it", async () => {
    const r = tabsRig();
    const till = await r.open({ sub: "till-1", deviceId: "till", cfg: { mode: "shared-device" } });
    expect(await till.engine.signOut()).toEqual({ done: true, outcome: "locked", unsynced: 0 });
    expect(await r.device.storage.list(ORIGIN, "app")).toHaveLength(1);
  });
});

describe("a Retry that keeps the lane busy does not hand the flusher role anywhere", () => {
  it("the flusher keeps its role while its actions are retried", async () => {
    const r = tabsRig();
    const a = await r.open();
    const b = await r.open();
    r.server.inject("mutate", { kind: "result", result: (m) => retryResult(m, RetryReason.CONTENTION, 1000) });
    await a.engine.submit(setA("settings/a"));
    await settleAll(a.engine, b.engine);
    expect(a.engine.status().role).toBe("flusher");
    expect(b.engine.status().role).toBe("follower");
  });
});

// ---------------------------------------------------------------------------------------------------------------------

function kcSet(key: string, v: unknown): ReturnType<typeof create<typeof KeyChangeSchema>> {
  return create(KeyChangeSchema, { key, kind: ChangeKind.SET, value: jsonBytes(v), encoding: "json/v1" });
}

/** The server drops every session: the engines are offline until the clock moves. */
async function goOfflineAll(r: TabsRig, ...engines: Engine[]): Promise<void> {
  r.server.disconnect();
  await settleAll(...engines);
}

async function tick2(r: TabsRig, e: Engine, ms: number): Promise<void> {
  await r.clock.advance(ms);
  await settleAll(e);
}

function mutateRequests2(r: TabsRig): ReturnType<typeof create<typeof MutateRequestSchema>>[] {
  return r.server.calls.filter((c) => c.method === "mutate").map((c) => c.request as never);
}

