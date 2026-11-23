// 14 §14.9 Revocation and quarantine ([CLI-66] to [CLI-69]), with [CLI-79], [CLI-50] (the revocation detail), [CLI-77] (a deleted keyspace) and
// 10 §10.7 ([AUTH-49] to [AUTH-55]), 12 [ADM-87], [ADM-90].

import { describe, expect, it } from "vitest";
import { LocalRefusal, set, type ExportRecord } from "../src/engine/index.js";
import { Code, RetryReason } from "../src/gen/abcp/v2/types_pb.js";
import { reject as mk, type FakeTransport } from "../src/fake/index.js";
import { kc, KS, mutateRequests, ofType, rejectResult, retryResult, rig, sentIds, tick, types, type Rig } from "./helpers/engine.js";
import { settleAll, tabsRig, typesOf } from "./helpers/tabs.js";

const setK = (key: string, v: unknown = 1): { keyspaceId: string; opType: string; changes: ReturnType<typeof set>[] } => ({ keyspaceId: KS, opType: "Set", changes: [set(key, v)] });
const transportOf = (r: Rig): FakeTransport => r.config.transport as FakeTransport;

/**
 * One action that is sent and answered with a Retry that holds it back (A), one that depends on it and was never sent (B), and one that
 * committed (C), so that the keyspace has an action of each kind that 14 [CLI-66] tells apart.
 */
async function threeActions(r: Rig): Promise<{ a: string; b: string; c: string }> {
  const c = (await r.engine.submit(setK("settings/c"))).mutationId;
  await r.engine.settle();
  r.server.inject("mutate", { kind: "result", result: (m) => (m.changes[0]?.key === "settings/a" ? retryResult(m, RetryReason.CONTENTION, 3_600_000) : undefined) });
  const a = (await r.engine.submit(setK("settings/a"))).mutationId;
  await r.engine.settle();
  const b = (await r.engine.submit(setK("settings/a", 2))).mutationId; // the same key: it depends on A, and waits
  await r.engine.settle();
  expect(sentIds(r)).toContain(a);
  expect(sentIds(r)).not.toContain(b);
  return { a, b, c };
}

describe("14 [CLI-66] ACCESS_REVOKED: purge the local copy, quarantine the outbox, hand it to the app", () => {
  it("the purge list goes (replica, journal, the caches that components register), an action that was never sent is quarantined and exported, one that was sent is asked about, and `quarantined` comes before `exported`", async () => {
    const hook: ExportRecord[] = [];
    const purged: string[] = [];
    const r = await rig({ cfg: { exportHook: (x) => void hook.push(x) } });
    r.engine.registerPurgeHook((ks, cause) => void purged.push(`${ks}:${cause}`));
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    const { a, b, c } = await threeActions(r);
    expect(r.engine.journalEntries().map((j) => j.mutationId)).toEqual([c]);
    expect(r.storage.replica(KS)).toBeDefined();
    const before = mutateRequests(r).length;

    r.server.revokeKeyspaces(KS);
    await r.engine.settle();

    // the purge list: replica, journal, and what components keep outside the storage
    expect(r.storage.replica(KS)).toBeUndefined();
    expect(r.engine.journalEntries()).toEqual([]);
    expect(purged).toEqual([`${KS}:ACCESS_REVOKED`]);
    expect(r.engine.status().stopped[KS]).toBe("ACCESS_REVOKED");
    expect(r.engine.status().cursors[KS]).toBeUndefined();
    expect(() => r.engine.view(KS)).toThrow(LocalRefusal);
    expect(() => r.engine.confirmed(KS)).toThrow(LocalRefusal);
    // the entries: B was never sent: quarantined. A was sent: GetMutationResult says unknown, so it is outcome unknown (A.11)
    const q = ofType(r, "quarantined");
    expect(q).toHaveLength(1);
    expect(q[0]).toMatchObject({ keyspaceId: KS, cause: "ACCESS_REVOKED", mutationIds: [b] });
    expect(types(r, b)).toEqual(["exported"]);
    expect(types(r, a)).toEqual(["outcome-unknown", "exported"]);
    expect(ofType(r, "outcome-unknown")[0]).toMatchObject({ mutationId: a, cause: "ACCESS_REVOKED" });
    const order = r.events.filter((e) => e.type === "quarantined" || (e.type === "exported" && "mutationId" in e && e.mutationId === b)).map((e) => e.type);
    expect(order).toEqual(["quarantined", "exported"]);
    expect(r.engine.outbox()).toEqual([]);
    expect(r.engine.exportEntries().map((x) => [x.mutationId, x.reason]).sort()).toEqual([[a, "ACCESS_REVOKED"], [b, "ACCESS_REVOKED"]].sort());
    expect(hook.map((x) => x.mutationId).sort()).toEqual([a, b].sort());
    // never sent automatically, never deleted unreported
    await tick(r, 3_600_000 * 3);
    expect(mutateRequests(r)).toHaveLength(before);
    expect(r.engine.exportEntries()).toHaveLength(2);
    expect(r.engine.status().exports).toBe(2);
    // an action that retired is not reported as lost
    expect(types(r, c)).toEqual(["committed"]);
  });

  it("the purge (with the record that the keyspace is revoked) is one local transaction, and the quarantine of each entry is one of its own, after it: the purge never waits for the export store (V14 MUST-2)", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    const { b } = await threeActions(r);
    r.storage.log.length = 0;
    r.server.revokeKeyspaces(KS);
    await r.engine.settle();
    const purge = r.storage.log.findIndex((ops) => ops.some((o) => o.op === "deleteReplica"));
    const quarantine = r.storage.log.findIndex((ops) => ops.some((o) => o.op === "putExport" && o.entry.mutationId === b));
    expect(purge).toBeGreaterThanOrEqual(0);
    expect(quarantine).toBeGreaterThan(purge); // the text orders them: the purge list, then the quarantine
    expect(r.storage.log[purge]?.map((o) => o.op)).toEqual(expect.arrayContaining(["deleteReplica", "deleteJournal", "setMeta"]));
    expect(r.storage.log[quarantine]?.map((o) => o.op)).toEqual(["deleteEntry", "putExport"]);
    expect(r.storage.metaValue("revoked")).toContain(KS); // a restart finds the keyspace revoked
  });

  it("an action that retires after the revocation (its outcome comes with GetMutationResult) writes no journal entry: the journal is on the purge list", async () => {
    const r = await rig();
    // the response is lost and the record never reaches the client: the action is sent with an unknown outcome that the server has recorded
    r.server.faults.paused = true;
    r.server.inject("mutate", { kind: "lose" });
    const x = await r.engine.submit(setK("settings/x"));
    await r.engine.settle();
    r.server.faults.paused = false;
    r.server.revokeKeyspaces(KS);
    await r.engine.settle();
    expect(types(r, x.mutationId)).toEqual(["committed"]); // [CLI-7]: a successful outcome retires at once, learned by GetMutationResult
    expect(r.engine.journalEntries()).toEqual([]);
    expect(r.engine.exportEntries()).toEqual([]);
  });

  it("the app may re-submit a quarantined entry once it follows the keyspace again, as a new action at its instruction; the entry is then acknowledged", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    const { b } = await threeActions(r);
    r.server.revokeKeyspaces(KS);
    await r.engine.settle();
    await expect(r.engine.resubmitExport(b)).rejects.toBeInstanceOf(LocalRefusal); // 14 [CLI-69]: the keyspace is not followed
    // the grant comes back: a token issued after the revocation (and its 60 s leeway) holds it
    await tick(r, 61_000);
    transportOf(r).renewToken();
    await r.engine.addKeyspace(KS);
    await r.engine.settle();
    expect(r.engine.status().stopped[KS]).toBeUndefined();
    expect(r.engine.view(KS).json("settings/s")).toBe(1); // a cold load: the keyspace is followed again
    const again = await r.engine.resubmitExport(b);
    await r.engine.settle();
    expect(again.mutationId).not.toBe(b);
    expect(types(r, again.mutationId)).toEqual(["committed"]);
    expect(r.engine.exportEntries().map((x) => x.mutationId)).not.toContain(b);
  });

  it("a keyspace with no entry to quarantine still gets `quarantined`, with an empty list: the app learns of the revocation and of the purge", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    r.server.revokeKeyspaces(KS);
    await r.engine.settle();
    expect(ofType(r, "quarantined")).toEqual([expect.objectContaining({ keyspaceId: KS, cause: "ACCESS_REVOKED", mutationIds: [] })]);
  });

  it("a keyspace that is still revoked stays out of reach: an add with a token issued before the revocation is answered as for a keyspace that the caller may not read, never ACCESS_REVOKED (09 [READ-32], 10 [AUTH-52])", async () => {
    const r = await rig();
    r.server.revokeKeyspaces(KS);
    await r.engine.settle();
    await r.engine.addKeyspace(KS);
    await r.engine.settle();
    expect(r.engine.status().stopped[KS]).toBe("GRANT_CHANGED");
    expect(ofType(r, "quarantined")).toHaveLength(1); // no second one for the refusal
  });

  it("the tabs of one engine share one store, so one purge covers them: a follower tab no longer shows the keyspace, and sees the quarantined entries", async () => {
    const t = tabsRig();
    const a = await t.open();
    const b = await t.open();
    expect(b.engine.view(KS)).toBeDefined();
    t.server.revokeKeyspaces(KS);
    await settleAll(a.engine, b.engine);
    expect(() => b.engine.view(KS)).toThrow(LocalRefusal);
    expect(b.engine.status().stopped[KS]).toBe("ACCESS_REVOKED");
    expect(typesOf(b)).toContain("quarantined"); // the flusher's event, relayed
    expect(t.device.storage.storageOf(a.engine.partitionKey()).replica(KS)).toBeUndefined();
  });
});

describe("14 [CLI-79], [CLI-50] SUBJECT_REVOKED: every keyspace of the subject is purged and its outbox quarantined, and the engine resumes only with a new token", () => {
  it("Closing{SUBJECT_REVOKED} after the drops: every keyspace is purged and quarantined, `quarantined` for each, the app is held; a second notice changes nothing", async () => {
    let refreshes = 0;
    const r = await rig({ keyspaces: [KS, "ks2"], cfg: { tokens: { refresh: () => (refreshes++, Promise.resolve(true)) } } });
    r.random.fixed = 0.5;
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    const { a, b } = await threeActions(r);
    r.server.revokeSubject("ana");
    await r.engine.settle();
    const q = ofType(r, "quarantined");
    // each keyspace once; the cause is the drop reason, or the close reason when a call that the revocation refused (the poll for A) told the engine first
    expect(q.map((e) => e.keyspaceId).sort()).toEqual([KS, "ks2"]);
    for (const e of q) expect(["ACCESS_REVOKED", "SUBJECT_REVOKED"]).toContain(e.cause);
    expect(q.find((e) => e.keyspaceId === KS)?.mutationIds).toEqual([b]);
    expect(q.find((e) => e.keyspaceId === "ks2")?.mutationIds).toEqual([]);
    // A was sent: it is asked about with GetMutationResult, which a token issued before the revocation cannot do: it waits for the new token
    expect(types(r, a)).toEqual([]);
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([a]);
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "app", code: "SUBJECT_REVOKED" })]);
    expect(r.engine.status().connection).toBe("offline");
    expect(Object.keys(r.engine.status().stopped).sort()).toEqual([KS, "ks2"]);
    expect(r.storage.replicaKeyspaces()).toEqual([]);
    // a token that was issued before the revocation is refused, and each refusal says why: the notice is no second revocation
    const before = mutateRequests(r).length;
    await tick(r, 31_000);
    expect(refreshes).toBeGreaterThan(0); // the app is asked for a new token, with the hold backoff
    expect(r.server.calls.filter((c) => c.method === "subscribe").length).toBeGreaterThan(1);
    expect(r.engine.status().connection).toBe("offline");
    expect(ofType(r, "quarantined")).toHaveLength(2);
    expect(mutateRequests(r)).toHaveLength(before);
    // a new token: the hold ends with the Subscribe that gets it; the keyspaces stay stopped until the app asks again (14 [CLI-70])
    await tick(r, 40_000);
    transportOf(r).renewToken();
    await tick(r, 130_000);
    expect(r.engine.status().connection).toBe("live");
    expect(r.engine.status().holds).toEqual([]);
    expect(r.engine.status().stopped[KS]).toBeDefined();
    expect(types(r, a)).toEqual(["outcome-unknown", "exported"]); // the new token asked, and the answer was unknown
    expect(r.engine.outbox()).toEqual([]);
    await r.engine.addKeyspace(KS);
    await r.engine.settle();
    expect(r.engine.view(KS).json("settings/s")).toBe(1);
  });

  it("a device that was offline when the subject was revoked learns of it from its next call: Unauthenticated with the SUBJECT_REVOKED detail purges and quarantines, before it gets a new token", async () => {
    const r = await rig();
    r.random.fixed = 0.5;
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    r.server.disconnect();
    await r.engine.settle();
    const p = await r.engine.submit(setK("settings/p")); // authored offline, never sent
    r.server.revokeSubject("ana"); // no session to tell: the device is offline
    await tick(r, 2000); // the reconnect
    expect(ofType(r, "quarantined")).toEqual([expect.objectContaining({ keyspaceId: KS, cause: "SUBJECT_REVOKED", mutationIds: [p.mutationId] })]);
    expect(r.engine.exportEntries()).toEqual([expect.objectContaining({ mutationId: p.mutationId, reason: "SUBJECT_REVOKED" })]);
    expect(r.storage.replica(KS)).toBeUndefined();
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ code: "SUBJECT_REVOKED" })]);
    expect(sentIds(r)).not.toContain(p.mutationId);
  });

  it("a call of any kind that fails with the detail is the same revocation: a Mutate refused before the body was read leaves its action never sent, quarantined, and never resent", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "status", status: "Unauthenticated", reject: mk(Code.PERMISSION_DENIED, { details: { cause: "SUBJECT_REVOKED" } }) });
    const a = await r.engine.submit(setK("settings/a"));
    await r.engine.settle();
    await tick(r, 3_600_000);
    expect(sentIds(r)).toEqual([a.mutationId]); // once
    expect(r.engine.outbox()).toEqual([]);
    expect(r.engine.exportEntries()).toEqual([expect.objectContaining({ mutationId: a.mutationId, reason: "SUBJECT_REVOKED" })]);
    expect(types(r, a.mutationId)).toEqual(["exported"]);
    expect(ofType(r, "quarantined")).toEqual([expect.objectContaining({ keyspaceId: KS, cause: "SUBJECT_REVOKED", mutationIds: [a.mutationId] })]);
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "app", code: "SUBJECT_REVOKED" })]);
  });

  it("an actor-chain match, a revoked host: `revokeSubject` of an actor that the principal's token names reaches the session as well (10 [AUTH-51])", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    expect(r.server.revokeSubject("somebody-else")).toBe(0); // no principal of the app matches
    expect(r.engine.status().connection).toBe("live");
    expect(r.server.revokeSubject("ana")).toBe(1);
    await r.engine.settle();
    expect(r.engine.status().connection).toBe("offline");
  });
});

describe("14 [CLI-67] GRANT_CHANGED: stop following; the outbox entries stay and get PERMISSION_DENIED when sent; the app's policy keeps or purges the copy", () => {
  it("keep (the default): the copy stays, the queued actions are sent and dead-lettered by [CLI-46], and the engine authors nothing more into the keyspace ([CLI-69])", async () => {
    const r = await rig({ cfg: { tokens: { refresh: () => Promise.resolve(true) } } });
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    const { a, b } = await threeActions(r);
    r.server.denyRead("ana", KS);
    await r.engine.settle();
    expect(r.engine.status().stopped[KS]).toBe("GRANT_CHANGED");
    expect(r.engine.view(KS).json("settings/s")).toBe(1); // kept
    await expect(r.engine.submit(setK("settings/z"))).rejects.toMatchObject({ code: "NOT_FOLLOWED" });
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([a, b]); // the entries stay
    await tick(r, 3_700_000); // A's backoff ends: it is sent, refused with PERMISSION_DENIED, the token is refreshed once, and it is final
    await tick(r, 10_000);
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId).sort()).toEqual([a, b].sort());
    expect(r.engine.deadLetterEntries().find((d) => d.mutationId === a)?.reason).toBe("PERMISSION_DENIED");
    expect(r.engine.deadLetterEntries().find((d) => d.mutationId === b)?.reason).toBe("DEPENDENCY_FAILED");
    expect(r.engine.exportEntries()).toEqual([]);
  });

  it("purge: the local copy goes with the policy, the outbox entries stay and are still sent", async () => {
    const r = await rig({ cfg: { copyPolicy: "purge" } });
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    const { c } = await threeActions(r);
    expect(r.engine.journalEntries().map((j) => j.mutationId)).toEqual([c]);
    r.server.denyRead("ana", KS);
    await r.engine.settle();
    expect(r.storage.replica(KS)).toBeUndefined();
    expect(r.engine.journalEntries()).toEqual([]);
    expect(() => r.engine.view(KS)).toThrow(LocalRefusal);
    expect(r.engine.outbox()).toHaveLength(2);
    await tick(r, 3_700_000);
    expect(mutateRequests(r).length).toBeGreaterThan(3); // A was sent again
  });

  it("the policy can decide for each keyspace and reason; ARCHIVED is one (14 [CLI-77]): a function that purges the archived keyspace and keeps the other", async () => {
    const seen: string[] = [];
    const r = await rig({ keyspaces: [KS, "ks2"], cfg: { copyPolicy: (ks, why) => (seen.push(`${ks}:${why}`), ks === KS ? "purge" : "keep") } });
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    r.server.write("ks2", [kc("settings/s", "set", 2)]);
    await r.engine.settle();
    r.server.archive(KS);
    r.server.archive("ks2");
    await r.engine.settle();
    expect(seen.sort()).toEqual([`${KS}:ARCHIVED`, "ks2:ARCHIVED"].sort());
    expect(() => r.engine.view(KS)).toThrow(LocalRefusal);
    expect(r.engine.view("ks2").json("settings/s")).toBe(2);
    expect(r.storage.replicaKeyspaces()).toEqual(["ks2"]);
  });
});

describe("14 [CLI-68] the token is refreshed in band before it expires; a failed refresh is no revocation", () => {
  it("10 [AUTH-48]: the engine asks the app for a new token at about 80% of its lifetime and tells the session with RefreshSession; the data and the outbox are untouched", async () => {
    let refreshes = 0;
    let expiry: number | undefined;
    const r = await rig({ cfg: { tokens: { refresh: () => (refreshes++, Promise.resolve(true)), expiresAt: () => expiry } } });
    expiry = r.clock.now() + 3_600_000;
    // the session was subscribed before the expiry was known: a new Subscribe arms the refresh
    r.server.reconnect();
    await tick(r, 2000);
    expiry = r.clock.now() + 3_600_000 - 2000;
    await r.engine.settle();
    expect(refreshes).toBe(0);
    // 10 [AUTH-48]: at about 80% of the lifetime measured from receipt (the Subscribe, ~2 s ago): 2,880 s, well before the
    // old 60-s lead
    await tick(r, 2_880_000 - 2000 - 3000);
    expect(refreshes).toBe(0);
    await tick(r, 6000);
    expect(refreshes).toBe(1);
    const calls = r.server.calls.filter((c) => c.method === "refreshSession");
    expect(calls).toHaveLength(1);
    expect((calls[0]?.request as { sessionId: string }).sessionId).toMatch(/^session-/);
    expect(r.engine.status().holds).toEqual([]);
    expect(r.engine.status().connection).toBe("live");
  });

  it("a refresh that fails is not a revocation: the hold NO_TOKEN, the data and the outbox are kept, and the engine goes on when the app has a token", async () => {
    let ok = false;
    let calls = 0;
    let expiry: number | undefined;
    const r = await rig({ cfg: { tokens: { refresh: () => (calls++, Promise.resolve(ok)), expiresAt: () => expiry } } });
    expiry = r.clock.now() + 600_000;
    r.random.fixed = 0.5;
    r.server.reconnect();
    await tick(r, 2000);
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    const x = await r.engine.submit(setK("settings/x"));
    await r.engine.settle();
    await tick(r, 600_000 - 60_000 - 2000 + 100);
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "app", code: "NO_TOKEN" })]);
    expect(ofType(r, "quarantined")).toEqual([]);
    expect(r.engine.view(KS).json("settings/s")).toBe(1);
    expect(types(r, x.mutationId)).toEqual(["committed"]);
    ok = true;
    const refreshSessions = r.server.calls.filter((c) => c.method === "refreshSession").length;
    await tick(r, 3_700_000);
    expect(r.engine.status().holds).toEqual([]);
    expect(r.server.calls.filter((c) => c.method === "refreshSession").length).toBeGreaterThan(refreshSessions); // the live session is told
    expect(calls).toBeGreaterThan(1);
  });

  it("the new token no longer grants a keyspace: the KeyspaceDropped{GRANT_CHANGED} frame is what the engine acts on, and the response's list is informational", async () => {
    let expiry: number | undefined;
    const r = await rig({ keyspaces: [KS, "ks2"], cfg: { tokens: { refresh: () => Promise.resolve(true), expiresAt: () => expiry } } });
    expiry = r.clock.now() + 3_600_000;
    r.server.reconnect();
    await tick(r, 2000);
    r.server.write("ks2", [kc("settings/s", "set", 4)]);
    await r.engine.settle();
    r.server.denyRead("ana", "ks2", { drop: false }); // noticed at the next refresh
    expiry = r.clock.now() + 3_600_000;
    await tick(r, 3_600_000 - 60_000 + 10);
    expect(r.engine.status().stopped).toEqual({ ks2: "GRANT_CHANGED" });
    expect(r.engine.status().connection).toBe("live"); // the session and its other keyspaces go on
    expect(r.engine.view(KS)).toBeDefined();
  });
});

describe("14 [CLI-77] DELETED: the purge list, and the dead-letter and export entries of the keyspace, each handed to the app once more first", () => {
  it("an entry that the app has not acknowledged is handed to it again and purged; the entries that the deletion itself writes stay; the replica and the journal go", async () => {
    const hook: ExportRecord[] = [];
    const r = await rig({ cfg: { exportHook: (x) => void hook.push(x) } });
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.changes[0]?.key === "settings/dead" ? rejectResult(m, mk(Code.SCHEMA_INVALID)) : undefined) });
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.changes[0]?.key === "settings/old" ? rejectResult(m, mk(Code.OFFLINE_TOO_LONG, { details: { cause: "HORIZON" } })) : undefined) });
    const dead = await r.engine.submit(setK("settings/dead"));
    const old = await r.engine.submit(setK("settings/old"));
    await r.engine.settle();
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual([dead.mutationId]);
    expect(r.engine.exportEntries().map((d) => d.mutationId)).toEqual([old.mutationId]);
    const { a, b } = await threeActions(r);
    const eventsBefore = r.events.length;
    hook.length = 0;
    r.server.deleteKeyspace(KS);
    await r.engine.settle();
    const after = r.events.slice(eventsBefore);
    // handed to the app once more: `dead-lettered` for the dead-letter entry and `exported` for the export entry
    expect(after.filter((e) => e.type === "dead-lettered").map((e) => ("mutationId" in e ? e.mutationId : ""))).toEqual(expect.arrayContaining([dead.mutationId]));
    expect(after.filter((e) => e.type === "exported").map((e) => ("mutationId" in e ? e.mutationId : ""))).toEqual(expect.arrayContaining([old.mutationId]));
    expect(hook.map((x) => x.mutationId)).toContain(old.mutationId);
    // and purged: the entries that the endings of the deletion wrote stay
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual([b]); // never sent: dead-lettered at once with KEYSPACE_DELETED
    expect(r.engine.deadLetterEntries()[0]?.reason).toBe("KEYSPACE_DELETED");
    expect(r.engine.exportEntries().map((d) => d.mutationId)).toEqual([a]); // sent, outcome unknown: the export hook (14 [CLI-7], [CLI-83])
    expect(r.storage.replica(KS)).toBeUndefined();
    expect(r.engine.journalEntries()).toEqual([]);
    expect(() => r.engine.view(KS)).toThrow(LocalRefusal);
    expect(r.engine.status().stopped[KS]).toBe("DELETED");
    expect(r.engine.outbox()).toEqual([]);
  });
});
