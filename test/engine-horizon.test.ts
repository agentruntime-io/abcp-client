// 14 §14.7 Offline horizon: [CLI-54] to [CLI-58] (and [CLI-82], tested with the holds), with 07 §7.10 ([MUT-75], [MUT-84], [MUT-88], [MUT-89]) and
// SPEC-QUESTIONS Q32 (the floor of `max_offline_seconds` is 3,600 s).

import { describe, expect, it } from "vitest";
import { set, type ExportRecord } from "../src/engine/index.js";
import { FakeServer, ManualClock } from "../src/fake/index.js";
import { StaticManifests } from "../src/engine/index.js";
import { Code, RetryReason } from "../src/gen/abcp/v2/types_pb.js";
import { contentOf, kc, KS, MANIFEST, mutateRequests, ofType, retryResult, rig, sentIds, tick, types, type Rig } from "./helpers/engine.js";

const DAY = 24 * 3600 * 1000;
const setK = (key: string, v: unknown = 1): { keyspaceId: string; opType: string; changes: ReturnType<typeof set>[] } => ({ keyspaceId: KS, opType: "Set", changes: [set(key, v)] });
const warnings = (r: Rig): { kind?: string; ageMs: number; maxOfflineMs: number; keyspaceId: string; oldestMutationId?: string }[] => ofType(r, "offline-horizon");
const limits = (days: number): { serverOptions: { limits: Record<string, number> }; heartbeatMs: number } => ({ serverOptions: { limits: { maxOfflineSeconds: days * 24 * 3600 } }, heartbeatMs: 0 });

describe("14 [CLI-54]: the horizon comes from AppLimits.max_offline_seconds and is kept with the stored state", () => {
  it("3,600 s until a value arrives (the floor, 07 [MUT-75], Q32); then the server's value; and it is stored, so that it applies while the client is offline after a restart", async () => {
    const r = await rig({ start: false, heartbeatMs: 0 });
    expect(r.engine.limits.maxOfflineMs).toBe(3_600_000);
    r.engine.start();
    await r.engine.settle();
    expect(r.engine.limits.maxOfflineMs).toBe(30 * DAY); // `LoadStateResponse.limits` and `Subscribed.limits` carry it
    await r.engine.stop();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, start: false, seed: 5 });
    expect(r2.engine.limits.maxOfflineMs).toBe(30 * DAY); // before any server data of the new run
    expect(r.storage.metaValue("maxOfflineSeconds")).toBe(30 * 24 * 3600);
  });

  it("a server that sends 0 gets the floor of 3,600 s and a `protocol-error` (07 [MUT-75]), and the horizon follows the next non-zero value", async () => {
    const r = await rig({ serverOptions: { limits: { maxOfflineSeconds: 0 } }, heartbeatMs: 0 });
    expect(r.engine.limits.maxOfflineMs).toBe(3_600_000);
    expect(ofType(r, "protocol-error").filter((e) => e.what === "AppLimits.max_offline_seconds")).toHaveLength(1);
    r.server.limits.maxOfflineSeconds = 7200;
    r.server.reconnect();
    await r.engine.settle();
    await tick(r, 2000);
    expect(r.engine.limits.maxOfflineMs).toBe(7_200_000);
  });
});

describe("14 [CLI-55]: ages are measured in server time, kept across restarts", () => {
  it("an action's age counts from the server time that the client estimated when it authored it, not from the device's wall clock; after a restart the estimate is the wall clock plus the persisted offset until the first server data", async () => {
    const serverClock = new ManualClock(Date.UTC(2026, 5, 1, 12, 0, 0));
    const deviceClock = new ManualClock(Date.UTC(2026, 5, 1, 10, 0, 0)); // the device is two hours behind the server
    const manifests = new StaticManifests(MANIFEST);
    const server = new FakeServer({ manifests, clock: serverClock, heartbeatMs: 0 });
    server.createKeyspaceNow(KS);
    const r = await rig({ server, clock: deviceClock, manifests, start: false });
    server.write(KS, [kc("settings/a", "set", 1)]);
    r.engine.start();
    await r.engine.settle();
    server.heartbeat(); // server data: a heartbeat carries the server's `server_time` (09 [READ-54])
    await r.engine.settle();
    expect(Math.abs(r.engine.serverNow() - serverClock.now())).toBeLessThan(1000);
    // both clocks tick on
    await serverClock.advance(60_000);
    await deviceClock.advance(60_000);
    expect(Math.abs(r.engine.serverNow() - serverClock.now())).toBeLessThan(1000); // monotonic clock plus the last server time
    await r.engine.stop();
    // a restart resets the monotonic clock: the estimate is the wall clock plus the persisted offset
    const r2 = await rig({ storage: r.storage, server, clock: deviceClock, manifests, start: false, seed: 9 });
    expect(Math.abs(r2.engine.serverNow() - serverClock.now())).toBeLessThan(1000);
    expect(Math.abs(r2.engine.serverNow() - deviceClock.now())).toBeGreaterThan(3_600_000); // not the device's own clock
    const a = await r2.engine.submit(setK("settings/b"));
    const entry = r2.engine.outbox().find((e) => e.mutationId === a.mutationId);
    expect(Math.abs((entry?.authoredServerMs ?? 0) - serverClock.now())).toBeLessThan(1000); // the start of the action's age, kept outside its content
    expect(contentOf(r2, a.mutationId).authoredAt).toBeDefined();
  });
});

describe("14 [CLI-56]: `offline-horizon` before the horizon is reached", () => {
  it("at 80% of max_offline by default: the age of the oldest unsynced action, and the time since the last successful sync, each once", async () => {
    const r = await rig(limits(10));
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    r.server.suspend(); // the engine cannot sync
    r.server.disconnect();
    await r.engine.settle();
    const x = await r.engine.submit(setK("settings/x"));
    await tick(r, 8 * DAY - 3_600_000);
    expect(warnings(r)).toEqual([]); // 79% of ten days
    await tick(r, 2 * 3_600_000);
    const w = warnings(r);
    expect(w.map((e) => e.kind).sort()).toEqual(["ACTION", "SYNC"]);
    for (const e of w) {
      expect(e.keyspaceId).toBe(KS);
      expect(e.maxOfflineMs).toBe(10 * DAY);
      expect(e.ageMs).toBeGreaterThanOrEqual(8 * DAY);
      expect(e.ageMs).toBeLessThan(10 * DAY);
    }
    expect(w.find((e) => e.kind === "ACTION")?.oldestMutationId).toBe(x.mutationId);
    expect(types(r, x.mutationId)).not.toContain("offline-horizon"); // it is no outcome of the action ([CLI-38])
    await tick(r, DAY / 2);
    expect(warnings(r)).toHaveLength(2); // once for each cause in the episode
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ code: "APP_SUSPENDED" })]);
  });

  it("the threshold is the app's, above 0 and below 100% of max_offline: 50% here; a threshold outside that is refused when the engine is created", async () => {
    const r = await rig({ ...limits(10), cfg: { horizonWarning: 0.5 } });
    r.server.suspend();
    r.server.disconnect();
    await r.engine.settle();
    await r.engine.submit(setK("settings/x"));
    await tick(r, 4 * DAY);
    expect(warnings(r)).toEqual([]);
    await tick(r, DAY + 60_000);
    expect(warnings(r).map((e) => e.kind).sort()).toEqual(["ACTION", "SYNC"]);
    for (const bad of [1, 0, -0.5, 1.5, Number.NaN]) await expect(rig({ cfg: { horizonWarning: bad } })).rejects.toBeInstanceOf(RangeError);
  });

  it("the time since the last successful sync warns for a keyspace that has no unsynced action; the episode ends with the next sync, and a later loss warns again", async () => {
    const r = await rig(limits(10));
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    r.server.suspend();
    r.server.disconnect();
    await tick(r, 8 * DAY + 60_000);
    expect(warnings(r).map((e) => e.kind)).toEqual(["SYNC"]); // no action: the sync alone
    expect(r.engine.horizonState()).toEqual([{ keyspaceId: KS, cause: "SYNC" }]);
    r.server.resume();
    await tick(r, 3_700_000); // the hold's probe: the next Subscribe succeeds
    expect(r.engine.status().connection).toBe("live");
    expect(r.engine.horizonState()).toEqual([]);
    r.server.suspend();
    r.server.disconnect();
    await tick(r, 8 * DAY + 60_000);
    expect(warnings(r).map((e) => e.kind)).toEqual(["SYNC", "SYNC"]);
  });

  it("while the session is live the time since the sync is 0: a quiet keyspace on a live session never warns, however long it is quiet", async () => {
    const r = await rig({ ...limits(1), heartbeatMs: 15_000 });
    await tick(r, 3 * DAY);
    expect(warnings(r)).toEqual([]);
  });

  it("the thresholds follow the limits in force: a value that arrives lower warns at once, and the floor of 3,600 s applies when the server sends 0", async () => {
    const r = await rig({ keyspaces: [KS], heartbeatMs: 0 });
    r.server.inject("mutate", { kind: "result", result: (m) => retryResult(m, RetryReason.CONTENTION, 3_600_000) }, { times: 1_000_000 }); // the action is sent, and kept waiting: unsynced
    const x = await r.engine.submit(setK("settings/x"));
    await tick(r, 2 * 3_600_000);
    expect(warnings(r)).toEqual([]); // 2 hours of 30 days
    r.server.limits.maxOfflineSeconds = 3600; // the app lowers its horizon (07 [MUT-88]): a lowered value is in force at once
    r.server.reconnect();
    await r.engine.settle();
    await tick(r, 40_000); // a session that lives under a minute reconnects with a growing backoff, 30 s at most (09 [READ-57])
    expect(r.engine.limits.maxOfflineMs).toBe(3_600_000);
    expect(warnings(r).map((e) => e.kind)).toEqual(["ACTION"]);
    expect(warnings(r)[0]?.oldestMutationId).toBe(x.mutationId);
    expect(warnings(r)[0]?.ageMs).toBeGreaterThanOrEqual(2 * 3_600_000);
    // a server that sends 0: the floor's 80% is 48 minutes
    const f = await rig({ serverOptions: { limits: { maxOfflineSeconds: 0 } }, heartbeatMs: 0 });
    f.server.suspend();
    f.server.disconnect();
    await f.engine.settle();
    await f.engine.submit(setK("settings/y"));
    await tick(f, 47 * 60_000);
    expect(warnings(f)).toEqual([]);
    await tick(f, 2 * 60_000);
    expect(warnings(f).map((e) => e.kind)).toContain("ACTION");
  });
});

describe("14 [CLI-57]: the client never refuses or drops an action for its age; the server decides", () => {
  it("an action that is not held is sent, whatever its age; it is the server's rule (07 [MUT-84]) that applies: nothing was written after its base, so it commits", async () => {
    const r = await rig({ heartbeatMs: 0 });
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    await r.engine.stop();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, start: false, seed: 6 });
    const x = await r2.engine.submit(setK("settings/x")); // authored offline, against the persisted state at seq 1
    await r.clock.advance(40 * DAY); // older than max_offline (30 d), with no record after its base
    r2.engine.start();
    await r2.engine.settle();
    expect(sentIds(r2)).toContain(x.mutationId); // the client did not judge it
    expect(types(r2, x.mutationId)).toEqual(["committed"]);
  });

  it("OFFLINE_TOO_LONG from the server (a record after its base is older than max_offline): the action and its dependents go to the export hook, never to the dead-letter store; the client never assigns the code itself", async () => {
    const hook: ExportRecord[] = [];
    const r = await rig({ heartbeatMs: 0 });
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    await r.engine.stop();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, start: false, seed: 7, cfg: { exportHook: (e) => void hook.push(e) } });
    const x = await r2.engine.submit(setK("settings/x"));
    const y = await r2.engine.submit(setK("settings/x", 2)); // depends on x
    const z = await r2.engine.submit(setK("settings/z"));
    r.server.write(KS, [kc("settings/other", "set", 1)], { sub: "bob" }); // the record at seq base + 1, written on day 0 by somebody else
    await r.clock.advance(40 * DAY);
    r2.engine.start();
    await r2.engine.settle();
    await tick(r2, 5000);
    // the server answered X with OFFLINE_TOO_LONG (HORIZON), recorded. A request carried X, so it was ever sent: outcome unknown (14 [CLI-57], [CLI-83]), then `exported`
    expect(types(r2, x.mutationId)).toEqual(["outcome-unknown", "exported"]);
    expect(ofType(r2, "outcome-unknown").find((e) => e.mutationId === x.mutationId)).toMatchObject({ cause: "HORIZON" });
    // Y depends on X and was never sent: `rejected` (DEPENDENCY_FAILED), then `exported`, with X, never the dead-letter store
    expect(types(r2, y.mutationId)).toEqual(["rejected", "exported"]);
    expect(ofType(r2, "rejected").find((e) => e.mutationId === y.mutationId)?.reject?.code).toBe(Code.DEPENDENCY_FAILED);
    expect(r2.engine.deadLetterEntries()).toEqual([]);
    // Z is not Y's dependency, but it is behind X on the lane, and the server judges it by the same rule
    expect(types(r2, z.mutationId)).toEqual(["outcome-unknown", "exported"]);
    expect(hook.map((h) => h.mutationId).sort()).toEqual([x.mutationId, y.mutationId, z.mutationId].sort());
    // every send was the stored content, and no request carried a code of the client's own
    expect(mutateRequests(r2).flatMap((q) => q.mutations).every((m) => m.mutationId !== "")).toBe(true);
  });
});

describe("14 [CLI-58]: the client shows the deadline of a pending GATED action", () => {
  it("the `deadline` of the intent row is read from the confirmed state, in server time", async () => {
    const r = await rig({ heartbeatMs: 0 });
    r.server.decisionDeadlineMs = 90_000;
    const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("claims/c1", 1)] });
    await r.engine.settle();
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([g.mutationId]); // registered, and pending: the gate has not decided
    expect(r.engine.intentDeadline(g.mutationId)).toBe(r.clock.now() + 90_000);
    expect(r.engine.intentDeadline("00000000-0000-4000-8000-000000000000")).toBeUndefined();
  });
});

describe("scenario: an offline horizon that ends held actions (14 [CLI-45], [CLI-56], [CLI-57], [CLI-83])", () => {
  it("the app is suspended: `offline-horizon` warns before the horizon; at the bound an action that was sent is outcome unknown, one that never was is dead-lettered with the hold's code, and its dependents fail", async () => {
    const r = await rig({ ...limits(7), heartbeatMs: 0 });
    r.random.fixed = 0.5;
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    // A is sent and answered with a Retry that holds it; B depends on A and was never sent
    r.server.inject("mutate", { kind: "result", result: (m) => (m.changes[0]?.key === "settings/a" ? retryResult(m, RetryReason.CONTENTION, 3_600_000) : undefined) });
    const a = await r.engine.submit(setK("settings/a"));
    await r.engine.settle();
    const b = await r.engine.submit(setK("settings/a", 2));
    r.server.suspend();
    r.server.disconnect();
    await r.engine.settle();
    // D is authored while the app is suspended: held, never sent
    await tick(r, 2 * DAY);
    const d = await r.engine.submit(setK("settings/d"));
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "app", code: "APP_SUSPENDED" })]);
    await tick(r, 3 * DAY + 12 * 3_600_000); // 5.5 days: 80% of seven days is 5.6
    expect(warnings(r)).toEqual([]);
    await tick(r, 4 * 3_600_000);
    const w = warnings(r);
    expect(w.find((e) => e.kind === "ACTION")?.oldestMutationId).toBe(a.mutationId);
    expect(types(r, a.mutationId)).toEqual([]); // nothing has ended yet: the warning came first
    // the bound of A is seven days after it was authored
    await tick(r, 2 * DAY);
    expect(types(r, a.mutationId)).toEqual(["outcome-unknown", "exported"]);
    expect(ofType(r, "outcome-unknown").find((e) => e.mutationId === a.mutationId)?.cause).toBe("HOLD_BOUND");
    expect(types(r, b.mutationId)).toEqual(["rejected", "exported"]); // DEPENDENCY_FAILED, in the sink of the action that it depended on ([CLI-34])
    expect(ofType(r, "rejected").find((e) => e.mutationId === b.mutationId)?.reject?.code).toBe(Code.DEPENDENCY_FAILED);
    expect(types(r, d.mutationId)).toEqual([]); // D was authored two days later: its own bound is not reached
    await tick(r, 2 * DAY);
    expect(types(r, d.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(r.engine.deadLetterEntries().find((x) => x.mutationId === d.mutationId)?.reason).toBe("APP_SUSPENDED");
    const first = r.events.findIndex((e) => e.type === "offline-horizon");
    const ended = r.events.findIndex((e) => e.type === "outcome-unknown");
    expect(first).toBeGreaterThan(-1);
    expect(first).toBeLessThan(ended);
    // after the app resumes, nothing of the ended actions is sent
    const sent = mutateRequests(r).length;
    r.server.resume();
    await tick(r, 3_700_000);
    expect(mutateRequests(r)).toHaveLength(sent);
    expect(r.engine.outbox()).toEqual([]);
  });
});
