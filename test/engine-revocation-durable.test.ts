// 14 [CLI-66], [CLI-84], [CLI-7], [CLI-8] (V14: MUST-2, MUST-4, MUST-6): a revocation's purge and quarantine are two steps of the text, each durable and
// retried; the purge runs when the export store is full; a revoked keyspace outlives a restart as a revoked keyspace; an outcome from an earlier
// epoch is confirmed with `GetMutationResult` before it retires, and is never exported for re-submission while the server may hold it.

import { describe, expect, it } from "vitest";
import { MemoryStorage, set, StorageError, type ExportRecord } from "../src/engine/index.js";
import { RetryReason } from "../src/gen/abcp/v2/types_pb.js";
import type { FakeTransport } from "../src/fake/index.js";
import { kc, KS, mutateRequests, ofType, retryResult, rig, tick, types, type Rig } from "./helpers/engine.js";

const setK = (key: string, v: unknown = 1): { keyspaceId: string; opType: string; changes: ReturnType<typeof set>[] } => ({ keyspaceId: KS, opType: "Set", changes: [set(key, v)] });
const transportOf = (r: Rig): FakeTransport => r.config.transport as FakeTransport;
const calls = (r: Rig, method: string): number => r.server.calls.filter((c) => c.method === method).length;

/** A is sent and held back by a Retry; B depends on A and was never sent. */
async function sentAndQueued(r: Rig): Promise<{ a: string; b: string }> {
  r.server.write(KS, [kc("settings/s", "set", 1)]);
  await r.engine.settle();
  r.server.inject("mutate", { kind: "result", result: (m) => (m.changes[0]?.key === "settings/a" ? retryResult(m, RetryReason.CONTENTION, 3_600_000) : undefined) });
  const a = (await r.engine.submit(setK("settings/a"))).mutationId;
  await r.engine.settle();
  const b = (await r.engine.submit(setK("settings/a", 2))).mutationId; // depends on A, never sent
  await r.engine.settle();
  return { a, b };
}

describe("14 [CLI-66], [CLI-84]: the purge by a revocation still runs when the export store is full (V14 MUST-2)", () => {
  it("the replica and the journal are purged, the quarantined and the asked-about actions stay in the outbox, final and never sent again, and nothing is lost or retried for ever", async () => {
    const storage = new MemoryStorage({ maxExports: 0 });
    const r = await rig({ storage, cfg: { exportHook: () => undefined } });
    const { a, b } = await sentAndQueued(r);
    expect(storage.replica(KS)).toBeDefined();
    const sent = mutateRequests(r).length;
    r.server.revokeKeyspaces(KS);
    await r.engine.settle();
    expect(storage.replica(KS)).toBeUndefined(); // the purge ran
    expect(r.engine.journalEntries()).toEqual([]);
    expect(r.engine.status().stopped[KS]).toBe("ACCESS_REVOKED");
    expect(() => r.engine.view(KS)).toThrow();
    // the actions are in the outbox, due for the export hook: final, never sent again ([CLI-84])
    expect(r.engine.outbox().map((e) => [e.mutationId, e.stuck?.sink])).toEqual([[a, "export"], [b, "export"]]);
    expect(r.engine.outbox().find((e) => e.mutationId === b)?.stuck?.quarantine).toBe(true);
    expect(ofType(r, "quarantined")).toHaveLength(1);
    expect(ofType(r, "quarantined")[0]).toMatchObject({ keyspaceId: KS, cause: "ACCESS_REVOKED", mutationIds: [b] });
    expect(r.engine.status().fullStores).toContain("exports");
    expect(r.engine.internalErrors.filter((e) => e instanceof Error && e.name === "StoreFull")).toEqual([]);
    await tick(r, 3_600_000 * 3);
    expect(mutateRequests(r)).toHaveLength(sent);
    expect(r.engine.exportEntries()).toEqual([]);
  });

  it("an export store with room for one: the first quarantined action goes, the second waits, and goes when the first is acknowledged", async () => {
    const hook: ExportRecord[] = [];
    const storage = new MemoryStorage({ maxExports: 1 });
    const r = await rig({ storage, cfg: { exportHook: (x) => void hook.push(x) } });
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    r.server.inject("mutate", { kind: "result", result: (m) => (m.changes[0]?.key === "settings/a" ? retryResult(m, RetryReason.CONTENTION, 3_600_000) : undefined) });
    const a = (await r.engine.submit(setK("settings/a"))).mutationId;
    await r.engine.settle();
    const b1 = (await r.engine.submit(setK("settings/a", 2))).mutationId;
    const b2 = (await r.engine.submit(setK("settings/a", 3))).mutationId;
    await r.engine.settle();
    r.server.revokeKeyspaces(KS);
    await r.engine.settle();
    expect(storage.replica(KS)).toBeUndefined();
    const q = ofType(r, "quarantined");
    expect(q).toHaveLength(1);
    expect(q[0]?.mutationIds.sort()).toEqual([b1, b2].sort()); // both are quarantined; one of them is handed over
    const handed = r.engine.exportEntries().map((x) => x.mutationId);
    expect(handed).toHaveLength(1);
    const waiting = r.engine.outbox().filter((e) => e.stuck?.quarantine === true).map((e) => e.mutationId);
    expect(waiting).toHaveLength(1);
    expect(ofType(r, "exported").map((e) => e.mutationId)).toEqual([handed[0]]);
    // the app acknowledges the entry: the one that waited is handed over
    await r.engine.ackExport(handed[0] as string);
    await r.engine.settle();
    expect(r.engine.exportEntries().map((x) => x.mutationId)).toEqual(waiting);
    expect(ofType(r, "exported").map((e) => e.mutationId)).toEqual([handed[0], waiting[0]]);
    expect(hook.map((x) => x.mutationId)).toEqual([handed[0], waiting[0]]);
    expect(r.engine.outbox().map((e) => e.mutationId)).not.toContain(waiting[0]);
    expect(a).toBeTruthy();
  });

  it("a transaction of the revocation that fails is made again, with the client backoff: the purge, then the quarantine, each once it can", async () => {
    const storage = new MemoryStorage();
    const r = await rig({ storage, cfg: { exportHook: () => undefined } });
    const { b } = await sentAndQueued(r);
    const commit = storage.commit.bind(storage);
    let failPurge = 2;
    let failQuarantine = 2;
    storage.commit = (ops): Promise<void> => {
      if (ops.some((o) => o.op === "deleteReplica") && failPurge > 0) {
        failPurge--;
        return Promise.reject(new StorageError("the purge failed"));
      }
      if (ops.some((o) => o.op === "putExport") && failQuarantine > 0) {
        failQuarantine--;
        return Promise.reject(new StorageError("the quarantine failed"));
      }
      return commit(ops);
    };
    r.server.revokeKeyspaces(KS);
    await r.engine.settle();
    expect(storage.replica(KS)).toBeDefined(); // not purged yet, and not given up
    expect(ofType(r, "quarantined")).toHaveLength(0);
    expect(r.engine.outbox().map((e) => e.mutationId)).toContain(b);
    for (let i = 0; i < 20 && (storage.replica(KS) !== undefined || ofType(r, "quarantined").length === 0); i++) await tick(r, 60_000);
    expect(storage.replica(KS)).toBeUndefined();
    expect(ofType(r, "quarantined")).toHaveLength(1);
    expect(ofType(r, "quarantined")[0]).toMatchObject({ mutationIds: [b] });
    expect(types(r, b)).toEqual(["exported"]);
    expect(r.engine.exportEntries().map((x) => x.mutationId)).toContain(b);
  });
});

describe("14 [CLI-66] (V19 SHOULD-2): `quarantined` for the keyspace, then `exported` for each entry, whichever step failed first", () => {
  it("the quarantine of the second action fails once: nothing is announced until it is made, and then `quarantined` comes first", async () => {
    const hook: ExportRecord[] = [];
    const storage = new MemoryStorage();
    const r = await rig({ storage, cfg: { exportHook: (x) => void hook.push(x) } });
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    r.server.inject("mutate", { kind: "result", result: (m) => (m.changes[0]?.key === "settings/a" ? retryResult(m, RetryReason.CONTENTION, 3_600_000) : undefined) });
    await r.engine.submit(setK("settings/a"));
    await r.engine.settle();
    const b1 = (await r.engine.submit(setK("settings/a", 2))).mutationId;
    const b2 = (await r.engine.submit(setK("settings/a", 3))).mutationId;
    await r.engine.settle();
    const commit = storage.commit.bind(storage);
    let failing = true;
    storage.commit = (ops): Promise<void> => {
      if (failing && ops.some((o) => o.op === "putExport" && o.entry.mutationId === b2)) return Promise.reject(new StorageError("the quarantine failed"));
      return commit(ops);
    };
    r.server.revokeKeyspaces(KS);
    await r.engine.settle();
    // b1 moved to the export store and b2 did not: neither is announced, the keyspace's step is not done
    expect(r.engine.exportEntries().map((x) => x.mutationId)).toContain(b1);
    expect(r.engine.exportEntries().map((x) => x.mutationId)).not.toContain(b2);
    expect(ofType(r, "quarantined")).toHaveLength(0);
    expect(ofType(r, "exported")).toHaveLength(0);
    expect(hook).toEqual([]);
    failing = false;
    for (let i = 0; i < 20 && ofType(r, "quarantined").length === 0; i++) await tick(r, 60_000);
    const order = r.events.filter((e) => e.type === "quarantined" || e.type === "exported").map((e) => e.type);
    expect(order).toEqual(["quarantined", "exported", "exported"]);
    expect(ofType(r, "quarantined")[0]?.mutationIds.sort()).toEqual([b1, b2].sort());
    expect(hook.map((x) => x.mutationId).sort()).toEqual([b1, b2].sort());
  });
});

describe("14 [CLI-7], [CLI-8], [CLI-66]: a revocation is durable across a restart (V14 MUST-4)", () => {
  it("the action that was sent and is still being asked about is asked about after a restart, and the revoked keyspace is not loaded again with the old token", async () => {
    const r = await rig({ cfg: { exportHook: () => undefined } });
    const { a } = await sentAndQueued(r);
    r.server.inject("getMutationResult", { kind: "status", status: "Unavailable" }, { times: 1_000_000 });
    r.server.revokeKeyspaces(KS);
    await r.engine.settle();
    expect(r.engine.outbox().map((e) => e.mutationId)).toContain(a);
    await r.engine.stop();
    r.server.clearInjections();
    const before = r.server.calls.length;
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 99, cfg: { exportHook: () => undefined }, keyspaces: [] });
    await r2.clock.advance(60_000);
    await r2.engine.settle();
    await r2.clock.advance(600_000);
    await r2.engine.settle();
    const after = r.server.calls.slice(before).map((c) => c.method);
    expect(after).not.toContain("loadState"); // the keyspace is not followed again with the old token
    expect(after).toContain("getMutationResult");
    expect(r2.engine.outbox().map((e) => e.mutationId)).toEqual([]); // the answer ended it: outcome unknown
    expect(types(r2, a)).toEqual(["outcome-unknown", "exported"]);
    expect(r2.engine.status().stopped[KS]).toBe("ACCESS_REVOKED");
    expect(r2.engine.exportEntries().map((x) => x.mutationId).sort()).toEqual([a, ...r2.engine.exportEntries().map((x) => x.mutationId).filter((x) => x !== a)].sort());
  });

  it("a restart after the purge and before the quarantine finishes the quarantine, once: `quarantined`, then `exported`, and nothing is sent", async () => {
    const storage = new MemoryStorage();
    const r = await rig({ storage, cfg: { exportHook: () => undefined } });
    const { b } = await sentAndQueued(r);
    const commit = storage.commit.bind(storage);
    storage.commit = (ops): Promise<void> => (ops.some((o) => o.op === "putExport") ? Promise.reject(new StorageError("the process died")) : commit(ops));
    r.server.revokeKeyspaces(KS);
    await r.engine.settle();
    expect(storage.replica(KS)).toBeUndefined(); // the purge is committed, with the record that the keyspace is revoked
    expect(storage.entry(b)).toBeDefined(); // the quarantine is not
    await r.engine.stop();
    storage.commit = commit;
    const hook: ExportRecord[] = [];
    const sent = mutateRequests(r).length;
    const r2 = await rig({ storage, server: r.server, clock: r.clock, seed: 5, cfg: { exportHook: (x) => void hook.push(x) }, keyspaces: [] });
    await r2.engine.settle();
    await tick(r2, 120_000);
    expect(ofType(r2, "quarantined")).toHaveLength(1);
    expect(ofType(r2, "quarantined")[0]).toMatchObject({ keyspaceId: KS, cause: "ACCESS_REVOKED", mutationIds: [b] });
    expect(r2.engine.exportEntries().map((x) => x.mutationId)).toContain(b);
    expect(hook.map((x) => x.mutationId)).toContain(b);
    expect(storage.entry(b)).toBeUndefined();
    expect(mutateRequests(r2)).toHaveLength(sent);
    // a third start finds nothing left to do: no second `quarantined`
    await r2.engine.stop();
    const r3 = await rig({ storage, server: r.server, clock: r.clock, seed: 6, keyspaces: [] });
    await tick(r3, 120_000);
    expect(ofType(r3, "quarantined")).toHaveLength(0);
  });

  it("the app asks for the keyspace again, and the record of the revocation goes: a later restart follows it", async () => {
    const r = await rig({ cfg: { exportHook: () => undefined } });
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    r.server.revokeKeyspaces(KS);
    await r.engine.settle();
    expect(r.storage.metaValue("revoked")).toContain(KS);
    await tick(r, 61_000);
    transportOf(r).renewToken();
    await r.engine.addKeyspace(KS);
    await r.engine.settle();
    expect(r.engine.view(KS).json("settings/s")).toBe(1);
    expect(r.storage.metaValue("revoked")).toBe("{}");
    await r.engine.stop();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 9, keyspaces: [] });
    await r2.engine.settle();
    expect(r2.engine.view(KS).json("settings/s")).toBe(1);
    expect(r2.engine.status().stopped[KS]).toBeUndefined();
  });
});

describe("14 [CLI-7]: an outcome from an earlier epoch retires at a revocation only once `GetMutationResult` confirms it (V14 MUST-6, Q54.3)", () => {
  /**
   * A was appended in epoch 1 and is still in the outbox (delivery is held back), the keyspace was restored (epoch 2) and the engine has loaded it
   * again, and its epoch flow cannot ask (`GetMutationResults` fails): the keyspace is revoked with A's outcome from an earlier epoch.
   */
  async function earlier(restoreTo: bigint, opts: { rewindBindings?: boolean } = {}): Promise<{ r: Rig; a: string; hook: ExportRecord[] }> {
    const hook: ExportRecord[] = [];
    const r = await rig({ cfg: { exportHook: (x) => void hook.push(x) } });
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    r.server.faults.paused = true;
    const a = (await r.engine.submit(setK("settings/a"))).mutationId;
    await r.engine.settle();
    expect(r.engine.outbox()[0]?.outcome).toMatchObject({ kind: "appended", logEpoch: 1 });
    r.server.inject("getMutationResults", { kind: "status", status: "Unavailable" }, { times: 1_000_000 });
    r.server.restore(KS, restoreTo, opts);
    r.server.resumeDelivery();
    await r.engine.settle();
    expect(r.engine.cursor(KS)?.logEpoch).toBe(2);
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([a]);
    // the subject is revoked (the keyspace's session had ended with the restore); the token that the app gets after the revocation is the new one
    r.server.revokeSubject("ana");
    await r.engine.settle();
    expect(r.engine.status().stopped[KS]).toBe("SUBJECT_REVOKED");
    return { r, a, hook };
  }

  it("an outcome that survived the restore: the action retires on the answer, and is never exported for re-submission (it would apply twice)", async () => {
    const { r, a, hook } = await earlier(2n);
    expect(r.engine.exportEntries()).toEqual([]); // not quarantined
    expect(hook).toEqual([]);
    expect(types(r, a)).not.toContain("exported");
    await tick(r, 61_000);
    transportOf(r).renewToken();
    for (let i = 0; i < 10 && r.engine.outbox().length > 0; i++) await tick(r, 120_000);
    expect(calls(r, "getMutationResult")).toBeGreaterThan(0);
    expect(types(r, a)).toEqual(["committed"]);
    expect(r.engine.outbox()).toEqual([]);
    expect(r.engine.exportEntries()).toEqual([]);
    expect(hook).toEqual([]);
    expect(r.engine.journalEntries()).toEqual([]); // the journal is on the purge list
  });

  it("an outcome that the restore voided: the action is lost, the final rejection STALE{EPOCH}; not exported", async () => {
    const { r, a, hook } = await earlier(0n);
    await tick(r, 61_000);
    transportOf(r).renewToken();
    for (let i = 0; i < 10 && r.engine.outbox().length > 0; i++) await tick(r, 120_000);
    expect(types(r, a)).toEqual(["conflict", "rejected"]);
    expect(r.engine.exportEntries()).toEqual([]);
    expect(hook).toEqual([]);
  });

  it("an action that was sent with no answer, in an epoch that the restore replaced, is asked about by `GetMutationResult` as well: a voided outcome is lost, and the action does not stay pending", async () => {
    const hook: ExportRecord[] = [];
    const r = await rig({ cfg: { exportHook: (x) => void hook.push(x) } });
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await r.engine.settle();
    r.server.faults.paused = true;
    r.server.inject("mutate", { kind: "lose" }, { times: 1 }); // the server records it, the answer is lost: no outcome at the client
    const a = (await r.engine.submit(setK("settings/a"))).mutationId;
    await r.engine.settle();
    expect(r.engine.outbox()[0]?.outcome).toBeUndefined();
    r.server.inject("getMutationResults", { kind: "status", status: "Unavailable" }, { times: 1_000_000 });
    r.server.restore(KS, 0n);
    r.server.resumeDelivery();
    await r.engine.settle();
    r.server.revokeSubject("ana");
    await r.engine.settle();
    await tick(r, 61_000);
    transportOf(r).renewToken();
    for (let i = 0; i < 10 && r.engine.outbox().length > 0; i++) await tick(r, 120_000);
    expect(r.engine.outbox()).toEqual([]);
    expect(types(r, a)).toEqual(["conflict", "rejected"]);
    expect(hook).toEqual([]);
  });

  it("an outcome that the server can no longer tell (the restore rewound the bindings): outcome unknown, then exported (14 [CLI-83])", async () => {
    const { r, a, hook } = await earlier(0n, { rewindBindings: true });
    await tick(r, 61_000);
    transportOf(r).renewToken();
    for (let i = 0; i < 10 && r.engine.outbox().length > 0; i++) await tick(r, 120_000);
    expect(types(r, a)).toEqual(["outcome-unknown", "exported"]);
    expect(ofType(r, "outcome-unknown")[0]).toMatchObject({ mutationId: a, cause: "SUBJECT_REVOKED" });
    expect(hook.map((x) => x.mutationId)).toEqual([a]);
    expect(mutateRequests(r).flatMap((q) => q.mutations).filter((m) => m.mutationId === a)).toHaveLength(1); // never sent again
  });
});
