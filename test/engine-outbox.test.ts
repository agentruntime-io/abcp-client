// 14 §14.3 The durable outbox.

import { describe, expect, it } from "vitest";
import { LocalRefusal, MemoryStorage, set, type EngineEvent } from "../src/engine/index.js";
import { decodeContent, encodeContent } from "../src/engine/entry.js";
import { Code, RetryReason } from "../src/gen/abpc/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { goOffline, goOnline, kc, KS, mutateRequests, ofType, rejectResult, retryResult, rig, tick, types } from "./helpers/engine.js";

describe("14 [CLI-21] an action is accepted only after the local storage transaction has completed", () => {
  it("is in the storage when `submit` resolves", async () => {
    const r = await rig();
    await goOffline(r);
    const { mutationId } = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    expect(r.storage.entry(mutationId)).toBeDefined();
  });

  it("a failed commit refuses the action with a local error, and the optimistic display is rolled back", async () => {
    const r = await rig();
    await goOffline(r);
    r.storage.failNext = 1;
    await expect(r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] })).rejects.toMatchObject({ name: "LocalRefusal", code: "STORAGE" });
    expect(r.engine.view(KS).get("settings/a")).toBeUndefined();
    expect(r.engine.outbox()).toHaveLength(0);
    expect(r.engine.status().pending).toBe(0);
  });

  it("the storage mode is part of the sync status, and memory-only storage is never silent", async () => {
    const r = await rig();
    expect(r.engine.status().storageMode).toBe("memory");
  });
});

describe("14 [CLI-22] the outbox stores the content once, and every send repeats it byte for byte", () => {
  it("a resend after a lost response carries the identical content, base_seq, base_log_epoch and authored_at included", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "lose" });
    r.server.faults.paused = true; // the record does not arrive: the outcome stays unknown (14 [CLI-7])
    const { mutationId } = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    // confirmed state moves on and the clock too, so that nothing recomputed could equal the stored content
    r.server.write(KS, [kc("settings/z", "set", 1)]);
    await tick(r, 120_000);
    r.server.resumeDelivery();
    await r.engine.settle();
    const reqs = mutateRequests(r);
    expect(reqs.length).toBeGreaterThanOrEqual(2);
    const first = reqs[0]?.mutations[0];
    const second = reqs[1]?.mutations[0];
    expect(first?.mutationId).toBe(mutationId);
    expect(second?.mutationId).toBe(mutationId);
    expect(Buffer.from(encodeContent(second as never)).equals(Buffer.from(encodeContent(first as never)))).toBe(true);
    expect(second?.baseSeq).toBe(first?.baseSeq);
    expect(second?.authoredAt?.seconds).toBe(first?.authoredAt?.seconds);
    expect(types(r, mutationId)).toEqual(["committed"]);
    // the server applied it once
    expect(r.server.records(KS).filter((x) => x.mutationId === mutationId)).toHaveLength(1);
  });

  it("the stored content is the canonical encoding of the Mutation without its stamping fields", async () => {
    const r = await rig();
    await goOffline(r);
    const { mutationId } = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const stored = r.storage.entry(mutationId);
    const m = decodeContent(stored?.content as Uint8Array);
    expect(m.clientId).toBe("");
    expect(m.clientSeq).toBe(0n);
    expect(m.afterClientSeq).toBe(0n);
    expect(Buffer.from(encodeContent(m)).equals(Buffer.from(stored?.content as Uint8Array))).toBe(true);
  });
});

describe("14 [CLI-23] a mutation id is minted once and never changes", () => {
  it("survives a retry, a restart of the engine and re-stamping", async () => {
    const storage = new MemoryStorage();
    const r = await rig({ storage });
    await goOffline(r);
    const { mutationId } = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const firstClient = (r.engine as unknown as { clientId: string }).clientId;
    await r.engine.stop();
    // a new engine over the same storage and server: a new client_id, the same action
    const r2 = await rig({ storage, server: r.server, clock: r.clock, seed: 99 });
    const after = r2.engine.outbox().length === 0 ? r2.server.records(KS).filter((x) => x.mutationId === mutationId).length : 1;
    expect(after).toBe(1);
    expect((r2.engine as unknown as { clientId: string }).clientId).not.toBe(firstClient);
    expect(r2.server.records(KS).map((x) => x.mutationId)).toContain(mutationId);
    expect(r2.server.records(KS).filter((x) => x.mutationId === mutationId)).toHaveLength(1);
    expect(types(r2, mutationId)).toEqual(["committed"]);
  });

  it("every new action gets a fresh mutation id", async () => {
    const r = await rig();
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    expect(a.mutationId).not.toBe(b.mutationId);
    expect(a.mutationId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  });
});

describe("14 [CLI-24] an action with no definitive result is resent identically, never rebased, dead-lettered or dropped", () => {
  it.each(["Unavailable", "DeadlineExceeded", "Aborted", "Unknown", "Internal", "Cancelled", "DataLoss", "OutOfRange"] as const)("status %s: the action is resent with the client backoff until a resend gets the stored outcome", async (status) => {
    const r = await rig();
    r.server.faults.paused = true; // no record: only a resend can resolve the action
    r.server.inject("mutate", { kind: "lose", status }); // applied, the response lost
    r.server.inject("mutate", { kind: "status", status }); // then not even handled
    const { mutationId } = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(r.engine.outbox()).toHaveLength(1);
    expect(r.engine.status().deadLetters).toBe(0);
    await tick(r, 3_000);
    await tick(r, 3_000);
    r.server.resumeDelivery();
    await r.engine.settle();
    expect(types(r, mutationId)).toEqual(["committed"]);
    expect(r.server.records(KS).filter((x) => x.mutationId === mutationId)).toHaveLength(1);
    expect(mutateRequests(r).length).toBeGreaterThanOrEqual(3);
  });
});

describe("14 [CLI-26] at most one Mutate in flight on a SEQUENCED lane, batched in client_seq order", () => {
  it("sends the queued actions in one request, in order, and the next request waits for the answer", async () => {
    const r = await rig();
    await goOffline(r);
    const ids: string[] = [];
    for (let i = 0; i < 3; i++) ids.push((await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set(`settings/k${i}`, i)] })).mutationId);
    const before = mutateRequests(r).length;
    await goOnline(r);
    const reqs = mutateRequests(r).slice(before);
    expect(reqs).toHaveLength(1);
    expect(reqs[0]?.mutations.map((m) => m.mutationId)).toEqual(ids);
    expect(reqs[0]?.mutations.map((m) => Number(m.clientSeq))).toEqual([1, 2, 3]);
  });

  it("while a request is in flight no other request leaves on the lane", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "delay", ms: 5000 });
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
    await r.engine.settle();
    expect(mutateRequests(r)).toHaveLength(1);
    await tick(r, 5000);
    expect(mutateRequests(r)).toHaveLength(2);
    expect(r.engine.outbox()).toHaveLength(0);
  });

  it("splits a batch at AppLimits.max_mutations_per_request", async () => {
    const r = await rig({ serverOptions: { limits: { maxMutationsPerRequest: 2 } } });
    await goOffline(r);
    for (let i = 0; i < 5; i++) await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set(`settings/k${i}`, i)] });
    const before = mutateRequests(r).length;
    await goOnline(r);
    const sizes = mutateRequests(r).slice(before).map((q) => q.mutations.length);
    expect(sizes.every((n) => n <= 2)).toBe(true);
    expect(sizes.reduce((a, b) => a + b, 0)).toBe(5);
    expect(r.engine.outbox()).toHaveLength(0);
  });
});

describe("14 [CLI-27] client_seq per lane and keyspace, contiguous from 1, decided by the lane's watermark", () => {
  it("numbers each lane separately from 1 and records the slot in the outbox before the request leaves", async () => {
    const r = await rig();
    await goOffline(r);
    const s1 = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const g1 = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] });
    const s2 = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
    const before = mutateRequests(r).length;
    r.server.inject("mutate", { kind: "delay", ms: 1000 });
    await goOnline(r);
    // the slot is stored before the answer arrives
    expect(r.storage.entry(s1.mutationId)?.clientSeq).toBe(1);
    expect(r.storage.entry(s2.mutationId)?.clientSeq).toBe(2);
    expect(r.storage.entry(g1.mutationId)?.clientSeq).toBe(1);
    await tick(r, 1000);
    const m = mutateRequests(r)[before]?.mutations ?? [];
    expect(m.map((x) => `${x.lane}:${x.clientSeq}`).sort()).toEqual(["1:1", "1:2", "2:1"].sort());
  });

  it("a Retry consumes no slot: the action is resent at the same client_seq", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "result", result: (m) => retryResult(m, RetryReason.STORAGE_UNAVAILABLE, 100) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(r.storage.entry(a.mutationId)?.clientSeq).toBe(1);
    await tick(r, 3000);
    const reqs = mutateRequests(r);
    expect(reqs.map((q) => Number(q.mutations[0]?.clientSeq))).toEqual([1, 1]);
    expect(types(r, a.mutationId)).toEqual(["committed"]);
  });

  it("an action that gives up its slot without consuming it hands the slot to the next action of the lane", async () => {
    const r = await rig();
    // an unrecorded reject (PERMISSION_DENIED) twice: the refresh and the resend of 14 [CLI-46], then the action is final and its slot is free
    let refreshed = 0;
    r.engine.tokens = { refresh: () => (refreshed++, Promise.resolve(true)) };
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(Code.PERMISSION_DENIED)) }, { times: 2 });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 1);
    await r.engine.settle();
    expect(r.engine.status().deadLetters).toBe(1);
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
    await r.engine.settle();
    const last = mutateRequests(r).at(-1)?.mutations[0];
    expect(last?.mutationId).toBe(b.mutationId);
    expect(Number(last?.clientSeq)).toBe(1); // a's slot, since the server recorded nothing for a
    expect(types(r, b.mutationId)).toEqual(["committed"]);
    expect(refreshed).toBe(1);
    expect(types(r, a.mutationId)).toEqual(["rejected", "dead-lettered"]);
  });
});

describe("14 [CLI-28] the dead-letter store", () => {
  it("moving an action to it is one local transaction that records the reject, the attempts, the times and the failed dependents", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(Code.SCHEMA_INVALID)), recorded: true });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    const dl = r.engine.deadLetterEntries();
    expect(dl).toHaveLength(1);
    expect(dl[0]?.mutationId).toBe(a.mutationId);
    expect(dl[0]?.reject.code).toBe(Code.SCHEMA_INVALID);
    expect(dl[0]?.reason).toBe("SCHEMA_INVALID");
    expect(dl[0]?.firstFailureMs).toBeGreaterThan(0);
    expect(dl[0]?.lastFailureMs).toBeGreaterThanOrEqual(dl[0]?.firstFailureMs ?? 0);
    // one transaction: the delete of the outbox entry and the put of the dead letter are in the same commit
    const commit = r.storage.log.find((ops) => ops.some((o) => o.op === "putDeadLetter"));
    expect(commit?.map((o) => o.op)).toEqual(expect.arrayContaining(["deleteEntry", "putDeadLetter"]));
    expect(r.storage.entry(a.mutationId)).toBeUndefined();
  });

  it("entries stay durable until the app acknowledges them, never go back into the outbox, and can be resubmitted as new actions", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(Code.SCHEMA_INVALID)), recorded: true });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 600_000);
    expect(r.engine.status().deadLetters).toBe(1);
    expect(r.engine.status().unacknowledgedRejections).toBe(1);
    expect(r.engine.outbox()).toHaveLength(0);
    const again = await r.engine.resubmitDeadLetter(a.mutationId);
    expect(again.mutationId).not.toBe(a.mutationId); // a fresh mutation id: a new action (14 [CLI-23])
    await r.engine.settle();
    expect(r.engine.status().deadLetters).toBe(0);
    expect(types(r, again.mutationId)).toEqual(["committed"]);
    await r.engine.ackRejection(a.mutationId);
    expect(r.engine.status().unacknowledgedRejections).toBe(0);
  });
});

describe("14 [CLI-29] each outbox entry is processed in isolation", () => {
  it("an entry that cannot be decoded goes to the dead-letter store, and does not stop the flush of the others", async () => {
    const storage = new MemoryStorage();
    const r = await rig({ storage });
    await goOffline(r);
    const ok = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/good", 1)] });
    const bad = r.storage.entry(ok.mutationId);
    if (bad === undefined) throw new Error("no entry");
    await r.engine.stop();
    // a second entry whose stored content is garbage
    await storage.commit([{ op: "putEntry", entry: { ...bad, mutationId: "11111111-1111-4111-8111-111111111111", content: new Uint8Array([0xff, 0xff, 0xff]), ordinal: bad.ordinal + 1, dependsOn: [] } }]);
    const r2 = await rig({ storage, server: r.server, clock: r.clock });
    expect(r2.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual(["11111111-1111-4111-8111-111111111111"]);
    expect(types(r2, ok.mutationId)).toEqual(["committed"]);
  });
});

describe("14 [CLI-81] cancelling", () => {
  it("an action that was never sent leaves the outbox locally with `rejected` and CANCELLED, and no RPC", async () => {
    const r = await rig();
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] }); // depends on a (same key)
    expect(await r.engine.cancel(a.mutationId)).toBe(true);
    expect(r.engine.view(KS).get("settings/a")).toBeUndefined(); // the overlay is recomputed; b failed with it
    await goOnline(r);
    expect(mutateRequests(r).flatMap((q) => q.mutations.map((m) => m.mutationId))).toEqual([]);
    const rej = ofType(r, "rejected");
    expect(rej.map((e) => [e.mutationId, e.reject?.code])).toEqual([
      [a.mutationId, Code.CANCELLED],
      [b.mutationId, Code.DEPENDENCY_FAILED],
    ]);
  });

  it("a GATED action that was sent is cancelled with CancelIntent, without a resend, and the answer is its final outcome", async () => {
    const r = await rig();
    const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] });
    await r.engine.settle();
    const sent = mutateRequests(r).length;
    expect(await r.engine.cancel(g.mutationId)).toBe(true);
    await r.engine.settle();
    expect(mutateRequests(r)).toHaveLength(sent);
    expect(r.server.calls.some((c) => c.method === "cancelIntent")).toBe(true);
    expect(types(r, g.mutationId)).toEqual(expect.arrayContaining(["rejected"]));
    expect(ofType(r, "rejected").at(-1)?.reject?.code).toBe(Code.CANCELLED);
  });
});

describe("14 [CLI-84] local storage bounds", () => {
  it("a full outbox refuses a new action locally, as a failed commit", async () => {
    const storage = new MemoryStorage({ maxOutbox: 1 });
    const r = await rig({ storage });
    await goOffline(r);
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await expect(r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] })).rejects.toBeInstanceOf(LocalRefusal);
    expect(r.engine.status().fullStores).toContain("outbox");
  });

  it("14 [CLI-21] (draft 14): a refused commit is a typed local error that names its cause: a full outbox, an exhausted quota, an abort of the transaction", async () => {
    // a full outbox ([CLI-84])
    const full = await rig({ storage: new MemoryStorage({ maxOutbox: 1 }) });
    await goOffline(full);
    await full.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const e1 = await full.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] }).catch((e: unknown) => e);
    expect(e1).toBeInstanceOf(LocalRefusal);
    expect(e1).toMatchObject({ name: "LocalRefusal", code: "STORAGE", cause: "OUTBOX_FULL" });
    // a quota error of the platform (IndexedDB's QuotaExceededError), and any other abort of the transaction
    const q = await rig();
    await goOffline(q);
    const quota = Object.assign(new Error("the quota is exceeded"), { name: "QuotaExceededError" });
    q.storage.commit = () => Promise.reject(quota);
    const e2 = await q.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] }).catch((e: unknown) => e);
    expect(e2).toMatchObject({ name: "LocalRefusal", code: "STORAGE", cause: "QUOTA" });
    const a = await rig();
    await goOffline(a);
    a.storage.failNext = 1;
    const e3 = await a.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] }).catch((e: unknown) => e);
    expect(e3).toMatchObject({ name: "LocalRefusal", code: "STORAGE", cause: "ABORT" });
    // an error of the client, not a result of the server: nothing of it is a Reject
    expect(e3).not.toHaveProperty("reject");
    expect(a.engine.outbox()).toHaveLength(0);
  });

  it("a full dead-letter store keeps the entry in the outbox, final and never sent again, until the app acknowledges entries", async () => {
    const storage = new MemoryStorage({ maxDeadLetters: 1 });
    const r = await rig({ storage });
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(Code.SCHEMA_INVALID)), recorded: true }, { times: 2 });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
    await r.engine.settle();
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual([a.mutationId]);
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([b.mutationId]); // b stays, final, never sent again
    const sent = mutateRequests(r).length;
    await tick(r, 600_000);
    expect(mutateRequests(r)).toHaveLength(sent);
    expect(r.engine.status().fullStores).toContain("deadLetters");
    await r.engine.ackDeadLetter(a.mutationId);
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual([b.mutationId]);
    expect(r.engine.outbox()).toHaveLength(0);
  });
});

describe("14 [CLI-8] one rehydration path", () => {
  it("start, resume and reconnect re-read: they enqueue no action again, and an unknown outcome is resolved by a resend that the server deduplicates", async () => {
    const storage = new MemoryStorage();
    const r = await rig({ storage });
    r.server.inject("mutate", { kind: "lose" });
    r.server.faults.paused = true; // the first run never learns the outcome
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(r.storage.entry(a.mutationId)?.everSent).toBe(true);
    await r.engine.stop();
    r.server.resumeDelivery();
    // the second run reads the durable outbox, loads confirmed state, and resolves the unknown outcome by a resend
    const r2 = await rig({ storage, server: r.server, clock: r.clock, seed: 99 });
    const ev: EngineEvent[] = r2.events;
    expect(r.server.records(KS).filter((x) => x.mutationId === a.mutationId)).toHaveLength(1); // not applied again
    expect(types(r2, a.mutationId)).toEqual(["committed"]);
    expect(ev.filter((e) => e.type === "reloaded")).toHaveLength(1);
    expect(r2.engine.outbox()).toHaveLength(0);
  });
});
