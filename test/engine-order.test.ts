// 14 §14.4 Ordering and dependencies, and the lane watermarks of 14 [CLI-78].

import { describe, expect, it } from "vitest";
import { create } from "@bufbuild/protobuf";
import { del, set } from "../src/engine/index.js";
import { HeartbeatFrame_CursorSchema, HeartbeatFrameSchema, LaneWatermarkSchema, SubscribeResponseSchema } from "../src/gen/abpc/v2/sync_pb.js";
import { Code, ErrorClass, Lane, RetryReason } from "../src/gen/abpc/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { goOffline, goOnline, KS, mutateRequests, ofType, rejectResult, retryResult, rig, sentIds, tick, types, type Rig } from "./helpers/engine.js";

const dependsOf = (r: Rig, id: string): string[] => r.engine.outbox().find((e) => e.mutationId === id)?.dependsOn ?? [];

async function two(r: Rig, a: Parameters<Rig["engine"]["submit"]>[0], b: Parameters<Rig["engine"]["submit"]>[0], options?: Parameters<Rig["engine"]["submit"]>[1]): Promise<[string, string]> {
  const x = await r.engine.submit(a);
  const y = await r.engine.submit(b, options);
  return [x.mutationId, y.mutationId];
}

describe("14 [CLI-30] an action depends on an earlier action of the same keyspace", () => {
  it("item 1: they write the same key, or a key below a key that the other writes (parents and entity markers)", async () => {
    const r = await rig();
    await goOffline(r);
    const [a, b] = await two(r, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] }, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] });
    expect(dependsOf(r, b)).toEqual([a]);
    const [p, c] = await two(r, { keyspaceId: KS, opType: "Create", changes: [set("issues/i1", {})] }, { keyspaceId: KS, opType: "Set", changes: [set("issues/i1/title", "t")] });
    expect(dependsOf(r, c)).toContain(p);
    const [t, d] = await two(r, { keyspaceId: KS, opType: "Set", changes: [set("issues/i2/title", "t")] }, { keyspaceId: KS, opType: "Set", changes: [del("issues/i2")] });
    expect(dependsOf(r, d)).toContain(t); // the reverse: the later action writes the parent
    const u = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/unrelated", 1)] });
    expect(dependsOf(r, u.mutationId)).toEqual([]);
  });

  it("item 2: a precondition on a key A writes, on a prefix of a key A writes, or after A's mutation id", async () => {
    const r = await rig();
    await goOffline(r);
    const a = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] })).mutationId;
    const onKey = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/x1", 1)], preconditions: [{ key: "settings/a", exists: true }] });
    const onPrefix = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/x2", 1)], preconditions: [{ prefix: "settings/", unchangedSince: "confirmed" }] });
    const after = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/x3", 1)], preconditions: [{ key: "settings/q", afterMutation: a }] });
    const none = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/x4", 1)], preconditions: [{ prefix: "issues/", unchangedSince: "confirmed" }] });
    expect(dependsOf(r, onKey.mutationId)).toContain(a);
    expect(dependsOf(r, onPrefix.mutationId)).toContain(a);
    expect(dependsOf(r, after.mutationId)).toContain(a);
    expect(dependsOf(r, none.mutationId)).toEqual([]);
  });

  it("item 3: a precondition on A's intent row `abpc.intent/{A}`", async () => {
    const r = await rig();
    await goOffline(r);
    const g = (await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] })).mutationId;
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)], preconditions: [{ key: `abpc.intent/${g}`, exists: true }] });
    expect(dependsOf(r, b.mutationId)).toEqual([g]);
  });

  it("item 4: an explicit link, client-only", async () => {
    const r = await rig();
    await goOffline(r);
    const [a, b] = await two(r, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] }, { keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] }, { after: [] });
    expect(dependsOf(r, b)).toEqual([]);
    const c = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/c", 1)] }, { after: [a] });
    expect(dependsOf(r, c.mutationId)).toEqual([a]);
    await goOnline(r);
    // nothing about the link is on the wire
    expect(mutateRequests(r).flatMap((q) => q.mutations.map((m) => m.preconditions.length))).toEqual([0, 0, 0]);
  });

  it("item 5: an op type that declares `barrier: true`, as A or as B", async () => {
    const r = await rig();
    await goOffline(r);
    const a = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] })).mutationId;
    const bar = (await r.engine.submit({ keyspaceId: KS, opType: "Barrier", changes: [set("settings/bar", 1)] })).mutationId;
    const after = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/c", 1)] })).mutationId;
    expect(dependsOf(r, bar)).toContain(a); // B is a barrier: it depends on every earlier action
    expect(dependsOf(r, after)).toContain(bar); // A is a barrier: every later action depends on it
    expect(dependsOf(r, after)).not.toContain(a); // directly it does not; through the barrier it does (dependency is transitive)
  });

  it("item 6: B writes a fenced key whose owning key A writes", async () => {
    const r = await rig();
    await goOffline(r);
    const [a, b] = await two(r, { keyspaceId: KS, opType: "Set", changes: [set("claims/i7", "ana")] }, { keyspaceId: KS, opType: "Fenced", changes: [set("issues/i7/status", "doing")] });
    expect(dependsOf(r, b)).toContain(a);
  });

  it("dependency is transitive", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.changes[0]?.key === "settings/a" ? rejectResult(m, mk(Code.SCHEMA_INVALID)) : undefined) });
    await goOffline(r);
    const a = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] })).mutationId;
    const b = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2), set("settings/b", 1)] })).mutationId;
    const c = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 2)] })).mutationId;
    expect(dependsOf(r, c)).toEqual([b]); // c touches only b's key: transitive through b
    await goOnline(r);
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId).sort()).toEqual([a, b, c].sort());
  });
});

describe("14 [CLI-31] an action is not sent before every action it depends on has a final successful result", () => {
  it("waits for the Appended of A; both SEQUENCED, B may follow A on the same lane only when the engine pipelines", async () => {
    const r = await rig();
    await goOffline(r);
    const [a, b] = await two(r, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] }, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] });
    const before = mutateRequests(r).length;
    await goOnline(r);
    const reqs = mutateRequests(r).slice(before).map((q) => q.mutations.map((m) => m.mutationId));
    expect(reqs).toEqual([[a], [b]]);
  });

  it("the exception: with pipelining, B follows A on the lane in one request", async () => {
    const r = await rig();
    r.engine.pipelineSequenced = true;
    await goOffline(r);
    const [a, b] = await two(r, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] }, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] });
    const before = mutateRequests(r).length;
    await goOnline(r);
    expect(mutateRequests(r).slice(before).map((q) => q.mutations.map((m) => m.mutationId))).toEqual([[a, b]]);
  });

  it("a GATED action that depends on a SEQUENCED action waits for its result", async () => {
    const r = await rig();
    await goOffline(r);
    const [s, g] = await two(r, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] }, { keyspaceId: KS, opType: "Approve", changes: [set("settings/a", 2)] });
    const before = mutateRequests(r).length;
    await goOnline(r);
    expect(mutateRequests(r).slice(before).map((q) => q.mutations.map((m) => m.mutationId))).toEqual([[s], [g]]);
  });

  it("a GATED action that is only ordered after a SEQUENCED action may be sent before it has a result", async () => {
    const r = await rig();
    await goOffline(r);
    const [s, g] = await two(r, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] }, { keyspaceId: KS, opType: "Approve", changes: [set("settings/b", 2)] });
    const before = mutateRequests(r).length;
    await goOnline(r);
    expect(mutateRequests(r).slice(before).map((q) => q.mutations.map((m) => m.mutationId))).toEqual([[s, g]]);
  });
});

describe("14 [CLI-32] a GATED action carries after_client_seq", () => {
  it("is the highest client_seq of an earlier SEQUENCED action of the keyspace, or 0 when there is none", async () => {
    const r = await rig();
    await goOffline(r);
    const g0 = (await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g0", 1)] })).mutationId;
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/s1", 1)] });
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/s2", 1)] });
    const g1 = (await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g1", 1)] })).mutationId;
    const before = mutateRequests(r).length;
    await goOnline(r);
    const ms = mutateRequests(r).slice(before).flatMap((q) => q.mutations);
    expect(Number(ms.find((m) => m.mutationId === g0)?.afterClientSeq)).toBe(0);
    expect(Number(ms.find((m) => m.mutationId === g1)?.afterClientSeq)).toBe(2);
    expect(ms.filter((m) => m.lane === Lane.SEQUENCED).map((m) => Number(m.clientSeq))).toEqual([1, 2]);
  });

  it("waits in the outbox while an earlier SEQUENCED action has no slot yet, and is never sent with a lower value", async () => {
    const r = await rig();
    await goOffline(r);
    const x = (await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/a", 1)] })).mutationId; // pending intent
    const s = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] })).mutationId; // depends on x: no slot yet
    const g = (await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] })).mutationId; // after s
    await goOnline(r);
    expect(sentIds(r)).toEqual([x]); // s waits for x; g waits for the slot of s
    r.server.decide(KS, x, { status: "COMMITTED" });
    await r.engine.settle();
    const last = mutateRequests(r).at(-1);
    expect(last?.mutations.map((m) => m.mutationId)).toEqual([s, g]);
    expect(Number(last?.mutations.find((m) => m.mutationId === g)?.afterClientSeq)).toBe(1); // the slot that s was given
  });
});

describe("14 [CLI-33] an action with no unresolved dependency is sent without waiting for unrelated actions", () => {
  it("a GATED action does not wait for an unrelated GATED action, and a SEQUENCED one does not wait for a dependent that waits", async () => {
    const r = await rig();
    const g1 = (await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g1", 1)] })).mutationId; // undecided
    await r.engine.settle();
    const g2 = (await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g2", 1)] })).mutationId;
    await r.engine.settle();
    expect(sentIds(r)).toEqual([g1, g2]);
    // s2 depends on s1's key; the unrelated s3 is sent although s2 waits
    await goOffline(r);
    const s1 = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] })).mutationId;
    const s2 = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] })).mutationId;
    const s3 = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/c", 1)] })).mutationId;
    const before = mutateRequests(r).length;
    await goOnline(r);
    const first = mutateRequests(r)[before]?.mutations.map((m) => m.mutationId);
    expect(first).toEqual([s1, s3]);
    expect(sentIds(r).includes(s2)).toBe(true); // sent later, after s1's result
  });
});

describe("14 [CLI-34] an action that gets a final rejection fails its dependents with DEPENDENCY_FAILED", () => {
  async function rejectA(r: Rig, aKey: string): Promise<void> {
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.changes[0]?.key === aKey ? rejectResult(m, mk(Code.INVALID_ENVELOPE)) : undefined) });
  }

  it("every dependent that was never sent fails with `DEPENDENCY_FAILED` (class DEAD_LETTER) naming A, and goes to the dead-letter store", async () => {
    const r = await rig();
    await rejectA(r, "settings/a");
    await goOffline(r);
    const [a, b] = await two(r, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] }, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] });
    const c = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 3)] })).mutationId;
    await goOnline(r);
    expect(sentIds(r)).toEqual([a]); // b and c were never sent
    const dl = new Map(r.engine.deadLetterEntries().map((d) => [d.mutationId, d]));
    expect([...dl.keys()].sort()).toEqual([a, b, c].sort());
    expect(dl.get(b)?.reject.code).toBe(Code.DEPENDENCY_FAILED);
    expect(dl.get(b)?.reject.errorClass).toBe(ErrorClass.DEAD_LETTER);
    expect(dl.get(b)?.dependencyOf).toBe(a);
    expect(dl.get(a)?.failedDependents.sort()).toEqual([b, c].sort());
    // the events of A come before those of its dependents (14 [CLI-38])
    const ids: (string | undefined)[] = [a, b, c];
    expect(r.events.filter((e) => "mutationId" in e && ids.includes(e.mutationId)).map((e) => `${e.type}:${"mutationId" in e ? ids.indexOf(e.mutationId) : ""}`)).toEqual([
      "rejected:0",
      "dead-lettered:0",
      "rejected:1",
      "dead-lettered:1",
      "rejected:2",
      "dead-lettered:2",
    ]);
  });

  it("a dependent that was itself sent is not failed, whatever became of A: its own answer ends it", async () => {
    const r = await rig();
    r.engine.pipelineSequenced = true; // A and B go together
    await rejectA(r, "settings/a");
    await goOffline(r);
    const [a, b] = await two(r, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] }, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] });
    const c = (await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/a", 3)] })).mutationId; // GATED: never sent
    await goOnline(r);
    // the server stopped the lane at A: B got Retry{NOT_PROCESSED}; C was never sent and fails
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId).sort()).toEqual([a, c].sort());
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([b]);
    await tick(r, 3000);
    expect(types(r, b)).toEqual(["committed"]); // resent unchanged, and applied by its own answer
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).not.toContain(b);
  });

  it("a sent dependent whose answer leaves its outcome unknown goes to the export hook, not to the dead-letter store", async () => {
    const r = await rig();
    r.engine.pipelineSequenced = true;
    await rejectA(r, "settings/a");
    r.server.inject("mutate", { kind: "result", result: (m) => (m.changes[0]?.key === "settings/a" && m.changes[0].value[0] === 0x32 ? rejectResult(m, mk(Code.OFFLINE_TOO_LONG, { details: { cause: "HORIZON" } })) : undefined) }, { times: 2 });
    await goOffline(r);
    const [a, b] = await two(r, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] }, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] });
    await goOnline(r);
    await tick(r, 3000);
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual([a]);
    expect(r.engine.exportEntries().map((d) => d.mutationId)).toEqual([b]);
    expect(types(r, b)).toEqual(["outcome-unknown", "exported"]);
  });

  it("a GATED action that is only ordered after a rejected SEQUENCED action does not fail", async () => {
    const r = await rig();
    await rejectA(r, "settings/a");
    await goOffline(r);
    const s = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] })).mutationId;
    const g = (await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/other", 1)] })).mutationId;
    await goOnline(r);
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual([s]);
    expect(sentIds(r)).toContain(g);
    r.server.decide(KS, g, { status: "COMMITTED" });
    await r.engine.settle();
    expect(types(r, g)).toEqual(["committed"]);
  });

  it("[CLI-36] DEPENDENCY_FAILED is a client-side code: it never appears on the wire", async () => {
    const r = await rig();
    await rejectA(r, "settings/a");
    await goOffline(r);
    const [, b] = await two(r, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] }, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] });
    await goOnline(r);
    expect(sentIds(r)).not.toContain(b);
    expect(r.engine.deadLetterEntries().find((d) => d.mutationId === b)?.reject.code).toBe(Code.DEPENDENCY_FAILED);
  });
});

describe("14 [CLI-35] while A is held or retried, its dependents keep waiting", () => {
  it("does not send B while A waits for its retry", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "result", result: (m) => (m.changes[0]?.key === "settings/a" ? retryResult(m, RetryReason.STORAGE_UNAVAILABLE, 20_000) : undefined) });
    await goOffline(r);
    const [a, b] = await two(r, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] }, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] });
    await goOnline(r);
    expect(sentIds(r)).toEqual([a]);
    await tick(r, 10_000);
    expect(sentIds(r)).toEqual([a]);
    await tick(r, 20_000);
    expect(sentIds(r)).toEqual([a, a, b]);
    expect(types(r, a)).toEqual(["committed"]);
    // b rewrites the key that a wrote: its seen_version is the confirmed one (14 [CLI-18]), so the server lists it as a stale write
    expect(types(r, b)).toEqual(["overwritten", "committed"]);
  });
});

describe("14 [CLI-37] dependencies are kept by mutation id, so re-stamping never changes them", () => {
  it("a re-identification (CLIENT_UNKNOWN) changes the stamping and not the dependencies", async () => {
    const r = await rig();
    await goOffline(r);
    const [a, b] = await two(r, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] }, { keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] });
    r.server.inject("mutate", { kind: "result", result: (m) => (m.mutationId === a ? rejectResult(m, mk(Code.CLIENT_UNKNOWN)) : undefined) });
    const clientBefore = (r.engine as unknown as { clientId: string }).clientId;
    await goOnline(r);
    await tick(r, 1000);
    expect((r.engine as unknown as { clientId: string }).clientId).not.toBe(clientBefore);
    expect(types(r, a)).toEqual(["committed"]);
    expect(types(r, b)).toEqual(["overwritten", "committed"]);
    expect(dependsOf(r, b)).toEqual([]); // both retired; the edge was by id, and a retired
  });
});

describe("14 [CLI-78] lane watermarks", () => {
  const wmFrame = (r: Rig, clientId: string, epoch: number, processed: number): ReturnType<typeof create<typeof SubscribeResponseSchema>> =>
    create(SubscribeResponseSchema, {
      frame: {
        case: "heartbeat",
        value: create(HeartbeatFrameSchema, {
          cursors: [create(HeartbeatFrame_CursorSchema, { keyspaceId: KS, cursorSeq: r.server.head(KS), headSeq: r.server.head(KS), logEpoch: r.server.epoch(KS) })],
          laneWatermarks: [create(LaneWatermarkSchema, { keyspaceId: KS, lane: Lane.SEQUENCED, processedClientSeq: BigInt(processed), clientId, logEpoch: epoch })],
        }),
      },
    });
  const watermark = (r: Rig): number => (r.engine as unknown as { follows: Map<string, { lanes: { SEQUENCED: { watermark: number } } }> }).follows.get(KS)?.lanes.SEQUENCED.watermark ?? -1;
  const clientOf = (r: Rig): string => (r.engine as unknown as { clientId: string }).clientId;

  it("sends its client_id in Subscribe and LoadState, and tracks the watermark from MutateResponse, LoadState and heartbeats", async () => {
    const r = await rig();
    const sub = r.server.calls.find((c) => c.method === "subscribe")?.request as { clientId: string };
    const load = r.server.calls.find((c) => c.method === "loadState")?.request as { clientId: string };
    expect(sub.clientId).toBe(clientOf(r));
    expect(load.clientId).toBe(clientOf(r));
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(watermark(r)).toBe(1);
  });

  it("ignores a watermark of another client_id or another epoch, and never lets one decrease", async () => {
    const r = await rig();
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(watermark(r)).toBe(1);
    r.server.sendFrame(wmFrame(r, "00000000-0000-4000-8000-000000000000", 1, 9)); // another client instance
    r.server.sendFrame(wmFrame(r, clientOf(r), 2, 9)); // another epoch
    await r.engine.settle();
    expect(watermark(r)).toBe(1);
    r.server.sendFrame(wmFrame(r, clientOf(r), 1, 5));
    await r.engine.settle();
    expect(watermark(r)).toBe(5);
    r.server.sendFrame(wmFrame(r, clientOf(r), 1, 3)); // lower than the one it holds: ignored
    await r.engine.settle();
    expect(watermark(r)).toBe(5);
  });

  it("a watermark never retires, rebases or dead-letters an action by itself", async () => {
    const r = await rig();
    await goOffline(r);
    const a = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] })).mutationId;
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([a]);
    r.server.faults.paused = true;
    r.server.inject("mutate", { kind: "lose" });
    await goOnline(r);
    // the server processed it and the response was lost; a heartbeat watermark covers its slot: the engine asks for the result
    r.server.heartbeat();
    await r.engine.settle();
    expect(r.engine.status().deadLetters).toBe(0);
    r.server.resumeDelivery();
    await tick(r, 3000);
    expect(types(r, a)).toEqual(["committed"]);
    expect(ofType(r, "rejected")).toHaveLength(0);
  });
});
