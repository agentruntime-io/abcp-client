// 14 §14.6.1 RETRY, and the class reaction of 14 [CLI-42].

import { describe, expect, it } from "vitest";
import { set } from "../src/engine/index.js";
import { bound, CLIENT_BACKOFF } from "../src/engine/backoff.js";
import { DEFAULT_CLASS, codeName } from "../src/engine/codes.js";
import { Code, ErrorClass, RetryReason } from "../src/gen/abcp/v2/types_pb.js";
import { rejectWith, reject as mk } from "../src/fake/index.js";
import { KS, mutateRequests, ofType, rejectResult, retryResult, rig, sentIds, tick, types } from "./helpers/engine.js";

describe("14 [CLI-42] a client reacts to a Reject by its error_class, also for codes it does not know", () => {
  // the codes that this suite does not treat apart: every other code of class DEAD_LETTER or CONFLICT (17 [CONF-33], no automatic re-authoring)
  const special = new Set<number>([Code.DEPENDENCY_FAILED, Code.OFFLINE_TOO_LONG, Code.PERMISSION_DENIED, Code.ACTOR_MISMATCH, Code.KEYSPACE_ARCHIVED]);
  const finals = [...DEFAULT_CLASS].filter(([code, cls]) => (cls === ErrorClass.DEAD_LETTER || cls === ErrorClass.CONFLICT) && !special.has(code));

  it.each(finals)("code %s of class %s: the reject is reported, the action is sent once, and nothing is re-authored, re-driven or resent", async (code, cls) => {
    let handled = 0;
    const r = await rig({ cfg: { conflictHandler: () => void handled++ } });
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, mk(code, code === Code.STALE ? { stale: { cause: 1, currentSeq: 0n } } : {})) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 700_000); // no age bound, no resend
    expect(sentIds(r)).toEqual([a.mutationId]);
    expect(r.engine.outbox()).toHaveLength(0);
    expect(ofType(r, "rejected").filter((e) => e.mutationId === a.mutationId)).toHaveLength(1);
    expect(ofType(r, "rejected")[0]?.reject?.code).toBe(code);
    if (cls === ErrorClass.CONFLICT) {
      expect(handled).toBe(1);
      expect(types(r, a.mutationId)).toEqual(["conflict", "rejected"]);
    } else {
      expect(r.engine.deadLetterEntries().map((d) => d.reason)).toEqual([codeName(code)]);
    }
    expect(r.server.records(KS).filter((x) => x.opType === "Set")).toHaveLength(0); // nothing was written in its place
  });

  it("a code that the client does not know is handled by its class: DEAD_LETTER, CONFLICT and RETRY", async () => {
    const r = await rig({ cfg: { conflictHandler: () => undefined } });
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, rejectWith(900, ErrorClass.DEAD_LETTER)) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(r.engine.deadLetterEntries()[0]?.reason).toBe("CODE_900");
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, rejectWith(901, ErrorClass.CONFLICT)) });
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
    await r.engine.settle();
    expect(types(r, b.mutationId)).toEqual(["conflict", "rejected"]);
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, rejectWith(902, ErrorClass.RETRY)) });
    const c = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/c", 1)] });
    await r.engine.settle();
    await tick(r, 3000);
    expect(types(r, c.mutationId)).toEqual(["committed"]); // resent unchanged
  });
});

describe("14 [CLI-43] on Retry the client resends the identical content after the larger of after_ms and its backoff", () => {
  async function times(afterMs: number, fixed: number, n: number): Promise<number[]> {
    const r = await rig({ heartbeatMs: 15_000 });
    r.random.fixed = fixed;
    for (let i = 0; i < n; i++) r.server.inject("mutate", { kind: "result", result: (m) => retryResult(m, RetryReason.RATE_LIMITED, afterMs) });
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    for (let i = 0; i < 5000 && mutateRequests(r).length <= n; i++) await tick(r, 1_000);
    return r.server.calls.filter((c) => c.method === "mutate").map((c) => c.at);
  }
  const gapsOf = (t: number[]): number[] => t.slice(1).map((x, i) => x - (t[i] as number));

  it("waits a delay drawn from [0, min(5 min, 1 s x 2^n)]: the largest draw is the bound, the smallest is 0", async () => {
    const hi = gapsOf(await times(0, 0.999999, 11));
    // before the n-th consecutive resend, counted from 0: [0, min(300 s, 1 s x 2^n)]; the largest draw is the bound itself
    hi.forEach((gap, n) => expect(gap).toBe(bound(n, CLIENT_BACKOFF)));
    expect(hi.slice(0, 4)).toEqual([1000, 2000, 4000, 8000]);
    expect(hi.slice(-2)).toEqual([300_000, 300_000]); // the cap of 5 min
    const lo = gapsOf(await times(0, 0, 4));
    expect(lo).toEqual([0, 0, 0, 0]); // the smallest draw is 0: the resend is at once
  });

  it("resends after Retry.after_ms when it is the larger", async () => {
    const gaps = gapsOf(await times(30_000, 0, 2));
    expect(gaps[0]).toBe(30_000);
    expect(gaps[1]).toBe(30_000);
    const both = gapsOf(await times(1500, 0.999999, 4)); // the backoff is larger from the second resend on
    expect(both).toEqual([1500, 2000, 4000, 8000]);
  });

  it("an after_ms above the table's maximum counts as the maximum (04 [MAN-91])", async () => {
    const r = await rig();
    r.random.fixed = 0;
    r.server.inject("mutate", { kind: "result", result: (m) => retryResult(m, RetryReason.RATE_LIMITED, 4_000_000) });
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 3_500_000);
    expect(mutateRequests(r)).toHaveLength(1);
    await tick(r, 100_000);
    expect(mutateRequests(r)).toHaveLength(2);
  });

  it("a Retry loop has no age bound of its own: the action stays pending", async () => {
    const r = await rig({ serverOptions: { limits: { maxOfflineSeconds: 120 } } });
    r.server.inject("mutate", { kind: "result", result: (m) => retryResult(m, RetryReason.STORAGE_UNAVAILABLE, 1000) }, { times: 1000 });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    for (let i = 0; i < 30; i++) await tick(r, 60_000); // far beyond max_offline
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([a.mutationId]);
    expect(r.engine.status().deadLetters).toBe(0);
    expect(r.engine.status().exports).toBe(0);
  });
});

describe("14 [CLI-44] the specific retry reasons", () => {
  it("OUT_OF_ORDER: the client first sends what the server misses; with none to send it re-identifies", async () => {
    const r = await rig();
    r.engine.pipelineSequenced = true;
    await goOff(r);
    const a = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] })).mutationId;
    const b = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] })).mutationId;
    // a gets an unrecorded Retry, b NOT_PROCESSED; then the client loses a (it lost its outbox): b's slot has nothing before it
    r.server.inject("mutate", { kind: "result", result: (m) => (m.mutationId === a ? retryResult(m, RetryReason.STORAGE_UNAVAILABLE, 60_000) : undefined) });
    const client0 = (r.engine as unknown as { clientId: string }).clientId;
    await tick(r, 1000);
    (r.engine as unknown as { entries: Map<string, unknown> }).entries.delete(a);
    await tick(r, 70_000);
    expect((r.engine as unknown as { clientId: string }).clientId).not.toBe(client0);
    expect(types(r, b)).toEqual(["committed"]);
  });

  it("LANE_NOT_READY and NOT_PROCESSED and CONTENTION: resent unchanged, at the same client_seq", async () => {
    for (const reason of [RetryReason.LANE_NOT_READY, RetryReason.NOT_PROCESSED, RetryReason.CONTENTION, RetryReason.KEYSPACE_SEEDING, RetryReason.BLOB_PENDING]) {
      const r = await rig();
      r.server.inject("mutate", { kind: "result", result: (m) => retryResult(m, reason, 100) });
      const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
      await r.engine.settle();
      await tick(r, 3000);
      const reqs = mutateRequests(r);
      expect(reqs.map((q) => Number(q.mutations[0]?.clientSeq))).toEqual([1, 1]);
      expect(types(r, a.mutationId)).toEqual(["committed"]);
    }
  });

  it("a RetryReason that the client does not know is a retry after Retry.after_ms (15 [COMPAT-40])", async () => {
    const r = await rig();
    r.random.fixed = 0;
    r.server.inject("mutate", { kind: "result", result: (m) => retryResult(m, 99 as RetryReason, 20_000) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 15_000);
    expect(mutateRequests(r)).toHaveLength(1);
    await tick(r, 6000);
    expect(mutateRequests(r)).toHaveLength(2);
    expect(types(r, a.mutationId)).toEqual(["committed"]);
  });

  it("GATE_UNAVAILABLE and GATE_TIMEOUT for a registered intent: the client waits for the DECISION or polls GetMutationResult, and does not resend", async () => {
    const r = await rig();
    const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] }); // the fake answers GATE_TIMEOUT with intent_seq
    await r.engine.settle();
    await tick(r, 20_000);
    await tick(r, 40_000);
    expect(mutateRequests(r)).toHaveLength(1);
    expect(r.server.calls.filter((c) => c.method === "getMutationResult").length).toBeGreaterThan(0);
    // the decision comes while the stream is down: the poll finds it
    r.server.faults.paused = true;
    r.server.decide(KS, g.mutationId, { status: "COMMITTED" });
    await tick(r, 100_000);
    r.server.resumeDelivery();
    await r.engine.settle();
    expect(types(r, g.mutationId)).toEqual(["committed"]);
    expect(mutateRequests(r)).toHaveLength(1); // never resent
  });
});

async function goOff(r: Awaited<ReturnType<typeof rig>>): Promise<void> {
  r.server.disconnect();
  await r.engine.settle();
}

