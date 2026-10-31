// The probes of the review of the client engine, one test for each finding and each reading that the text decides, run against the
// fake server as it answers since it follows the Go server (duplicates first, unlogged intents, the lifecycle hold, the limits).

import { describe, expect, it } from "vitest";
import { create, toBinary } from "@bufbuild/protobuf";
import { del, jsonBytes, set, StaticManifests, type ConflictContext, type EngineEvent } from "../src/engine/index.js";
import { MutateRequestSchema } from "../src/gen/abpc/v2/sync_pb.js";
import { ChangeKind, Code, ErrorClass, RetryReason, StaleCause, SuccessorRuleSchema, type SuccessorRule } from "../src/gen/abpc/v2/types_pb.js";
import { LaneState } from "../src/engine/follow.js";
import { ManualClock, reject as mk, rejectWith } from "../src/fake/index.js";
import { goOffline, goOnline, kc, KS, MANIFEST, mutateRequests, ofType, rejectResult, retryResult, rig, sentIds, tick, types, type Rig } from "./helpers/engine.js";

const rule = (prefix: string, keyspaceId: string): SuccessorRule => create(SuccessorRuleSchema, { prefix, keyspaceId });
const calls = (r: Rig): string[] => r.server.calls.map((c) => c.method);
const kinds = (r: Rig, id: string): string[] => types(r, id).filter((t) => t !== "held");
const resultCalls = (r: Rig, id: string): number => r.server.calls.filter((c) => c.method === "getMutationResult" && (c.request as { mutationId: string }).mutationId === id).length;

// ---------------------------------------------------------------------------------------------------------------------
// Finding 1: [CLI-52], [ARC-50]: a GATED action whose last result is a Retry may be registered, logged or not

/**
 * A GATED action of an op type with `log_intent: false` that the server registered and then decided, while the client's last result
 * for it is `Retry{LANE_NOT_READY}` with `intent_seq` 0 (the lane wait was not satisfied when it was answered, 08 [GATE-57]), and
 * the DECISION record has not reached the client: only `GetMutationResult` from the source can tell it that the intent exists.
 */
async function decidedBehindTheClient(r: Rig, status: "COMMITTED" | "REJECTED" = "COMMITTED"): Promise<{ s: string; g: string }> {
  r.random.fixed = 0.999999; // the first resend waits a full second, so that the test is in time
  r.server.inject("mutate", { kind: "delay", ms: 500 }, { match: (req) => (req as { mutations: { opType: string }[] }).mutations.some((m) => m.opType === "Set") });
  const s = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/s", 1)] }); // slot 1 of the SEQUENCED lane, answered late
  const g = await r.engine.submit({ keyspaceId: KS, opType: "ApproveQuiet", changes: [set("settings/g", 1)] }); // waits for slot 1
  await r.engine.settle();
  const q = mutateRequests(r).find((x) => x.mutations.some((m) => m.mutationId === g.mutationId));
  expect(q?.mutations.find((m) => m.mutationId === g.mutationId)).toMatchObject({ afterClientSeq: 1n });
  expect(r.engine.outbox().find((e) => e.mutationId === g.mutationId)).toMatchObject({ everSent: true, registered: false }); // LANE_NOT_READY, intent_seq 0: nothing says "registered"
  r.server.faults.paused = true; // no record reaches the client
  await tick(r, 500); // the request of s is handled: slot 1 is processed, the wait is satisfied
  r.server.decide(KS, g.mutationId, status === "COMMITTED" ? { status } : { status, reject: mk(Code.GATE_REJECTED) });
  return { s: s.mutationId, g: g.mutationId };
}

describe("14 [CLI-52], 02 [ARC-50]: a GATED action whose last result is a Retry is moved only after GetMutationResult answers known = false", () => {
  it("ARCHIVED: an unlogged intent that the client never heard of as registered is not dead-lettered; the source's answer is its result", async () => {
    const r = await rig({ keyspaces: [KS, "dst1"] });
    const { g } = await decidedBehindTheClient(r);
    r.server.archive(KS, [rule("", "dst1")]);
    await r.engine.settle();
    await tick(r, 10_000);
    expect(resultCalls(r, g)).toBeGreaterThan(0);
    expect(types(r, g)).not.toContain("dead-lettered");
    expect(types(r, g)).not.toContain("rejected");
    expect(r.engine.deadLetterEntries()).toHaveLength(0);
  });

  it("SPLIT: it is not re-routed, so the decided intent is not registered a second time in the successor", async () => {
    const r = await rig({ keyspaces: [KS, "dst1"] });
    const { g } = await decidedBehindTheClient(r);
    r.server.split(KS, [rule("", "dst1")]);
    await r.engine.settle();
    await tick(r, 10_000);
    expect(resultCalls(r, g)).toBeGreaterThan(0);
    expect(ofType(r, "rerouted")).toHaveLength(0);
    expect(mutateRequests(r).flatMap((q) => q.mutations).filter((m) => m.keyspaceId === "dst1")).toHaveLength(0);
    expect(r.server.records("dst1").filter((x) => x.mutationId === g)).toHaveLength(0);
  });

  it("when the source answers known = false (the action was never registered: Retry{RATE_LIMITED}), it is moved as before", async () => {
    const r = await rig({ keyspaces: [KS, "dst1"] });
    r.random.fixed = 0.999999;
    // the server answers the over-the-cap action as it does: RATE_LIMITED, not registered, intent_seq 0
    r.server.inject("mutate", { kind: "result", result: (m) => (m.opType === "ApproveQuiet" ? retryResult(m, RetryReason.RATE_LIMITED, 0) : undefined) });
    const over = await r.engine.submit({ keyspaceId: KS, opType: "ApproveQuiet", changes: [set("settings/over", 1)] });
    await r.engine.settle();
    expect(r.engine.outbox().find((e) => e.mutationId === over.mutationId)).toMatchObject({ everSent: true, registered: false, lastResult: { kind: "retry", what: "RATE_LIMITED" } });
    r.server.split(KS, [rule("", "dst1")]);
    await tick(r, 10_000);
    expect(resultCalls(r, over.mutationId)).toBeGreaterThan(0);
    expect(ofType(r, "rerouted").map((e) => [e.mutationId, e.to])).toEqual([[over.mutationId, "dst1"]]);
    expect(kinds(r, over.mutationId)).toEqual(["rerouted"]);
    expect(r.server.openItems("dst1").pendingIntents).toBe(1); // registered once, in the successor
  });
});

// ---------------------------------------------------------------------------------------------------------------------
// Finding 2: [CLI-24], [CLI-53], [CLI-64], [ARC-52] item 6: nothing is sent to a keyspace on a new session before its KeyspaceAdded

describe("14 [CLI-24], [CLI-53], [CLI-64], 02 [ARC-52] item 6: an action of an earlier epoch is not resent before the client has learned the epoch changed", () => {
  it("a spend action whose response was lost, then a restore while offline: after the reconnect the answer of GetMutationResults comes first, then the one resend, recorded as such", async () => {
    const r = await rig();
    r.server.faults.paused = true; // the record is not delivered either
    r.server.inject("mutate", { kind: "lose" });
    const p = await r.engine.submit({ keyspaceId: KS, opType: "Pay", changes: [set("settings/p", 1)] });
    await r.engine.settle(); // applied at the server, no answer at the client
    expect(r.server.records(KS).filter((x) => x.mutationId === p.mutationId)).toHaveLength(1);
    await goOffline(r);
    r.server.restore(KS, 0n); // the sale is lost; its id is voided
    r.server.faults.paused = false;
    await goOnline(r);
    await tick(r, 5000);
    const order = calls(r);
    const resolve = order.indexOf("getMutationResults");
    const secondMutate = order.indexOf("mutate", order.indexOf("mutate") + 1);
    expect(resolve).toBeGreaterThan(-1);
    expect(secondMutate).toBeGreaterThan(resolve); // never resent before the epoch is resolved
    expect(kinds(r, p.mutationId)).toEqual(["resent", "committed"]);
    expect(ofType(r, "resent")[0]).toMatchObject({ mutationId: p.mutationId, cause: "VOIDED" });
    expect(r.engine.journalEntries().find((j) => j.mutationId === p.mutationId)?.spendResent).toBe(true);
    expect(sentIds(r).filter((id) => id === p.mutationId)).toHaveLength(2);
  });

  it("an ordinary action that was sent in the old epoch is never resent: it is resolved by GetMutationResults and ends as outcome unknown or as the conflict", async () => {
    const r = await rig();
    r.server.faults.paused = true;
    r.server.inject("mutate", { kind: "lose" });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await goOffline(r);
    r.server.restore(KS, 0n);
    r.server.faults.paused = false;
    await goOnline(r);
    await tick(r, 5000);
    expect(sentIds(r).filter((id) => id === a.mutationId)).toHaveLength(1); // the one send that was lost
    expect(kinds(r, a.mutationId)).toEqual(["conflict", "rejected"]);
    expect(ofType(r, "conflict")[0]?.reject.stale?.cause).toBe(StaleCause.EPOCH);
  });
});

// ---------------------------------------------------------------------------------------------------------------------
// Finding 3: [CLI-7]: an outcome learned after the client stopped following a keyspace retires the action at once

describe("14 [CLI-7]: when the client stops following a keyspace, an action with a successful outcome retires, also when the outcome is learned afterwards", () => {
  it("ARCHIVED: the response is lost, the record is never delivered; the poll's Appended retires the action at once", async () => {
    const r = await rig({ keyspaces: [KS, "dst1"] });
    r.server.faults.paused = true;
    r.server.inject("mutate", { kind: "lose" });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    r.server.archive(KS, [rule("", "dst1")]);
    await r.engine.settle(); // the drop is handled before time moves
    await tick(r, 600_000);
    expect(kinds(r, a.mutationId)).toEqual(["committed"]);
    expect(r.engine.outbox()).toHaveLength(0);
    expect(r.engine.journalEntries().map((j) => j.mutationId)).toEqual([a.mutationId]);
  });

  it("MOVED: it retires once every successor that the client loads and whose rule takes a key of the action is loaded at a checkpoint at or above its seq, and at once when no rule takes a key", async () => {
    const r = await rig();
    r.server.createKeyspaceNow("dst1"); // the successors are new keyspaces: the client does not follow them yet
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Create", changes: [set("issues/i1", {})] }); // dst1
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] }); // matches no rule
    r.server.faults.paused = true;
    r.server.inject("mutate", { kind: "lose" });
    await goOnline(r); // one request carries both; applied, and the response is lost
    r.server.split(KS, [rule("issues/", "dst1")]);
    await r.engine.settle();
    await tick(r, 600_000);
    expect(kinds(r, b.mutationId)).toEqual(["committed"]); // no successor takes its key: at once
    expect(kinds(r, a.mutationId)).toEqual(["committed"]); // dst1 is loaded at its seq
    expect(r.engine.cursor("dst1")?.seq).toBeGreaterThanOrEqual(1n);
    expect(r.engine.outbox()).toHaveLength(0);
  });

  it("MOVED: an action whose successor is not loaded at its seq stays pending until it is", async () => {
    const r = await rig();
    r.server.createKeyspaceNow("dst1");
    r.server.faults.paused = true;
    r.server.inject("mutate", { kind: "lose" });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Create", changes: [set("issues/i1", {})] });
    await r.engine.settle();
    r.server.inject("loadState", { kind: "status", status: "Unavailable" }, { times: 1, match: (req) => (req as { cursor?: { keyspaceId: string } }).cursor?.keyspaceId === "dst1" });
    await r.engine.settle();
    r.server.split(KS, [rule("issues/", "dst1")]);
    await r.engine.settle();
    expect(r.engine.cursor("dst1")?.loaded).toBe(false);
    expect(kinds(r, a.mutationId)).toEqual([]); // dst1 is not loaded yet, so the action is still pending
    await tick(r, 60_000);
    expect(kinds(r, a.mutationId)).toEqual(["committed"]);
  });
});

// ---------------------------------------------------------------------------------------------------------------------
// Finding 4: [CLI-53]: the events of the answers come in the authoring order

describe("14 [CLI-53]: the events that the answers cause come in the authoring order of the actions, each conflict finished before the next answer is acted on", () => {
  it("a journalled action that a restore voided (conflict, handler, rejected) before a pending one that stays unknown (outcome-unknown, exported)", async () => {
    const clock = new ManualClock();
    const handled: string[] = [];
    const r = await rig({
      clock,
      cfg: {
        conflictHandler: async (c: ConflictContext) => {
          await new Promise<void>((resolve) => clock.setTimer(5000, resolve)); // the user takes five seconds to decide
          handled.push(c.action.mutationId);
        },
      },
    });
    r.random.fixed = 0.999999;
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    r.server.inject("mutate", { kind: "status", status: "Unavailable" }); // b was sent, and no server answered
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
    await r.engine.settle();
    r.server.restore(KS, 0n); // a is voided; b has no binding at all: unknown
    await r.engine.settle();
    expect(handled).toEqual([]);
    await tick(r, 5000);
    await tick(r, 1000);
    const mine = (e: EngineEvent): boolean => "mutationId" in e && (e.mutationId === a.mutationId || e.mutationId === b.mutationId) && e.type !== "held" && e.type !== "overwritten";
    expect(r.events.filter(mine).map((e) => `${(e as { mutationId: string }).mutationId === a.mutationId ? "A" : "B"}:${e.type}`)).toEqual(["A:committed", "A:reverted", "A:conflict", "A:rejected", "B:outcome-unknown", "B:exported"]);
    expect(handled).toEqual([a.mutationId]);
  });
});

// ---------------------------------------------------------------------------------------------------------------------
// Finding 5: Table 14.1 UNKNOWN_OP_TYPE: the version in force comes from LoadState, and a resend waits for the hold backoff

describe("14 Table 14.1 UNKNOWN_OP_TYPE: the client reloads the version that LoadStateResponse.manifest_version names, and resends with the hold backoff", () => {
  const v2 = (...ops: string[]): StaticManifests => new StaticManifests(MANIFEST, { version: 2, templates: MANIFEST.templates, opTypes: Object.fromEntries(ops.map((o) => [o, {}])) });

  it("the manifest in force no longer declares the op type: one LoadState, the reload of that version, and the action is dead-lettered at once; no loop of Mutate calls", async () => {
    const manifests = v2("Create");
    const r = await rig({ manifests });
    r.server.faults.paused = true; // the client has not heard of version 2
    r.server.activateManifest(KS, 2);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(manifests.reloads).toEqual([2]);
    expect(kinds(r, a.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(r.engine.deadLetterEntries()[0]?.reason).toBe("UNKNOWN_OP_TYPE");
    expect(mutateRequests(r)).toHaveLength(1);
    expect(r.server.calls.filter((c) => c.method === "loadState").length).toBeGreaterThanOrEqual(2); // the cold load, and the one that names the version in force
  });

  it("a server that keeps answering UNKNOWN_OP_TYPE for a type that the version in force declares is resent only with the hold backoff, never in a loop", async () => {
    const manifests = new StaticManifests(MANIFEST);
    const r = await rig({ manifests });
    r.random.fixed = 0.5;
    r.server.inject("mutate", { kind: "result", result: (m) => (m.opType === "Set" ? rejectResult(m, mk(Code.UNKNOWN_OP_TYPE)) : undefined) }, { times: 1000 });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(mutateRequests(r)).toHaveLength(1); // the clock has not moved: no resend
    expect(manifests.reloads).toEqual([1]);
    await tick(r, 30_000); // the first probe of the hold backoff: 1 min bound, drawn at 0.5
    const after30 = mutateRequests(r).length;
    expect(after30).toBeLessThanOrEqual(2);
    await tick(r, 600_000);
    expect(mutateRequests(r).length).toBeLessThanOrEqual(8); // a few probes in ten minutes: the hold backoff doubles from one minute
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([a.mutationId]);
  });
});

// ---------------------------------------------------------------------------------------------------------------------
// Finding 6: [CLI-38], [CLI-40]: `corrected` where it is due, whatever a restart did to the memory

describe("14 [CLI-38], [CLI-40]: the verdict `corrected` or `committed` is decided from state that survives a restart", () => {
  it("a GATED action whose gate made effects while the engine was stopped is `corrected` after the restart, naming the effects (CLI-15: placeholders replaced)", async () => {
    const r = await rig();
    const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] });
    await r.engine.settle();
    await r.engine.stop();
    r.server.decide(KS, g.mutationId, { status: "COMMITTED", effects: [kc("settings/effect", "set", 7)] });
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 5 });
    await tick(r2, 5000);
    expect(r2.events.filter((e) => e.type === "committed" || e.type === "corrected").map((e) => [e.type, (e as { keys?: string[] }).keys])).toEqual([["corrected", ["settings/effect"]]]);
    expect(r2.engine.journalEntries().find((j) => j.mutationId === g.mutationId)?.reported).toBe("corrected");
  });

  it("the same without a restart is `corrected` as well, and a gate that made no effect is `committed` after a restart", async () => {
    const r = await rig();
    const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] });
    await r.engine.settle();
    r.server.decide(KS, g.mutationId, { status: "COMMITTED", effects: [kc("settings/effect", "set", 7)] });
    await r.engine.settle();
    expect(ofType(r, "corrected").map((e) => e.keys)).toEqual([["settings/effect"]]);

    const q = await rig();
    const h = await q.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/h", 1)] });
    await q.engine.settle();
    await q.engine.stop();
    q.server.decide(KS, h.mutationId, { status: "COMMITTED" });
    const q2 = await rig({ storage: q.storage, server: q.server, clock: q.clock, seed: 5 });
    await tick(q2, 5000);
    expect(q2.events.filter((e) => e.type === "committed" || e.type === "corrected").map((e) => e.type)).toEqual(["committed"]);
  });

  it("a CAS that was shown as having no effect and then commits is `corrected` also across a restart of the engine (CLI-11)", async () => {
    const r = await rig();
    await goOffline(r);
    // the confirmed state lacks the expected value: the prediction gives the CAS no effect
    const c = await r.engine.submit({ keyspaceId: KS, opType: "CasSet", changes: [{ key: "notes/n1", kind: ChangeKind.CAS, value: jsonBytes("new"), encoding: "json/v1", expected: { value: jsonBytes("old") } }] });
    expect(r.engine.view(KS).noEffectKeys(c.mutationId)).toEqual(["notes/n1"]);
    await r.engine.stop();
    // another writer makes the expectation true; the engine restarts; the CAS commits
    r.server.write(KS, [kc("notes/n1", "set", "old")]);
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 8 });
    await tick(r2, 5000);
    expect(r2.events.filter((e) => e.type === "committed" || e.type === "corrected").map((e) => [e.type, (e as { keys?: string[] }).keys])).toEqual([["corrected", ["notes/n1"]]]);
  });
});

// ---------------------------------------------------------------------------------------------------------------------
// Finding 7: [CLI-26]: a request never passes the limits of 07 §7.9

describe("14 [CLI-26], 07 [MUT-4]: GATED actions count towards max_mutations_per_request and max_request_bytes", () => {
  it("six GATED actions and three SEQUENCED ones with a limit of four mutations: no request is refused, none carries more than four", async () => {
    const r = await rig({ serverOptions: { limits: { maxMutationsPerRequest: 4 } } });
    await goOffline(r);
    const ids: string[] = [];
    for (let i = 0; i < 3; i++) ids.push((await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set(`settings/s${i}`, i)] })).mutationId);
    for (let i = 0; i < 6; i++) ids.push((await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set(`settings/g${i}`, i)] })).mutationId);
    await goOnline(r);
    await tick(r, 10_000);
    const reqs = mutateRequests(r);
    expect(Math.max(...reqs.map((q) => q.mutations.length))).toBeLessThanOrEqual(4);
    expect(r.server.calls.filter((c) => c.method === "mutate").length).toBe(reqs.length);
    for (const id of ids) expect(sentIds(r).filter((x) => x === id)).toHaveLength(1); // each sent once: no fallback to one request each
    expect(r.engine.outbox().filter((e) => e.lane === "SEQUENCED")).toHaveLength(0);
  });

  it("with a limit of max_request_bytes that two of the GATED actions fill, a request never passes it", async () => {
    const big = "x".repeat(400);
    const r = await rig({ serverOptions: { limits: { maxRequestBytes: 1500 } } });
    await goOffline(r);
    for (let i = 0; i < 6; i++) await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set(`settings/g${i}`, big)] });
    await goOnline(r);
    await tick(r, 10_000);
    const reqs = mutateRequests(r);
    expect(reqs.length).toBeGreaterThan(1);
    for (const q of reqs) expect(toBinary(MutateRequestSchema, q).byteLength).toBeLessThanOrEqual(1500);
    expect(sentIds(r)).toHaveLength(6); // once each
  });
});

// ---------------------------------------------------------------------------------------------------------------------
// Finding 8: the memos are bounded

describe("the engine's memos are bounded: overwrittenSeen and internalErrors", () => {
  it("overwrittenSeen holds a bounded number of mutation ids, and a record that is delivered again still emits `overwritten` once", async () => {
    const r = await rig();
    const seen = (r.engine as unknown as { overwrittenSeen: Set<string> }).overwrittenSeen;
    for (let i = 0; i < 1200; i++) r.server.write(KS, [kc("settings/a", "set", i)], { overwroteNewer: [0] });
    await r.engine.settle();
    expect(ofType(r, "overwritten")).toHaveLength(1200);
    expect(seen.size).toBeLessThanOrEqual(1024);
  });

  it("internalErrors keeps the last ones only", async () => {
    const r = await rig();
    const run = (r.engine as unknown as { run(fn: () => unknown): Promise<unknown> }).run.bind(r.engine);
    for (let i = 0; i < 400; i++) await run(() => Promise.reject(new Error(`boom ${i}`))).catch(() => undefined);
    const errs = (r.engine as unknown as { internalErrors: unknown[] }).internalErrors;
    expect(errs.length).toBeLessThanOrEqual(100);
    expect(String((errs.at(-1) as Error).message)).toBe("boom 399");
  });
});

// ---------------------------------------------------------------------------------------------------------------------
// Reading 2: [CLI-27]: whether a slot is consumed is decided by the lane's watermark, never by the result's code

describe("14 [CLI-27]: slot use is decided by the lane's watermark, never by the code of a result", () => {
  it("a reject that the client makes itself (a lone mutation that gets ResourceExhausted with no detail) recorded nothing: its slot goes to the next action, which is at slot 1, with no CLIENT_UNKNOWN and no re-identification", async () => {
    const r = await rig();
    const clientId = (r.engine as unknown as { clientId: string }).clientId;
    r.server.inject("mutate", { kind: "status", status: "ResourceExhausted" });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(kinds(r, a.mutationId)).toEqual(["rejected", "dead-lettered"]);
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
    await r.engine.settle();
    const sentB = mutateRequests(r).flatMap((q) => q.mutations).filter((m) => m.mutationId === b.mutationId);
    expect(sentB.map((m) => m.clientSeq)).toEqual([1n]);
    expect(kinds(r, b.mutationId)).toEqual(["committed"]);
    expect(r.events.filter((e) => e.type === "held")).toHaveLength(0);
    expect((r.engine as unknown as { clientId: string }).clientId).toBe(clientId);
  });

  it("an undecodable entry was never in a request: its slot is not consumed either", async () => {
    const r = await rig();
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const e = r.engine.outbox().find((x) => x.mutationId === a.mutationId);
    if (e === undefined) throw new Error("no entry");
    (r.engine as unknown as { decoded: Map<string, unknown> }).decoded.delete(a.mutationId);
    e.content = new Uint8Array([0xff, 0xff, 0xff]); // the stored content cannot be decoded
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
    await goOnline(r);
    await tick(r, 3000);
    expect(kinds(r, a.mutationId)).toEqual(["rejected", "dead-lettered"]);
    const sentB = mutateRequests(r).flatMap((q) => q.mutations).filter((m) => m.mutationId === b.mutationId);
    expect(sentB.map((m) => m.clientSeq)).toEqual([1n]);
    expect(r.events.filter((x) => x.type === "held")).toHaveLength(0);
  });

  it("LaneState: a slot that an ended action left waits for a reading; a reading that leaves it just above the watermark gives it to the next action, one that passes it keeps it consumed", () => {
    const lane = new LaneState();
    expect([lane.allocate("a"), lane.allocate("b"), lane.allocate("c")]).toEqual([1, 2, 3]);
    lane.defer(1, "a");
    lane.defer(3, "c");
    expect(lane.allocate("d")).toBe(4); // nothing is decided before a reading
    lane.settle(); // watermark 0: slot 1 is just above it and unconsumed; slot 3 is not just above it
    expect(lane.allocate("e")).toBe(1);
    lane.raise(2); // the server consumed slots 1 and 2
    lane.settle(); // slot 3 is just above the watermark now, and the reading leaves it unconsumed
    expect(lane.allocate("f")).toBe(3);
    lane.defer(4, "d");
    lane.raise(4); // consumed after all
    lane.settle();
    expect(lane.allocate("g")).toBe(5);
  });

  it("an outcome unknown keeps its slot until the watermark says: the next action does not take it", async () => {
    const r = await rig();
    r.server.faults.paused = true;
    r.server.inject("mutate", { kind: "lose" }); // a is applied, its response is lost
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    const lane = (r.engine as unknown as { follows: Map<string, { lanes: { SEQUENCED: LaneState } }> }).follows.get(KS)?.lanes.SEQUENCED;
    expect(lane?.assigned.get(1)).toBe(a.mutationId);
  });
});

// ---------------------------------------------------------------------------------------------------------------------
// Reading 4: A.7 "stop and report"

describe("A.7: a Mutate call that fails with a status that has its own row and no ABPC detail", () => {
  for (const status of ["NotFound", "AlreadyExists", "FailedPrecondition"] as const) {
    it(`${status} with no detail: stop and report (the lane is held and held says so), resend only with the hold backoff, clear on a result`, async () => {
      const r = await rig();
      r.random.fixed = 0.5;
      r.server.inject("mutate", { kind: "status", status });
      const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
      await r.engine.settle();
      expect(r.events.filter((e) => e.type === "held").map((e) => [(e as { state: string }).state, (e as { scope: string }).scope, (e as { code: string }).code])).toEqual([["start", "lane", status]]);
      expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "lane", lane: "SEQUENCED", code: status })]);
      expect(mutateRequests(r)).toHaveLength(1);
      const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
      await tick(r, 20_000);
      expect(mutateRequests(r)).toHaveLength(1); // not resent: no loop, and the lane is stopped
      await tick(r, 15_000); // the first probe of the hold backoff, drawn at 0.5 of a minute
      expect(kinds(r, a.mutationId)).toEqual(["committed"]);
      expect(kinds(r, b.mutationId)).toEqual(["committed"]);
      expect(r.events.filter((e) => e.type === "held").map((e) => (e as { state: string }).state)).toEqual(["start", "stop"]);
    });
  }

  it("PermissionDenied with no detail is read as PERMISSION_DENIED: refresh the token once and resend; the second one stops and reports", async () => {
    let refreshes = 0;
    const r = await rig({ cfg: { tokens: { refresh: () => (refreshes++, Promise.resolve(true)) } } });
    r.random.fixed = 0.5;
    r.server.inject("mutate", { kind: "status", status: "PermissionDenied" }, { times: 2 });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(refreshes).toBe(1);
    expect(mutateRequests(r)).toHaveLength(2); // the one resend after the refresh
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "lane", code: "PermissionDenied" })]);
    await tick(r, 40_000);
    expect(kinds(r, a.mutationId)).toEqual(["committed"]);
    expect(refreshes).toBe(1);
  });
});

// ---------------------------------------------------------------------------------------------------------------------
// Reading 5: [CLI-44]: the Retry that answers the add of a keyspace has no age bound of its own

describe("14 [CLI-44]: a Retry that answers an add (KEYSPACE_SEEDING) is retried with the client backoff, with no hold and no age bound", () => {
  it("UpdateSubscription: the keyspace is added again after the larger of after_ms and the client backoff, for as long as the server answers Retry, and the queued action is not ended", async () => {
    const r = await rig({ serverOptions: { limits: { maxOfflineSeconds: 3600 } } });
    r.random.fixed = 0.5;
    r.server.createKeyspaceNow("dst1");
    r.server.setSeeding("dst1", true);
    await r.engine.addKeyspace("dst1");
    await r.engine.settle();
    const adds = (): number => r.server.calls.filter((c) => c.method === "updateSubscription").length;
    expect(r.engine.status().holds).toEqual([]); // a Retry is no hold of Table 14.1
    expect(r.events.filter((e) => e.type === "held")).toHaveLength(0);
    r.server.setSeeding("dst1", false); // a mutation to it is accepted; the add is what is refused: seeding is per keyspace, so set it again
    r.server.setSeeding("dst1", true);
    const a = await r.engine.submit({ keyspaceId: "dst1", opType: "Set", changes: [set("settings/a", 1)] });
    await tick(r, 2 * 3600 * 1000); // twice the age bound that the default hold would have had
    expect(adds()).toBeGreaterThan(5);
    expect(adds()).toBeLessThan(60); // the client backoff doubles to its cap of five minutes
    expect(r.engine.status().holds).toEqual([]);
    expect(r.events.filter((e) => e.type === "held")).toHaveLength(0);
    expect(types(r, a.mutationId)).toEqual([]); // still pending: it was not sent, and it was not ended at a bound
    r.server.setSeeding("dst1", false);
    await tick(r, 10 * 60 * 1000);
    expect(kinds(r, a.mutationId)).toEqual(["committed"]);
    expect(r.server.records("dst1").some((x) => x.mutationId === a.mutationId)).toBe(true);
  });

  it("Subscribe: a successor that is not seeded refuses the whole call; the client sends the whole call again with the client backoff, with no hold", async () => {
    const r = await rig({ keyspaces: [KS, "dst1"] });
    r.random.fixed = 0.5;
    r.server.setSeeding("dst1", true);
    r.server.disconnect();
    await r.engine.settle();
    const subs = (): number => r.server.calls.filter((c) => c.method === "subscribe").length;
    const before = subs();
    await tick(r, 3 * 60 * 1000);
    expect(subs()).toBeGreaterThan(before + 1);
    expect(r.engine.status().holds).toEqual([]);
    expect(r.engine.status().connection).not.toBe("live");
    r.server.setSeeding("dst1", false);
    await tick(r, 6 * 60 * 1000);
    expect(r.engine.status().connection).toBe("live");
  });
});

// ---------------------------------------------------------------------------------------------------------------------
// Reading 9: [CLI-45]: an action that a hold ends before it was sent is dead-lettered with the hold's code

describe("14 [CLI-45]: dead-lettered with the hold's code", () => {
  it("a hold that a reject of an unknown class started: the reject that the action carries is that reject, and the hold's code is its reason and details", async () => {
    const r = await rig({ serverOptions: { limits: { maxOfflineSeconds: 3600 } } });
    r.random.fixed = 0.5;
    r.server.inject("mutate", { kind: "result", result: (m) => (m.changes[0]?.key === "settings/a" ? rejectResult(m, rejectWith(901, 77)) : undefined) }, { times: 1000 });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] }); // never sent: the lane is held
    await tick(r, 3700 * 1000);
    expect(kinds(r, a.mutationId)).toEqual(["outcome-unknown", "exported"]);
    expect(kinds(r, b.mutationId)).toEqual(["rejected", "dead-lettered"]);
    const dl = r.engine.deadLetterEntries().find((d) => d.mutationId === b.mutationId);
    expect(dl?.reason).toBe("CLASS_UNKNOWN");
    expect(dl?.reject).toMatchObject({ code: 901, errorClass: 77, details: { hold: "CLASS_UNKNOWN" } });
  });

  it("a hold with a code of its own row keeps that code, and the hold's code is named in details", async () => {
    const r = await rig({ serverOptions: { limits: { maxOfflineSeconds: 3600 } } });
    r.server.inject("mutate", { kind: "result", result: (m) => (m.changes[0]?.key === "settings/a" ? rejectResult(m, mk(Code.UPGRADE_REQUIRED)) : undefined) }, { times: 1000 });
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
    await tick(r, 3700 * 1000);
    const dl = r.engine.deadLetterEntries().find((d) => d.mutationId === b.mutationId);
    expect(dl?.reason).toBe("UPGRADE_REQUIRED");
    expect(dl?.reject).toMatchObject({ code: Code.UPGRADE_REQUIRED, details: { hold: "UPGRADE_REQUIRED" } });
  });
});

// ---------------------------------------------------------------------------------------------------------------------
// Reading 10: [CLI-53]: the journal window runs from the outcome

describe("14 [CLI-53]: the journal keeps an action for max_offline after the outcome, not after the retirement", () => {
  it("an action whose outcome waited for the cursor leaves the journal max_offline after the outcome", async () => {
    const r = await rig({ serverOptions: { limits: { maxOfflineSeconds: 3600 } } });
    r.server.faults.paused = true; // the record does not come: the action has its outcome and does not retire
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(r.engine.outbox()[0]?.outcome).toBeDefined();
    await tick(r, 3000 * 1000);
    r.server.resumeDelivery();
    await r.engine.settle();
    expect(kinds(r, a.mutationId)).toEqual(["committed"]);
    const j = r.engine.journalEntries().find((x) => x.mutationId === a.mutationId);
    expect(j !== undefined && j.retiredServerMs - j.outcomeServerMs >= 2_990_000).toBe(true);
    await tick(r, 700 * 1000); // 3,700 s after the outcome, 700 s after the retirement
    expect(r.engine.journalEntries().find((x) => x.mutationId === a.mutationId)).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------------------------------------------------
// The readings that have two readings: the choice is pinned

describe("SPEC-QUESTION readings, as taken", () => {
  it("14 [CLI-63] with [CLI-33]: a later unrelated action takes slot 1 while an earlier one waits for its dependency, which takes the next slot when it is sent", async () => {
    const r = await rig();
    const d = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] }); // pending, undecided
    await r.engine.settle();
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/g", 2)] }); // depends on d: the same key
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] }); // unrelated
    await r.engine.settle();
    const seqOf = (id: string): bigint[] => mutateRequests(r).flatMap((q) => q.mutations).filter((m) => m.mutationId === id).map((m) => m.clientSeq);
    expect(seqOf(b.mutationId)).toEqual([1n]);
    expect(seqOf(a.mutationId)).toEqual([]);
    r.server.decide(KS, d.mutationId, { status: "COMMITTED" });
    await r.engine.settle();
    expect(seqOf(a.mutationId)).toEqual([2n]);
    expect(kinds(r, a.mutationId).filter((t) => t !== "overwritten")).toEqual(["committed"]);
  });

  it("Q42.2 (ruled) 15 [COMPAT-39] with 14 [CLI-7], [CLI-34]: a REJECTED decision whose reject has a class that the client does not know is final, handled as DEAD_LETTER: no hold, the dependents fail, the class is reported", async () => {
    for (const cls of [77, ErrorClass.UNSPECIFIED]) {
      const r = await rig();
      const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] });
      await r.engine.settle();
      const dep = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [del("settings/g")] }); // depends on g, never sent
      await r.engine.settle();
      r.server.decide(KS, g.mutationId, { status: "REJECTED", reject: rejectWith(901, cls) });
      await r.engine.settle();
      expect(r.events.filter((e) => e.type === "held"), `class ${cls}: a hold that no resend can clear`).toEqual([]);
      expect(kinds(r, g.mutationId)).toEqual(["rejected", "dead-lettered"]);
      expect(kinds(r, dep.mutationId)).toEqual(["rejected", "dead-lettered"]); // 14 [CLI-34]
      const rejected = r.events.find((e) => e.type === "rejected" && (e as { mutationId: string }).mutationId === g.mutationId) as { reject?: { errorClass: number; code: number } };
      expect(rejected.reject).toMatchObject({ errorClass: cls, code: 901 }); // the app is told the class that the client did not know
      expect(r.engine.status().holds).toEqual([]);
      expect(r.engine.outbox()).toEqual([]);
      expect(r.engine.deadLetterEntries().map((d) => d.mutationId).sort()).toEqual([g.mutationId, dep.mutationId].sort());
    }
  });

  it("Q42.2 (ruled): the hold is for a `Reject` result only: a `Reject` of an unknown class that is no decision still holds the lane", async () => {
    const r = await rig();
    r.random.fixed = 0.999999;
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, rejectWith(901, 77)) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(r.events.filter((e) => e.type === "held").map((e) => (e as { code: string }).code)).toEqual(["CLASS_UNKNOWN"]);
    expect(kinds(r, a.mutationId)).toEqual([]);
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "lane", code: "CLASS_UNKNOWN" })]);
  });
});
