// The scriptable fake server of 17 [CONF-4]: what the client suites depend on. It drops, reorders, duplicates and delays frames,
// disconnects, and injects every code with every class and every RPC status; and it answers as a server does for lanes (07 §7.4),
// idempotency (07 §7.6), results (07 [MUT-53]) and a restore (07 [MUT-104], [MUT-105]).

import { describe, expect, it } from "vitest";
import { create } from "@bufbuild/protobuf";
import { RpcError, StaticManifests, jsonBytes, type RpcStatus } from "../src/engine/index.js";
import { encodeContent } from "../src/engine/entry.js";
import { MutateRequestSchema, MutationSchema, type Mutation, type MutateResponse } from "../src/gen/abpc/v2/sync_pb.js";
import { ChangeKind, Code, ErrorClass, KeyChangeSchema, Lane, RetryReason, StaleCause } from "../src/gen/abpc/v2/types_pb.js";
import { DEFAULT_CLASS } from "../src/engine/codes.js";
import { FakeServer, ManualClock, reject as mk, rejectWith } from "../src/fake/index.js";
import { KS, MANIFEST, rejectResult } from "./helpers/engine.js";

function setup(): { server: FakeServer; clock: ManualClock; t: ReturnType<FakeServer["transport"]>; ana: ReturnType<FakeServer["principal"]> } {
  const clock = new ManualClock();
  const server = new FakeServer({ manifests: new StaticManifests(MANIFEST), clock });
  server.createKeyspaceNow(KS);
  const ana = server.principal("ana");
  return { server, clock, t: server.transport(ana), ana };
}

let counter = 0;
function mut(over: { id?: string; client?: string; seq?: number; key?: string; value?: unknown; lane?: Lane; op?: string; after?: number; baseEpoch?: number } = {}): Mutation {
  const n = ++counter;
  return create(MutationSchema, {
    mutationId: over.id ?? `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`,
    keyspaceId: KS,
    opType: over.op ?? (over.lane === Lane.GATED ? "Approve" : "Set"),
    lane: over.lane ?? Lane.SEQUENCED,
    clientId: over.client ?? "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    clientSeq: BigInt(over.seq ?? 1),
    afterClientSeq: BigInt(over.after ?? 0),
    baseLogEpoch: over.baseEpoch ?? 0,
    changes: [create(KeyChangeSchema, { key: over.key ?? "settings/a", kind: ChangeKind.SET, value: jsonBytes(over.value ?? 1), encoding: "json/v1" })],
  });
}

const send = (t: ReturnType<typeof setup>["t"], ...ms: Mutation[]): Promise<MutateResponse> => t.mutate(create(MutateRequestSchema, { mutations: ms }));
const out = (r: MutateResponse, i = 0): string => r.results[i]?.outcome.case ?? "none";
const rejectCode = (r: MutateResponse, i = 0): number => (r.results[i]?.outcome.case === "rejected" ? (r.results[i]?.outcome.value.code ?? -1) : -1);

describe("the fake server: lanes and idempotency (07 §7.4, §7.6)", () => {
  it("processes the SEQUENCED lane in client_seq order, answers OUT_OF_ORDER past a gap and NOT_PROCESSED after a stop, and returns the lane watermark", async () => {
    const { t } = setup();
    const a = mut({ seq: 1 });
    const gap = mut({ seq: 3 });
    const after = mut({ seq: 4 });
    const r = await send(t, a, gap, after);
    expect(out(r, 0)).toBe("appended");
    expect(r.results[1]?.outcome.value).toMatchObject({ reason: RetryReason.OUT_OF_ORDER });
    expect(r.results[2]?.outcome.value).toMatchObject({ reason: RetryReason.NOT_PROCESSED });
    expect(r.watermarks).toHaveLength(1);
    expect(r.watermarks[0]).toMatchObject({ lane: Lane.SEQUENCED, processedClientSeq: 1n, logEpoch: 1 });
    // a request in the wrong order inside is sorted by client_seq (07 [MUT-56])
    const two = mut({ seq: 2 });
    const r2 = await send(t, gap, two);
    expect(r2.results.map((x) => x.outcome.case)).toEqual(["appended", "appended"]);
  });

  it("CLIENT_UNKNOWN for a client_seq above 1 with no lane state; CLIENT_FORKED when a slot is bound to another id", async () => {
    const { t } = setup();
    expect(rejectCode(await send(t, mut({ seq: 5, client: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb" })))).toBe(Code.CLIENT_UNKNOWN);
    await send(t, mut({ seq: 1 }));
    expect(rejectCode(await send(t, mut({ seq: 1 })))).toBe(Code.CLIENT_FORKED);
  });

  it("a resend of the same id and content is a duplicate with the stored outcome; other content under the id is IDEMPOTENCY_KEY_REUSED; another subject gets ID_CONFLICT", async () => {
    const { t, server } = setup();
    const m = mut({ seq: 1 });
    const first = await send(t, m);
    const again = await send(t, m);
    expect(again.results[0]?.duplicate).toBe(true);
    expect(again.results[0]?.outcome.value).toMatchObject({ seq: (first.results[0]?.outcome.value as { seq: bigint }).seq });
    expect(server.records(KS).filter((r) => r.mutationId === m.mutationId)).toHaveLength(1);
    // only the stamping fields may change on a resend
    const restamped = create(MutationSchema, { ...m, clientId: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", clientSeq: 1n });
    expect((await send(t, restamped)).results[0]?.duplicate).toBe(true);
    const changed = create(MutationSchema, { ...m, opType: "Create" });
    expect(rejectCode(await send(t, changed))).toBe(Code.IDEMPOTENCY_KEY_REUSED);
    const bob = server.transport(server.principal("bob"));
    expect(rejectCode(await send(bob, create(MutationSchema, { ...m, clientId: "dddddddd-dddd-4ddd-8ddd-dddddddddddd" })))).toBe(Code.ID_CONFLICT);
  });

  it("a recorded reject consumes its slot; an unrecorded one does not; a GATED slot is consumed by its decision", async () => {
    const { t, server } = setup();
    const bad = mut({ seq: 1, key: "nowhere/x" }); // INVALID_KEY: recorded
    const r = await send(t, bad);
    expect(rejectCode(r)).toBe(Code.INVALID_KEY);
    expect(r.watermarks[0]?.processedClientSeq).toBe(1n);
    const ok = mut({ seq: 2 });
    expect(out(await send(t, ok))).toBe("appended");
    const unknownOp = mut({ seq: 3, op: "Nope" }); // UNKNOWN_OP_TYPE: HOLD, unrecorded
    const r3 = await send(t, unknownOp);
    expect(rejectCode(r3)).toBe(Code.UNKNOWN_OP_TYPE);
    // the slot was not consumed: the same slot is free for another action, which is appended
    expect(out(await send(t, mut({ seq: 3 })))).toBe("appended");
    const g = mut({ seq: 1, lane: Lane.GATED, key: "settings/g" });
    const rg = await send(t, g);
    expect(rg.results[0]?.outcome.case).toBe("retry");
    expect(rg.results[0]?.intentSeq).toBeGreaterThan(0n);
    expect(rg.watermarks.find((w) => w.lane === Lane.GATED)?.processedClientSeq).toBe(0n);
    server.decide(KS, g.mutationId, { status: "COMMITTED" });
    const again = await send(t, g);
    expect(again.results[0]?.outcome.case).toBe("decided");
    expect(again.watermarks.find((w) => w.lane === Lane.GATED)?.processedClientSeq).toBe(1n);
  });

  it("checks preconditions, CAS, entity tombstones and the base epoch", async () => {
    const { t, server } = setup();
    await send(t, mut({ seq: 1, key: "settings/a", value: 1 }));
    const pre = mut({ seq: 2, key: "settings/b" });
    pre.preconditions = [{ $typeName: "abpc.v2.Precondition", target: { case: "key", value: "settings/a" }, check: { case: "unchangedSince", value: 0n }, logEpoch: 1 }];
    const r = await send(t, pre);
    expect(r.results[0]?.outcome.value).toMatchObject({ code: Code.STALE, stale: { cause: StaleCause.VERSION_MISMATCH } });
    const stale = mut({ seq: 3, baseEpoch: 9 });
    expect(r.results.length).toBe(1);
    expect((await send(t, stale)).results[0]?.outcome.value).toMatchObject({ code: Code.STALE, stale: { cause: StaleCause.EPOCH } });
    server.write(KS, [create(KeyChangeSchema, { key: "issues/i1", kind: ChangeKind.SET, value: jsonBytes({}), encoding: "json/v1" })]);
    server.write(KS, [create(KeyChangeSchema, { key: "issues/i1", kind: ChangeKind.DELETE })]);
    const child = mut({ seq: 4, key: "issues/i1/title", op: "Set" });
    expect((await send(t, child)).results[0]?.outcome.value).toMatchObject({ code: Code.STALE, stale: { cause: StaleCause.PARENT_DELETED } });
    expect((await send(t, mut({ seq: 5, key: "issues/i1", op: "Create" }))).results[0]?.outcome.value).toMatchObject({ code: Code.TOMBSTONED });
  });
});

describe("the fake server: results and a restore (07 [MUT-53], [MUT-104], [MUT-105])", () => {
  it("GetMutationResult: known for the bound subject; unknown for anyone else and for an id never seen; voided after a restore", async () => {
    const { t, server } = setup();
    const m = mut({ seq: 1 });
    await send(t, m);
    const known = await t.getMutationResult({ $typeName: "abpc.v2.GetMutationResultRequest", keyspaceId: KS, mutationId: m.mutationId });
    expect(known.known).toBe(true);
    expect(known.result?.duplicate).toBe(true);
    const bob = server.transport(server.principal("bob"));
    expect((await bob.getMutationResult({ $typeName: "abpc.v2.GetMutationResultRequest", keyspaceId: KS, mutationId: m.mutationId })).known).toBe(false);
    expect((await t.getMutationResult({ $typeName: "abpc.v2.GetMutationResultRequest", keyspaceId: KS, mutationId: "nope" })).known).toBe(false);
    server.restore(KS, 0n);
    const voided = await t.getMutationResult({ $typeName: "abpc.v2.GetMutationResultRequest", keyspaceId: KS, mutationId: m.mutationId });
    expect([voided.known, voided.voided]).toEqual([false, true]);
    // a resend of a voided id is recorded as STALE{EPOCH}, never applied (07 [MUT-105])
    const resend = await send(t, create(MutationSchema, { ...m, clientId: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee", baseLogEpoch: 0 }));
    expect(resend.results[0]?.outcome.value).toMatchObject({ code: Code.STALE, stale: { cause: StaleCause.EPOCH } });
    expect(server.records(KS).filter((r) => r.mutationId === m.mutationId)).toHaveLength(0);
    expect(server.epoch(KS)).toBe(2);
  });

  it("a frozen keyspace answers HISTORY_DIVERGED; LoadState pages are consistent at one checkpoint and a resume check says resumed", async () => {
    const { t, server } = setup();
    for (let i = 0; i < 5; i++) server.write(KS, [create(KeyChangeSchema, { key: `settings/k${i}`, kind: ChangeKind.SET, value: jsonBytes(i), encoding: "json/v1" })]);
    const base = { $typeName: "abpc.v2.LoadStateRequest" as const, clientId: "", pageToken: "", pageSize: 2, supportedFeatures: [] };
    const p1 = await t.loadState({ ...base, cursor: { $typeName: "abpc.v2.KeyspaceCursor", keyspaceId: KS, afterSeq: 0n, logEpoch: 0, interestPrefixes: [], afterRecordHash: new Uint8Array() } });
    expect(p1.entries).toHaveLength(2);
    expect(p1.nextPageToken).not.toBe("");
    server.write(KS, [create(KeyChangeSchema, { key: "settings/late", kind: ChangeKind.SET, value: jsonBytes(1), encoding: "json/v1" })]);
    const p2 = await t.loadState({ ...base, pageToken: p1.nextPageToken, cursor: { $typeName: "abpc.v2.KeyspaceCursor", keyspaceId: KS, afterSeq: 0n, logEpoch: 0, interestPrefixes: [], afterRecordHash: new Uint8Array() } });
    expect(p2.checkpointSeq).toBe(p1.checkpointSeq); // the late write is not in this load
    const resume = await t.loadState({ ...base, cursor: { $typeName: "abpc.v2.KeyspaceCursor", keyspaceId: KS, afterSeq: 3n, logEpoch: 1, interestPrefixes: [], afterRecordHash: new Uint8Array() } });
    expect(resume.resumed).toBe(true);
    server.freeze(KS);
    await expect(t.loadState({ ...base, cursor: { $typeName: "abpc.v2.KeyspaceCursor", keyspaceId: KS, afterSeq: 0n, logEpoch: 0, interestPrefixes: [], afterRecordHash: new Uint8Array() } })).rejects.toMatchObject({ status: "FailedPrecondition" });
  });
});

describe("17 [CONF-4] the fake injects every code with every class, and every RPC status", () => {
  it("injects any code of A.3 with its class, and a code and a class that no client knows", async () => {
    const { t, server } = setup();
    const codes: [number, number][] = [...DEFAULT_CLASS.entries(), [901, ErrorClass.CONFLICT], [902, 77]];
    for (const [code, cls] of codes) {
      server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, rejectWith(code, cls)) });
      const m = mut({ seq: 1, client: `ffffffff-ffff-4fff-8fff-${String(code).padStart(12, "0")}` });
      const r = await send(t, m);
      expect(r.results[0]?.outcome.case).toBe("rejected");
      expect(r.results[0]?.outcome.value).toMatchObject({ code, errorClass: cls });
    }
  });

  it.each(["Cancelled", "Unknown", "InvalidArgument", "DeadlineExceeded", "NotFound", "AlreadyExists", "PermissionDenied", "ResourceExhausted", "FailedPrecondition", "Aborted", "OutOfRange", "Unimplemented", "Internal", "Unavailable", "DataLoss", "Unauthenticated"] as RpcStatus[])("fails a call of each method with the status %s, with an ABPC detail or without", async (status) => {
    const { t, server } = setup();
    const methods = ["mutate", "getMutationResult", "getMutationResults", "cancelIntent", "createKeyspace", "loadState", "updateSubscription", "refreshSession", "getManifest"] as const;
    for (const method of methods) {
      server.inject(method, { kind: "status", status, reject: mk(Code.APP_SUSPENDED) });
      const call = (): Promise<unknown> => {
        switch (method) {
          case "mutate":
            return t.mutate(create(MutateRequestSchema, { mutations: [mut()] }));
          case "getMutationResult":
            return t.getMutationResult({ $typeName: "abpc.v2.GetMutationResultRequest", keyspaceId: KS, mutationId: "x" });
          case "getMutationResults":
            return t.getMutationResults({ $typeName: "abpc.v2.GetMutationResultsRequest", keyspaceId: KS, mutationIds: ["x"] });
          case "cancelIntent":
            return t.cancelIntent({ $typeName: "abpc.v2.CancelIntentRequest", keyspaceId: KS, intentId: "x" });
          case "createKeyspace":
            return t.createKeyspace({ $typeName: "abpc.v2.CreateKeyspaceRequest", keyspaceId: "k2", kind: "kind" });
          case "loadState":
            return t.loadState({ $typeName: "abpc.v2.LoadStateRequest", clientId: "", pageToken: "", pageSize: 0, supportedFeatures: [], cursor: { $typeName: "abpc.v2.KeyspaceCursor", keyspaceId: KS, afterSeq: 0n, logEpoch: 0, interestPrefixes: [], afterRecordHash: new Uint8Array() } });
          case "updateSubscription":
            return t.updateSubscription({ $typeName: "abpc.v2.UpdateSubscriptionRequest", sessionId: "s", add: [], remove: [], setInterest: [], clientId: "" });
          case "refreshSession":
            return t.refreshSession({ $typeName: "abpc.v2.RefreshSessionRequest", sessionId: "s" });
          case "getManifest":
            return t.getManifest({ $typeName: "abpc.v2.GetManifestRequest", version: 0 });
        }
      };
      await expect(call()).rejects.toBeInstanceOf(RpcError);
      server.inject(method, { kind: "status", status });
      const err = (await call().catch((e: unknown) => e)) as RpcError;
      expect(err.status).toBe(status);
      expect(err.reject).toBeUndefined();
    }
    const it2 = t.subscribe({ $typeName: "abpc.v2.SubscribeRequest", keyspaces: [], supportedFeatures: [], clientId: "" })[Symbol.asyncIterator]();
    server.inject("subscribe", { kind: "status", status });
    await expect(it2.next()).rejects.toBeInstanceOf(RpcError);
  });
});

describe("17 [CONF-4] the fake drops, reorders, duplicates and delays frames, disconnects, and loses responses", () => {
  async function open(server: FakeServer, t: ReturnType<FakeServer["transport"]>): Promise<{ next: () => Promise<{ kind: string; first?: bigint; prev?: bigint; cursor?: bigint }>; frames: () => { kind: string; prev?: bigint; cursor?: bigint }[] }> {
    const seen: { kind: string; prev?: bigint; cursor?: bigint }[] = [];
    const it = t.subscribe({ $typeName: "abpc.v2.SubscribeRequest", keyspaces: [{ $typeName: "abpc.v2.KeyspaceCursor", keyspaceId: KS, afterSeq: 0n, logEpoch: 1, interestPrefixes: [], afterRecordHash: new Uint8Array() }], supportedFeatures: [], clientId: "" })[Symbol.asyncIterator]();
    const next = async (): Promise<{ kind: string; prev?: bigint; cursor?: bigint }> => {
      const r = await it.next();
      if (r.done === true) return { kind: "end" };
      const f = r.value.frame;
      const d = f.case === "records" ? { kind: "records", prev: f.value.prevCursor, cursor: f.value.cursor } : { kind: f.case ?? "none" };
      seen.push(d);
      return d;
    };
    await next(); // Subscribed
    await next(); // KeyspaceAdded
    void server;
    return { next, frames: () => seen };
  }

  const w = (server: FakeServer): bigint => server.write(KS, [create(KeyChangeSchema, { key: "settings/a", kind: ChangeKind.SET, value: jsonBytes(1), encoding: "json/v1" })]);

  it("drops a frame (the next one does not continue the cursor), duplicates one, reorders two, and holds frames while paused", async () => {
    const { server, t } = setup();
    const s = await open(server, t);
    server.faults.dropNext = 1;
    w(server);
    w(server);
    expect(await s.next()).toMatchObject({ kind: "records", prev: 1n, cursor: 2n }); // the first frame was dropped
    server.faults.duplicateNext = 1;
    w(server);
    expect(await s.next()).toMatchObject({ prev: 2n, cursor: 3n });
    expect(await s.next()).toMatchObject({ prev: 2n, cursor: 3n });
    server.faults.reorderNext = true;
    w(server);
    w(server);
    expect(await s.next()).toMatchObject({ prev: 4n, cursor: 5n });
    expect(await s.next()).toMatchObject({ prev: 3n, cursor: 4n });
    server.faults.paused = true;
    w(server);
    server.resumeDelivery();
    expect(await s.next()).toMatchObject({ prev: 5n, cursor: 6n });
  });

  it("delays a frame on the server's clock, and a disconnect ends the stream, losing what was queued", async () => {
    const { server, t, clock } = setup();
    const s = await open(server, t);
    server.faults.delayMs = 3000;
    w(server);
    let got = false;
    const p = s.next().then((x) => ((got = true), x));
    await clock.advance(2000);
    expect(got).toBe(false);
    await clock.advance(1500);
    expect(await p).toMatchObject({ kind: "records" });
    server.faults.delayMs = 0;
    w(server);
    server.disconnect();
    expect((await s.next()).kind).toBe("end");
  });

  it("a response that is lost after the effect was applied: the call fails, the action stands", async () => {
    const { server, t } = setup();
    server.inject("mutate", { kind: "lose" });
    const m = mut({ seq: 1 });
    await expect(send(t, m)).rejects.toMatchObject({ status: "Unavailable" });
    expect(server.records(KS).filter((r) => r.mutationId === m.mutationId)).toHaveLength(1);
    expect((await send(t, m)).results[0]?.duplicate).toBe(true);
  });

  it("the content hash of a mutation ignores the stamping fields, as the idempotency check needs", () => {
    const a = mut({ seq: 1, client: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" });
    const b = create(MutationSchema, { ...a, clientId: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", clientSeq: 9n, afterClientSeq: 4n });
    expect(Buffer.from(encodeContent(a)).equals(Buffer.from(encodeContent(b)))).toBe(true);
  });
});
