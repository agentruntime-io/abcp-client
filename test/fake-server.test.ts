// The scriptable fake server of 17 [CONF-4]: what the client suites depend on. It drops, reorders, duplicates and delays frames,
// disconnects, and injects every code with every class and every RPC status; and it answers as a server does for lanes (07 §7.4),
// idempotency (07 §7.6), results (07 [MUT-53]) and a restore (07 [MUT-104], [MUT-105]).

import { describe, expect, it } from "vitest";
import { create } from "@bufbuild/protobuf";
import { RpcError, StaticManifests, jsonBytes, type RpcStatus } from "../src/engine/index.js";
import { encodeContent } from "../src/engine/entry.js";
import { MutateRequestSchema, MutationSchema, type Mutation, type MutateResponse, type MutationResult } from "../src/gen/abcp/v2/sync_pb.js";
import { ChangeKind, Code, ErrorClass, KeyChangeSchema, Lane, RetryReason, StaleCause, SuccessorRuleSchema, type SuccessorRule } from "../src/gen/abcp/v2/types_pb.js";
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
    pre.preconditions = [{ $typeName: "abcp.v2.Precondition", target: { case: "key", value: "settings/a" }, check: { case: "unchangedSince", value: 0n }, logEpoch: 1 }];
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
    const known = await t.getMutationResult({ $typeName: "abcp.v2.GetMutationResultRequest", keyspaceId: KS, mutationId: m.mutationId });
    expect(known.known).toBe(true);
    expect(known.result?.duplicate).toBe(true);
    const bob = server.transport(server.principal("bob"));
    expect((await bob.getMutationResult({ $typeName: "abcp.v2.GetMutationResultRequest", keyspaceId: KS, mutationId: m.mutationId })).known).toBe(false);
    expect((await t.getMutationResult({ $typeName: "abcp.v2.GetMutationResultRequest", keyspaceId: KS, mutationId: "nope" })).known).toBe(false);
    server.restore(KS, 0n);
    const voided = await t.getMutationResult({ $typeName: "abcp.v2.GetMutationResultRequest", keyspaceId: KS, mutationId: m.mutationId });
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
    const base = { $typeName: "abcp.v2.LoadStateRequest" as const, clientId: "", pageToken: "", pageSize: 2, supportedFeatures: [] };
    const p1 = await t.loadState({ ...base, cursor: { $typeName: "abcp.v2.KeyspaceCursor", keyspaceId: KS, afterSeq: 0n, logEpoch: 0, interestPrefixes: [], afterRecordHash: new Uint8Array() } });
    expect(p1.entries).toHaveLength(2);
    expect(p1.nextPageToken).not.toBe("");
    server.write(KS, [create(KeyChangeSchema, { key: "settings/late", kind: ChangeKind.SET, value: jsonBytes(1), encoding: "json/v1" })]);
    const p2 = await t.loadState({ ...base, pageToken: p1.nextPageToken, cursor: { $typeName: "abcp.v2.KeyspaceCursor", keyspaceId: KS, afterSeq: 0n, logEpoch: 0, interestPrefixes: [], afterRecordHash: new Uint8Array() } });
    expect(p2.checkpointSeq).toBe(p1.checkpointSeq); // the late write is not in this load
    const resume = await t.loadState({ ...base, cursor: { $typeName: "abcp.v2.KeyspaceCursor", keyspaceId: KS, afterSeq: 3n, logEpoch: 1, interestPrefixes: [], afterRecordHash: new Uint8Array() } });
    expect(resume.resumed).toBe(true);
    server.freeze(KS);
    await expect(t.loadState({ ...base, cursor: { $typeName: "abcp.v2.KeyspaceCursor", keyspaceId: KS, afterSeq: 0n, logEpoch: 0, interestPrefixes: [], afterRecordHash: new Uint8Array() } })).rejects.toMatchObject({ status: "FailedPrecondition" });
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

  it.each(["Cancelled", "Unknown", "InvalidArgument", "DeadlineExceeded", "NotFound", "AlreadyExists", "PermissionDenied", "ResourceExhausted", "FailedPrecondition", "Aborted", "OutOfRange", "Unimplemented", "Internal", "Unavailable", "DataLoss", "Unauthenticated"] as RpcStatus[])("fails a call of each method with the status %s, with an ABCP detail or without", async (status) => {
    const { t, server } = setup();
    const methods = ["mutate", "getMutationResult", "getMutationResults", "cancelIntent", "createKeyspace", "loadState", "updateSubscription", "refreshSession", "getManifest"] as const;
    for (const method of methods) {
      server.inject(method, { kind: "status", status, reject: mk(Code.APP_SUSPENDED) });
      const call = (): Promise<unknown> => {
        switch (method) {
          case "mutate":
            return t.mutate(create(MutateRequestSchema, { mutations: [mut()] }));
          case "getMutationResult":
            return t.getMutationResult({ $typeName: "abcp.v2.GetMutationResultRequest", keyspaceId: KS, mutationId: "x" });
          case "getMutationResults":
            return t.getMutationResults({ $typeName: "abcp.v2.GetMutationResultsRequest", keyspaceId: KS, mutationIds: ["x"] });
          case "cancelIntent":
            return t.cancelIntent({ $typeName: "abcp.v2.CancelIntentRequest", keyspaceId: KS, intentId: "x" });
          case "createKeyspace":
            return t.createKeyspace({ $typeName: "abcp.v2.CreateKeyspaceRequest", keyspaceId: "k2", kind: "kind" });
          case "loadState":
            return t.loadState({ $typeName: "abcp.v2.LoadStateRequest", clientId: "", pageToken: "", pageSize: 0, supportedFeatures: [], cursor: { $typeName: "abcp.v2.KeyspaceCursor", keyspaceId: KS, afterSeq: 0n, logEpoch: 0, interestPrefixes: [], afterRecordHash: new Uint8Array() } });
          case "updateSubscription":
            return t.updateSubscription({ $typeName: "abcp.v2.UpdateSubscriptionRequest", sessionId: "s", add: [], remove: [], setInterest: [], clientId: "" });
          case "refreshSession":
            return t.refreshSession({ $typeName: "abcp.v2.RefreshSessionRequest", sessionId: "s" });
          case "getManifest":
            return t.getManifest({ $typeName: "abcp.v2.GetManifestRequest", version: 0 });
        }
      };
      await expect(call()).rejects.toBeInstanceOf(RpcError);
      server.inject(method, { kind: "status", status });
      const err = (await call().catch((e: unknown) => e)) as RpcError;
      expect(err.status).toBe(status);
      expect(err.reject).toBeUndefined();
    }
    const it2 = t.subscribe({ $typeName: "abcp.v2.SubscribeRequest", keyspaces: [], supportedFeatures: [], clientId: "" })[Symbol.asyncIterator]();
    server.inject("subscribe", { kind: "status", status });
    await expect(it2.next()).rejects.toBeInstanceOf(RpcError);
  });
});

describe("17 [CONF-4] the fake drops, reorders, duplicates and delays frames, disconnects, and loses responses", () => {
  async function open(server: FakeServer, t: ReturnType<FakeServer["transport"]>): Promise<{ next: () => Promise<{ kind: string; first?: bigint; prev?: bigint; cursor?: bigint }>; frames: () => { kind: string; prev?: bigint; cursor?: bigint }[] }> {
    const seen: { kind: string; prev?: bigint; cursor?: bigint }[] = [];
    const it = t.subscribe({ $typeName: "abcp.v2.SubscribeRequest", keyspaces: [{ $typeName: "abcp.v2.KeyspaceCursor", keyspaceId: KS, afterSeq: 0n, logEpoch: 1, interestPrefixes: [], afterRecordHash: new Uint8Array() }], supportedFeatures: [], clientId: "" })[Symbol.asyncIterator]();
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

// ---------------------------------------------------------------------------------------------------------------------
// The fake as the Go server is: the order of 07 Table 7-1 (server/internal/mutate/one.go), the lane checks that a duplicate meets
// (server/internal/store/lanes.go), the intents of server/internal/store/gated.go, and the lifecycle hold of 11 [COORD-96].

function setupWith(manifests: StaticManifests, limits: Record<string, number> = {}): ReturnType<typeof setup> & { manifests: StaticManifests } {
  const clock = new ManualClock();
  const server = new FakeServer({ manifests, clock, limits });
  server.createKeyspaceNow(KS);
  const ana = server.principal("ana");
  return { server, clock, t: server.transport(ana), ana, manifests };
}

const C1 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const C2 = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const getResult = (t: ReturnType<typeof setup>["t"], id: string): ReturnType<ReturnType<typeof setup>["t"]["getMutationResult"]> => t.getMutationResult({ $typeName: "abcp.v2.GetMutationResultRequest", keyspaceId: KS, mutationId: id });
const retryReason = (r: MutationResult | undefined): number => (r?.outcome.case === "retry" ? r.outcome.value.reason : -1);

describe("the fake server: the duplicate lookup comes first (07 Table 7-1 step 6; mutate/one.go)", () => {
  it("a duplicate gets its stored outcome, whatever the lifecycle, the op types and the lanes of the manifest in force have become; a new mutation gets KEYSPACE_ARCHIVED, UNKNOWN_OP_TYPE or INVALID_ENVELOPE", async () => {
    const manifests = new StaticManifests(MANIFEST, { version: 2, templates: MANIFEST.templates, opTypes: { Create: {}, Approve: { lane: "GATED" } } });
    const { t, server } = setupWith(manifests);
    const a = mut({ seq: 1 });
    expect(out(await send(t, a))).toBe("appended");
    const wrong = mut({ seq: 2, op: "Approve" }); // a GATED op type on the SEQUENCED lane: a recorded INVALID_ENVELOPE
    expect(rejectCode(await send(t, wrong))).toBe(Code.INVALID_ENVELOPE);
    const dupWrong = await send(t, wrong);
    expect([dupWrong.results[0]?.duplicate, rejectCode(dupWrong)]).toEqual([true, Code.INVALID_ENVELOPE]);

    // version 2 declares no `Set`: a new `Set` is UNKNOWN_OP_TYPE, a resend of the one that committed is its stored outcome
    server.activateManifest(KS, 2);
    const fresh = mut({ seq: 3 });
    expect(rejectCode(await send(t, fresh))).toBe(Code.UNKNOWN_OP_TYPE);
    const dupA = await send(t, a);
    expect([out(dupA), dupA.results[0]?.duplicate]).toEqual(["appended", true]);
    // an archived keyspace: the same
    server.archive(KS, []);
    const dupA2 = await send(t, a);
    expect([out(dupA2), dupA2.results[0]?.duplicate]).toEqual(["appended", true]);
    const created = mut({ seq: 3, op: "Create", key: "issues/i1" });
    const archived = await send(t, created);
    expect(rejectCode(archived)).toBe(Code.KEYSPACE_ARCHIVED);
    expect(archived.results[0]?.outcome.value).toMatchObject({ details: { lifecycle: "ARCHIVED" } });
    expect((await getResult(t, created.mutationId)).known).toBe(false); // KEYSPACE_ARCHIVED is not recorded (07 §7.6)
    expect(archived.watermarks[0]?.logEpoch).toBe(1); // it passed the binding check (07 [MUT-7])
  });

  it("a duplicate still meets the lane checks: CLIENT_UNKNOWN and OUT_OF_ORDER for a slot that its client instance cannot have", async () => {
    const { t } = setup();
    const a = mut({ seq: 1, client: C1 });
    expect(out(await send(t, a))).toBe("appended");
    // under a client instance that the keyspace has no lane state for, a slot above 1 is CLIENT_UNKNOWN, duplicate or not (07 [MUT-31])
    expect(rejectCode(await send(t, create(MutationSchema, { ...a, clientId: C2, clientSeq: 4n })))).toBe(Code.CLIENT_UNKNOWN);
    // the resend at slot 1 of the new instance binds it (07 [MUT-50]) and returns the stored outcome
    const re1 = await send(t, create(MutationSchema, { ...a, clientId: C2, clientSeq: 1n }));
    expect([out(re1), re1.results[0]?.duplicate]).toEqual(["appended", true]);
    expect(re1.watermarks[0]?.processedClientSeq).toBe(1n);
    // slot 3 of that instance with the lane at 1: OUT_OF_ORDER for the duplicate too, not the stored outcome
    const re3 = await send(t, create(MutationSchema, { ...a, clientId: C2, clientSeq: 3n }));
    expect(retryReason(re3.results[0])).toBe(RetryReason.OUT_OF_ORDER);
    // and a duplicate whose slot is bound to another id is CLIENT_FORKED (07 [MUT-32])
    const b = mut({ seq: 2, client: C2 });
    expect(out(await send(t, b))).toBe("appended");
    expect(rejectCode(await send(t, create(MutationSchema, { ...a, clientId: C2, clientSeq: 2n })))).toBe(Code.CLIENT_FORKED);
  });

  it("a frozen keyspace answers a duplicate too (07 Table 7-1 step 8)", async () => {
    const { t, server } = setup();
    const a = mut({ seq: 1 });
    await send(t, a);
    server.freeze(KS);
    expect(rejectCode(await send(t, a))).toBe(Code.HISTORY_DIVERGED);
  });
});

describe("the fake server: GATED intents as store/gated.go answers them (07 [MUT-8], [MUT-37], [MUT-39], 08 [GATE-57])", () => {
  it("log_intent: false logs no INTENT record, and every result for the intent has intent_seq 0; a logged intent has the seq of its INTENT", async () => {
    const { t, server } = setup();
    const q = mut({ seq: 1, lane: Lane.GATED, op: "ApproveQuiet", key: "settings/q" });
    const rq = await send(t, q);
    expect(rq.results[0]?.intentSeq).toBe(0n);
    expect(retryReason(rq.results[0])).toBe(RetryReason.GATE_TIMEOUT);
    expect(server.records(KS)).toHaveLength(0);
    const g = mut({ seq: 2, lane: Lane.GATED, op: "Approve", key: "settings/g" });
    const rg = await send(t, g);
    expect(rg.results[0]?.intentSeq).toBe(1n);
    expect(server.records(KS).map((r) => r.mutationId)).toEqual([g.mutationId]);
    // every later answer for the unlogged intent: a Retry, known = true, intent_seq 0, never known = false
    const known = await getResult(t, q.mutationId);
    expect([known.known, known.result?.intentSeq, retryReason(known.result)]).toEqual([true, 0n, RetryReason.GATE_TIMEOUT]);
    const dup = await send(t, q);
    expect([dup.results[0]?.duplicate, dup.results[0]?.intentSeq, retryReason(dup.results[0])]).toEqual([true, 0n, RetryReason.GATE_TIMEOUT]);
    // the DECISION of an unlogged intent carries no intent row; a REJECTED one carries the requested changes for audit, none applied (06 [LOG-32])
    server.decide(KS, q.mutationId, { status: "REJECTED", reject: mk(Code.GATE_REJECTED) });
    const rec = server.records(KS).at(-1);
    expect(rec?.mutationId).toBe(q.mutationId);
    expect(rec?.changes.map((c) => c.key)).toEqual(["settings/q"]);
    expect(server.value(KS, "settings/q")).toBeUndefined();
    const done = await getResult(t, q.mutationId);
    expect(done.result?.outcome.case).toBe("decided");
    server.decide(KS, g.mutationId, { status: "COMMITTED" });
    expect(server.records(KS).at(-1)?.changes.map((c) => c.key)).toEqual(["settings/g", `abcp.intent/${g.mutationId}`]);
  });

  it("a GATED action whose lane wait is not satisfied is answered Retry{LANE_NOT_READY} with intent_seq 0 when unlogged, until the lane reaches the slot; a resend of the newest client replaces the wait", async () => {
    const { t } = setup();
    const q = mut({ seq: 1, lane: Lane.GATED, op: "ApproveQuiet", key: "settings/q", after: 1, client: C1 }); // waits for slot 1 of the SEQUENCED lane
    const r1 = await send(t, q);
    expect([retryReason(r1.results[0]), r1.results[0]?.intentSeq]).toEqual([RetryReason.LANE_NOT_READY, 0n]);
    const g = mut({ seq: 2, lane: Lane.GATED, op: "Approve", key: "settings/g", after: 1, client: C1 });
    const rg = await send(t, g);
    expect([retryReason(rg.results[0]), rg.results[0]?.intentSeq]).toEqual([RetryReason.LANE_NOT_READY, 1n]); // logged: the seq of the INTENT
    const poll = await getResult(t, q.mutationId);
    expect([poll.known, retryReason(poll.result), poll.result?.intentSeq]).toEqual([true, RetryReason.LANE_NOT_READY, 0n]);
    // the SEQUENCED slot 1 is processed: the wait is satisfied, the reason is GATE_TIMEOUT from now on
    expect(out(await send(t, mut({ seq: 1, client: C1 })))).toBe("appended");
    expect(retryReason((await getResult(t, q.mutationId)).result)).toBe(RetryReason.GATE_TIMEOUT);
    expect(retryReason((await send(t, g)).results[0])).toBe(RetryReason.GATE_TIMEOUT);

    // a resend under a new client id (the newest of the subject) replaces a wait that is not satisfied (07 [MUT-39])
    const h = mut({ seq: 3, lane: Lane.GATED, op: "ApproveQuiet", key: "settings/h", after: 5, client: C1 });
    expect(retryReason((await send(t, h)).results[0])).toBe(RetryReason.LANE_NOT_READY);
    const h2 = create(MutationSchema, { ...h, clientId: C2, clientSeq: 1n, afterClientSeq: 0n });
    const re = await send(t, h2);
    expect([re.results[0]?.duplicate, retryReason(re.results[0])]).toEqual([true, RetryReason.GATE_TIMEOUT]);
  });

  it("a GATED mutation over max_inflight_gated_per_client gets Retry{RATE_LIMITED}, is not registered, binds no slot and is not logged; a resend of a registered one is not counted again; a decision frees room; another client instance has its own cap", async () => {
    const { t, server } = setupWith(new StaticManifests(MANIFEST), { maxInflightGatedPerClient: 2 });
    const a = mut({ seq: 1, lane: Lane.GATED, op: "Approve", key: "settings/a", client: C1 });
    const b = mut({ seq: 2, lane: Lane.GATED, op: "ApproveQuiet", key: "settings/b", client: C1 });
    const c = mut({ seq: 3, lane: Lane.GATED, op: "Approve", key: "settings/c", client: C1 });
    expect(retryReason((await send(t, a)).results[0])).toBe(RetryReason.GATE_TIMEOUT);
    expect(retryReason((await send(t, b)).results[0])).toBe(RetryReason.GATE_TIMEOUT);
    const over = await send(t, c);
    expect(retryReason(over.results[0])).toBe(RetryReason.RATE_LIMITED);
    expect(over.results[0]?.intentSeq).toBe(0n);
    expect((await getResult(t, c.mutationId)).known).toBe(false);
    expect(server.records(KS).map((r) => r.mutationId)).toEqual([a.mutationId]); // c logged nothing
    expect(retryReason((await send(t, a)).results[0])).toBe(RetryReason.GATE_TIMEOUT); // a duplicate is never counted again
    // slot 3 is free: another action at it is over the cap as well
    const d = mut({ seq: 3, lane: Lane.GATED, op: "Approve", key: "settings/d", client: C1 });
    expect(retryReason((await send(t, d)).results[0])).toBe(RetryReason.RATE_LIMITED);
    // another instance is counted on its own
    const e = mut({ seq: 1, lane: Lane.GATED, op: "Approve", key: "settings/e", client: C2 });
    expect(retryReason((await send(t, e)).results[0])).toBe(RetryReason.GATE_TIMEOUT);
    // the decision of a ends its reservation
    server.decide(KS, a.mutationId, { status: "COMMITTED" });
    expect(retryReason((await send(t, c)).results[0])).toBe(RetryReason.GATE_TIMEOUT);
  });
});

describe("the fake server: the limits of 07 [MUT-4] and [MUT-76], and the lifecycle hold of 11 [COORD-96]", () => {
  it("a request over max_mutations_per_request or max_request_bytes is ResourceExhausted with no detail and nothing is processed; an empty request and one with two client ids are InvalidArgument", async () => {
    const { t, server } = setupWith(new StaticManifests(MANIFEST), { maxMutationsPerRequest: 2, maxRequestBytes: 600 });
    const ms = [mut({ seq: 1, client: C1 }), mut({ seq: 2, client: C1 }), mut({ seq: 3, client: C1 })];
    const e1 = (await send(t, ...ms).catch((e: unknown) => e)) as RpcError;
    expect([e1.status, e1.reject, e1.retry]).toEqual(["ResourceExhausted", undefined, undefined]);
    const big = mut({ seq: 1, client: C1, key: "settings/big", value: "x".repeat(700) });
    const e2 = (await send(t, big).catch((e: unknown) => e)) as RpcError;
    expect([e2.status, e2.reject]).toEqual(["ResourceExhausted", undefined]);
    expect(server.records(KS)).toHaveLength(0);
    expect(out(await send(t, ms[0] as Mutation, ms[1] as Mutation))).toBe("appended"); // at the count limit
    const e3 = (await send(t).catch((e: unknown) => e)) as RpcError;
    expect([e3.status, e3.reject?.code]).toEqual(["InvalidArgument", Code.INVALID_ENVELOPE]);
    const e4 = (await send(t, mut({ seq: 3, client: C1 }), mut({ seq: 1, client: C2 })).catch((e: unknown) => e)) as RpcError;
    expect([e4.status, e4.reject?.code]).toEqual(["InvalidArgument", Code.INVALID_ENVELOPE]);
  });

  it("an action over max_action_bytes, or with more changes than max_changes_per_action, is a recorded PAYLOAD_TOO_LARGE", async () => {
    const { t } = setupWith(new StaticManifests(MANIFEST), { maxActionBytes: 400, maxChangesPerAction: 2 });
    const r = await send(t, mut({ seq: 1, value: "x".repeat(450) }));
    expect(rejectCode(r)).toBe(Code.PAYLOAD_TOO_LARGE);
    expect(r.watermarks[0]?.processedClientSeq).toBe(1n); // recorded: it consumes its slot
    const many = create(MutationSchema, { ...mut({ seq: 2 }), changes: ["a", "b", "c"].map((k) => create(KeyChangeSchema, { key: `settings/${k}`, kind: ChangeKind.SET, value: jsonBytes(1), encoding: "json/v1" })) });
    expect(rejectCode(await send(t, many))).toBe(Code.PAYLOAD_TOO_LARGE);
  });

  it("ArchiveKeyspace, SplitKeyspace and DeleteKeyspace are refused while an intent is pending, logged or not, with LIFECYCLE_HOLD and the counts (11 [COORD-96], [COORD-97]); a frozen keyspace may be deleted", async () => {
    const { t, server } = setup();
    server.createKeyspaceNow("dst");
    await send(t, mut({ seq: 1, lane: Lane.GATED, op: "ApproveQuiet", key: "settings/q" }));
    const g = mut({ seq: 2, lane: Lane.GATED, op: "Approve", key: "settings/g" });
    await send(t, g);
    expect(server.openItems(KS)).toEqual({ pendingIntents: 2, ownedKeys: 0, holdKeys: 0 });
    for (const call of [() => server.archive(KS), () => server.split(KS, []), () => server.deleteKeyspace(KS)]) {
      let err: unknown;
      try {
        call();
      } catch (e) {
        err = e;
      }
      expect(err).toBeInstanceOf(RpcError);
      expect((err as RpcError).status).toBe("FailedPrecondition");
      expect((err as RpcError).reject).toMatchObject({ code: Code.ADMIN_REFUSED, details: { reason: "LIFECYCLE_HOLD", pending_intents: "2", owned_keys: "0", hold_keys: "0" } });
    }
    // it changed nothing: the keyspace still takes actions
    expect(out(await send(t, mut({ seq: 1 })))).toBe("appended");
    server.decide(KS, g.mutationId, { status: "COMMITTED" });
    expect(server.openItems(KS).pendingIntents).toBe(1);
    server.freeze(KS);
    server.deleteKeyspace(KS); // 12 [ADM-35]: the frozen keyspace's open items cannot end otherwise
    expect((await getResult(t, g.mutationId)).known).toBe(false);
  });

  it("a split seeds the successors from the source at its split seq, each key to the rule that it matches, with the versions of the source (12 [ADM-45], 06 [LOG-67])", async () => {
    const { t, server } = setup();
    server.createKeyspaceNow("dst1");
    server.createKeyspaceNow("dst2");
    await send(t, mut({ seq: 1, key: "issues/i1", op: "Create" }));
    await send(t, mut({ seq: 2, key: "settings/s" }));
    const rule = (prefix: string, keyspaceId: string): SuccessorRule => create(SuccessorRuleSchema, { prefix, keyspaceId });
    server.split(KS, [rule("issues/", "dst1"), rule("", "dst2")]);
    expect(server.head("dst1")).toBe(2n);
    expect(server.head("dst2")).toBe(2n);
    expect(server.value("dst1", "issues/i1")).toBeDefined();
    expect(server.value("dst1", "settings/s")).toBeUndefined();
    expect(server.value("dst2", "settings/s")).toBeDefined();
    expect(server.value("dst2", "issues/i1")).toBeUndefined();
    expect(server.stateOf("dst1").key("issues/i1")?.version).toBe(1n);
    // the successor takes actions after its seed
    const m = create(MutationSchema, { ...mut({ seq: 1, key: "settings/t" }), keyspaceId: "dst2" });
    const r = await send(t, m);
    expect(out(r)).toBe("appended");
    expect(r.results[0]?.outcome.value).toMatchObject({ seq: 3n });
    // a mutation to the split source is KEYSPACE_ARCHIVED with the lifecycle SPLIT
    expect((await send(t, mut({ seq: 3 }))).results[0]?.outcome.value).toMatchObject({ code: Code.KEYSPACE_ARCHIVED, details: { lifecycle: "SPLIT" } });
  });
});
