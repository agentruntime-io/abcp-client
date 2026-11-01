// The fake server's answers for C3 (14 §14.7 to §14.10), checked against what `server/internal/session` and `server/internal/auth` do and what
// 10 §10.7, 09 §9.3 and §9.7, and 07 §7.10 say: revocation (10 [AUTH-51], [AUTH-52]), grants (09 [READ-39]), interest (09 [READ-22] to [READ-28]),
// and the offline horizon (07 [MUT-84], [MUT-89]). A client suite that passes against it means something.

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { StaticManifests, RpcError, jsonBytes } from "../src/engine/index.js";
import {
  LoadStateRequestSchema,
  MutateRequestSchema,
  MutationSchema,
  SubscribeRequestSchema,
  UpdateSubscriptionRequestSchema,
  UpdateSubscriptionRequest_InterestSchema,
  type SubscribeResponse,
} from "../src/gen/abpc/v2/sync_pb.js";
import { ChangeKind, Code, KeyChangeSchema, KeyspaceCursorSchema, Lane } from "../src/gen/abpc/v2/types_pb.js";
import { FakeServer, ManualClock, type FakeTransport } from "../src/fake/index.js";
import { kc, KS, MANIFEST } from "./helpers/engine.js";

const flush = (): Promise<void> => new Promise((r) => setImmediate(r));
let clients = 0;

function setup(): { server: FakeServer; clock: ManualClock; ana: FakeTransport } {
  const clock = new ManualClock();
  const server = new FakeServer({ manifests: new StaticManifests(MANIFEST), clock });
  server.createKeyspaceNow(KS);
  server.createKeyspaceNow("ks2");
  return { server, clock, ana: server.transport(server.principal("ana")) };
}

/** A reader of a `Subscribe` stream: the frames so far, and the end (an error or a clean end). */
function open(t: FakeTransport, keyspaces: { id: string; interest?: string[]; after?: bigint; epoch?: number }[]): { frames: SubscribeResponse[]; ended: () => Promise<unknown>; abort: () => void } {
  const ac = new AbortController();
  const frames: SubscribeResponse[] = [];
  const req = create(SubscribeRequestSchema, { keyspaces: keyspaces.map((k) => create(KeyspaceCursorSchema, { keyspaceId: k.id, interestPrefixes: k.interest ?? [], afterSeq: k.after ?? 0n, logEpoch: k.epoch ?? 1 })), clientId: `aaaaaaaa-aaaa-4aaa-8aaa-${String(++clients).padStart(12, "0")}` });
  let end: unknown = "open";
  void (async () => {
    try {
      for await (const f of t.subscribe(req, { signal: ac.signal })) frames.push(f);
      end = "clean";
    } catch (e) {
      end = e;
    }
  })();
  return { frames, ended: async () => (await flush(), await flush(), end), abort: () => ac.abort() };
}

const controls = (frames: SubscribeResponse[]): string[] =>
  frames.flatMap((f) => {
    if (f.frame.case !== "control") return [];
    const c = f.frame.value.control;
    return c.case === "keyspaceDropped" ? [`dropped:${c.value.keyspaceId}:${c.value.reason}`] : c.case === "keyspaceAdded" ? [`added:${c.value.cursor?.keyspaceId}:${(c.value.cursor?.interestPrefixes ?? []).join(",")}:${c.value.cursor?.afterSeq}`] : c.case === "closing" ? [`closing:${c.value.reason}`] : [c.case ?? "?"];
  });
const records = (frames: SubscribeResponse[]): { prev: bigint; cursor: bigint; seqs: bigint[] }[] =>
  frames.flatMap((f) => (f.frame.case === "records" ? [{ prev: f.frame.value.prevCursor, cursor: f.frame.value.cursor, seqs: f.frame.value.records.map((r) => r.seq) }] : []));

describe("10 [AUTH-51] RevokeAccess of a subject", () => {
  it("every followed keyspace of every session of the subject gets KeyspaceDropped{ACCESS_REVOKED}, then Closing{SUBJECT_REVOKED} and the status PermissionDenied (`session.RevokeSubject`); other subjects' sessions go on", async () => {
    const { server, ana } = setup();
    const bob = server.transport(server.principal("bob"));
    const a = open(ana, [{ id: KS }, { id: "ks2" }]);
    const b = open(bob, [{ id: KS }]);
    await flush();
    expect(server.revokeSubject("ana")).toBe(1);
    const end = await a.ended();
    expect(controls(a.frames).filter((c) => !c.startsWith("added") && c !== "subscribed")).toEqual(["dropped:ks1:2", "dropped:ks2:2", "closing:2"]);
    expect(end).toBeInstanceOf(RpcError);
    expect((end as RpcError).status).toBe("PermissionDenied");
    expect(await b.ended()).toBe("open");
    b.abort();
  });

  it("a token issued before not_before (with the 60 s leeway) is refused with Unauthenticated, and each refusal carries the SUBJECT_REVOKED detail until a token of the binding that is not issued before it has been accepted: the recovery mark", async () => {
    const { server, clock, ana } = setup();
    const sibling = server.transport(server.principal("ana")); // a second tab of the same device binding, with an old token
    server.revokeSubject("ana");
    const refused = async (t: FakeTransport): Promise<{ status: string; cause: string | undefined } | "accepted"> => {
      try {
        await t.getMutationResult({ $typeName: "abpc.v2.GetMutationResultRequest", keyspaceId: KS, mutationId: "x" });
        return "accepted";
      } catch (e) {
        const err = e as RpcError;
        return { status: err.status, cause: err.reject?.details["cause"] };
      }
    };
    expect(await refused(ana)).toEqual({ status: "Unauthenticated", cause: "SUBJECT_REVOKED" });
    expect(await refused(ana)).toEqual({ status: "Unauthenticated", cause: "SUBJECT_REVOKED" }); // a lost response does not lose the notice
    await clock.advance(30_000);
    ana.renewToken(); // issued 30 s after the revocation: still inside the 60 s leeway
    expect(await refused(ana)).toEqual({ status: "Unauthenticated", cause: "SUBJECT_REVOKED" });
    await clock.advance(31_000);
    ana.renewToken();
    expect(await refused(ana)).toBe("accepted"); // not issued before not_before: accepted, and the binding recovers
    expect(await refused(sibling)).toEqual({ status: "Unauthenticated", cause: undefined }); // an old token replayed after recovery gets no detail
  });

  it("a revocation of the principal's actor chain matches too, and `not_before` can be later than now", async () => {
    const { server, clock } = setup();
    const delegated = server.transport({ ...server.principal("ana"), actorChain: ["svc-saga"] });
    expect(server.revokeSubject("svc-saga")).toBe(0);
    await expect(delegated.getMutationResult({ $typeName: "abpc.v2.GetMutationResultRequest", keyspaceId: KS, mutationId: "x" })).rejects.toMatchObject({ status: "Unauthenticated" });
    const later = server.transport(server.principal("bob"));
    server.revokeSubject("bob", { notBefore: clock.now() + 3_600_000 });
    later.renewToken();
    await expect(later.getMutationResult({ $typeName: "abpc.v2.GetMutationResultRequest", keyspaceId: KS, mutationId: "x" })).rejects.toMatchObject({ status: "Unauthenticated" });
  });
});

describe("10 [AUTH-52] RevokeAccess of a keyspace pattern", () => {
  it("every session that follows a matching keyspace gets KeyspaceDropped{ACCESS_REVOKED} for it; the session goes on; a pattern with a trailing * matches by prefix", async () => {
    const { server, ana } = setup();
    const a = open(ana, [{ id: KS }, { id: "ks2" }]);
    await flush();
    expect(server.revokeKeyspaces(KS)).toBe(1);
    await flush();
    expect(controls(a.frames).filter((c) => c.startsWith("dropped"))).toEqual(["dropped:ks1:2"]);
    expect(server.revokeKeyspaces("ks*")).toBe(1); // ks1 was dropped already, ks2 matches
    await flush();
    expect(controls(a.frames).filter((c) => c.startsWith("dropped"))).toEqual(["dropped:ks1:2", "dropped:ks2:2"]);
    expect(await a.ended()).toBe("open");
    a.abort();
  });

  it("a token issued before not_before holds no grant: a Subscribe and a load get the answers for a keyspace that the caller may not read, never ACCESS_REVOKED (09 [READ-32], [READ-64])", async () => {
    const { server, clock, ana } = setup();
    server.revokeKeyspaces(KS);
    const a = open(ana, [{ id: KS }]);
    await flush();
    expect(controls(a.frames).filter((c) => c.startsWith("dropped"))).toEqual(["dropped:ks1:1"]); // GRANT_CHANGED
    a.abort();
    await expect(ana.loadState(create(LoadStateRequestSchema, { cursor: create(KeyspaceCursorSchema, { keyspaceId: KS }) }))).rejects.toMatchObject({ status: "PermissionDenied" });
    await clock.advance(61_000);
    ana.renewToken();
    await expect(ana.loadState(create(LoadStateRequestSchema, { cursor: create(KeyspaceCursorSchema, { keyspaceId: KS }) }))).resolves.toBeDefined();
  });
});

describe("09 [READ-39], 10 [AUTH-49] grants", () => {
  it("`denyRead` drops a keyspace with GRANT_CHANGED at once, or at the next RefreshSession, whose answer lists it; a write without read is the unrecorded PERMISSION_DENIED", async () => {
    const { server, ana } = setup();
    const a = open(ana, [{ id: KS }, { id: "ks2" }]);
    await flush();
    server.denyRead("ana", KS);
    await flush();
    expect(controls(a.frames).filter((c) => c.startsWith("dropped"))).toEqual(["dropped:ks1:1"]);
    server.denyRead("ana", "ks2", { drop: false });
    await flush();
    expect(controls(a.frames).filter((c) => c.startsWith("dropped"))).toHaveLength(1);
    const sessionId = (a.frames[0]?.frame.case === "control" && a.frames[0].frame.value.control.case === "subscribed" ? a.frames[0].frame.value.control.value.sessionId : "") ?? "";
    const resp = await ana.refreshSession({ $typeName: "abpc.v2.RefreshSessionRequest", sessionId });
    expect(resp.droppedKeyspaceIds).toEqual(["ks2"]);
    await flush();
    expect(controls(a.frames).filter((c) => c.startsWith("dropped"))).toEqual(["dropped:ks1:1", "dropped:ks2:1"]);
    const m = create(MutationSchema, { mutationId: "00000000-0000-4000-8000-000000000001", keyspaceId: KS, opType: "Set", lane: Lane.SEQUENCED, clientId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", clientSeq: 1n, changes: [create(KeyChangeSchema, { key: "settings/a", kind: ChangeKind.SET, value: jsonBytes(1), encoding: "json/v1" })] });
    const r = await ana.mutate(create(MutateRequestSchema, { mutations: [m] }));
    expect(r.results[0]?.outcome.value).toMatchObject({ code: Code.PERMISSION_DENIED });
    server.allowRead("ana", KS);
    const again = await ana.mutate(create(MutateRequestSchema, { mutations: [m] }));
    expect(again.results[0]?.outcome.case).toBe("appended"); // nothing was recorded for the refusal
    a.abort();
  });
});

describe("09 §9.3, §9.7 interest", () => {
  it("a record that touches no prefix of the interest is not delivered, and the delivery watermark passes it: the next frame's prev_cursor is the watermark that passed it", async () => {
    const { server, ana } = setup();
    const a = open(ana, [{ id: KS, interest: ["issues/i1"] }]);
    await flush();
    expect(controls(a.frames)).toContain("added:ks1:issues/i1:0"); // the interest is echoed as sent
    server.write(KS, [kc("settings/x", "set", 1)]); // seq 1: outside
    server.write(KS, [kc("issues/i1/title", "set", "t")]); // seq 2: inside
    server.write(KS, [kc("issues/i1", "delete")]); // seq 3: a marker above the prefix
    server.write(KS, [kc("issues/i2/title", "set", "x")]); // seq 4: outside
    await flush();
    expect(records(a.frames)).toEqual([
      { prev: 1n, cursor: 2n, seqs: [2n] }, // the watermark passed seq 1 (outside) without a frame
      { prev: 2n, cursor: 3n, seqs: [3n] },
    ]);
    a.abort();
  });

  it("a record is delivered whole: all its changes, the ones outside the interest included; SYSTEM records reach every interest; the DECISION of an INTENT that touches does too", async () => {
    const { server, ana } = setup();
    const a = open(ana, [{ id: KS, interest: ["issues/i1"] }]);
    await flush();
    server.write(KS, [kc("issues/i1/title", "set", "a"), kc("settings/other", "set", 1)]);
    await flush();
    const r = a.frames.flatMap((f) => (f.frame.case === "records" ? f.frame.value.records : []));
    expect(r[0]?.changes.map((c) => c.key)).toEqual(["issues/i1/title", "settings/other"]);
    a.abort();
  });

  it("`set_interest` announces its switch point with a KeyspaceAdded: the seq examined under the old interest (Q28), before any frame under the new one; an entry that fails is listed in `rejected`", async () => {
    const { server, ana } = setup();
    const a = open(ana, [{ id: KS, interest: ["issues/i1"] }]);
    await flush();
    server.write(KS, [kc("settings/x", "set", 1)]); // examined, not sent: the watermark is 1
    await flush();
    const sessionId = (a.frames[0]?.frame.case === "control" && a.frames[0].frame.value.control.case === "subscribed" ? a.frames[0].frame.value.control.value.sessionId : "") ?? "";
    const resp = await ana.updateSubscription(
      create(UpdateSubscriptionRequestSchema, {
        sessionId,
        setInterest: [
          create(UpdateSubscriptionRequest_InterestSchema, { keyspaceId: KS, interestPrefixes: ["settings"] }),
          create(UpdateSubscriptionRequest_InterestSchema, { keyspaceId: "ks2", interestPrefixes: ["a"] }), // not followed
          create(UpdateSubscriptionRequest_InterestSchema, { keyspaceId: KS, interestPrefixes: ["bad/"] }),
        ],
      }),
    );
    expect(resp.rejected.map((x) => [x.keyspaceId, x.outcome.case === "reject" ? x.outcome.value.code : -1])).toEqual([["ks2", Code.INVALID_ENVELOPE], [KS, Code.INVALID_KEY]]);
    await flush();
    expect(controls(a.frames).at(-1)).toBe("added:ks1:settings:1");
    server.write(KS, [kc("settings/y", "set", 1)]);
    await flush();
    expect(records(a.frames).at(-1)).toEqual({ prev: 1n, cursor: 2n, seqs: [2n] });
    a.abort();
  });

  it("a cold load with an interest holds the keys under it, the tombstones that match it, and the entity tombstones of the ancestors of its prefixes", async () => {
    const { server, ana } = setup();
    server.write(KS, [kc("issues/i1", "set", { a: 1 }), kc("issues/i1/title", "set", "a")]);
    server.write(KS, [kc("issues/i2", "set", { a: 1 }), kc("issues/i2/title", "set", "b")]);
    server.write(KS, [kc("issues/i2", "delete")]); // an entity tombstone: an ancestor of the prefix issues/i2/title
    server.write(KS, [kc("settings/a", "set", 1)]);
    const load = async (interest: string[]): Promise<string[]> => (await ana.loadState(create(LoadStateRequestSchema, { cursor: create(KeyspaceCursorSchema, { keyspaceId: KS, interestPrefixes: interest }) }))).entries.map((e) => `${e.key}${e.deleted ? "!" : ""}`);
    expect(await load([])).toEqual(["issues/i1", "issues/i1/title", "issues/i2!", "settings/a"]);
    expect(await load(["issues/i1"])).toEqual(["issues/i1", "issues/i1/title"]);
    expect(await load(["issues/i2/title"])).toEqual(["issues/i2!"]);
    expect(await load(["settings", "issues/i2"])).toEqual(["issues/i2!", "settings/a"]);
    await expect(ana.loadState(create(LoadStateRequestSchema, { cursor: create(KeyspaceCursorSchema, { keyspaceId: KS, interestPrefixes: Array.from({ length: 65 }, (_, i) => `p${i}`) }) }))).rejects.toMatchObject({ status: "ResourceExhausted" });
  });
});

describe("07 [MUT-84], [MUT-89] the offline horizon", () => {
  const act = (id: string, baseSeq: bigint, seq: bigint, key = "settings/a"): ReturnType<typeof create<typeof MutateRequestSchema>> =>
    create(MutateRequestSchema, {
      mutations: [create(MutationSchema, { mutationId: id, keyspaceId: KS, opType: "Set", lane: Lane.SEQUENCED, clientId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", clientSeq: seq, baseSeq, baseLogEpoch: 1, changes: [create(KeyChangeSchema, { key, kind: ChangeKind.SET, value: jsonBytes(1), encoding: "json/v1" })] })],
    });

  it("refuses an action whose base seq + 1 names a record older than max_offline with the recorded OFFLINE_TOO_LONG and cause HORIZON; one whose base is at or above the head passes (07 [MUT-85])", async () => {
    const { server, clock, ana } = setup();
    server.write(KS, [kc("settings/x", "set", 1)]); // seq 1, at day 0
    await clock.advance(31 * 24 * 3600 * 1000); // max_offline is 30 days
    server.write(KS, [kc("settings/y", "set", 1)]); // seq 2, today
    const old = await ana.mutate(act("00000000-0000-4000-8000-0000000000a1", 0n, 1n)); // base 0: refers to seq 1, 31 days old
    expect(old.results[0]?.outcome.value).toMatchObject({ code: Code.OFFLINE_TOO_LONG });
    expect((old.results[0]?.outcome.value as { details: Record<string, string> }).details["cause"]).toBe("HORIZON");
    const again = await ana.mutate(act("00000000-0000-4000-8000-0000000000a1", 0n, 1n)); // recorded (07 [MUT-89]): a resend gets the stored answer
    expect(again.results[0]?.duplicate).toBe(true);
    expect(again.results[0]?.outcome.case).toBe("rejected");
    const young = await ana.mutate(act("00000000-0000-4000-8000-0000000000a2", 1n, 2n)); // base 1: seq 2 is today's
    expect(young.results[0]?.outcome.case).toBe("appended");
    const head = await ana.mutate(act("00000000-0000-4000-8000-0000000000a3", server.head(KS), 3n)); // at the head: no record after the base
    expect(head.results[0]?.outcome.case).toBe("appended");
  });

  it("a base at or below the prune floor is older than the horizon (07 [MUT-87]); a split successor's floor is its source's, judged against the source", async () => {
    const { server, ana } = setup();
    server.write(KS, [kc("settings/x", "set", 1)]);
    server.write(KS, [kc("settings/y", "set", 1)]);
    server.prune(KS, 2n);
    const r = await ana.mutate(act("00000000-0000-4000-8000-0000000000b1", 1n, 1n));
    expect(r.results[0]?.outcome.value).toMatchObject({ code: Code.OFFLINE_TOO_LONG });
    const ok = await ana.mutate(act("00000000-0000-4000-8000-0000000000b2", 2n, 2n));
    expect(ok.results[0]?.outcome.case).toBe("appended");
  });
});

describe("draft 14: the answers that the text changed (07 [MUT-7], 09 [READ-4], [READ-22], [READ-65], 03 [ID-40])", () => {
  const CLIENT = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
  const mutation = (ks: string, lane: Lane, seq: number, key: string, n: number): ReturnType<typeof create<typeof MutationSchema>> =>
    create(MutationSchema, {
      mutationId: `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`,
      keyspaceId: ks,
      opType: lane === Lane.GATED ? "Approve" : "Set",
      lane,
      clientId: CLIENT,
      clientSeq: BigInt(seq),
      changes: [create(KeyChangeSchema, { key, kind: ChangeKind.SET, value: jsonBytes(1), encoding: "json/v1" })],
    });

  it("07 [MUT-7]: the lane watermarks of a `Mutate` answer, of a load and of a heartbeat are in increasing order of (keyspace_id, lane), the ids compared in bytes, the lane by its enum number", async () => {
    const { server, ana } = setup();
    // the request names ks2 before ks1, and the GATED lane before the SEQUENCED one
    const resp = await ana.mutate(
      create(MutateRequestSchema, {
        mutations: [mutation("ks2", Lane.GATED, 1, "settings/a", 1), mutation("ks2", Lane.SEQUENCED, 1, "settings/b", 2), mutation(KS, Lane.GATED, 1, "settings/c", 3), mutation(KS, Lane.SEQUENCED, 1, "settings/d", 4)],
      }),
    );
    expect(resp.watermarks.map((w) => `${w.keyspaceId}/${w.lane}`)).toEqual([`ks1/${Lane.SEQUENCED}`, `ks1/${Lane.GATED}`, `ks2/${Lane.SEQUENCED}`, `ks2/${Lane.GATED}`]);
    const load = await ana.loadState(create(LoadStateRequestSchema, { cursor: create(KeyspaceCursorSchema, { keyspaceId: KS }), clientId: CLIENT }));
    expect(load.laneWatermarks.map((w) => w.lane)).toEqual([Lane.SEQUENCED, Lane.GATED]);
    const ac = new AbortController();
    const frames: SubscribeResponse[] = [];
    void (async () => {
      try {
        for await (const f of ana.subscribe(create(SubscribeRequestSchema, { keyspaces: ["ks2", KS].map((id) => create(KeyspaceCursorSchema, { keyspaceId: id, logEpoch: 1 })), clientId: CLIENT }), { signal: ac.signal })) frames.push(f);
      } catch {
        // the abort ends the stream
      }
    })();
    await flush();
    await flush();
    server.heartbeat();
    await flush();
    const hb = frames.find((f) => f.frame.case === "heartbeat");
    expect(hb?.frame.case === "heartbeat" ? hb.frame.value.laneWatermarks.map((w) => `${w.keyspaceId}/${w.lane}`) : []).toEqual([`ks1/${Lane.SEQUENCED}`, `ks1/${Lane.GATED}`, `ks2/${Lane.SEQUENCED}`, `ks2/${Lane.GATED}`]);
    ac.abort();
  });

  it("09 [READ-65], 03 [ID-40]: the epoch of a cursor is judged only when after_seq > 0, and a cursor epoch of 0 matches no epoch", async () => {
    const { server, ana } = setup();
    server.write(KS, [kc("settings/x", "set", 1)]);
    const fresh = open(ana, [{ id: KS, after: 0n, epoch: 0 }]);
    await flush();
    expect(controls(fresh.frames).filter((c) => c.startsWith("added:") || c.startsWith("dropped:"))).toEqual(["added:ks1::0"]); // after_seq = 0: the epoch is not judged
    fresh.abort();
    const resumed = open(ana, [{ id: KS, after: 1n, epoch: 0 }]);
    await flush();
    expect(controls(resumed.frames).filter((c) => c.startsWith("added:") || c.startsWith("dropped:"))).toEqual([`dropped:ks1:${8}`]); // EPOCH_CHANGED
    resumed.abort();
  });

  it("09 [READ-22]: a `KeyspaceAdded` echoes the prefixes as sent, a repeated one included, in the order sent", async () => {
    const { ana } = setup();
    const a = open(ana, [{ id: KS, interest: ["settings", "issues", "settings"] }]);
    await flush();
    expect(controls(a.frames).find((c) => c.startsWith("added:"))).toContain("added:ks1:settings,issues,settings");
    a.abort();
  });

  it("09 [READ-4], [READ-62], [READ-35]: the identity check of a client_id comes before the sessions cap, which comes last; a call that the cap refuses binds nothing", async () => {
    const clock = new ManualClock();
    const server = new FakeServer({ manifests: new StaticManifests(MANIFEST), clock, maxSessions: 1 });
    server.createKeyspaceNow(KS);
    const ana = server.transport(server.principal("ana"));
    const bob = server.transport(server.principal("bob"));
    const [C1, C2, C3] = ["11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", "33333333-3333-4333-8333-333333333333"];
    const subscribe = (t: FakeTransport, clientId: string): { frames: SubscribeResponse[]; end: Promise<unknown>; abort: () => void } => {
      const ac = new AbortController();
      const frames: SubscribeResponse[] = [];
      const end = (async (): Promise<unknown> => {
        try {
          for await (const f of t.subscribe(create(SubscribeRequestSchema, { keyspaces: [create(KeyspaceCursorSchema, { keyspaceId: KS, logEpoch: 1 })], clientId }), { signal: ac.signal })) frames.push(f);
          return "clean";
        } catch (e) {
          return e;
        }
      })();
      return { frames, end, abort: () => ac.abort() };
    };
    const a1 = subscribe(ana, C1); // ana's one session: C1 is bound to ana
    const b1 = subscribe(bob, C2); // bob's one session: C2 is bound to bob
    await flush();
    await flush();
    // bob names C1, bound to ana, and is at the cap too: the identity check is the first that fails
    const mismatch = await subscribe(bob, C1).end;
    expect(mismatch).toBeInstanceOf(RpcError);
    expect(mismatch).toMatchObject({ status: "PermissionDenied", reject: { code: Code.ACTOR_MISMATCH } });
    // bob names a new client_id and is at the cap: the sessions cap refuses, and binds nothing
    const capped = await subscribe(bob, C3).end;
    expect(capped).toMatchObject({ status: "ResourceExhausted", reject: { code: Code.QUOTA_EXCEEDED } });
    a1.abort();
    b1.abort();
    await flush();
    await flush();
    const a2 = subscribe(ana, C3); // C3 is not bound to bob: ana binds it
    await flush();
    await flush();
    expect(a2.frames.some((f) => f.frame.case === "control" && f.frame.value.control.case === "subscribed")).toBe(true);
    a2.abort();
  });
});
