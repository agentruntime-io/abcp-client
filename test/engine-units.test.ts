// Units of the engine: the backoffs of 04 Table 4.11, the classes of Appendix A, identifiers, the storage, the manifests, the content codec,
// and the facts that 14 [CLI-54] and [CLI-55] keep with the stored state.

import { describe, expect, it } from "vitest";
import { create } from "@bufbuild/protobuf";
import { CLIENT_BACKOFF, HOLD_BACKOFF, RECONNECT_BACKOFF, bound, fullJitter, resendDelay, MAX_RETRY_AFTER_MS, clampAfter } from "../src/engine/backoff.js";
import { DEFAULT_CLASS, isKnownCode, isRecordedReject, isKnownRetryReason, isKnownStaleCause, reactionOf, codeName } from "../src/engine/codes.js";
import { dependsOn, decodeContent, encodeContent, stamped, type DepView } from "../src/engine/entry.js";
import { isMutationId, newUuid, newYjsClientId } from "../src/engine/ids.js";
import { MemoryStorage, StaticManifests, StoreFull, StorageError, Engine } from "../src/engine/index.js";
import { MutationSchema } from "../src/gen/abpc/v2/sync_pb.js";
import { ChangeKind, Code, ErrorClass, KeyChangeSchema, PreconditionSchema, RetryReason } from "../src/gen/abpc/v2/types_pb.js";
import { FakeServer, ManualClock, SeededRandom } from "../src/fake/index.js";
import { KS, MANIFEST, rig } from "./helpers/engine.js";

describe("04 Table 4.11, 14 [CLI-43], [CLI-45], 09 [READ-57]: full jitter", () => {
  it("the n-th delay is drawn from [0, min(cap, base x 2^n)]", () => {
    expect([0, 1, 2, 3, 8, 9, 10, 30].map((n) => bound(n, CLIENT_BACKOFF))).toEqual([1000, 2000, 4000, 8000, 256_000, 300_000, 300_000, 300_000]);
    expect([0, 1, 4, 5, 20].map((n) => bound(n, RECONNECT_BACKOFF))).toEqual([1000, 2000, 16_000, 30_000, 30_000]);
    expect([0, 1, 5, 6, 40].map((n) => bound(n, HOLD_BACKOFF))).toEqual([60_000, 120_000, 1_920_000, 3_600_000, 3_600_000]);
    const rnd = new SeededRandom(5);
    for (let n = 0; n < 12; n++) {
      for (let i = 0; i < 50; i++) {
        const d = fullJitter(n, CLIENT_BACKOFF, rnd);
        expect(d).toBeGreaterThanOrEqual(0);
        expect(d).toBeLessThanOrEqual(bound(n, CLIENT_BACKOFF));
      }
    }
    rnd.fixed = 0.999999;
    expect(fullJitter(3, CLIENT_BACKOFF, rnd)).toBe(8000);
    rnd.fixed = 0;
    expect(fullJitter(3, CLIENT_BACKOFF, rnd)).toBe(0);
  });

  it("Retry.after_ms is used when it is the larger, and a value above the maximum counts as the maximum (04 [MAN-91])", () => {
    const rnd = new SeededRandom(1);
    rnd.fixed = 0.5;
    expect(resendDelay(0, 10_000, rnd)).toBe(10_000);
    expect(resendDelay(5, 10_000, rnd)).toBe(16_000); // 0.5 x 32 s
    expect(resendDelay(0, undefined, rnd)).toBe(500);
    expect(clampAfter(10_000_000)).toBe(MAX_RETRY_AFTER_MS);
    expect(clampAfter(-5)).toBe(0);
    expect(clampAfter(Number.NaN)).toBe(0);
  });
});

describe("Appendix A: classes", () => {
  it("every code of the proto has a default class, so a v2.0 client knows every code it can be sent", () => {
    for (const [name, v] of Object.entries(Code)) {
      if (typeof v !== "number" || name === "UNSPECIFIED") continue;
      expect(DEFAULT_CLASS.has(v), name).toBe(true);
      expect(isKnownCode(v)).toBe(true);
      expect(codeName(v)).toBe(name);
    }
    expect(DEFAULT_CLASS.get(Code.BLOB_MISSING)).toBe(ErrorClass.RETRY); // the one Reject of class RETRY
    expect(DEFAULT_CLASS.get(Code.UNKNOWN_OP_TYPE)).toBe(ErrorClass.HOLD);
    expect(DEFAULT_CLASS.get(Code.STALE)).toBe(ErrorClass.CONFLICT);
    expect(DEFAULT_CLASS.get(Code.DEPENDENCY_FAILED)).toBe(ErrorClass.DEAD_LETTER);
  });

  it("15 [COMPAT-39] a class that is unknown or UNSPECIFIED is handled as HOLD", () => {
    expect([ErrorClass.RETRY, ErrorClass.HOLD, ErrorClass.DEAD_LETTER, ErrorClass.CONFLICT, ErrorClass.UNSPECIFIED, 77].map(reactionOf)).toEqual(["RETRY", "HOLD", "DEAD_LETTER", "CONFLICT", "HOLD", "HOLD"]);
    expect(codeName(901)).toBe("CODE_901");
    expect(isKnownRetryReason(RetryReason.BLOB_WITHHELD)).toBe(true);
    expect(isKnownRetryReason(99)).toBe(false);
    expect(isKnownStaleCause(8)).toBe(true);
    expect(isKnownStaleCause(9)).toBe(false);
  });

  it("07 §7.6 a reject is recorded when its class is DEAD_LETTER or CONFLICT, but PERMISSION_DENIED, ACTOR_MISMATCH, KEYSPACE_ARCHIVED and KEYSPACE_DELETED", () => {
    const r = (code: Code): boolean => isRecordedReject({ code, errorClass: DEFAULT_CLASS.get(code) ?? 0 });
    expect(r(Code.SCHEMA_INVALID)).toBe(true);
    expect(r(Code.STALE)).toBe(true);
    expect(r(Code.PERMISSION_DENIED)).toBe(false);
    expect(r(Code.ACTOR_MISMATCH)).toBe(false);
    expect(r(Code.KEYSPACE_ARCHIVED)).toBe(false);
    expect(r(Code.KEYSPACE_DELETED)).toBe(false);
    expect(r(Code.UNKNOWN_OP_TYPE)).toBe(false); // HOLD
    expect(r(Code.BLOB_MISSING)).toBe(false); // RETRY
  });
});

describe("03 [ID-35] identifiers", () => {
  it("a mutation id and a client id are version-4 UUIDs in lowercase; the Yjs client id has 53 random bits", () => {
    const rnd = new SeededRandom(11);
    for (let i = 0; i < 100; i++) {
      const id = newUuid(rnd);
      expect(isMutationId(id)).toBe(true);
      expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
      const y = newYjsClientId(rnd);
      expect(Number.isSafeInteger(y)).toBe(true);
      expect(y).toBeGreaterThan(0);
      expect(y).toBeLessThan(2 ** 53);
    }
    expect(isMutationId("not-a-uuid")).toBe(false);
    expect(isMutationId("00000000-0000-1000-8000-000000000000")).toBe(false); // version 1
    expect(new Set(Array.from({ length: 200 }, () => newUuid(rnd))).size).toBe(200);
  });
});

describe("14 [CLI-21], [CLI-28], [CLI-84] the storage", () => {
  it("a commit is all or none, and what the engine holds never aliases what the storage holds", async () => {
    const s = new MemoryStorage();
    const entry = { mutationId: "a", content: new Uint8Array([1, 2, 3]) } as never;
    s.failNext = 1;
    await expect(s.commit([{ op: "putEntry", entry }])).rejects.toBeInstanceOf(StorageError);
    expect((await s.load()).entries).toHaveLength(0);
    await s.commit([{ op: "putEntry", entry }, { op: "setMeta", key: "k", value: 5 }]);
    const loaded = await s.load();
    loaded.entries[0]!.content[0] = 99; // mutating a loaded copy changes nothing stored
    expect((await s.load()).entries[0]?.content[0]).toBe(1);
    expect(loaded.meta["k"]).toBe(5);
  });

  it("a bounded store refuses growth with StoreFull, applies nothing, and reports itself full", async () => {
    const s = new MemoryStorage({ maxDeadLetters: 1 });
    const dl = (id: string): never => ({ mutationId: id }) as never;
    await s.commit([{ op: "putDeadLetter", entry: dl("1") }]);
    expect(s.full()).toEqual(["deadLetters"]);
    await expect(s.commit([{ op: "putDeadLetter", entry: dl("2") }, { op: "setMeta", key: "x", value: 1 }])).rejects.toBeInstanceOf(StoreFull);
    const st = await s.load();
    expect(st.deadLetters.map((d) => d.mutationId)).toEqual(["1"]);
    expect(st.meta["x"]).toBeUndefined();
    await s.commit([{ op: "ackDeadLetter", mutationId: "1" }]); // the purge by an acknowledgement still runs when a store is full
    expect(s.full()).toEqual([]);
  });
});

describe("14 [CLI-22] the content of an outbox entry", () => {
  const m = create(MutationSchema, {
    mutationId: "11111111-1111-4111-8111-111111111111",
    keyspaceId: "ks",
    opType: "Set",
    clientId: "22222222-2222-4222-8222-222222222222",
    clientSeq: 7n,
    afterClientSeq: 3n,
    baseSeq: 5n,
    baseLogEpoch: 2,
    changes: [create(KeyChangeSchema, { key: "k", kind: ChangeKind.SET, value: new Uint8Array([49]), encoding: "json/v1" })],
  });

  it("is the canonical encoding without the stamping fields, and decodes back to the same action", () => {
    const content = encodeContent(m);
    const d = decodeContent(content);
    expect([d.clientId, d.clientSeq, d.afterClientSeq]).toEqual(["", 0n, 0n]);
    expect(d.baseSeq).toBe(5n);
    expect(d.baseLogEpoch).toBe(2);
    expect(Buffer.from(encodeContent(d)).equals(Buffer.from(content))).toBe(true);
    expect(Buffer.from(encodeContent(m)).equals(Buffer.from(content))).toBe(true); // the stamping fields do not matter to it
  });

  it("stamping changes only the stamping fields", () => {
    const d = decodeContent(encodeContent(m));
    const s = stamped(d, { clientId: "33333333-3333-4333-8333-333333333333", clientSeq: 1, afterClientSeq: 0 });
    expect([s.clientId, s.clientSeq, s.afterClientSeq]).toEqual(["33333333-3333-4333-8333-333333333333", 1n, 0n]);
    expect(Buffer.from(encodeContent(s)).equals(Buffer.from(encodeContent(d)))).toBe(true);
  });

  it("the strict decoder refuses bytes that are not a Mutation", () => {
    expect(() => decodeContent(new Uint8Array([0xff, 0xff, 0xff]))).toThrow();
  });
});

describe("14 [CLI-30] dependency rules as functions", () => {
  const view = (id: string, keys: string[], pre: { key?: string; prefix?: string; after?: string }[] = [], opType = "Op"): DepView => ({
    mutationId: id,
    opType,
    keys,
    mutation: create(MutationSchema, {
      mutationId: id,
      preconditions: pre.map((p) => create(PreconditionSchema, { ...(p.key !== undefined ? { target: { case: "key", value: p.key } } : { target: { case: "prefix", value: p.prefix ?? "" } }), check: p.after !== undefined ? { case: "afterMutation", value: p.after } : { case: "exists", value: true } })),
    }),
  });
  const ctx = { barrier: (o: string): boolean => o === "Bar", fencedBy: (k: string): string | undefined => (k === "status/1" ? "claim/1" : undefined) };
  const dep = (b: DepView, a: DepView, ex: string[] = []): boolean => dependsOn(b, a, new Set(ex), ctx);

  it("keys: equal, or one below the other by a segment boundary; a shared string prefix is not enough", () => {
    expect(dep(view("b", ["a/b"]), view("a", ["a/b"]))).toBe(true);
    expect(dep(view("b", ["a/b/c"]), view("a", ["a/b"]))).toBe(true);
    expect(dep(view("b", ["a/b"]), view("a", ["a/b/c"]))).toBe(true);
    expect(dep(view("b", ["a/bc"]), view("a", ["a/b"]))).toBe(false);
    expect(dep(view("b", ["x"]), view("a", ["y"]))).toBe(false);
  });

  it("preconditions, the intent row, explicit links, barriers and fences", () => {
    expect(dep(view("b", ["x"], [{ key: "y" }]), view("a", ["y"]))).toBe(true);
    expect(dep(view("b", ["x"], [{ prefix: "p/" }]), view("a", ["p/q"]))).toBe(true);
    expect(dep(view("b", ["x"], [{ prefix: "p/" }]), view("a", ["z/q"]))).toBe(false);
    expect(dep(view("b", ["x"], [{ key: "k", after: "a" }]), view("a", ["y"]))).toBe(true);
    expect(dep(view("b", ["x"], [{ key: "abpc.intent/a" }]), view("a", ["y"]))).toBe(true);
    expect(dep(view("b", ["x"]), view("a", ["y"]), ["a"])).toBe(true);
    expect(dep(view("b", ["x"], [], "Bar"), view("a", ["y"]))).toBe(true);
    expect(dep(view("b", ["x"]), view("a", ["y"], [], "Bar"))).toBe(true);
    expect(dep(view("b", ["status/1"]), view("a", ["claim/1"]))).toBe(true);
    expect(dep(view("b", ["status/2"]), view("a", ["claim/1"]))).toBe(false);
  });
});

describe("the manifests of the engine", () => {
  it("StaticManifests answers the fold's Rules and the client's own: op types, the owning key of a fenced key, roots", () => {
    const m = new StaticManifests(MANIFEST);
    const v = m.get(1);
    expect(m.template(1, "issues/i1")).toMatchObject({ rule: "lww", entity: true });
    expect(m.template(1, "issues/i1/title")).toMatchObject({ rule: "lww", entity: false, parent: "issues/{id}" });
    expect(m.template(1, "nowhere/x")).toBeUndefined();
    expect(v?.opType("Approve")).toMatchObject({ lane: "GATED", optimistic: true, barrier: false, offlineMode: "queue_recheck", spend: false });
    expect(v?.opType("Pay")?.spend).toBe(true);
    expect(v?.opType("Quick")?.offlineMode).toBe("fail_fast");
    expect(v?.fencedBy("issues/i7/status")).toBe("claims/i7");
    expect(v?.fencedBy("issues/i7/title")).toBeUndefined();
    expect([...(v?.crdtRoots("docs/d1") ?? [])]).toEqual([["text", "text"]]);
    expect(v?.templatePattern("issues/i7/title")).toBe("issues/{id}/title");
  });
});

describe("14 [CLI-54], [CLI-55] the offline horizon and the server-time offset are kept with the stored state", () => {
  it("takes max_offline_seconds from the load and keeps it, so that it applies while offline after a restart", async () => {
    const r = await rig({ serverOptions: { limits: { maxOfflineSeconds: 86_400 } } });
    expect(r.engine.limits.maxOfflineMs).toBe(86_400_000);
    await r.engine.stop();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 3, start: false });
    expect(r2.engine.limits.maxOfflineMs).toBe(86_400_000); // before any load of this run
  });

  it("estimates server time from server data and the monotonic clock, persists the offset, and uses it after a restart until the first heartbeat", async () => {
    const device = new ManualClock(Date.UTC(2026, 0, 1, 12, 0, 0));
    const serverClock = new ManualClock(Date.UTC(2026, 0, 1, 12, 0, 0) + 600_000); // the server is ten minutes ahead
    const server = new FakeServer({ manifests: new StaticManifests(MANIFEST), clock: serverClock, heartbeatMs: 15_000 });
    server.createKeyspaceNow(KS);
    const storage = new MemoryStorage();
    const make = async (seed: number): Promise<Engine> => {
      const e = await Engine.create({ appId: "app", principal: { issuer: "https://issuer.test", sub: "ana", deviceId: "d" }, transport: server.transport(server.principal("ana")), manifests: new StaticManifests(MANIFEST), storage, clock: device, random: new SeededRandom(seed) });
      await e.addKeyspace(KS);
      e.start();
      await e.settle();
      return e;
    };
    const e1 = await make(1);
    await device.advance(15_000);
    await serverClock.advance(15_000); // a heartbeat with server_time
    await e1.settle();
    expect(Math.abs(e1.serverNow() - serverClock.now())).toBeLessThan(2000);
    expect((await storage.load()).meta["serverOffsetMs"]).toBeCloseTo(600_000, -4);
    await device.advance(60_000); // the device clock moves; so does the estimate, by the monotonic clock
    expect(Math.abs(e1.serverNow() - (serverClock.now() + 60_000))).toBeLessThan(2000);
    await serverClock.advance(60_000);
    await e1.stop();
    // after a restart: the wall clock plus the persisted offset, until the new run has seen server time
    const e2 = await Engine.create({ appId: "app", principal: { issuer: "https://issuer.test", sub: "ana", deviceId: "d" }, transport: server.transport(server.principal("ana")), manifests: new StaticManifests(MANIFEST), storage, clock: device, random: new SeededRandom(2) });
    expect(Math.abs(e2.serverNow() - (device.now() + 600_000))).toBeLessThan(2000);
  });
});
