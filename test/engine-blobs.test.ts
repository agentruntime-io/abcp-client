// C6, blobs: 09 §9.9 ([READ-84] to [READ-94], [READ-102]), 14 [CLI-43], [CLI-44], [CLI-66], [CLI-76], 16 [SEC-7], A.7, A.3. Against the fake server and its blob store (src/fake/blobs.ts), which answer as
// `server/internal/blob` and `store/blob.go` do on the points that a client's behaviour depends on.

import { create, toBinary } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import {
  BlobError,
  BytesTransport,
  digestSha256,
  isBlobId,
  LocalRefusal,
  MAX_UPLOAD_ROUNDS,
  newBlobId,
  ProtocolViolation,
  set,
  StaticManifests,
} from "../src/engine/index.js";
import { FAKE_CHECKSUM_HEADER, FakeServer, ManualClock, reject as mk, SeededRandom } from "../src/fake/index.js";
import { CreateUploadRequestSchema, CreateUploadResponseSchema, MutationResultSchema } from "../src/gen/abpc/v2/sync_pb.js";
import { ChangeKind, Code, KeyChangeSchema, RetryReason, RetrySchema, SystemKind } from "../src/gen/abpc/v2/types_pb.js";
import { sha256 } from "../src/rules/crypto.js";
import { goOffline, goOnline, KS, MANIFEST, ofType, types } from "./helpers/engine.js";
import { brig, bytesOf, drive } from "./helpers/presence.js";
import { FakeRaw } from "./helpers/raw.js";
import { settleAll, tabsRig } from "./helpers/tabs.js";

const calls = (s: { calls: { method: string }[] }, m: string): number => s.calls.filter((c) => c.method === m).length;
const putCount = (s: { blobs: { http: { method: string }[] } }): number => s.blobs.http.filter((h) => h.method === "PUT").length;
const getCount = (s: { blobs: { http: { method: string }[] } }): number => s.blobs.http.filter((h) => h.method === "GET").length;
const submitWith = (r: { engine: { submit: (a: never) => Promise<{ mutationId: string }> } }, refs: string[], key = "settings/a"): Promise<{ mutationId: string }> =>
  r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set(key, 1)], blobRefs: refs } as never);
const committed = (r: { events: { type: string; mutationId?: string }[] }, id: string): boolean => r.events.some((e) => e.type === "committed" && e.mutationId === id);

describe("09 [READ-84]: the client generates the blob id", () => {
  it("a blob id is 22 characters of base64url from the random source, and two blobs of the same bytes have two ids", async () => {
    const rnd = new SeededRandom(3);
    const ids = new Set(Array.from({ length: 50 }, () => newBlobId(rnd)));
    expect(ids.size).toBe(50);
    for (const id of ids) {
      expect(id).toMatch(/^[A-Za-z0-9_-]{22}$/); // 128 bits in 22 characters
      expect(isBlobId(id)).toBe(true);
    }
    const r = await brig();
    const a = await r.engine.stageBlob(KS, bytesOf(10));
    const b = await r.engine.stageBlob(KS, bytesOf(10));
    expect(a.blobId).not.toBe(b.blobId); // never derived from the content
    expect(a.blobId).toMatch(/^[A-Za-z0-9_-]{22}$/);
  });

  it("the syntax of 03 Table 3.1: [A-Za-z0-9_-]{21,64}; an id of the caller that breaks it is refused, and so is a blob_refs entry of an action", async () => {
    expect(isBlobId("a".repeat(20))).toBe(false);
    expect(isBlobId("a".repeat(21))).toBe(true);
    expect(isBlobId("a".repeat(64))).toBe(true);
    expect(isBlobId("a".repeat(65))).toBe(false);
    expect(isBlobId(`${"a".repeat(21)}/`)).toBe(false);
    const r = await brig();
    await expect(r.engine.stageBlob(KS, bytesOf(3), { blobId: "short" })).rejects.toBeInstanceOf(LocalRefusal);
    await expect(submitWith(r, ["not a blob id"])).rejects.toBeInstanceOf(LocalRefusal); // 03 [ID-23]: INVALID_ENVELOPE on the server, refused here (14 [CLI-19])
    await expect(submitWith(r, Array.from({ length: 1001 }, (_, i) => `${"a".repeat(20)}${String(i).padStart(4, "0")}`))).rejects.toBeInstanceOf(LocalRefusal);
  });
});

describe("04 Table 4.10a, 07 [MUT-75]: the blob limit", () => {
  it("max_blob_bytes has no floor: a 0 is a protocol error and the default (100 MiB) applies; the value in force is the server's", async () => {
    const r = await brig({ limits: { maxBlobBytes: 0 } });
    expect(r.engine.limits.maxBlobBytes).toBe(100 * 1024 * 1024);
    expect(ofType(r, "protocol-error").map((e) => e.what)).toContain("AppLimits.max_blob_bytes");
    const c = await brig({ limits: { maxBlobBytes: 5000 } });
    expect(c.engine.limits.maxBlobBytes).toBe(5000);
    expect(ofType(c, "protocol-error")).toEqual([]);
  });
});

describe("09 [READ-86], [READ-87], [READ-88]: the upload", () => {
  it("CreateUpload, the PUT with exactly the headers of the answer, CommitBlob; the row holds the declared digest, size and content type", async () => {
    const r = await brig();
    const bytes = bytesOf(5000);
    const ref = await drive(r, r.engine.uploadBlob(KS, bytes, { contentType: "image/png" }));
    expect(calls(r.server, "createUpload")).toBe(1);
    expect(calls(r.server, "commitBlob")).toBe(1);
    const puts = r.server.blobs.http.filter((h) => h.method === "PUT");
    expect(puts).toHaveLength(1);
    expect(puts[0]?.headers[FAKE_CHECKSUM_HEADER]).toBe(Buffer.from(sha256(bytes)).toString("base64")); // the headers of `upload_headers`, unchanged
    expect(Object.keys(puts[0]?.headers ?? {})).toEqual([FAKE_CHECKSUM_HEADER]);
    expect(puts[0]?.octets).toBe(5000);
    expect(Buffer.from(ref.sha256).equals(Buffer.from(sha256(bytes)))).toBe(true);
    const row = r.server.blobs.row(KS, ref.blobId);
    expect(row).toMatchObject({ state: "committed", size: 5000n, contentType: "image/png", uploader: "ana" });
    expect(Buffer.from(r.server.blobs.object(KS, ref.blobId) as Uint8Array).equals(Buffer.from(bytes))).toBe(true);
    // the request declared them
    const req = r.server.calls.find((c) => c.method === "createUpload")?.request as { sha256: Uint8Array; size: bigint; contentType: string; blobId: string };
    expect(req).toMatchObject({ size: 5000n, contentType: "image/png", blobId: ref.blobId });
    expect(Buffer.from(req.sha256).equals(Buffer.from(sha256(bytes)))).toBe(true);
  });

  it("a blob of 0 octets is valid, and an empty content type is stored as application/octet-stream", async () => {
    const r = await brig();
    const ref = await drive(r, r.engine.uploadBlob(KS, new Uint8Array(0)));
    expect(r.server.blobs.row(KS, ref.blobId)).toMatchObject({ state: "committed", size: 0n, contentType: "application/octet-stream" });
    const got = await drive(r, r.engine.getBlob(KS, ref.blobId));
    expect(got.bytes.length).toBe(0);
    expect(got.declaredContentType).toBe("application/octet-stream");
  });

  it("a size over limits.max_blob_bytes is refused before a call is made; so is a content type that is not printable ASCII", async () => {
    const r = await brig({ limits: { maxBlobBytes: 1000 } });
    expect(r.engine.limits.maxBlobBytes).toBe(1000);
    await expect(r.engine.uploadBlob(KS, bytesOf(1001))).rejects.toBeInstanceOf(LocalRefusal);
    await expect(r.engine.uploadBlob(KS, bytesOf(10), { contentType: "text/plain\n" })).rejects.toBeInstanceOf(LocalRefusal);
    await expect(r.engine.uploadBlob(KS, bytesOf(10), { contentType: "x".repeat(256) })).rejects.toBeInstanceOf(LocalRefusal);
    expect(calls(r.server, "createUpload")).toBe(0);
    await drive(r, r.engine.uploadBlob(KS, bytesOf(1000))); // the limit itself
  });

  it("a blob that is committed with the same digest and size answers already_committed: no PUT", async () => {
    const r = await brig();
    const bytes = bytesOf(300);
    const first = await drive(r, r.engine.uploadBlob(KS, bytes, { blobId: "a".repeat(22) }));
    const again = await drive(r, r.engine.uploadBlob(KS, bytes, { blobId: first.blobId }));
    expect(again.blobId).toBe(first.blobId);
    expect(calls(r.server, "createUpload")).toBe(2);
    expect(putCount(r.server)).toBe(1);
    expect(calls(r.server, "commitBlob")).toBe(1);
  });

  it("an id that names another digest or size is IDEMPOTENCY_KEY_REUSED: final, never resent; a quota is final too", async () => {
    const r = await brig();
    await drive(r, r.engine.uploadBlob(KS, bytesOf(300), { blobId: "b".repeat(22) }));
    const e1 = await drive(r, r.engine.uploadBlob(KS, bytesOf(301), { blobId: "b".repeat(22) })).catch((e: unknown) => e);
    expect(e1).toBeInstanceOf(BlobError);
    expect(e1).toMatchObject({ code: "IDEMPOTENCY_KEY_REUSED", status: "AlreadyExists" });
    r.server.inject("createUpload", { kind: "status", status: "ResourceExhausted", reject: mk(Code.QUOTA_EXCEEDED, { details: { quota: "bytes_per_app" } }) });
    const before = calls(r.server, "createUpload");
    const e2 = await drive(r, r.engine.uploadBlob(KS, bytesOf(20))).catch((e: unknown) => e);
    expect(e2).toMatchObject({ code: "QUOTA_EXCEEDED", status: "ResourceExhausted" });
    expect(calls(r.server, "createUpload")).toBe(before + 1);
    // the server's own PAYLOAD_TOO_LARGE is final too
    r.server.inject("createUpload", { kind: "status", status: "InvalidArgument", reject: mk(Code.PAYLOAD_TOO_LARGE) });
    await expect(drive(r, r.engine.uploadBlob(KS, bytesOf(20)))).rejects.toMatchObject({ code: "PAYLOAD_TOO_LARGE" });
  });

  it("a URL that the store refuses as expired (403): CreateUpload again gives a new one (04 Table 4.11)", async () => {
    const r = await brig();
    r.server.blobs.failPuts = { times: 1, status: 403 };
    const ref = await drive(r, r.engine.uploadBlob(KS, bytesOf(100)));
    expect(calls(r.server, "createUpload")).toBe(2);
    expect(putCount(r.server)).toBe(2);
    expect(r.server.blobs.row(KS, ref.blobId)?.state).toBe("committed");
  });

  it("a URL that has expired by the clock: the same", async () => {
    const r = await brig();
    const staged = await r.engine.stageBlob(KS, bytesOf(100));
    // the signed URL is valid for an hour: the first one is spent by a client that waited
    const up = r.server.blobs.createUpload("ana", create(CreateUploadRequestSchema, { keyspaceId: KS, blobId: staged.blobId, sha256: staged.sha256, size: 100n }));
    await r.clock.advance(3_700_000);
    expect(r.server.blobs.blobHttp().put(up.uploadUrl, up.uploadHeaders, bytesOf(100))).resolves.toEqual({ status: 403 });
    await drive(r, r.engine.uploadBlob(KS, bytesOf(100), { blobId: staged.blobId }));
    expect(r.server.blobs.row(KS, staged.blobId)?.state).toBe("committed");
  });

  it("a PUT that fails in a way a resend may mend (503, no answer) is sent again to the same URL, with the client backoff (14 [CLI-43])", async () => {
    const r = await brig();
    r.random.fixed = 0.5;
    r.server.blobs.failPuts = { times: 2, status: 503 };
    const waits: string[] = [];
    const ref = await drive(r, r.engine.uploadBlob(KS, bytesOf(100), { onWait: (w) => waits.push(`${w.reason}:${w.delayMs}`) }));
    expect(calls(r.server, "createUpload")).toBe(1);
    expect(putCount(r.server)).toBe(3);
    expect(waits).toEqual(["PUT_503:500", "PUT_503:1000"]); // 0.5 of 1 s, 0.5 of 2 s
    expect(r.server.blobs.row(KS, ref.blobId)?.state).toBe("committed");
  });

  it("a PUT that the store refuses for its length or checksum (400) is final: nothing mends it by resending (09 [READ-87])", async () => {
    const r = await brig();
    r.server.blobs.failPuts = { times: 1, status: 400 };
    await expect(drive(r, r.engine.uploadBlob(KS, bytesOf(100)))).rejects.toMatchObject({ code: "UPLOAD_REFUSED" });
    expect(putCount(r.server)).toBe(1);
  });

  it("CommitBlob answers BLOB_MISSING (the object is missing or differs): the client uploads again, then commits (09 [READ-88])", async () => {
    const r = await brig();
    r.server.inject("commitBlob", { kind: "status", status: "FailedPrecondition", reject: mk(Code.BLOB_MISSING) });
    const ref = await drive(r, r.engine.uploadBlob(KS, bytesOf(100)));
    expect(calls(r.server, "createUpload")).toBe(2);
    expect(putCount(r.server)).toBe(2);
    expect(calls(r.server, "commitBlob")).toBe(2);
    expect(r.server.blobs.row(KS, ref.blobId)?.state).toBe("committed");
  });

  it(`a store that never keeps the object: ${MAX_UPLOAD_ROUNDS} rounds, then UPLOAD_FAILED (SPEC-QUESTION: no bound in the text)`, async () => {
    const r = await brig();
    r.server.inject("commitBlob", { kind: "status", status: "FailedPrecondition", reject: mk(Code.BLOB_MISSING) }, { times: 100 });
    await expect(drive(r, r.engine.uploadBlob(KS, bytesOf(100)))).rejects.toMatchObject({ code: "UPLOAD_FAILED" });
    expect(calls(r.server, "createUpload")).toBe(MAX_UPLOAD_ROUNDS);
  });
});

describe("A.7, 14 [CLI-43]: a blob call is resent as its status says", () => {
  it("Unavailable and an unknown outcome are resent unchanged with the client backoff", async () => {
    const r = await brig();
    r.random.fixed = 0.5;
    r.server.inject("createUpload", { kind: "status", status: "Unavailable" }, { times: 2 });
    const waits: number[] = [];
    await drive(r, r.engine.uploadBlob(KS, bytesOf(100), { onWait: (w) => waits.push(w.delayMs) }));
    expect(calls(r.server, "createUpload")).toBe(3);
    expect(waits).toEqual([500, 1000]);
  });

  it("Retry{RATE_LIMITED} waits the larger of after_ms and the backoff", async () => {
    const r = await brig();
    r.random.fixed = 0;
    r.server.inject("createUpload", { kind: "status", status: "ResourceExhausted", retry: create(RetrySchema, { reason: RetryReason.RATE_LIMITED, afterMs: 7000 }) });
    const waits: number[] = [];
    await drive(r, r.engine.uploadBlob(KS, bytesOf(10), { onWait: (w) => waits.push(w.delayMs) }), 1000);
    expect(waits).toEqual([7000]);
  });

  it("Unauthenticated: the token is refreshed and the call resent once; a second Unauthenticated stops and reports", async () => {
    let refreshes = 0;
    const r = await brig({ cfg: { tokens: { refresh: () => Promise.resolve(++refreshes > 0) } as never } });
    r.server.inject("createUpload", { kind: "status", status: "Unauthenticated" });
    await drive(r, r.engine.uploadBlob(KS, bytesOf(10)));
    expect(refreshes).toBe(1);
    r.server.inject("createUpload", { kind: "status", status: "Unauthenticated" }, { times: 2 });
    await expect(drive(r, r.engine.uploadBlob(KS, bytesOf(10)))).rejects.toMatchObject({ code: "UNAUTHENTICATED" });
    expect(refreshes).toBe(2);
  });

  it("PermissionDenied and the other rejects are final", async () => {
    const r = await brig();
    r.server.denyRead("ana", KS, { drop: false });
    await expect(drive(r, r.engine.uploadBlob(KS, bytesOf(10)))).rejects.toMatchObject({ code: "PERMISSION_DENIED", status: "PermissionDenied" });
    expect(calls(r.server, "createUpload")).toBe(1);
    const r2 = await brig();
    r2.server.archive(KS);
    await expect(drive(r2, r2.engine.uploadBlob(KS, bytesOf(10)))).rejects.toMatchObject({ code: "KEYSPACE_ARCHIVED" }); // an archived keyspace takes no upload (09 [READ-86])
    await expect(drive(r2, r2.engine.uploadBlob("no-such-keyspace", bytesOf(10)))).rejects.toMatchObject({ code: "KEYSPACE_NOT_FOUND" });
  });

  it("a call stops when the app aborts it, and when the engine stops", async () => {
    const r = await brig();
    r.server.inject("createUpload", { kind: "status", status: "Unavailable" }, { times: 100 });
    const ac = new AbortController();
    const p = r.engine.uploadBlob(KS, bytesOf(10), { signal: ac.signal });
    await r.engine.settle();
    ac.abort();
    await expect(drive(r, p)).rejects.toMatchObject({ code: "ABORTED" });
    r.server.inject("createUpload", { kind: "status", status: "Unavailable" }, { times: 100 });
    const p2 = r.engine.uploadBlob(KS, bytesOf(10)).catch((e: unknown) => e);
    await r.engine.settle();
    await r.engine.stop();
    expect(await drive(r, p2)).toMatchObject({ code: "ABORTED" });
  });
});

describe("09 [READ-89], 14 [CLI-44]: an action that names a blob that is not committed", () => {
  it("BLOB_MISSING (class RETRY): the engine uploads the same bytes, then resends the identical action; the bytes go when the action is confirmed (09 [READ-94])", async () => {
    const r = await brig();
    r.random.fixed = 0;
    await goOffline(r);
    const ref = await r.engine.stageBlob(KS, bytesOf(200)); // offline: nothing is sent
    expect(calls(r.server, "createUpload")).toBe(0);
    const { mutationId } = await submitWith(r, [ref.blobId]);
    expect(await r.engine.stagedBlobs()).toEqual([{ keyspaceId: KS, blobId: ref.blobId, size: 200 }]);
    await goOnline(r);
    await r.clock.advance(5000);
    await r.engine.settle();
    expect(committed(r, mutationId)).toBe(true);
    expect(r.server.blobs.row(KS, ref.blobId)?.state).toBe("committed");
    expect(calls(r.server, "createUpload")).toBe(1);
    // the action went twice, unchanged: the same mutation id and the same blob_refs
    const sent = r.server.calls.filter((c) => c.method === "mutate").flatMap((c) => (c.request as { mutations: { mutationId: string; blobRefs: string[] }[] }).mutations);
    expect(sent.filter((m) => m.mutationId === mutationId).map((m) => m.blobRefs)).toEqual([[ref.blobId], [ref.blobId]]);
    expect(r.engine.status().holds).toEqual([]); // a Retry holds no lane, keyspace or action
    // 09 [READ-94]: confirmed: the bytes that the action kept are gone
    expect(await r.engine.stagedBlobs()).toEqual([]);
    expect(calls(r.server, "deleteBlob")).toBe(0); // the engine never deletes server blobs
  });

  it("BLOB_PENDING: the upload is completed, and the action is resent after the larger of after_ms and the backoff", async () => {
    const r = await brig();
    r.random.fixed = 0; // the backoff draws 0: the wait is after_ms, 1,000 ms
    await goOffline(r);
    const ref = await r.engine.stageBlob(KS, bytesOf(200));
    // a CreateUpload that no PUT followed: the row is pending
    r.server.blobs.createUpload("ana", create(CreateUploadRequestSchema, { keyspaceId: KS, blobId: ref.blobId, sha256: ref.sha256, size: 200n }));
    const { mutationId } = await submitWith(r, [ref.blobId]);
    await r.clock.advance(1); // the engine subscribes again and sends the action: the reconnect has no wait (the backoff draws 0)
    await r.engine.settle();
    const mutates = (): number => calls(r.server, "mutate");
    expect(mutates()).toBe(1);
    expect(r.server.blobs.row(KS, ref.blobId)?.state).toBe("committed"); // completed by the engine
    await r.clock.advance(900);
    expect(mutates()).toBe(1);
    await r.clock.advance(200);
    await r.engine.settle();
    expect(mutates()).toBe(2);
    expect(committed(r, mutationId)).toBe(true);
    expect(ofType(r, "held")).toEqual([]);
  });

  it("BLOB_MISSING for a blob that the engine holds no bytes of: resent with the backoff, no hold, nothing dead-lettered, and no server blob is deleted", async () => {
    const r = await brig();
    r.random.fixed = 0.5;
    const { mutationId } = await submitWith(r, ["z".repeat(22)]);
    await r.engine.settle();
    expect(calls(r.server, "mutate")).toBe(1);
    await r.clock.advance(500);
    await r.engine.settle();
    expect(calls(r.server, "mutate")).toBe(2);
    await r.clock.advance(1000);
    await r.engine.settle();
    expect(calls(r.server, "mutate")).toBe(3);
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([mutationId]);
    expect(r.engine.deadLetterEntries()).toEqual([]);
    expect(r.engine.status().holds).toEqual([]);
    expect(calls(r.server, "deleteBlob")).toBe(0);
  });

  it("BLOB_DELETED (class DEAD_LETTER): the action is dead-lettered", async () => {
    const r = await brig();
    const ref = await drive(r, r.engine.uploadBlob(KS, bytesOf(50)));
    await drive(r, r.engine.deleteBlob(KS, ref.blobId, "no longer needed"));
    const { mutationId } = await submitWith(r, [ref.blobId]);
    await r.engine.settle();
    expect(r.engine.deadLetterEntries().map((d) => [d.mutationId, d.reason])).toEqual([[mutationId, "BLOB_DELETED"]]);
    expect(types(r, mutationId)).toContain("dead-lettered");
  });

  it("a withheld blob never stops an action: it is accepted like any committed blob, and no lane is held for it (14 [CLI-44])", async () => {
    const r = await brig();
    const ref = await drive(r, r.engine.uploadBlob(KS, bytesOf(50)));
    r.server.blobs.withhold(KS, ref.blobId);
    const first = await submitWith(r, [ref.blobId], "settings/a");
    const second = await submitWith(r, [], "settings/b");
    await r.engine.settle();
    expect(committed(r, first.mutationId)).toBe(true);
    expect(committed(r, second.mutationId)).toBe(true);
    expect(r.engine.status().holds).toEqual([]);
  });

  it("the bytes stay while the action is not confirmed: an action that is dead-lettered keeps them (09 [READ-94])", async () => {
    const r = await brig();
    await goOffline(r);
    const ref = await r.engine.stageBlob(KS, bytesOf(30));
    const { mutationId } = await submitWith(r, [ref.blobId]);
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.mutationId === mutationId ? create(MutationResultSchema, { mutationId: m.mutationId, outcome: { case: "rejected", value: mk(Code.SCHEMA_INVALID) } }) : undefined) });
    await goOnline(r);
    await r.engine.settle();
    expect(r.engine.deadLetterEntries()).toHaveLength(1);
    expect(await r.engine.stagedBlobs()).toHaveLength(1);
    await r.engine.releaseBlob(KS, ref.blobId);
    expect(await r.engine.stagedBlobs()).toEqual([]);
  });
});


describe("09 [READ-90], 16 [SEC-7]: GetBlob and its download", () => {
  it("downloads the bytes, checks them against the digest and the size, caches them, and reads the declared content type", async () => {
    const r = await brig();
    const bytes = bytesOf(4096);
    const ref = await drive(r, r.engine.uploadBlob(KS, bytes, { contentType: "application/pdf" }));
    // another engine reads it
    const b = await brig({ server: r.server, clock: r.clock, manifests: r.manifests, sub: "bob", seed: 31 });
    const got = await drive(b, b.engine.getBlob(KS, ref.blobId));
    expect(Buffer.from(got.bytes).equals(Buffer.from(bytes))).toBe(true);
    expect(Buffer.from(got.sha256).equals(Buffer.from(await digestSha256(bytes)))).toBe(true);
    expect(got).toMatchObject({ size: 4096, declaredContentType: "application/pdf", blobId: ref.blobId, keyspaceId: KS });
    expect(b.engine.blobCached(KS, ref.blobId)).toBe(true);
    expect(getCount(r.server)).toBe(1);
    const again = await drive(b, b.engine.getBlob(KS, ref.blobId));
    expect(again.bytes).toBe(got.bytes); // the cache
    expect(getCount(r.server)).toBe(1);
    expect(calls(r.server, "getBlob")).toBe(1);
  });

  it("a pending blob answers Retry{BLOB_PENDING}: the call asks again after the larger of after_ms and the backoff, and holds nothing", async () => {
    const r = await brig();
    r.random.fixed = 0;
    const bytes = bytesOf(100);
    const sha = await digestSha256(bytes);
    const id = "p".repeat(22);
    r.server.blobs.createUpload("bob", create(CreateUploadRequestSchema, { keyspaceId: KS, blobId: id, sha256: sha, size: 100n }));
    const waits: string[] = [];
    const p = r.engine.getBlob(KS, id, { onWait: (w) => waits.push(`${w.reason}:${w.delayMs}`) });
    await r.engine.settle();
    await r.clock.advance(1000);
    await r.clock.advance(1000);
    expect(waits.slice(0, 2)).toEqual(["BLOB_PENDING:1000", "BLOB_PENDING:1000"]);
    // the upload completes
    const up = r.server.blobs.createUpload("bob", create(CreateUploadRequestSchema, { keyspaceId: KS, blobId: id, sha256: sha, size: 100n }));
    await r.server.blobs.blobHttp().put(up.uploadUrl, up.uploadHeaders, bytes);
    r.server.blobs.commitBlob({ keyspaceId: KS, blobId: id } as never);
    const got = await drive(r, p);
    expect(got.size).toBe(100);
    expect(r.engine.status().holds).toEqual([]);
  });

  it("a withheld blob answers Retry{BLOB_WITHHELD}: the call waits with no bound of its own, until the app stops it or the blob is served again", async () => {
    const r = await brig();
    const ref = await drive(r, r.engine.uploadBlob(KS, bytesOf(100)));
    const b = await brig({ server: r.server, clock: r.clock, manifests: r.manifests, sub: "bob", seed: 31 });
    r.server.blobs.withhold(KS, ref.blobId);
    const waits: string[] = [];
    const ac = new AbortController();
    const p = b.engine.getBlob(KS, ref.blobId, { signal: ac.signal, onWait: (w) => waits.push(w.reason) });
    for (let i = 0; i < 5; i++) {
      await b.engine.settle();
      await b.clock.advance(60_000);
    }
    expect(waits.length).toBeGreaterThanOrEqual(4);
    expect(new Set(waits)).toEqual(new Set(["BLOB_WITHHELD"]));
    expect(b.engine.status().holds).toEqual([]); // a lane, a keyspace and an action are not held for it
    r.server.blobs.withhold(KS, ref.blobId, false); // every blob step has judged it: it is served again
    const got = await drive(b, p);
    expect(got.size).toBe(100);
    // and a call that the app stops
    const c = await brig({ server: r.server, clock: r.clock, manifests: r.manifests, sub: "cat", seed: 41 });
    r.server.blobs.withhold(KS, ref.blobId);
    const ac2 = new AbortController();
    const p2 = c.engine.getBlob(KS, ref.blobId, { signal: ac2.signal }).catch((e: unknown) => e);
    await c.engine.settle();
    await c.clock.advance(5000);
    ac2.abort();
    expect(await drive(c, p2)).toMatchObject({ code: "ABORTED" });
    void ac;
  });

  it("BLOB_MISSING and BLOB_DELETED (NotFound) are final: one call each", async () => {
    const r = await brig();
    await expect(drive(r, r.engine.getBlob(KS, "m".repeat(22)))).rejects.toMatchObject({ code: "BLOB_MISSING", status: "NotFound" });
    const ref = await drive(r, r.engine.uploadBlob(KS, bytesOf(10)));
    await drive(r, r.engine.deleteBlob(KS, ref.blobId, "done"));
    await r.engine.releaseBlob(KS, ref.blobId);
    await expect(drive(r, r.engine.getBlob(KS, ref.blobId))).rejects.toMatchObject({ code: "BLOB_DELETED", status: "NotFound" });
    expect(calls(r.server, "getBlob")).toBe(2);
  });

  it("16 [SEC-7]: bytes that differ from the digest are refused and never cached; so are bytes longer than the declared size", async () => {
    const r = await brig();
    const ref = await drive(r, r.engine.uploadBlob(KS, bytesOf(500)));
    const b = await brig({ server: r.server, clock: r.clock, manifests: r.manifests, sub: "bob", seed: 31 });
    r.server.blobs.corruptNextGet = true;
    await expect(drive(b, b.engine.getBlob(KS, ref.blobId))).rejects.toMatchObject({ code: "CHECKSUM_MISMATCH" });
    expect(b.engine.blobCached(KS, ref.blobId)).toBe(false);
    r.server.blobs.overlongNextGet = true;
    await expect(drive(b, b.engine.getBlob(KS, ref.blobId))).rejects.toMatchObject({ code: "CHECKSUM_MISMATCH" });
    expect(b.engine.blobCached(KS, ref.blobId)).toBe(false);
    const ok = await drive(b, b.engine.getBlob(KS, ref.blobId)); // the next one is whole
    expect(ok.size).toBe(500);
  });

  it("a download URL that has expired (403) or a blob that went (404): GetBlob is asked again, and says which; a GET that fails is sent again with the backoff", async () => {
    const r = await brig();
    const ref = await drive(r, r.engine.uploadBlob(KS, bytesOf(500)));
    const b = await brig({ server: r.server, clock: r.clock, manifests: r.manifests, sub: "bob", seed: 31 });
    r.server.blobs.failGets = { times: 1, status: 403 };
    await drive(b, b.engine.getBlob(KS, ref.blobId));
    expect(calls(r.server, "getBlob")).toBe(2);
    const c = await brig({ server: r.server, clock: r.clock, manifests: r.manifests, sub: "cat", seed: 41 });
    c.random.fixed = 0.5;
    r.server.blobs.failGets = { times: 2, status: 503 };
    const waits: string[] = [];
    await drive(c, c.engine.getBlob(KS, ref.blobId, { onWait: (w) => waits.push(`${w.reason}:${w.delayMs}`) }));
    expect(waits).toEqual(["GET_503:500", "GET_503:1000"]);
    expect(calls(r.server, "getBlob")).toBe(3); // one more: the 503s were sent again to the same URL
  });

  it("the bytes that this engine staged are shown without a download", async () => {
    const r = await brig();
    await goOffline(r);
    const ref = await r.engine.stageBlob(KS, bytesOf(77), { contentType: "text/plain" });
    const got = await r.engine.getBlob(KS, ref.blobId);
    expect(got.size).toBe(77);
    expect(got.declaredContentType).toBe("text/plain");
    expect(calls(r.server, "getBlob")).toBe(0);
  });
});

describe("09 [READ-91]: DeleteBlob", () => {
  it("the reason is required, at most 1,024 octets, with no U+0000: refused before a call is made", async () => {
    const r = await brig();
    const id = "d".repeat(22);
    await expect(r.engine.deleteBlob(KS, id, "")).rejects.toBeInstanceOf(LocalRefusal);
    await expect(r.engine.deleteBlob(KS, id, "x".repeat(1025))).rejects.toBeInstanceOf(LocalRefusal);
    await expect(r.engine.deleteBlob(KS, id, "a\u0000b")).rejects.toBeInstanceOf(LocalRefusal);
    await expect(r.engine.deleteBlob(KS, "nope", "why")).rejects.toBeInstanceOf(LocalRefusal);
    expect(calls(r.server, "deleteBlob")).toBe(0);
    await expect(r.engine.deleteBlob(KS, id, "é".repeat(512))).rejects.toMatchObject({ code: "PERMISSION_DENIED" }); // 1,024 octets: within; no row: no grant for a caller that is not admin
  });

  it("the uploader deletes it: the row is a tombstone, the bytes are gone, the cache and the stash drop it; a repeated delete succeeds and changes nothing", async () => {
    const r = await brig();
    const ref = await drive(r, r.engine.uploadBlob(KS, bytesOf(64)));
    await drive(r, r.engine.getBlob(KS, ref.blobId));
    expect(r.engine.blobCached(KS, ref.blobId)).toBe(false); // the stash served it
    await drive(r, r.engine.deleteBlob(KS, ref.blobId, "the user removed it"));
    expect(r.server.blobs.row(KS, ref.blobId)?.state).toBe("deleted");
    expect(r.server.blobs.object(KS, ref.blobId)).toBeUndefined();
    expect(await r.engine.stagedBlobs()).toEqual([]);
    const records = r.server.records(KS).filter((x) => x.system?.kind === SystemKind.BLOB_DELETED);
    expect(records).toHaveLength(1);
    await drive(r, r.engine.deleteBlob(KS, ref.blobId, "again"));
    expect(r.server.records(KS).filter((x) => x.system?.kind === SystemKind.BLOB_DELETED)).toHaveLength(1);
  });

  it("an id with no row: PermissionDenied to a caller without admin, BLOB_MISSING to an admin", async () => {
    const r = await brig();
    await expect(drive(r, r.engine.deleteBlob(KS, "n".repeat(22), "x"))).rejects.toMatchObject({ code: "PERMISSION_DENIED" });
    r.server.blobs.admins.add("ana");
    await expect(drive(r, r.engine.deleteBlob(KS, "n".repeat(22), "x"))).rejects.toMatchObject({ code: "BLOB_MISSING", status: "NotFound" });
  });

  it("a withheld blob answers Retry{BLOB_WITHHELD}: the call waits; ADMIN_REFUSED under a legal hold or a retention floor is final, with its reason", async () => {
    const r = await brig();
    const ref = await drive(r, r.engine.uploadBlob(KS, bytesOf(64)));
    r.server.blobs.withhold(KS, ref.blobId);
    const waits: string[] = [];
    const p = r.engine.deleteBlob(KS, ref.blobId, "why", { onWait: (w) => waits.push(w.reason) });
    await r.engine.settle();
    await r.clock.advance(10_000);
    expect(waits.length).toBeGreaterThan(0);
    expect(new Set(waits)).toEqual(new Set(["BLOB_WITHHELD"]));
    r.server.blobs.withhold(KS, ref.blobId, false);
    await drive(r, p);
    expect(r.server.blobs.row(KS, ref.blobId)?.state).toBe("deleted");
    r.server.inject("deleteBlob", { kind: "status", status: "FailedPrecondition", reject: mk(Code.ADMIN_REFUSED, { details: { reason: "LEGAL_HOLD" } }) });
    const e = await drive(r, r.engine.deleteBlob(KS, ref.blobId, "x")).catch((x: unknown) => x);
    expect(e).toBeInstanceOf(BlobError);
    expect(e).toMatchObject({ code: "ADMIN_REFUSED", reason: "LEGAL_HOLD", status: "FailedPrecondition" });
  });
});

describe("14 [CLI-76], [CLI-66]: what the log and the loads say of the cached bytes", () => {
  async function reader(): Promise<{ a: Awaited<ReturnType<typeof brig>>; b: Awaited<ReturnType<typeof brig>>; id: string }> {
    const a = await brig();
    const ref = await drive(a, a.engine.uploadBlob(KS, bytesOf(300)));
    const b = await brig({ server: a.server, clock: a.clock, manifests: a.manifests, sub: "bob", seed: 31 });
    await drive(b, b.engine.getBlob(KS, ref.blobId));
    expect(b.engine.blobCached(KS, ref.blobId)).toBe(true);
    return { a, b, id: ref.blobId };
  }

  it("a BLOB_DELETED record drops the cached bytes of that blob, and only that blob", async () => {
    const { a, b, id } = await reader();
    const other = await drive(a, a.engine.uploadBlob(KS, bytesOf(20)));
    await drive(b, b.engine.getBlob(KS, other.blobId));
    await drive(a, a.engine.deleteBlob(KS, id, "gone"));
    await b.engine.settle();
    expect(b.engine.blobCached(KS, id)).toBe(false);
    expect(b.engine.blobCached(KS, other.blobId)).toBe(true);
    await expect(drive(b, b.engine.getBlob(KS, id))).rejects.toMatchObject({ code: "BLOB_DELETED" });
  });

  it("a load with resumed = false drops the cache of the keyspace, and never the bytes that an unsent action carries (09 [READ-94])", async () => {
    const { a, b, id } = await reader();
    await goOffline(b);
    const staged = await b.engine.stageBlob(KS, bytesOf(15));
    await submitWith(b, [staged.blobId]);
    a.server.write(KS, [create(KeyChangeSchema, { key: "settings/z", kind: ChangeKind.SET, value: new TextEncoder().encode("1"), encoding: "json/v1" })]);
    a.server.prune(KS, a.server.head(KS)); // the cursor of B is below the prune floor: RESUME_TOO_OLD, a cold load (resumed = false)
    b.server.inject("mutate", { kind: "status", status: "Unavailable" }, { times: 100 });
    await goOnline(b);
    await b.engine.settle();
    expect(b.events.some((e) => e.type === "reloaded")).toBe(true);
    expect(b.engine.blobCached(KS, id)).toBe(false);
    expect((await b.engine.stagedBlobs()).map((s) => s.blobId)).toEqual([staged.blobId]);
  });

  it("14 [CLI-66]: ACCESS_REVOKED purges the blob cache with the rest of the purge list", async () => {
    const { a, b, id } = await reader();
    a.server.revokeKeyspaces(KS);
    await b.engine.settle();
    expect(b.engine.blobCached(KS, id)).toBe(false);
    expect(b.engine.blobCacheStats()).toEqual({ blobs: 0, bytes: 0 });
  });
});


describe("02 [ARC-61], 16 [SEC-1]: what the server sends in the blob calls is decoded strictly and bounded", () => {
  const upload = (n: number, extra: Record<string, string> = {}): Uint8Array => {
    const headers: Record<string, string> = { ...extra };
    for (let i = 0; i < n; i++) headers[`h${i}`] = "v";
    return toBinary(CreateUploadResponseSchema, create(CreateUploadResponseSchema, { uploadUrl: "https://x/y", uploadHeaders: headers }));
  };
  const tr = (bytes: Uint8Array): BytesTransport => new BytesTransport({ unary: () => Promise.resolve(bytes), stream: () => (async function* (): AsyncGenerator<Uint8Array> {})() });
  const req = create(CreateUploadRequestSchema, { keyspaceId: KS, blobId: "a".repeat(22) });

  it("the headers of an upload are at most 32 (the bound of counts.ts); one more is a protocol violation, before it is decoded", async () => {
    await expect(tr(upload(32)).createUpload(req)).resolves.toMatchObject({ uploadUrl: "https://x/y" });
    await expect(tr(upload(33)).createUpload(req)).rejects.toBeInstanceOf(ProtocolViolation);
  });

  it("a header that holds invalid UTF-8 is a protocol violation", async () => {
    const bytes = upload(1, { name: "value" });
    const i = bytes.findIndex((_, k) => bytes[k] === 0x76 && bytes[k + 1] === 0x61 && bytes[k + 2] === 0x6c); // "val"
    const bad = bytes.slice();
    bad[i + 1] = 0xff;
    await expect(tr(bad).createUpload(req)).rejects.toBeInstanceOf(ProtocolViolation);
  });

  it("an engine that gets a message that breaks 02 [ARC-61] in a blob call reports protocol-error and holds the keyspace (16 [SEC-3], 14 [CLI-50])", async () => {
    const clock = new ManualClock();
    const manifests = new StaticManifests(MANIFEST);
    const server = new FakeServer({ manifests, clock, heartbeatMs: 15_000 });
    server.createKeyspaceNow(KS);
    const raw = new FakeRaw(server.transport(server.principal("ana")));
    raw.tamperUnary = (method, bytes) => (method === "CreateUpload" ? upload(40) : bytes);
    const r = await brig({ server, clock, manifests, cfg: { transport: new BytesTransport(raw) } });
    await expect(drive(r, r.engine.uploadBlob(KS, bytesOf(10)))).rejects.toMatchObject({ code: "PROTOCOL_ERROR" });
    const errs = ofType(r, "protocol-error");
    expect(errs).toHaveLength(1);
    expect(errs[0]).toMatchObject({ what: "CreateUploadResponse", keyspaces: [KS] });
    expect(r.engine.status().holds.some((h) => h.code === "PROTOCOL_ERROR")).toBe(true);
  });
});

describe("14 [CLI-61]: the bytes of a blob that a follower tab stages are the flusher's", () => {
  it("a follower tab stages and uploads through the flusher, which keeps the bytes for the actions that name them until they are confirmed", async () => {
    const r = tabsRig();
    const open = (): ReturnType<typeof r.open> => r.open({ cfg: { blobs: { http: r.server.blobs.blobHttp() } } });
    const t1 = await open();
    const t2 = await open();
    await settleAll(t1.engine, t2.engine);
    expect(t2.engine.status().role).toBe("follower");
    const done = t2.engine.uploadBlob(KS, bytesOf(120));
    const ref = await drive({ clock: r.clock, engine: t1.engine } as never, done);
    expect(r.server.blobs.row(KS, ref.blobId)?.state).toBe("committed");
    expect((await t1.engine.stagedBlobs()).map((x) => x.blobId)).toEqual([ref.blobId]); // the flusher holds the bytes
    expect(await t2.engine.stagedBlobs()).toEqual([]);
    const { mutationId } = await t2.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)], blobRefs: [ref.blobId] });
    await settleAll(t1.engine, t2.engine);
    expect(t1.events.some((e) => e.type === "committed" && e.mutationId === mutationId)).toBe(true);
    expect(await t1.engine.stagedBlobs()).toEqual([]);
  });
});
