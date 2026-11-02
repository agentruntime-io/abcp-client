// C6: presence (09 §9.8) and blobs (09 §9.9) against the real Go server (server/cmd/abpc-server, M11b: `session/presence*.go`, `internal/blob`), through the engine and the real transport:
// a presence round trip between two clients, and a blob's upload, commit, get and delete, with an action that names it. The server's blob store serves its own signed URLs over HTTP
// at the address that the server listens on; the engine moves the bytes with the platform's `fetch`.

import { create } from "@bufbuild/protobuf";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { BlobError, LocalRefusal, set, type PresenceEvent } from "../../src/engine/index.js";
import { CreateUploadRequestSchema } from "../../src/gen/abpc/v2/sync_pb.js";
import { createQueries } from "../../src/query/index.js";
import { digestSha256 } from "../../src/engine/index.js";
import { dump, Env, waitFor, type TestClient } from "./harness/env.js";
import { goAvailable, MANIFEST, SKIP_REASON } from "./harness/server.js";

if (!goAvailable) console.warn(`[integration] ${SKIP_REASON}`);

const GRANTS = [{ ks: "acme/**", actions: ["read", "write:*", "keyspace:create", "presence"] }];
const dec = new TextDecoder();

/** Deterministic test bytes. */
const bytesOf = (n: number): Uint8Array => Uint8Array.from({ length: n }, (_, i) => (i * 131 + 17) & 0xff);

describe.skipIf(!goAvailable)("presence and blobs against the Go server", () => {
  let env: Env;
  beforeAll(async () => {
    env = await Env.start({ manifest: MANIFEST });
  }, 600_000);
  afterAll(async () => {
    await env?.close();
  });

  const until = (c: TestClient, cond: () => boolean, what: string, ms?: number): Promise<void> =>
    waitFor(cond, what, ms).catch((e: Error) => {
      throw new Error(`${e.message}\n${dump(c, env)}`);
    });
  const loaded = (c: TestClient, ks: string): Promise<void> => until(c, () => c.engine.cursor(ks)?.loaded === true, `${ks} loaded`);
  const committed = (c: TestClient, id: string): boolean => c.events.some((e) => e.type === "committed" && e.mutationId === id);

  /** Two clients that follow a new keyspace of the kind `room`. */
  async function pair(ks: string): Promise<{ a: TestClient; b: TestClient }> {
    const a = await env.client({ sub: "ana", grants: GRANTS });
    const b = await env.client({ sub: "bob", grants: GRANTS });
    await a.engine.createKeyspace(ks, "room");
    await loaded(a, ks);
    await b.engine.addKeyspace(ks);
    await loaded(b, ks);
    await until(a, () => a.engine.status().connection === "live" && b.engine.status().connection === "live", "both live");
    return { a, b };
  }

  const stateOf = (b: Uint8Array): unknown => JSON.parse(dec.decode(b));

  describe("presence (09 §9.8, 14 [CLI-61], [CLI-78], 02 [ARC-51] item 4)", () => {
    it("a round trip between two clients: join, snapshot, publish, upsert, left_client_ids", async () => {
      const KS = "acme/pres1";
      const TOPIC = "note/n1";
      const { a, b } = await pair(KS);
      const qa = createQueries(a.engine);
      const view = qa.presence(KS, TOPIC);
      const updates: string[] = [];
      view.watch((u) => updates.push(`${u.cause}:${u.added.length}:${u.changed.length}:${u.removed.length}`));
      await view.join();
      await b.engine.joinPresence(KS, TOPIC);
      await until(a, () => a.calls["joinPresence"] === 1 && b.calls["joinPresence"] === 1, "both joined");
      // B publishes: A is told by an upsert; the entry has B's principal and client instance, never a session id (09 [READ-75])
      await b.engine.publishPresence(KS, TOPIC, { viewing: true, cursor: "line 3", n: 12 });
      await until(a, () => view.entries().length === 1, "A saw B's entry");
      const e = view.entries()[0];
      expect(e?.clientId).toBe(b.engine.clientId);
      expect(e?.principal?.sub).toBe("bob");
      expect(e?.state).toEqual({ viewing: true, cursor: "line 3", n: 12n });
      expect(e?.expiresAt).toBeGreaterThan(Date.now() - 60_000);
      expect(updates.at(-1)).toBe("changed:1:0:0");
      // A publishes; B's engine reads it from the engine's view (no query layer)
      await view.publish({ viewing: false });
      await until(b, () => b.engine.presenceEntries(KS, TOPIC).some((x) => x.clientId === a.engine.clientId), "B saw A's entry");
      expect(stateOf(b.engine.presenceEntries(KS, TOPIC).find((x) => x.clientId === a.engine.clientId)?.state as Uint8Array)).toEqual({ viewing: false });
      // B publishes again after the interval; A is told of the change
      await new Promise((r) => setTimeout(r, 1100));
      await b.engine.publishPresence(KS, TOPIC, { viewing: false, n: 13 });
      await until(a, () => (view.entries().find((x) => x.principal?.sub === "bob")?.state as { n?: bigint } | undefined)?.n === 13n, "A saw B's new state");
      // B leaves: the server sends left_client_ids
      await b.engine.leavePresence(KS, TOPIC);
      await until(a, () => view.entries().every((x) => x.principal?.sub !== "bob"), "A saw B go");
      expect(updates.at(-1)).toContain(":1"); // an entry removed
      expect(b.engine.presenceTopics()).toEqual([]);
      expect(a.ofType("protocol-error")).toEqual([]);
      expect(b.ofType("protocol-error")).toEqual([]);
      await view.close();
    });

    it("the limits and rules that the server answers: a state over its size, a schema that it breaks, a topic that no template matches, the publish interval", async () => {
      const KS = "acme/pres2";
      const { a } = await pair(KS);
      const seen: PresenceEvent[] = [];
      a.engine.onPresence((e) => seen.push(e));
      // the size: the limit is the server's (4 KiB), which the engine learned from Subscribed: refused before it is sent
      await a.engine.joinPresence(KS, "note/n1");
      await until(a, () => a.calls["joinPresence"] === 1, "joined");
      expect(a.engine.limits.maxPresenceStateBytes).toBe(4096);
      await expect(a.engine.publishPresence(KS, "note/n1", { cursor: "x".repeat(5000) })).rejects.toBeInstanceOf(LocalRefusal);
      // the schema: `unknown` is no member of the presence schema; the server answers SCHEMA_INVALID, which is reported and never resent
      await a.engine.publishPresence(KS, "note/n1", { unknown: 1 });
      await until(a, () => seen.some((e) => e.type === "refused" && e.code === "SCHEMA_INVALID"), "SCHEMA_INVALID reported");
      expect(a.calls["publishPresence"]).toBe(1);
      // a state that fits is accepted afterwards
      await a.engine.publishPresence(KS, "note/n1", { viewing: true });
      await until(a, () => seen.some((e) => e.type === "published"), "published");
      // a topic that matches no template of the kind: INVALID_KEY
      await a.engine.joinPresence(KS, "nothing/here");
      await until(a, () => seen.some((e) => e.type === "refused" && e.operation === "join" && e.code === "INVALID_KEY"), "INVALID_KEY reported");
      // the rate (09 [READ-79]): a burst of states is coalesced and the interval is waited for, never refused: three states give at most two calls
      const before = a.calls["publishPresence"] ?? 0;
      await a.engine.publishPresence(KS, "note/n1", { n: 1 });
      await a.engine.publishPresence(KS, "note/n1", { n: 2 });
      await a.engine.publishPresence(KS, "note/n1", { n: 3 });
      await new Promise((r) => setTimeout(r, 2500));
      expect((a.calls["publishPresence"] ?? 0) - before).toBeLessThanOrEqual(2);
      expect(seen.filter((e) => e.type === "refused" && e.code === "RATE_LIMITED")).toEqual([]); // the engine is never over the rate
    });

    it("after the network is cut and comes back, the engine joins its topic again and publishes its entry again, and the other client sees it (02 [ARC-51] item 4)", async () => {
      const KS = "acme/pres3";
      const TOPIC = "note/n1";
      const net = await env.network();
      const a = await env.client({ sub: "ana", grants: GRANTS, network: net });
      const b = await env.client({ sub: "bob", grants: GRANTS });
      await a.engine.createKeyspace(KS, "room");
      await loaded(a, KS);
      await b.engine.addKeyspace(KS);
      await loaded(b, KS);
      await a.engine.joinPresence(KS, TOPIC);
      await b.engine.joinPresence(KS, TOPIC);
      await a.engine.publishPresence(KS, TOPIC, { viewing: true });
      await until(b, () => b.engine.presenceEntries(KS, TOPIC).some((e) => e.clientId === a.engine.clientId), "B saw A");
      expect(a.calls["joinPresence"]).toBe(1);
      net.down();
      await until(a, () => a.engine.status().connection !== "live", "A offline");
      // the server ended A's session: its entry is gone for B (the server sends left_client_ids; if the connection is only noticed later, the TTL ends it)
      net.up();
      await until(a, () => a.engine.status().connection === "live" && (a.calls["joinPresence"] ?? 0) >= 2, "A joined again", 30_000);
      await until(a, () => (a.calls["publishPresence"] ?? 0) >= 2, "A published again", 30_000);
      await until(b, () => b.engine.presenceEntries(KS, TOPIC).some((e) => e.clientId === a.engine.clientId), "B sees A again", 30_000);
      expect(a.ofType("protocol-error")).toEqual([]);
    });

    it("Yjs awareness: the entry of the engine maps its tab id to the tab's state, and the other client reads it so (09 [READ-83])", async () => {
      const KS = "acme/pres4";
      const { a, b } = await pair(KS);
      const qb = createQueries(b.engine);
      const view = qb.presence(KS, "doc/d1");
      await view.join();
      await a.engine.joinPresence(KS, "doc/d1");
      expect(a.engine.presenceTopics()).toMatchObject([{ topic: "doc/d1", awareness: true }]); // a key of a `crdt` template: the manifest says so
      await a.engine.publishPresence(KS, "doc/d1", { cursor: "p3" });
      await until(b, () => view.entries().length === 1, "B saw the awareness entry");
      expect(view.entries()[0]?.tabs).toEqual({ main: { cursor: "p3" } });
      await view.close();
    });
  });

  describe("blobs (09 §9.9, 14 [CLI-44], [CLI-76], 16 [SEC-7])", () => {
    it("upload, commit, get and delete; an action that names the blob; the other client's cache drops it when the BLOB_DELETED record arrives", async () => {
      const KS = "acme/blob1";
      const { a, b } = await pair(KS);
      const bytes = bytesOf(70_000);
      // CreateUpload, the PUT to the signed URL with the headers that the server gave, CommitBlob (09 [READ-86], [READ-88])
      const ref = await a.engine.uploadBlob(KS, bytes, { contentType: "application/x-test" });
      expect(a.calls["createUpload"]).toBe(1);
      expect(a.calls["commitBlob"]).toBe(1);
      expect(ref.size).toBe(70_000);
      expect(Buffer.from(ref.sha256).equals(Buffer.from(await digestSha256(bytes)))).toBe(true);
      // the same call again: committed with the same digest and size: already_committed, no second upload
      await a.engine.uploadBlob(KS, bytes, { contentType: "application/x-test", blobId: ref.blobId });
      expect(a.calls["createUpload"]).toBe(2);
      expect(a.calls["commitBlob"]).toBe(1);
      // an action that names the blob is accepted (09 [READ-89]); the bytes that it kept are released when it is confirmed (09 [READ-94])
      const w = await a.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("note/with-blob", "see attachment")], blobRefs: [ref.blobId] });
      await until(a, () => committed(a, w.mutationId), "the action committed");
      expect(await a.engine.stagedBlobs()).toEqual([]);
      // the other client downloads it, checked against the digest (16 [SEC-7]), and caches it
      const got = await b.engine.getBlob(KS, ref.blobId);
      expect(Buffer.from(got.bytes).equals(Buffer.from(bytes))).toBe(true);
      expect(got.declaredContentType).toBe("application/x-test");
      expect(b.engine.blobCached(KS, ref.blobId)).toBe(true);
      expect(b.calls["getBlob"]).toBe(1);
      await b.engine.getBlob(KS, ref.blobId);
      expect(b.calls["getBlob"]).toBe(1); // the cache
      // a caller who is not the uploader and not admin cannot delete it (09 [READ-91])
      await expect(b.engine.deleteBlob(KS, ref.blobId, "not mine")).rejects.toMatchObject({ code: "PERMISSION_DENIED" });
      // the uploader deletes it; the BLOB_DELETED record reaches B, which drops the bytes (14 [CLI-76])
      await a.engine.deleteBlob(KS, ref.blobId, "the user removed the attachment");
      await until(b, () => !b.engine.blobCached(KS, ref.blobId), "B dropped its cached bytes");
      await expect(b.engine.getBlob(KS, ref.blobId)).rejects.toMatchObject({ code: "BLOB_DELETED", status: "NotFound" });
      await expect(a.engine.getBlob(KS, ref.blobId)).rejects.toBeInstanceOf(BlobError);
      // a repeated delete of a deleted blob succeeds and changes nothing (09 [READ-91])
      await a.engine.deleteBlob(KS, ref.blobId, "again");
      // an id that was never declared
      await expect(b.engine.getBlob(KS, "never-declared-blob-id-0001")).rejects.toMatchObject({ code: "BLOB_MISSING", status: "NotFound" });
      expect(a.ofType("protocol-error")).toEqual([]);
      expect(b.ofType("protocol-error")).toEqual([]);
    });

    it("an action authored offline that names a staged blob: the server answers BLOB_MISSING, the engine uploads the same bytes and resends the identical action", async () => {
      const KS = "acme/blob2";
      const net = await env.network();
      const a = await env.client({ sub: "ana", grants: GRANTS, network: net });
      await a.engine.createKeyspace(KS, "room");
      await loaded(a, KS);
      await until(a, () => a.engine.status().connection === "live", "A live");
      net.down();
      await until(a, () => a.engine.status().connection !== "live", "A offline");
      const bytes = bytesOf(2048);
      const ref = await a.engine.stageBlob(KS, bytes);
      const w = await a.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("note/offline-blob", "x")], blobRefs: [ref.blobId] });
      expect(await a.engine.stagedBlobs()).toHaveLength(1);
      net.up();
      await until(a, () => committed(a, w.mutationId), "the action committed after the upload", 40_000);
      expect(a.calls["createUpload"]).toBeGreaterThanOrEqual(1);
      expect(await a.engine.stagedBlobs()).toEqual([]);
      const got = await a.engine.getBlob(KS, ref.blobId);
      expect(Buffer.from(got.bytes).equals(Buffer.from(bytes))).toBe(true);
      expect(a.ofType("held")).toEqual([]);
    });

    it("an action that names a blob that is uploaded and not committed: Retry{BLOB_PENDING}; the engine completes the upload and resends", async () => {
      const KS = "acme/blob3";
      const net = await env.network();
      const a = await env.client({ sub: "ana", grants: GRANTS, network: net });
      await a.engine.createKeyspace(KS, "room");
      await loaded(a, KS);
      await until(a, () => a.engine.status().connection === "live", "A live");
      net.down();
      await until(a, () => a.engine.status().connection !== "live", "A offline");
      const bytes = bytesOf(512);
      const ref = await a.engine.stageBlob(KS, bytes);
      // a CreateUpload that no PUT followed: the row is pending (made by another route to the server, as an earlier run of the app would have)
      const other = await env.client({ sub: "ana", grants: GRANTS, deviceId: "dev-ana-2", start: false });
      await other.engine.transport.createUpload(create(CreateUploadRequestSchema, { keyspaceId: KS, blobId: ref.blobId, sha256: ref.sha256, size: BigInt(ref.size) }));
      const w = await a.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("note/pending-blob", "x")], blobRefs: [ref.blobId] });
      net.up();
      await until(a, () => committed(a, w.mutationId), "the action committed after BLOB_PENDING", 40_000);
      expect(a.ofType("held")).toEqual([]);
      expect(a.engine.deadLetterEntries()).toEqual([]);
    });

    it("a blob that is uploaded by the app's own call is not uploaded twice: the stash serves reads of it, and the cache serves a second reader", async () => {
      const KS = "acme/blob4";
      const { a } = await pair(KS);
      const ref = await a.engine.uploadBlob(KS, bytesOf(10), { contentType: "text/plain" });
      const got = await a.engine.getBlob(KS, ref.blobId);
      expect(got.size).toBe(10);
      expect(a.calls["getBlob"] ?? 0).toBe(0); // the engine's own bytes: no download
      await a.engine.releaseBlob(KS, ref.blobId);
      const down = await a.engine.getBlob(KS, ref.blobId); // now by GetBlob and the signed URL
      expect(Buffer.from(down.bytes).equals(Buffer.from(bytesOf(10)))).toBe(true);
      expect(a.calls["getBlob"]).toBe(1);
    });
  });
});
