// The blobs of the fake server (09 §9.9) and the blob store that serves its signed URLs, as `server/internal/blob` and `store/blob.go` answer on the points that a client's behaviour
// depends on:
//
//  - [READ-86] `CreateUpload` of an id with no row makes a pending row and returns a signed PUT that binds the digest and the size, with `upload_headers` (the checksum header, which the
//    client sends unchanged) and `expires_at`; a repeated call for a pending blob with the same digest and size gives a new URL; another digest or size is `IDEMPOTENCY_KEY_REUSED`
//    (AlreadyExists); a committed blob with the same digest and size answers `already_committed`; a deleted one `BLOB_DELETED` (FailedPrecondition); a size over `max_blob_bytes`
//    `PAYLOAD_TOO_LARGE`, a bad digest or content type `INVALID_ENVELOPE`; a withheld blob `Retry{BLOB_WITHHELD}`;
//  - [READ-87] the store refuses a PUT whose headers, length or checksum differ from the bound ones (400), and a URL that is not valid or has expired (403);
//  - [READ-88] `CommitBlob` checks the stored object: missing or different is `BLOB_MISSING` (FailedPrecondition), and the blob stays pending; a committed blob commits again;
//  - [READ-89] an action that names a blob: no row `Reject{BLOB_MISSING}` (class RETRY), pending `Retry{BLOB_PENDING}` (nothing recorded), deleted `Reject{BLOB_DELETED}`; committed and
//    withheld pass, in the order of the refs;
//  - [READ-90] `GetBlob` of a committed blob answers a short-lived download URL, the digest, the size and the content type; pending `Retry{BLOB_PENDING}` (Unavailable), withheld
//    `Retry{BLOB_WITHHELD}`, no row `BLOB_MISSING` and deleted `BLOB_DELETED` (NotFound);
//  - [READ-91] `DeleteBlob` by the uploader (or an admin), an id with no row: NotFound `BLOB_MISSING` to an admin, PermissionDenied to everyone else; a deleted blob deletes again and
//    changes nothing; a withheld blob `Retry{BLOB_WITHHELD}`.

import { create } from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import {
  CreateUploadResponseSchema,
  GetBlobResponseSchema,
  type CommitBlobRequest,
  type CreateUploadRequest,
  type CreateUploadResponse,
  type DeleteBlobRequest,
  type GetBlobRequest,
  type GetBlobResponse,
} from "../gen/abpc/v2/sync_pb.js";
import { Code, ErrorClass, RejectSchema, RetryReason, RetrySchema, type Reject, type Retry } from "../gen/abpc/v2/types_pb.js";
import type { BlobHttp } from "../engine/blob-store.js";
import type { Clock } from "../engine/clock.js";
import { DEFAULT_CLASS } from "../engine/codes.js";
import { RpcError } from "../engine/errors.js";
import { sha256 } from "../rules/crypto.js";

export interface BlobRow {
  state: "pending" | "committed" | "deleted";
  sha256: Uint8Array;
  size: bigint;
  contentType: string;
  uploader: string;
  /** A candidate of a redaction that a frozen keyspace deferred (12 [ADM-103]): every blob call answers `Retry{BLOB_WITHHELD}`. */
  withheld: boolean;
}

interface Grant {
  op: "put" | "get";
  keyspaceId: string;
  blobId: string;
  size: bigint;
  headers: Record<string, string>;
  expiresMs: number;
}

const BLOB_ID_RE = /^[A-Za-z0-9_-]{21,64}$/;
export const FAKE_CHECKSUM_HEADER = "x-fake-checksum-sha256";
const b64 = (b: Uint8Array): string => Buffer.from(b).toString("base64");
const same = (a: Uint8Array, b: Uint8Array): boolean => a.length === b.length && a.every((x, i) => x === b[i]);

function rejectOf(code: Code, details: Record<string, string> = {}): Reject {
  return create(RejectSchema, { code, errorClass: DEFAULT_CLASS.get(code) ?? ErrorClass.DEAD_LETTER, details });
}
const fail = (status: ConstructorParameters<typeof RpcError>[0], code: Code, message = "", details: Record<string, string> = {}): RpcError => new RpcError(status, message, { reject: rejectOf(code, details) });
const retry = (reason: RetryReason, afterMs = 1000): Retry => create(RetrySchema, { reason, afterMs });

export interface FakeBlobsOptions {
  clock: Clock;
  /** The validity of an upload URL, in ms (04 Table 4.11: at most 1 h); default 1 h. */
  uploadValidityMs?: number;
  /** The validity of a download URL, in ms (at most 15 min); default 15 min. */
  downloadValidityMs?: number;
  maxBlobBytes: () => bigint;
}

export class FakeBlobs {
  readonly rows = new Map<string, BlobRow>();
  readonly objects = new Map<string, Uint8Array>();
  private readonly grants = new Map<string, Grant>();
  private nextToken = 1;
  /** Every request that reached the blob store, in order, for a test. */
  readonly http: { method: "PUT" | "GET"; url: string; headers: Record<string, string>; status: number; octets: number }[] = [];
  /** The next PUTs answer this status without storing (a store that is down, an expired URL). */
  failPuts: { times: number; status: number } | undefined;
  /** The next GETs answer this status. */
  failGets: { times: number; status: number } | undefined;
  /** The next GET returns different bytes (a corrupt or tampered object). */
  corruptNextGet = false;
  /** The next GET returns more bytes than the blob has. */
  overlongNextGet = false;
  /** Subjects that hold `admin` (09 [READ-91]). */
  readonly admins = new Set<string>();

  constructor(private readonly o: FakeBlobsOptions) {}

  private k(ks: string, id: string): string {
    return `${ks}\u0000${id}`;
  }

  row(ks: string, id: string): BlobRow | undefined {
    return this.rows.get(this.k(ks, id));
  }

  /** The stored bytes of a blob, for a test. */
  object(ks: string, id: string): Uint8Array | undefined {
    return this.objects.get(this.k(ks, id));
  }

  /** Removes the stored object and leaves the row: a store that lost it (09 [READ-88]). */
  dropObject(ks: string, id: string): void {
    this.objects.delete(this.k(ks, id));
  }

  /** Marks a blob withheld, or not (12 [ADM-103]). */
  withhold(ks: string, id: string, on = true): void {
    const r = this.row(ks, id);
    if (r === undefined) throw new Error(`the fake has no blob ${id}`);
    r.withheld = on;
  }

  private sign(g: Omit<Grant, "expiresMs">, validityMs: number): string {
    const token = `t${this.nextToken++}`;
    this.grants.set(token, { ...g, expiresMs: this.o.clock.now() + validityMs });
    return `https://blobs.fake.test/${g.op}/${token}`;
  }

  /** Whether the check of a blob call's ids: `INVALID_KEY` (09 [READ-84]). */
  private ids(blobId: string): void {
    if (!BLOB_ID_RE.test(blobId)) throw fail("InvalidArgument", Code.INVALID_KEY, "a malformed blob id");
  }

  // -- the calls (the server has judged the token, the grant and the keyspace) --

  createUpload(sub: string, req: CreateUploadRequest): CreateUploadResponse {
    this.ids(req.blobId);
    if (req.sha256.length !== 32) throw fail("InvalidArgument", Code.INVALID_ENVELOPE, "sha256 is not 32 bytes");
    if (req.contentType.length > 255 || !/^[\x20-\x7e]*$/.test(req.contentType)) throw fail("InvalidArgument", Code.INVALID_ENVELOPE, "content_type");
    if (req.size > this.o.maxBlobBytes()) throw fail("InvalidArgument", Code.PAYLOAD_TOO_LARGE, "size is over max_blob_bytes");
    const key = this.k(req.keyspaceId, req.blobId);
    const row = this.rows.get(key);
    if (row !== undefined) {
      if (row.withheld) throw new RpcError("Unavailable", "the blob is withheld", { retry: retry(RetryReason.BLOB_WITHHELD) });
      if (row.state === "deleted") throw fail("FailedPrecondition", Code.BLOB_DELETED, "the blob was deleted");
      if (!same(row.sha256, req.sha256) || row.size !== req.size) throw fail("AlreadyExists", Code.IDEMPOTENCY_KEY_REUSED, "the blob id names another sha256 or size");
      if (row.state === "committed") return create(CreateUploadResponseSchema, { alreadyCommitted: true });
    } else {
      this.rows.set(key, { state: "pending", sha256: req.sha256, size: req.size, contentType: req.contentType === "" ? "application/octet-stream" : req.contentType, uploader: sub, withheld: false });
    }
    const headers = { [FAKE_CHECKSUM_HEADER]: b64(req.sha256) };
    const validity = this.o.uploadValidityMs ?? 3_600_000;
    const url = this.sign({ op: "put", keyspaceId: req.keyspaceId, blobId: req.blobId, size: req.size, headers }, validity);
    return create(CreateUploadResponseSchema, { uploadUrl: url, expiresAt: timestampFromMs(this.o.clock.now() + validity), uploadHeaders: headers });
  }

  commitBlob(req: CommitBlobRequest): void {
    this.ids(req.blobId);
    const key = this.k(req.keyspaceId, req.blobId);
    const row = this.rows.get(key);
    if (row === undefined) throw fail("FailedPrecondition", Code.BLOB_MISSING, "the blob has no row");
    if (row.withheld) throw new RpcError("Unavailable", "the blob is withheld", { retry: retry(RetryReason.BLOB_WITHHELD) });
    if (row.state === "deleted") throw fail("FailedPrecondition", Code.BLOB_DELETED, "the blob was deleted");
    if (row.state === "committed") return;
    const obj = this.objects.get(key);
    if (obj === undefined || BigInt(obj.length) !== row.size || !same(sha256(obj), row.sha256)) throw fail("FailedPrecondition", Code.BLOB_MISSING, "the stored object is missing or differs");
    row.state = "committed";
  }

  getBlob(req: GetBlobRequest): GetBlobResponse {
    this.ids(req.blobId);
    const row = this.rows.get(this.k(req.keyspaceId, req.blobId));
    if (row === undefined) throw fail("NotFound", Code.BLOB_MISSING, "the blob has no row");
    if (row.state === "deleted") throw fail("NotFound", Code.BLOB_DELETED, "the blob was deleted");
    if (row.withheld) throw new RpcError("Unavailable", "the blob is withheld", { retry: retry(RetryReason.BLOB_WITHHELD) });
    if (row.state === "pending") throw new RpcError("Unavailable", "the blob is uploaded and not committed", { retry: retry(RetryReason.BLOB_PENDING) });
    const url = this.sign({ op: "get", keyspaceId: req.keyspaceId, blobId: req.blobId, size: row.size, headers: {} }, this.o.downloadValidityMs ?? 900_000);
    return create(GetBlobResponseSchema, { downloadUrl: url, sha256: row.sha256, size: row.size, contentType: row.contentType });
  }

  /** Returns true when the blob was deleted by this call (the server then appends the `BLOB_DELETED` record), false when there was nothing to do. */
  deleteBlob(sub: string, req: DeleteBlobRequest): boolean {
    this.ids(req.blobId);
    if (req.reason === "" || req.reason.length > 1024 || req.reason.includes("\u0000")) throw fail("InvalidArgument", Code.INVALID_ENVELOPE, "reason");
    const key = this.k(req.keyspaceId, req.blobId);
    const row = this.rows.get(key);
    const admin = this.admins.has(sub);
    if (row === undefined) {
      if (admin) throw fail("NotFound", Code.BLOB_MISSING, "the blob has no row");
      throw fail("PermissionDenied", Code.PERMISSION_DENIED, "no grant");
    }
    if (row.uploader !== sub && !admin) throw fail("PermissionDenied", Code.PERMISSION_DENIED, "no grant");
    if (row.withheld) throw new RpcError("Unavailable", "the blob is withheld", { retry: retry(RetryReason.BLOB_WITHHELD) });
    if (row.state === "deleted") return false;
    const wasPending = row.state === "pending";
    row.state = "deleted";
    row.sha256 = new Uint8Array(0); // the tombstone keeps no hash of the bytes (09 [READ-91])
    this.objects.delete(key);
    return !wasPending; // a pending blob's deletion has no record (09 [READ-91])
  }

  /** 09 [READ-89]: the first failure in the order of `refs` decides. */
  judgeRefs(keyspaceId: string, refs: readonly string[]): { reject?: Reject; retry?: Retry } {
    for (const id of refs) {
      const row = this.rows.get(this.k(keyspaceId, id));
      if (row === undefined) return { reject: rejectOf(Code.BLOB_MISSING) };
      if (row.state === "pending") return { retry: retry(RetryReason.BLOB_PENDING) };
      if (row.state === "deleted") return { reject: rejectOf(Code.BLOB_DELETED) };
    }
    return {};
  }

  // -- the blob store that serves the signed URLs --

  /** The `BlobHttp` of the engine over this store. */
  blobHttp(): BlobHttp {
    return {
      put: (url, headers, body) => Promise.resolve({ status: this.servePut(url, headers, body) }),
      get: (url, o) => Promise.resolve(this.serveGet(url, o.maxBytes)),
    };
  }

  private grantOf(url: string, op: "put" | "get"): Grant | undefined {
    const m = /^https:\/\/blobs\.fake\.test\/(put|get)\/(t\d+)$/.exec(url);
    if (m === null || m[1] !== op) return undefined;
    const g = this.grants.get(m[2] as string);
    return g !== undefined && this.o.clock.now() < g.expiresMs ? g : undefined;
  }

  private servePut(url: string, headers: Readonly<Record<string, string>>, body: Uint8Array): number {
    const rec = (status: number): number => {
      this.http.push({ method: "PUT", url, headers: { ...headers }, status, octets: body.length });
      return status;
    };
    if (this.failPuts !== undefined && this.failPuts.times > 0) {
      this.failPuts.times--;
      return rec(this.failPuts.status);
    }
    const g = this.grantOf(url, "put");
    if (g === undefined) return rec(403); // not valid, or expired (09 [READ-87])
    const sent = Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v]));
    for (const [k, v] of Object.entries(g.headers)) if (sent[k] !== v) return rec(400); // "exactly those headers", unchanged
    if (BigInt(body.length) !== g.size) return rec(400);
    const row = this.rows.get(this.k(g.keyspaceId, g.blobId));
    if (row === undefined || row.state === "deleted" || !same(sha256(body), row.sha256)) return rec(400);
    this.objects.set(this.k(g.keyspaceId, g.blobId), new Uint8Array(body));
    return rec(200);
  }

  private serveGet(url: string, maxBytes: number): { status: number; bytes: Uint8Array; tooLarge?: boolean } {
    const rec = (status: number, n: number): void => void this.http.push({ method: "GET", url, headers: {}, status, octets: n });
    if (this.failGets !== undefined && this.failGets.times > 0) {
      this.failGets.times--;
      rec(this.failGets.status, 0);
      return { status: this.failGets.status, bytes: new Uint8Array(0) };
    }
    const g = this.grantOf(url, "get");
    if (g === undefined) {
      rec(403, 0);
      return { status: 403, bytes: new Uint8Array(0) };
    }
    const obj = this.objects.get(this.k(g.keyspaceId, g.blobId));
    if (obj === undefined) {
      rec(404, 0);
      return { status: 404, bytes: new Uint8Array(0) };
    }
    let bytes = new Uint8Array(obj);
    if (this.corruptNextGet && bytes.length > 0) {
      this.corruptNextGet = false;
      bytes[0] = (bytes[0] as number) ^ 0xff;
    }
    if (this.overlongNextGet) {
      this.overlongNextGet = false;
      const longer = new Uint8Array(bytes.length + 5);
      longer.set(bytes);
      bytes = longer;
    }
    if (bytes.length > maxBytes) {
      rec(200, bytes.length);
      return { status: 200, bytes: new Uint8Array(0), tooLarge: true };
    }
    rec(200, bytes.length);
    return { status: 200, bytes };
  }
}
