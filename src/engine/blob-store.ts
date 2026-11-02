// The leaf of the blob support (09 §9.9): the HTTP of the blob store, the stash of the bytes that actions name, the cache of downloaded bytes, the errors, and the
// helpers (the blob id, the digest). It imports nothing of the engine, so that `core.ts` may read its types and defaults; the behaviour is `blobs.ts`'s.

import type { Reject } from "../gen/abpc/v2/types_pb.js";
import { sha256 as sha256Sync } from "../rules/crypto.js";
import type { RpcStatus } from "./errors.js";

/** The HTTP of the blob store (09 §9.9): the signed URLs that `CreateUpload` and `GetBlob` return. The default is `fetchBlobHttp` (the platform's `fetch`). */
export interface BlobHttp {
  /**
   * Sends the bytes with `PUT` to the signed URL with exactly these headers (09 [READ-86]); `Content-Length` is the platform's. Resolves with the HTTP status of the answer, whatever
   * it is; rejects when the connection failed.
   */
  put(url: string, headers: Readonly<Record<string, string>>, body: Uint8Array, o?: { signal?: AbortSignal }): Promise<{ status: number }>;
  /** Reads the signed URL with `GET`, at most `maxBytes` octets of the body: a longer body is refused with `tooLarge`. */
  get(url: string, o: { maxBytes: number; signal?: AbortSignal }): Promise<{ status: number; bytes: Uint8Array; tooLarge?: boolean }>;
}

/** A blob that the app handed the engine and an action may name (09 [READ-94]). */
export interface StagedBlob {
  keyspaceId: string;
  blobId: string;
  bytes: Uint8Array;
  /** The SHA-256 of `bytes`, 32 octets (09 [READ-86]). */
  sha256: Uint8Array;
  size: number;
  /** At most 255 octets of printable US-ASCII; empty is `application/octet-stream` on the server (09 [READ-86]). */
  contentType: string;
  /** The device clock time at which the blob was staged, in ms. */
  stagedAtMs: number;
}

/**
 * Where the engine keeps the bytes of the blobs that actions name until the actions are confirmed (09 [READ-94]). A durable one survives a restart: an action that names a staged blob
 * finds its bytes after a reload, and the engine uploads them again on `BLOB_MISSING` (14 [CLI-44]). The default is in memory; the browser host's is `IndexedDbBlobStash`, and a Node
 * process's is `FileBlobStash`, which the app gives the engine.
 */
export interface BlobStash {
  put(blob: StagedBlob): Promise<void>;
  get(keyspaceId: string, blobId: string): Promise<StagedBlob | undefined>;
  /** Whether the blob is staged: what a stash that keeps its bytes in a database or on a disk answers without reading them. */
  has(keyspaceId: string, blobId: string): Promise<boolean>;
  delete(keyspaceId: string, blobId: string): Promise<void>;
  /** What is staged, without the bytes. */
  list(): Promise<{ keyspaceId: string; blobId: string; size: number }[]>;
  /**
   * Erases every staged blob: a sign-out purges what the principal's partition holds (14 [CLI-65]), and the bytes that an action carries are the principal's data too. Resolves when
   * nothing is left, and fails when something could not be erased.
   */
  clear(): Promise<void>;
}

/** A `BlobStash` in memory. */
export class MemoryBlobStash implements BlobStash {
  private readonly rows = new Map<string, StagedBlob>();

  put(blob: StagedBlob): Promise<void> {
    this.rows.set(`${blob.keyspaceId}\u0000${blob.blobId}`, blob);
    return Promise.resolve();
  }
  get(keyspaceId: string, blobId: string): Promise<StagedBlob | undefined> {
    return Promise.resolve(this.rows.get(`${keyspaceId}\u0000${blobId}`));
  }
  has(keyspaceId: string, blobId: string): Promise<boolean> {
    return Promise.resolve(this.rows.has(`${keyspaceId}\u0000${blobId}`));
  }
  delete(keyspaceId: string, blobId: string): Promise<void> {
    this.rows.delete(`${keyspaceId}\u0000${blobId}`);
    return Promise.resolve();
  }
  list(): Promise<{ keyspaceId: string; blobId: string; size: number }[]> {
    return Promise.resolve([...this.rows.values()].map((b) => ({ keyspaceId: b.keyspaceId, blobId: b.blobId, size: b.size })));
  }
  clear(): Promise<void> {
    this.rows.clear();
    return Promise.resolve();
  }
}

/** The bytes of a downloaded blob, checked against its digest (16 [SEC-7]). */
export interface BlobData {
  keyspaceId: string;
  blobId: string;
  /** The bytes are the cache's too: a caller that changes them copies them first. */
  bytes: Uint8Array;
  sha256: Uint8Array;
  size: number;
  /**
   * The content type that the uploader declared. It is the server's word for what the bytes are: never render the bytes as active content on the strength of it alone
   * (16 [SEC-7]).
   */
  declaredContentType: string;
}

/** What a call that waits shows (09 [READ-90], 14 [CLI-44]). */
export interface BlobWait {
  /** The retry reason: `BLOB_PENDING` (uploaded and not committed), `BLOB_WITHHELD`, `RATE_LIMITED`, `STORAGE_UNAVAILABLE`, ..., or the status of a failed call. */
  reason: string;
  /** The wait before the call is asked again, in ms. */
  delayMs: number;
}

export interface BlobCallOptions {
  /** Stops the call: the app decides when to stop asking (14 [CLI-44]). The call fails with `ABORTED`. */
  signal?: AbortSignal;
  /** Called each time the call waits before it asks again. */
  onWait?: (wait: BlobWait) => void;
}

export interface StageOptions {
  contentType?: string;
  /** The blob id; default a new one (09 [READ-84]). A caller that supplies one supplies at least 122 random bits, never a hash of the content. */
  blobId?: string;
}

/** The blob that a stage or an upload made: what an action names in `blob_refs`. */
export interface BlobRef {
  keyspaceId: string;
  blobId: string;
  sha256: Uint8Array;
  size: number;
  contentType: string;
}

export type BlobErrorCode =
  | "BLOB_MISSING"
  | "BLOB_DELETED"
  /** The blob is withheld (12 [ADM-103]) and the call does not wait for it: an upload that an engine makes for an action it sends (14 [CLI-44]). */
  | "BLOB_WITHHELD"
  | "PAYLOAD_TOO_LARGE"
  | "QUOTA_EXCEEDED"
  | "IDEMPOTENCY_KEY_REUSED"
  | "PERMISSION_DENIED"
  | "KEYSPACE_ARCHIVED"
  | "KEYSPACE_DELETED"
  | "KEYSPACE_NOT_FOUND"
  | "HISTORY_DIVERGED"
  | "ADMIN_REFUSED"
  | "INVALID_KEY"
  | "INVALID_ENVELOPE"
  /** The downloaded bytes are not the declared ones (16 [SEC-7]). */
  | "CHECKSUM_MISMATCH"
  /** The blob store refused the upload (a status that no resend changes). */
  | "UPLOAD_REFUSED"
  /** The rounds of the upload were used up (see the SPEC-QUESTION above). */
  | "UPLOAD_FAILED"
  | "DOWNLOAD_FAILED"
  | "ABORTED"
  | "NO_TOKEN"
  | "SUBJECT_REVOKED"
  | "UNAUTHENTICATED"
  | "PROTOCOL_ERROR"
  | "UNIMPLEMENTED"
  | "UNKNOWN";

/** A blob call that ended without its result. The server's own words are `reject` and `status`; `reason` is `details["reason"]` of an `ADMIN_REFUSED` (`LEGAL_HOLD`, `RETENTION_FLOOR`). */
export class BlobError extends Error {
  readonly code: BlobErrorCode;
  readonly status: RpcStatus | undefined;
  readonly reject: Reject | undefined;
  readonly reason: string | undefined;

  constructor(code: BlobErrorCode, detail: string, from: { status?: RpcStatus; reject?: Reject } = {}) {
    super(`blob: ${code}: ${detail}`);
    this.name = "BlobError";
    this.code = code;
    this.status = from.status;
    this.reject = from.reject;
    this.reason = from.reject?.details["reason"];
  }
}

/** The rounds of `CreateUpload`, PUT and `CommitBlob` that one call makes at most (see the SPEC-QUESTION above). */
export const MAX_UPLOAD_ROUNDS = 4;
/** The attempts at one PUT or one GET of a signed URL that fail in a way that a resend may mend, before the round is given up. */
export const MAX_TRANSFER_ATTEMPTS = 4;
/** The default size of the cache of downloaded bytes. */
export const DEFAULT_BLOB_CACHE_BYTES = 32 * 1024 * 1024;

const BLOB_ID_RE = /^[A-Za-z0-9_-]{21,64}$/;
/** `Content-Length` and `Host` are the platform's: a header that names one is not set by the engine (09 [READ-86] "besides Content-Length"). */
export const PLATFORM_HEADERS: ReadonlySet<string> = new Set(["content-length", "host"]);

/** Whether the text is a blob id (03 Table 3.1, 09 [READ-84]). */
export function isBlobId(s: string): boolean {
  return BLOB_ID_RE.test(s);
}

const B64URL = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/** A blob id from the random source: 16 octets, 22 characters of base64url, 128 bits (09 [READ-84]). */
export function newBlobId(random: { bytes(n: number): Uint8Array }): string {
  const b = random.bytes(16);
  let out = "";
  for (let i = 0; i < 15; i += 3) {
    const n = ((b[i] as number) << 16) | ((b[i + 1] as number) << 8) | (b[i + 2] as number);
    out += B64URL.charAt((n >> 18) & 63) + B64URL.charAt((n >> 12) & 63) + B64URL.charAt((n >> 6) & 63) + B64URL.charAt(n & 63);
  }
  const last = b[15] as number;
  return out + B64URL.charAt(last >> 2) + B64URL.charAt((last & 3) << 4); // 15 octets in 20 characters, the 16th in 2 more: 22
}

/** The SHA-256 of the bytes: the platform's WebCrypto when there is one (it does not block the thread), else the hash provider of `rules/crypto`. */
export async function digestSha256(bytes: Uint8Array): Promise<Uint8Array> {
  const subtle = (globalThis as { crypto?: { subtle?: { digest(alg: string, data: Uint8Array): Promise<ArrayBuffer> } } }).crypto?.subtle;
  if (subtle !== undefined) {
    try {
      return new Uint8Array(await subtle.digest("SHA-256", bytes));
    } catch {
      // a platform that refuses (an insecure context): the provider answers
    }
  }
  return sha256Sync(bytes);
}

export const sameBytes = (a: Uint8Array, b: Uint8Array): boolean => a.length === b.length && a.every((x, i) => x === b[i]);

/** `BlobHttp` over the platform's `fetch`: no credentials and no redirect (a signed URL is a bearer capability, 09 [READ-90]). */
export function fetchBlobHttp(fetchFn: typeof fetch = globalThis.fetch): BlobHttp {
  return {
    async put(url, headers, body, o) {
      const res = await fetchFn(url, { method: "PUT", headers: { ...headers }, body, credentials: "omit", redirect: "error", ...(o?.signal !== undefined ? { signal: o.signal } : {}) });
      await res.arrayBuffer().catch(() => undefined); // the answer is read to its end, so that the connection is reused
      return { status: res.status };
    },
    async get(url, o) {
      const res = await fetchFn(url, { method: "GET", credentials: "omit", redirect: "error", ...(o.signal !== undefined ? { signal: o.signal } : {}) });
      if (res.status !== 200 || res.body === null) {
        await res.arrayBuffer().catch(() => undefined);
        return { status: res.status, bytes: new Uint8Array(0) };
      }
      const reader = res.body.getReader();
      const chunks: Uint8Array[] = [];
      let n = 0;
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        n += value.length;
        if (n > o.maxBytes) {
          await reader.cancel().catch(() => undefined);
          return { status: res.status, bytes: new Uint8Array(0), tooLarge: true };
        }
        chunks.push(value);
      }
      const out = new Uint8Array(n);
      let at = 0;
      for (const c of chunks) {
        out.set(c, at);
        at += c.length;
      }
      return { status: res.status, bytes: out };
    },
  };
}

/** The cache of downloaded bytes, bounded by the octets that it holds: the least recently read go first. It is memory only. */
export class BlobCache {
  private readonly rows = new Map<string, BlobData>();
  private octets = 0;

  constructor(private readonly maxBytes: number) {}

  private key(ks: string, id: string): string {
    return `${ks}\u0000${id}`;
  }

  get(ks: string, id: string): BlobData | undefined {
    const k = this.key(ks, id);
    const b = this.rows.get(k);
    if (b !== undefined) {
      this.rows.delete(k);
      this.rows.set(k, b); // read last
    }
    return b;
  }

  put(b: BlobData): void {
    if (b.size > this.maxBytes) return;
    this.delete(b.keyspaceId, b.blobId);
    this.rows.set(this.key(b.keyspaceId, b.blobId), b);
    this.octets += b.size;
    for (const [k, v] of this.rows) {
      if (this.octets <= this.maxBytes) break;
      this.rows.delete(k);
      this.octets -= v.size;
    }
  }

  delete(ks: string, id: string): boolean {
    const k = this.key(ks, id);
    const b = this.rows.get(k);
    if (b === undefined) return false;
    this.rows.delete(k);
    this.octets -= b.size;
    return true;
  }

  /** Drops every blob. */
  clear(): void {
    this.rows.clear();
    this.octets = 0;
  }

  /** Drops every blob of the keyspace; returns how many. */
  dropKeyspace(ks: string): number {
    let n = 0;
    for (const [k, v] of [...this.rows]) {
      if (v.keyspaceId !== ks) continue;
      this.rows.delete(k);
      this.octets -= v.size;
      n++;
    }
    return n;
  }

  get size(): number {
    return this.rows.size;
  }

  get bytes(): number {
    return this.octets;
  }

  has(ks: string, id: string): boolean {
    return this.rows.has(this.key(ks, id));
  }
}

