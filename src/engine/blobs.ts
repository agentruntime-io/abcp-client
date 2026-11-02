// Blobs on the engine (09 §9.9, 14 [CLI-44], [CLI-76], 16 [SEC-7]): the upload of bytes to a signed URL, the commit, the download, the deletion, and the bytes that an
// action that names a blob keeps until it is confirmed.
//
//  - [READ-84] The client generates the blob id: 16 bytes of the engine's random source, in base64url (22 characters, 128 bits), never derived from the content or from
//    personal data. A blob id that a caller supplies is checked for its syntax, `[A-Za-z0-9_-]{21,64}`; its randomness is the caller's.
//  - [READ-86] `CreateUpload{keyspace_id, blob_id, sha256, size, content_type}` declares the blob; the answer is a signed PUT (`upload_url`, `upload_headers`, `expires_at`), or
//    `already_committed` for an id that is committed with the same digest and size. "The client MUST send the PUT with exactly those headers, with their values unchanged
//    (they are opaque, 03 [ID-27]), and `Content-Length`". A `size` over `limits.max_blob_bytes` is refused here before the call; the server answers `PAYLOAD_TOO_LARGE`.
//  - [READ-87], [READ-88] The store refuses an upload whose length or checksum differs; `CommitBlob` verifies the stored object. "If the object is missing or differs, the
//    call fails with FailedPrecondition and `Reject{BLOB_MISSING}`, and the client uploads again": the engine repeats `CreateUpload`, the PUT and `CommitBlob`.
//  - [READ-89], 14 [CLI-44] An action that names a blob that is not committed gets `Retry{BLOB_PENDING}` (the upload is completed) or `Reject{BLOB_MISSING}` (the upload is
//    repeated); the engine does it with the bytes that it kept for the action (`BlobStash`), then resends the identical action after the larger of `after_ms` and its backoff
//    (`repairBlobs`, called by the reactions). A withheld blob never stops an action: the server accepts it (no `Retry{BLOB_WITHHELD}` for a mutation).
//  - [READ-90] `GetBlob` gives a short-lived `download_url`, the digest, the size and the declared content type. A pending blob answers `Retry{BLOB_PENDING}` and a withheld one
//    `Retry{BLOB_WITHHELD}`: the call is asked again after the larger of `after_ms` and the client backoff, holding no lane, keyspace or action, with no bound of its own ("the
//    app decides when to stop asking": the `signal` of the call, and `onWait` for what it shows meanwhile). `BLOB_MISSING` and `BLOB_DELETED` are final.
//  - [SEC-7] A downloaded blob is checked against its `sha256` and `size` and never cached or returned when it differs; its `content_type` is the server's declaration and the
//    app does not render the bytes as active content on the strength of it alone. The read is bounded by the declared size.
//  - [READ-91] `DeleteBlob{keyspace_id, blob_id, reason}`: `reason` is required, at most 1,024 octets, with no U+0000. A deleted blob's `DeleteBlob` succeeds and changes nothing.
//    `ADMIN_REFUSED{LEGAL_HOLD | RETENTION_FLOOR}` is final.
//  - [READ-94] "A client keeps a blob's bytes locally until the action that references it is confirmed, and re-uploads on `BLOB_MISSING` (14 [CLI-44]). Its local garbage collection
//    covers its own cache only: it MUST NOT delete server blobs to clean up": the engine never calls `DeleteBlob` on its own.
//  - [CLI-76] "On a `BLOB_DELETED` record (06 [LOG-79]), a client MUST drop its cached bytes of that blob"; a load with `resumed = false` "MUST drop its cache of the keyspace's downloaded
//    blob bytes, never the bytes that an unsent action still carries (09 [READ-94])". The downloaded-bytes cache is in memory, so no cursor is persisted over a purge that is not done.
//  - [CLI-66] The blob cache is on the purge list of `ACCESS_REVOKED`: `blobsPurge`.
//  - [CLI-43], A.7 Every call is resent, unchanged, after `Unavailable`, `DeadlineExceeded`, `Aborted` and the statuses of a proxy, and after `Retry{RATE_LIMITED}`, with the client
//    backoff; `Unauthenticated` is resent once after a refresh of the token; `QUOTA_EXCEEDED`, `IDEMPOTENCY_KEY_REUSED` and every `Reject` are final.
//
// SPEC-QUESTION (09 [READ-86], [READ-88], 14 [CLI-44]): the text gives no bound on the repetition of the upload. "The client uploads again" after `CommitBlob` answers `BLOB_MISSING` could
// loop for ever against a store that never keeps the object. Taken: at most `MAX_UPLOAD_ROUNDS` rounds of `CreateUpload`, PUT and `CommitBlob` for one call; the call then fails with
// `UPLOAD_FAILED`. An action that names the blob goes on with `Reject{BLOB_MISSING}` and the client backoff, as a `RETRY` result does (14 [CLI-43]).
//
// SPEC-QUESTION (09 [READ-86]): the text gives no count for `CreateUploadResponse.upload_headers`. Taken: at most `MAX_UPLOAD_HEADERS` (counts.ts).

import { create } from "@bufbuild/protobuf";
import {
  CommitBlobRequestSchema,
  CreateUploadRequestSchema,
  DeleteBlobRequestSchema,
  GetBlobRequestSchema,
  type CreateUploadResponse,
  type GetBlobResponse,
} from "../gen/abpc/v2/sync_pb.js";
import { Code, RecordKind, RetryReason, SystemKind, type Record as RecordMessage } from "../gen/abpc/v2/types_pb.js";
import { parse, getMember } from "../rules/jsonv1/index.js";
import { fullJitter, HOLD_BACKOFF, resendDelay } from "./backoff.js";
import {
  BlobCache,
  BlobError,
  digestSha256,
  isBlobId,
  MAX_TRANSFER_ATTEMPTS,
  MAX_UPLOAD_ROUNDS,
  PLATFORM_HEADERS,
  sameBytes,
  newBlobId,
  type BlobCallOptions,
  type BlobData,
  type BlobErrorCode,
  type BlobRef,
  type StagedBlob,
  type StageOptions,
} from "./blob-store.js";
import { codeName, retryReasonName } from "./codes.js";
import { decodeContent, CanonError } from "./entry.js";
import { LocalRefusal, ProtocolViolation, RpcError } from "./errors.js";
import type { Follow } from "./follow.js";
import { PresenceLayer } from "./presence.js";
import type { OutboxEntry } from "./storage.js";

type Classified =
  | { kind: "retry"; reason: string; afterMs: number | undefined; hold?: boolean }
  | { kind: "auth" }
  | { kind: "protocol"; violation: ProtocolViolation }
  | { kind: "final"; error: BlobError };

/** What a failed blob call is (A.7, 09 [READ-4], [READ-90], 14 [CLI-44]). */
function classify(e: unknown): Classified {
  if (e instanceof ProtocolViolation) return { kind: "protocol", violation: e };
  if (!(e instanceof RpcError)) return { kind: "retry", reason: "ERROR", afterMs: undefined }; // a transport that failed in its own way: as `Unavailable` (A.7)
  const reject = e.reject;
  const final = (code: BlobErrorCode, detail: string): Classified => ({ kind: "final", error: new BlobError(code, detail, { status: e.status, ...(reject !== undefined ? { reject } : {}) }) });
  const named = (): BlobErrorCode => {
    const n = reject === undefined ? "" : codeName(reject.code);
    switch (n) {
      case "BLOB_MISSING":
      case "BLOB_DELETED":
      case "PAYLOAD_TOO_LARGE":
      case "QUOTA_EXCEEDED":
      case "IDEMPOTENCY_KEY_REUSED":
      case "PERMISSION_DENIED":
      case "KEYSPACE_ARCHIVED":
      case "KEYSPACE_DELETED":
      case "KEYSPACE_NOT_FOUND":
      case "HISTORY_DIVERGED":
      case "ADMIN_REFUSED":
      case "INVALID_KEY":
      case "INVALID_ENVELOPE":
        return n;
      default:
        return "UNKNOWN";
    }
  };
  switch (e.status) {
    case "Unauthenticated":
      return { kind: "auth" };
    case "PermissionDenied":
      if (reject?.code === Code.APP_SUSPENDED) return { kind: "retry", reason: "APP_SUSPENDED", afterMs: undefined, hold: true }; // 14 Table 14.1: retried with the hold backoff
      return final(reject?.code === Code.ACTOR_MISMATCH ? "UNKNOWN" : "PERMISSION_DENIED", "the call was refused");
    case "Unimplemented":
      return final("UNIMPLEMENTED", "the server does not offer the call");
    case "ResourceExhausted":
      if (e.retry?.reason === RetryReason.RATE_LIMITED) return { kind: "retry", reason: "RATE_LIMITED", afterMs: e.retry.afterMs };
      return final(named(), "a quota or a limit was reached");
    case "NotFound":
    case "FailedPrecondition":
    case "InvalidArgument":
    case "AlreadyExists":
    case "OutOfRange":
      // a status that no resend changes, except `Retry{...}` that a server attaches to a state that passes (`BLOB_PENDING` on a `GetBlob` is Unavailable, below)
      if (e.retry !== undefined) return { kind: "retry", reason: retryReasonName(e.retry.reason).replace(/^RETRY_REASON_/, ""), afterMs: e.retry.afterMs };
      return final(named(), reject === undefined ? `${e.status} with no detail` : `${e.status}`);
    default:
      // Unavailable, DeadlineExceeded, Aborted, Unknown, Internal, Cancelled, DataLoss: outcome unknown, or transient: resend with the client backoff (A.7, 14 [CLI-43])
      return { kind: "retry", reason: e.retry !== undefined ? retryReasonName(e.retry.reason).replace(/^RETRY_REASON_/, "") : e.status.toUpperCase(), afterMs: e.retry?.afterMs };
  }
}

export abstract class BlobLayer extends PresenceLayer {
  private readonly cache = new BlobCache(this.blobCacheBytes);
  /** Uploads of staged blobs that are under way, by blob: a second caller shares the first one's work. */
  private readonly uploads = new Map<string, Promise<void>>();
  private readonly sleepers = new Set<() => void>();

  /** 14 [CLI-61] and the sleepers: a call that waits for time stops when the engine does. */
  protected cancelTimers(): void {
    super.cancelTimers();
    for (const s of [...this.sleepers]) s();
  }

  protected resetRuntime(): void {
    super.resetRuntime();
    this.uploads.clear();
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Calls with their retries

  private blobSleep(ms: number, signal: AbortSignal | undefined): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let cancel: () => void = () => undefined;
      const stop = (): void => {
        cancel();
        this.sleepers.delete(stop);
        signal?.removeEventListener("abort", stop);
        reject(new BlobError("ABORTED", this.stopped ? "the engine stopped" : "the call was aborted"));
      };
      cancel = this.clock.setTimer(Math.max(0, Math.floor(ms)), () => {
        this.sleepers.delete(stop);
        signal?.removeEventListener("abort", stop);
        resolve();
      });
      this.sleepers.add(stop);
      signal?.addEventListener("abort", stop, { once: true });
    });
  }

  private check(o: BlobCallOptions | undefined): void {
    if (o?.signal?.aborted === true) throw new BlobError("ABORTED", "the call was aborted");
    if (this.stopped) throw new BlobError("ABORTED", "the engine stopped");
  }

  /** One call, resent as A.7 says (see the header). `keyspaceId` is the keyspace that a protocol violation holds. */
  private async rpc<T>(keyspaceId: string, o: BlobCallOptions | undefined, fn: () => Promise<T>): Promise<T> {
    let n = 0;
    let refreshed = false;
    for (;;) {
      this.check(o);
      try {
        return await fn();
      } catch (e) {
        if (e instanceof BlobError) throw e;
        const c = classify(e);
        switch (c.kind) {
          case "protocol":
            this.post(() => this.onProtocolViolation(c.violation, [keyspaceId])); // 16 [SEC-3]: held and reported as any message that breaks 02 [ARC-61]
            throw new BlobError("PROTOCOL_ERROR", c.violation.message);
          case "auth":
            if (this.noteRevoked(e)) throw new BlobError("SUBJECT_REVOKED", "the subject was revoked (10 [AUTH-51])");
            // A.7: "Refresh the token, then resend once; a second Unauthenticated for the request stops and reports"
            if (refreshed) throw new BlobError("UNAUTHENTICATED", "the token was refused again after a refresh", { status: "Unauthenticated" });
            refreshed = true;
            if (!(await this.refreshToken())) throw new BlobError("NO_TOKEN", "the app gave no valid token");
            break;
          case "retry": {
            const delayMs = c.hold === true ? fullJitter(n++, HOLD_BACKOFF, this.random) : resendDelay(n++, c.afterMs, this.random);
            o?.onWait?.({ reason: c.reason, delayMs });
            await this.blobSleep(delayMs, o?.signal);
            break;
          }
          default:
            throw c.error;
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Staging and uploading

  /** The id of a new blob (09 [READ-84]). */
  protected mintBlobId(): string {
    return newBlobId(this.random);
  }

  private blobKey(ks: string, id: string): string {
    return `${ks}\u0000${id}`;
  }

  /**
   * Keeps the bytes for the actions that will name the blob (09 [READ-94]) and returns its reference; nothing is sent. The engine uploads them in the background while it is online, and
   * again when an action that names the blob gets `BLOB_PENDING` or `BLOB_MISSING` (14 [CLI-44]). Refused locally for a blob over `limits.max_blob_bytes` (09 [READ-86]).
   */
  protected async stageBlobNow(keyspaceId: string, bytes: Uint8Array, o: StageOptions = {}): Promise<BlobRef> {
    if (bytes.length > this.limits.maxBlobBytes) throw new LocalRefusal("INVALID", `a blob of ${bytes.length} octets, over max_blob_bytes (${this.limits.maxBlobBytes}, 09 [READ-86])`);
    const contentType = o.contentType ?? "";
    // 09 [READ-86]: at most 255 octets of printable US-ASCII (%x20-7E)
    if (contentType.length > 255 || !/^[\x20-\x7e]*$/.test(contentType)) throw new LocalRefusal("INVALID", "a content type is at most 255 octets of printable US-ASCII (09 [READ-86])");
    const blobId = o.blobId ?? this.mintBlobId();
    if (!isBlobId(blobId)) throw new LocalRefusal("INVALID", `"${blobId}" is not a blob id: [A-Za-z0-9_-]{21,64} (09 [READ-84])`);
    const sha256 = await digestSha256(bytes);
    const copy = new Uint8Array(bytes); // the app may reuse its buffer
    const staged: StagedBlob = { keyspaceId, blobId, bytes: copy, sha256, size: copy.length, contentType, stagedAtMs: this.clock.now() };
    await this.blobStash.put(staged);
    return { keyspaceId, blobId, sha256, size: staged.size, contentType };
  }

  /** Starts the upload of a staged blob in the background, when the engine can reach the server; a failure leaves the blob staged for the repair of 14 [CLI-44]. */
  protected backgroundUpload(ref: BlobRef): void {
    if (!this.online()) return;
    this.detach(this.shareUpload(ref.keyspaceId, ref.blobId).catch(() => undefined));
  }

  /** Stages the bytes and uploads them: CreateUpload, the PUT, CommitBlob (09 [READ-86], [READ-88]). Resolves when the blob is committed. */
  protected async uploadBlobNow(keyspaceId: string, bytes: Uint8Array, o: StageOptions & BlobCallOptions = {}): Promise<BlobRef> {
    const ref = await this.stageBlobNow(keyspaceId, bytes, o);
    // a call with no signal and no `onWait` shares the work of an upload of the same blob that is under way (a follower tab repeats its command until it is answered, 14 [CLI-61])
    if (o.signal === undefined && o.onWait === undefined) await this.shareUpload(ref.keyspaceId, ref.blobId);
    else await this.uploadStaged(ref.keyspaceId, ref.blobId, o);
    return ref;
  }

  /** A shared upload of a staged blob: the call of the repair and the call of the app wait for the same work. */
  private shareUpload(keyspaceId: string, blobId: string): Promise<void> {
    const key = this.blobKey(keyspaceId, blobId);
    let p = this.uploads.get(key);
    if (p === undefined) {
      p = this.uploadStaged(keyspaceId, blobId, undefined).finally(() => this.uploads.delete(key));
      this.uploads.set(key, p);
    }
    return p;
  }

  /** The rounds of the upload of one staged blob (see the SPEC-QUESTION in the header). */
  private async uploadStaged(keyspaceId: string, blobId: string, o: BlobCallOptions | undefined): Promise<void> {
    const staged = await this.blobStash.get(keyspaceId, blobId);
    if (staged === undefined) throw new BlobError("BLOB_MISSING", `the engine holds no bytes of the blob ${blobId} to upload (09 [READ-94])`);
    let last = "";
    for (let round = 0; round < MAX_UPLOAD_ROUNDS; round++) {
      this.check(o);
      const up = await this.rpc(keyspaceId, o, () => this.transport.createUpload(create(CreateUploadRequestSchema, { keyspaceId, blobId, sha256: staged.sha256, size: BigInt(staged.size), contentType: staged.contentType })));
      if (up.alreadyCommitted) return; // 09 [READ-86]: committed with the same digest and size: nothing to send
      const sent = await this.put(up, staged, o);
      if (sent === "expired") {
        last = "the signed URL was refused as expired";
        continue; // a new `CreateUpload` gives a new URL (04 Table 4.11, "The validity of a blob upload")
      }
      try {
        await this.rpc(keyspaceId, o, () => this.transport.commitBlob(create(CommitBlobRequestSchema, { keyspaceId, blobId })));
        return;
      } catch (e) {
        if (e instanceof BlobError && e.code === "BLOB_MISSING") {
          last = "the store does not hold the object (09 [READ-88])";
          continue; // "the client uploads again"
        }
        throw e;
      }
    }
    throw new BlobError("UPLOAD_FAILED", `${last || "the upload did not commit"}, after ${MAX_UPLOAD_ROUNDS} rounds`);
  }

  /** The PUT of the bytes with exactly the headers of the call (09 [READ-86]); returns `expired` for a URL that the store refuses as invalid or expired. */
  private async put(up: CreateUploadResponse, staged: StagedBlob, o: BlobCallOptions | undefined): Promise<"sent" | "expired"> {
    const url = up.uploadUrl;
    if (!/^https?:\/\//i.test(url)) throw new BlobError("PROTOCOL_ERROR", "the upload URL is not an http(s) URL");
    const headers: Record<string, string> = {};
    for (const [k, v] of Object.entries(up.uploadHeaders)) if (!PLATFORM_HEADERS.has(k.toLowerCase())) headers[k] = v; // 03 [ID-27]: the values go as they are
    const http = this.blobHttp;
    for (let attempt = 0; ; attempt++) {
      this.check(o);
      let status: number | undefined;
      try {
        status = (await http.put(url, headers, staged.bytes, o?.signal !== undefined ? { signal: o.signal } : undefined)).status;
      } catch (e) {
        if (o?.signal?.aborted === true) throw new BlobError("ABORTED", "the call was aborted");
        status = undefined; // the connection failed: resent
        void e;
      }
      if (status !== undefined && status >= 200 && status < 300) return "sent";
      if (status === 403 || status === 401) return "expired"; // the URL stopped serving (04 Table 4.11): a repeated `CreateUpload` returns a new one
      // 09 [READ-87]: the store refuses a body whose length or checksum differs, which no resend changes
      if (status !== undefined && status !== 408 && status !== 429 && status < 500) throw new BlobError("UPLOAD_REFUSED", `the blob store answered ${status} to the PUT`);
      if (attempt + 1 >= MAX_TRANSFER_ATTEMPTS) throw new BlobError("UPLOAD_FAILED", `the PUT failed ${MAX_TRANSFER_ATTEMPTS} times (${status === undefined ? "no answer" : status})`);
      const delayMs = resendDelay(attempt, undefined, this.random);
      o?.onWait?.({ reason: status === undefined ? "PUT_FAILED" : `PUT_${status}`, delayMs });
      await this.blobSleep(delayMs, o?.signal);
    }
  }

  /**
   * 09 [READ-89], 14 [CLI-44]: an action got `Retry{BLOB_PENDING}` or `Reject{BLOB_MISSING}`: the upload of every blob that it names, whose bytes the engine holds, is completed or
   * repeated (an upload that is committed answers `already_committed`, and costs the one call). A blob that the engine holds no bytes of is left to the resend. The result of an
   * upload that fails is the resend's: the action meets `BLOB_MISSING` again and the backoff applies.
   */
  protected async repairBlobs(e: OutboxEntry): Promise<void> {
    let refs: readonly string[];
    try {
      refs = decodeContent(e.content).blobRefs;
    } catch (err) {
      if (err instanceof CanonError) return;
      throw err;
    }
    for (const id of new Set(refs)) {
      for (const ks of new Set([e.keyspaceId, ...(e.authoredIn !== undefined ? [e.authoredIn] : [])])) {
        if ((await this.blobStash.get(ks, id)) === undefined) continue;
        try {
          await this.shareUpload(ks, id);
        } catch {
          // the resend meets BLOB_MISSING again; its backoff applies
        }
        break;
      }
    }
  }

  /** The action retired: "A client keeps a blob's bytes locally until the action that references it is confirmed" (09 [READ-94]): the bytes it staged go. */
  protected onActionRetiredBlobs(e: OutboxEntry): void {
    let refs: readonly string[];
    try {
      refs = decodeContent(e.content).blobRefs;
    } catch {
      return;
    }
    if (refs.length === 0) return;
    const stash = this.blobStash;
    for (const id of new Set(refs)) {
      for (const ks of new Set([e.keyspaceId, ...(e.authoredIn !== undefined ? [e.authoredIn] : [])])) this.detach(stash.delete(ks, id));
    }
  }

  /** Releases the bytes of a staged blob that no action will name (09 [READ-94]: the engine's own cache; the server's blob stays until its sweep). */
  protected async releaseBlobNow(keyspaceId: string, blobId: string): Promise<void> {
    await this.blobStash.delete(keyspaceId, blobId);
  }

  /** What is staged: the bytes that the engine keeps for actions (09 [READ-94]). */
  async stagedBlobs(): Promise<{ keyspaceId: string; blobId: string; size: number }[]> {
    return this.blobStash.list();
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Downloading

  /** The bytes of a blob, checked against the digest that the server declared (16 [SEC-7]). Waits while the blob is pending or withheld, without a bound (09 [READ-90], 14 [CLI-44]). */
  async getBlob(keyspaceId: string, blobId: string, o: BlobCallOptions = {}): Promise<BlobData> {
    if (!isBlobId(blobId)) throw new LocalRefusal("INVALID", `"${blobId}" is not a blob id: [A-Za-z0-9_-]{21,64} (09 [READ-84])`);
    const hit = this.cache.get(keyspaceId, blobId);
    if (hit !== undefined) return hit;
    // the bytes that this engine staged are the blob's own: shown without a download (09 [READ-94])
    const staged = await this.blobStash.get(keyspaceId, blobId);
    if (staged !== undefined) return { keyspaceId, blobId, bytes: staged.bytes, sha256: staged.sha256, size: staged.size, declaredContentType: staged.contentType === "" ? "application/octet-stream" : staged.contentType };
    for (let round = 0; ; round++) {
      const info = await this.rpc(keyspaceId, o, () => this.transport.getBlob(create(GetBlobRequestSchema, { keyspaceId, blobId })));
      const data = await this.download(keyspaceId, blobId, info, o);
      if (data === "again" && round < 3) continue;
      if (data === "again") throw new BlobError("DOWNLOAD_FAILED", "the download URL kept being refused");
      this.cache.put(data);
      return data;
    }
  }

  private async download(keyspaceId: string, blobId: string, info: GetBlobResponse, o: BlobCallOptions): Promise<BlobData | "again"> {
    if (info.sha256.length !== 32) throw new BlobError("PROTOCOL_ERROR", "GetBlobResponse.sha256 is not 32 octets");
    if (!/^https?:\/\//i.test(info.downloadUrl)) throw new BlobError("PROTOCOL_ERROR", "the download URL is not an http(s) URL");
    if (info.size > BigInt(this.limits.maxBlobBytes)) throw new BlobError("PROTOCOL_ERROR", `GetBlobResponse.size (${info.size}) is over max_blob_bytes (${this.limits.maxBlobBytes})`);
    const size = Number(info.size);
    const http = this.blobHttp;
    for (let attempt = 0; ; attempt++) {
      this.check(o);
      let res: { status: number; bytes: Uint8Array; tooLarge?: boolean } | undefined;
      try {
        res = await http.get(info.downloadUrl, { maxBytes: size, ...(o.signal !== undefined ? { signal: o.signal } : {}) });
      } catch {
        if (o.signal?.aborted === true) throw new BlobError("ABORTED", "the call was aborted");
        res = undefined;
      }
      if (res !== undefined && res.status === 200 && res.tooLarge !== true) {
        // 16 [SEC-7]: "A client MUST check a downloaded blob against its `sha256`", and the size that the call declared
        if (res.bytes.length !== size) throw new BlobError("CHECKSUM_MISMATCH", `the download is ${res.bytes.length} octets, the blob's size is ${size}`);
        if (!sameBytes(await digestSha256(res.bytes), info.sha256)) throw new BlobError("CHECKSUM_MISMATCH", "the download's SHA-256 is not the blob's");
        return { keyspaceId, blobId, bytes: res.bytes, sha256: info.sha256, size, declaredContentType: info.contentType };
      }
      if (res?.tooLarge === true) throw new BlobError("CHECKSUM_MISMATCH", `the download is longer than the blob's size (${size})`);
      if (res !== undefined && (res.status === 403 || res.status === 404 || res.status === 401)) return "again"; // the URL expired, or the blob went: `GetBlob` says which
      if (attempt + 1 >= MAX_TRANSFER_ATTEMPTS) throw new BlobError("DOWNLOAD_FAILED", `the GET failed ${MAX_TRANSFER_ATTEMPTS} times (${res === undefined ? "no answer" : res.status})`);
      const delayMs = resendDelay(attempt, undefined, this.random);
      o.onWait?.({ reason: res === undefined ? "GET_FAILED" : `GET_${res.status}`, delayMs });
      await this.blobSleep(delayMs, o.signal);
    }
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Deleting

  /**
   * `DeleteBlob` (09 [READ-91]): deletes the blob on the server, for a caller that may (the uploader with a write grant, or `admin`). `reason` is required, at most 1,024 octets, with no
   * U+0000. Waits while the blob is withheld (`Retry{BLOB_WITHHELD}`, 14 [CLI-44]); `ADMIN_REFUSED` (a legal hold, a retention floor) is final. The cached and staged bytes go too.
   */
  protected async deleteBlobNow(keyspaceId: string, blobId: string, reason: string, o: BlobCallOptions = {}): Promise<void> {
    if (!isBlobId(blobId)) throw new LocalRefusal("INVALID", `"${blobId}" is not a blob id: [A-Za-z0-9_-]{21,64} (09 [READ-84])`);
    if (reason === "" || new TextEncoder().encode(reason).length > 1024 || reason.includes("\u0000")) throw new LocalRefusal("INVALID", "a DeleteBlob reason is required, at most 1,024 octets, with no U+0000 (09 [READ-91])");
    await this.rpc(keyspaceId, o, () => this.transport.deleteBlob(create(DeleteBlobRequestSchema, { keyspaceId, blobId, reason })));
    this.cache.delete(keyspaceId, blobId);
    await this.blobStash.delete(keyspaceId, blobId);
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // What the log and the loads say of blobs (14 [CLI-76], [CLI-66])

  /** A `BLOB_DELETED` record (06 [LOG-79]) names a blob: "a client MUST drop its cached bytes of that blob" (14 [CLI-76]). The staged bytes of an unsent action stay (09 [READ-94]). */
  protected blobDeletedRecord(f: Follow, rec: RecordMessage): void {
    if (rec.kind !== RecordKind.SYSTEM || rec.system?.kind !== SystemKind.BLOB_DELETED) return;
    let id: string | undefined;
    try {
      const body = parse(rec.system.body);
      const v = getMember(body, "blob_id");
      if (typeof v === "string") id = v;
    } catch {
      id = undefined;
    }
    if (id !== undefined) this.cache.delete(f.id, id);
    else this.cache.dropKeyspace(f.id); // a body that names no blob: what the client cannot judge it drops (a download that is needed again is one call)
  }

  /** A load with `resumed = false` installed confirmed state: it passed over the `BLOB_DELETED` records below its checkpoint, so the cache of the keyspace goes (14 [CLI-76]). */
  protected blobCacheLoadReplaced(f: Follow): void {
    this.cache.dropKeyspace(f.id);
  }

  /** The purge list (14 [CLI-66]): the downloaded bytes of the keyspace go. */
  protected blobsPurge(keyspaceId: string): void {
    this.cache.dropKeyspace(keyspaceId);
  }

  /** The downloaded blobs that the engine holds, for the sync status and for a test. */
  blobCacheStats(): { blobs: number; bytes: number } {
    return { blobs: this.cache.size, bytes: this.cache.bytes };
  }

  /** Whether the bytes of a blob are in the cache of downloads. */
  blobCached(keyspaceId: string, blobId: string): boolean {
    return this.cache.has(keyspaceId, blobId);
  }
}
