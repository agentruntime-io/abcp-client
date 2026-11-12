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
//  - [CLI-44] "the client completes or repeats the upload of the same blob bytes (09 §9.9), then resends": the engine does not wait for the answer to do the first upload. An action whose
//    `blob_refs` name a blob that it holds the bytes of, and does not know to be committed, is not sent until the upload is done (`blobsReady`, asked by the sender): the server then never
//    has to answer `BLOB_PENDING` or `BLOB_MISSING` for the first send. The upload is the call that `repairBlobs` makes, and an upload that fails does not stop the action: it is sent, and the
//    answer of the server starts the repair above, with its backoff. Only that action waits: unrelated actions are sent meanwhile (14 [CLI-33]). A blob that is committed and withheld answers
//    the upload `Retry{BLOB_WITHHELD}`, and the upload of an action gives up at it ([CLI-44]: the server accepts the action, and "holds no lane, keyspace or action for it"); the upload that
//    an app asks for keeps asking.
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
//  - [CLI-66] The blob cache is on the purge list of `ACCESS_REVOKED`: `blobsPurge`. The bytes that the engine staged are the principal's own, kept for actions that it authored: the purge of a
//    keyspace erases them too, but for those that an action that the engine still holds names (an outbox entry, a quarantined or dead-lettered one, an export-hook entry: [CLI-28], [CLI-66]
//    keep these, and `resubmitExport` names the same blobs again); and a sign-out that purges the partition erases all of them (`blobsPurgeAll`, [CLI-65]).
//  - [CLI-43], A.7 Every call is resent, unchanged, after `Unavailable`, `DeadlineExceeded`, `Aborted` and the statuses of a proxy, and after `Retry{RATE_LIMITED}`, with the client
//    backoff; `Unauthenticated` is resent once after a refresh of the token; `QUOTA_EXCEEDED`, `IDEMPOTENCY_KEY_REUSED` and every `Reject` are final.
//
// SPEC-QUESTION (09 [READ-86], [READ-88], 14 [CLI-44]): the text gives no bound on the repetition of the upload. "The client uploads again" after `CommitBlob` answers `BLOB_MISSING` could
// loop for ever against a store that never keeps the object. Taken: at most `MAX_UPLOAD_ROUNDS` rounds of `CreateUpload`, PUT and `CommitBlob` for one call; the call then fails with
// `UPLOAD_FAILED`. An action that names the blob goes on with `Reject{BLOB_MISSING}` and the client backoff, as a `RETRY` result does (14 [CLI-43]).
//
// SPEC-QUESTION (09 [READ-86]): the text gives no count for `CreateUploadResponse.upload_headers`. Taken: at most `MAX_UPLOAD_HEADERS` (counts.ts).
//
// 14 Table 14.1, the row for `BLOB_MISSING` for a name in an action's `blob_refs`, when the client holds the blob's bytes in no stash (09 [READ-94], 14 [CLI-44], [CLI-45]): "re-uploads on
// `BLOB_MISSING`" needs the bytes. When the server answers `BLOB_MISSING` to an action, and no stash of the engine holds the bytes of a blob that the action names (a memory stash after a restart, a
// cleared storage), no upload can mend it. The client keeps the action and holds it (scope `action`: the action alone), resends it with the hold backoff (04 Table 4.11: the probe of the hold, `armProbe`),
// and reports to the app that the bytes are missing, so that the app can supply them: the `held` event (code `BLOB_MISSING`) and the sync status name the blobs. The app supplies the bytes with
// `stageBlob` under the same blob id: the client uploads them and resends the action, and the hold clears when a resend gets another result than `Reject{BLOB_MISSING}`. The bound is the default one
// (14 [CLI-45]): an action that was ever sent is reported as outcome unknown when it is reached. The app may also cancel the action (nothing was recorded: `Reject{BLOB_MISSING}` records nothing,
// 09 [READ-89]). `Retry{BLOB_PENDING}` with no bytes is resent as before, and holds nothing.
//
// SPEC-QUESTION (14 Table 14.1, the row for `BLOB_MISSING`): the row says that the client "resends it with the hold backoff", and clears "when the app supplies the bytes: the client uploads them and a
// resend gets another result". Reading A (taken): the supply of the bytes is an event of its own, so the client uploads them and resends at once, as the probe of the hold, without waiting out the backoff
// (an app that staged the bytes again would otherwise wait up to an hour); the hold clears with that resend's result, and when the result is `BLOB_MISSING` again with the bytes now held, the row no longer
// applies and the action is repaired and resent as 14 [CLI-44] says for every other `BLOB_MISSING`. Reading B: every resend of a held action waits for the next time that the hold backoff gives, the one after
// the supply of the bytes included.
//
// SPEC-QUESTION (14 [CLI-66], [CLI-76], 09 [READ-94]): [CLI-66] lists "blob cache" on the purge list of a keyspace, and [CLI-76] speaks of "downloaded blob bytes" and says never "the bytes that
// an unsent action still carries (09 [READ-94])"; no rule names the bytes that a client staged. Reading A (taken): staged bytes are the principal's data and part of the local copy, so the
// purge of a keyspace erases the staged bytes of the keyspace that no action held by the engine names, and a sign-out of a personal partition erases them all; the bytes that a held action
// names are the action's ([READ-94]: kept until it is confirmed) and stay with it. Reading B: the purge list never touches staged bytes, which stay until their action is confirmed. Reading C:
// the purge erases every staged byte of the keyspace, and an action that was quarantined and is re-submitted (`resubmitExport`) meets `BLOB_MISSING` for good.
//
// SPEC-QUESTION (14 Table 14.1, 09 [READ-4]): a blob call that a frozen keyspace refuses answers FailedPrecondition with `Reject{HISTORY_DIVERGED}`; the row of the table for `HISTORY_DIVERGED` says
// "as a code" and holds the keyspace, without saying that a call of the blob family is one that does. Taken: yes, as any call is (a `BlobError` `HISTORY_DIVERGED` is final for the call, and the
// keyspace is held as for a load or an action that gets it). Not taken for `Retry{BLOB_WITHHELD}`, which [CLI-44] says is never the `HISTORY_DIVERGED` of the table.

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
import type { Runtime } from "./core.js";
import {
  BlobCache,
  BlobError,
  blobUrlAllowed,
  digestSha256,
  isBlobId,
  MAX_TRANSFER_ATTEMPTS,
  MAX_UPLOAD_ROUNDS,
  PLATFORM_HEADERS,
  sameBytes,
  newBlobId,
  STAGED_GRACE_MS,
  STAGED_SWEEP_MS,
  type BlobCallOptions,
  type BlobData,
  type BlobErrorCode,
  type BlobRef,
  type BlobStash,
  type RepairResult,
  type StagedBlob,
  type StageOptions,
} from "./blob-store.js";
import { codeName, retryReasonName } from "./codes.js";
import { decodeContent, CanonError } from "./entry.js";
import { LocalRefusal, ProtocolViolation, RpcError, storageCauseOf } from "./errors.js";
import type { Follow } from "./follow.js";
import { PresenceLayer } from "./presence.js";
import type { OutboxEntry } from "./storage.js";

/** The part of an action that names blobs: an outbox entry, a dead letter, an export-hook entry. */
export interface HeldContent {
  keyspaceId: string;
  content: Uint8Array;
  /** The keyspace that the action was authored in, when a split moved it (14 [CLI-52]). */
  authoredIn?: string;
}

/**
 * The stash lists the blob and cannot give its bytes (a damaged file or row, `BlobStash.get` answers nothing): the engine holds none of it (09 [READ-94], V23 SHOULD-3). It is `BLOB_MISSING` to
 * the call that asked for the upload, and the repair of an action counts the blob as lacking, so that the hold for lost bytes starts (14 [CLI-44], [CLI-45]) instead of the action being sent
 * again for ever.
 */
class NoStagedBytes extends BlobError {
  constructor(blobId: string) {
    super("BLOB_MISSING", `the engine holds no bytes of the blob ${blobId} to upload (09 [READ-94])`);
  }
}

/** A transfer that took longer than the engine allows it (`EngineConfig.blobs.transferTimeoutMs`): counted as a failed attempt. */
class TransferTimeout extends Error {
  constructor(readonly ms: number) {
    super(`the transfer took longer than ${ms} ms`);
    this.name = "TransferTimeout";
  }
}

/** What a call of the engine takes besides the app's options. */
interface CallOptions extends BlobCallOptions {
  /**
   * A `Retry{BLOB_WITHHELD}` ends the call with `BLOB_WITHHELD` instead of being asked again. [CLI-44]: the server accepts an action that names a withheld blob, "so that the client sends it, and
   * every action behind it on its lane", and "holds no lane, keyspace or action for it": the upload that the engine makes for an action does not wait for a blob that is committed and withheld.
   * The calls that an app makes keep asking: "the app decides when to stop asking".
   */
  giveUpWhenWithheld?: boolean;
}

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
  /**
   * The staged blobs that this run knows to be committed (its own `CommitBlob` was answered, or `CreateUpload` said `already_committed`), by blob: an action that names only these is sent at once
   * (14 [CLI-44]). A fact of the server that the engine may hold stale (a restore, a sweep), so it is memory only, and the server's answer to an action is still the last word.
   */
  private readonly committed = new Set<string>();
  private readonly sleepers = new Set<() => void>();
  /** The PUTs and GETs of the engine that are under way: they end when the engine stops (a browser's `fetch` has no timeout, and a hung one would hold a call for ever). */
  private readonly transferAborts = new Set<AbortController>();
  /** What the stash holds, by blob (`blobKey`) with the octets: loaded from `list()` once, and kept as the engine puts and deletes. Undefined until it is loaded (or when the stash fails). */
  private stashIndex: Map<string, number> | undefined;
  private stashIndexLoading: Promise<Map<string, number> | undefined> | undefined;
  /** The octets of blobs that are being put (the cap of `maxStagedBytes` counts them before they are in the index). */
  private stagingBytes = 0;
  private sweepTimer: (() => void) | undefined;

  /** 14 [CLI-61] and the sleepers: a call that waits for time stops when the engine does; so do the transfers that are under way. */
  protected cancelTimers(): void {
    super.cancelTimers();
    for (const s of [...this.sleepers]) s();
    for (const c of [...this.transferAborts]) c.abort();
    this.sweepTimer = undefined; // an engine timer: `super.cancelTimers` cancelled it
  }

  protected resetRuntime(): void {
    super.resetRuntime();
    this.uploads.clear();
    this.committed.clear();
    this.sweepTimer?.();
    this.sweepTimer = undefined;
  }

  protected useBlobStash(stash: BlobStash): void {
    super.useBlobStash(stash);
    this.stashIndex = undefined;
    this.stashIndexLoading = undefined;
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // The stash, with its count (09 [READ-94]): every change of it by the engine goes through these, so that the sync status can say how much is staged and the cap can hold

  /** Whether the bytes of the stash outlive the process (`BlobStash.durable`; a stash that does not say is the app's, and taken as durable). */
  blobStashMode(): "memory" | "durable" {
    return this.blobStash.durable === false ? "memory" : "durable";
  }

  /** The staged blobs and octets, as the engine counts them (0 until the stash has been read once). */
  protected stashStats(): { count: number; bytes: number } {
    let bytes = 0;
    for (const n of this.stashIndex?.values() ?? []) bytes += n;
    return { count: this.stashIndex?.size ?? 0, bytes };
  }

  /** Reads what the stash holds, once; a stash that fails leaves the engine without a count (and without the cap), and the failure is recorded. */
  protected stashIndexNow(): Promise<Map<string, number> | undefined> {
    if (this.stashIndex !== undefined) return Promise.resolve(this.stashIndex);
    this.stashIndexLoading ??= this.blobStash.list().then(
      (rows) => {
        const m = new Map<string, number>();
        for (const r of rows) m.set(this.blobKey(r.keyspaceId, r.blobId), r.size);
        this.stashIndex = m;
        this.onRuntimeChanged();
        return m;
      },
      (e: unknown) => {
        this.stashIndexLoading = undefined;
        this.noteInternalError(e);
        return undefined;
      },
    );
    return this.stashIndexLoading;
  }

  /** Forgets the count and reads it again: the flusher that took the role finds what the one before it staged. */
  protected stashIndexReload(): void {
    this.stashIndex = undefined;
    this.stashIndexLoading = undefined;
    this.detach(this.stashIndexNow().then(() => undefined));
  }

  private async stashPut(staged: StagedBlob): Promise<void> {
    const idx = await this.stashIndexNow();
    const key = this.blobKey(staged.keyspaceId, staged.blobId);
    if (idx !== undefined) {
      let bytes = 0;
      for (const [k, n] of idx) if (k !== key) bytes += n;
      if (bytes + this.stagingBytes + staged.size > this.blobMaxStaged) {
        throw new LocalRefusal("STORAGE", `the stash holds ${bytes} octets of staged bytes, and ${staged.size} more would pass maxStagedBytes (${this.blobMaxStaged}, 09 [READ-94])`, "QUOTA");
      }
    }
    this.stagingBytes += staged.size;
    try {
      await this.blobStash.put(staged);
    } finally {
      this.stagingBytes -= staged.size;
    }
    this.stashIndex?.set(key, staged.size);
    this.onRuntimeChanged();
  }

  private async stashDelete(keyspaceId: string, blobId: string): Promise<void> {
    await this.blobStash.delete(keyspaceId, blobId);
    if (this.stashIndex?.delete(this.blobKey(keyspaceId, blobId)) === true) this.onRuntimeChanged();
  }

  private async stashClear(): Promise<void> {
    await this.blobStash.clear();
    this.stashIndex = new Map();
    this.onRuntimeChanged();
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

  /**
   * One PUT or GET under the engine's bounds (14 [CLI-44], 16 [SEC-1]): it ends when the app aborts the call (`ABORTED`), when the engine stops (`ABORTED`), and when it has taken longer than
   * `blobTransferTimeout(octets)` on the engine's clock (a `TransferTimeout`, which the caller counts as a failed attempt). A `BlobHttp` that ignores its signal is raced against it, so a hung one
   * never holds the call.
   */
  private async bounded<T>(o: BlobCallOptions | undefined, octets: number, run: (signal: AbortSignal) => Promise<T>): Promise<T> {
    const ctl = new AbortController();
    this.transferAborts.add(ctl);
    const onAbort = (): void => ctl.abort();
    o?.signal?.addEventListener("abort", onAbort, { once: true });
    if (o?.signal?.aborted === true) ctl.abort();
    const ms = Math.max(1, Math.floor(this.blobTransferTimeout(octets)));
    let timedOut = false;
    const cancelTimer = this.clock.setTimer(ms, () => {
      timedOut = true;
      ctl.abort();
    });
    try {
      const call = run(ctl.signal);
      call.catch(() => undefined); // the failure of a call that was given up on is nobody's
      return await Promise.race([
        call,
        new Promise<never>((_, reject) => {
          const fail = (): void => reject(new Error("the transfer was aborted"));
          if (ctl.signal.aborted) fail();
          else ctl.signal.addEventListener("abort", fail, { once: true });
        }),
      ]);
    } catch (e) {
      if (o?.signal?.aborted === true) throw new BlobError("ABORTED", "the call was aborted");
      if (this.stopped) throw new BlobError("ABORTED", "the engine stopped");
      if (timedOut) throw new TransferTimeout(ms);
      throw e;
    } finally {
      cancelTimer();
      o?.signal?.removeEventListener("abort", onAbort);
      this.transferAborts.delete(ctl);
    }
  }

  private check(o: BlobCallOptions | undefined): void {
    if (o?.signal?.aborted === true) throw new BlobError("ABORTED", "the call was aborted");
    if (this.stopped) throw new BlobError("ABORTED", "the engine stopped");
  }

  /** One call, resent as A.7 says (see the header). `keyspaceId` is the keyspace that a protocol violation holds. */
  private async rpc<T>(keyspaceId: string, o: CallOptions | undefined, fn: () => Promise<T>): Promise<T> {
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
            if (o?.giveUpWhenWithheld === true && c.reason === "BLOB_WITHHELD") throw new BlobError("BLOB_WITHHELD", "the blob is withheld (12 [ADM-103]); an action that names it is sent all the same (14 [CLI-44])", { status: e instanceof RpcError ? e.status : "Unavailable" });
            const delayMs = c.hold === true ? fullJitter(n++, HOLD_BACKOFF, this.random) : resendDelay(n++, c.afterMs, this.random);
            o?.onWait?.({ reason: c.reason, delayMs });
            await this.blobSleep(delayMs, o?.signal);
            break;
          }
          default:
            // 14 Table 14.1: `HISTORY_DIVERGED` "as a code" holds the keyspace, from a blob call as from any other (09 [READ-4]: a frozen keyspace refuses `CreateUpload`, `CommitBlob` and `DeleteBlob`)
            if (c.error.code === "HISTORY_DIVERGED") this.post(() => this.holdDiverged(keyspaceId));
            throw c.error;
        }
      }
    }
  }

  /** The keyspace is held (14 Table 14.1): the engine sends nothing to it, and learns of its reopening from a `LoadState` that it repeats with the hold backoff (09 [READ-70]). */
  private holdDiverged(keyspaceId: string): void {
    // a keyspace that the engine does not follow has no copy to load again, and nothing of it to send: nothing is held
    if (this.follows.get(keyspaceId) === undefined || this.holds.has("HISTORY_DIVERGED", keyspaceId)) return;
    this.holdKeyspace("HISTORY_DIVERGED", codeName(Code.HISTORY_DIVERGED), keyspaceId);
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
    // 14 [CLI-21]: "A client MUST NOT fall back to memory-only storage silently": the bytes that a durable outbox's actions name are not kept in memory only (09 [READ-94]); nothing is staged
    if (this.storage.mode !== "memory" && this.blobStash.durable === false) {
      throw new LocalRefusal("REFUSED", `the outbox is ${this.storage.mode} and the stash of blob bytes is memory only: an action that names the blob would lose its bytes at a restart (09 [READ-94], 14 [CLI-21]); give the engine a durable stash (the platform's, or EngineConfig.blobs.stash)`);
    }
    if (bytes.length > this.limits.maxBlobBytes) throw new LocalRefusal("INVALID", `a blob of ${bytes.length} octets, over max_blob_bytes (${this.limits.maxBlobBytes}, 09 [READ-86])`);
    const contentType = o.contentType ?? "";
    // 09 [READ-86]: at most 255 octets of printable US-ASCII (%x20-7E)
    if (contentType.length > 255 || !/^[\x20-\x7e]*$/.test(contentType)) throw new LocalRefusal("INVALID", "a content type is at most 255 octets of printable US-ASCII (09 [READ-86])");
    const blobId = o.blobId ?? this.mintBlobId();
    if (!isBlobId(blobId)) throw new LocalRefusal("INVALID", `"${blobId}" is not a blob id: [A-Za-z0-9_-]{21,64} (09 [READ-84])`);
    const sha256 = await digestSha256(bytes);
    // a blob id that the caller supplies and an action that this engine holds already names, with other bytes: the action's bytes are not replaced (09 [READ-94]); the same bytes are staged again
    if (o.blobId !== undefined) {
      const have = await this.blobStash.get(keyspaceId, blobId).catch(() => undefined);
      if (have !== undefined && !sameBytes(have.sha256, sha256) && this.namedByHeldActions().has(this.blobKey(keyspaceId, blobId))) {
        throw new LocalRefusal("REFUSED", `the blob ${blobId} is staged with other bytes, and an action that the engine holds names it (09 [READ-94])`);
      }
    }
    const copy = new Uint8Array(bytes); // the app may reuse its buffer
    const staged: StagedBlob = { keyspaceId, blobId, bytes: copy, sha256, size: copy.length, contentType, stagedAtMs: this.clock.now() };
    try {
      await this.stashPut(staged);
    } catch (e) {
      if (e instanceof LocalRefusal) throw e;
      // a durable stash can fail (a quota, a database that another tab closed): nothing was staged, and the app is told as for an action that cannot be stored (14 [CLI-21])
      throw new LocalRefusal("STORAGE", `the bytes of the blob could not be kept: ${e instanceof Error ? e.message : String(e)}`, storageCauseOf(e));
    }
    // an action that is held because the client held these bytes in no stash is resent (14 Table 14.1, `BLOB_MISSING`; see the header)
    this.post(() => this.bytesStagedAgain(keyspaceId, blobId));
    return { keyspaceId, blobId, sha256, size: staged.size, contentType };
  }

  /** Starts the upload of a staged blob in the background, when the engine can reach the server; a failure leaves the blob staged for the repair of 14 [CLI-44]. */
  protected backgroundUpload(ref: BlobRef): void {
    if (!this.online()) return;
    this.detach(this.shareUpload(ref.keyspaceId, ref.blobId, true).catch(() => undefined));
  }

  /** Stages the bytes and uploads them: CreateUpload, the PUT, CommitBlob (09 [READ-86], [READ-88]). Resolves when the blob is committed. */
  protected async uploadBlobNow(keyspaceId: string, bytes: Uint8Array, o: StageOptions & BlobCallOptions = {}): Promise<BlobRef> {
    const ref = await this.stageBlobNow(keyspaceId, bytes, o);
    // a call with no signal and no `onWait` shares the work of an upload of the same blob that is under way (a follower tab repeats its command until it is answered, 14 [CLI-61])
    if (o.signal === undefined && o.onWait === undefined) await this.shareUpload(ref.keyspaceId, ref.blobId);
    else await this.uploadStaged(ref.keyspaceId, ref.blobId, o);
    return ref;
  }

  /**
   * A shared upload of a staged blob: the call of the repair, the upload before the first send and the call of the app wait for the same work. `forAction`: it is for an action, and gives up
   * when the blob is withheld (see `CallOptions`); an upload that the app waits for is not shared with it, since it asks again.
   */
  private shareUpload(keyspaceId: string, blobId: string, forAction = false): Promise<void> {
    const key = this.blobKey(keyspaceId, blobId) + (forAction ? "\u0000action" : "");
    let p = this.uploads.get(key);
    if (p === undefined) {
      p = this.uploadStaged(keyspaceId, blobId, forAction ? { giveUpWhenWithheld: true } : undefined).finally(() => this.uploads.delete(key));
      this.uploads.set(key, p);
    }
    return p;
  }

  /** The rounds of the upload of one staged blob (see the SPEC-QUESTION in the header). */
  private async uploadStaged(keyspaceId: string, blobId: string, o: CallOptions | undefined): Promise<void> {
    const staged = await this.blobStash.get(keyspaceId, blobId);
    if (staged === undefined) throw new NoStagedBytes(blobId);
    let last = "";
    for (let round = 0; round < MAX_UPLOAD_ROUNDS; round++) {
      this.check(o);
      const up = await this.rpc(keyspaceId, o, () => this.transport.createUpload(create(CreateUploadRequestSchema, { keyspaceId, blobId, sha256: staged.sha256, size: BigInt(staged.size), contentType: staged.contentType })));
      if (up.alreadyCommitted) {
        this.committed.add(this.blobKey(keyspaceId, blobId));
        return; // 09 [READ-86]: committed with the same digest and size: nothing to send
      }
      const sent = await this.put(up, staged, o);
      if (sent === "expired") {
        last = "the signed URL was refused as expired";
        continue; // a new `CreateUpload` gives a new URL (04 Table 4.11, "The validity of a blob upload")
      }
      try {
        await this.rpc(keyspaceId, o, () => this.transport.commitBlob(create(CommitBlobRequestSchema, { keyspaceId, blobId })));
        this.committed.add(this.blobKey(keyspaceId, blobId));
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
    if (!blobUrlAllowed(url, this.blobAllowInsecure)) throw new BlobError("PROTOCOL_ERROR", "the upload URL is not an https URL (http only on the loopback, 02 note after [ARC-56], 16 [SEC-45])");
    const headers: Record<string, string> = {};
    for (const [k, v] of Object.entries(up.uploadHeaders)) if (!PLATFORM_HEADERS.has(k.toLowerCase())) headers[k] = v; // 03 [ID-27]: the values go as they are
    const http = this.blobHttp;
    for (let attempt = 0; ; attempt++) {
      this.check(o);
      let status: number | undefined;
      try {
        // 14 [CLI-44]: a PUT that never answers is a failed attempt after its time (a browser's `fetch` has none), and ends with the call or the engine
        status = (await this.bounded(o, staged.size, (signal) => http.put(url, headers, staged.bytes, { signal }))).status;
      } catch (e) {
        if (e instanceof BlobError) throw e;
        status = undefined; // the connection failed, or timed out: resent
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
  protected async repairBlobs(e: OutboxEntry): Promise<RepairResult> {
    let refs: readonly string[];
    try {
      refs = decodeContent(e.content).blobRefs;
    } catch (err) {
      if (err instanceof CanonError) return { uploaded: 0, lacking: [] };
      throw err;
    }
    // the server says that a blob is not committed: what this run knew of it is stale
    for (const id of refs) for (const ks of this.stashKeyspaces(e)) this.committed.delete(this.blobKey(ks, id));
    return this.uploadNamed(e, refs, false);
  }

  /** The keyspaces in which the bytes of the blobs of an action may be staged: the one that it is in now, and the one that it was authored in, when a split moved it (14 [CLI-52]). */
  private stashKeyspaces(e: OutboxEntry): Set<string> {
    return new Set([e.keyspaceId, ...(e.authoredIn !== undefined ? [e.authoredIn] : [])]);
  }

  /**
   * Completes or repeats the upload of each blob that `refs` names and the engine holds the bytes of (09 [READ-89], 14 [CLI-44]): for each, the first keyspace of the action that has them.
   * `skipCommitted`: a blob that this run knows to be committed is not asked about again. A blob that the engine holds no bytes of is left to the server's answer, and an upload that fails is
   * too: the action meets `BLOB_MISSING` again, and its backoff applies.
   */
  private async uploadNamed(e: OutboxEntry, refs: Iterable<string>, skipCommitted: boolean): Promise<RepairResult> {
    let uploaded = 0;
    const lacking: string[] = [];
    for (const id of new Set(refs)) {
      let held = false;
      for (const ks of this.stashKeyspaces(e)) {
        if (!(await this.blobStash.has(ks, id))) continue;
        if (skipCommitted && this.committed.has(this.blobKey(ks, id))) {
          held = true;
          break;
        }
        try {
          await this.shareUpload(ks, id, true);
        } catch (err) {
          // V23 SHOULD-3: a listed entry that gives no bytes is bytes that the engine does not hold; the next keyspace of the action may have them
          if (err instanceof NoStagedBytes) continue;
          // the send meets BLOB_MISSING or BLOB_PENDING again; its backoff applies
        }
        held = true;
        uploaded++;
        break;
      }
      if (!held) lacking.push(id);
    }
    return { uploaded, lacking };
  }

  /**
   * The bytes of blobs that a held action names are staged again (`stageBlob` under the same blob id): the app supplies the bytes that the hold of 14 Table 14.1 (`BLOB_MISSING`) waits for. "The client
   * uploads them and a resend gets another result": the upload is made before the send (`blobsReady`), and the action is resent at once, as the probe of the hold (see the SPEC-QUESTION in the header).
   * The hold itself clears with the result of that resend (`clearProbeHolds`).
   */
  private bytesStagedAgain(keyspaceId: string, blobId: string): void {
    for (const h of this.holds.all()) {
      if (h.kind !== "BLOB_MISSING" || h.blobIds === undefined || !h.blobIds.includes(blobId)) continue;
      const e = h.mutationId === undefined ? undefined : this.entries.get(h.mutationId);
      if (e === undefined || !this.stashKeyspaces(e).has(keyspaceId)) continue;
      h.blobIds = h.blobIds.filter((id) => id !== blobId);
      if (h.blobIds.length > 0) continue; // the other blobs are still missing
      const rt = this.rt(e.mutationId);
      rt.blobs = "unchecked"; // the upload before the send is made again, from the bytes that were staged
      rt.blobRepairs = 0;
      rt.nextAttemptAt = 0;
      this.probeNow(h);
      this.schedulePump();
    }
  }

  /**
   * 14 [CLI-44], 09 [READ-89]: whether the action may be sent now as far as its blobs go (asked by the sender when everything else lets the action go). An action that names a blob that the
   * engine holds the bytes of, and does not know to be committed, is first uploaded: the engine finds the staged blobs, completes or repeats each upload, and sends the action after that, so
   * that the server does not first answer `BLOB_PENDING` or `BLOB_MISSING`. It is the upload that the answer would have caused, made before the first send; the repair of the answer stays for the
   * action that gets one all the same (a store that lost the object, a blob that a restore took, an upload that failed here). An action that names no blob, or none that is staged here, or only
   * blobs that are committed, is not delayed.
   */
  protected blobsReady(e: OutboxEntry): boolean {
    const rt = this.rt(e.mutationId);
    if (rt.blobs === "ready") return true;
    if (rt.blobs === "checking") return false;
    const refs = this.content(e)?.blobRefs ?? [];
    if (refs.every((id) => this.committed.has(this.blobKey(e.keyspaceId, id)))) {
      rt.blobs = "ready"; // no blob, or the blobs that this run uploaded or found committed
      return true;
    }
    rt.blobs = "checking";
    this.detach(this.gateBlobs(e, rt, refs));
    return false;
  }

  /** The upload before the first send (`blobsReady`): outside the queue, as the calls of the engine are; the entry is sent after it, whatever came of it, by a task of the queue. */
  private async gateBlobs(e: OutboxEntry, rt: Runtime, refs: readonly string[]): Promise<void> {
    try {
      await this.uploadNamed(e, refs, true);
    } catch (err) {
      if (this.stopped) return;
      this.noteInternalError(err); // the stash failed (a database that another tab closed): the action is sent, and the server's answer is the repair's to act on
    }
    if (this.stopped) return;
    await this.run(() => {
      if (this.runtimes.get(e.mutationId) !== rt) return; // the runtime was reset (a handover): the new one checks again
      rt.blobs = "ready";
      this.schedulePump();
    });
  }

  /**
   * The action left the engine's hands: it was confirmed, or cancelled, or the app acknowledged its dead letter or its export entry. "A client keeps a blob's bytes locally until the action that
   * references it is confirmed" (09 [READ-94]): the bytes that it staged go, but those that another action held by the engine still names.
   */
  protected onActionRetiredBlobs(e: HeldContent): void {
    this.detach(this.dropStagedOf(e).catch((err: unknown) => this.noteInternalError(err)));
  }

  /** Erases the staged bytes of the blobs that an action names, except those that an action that the engine still holds names, and what this run knew of their uploads. */
  protected async dropStagedOf(e: HeldContent): Promise<void> {
    let refs: readonly string[];
    try {
      refs = decodeContent(e.content).blobRefs;
    } catch {
      return;
    }
    const keyspaces = new Set([e.keyspaceId, ...(e.authoredIn !== undefined ? [e.authoredIn] : [])]);
    for (const id of new Set(refs)) {
      for (const ks of keyspaces) {
        // judged when the bytes are about to go: another action may have been authored while this one waited for the stash
        if (this.namedByHeldActions().has(this.blobKey(ks, id))) continue;
        this.committed.delete(this.blobKey(ks, id));
        await this.stashDelete(ks, id);
      }
    }
  }

  /** Releases the bytes of a staged blob that no action will name (09 [READ-94]: the engine's own cache; the server's blob stays until its sweep). */
  protected async releaseBlobNow(keyspaceId: string, blobId: string): Promise<void> {
    await this.stashDelete(keyspaceId, blobId);
  }

  /**
   * Starts the collection of staged bytes that no action names (09 [READ-94]): at once, and every `STAGED_SWEEP_MS`, a staged blob that no action held by the engine names and that was staged more than
   * `STAGED_GRACE_MS` ago is erased: a stage that no action followed (a tab that went away between `stageBlob` and `submit`), or bytes that an acknowledgement or a restart left. A flusher's job: the
   * stash is the partition's, and the tabs share it.
   */
  protected startStagedSweep(): void {
    this.sweepTimer?.();
    this.stashIndexReload();
    this.detach(this.sweepStaged());
    this.sweepTimer = this.after(STAGED_SWEEP_MS, () => this.startStagedSweep());
  }

  protected async sweepStaged(): Promise<void> {
    let rows: Awaited<ReturnType<BlobStash["list"]>>;
    try {
      rows = await this.blobStash.list();
    } catch (e) {
      this.noteInternalError(e);
      return;
    }
    for (const s of rows) {
      if (this.stopped || this.role !== "flusher") return;
      if (s.stagedAtMs === undefined || this.clock.now() - s.stagedAtMs <= STAGED_GRACE_MS) continue; // a stash that keeps no time never collects by age
      if (this.namedByHeldActions().has(this.blobKey(s.keyspaceId, s.blobId))) continue;
      try {
        await this.stashDelete(s.keyspaceId, s.blobId);
      } catch (e) {
        this.noteInternalError(e);
      }
    }
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
    if (!blobUrlAllowed(info.downloadUrl, this.blobAllowInsecure)) throw new BlobError("PROTOCOL_ERROR", "the download URL is not an https URL (http only on the loopback, 02 note after [ARC-56], 16 [SEC-45])");
    if (info.size > BigInt(this.limits.maxBlobBytes)) throw new BlobError("PROTOCOL_ERROR", `GetBlobResponse.size (${info.size}) is over max_blob_bytes (${this.limits.maxBlobBytes})`);
    // 16 [SEC-1]: `max_blob_bytes` is the server's and has no protocol maximum, so the app's bound is the one that holds what the call buffers
    const cap = o.maxBytes ?? this.blobMaxDownload;
    if (info.size > BigInt(cap)) throw new BlobError("DOWNLOAD_TOO_LARGE", `the blob is ${info.size} octets, over what this call buffers (${cap}): raise BlobCallOptions.maxBytes or EngineConfig.blobs.maxDownloadBytes`);
    const size = Number(info.size);
    const http = this.blobHttp;
    for (let attempt = 0; ; attempt++) {
      this.check(o);
      let res: { status: number; bytes: Uint8Array; tooLarge?: boolean } | undefined;
      try {
        res = await this.bounded(o, size, (signal) => http.get(info.downloadUrl, { maxBytes: size, signal }));
      } catch (e) {
        if (e instanceof BlobError) throw e;
        res = undefined; // the connection failed, or timed out: asked again
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
    this.committed.delete(this.blobKey(keyspaceId, blobId));
    await this.stashDelete(keyspaceId, blobId);
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
    if (id !== undefined) {
      this.cache.delete(f.id, id);
      this.committed.delete(this.blobKey(f.id, id));
    } else this.cache.dropKeyspace(f.id); // a body that names no blob: what the client cannot judge it drops (a download that is needed again is one call)
  }

  /** The blobs of the keyspace that the engine holds bytes of: the cache of downloads, the staged bytes, and the blobs that this run knows to be committed (14 [CLI-76], `erasure.ts`). */
  protected async blobIdsHeld(keyspaceId: string): Promise<Set<string>> {
    const out = new Set(this.cache.ids(keyspaceId));
    for (const s of await this.blobStash.list()) if (s.keyspaceId === keyspaceId) out.add(s.blobId);
    return out;
  }

  /**
   * A redaction erased the only values that named these blobs (14 [CLI-76], `erasure.ts`): their cached bytes go, and so do the bytes that the engine staged for them, but those that a pending action
   * names (the outbox, the locked entries of other principals): the user's new writes stay, with what they carry (09 [READ-94]). A dead-letter entry or an export-hook entry that names one
   * keeps its place with its values erased, and no longer keeps the bytes.
   *
   * FBA-28, 14 [CLI-76] "or before that advance, so that a crash repeats the purge and never skips it": the stash is no part of the outbox's transaction, so the caller awaits this
   * before the transaction that moves the persisted cursor past the record. A stash that fails leaves its bytes, the others are still tried, and the first failure is thrown: the
   * caller does not move the cursor, and tries the erasure again.
   */
  protected async blobsErased(keyspaceId: string, ids: ReadonlySet<string>): Promise<void> {
    for (const id of ids) {
      this.cache.delete(keyspaceId, id);
      this.committed.delete(this.blobKey(keyspaceId, id));
    }
    const pending = new Set<string>();
    for (const e of [...this.entries.values(), ...this.lockedEntries]) {
      if (!this.stashKeyspaces(e).has(keyspaceId)) continue;
      try {
        for (const id of decodeContent(e.content).blobRefs) pending.add(id);
      } catch (err) {
        if (!(err instanceof CanonError)) throw err;
      }
    }
    let failed: unknown;
    for (const id of ids) {
      if (pending.has(id)) continue;
      try {
        await this.stashDelete(keyspaceId, id);
      } catch (e) {
        failed ??= e;
      }
    }
    if (failed !== undefined) throw failed;
  }

  /** A load with `resumed = false` installed confirmed state: it passed over the `BLOB_DELETED` records below its checkpoint, so the cache of the keyspace goes (14 [CLI-76]). */
  protected blobCacheLoadReplaced(f: Follow): void {
    this.cache.dropKeyspace(f.id);
  }

  /**
   * The purge list (14 [CLI-66], [CLI-77]): the downloaded bytes of the keyspace go, and so do the bytes that the engine staged in it for actions, but those that an action held by the engine
   * still names (see the SPEC-QUESTION in the header, reading A). A stash that fails leaves its bytes where they are and the failure is recorded: it purges nothing more, and stops no other
   * purge.
   */
  protected async blobsPurge(keyspaceId: string): Promise<void> {
    this.cache.dropKeyspace(keyspaceId);
    for (const k of [...this.committed]) if (k.startsWith(`${keyspaceId}\u0000`)) this.committed.delete(k);
    try {
      const named = this.namedByHeldActions();
      for (const s of await this.blobStash.list()) {
        if (s.keyspaceId === keyspaceId && !named.has(this.blobKey(s.keyspaceId, s.blobId))) await this.stashDelete(s.keyspaceId, s.blobId);
      }
    } catch (e) {
      this.noteInternalError(e);
    }
  }

  /** The blobs (as `blobKey`) that an action held by the engine names: the outbox (the locked entries of other principals too), the dead-letter store and the export-hook entries (14 [CLI-28], [CLI-83]). */
  private namedByHeldActions(): Set<string> {
    const named = new Set<string>();
    const add = (keyspaces: Iterable<string>, content: Uint8Array): void => {
      let refs: readonly string[];
      try {
        refs = decodeContent(content).blobRefs;
      } catch {
        return;
      }
      for (const ks of keyspaces) for (const id of refs) named.add(this.blobKey(ks, id));
    };
    for (const e of [...this.entries.values(), ...this.lockedEntries]) add(this.stashKeyspaces(e), e.content);
    for (const d of this.deadLetters.values()) add([d.keyspaceId], d.content);
    for (const x of this.exports.values()) add([x.keyspaceId], x.content);
    return named;
  }

  /**
   * 14 [CLI-65]: the sign-out of a personal partition purges what the partition holds, and the bytes that its actions carried are the principal's data: every staged blob goes, and the
   * downloaded bytes of every keyspace. Fails when the stash could not erase them.
   */
  protected async blobsPurgeAll(): Promise<void> {
    this.uploads.clear();
    this.committed.clear();
    this.cache.clear();
    await this.stashClear();
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
