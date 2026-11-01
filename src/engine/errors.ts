// The failures that the engine tells apart: an RPC status with its error detail (A.7), a message that breaks 02 [ARC-61]
// (16 [SEC-3]), and the local refusals of an action (14 [CLI-19], [CLI-21], [CLI-82]).

import type { Reject, Retry } from "../gen/abpc/v2/types_pb.js";

/** The RPC statuses of A.7, by the names of the gRPC codes (02 §2.9 binds every protocol to them). */
export type RpcStatus =
  | "Cancelled"
  | "Unknown"
  | "InvalidArgument"
  | "DeadlineExceeded"
  | "NotFound"
  | "AlreadyExists"
  | "PermissionDenied"
  | "ResourceExhausted"
  | "FailedPrecondition"
  | "Aborted"
  | "OutOfRange"
  | "Unimplemented"
  | "Internal"
  | "Unavailable"
  | "DataLoss"
  | "Unauthenticated";

/**
 * A call that failed with an RPC status. `reject` or `retry` is the error detail that 02 [ARC-57] attaches, when one applies;
 * A.7 says that a client acts on the detail when it is present and on the status otherwise.
 */
export class RpcError extends Error {
  readonly status: RpcStatus;
  readonly reject: Reject | undefined;
  readonly retry: Retry | undefined;

  constructor(status: RpcStatus, message = "", detail: { reject?: Reject; retry?: Retry } = {}) {
    super(`rpc: ${status}${message === "" ? "" : `: ${message}`}`);
    this.name = "RpcError";
    this.status = status;
    this.reject = detail.reject;
    this.retry = detail.retry;
  }
}

/**
 * A message from the server that breaks 02 [ARC-61]: bytes that do not decode (a `CanonError` of `rules/canon`), a string that
 * is not valid UTF-8, a response that does not answer its request. 16 [SEC-3] and 14 [CLI-50]: the client applies, stores and
 * repairs none of it, emits `protocol-error` and holds each keyspace that the message concerns.
 */
export class ProtocolViolation extends Error {
  /** The keyspaces that the message concerns; empty when it concerns none, or every one the session follows. */
  readonly keyspaces: readonly string[];
  /** The type of the offending message, for the `protocol-error` event. */
  readonly messageType: string;

  constructor(messageType: string, detail: string, keyspaces: readonly string[] = []) {
    super(`protocol violation in ${messageType}: ${detail}`);
    this.name = "ProtocolViolation";
    this.messageType = messageType;
    this.keyspaces = keyspaces;
  }
}

/** Why a client refused an action locally, before it entered the outbox. */
export type LocalRefusalCode =
  /** The local storage transaction failed (14 [CLI-21]), or the outbox is full (14 [CLI-84]); `LocalRefusal.cause` names which. */
  | "STORAGE"
  /** The action's keyspace is not one the engine follows (14 [CLI-69]). */
  | "NOT_FOLLOWED"
  /** The action breaks the manifest, and is certain to be dead-lettered (14 [CLI-19]). */
  | "INVALID"
  /** An action of an op type with `offline_mode: fail_fast`, authored while offline (14 [CLI-82]). */
  | "OFFLINE"
  /** Any other refusal that the app causes: a write under an entity that is deleted, an unknown dependency. */
  | "REFUSED"
  /** A follower tab carried a call to the flusher, and no flusher answered (14 [CLI-61]): nothing was accepted. */
  | "NO_FLUSHER"
  /** The call is the flusher tab's: a follower tab has no view document of a CRDT key, and does not sign the principal out. */
  | "NOT_FLUSHER";

/**
 * Why the local storage transaction of an accepted action failed (14 [CLI-21]): the error "names its cause".
 * `OUTBOX_FULL`: the outbox is at its bound and the client evicts nothing ([CLI-84]); `QUOTA`: the platform's storage quota is used up; `ABORT`: the
 * transaction was aborted or failed for another reason.
 */
export type StorageCause = "OUTBOX_FULL" | "QUOTA" | "ABORT";

/**
 * A local refusal of an action: nothing was stored and nothing is shown. It is an error of the client that the app can tell from a result of the
 * server (`RpcError`, a `Reject`): when the local commit fails (`STORAGE`, 14 [CLI-21]) it names its cause.
 */
export class LocalRefusal extends Error {
  readonly code: LocalRefusalCode;
  /** For `STORAGE`: why the commit failed (14 [CLI-21]). */
  readonly cause: StorageCause | undefined;

  constructor(code: LocalRefusalCode, detail: string, cause?: StorageCause) {
    super(`refused locally (${code}${cause === undefined ? "" : `, ${cause}`}): ${detail}`);
    this.name = "LocalRefusal";
    this.code = code;
    this.cause = cause;
  }
}

/** The cause of a failed storage commit (14 [CLI-21]): a full store, an exhausted quota, or an abort of the transaction. */
export function storageCauseOf(e: unknown): StorageCause {
  if (e instanceof Error) {
    if (e.name === "StoreFull") return "OUTBOX_FULL";
    if (e.name === "QuotaExceededError" || /quota/i.test(e.message)) return "QUOTA";
  }
  return "ABORT";
}

/** The local storage failed a transaction (14 [CLI-21]): nothing of it was applied. */
export class StorageError extends Error {
  constructor(detail: string) {
    super(`storage: ${detail}`);
    this.name = "StorageError";
  }
}
