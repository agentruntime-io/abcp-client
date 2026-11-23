// The codes of Appendix A.3 with their default classes (A.2), and how the engine reads a class: by `Reject.error_class`, which
// the server sets (07 [MUT-22], 14 [CLI-42]), also for a code the client does not know (15 [COMPAT-39]).

import { Code, ErrorClass, RetryReason, StaleCause } from "../gen/abcp/v2/types_pb.js";

/** The class of a reaction (A.2). */
export type Reaction = "RETRY" | "HOLD" | "DEAD_LETTER" | "CONFLICT";

/** The default class of every code of A.3. A v2.x code is not in it: a client reads its `Reject.error_class` instead (15 [COMPAT-25]). */
export const DEFAULT_CLASS: ReadonlyMap<number, ErrorClass> = new Map<number, ErrorClass>([
  [Code.INVALID_ENVELOPE, ErrorClass.DEAD_LETTER],
  [Code.INVALID_KEY, ErrorClass.DEAD_LETTER],
  [Code.WRITE_NOT_ALLOWED, ErrorClass.DEAD_LETTER],
  [Code.UNKNOWN_OP_TYPE, ErrorClass.HOLD],
  [Code.SCHEMA_INVALID, ErrorClass.DEAD_LETTER],
  [Code.PAYLOAD_TOO_LARGE, ErrorClass.DEAD_LETTER],
  [Code.QUOTA_EXCEEDED, ErrorClass.DEAD_LETTER],
  [Code.RANGE_EXCEEDED, ErrorClass.CONFLICT],
  [Code.STALE, ErrorClass.CONFLICT],
  [Code.TOMBSTONED, ErrorClass.DEAD_LETTER],
  [Code.POLICY_CHANGED, ErrorClass.CONFLICT],
  [Code.IDEMPOTENCY_KEY_REUSED, ErrorClass.DEAD_LETTER],
  [Code.ID_CONFLICT, ErrorClass.DEAD_LETTER],
  [Code.CLIENT_FORKED, ErrorClass.HOLD],
  [Code.CLIENT_UNKNOWN, ErrorClass.HOLD],
  [Code.PERMISSION_DENIED, ErrorClass.DEAD_LETTER],
  [Code.KEYSPACE_NOT_FOUND, ErrorClass.HOLD],
  [Code.KEYSPACE_ARCHIVED, ErrorClass.DEAD_LETTER],
  [Code.KEYSPACE_DELETED, ErrorClass.DEAD_LETTER],
  [Code.UPGRADE_REQUIRED, ErrorClass.HOLD],
  [Code.UNSUPPORTED_FEATURE, ErrorClass.HOLD],
  [Code.GATE_REJECTED, ErrorClass.DEAD_LETTER],
  [Code.CANCELLED, ErrorClass.DEAD_LETTER],
  [Code.INTENT_EXPIRED, ErrorClass.DEAD_LETTER],
  [Code.OFFLINE_TOO_LONG, ErrorClass.DEAD_LETTER],
  [Code.HISTORY_DIVERGED, ErrorClass.HOLD],
  [Code.OVERRIDE_FORBIDDEN, ErrorClass.DEAD_LETTER],
  [Code.ACTOR_MISMATCH, ErrorClass.DEAD_LETTER],
  [Code.BLOB_MISSING, ErrorClass.RETRY],
  [Code.BLOB_DELETED, ErrorClass.DEAD_LETTER],
  [Code.DEPENDENCY_FAILED, ErrorClass.DEAD_LETTER],
  [Code.APP_SUSPENDED, ErrorClass.HOLD],
  [Code.ADMIN_REFUSED, ErrorClass.DEAD_LETTER],
  [Code.CONTENDED, ErrorClass.CONFLICT],
]);

/**
 * Whether a server records a final reject as the outcome of its mutation id, which consumes the action's slot (07 §7.6): a reject of
 * class DEAD_LETTER or CONFLICT, but `PERMISSION_DENIED`, `ACTOR_MISMATCH`, `KEYSPACE_ARCHIVED` and `KEYSPACE_DELETED`. The
 * client decides whether a slot is consumed from the lane's watermark (14 [CLI-27]); this is what it expects until the watermark says.
 */
export function isRecordedReject(r: { code: number; errorClass: number }): boolean {
  if (r.errorClass !== ErrorClass.DEAD_LETTER && r.errorClass !== ErrorClass.CONFLICT) return false;
  return r.code !== Code.PERMISSION_DENIED && r.code !== Code.ACTOR_MISMATCH && r.code !== Code.KEYSPACE_ARCHIVED && r.code !== Code.KEYSPACE_DELETED;
}

/** Whether this client knows the code (it is in A.3 as of v2.0). */
export function isKnownCode(code: number): boolean {
  return DEFAULT_CLASS.has(code);
}

/** The name of a code for a report: its enum name, or `CODE_<n>` for one that a later revision added. */
export function codeName(code: number): string {
  return Code[code] ?? `CODE_${code}`;
}

/** The name of a retry reason, or `RETRY_REASON_<n>` for an unknown one. */
export function retryReasonName(reason: number): string {
  return RetryReason[reason] ?? `RETRY_REASON_${reason}`;
}

/**
 * How a client reacts to a class (14 [CLI-42], A.2): `RETRY`, `HOLD`, `DEAD_LETTER` or `CONFLICT`. A class that the client does
 * not know, and `UNSPECIFIED`, is handled as `HOLD` (15 [COMPAT-39]).
 */
export function reactionOf(errorClass: number): Reaction {
  switch (errorClass) {
    case ErrorClass.RETRY:
      return "RETRY";
    case ErrorClass.DEAD_LETTER:
      return "DEAD_LETTER";
    case ErrorClass.CONFLICT:
      return "CONFLICT";
    default:
      return "HOLD";
  }
}

/** Whether the class is one of the four that a v2.0 client knows (the class enum is closed, 15 Table 15.2). */
export function isKnownClass(errorClass: number): boolean {
  return errorClass === ErrorClass.RETRY || errorClass === ErrorClass.HOLD || errorClass === ErrorClass.DEAD_LETTER || errorClass === ErrorClass.CONFLICT;
}

/** The causes of `STALE` that a v2.0 client knows (A.4). An unknown cause is a visible conflict, never resent (15 [COMPAT-41]). */
export function isKnownStaleCause(cause: number): boolean {
  return cause >= StaleCause.VERSION_MISMATCH && cause <= StaleCause.REDACTED;
}

/** The retry reasons that a v2.0 client knows (A.5). An unknown reason is a retry after `Retry.after_ms` (15 [COMPAT-40]). */
export function isKnownRetryReason(reason: number): boolean {
  return reason >= RetryReason.GATE_UNAVAILABLE && reason <= RetryReason.BLOB_WITHHELD;
}
