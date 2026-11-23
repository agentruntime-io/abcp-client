// What a failed call becomes for the engine (02 [ARC-57], A.7): an `RpcError` with the status of the call and its one ABCP detail, a
// `Reject` or a `Retry`, decoded strictly (rules/canon, 02 [ARC-61]); or a `ProtocolViolation` for a detail that breaks the codec's rules.
//
// A failure that has no status of the server's making, a connection that was refused or cut, a deadline, an abort, is `Unavailable`,
// `DeadlineExceeded` or `Cancelled`: A.7 treats a status of a proxy or of the transport "as `Unavailable`": the outcome of a sent action
// is unknown, and the call is sent again with the client backoff.

import { Code, ConnectError } from "@connectrpc/connect";
import { RejectSchema, RetrySchema, type Reject, type Retry } from "../gen/abcp/v2/types_pb.js";
import { ProtocolViolation, RpcError, type RpcStatus } from "../engine/errors.js";
import { decodeMessage } from "../engine/wire.js";

/** The names of A.7 by the numeric codes of gRPC, which Connect's own codes are (the `Code` enum of connect-es). */
const STATUS: Readonly<Record<number, RpcStatus>> = {
  [Code.Canceled]: "Cancelled",
  [Code.Unknown]: "Unknown",
  [Code.InvalidArgument]: "InvalidArgument",
  [Code.DeadlineExceeded]: "DeadlineExceeded",
  [Code.NotFound]: "NotFound",
  [Code.AlreadyExists]: "AlreadyExists",
  [Code.PermissionDenied]: "PermissionDenied",
  [Code.ResourceExhausted]: "ResourceExhausted",
  [Code.FailedPrecondition]: "FailedPrecondition",
  [Code.Aborted]: "Aborted",
  [Code.OutOfRange]: "OutOfRange",
  [Code.Unimplemented]: "Unimplemented",
  [Code.Internal]: "Internal",
  [Code.Unavailable]: "Unavailable",
  [Code.DataLoss]: "DataLoss",
  [Code.Unauthenticated]: "Unauthenticated",
};

/** The status of a Connect code; a code that is none of the sixteen is `Unknown`, which A.7 treats as `Unavailable`. */
export function statusOfCode(code: number): RpcStatus {
  return STATUS[code] ?? "Unknown";
}

const REJECT_TYPE = "abcp.v2.Reject";
const RETRY_TYPE = "abcp.v2.Retry";

/** The type name of a detail: its type URL without the prefix (02 [ARC-57]: the prefixed form is accepted too). */
function detailType(type: string): string {
  const i = type.lastIndexOf("/");
  return i < 0 ? type : type.slice(i + 1);
}

/**
 * The one ABCP detail of a status (02 [ARC-57]): the first `abcp.v2.Reject` or `abcp.v2.Retry`; a detail of any other type is
 * ignored. The message is decoded as every message of the server is (rules/canon): a `ProtocolViolation` when its bytes break
 * 02 [ARC-61].
 */
export function abcpDetail(details: readonly { type?: string; value?: Uint8Array }[]): { reject?: Reject; retry?: Retry } {
  for (const d of details) {
    if (typeof d.type !== "string" || !(d.value instanceof Uint8Array)) continue; // an outgoing detail never arrives
    switch (detailType(d.type)) {
      case REJECT_TYPE:
        return { reject: decodeMessage<Reject>(RejectSchema, d.value) };
      case RETRY_TYPE:
        return { retry: decodeMessage<Retry>(RetrySchema, d.value) };
      default:
        break;
    }
  }
  return {};
}

/** Why a call ended before the server answered it: the caller's signal, or the call's own deadline. */
export interface Ending {
  /** The call's deadline passed. */
  deadline: boolean;
  /** The caller aborted the call. */
  aborted: boolean;
}

/**
 * What the engine gets of a failure: an `RpcError` (a status, with the detail when there is one), or a `ProtocolViolation`
 * (a detail that is not a message of this protocol). Anything that is neither a status of the server's nor a status of the
 * deadline or the caller is `Unavailable` (A.7).
 */
export function toFailure(e: unknown, ending: Ending = { deadline: false, aborted: false }): RpcError | ProtocolViolation {
  if (e instanceof ProtocolViolation || e instanceof RpcError) return e;
  if (ending.deadline) return new RpcError("DeadlineExceeded", "the call's deadline passed");
  if (ending.aborted) return new RpcError("Cancelled", "the call was aborted");
  if (e instanceof ConnectError) {
    try {
      return new RpcError(statusOfCode(e.code), e.rawMessage, abcpDetail(e.details as readonly { type?: string; value?: Uint8Array }[]));
    } catch (v) {
      if (v instanceof ProtocolViolation) return v;
      throw v;
    }
  }
  // a refused or reset connection, a failed TLS handshake, `fetch` that could not reach the server: no status from the server
  const name = e instanceof Error ? e.name : "";
  if (name === "AbortError") return new RpcError("Cancelled", "the call was aborted");
  return new RpcError("Unavailable", e instanceof Error ? e.message : String(e));
}
