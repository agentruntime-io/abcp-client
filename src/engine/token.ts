// The read-your-writes token (13 [PLC-27]): what an app's back end needs to know whether a copy (placement C) holds a write that the client saw committed.
// The wire carries nothing new for it; the token is read off the `committed` event.

import type { CommittedEvent } from "./types.js";

/** `(keyspace_id, log_epoch, seq)`; `seq` is `Appended.seq` or `Decided.decision_seq`, as the event carries it. */
export interface ReadYourWritesToken {
  keyspaceId: string;
  logEpoch: number;
  seq: bigint;
}

/** A row of the copy's progress table, as far as the test needs it (13 [PLC-27]). */
export interface CopyProgress {
  /** The epoch the copy has reached; 0 with `purged` is the tombstone. */
  epoch: number;
  copiedSeq: bigint;
  purged?: boolean;
}

/**
 * What the progress-row test says of a copy for a token:
 *  - "has-write": the copy holds the write;
 *  - "not-yet": not yet; ask again later;
 *  - "cannot-tell": the token cannot be relied on. Either the progress row is in a later epoch than the token (13 [PLC-27]: the copy was rebuilt after a restore or a
 *    split, and the write may be in it or not), or the token has no epoch (0);
 *  - "deleted": the keyspace is deleted (the tombstone).
 */
export type CopyVerdict = "has-write" | "not-yet" | "cannot-tell" | "deleted";

/**
 * The token for the app's back end, from a `committed` event (13 [PLC-27]).
 *
 * The back end reads the keyspace's progress row in the copy. The copy has the write only when (13 [PLC-27]):
 *  - the progress row's `log_epoch` equals the token's `logEpoch`, and
 *  - the row's `copied_seq` is at or above the token's `seq`.
 * Otherwise:
 *  - a row in the token's epoch with `copied_seq` below `seq`, or in an earlier epoch, or no row at all: "not yet";
 *  - a row in a later epoch: the copy was rebuilt after a restore or a split, and the reader MUST NOT rely on the token (13 [PLC-27]). `copyHasWrite` answers
 *    "cannot-tell", never "has-write".
 *
 * CRDT note (N8): a CRDT row that is absent, or whose `crdt_covered_seq` is below the token's `seq`, is "not yet" (`copyHasWrite`'s `crdt` argument).
 *
 * Tombstone: a progress row `purged` with epoch 0 means the keyspace is deleted (`copyHasWrite` answers "deleted").
 *
 * A committed event's `logEpoch` is the epoch of its `Appended` or `Decided` (at least 1). If the engine held no outcome for the action, it is 0, and the token is
 * never relied on: "cannot-tell" against every row.
 */
export function readYourWritesToken(event: CommittedEvent): ReadYourWritesToken {
  return { keyspaceId: event.keyspaceId, logEpoch: event.logEpoch, seq: event.seq };
}

/**
 * The progress-row test of the doc comment above as a pure function, for an app's back end and for the tests.
 * `crdt` is for a token that names a CRDT row: `covered` is `undefined` for an absent row, otherwise its `crdt_covered_seq`.
 */
export function copyHasWrite(token: ReadYourWritesToken, progress: CopyProgress, crdt?: { covered: bigint | undefined }): CopyVerdict {
  if (progress.purged === true && progress.epoch === 0) return "deleted";
  // a token with no epoch (none is below 1), or a row in a later epoch than the token's (13 [PLC-27]: "the reader MUST NOT rely on the token")
  if (!(token.logEpoch >= 1) || progress.epoch > token.logEpoch) return "cannot-tell";
  if (progress.epoch < token.logEpoch || progress.copiedSeq < token.seq) return "not-yet";
  if (crdt !== undefined && (crdt.covered === undefined || crdt.covered < token.seq)) return "not-yet";
  return "has-write";
}
