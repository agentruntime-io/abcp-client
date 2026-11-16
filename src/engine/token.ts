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

/** What the progress-row test says of a copy for a token. */
export type CopyVerdict = "has-write" | "not-yet" | "deleted";

/**
 * The token for the app's back end, from a `committed` event (13 [PLC-27]).
 *
 * The back end asks the copy's progress row of the keyspace, and the copy has the write when (13 [PLC-27]'s example):
 *  - the progress row's epoch is above the token's `logEpoch`, or
 *  - the epochs are equal and the row's `copied_seq` is at or above the token's `seq`.
 * Otherwise it is "not yet" (copied_seq below seq in the same epoch, or an epoch below the token's).
 *
 * CRDT note (N8): a CRDT row that is absent, or whose `crdt_covered_seq` is below the token's `seq`, is "not yet" (`copyHasWrite`'s `crdtCoveredSeq`).
 *
 * Tombstone: a progress row `purged` with epoch 0 means the keyspace is deleted (`copyHasWrite` answers "deleted").
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
  const has = progress.epoch > token.logEpoch || (progress.epoch === token.logEpoch && progress.copiedSeq >= token.seq);
  if (!has) return "not-yet";
  if (crdt !== undefined && (crdt.covered === undefined || crdt.covered < token.seq)) return "not-yet";
  return "has-write";
}
