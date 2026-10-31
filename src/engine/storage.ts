// The durable outbox of 14 §14.3 behind a storage interface. One interface serves the outbox, the dead-letter store, the
// journal, the export-hook entries, the unacknowledged rejections, the drafts of CRDT keys and a few persisted facts (the
// offline horizon and the server-time offset, 14 [CLI-54], [CLI-55]).
//
// Every write goes through `commit`, one local transaction: all of its operations are applied, or none. This is what 14 [CLI-21]
// ("the local storage transaction has completed, not merely started"), [CLI-28] (a move to the dead-letter store is one local
// transaction) and [CLI-53] (the journal entry is written in the transaction that removes the retired action) need. An
// IndexedDB or SQLite implementation maps `commit` to one transaction; `MemoryStorage` is the one for now.
//
// TODO(C3, 14 [CLI-59], [CLI-74]): one storage per principal and per server origin and app; the engine holds one.

import type { Reject } from "../gen/abpc/v2/types_pb.js";
import type { LaneName } from "./manifests.js";

/** The author of an entry (14 [CLI-59]): the principal's issuer, subject and device. */
export interface Author {
  issuer: string;
  sub: string;
  deviceId: string;
}

/** The outcome of an action that the server has recorded and the client has not retired yet, or that a journal entry keeps (14 [CLI-7]). */
export type StoredOutcome =
  | { kind: "appended"; seq: bigint; logEpoch: number; overwroteNewer: number[] }
  | { kind: "decided"; status: "COMMITTED" | "REJECTED"; decisionSeq: bigint; logEpoch: number; reject?: Reject };

/** The last answer an action got, for the report of 14 [CLI-83] and the dead-letter entry of 14 [CLI-28]. */
export interface LastResult {
  kind: "retry" | "reject" | "appended" | "decided" | "rpc";
  /** A code name, a retry reason name or an RPC status. */
  what: string;
  errorClass?: number;
  atMs: number;
}

/** An action in the outbox (14 [CLI-22]). */
export interface OutboxEntry {
  mutationId: string;
  keyspaceId: string;
  lane: LaneName;
  opType: string;
  /** Every `Mutation` field but the stamping fields, in the canonical encoding, written once at authoring (14 [CLI-22]). */
  content: Uint8Array;
  author: Author;
  /** The authoring order in this outbox, which is the outbox order of 14 [CLI-6]. */
  ordinal: number;
  /** The server time that the client estimated when it authored the action (14 [CLI-55]); the start of its age. Outside the content. */
  authoredServerMs: number;
  /** The direct dependencies, by mutation id (14 [CLI-30], [CLI-37]). */
  dependsOn: string[];
  // -- the stamping fields (14 [CLI-27], [CLI-63]) --
  clientId: string;
  /** 0: no slot yet. */
  clientSeq: number;
  afterClientSeq: number;
  // -- delivery --
  /** A request that carries it has left the client, whatever answer it got (14 [CLI-83]). */
  everSent: boolean;
  /** The epoch of the keyspace when it was last sent; 0 when never. */
  sentInEpoch: number;
  /** Consecutive resends since the last answer that was not a `Retry` (14 [CLI-43]). */
  attempts: number;
  lastResult?: LastResult;
  /** A GATED action that the server registered as an intent (14 [CLI-52], [CLI-53]). */
  registered: boolean;
  // -- outcome --
  outcome?: StoredOutcome;
  // -- one-shot markers (14 [CLI-46], [CLI-64]) --
  permissionRetried: boolean;
  actorMismatchRetried: boolean;
  /** A lost spend action that was resent once (14 [CLI-64]). */
  spendResent: boolean;
  /** The keyspace the action was authored in, when it was re-routed to a split successor (14 [CLI-52]). */
  authoredIn?: string;
  /** The server time of the first failure of the action, for the dead-letter entry. */
  firstFailureMs?: number;
  /**
   * The action reached its end, but the store that it is due in is full (14 [CLI-84]): it stays in the outbox, final and never
   * sent again, until the app acknowledges entries of that store and the move can be made.
   */
  stuck?: { sink: "deadLetter" | "export" | "journal"; reason: string; reject?: Reject; cause?: string; dependencyOf?: string };
}

/** An entry of the dead-letter store (14 [CLI-28]). */
export interface DeadLetterEntry {
  mutationId: string;
  keyspaceId: string;
  opType: string;
  content: Uint8Array;
  author: Author;
  reject: Reject;
  /** The reason that the app is told: a code name, or the hold's code. */
  reason: string;
  attempts: number;
  firstFailureMs: number;
  lastFailureMs: number;
  failedDependents: string[];
  /** For `DEPENDENCY_FAILED`: the action that failed. */
  dependencyOf?: string;
}

/** An entry of the journal of 14 [CLI-53]: the content of an action that retired with a successful outcome. */
export interface JournalEntry {
  mutationId: string;
  keyspaceId: string;
  opType: string;
  content: Uint8Array;
  author: Author;
  outcome: StoredOutcome;
  /** What the app was told when it retired. */
  reported: "committed" | "corrected";
  /** The server time at which the outcome retired: the window of 14 [CLI-53] runs from it. */
  retiredServerMs: number;
  spend: boolean;
  /** The action is a resend of a lost spend action: a later restore never makes it be resent again (14 [CLI-64]). */
  spendResent: boolean;
  /** The lane the action was sent on. */
  lane: LaneName;
  /** Its place in the authoring order, which orders the events of 14 [CLI-53] across journalled and pending actions. */
  ordinal: number;
  /** The epoch in which `GetMutationResult` last confirmed the outcome (14 [CLI-53]); the outcome's own epoch until then. */
  verifiedEpoch: number;
}

/** An entry that the app's export hook was handed and has not acknowledged (14 [CLI-83]). */
export interface ExportEntry {
  mutationId: string;
  keyspaceId: string;
  opType: string;
  content: Uint8Array;
  author: Author;
  reason: string;
  reject?: Reject;
  lastResult?: string;
  dependencyOf?: string;
  atMs: number;
}

/** A rejection that the app has not acknowledged (14 [CLI-41]). */
export interface RejectionRecord {
  mutationId: string;
  keyspaceId: string;
  opType: string;
  content: Uint8Array;
  reject?: Reject;
  cause?: string;
  atMs: number;
}

/** The text of a CRDT key's pending and unsent edits that a redaction reset took out of the view (14 [CLI-13], [CLI-49]). */
export interface DraftEntry {
  keyspaceId: string;
  key: string;
  /** Yjs updates: the pending updates of the key that the reset ended, and the unsent edits. */
  updates: Uint8Array[];
  reason: string;
  atMs: number;
}

export type StorageOp =
  | { op: "putEntry"; entry: OutboxEntry }
  | { op: "deleteEntry"; mutationId: string }
  | { op: "putDeadLetter"; entry: DeadLetterEntry }
  | { op: "ackDeadLetter"; mutationId: string }
  | { op: "putJournal"; entry: JournalEntry }
  | { op: "deleteJournal"; mutationId: string }
  | { op: "putExport"; entry: ExportEntry }
  | { op: "ackExport"; mutationId: string }
  | { op: "putRejection"; entry: RejectionRecord }
  | { op: "ackRejection"; mutationId: string }
  | { op: "putDraft"; entry: DraftEntry }
  | { op: "deleteDraft"; keyspaceId: string; key: string }
  | { op: "setMeta"; key: string; value: string | number | bigint };

export interface StoredState {
  entries: OutboxEntry[];
  deadLetters: DeadLetterEntry[];
  journal: JournalEntry[];
  exports: ExportEntry[];
  rejections: RejectionRecord[];
  drafts: DraftEntry[];
  meta: Record<string, string | number | bigint>;
}

/** The names of the stores that a bound can fill (14 [CLI-84]). */
export type StoreName = "outbox" | "deadLetters" | "journal" | "exports";

export interface OutboxStorage {
  /** `durable` for a store that survives the process; `memory` is reported in the sync status (14 [CLI-21]). */
  readonly mode: "memory" | "durable";
  /** Everything stored, read once at start (14 [CLI-8]). */
  load(): Promise<StoredState>;
  /**
   * Applies the operations as one local transaction: all or none. Rejects with a `StorageError` when it cannot, and then
   * nothing was applied. A bounded store (14 [CLI-84]) rejects an operation that would grow a full store beyond its bound with
   * `StoreFull`, naming the store.
   */
  commit(ops: readonly StorageOp[]): Promise<void>;
  /** The stores that are at their bound (14 [CLI-84]). */
  full(): StoreName[];
}

/** A commit that a full store refuses (14 [CLI-84]): nothing was applied. */
export class StoreFull extends Error {
  readonly store: StoreName;
  constructor(store: StoreName) {
    super(`storage: the ${store} store is full`);
    this.name = "StoreFull";
    this.store = store;
  }
}
