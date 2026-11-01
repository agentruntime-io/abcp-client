// The durable outbox of 14 §14.3 behind a storage interface. One interface serves the outbox, the dead-letter store, the
// journal, the export-hook entries, the unacknowledged rejections, the drafts of CRDT keys and a few persisted facts (the
// offline horizon and the server-time offset, 14 [CLI-54], [CLI-55]).
//
// Every write goes through `commit`, one local transaction: all of its operations are applied, or none. This is what 14 [CLI-21]
// ("the local storage transaction has completed, not merely started"), [CLI-28] (a move to the dead-letter store is one local
// transaction) and [CLI-53] (the journal entry is written in the transaction that removes the retired action) need. An
// IndexedDB or SQLite implementation maps `commit` to one transaction; `MemoryStorage` is the one for now.
//
// One `OutboxStorage` is the storage of one **partition** (14 [CLI-59], [CLI-74]): the outbox, dead-letter store, journal,
// export-hook entries, drafts and confirmed replica of one principal (or, in shared-device mode, one device) of one app on one
// server origin. A `StorageProvider` (tabs.ts) opens it by its `PartitionKey`; every row of it is keyed by `keyspace_id` or
// `mutation_id` inside the partition, so apps, servers and principals never share local data.
//
// The **persisted replica** (14 [CLI-2], §14.10) is kept per keyspace as the pages of the load that began it, exactly as the server
// served them, and the records that the engine applied since: a restart installs the pages and folds the records again by the same
// fold, so that the restored state is the state that the server's fold gives, whatever a Yjs library would encode (a client never
// builds a CRDT base, 05 [STATE-74]).

import type { KeyEntry, Record as RecordMessage, Reject, SuccessorRule } from "../gen/abpc/v2/types_pb.js";
import type { LaneName } from "./manifests.js";

/** The author of an entry (14 [CLI-59]): the principal's issuer, subject and device. */
export interface Author {
  issuer: string;
  sub: string;
  deviceId: string;
}

/**
 * The outcome of an action that the server has recorded and the client has not retired yet, or that a journal entry keeps (14 [CLI-7]).
 * `atServerMs` is the server time of the outcome (14 [CLI-53], [CLI-55]): the time of its record when the client has seen it, otherwise
 * the client's estimate of the server time when it learned the outcome, which is never earlier than the outcome.
 */
export type StoredOutcome =
  | { kind: "appended"; seq: bigint; logEpoch: number; overwroteNewer: number[]; atServerMs?: number }
  | { kind: "decided"; status: "COMMITTED" | "REJECTED"; decisionSeq: bigint; logEpoch: number; reject?: Reject; atServerMs?: number };

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
  /**
   * How the applied result differs from the prediction, as far as the client has learned it (14 [CLI-38], [CLI-40]); it is kept with
   * the entry, so that a restart between the learning and the retirement does not turn `corrected` into `committed`.
   */
  verdict?: Verdict;
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
  stuck?: { sink: "deadLetter" | "export" | "journal"; reason: string; reject?: Reject; cause?: string; dependencyOf?: string; quarantine?: boolean };
}

/** The keys on which an action's applied result differs from its prediction (14 [CLI-38], [CLI-40]). */
export interface Verdict {
  /** A `CAS` that was shown as having no effect, which sticks even when it commits later (14 [CLI-11]). */
  noEffect: string[];
  /** A CRDT update that folded to no effect (05 [STATE-76], 14 [CLI-40]). */
  crdtNoEffect: string[];
  /** The keys of a gate's effects that the prediction lacked or got wrong (14 [CLI-15]); absent until the DECISION that made them was seen. */
  effects?: string[];
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
  /**
   * The authoring order of the action in its outbox (`OutboxEntry.ordinal`): the order in which the entries are listed (14 [CLI-38], [CLI-77]: "in the authoring
   * order of the actions"), which is neither the order in which they ended nor the key order of a storage that reads by key. Absent in an entry that an earlier
   * build wrote: it is listed before the ones that have one, in the order that the storage gave.
   */
  ordinal?: number;
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
  /** The server time at which the action retired. */
  retiredServerMs: number;
  /** The server time of the outcome, from which the window of 14 [CLI-53] runs: the server keeps the binding for `max_offline` after it (07 [MUT-51]). */
  outcomeServerMs: number;
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
  /** The authoring order of the action (see `DeadLetterEntry.ordinal`). */
  ordinal?: number;
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
  /** The author of the action (14 [CLI-59]): on a shared device another principal never sees it. */
  author?: Author;
}

/** The text of a CRDT key's pending and unsent edits that a redaction reset took out of the view (14 [CLI-13], [CLI-49]). */
export interface DraftEntry {
  keyspaceId: string;
  key: string;
  /** The author of the draft (14 [CLI-59]). */
  author?: Author;
  /** Yjs updates: the pending updates of the key that the reset ended, and the unsent edits. */
  updates: Uint8Array[];
  reason: string;
  atMs: number;
}

/**
 * What a persisted replica keeps of a keyspace besides its keys (14 [CLI-1], [CLI-3], [CLI-71]): the cursor and epoch, the record hash
 * for a verifiable resume (09 [READ-17]), the kind, lifecycle and successors, the interest, and the time of the last successful sync
 * (14 [CLI-56]).
 */
export interface ReplicaHead {
  logEpoch: number;
  /** The confirmed cursor: the seq of the last record applied, or of the last delivery watermark (14 [CLI-3]). */
  seq: bigint;
  /** The delivery watermark from which the keyspace resumes (09 [READ-45]). */
  watermark: bigint;
  /** The `record_hash` of the last record applied, with its seq: the `after_record_hash` of the resume (09 [READ-17]). */
  lastRecordHash?: Uint8Array;
  lastRecordSeq: bigint;
  manifestVersion: number;
  kind: string;
  /** A `KeyspaceLifecycle` value. */
  lifecycle: number;
  successors: SuccessorRule[];
  /** The interest prefixes (14 [CLI-71]); empty is the whole keyspace. */
  interest: string[];
  /** The estimate of the server time of the last successful sync of the keyspace, in ms (14 [CLI-55], [CLI-56]). */
  lastSyncMs: number;
  /** The principal whose cache this is (14 [CLI-59]): the partition of a shared device holds the caches of the principals that used it, one at a time, and never lends one to another. */
  author?: Author;
}

/** The load that began a persisted replica: its pages as the server served them (09 §9.2). */
export interface ReplicaBase {
  manifestVersion: number;
  logEpoch: number;
  checkpointSeq: bigint;
  entries: KeyEntry[];
}

/** The persisted replica of one keyspace (14 [CLI-2]): the pages of the load that began it, and the records applied since, in seq order. */
export interface ReplicaImage {
  keyspaceId: string;
  head: ReplicaHead;
  base: ReplicaBase;
  tail: RecordMessage[];
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
  | { op: "deleteDraft"; keyspaceId: string; key: string; author?: Author }
  | { op: "setMeta"; key: string; value: string | number | bigint }
  // -- the persisted replica (14 [CLI-2], [CLI-74]) --
  /** A load installed confirmed state: the image of the keyspace is the load's pages, with no tail. */
  | { op: "replicaBegin"; keyspaceId: string; head: ReplicaHead; base: ReplicaBase }
  /** Records were applied: they join the tail, and the head moves with them. */
  | { op: "replicaAppend"; keyspaceId: string; head: ReplicaHead; records: RecordMessage[] }
  /** Only the head moved (a delivery watermark, the time of a sync). */
  | { op: "replicaHead"; keyspaceId: string; head: ReplicaHead }
  /** A widening load (14 [CLI-72]) added entries for new interest prefixes to the pages. */
  | { op: "replicaExtend"; keyspaceId: string; head: ReplicaHead; entries: KeyEntry[] }
  /** Purges the replica of the keyspace (14 [CLI-66], [CLI-77]). */
  | { op: "deleteReplica"; keyspaceId: string }
  /** Removes everything of the partition (14 [CLI-65]: a sign-out in personal mode, a discard of a locked partition). */
  | { op: "purgeAll" };

/** What a replica image is, without its pages and its tail: its head and the sizes of the rest (see `OutboxStorage.loadLight`). */
export interface ReplicaSummary {
  keyspaceId: string;
  head: ReplicaHead;
  base: Omit<ReplicaBase, "entries">;
  /** The entries of the pages: they grow with a widening (14 [CLI-72]). */
  entryCount: number;
}

/** A stored state with the replicas as summaries. */
export type StoredLight = Omit<StoredState, "replicas"> & { replicas: ReplicaSummary[] };

/** A row of a storage that is not what the storage wrote (damage, another build's shape): it is left out of what a read returns, and listed here (V19 N5). */
export interface UnreadableRow {
  /** The store of the row (`entries`, `deadLetters`, `replicas`, ...). */
  store: string;
  /** The key of the row in that store. */
  key: string;
  why: string;
}

export interface StoredState {
  entries: OutboxEntry[];
  deadLetters: DeadLetterEntry[];
  journal: JournalEntry[];
  exports: ExportEntry[];
  rejections: RejectionRecord[];
  drafts: DraftEntry[];
  replicas: ReplicaImage[];
  meta: Record<string, string | number | bigint>;
  /** The rows that the read left out because they are malformed, at most `MAX_UNREADABLE_LISTED` of them (a storage that cannot find a malformed row leaves this out). */
  unreadable?: UnreadableRow[];
}

/** How many unreadable rows a read lists: a damaged partition may hold any number, and the list is for a report. */
export const MAX_UNREADABLE_LISTED = 100;

/** The key of a draft in a partition: the keyspace, the key and the author, so that two principals of a shared device never overwrite each other's (14 [CLI-59]). */
export function draftKey(d: { keyspaceId: string; key: string; author?: Author | undefined }): string {
  return `${d.keyspaceId}\u0000${d.key}\u0000${d.author === undefined ? "" : `${d.author.issuer}|${d.author.sub}`}`;
}

/** The names of the stores that a bound can fill (14 [CLI-84]). */
export type StoreName = "outbox" | "deadLetters" | "journal" | "exports";

/**
 * The storage mode of the sync status (14 [CLI-21], [CLI-75]): `persisted` survives the process and the platform promised to keep it,
 * `best-effort` survives the process but the platform may evict it (best-effort browser storage has no durability guarantee), and
 * `memory` does not survive the process.
 */
export type StorageMode = "memory" | "best-effort" | "persisted";

export interface OutboxStorage {
  /** The storage mode, reported in the sync status (14 [CLI-21], [CLI-75]). It can change: a request for persistence can be granted. */
  readonly mode: StorageMode;
  /**
   * Asks the platform to keep the storage (14 [CLI-75]: `navigator.storage.persist()` in a browser). Resolves whether it is kept now;
   * the `mode` then says `persisted`. A platform with no such request leaves this out.
   */
  requestPersistence?(): Promise<boolean>;
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
  /**
   * `load()` without the pages and the tails of the replicas, which are most of what a partition holds: a follower tab (14 [CLI-61]) reads the
   * flusher's state on every message that the flusher sends, and reads the pages of a keyspace only when they changed. A storage that leaves
   * this out is read whole.
   */
  loadLight?(): Promise<StoredLight>;
  /**
   * The image of one keyspace: its pages unless `entries` is false, and its tail from the first record above `tailAfter` (all of it when unset).
   * Undefined when the storage holds none. Part of `loadLight`'s pair.
   */
  loadImage?(keyspaceId: string, opts?: { entries?: boolean; tailAfter?: bigint }): Promise<ReplicaImage | undefined>;
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
