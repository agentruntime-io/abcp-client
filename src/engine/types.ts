// The public types of the engine: what an app submits, what it is told (14 §14.5), the hooks it registers, and the sync status
// it can read (14 [CLI-41]). The stored records of the durable outbox are in storage.ts.

import type { ChangeKind, Reject, Stale } from "../gen/abpc/v2/types_pb.js";
import type { KeyChange, Precondition } from "../gen/abpc/v2/types_pb.js";
import type { LaneName } from "./manifests.js";
import type { StorageMode } from "./storage.js";

export type { LaneName } from "./manifests.js";

// ---------------------------------------------------------------------------------------------------------------------
// Authoring

/** One requested change of an action. `seen_version` is the engine's: it takes it from the confirmed state (14 [CLI-17]). */
export interface ChangeInput {
  key: string;
  kind: ChangeKind;
  /** SET, CAS and OVERRIDE: the value bytes. */
  value?: Uint8Array;
  isNull?: boolean;
  /** The value encoding; default `json/v1`. */
  encoding?: string;
  /** INCR. */
  delta?: bigint;
  /** CRDT_UPDATE. */
  update?: Uint8Array;
  /** CAS: exactly one member. A `version` that is left out is read from the confirmed state (14 [CLI-17]). */
  expected?: { version?: bigint } | { value: Uint8Array } | { absent: true };
}

/** A precondition of an action (05 §5.7). The engine sets its `log_epoch` and `unchanged_since` from the confirmed state (14 [CLI-18]). */
export interface PreconditionInput {
  key?: string;
  prefix?: string;
  exists?: true;
  absent?: true;
  afterMutation?: string;
  expectedSeq?: bigint;
  /** A seq, or `"confirmed"` for the confirmed watermark of the keyspace (14 [CLI-18]). */
  unchangedSince?: bigint | "confirmed";
}

/** What an app submits. */
export interface ActionInput {
  keyspaceId: string;
  opType: string;
  changes: ChangeInput[];
  preconditions?: PreconditionInput[];
  blobRefs?: string[];
  /** Only an override-only action has a reason (07 [MUT-18]). */
  reason?: string;
  causationId?: string;
  correlationId?: string;
}

export interface SubmitOptions {
  /** Explicit links, client-only (14 [CLI-30] item 4): this action depends on each of these earlier actions. */
  after?: readonly string[];
  /**
   * @internal The action that this one replaces in the conflict handler (14 [CLI-47]): it takes the place of that action in the
   * order, so it depends on what that action depended on, and never on the actions that move to it.
   */
  replaces?: string;
}

export interface SubmitResult {
  /** The mutation id, minted once (14 [CLI-23]). The action is in the durable outbox when `submit` returns (14 [CLI-21]). */
  mutationId: string;
}

/** An action as the hooks see it: the stored content, decoded. */
export interface ActionSnapshot {
  mutationId: string;
  keyspaceId: string;
  opType: string;
  lane: LaneName;
  changes: readonly KeyChange[];
  preconditions: readonly Precondition[];
  /** The stored content, byte for byte (14 [CLI-22]). */
  content: Uint8Array;
}

// ---------------------------------------------------------------------------------------------------------------------
// Events (14 §14.5)

interface Base {
  /** A sequence number of the engine's event stream, from 1. */
  n?: number;
}

export interface CommittedEvent extends Base {
  type: "committed";
  mutationId: string;
  keyspaceId: string;
  /** `seq` of an `Appended`, `decision_seq` of a `Decided`. */
  seq: bigint;
}

export interface CorrectedEvent extends Base {
  type: "corrected";
  mutationId: string;
  keyspaceId: string;
  seq: bigint;
  /** The keys whose applied changes differ from the prediction. */
  keys: string[];
  /** `NO_EFFECT` for a CRDT update that folded to no effect (14 [CLI-40], A.11). */
  cause?: "NO_EFFECT";
}

export interface RejectedEvent extends Base {
  type: "rejected";
  mutationId: string;
  keyspaceId: string;
  /** The reject; absent for the local refusal `OFFLINE` (14 [CLI-82]). */
  reject?: Reject;
  /** `OFFLINE` for an action of a `fail_fast` op type authored offline (A.11). */
  cause?: "OFFLINE";
}

export interface OverwrittenEvent extends Base {
  type: "overwritten";
  mutationId: string;
  keyspaceId: string;
  seq: bigint;
  keys: string[];
  /** The overwriting principal's subject; for a CRDT override, the override's. */
  by: string;
}

export interface ConflictEvent extends Base {
  type: "conflict";
  mutationId: string;
  keyspaceId: string;
  reject: Reject;
  stale?: Stale;
}

export interface DeadLetteredEvent extends Base {
  type: "dead-lettered";
  mutationId: string;
  keyspaceId: string;
  reason: string;
}

export interface OutcomeUnknownEvent extends Base {
  type: "outcome-unknown";
  mutationId: string;
  keyspaceId: string;
  /** A.11: `EPOCH_CHANGED`, `HORIZON`, `SUPPORT_WINDOW`, `HOLD_BOUND`, or a drop reason. */
  cause: string;
  lastResult?: string;
}

export interface ExportedEvent extends Base {
  type: "exported";
  mutationId: string;
  keyspaceId: string;
  reason: string;
}

export interface QuarantinedEvent extends Base {
  type: "quarantined";
  keyspaceId: string;
  cause: string;
  mutationIds: string[];
}

export interface HeldEvent extends Base {
  type: "held";
  state: "start" | "stop";
  scope: "lane" | "keyspace" | "app" | "action";
  keyspaceId?: string;
  lane?: LaneName;
  mutationId?: string;
  /** The code or reason: a code name, `PROTOCOL_ERROR`, `CONFLICT_WAIT`, `EPOCH_RESOLUTION`, `NO_TOKEN` (A.11). */
  code: string;
  /** The hold's bound, in server time (ms since the epoch), when it has one (14 [CLI-45]). */
  bound?: number;
  /** A hold for bytes that the engine lost (`BLOB_MISSING`, scope `action`): the blobs that the app stages again, under these ids, to clear it. */
  blobIds?: string[];
}

export interface ReloadedEvent extends Base {
  type: "reloaded";
  keyspaceId: string;
  /** `COLD_LOAD`, `EPOCH_CHANGED` or `RESUME_TOO_OLD` (A.11). */
  cause: string;
  logEpoch: number;
  checkpointSeq: bigint;
}

export interface ReroutedEvent extends Base {
  type: "rerouted";
  mutationId: string;
  from: string;
  to: string;
}

export interface SagaEndedEvent extends Base {
  type: "saga-ended";
  keyspaceId: string;
  sagaId: string;
  status: string;
  cause?: string;
}

export interface RevertedEvent extends Base {
  type: "reverted";
  mutationId: string;
  keyspaceId: string;
  /** The seq and epoch of the outcome that the restore lost. */
  seq: bigint;
  logEpoch: number;
}

export interface ResentEvent extends Base {
  type: "resent";
  mutationId: string;
  keyspaceId: string;
  /** `VOIDED` or `OUTCOME_UNKNOWN` (A.11). */
  cause: "VOIDED" | "OUTCOME_UNKNOWN";
  seq?: bigint;
  logEpoch?: number;
}

export interface RebuiltEvent extends Base {
  type: "rebuilt";
  keyspaceId: string;
  key: string;
  /** The rejected or cancelled mutation id, or `EPOCH_CHANGED`, `RESUME_TOO_OLD`, `COLD_LOAD`, or `REDACTED` (A.11). */
  cause: string;
}

export interface SkippedEvent extends Base {
  type: "skipped";
  keyspaceId: string;
  /** The record's seq, or the load's `checkpoint_seq` for a load that skipped records. */
  seq: bigint;
  /** The keys whose value could not be decoded; empty for a load that skipped records. */
  keys: string[];
  /**
   * `ANOMALY`: the record carried changes that its kind does not define, and the fold ignored them (02 [ARC-46], 06 [LOG-41]); the keys
   * are theirs. Absent for an undecodable value ([CLI-5]) and for a load that skipped records.
   */
  cause?: "ANOMALY";
  /** For `ANOMALY`: each ignored change, with the rule that makes it ignored. */
  anomalies?: { index: number; key: string; rule: string; detail: string }[];
}

export interface LeaseExpiringEvent extends Base {
  type: "lease-expiring";
  keyspaceId: string;
  key: string;
  expiresAt: number;
}

export interface OfflineHorizonEvent extends Base {
  type: "offline-horizon";
  keyspaceId: string;
  /** The age that reached the warning threshold (14 [CLI-56]), in ms of server time ([CLI-55]). */
  ageMs: number;
  /** The `max_offline` in force, in ms ([CLI-54]). */
  maxOfflineMs: number;
  /** `ACTION`: the oldest unsynced action of the keyspace; `SYNC`: the time since the keyspace's last successful sync. */
  kind?: "ACTION" | "SYNC";
  /** For `ACTION`: the oldest unsynced action (not `mutationId`, which would make the warning an event of that action: it is not one of its outcomes, 14 [CLI-38]). */
  oldestMutationId?: string;
}

/** The engine's role among the tabs of its device changed (14 [CLI-61]). */
export interface RoleEvent extends Base {
  type: "role";
  role: "flusher" | "follower" | "stopped";
  /** `ELECTED`: it holds the flusher lock (at start, or as the tab that a handover chose); `HANDED_OVER`: a hidden flusher gave the role to a visible tab and is a follower now; `STOPPED`: it gave the lock up and left. */
  cause: "ELECTED" | "FOLLOWING" | "STOPPED" | "HANDED_OVER";
  /** For a flusher: its new `client_id` (14 [CLI-63]). */
  clientId?: string;
}

/** The interest of a keyspace changed (14 [CLI-71], [CLI-72]). */
export interface InterestChangedEvent extends Base {
  type: "interest-changed";
  keyspaceId: string;
  /** The interest now: empty is the whole keyspace. */
  interest: string[];
  cause: "WIDENED" | "NARROWED";
}

export interface ProtocolErrorEvent extends Base {
  type: "protocol-error";
  /** The message's type, or the field of an `AppLimits` that carried a forbidden value; `Mutate` for a whole-call failure (14 §14.5). */
  what: string;
  detail: string;
  keyspaces: string[];
  /** For a `Mutate` call that failed as a whole with a status that has no ABPC detail and no reaction in A.7 (14 [CLI-50]): the RPC status. */
  status?: string;
  /** For such a `Mutate` failure: the lanes of the request, which the client holds (Table 14.1). */
  lanes?: { keyspaceId: string; lane: LaneName }[];
}

export type EngineEvent =
  | CommittedEvent
  | CorrectedEvent
  | RejectedEvent
  | OverwrittenEvent
  | ConflictEvent
  | DeadLetteredEvent
  | OutcomeUnknownEvent
  | ExportedEvent
  | QuarantinedEvent
  | HeldEvent
  | ReloadedEvent
  | ReroutedEvent
  | SagaEndedEvent
  | RevertedEvent
  | ResentEvent
  | RebuiltEvent
  | SkippedEvent
  | LeaseExpiringEvent
  | OfflineHorizonEvent
  | RoleEvent
  | InterestChangedEvent
  | ProtocolErrorEvent;

export type EngineEventType = EngineEvent["type"];

export type EngineEventListener = (event: EngineEvent) => void;

// ---------------------------------------------------------------------------------------------------------------------
// Hooks

/** What the conflict handler is called with (14 [CLI-47]). */
export interface ConflictContext {
  action: ActionSnapshot;
  reject: Reject;
  stale: Stale | undefined;
  /** The confirmed state at the cursor, which is at least `Stale.current_seq` unless the wait ended at its bound ([CLI-45], `CONFLICT_WAIT`). */
  confirmed: import("./overlay.js").KeyspaceView;
  /** Keep mine or merge: authors a new action with a fresh mutation id (14 [CLI-23]), only because the user chose it. */
  submit(input: ActionInput, options?: SubmitOptions): Promise<SubmitResult>;
  /** Replace: authors a new action that the old one's dependents move to (14 [CLI-34], [CLI-47]). */
  replaceWith(input: ActionInput, options?: SubmitOptions): Promise<SubmitResult>;
}

/** The app's conflict handler (14 [CLI-47]). The engine awaits it, then emits `rejected`. */
export type ConflictHandler = (ctx: ConflictContext) => void | Promise<void>;

/** An entry that the engine hands to the app's export hook (14 [CLI-57], [CLI-83]). */
export interface ExportRecord {
  mutationId: string;
  keyspaceId: string;
  opType: string;
  content: Uint8Array;
  reason: string;
  reject: Reject | undefined;
  lastResult: string | undefined;
  /** The mutation id of the action whose end made this one go to the hook (14 [CLI-34]). */
  dependencyOf: string | undefined;
}

export type ExportHook = (entry: ExportRecord) => void | Promise<void>;

/** The token the engine sends is the transport's. When a call fails with `Unauthenticated`, the engine asks the app for a new one (14 [CLI-50], [CLI-68]). */
export interface TokenProvider {
  /** Gets a valid token and installs it in the transport. Resolves false when none can be had (the hold `NO_TOKEN`). */
  refresh(): Promise<boolean>;
  /**
   * The expiry of the token that the transport sends now, in ms since the epoch (server time), when the app knows it. The engine
   * refreshes the token in band before it expires (14 [CLI-68], 10 §10.7): it asks for a new one with `refresh` and tells the session with
   * `RefreshSession`. Without it the engine refreshes when a call or the session is refused for the token.
   */
  expiresAt?(): number | undefined;
}

/**
 * A predictor for the effects of a GATED op type (14 [CLI-15]): the changes that the gate is expected to make. Without one, the
 * effects are shown as placeholders: nothing is predicted for them, and an action that commits with effects is `corrected`.
 */
export type Predictor = (action: ActionSnapshot) => ChangeInput[];

// ---------------------------------------------------------------------------------------------------------------------
// Sync status (14 [CLI-41])

export interface HoldInfo {
  scope: "lane" | "keyspace" | "app" | "action";
  keyspaceId?: string;
  lane?: LaneName;
  mutationId?: string;
  code: string;
  /** The hold's bound in server time, ms since the epoch. */
  bound?: number;
  /** A hold for bytes that the engine lost (`BLOB_MISSING`, scope `action`): the blobs that the app stages again, under these ids, to clear it. */
  blobIds?: string[];
}

export interface SyncStatus {
  pending: number;
  deadLetters: number;
  /** Export-hook entries that the app has not acknowledged (14 [CLI-83]). */
  exports: number;
  skippedChanges: number;
  loadsWithSkipped: number;
  /** The changes that records carried and that their kind does not define, per keyspace (02 [ARC-46]); they count among `skippedChanges`. */
  anomalies: Record<string, number>;
  unacknowledgedRejections: number;
  /** The rows of the storage that were malformed and were left out when it was read (V19 N5); `Engine.unreadableRows()` lists them. */
  unreadableRows: number;
  cursors: Record<string, { seq: bigint; logEpoch: number }>;
  /** The interest prefixes of each followed keyspace; empty is the whole keyspace (14 [CLI-71]). */
  interest: Record<string, string[]>;
  /** The keyspaces that the engine no longer follows, with the reason (a drop reason of A.6): `ACCESS_REVOKED`, `GRANT_CHANGED`, `DELETED`, `MOVED`, `ARCHIVED`, `NOT_FOUND`. */
  stopped: Record<string, string>;
  /** The engine's role among the tabs of its device (14 [CLI-61]). */
  role: "flusher" | "follower" | "stopped";
  /** Entries of the partition whose author is not the current principal: kept in place and never sent (14 [CLI-60]). */
  lockedEntries: number;
  /** `persisted`, `best-effort` or `memory` (14 [CLI-75]). */
  storageMode: StorageMode;
  fullStores: string[];
  connection: "live" | "connecting" | "offline" | "stopped";
  /** A `Subscribe` that is being retried after `Retry{KEYSPACE_SEEDING}` (14 [CLI-41], [CLI-44]): the reason; undefined when none is. */
  subscribeRetry: string | undefined;
  suspended: boolean;
  holds: HoldInfo[];
  /** The calls of this tab that wait for the flusher's answer (14 [CLI-61], [CLI-21]): a follower never refuses a call while the flusher may still commit it. */
  pendingCalls: number;
  /** Where the bytes that actions name are kept (09 [READ-94], 14 [CLI-21]): `durable` outlives the process, `memory` does not. */
  blobStash: "memory" | "durable";
  /** The blobs staged for actions, and their octets, as the flusher counts them (a follower reports the flusher's count). */
  stagedBlobs: number;
  stagedBytes: number;
}
