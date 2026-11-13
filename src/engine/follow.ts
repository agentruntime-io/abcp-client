// What the engine keeps for each keyspace it follows (14 [CLI-1]): the confirmed state, its cursor and epoch, the keyspace's
// kind, lifecycle and manifest version, the lane state of the client instance (14 [CLI-27], [CLI-78]) and the CRDT documents.

import { KeyspaceLifecycle, type SuccessorRule } from "../gen/abpc/v2/types_pb.js";
import { ReplicaState } from "../rules/fold/index.js";
import type { Random } from "./clock.js";
import { CrdtStore } from "./crdt-view.js";
import type { LaneName } from "./manifests.js";
import type { PendingErasure } from "./erasure.js";
import type { KeyspaceView } from "./overlay.js";
import type { ExportEntry } from "./storage.js";
import type { KeyTouch, Shown, ViewFeed } from "./view-feed.js";

/** The state of one lane of the current client instance in one keyspace (07 §7.4). */
export class LaneState {
  /** The processed watermark for the current `client_id` and the keyspace's epoch; it never decreases (14 [CLI-78]). */
  watermark = 0;
  /** The slots that are assigned and not consumed, with the action at each (14 [CLI-27]). */
  readonly assigned = new Map<number, string>();

  /**
   * The authoring order (`OutboxEntry.ordinal`) of the action that holds each slot that this client instance assigned in the lane, the
   * slots that the server consumed included, whether or not the action has left the outbox since (14 [CLI-32]: `after_client_seq` is
   * "the highest `client_seq` the current client instance assigned to an earlier SEQUENCED action"). A slot that was given up is
   * forgotten: it holds no action, and the next one of the lane takes it.
   */
  private readonly ordinals = new Map<number, number>();
  /** The highest slot of the consumed slots that `compact` folded away: every action that can still ask was authored after them. */
  private floor = 0;

  /** The lowest slot above the watermark that no action holds: slots are contiguous from 1 (14 [CLI-27]). `ordinal` is the action's authoring order. */
  allocate(mutationId: string, ordinal = 0): number {
    let n = this.watermark + 1;
    while (this.assigned.has(n)) n++;
    this.assigned.set(n, mutationId);
    this.ordinals.set(n, ordinal);
    return n;
  }

  /**
   * The highest slot that this client instance assigned in the lane to an action authored before `ordinal`, a retired one included; 0 when
   * it assigned none (14 [CLI-32], 07 §7.5, the Q42.3 ruling: the text decides reading B).
   */
  highestBefore(ordinal: number): number {
    let h = this.floor;
    for (const [slot, o] of this.ordinals) if (o < ordinal && slot > h) h = slot;
    return h;
  }

  /**
   * Folds the slots that the server consumed, of actions authored before `before`, into one number, so that the memory of the slots that
   * have left the outbox is bounded by the actions in flight and not by the session: `before` is the authoring order of the earliest
   * action that can still ask `highestBefore` (every later one is authored after all of these), or Infinity when none is.
   */
  compact(before: number): void {
    for (const [slot, o] of [...this.ordinals]) {
      if (slot > this.watermark || o >= before) continue;
      if (slot > this.floor) this.floor = slot;
      this.ordinals.delete(slot);
    }
  }

  /** Slots whose action left the outbox before a reading of the watermark said whether the server consumed them (14 [CLI-27]). */
  private readonly deferred = new Map<number, string>();

  /** Raises the watermark (never lowers it) and forgets the slots it covers; returns whether it rose. */
  raise(to: number): boolean {
    if (to <= this.watermark) return false;
    this.watermark = to;
    for (const slot of [...this.assigned.keys()]) if (slot <= to) this.assigned.delete(slot);
    for (const slot of [...this.deferred.keys()]) if (slot <= to) this.deferred.delete(slot);
    return true;
  }

  release(slot: number, mutationId: string): void {
    if (this.assigned.get(slot) === mutationId || this.deferred.get(slot) === mutationId) this.ordinals.delete(slot);
    if (this.assigned.get(slot) === mutationId) this.assigned.delete(slot);
    if (this.deferred.get(slot) === mutationId) this.deferred.delete(slot);
  }

  /** The slot stays with the lane until a reading of the watermark says whether it was consumed (see `settle`). */
  defer(slot: number, mutationId: string): void {
    if (this.assigned.get(slot) === mutationId) this.deferred.set(slot, mutationId);
  }

  /**
   * A reading of the watermark came. 14 [CLI-27]: "If the watermark stays just below a slot after the slot's action got a result, that
   * result recorded nothing": the slot of an action that has left the outbox, which the reading leaves just above the watermark, is given
   * to the next action of the lane. A slot higher up waits until the watermark has reached the slot below it.
   */
  settle(): void {
    for (const [slot, id] of [...this.deferred]) {
      if (slot <= this.watermark) this.deferred.delete(slot);
      else if (slot === this.watermark + 1) this.release(slot, id);
    }
  }

  reset(): void {
    this.watermark = 0;
    this.assigned.clear();
    this.deferred.clear();
    this.ordinals.clear();
    this.floor = 0;
  }
}

/** A keyspace that the engine follows. */
export class Follow {
  state = new ReplicaState();
  /** A load installed confirmed state: its watermark and epoch are real, even for an empty keyspace (14 [CLI-18]). */
  loaded = false;
  kind = "";
  lifecycle: KeyspaceLifecycle = KeyspaceLifecycle.ACTIVE;
  successors: SuccessorRule[] = [];
  /** The last delivery watermark received (09 [READ-45]); a frame's `prev_cursor` must equal it (14 [CLI-3]). */
  watermark = 0n;
  /** The `record_hash` of the last record applied, with its seq, for a verifiable resume (09 [READ-45]). */
  lastRecordHash: Uint8Array | undefined;
  lastRecordSeq = 0n;
  /** The keyspace must be loaded before it is followed again: the cause of the `reloaded` event (A.11), and whether the load is a resume check from the cursor (14 [CLI-51]). */
  needsLoad: { cause: string; resume: boolean } | undefined;
  readonly crdt: CrdtStore;
  readonly lanes: Record<LaneName, LaneState> = { SEQUENCED: new LaneState(), GATED: new LaneState() };
  /**
   * The engine no longer follows the keyspace (a drop reason of A.6): the local copy stays, the outbox stays. A change of it is reported to the change
   * feed (view-feed.ts): a reader of the copy says that the engine stopped following it, though no key changed.
   */
  get stopped(): string | undefined {
    return this.stoppedBecause;
  }

  set stopped(v: string | undefined) {
    if (v === this.stoppedBecause) return;
    this.stoppedBecause = v;
    this.feed?.mark(this);
  }

  private stoppedBecause: string | undefined;
  /** The fold stopped at a record it cannot read (14 [CLI-4]): the confirmed state is frozen at the previous seq. */
  frozen: string | undefined;
  /** The live session follows the keyspace: a `KeyspaceAdded` was seen and no `KeyspaceDropped` since. */
  onSession = false;
  /** A resume of the keyspace is under way after a chain break (09 [READ-46]): frames that do not continue are ignored. */
  resuming = false;
  /**
   * The keyspace was named to a live session and its `KeyspaceAdded` (or `KeyspaceDropped`) has not come: the client does not know yet
   * whether the epoch that it follows is the keyspace's, and sends it nothing (14 [CLI-24], [CLI-53], [CLI-64]).
   */
  awaitingAdd = false;
  /** A load is under way. */
  loading = false;
  /** Consecutive loads that failed with a transient error, the exponent of the client backoff before the next one (14 [CLI-43]). */
  loadFailures = 0;
  /** Consecutive adds that a `Retry` rejected, the exponent of the client backoff before the next one (14 [CLI-43], [CLI-44]). */
  addFailures = 0;
  /**
   * The add of the keyspace was rejected `QUOTA_EXCEEDED` (09 [READ-35], A.7 "Stop and report; never resend unchanged in a loop"): how many keyspaces the
   * engine followed, and the `max_keyspaces_per_session` in force, then. It is not added again until the engine follows fewer or the limit rises; which
   * keyspaces to stop following is the app's choice. Reported in `SyncStatus.stopped` as `QUOTA_EXCEEDED`.
   */
  overSessionCap: { followed: number; limit: number } | undefined = undefined;
  /** Whether the first cold load of this follow was made (for the `COLD_LOAD` cause). */
  everLoaded = false;
  /** The load answered `skipped_unsupported` (14 [CLI-41]). */
  loadsWithSkipped = 0;
  /** The view, until the overlay changes (14 [CLI-6]). */
  cachedView: KeyspaceView | undefined;
  /** Stamped by the engine's own `CreateKeyspace` and not yet answered (07 [MUT-101]). */
  creating = false;
  /** The interest prefixes of the keyspace (14 [CLI-71]); empty is the whole keyspace. The state holds only the keys under them. */
  interest: string[] = [];
  /**
   * A widening of the interest is under way or waits for a session (14 [CLI-72]): `to` is the interest that the app asked for, `added`
   * the prefixes that the load of the widening names. `interest` is the old one until the load is installed.
   */
  widening: { to: string[]; added: string[] } | undefined;
  /** The persisted replica (§14.10): whether the image of this keyspace follows the in-memory state, and what its tail holds. */
  image: { ok: boolean; seqs: Set<bigint>; headMs: number; baseSeq?: bigint; entryCount?: number } = { ok: false, seqs: new Set(), headMs: 0 };
  /** The estimate of the server time, in ms, of the last successful sync of the keyspace (14 [CLI-56]); 0 when none was. */
  lastSyncMs = 0;
  /** The local copy was purged (14 [CLI-66], [CLI-67]): nothing of the keyspace is shown, and nothing is authored into it. */
  purged = false;
  /** `ACCESS_REVOKED` or the revocation of the subject was handled for this keyspace (14 [CLI-66]): its copy is purged and its outbox quarantined, once. */
  revoked = false;
  /** The purge of the revocation is committed to the storage, with the record that the keyspace is revoked (revocation.ts). */
  revocationPurged = false;
  /** The purge and the quarantine of the revocation are both done and recorded: a restart has nothing left of them to do. */
  revocationDone = false;
  /** The actions of the keyspace that the revocation left to `GetMutationResult` were asked about (14 [CLI-7]). */
  revocationAsked = false;
  /** The failed attempts of the durable part of the revocation, the exponent of the client backoff before the next one. */
  revocationAttempts = 0;
  /** The journal entries that the purge removes, as they were when the memory was purged: a retry of the purge removes them from the storage. */
  purgeJournal: string[] = [];
  /** The actions that the quarantine took, for the one `quarantined` event of the keyspace. */
  quarantinedIds: string[] = [];
  /** The export entries that the quarantine made and that the app has not been told of: `exported` follows the `quarantined` event of the keyspace (14 [CLI-66]). */
  exportedPending: ExportEntry[] = [];
  /** The changes that records carried and that their kind does not define, counted (02 [ARC-46]). */
  anomalies = 0;
  /** What redactions of the keyspace have erased since the last transaction that made it durable (14 [CLI-76], `erasure.ts`): committed with the records or the load that carried them. */
  erasure: PendingErasure | undefined;
  /** Keys outside the interest that the app loaded to author a change (14 [CLI-73]): the version that the load showed, or 0 for a key that was absent. */
  readonly outside = new Map<string, bigint>();

  constructor(
    readonly id: string,
    random: Random,
  ) {
    this.crdt = new CrdtStore(random);
  }

  /** The confirmed cursor: the seq of the last record applied, or of the last delivery watermark (14 [CLI-3]). */
  get seq(): bigint {
    return this.state.seq;
  }

  get logEpoch(): number {
    return this.state.logEpoch;
  }

  /** The engine's change feed, which `FollowMap` sets when the follow enters the map (view-feed.ts). */
  feed: ViewFeed | undefined;
  /** What the feed last reported of this follow, which its next report is compared with; only kept while the feed has a listener. */
  shown: Shown | undefined;
  private touchedKeys = new Set<string>();
  private touchedSubtrees = new Set<string>();

  /** The view changed: the overlay (the pending actions) or the confirmed state; the feed reports it at the end of the engine's task. */
  invalidate(): void {
    this.cachedView = undefined;
    this.feed?.mark(this);
  }

  /**
   * Records the keys that an applied record names, for the change feed: each key's row may differ, and, for a `DELETE`, the subtree below
   * the key may too (05 [STATE-43]). Nothing is kept while no one listens.
   */
  touch(keys: Iterable<string>, subtrees: Iterable<string> = []): void {
    if (this.feed === undefined || !this.feed.active) return;
    for (const k of keys) this.touchedKeys.add(k);
    for (const k of subtrees) this.touchedSubtrees.add(k);
    this.feed.mark(this);
  }

  /** The keys recorded by `touch` since the last call. */
  takeTouched(): KeyTouch {
    const out = { keys: this.touchedKeys, subtrees: this.touchedSubtrees };
    this.touchedKeys = new Set();
    this.touchedSubtrees = new Set();
    return out;
  }
}
