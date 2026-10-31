// What the engine keeps for each keyspace it follows (14 [CLI-1]): the confirmed state, its cursor and epoch, the keyspace's
// kind, lifecycle and manifest version, the lane state of the client instance (14 [CLI-27], [CLI-78]) and the CRDT documents.

import { KeyspaceLifecycle, type SuccessorRule } from "../gen/abpc/v2/types_pb.js";
import { ReplicaState } from "../rules/fold/index.js";
import type { Random } from "./clock.js";
import { CrdtStore } from "./crdt-view.js";
import type { LaneName } from "./manifests.js";
import type { KeyspaceView } from "./overlay.js";

/** The state of one lane of the current client instance in one keyspace (07 §7.4). */
export class LaneState {
  /** The processed watermark for the current `client_id` and the keyspace's epoch; it never decreases (14 [CLI-78]). */
  watermark = 0;
  /** The slots that are assigned and not consumed, with the action at each (14 [CLI-27]). */
  readonly assigned = new Map<number, string>();

  /** The lowest slot above the watermark that no action holds: slots are contiguous from 1 (14 [CLI-27]). */
  allocate(mutationId: string): number {
    let n = this.watermark + 1;
    while (this.assigned.has(n)) n++;
    this.assigned.set(n, mutationId);
    return n;
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
  /** The engine no longer follows the keyspace (a drop reason of A.6): the local copy stays, the outbox stays. */
  stopped: string | undefined;
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
  /** Whether the first cold load of this follow was made (for the `COLD_LOAD` cause). */
  everLoaded = false;
  /** The load answered `skipped_unsupported` (14 [CLI-41]). */
  loadsWithSkipped = 0;
  /** The view, until the overlay changes (14 [CLI-6]). */
  cachedView: KeyspaceView | undefined;
  /** Stamped by the engine's own `CreateKeyspace` and not yet answered (07 [MUT-101]). */
  creating = false;

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

  invalidate(): void {
    this.cachedView = undefined;
  }
}
