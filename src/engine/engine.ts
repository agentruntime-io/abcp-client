// The client engine of ABPC v2 (14 §14.1 to §14.6): the engine model, prediction, the durable outbox, ordering and dependencies,
// events, and the reactions by class and cause. The public API is this class; the behaviour is in the layers it extends
// (core, endings, replica, reactions, sender, session).
//
// Out of scope here, each a TODO that names its rule: the offline horizon (14 §14.7), principals, tabs and devices (§14.8),
// revocation and quarantine (§14.9), interest and storage of the replica (§14.10), the local query layer (C4), the real network (C5).

import { create } from "@bufbuild/protobuf";
import type * as Y from "yjs";
import { CreateKeyspaceRequestSchema } from "../gen/abpc/v2/sync_pb.js";
import { ChangeKind } from "../gen/abpc/v2/types_pb.js";
import { CrdtKey } from "./crdt-view.js";
import { decodeContent, CanonError } from "./entry.js";
import { makeReject } from "./endings.js";
import type { EngineConfig } from "./core.js";
import { LocalRefusal } from "./errors.js";
import { Follow } from "./follow.js";
import type { KeyspaceView } from "./overlay.js";
import { SessionLayer } from "./session.js";
import type { DeadLetterEntry, DraftEntry, ExportEntry, JournalEntry, OutboxEntry, RejectionRecord } from "./storage.js";
import type { ActionInput, ChangeInput, EngineEvent, PreconditionInput, SubmitOptions, SubmitResult, SyncStatus } from "./types.js";
import { Code, ErrorClass } from "../gen/abpc/v2/types_pb.js";

/** The handle of a CRDT key shown in the view (14 [CLI-13]). */
export class CrdtHandle {
  constructor(
    private readonly engine: Engine,
    readonly keyspaceId: string,
    readonly key: string,
  ) {}

  private ck(): CrdtKey {
    const k = this.engine.crdtKey(this.keyspaceId, this.key);
    if (k === undefined) throw new LocalRefusal("NOT_FOLLOWED", `the key ${this.key} has no document`);
    return k;
  }

  /** The view document, which the user edits: the confirmed document with the user's pending updates applied. It is replaced only at a rebuild (`rebuilt` event); read it again then. */
  get doc(): Y.Doc {
    return this.ck().view as Y.Doc;
  }

  /** The confirmed document: the confirmed base and updates, by the fold of 05 [STATE-76]. */
  get confirmedDoc(): Y.Doc {
    return this.ck().confirmed;
  }

  /** The user has edits that are in no action yet. */
  get hasUnsent(): boolean {
    return this.ck().hasUnsent;
  }

  /** Puts the user's edits that are in no action yet into one action of the op type; resolves undefined when there are none. */
  async flush(opType: string): Promise<SubmitResult | undefined> {
    const ck = this.ck();
    const update = ck.takeUnsent();
    if (update === undefined) return undefined;
    try {
      return await this.engine.submit({ keyspaceId: this.keyspaceId, opType, changes: [{ key: this.key, kind: ChangeKind.CRDT_UPDATE, update }] });
    } catch (e) {
      ck.unsent.unshift(update); // nothing was accepted: the edits stay unsent
      throw e;
    }
  }
}

export class Engine extends SessionLayer {
  private constructor(cfg: EngineConfig) {
    super(cfg);
  }

  /**
   * Creates the engine and rehydrates it (14 [CLI-8]): reads the durable outbox, mints this run's `client_id` (14 [CLI-63]), and
   * restamps what has no definitive result. Confirmed state is loaded when the engine connects (`start`).
   */
  static async create(cfg: EngineConfig): Promise<Engine> {
    const e = new Engine(cfg);
    await e.run(() => e.rehydrate());
    return e;
  }

  private async rehydrate(): Promise<void> {
    await this.loadStored();
    // 14 [CLI-63]: a new client_id at every engine start, kept in memory only
    this.clientId = this.mintClientId();
    const keyspaces = new Set<string>();
    for (const e of [...this.entries.values()].sort((a, b) => a.ordinal - b.ordinal)) {
      // 14 [CLI-29]: an entry that cannot be decoded goes to the dead-letter store, and stops no other entry
      let ok = true;
      try {
        const m = decodeContent(e.content);
        ok = m.mutationId === e.mutationId;
      } catch (err) {
        if (!(err instanceof CanonError)) throw err;
        ok = false;
      }
      if (!ok) {
        this.follows.set(e.keyspaceId, this.follows.get(e.keyspaceId) ?? new Follow(e.keyspaceId, this.random));
        await this.endAction(e, { sink: "deadLetter", reject: makeReject(Code.INVALID_ENVELOPE, ErrorClass.DEAD_LETTER, "the stored content cannot be decoded"), reason: "UNDECODABLE", dependents: "keep" });
        continue;
      }
      // 14 [CLI-63]: what has no definitive result is re-stamped when it is next sent; the old slots mean nothing to a new instance
      if (e.outcome === undefined) {
        e.clientId = "";
        e.clientSeq = 0;
        e.afterClientSeq = 0;
        await this.tx([this.putOp(e)]);
      }
      keyspaces.add(e.keyspaceId);
    }
    for (const ks of keyspaces) this.addFollow(ks);
    await this.drainStuck();
    await this.purgeJournal().catch(() => undefined);
  }

  private addFollow(id: string): Follow {
    let f = this.follows.get(id);
    if (f === undefined) {
      f = new Follow(id, this.random);
      f.needsLoad = { cause: "COLD_LOAD", resume: false };
      this.follows.set(id, f);
    }
    return f;
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Lifecycle

  /** Starts the session: loads the followed keyspaces and subscribes (14 [CLI-8]). */
  start(): void {
    this.beginRunning();
  }

  /** Stops the session and every timer. The outbox stays in the storage. */
  async stop(): Promise<void> {
    this.stopped = true;
    this.endRunning();
    this.cancelTimers();
    for (const f of this.follows.values()) f.crdt.destroy();
    await this.settle().catch(() => undefined);
  }

  /** Follows a keyspace; it is loaded when the engine is online (14 [CLI-2]). */
  async addKeyspace(id: string): Promise<void> {
    await this.run(() => {
      if (this.follows.has(id)) return;
      this.addFollow(id);
    });
    this.kick(this.follows.get(id));
  }

  /**
   * Creates a keyspace (07 §7.11) and follows it. TODO(07 [MUT-101], 14 [CLI-45]): the wait of the hold `KEYSPACE_NOT_FOUND`
   * that counts from the end of a pending `CreateKeyspace` is counted from the first `KEYSPACE_NOT_FOUND` here.
   */
  async createKeyspace(id: string, kind: string): Promise<{ created: boolean; logEpoch: number }> {
    const r = await this.transport.createKeyspace(create(CreateKeyspaceRequestSchema, { keyspaceId: id, kind }));
    await this.addKeyspace(id);
    return { created: r.created, logEpoch: r.logEpoch };
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Authoring and reading

  /** Submits an action (14 [CLI-21]): resolves when it is in the durable outbox, or rejects with a `LocalRefusal`. */
  submit(input: ActionInput, options?: SubmitOptions): Promise<SubmitResult> {
    return this.submitAction(input, options);
  }

  /** The view of a keyspace: confirmed state with the prediction of the pending actions (14 [CLI-6]). */
  view(keyspaceId: string): KeyspaceView {
    const f = this.follows.get(keyspaceId);
    if (f === undefined) throw new LocalRefusal("NOT_FOLLOWED", `the keyspace ${keyspaceId} is not followed`);
    return this.viewOf(f);
  }

  /** The confirmed state alone. */
  confirmed(keyspaceId: string): KeyspaceView {
    const f = this.follows.get(keyspaceId);
    if (f === undefined) throw new LocalRefusal("NOT_FOLLOWED", `the keyspace ${keyspaceId} is not followed`);
    return this.confirmedViewOf(f);
  }

  /** Whether a keyspace's confirmed state is loaded, and its cursor. */
  cursor(keyspaceId: string): { loaded: boolean; seq: bigint; logEpoch: number } | undefined {
    const f = this.follows.get(keyspaceId);
    return f === undefined ? undefined : { loaded: f.loaded, seq: f.state.seq, logEpoch: f.state.logEpoch };
  }

  /** Opens a CRDT key for the user: its view document (14 [CLI-13]). */
  crdt(keyspaceId: string, key: string): CrdtHandle {
    const f = this.follows.get(keyspaceId);
    if (f === undefined || !f.loaded) throw new LocalRefusal("NOT_FOLLOWED", `the keyspace ${keyspaceId} is not loaded`);
    const ck = f.crdt.getOrCreate(key);
    const roots = this.manifests.get(f.state.manifestVersion)?.crdtRoots(key);
    if (roots !== undefined) ck.roots = roots;
    ck.open(this.pendingUpdates(keyspaceId, key, new Set()));
    return new CrdtHandle(this, keyspaceId, key);
  }

  /** @internal The CRDT key of a handle. */
  crdtKey(keyspaceId: string, key: string): CrdtKey | undefined {
    return this.follows.get(keyspaceId)?.crdt.get(key);
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Sync status and the app's stores (14 [CLI-41], CONF-6 test adapter)

  status(): SyncStatus {
    const cursors: SyncStatus["cursors"] = {};
    let loadsWithSkipped = 0;
    for (const f of this.follows.values()) {
      cursors[f.id] = { seq: f.state.seq, logEpoch: f.state.logEpoch };
      loadsWithSkipped += f.loadsWithSkipped;
    }
    return {
      pending: this.entries.size,
      deadLetters: this.deadLetters.size,
      exports: this.exports.size,
      skippedChanges: this.skippedChanges,
      loadsWithSkipped,
      unacknowledgedRejections: this.rejections.size,
      cursors,
      storageMode: this.storage.mode,
      fullStores: this.storage.full(),
      connection: this.connection(),
      suspended: this.holds.all().some((h) => h.kind === "APP_SUSPENDED"),
      holds: this.holds.info(),
    };
  }

  /** The outbox, in outbox order. */
  outbox(): OutboxEntry[] {
    return [...this.entries.values()].sort((a, b) => a.ordinal - b.ordinal);
  }

  deadLetterEntries(): DeadLetterEntry[] {
    return [...this.deadLetters.values()];
  }

  journalEntries(): JournalEntry[] {
    return [...this.journal.values()];
  }

  exportEntries(): ExportEntry[] {
    return [...this.exports.values()];
  }

  rejectionEntries(): RejectionRecord[] {
    return [...this.rejections.values()];
  }

  draftEntries(): DraftEntry[] {
    return [...this.drafts.values()];
  }

  /** The events emitted so far. */
  eventLog(): EngineEvent[] {
    return [...this.bus.history];
  }

  /**
   * Resubmits a dead-letter entry as a new action, at the user's instruction (02 [ARC-50], 14 [CLI-28]): a fresh mutation id, the
   * values of 14 [CLI-17] and [CLI-18] taken from the confirmed state now. The entry is acknowledged.
   */
  async resubmitDeadLetter(mutationId: string): Promise<SubmitResult> {
    const d = this.deadLetters.get(mutationId);
    if (d === undefined) throw new LocalRefusal("REFUSED", `no dead-letter entry ${mutationId}`);
    const m = decodeContent(d.content);
    const changes: ChangeInput[] = m.changes.map((c) => {
      const ci: ChangeInput = { key: c.key, kind: c.kind };
      if (c.value.length > 0) ci.value = c.value;
      if (c.isNull) ci.isNull = true;
      if (c.encoding !== "") ci.encoding = c.encoding;
      if (c.delta !== 0n) ci.delta = c.delta;
      if (c.update.length > 0) ci.update = c.update;
      const ex = c.expected?.expect;
      if (ex?.case === "value") ci.expected = { value: ex.value };
      else if (ex?.case === "absent") ci.expected = { absent: true };
      else if (ex?.case === "version") ci.expected = {};
      return ci;
    });
    const preconditions: PreconditionInput[] = m.preconditions.map((p) => {
      const pi: PreconditionInput = {};
      if (p.target.case === "key") pi.key = p.target.value;
      if (p.target.case === "prefix") pi.prefix = p.target.value;
      if (p.check.case === "unchangedSince") pi.unchangedSince = "confirmed";
      else if (p.check.case === "exists") pi.exists = true;
      else if (p.check.case === "absent") pi.absent = true;
      else if (p.check.case === "afterMutation") pi.afterMutation = p.check.value;
      else if (p.check.case === "expectedSeq") pi.expectedSeq = p.check.value;
      return pi;
    });
    const r = await this.submit({ keyspaceId: d.keyspaceId, opType: d.opType, changes, preconditions, blobRefs: [...m.blobRefs] });
    await this.ackDeadLetter(mutationId);
    return r;
  }
}
