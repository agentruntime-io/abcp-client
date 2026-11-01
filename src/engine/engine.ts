// The client engine of ABPC v2 (14 §14.1 to §14.10): the engine model, prediction, the durable outbox, ordering and dependencies,
// events, the reactions by class and cause, the offline horizon, principals, tabs and devices, revocation and quarantine, and interest
// and storage. The public API is this class; the behaviour is in the layers it extends (core, endings, replica-store, replica,
// reactions, sender, session, horizon, interest, revocation, tab-layer).
//
// Out of scope here, each a TODO that names its rule: the local query layer (C4), the real network (C5), a redaction's purge of the
// journal and the dead-letter store (14 [CLI-76], R2).

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
import { checkInterest, covers } from "./replica-store.js";
import { getMember, parse as parseJson } from "../rules/jsonv1/index.js";
import { StateValue } from "../rules/fold/index.js";
import { TabLayer } from "./tab-layer.js";
import type { DeadLetterEntry, DraftEntry, ExportEntry, JournalEntry, OutboxEntry, RejectionRecord } from "./storage.js";
import type { PartitionKey } from "./tabs.js";
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

/** What `signOut` did, or what it needs the app to decide (14 [CLI-65]). */
export type SignOutResult =
  | { done: true; outcome: "purged" | "locked" | "discarded"; unsynced: number }
  | { done: false; unsynced: number; options: ("keep-locked" | "discard")[] };

export interface SignOutOptions {
  /** What to do with entries that are not synced and cannot be flushed now: keep them in a locked partition, or discard them with a report to the export hook. */
  unsynced?: "keep-locked" | "discard";
}

export class Engine extends TabLayer {
  private constructor(cfg: EngineConfig) {
    super(cfg);
  }

  /**
   * Creates the engine: opens the storage of its partition, elects the flusher among the tabs of the device (14 [CLI-61]), and, as the
   * flusher, rehydrates (14 [CLI-8]): reads the durable outbox, mints this run's `client_id` (14 [CLI-63]), restores the persisted
   * replicas (14 [CLI-2]) and re-stamps what has no definitive result. Confirmed state is loaded or resumed when the engine connects
   * (`start`).
   */
  static async create(cfg: EngineConfig): Promise<Engine> {
    const e = new Engine(cfg);
    await e.run(() => e.boot(cfg));
    return e;
  }

  /** The flusher's start, which a restart and a handover share (14 [CLI-8], [CLI-63]). */
  protected async becomeFlusher(): Promise<void> {
    this.role = "flusher";
    // what a follower mirrored is read again from the storage, which is the truth
    for (const f of this.follows.values()) f.crdt.destroy();
    this.follows.clear();
    this.entries.clear();
    this.deadLetters.clear();
    this.journal.clear();
    this.exports.clear();
    this.rejections.clear();
    this.drafts.clear();
    this.decoded.clear();
    this.runtimes.clear();
    const s = await this.loadStored();
    // 14 [CLI-63]: a new client_id at every engine start and every flusher handover, kept in memory only
    this.clientId = this.mintClientId();
    // 14 [CLI-2]: the persisted replicas; one rehydration path (14 [CLI-8]): each is asked whether it can resume, by a load from its cursor
    for (const img of s.replicas) {
      const f = await this.restoreImage(img);
      if (f === undefined) {
        await this.tx([{ op: "deleteReplica", keyspaceId: img.keyspaceId }]).catch(() => undefined);
        continue;
      }
      f.image.baseSeq = img.base.checkpointSeq;
      f.needsLoad = { cause: "RESUME_TOO_OLD", resume: true };
      this.follows.set(f.id, f);
    }
    // 14 [CLI-70]: the keyspaces that the app asked for, with their interest
    for (const [id, v] of this.followedFromMeta(s.meta["followed"])) this.applyFollowedInterest(this.addFollow(id), v.interest);
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
    await this.persistFollowed();
    if (this.startRequested) this.beginRunning();
    this.queueState();
  }

  /** A followed keyspace that a restored image holds with another interest than the app asked for last: widened or narrowed on the way. */
  private applyFollowedInterest(f: Follow, interest: readonly string[]): void {
    if (interest.length === f.interest.length && interest.every((p, i) => p === f.interest[i])) return;
    if (!f.loaded) {
      f.interest = [...interest];
      return;
    }
    const added = interest.length === 0 ? (f.interest.length === 0 ? [] : [""]) : interest.filter((p) => !covers(f.interest, p));
    if (added.length > 0) f.widening = { to: [...interest], added: interest.length === 0 ? [] : added };
    else {
      f.interest = [...interest];
      this.pruneAllOutside(f);
    }
  }

  private addFollow(id: string, interest: readonly string[] = []): Follow {
    let f = this.follows.get(id);
    if (f === undefined) {
      f = new Follow(id, this.random);
      f.interest = [...interest];
      f.needsLoad = { cause: "COLD_LOAD", resume: false };
      this.follows.set(id, f);
    }
    return f;
  }

  protected commands(): Record<string, (args: unknown[]) => Promise<unknown>> {
    return {
      submit: (a) => this.submitAction(a[0] as ActionInput, a[1] as SubmitOptions | undefined),
      cancel: (a) => super.cancel(a[0] as string),
      ackDeadLetter: (a) => super.ackDeadLetter(a[0] as string),
      ackExport: (a) => super.ackExport(a[0] as string),
      ackRejection: (a) => super.ackRejection(a[0] as string),
      addKeyspace: (a) => this.addKeyspaceHere(a[0] as string, a[1] as { interest?: string[] } | undefined),
      setInterest: (a) => super.setInterest(a[0] as string, a[1] as string[]),
      createKeyspace: (a) => this.createKeyspaceHere(a[0] as string, a[1] as string),
    };
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Lifecycle

  /** Starts the session: loads the followed keyspaces and subscribes (14 [CLI-8]). A follower starts it when it becomes the flusher. */
  start(): void {
    this.startRequested = true;
    if (this.role === "flusher") this.beginRunning();
  }

  /**
   * Stops the session and every timer, persists what a restart reads, and gives up the flusher lock to the next tab in line (14 [CLI-61]).
   * The outbox stays in the storage.
   */
  async stop(): Promise<void> {
    if (this.role === "stopped") return;
    if (this.role === "flusher") {
      await this.run(async () => {
        for (const f of this.follows.values()) {
          if (f.onSession) f.lastSyncMs = this.serverNow();
          await this.persistHead(f, 0, true);
        }
      }).catch(() => undefined);
    }
    this.stopped = true;
    this.endRunning();
    this.cancelTimers();
    for (const f of this.follows.values()) f.crdt.destroy();
    await this.settle().catch(() => undefined);
    await this.leave("STOPPED");
  }

  /**
   * Follows a keyspace; it is loaded when the engine is online (14 [CLI-2]). `interest` names the prefixes of the keyspace that it holds
   * (14 [CLI-71]); none is the whole keyspace. Asking for a keyspace that the engine stopped following (a revocation, a deletion, a drop)
   * follows it again, with a cold load (14 [CLI-70]).
   */
  addKeyspace(id: string, opts: { interest?: string[] } = {}): Promise<void> {
    return this.viaFlusher("addKeyspace", [id, opts], () => this.addKeyspaceHere(id, opts));
  }

  private async addKeyspaceHere(id: string, opts: { interest?: string[] } | undefined): Promise<void> {
    const interest = [...new Set(opts?.interest ?? [])];
    await this.run(async () => {
      const bad = checkInterest(interest, this.limits.maxInterestPrefixes);
      if (bad !== undefined) throw new LocalRefusal("INVALID", bad);
      const have = this.follows.get(id);
      if (have === undefined) {
        this.addFollow(id, interest);
      } else if (have.stopped !== undefined || have.purged) {
        // the app asks for the keyspace again: nothing of the old copy is shown, and the first load is a cold one
        have.stopped = undefined;
        have.revoked = false;
        have.frozen = undefined;
        have.interest = [...interest];
        have.widening = undefined;
        if (have.purged || !have.loaded) {
          have.crdt.destroy();
          have.purged = false;
          have.loaded = false;
          have.needsLoad = { cause: "COLD_LOAD", resume: false };
        } else have.needsLoad = { cause: "RESUME_TOO_OLD", resume: true };
        have.invalidate();
      } else if (opts?.interest !== undefined) {
        await this.setInterestNow(id, interest);
        return;
      } else return;
      await this.persistFollowed();
    });
    this.kick(this.follows.get(id));
  }

  /**
   * Creates a keyspace (07 §7.11) and follows it. TODO(07 [MUT-101], 14 [CLI-45]): the wait of the hold `KEYSPACE_NOT_FOUND`
   * that counts from the end of a pending `CreateKeyspace` is counted from the first `KEYSPACE_NOT_FOUND` here.
   */
  createKeyspace(id: string, kind: string): Promise<{ created: boolean; logEpoch: number }> {
    return this.viaFlusher("createKeyspace", [id, kind], () => this.createKeyspaceHere(id, kind));
  }

  private async createKeyspaceHere(id: string, kind: string): Promise<{ created: boolean; logEpoch: number }> {
    const r = await this.transport.createKeyspace(create(CreateKeyspaceRequestSchema, { keyspaceId: id, kind }));
    await this.addKeyspaceHere(id, undefined);
    return { created: r.created, logEpoch: r.logEpoch };
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Authoring and reading

  /** Submits an action (14 [CLI-21]): resolves when it is in the durable outbox, or rejects with a `LocalRefusal`. A follower tab's action is the flusher's to author and store (tab-layer.ts). */
  submit(input: ActionInput, options?: SubmitOptions): Promise<SubmitResult> {
    return this.viaFlusher("submit", [input, options], () => this.submitAction(input, options));
  }

  private followed(keyspaceId: string): Follow {
    const f = this.follows.get(keyspaceId);
    // 14 [CLI-66], [CLI-69]: a keyspace whose copy was purged is shown nothing of, and nothing is authored into it
    if (f === undefined || f.purged) throw new LocalRefusal("NOT_FOLLOWED", `the keyspace ${keyspaceId} is not followed`);
    return f;
  }

  /** The view of a keyspace: confirmed state with the prediction of the pending actions (14 [CLI-6]). */
  view(keyspaceId: string): KeyspaceView {
    return this.viewOf(this.followed(keyspaceId));
  }

  /** The confirmed state alone. */
  confirmed(keyspaceId: string): KeyspaceView {
    return this.confirmedViewOf(this.followed(keyspaceId));
  }

  /** Whether a keyspace's confirmed state is loaded, and its cursor. */
  cursor(keyspaceId: string): { loaded: boolean; seq: bigint; logEpoch: number } | undefined {
    const f = this.follows.get(keyspaceId);
    return f === undefined || f.purged ? undefined : { loaded: f.loaded, seq: f.state.seq, logEpoch: f.state.logEpoch };
  }

  /** The interest prefixes of a followed keyspace; empty is the whole keyspace (14 [CLI-71]). */
  interest(keyspaceId: string): string[] | undefined {
    const f = this.follows.get(keyspaceId);
    return f === undefined ? undefined : [...f.interest];
  }

  /** Opens a CRDT key for the user: its view document (14 [CLI-13]). Only the flusher holds the documents of a device's engine. */
  crdt(keyspaceId: string, key: string): CrdtHandle {
    if (this.role === "follower") throw new LocalRefusal("NOT_FLUSHER", "the view documents of CRDT keys are the flusher tab's");
    const f = this.follows.get(keyspaceId);
    if (f === undefined || !f.loaded || f.purged) throw new LocalRefusal("NOT_FOLLOWED", `the keyspace ${keyspaceId} is not loaded`);
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
    const interest: SyncStatus["interest"] = {};
    const stopped: Record<string, string> = {};
    const anomalies: Record<string, number> = {};
    let loadsWithSkipped = 0;
    for (const f of this.follows.values()) {
      if (!f.purged) cursors[f.id] = { seq: f.state.seq, logEpoch: f.state.logEpoch };
      interest[f.id] = [...f.interest];
      if (f.stopped !== undefined) stopped[f.id] = f.stopped;
      if (f.anomalies > 0) anomalies[f.id] = f.anomalies;
      loadsWithSkipped += f.loadsWithSkipped;
    }
    // a follower reports the flusher's runtime (tab-layer.ts)
    const rt = this.role === "follower" ? this.runtimeMirror : undefined;
    return {
      pending: this.entries.size,
      deadLetters: this.deadLetters.size,
      exports: this.exports.size,
      skippedChanges: rt?.skippedChanges ?? this.skippedChanges,
      loadsWithSkipped: rt?.loadsWithSkipped ?? loadsWithSkipped,
      anomalies: rt?.anomalies ?? anomalies,
      unacknowledgedRejections: this.rejections.size,
      cursors,
      interest,
      stopped: rt?.stopped ?? stopped,
      role: this.role,
      lockedEntries: this.lockedEntries.length,
      storageMode: this.storage.mode,
      fullStores: rt?.fullStores ?? this.storage.full(),
      connection: this.role === "follower" ? (rt?.connection ?? "offline") : this.connection(),
      suspended: rt?.suspended ?? this.holds.all().some((h) => h.kind === "APP_SUSPENDED"),
      holds: rt?.holds ?? this.holds.info(),
    };
  }

  /**
   * The `decision_deadline` of the intent row of a pending GATED action, in ms since the epoch of server time (14 [CLI-58]: the app SHOULD show it; the
   * server keeps deciding registered intents while the device is offline, and the client learns the outcome on reconnect). The intent row is read from
   * the confirmed state (06 §6.5), so the keyspace's interest has to include `abpc.intent` for it to be there; undefined when the engine holds no row.
   */
  intentDeadline(mutationId: string): number | undefined {
    const e = this.entries.get(mutationId);
    if (e === undefined || e.lane !== "GATED") return undefined;
    const row = this.follows.get(e.keyspaceId)?.state.key(`abpc.intent/${mutationId}`);
    if (row === undefined || row.state !== StateValue) return undefined;
    try {
      const d = getMember(parseJson(row.value), "deadline");
      const ms = typeof d === "string" ? Date.parse(d) : Number.NaN;
      return Number.isNaN(ms) ? undefined : ms;
    } catch {
      return undefined;
    }
  }

  /** The outbox, in outbox order. */
  outbox(): OutboxEntry[] {
    return [...this.entries.values()].sort((a, b) => a.ordinal - b.ordinal);
  }

  /** The entries of the partition whose author is not the current principal: kept in place and never sent (14 [CLI-59], [CLI-60]). */
  locked(): OutboxEntry[] {
    return [...this.lockedEntries];
  }

  /** The partitions of this origin and app that exist on the device, the engine's own and locked ones included (14 [CLI-59], [CLI-65]). */
  async partitions(): Promise<PartitionKey[]> {
    return this.provider === undefined ? [this.partition] : this.provider.list(this.partition.origin, this.partition.appId);
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

  // -- the acknowledgements of the app, and the other calls that a follower carries to the flusher --

  cancel(mutationId: string): Promise<boolean> {
    return this.viaFlusher("cancel", [mutationId], () => super.cancel(mutationId));
  }

  ackDeadLetter(mutationId: string): Promise<void> {
    return this.viaFlusher("ackDeadLetter", [mutationId], () => super.ackDeadLetter(mutationId));
  }

  ackExport(mutationId: string): Promise<void> {
    return this.viaFlusher("ackExport", [mutationId], () => super.ackExport(mutationId));
  }

  ackRejection(mutationId: string): Promise<void> {
    return this.viaFlusher("ackRejection", [mutationId], () => super.ackRejection(mutationId));
  }

  setInterest(keyspaceId: string, prefixes: readonly string[]): Promise<void> {
    return this.viaFlusher("setInterest", [keyspaceId, [...prefixes]], () => super.setInterest(keyspaceId, prefixes));
  }

  /**
   * Resubmits a dead-letter entry as a new action, at the user's instruction (02 [ARC-50], 14 [CLI-28]): a fresh mutation id, the
   * values of 14 [CLI-17] and [CLI-18] taken from the confirmed state now. The entry is acknowledged.
   */
  async resubmitDeadLetter(mutationId: string): Promise<SubmitResult> {
    const d = this.deadLetters.get(mutationId);
    if (d === undefined) throw new LocalRefusal("REFUSED", `no dead-letter entry ${mutationId}`);
    const r = await this.resubmitContent(d);
    await this.ackDeadLetter(mutationId);
    return r;
  }

  /**
   * Resubmits an export-hook entry as a new action, at the user's instruction (02 [ARC-50]): the entry of an action that a quarantine
   * handed to the app (14 [CLI-66]) can be re-submitted once the engine follows the keyspace again, by a principal that may write there.
   * The entry is acknowledged.
   */
  async resubmitExport(mutationId: string): Promise<SubmitResult> {
    const d = this.exports.get(mutationId);
    if (d === undefined) throw new LocalRefusal("REFUSED", `no export entry ${mutationId}`);
    const r = await this.resubmitContent(d);
    await this.ackExport(mutationId);
    return r;
  }

  private async resubmitContent(d: { content: Uint8Array; keyspaceId: string; opType: string }): Promise<SubmitResult> {
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
    return this.submit({ keyspaceId: d.keyspaceId, opType: d.opType, changes, preconditions, blobRefs: [...m.blobRefs] });
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Sign-out (14 [CLI-65])

  /**
   * Waits until the outbox is empty, or `timeoutMs` has passed on the engine's clock; resolves with the number of entries left. The
   * flush that 14 [CLI-65] has a sign-out make while online.
   */
  async flush(timeoutMs = 10_000): Promise<number> {
    const deadline = this.clock.now() + timeoutMs;
    for (;;) {
      this.schedulePump();
      if (this.entries.size === 0) return 0;
      if (this.clock.now() >= deadline) return this.entries.size;
      await new Promise<void>((resolve) => this.clock.setTimer(100, resolve));
    }
  }

  /**
   * Signs the principal out (14 [CLI-65]): the engine should flush and then purge the principal's partition (personal mode) or lock it
   * (shared-device mode). It never purges unsynced entries silently: when entries are left, it returns `done: false` and the options,
   * and the app calls again with the user's choice: `keep-locked` leaves the partition in place, locked until its author is the current
   * principal again, or `discard` hands each entry to the app's export hook, with `exported`, and then purges. A discard needs an export
   * hook: without one it refuses (`REFUSED`), since it would be silent. The engine is stopped afterwards.
   */
  async signOut(opts: SignOutOptions = {}): Promise<SignOutResult> {
    if (this.role !== "flusher") throw new LocalRefusal("NOT_FLUSHER", "only the flusher tab signs the principal out");
    const unsynced = this.entries.size;
    if (unsynced > 0 && opts.unsynced === undefined) return { done: false, unsynced, options: ["keep-locked", "discard"] };
    const shared = this.partition.partition === this.author.deviceId && this.partition.partition !== `${this.author.issuer}|${this.author.sub}`;
    if (unsynced > 0 && opts.unsynced === "keep-locked") {
      await this.stop();
      return { done: true, outcome: "locked", unsynced };
    }
    if (unsynced > 0) {
      const hook = this.exportHook;
      if (hook === undefined) throw new LocalRefusal("REFUSED", "a discard of unsynced entries is reported to the app's export hook, and the engine has none");
      for (const e of this.outbox()) {
        await hook({ mutationId: e.mutationId, keyspaceId: e.keyspaceId, opType: e.opType, content: e.content, reason: "SIGN_OUT", reject: undefined, lastResult: this.lastResultText(e), dependencyOf: undefined });
        this.emit({ type: "exported", mutationId: e.mutationId, keyspaceId: e.keyspaceId, reason: "SIGN_OUT" });
      }
    }
    if (shared) {
      // the partition of a device is locked, not purged: the next person to use it is another principal
      if (unsynced > 0) await this.run(() => this.tx([...this.outbox().map((e) => ({ op: "deleteEntry" as const, mutationId: e.mutationId }))]));
      await this.stop();
      return { done: true, outcome: unsynced > 0 ? "discarded" : "locked", unsynced };
    }
    await this.run(() => this.tx([{ op: "purgeAll" }]));
    this.entries.clear();
    await this.stop();
    await this.provider?.destroy(this.partition);
    return { done: true, outcome: unsynced > 0 ? "discarded" : "purged", unsynced };
  }
}
