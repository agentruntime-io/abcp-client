// The client engine of ABPC v2 (14 §14.1 to §14.10): the engine model, prediction, the durable outbox, ordering and dependencies,
// events, the reactions by class and cause, the offline horizon, principals, tabs and devices, revocation and quarantine, and interest
// and storage, and (C6) presence (09 §9.8: `presence.ts`) and blobs (09 §9.9: `blobs.ts`, with `blob-store.ts`). The public API is this class; the
// behaviour is in the layers it extends (core, endings, replica-store, replica, reactions, sender, session, presence, blobs, horizon, interest,
// revocation, tab-layer).
//
// Out of scope here, each a TODO that names its rule: the local query layer (C4), the real network (C5), a redaction's purge of the
// journal and the dead-letter store (14 [CLI-76], R2).

import { create } from "@bufbuild/protobuf";
import type * as Y from "yjs";
import { CreateKeyspaceRequestSchema, UpdateSubscriptionRequestSchema } from "../gen/abpc/v2/sync_pb.js";
import { ChangeKind } from "../gen/abpc/v2/types_pb.js";
import { CrdtKey } from "./crdt-view.js";
import type { BlobCallOptions, BlobRef, StageOptions } from "./blob-store.js";
import { jsonBytes } from "./changes.js";
import { decodeContent, CanonError } from "./entry.js";
import { makeReject } from "./endings.js";
import type { EngineConfig } from "./core.js";
import { LocalRefusal } from "./errors.js";
import { newUuid } from "./ids.js";
import { Follow } from "./follow.js";
import type { JoinOptions } from "./presence.js";
import type { KeyspaceView } from "./overlay.js";
import { checkInterest, covers } from "./replica-store.js";
import { getMember, parse as parseJson } from "../rules/jsonv1/index.js";
import { StateValue } from "../rules/fold/index.js";
import { TabLayer } from "./tab-layer.js";
import type { DeadLetterEntry, DraftEntry, ExportEntry, JournalEntry, OutboxEntry, RejectionRecord, UnreadableRow } from "./storage.js";
import type { PartitionKey } from "./tabs.js";
import type { ActionInput, ChangeInput, EngineEvent, PreconditionInput, SubmitOptions, SubmitResult, SyncStatus } from "./types.js";
import { Code, ErrorClass } from "../gen/abpc/v2/types_pb.js";

/** What a reader of a keyspace's copy needs to know of it, in one call (the query layer, `query/`). */
export interface KeyspaceInfo {
  /** A load or a restored replica gave the copy its confirmed state; before that the copy is empty and says nothing of the keyspace. */
  loaded: boolean;
  /** The copy was purged (14 [CLI-66], [CLI-67], [CLI-77]): nothing of the keyspace is shown. */
  purged: boolean;
  /** Why the engine no longer follows the keyspace (a drop reason of A.6), when it does not; its copy stays unless it was purged. */
  stopped: string | undefined;
  seq: bigint;
  logEpoch: number;
  /** The manifest version of the confirmed state. */
  manifestVersion: number;
  /** The interest prefixes; empty is the whole keyspace (14 [CLI-71]). */
  interest: string[];
}

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

/** The presence registrations of this tab (09 [READ-73], [READ-74]): kept whatever the tab's role, to be told to a new flusher (`onFlusherChanged`). */
interface OwnPresence {
  keyspaceId: string;
  topic: string;
  opts: JoinOptions;
  state: Uint8Array | undefined;
}

export class Engine extends TabLayer {
  private readonly ownPresence = new Map<string, OwnPresence>();

  private constructor(cfg: EngineConfig) {
    super(cfg);
    // 14 [CLI-66], [CLI-77]: the purge list of a keyspace holds the blob cache and the presence state: they go with the rest
    this.registerPurgeHook(async (keyspaceId, cause) => {
      await this.blobsPurge(keyspaceId);
      await this.run(() => {
        this.presencePurge(keyspaceId, cause);
        for (const [k, p] of [...this.ownPresence]) if (p.keyspaceId === keyspaceId) this.ownPresence.delete(k);
      });
    });
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
    this.clientId = this.newClientId();
    // 14 [CLI-2]: the persisted replicas; one rehydration path (14 [CLI-8]): each is asked whether it can resume, by a load from its cursor
    // 14 [CLI-66], [CLI-7], [CLI-8]: a keyspace that a revocation stopped is not followed again with the old token: its copy is purged, and what the
    // revocation left (the quarantine, the actions that are asked about) is finished
    const revoked = this.revokedFromMeta(s.meta[this.metaKey("revoked")]);
    this.restoreSagas(s.meta[this.metaKey("sagas")]); // 14 [CLI-14]: the sagas that were followed, with what was said of them
    for (const img of s.replicas) {
      // 14 [CLI-59]: the cache of another principal of a shared device is not this one's: it goes
      if (!this.isMyImage(img)) {
        await this.tx([{ op: "deleteReplica", keyspaceId: img.keyspaceId }]).catch(() => undefined);
        continue;
      }
      if (revoked.has(img.keyspaceId)) continue; // the purge removes it (`finishRevocation`)
      const f = await this.restoreImage(img);
      if (f === undefined) {
        await this.tx([{ op: "deleteReplica", keyspaceId: img.keyspaceId }]).catch(() => undefined);
        continue;
      }
      f.image.baseSeq = img.base.checkpointSeq;
      f.needsLoad = { cause: "RESUME_TOO_OLD", resume: true };
      this.follows.set(f.id, f);
    }
    for (const [id, v] of revoked) this.follows.set(id, this.revokedFollow(id, v));
    // 14 [CLI-70]: the keyspaces that the app asked for, with their interest
    for (const [id, v] of this.followedFromMeta(s.meta[this.metaKey("followed")])) {
      if (revoked.has(id)) continue;
      const f = this.addFollow(id);
      this.applyFollowedInterest(f, v.interest);
      // 14 [CLI-56]: the time of the last sync outlives a restart that has no replica image
      if (v.lastSyncMs > f.lastSyncMs) f.lastSyncMs = v.lastSyncMs;
    }
    for (const f of this.follows.values()) if (f.loaded && !f.revoked) this.sagaScan(f); // 14 [CLI-14]: the pointers of the restored copies
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
    // what a revocation left before the restart, finished after this task
    for (const f of this.follows.values()) if (f.revoked) this.post(() => this.resumeRevocation(f));
    this.queueState();
  }

  /** The revoked keyspaces that the stored state names (14 [CLI-66]), with the cause and the cursor that the revocation left. */
  private revokedFromMeta(v: string | number | bigint | undefined): Map<string, { cause: string; done: boolean; seq: bigint; logEpoch: number; manifestVersion: number }> {
    const out = new Map<string, { cause: string; done: boolean; seq: bigint; logEpoch: number; manifestVersion: number }>();
    if (typeof v !== "string") return out;
    try {
      const o = JSON.parse(v) as Record<string, { cause?: string; done?: boolean; seq?: string; logEpoch?: number; manifestVersion?: number }>;
      for (const [id, x] of Object.entries(o)) {
        if (x.cause !== "ACCESS_REVOKED" && x.cause !== "SUBJECT_REVOKED") continue;
        out.set(id, { cause: x.cause, done: x.done === true, seq: BigInt(x.seq ?? "0"), logEpoch: x.logEpoch ?? 0, manifestVersion: x.manifestVersion ?? 0 });
      }
    } catch {
      // not what this engine wrote: no keyspace is revoked because of it
    }
    return out;
  }

  /** The copy of a keyspace that a revocation purged, as a restart has it: nothing is shown, the cursor's epoch is the one that the outcomes are judged against. */
  private revokedFollow(id: string, v: { cause: string; done: boolean; seq: bigint; logEpoch: number; manifestVersion: number }): Follow {
    const f = new Follow(id, this.random);
    f.state.setCursor(v.seq, v.logEpoch, v.manifestVersion);
    f.loaded = true;
    f.purged = true;
    f.revoked = true;
    f.stopped = v.cause;
    f.revocationPurged = v.done;
    f.revocationDone = v.done;
    return f;
  }

  /** What a revocation left at a restart (14 [CLI-66], [CLI-7], [CLI-8]): the outcomes of the epoch of the cursor retire, the purge and the quarantine are finished, the rest is asked about. */
  private async resumeRevocation(f: Follow): Promise<void> {
    if (!f.revoked || this.role !== "flusher") return;
    await this.checkRetirements(f);
    await this.finishRevocation(f);
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
      submit: (a) => this.submitAction(a[0] as ActionInput, a[1] as SubmitOptions | undefined, a[2] as string | undefined),
      cancel: (a) => super.cancel(a[0] as string),
      ackDeadLetter: (a) => super.ackDeadLetter(a[0] as string),
      ackExport: (a) => super.ackExport(a[0] as string),
      ackRejection: (a) => super.ackRejection(a[0] as string),
      addKeyspace: (a) => this.addKeyspaceHere(a[0] as string, a[1] as { interest?: string[] } | undefined),
      setInterest: (a) => super.setInterest(a[0] as string, a[1] as string[]),
      createKeyspace: (a) => this.createKeyspaceHere(a[0] as string, a[1] as string),
      removeKeyspace: (a) => this.removeKeyspaceHere(a[0] as string),
      presenceJoin: (a) => this.guarded(() => this.presenceJoin(a[0] as string, a[1] as string, a[2] as string, a[3] as JoinOptions | undefined)),
      presenceSet: (a) => this.guarded(() => this.presenceSet(a[0] as string, a[1] as string, a[2] as string, a[3] as Uint8Array)),
      presenceLeave: (a) => this.guarded(() => this.presenceLeave(a[0] as string, a[1] as string, a[2] as string)),
      blobStage: (a) => this.stageBlobNow(a[0] as string, a[1] as Uint8Array, a[2] as StageOptions | undefined).then((r) => this.afterStage(r)),
      blobUpload: (a) => this.uploadBlobNow(a[0] as string, a[1] as Uint8Array, a[2] as StageOptions | undefined),
      blobDelete: (a) => this.deleteBlobNow(a[0] as string, a[1] as string, a[2] as string),
      blobRelease: (a) => this.releaseBlobNow(a[0] as string, a[1] as string),
    };
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Lifecycle

  /** Starts the session: loads the followed keyspaces and subscribes (14 [CLI-8]). A follower starts it when it becomes the flusher. */
  start(): void {
    this.startRequested = true;
    if (this.role === "flusher") {
      this.beginRunning();
      for (const f of this.follows.values()) if (f.revoked && !f.revocationAsked) this.post(() => this.finishRevocation(f));
    }
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
        await this.persistFollowed(); // 14 [CLI-56]: the time of the last sync outlives a restart that has no replica image
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
        have.revocationPurged = false;
        have.revocationDone = false;
        have.revocationAsked = false;
        have.revocationAttempts = 0;
        have.purgeJournal = [];
        have.quarantinedIds = [];
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
   * Stops following a keyspace (14 [CLI-70]: the engine follows only the keyspaces that the app asks for): it leaves the live session, the local copy
   * and its persisted replica go, and the keyspace is not followed again at a restart or a handover. It is refused (`REFUSED`) while the outbox
   * holds an action of the keyspace, which the engine still has to send or to hear the outcome of; the app asks for the keyspace again with `addKeyspace`.
   * Resolves false for a keyspace that is not followed.
   */
  removeKeyspace(id: string): Promise<boolean> {
    return this.viaFlusher("removeKeyspace", [id], () => this.removeKeyspaceHere(id));
  }

  private async removeKeyspaceHere(id: string): Promise<boolean> {
    return this.run(async () => {
      const f = this.follows.get(id);
      if (f === undefined) return false;
      if (this.entriesOf(id).length > 0) throw new LocalRefusal("REFUSED", `the outbox holds actions of the keyspace ${id}: it is followed until they are ended`);
      if (f.revoked) f.revoked = false; // the app lets go of a keyspace that a revocation stopped: nothing is left of it to follow, or to finish
      f.crdt.destroy();
      this.follows.delete(id);
      this.presencePurge(id, "REMOVED"); // 09 [READ-63]: the session's memberships end with the removal; the engine follows the keyspace no more
      await this.blobsPurge(id);
      for (const [k, p] of [...this.ownPresence]) if (p.keyspaceId === id) this.ownPresence.delete(k);
      await this.tx([{ op: "deleteReplica", keyspaceId: id }, ...this.followedOps()]);
      this.touchPersisted();
      if (this.online() && f.onSession) {
        this.detach(
          (async () => {
            try {
              await this.transport.updateSubscription(create(UpdateSubscriptionRequestSchema, { sessionId: this.sessionId(), remove: [id] }));
            } catch (e) {
              this.noteRevoked(e);
              this.abortSession(); // the session resumes from the cursors that the client holds now (09 [READ-57])
            }
          })(),
        );
      }
      return true;
    });
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
    // a follower mints the id of the action: the flusher authors it under that id, once, whoever runs the call (tab-layer.ts)
    const id = this.role === "follower" ? newUuid(this.random) : undefined;
    return this.viaFlusher("submit", [input, options, id], () => this.submitAction(input, options, id));
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

  /** The state of a keyspace's local copy, or undefined for a keyspace that the engine does not follow. */
  keyspaceInfo(keyspaceId: string): KeyspaceInfo | undefined {
    const f = this.follows.get(keyspaceId);
    if (f === undefined) return undefined;
    return { loaded: f.loaded, purged: f.purged, stopped: f.stopped, seq: f.state.seq, logEpoch: f.state.logEpoch, manifestVersion: f.state.manifestVersion, interest: [...f.interest] };
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

  // ---------------------------------------------------------------------------------------------------------------------
  // Presence (09 §9.8): one entry per topic for all the tabs of the engine, published through its one session (14 [CLI-61]); `presence.ts`

  private tabKey(): string {
    return this.tabId === "" ? "main" : this.tabId;
  }

  /** Runs a change of the presence registrations as a task of the engine; a local refusal reaches the caller and is no internal error of the engine. */
  private async guarded(fn: () => void): Promise<void> {
    const refused = await this.run(() => {
      try {
        fn();
        return undefined;
      } catch (e) {
        return e;
      }
    });
    if (refused !== undefined) throw refused;
  }

  /**
   * This tab joins a presence topic of a followed keyspace (09 [READ-73]). The engine joins the topic on its session as soon as the session follows the keyspace, joins again after
   * every new session, and leaves it when the last tab of the engine has left (09 [READ-74]). `opts.awareness` marks a Yjs awareness topic (09 [READ-83]); by default the manifest says.
   * Resolves when the tab is registered; what the server answers comes as presence events (`onPresence`).
   */
  async joinPresence(keyspaceId: string, topic: string, opts: JoinOptions = {}): Promise<void> {
    const tab = this.tabKey();
    const key = `${keyspaceId}\u0000${topic}`;
    const had = this.ownPresence.get(key);
    this.ownPresence.set(key, { keyspaceId, topic, opts, state: had?.state });
    try {
      await this.viaFlusher("presenceJoin", [tab, keyspaceId, topic, opts], () => this.guarded(() => this.presenceJoin(tab, keyspaceId, topic, opts)));
    } catch (e) {
      if (had === undefined) this.ownPresence.delete(key);
      throw e;
    }
  }

  /**
   * Sets the state of this tab in a topic that it has joined (09 [READ-74]): any JSON value of json/v1 (14 [CLI-22]); it is published at once, or after the interval of the topic
   * (09 [READ-79]), and again at about half the TTL (09 [READ-76]) while the tab is in the topic. For a Yjs awareness topic the state of the entry maps the tab ids to their
   * states (09 [READ-83]). A state over `max_presence_state_bytes` is refused here (`LocalRefusal`, `INVALID`).
   */
  async publishPresence(keyspaceId: string, topic: string, state: unknown): Promise<void> {
    const tab = this.tabKey();
    const bytes = jsonBytes(state);
    await this.viaFlusher("presenceSet", [tab, keyspaceId, topic, bytes], () => this.guarded(() => this.presenceSet(tab, keyspaceId, topic, bytes)));
    const own = this.ownPresence.get(`${keyspaceId}\u0000${topic}`);
    if (own !== undefined) own.state = bytes;
  }

  /** This tab leaves a topic. The engine sends `LeavePresence` when no tab of it is in the topic any more (09 [READ-74], [READ-77]). */
  async leavePresence(keyspaceId: string, topic: string): Promise<void> {
    const tab = this.tabKey();
    this.ownPresence.delete(`${keyspaceId}\u0000${topic}`);
    await this.viaFlusher("presenceLeave", [tab, keyspaceId, topic], () => this.guarded(() => this.presenceLeave(tab, keyspaceId, topic)));
  }

  /** A new flusher: the memberships and the states of this tab are told to it again (09 [READ-73]; the session of the old flusher ended with it). */
  protected onFlusherChanged(): void {
    const tab = this.tabKey();
    for (const p of [...this.ownPresence.values()]) {
      const tell = (name: string, args: unknown[], local: () => void): void => {
        this.viaFlusher(name, args, () => this.guarded(local)).catch(() => undefined);
      };
      tell("presenceJoin", [tab, p.keyspaceId, p.topic, p.opts], () => this.presenceJoin(tab, p.keyspaceId, p.topic, p.opts));
      if (p.state !== undefined) {
        const state = p.state;
        tell("presenceSet", [tab, p.keyspaceId, p.topic, state], () => this.presenceSet(tab, p.keyspaceId, p.topic, state));
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Blobs (09 §9.9): `blobs.ts`. A call of a follower tab that touches the bytes that actions name goes to the flusher, which keeps them (14 [CLI-61]).

  /**
   * Keeps the bytes for the actions that will name the blob in `blob_refs`, and returns the blob's reference; nothing is sent now. The engine uploads the bytes in the background while it
   * is online, and again when an action that names the blob gets `BLOB_PENDING` or `BLOB_MISSING` (14 [CLI-44]); it keeps them until that action is confirmed (09 [READ-94]).
   */
  stageBlob(keyspaceId: string, bytes: Uint8Array, opts: StageOptions = {}): Promise<BlobRef> {
    const o: StageOptions = { ...opts, blobId: opts.blobId ?? this.mintBlobId() }; // a follower mints the id: the flusher stages once under it, whoever asks again
    return this.viaFlusher("blobStage", [keyspaceId, bytes, o], () => this.stageBlobNow(keyspaceId, bytes, o).then((r) => this.afterStage(r)));
  }

  private afterStage(ref: BlobRef): BlobRef {
    this.backgroundUpload(ref);
    return ref;
  }

  /**
   * Stages the bytes and uploads them: `CreateUpload`, the PUT to the signed URL with the headers that it gives, `CommitBlob` (09 [READ-86], [READ-88]); resolves with the blob's
   * reference once the blob is committed. The `signal` and `onWait` of a call that a follower tab makes do not reach the flusher.
   */
  uploadBlob(keyspaceId: string, bytes: Uint8Array, opts: StageOptions & BlobCallOptions = {}): Promise<BlobRef> {
    const { signal, onWait, ...stage } = opts;
    const o: StageOptions = { ...stage, blobId: stage.blobId ?? this.mintBlobId() };
    return this.viaFlusher("blobUpload", [keyspaceId, bytes, o], () => this.uploadBlobNow(keyspaceId, bytes, { ...o, ...(signal !== undefined ? { signal } : {}), ...(onWait !== undefined ? { onWait } : {}) }));
  }

  /** `DeleteBlob` (09 [READ-91]); the cached and staged bytes of the blob go with it. */
  deleteBlob(keyspaceId: string, blobId: string, reason: string, opts: BlobCallOptions = {}): Promise<void> {
    return this.viaFlusher("blobDelete", [keyspaceId, blobId, reason], () => this.deleteBlobNow(keyspaceId, blobId, reason, opts));
  }

  /** Lets go of the bytes of a staged blob that no action will name (the server's blob stays until the sweep of 09 [READ-92]; the engine never deletes one on its own, [READ-94]). */
  releaseBlob(keyspaceId: string, blobId: string): Promise<void> {
    return this.viaFlusher("blobRelease", [keyspaceId, blobId], () => this.releaseBlobNow(keyspaceId, blobId));
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
      unreadableRows: this.unreadable.length,
      cursors,
      interest,
      stopped: rt?.stopped ?? stopped,
      role: this.role,
      lockedEntries: this.lockedEntries.length,
      storageMode: this.storage.mode,
      fullStores: rt?.fullStores ?? this.storage.full(),
      connection: this.role === "follower" ? (rt?.connection ?? "offline") : this.connection(),
      subscribeRetry: this.role === "follower" ? rt?.subscribeRetry : this.subscribeRetry(),
      suspended: rt?.suspended ?? this.holds.all().some((h) => h.kind === "APP_SUSPENDED"),
      holds: rt?.holds ?? this.holds.info(),
      pendingCalls: this.callsWaiting(),
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

  /** The dead-letter store, in the authoring order of the actions (14 [CLI-28], [CLI-38]). */
  deadLetterEntries(): DeadLetterEntry[] {
    return this.authoringOrder(this.deadLetters.values());
  }

  journalEntries(): JournalEntry[] {
    return [...this.journal.values()];
  }

  /** The export-hook entries that the app has not acknowledged, in the authoring order of the actions (14 [CLI-83]). */
  exportEntries(): ExportEntry[] {
    return this.authoringOrder(this.exports.values());
  }

  /** The rows of the storage that the engine could not read and left out (V19 N5): a malformed row is reported, and the rest of the partition is the engine's. */
  unreadableRows(): UnreadableRow[] {
    return [...this.unreadable];
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
      if (unsynced > 0) {
        const discarded = this.outbox();
        await this.run(() => this.tx([...discarded.map((e) => ({ op: "deleteEntry" as const, mutationId: e.mutationId }))]));
        for (const e of discarded) await this.dropStagedOf(e).catch((err: unknown) => this.noteInternalError(err)); // the actions are gone: so are the bytes that they carried
      }
      await this.stop();
      return { done: true, outcome: unsynced > 0 ? "discarded" : "locked", unsynced };
    }
    // 14 [CLI-28], [CLI-83]: "Entries stay durable until the app acknowledges them": the purge takes the ones that it has not, so each is handed to the app once
    // more first (a dead-letter entry with `dead-lettered`, an export-hook entry with `exported` and the hook), as 14 [CLI-77] does for a deleted keyspace
    for (const d of this.deadLetterEntries()) this.emit({ type: "dead-lettered", mutationId: d.mutationId, keyspaceId: d.keyspaceId, reason: d.reason });
    for (const x of this.exportEntries()) {
      this.emit({ type: "exported", mutationId: x.mutationId, keyspaceId: x.keyspaceId, reason: x.reason });
      this.callExportHook(this.exportRecord(x));
    }
    await this.settle();
    await this.run(() => this.tx([{ op: "purgeAll" }]));
    this.entries.clear();
    await this.stop();
    // 14 [CLI-65]: what the partition holds goes, the bytes that its actions carried included. The provider deletes the stash of the partition with it where it keeps one (a browser's); an
    // app's own stash is erased here, and a failure of either is told after the other was tried
    let erased: unknown;
    try {
      await this.blobsPurgeAll();
    } catch (e) {
      erased = e;
    }
    await this.provider?.destroy(this.partition);
    if (erased !== undefined) throw erased;
    return { done: true, outcome: unsynced > 0 ? "discarded" : "purged", unsynced };
  }
}
