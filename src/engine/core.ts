// The engine's shared state and helpers: the serialized task queue, timers, the server-time estimate, the storage transaction
// wrapper, the holds, and the view of a keyspace. The behaviour is in the layers above (endings, replica, sender, session),
// each a class that extends this one; what a layer needs from one above it is declared here as an abstract method.
//
// Concurrency. Every change of engine state runs as a task of one queue, one at a time, in order (`run`): a frame, an RPC result,
// a timer, an API call. A task never waits for another task of the queue. A call on the network is started from a task but is
// awaited outside the queue (`detach`), and its result comes back as a task: so a slow call never keeps a frame waiting, and a
// hook of the app that is awaited outside the queue may call the API.

import { create } from "@bufbuild/protobuf";
import { type AppLimits, type KeyChange, KeyChangeSchema } from "../gen/abpc/v2/types_pb.js";
import type { Mutation } from "../gen/abpc/v2/sync_pb.js";
import { VersionTopology, DEFAULT_FEATURES } from "../rules/fold/index.js";
import { pushBounded } from "./bounded.js";
import { systemClock, systemRandom, type Clock, type Random } from "./clock.js";
import { CanonError, decodeContent } from "./entry.js";
import { StorageError } from "./errors.js";
import { EventBus } from "./events.js";
import type { Follow } from "./follow.js";
import { HoldRegistry, type Hold, type HoldKind, holdKey, type HoldScope } from "./holds.js";
import type { LaneName, Manifests, OpTypeInfo } from "./manifests.js";
import { MemoryStorage } from "./memory-storage.js";
import { buildOverlay, KeyspaceView, type PendingAction } from "./overlay.js";
import type {
  Author,
  DeadLetterEntry,
  DraftEntry,
  ExportEntry,
  JournalEntry,
  OutboxEntry,
  OutboxStorage,
  RejectionRecord,
  StorageOp,
  StoredState,
} from "./storage.js";
import type { Transport } from "./transport.js";
import type {
  ActionSnapshot,
  ChangeInput,
  ConflictHandler,
  EngineEvent,
  EngineEventListener,
  ExportHook,
  Predictor,
  TokenProvider,
} from "./types.js";
import { ChangeKind } from "../gen/abpc/v2/types_pb.js";

const INTERNAL_ERRORS_KEPT = 100;

export interface EngineConfig {
  appId: string;
  /** The principal's author tag (14 [CLI-59]). */
  principal: Author & { kind?: "USER" | "DEVICE" | "SERVICE" };
  transport: Transport;
  manifests: Manifests;
  /** The outbox storage. The default is in memory, and the sync status says so (14 [CLI-21]). */
  storage?: OutboxStorage;
  clock?: Clock;
  random?: Random;
  conflictHandler?: ConflictHandler;
  exportHook?: ExportHook;
  tokens?: TokenProvider;
  /** Predictors of the effects of GATED op types (14 [CLI-15]). */
  predictors?: Record<string, Predictor>;
  /** The feature ids that this client supports (15 §15.2). Default: those of the fold. */
  features?: readonly string[];
  /** Mints a client id; the default is a random UUID of version 4 (03 [ID-35]). */
  newClientId?: () => string;
}

/** The limits in force, as the engine uses them (04 Table 4.10a). */
export interface Limits {
  maxOfflineMs: number;
  maxMutationsPerRequest: number;
  maxRequestBytes: number;
  maxInflightGated: number;
  maxChangesPerAction: number;
  maxActionBytes: number;
  maxValueBytes: number;
  keyspaceNotFoundWaitMs: number;
}

/**
 * The floors of v2.0 (04 Table 4.10a), which the engine uses until a correct value arrives (07 [MUT-75]).
 *
 * 07 [MUT-75] "MUST use the field's floor in 04 §4.10 until a non-zero value for the field arrives". For
 * `max_offline_seconds` the floor is the lower end of its range, 3,600 s (04 §4.10 after Table 4.10b: "the lower end of its
 * range is its floor", with [MAN-73]'s range 3,600 s to 31,536,000 s). Ruled by the spec coordinator on 2026-10-31
 * (SPEC-QUESTIONS Q32); a 0 is also reported as a protocol error.
 */
export const FLOOR_LIMITS: Limits = {
  maxOfflineMs: 3600 * 1000,
  maxMutationsPerRequest: 50,
  maxRequestBytes: 1 << 20,
  maxInflightGated: 8,
  maxChangesPerAction: 100,
  maxActionBytes: 256 << 10,
  maxValueBytes: 64 << 10,
  keyspaceNotFoundWaitMs: 60 * 1000,
};

/** What the engine keeps of an outbox entry in memory only: its runtime state, rebuilt from the answers of the server after a restart. */
export interface Runtime {
  /** A request that carries the entry is under way. */
  inflight: boolean;
  /** The device clock time before which the entry is not sent again (a `Retry`, the client backoff); 0 is due now. */
  nextAttemptAt: number;
  /** Cancels the timer that wakes the entry. */
  timer: (() => void) | undefined;
  /** The entry was answered with a pending-intent `Retry`: the server drives it, and the client polls `GetMutationResult` (14 [CLI-44]). */
  polling: boolean;
  /** The entry is sent alone in its request (14 [CLI-50], a request split). */
  alone: boolean;
  /** How often a request that carried it got `Unauthenticated` (14 [CLI-50]). */
  authTries: number;
}

export abstract class EngineCore {
  readonly appId: string;
  readonly author: Author;
  readonly transport: Transport;
  readonly manifests: Manifests;
  readonly storage: OutboxStorage;
  readonly clock: Clock;
  readonly random: Random;
  readonly bus = new EventBus();
  readonly features: readonly string[];
  conflictHandler: ConflictHandler | undefined;
  exportHook: ExportHook | undefined;
  tokens: TokenProvider | undefined;
  readonly predictors: Record<string, Predictor>;
  protected readonly mintClientId: () => string;

  // -- the outbox and the other stores, mirrored in memory (the storage holds the durable copy) --
  readonly entries = new Map<string, OutboxEntry>();
  readonly deadLetters = new Map<string, DeadLetterEntry>();
  readonly journal = new Map<string, JournalEntry>();
  readonly exports = new Map<string, ExportEntry>();
  readonly rejections = new Map<string, RejectionRecord>();
  readonly drafts = new Map<string, DraftEntry>();
  protected readonly decoded = new Map<string, Mutation>();
  protected readonly runtimes = new Map<string, Runtime>();
  protected nextOrdinal = 1;

  // -- followed keyspaces --
  readonly follows = new Map<string, Follow>();

  // -- identity --
  clientId = "";

  // -- limits and time --
  limits: Limits = { ...FLOOR_LIMITS };
  private limitsReported = new Set<string>();
  private serverOffsetMs = 0;
  private obsServerMs: number | undefined;
  private obsMono = 0;

  // -- holds --
  readonly holds = new HoldRegistry();

  // -- counters of the sync status (14 [CLI-41]) --
  skippedChanges = 0;

  // -- the queue --
  private tail: Promise<void> = Promise.resolve();
  protected queued = 0;
  protected busy = 0;
  /** Counts the tasks and calls that started or ended, so that `settle` sees a state that no longer changes. */
  protected activity = 0;
  /** The failures of a task that nothing awaited, the last `INTERNAL_ERRORS_KEPT` of them (a long-lived engine must not keep them all). */
  readonly internalErrors: unknown[] = [];
  /** How many failures there were in all. */
  internalErrorCount = 0;
  protected stopped = false;
  private readonly timers = new Set<() => void>();

  protected constructor(cfg: EngineConfig) {
    this.appId = cfg.appId;
    this.author = { issuer: cfg.principal.issuer, sub: cfg.principal.sub, deviceId: cfg.principal.deviceId };
    this.transport = cfg.transport;
    this.manifests = cfg.manifests;
    this.storage = cfg.storage ?? new MemoryStorage();
    this.clock = cfg.clock ?? systemClock;
    this.random = cfg.random ?? systemRandom;
    this.conflictHandler = cfg.conflictHandler;
    this.exportHook = cfg.exportHook;
    this.tokens = cfg.tokens;
    this.predictors = cfg.predictors ?? {};
    this.features = cfg.features ?? DEFAULT_FEATURES;
    this.mintClientId = cfg.newClientId ?? ((): string => this.newUuidFromRandom());
  }

  private newUuidFromRandom(): string {
    const b = this.random.bytes(16);
    b[6] = ((b[6] as number) & 0x0f) | 0x40;
    b[8] = ((b[8] as number) & 0x3f) | 0x80;
    const h = [...b].map((x) => x.toString(16).padStart(2, "0")).join("");
    return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // The queue

  /** Runs fn as the next task of the queue and resolves with its result. A task never calls `run` and waits for it. */
  protected run<T>(fn: () => T | Promise<T>): Promise<T> {
    this.queued++;
    this.activity++;
    const result = this.tail.then(() => {
      this.activity++;
      return fn();
    });
    this.tail = result.then(
      () => {
        this.queued--;
      },
      (e: unknown) => {
        this.queued--;
        this.noteInternalError(e);
      },
    );
    return result;
  }

  /** Records a failure that no caller sees; only the last ones are kept. */
  protected noteInternalError(e: unknown): void {
    this.internalErrorCount++;
    pushBounded(this.internalErrors, e, INTERNAL_ERRORS_KEPT);
  }

  /** Runs fn as a task, and records (never throws) a failure. For work that no caller waits for. */
  protected post(fn: () => void | Promise<void>): void {
    void this.run(fn).catch(() => undefined);
  }

  /** Follows a call or a long task that runs outside the queue, so that `settle` waits for it. */
  protected detach(p: Promise<unknown>): void {
    this.busy++;
    this.activity++;
    void p.then(
      () => {
        this.busy--;
        this.activity++;
      },
      (e: unknown) => {
        this.busy--;
        this.activity++;
        this.noteInternalError(e);
      },
    );
  }

  /**
   * Resolves when the engine has nothing left to do that does not wait for time or for the server: no task queued or running,
   * and no call under way that makes progress (a call that waits for a timer, such as a delayed response of the fake server, makes
   * none). A test advances the clock and settles again.
   */
  async settle(): Promise<void> {
    const tick = (): Promise<void> => new Promise<void>((r) => (typeof setImmediate === "function" ? setImmediate(r) : setTimeout(r, 0)));
    let still = 0;
    let last = this.activity;
    for (let i = 0; i < 100_000; i++) {
      await tick();
      if (this.busy === 0 && this.queued === 0) {
        await tick();
        if (this.busy === 0 && this.queued === 0) return;
      }
      // nothing has started or ended for a while: what is left waits for time or for the outside
      if (this.activity === last) {
        if (++still >= 10) return;
      } else {
        still = 0;
        last = this.activity;
      }
    }
    throw new Error("engine: settle did not reach a quiet state");
  }

  /** Runs fn after ms on the engine's clock, as a task; returns the cancel function. Cancelled when the engine stops. */
  protected after(ms: number, fn: () => void | Promise<void>): () => void {
    let cancelled = false;
    let cancelClock: () => void = () => undefined;
    const cancel = (): void => {
      cancelled = true;
      cancelClock();
      this.timers.delete(cancel);
    };
    cancelClock = this.clock.setTimer(Math.max(0, Math.floor(ms)), () => {
      this.timers.delete(cancel);
      if (cancelled || this.stopped) return;
      this.post(fn);
    });
    this.timers.add(cancel);
    return cancel;
  }

  protected cancelTimers(): void {
    for (const c of [...this.timers]) c();
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Events

  protected emit(e: EngineEvent): void {
    this.bus.emit(e);
  }

  on(listener: EngineEventListener): () => void {
    return this.bus.on(listener);
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Server time (14 [CLI-55])

  /**
   * The estimate of the server's time, in ms since the epoch, from server data and the monotonic clock: the latest `server_time`
   * seen, advanced by the monotonic clock. Until the new run has seen one, the wall clock plus the persisted offset.
   */
  serverNow(): number {
    if (this.obsServerMs !== undefined) return this.obsServerMs + (this.clock.mono() - this.obsMono);
    return this.clock.now() + this.serverOffsetMs;
  }

  /**
   * Notes a `server_time` from a heartbeat or a record. 14 [CLI-55] has the estimate come from "the latest `server_time` seen": the
   * time of a record that is delivered late (a held frame, a replay after a resume) is older than the estimate and says nothing new,
   * since the server's time never goes back, so it moves the estimate forward or not at all. Taking it as now would pull every bound
   * that the estimate measures (holds, the journal window, warnings) back by the delay of the record.
   */
  observeServerTime(ms: number): void {
    if (this.obsServerMs !== undefined && ms <= this.serverNow()) return;
    this.obsServerMs = ms;
    this.obsMono = this.clock.mono();
    const offset = ms - this.clock.now();
    // 14 [CLI-55] has the client persist its latest offset: it is written when it moved by more than a second, not at every
    // heartbeat, because the bounds that read it (holds, the journal window, warnings) are counted in minutes to days
    if (Math.abs(offset - this.serverOffsetMs) > 1000) {
      this.serverOffsetMs = offset;
      this.post(() => this.tx([{ op: "setMeta", key: "serverOffsetMs", value: offset }]).catch(() => undefined));
    }
  }

  protected restoreServerOffset(v: number): void {
    this.serverOffsetMs = v;
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Limits (14 [CLI-54]; 07 [MUT-75])

  /** Takes the limits of `LoadStateResponse.limits` and `Subscribed.limits`; 0 in a field that v2.0 defines is a protocol error, and the floor stays. */
  protected applyLimits(l: AppLimits | undefined): void {
    if (l === undefined) return;
    const next: Limits = { ...this.limits };
    const take = (field: string, v: number | bigint, floor: number, set: (n: number) => void): void => {
      const n = Number(v);
      if (n === 0) {
        if (!this.limitsReported.has(field)) {
          this.limitsReported.add(field);
          this.emit({ type: "protocol-error", what: `AppLimits.${field}`, detail: "0 in an AppLimits field that the server must set (07 [MUT-75])", keyspaces: [] });
        }
        set(floor);
      } else {
        this.limitsReported.delete(field);
        set(n);
      }
    };
    take("max_offline_seconds", l.maxOfflineSeconds, FLOOR_LIMITS.maxOfflineMs / 1000, (n) => (next.maxOfflineMs = n * 1000));
    take("max_changes_per_action", l.maxChangesPerAction, FLOOR_LIMITS.maxChangesPerAction, (n) => (next.maxChangesPerAction = n));
    take("max_action_bytes", l.maxActionBytes, FLOOR_LIMITS.maxActionBytes, (n) => (next.maxActionBytes = n));
    take("max_value_bytes", l.maxValueBytes, FLOOR_LIMITS.maxValueBytes, (n) => (next.maxValueBytes = n));
    take("max_mutations_per_request", l.maxMutationsPerRequest, FLOOR_LIMITS.maxMutationsPerRequest, (n) => (next.maxMutationsPerRequest = n));
    take("max_request_bytes", l.maxRequestBytes, FLOOR_LIMITS.maxRequestBytes, (n) => (next.maxRequestBytes = n));
    take("max_inflight_gated_per_client", l.maxInflightGatedPerClient, FLOOR_LIMITS.maxInflightGated, (n) => (next.maxInflightGated = n));
    take("keyspace_not_found_wait_seconds", l.keyspaceNotFoundWaitSeconds, FLOOR_LIMITS.keyspaceNotFoundWaitMs / 1000, (n) => (next.keyspaceNotFoundWaitMs = n * 1000));
    const changed = next.maxOfflineMs !== this.limits.maxOfflineMs;
    this.limits = next;
    if (changed) this.post(() => this.tx([{ op: "setMeta", key: "maxOfflineSeconds", value: next.maxOfflineMs / 1000 }]).catch(() => undefined));
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Storage

  /** Commits the operations as one local transaction (14 [CLI-21]). Rejects, with nothing applied, when the storage cannot. */
  protected async tx(ops: readonly StorageOp[]): Promise<void> {
    if (ops.length === 0) return;
    try {
      await this.storage.commit(ops);
    } catch (e) {
      if (e instanceof StorageError || (e instanceof Error && e.name === "StoreFull")) throw e;
      throw new StorageError(e instanceof Error ? e.message : String(e));
    }
  }

  protected putOp(e: OutboxEntry): StorageOp {
    return { op: "putEntry", entry: e };
  }

  /** Loads what the storage holds into the mirrors (14 [CLI-8]). */
  protected async loadStored(): Promise<StoredState> {
    const s = await this.storage.load();
    for (const e of s.entries) this.entries.set(e.mutationId, e);
    for (const e of s.deadLetters) this.deadLetters.set(e.mutationId, e);
    for (const e of s.journal) this.journal.set(e.mutationId, e);
    for (const e of s.exports) this.exports.set(e.mutationId, e);
    for (const e of s.rejections) this.rejections.set(e.mutationId, e);
    for (const e of s.drafts) this.drafts.set(`${e.keyspaceId}\u0000${e.key}`, e);
    const ord = s.meta["nextOrdinal"];
    this.nextOrdinal = typeof ord === "number" ? ord : 1;
    for (const e of s.entries) if (e.ordinal >= this.nextOrdinal) this.nextOrdinal = e.ordinal + 1;
    const off = s.meta["serverOffsetMs"];
    if (typeof off === "number") this.restoreServerOffset(off);
    const mo = s.meta["maxOfflineSeconds"];
    if (typeof mo === "number" && mo > 0) this.limits = { ...this.limits, maxOfflineMs: mo * 1000 };
    return s;
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Entries

  /** The entry's decoded content, or undefined when it cannot be decoded (14 [CLI-29]). */
  protected content(e: OutboxEntry): Mutation | undefined {
    const c = this.decoded.get(e.mutationId);
    if (c !== undefined) return c;
    try {
      const m = decodeContent(e.content);
      if (m.mutationId !== e.mutationId) return undefined;
      this.decoded.set(e.mutationId, m);
      return m;
    } catch (err) {
      if (err instanceof CanonError) return undefined;
      throw err;
    }
  }

  protected snapshot(e: { mutationId: string; keyspaceId: string; opType: string; lane: LaneName; content: Uint8Array }): ActionSnapshot {
    let m: Mutation | undefined;
    try {
      m = decodeContent(e.content);
    } catch {
      m = undefined;
    }
    return {
      mutationId: e.mutationId,
      keyspaceId: e.keyspaceId,
      opType: e.opType,
      lane: e.lane,
      changes: m?.changes ?? [],
      preconditions: m?.preconditions ?? [],
      content: e.content,
    };
  }

  /** The runtime state of an entry. */
  protected rt(id: string): Runtime {
    let r = this.runtimes.get(id);
    if (r === undefined) {
      r = { inflight: false, nextAttemptAt: 0, timer: undefined, polling: false, alone: false, authTries: 0 };
      this.runtimes.set(id, r);
    }
    return r;
  }

  /** Forgets the runtime state of an entry that left the outbox. */
  protected dropRuntime(id: string): void {
    const r = this.runtimes.get(id);
    r?.timer?.();
    this.runtimes.delete(id);
    this.decoded.delete(id);
  }

  /** The outbox entries of a keyspace, in outbox order (14 [CLI-6]). */
  entriesOf(keyspaceId: string): OutboxEntry[] {
    const out: OutboxEntry[] = [];
    for (const e of this.entries.values()) if (e.keyspaceId === keyspaceId) out.push(e);
    return out.sort((a, b) => a.ordinal - b.ordinal);
  }

  /** The op type of an entry in the manifest version it was authored under. */
  protected opInfo(e: { opType: string; keyspaceId: string }, version?: number): OpTypeInfo | undefined {
    const f = this.follows.get(e.keyspaceId);
    const v = version ?? f?.state.manifestVersion ?? 0;
    return this.manifests.get(v)?.opType(e.opType);
  }

  protected opInfoFor(entry: OutboxEntry): OpTypeInfo | undefined {
    const m = this.content(entry);
    return this.opInfo(entry, m?.authoredManifestVersion);
  }

  /** The keyspace's view: confirmed state with the pending actions' prediction (14 [CLI-6]). */
  viewOf(f: Follow): KeyspaceView {
    if (f.cachedView !== undefined) return f.cachedView;
    const version = f.state.manifestVersion;
    const top = new VersionTopology(this.manifests, version);
    const manifest = this.manifests.get(version);
    const pending: PendingAction[] = [];
    for (const e of this.entriesOf(f.id)) {
      if (this.isExcludedFromOverlay(e)) continue;
      const m = this.content(e);
      if (m === undefined) continue;
      const info = this.opInfo(e, m.authoredManifestVersion);
      const gated = e.lane === "GATED";
      const p: PendingAction = { mutationId: e.mutationId, optimistic: info?.optimistic ?? true, gated, changes: m.changes };
      const predictor = gated ? this.predictors[e.opType] : undefined;
      if (predictor !== undefined) {
        try {
          p.effects = predictor(this.snapshot(e)).map((c) => this.toKeyChange(c));
        } catch {
          // a predictor that throws predicts nothing (14 [CLI-20])
        }
      }
      pending.push(p);
    }
    const o = buildOverlay(f.state, { top }, pending);
    f.cachedView = new KeyspaceView(f.id, f.state, o, top);
    return f.cachedView;
  }

  /** The confirmed state alone, as the conflict handler sees it (14 [CLI-47]). */
  confirmedViewOf(f: Follow): KeyspaceView {
    const top = new VersionTopology(this.manifests, f.state.manifestVersion);
    return new KeyspaceView(f.id, f.state, buildOverlay(f.state, { top }, []), top);
  }

  protected toKeyChange(c: ChangeInput): KeyChange {
    return create(KeyChangeSchema, {
      key: c.key,
      kind: c.kind,
      value: c.value ?? new Uint8Array(0),
      isNull: c.isNull ?? false,
      encoding: c.encoding ?? (c.kind === ChangeKind.SET ? "json/v1" : ""),
      delta: c.delta ?? 0n,
      update: c.update ?? new Uint8Array(0),
    });
  }

  /** Entries that no longer shape the view: an action that ended and awaits the app's handler, or is stuck in a full sink (14 [CLI-84]). */
  protected abstract isExcludedFromOverlay(e: OutboxEntry): boolean;

  // ---------------------------------------------------------------------------------------------------------------------
  // Holds (14 [CLI-45])

  /** Starts a hold and emits `held`; a hold that is already in force is left as it is. */
  protected startHold(h: Omit<Hold, "key" | "startedMs" | "attempts"> & { startedMs?: number; attempts?: number }): Hold {
    const key = holdKey(h.kind, h.keyspaceId, h.lane, h.mutationId);
    const have = this.holds.get(key);
    if (have !== undefined) return have;
    const hold: Hold = { ...h, key, startedMs: h.startedMs ?? this.serverNow(), attempts: h.attempts ?? 0 };
    this.holds.set(hold);
    const ev: EngineEvent = { type: "held", state: "start", scope: hold.scope, code: hold.code };
    if (hold.keyspaceId !== undefined) ev.keyspaceId = hold.keyspaceId;
    if (hold.lane !== undefined) ev.lane = hold.lane;
    if (hold.mutationId !== undefined) ev.mutationId = hold.mutationId;
    if (hold.boundMs !== undefined) ev.bound = hold.boundMs;
    this.emit(ev);
    this.onHoldChanged();
    return hold;
  }

  protected stopHold(kind: HoldKind, keyspaceId?: string, lane?: LaneName, mutationId?: string): boolean {
    const h = this.holds.delete(holdKey(kind, keyspaceId, lane, mutationId));
    if (h === undefined) return false;
    const ev: EngineEvent = { type: "held", state: "stop", scope: h.scope, code: h.code };
    if (h.keyspaceId !== undefined) ev.keyspaceId = h.keyspaceId;
    if (h.lane !== undefined) ev.lane = h.lane;
    if (h.mutationId !== undefined) ev.mutationId = h.mutationId;
    this.emit(ev);
    this.onHoldChanged();
    return true;
  }

  protected holdScopeOf(kind: HoldKind): HoldScope {
    switch (kind) {
      case "UNKNOWN_OP_TYPE":
      case "CLIENT_FORKED":
      case "CLIENT_UNKNOWN":
      case "UNSUPPORTED_FEATURE":
      case "UNKNOWN_HOLD":
        return "lane";
      case "KEYSPACE_NOT_FOUND":
      case "UPGRADE_REQUIRED":
      case "PROTOCOL_ERROR":
      case "HISTORY_DIVERGED":
      case "UNKNOWN_DROP_REASON":
      case "EPOCH_RESOLUTION":
      case "ACCESS_REVOKED":
        return "keyspace";
      case "CONFLICT_WAIT":
        return "action";
      default:
        return "app";
    }
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Upward calls: what a lower layer needs from a higher one

  /** Something changed that may let an action go: plan and send. */
  protected abstract schedulePump(): void;
  /** A hold started or stopped. */
  protected abstract onHoldChanged(): void;
}
