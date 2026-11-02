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
import { BoundedSet, pushBounded } from "./bounded.js";
import { DEFAULT_BLOB_CACHE_BYTES, DEFAULT_MAX_DOWNLOAD_BYTES, DEFAULT_MAX_STAGED_BYTES, DEFAULT_TRANSFER_TIMEOUT, fetchBlobHttp, MemoryBlobStash, type BlobHttp, type BlobStash } from "./blob-store.js";
import { systemClock, systemRandom, type Clock, type Random } from "./clock.js";
import { MAX_CHANGES_PER_ACTION, MAX_INTEREST_PREFIXES, MAX_PRESENCE_MIN_INTERVAL_MS, MAX_PRESENCE_STATE_BYTES } from "./counts.js";
import { CanonError, decodeContent } from "./entry.js";
import { StorageError } from "./errors.js";
import { EventBus } from "./events.js";
import type { Follow } from "./follow.js";
import { HoldRegistry, type Hold, type HoldKind, holdKey, type HoldScope } from "./holds.js";
import { newUuid } from "./ids.js";
import type { LaneName, Manifests, OpTypeInfo } from "./manifests.js";
import { MemoryStorage } from "./memory-storage.js";
import { buildOverlay, KeyspaceView, type PendingAction } from "./overlay.js";
import { FollowMap, ViewFeed, type ViewChangeListener } from "./view-feed.js";
import {
  draftKey,
  type Author,
  type DeadLetterEntry,
  type DraftEntry,
  type ExportEntry,
  type JournalEntry,
  type OutboxEntry,
  type OutboxStorage,
  type RejectionRecord,
  type StorageOp,
  type StoredLight,
  type StoredState,
  type UnreadableRow,
} from "./storage.js";
import type { Transport } from "./transport.js";
import type { StorageProvider, TabHost } from "./tabs.js";
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
/** How many of its own client ids an engine remembers: an engine mints one at each start, handover and fork, which are rare. */
const CLIENT_IDS_REMEMBERED = 1024;

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
  /** The server origin, which keys the local storage with the app and the partition (14 [CLI-74]); default `local`. */
  origin?: string;
  /**
   * `personal` (the default): the partition is the user's subject. `shared-device`: the partition is the `device_id`, and the engine of a
   * kiosk or a till authenticates as the device (14 [CLI-59], 10 [AUTH-28]).
   */
  mode?: "personal" | "shared-device";
  /** The tab's platform: the lock that elects the flusher, the broadcast between tabs, the storage (14 [CLI-61]). Without it the engine is its own flusher. */
  tab?: TabHost;
  /** Opens the storage of the partition when `storage` is not given (14 [CLI-74]); a tab's host has one. */
  store?: StorageProvider;
  /** The flusher hands its role over to a visible tab when its own tab is hidden (14 [CLI-61]); default false. */
  handOverWhenHidden?: boolean;
  /** The share of `max_offline` at which `offline-horizon` is emitted: above 0 and below 1; default 0.8 (14 [CLI-56]). */
  horizonWarning?: number;
  /** What the engine does with the local copy of a keyspace that it stops following because of `GRANT_CHANGED` or `ARCHIVED` (14 [CLI-67], [CLI-77]); default `keep`. */
  copyPolicy?: "keep" | "purge" | ((keyspaceId: string, reason: "GRANT_CHANGED" | "ARCHIVED") => "keep" | "purge");
  /** Whether the engine keeps the persisted replica (14 [CLI-2], §14.10); default true. */
  persistReplica?: boolean;
  /** The longest tail of records that a persisted replica may hold at a restart; a longer one is dropped and the keyspace is loaded again. */
  replicaTailLimit?: number;
  /**
   * The most octets of a `SubscribeRequest` that the engine builds, in the binary encoding that it calls with; default 16 MiB, the bound of 02 [ARC-59].
   * The cursors that do not fit are named in `UpdateSubscription` adds on the new session (09 [READ-57]). A test lowers it.
   */
  maxSubscribeBytes?: number;
  /** Blobs (09 §9.9): the HTTP of the blob store, where the bytes that actions name are kept, and the size of the cache of downloads. */
  blobs?: {
    /** Moves bytes to and from signed URLs; default the platform's `fetch` (`fetchBlobHttp`). */
    http?: BlobHttp;
    /**
     * Keeps the bytes of the blobs that actions name until the actions are confirmed (09 [READ-94]); default: the platform's stash for the partition (a browser's IndexedDB, a Node host's
     * directory), else memory. One stash is one partition's: a sign-out erases it whole, and `getBlob` serves what it holds, so two principals never share one.
     */
    stash?: BlobStash;
    /** The most octets of downloaded blobs that the engine keeps; default 32 MiB. */
    cacheBytes?: number;
    /**
     * How long one PUT or GET of a signed URL may take before the engine gives it up and counts it as a failed attempt (14 [CLI-44]), in ms on the engine's clock, or a function of the
     * octets that it moves. Default: 30 s and 32 KiB/s (`DEFAULT_TRANSFER_TIMEOUT`). A browser's `fetch` has no timeout of its own.
     */
    transferTimeoutMs?: number | ((octets: number) => number);
    /** The most octets that `getBlob` buffers (16 [SEC-1]); default 100 MiB. A call may lower it with `BlobCallOptions.maxBytes`. */
    maxDownloadBytes?: number;
    /** The most octets of staged bytes that the stash holds at once (09 [READ-94]); a blob that would pass it is refused locally, as for a full quota (14 [CLI-21]). Default 256 MiB. */
    maxStagedBytes?: number;
    /** Upload and download URLs may be `http:` for any host (a development deployment); by default only `https:`, and `http:` on the loopback (02 note after [ARC-56], 16 [SEC-45]). */
    allowInsecureUrls?: boolean;
  };
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
  /** Interest prefixes per followed keyspace (09 [READ-22], 04 Table 4.10). */
  maxInterestPrefixes: number;
  /** One presence state, in octets (09 [READ-74], 04 Table 4.10a): a larger one is refused by the engine before it is sent. */
  maxPresenceStateBytes: number;
  /** Members of one presence topic (09 [READ-79]); the server refuses a join over it with `QUOTA_EXCEEDED`. */
  maxPresenceMembersPerTopic: number;
  /** The presence TTL (09 [READ-76]): an entry expires this long after its last publish; the engine publishes again at about half of it. */
  presenceTtlMs: number;
  /** The shortest interval between two publishes of one member of a topic that the server applies (09 [READ-79]). */
  presenceMinPublishIntervalMs: number;
  /** One blob, in octets (09 [READ-86], 04 Table 4.10a). */
  maxBlobBytes: number;
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
  maxInterestPrefixes: 16,
  maxPresenceStateBytes: 4 << 10,
  maxPresenceMembersPerTopic: 100,
  presenceTtlMs: 10_000,
  // 04 Table 4.10a: "a 0 from a server that does not define the field means the default", 1,000 ms; the field has no other floor than its range's 100 ms
  presenceMinPublishIntervalMs: 1_000,
  // no floor and no maximum in 04 Table 4.10a: the default
  maxBlobBytes: 100 * 1024 * 1024,
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
  /**
   * The upload that comes before the first send of an action that names a staged blob (14 [CLI-44]): `unchecked` until the entry could be sent, `checking` while the engine finds
   * its staged blobs and uploads them, `ready` when the send may go (`blobs.ts`).
   */
  blobs: "unchecked" | "checking" | "ready";
  /** How many repairs of the action's blobs uploaded something (`BLOB_MISSING`, 09 [READ-89]): a second one that meets the same answer, with bytes lacking, holds the action. */
  blobRepairs: number;
  /** How often a request that carried it got `Unauthenticated` (14 [CLI-50]). */
  authTries: number;
}

export abstract class EngineCore {
  readonly appId: string;
  readonly author: Author;
  readonly transport: Transport;
  readonly manifests: Manifests;
  /** Where the bytes that actions name are kept (09 [READ-94]). */
  private blobStashIn: BlobStash;
  /** The most octets of downloaded blobs that the cache holds. */
  protected readonly blobCacheBytes: number;
  private blobHttpGiven: BlobHttp | undefined;
  /** How long a transfer of that many octets may take (see `EngineConfig.blobs.transferTimeoutMs`). */
  protected readonly blobTransferTimeout: (octets: number) => number;
  protected readonly blobMaxDownload: number;
  protected readonly blobMaxStaged: number;
  protected readonly blobAllowInsecure: boolean;
  storage!: OutboxStorage;
  readonly clock: Clock;
  readonly random: Random;
  readonly bus = new EventBus();
  readonly features: readonly string[];
  conflictHandler: ConflictHandler | undefined;
  exportHook: ExportHook | undefined;
  tokens: TokenProvider | undefined;
  readonly predictors: Record<string, Predictor>;
  private readonly mintClientId: () => string;
  /** The client ids that this engine has had (a bounded memory of them, in memory only): 10 [AUTH-19] "MUST NOT reuse a `client_id`". */
  private readonly clientIdsUsed = new BoundedSet<string>(CLIENT_IDS_REMEMBERED);

  // -- the outbox and the other stores, mirrored in memory (the storage holds the durable copy) --
  /** Entries of the partition whose author is not the current principal: kept in place, never sent, never shown (14 [CLI-59], [CLI-60]). */
  lockedEntries: OutboxEntry[] = [];
  readonly entries = new Map<string, OutboxEntry>();
  readonly deadLetters = new Map<string, DeadLetterEntry>();
  /** The rows of the storage that the last read left out because they were malformed (V19 N5). */
  protected unreadable: UnreadableRow[] = [];
  readonly journal = new Map<string, JournalEntry>();
  readonly exports = new Map<string, ExportEntry>();
  readonly rejections = new Map<string, RejectionRecord>();
  readonly drafts = new Map<string, DraftEntry>();
  protected readonly decoded = new Map<string, Mutation>();
  protected readonly runtimes = new Map<string, Runtime>();
  protected nextOrdinal = 1;

  // -- followed keyspaces --
  /** What changed in the view of each keyspace, reported at the end of each task, for the query layer (view-feed.ts). */
  protected readonly feed = new ViewFeed();
  readonly follows: Map<string, Follow> = new FollowMap(this.feed);

  // -- identity --
  clientId = "";
  /**
   * The engine's role among the tabs of its device (14 [CLI-61]): the flusher owns the outbox, the session and the `client_id`; a
   * follower reads through the flusher and never sends. A stopped engine is neither. An engine with no tab host is its own flusher.
   */
  role: "flusher" | "follower" | "stopped" = "flusher";

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
    this.blobStashIn = cfg.blobs?.stash ?? new MemoryBlobStash();
    this.blobCacheBytes = cfg.blobs?.cacheBytes ?? DEFAULT_BLOB_CACHE_BYTES;
    this.blobHttpGiven = cfg.blobs?.http;
    const tt = cfg.blobs?.transferTimeoutMs;
    this.blobTransferTimeout = typeof tt === "function" ? tt : typeof tt === "number" ? () => tt : DEFAULT_TRANSFER_TIMEOUT;
    this.blobMaxDownload = cfg.blobs?.maxDownloadBytes ?? DEFAULT_MAX_DOWNLOAD_BYTES;
    this.blobMaxStaged = cfg.blobs?.maxStagedBytes ?? DEFAULT_MAX_STAGED_BYTES;
    this.blobAllowInsecure = cfg.blobs?.allowInsecureUrls === true;
    this.storage = cfg.storage ?? new MemoryStorage();
    this.clock = cfg.clock ?? systemClock;
    this.random = cfg.random ?? systemRandom;
    this.conflictHandler = cfg.conflictHandler;
    this.exportHook = cfg.exportHook;
    this.tokens = cfg.tokens;
    this.predictors = cfg.predictors ?? {};
    this.features = cfg.features ?? DEFAULT_FEATURES;
    this.mintClientId = cfg.newClientId ?? ((): string => newUuid(this.random));
  }

  /** Where the bytes that actions name are kept (09 [READ-94]): the app's stash, else the platform's for the partition (`useBlobStash`), else one in memory. */
  get blobStash(): BlobStash {
    return this.blobStashIn;
  }

  /** The stash of the partition that the platform's storage provider gives (a browser's IndexedDB, 09 [READ-94]); called once, when the partition is opened, and never over the app's own. */
  protected useBlobStash(stash: BlobStash): void {
    this.blobStashIn = stash;
  }

  /** The HTTP of the blob store: the app's, or the platform's `fetch` (09 §9.9). */
  protected get blobHttp(): BlobHttp {
    return (this.blobHttpGiven ??= fetchBlobHttp());
  }

  /**
   * A new `client_id` (03 [ID-35], 14 [CLI-63]): at an engine's start and at every flusher handover, and when a fork or an unknown instance has the engine
   * re-identify. It is kept in memory only (`putOp`) and never one that this engine has had before (10 [AUTH-19]: "MUST NOT reuse a `client_id`"): an id that the
   * source gives again is asked for again, and a source that keeps repeating is given up for the default one; a random source that repeats too is broken, and
   * the engine goes on with what it has rather than wait for it.
   */
  protected newClientId(): string {
    let id = this.mintClientId();
    for (let tries = 0; this.clientIdsUsed.has(id) && tries < 16; tries++) id = tries < 8 ? this.mintClientId() : newUuid(this.random);
    this.clientIdsUsed.add(id);
    return id;
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // The queue

  /** Runs fn as the next task of the queue and resolves with its result. A task never calls `run` and waits for it. */
  protected run<T>(fn: () => T | Promise<T>): Promise<T> {
    this.queued++;
    this.activity++;
    const result = this.tail
      .then(() => {
        this.activity++;
        return fn();
      })
      .then(
        (v) => {
          this.flushViewChanges();
          return v;
        },
        (e: unknown) => {
          this.flushViewChanges();
          throw e;
        },
      );
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

  /**
   * Forgets the runtime of a flusher that gave the role up (14 [CLI-61]): the next time the tab is the flusher it starts as a tab that was never
   * one, from the stored state (`becomeFlusher`). The layers that keep runtime state override it and call this one.
   */
  protected resetRuntime(): void {
    this.runtimes.clear();
    this.decoded.clear();
    this.holds.clear();
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Events

  protected emit(e: EngineEvent): void {
    this.bus.emit(e);
  }

  on(listener: EngineEventListener): () => void {
    return this.bus.on(listener);
  }

  /**
   * Registers a listener for the changes of the view of the followed keyspaces, which the local query layer is driven by (`query/`): at the
   * end of each task of the engine, one `ViewChange` for each keyspace whose confirmed state or prediction changed in it, with the keys that
   * changed (view-feed.ts). A listener that throws never stops the engine. Returns the function that removes it.
   */
  onViewChange(listener: ViewChangeListener): () => void {
    return this.feed.on(listener, () => {
      for (const f of this.follows.values()) f.shown = this.feed.shownOf(f, this.viewOf(f));
    });
  }

  /**
   * Reports the pending changes of the view to the listeners now, which the end of the task would do: for a reader that is asked in the middle of one
   * (an app's listener of an event), so that what it keeps in step with the feed is the engine's state at that moment. Nothing pending: no cost.
   */
  flushViewChanges(): void {
    try {
      this.feed.flush(this.follows, (f) => this.viewOf(f));
    } catch (e) {
      this.noteInternalError(e);
    }
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
    // `max`: the field's protocol maximum where 04 Table 4.10a gives one that 02 [ARC-63] lets a server refuse a message for. 07 [MUT-75] has a server
    // advertise at most the maximum; a value above it is a server that breaks that rule, which the client reports as it reports a 0, and then holds itself to
    // the maximum, so that it never builds a `Mutation` of more than 2,000 changes or an interest of more than 1,000 prefixes, which a server refuses whole.
    const take = (field: string, v: number | bigint, floor: number, set: (n: number) => void, max?: number): void => {
      const n = Number(v);
      if (max !== undefined && n > max) {
        if (!this.limitsReported.has(field)) {
          this.limitsReported.add(field);
          this.emit({ type: "protocol-error", what: `AppLimits.${field}`, detail: `${n}, over its protocol maximum of ${max} (04 Table 4.10a, 07 [MUT-75]); the engine uses ${max} (02 [ARC-63])`, keyspaces: [] });
        }
        set(max);
        return;
      }
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
    take("max_changes_per_action", l.maxChangesPerAction, FLOOR_LIMITS.maxChangesPerAction, (n) => (next.maxChangesPerAction = n), MAX_CHANGES_PER_ACTION);
    take("max_action_bytes", l.maxActionBytes, FLOOR_LIMITS.maxActionBytes, (n) => (next.maxActionBytes = n));
    take("max_value_bytes", l.maxValueBytes, FLOOR_LIMITS.maxValueBytes, (n) => (next.maxValueBytes = n));
    take("max_mutations_per_request", l.maxMutationsPerRequest, FLOOR_LIMITS.maxMutationsPerRequest, (n) => (next.maxMutationsPerRequest = n));
    take("max_request_bytes", l.maxRequestBytes, FLOOR_LIMITS.maxRequestBytes, (n) => (next.maxRequestBytes = n));
    take("max_inflight_gated_per_client", l.maxInflightGatedPerClient, FLOOR_LIMITS.maxInflightGated, (n) => (next.maxInflightGated = n));
    take("keyspace_not_found_wait_seconds", l.keyspaceNotFoundWaitSeconds, FLOOR_LIMITS.keyspaceNotFoundWaitMs / 1000, (n) => (next.keyspaceNotFoundWaitMs = n * 1000));
    take("max_interest_prefixes", l.maxInterestPrefixes, FLOOR_LIMITS.maxInterestPrefixes, (n) => (next.maxInterestPrefixes = n), MAX_INTEREST_PREFIXES);
    // 09 §9.8, §9.9 (04 Table 4.10a): the presence and blob limits. Floors: the state size 4 KiB, the members 100, the TTL 10 s; a field with no floor
    // (the blob size) takes its default. 07 [MUT-75]: `presence_min_publish_interval_ms` is 0 from a server whose revision does not define it, and the client
    // then uses its default and reports no protocol error; a value above its maximum of 5,000 is reported and held to the maximum, as the others are.
    take("max_presence_state_bytes", l.maxPresenceStateBytes, FLOOR_LIMITS.maxPresenceStateBytes, (n) => (next.maxPresenceStateBytes = n), MAX_PRESENCE_STATE_BYTES);
    take("max_presence_members_per_topic", l.maxPresenceMembersPerTopic, FLOOR_LIMITS.maxPresenceMembersPerTopic, (n) => (next.maxPresenceMembersPerTopic = n));
    take("presence_ttl_seconds", l.presenceTtlSeconds, FLOOR_LIMITS.presenceTtlMs / 1000, (n) => (next.presenceTtlMs = n * 1000));
    take("max_blob_bytes", l.maxBlobBytes, FLOOR_LIMITS.maxBlobBytes, (n) => (next.maxBlobBytes = n));
    if (l.presenceMinPublishIntervalMs === 0) next.presenceMinPublishIntervalMs = FLOOR_LIMITS.presenceMinPublishIntervalMs;
    else take("presence_min_publish_interval_ms", l.presenceMinPublishIntervalMs, FLOOR_LIMITS.presenceMinPublishIntervalMs, (n) => (next.presenceMinPublishIntervalMs = n), MAX_PRESENCE_MIN_INTERVAL_MS);
    const changed = next.maxOfflineMs !== this.limits.maxOfflineMs;
    const presenceChanged = next.presenceTtlMs !== this.limits.presenceTtlMs || next.presenceMinPublishIntervalMs !== this.limits.presenceMinPublishIntervalMs;
    this.limits = next;
    if (presenceChanged) this.onPresenceLimitsChanged();
    if (changed) {
      // 14 [CLI-54]: the horizon is kept with the stored state, so that it applies while the client is offline
      if (this.role === "flusher") this.post(() => this.tx([{ op: "setMeta", key: "maxOfflineSeconds", value: next.maxOfflineMs / 1000 }]).catch(() => undefined));
      this.onLimitsChanged();
    }
  }

  /** Something durable changed: the other tabs of the engine are told (tab-layer.ts). */
  protected touchPersisted(): void {
    // nothing here
  }

  /** The runtime that a follower mirrors changed: the connection, a hold (tab-layer.ts). */
  protected onRuntimeChanged(): void {
    // nothing here
  }

  /** The limits changed in a way that other layers read (the offline horizon, 14 [CLI-56]); a layer that does overrides it. */
  protected onLimitsChanged(): void {
    // nothing here
  }

  /** The presence TTL or the publish interval changed (09 [READ-76], [READ-79]); the presence layer overrides it. */
  protected onPresenceLimitsChanged(): void {
    // nothing here
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Storage

  /**
   * Commits the operations as one local transaction (14 [CLI-21]). Rejects, with nothing applied, when the storage cannot. A follower tab never
   * writes the store that the flusher owns (14 [CLI-61], 10 [AUTH-25]): the work of a flusher that was handed over, and that ends after the
   * lock went, finds its writes refused.
   */
  protected async tx(ops: readonly StorageOp[]): Promise<void> {
    if (ops.length === 0) return;
    if (this.role === "follower") throw new StorageError("this tab is not the flusher: it writes nothing (14 [CLI-61])");
    try {
      await this.storage.commit(ops);
      this.touchPersisted();
    } catch (e) {
      if (e instanceof StorageError || (e instanceof Error && e.name === "StoreFull")) throw e;
      throw new StorageError(e instanceof Error ? e.message : String(e));
    }
  }

  /**
   * The operation that stores an entry. The `client_id` is kept in memory only and never persisted (03 [ID-35]: so that backups, disk images and
   * cloned profiles never carry it): the stored copy has none. It is never read back as an identity either: every start and every handover
   * mints a new one and re-stamps what has no definitive result (14 [CLI-63]).
   */
  protected putOp(e: OutboxEntry): StorageOp {
    return { op: "putEntry", entry: e.clientId === "" ? e : { ...e, clientId: "" } };
  }

  /** Loads what the storage holds into the mirrors (14 [CLI-8]). */
  protected async loadStored(): Promise<StoredState> {
    const s = await this.storage.load();
    this.adoptStored(s);
    return s;
  }

  /**
   * `loadStored` without the pages and tails of the replicas (a follower reads the stored state on every message of the flusher, 14 [CLI-61]);
   * undefined when the storage has no light read, and the state is read whole.
   */
  protected async loadStoredLight(): Promise<StoredLight | undefined> {
    if (this.storage.loadLight === undefined || this.storage.loadImage === undefined) return undefined;
    const s = await this.storage.loadLight();
    this.adoptStored(s);
    return s;
  }

  private adoptStored(s: Omit<StoredState, "replicas">): void {
    this.lockedEntries = [];
    // 14 [CLI-60]: only the entries whose author is the current principal are the engine's; the others stay in the store, locked
    const mine = (a: Author | undefined): boolean => a === undefined || (a.issuer === this.author.issuer && a.sub === this.author.sub);
    for (const e of s.entries) {
      if (mine(e.author)) this.entries.set(e.mutationId, e);
      else this.lockedEntries.push(e);
    }
    for (const e of s.deadLetters) if (mine(e.author)) this.deadLetters.set(e.mutationId, e);
    for (const e of s.journal) if (mine(e.author)) this.journal.set(e.mutationId, e);
    for (const e of s.exports) if (mine(e.author)) this.exports.set(e.mutationId, e);
    for (const e of s.rejections) if (mine(e.author)) this.rejections.set(e.mutationId, e);
    for (const e of s.drafts) if (mine(e.author)) this.drafts.set(draftKey(e), e);
    this.unreadable = s.unreadable === undefined ? [] : [...s.unreadable];
    const ord = s.meta["nextOrdinal"];
    this.nextOrdinal = typeof ord === "number" ? ord : 1;
    for (const e of s.entries) if (e.ordinal >= this.nextOrdinal) this.nextOrdinal = e.ordinal + 1;
    const off = s.meta["serverOffsetMs"];
    if (typeof off === "number") this.restoreServerOffset(off);
    const mo = s.meta["maxOfflineSeconds"];
    if (typeof mo === "number" && mo > 0) this.limits = { ...this.limits, maxOfflineMs: mo * 1000 };
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Entries

  /**
   * The entries of a store of the engine in the authoring order of their actions (14 [CLI-38], [CLI-77]): not the order in which they ended, and not the key
   * order of a storage that reads by key. An entry with no ordinal (an earlier build's) goes first, in the order that it came.
   */
  protected authoringOrder<T extends { ordinal?: number }>(entries: Iterable<T>): T[] {
    return [...entries].sort((a, b) => (a.ordinal ?? 0) - (b.ordinal ?? 0));
  }

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

  /** Whether a store of the partition holds a mutation with this id: the outbox, the journal, the dead-letter store, the export entries, the rejections. */
  protected knownMutation(id: string): boolean {
    return this.entries.has(id) || this.journal.has(id) || this.deadLetters.has(id) || this.exports.has(id) || this.rejections.has(id);
  }

  /** The runtime state of an entry. */
  protected rt(id: string): Runtime {
    let r = this.runtimes.get(id);
    if (r === undefined) {
      r = { inflight: false, nextAttemptAt: 0, timer: undefined, polling: false, alone: false, blobs: "unchecked", blobRepairs: 0, authTries: 0 };
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
    // the action is gone (retired, cancelled, ended): the hold that waited for the app to stage its bytes again goes with it (`blobs.ts`)
    for (const h of this.holds.all()) if (h.kind === "BLOB_BYTES_LOST" && h.mutationId === id) this.stopHold(h.kind, h.keyspaceId, h.lane, h.mutationId);
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
    if (hold.blobIds !== undefined) ev.blobIds = [...hold.blobIds];
    this.emit(ev);
    this.onHoldChanged();
    this.onRuntimeChanged();
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
    this.onRuntimeChanged();
    return true;
  }

  protected holdScopeOf(kind: HoldKind): HoldScope {
    switch (kind) {
      case "UNKNOWN_OP_TYPE":
      case "CLIENT_FORKED":
      case "CLIENT_UNKNOWN":
      case "UNSUPPORTED_FEATURE":
      case "UNKNOWN_HOLD":
      case "MUTATE_FAILURE":
        return "lane";
      case "KEYSPACE_NOT_FOUND":
      case "UPGRADE_REQUIRED":
      case "PROTOCOL_ERROR":
      case "HISTORY_DIVERGED":
      case "UNKNOWN_DROP_REASON":
      case "EPOCH_RESOLUTION":
        return "keyspace";
      case "CONFLICT_WAIT":
      case "BLOB_BYTES_LOST":
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
