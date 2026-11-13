// One engine across the tabs of a device (14 §14.8): the flusher election, the handover, and the read-through of the other tabs.
//
//  - [CLI-61] The tabs and windows of one engine on one device share one outbox through a single **flusher**, elected by a cross-tab lock
//    (`tabs.ts`, one exclusive lock per partition). The engine of the tab that holds the lock owns the outbox, the session and the
//    `client_id`: it is the only one that sends, and the only one that writes the outbox. The engines of the other tabs are
//    **followers**: they open no session, read through the flusher by a local broadcast, and wait in the lock's queue. A follower's
//    actions (`submit`, `cancel`, an acknowledgement, a new keyspace) are carried to the flusher, which authors and stores them: a
//    follower never writes the outbox, so two tabs never author against two views of it. When the flusher goes away (it closes, hands
//    over, or crashes: the platform frees its lock), the next tab in line is granted the lock and becomes the flusher. Where no
//    reliable cross-tab lock exists, each tab is an engine of its own, with an outbox of its own (the partition key names the tab),
//    its own session and its own `client_id`.
//  - [CLI-63] A flusher mints a new `client_id` at its start and at every handover (`becomeFlusher`), keeps it in memory only, and
//    re-stamps every action without a definitive result before it sends under it (the rehydration of `engine.ts`). The tab that hands the role over keeps
//    none (03 [ID-35]: a follower has no client instance), and mints another if it takes the role back.
//  - What a follower reads is the flusher's durable state: the outbox, the stores and the persisted replica are re-read from the
//    storage when the flusher says that they changed (`state` messages); its events are the flusher's, relayed (`event`); the
//    connection and the holds are the flusher's runtime (`state`). A follower that reads the replica folds the pages and the tail
//    again (`restoreImage`): a later change could carry the records of the broadcast instead, which is an optimization.
//  - [CLI-61] "where the platform throttles a hidden tab so that it cannot [keep the session and the presence entry alive], the flusher
//    MUST hand its role over to a visible tab of the engine, when there is one": `handOverWhenHidden` does it from the tab's visibility.
//    The hidden flusher offers the role to the tabs of the engine, and only a tab that answers (it is alive, a follower, visible) is given it:
//    the flusher then ends its session, its pump and its work, releases the lock, and queues for it again as a follower; a tab that is
//    ahead of the chosen one in the lock's queue passes the lock on. With no answer the flusher keeps the role. "Two flushers MUST NOT drain
//    one outbox": the lock is released only after the work of the old flusher has ended, and a tab that is not the flusher writes nothing.
//
//  - [CLI-61], 09 [READ-74], [READ-83] A follower tab that closes keeps no presence: each tab holds a lock named for its tab and engine (`tabLockName`) for as long as it lives, and the flusher asks for
//    it for each follower that it hears of; the platform grants it when the tab closes or crashes, and the flusher then ends the tab's registrations (`presenceTabGone`), leaving a topic when the
//    last of its tabs has gone. A `pagehide` is a hint that says so at once (`gone`). Where there is no reliable cross-tab lock, each tab is an engine of its own and there is no follower to lose.
//
// SPEC-QUESTION (14 [CLI-61], Q54.1): the text does not say where a follower's `submit` goes. Reading A (taken): to the flusher, which authors,
// stores and answers it: [CLI-21] asks that the call resolve only when the action is in a durable outbox, which holds, and the flusher
// is the one writer of the outbox, so the dependencies of [CLI-30] are computed from one view. Reading B: the follower appends to the
// shared storage itself and the flusher re-reads; two tabs would then compute the dependencies of their actions from two views of the
// outbox, and each would need a cross-tab lock around authoring. [CLI-21] makes a typed local error a refusal ("nothing was accepted"), so a
// call is never refused because the flusher is slow, or because it went away while it may have committed: the follower mints the
// mutation id, asks again every `COMMAND_TIMEOUT_MS`, reports the call as waiting (`status().pendingCalls`), hands it to the next flusher
// when one is announced, and, when its own tab is promoted, runs it itself; a `submit` that carries an id that the outbox, the journal or a
// store already holds is answered with it, authored once. `NO_FLUSHER` is for an engine that is stopping, and for one with no tab host.

import { KeyspaceLifecycle } from "../gen/abpc/v2/types_pb.js";
import { BoundedSet } from "./bounded.js";
import { BlobError, type BlobErrorCode, type BlobWait } from "./blob-store.js";
import { LocalRefusal, type LocalRefusalCode, type StorageCause } from "./errors.js";
import type { EngineConfig } from "./core.js";
import { Follow } from "./follow.js";
import type { PresenceEntryRaw, PresenceEvent } from "./presence.js";
import { RevocationLayer } from "./revocation.js";
import type { ReplicaImage, ReplicaSummary } from "./storage.js";
import type { LockLease, PartitionKey, StorageProvider, TabChannel, TabHost } from "./tabs.js";
import { lockName, tabLockName } from "./tabs.js";
import type { EngineEvent, HoldInfo } from "./types.js";

/** How long a follower waits for the flusher to answer a command before it asks again, on the engine's clock (it never refuses a call for it). */
export const COMMAND_TIMEOUT_MS = 5_000;

/** How long a hidden flusher waits for a visible tab to answer its offer of the role, on the engine's clock. */
export const HANDOVER_PROBE_MS = 1_000;

/** How long a tab acts on the announcement of a handover, on the engine's clock: it is a safety net, the claim of the chosen tab ends it. */
const HANDOFF_VALID_MS = 10_000;

/** The flusher's runtime, which a follower mirrors: what the sync status reports besides the durable stores. */
export interface RuntimeSnapshot {
  connection: "live" | "connecting" | "offline" | "stopped";
  subscribeRetry?: string | undefined;
  holds: HoldInfo[];
  suspended: boolean;
  fullStores: string[];
  skippedChanges: number;
  loadsWithSkipped: number;
  anomalies: Record<string, number>;
  stopped: Record<string, string>;
  /** The keyspaces whose local copy the flusher purged (14 [CLI-66], [CLI-67], [CLI-77]): a follower shows nothing of them either, and runs its purge hooks (B3-M4). */
  purged?: string[];
  /** The `client_id` of the flusher's session: what a follower tab tells the user's own presence entry by (14 [CLI-61], 09 [READ-75]). */
  clientId: string;
  /** The stash of blob bytes, as the flusher has it (see `SyncStatus`). */
  blobStash: "memory" | "durable";
  stagedBlobs: number;
  stagedBytes: number;
}

type TabMessage =
  /** `inc`: the engine of the tab (a reload is another one); `locked`: the tab holds its lock (`tabLockName`), so the flusher can learn that it closed. */
  | { t: "hello"; from: string; visible: boolean; reply: boolean; inc?: string; locked?: boolean }
  | { t: "visible"; from: string; visible: boolean }
  | { t: "gone"; from: string; inc?: string }
  /** The flusher offers its role to the tabs of the engine; a visible follower answers with `accept`. */
  | { t: "offer"; from: string; id: string }
  | { t: "accept"; from: string; to: string; id: string }
  /** The flusher gives the role to `to`: a tab ahead of it in the lock's queue passes the lock on. */
  | { t: "handoff"; from: string; to: string; id: string }
  /** A tab became the flusher: the calls that followers carried to the flusher that went away are carried to it. */
  | { t: "claimed"; from: string }
  | { t: "state"; from: string; rt: RuntimeSnapshot }
  | { t: "event"; from: string; event: EngineEvent }
  /** The flusher's view of a presence topic (09 §9.8, 14 [CLI-61]): the other tabs read it here; a presence entry is one for all the tabs of the engine. */
  | { t: "presence"; from: string; keyspaceId: string; topic: string; entries: PresenceEntryRaw[] }
  /** Any other presence event of the flusher (`joined`, `published`, `refused`, `ended`), for the tabs whose calls it concerns. */
  | { t: "presence-event"; from: string; event: PresenceEvent }
  /** `abortable`: the app of the tab may stop the call (`cmd-abort`); `wait`: it wants to hear what the call waits for (`wait`). */
  | { t: "cmd"; from: string; inc?: string; id: number; name: string; args: unknown[]; abortable?: boolean; wait?: boolean }
  /** The app of a follower tab stopped a call that it carried to the flusher. */
  | { t: "cmd-abort"; from: string; inc?: string; id: number }
  /** The flusher runs the call (or has run it): the follower does not carry it again. */
  | { t: "accepted"; from: string; to: string; id: number }
  /** What a call of a follower tab waits for on the flusher (`BlobCallOptions.onWait`). */
  | { t: "wait"; from: string; to: string; id: number; wait: BlobWait }
  | { t: "reply"; from: string; to: string; id: number; ok: true; value: unknown }
  | { t: "reply"; from: string; to: string; id: number; ok: false; error: { name: string; code?: string; cause?: string; detail: string; message: string } };

interface Wait {
  name: string;
  args: unknown[];
  resolve: (v: unknown) => void;
  reject: (e: unknown) => void;
  /** Cancels the timer that asks again. */
  cancel: () => void;
  /** The flusher answered that it runs the call: it is not carried again unless another tab becomes the flusher. */
  accepted: boolean;
  /** What the app gave the call: it may stop it, and hears what it waits for. */
  signal?: AbortSignal;
  onWait?: (w: BlobWait) => void;
  /** Removes the listener of the signal. */
  detach?: () => void;
}

/** What the app of a follower tab gave a call that goes to the flusher (14 [CLI-44]: "the app decides when to stop asking"). */
export interface CommandOptions {
  signal?: AbortSignal;
  onWait?: (w: BlobWait) => void;
}

/** What a command that a follower's call carries to the flusher may use: the signal that stops it, and where its waits go. */
export interface CommandContext {
  signal?: AbortSignal;
  onWait?: (w: BlobWait) => void;
}

/** A command that the flusher runs for a follower tab, with the answer that it gave (kept for a while, so that a call that is carried again is answered, not run again). */
interface RunningCommand {
  ctl: AbortController;
  reply?: Extract<TabMessage, { t: "reply" }>;
}

/** How many answers of finished commands the flusher keeps for the calls that are carried again. */
const COMMANDS_REMEMBERED = 256;

/** A hidden flusher's offer of the role. */
interface HandoverRound {
  id: string;
  /** The tab became visible again before an answer came. */
  cancelled: boolean;
  /** An answer came and the handover is made: it cannot be cancelled. */
  committed: boolean;
  /** Ends the wait for an answer. */
  answer: (tab: string | undefined) => void;
  done: Promise<boolean>;
}

export abstract class TabLayer extends RevocationLayer {
  protected readonly tabHost: TabHost | undefined;
  protected partition!: PartitionKey;
  protected provider: StorageProvider | undefined;
  protected lease: LockLease | undefined;
  /** The flusher's runtime, as a follower last heard it. */
  protected runtimeMirror: RuntimeSnapshot | undefined;
  private channel: TabChannel | undefined;
  private offChannel: (() => void) | undefined;
  private offVisibility: (() => void) | undefined;
  private lockAbort: AbortController | undefined;
  private readonly peers = new Map<string, boolean>();
  private readonly waits = new Map<number, Wait>();
  private cmdSeq = 0;
  /** This engine among the engines of its tab (a reload is the same tab id): the lock of this tab is named with it, and so are its calls. Not the engine's random source: it tells engines apart, no more. */
  private readonly incarnation = `${Date.now().toString(36)}${Math.random().toString(36).slice(2)}`;
  /** The lock that this tab holds for as long as it lives (`tabLockName`); undefined where there is none, or when the tab did not get it. */
  private tabLease: LockLease | undefined;
  /** The flusher's requests for the locks of the follower tabs that it knows of: one is granted when its tab is gone. By tab id. */
  private readonly tabWatch = new Map<string, { inc: string; abort: AbortController }>();
  /** The engines of tabs that are known to be gone: what they still send is not run. */
  private readonly goneIncarnations = new BoundedSet<string>(1024);
  /** The commands that followers carried to this flusher, by `from`, `inc` and `id`. */
  private readonly commandsRun = new Map<string, RunningCommand>();
  private offPageHide: (() => void) | undefined;
  private round: HandoverRound | undefined;
  private handoffSeq = 0;
  /** The handover that the flusher announced: the tab that it is for, and until when this tab acts on it. */
  private handoff: { to: string; id: string; untilMs: number } | undefined;
  /** The handover whose lock this tab already passed on (once each, so that a chosen tab that went away never makes the lock go round for ever). */
  private passedHandoff: string | undefined;
  private refreshing = false;
  private refreshAgain = false;
  private stateQueued = false;
  protected handOverWhenHidden = false;
  /** The commands that a flusher runs for a follower, by name. */
  protected abstract commands(): Record<string, (args: unknown[], ctx?: CommandContext) => Promise<unknown>>;
  protected abstract becomeFlusher(): Promise<void>;

  protected constructor(cfg: EngineConfig) {
    super(cfg);
    this.tabHost = cfg.tab;
    this.provider = cfg.store ?? cfg.tab?.storage;
    // 14 [CLI-61] (S-07, A8-S3): "where the platform throttles a hidden tab so that it cannot, the flusher MUST hand its role over to a visible tab"
    this.handOverWhenHidden = cfg.handOverWhenHidden ?? cfg.tab?.throttlesHidden === true;
    this.sharedDevice = (cfg.mode ?? "personal") === "shared-device";
    this.setHorizonWarning(cfg.horizonWarning);
    this.copyPolicy = cfg.copyPolicy ?? "keep";
    this.persistReplica = cfg.persistReplica !== false;
    if (cfg.replicaTailLimit !== undefined) this.replicaTailLimit = cfg.replicaTailLimit;
    if (cfg.maxSubscribeBytes !== undefined) this.maxSubscribeBytes = cfg.maxSubscribeBytes;
    if (cfg.maxSubscribeCursors !== undefined) this.maxSubscribeCursors = cfg.maxSubscribeCursors;
  }

  get tabId(): string {
    return this.tabHost?.tabId ?? "";
  }

  /** The partition that this engine's storage is: origin, app, and the principal or the device (14 [CLI-59], [CLI-74]). */
  partitionKey(): PartitionKey {
    return this.partition;
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Opening the storage, and the election

  /** Opens the partition's storage (14 [CLI-74]), asks the platform to keep it (14 [CLI-75]), and elects the flusher (14 [CLI-61]). */
  protected async boot(cfg: EngineConfig): Promise<void> {
    await this.tabHost?.ready?.(); // the tab id names the outbox where no reliable lock exists, and is the sender of every message: it is read after this
    const mode = cfg.mode ?? "personal";
    const reliable = this.tabHost === undefined || this.tabHost.locks.reliable;
    this.partition = {
      origin: cfg.origin ?? "local",
      appId: cfg.appId,
      partition: mode === "shared-device" ? this.author.deviceId : `${this.author.issuer}|${this.author.sub}`,
      ...(this.tabHost !== undefined && !reliable ? { tab: this.tabHost.tabId } : {}),
    };
    if (cfg.storage === undefined && this.provider !== undefined) this.storage = await this.provider.open(this.partition);
    // 09 [READ-94]: the bytes that the partition's actions name outlive a restart as its outbox does, where the platform has a durable stash (a browser's IndexedDB); the app's own goes first
    if (cfg.blobs?.stash === undefined && this.provider?.blobStash !== undefined) this.useBlobStash(this.provider.blobStash(this.partition));
    if (this.storage.requestPersistence !== undefined) {
      try {
        await this.storage.requestPersistence(); // 14 [CLI-75]: the mode of the sync status says what the platform promised
      } catch {
        // a refused request leaves the storage as it is
      }
    }
    if (this.tabHost === undefined || !reliable) {
      await this.becomeFlusher();
      this.emit({ type: "role", role: "flusher", cause: "ELECTED", clientId: this.clientId });
      return;
    }
    // 09 [READ-74]: the lock that tells the flusher that this tab is gone: held before the first message of this tab, so that no message precedes it
    try {
      this.tabLease = await this.tabHost.locks.tryAcquire(tabLockName(this.partition, this.tabHost.tabId, this.incarnation));
    } catch {
      this.tabLease = undefined; // no lock: this tab's registrations end by its `gone` message, or with the flusher
    }
    this.openChannel();
    this.watchPageHide();
    const lease = await this.tabHost.locks.tryAcquire(lockName(this.partition));
    if (lease !== undefined) {
      this.lease = lease;
      await this.becomeFlusher();
      this.emit({ type: "role", role: "flusher", cause: "ELECTED", clientId: this.clientId });
    } else {
      this.role = "follower";
      await this.refreshMirrors();
      this.emit({ type: "role", role: "follower", cause: "FOLLOWING" });
      this.queueForLock();
    }
    this.announce();
    this.watchVisibility();
  }

  private queueForLock(): void {
    const host = this.tabHost;
    if (host === undefined) return;
    this.lockAbort = new AbortController();
    const signal = this.lockAbort.signal;
    void host.locks.acquire(lockName(this.partition), signal).then(
      (lease) => {
        if (this.stopped || this.role !== "follower") {
          void lease.release();
          return;
        }
        // a handover to another tab is under way: the lock goes on to it, once (14 [CLI-61]: the role goes to a visible tab, not to the next in line)
        const h = this.handoff;
        if (h !== undefined && h.untilMs <= this.clock.now()) this.handoff = undefined;
        if (this.handoff !== undefined && this.handoff.to !== host.tabId && this.passedHandoff !== this.handoff.id) {
          this.passedHandoff = this.handoff.id;
          void lease.release().then(() => {
            if (!this.stopped && this.role === "follower") this.queueForLock();
          });
          return;
        }
        this.handoff = undefined;
        this.lease = lease;
        // the previous flusher is gone: this tab is the next in line (14 [CLI-61], [CLI-63]): it reads the durable state, mints a `client_id`,
        // and re-stamps what has no definitive result before it sends
        void this.run(async () => {
          await this.becomeFlusher();
          this.emit({ type: "role", role: "flusher", cause: "ELECTED", clientId: this.clientId });
          this.send({ t: "claimed", from: host.tabId });
          this.announce();
          this.queueState();
          this.runOwnWaits();
          this.onFlusherChanged();
        }).catch((e: unknown) => this.noteInternalError(e));
      },
      () => undefined, // aborted: this tab stopped waiting
    );
  }

  /** The tabs that the engine knows of, with their visibility; for a test. */
  peerTabs(): { tabId: string; visible: boolean }[] {
    return [...this.peers].map(([tabId, visible]) => ({ tabId, visible }));
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // The channel

  private openChannel(): void {
    const host = this.tabHost;
    if (host === undefined) return;
    this.channel = host.channel(lockName(this.partition));
    this.offChannel = this.channel.onMessage((m) => this.onMessage(m as TabMessage));
  }

  private send(m: TabMessage): void {
    this.channel?.post(m);
  }

  private visibleNow(): boolean {
    return this.tabHost?.visible?.() ?? true;
  }

  private announce(): void {
    if (this.channel === undefined || this.tabHost === undefined) return;
    this.send({ t: "hello", from: this.tabHost.tabId, visible: this.visibleNow(), reply: false, inc: this.incarnation, locked: this.tabLease !== undefined });
  }

  /** The page is going away: the other tabs are told at once (a hint; the lock of the tab is what tells for sure), and the tab says so again when it comes back. */
  private watchPageHide(): void {
    const host = this.tabHost;
    if (host?.onPageHide === undefined) return;
    this.offPageHide = host.onPageHide((event) => {
      if (this.stopped || this.role === "stopped") return;
      if (event === "hide") {
        this.send({ t: "gone", from: host.tabId, inc: this.incarnation });
        return;
      }
      // a page that came back from the back-forward cache: the flusher ended what it registered, so the tab registers it again
      this.announce();
      if (this.role === "follower") this.onFlusherChanged();
    });
  }

  /**
   * The flusher asks for the lock of a follower tab, and is granted it when the tab is gone (it closed or crashed: the platform frees the lock). Not for a tab that holds none, nor with no reliable
   * lock; a tab that is known by another engine of it (a reload) is watched under the new one.
   */
  private watchTab(tab: string, inc: string | undefined, locked: boolean | undefined): void {
    const host = this.tabHost;
    if (this.role !== "flusher" || host === undefined || !host.locks.reliable || tab === host.tabId || locked !== true || inc === undefined) return;
    if (this.goneIncarnations.has(inc)) return;
    const have = this.tabWatch.get(tab);
    if (have?.inc === inc) return;
    have?.abort.abort();
    const ctl = new AbortController();
    this.tabWatch.set(tab, { inc, abort: ctl });
    host.locks.acquire(tabLockName(this.partition, tab, inc), ctl.signal).then(
      (lease) => {
        void lease.release();
        if (ctl.signal.aborted || this.tabWatch.get(tab)?.abort !== ctl) return; // another engine of the tab replaced it, or this tab is not the flusher any more
        this.tabWatch.delete(tab);
        this.tabIsGone(tab, inc);
      },
      () => undefined, // aborted: this tab stopped watching
    );
  }

  /** A follower tab is gone, said by its message or by its lock: what it registered ends (09 [READ-74]: the engine leaves a topic when the last of its tabs has). */
  private tabIsGone(tab: string, inc: string | undefined): void {
    if (inc !== undefined) this.goneIncarnations.add(inc);
    this.peers.delete(tab);
    const w = this.tabWatch.get(tab);
    if (w !== undefined && (inc === undefined || w.inc === inc)) {
      w.abort.abort();
      this.tabWatch.delete(tab);
    }
    for (const [key, r] of [...this.commandsRun]) if (key.startsWith(`${tab}\u0000${inc ?? ""}\u0000`)) r.ctl.abort(); // a call of a tab that is gone is of no use to it
    if (this.role === "flusher") this.post(() => this.presenceTabGone(tab));
  }

  private stopWatching(): void {
    for (const w of this.tabWatch.values()) w.abort.abort();
    this.tabWatch.clear();
    for (const r of this.commandsRun.values()) r.ctl.abort();
    this.commandsRun.clear();
  }

  private watchVisibility(): void {
    const host = this.tabHost;
    if (host?.onVisibility === undefined) return;
    this.offVisibility = host.onVisibility((visible) => {
      this.send({ t: "visible", from: host.tabId, visible });
      if (visible) {
        this.cancelOffer();
        return;
      }
      // 14 [CLI-61]: a hidden flusher hands its role over to a visible tab, when there is one: the tabs are asked, and the ones that answer are visible
      if (this.handOverWhenHidden && this.role === "flusher") void this.handOver().catch((e: unknown) => this.noteInternalError(e));
    });
  }

  /**
   * A tab of the engine is visible (it joined, or it was shown) and this tab is a flusher that is hidden: 14 [CLI-61] says it "MUST hand its role over to a
   * visible tab of the engine, when there is one", and there is one now, though there was none when this tab was hidden (V19, V14 MUST-3). The offer is made
   * again; the tab that answers it, as before, is a follower that is alive and visible.
   */
  private offerAgain(): void {
    if (this.handOverWhenHidden && this.role === "flusher" && !this.stopped && !this.visibleNow()) void this.handOver().catch((e: unknown) => this.noteInternalError(e));
  }

  private onMessage(m: TabMessage): void {
    switch (m.t) {
      case "hello":
        this.peers.set(m.from, m.visible);
        if (m.inc !== undefined) this.goneIncarnations.delete(m.inc); // a page that came back from the back-forward cache is no longer gone
        if (!m.reply && this.tabHost !== undefined) this.send({ t: "hello", from: this.tabHost.tabId, visible: this.visibleNow(), reply: true, inc: this.incarnation, locked: this.tabLease !== undefined });
        if (this.role === "flusher") {
          this.watchTab(m.from, m.inc, m.locked);
          if (!m.reply) this.queueState(); // a tab that comes learns what the flusher is (its `client_id`, its holds, what it stages)
        }
        if (m.visible) this.offerAgain();
        return;
      case "visible":
        this.peers.set(m.from, m.visible);
        if (m.visible) this.offerAgain();
        return;
      case "gone":
        // 09 [READ-74]: "The engine MUST NOT leave a topic while any of its tabs is still in it": a tab that left, left its topics
        this.tabIsGone(m.from, m.inc);
        return;
      case "offer":
        // 14 [CLI-61]: a tab that is alive, a follower in the lock's queue and visible takes the role that a hidden flusher offers
        if (this.role === "follower" && !this.stopped && this.lockAbort?.signal.aborted === false && this.visibleNow() && this.tabHost !== undefined) {
          this.send({ t: "accept", from: this.tabHost.tabId, to: m.from, id: m.id });
        }
        return;
      case "accept":
        if (m.to === this.tabHost?.tabId && this.round?.id === m.id) this.round.answer(m.from);
        return;
      case "handoff":
        if (this.role === "follower") this.handoff = { to: m.to, id: m.id, untilMs: this.clock.now() + HANDOFF_VALID_MS };
        return;
      case "claimed":
        if (this.handoff?.to === m.from) this.handoff = undefined;
        // the flusher that the calls of this tab went to is gone, and this is the next: they are carried to it, as ones that may have been answered
        if (this.role === "follower") {
          for (const [id, w] of [...this.waits]) this.postCommand(id, w);
          this.onFlusherChanged();
        }
        return;
      case "state":
        if (this.role !== "follower") return;
        this.runtimeMirror = m.rt;
        this.refreshFollower();
        return;
      case "event":
        if (this.role !== "follower") return;
        this.bus.emit(m.event); // the flusher's event, as the app of this tab hears it
        return;
      case "presence":
        if (this.role === "follower") this.presenceMirrorSet(m.keyspaceId, m.topic, m.entries);
        return;
      case "presence-event":
        if (this.role === "follower") this.deliverPresence(m.event);
        return;
      case "cmd":
        if (this.role === "flusher" && !this.stopped) this.runCommand(m);
        return;
      case "cmd-abort":
        if (this.role === "flusher") this.commandsRun.get(`${m.from}\u0000${m.inc ?? ""}\u0000${m.id}`)?.ctl.abort();
        return;
      case "accepted": {
        const w = m.to === this.tabHost?.tabId ? this.waits.get(m.id) : undefined;
        if (w !== undefined) {
          w.accepted = true;
          w.cancel(); // the flusher runs it: it is not carried again
        }
        return;
      }
      case "wait": {
        const w = m.to === this.tabHost?.tabId ? this.waits.get(m.id) : undefined;
        if (w?.onWait !== undefined) {
          try {
            w.onWait(m.wait);
          } catch {
            // a callback of the app never stops the engine
          }
        }
        return;
      }
      case "reply": {
        if (m.to !== this.tabHost?.tabId) return;
        const w = this.waits.get(m.id);
        if (w === undefined) return;
        this.waits.delete(m.id);
        w.cancel();
        w.detach?.();
        if (m.ok) {
          // the call resolves when this tab shows what it did: the app reads its own writes through its own engine (14 [CLI-21])
          const value = m.value;
          const done = (): void => w.resolve(value);
          if (this.role === "follower") this.detach(this.run(() => (this.role === "follower" ? this.refreshMirrors() : undefined)).then(done, done));
          else done();
        } else if (m.error.name === "LocalRefusal") w.reject(new LocalRefusal(m.error.code as LocalRefusalCode, m.error.detail, m.error.cause as StorageCause | undefined));
        else if (m.error.name === "BlobError") w.reject(new BlobError(m.error.code as BlobErrorCode, m.error.detail.replace(/^blob: [A-Z_]+: /, "")));
        else w.reject(new Error(m.error.message));
        return;
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // The flusher's side

  /** An event of the flusher goes to the followers too (14 [CLI-61]: "by a local broadcast from the flusher"). */
  protected emit(e: EngineEvent): void {
    super.emit(e);
    // `role` is the tab's own: a follower does not hear that another tab became the flusher as a change of its own role
    if (e.type !== "role" && this.role === "flusher" && this.channel !== undefined && this.tabHost !== undefined) this.send({ t: "event", from: this.tabHost.tabId, event: e });
  }

  /** A presence event of the flusher goes to the followers too (14 [CLI-61]). */
  protected onPresenceEmitted(e: PresenceEvent): void {
    if (this.role !== "flusher" || this.channel === undefined || this.tabHost === undefined || this.stopped) return;
    if (e.type === "changed") this.send({ t: "presence", from: this.tabHost.tabId, keyspaceId: e.keyspaceId, topic: e.topic, entries: this.presenceEntries(e.keyspaceId, e.topic) });
    else this.send({ t: "presence-event", from: this.tabHost.tabId, event: e });
  }

  /**
   * Another tab is the flusher now (it claimed the role), or this one is: what the tabs asked of the engine's session, such as their presence memberships and states, was the
   * old flusher's and is told to the new one again (09 [READ-73], 02 [ARC-51] item 4: after a new session a client joins its topics again). `Engine` implements it.
   */
  protected abstract onFlusherChanged(): void;

  protected touchPersisted(): void {
    this.queueState();
  }

  protected onRuntimeChanged(): void {
    this.queueState();
  }

  protected runtimeSnapshot(): RuntimeSnapshot {
    const anomalies: Record<string, number> = {};
    const stopped: Record<string, string> = {};
    const purged: string[] = [];
    let loadsWithSkipped = 0;
    for (const f of this.follows.values()) {
      if (f.anomalies > 0) anomalies[f.id] = f.anomalies;
      if (f.stopped !== undefined) stopped[f.id] = f.stopped;
      else if (f.overSessionCap !== undefined) stopped[f.id] = "QUOTA_EXCEEDED"; // 09 [READ-35], A.7 (V43 MUST 1)
      if (f.purged) purged.push(f.id);
      loadsWithSkipped += f.loadsWithSkipped;
    }
    return {
      connection: this.connection(),
      subscribeRetry: this.subscribeRetry(),
      holds: this.holds.info(),
      suspended: this.holds.all().some((h) => h.kind === "APP_SUSPENDED"),
      fullStores: this.storage.full(),
      skippedChanges: this.skippedChanges,
      loadsWithSkipped,
      anomalies,
      stopped,
      purged,
      clientId: this.clientId,
      blobStash: this.blobStashMode(),
      stagedBlobs: this.stashStats().count,
      stagedBytes: this.stashStats().bytes,
    };
  }

  /**
   * Tells the followers that the durable state or the runtime changed. One message for a burst of changes, and, while a task of the engine runs, one
   * message when the task has ended: a follower reads the stores and the replica that the flusher left at the end of a task, not between two of its
   * transactions (the record that retires an action, and the retirement, are one change of what the app sees, 14 [CLI-2]).
   */
  protected queueState(): void {
    if (this.channel === undefined || this.role !== "flusher" || this.stateQueued || this.stopped) return;
    this.stateQueued = true;
    queueMicrotask(() => {
      if (this.queued > 0 && this.stateQueued) return; // a task is under way: its end sends the message (`flushViewChanges`)
      this.sendState();
    });
  }

  private sendState(): void {
    this.stateQueued = false;
    if (this.role !== "flusher" || this.stopped || this.tabHost === undefined || this.channel === undefined) return;
    this.send({ t: "state", from: this.tabHost.tabId, rt: this.runtimeSnapshot() });
  }

  /** The end of a task of the engine: the followers are told what it changed (see `queueState`). */
  flushViewChanges(): void {
    super.flushViewChanges();
    if (this.stateQueued) queueMicrotask(() => this.stateQueued && this.sendState());
  }

  private runCommand(m: Extract<TabMessage, { t: "cmd" }>): void {
    const host = this.tabHost;
    if (host === undefined) return;
    if (m.inc !== undefined && this.goneIncarnations.has(m.inc)) return; // the tab is gone: nobody is waiting for the answer
    const key = `${m.from}\u0000${m.inc ?? ""}\u0000${m.id}`;
    const send = (r: Extract<TabMessage, { t: "reply" }>): void => this.send(r);
    // a call that is carried again (the follower asked again, or its message came twice) is run once: the flusher says that it has it, and answers when it is done
    const known = this.commandsRun.get(key);
    if (known !== undefined) {
      this.send({ t: "accepted", from: host.tabId, to: m.from, id: m.id });
      if (known.reply !== undefined) send(known.reply);
      return;
    }
    const fn = this.commands()[m.name];
    const reply = (r: { ok: true; value: unknown } | { ok: false; error: { name: string; code?: string; cause?: string; detail: string; message: string } }): void => {
      const msg = { t: "reply", from: host.tabId, to: m.from, id: m.id, ...r } as Extract<TabMessage, { t: "reply" }>;
      const rc = this.commandsRun.get(key);
      if (rc !== undefined) rc.reply = msg;
      send(msg);
    };
    if (fn === undefined) {
      this.send({ t: "reply", from: host.tabId, to: m.from, id: m.id, ok: false, error: { name: "Error", detail: m.name, message: `unknown command ${m.name}` } });
      return;
    }
    const ctl = new AbortController();
    this.commandsRun.set(key, { ctl });
    while (this.commandsRun.size > COMMANDS_REMEMBERED) {
      const oldest = [...this.commandsRun].find(([, r]) => r.reply !== undefined);
      if (oldest === undefined) break;
      this.commandsRun.delete(oldest[0]);
    }
    this.send({ t: "accepted", from: host.tabId, to: m.from, id: m.id });
    const ctx: CommandContext = {
      ...(m.abortable === true ? { signal: ctl.signal } : {}),
      ...(m.wait === true ? { onWait: (wait: BlobWait) => this.send({ t: "wait", from: host.tabId, to: m.from, id: m.id, wait }) } : {}),
    };
    this.detach(
      fn(m.args, ctx).then(
        (value) => reply({ ok: true, value }),
        (e: unknown) => {
          if (e instanceof LocalRefusal) reply({ ok: false, error: { name: "LocalRefusal", code: e.code, ...(e.cause !== undefined ? { cause: e.cause } : {}), detail: e.message.replace(/^refused locally \([A-Z_, ]+\): /, ""), message: e.message } });
          else if (e instanceof BlobError) reply({ ok: false, error: { name: "BlobError", code: e.code, detail: e.message, message: e.message } });
          else reply({ ok: false, error: { name: "Error", detail: String(e), message: e instanceof Error ? e.message : String(e) } });
        },
      ),
    );
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // The follower's side

  /**
   * Runs an action of the app where it belongs: on the flusher itself, or, for a follower, on the flusher through the channel
   * (see the SPEC-QUESTION above). A call that the flusher has not answered is asked again every `COMMAND_TIMEOUT_MS`, and stays waiting: it is
   * never refused while the flusher may still commit it ([CLI-21]).
   */
  protected viaFlusher<T>(name: string, args: unknown[], local: () => Promise<T>, opts: CommandOptions = {}): Promise<T> {
    if (this.role !== "follower") return local();
    const host = this.tabHost;
    if (host === undefined || this.channel === undefined) return Promise.reject(new LocalRefusal("NO_FLUSHER", "this engine has no flusher to carry the call"));
    // 14 [CLI-44]: "the app decides when to stop asking": a call that the app has stopped is not carried at all
    if (opts.signal?.aborted === true) return Promise.reject(new BlobError("ABORTED", "the call was aborted"));
    return new Promise<T>((resolve, reject) => {
      const id = ++this.cmdSeq;
      const w: Wait = { name, args, resolve: resolve as (v: unknown) => void, reject, cancel: () => undefined, accepted: false, ...(opts.signal !== undefined ? { signal: opts.signal } : {}), ...(opts.onWait !== undefined ? { onWait: opts.onWait } : {}) };
      if (opts.signal !== undefined) {
        const signal = opts.signal;
        const onAbort = (): void => {
          if (this.waits.get(id) !== w) return;
          this.waits.delete(id);
          w.cancel();
          this.send({ t: "cmd-abort", from: host.tabId, inc: this.incarnation, id }); // the flusher stops the call that it runs
          reject(new BlobError("ABORTED", "the call was aborted"));
        };
        signal.addEventListener("abort", onAbort, { once: true });
        w.detach = () => signal.removeEventListener("abort", onAbort);
      }
      this.waits.set(id, w);
      this.postCommand(id, w);
    });
  }

  /** Carries a call to the flusher, and arms the timer that carries it again, until the flusher says that it runs it. */
  private postCommand(id: number, w: Wait): void {
    const host = this.tabHost;
    if (host === undefined) return;
    w.cancel();
    w.accepted = false; // a flusher that has not heard of the call (it is a new one) has not accepted it
    w.cancel = this.clock.setTimer(COMMAND_TIMEOUT_MS, () => {
      if (this.waits.get(id) === w && this.role === "follower" && !w.accepted) this.postCommand(id, w);
    });
    this.send({ t: "cmd", from: host.tabId, inc: this.incarnation, id, name: w.name, args: w.args, ...(w.signal !== undefined ? { abortable: true } : {}), ...(w.onWait !== undefined ? { wait: true } : {}) });
  }

  /** The calls of this tab that wait for the flusher's answer (14 [CLI-21], [CLI-41]): none is refused while the flusher may still commit it. */
  callsWaiting(): number {
    return this.waits.size;
  }

  /**
   * This tab is the flusher now: the calls that it carried to the flusher that went away are its own to run. A `submit` carries the mutation id that
   * the follower minted, which a store of the partition may already hold (the flusher committed it and went away before it answered): then it
   * is answered, never authored twice.
   */
  private runOwnWaits(): void {
    const mine = [...this.waits.values()];
    this.waits.clear();
    for (const w of mine) {
      w.cancel();
      w.detach?.();
      const fn = this.commands()[w.name];
      if (fn === undefined) {
        w.reject(new Error(`unknown command ${w.name}`));
        continue;
      }
      this.detach(fn(w.args, { ...(w.signal !== undefined ? { signal: w.signal } : {}), ...(w.onWait !== undefined ? { onWait: w.onWait } : {}) }).then(w.resolve, w.reject));
    }
  }

  /** Refuses the calls that wait, with `NO_FLUSHER`, when the engine ends: a `submit` that the storage holds was accepted, and is answered with its id. */
  private async failWaits(why: string, lookup: boolean): Promise<void> {
    const waiting = [...this.waits.values()];
    this.waits.clear();
    let held: Set<string> | undefined;
    if (lookup && waiting.some((w) => w.name === "submit")) {
      try {
        const st = await this.storage.load();
        held = new Set([...st.entries, ...st.journal, ...st.deadLetters, ...st.exports, ...st.rejections].map((e) => e.mutationId));
      } catch {
        held = undefined;
      }
    }
    for (const w of waiting) {
      w.cancel();
      w.detach?.();
      const id = w.name === "submit" ? (w.args[2] as string | undefined) : undefined;
      if (id !== undefined && held?.has(id) === true) w.resolve({ mutationId: id });
      else w.reject(new LocalRefusal("NO_FLUSHER", why));
    }
  }

  /** Reads the flusher's durable state again, once at a time: a message that comes while it reads makes it read once more. */
  private refreshFollower(): void {
    if (this.refreshing) {
      this.refreshAgain = true;
      return;
    }
    this.refreshing = true;
    this.detach(
      (async () => {
        try {
          do {
            this.refreshAgain = false;
            await this.run(() => (this.role === "follower" ? this.refreshMirrors() : undefined));
          } while (this.refreshAgain);
        } finally {
          this.refreshing = false;
        }
      })(),
    );
  }

  /**
   * Makes the follower's mirrors what the storage holds: the outbox and the stores, and the replica of each followed keyspace, restored
   * from its image when the image moved. The follower folds what the flusher persisted by the same fold, so what it shows is what the
   * flusher confirmed (14 [CLI-2]).
   */
  protected async refreshMirrors(): Promise<void> {
    this.entries.clear();
    this.deadLetters.clear();
    this.journal.clear();
    this.exports.clear();
    this.rejections.clear();
    this.drafts.clear();
    this.decoded.clear();
    // 14 [CLI-61], V14 S4: the pages and the tails of the replicas are most of what a partition holds, and a message of the flusher says that
    // something changed, not what: the follower reads the heads, and the pages and the tail of a keyspace only when its image moved
    const light = await this.loadStoredLight();
    const s = light ?? (await this.loadStored());
    const ids = new Set<string>();
    const purged = new Set(this.runtimeMirror?.purged ?? []);
    for (const img of s.replicas) {
      if (!this.isMyImage(img)) continue; // another principal's cache on a shared device (14 [CLI-59])
      ids.add(img.keyspaceId);
      const have = this.follows.get(img.keyspaceId);
      if (purged.has(img.keyspaceId) && have?.purged === true) continue; // B3-M4: an image that the flusher's purge has not removed yet is not shown again
      const entryCount = "entries" in img.base ? img.base.entries.length : (img as ReplicaSummary).entryCount;
      const same =
        have !== undefined &&
        have.loaded &&
        have.state.seq === img.head.seq &&
        have.state.logEpoch === img.head.logEpoch &&
        have.image.baseSeq === img.base.checkpointSeq &&
        (have.image.entryCount === undefined || have.image.entryCount === entryCount) &&
        have.interest.length === img.head.interest.length &&
        have.interest.every((p, i) => p === img.head.interest[i]);
      if (same) {
        have.lastSyncMs = img.head.lastSyncMs;
        continue;
      }
      let image: ReplicaImage | undefined;
      if (light === undefined) {
        image = img as ReplicaImage;
        // the flusher applied records since: fold only those (the cost of a frame is the frame, not the whole persisted tail)
        if (have !== undefined && (await this.advanceMirror(have, image, true))) continue;
      } else {
        const sum = img as ReplicaSummary;
        if (have !== undefined && have.loaded && !have.purged) {
          // the records above the last one that this mirror folded: the tail of an image only grows while its pages are the same
          const after = [...have.image.seqs].reduce((m, x) => (x > m ? x : m), have.image.baseSeq ?? sum.base.checkpointSeq);
          const part = await this.storage.loadImage?.(sum.keyspaceId, { entries: false, tailAfter: after });
          if (part !== undefined && (await this.advanceMirror(have, { ...part, head: sum.head, base: { ...sum.base, entries: [] } }, false, entryCount))) continue;
        }
        image = await this.storage.loadImage?.(sum.keyspaceId);
        if (image === undefined) continue;
      }
      const f = await this.restoreImage(image);
      if (f === undefined) continue;
      have?.crdt.destroy();
      f.image.baseSeq = img.base.checkpointSeq;
      this.follows.set(f.id, f);
    }
    const followed = this.followedFromMeta(s.meta[this.metaKey("followed")]);
    for (const [id, v] of followed) {
      ids.add(id);
      if (!this.follows.has(id)) {
        const f = new Follow(id, this.random);
        f.interest = [...v.interest];
        this.follows.set(id, f);
      }
    }
    for (const e of this.entries.values()) ids.add(e.keyspaceId);
    for (const [id, f] of [...this.follows]) {
      if (!ids.has(id)) {
        f.crdt.destroy();
        this.follows.delete(id);
      } else f.invalidate();
    }
    const stopped = this.runtimeMirror?.stopped ?? {};
    for (const f of this.follows.values()) {
      f.stopped = stopped[f.id];
      // 14 [CLI-66], [CLI-61] (B3-M4): "one purge covers them": the flusher purged the store, and this tab purges what it holds of the keyspace in memory, its
      // downloaded blob bytes and, through its purge hooks, the pages of its query layer
      if (purged.has(f.id) && !f.purged) {
        this.purgeMemory(f);
        this.blobCacheLoadReplaced(f);
        this.detach(this.runPurgeHooks(f, f.stopped ?? "PURGED"));
      }
    }
  }

  /**
   * Folds the records that the flusher added to the tail of the image since this mirror last read it, when the image is otherwise the one
   * that the mirror holds (the same pages, epoch and interest, and every record that it folded still in the tail). Returns false when it is
   * not, or when a record does not fold: the caller then builds the mirror again from the whole image (`restoreImage`), which replaces it.
   */
  private async advanceMirror(have: Follow, img: ReplicaImage, whole: boolean, entryCount?: number): Promise<boolean> {
    if (!have.loaded || have.purged || have.image.baseSeq !== img.base.checkpointSeq) return false;
    if (img.head.logEpoch !== img.base.logEpoch || have.state.logEpoch !== img.head.logEpoch) return false;
    if (have.interest.length !== img.head.interest.length || !have.interest.every((p, i) => p === img.head.interest[i])) return false;
    if (entryCount !== undefined && have.image.entryCount !== undefined && have.image.entryCount !== entryCount) return false; // a widening added pages
    const fresh = img.tail.filter((r) => !have.image.seqs.has(r.seq));
    // the image that was read whole: every record that this mirror folded is still in the tail; a tail read from the last one folded is only what is new
    if ((whole && img.tail.length - fresh.length !== have.image.seqs.size) || img.head.seq < have.state.seq) return false;
    try {
      for (const r of fresh) if (r.logEpoch !== img.base.logEpoch || !(await this.manifests.ensure(r.manifestVersion))) return false;
      for (const r of fresh) {
        this.foldRecord(have, r);
        have.image.seqs.add(r.seq);
      }
    } catch {
      return false;
    }
    have.state.setCursor(img.head.seq, img.head.logEpoch, img.head.manifestVersion);
    have.kind = img.head.kind;
    if (img.head.lifecycle !== KeyspaceLifecycle.UNSPECIFIED) have.lifecycle = img.head.lifecycle as KeyspaceLifecycle;
    have.successors = [...img.head.successors];
    have.watermark = img.head.watermark;
    have.lastRecordHash = img.head.lastRecordHash;
    have.lastRecordSeq = img.head.lastRecordSeq;
    have.lastSyncMs = img.head.lastSyncMs;
    have.invalidate();
    return true;
  }

  /** Whether the image is this principal's: the partition of a shared device holds the caches of the principals that used it, one at a time (14 [CLI-59]). */
  protected isMyImage(img: { head: { author?: { issuer: string; sub: string } | undefined } }): boolean {
    const a = img.head.author;
    return a === undefined || (a.issuer === this.author.issuer && a.sub === this.author.sub);
  }

  /** The followed keyspaces that the flusher kept with the stored state ([CLI-70]). */
  protected followedFromMeta(v: string | number | bigint | undefined): Map<string, { interest: string[]; lastSyncMs: number }> {
    const out = new Map<string, { interest: string[]; lastSyncMs: number }>();
    if (typeof v !== "string") return out;
    try {
      const o = JSON.parse(v) as Record<string, { interest?: string[]; lastSyncMs?: number }>;
      for (const [id, x] of Object.entries(o)) out.set(id, { interest: Array.isArray(x.interest) ? [...x.interest] : [], lastSyncMs: typeof x.lastSyncMs === "number" ? x.lastSyncMs : 0 });
    } catch {
      // not what this engine wrote: no keyspace is followed because of it
    }
    return out;
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Leaving

  /**
   * Gives up the flusher role gracefully (14 [CLI-61]): offers it to the tabs of the engine, and, when a visible one takes it, ends the work
   * of the flusher (the session, the pump, the timers and the tasks in flight, as `stop` does), persists what the next flusher reads, releases the
   * lock, and joins the queue for it again as a follower. Resolves false, and keeps the role, when no tab answers the offer (14 [CLI-61]:
   * "when there is one"), when the tab is visible again, or when it is not the flusher.
   */
  handOver(): Promise<boolean> {
    const host = this.tabHost;
    if (this.role !== "flusher" || this.stopped || host === undefined || this.channel === undefined) return Promise.resolve(false);
    if (this.round !== undefined) return this.round.done;
    const round: HandoverRound = { id: `${host.tabId}.${++this.handoffSeq}`, cancelled: false, committed: false, answer: () => undefined, done: Promise.resolve(false) };
    this.round = round;
    round.done = this.runHandover(round, host).finally(() => {
      if (this.round === round) this.round = undefined;
    });
    return round.done;
  }

  /** The tab became visible again: an offer that no one has answered yet is withdrawn. */
  private cancelOffer(): void {
    const r = this.round;
    if (r === undefined || r.committed) return;
    r.cancelled = true;
    this.round = undefined;
    r.answer(undefined);
  }

  private async runHandover(round: HandoverRound, host: TabHost): Promise<boolean> {
    const target = await new Promise<string | undefined>((resolve) => {
      const cancel = this.clock.setTimer(HANDOVER_PROBE_MS, () => resolve(undefined));
      round.answer = (tab) => {
        cancel();
        resolve(tab);
      };
      this.send({ t: "offer", from: host.tabId, id: round.id });
    });
    if (target === undefined || round.cancelled || this.role !== "flusher" || this.stopped || this.visibleNow()) return false;
    round.committed = true;
    // the tabs that are ahead of the chosen one in the lock's queue pass the lock on to it, and so does this one when it queues again
    this.send({ t: "handoff", from: host.tabId, to: target, id: round.id });
    this.handoff = { to: target, id: round.id, untilMs: this.clock.now() + HANDOFF_VALID_MS };
    // 14 [CLI-61], 10 [AUTH-25]: the work of this flusher ends before the lock goes, so that two flushers never drain one outbox
    await this.run(async () => {
      for (const f of this.follows.values()) {
        if (f.onSession) f.lastSyncMs = this.serverNow();
        await this.persistHead(f, 0, true);
      }
      await this.persistFollowed();
    }).catch(() => undefined);
    this.stopped = true; // no pump, no timer, no new call runs from here
    this.endRunning();
    this.cancelTimers();
    await this.settle().catch(() => undefined);
    if (this.role !== "flusher") return false; // the engine was stopped meanwhile: `stop` has the lock
    // from here this tab is a follower: it writes nothing, and its runtime is forgotten, so that a later promotion starts from the stored state
    this.role = "follower";
    this.stopped = false;
    this.resetRuntime();
    this.runtimeMirror = undefined;
    // 03 [ID-35], 14 [CLI-63]: the instance that sent under this client_id is over, and a follower is none: the next flusher mints its own, and so does this tab if it takes the role back
    this.clientId = "";
    this.emit({ type: "role", role: "follower", cause: "HANDED_OVER" });
    const lease = this.lease;
    this.lease = undefined;
    if (lease !== undefined) await lease.release();
    await this.run(() => this.refreshMirrors()).catch((e: unknown) => this.noteInternalError(e));
    this.queueForLock();
    return true;
  }

  protected resetRuntime(): void {
    super.resetRuntime();
    this.stateQueued = false;
    this.stopWatching(); // a follower that watches no tab: the next flusher watches them
  }

  /** Releases what the tab holds: the lock, the channel, the visibility watch. */
  protected async leave(cause: "STOPPED"): Promise<void> {
    this.offVisibility?.();
    this.offVisibility = undefined;
    this.offPageHide?.();
    this.offPageHide = undefined;
    this.stopWatching();
    this.cancelOffer();
    this.lockAbort?.abort();
    await this.failWaits("the engine stopped", true);
    this.role = "stopped";
    // the event reaches the app of this tab and, while the channel is open, the followers: they learn that a flusher left
    this.emit({ type: "role", role: "stopped", cause });
    if (this.channel !== undefined && this.tabHost !== undefined) this.send({ t: "gone", from: this.tabHost.tabId, inc: this.incarnation });
    this.offChannel?.();
    this.channel?.close();
    this.channel = undefined;
    if (this.lease !== undefined) {
      await this.lease.release(); // the next tab in line is granted the lock, and becomes the flusher
      this.lease = undefined;
    }
    if (this.tabLease !== undefined) {
      await this.tabLease.release(); // the flusher is granted the lock of this tab: it is gone
      this.tabLease = undefined;
    }
  }

  /** Abandons the engine as a crashed process would: nothing is persisted and no lock is released (the platform frees it, `MemoryTab.crash`). A test's tool. */
  kill(): void {
    this.stopped = true;
    this.role = "stopped";
    this.offVisibility?.();
    this.offPageHide?.();
    this.stopWatching();
    this.lockAbort?.abort();
    void this.failWaits("the engine was killed", false);
    this.endRunning();
    this.cancelTimers();
    for (const f of this.follows.values()) f.crdt.destroy();
  }
}
