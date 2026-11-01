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
//    re-stamps every action without a definitive result before it sends under it (the rehydration of `engine.ts`).
//  - What a follower reads is the flusher's durable state: the outbox, the stores and the persisted replica are re-read from the
//    storage when the flusher says that they changed (`state` messages); its events are the flusher's, relayed (`event`); the
//    connection and the holds are the flusher's runtime (`state`). A follower that reads the replica folds the pages and the tail
//    again (`restoreImage`): a later change could carry the records of the broadcast instead, which is an optimization.
//  - [CLI-61] "where the platform throttles a hidden tab so that it cannot [keep the session and the presence entry alive], the flusher
//    MUST hand its role over to a visible tab of the engine, when there is one": `handOverWhenHidden` does it from the tab's visibility.
//
// SPEC-QUESTION (14 [CLI-61]): the text does not say where a follower's `submit` goes. Reading A (taken): to the flusher, which authors,
// stores and answers it: [CLI-21] asks that the call resolve only when the action is in a durable outbox, which holds, and the flusher
// is the one writer of the outbox, so the dependencies of [CLI-30] are computed from one view. Reading B: the follower appends to the
// shared storage itself and the flusher re-reads; two tabs would then compute the dependencies of their actions from two views of the
// outbox, and each would need a cross-tab lock around authoring. A follower whose `submit` finds no flusher answering is refused with
// the typed local error `NO_FLUSHER` (nothing was accepted); if the flusher went away after it stored the action and before it answered,
// the action is in the outbox all the same, which the app can read.

import { LocalRefusal, type LocalRefusalCode } from "./errors.js";
import type { EngineConfig } from "./core.js";
import { Follow } from "./follow.js";
import { RevocationLayer } from "./revocation.js";
import type { LockLease, PartitionKey, StorageProvider, TabChannel, TabHost } from "./tabs.js";
import { lockName } from "./tabs.js";
import type { EngineEvent, HoldInfo } from "./types.js";

/** How long a follower waits for the flusher to answer a command, on the engine's clock. */
export const COMMAND_TIMEOUT_MS = 5_000;

/** The flusher's runtime, which a follower mirrors: what the sync status reports besides the durable stores. */
export interface RuntimeSnapshot {
  connection: "live" | "connecting" | "offline" | "stopped";
  holds: HoldInfo[];
  suspended: boolean;
  fullStores: string[];
  skippedChanges: number;
  loadsWithSkipped: number;
  anomalies: Record<string, number>;
  stopped: Record<string, string>;
}

type TabMessage =
  | { t: "hello"; from: string; visible: boolean; reply: boolean }
  | { t: "visible"; from: string; visible: boolean }
  | { t: "gone"; from: string }
  | { t: "state"; from: string; rt: RuntimeSnapshot }
  | { t: "event"; from: string; event: EngineEvent }
  | { t: "cmd"; from: string; id: number; name: string; args: unknown[] }
  | { t: "reply"; from: string; to: string; id: number; ok: true; value: unknown }
  | { t: "reply"; from: string; to: string; id: number; ok: false; error: { name: string; code?: string; detail: string; message: string } };

interface Wait {
  resolve: (v: unknown) => void;
  reject: (e: unknown) => void;
  cancel: () => void;
}

export abstract class TabLayer extends RevocationLayer {
  protected readonly tabHost: TabHost | undefined;
  protected partition!: PartitionKey;
  protected provider: StorageProvider | undefined;
  protected lease: LockLease | undefined;
  /** The flusher's runtime, as a follower last heard it. */
  protected runtimeMirror: RuntimeSnapshot | undefined;
  /** `start()` was called: a flusher runs its session; a follower that is promoted does too. */
  protected startRequested = false;
  private channel: TabChannel | undefined;
  private offChannel: (() => void) | undefined;
  private offVisibility: (() => void) | undefined;
  private lockAbort: AbortController | undefined;
  private readonly peers = new Map<string, boolean>();
  private readonly waits = new Map<number, Wait>();
  private cmdSeq = 0;
  private refreshing = false;
  private refreshAgain = false;
  private stateQueued = false;
  protected handOverWhenHidden = false;
  /** The commands that a flusher runs for a follower, by name. */
  protected abstract commands(): Record<string, (args: unknown[]) => Promise<unknown>>;
  protected abstract becomeFlusher(): Promise<void>;

  protected constructor(cfg: EngineConfig) {
    super(cfg);
    this.tabHost = cfg.tab;
    this.provider = cfg.store ?? cfg.tab?.storage;
    this.handOverWhenHidden = cfg.handOverWhenHidden ?? false;
    this.setHorizonWarning(cfg.horizonWarning);
    this.copyPolicy = cfg.copyPolicy ?? "keep";
    this.persistReplica = cfg.persistReplica !== false;
    if (cfg.replicaTailLimit !== undefined) this.replicaTailLimit = cfg.replicaTailLimit;
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
    const mode = cfg.mode ?? "personal";
    const reliable = this.tabHost === undefined || this.tabHost.locks.reliable;
    this.partition = {
      origin: cfg.origin ?? "local",
      appId: cfg.appId,
      partition: mode === "shared-device" ? this.author.deviceId : `${this.author.issuer}|${this.author.sub}`,
      ...(this.tabHost !== undefined && !reliable ? { tab: this.tabHost.tabId } : {}),
    };
    if (cfg.storage === undefined && this.provider !== undefined) this.storage = await this.provider.open(this.partition);
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
    this.openChannel();
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
        this.lease = lease;
        // the previous flusher is gone: this tab is the next in line (14 [CLI-61], [CLI-63]): it reads the durable state, mints a `client_id`,
        // and re-stamps what has no definitive result before it sends
        void this.run(async () => {
          this.failWaits("the flusher went away");
          await this.becomeFlusher();
          this.emit({ type: "role", role: "flusher", cause: "ELECTED", clientId: this.clientId });
          this.announce();
          this.queueState();
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
    this.send({ t: "hello", from: this.tabHost.tabId, visible: this.visibleNow(), reply: false });
  }

  private watchVisibility(): void {
    const host = this.tabHost;
    if (host?.onVisibility === undefined) return;
    this.offVisibility = host.onVisibility((visible) => {
      this.send({ t: "visible", from: host.tabId, visible });
      // 14 [CLI-61]: a hidden flusher hands its role over to a visible tab, when there is one
      if (!visible && this.handOverWhenHidden && this.role === "flusher" && [...this.peers.values()].some((v) => v)) void this.handOver();
    });
  }

  private onMessage(m: TabMessage): void {
    switch (m.t) {
      case "hello":
        this.peers.set(m.from, m.visible);
        if (!m.reply && this.tabHost !== undefined) this.send({ t: "hello", from: this.tabHost.tabId, visible: this.visibleNow(), reply: true });
        return;
      case "visible":
        this.peers.set(m.from, m.visible);
        return;
      case "gone":
        this.peers.delete(m.from);
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
      case "cmd":
        if (this.role === "flusher") this.runCommand(m);
        return;
      case "reply": {
        if (m.to !== this.tabHost?.tabId) return;
        const w = this.waits.get(m.id);
        if (w === undefined) return;
        this.waits.delete(m.id);
        w.cancel();
        if (m.ok) w.resolve(m.value);
        else w.reject(m.error.name === "LocalRefusal" ? new LocalRefusal(m.error.code as LocalRefusalCode, m.error.detail) : new Error(m.error.message));
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

  protected touchPersisted(): void {
    this.queueState();
  }

  protected onRuntimeChanged(): void {
    this.queueState();
  }

  protected runtimeSnapshot(): RuntimeSnapshot {
    const anomalies: Record<string, number> = {};
    const stopped: Record<string, string> = {};
    let loadsWithSkipped = 0;
    for (const f of this.follows.values()) {
      if (f.anomalies > 0) anomalies[f.id] = f.anomalies;
      if (f.stopped !== undefined) stopped[f.id] = f.stopped;
      loadsWithSkipped += f.loadsWithSkipped;
    }
    return {
      connection: this.connection(),
      holds: this.holds.info(),
      suspended: this.holds.all().some((h) => h.kind === "APP_SUSPENDED"),
      fullStores: this.storage.full(),
      skippedChanges: this.skippedChanges,
      loadsWithSkipped,
      anomalies,
      stopped,
    };
  }

  /** Tells the followers that the durable state or the runtime changed; coalesced to one message for a burst of changes. */
  protected queueState(): void {
    if (this.channel === undefined || this.role !== "flusher" || this.stateQueued || this.stopped) return;
    this.stateQueued = true;
    queueMicrotask(() => {
      this.stateQueued = false;
      if (this.role !== "flusher" || this.stopped || this.tabHost === undefined) return;
      this.send({ t: "state", from: this.tabHost.tabId, rt: this.runtimeSnapshot() });
    });
  }

  private runCommand(m: Extract<TabMessage, { t: "cmd" }>): void {
    const host = this.tabHost;
    if (host === undefined) return;
    const fn = this.commands()[m.name];
    const reply = (r: { ok: true; value: unknown } | { ok: false; error: { name: string; code?: string; detail: string; message: string } }): void => {
      this.send({ t: "reply", from: host.tabId, to: m.from, id: m.id, ...r } as TabMessage);
    };
    if (fn === undefined) {
      reply({ ok: false, error: { name: "Error", detail: m.name, message: `unknown command ${m.name}` } });
      return;
    }
    this.detach(
      fn(m.args).then(
        (value) => reply({ ok: true, value }),
        (e: unknown) => {
          if (e instanceof LocalRefusal) reply({ ok: false, error: { name: "LocalRefusal", code: e.code, detail: e.message.replace(/^refused locally \([A-Z_]+\): /, ""), message: e.message } });
          else reply({ ok: false, error: { name: "Error", detail: String(e), message: e instanceof Error ? e.message : String(e) } });
        },
      ),
    );
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // The follower's side

  /**
   * Runs an action of the app where it belongs: on the flusher itself, or, for a follower, on the flusher through the channel
   * (see the SPEC-QUESTION above). A follower that gets no answer within `COMMAND_TIMEOUT_MS` refuses it with `NO_FLUSHER`.
   */
  protected viaFlusher<T>(name: string, args: unknown[], local: () => Promise<T>): Promise<T> {
    if (this.role !== "follower") return local();
    const host = this.tabHost;
    if (host === undefined || this.channel === undefined) return Promise.reject(new LocalRefusal("NO_FLUSHER", "this engine has no flusher to carry the call"));
    return new Promise<T>((resolve, reject) => {
      const id = ++this.cmdSeq;
      const cancel = this.clock.setTimer(COMMAND_TIMEOUT_MS, () => {
        if (this.waits.delete(id)) reject(new LocalRefusal("NO_FLUSHER", `no flusher answered ${name}`));
      });
      this.waits.set(id, { resolve: resolve as (v: unknown) => void, reject, cancel });
      this.send({ t: "cmd", from: host.tabId, id, name, args });
    });
  }

  private failWaits(why: string): void {
    for (const [id, w] of [...this.waits]) {
      this.waits.delete(id);
      w.cancel();
      w.reject(new LocalRefusal("NO_FLUSHER", why));
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
    const s = await this.loadStored();
    const ids = new Set<string>();
    for (const img of s.replicas) {
      ids.add(img.keyspaceId);
      const have = this.follows.get(img.keyspaceId);
      const same =
        have !== undefined &&
        have.loaded &&
        have.state.seq === img.head.seq &&
        have.state.logEpoch === img.head.logEpoch &&
        have.image.baseSeq === img.base.checkpointSeq &&
        have.interest.length === img.head.interest.length &&
        have.interest.every((p, i) => p === img.head.interest[i]);
      if (same) {
        have.lastSyncMs = img.head.lastSyncMs;
        continue;
      }
      const f = await this.restoreImage(img);
      if (f === undefined) continue;
      have?.crdt.destroy();
      f.image.baseSeq = img.base.checkpointSeq;
      this.follows.set(f.id, f);
    }
    const followed = this.followedFromMeta(s.meta["followed"]);
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
    for (const f of this.follows.values()) f.stopped = stopped[f.id];
  }

  /** The followed keyspaces that the flusher kept with the stored state ([CLI-70]). */
  protected followedFromMeta(v: string | number | bigint | undefined): Map<string, { interest: string[] }> {
    const out = new Map<string, { interest: string[] }>();
    if (typeof v !== "string") return out;
    try {
      const o = JSON.parse(v) as Record<string, { interest?: string[] }>;
      for (const [id, x] of Object.entries(o)) out.set(id, { interest: Array.isArray(x.interest) ? [...x.interest] : [] });
    } catch {
      // not what this engine wrote: no keyspace is followed because of it
    }
    return out;
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Leaving

  /** Gives up the flusher role gracefully: the session ends, the state is persisted, the lock goes to the next tab in line (14 [CLI-61]). */
  async handOver(): Promise<void> {
    await this.leave("HANDED_OVER");
  }

  /** Releases what the tab holds: the lock, the channel, the visibility watch. */
  protected async leave(cause: "STOPPED" | "HANDED_OVER"): Promise<void> {
    const wasFlusher = this.role === "flusher";
    this.offVisibility?.();
    this.offVisibility = undefined;
    this.lockAbort?.abort();
    this.failWaits("the engine stopped");
    this.role = "stopped";
    // the event reaches the app of this tab and, while the channel is open, the followers: they learn that a flusher left
    this.emit({ type: "role", role: "stopped", cause });
    if (this.channel !== undefined && this.tabHost !== undefined) this.send({ t: "gone", from: this.tabHost.tabId });
    this.offChannel?.();
    this.channel?.close();
    this.channel = undefined;
    if (this.lease !== undefined) {
      await this.lease.release(); // the next tab in line is granted the lock, and becomes the flusher
      this.lease = undefined;
    }
    void wasFlusher;
  }

  /** Abandons the engine as a crashed process would: nothing is persisted and no lock is released (the platform frees it, `MemoryTab.crash`). A test's tool. */
  kill(): void {
    this.stopped = true;
    this.role = "stopped";
    this.offVisibility?.();
    this.lockAbort?.abort();
    this.failWaits("the engine was killed");
    this.endRunning();
    this.cancelTimers();
    for (const f of this.follows.values()) f.crdt.destroy();
  }
}
