// The in-memory implementation of the tab interfaces of tabs.ts, and the test double that simulates two tabs (or two processes) on one
// device (14 §14.8): a `MemoryDevice` is one browser profile or one machine, with its storage, its lock manager and its broadcast
// channels; `device.newTab()` gives each tab its `TabHost`. A tab can be made to crash (`crash()`: its locks are freed and its channels
// close, with no cleanup by its engine, as a killed tab leaves them), to be hidden, or to have no reliable lock.
//
// What it does as the real platform does:
//  - a lock is exclusive, granted to the waiting tabs in the order that they asked, and granted later than the release (Web Locks
//    calls the waiter back, it never runs inside the release);
//  - a message reaches every other open tab of the device, later than it was posted, as a copy (a structured clone);
//  - two opens of one partition are two views of one store (each reads its own copy, as two readers of IndexedDB do).

import { MemoryStorage, type MemoryStorageOptions } from "./memory-storage.js";
import type { OutboxStorage } from "./storage.js";
import { partitionId, sameKey, type CrossTabLocks, type LockLease, type PartitionKey, type StorageProvider, type TabChannel, type TabHost } from "./tabs.js";

/** A `StorageProvider` of `MemoryStorage`s, one per partition. */
export class MemoryStorageProvider implements StorageProvider {
  private readonly stores = new Map<string, { key: PartitionKey; storage: MemoryStorage }>();

  constructor(private readonly options: MemoryStorageOptions = {}) {}

  open(key: PartitionKey): Promise<OutboxStorage> {
    return Promise.resolve(this.storageOf(key));
  }

  /** The `MemoryStorage` of a partition, created when it does not exist. */
  storageOf(key: PartitionKey): MemoryStorage {
    const id = partitionId(key);
    let s = this.stores.get(id);
    if (s === undefined) {
      s = { key: { ...key }, storage: new MemoryStorage(this.options) };
      this.stores.set(id, s);
    }
    return s.storage;
  }

  list(origin: string, appId: string): Promise<PartitionKey[]> {
    return Promise.resolve([...this.stores.values()].map((s) => s.key).filter((k) => k.origin === origin && k.appId === appId));
  }

  destroy(key: PartitionKey): Promise<void> {
    for (const [id, s] of [...this.stores]) if (sameKey(s.key, key)) this.stores.delete(id);
    return Promise.resolve();
  }
}

class MemoryLease implements LockLease {
  held = true;

  constructor(
    private readonly mgr: LockManager,
    readonly name: string,
    readonly owner: string,
  ) {}

  release(): Promise<void> {
    if (this.held) {
      this.held = false;
      this.mgr.free(this);
    }
    return Promise.resolve();
  }

  /** The platform took the lock back: the holder is gone. */
  revoke(): void {
    this.held = false;
  }
}

interface Waiter {
  owner: string;
  resolve: (l: LockLease) => void;
  reject: (e: unknown) => void;
  cleanup: () => void;
}

class LockManager {
  private readonly holders = new Map<string, MemoryLease>();
  private readonly queues = new Map<string, Waiter[]>();

  tryAcquire(name: string, owner: string): LockLease | undefined {
    if (this.holders.has(name)) return undefined;
    const l = new MemoryLease(this, name, owner);
    this.holders.set(name, l);
    return l;
  }

  acquire(name: string, owner: string, signal: AbortSignal | undefined): Promise<LockLease> {
    const got = this.tryAcquire(name, owner);
    if (got !== undefined) return Promise.resolve(got);
    return new Promise<LockLease>((resolve, reject) => {
      const q = this.queues.get(name) ?? [];
      this.queues.set(name, q);
      const abortErr = (): Error => Object.assign(new Error("the lock request was aborted"), { name: "AbortError" });
      if (signal?.aborted === true) return reject(abortErr());
      const w: Waiter = {
        owner,
        resolve,
        reject,
        cleanup: () => signal?.removeEventListener("abort", onAbort),
      };
      const onAbort = (): void => {
        const i = q.indexOf(w);
        if (i >= 0) q.splice(i, 1);
        reject(abortErr());
      };
      signal?.addEventListener("abort", onAbort, { once: true });
      q.push(w);
    });
  }

  free(lease: MemoryLease): void {
    if (this.holders.get(lease.name) !== lease) return;
    this.holders.delete(lease.name);
    // a waiter is called back later, never inside the release (Web Locks)
    queueMicrotask(() => this.grant(lease.name));
  }

  private grant(name: string): void {
    if (this.holders.has(name)) return;
    const q = this.queues.get(name);
    const w = q?.shift();
    if (w === undefined) return;
    w.cleanup();
    const l = new MemoryLease(this, name, w.owner);
    this.holders.set(name, l);
    w.resolve(l);
  }

  /** A tab went away: its locks are freed and its requests dropped. */
  dropOwner(owner: string): void {
    for (const q of this.queues.values()) {
      for (const w of q.filter((x) => x.owner === owner)) {
        q.splice(q.indexOf(w), 1);
        w.cleanup();
        w.reject(Object.assign(new Error("the tab went away"), { name: "AbortError" }));
      }
    }
    for (const l of [...this.holders.values()]) {
      if (l.owner !== owner) continue;
      l.revoke();
      this.holders.delete(l.name);
      queueMicrotask(() => this.grant(l.name));
    }
  }

  /** The tab that holds a lock, for a test. */
  holder(name: string): string | undefined {
    return this.holders.get(name)?.owner;
  }

  /** The number of tabs that wait for a lock, for a test. */
  waiting(name: string): number {
    return this.queues.get(name)?.length ?? 0;
  }
}

class MemoryChannel implements TabChannel {
  private readonly listeners = new Set<(m: unknown) => void>();
  closed = false;

  constructor(
    private readonly device: MemoryDevice,
    readonly name: string,
    readonly tabId: string,
  ) {}

  post(message: unknown): void {
    if (this.closed) return;
    this.device.deliver(this, message);
  }

  onMessage(fn: (message: unknown) => void): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  receive(message: unknown): void {
    if (this.closed) return;
    for (const l of [...this.listeners]) {
      try {
        l(message);
      } catch {
        // a listener never stops the channel
      }
    }
  }

  close(): void {
    this.closed = true;
    this.listeners.clear();
    this.device.forget(this);
  }
}

export interface MemoryTabOptions {
  tabId?: string;
  /** False: the tab's platform has no reliable cross-tab lock (14 [CLI-61]). Default true. */
  reliableLocks?: boolean;
  /** Default true. */
  visible?: boolean;
}

/** One tab of a `MemoryDevice`. */
export class MemoryTab implements TabHost {
  readonly tabId: string;
  readonly locks: CrossTabLocks;
  readonly storage: StorageProvider;
  private visible_: boolean;
  private readonly visibilityListeners = new Set<(v: boolean) => void>();
  private readonly pageListeners = new Set<(e: "hide" | "show") => void>();
  private readonly channels = new Set<MemoryChannel>();
  crashed = false;

  constructor(
    private readonly device: MemoryDevice,
    opts: Required<Pick<MemoryTabOptions, "tabId" | "reliableLocks" | "visible">>,
  ) {
    this.tabId = opts.tabId;
    this.visible_ = opts.visible;
    this.storage = device.storage;
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const tab = this;
    this.locks = {
      reliable: opts.reliableLocks,
      tryAcquire: (name) => Promise.resolve(tab.crashed ? undefined : device.lockManager.tryAcquire(name, tab.tabId)),
      acquire: (name, signal) => (tab.crashed ? Promise.reject(Object.assign(new Error("the tab crashed"), { name: "AbortError" })) : device.lockManager.acquire(name, tab.tabId, signal)),
    };
  }

  channel(name: string): TabChannel {
    const c = new MemoryChannel(this.device, name, this.tabId);
    this.channels.add(c);
    this.device.register(c);
    return c;
  }

  visible(): boolean {
    return this.visible_;
  }

  onVisibility(fn: (visible: boolean) => void): () => void {
    this.visibilityListeners.add(fn);
    return () => this.visibilityListeners.delete(fn);
  }

  onPageHide(fn: (event: "hide" | "show") => void): () => void {
    this.pageListeners.add(fn);
    return () => this.pageListeners.delete(fn);
  }

  /** The page is going away (`pagehide`), or comes back from the back-forward cache (`pageshow`): a test's tool. */
  pageEvent(event: "hide" | "show"): void {
    for (const l of [...this.pageListeners]) l(event);
  }

  /** The tab becomes visible or hidden. */
  setVisible(v: boolean): void {
    if (this.visible_ === v) return;
    this.visible_ = v;
    for (const l of [...this.visibilityListeners]) l(v);
  }

  /** The tab is killed: its locks are freed, its requests dropped and its channels closed, and nothing else happens (no cleanup by its engine). */
  crash(): void {
    this.crashed = true;
    this.device.lockManager.dropOwner(this.tabId);
    for (const c of [...this.channels]) c.close();
    this.channels.clear();
  }
}

/** One device: one browser profile or one machine, shared by its tabs. */
export class MemoryDevice {
  readonly storage: MemoryStorageProvider;
  readonly lockManager = new LockManager();
  private readonly channels = new Set<MemoryChannel>();
  private nextTab = 1;
  /** Every message that was posted, in order, for a test: (channel, from tab, message). */
  readonly posted: { channel: string; from: string; message: unknown }[] = [];

  constructor(storageOptions: MemoryStorageOptions = {}) {
    this.storage = new MemoryStorageProvider(storageOptions);
  }

  newTab(opts: MemoryTabOptions = {}): MemoryTab {
    return new MemoryTab(this, { tabId: opts.tabId ?? `tab${this.nextTab++}`, reliableLocks: opts.reliableLocks ?? true, visible: opts.visible ?? true });
  }

  register(c: MemoryChannel): void {
    this.channels.add(c);
  }

  forget(c: MemoryChannel): void {
    this.channels.delete(c);
  }

  /** Delivers a message to every other open channel of the same name, later, as a copy. */
  deliver(from: MemoryChannel, message: unknown): void {
    this.posted.push({ channel: from.name, from: from.tabId, message });
    for (const c of [...this.channels]) {
      if (c === from || c.name !== from.name || c.closed) continue;
      const copy = structuredClone(message);
      queueMicrotask(() => c.receive(copy));
    }
  }
}
