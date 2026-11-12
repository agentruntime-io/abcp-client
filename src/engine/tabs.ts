// Principals, tabs and devices (14 §14.8): the platform pieces that an engine needs to be one engine across the tabs of a device.
//
//  - [CLI-59], [CLI-74]: local storage is keyed by server origin, `app_id`, partition (the principal's subject, or in shared-device
//    mode the device) and `keyspace_id`. A `StorageProvider` opens the storage of one partition by its `PartitionKey`;
//  - [CLI-61]: the tabs and windows of one engine on one device share one outbox through a single flusher, elected by a cross-tab
//    lock. `CrossTabLocks` is that lock (Web Locks in a browser: one exclusive lock per partition, granted to the waiting tabs in
//    order, and freed when its holder closes or crashes); `TabChannel` is the local broadcast (BroadcastChannel) by which the other
//    tabs read through the engine. Where no reliable cross-tab lock exists (`reliable` is false), each tab is an engine of its own,
//    with an outbox, a session and a `client_id` of its own.
//
// This file holds the interfaces. `tabs-memory.ts` has the in-memory implementation, which doubles as the test double that
// simulates two tabs (or processes) on one device. A browser implementation (Web Locks, BroadcastChannel, IndexedDB) and a Node one
// (a lock file, a directory of rows) come later (C5): they implement these interfaces.

import type { BlobStash } from "./blob-store.js";
import type { OutboxStorage } from "./storage.js";

/** What keys all local storage (14 [CLI-74]). */
export interface PartitionKey {
  /** The server origin. */
  origin: string;
  appId: string;
  /** The partition: the principal's subject (`issuer|sub`) in personal mode, the device's id in shared-device mode (14 [CLI-59]). */
  partition: string;
  /**
   * Set only where no reliable cross-tab lock exists (14 [CLI-61]): the tab keeps an outbox of its own, which no other tab drains.
   * 10 [AUTH-25] has such a tab use a `device_id` of its own as well.
   */
  tab?: string;
}

/** A string that names a partition, for a lock, a channel or a row prefix. */
export function partitionId(k: PartitionKey): string {
  const parts = [k.origin, k.appId, k.partition];
  if (k.tab !== undefined) parts.push(k.tab);
  return parts.join("\u0000");
}

/** The name of the lock and the channel of a partition. */
export function lockName(k: PartitionKey): string {
  return `abpc.flusher\u0000${k.origin}\u0000${k.appId}\u0000${k.partition}`;
}

/**
 * The name of the lock that one tab holds for as long as it lives (14 [CLI-61], 09 [READ-74]): the flusher asks for it, and is granted it when the platform frees it, which is when the tab closes or
 * crashes, so that the flusher learns that a follower tab is gone without a message of the tab. `incarnation` names one engine of the tab (a reload is the same tab id and another engine), so that
 * the lock of a page that has gone is never taken for the one that replaced it.
 */
export function tabLockName(k: PartitionKey, tabId: string, incarnation: string): string {
  return `abpc.tab\u0000${k.origin}\u0000${k.appId}\u0000${k.partition}\u0000${tabId}\u0000${incarnation}`;
}

export function sameKey(a: PartitionKey, b: PartitionKey): boolean {
  return a.origin === b.origin && a.appId === b.appId && a.partition === b.partition && a.tab === b.tab;
}

/** Opens the storage of a partition (14 [CLI-59], [CLI-74]). */
export interface StorageProvider {
  /** The storage of the partition, created when it does not exist. Two opens of one key are two views of one store. */
  open(key: PartitionKey): Promise<OutboxStorage>;
  /** The partitions that exist for an origin and an app, locked ones included (14 [CLI-59]: the app may offer to flush or discard them). */
  list(origin: string, appId: string): Promise<PartitionKey[]>;
  /** Removes the partition and everything in it (14 [CLI-65]: a sign-out in personal mode, a discard of a locked partition), the staged bytes of its blobs included. */
  destroy(key: PartitionKey): Promise<void>;
  /**
   * The stash of the bytes that the partition's actions name (09 [READ-94]), as durable as the partition's storage is: a restart finds the bytes of an action that was never uploaded
   * (14 [CLI-44]). The tabs of one partition share it, as they share its outbox (14 [CLI-61]). A provider that has none leaves the engine its in-memory default; the app's own stash
   * (`EngineConfig.blobs.stash`) goes before it.
   */
  blobStash?(key: PartitionKey): BlobStash;
}

/** The exclusive lock of one partition, as its holder has it. */
export interface LockLease {
  /** True until the lease is released or the platform took the lock back (the holder crashed or closed). */
  readonly held: boolean;
  /** Frees the lock; the next tab that waits for it is granted it. */
  release(): Promise<void>;
}

/**
 * A cross-tab exclusive lock by name (14 [CLI-61]). The platform grants it to one holder at a time and to the waiting ones in the
 * order that they asked; it frees the lock of a holder that goes away without releasing it (a closed or crashed tab).
 */
export interface CrossTabLocks {
  /** Whether the platform has a reliable cross-tab lock. Where it has not, each tab is an engine of its own (14 [CLI-61]). */
  readonly reliable: boolean;
  /** Takes the lock if it is free now; undefined when another holder has it. */
  tryAcquire(name: string): Promise<LockLease | undefined>;
  /** Waits in the queue for the lock; rejects with an error named `AbortError` when `signal` aborts before it is granted. */
  acquire(name: string, signal?: AbortSignal): Promise<LockLease>;
}

/** A local broadcast between the tabs of one device (a `BroadcastChannel`): a message reaches every other tab that has the channel open. */
export interface TabChannel {
  post(message: unknown): void;
  onMessage(fn: (message: unknown) => void): () => void;
  close(): void;
}

/** What a tab gets from its platform. */
export interface TabHost {
  /**
   * The id of this tab, for the messages between tabs. Where no reliable cross-tab lock exists (`locks.reliable` is false) it also names the
   * tab's outbox (`PartitionKey.tab`, 14 [CLI-61]): a host MUST give a tab the same id for as long as the tab is the same one, across a reload
   * (a browser keeps it in `sessionStorage`), or the outbox of the old id belongs to no engine and its actions are never sent or reported. The
   * engine cannot tell such an outbox from another live tab's, so it never adopts one: `Engine.partitions()` lists them (the keys with a `tab`
   * other than the engine's own), for the app to flush or discard once the host knows that the tab is gone.
   */
  readonly tabId: string;
  /**
   * Resolves when `tabId` is settled, for a host that has to ask the other tabs first whether a live tab already has the id (a browser tab that was
   * duplicated copies its `sessionStorage`, and with it its id: two live tabs would share one outbox, V19 N5). The engine waits for it before it reads `tabId`
   * (the name of the outbox, the sender of every message); a host whose id never changes leaves it out.
   */
  ready?(): Promise<void>;
  readonly locks: CrossTabLocks;
  readonly storage: StorageProvider;
  /** Opens a local broadcast channel by name. */
  channel(name: string): TabChannel;
  /** Whether this tab is visible. A platform throttles a hidden tab, so that its flusher may not keep a session alive (14 [CLI-61]). */
  visible?(): boolean;
  /**
   * The platform throttles a hidden tab so that its flusher cannot keep the session and presence alive (a browser's timer throttling): the flusher
   * then hands its role over to a visible tab by default (14 [CLI-61]), unless `EngineConfig.handOverWhenHidden` says otherwise.
   */
  readonly throttlesHidden?: boolean;
  /** Calls `fn` when this tab's visibility changes. */
  onVisibility?(fn: (visible: boolean) => void): () => void;
  /**
   * Calls `fn("hide")` when the page is going away (a browser's `pagehide`) and `fn("show")` when a page that was put away comes back (`pageshow` of a page restored from the back-forward cache).
   * It is a hint, sent at once, that the tab is closing: the lock that each tab holds (`tabLockName`) is what tells for sure, when the platform frees it. A host with neither leaves it out.
   */
  onPageHide?(fn: (event: "hide" | "show") => void): () => void;
}
