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

export function sameKey(a: PartitionKey, b: PartitionKey): boolean {
  return a.origin === b.origin && a.appId === b.appId && a.partition === b.partition && a.tab === b.tab;
}

/** Opens the storage of a partition (14 [CLI-59], [CLI-74]). */
export interface StorageProvider {
  /** The storage of the partition, created when it does not exist. Two opens of one key are two views of one store. */
  open(key: PartitionKey): Promise<OutboxStorage>;
  /** The partitions that exist for an origin and an app, locked ones included (14 [CLI-59]: the app may offer to flush or discard them). */
  list(origin: string, appId: string): Promise<PartitionKey[]>;
  /** Removes the partition and everything in it (14 [CLI-65]: a sign-out in personal mode, a discard of a locked partition). */
  destroy(key: PartitionKey): Promise<void>;
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
  readonly locks: CrossTabLocks;
  readonly storage: StorageProvider;
  /** Opens a local broadcast channel by name. */
  channel(name: string): TabChannel;
  /** Whether this tab is visible. A platform throttles a hidden tab, so that its flusher may not keep a session alive (14 [CLI-61]). */
  visible?(): boolean;
  /** Calls `fn` when this tab's visibility changes. */
  onVisibility?(fn: (visible: boolean) => void): () => void;
}
