// The tab host of a Node process (14 §14.8): one process is one tab. Locks are in the process (`InProcessLocks`: two engines of one process,
// two principals or two apps, each have their flusher lock), the channel is Node's `BroadcastChannel`, and the storage is in memory or in files
// under a directory (`FileStorageProvider`), where a second process that opens a partition is refused by the partition's `LOCK`.
//
// A `NodeDevice` is what tabs of one device share. A process that runs one engine needs only `createNodeHost()`; two tabs on one device (a test of
// the handover of the flusher, 14 [CLI-61]) are `device.newTab()` twice.

import { randomUUID } from "node:crypto";
import { MemoryStorageProvider } from "../engine/tabs-memory.js";
import type { MemoryStorageOptions } from "../engine/memory-storage.js";
import type { CrossTabLocks, StorageProvider, TabChannel, TabHost } from "../engine/tabs.js";
import { BroadcastTabChannel, type BroadcastChannelConstructor } from "./channel.js";
import { FileStorageProvider } from "./file-storage.js";
export { FileBlobStash } from "./file-blob-stash.js";
import { InProcessLocks } from "./locks.js";

export interface NodeHostOptions extends MemoryStorageOptions {
  /** The directory of the persisted partitions; without it the storage is in memory and the engine's status says so (14 [CLI-21]). */
  directory?: string;
  /** The id of the tab; default a random one (a process that restarts is a tab that came back, which is what a new engine needs). */
  tabId?: string;
}

export class NodeTab implements TabHost {
  readonly tabId: string;
  private readonly channels = new Set<TabChannel>();

  constructor(
    private readonly owner: NodeDevice,
    tabId: string,
  ) {
    this.tabId = tabId;
  }

  get locks(): CrossTabLocks {
    return this.owner.locks;
  }

  get storage(): StorageProvider {
    return this.owner.storage;
  }

  channel(name: string): TabChannel {
    const c = new BroadcastTabChannel(globalThis.BroadcastChannel as unknown as BroadcastChannelConstructor, name, { unref: true });
    this.channels.add(c);
    return c;
  }

  /** A process has no tab to hide. */
  visible(): boolean {
    return true;
  }

  /** The channels of the tab are closed (the engine closes its own when it stops). */
  close(): void {
    for (const c of this.channels) c.close();
    this.channels.clear();
  }
}

export class NodeDevice {
  readonly locks = new InProcessLocks();
  readonly storage: StorageProvider & { close?(): Promise<void> };

  constructor(opts: NodeHostOptions = {}) {
    const { directory, tabId: _tabId, ...bounds } = opts;
    this.storage = directory !== undefined ? new FileStorageProvider(directory, bounds) : new MemoryStorageProvider(bounds);
  }

  newTab(tabId?: string): NodeTab {
    return new NodeTab(this, tabId ?? randomUUID());
  }

  /** Closes the files of the device. */
  async close(): Promise<void> {
    await this.storage.close?.();
  }
}

/** The host of a process that runs its engine alone. */
export function createNodeHost(opts: NodeHostOptions = {}): NodeTab & { device: NodeDevice } {
  const device = new NodeDevice(opts);
  return Object.assign(device.newTab(opts.tabId), { device });
}
