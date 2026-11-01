// The tab host of a browser (14 §14.8), written to the standard APIs: the flusher lock is a Web Lock (`navigator.locks`), the tabs talk over a
// `BroadcastChannel`, the storage is IndexedDB, the tab's id is kept in `sessionStorage` (so that a reload is the same tab, 14 [CLI-61]) and its
// visibility is `document.visibilityState`. Each API is taken from `env`, which defaults to the global scope, so that a test gives it doubles and
// a platform that lacks one is told so: without Web Locks `reliable` is false and each tab is an engine of its own; without IndexedDB there is no
// storage, and `createWebHost` throws instead of falling back to memory silently (14 [CLI-21]).
//
// Untested in a browser (nothing here runs in Node but the lock and the channel, against Node's own `navigator.locks` and `BroadcastChannel`).

import type { MemoryStorageOptions } from "../engine/memory-storage.js";
import type { CrossTabLocks, StorageProvider, TabChannel, TabHost } from "../engine/tabs.js";
import { BroadcastTabChannel, type BroadcastChannelConstructor } from "./channel.js";
import { IndexedDbStorageProvider, type IDBFactoryLike, type StorageManagerLike } from "./idb-storage.js";
import { WebLocks, type LockManagerLike } from "./web-locks.js";

export interface WebEnv {
  locks?: LockManagerLike;
  BroadcastChannel?: BroadcastChannelConstructor;
  indexedDB?: IDBFactoryLike;
  storageManager?: StorageManagerLike;
  sessionStorage?: { getItem(k: string): string | null; setItem(k: string, v: string): void };
  document?: {
    visibilityState?: string;
    addEventListener(type: "visibilitychange", fn: () => void): void;
    removeEventListener(type: "visibilitychange", fn: () => void): void;
  };
  randomUUID?: () => string;
}

export interface WebHostOptions extends MemoryStorageOptions {
  env?: WebEnv;
  /** The id of the tab; default the one that `sessionStorage` keeps for this tab, or a new one. */
  tabId?: string;
}

/** What the global scope gives. */
export function globalWebEnv(): WebEnv {
  const g = globalThis as unknown as {
    navigator?: { locks?: LockManagerLike; storage?: StorageManagerLike };
    BroadcastChannel?: BroadcastChannelConstructor;
    indexedDB?: IDBFactoryLike;
    sessionStorage?: WebEnv["sessionStorage"];
    document?: WebEnv["document"];
    crypto?: { randomUUID?: () => string };
  };
  let session: WebEnv["sessionStorage"];
  try {
    session = g.sessionStorage; // reading it throws when storage is blocked
  } catch {
    session = undefined;
  }
  return {
    ...(g.navigator?.locks !== undefined ? { locks: g.navigator.locks } : {}),
    ...(g.BroadcastChannel !== undefined ? { BroadcastChannel: g.BroadcastChannel } : {}),
    ...(g.indexedDB !== undefined ? { indexedDB: g.indexedDB } : {}),
    ...(g.navigator?.storage !== undefined ? { storageManager: g.navigator.storage } : {}),
    ...(session !== undefined ? { sessionStorage: session } : {}),
    ...(g.document !== undefined ? { document: g.document } : {}),
    ...(g.crypto?.randomUUID !== undefined ? { randomUUID: () => (g.crypto as { randomUUID: () => string }).randomUUID() } : {}),
  };
}

const TAB_KEY = "abpc.tabId";

export class WebTab implements TabHost {
  readonly tabId: string;
  readonly locks: CrossTabLocks;
  readonly storage: StorageProvider;
  private readonly channels = new Set<TabChannel>();

  constructor(
    private readonly env: WebEnv,
    storage: StorageProvider,
    tabId: string,
  ) {
    this.tabId = tabId;
    this.storage = storage;
    this.locks = new WebLocks(env.locks);
  }

  channel(name: string): TabChannel {
    const ctor = this.env.BroadcastChannel;
    if (ctor === undefined) throw new Error("this platform has no BroadcastChannel");
    const c = new BroadcastTabChannel(ctor, name);
    this.channels.add(c);
    return c;
  }

  visible(): boolean {
    return this.env.document?.visibilityState !== "hidden";
  }

  onVisibility(fn: (visible: boolean) => void): () => void {
    const d = this.env.document;
    if (d === undefined) return () => undefined;
    const h = (): void => fn(d.visibilityState !== "hidden");
    d.addEventListener("visibilitychange", h);
    return () => d.removeEventListener("visibilitychange", h);
  }
}

export function createWebHost(opts: WebHostOptions = {}): WebTab {
  const env = opts.env ?? globalWebEnv();
  if (env.indexedDB === undefined) throw new Error("this platform has no IndexedDB: the outbox would be memory only (14 [CLI-21]); give the engine a storage explicitly if that is meant");
  const { env: _e, tabId: _t, ...bounds } = opts;
  let tabId = opts.tabId;
  if (tabId === undefined) {
    try {
      tabId = env.sessionStorage?.getItem(TAB_KEY) ?? undefined;
    } catch {
      // storage blocked
    }
  }
  if (tabId === undefined) {
    tabId = env.randomUUID?.() ?? `tab-${Math.random().toString(36).slice(2)}${Date.now().toString(36)}`;
    try {
      env.sessionStorage?.setItem(TAB_KEY, tabId);
    } catch {
      // storage blocked: the id lasts for as long as this tab object
    }
  }
  return new WebTab(env, new IndexedDbStorageProvider(env.indexedDB, bounds, env.storageManager), tabId);
}
