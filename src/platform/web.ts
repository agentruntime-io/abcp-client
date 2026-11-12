// The tab host of a browser (14 §14.8), written to the standard APIs: the flusher lock is a Web Lock (`navigator.locks`), the tabs talk over a
// `BroadcastChannel`, the storage is IndexedDB, the tab's id is kept in `sessionStorage` (so that a reload is the same tab, 14 [CLI-61]) and its
// visibility is `document.visibilityState`. Each API is taken from `env`, which defaults to the global scope, so that a test gives it doubles and
// a platform that lacks one is told so: without Web Locks `reliable` is false and each tab is an engine of its own; without IndexedDB there is no
// storage, and `createWebHost` throws instead of falling back to memory silently (14 [CLI-21]).
//
// A duplicated tab copies `sessionStorage` (as does a window that a page opens), so two live tabs can find one id there (V19 N5). Where no reliable lock exists
// the id names the tab's outbox (14 [CLI-61]), and two tabs with one id would be two flushers of one outbox (10 [AUTH-25]). A page that finds its id in
// `sessionStorage` therefore asks the other pages of the origin, over a `BroadcastChannel`, whether a live tab holds it, and takes a fresh id when one says so
// (`ready` resolves when the id is settled, and the engine waits for it). A reload is the same tab: no live page answers for the id, and it is kept. A page that is
// frozen and cannot answer in time is not told apart: that is the limit of asking.
//
// The host's storage provider also keeps the stash of the bytes that the partition's actions name, in a database of its own (`idb-blob-stash.ts`), and the engine takes it as its default: an action
// that names a staged blob finds its bytes after a reload (09 [READ-94], 14 [CLI-44]), and a sign-out deletes them with the partition (14 [CLI-65]). An app's own stash goes before it.
//
// Untested in a browser (nothing here runs in Node but the lock and the channel, against Node's own `navigator.locks` and `BroadcastChannel`).

import type { MemoryStorageOptions } from "../engine/memory-storage.js";
import type { CrossTabLocks, StorageProvider, TabChannel, TabHost } from "../engine/tabs.js";
import { BroadcastTabChannel, type BroadcastChannelConstructor } from "./channel.js";
import { IndexedDbStorageProvider, type IDBFactoryLike, type KeyRangeFactory, type StorageManagerLike } from "./idb-storage.js";
import { WebLocks, type LockManagerLike } from "./web-locks.js";

export interface WebEnv {
  locks?: LockManagerLike;
  BroadcastChannel?: BroadcastChannelConstructor;
  indexedDB?: IDBFactoryLike;
  /** `IDBKeyRange`: the replica of one keyspace is read and purged by key range. */
  IDBKeyRange?: KeyRangeFactory;
  storageManager?: StorageManagerLike;
  sessionStorage?: { getItem(k: string): string | null; setItem(k: string, v: string): void };
  document?: {
    visibilityState?: string;
    addEventListener(type: "visibilitychange", fn: () => void): void;
    removeEventListener(type: "visibilitychange", fn: () => void): void;
  };
  /** The window: `pagehide` says that the page is going away, `pageshow` that one put away comes back (the back-forward cache). */
  page?: {
    addEventListener(type: "pagehide" | "pageshow", fn: (e: { persisted?: boolean }) => void): void;
    removeEventListener(type: "pagehide" | "pageshow", fn: (e: { persisted?: boolean }) => void): void;
  };
  randomUUID?: () => string;
}

export interface WebHostOptions extends MemoryStorageOptions {
  env?: WebEnv;
  /** How long a sign-out waits for another tab that keeps the database open, in real ms; default 10 s. */
  blockedWaitMs?: number;
  /** The id of the tab; default the one that `sessionStorage` keeps for this tab, or a new one. An id that is given is the app's: no other tab is asked about it. */
  tabId?: string;
  /**
   * How long a tab that found its id in `sessionStorage` waits for a live tab that holds it to say so, in real ms (a host has no engine clock); default 150.
   * A tab that mints its id, or is given one, waits for nothing.
   */
  claimWaitMs?: number;
}

/** What the global scope gives. */
export function globalWebEnv(): WebEnv {
  const g = globalThis as unknown as {
    navigator?: { locks?: LockManagerLike; storage?: StorageManagerLike };
    BroadcastChannel?: BroadcastChannelConstructor;
    indexedDB?: IDBFactoryLike;
    IDBKeyRange?: KeyRangeFactory;
    sessionStorage?: WebEnv["sessionStorage"];
    document?: WebEnv["document"];
    crypto?: { randomUUID?: () => string };
    addEventListener?: unknown;
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
    ...(g.IDBKeyRange !== undefined ? { IDBKeyRange: g.IDBKeyRange } : {}),
    ...(g.navigator?.storage !== undefined ? { storageManager: g.navigator.storage } : {}),
    ...(session !== undefined ? { sessionStorage: session } : {}),
    ...(g.document !== undefined ? { document: g.document } : {}),
    ...(g.document !== undefined && typeof g.addEventListener === "function" ? { page: g as unknown as NonNullable<WebEnv["page"]> } : {}),
    ...(g.crypto?.randomUUID !== undefined ? { randomUUID: () => (g.crypto as { randomUUID: () => string }).randomUUID() } : {}),
  };
}

const TAB_KEY = "abpc.tabId";
/** The channel on which the pages of an origin say which tab ids they hold (a `BroadcastChannel` reaches the pages of its origin only). */
const ID_CHANNEL = "abpc.tab-ids";
const DEFAULT_CLAIM_WAIT_MS = 150;

/** What the pages of an origin say to each other about tab ids. */
type IdMessage = { t: "claim"; id: string; nonce: string } | { t: "held"; id: string; to: string };

const newTabId = (env: WebEnv): string => env.randomUUID?.() ?? `tab-${Math.random().toString(36).slice(2)}${Date.now().toString(36)}`;

export class WebTab implements TabHost {
  private id: string;
  readonly locks: CrossTabLocks;
  readonly storage: StorageProvider;
  /** 14 [CLI-61]: a browser throttles the timers of a hidden tab, so its flusher hands over to a visible tab by default. */
  readonly throttlesHidden = true;
  private readonly channels = new Set<TabChannel>();
  /** The channel of tab ids, open for as long as the page is: it answers for the id that this page holds. */
  private readonly idChannel: TabChannel | undefined;
  /** This page among the pages of the origin: the pages of two tabs that copy one id are told apart by it. */
  private readonly nonce: string;
  private readonly settled: Promise<void>;
  /** The page has asked whether a live tab holds the id, and the wait is not over. */
  private asking = false;
  /** Another page holds the id, or won the tie of two pages that asked for it at once. */
  private taken = false;
  private stopAsking: (() => void) | undefined;

  /** `inherited`: the id came from `sessionStorage`, which a duplicate of the tab copies, so it is asked about before it is used. */
  constructor(
    private readonly env: WebEnv,
    storage: StorageProvider,
    tabId: string,
    o: { inherited?: boolean; claimWaitMs?: number } = {},
  ) {
    this.id = tabId;
    this.storage = storage;
    this.locks = new WebLocks(env.locks);
    this.nonce = `${Math.random().toString(36).slice(2)}${Date.now().toString(36)}`; // told apart, not secret: it is not a tab id, and costs the host's id source nothing
    const ctor = env.BroadcastChannel;
    if (ctor !== undefined) {
      this.idChannel = new BroadcastTabChannel(ctor, ID_CHANNEL, { unref: true });
      this.idChannel.onMessage((m) => this.onIdMessage(m));
    }
    this.settled = o.inherited === true && this.idChannel !== undefined ? this.ask(o.claimWaitMs ?? DEFAULT_CLAIM_WAIT_MS) : Promise.resolve();
  }

  get tabId(): string {
    return this.id;
  }

  ready(): Promise<void> {
    return this.settled;
  }

  /** Asks the other pages whether a live tab holds the id; takes a fresh one, and keeps it in `sessionStorage`, when one does. */
  private async ask(waitMs: number): Promise<void> {
    this.asking = true;
    this.idChannel?.post({ t: "claim", id: this.id, nonce: this.nonce } satisfies IdMessage);
    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, waitMs);
      this.stopAsking = (): void => {
        clearTimeout(timer);
        resolve();
      };
    });
    this.asking = false;
    if (!this.taken) return;
    this.id = newTabId(this.env);
    try {
      this.env.sessionStorage?.setItem(TAB_KEY, this.id);
    } catch {
      // storage blocked: the id lasts for as long as this tab object
    }
  }

  private onIdMessage(m: unknown): void {
    if (typeof m !== "object" || m === null) return;
    const msg = m as IdMessage;
    if (msg.t === "claim") {
      if (msg.id !== this.id || msg.nonce === this.nonce) return;
      // a page that holds the id says so; two pages that ask at once are settled by their nonces: the lower keeps the id
      if (!this.asking || this.nonce < msg.nonce) this.idChannel?.post({ t: "held", id: this.id, to: msg.nonce } satisfies IdMessage);
      else this.lose();
    } else if (msg.t === "held" && this.asking && msg.to === this.nonce && msg.id === this.id) {
      this.lose();
    }
  }

  private lose(): void {
    this.taken = true;
    this.stopAsking?.();
  }

  /** Closes the channels of the tab: the page is going away (a browser does it itself; a test, and an app that tears its host down, call it). */
  close(): void {
    this.idChannel?.close();
    for (const c of this.channels) c.close();
    this.channels.clear();
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

  /** 14 [CLI-61], 09 [READ-74]: `pagehide` is a hint that this tab closes (the lock that the tab holds says so for sure); `pageshow` of a persisted page says that it came back. */
  onPageHide(fn: (event: "hide" | "show") => void): () => void {
    const p = this.env.page;
    if (p === undefined) return () => undefined;
    const hide = (): void => fn("hide");
    const show = (e: { persisted?: boolean }): void => {
      if (e.persisted === true) fn("show");
    };
    p.addEventListener("pagehide", hide);
    p.addEventListener("pageshow", show);
    return () => {
      p.removeEventListener("pagehide", hide);
      p.removeEventListener("pageshow", show);
    };
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
  const { env: _e, tabId: _t, blockedWaitMs, claimWaitMs, ...bounds } = opts;
  let tabId = opts.tabId;
  let inherited = false;
  if (tabId === undefined) {
    try {
      tabId = env.sessionStorage?.getItem(TAB_KEY) ?? undefined;
      inherited = tabId !== undefined;
    } catch {
      // storage blocked
    }
  }
  if (tabId === undefined) {
    tabId = newTabId(env);
    try {
      env.sessionStorage?.setItem(TAB_KEY, tabId);
    } catch {
      // storage blocked: the id lasts for as long as this tab object
    }
  }
  return new WebTab(env, new IndexedDbStorageProvider(env.indexedDB, bounds, env.storageManager, { ...(env.IDBKeyRange !== undefined ? { keyRange: env.IDBKeyRange } : {}), ...(blockedWaitMs !== undefined ? { blockedWaitMs } : {}) }), tabId, {
    inherited,
    ...(claimWaitMs !== undefined ? { claimWaitMs } : {}),
  });
}
