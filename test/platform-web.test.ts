// C5: the browser platform (src/platform: web, web-locks, idb-storage, channel). What Node provides is tested against Node: `navigator.locks` (Web Locks, Node
// 24.5 and later) and `BroadcastChannel`. What it does not provide, IndexedDB, `sessionStorage` and `document`, is tested against doubles of the API subset that
// the code uses (test/helpers/fake-idb.ts): that shows the logic, not a browser's behaviour. NOT TESTED: a real browser (IndexedDB's own transaction and
// quota behaviour, the lock of a tab that crashes, `visibilitychange`, `navigator.storage.persist()`).

import { describe, expect, it } from "vitest";
import { create } from "@bufbuild/protobuf";
import { KeyEntrySchema, RecordSchema } from "../src/gen/abpc/v2/types_pb.js";
import { lockName, StoreFull, tabLockName, type OutboxEntry, type ReplicaHead, type StorageOp } from "../src/engine/index.js";
import { MemoryStorage } from "../src/engine/memory-storage.js";
import { BroadcastTabChannel, type BroadcastChannelConstructor } from "../src/platform/channel.js";
import { IndexedDbStorage, IndexedDbStorageProvider } from "../src/platform/idb-storage.js";
import { createWebHost, globalWebEnv, type WebEnv } from "../src/platform/web.js";
import { WebLocks, type LockManagerLike } from "../src/platform/web-locks.js";
import { FakeIDBFactory } from "./helpers/fake-idb.js";

const nodeLocks = (globalThis as unknown as { navigator?: { locks?: LockManagerLike } }).navigator?.locks;
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

describe.skipIf(nodeLocks === undefined)("WebLocks over Node's navigator.locks", () => {
  const locks = new WebLocks(nodeLocks);
  it("takes the lock if it is free, refuses it when another holder has it, and frees it on release", async () => {
    expect(locks.reliable).toBe(true);
    const a = await locks.tryAcquire("web-locks-test-1");
    expect(a?.held).toBe(true);
    expect(await locks.tryAcquire("web-locks-test-1")).toBeUndefined();
    await a?.release();
    expect(a?.held).toBe(false);
    const b = await locks.tryAcquire("web-locks-test-1");
    expect(b?.held).toBe(true);
    await b?.release();
  });

  it("queues the waiting tabs in the order that they asked, and an abort leaves the queue with an AbortError", async () => {
    const first = await locks.tryAcquire("web-locks-test-2");
    const order: string[] = [];
    const second = locks.acquire("web-locks-test-2").then((l) => (order.push("second"), l));
    const ac = new AbortController();
    const aborted = locks.acquire("web-locks-test-2", ac.signal);
    const third = locks.acquire("web-locks-test-2").then((l) => (order.push("third"), l));
    await sleep(20);
    ac.abort();
    await expect(aborted).rejects.toMatchObject({ name: "AbortError" });
    expect(order).toEqual([]);
    await first?.release();
    const s = await second;
    expect(order).toEqual(["second"]);
    await s.release();
    await (await third).release();
    expect(order).toEqual(["second", "third"]);
  });

  it("reports a lock that another context stole as no longer held", async () => {
    const a = await locks.tryAcquire("web-locks-test-3");
    const stolen = new Promise<void>((resolve) => {
      void nodeLocks?.request("web-locks-test-3", { steal: true }, () => {
        resolve();
        return new Promise<void>((done) => setTimeout(done, 20));
      });
    });
    await stolen;
    await sleep(10);
    expect(a?.held).toBe(false);
    await sleep(40);
  });

  it("is unreliable where there is no lock manager", async () => {
    const none = new WebLocks(undefined);
    expect(none.reliable).toBe(false);
    await expect(none.tryAcquire("x")).rejects.toThrow(/no Web Locks/);
  });
});

describe("the BroadcastChannel of a tab", () => {
  const ctor = globalThis.BroadcastChannel as unknown as BroadcastChannelConstructor;
  it("reaches the other channels of the name, never the one that posted, and not after it is closed", async () => {
    const a = new BroadcastTabChannel(ctor, "abpc-test-channel");
    const b = new BroadcastTabChannel(ctor, "abpc-test-channel");
    const other = new BroadcastTabChannel(ctor, "abpc-test-other");
    const gotA: unknown[] = [];
    const gotB: unknown[] = [];
    const gotO: unknown[] = [];
    a.onMessage((m) => gotA.push(m));
    const off = b.onMessage((m) => gotB.push(m));
    other.onMessage((m) => gotO.push(m));
    a.post({ n: 1n, bytes: new Uint8Array([1, 2]), nested: { s: "x" } });
    await sleep(30);
    expect(gotA).toEqual([]);
    expect(gotB).toEqual([{ n: 1n, bytes: new Uint8Array([1, 2]), nested: { s: "x" } }]);
    expect(gotO).toEqual([]);
    off();
    a.post("again");
    await sleep(30);
    expect(gotB).toHaveLength(1);
    b.onMessage((m) => gotB.push(m));
    a.close();
    a.post("closed"); // no error, no message
    b.post("to nobody");
    await sleep(20);
    expect(gotA).toEqual([]);
    b.close();
    other.close();
  });

  it("does not stop on a listener that throws, and does not throw for a message that cannot be cloned", async () => {
    const a = new BroadcastTabChannel(ctor, "abpc-test-throw");
    const b = new BroadcastTabChannel(ctor, "abpc-test-throw");
    const got: unknown[] = [];
    b.onMessage(() => {
      throw new Error("a bad listener");
    });
    b.onMessage((m) => got.push(m));
    a.post(1);
    a.post({ f: () => 1 });
    await sleep(30);
    expect(got).toEqual([1]);
    a.close();
    b.close();
  });
});

describe("the web host", () => {
  const session = (): { store: Map<string, string>; api: NonNullable<WebEnv["sessionStorage"]> } => {
    const store = new Map<string, string>();
    return { store, api: { getItem: (k) => store.get(k) ?? null, setItem: (k, v) => void store.set(k, v) } };
  };

  it("keeps a tab's id across a reload (sessionStorage), and gives another tab another", () => {
    const s = session();
    const env: WebEnv = { indexedDB: new FakeIDBFactory(), sessionStorage: s.api, randomUUID: (() => { let n = 0; return () => `uuid-${++n}`; })() };
    const first = createWebHost({ env });
    expect(first.tabId).toBe("uuid-1");
    expect(createWebHost({ env }).tabId).toBe("uuid-1"); // a reload: the same sessionStorage
    expect(createWebHost({ env: { ...env, sessionStorage: session().api } }).tabId).toBe("uuid-2");
    expect(createWebHost({ env, tabId: "mine" }).tabId).toBe("mine");
  });

  it("works with storage blocked, says that it has no reliable lock when it has none, and refuses to run without IndexedDB", () => {
    const blocked: NonNullable<WebEnv["sessionStorage"]> = {
      getItem: () => {
        throw new Error("SecurityError");
      },
      setItem: () => {
        throw new Error("SecurityError");
      },
    };
    const h = createWebHost({ env: { indexedDB: new FakeIDBFactory(), sessionStorage: blocked } });
    expect(h.tabId).toMatch(/^tab-/);
    expect(h.locks.reliable).toBe(false);
    expect(() => h.channel("c")).toThrow(/BroadcastChannel/);
    expect(() => createWebHost({ env: {} })).toThrow(/no IndexedDB/);
  });

  it("follows the visibility of the document", () => {
    const listeners = new Set<() => void>();
    const doc = {
      visibilityState: "visible",
      addEventListener: (_t: "visibilitychange", fn: () => void) => void listeners.add(fn),
      removeEventListener: (_t: "visibilitychange", fn: () => void) => void listeners.delete(fn),
    };
    const h = createWebHost({ env: { indexedDB: new FakeIDBFactory(), document: doc } });
    const seen: boolean[] = [];
    const off = h.onVisibility((v) => seen.push(v));
    expect(h.visible()).toBe(true);
    doc.visibilityState = "hidden";
    for (const l of listeners) l();
    expect(h.visible()).toBe(false);
    off();
    expect(listeners.size).toBe(0);
    expect(seen).toEqual([false]);
  });

  // V21 MUST-4, 09 [READ-74], 14 [CLI-61]: `pagehide` is the hint that a tab closes, and `pageshow` of a persisted page says that it came back; the lock that the tab holds is what tells for sure
  it("reports pagehide as a hint that the tab closes, and pageshow of a page that was put away (persisted) as its return", () => {
    const listeners = new Map<string, Set<(e: { persisted?: boolean }) => void>>();
    const page = {
      addEventListener: (t: "pagehide" | "pageshow", fn: (e: { persisted?: boolean }) => void) => void (listeners.get(t) ?? listeners.set(t, new Set()).get(t))?.add(fn),
      removeEventListener: (t: "pagehide" | "pageshow", fn: (e: { persisted?: boolean }) => void) => void listeners.get(t)?.delete(fn),
    };
    const fire = (t: "pagehide" | "pageshow", persisted?: boolean): void => {
      for (const l of [...(listeners.get(t) ?? [])]) l(persisted === undefined ? {} : { persisted });
    };
    const h = createWebHost({ env: { indexedDB: new FakeIDBFactory(), page } });
    const seen: string[] = [];
    const off = h.onPageHide((e) => seen.push(e));
    fire("pagehide");
    fire("pageshow", false); // a page that was loaded, not restored
    fire("pageshow", true);
    expect(seen).toEqual(["hide", "show"]);
    off();
    expect([...listeners.values()].every((s) => s.size === 0)).toBe(true);
    // without a window there is nothing to listen to
    expect(createWebHost({ env: { indexedDB: new FakeIDBFactory() } }).onPageHide(() => undefined)()).toBeUndefined();
  });

  it("names the lock of a tab by the tab and the engine that holds it: a reload is the same tab id and another engine, and the lock of the page that went is not the one of the page that replaced it", () => {
    const k = { origin: "https://abpc.test", appId: "app", partition: "i|ana" };
    expect(tabLockName(k, "tab1", "e1")).not.toBe(tabLockName(k, "tab1", "e2"));
    expect(tabLockName(k, "tab1", "e1")).not.toBe(tabLockName(k, "tab2", "e1"));
    expect(tabLockName(k, "tab1", "e1")).not.toBe(lockName(k)); // never the flusher's lock
    expect(tabLockName(k, "tab1", "e1")).not.toBe(tabLockName({ ...k, partition: "i|bob" }, "tab1", "e1"));
  });

  it("takes what the global scope gives", () => {
    const env = globalWebEnv();
    expect(env.BroadcastChannel).toBeDefined();
    expect(env.indexedDB).toBeUndefined(); // Node has none
    expect(env.randomUUID?.()).toMatch(/^[0-9a-f-]{36}$/);
  });
});

// ---- IndexedDB storage, against the double -----------------------------------------------------------------------------

const entry = (id: string, n = 1): OutboxEntry => ({
  mutationId: id,
  keyspaceId: "ks",
  lane: "SEQUENCED",
  opType: "Set",
  content: new Uint8Array([9, n]),
  author: { issuer: "i", sub: "s", deviceId: "d" },
  ordinal: n,
  authoredServerMs: 1_700_000_000_000,
  dependsOn: [],
  clientId: "",
  clientSeq: 0,
  afterClientSeq: 0,
  everSent: false,
  sentInEpoch: 0,
  attempts: 0,
  registered: false,
  permissionRetried: false,
  actorMismatchRetried: false,
  spendResent: false,
  outcome: { kind: "appended", seq: 99n, logEpoch: 1, overwroteNewer: [] },
});
const head = (seq: bigint): ReplicaHead => ({ logEpoch: 1, seq, watermark: seq, lastRecordSeq: seq, manifestVersion: 1, kind: "team", lifecycle: 1, successors: [], interest: [], lastSyncMs: 5 });
const rec = (seq: bigint) => create(RecordSchema, { seq, logEpoch: 1, manifestVersion: 1, mutationId: `m${seq}` });
const kentry = (key: string) => create(KeyEntrySchema, { key, encoding: "json/v1", value: new Uint8Array([1]), version: 1n });

describe("the IndexedDB storage against a double of the API", () => {
  it("reads back what was committed, as MemoryStorage would, including a replica with its entries and its tail", async () => {
    const factory = new FakeIDBFactory();
    const idb = await IndexedDbStorage.open(factory, "p1");
    const mem = new MemoryStorage();
    const ops: StorageOp[] = [
      { op: "putEntry", entry: entry("a", 1) },
      { op: "putEntry", entry: entry("b", 2) },
      { op: "setMeta", key: "horizon", value: 77n },
      { op: "replicaBegin", keyspaceId: "ks", head: head(3n), base: { manifestVersion: 1, logEpoch: 1, checkpointSeq: 3n, entries: [kentry("note/1"), kentry("note/2")] } },
      { op: "replicaAppend", keyspaceId: "ks", head: head(5n), records: [rec(4n), rec(5n)] },
      { op: "replicaExtend", keyspaceId: "ks", head: head(5n), entries: [kentry("task/1")] },
      { op: "replicaHead", keyspaceId: "ks", head: head(6n) },
      { op: "deleteEntry", mutationId: "a" },
    ];
    await idb.commit(ops);
    await mem.commit(ops);
    expect(await idb.load()).toEqual(await mem.load());
    // a second handle on the same database (another tab, or a reload) reads the same, and the replica's order is the order it was written in
    const again = await IndexedDbStorage.open(factory, "p1");
    expect(await again.load()).toEqual(await mem.load());
    const image = (await again.load()).replicas[0];
    expect(image?.base.entries.map((e) => e.key)).toEqual(["note/1", "note/2", "task/1"]);
    expect(image?.tail.map((r) => r.seq)).toEqual([4n, 5n]);
    // a new load replaces the old image; deleting the replica and purging everything leave nothing behind
    await idb.commit([{ op: "replicaBegin", keyspaceId: "ks", head: head(9n), base: { manifestVersion: 1, logEpoch: 1, checkpointSeq: 9n, entries: [kentry("x/1")] } }]);
    const replaced = (await idb.load()).replicas[0];
    expect(replaced?.base.entries.map((e) => e.key)).toEqual(["x/1"]);
    expect(replaced?.tail).toEqual([]);
    await idb.commit([{ op: "deleteReplica", keyspaceId: "ks" }]);
    expect((await idb.load()).replicas).toEqual([]);
    await idb.commit([{ op: "purgeAll" }]);
    const empty = await idb.load();
    expect(empty.entries).toEqual([]);
    expect(empty.meta).toEqual({});
    for (const rows of factory.dbs.get("p1")?.stores.values() ?? []) expect(rows.size).toBe(0);
  });

  it("applies a commit whole or not at all: an aborted transaction changes nothing, and neither does a refused bound", async () => {
    const factory = new FakeIDBFactory();
    const idb = await IndexedDbStorage.open(factory, "p2", { maxOutbox: 2 });
    await idb.commit([{ op: "putEntry", entry: entry("a") }, { op: "putEntry", entry: entry("b") }]);
    expect(idb.full()).toEqual(["outbox"]);
    await expect(idb.commit([{ op: "putEntry", entry: entry("c") }, { op: "setMeta", key: "k", value: 1 }])).rejects.toBeInstanceOf(StoreFull);
    expect((await idb.load()).entries.map((e) => e.mutationId)).toEqual(["a", "b"]);
    expect((await idb.load()).meta).toEqual({});
    // a delete and a put in one commit stay within the bound
    await idb.commit([{ op: "deleteEntry", mutationId: "a" }, { op: "putEntry", entry: entry("c") }]);
    expect((await idb.load()).entries.map((e) => e.mutationId)).toEqual(["b", "c"]);
    // the transaction aborts (a quota, a closed database): the state and the in-memory ids are what they were
    factory.failTransactions = 1;
    await expect(idb.commit([{ op: "deleteEntry", mutationId: "b" }, { op: "setMeta", key: "k", value: 2 }])).rejects.toThrow(/aborted/);
    expect((await idb.load()).entries.map((e) => e.mutationId)).toEqual(["b", "c"]);
    expect((await idb.load()).meta).toEqual({});
    expect(idb.full()).toEqual(["outbox"]); // `b` still counts
    await idb.commit([{ op: "deleteEntry", mutationId: "b" }]);
    expect(idb.full()).toEqual([]);
    // an append to a replica that does not exist is refused before anything is written
    await expect(idb.commit([{ op: "putEntry", entry: entry("d") }, { op: "replicaAppend", keyspaceId: "none", head: head(1n), records: [] }])).rejects.toThrow(/no replica image/);
    expect((await idb.load()).entries.map((e) => e.mutationId)).toEqual(["c"]);
  });

  it("serializes commits, asks the platform to keep the storage, and reports the mode", async () => {
    const factory = new FakeIDBFactory();
    let granted = false;
    const idb = await IndexedDbStorage.open(factory, "p3", {}, { persist: () => Promise.resolve(granted) });
    expect(idb.mode).toBe("best-effort");
    expect(await idb.requestPersistence()).toBe(false);
    granted = true;
    expect(await idb.requestPersistence()).toBe(true);
    expect(idb.mode).toBe("persisted");
    // twenty commits issued at once land in the order they were issued
    await Promise.all(Array.from({ length: 20 }, (_, i) => idb.commit([{ op: "setMeta", key: "n", value: i }])));
    expect((await idb.load()).meta["n"]).toBe(19);
    // a database that was persisted before is persisted when it is opened again
    const again = await IndexedDbStorage.open(factory, "p3", {}, { persist: () => Promise.resolve(true), persisted: () => Promise.resolve(true) });
    expect(again.mode).toBe("persisted");
  });

  it("is a StorageProvider: a database for each partition, listed by origin and app, destroyed with everything in it", async () => {
    const factory = new FakeIDBFactory();
    const p = new IndexedDbStorageProvider(factory);
    const k1 = { origin: "https://a.test", appId: "app", partition: "iss|ana" };
    const k2 = { origin: "https://a.test", appId: "app", partition: "iss|bob" };
    const k3 = { origin: "https://b.test", appId: "app", partition: "iss|ana" };
    const s1 = await p.open(k1);
    expect(await p.open(k1)).toBe(s1);
    await p.open(k2);
    await p.open(k3);
    await s1.commit([{ op: "putEntry", entry: entry("a") }]);
    expect((await p.list("https://a.test", "app")).map((k) => k.partition).sort()).toEqual(["iss|ana", "iss|bob"]);
    expect(await p.list("https://a.test", "other")).toEqual([]);
    await p.destroy(k1);
    expect((await p.list("https://a.test", "app")).map((k) => k.partition)).toEqual(["iss|bob"]);
    // the partition is opened again empty
    expect((await (await p.open(k1)).load()).entries).toEqual([]);
    // another provider on the same factory (a reload) lists what is there
    expect((await new IndexedDbStorageProvider(factory).list("https://b.test", "app")).map((k) => k.partition)).toEqual(["iss|ana"]);
  });
});
