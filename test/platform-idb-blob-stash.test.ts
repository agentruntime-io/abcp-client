// C6b: the stash of the bytes that actions name, in IndexedDB (src/platform/idb-blob-stash.ts), against the double of test/helpers/fake-idb.ts (09 [READ-94], 14 [CLI-44], [CLI-65]). The
// engine's use of it (a restart, the upload before the first send, the purge) is test/engine-blobs-idb.test.ts. NOT TESTED: a real browser's IndexedDB (its quota, its structured clone of
// a large typed array).

import { describe, expect, it } from "vitest";
import { digestSha256, type StagedBlob } from "../src/engine/index.js";
import { IndexedDbBlobStash } from "../src/platform/idb-blob-stash.js";
import { IndexedDbStorageProvider, type IDBFactoryLike, type IDBOpenRequestLike } from "../src/platform/idb-storage.js";
import { FakeIDBFactory } from "./helpers/fake-idb.js";
import { bytesOf } from "./helpers/presence.js";

const KEY = { origin: "https://a.test", appId: "app", partition: "iss|ana" };
const other = { origin: "https://a.test", appId: "app", partition: "iss|bob" };

async function blob(id: string, n: number, ks = "ks1", seed = 7): Promise<StagedBlob> {
  const bytes = bytesOf(n, seed);
  return { keyspaceId: ks, blobId: id, bytes, sha256: await digestSha256(bytes), size: n, contentType: "text/plain", stagedAtMs: 12345 };
}
const rowsOf = (f: FakeIDBFactory, db: string, store: string): string[] => [...(f.dbs.get(db)?.stores.get(store)?.keys() ?? [])].map((k) => k.replace("\u0000", "\\0"));
const blobDbs = (f: FakeIDBFactory): string[] => [...f.dbs.keys()].filter((k) => k.startsWith("abpc-blobs:"));

describe("09 [READ-94]: IndexedDbBlobStash keeps the bytes of the blobs that actions name, in IndexedDB", () => {
  it("put, get, has, list and delete; another instance on the database (a reload) reads the same", async () => {
    const f = new FakeIDBFactory();
    const s = new IndexedDbBlobStash(f, "blobs-1");
    const a = await blob("a".repeat(22), 1000);
    await s.put(a);
    await s.put(await blob("b".repeat(22), 3));
    await s.put(await blob("a".repeat(22), 5, "ks2")); // the same blob id in another keyspace is another blob
    expect(await s.has("ks1", a.blobId)).toBe(true);
    expect(await s.has("ks1", "c".repeat(22))).toBe(false);
    const reload = new IndexedDbBlobStash(f, "blobs-1"); // the page was reloaded: a new object on the same database
    const got = await reload.get("ks1", a.blobId);
    expect(got).toMatchObject({ keyspaceId: "ks1", blobId: a.blobId, size: 1000, contentType: "text/plain", stagedAtMs: 12345 });
    expect(Buffer.from(got?.bytes as Uint8Array).equals(Buffer.from(a.bytes))).toBe(true);
    expect(Buffer.from(got?.sha256 as Uint8Array).equals(Buffer.from(a.sha256))).toBe(true);
    expect((await reload.list()).map((x) => `${x.keyspaceId}:${x.size}`).sort()).toEqual(["ks1:1000", "ks1:3", "ks2:5"]);
    await reload.delete("ks1", a.blobId);
    expect(await s.get("ks1", a.blobId)).toBeUndefined();
    expect(await s.has("ks1", a.blobId)).toBe(false);
    expect((await s.get("ks2", a.blobId))?.size).toBe(5);
    await s.delete("ks1", a.blobId); // nothing to delete
    expect(await s.list()).toHaveLength(2);
  });

  it("stages a blob with its bytes and its facts in one transaction: a transaction that fails stages neither", async () => {
    const f = new FakeIDBFactory();
    const s = new IndexedDbBlobStash(f, "blobs-2");
    await s.put(await blob("a".repeat(22), 10)); // opens the database
    f.failTransactions = 1; // a quota, say: the transaction aborts when it would complete
    await expect(s.put(await blob("b".repeat(22), 20))).rejects.toThrow(/aborted/);
    expect(rowsOf(f, "blobs-2", "bytes")).toEqual([`ks1\\0${"a".repeat(22)}`]);
    expect(rowsOf(f, "blobs-2", "facts")).toEqual([`ks1\\0${"a".repeat(22)}`]);
    expect(await s.has("ks1", "b".repeat(22))).toBe(false);
    f.failTransactions = 1;
    await expect(s.delete("ks1", "a".repeat(22))).rejects.toThrow(/aborted/);
    expect(await s.has("ks1", "a".repeat(22))).toBe(true); // a delete that failed left both rows
    expect((await s.get("ks1", "a".repeat(22)))?.size).toBe(10);
  });

  it("a row that is not what put wrote is a blob that is not staged: never a wrong upload", async () => {
    const f = new FakeIDBFactory();
    const s = new IndexedDbBlobStash(f, "blobs-3");
    const a = await blob("a".repeat(22), 500);
    await s.put(a);
    const key = `ks1\u0000${a.blobId}`;
    const bytes = f.dbs.get("blobs-3")?.stores.get("bytes") as Map<string, unknown>;
    bytes.set(key, new Uint8Array(100)); // a short body
    expect(await s.get("ks1", a.blobId)).toBeUndefined();
    bytes.set(key, "not bytes");
    expect(await s.get("ks1", a.blobId)).toBeUndefined();
    bytes.delete(key); // the facts without the bytes
    expect(await s.get("ks1", a.blobId)).toBeUndefined();
    const facts = f.dbs.get("blobs-3")?.stores.get("facts") as Map<string, unknown>;
    facts.set(key, { keyspaceId: "ks1" }); // facts that are not facts
    expect(await s.has("ks1", a.blobId)).toBe(false);
    expect(await s.list()).toEqual([]);
  });

  it("14 [CLI-65]: clear erases every blob in one transaction, whoever staged it", async () => {
    const f = new FakeIDBFactory();
    const mine = new IndexedDbBlobStash(f, "blobs-4");
    const theirs = new IndexedDbBlobStash(f, "blobs-4"); // another tab of the partition
    await mine.put(await blob("a".repeat(22), 10));
    await theirs.put(await blob("b".repeat(22), 20, "ks2"));
    await mine.clear();
    expect(await theirs.list()).toEqual([]);
    expect(rowsOf(f, "blobs-4", "bytes")).toEqual([]);
    expect(rowsOf(f, "blobs-4", "facts")).toEqual([]);
    await mine.clear(); // nothing to erase
  });

  it("the database is opened by the first call, and a failed open is made again by the next one", async () => {
    const real = new FakeIDBFactory();
    let failOnce = true;
    const factory: IDBFactoryLike = {
      open: (name, version) => {
        if (!failOnce) return real.open(name, version);
        failOnce = false;
        const req = { result: undefined as unknown, error: new Error("InvalidStateError (injected)"), onsuccess: null, onerror: null, onupgradeneeded: null, onblocked: null } as unknown as IDBOpenRequestLike;
        queueMicrotask(() => req.onerror?.());
        return req;
      },
      deleteDatabase: (name) => real.deleteDatabase(name),
    };
    const s = new IndexedDbBlobStash(factory, "blobs-5");
    expect(real.dbs.has("blobs-5")).toBe(false);
    await expect(s.list()).rejects.toThrow(/cannot be opened/);
    expect(await s.list()).toEqual([]); // the next call opens it again
    expect(real.dbs.has("blobs-5")).toBe(true);
  });
});

describe("V19 SHOULD-8, 14 [CLI-65]: the stash yields to another tab's deletion of its database, and a deletion that it blocks is told, never waited for for ever", () => {
  it("another tab's connection closes at the versionchange, the deletion goes through, and the stash of that tab refuses what follows", async () => {
    const f = new FakeIDBFactory();
    const tabA = new IndexedDbStorageProvider(f);
    const tabB = new IndexedDbStorageProvider(f);
    const a = tabA.blobStash(KEY);
    const b = tabB.blobStash(KEY) as IndexedDbBlobStash;
    await a.put(await blob("a".repeat(22), 10));
    expect((await b.list()).length).toBe(1); // B has the database open too
    expect(blobDbs(f)).toHaveLength(1);
    await tabA.destroy(KEY); // resolves: B yielded
    expect(blobDbs(f)).toEqual([]);
    expect(b.isClosed).toBe(true);
    await expect(b.get("ks1", "a".repeat(22))).rejects.toThrow(/closed/);
    await expect(b.put(await blob("b".repeat(22), 5))).rejects.toThrow(/closed/);
    // the partition's stash is a new one in the tab that asks: it opens empty
    const fresh = tabB.blobStash(KEY);
    expect(fresh).not.toBe(b);
    expect(await fresh.list()).toEqual([]);
  });

  it("a connection that does not yield (a tab of a build without the handler) makes the sign-out fail after a wait; the deletion completes when it closes", async () => {
    const f = new FakeIDBFactory();
    const tabA = new IndexedDbStorageProvider(f, {}, undefined, { blockedWaitMs: 20 });
    await tabA.open(KEY);
    const stash = tabA.blobStash(KEY);
    await stash.put(await blob("a".repeat(22), 10));
    const name = blobDbs(f)[0] as string;
    const stubborn = f.open(name);
    const conn = await new Promise<{ close(): void; onversionchange?: unknown }>((resolve) => {
      stubborn.onsuccess = (): void => resolve(stubborn.result);
    });
    conn.onversionchange = null; // never closes
    await expect(tabA.destroy(KEY)).rejects.toThrow(/keeps the database .* open/);
    expect(f.dbs.has("abpc:" + [KEY.origin, KEY.appId, KEY.partition].join("\u0000"))).toBe(false); // the partition's own database went: the blobs' failure did not stop it
    conn.close(); // the other tab goes away: the deletion that the browser kept pending goes through
    await new Promise((r) => setTimeout(r, 0));
    expect(blobDbs(f)).toEqual([]);
  });
});

describe("14 [CLI-74], [CLI-65]: IndexedDbStorageProvider.blobStash is the stash of a partition", () => {
  it("is one stash for each partition, in a database of its own beside the partition's, shared by the tabs of the partition", async () => {
    const f = new FakeIDBFactory();
    const p = new IndexedDbStorageProvider(f);
    const s = p.blobStash(KEY);
    expect(p.blobStash(KEY)).toBe(s);
    expect(p.blobStash(other)).not.toBe(s);
    expect(blobDbs(f)).toEqual([]); // opened by the first call that needs it
    await s.put(await blob("a".repeat(22), 10));
    await p.blobStash(other).put(await blob("b".repeat(22), 20));
    expect(blobDbs(f)).toHaveLength(2);
    // another tab (another provider on the same database) sees the same blobs
    expect((await new IndexedDbStorageProvider(f).blobStash(KEY).list()).map((x) => x.size)).toEqual([10]);
    expect((await new IndexedDbStorageProvider(f).blobStash(other).list()).map((x) => x.size)).toEqual([20]);
  });

  it("destroy deletes the stash with the partition, and the other partitions keep theirs", async () => {
    const f = new FakeIDBFactory();
    const p = new IndexedDbStorageProvider(f);
    await p.open(KEY);
    await p.open(other);
    await p.blobStash(KEY).put(await blob("a".repeat(22), 10));
    await p.blobStash(other).put(await blob("b".repeat(22), 20));
    await p.destroy(KEY);
    expect(blobDbs(f)).toHaveLength(1);
    expect((await p.blobStash(other).list()).map((x) => x.size)).toEqual([20]);
    // the partition opens again, and its stash is empty
    await p.open(KEY);
    expect(await p.blobStash(KEY).list()).toEqual([]);
    // a partition whose stash was never opened is destroyed all the same
    const q = new IndexedDbStorageProvider(new FakeIDBFactory());
    await q.open(KEY);
    await q.destroy(KEY);
  });
});
