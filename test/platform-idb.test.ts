// V19 MUST-2, SHOULD-8, V14 S4 on the IndexedDB storage (src/platform/idb-storage.ts), against the double of test/helpers/fake-idb.ts: two tabs are two
// storages on one database. A tab that becomes the flusher wrote nothing before, and its index of the database is the one that it read at its boot: it must not
// write through that (14 [CLI-66]: a revocation's purge leaves nothing; 14 [CLI-84]: the bounds are the database's).

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { StoreFull, type DeadLetterEntry, type OutboxEntry, type ReplicaHead } from "../src/engine/index.js";
import { KeyEntrySchema, RecordSchema } from "../src/gen/abcp/v2/types_pb.js";
import { IndexedDbStorage, IndexedDbStorageProvider } from "../src/platform/idb-storage.js";
import { FakeIDBFactory, FakeKeyRange } from "./helpers/fake-idb.js";

const entry = (id: string, n = 1): OutboxEntry => ({
  mutationId: id, keyspaceId: "ks", lane: "SEQUENCED", opType: "Set", content: new Uint8Array([9, n]),
  author: { issuer: "i", sub: "s", deviceId: "d" }, ordinal: n, authoredServerMs: 1, dependsOn: [], clientId: "", clientSeq: 0,
  afterClientSeq: 0, everSent: false, sentInEpoch: 0, attempts: 0, registered: false, permissionRetried: false,
  actorMismatchRetried: false, spendResent: false,
});
const head = (seq: bigint, interest: string[] = []): ReplicaHead => ({ logEpoch: 1, seq, watermark: seq, lastRecordSeq: seq, manifestVersion: 1, kind: "team", lifecycle: 1, successors: [], interest, lastSyncMs: 5 });
const rec = (seq: bigint, epoch = 1) => create(RecordSchema, { seq, logEpoch: epoch, manifestVersion: 1, mutationId: `m${seq}` });
const kentry = (key: string) => create(KeyEntrySchema, { key, encoding: "json/v1", value: new Uint8Array([1]), version: 1n });
const rows = (f: FakeIDBFactory, db: string, store: string): string[] => [...(f.dbs.get(db)?.stores.get(store)?.keys() ?? [])].map((k) => k.replace("\u0000", "\\0"));

// the same suite with a key range (a browser) and without (a platform that has none: the rows are told apart by their keys)
for (const mode of ["with IDBKeyRange", "without a key range"] as const) {
  const range = mode === "with IDBKeyRange" ? FakeKeyRange : undefined;
  const open = (f: FakeIDBFactory, name: string, bounds = {}): Promise<IndexedDbStorage> => IndexedDbStorage.open(f, name, bounds, undefined, range);

  describe(`V19 MUST-2: a tab that became the flusher writes through what the database holds, not through what it read at its boot (${mode})`, () => {
    it("the purge of a revocation deletes every row of the replica that another tab wrote, and the next load finds nothing of it (14 [CLI-66])", async () => {
      const f = new FakeIDBFactory();
      const b = await open(f, "p"); // tab B boots as a follower: it indexes an empty database
      const a = await open(f, "p"); // tab A is the flusher
      await a.commit([{ op: "replicaBegin", keyspaceId: "ks", head: head(3n), base: { manifestVersion: 1, logEpoch: 1, checkpointSeq: 3n, entries: [kentry("x/1"), kentry("x/2")] } }]);
      await a.commit([{ op: "replicaAppend", keyspaceId: "ks", head: head(5n), records: [rec(4n), rec(5n)] }]);
      await a.commit([{ op: "replicaBegin", keyspaceId: "other", head: head(1n), base: { manifestVersion: 1, logEpoch: 1, checkpointSeq: 1n, entries: [kentry("o/1")] } }]);
      await b.load(); // B is promoted: it reads what A wrote
      await b.commit([{ op: "deleteReplica", keyspaceId: "ks" }]);
      expect(rows(f, "p", "replicas")).toEqual(["other"]);
      expect(rows(f, "p", "replicaEntries")).toEqual(["other\\00000000000"]); // the rows of the other keyspace stay
      expect(rows(f, "p", "replicaTail")).toEqual([]);
      expect((await (await open(f, "p")).load()).replicas.map((r) => r.keyspaceId)).toEqual(["other"]);
    });

    it("the purge deletes the rows even when B never called load (the index of its boot knows nothing of them)", async () => {
      const f = new FakeIDBFactory();
      const b = await open(f, "p");
      const a = await open(f, "p");
      await a.commit([{ op: "replicaBegin", keyspaceId: "ks", head: head(3n), base: { manifestVersion: 1, logEpoch: 1, checkpointSeq: 3n, entries: [kentry("x/1")] } }]);
      await a.commit([{ op: "replicaAppend", keyspaceId: "ks", head: head(4n), records: [rec(4n)] }]);
      await b.commit([{ op: "deleteReplica", keyspaceId: "ks" }]);
      for (const s of ["replicas", "replicaEntries", "replicaTail"]) expect(rows(f, "p", s)).toEqual([]);
    });

    it("an append to the replica that the other tab began is applied, and a new image replaces the old one whole: no row of the old image joins it", async () => {
      const f = new FakeIDBFactory();
      const b = await open(f, "p");
      const a = await open(f, "p");
      await a.commit([{ op: "replicaBegin", keyspaceId: "ks", head: head(3n), base: { manifestVersion: 1, logEpoch: 1, checkpointSeq: 3n, entries: [kentry("x/1")] } }]);
      await a.commit([{ op: "replicaAppend", keyspaceId: "ks", head: head(5n), records: [rec(4n), rec(5n)] }]);
      await b.commit([{ op: "replicaAppend", keyspaceId: "ks", head: head(6n), records: [rec(6n)] }]); // the V19 probe: "no replica image of ks to append to"
      expect((await b.load()).replicas[0]?.tail.map((r) => r.seq)).toEqual([4n, 5n, 6n]);
      await b.commit([{ op: "replicaExtend", keyspaceId: "ks", head: head(6n, ["y/"]), entries: [kentry("y/1")] }]); // the count of the pages is the database's
      expect((await b.load()).replicas[0]?.base.entries.map((e) => e.key)).toEqual(["x/1", "y/1"]);
      // B cold-loads the keyspace again, in epoch 2, with fewer pages than the old image had
      await b.commit([{ op: "replicaBegin", keyspaceId: "ks", head: { ...head(2n), logEpoch: 2 }, base: { manifestVersion: 1, logEpoch: 2, checkpointSeq: 2n, entries: [kentry("z/1")] } }]);
      const img = (await (await open(f, "p")).load()).replicas[0];
      expect(img?.base.entries.map((e) => e.key)).toEqual(["z/1"]);
      expect(img?.tail).toEqual([]); // the tail rows of epoch 1 are gone: a restart installs nothing that is no longer in the state
    });

    it("the bounds are the database's: a commit that would pass them is refused though this tab's own index was empty (14 [CLI-84])", async () => {
      const f = new FakeIDBFactory();
      const b = await open(f, "p", { maxOutbox: 2 });
      const a = await open(f, "p", { maxOutbox: 2 });
      await a.commit([{ op: "putEntry", entry: entry("a", 1) }, { op: "putEntry", entry: entry("b", 2) }]);
      await expect(b.commit([{ op: "putEntry", entry: entry("c", 3) }])).rejects.toBeInstanceOf(StoreFull);
      expect((await b.load()).entries.map((e) => e.mutationId)).toEqual(["a", "b"]);
      expect(b.full()).toEqual(["outbox"]);
      // an acknowledgement by A frees the room, and B sees it at its next commit
      await a.commit([{ op: "deleteEntry", mutationId: "a" }]);
      await b.commit([{ op: "putEntry", entry: entry("c", 3) }]);
      expect((await a.load()).entries.map((e) => e.mutationId)).toEqual(["b", "c"]);
    });

    it("a header that a commit reads, and a failure of its transaction, leave this tab's bounds as the last successful read had them", async () => {
      const f = new FakeIDBFactory();
      const a = await open(f, "p", { maxDeadLetters: 1 });
      const dl = (id: string): DeadLetterEntry => ({ mutationId: id, keyspaceId: "ks", opType: "Set", content: new Uint8Array([1]), author: { issuer: "i", sub: "s", deviceId: "d" }, reject: undefined as never, reason: "X", attempts: 1, firstFailureMs: 1, lastFailureMs: 1, failedDependents: [] });
      await a.commit([{ op: "putDeadLetter", entry: dl("x") }]);
      f.failTransactions = 1;
      await expect(a.commit([{ op: "ackDeadLetter", mutationId: "x" }])).rejects.toThrow(/aborted/);
      expect(a.full()).toEqual(["deadLetters"]);
    });
  });

  describe(`V14 S4: a follower reads the heads, and the pages of a keyspace only when its image moved (${mode})`, () => {
    it("loadLight has every store but the pages and the tails; loadImage reads one keyspace, its pages or not, its tail from a seq", async () => {
      const f = new FakeIDBFactory();
      const a = await open(f, "p");
      await a.commit([
        { op: "putEntry", entry: entry("a") },
        { op: "setMeta", key: "k", value: 7 },
        { op: "replicaBegin", keyspaceId: "ks", head: head(3n), base: { manifestVersion: 1, logEpoch: 1, checkpointSeq: 3n, entries: [kentry("x/1"), kentry("x/2")] } },
        { op: "replicaAppend", keyspaceId: "ks", head: head(6n), records: [rec(4n), rec(5n), rec(6n)] },
        { op: "replicaBegin", keyspaceId: "ksb", head: head(1n), base: { manifestVersion: 1, logEpoch: 1, checkpointSeq: 1n, entries: [kentry("b/1")] } },
        { op: "replicaAppend", keyspaceId: "ksb", head: head(2n), records: [rec(2n)] },
      ]);
      const light = await a.loadLight();
      expect(light.entries.map((e) => e.mutationId)).toEqual(["a"]);
      expect(light.meta).toEqual({ k: 7 });
      expect(light.replicas.map((r) => [r.keyspaceId, r.entryCount, r.head.seq, r.base.checkpointSeq])).toEqual([["ks", 2, 6n, 3n], ["ksb", 1, 2n, 1n]]);
      expect(light.replicas.every((r) => !("entries" in r.base))).toBe(true);
      const whole = await a.loadImage("ks");
      expect(whole?.base.entries.map((e) => e.key)).toEqual(["x/1", "x/2"]); // none of ksb's rows
      expect(whole?.tail.map((r) => r.seq)).toEqual([4n, 5n, 6n]);
      const part = await a.loadImage("ks", { entries: false, tailAfter: 4n });
      expect(part?.base.entries).toEqual([]);
      expect(part?.tail.map((r) => r.seq)).toEqual([5n, 6n]);
      expect(await a.loadImage("nope")).toBeUndefined();
      expect((await a.load()).replicas.map((r) => r.keyspaceId)).toEqual(["ks", "ksb"]);
    });
  });
}

describe("V19 SHOULD-8: a sign-out is not kept waiting by another tab that holds the database open", () => {
  const key = { origin: "o", appId: "a", partition: "i|s" };

  it("another tab's connection closes at the versionchange, the deletion goes through, and that tab's storage refuses what follows", async () => {
    const f = new FakeIDBFactory();
    const tabA = new IndexedDbStorageProvider(f);
    const tabB = new IndexedDbStorageProvider(f);
    const a = (await tabA.open(key)) as IndexedDbStorage;
    const b = (await tabB.open(key)) as IndexedDbStorage;
    await a.commit([{ op: "putEntry", entry: entry("a") }]);
    const name = [...f.dbs.keys()].find((k) => k !== "abcp:registry") as string;
    await tabA.destroy(key); // resolves: B yielded
    expect(f.dbs.has(name)).toBe(false);
    expect(await tabA.list("o", "a")).toEqual([]);
    expect(b.isClosed).toBe(true);
    await expect(b.commit([{ op: "putEntry", entry: entry("b") }])).rejects.toThrow(/closed/);
    await expect(b.load()).rejects.toThrow(/closed/);
    // the partition opens again, empty, in the tab that asks
    const fresh = (await tabB.open(key)) as IndexedDbStorage;
    expect(fresh).not.toBe(b);
    expect((await fresh.load()).entries).toEqual([]);
  });

  it("a connection that does not yield (a tab of a build without the handler) makes the sign-out fail after a wait, never hang; the deletion completes when it closes", async () => {
    const f = new FakeIDBFactory();
    const tabA = new IndexedDbStorageProvider(f, {}, undefined, { blockedWaitMs: 20 });
    await tabA.open(key);
    const name = [...f.dbs.keys()].find((k) => k !== "abcp:registry") as string;
    const stubborn = f.open(name);
    const conn = await new Promise<{ close(): void; onversionchange?: unknown }>((resolve) => {
      stubborn.onsuccess = (): void => resolve(stubborn.result);
    });
    conn.onversionchange = null; // never closes
    await expect(tabA.destroy(key)).rejects.toThrow(/keeps the database .* open/);
    conn.close(); // the other tab goes away: the deletion that the browser kept pending goes through
    await new Promise((r) => setTimeout(r, 0));
    expect(f.dbs.has(name)).toBe(false);
  });
});
