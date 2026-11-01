// C5: the Node platform (src/platform): the JSON form of the stored data, the in-process lock, the file storage (all or none, a torn tail, bounds, the
// lock of a directory), and two engines of one process on one `NodeDevice` (the flusher lock, `BroadcastChannel` and the files, with the engine of C3).

import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync, appendFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Engine, jsonBytes, MemoryStorage, StaticManifests, StoreFull, set, type OutboxEntry, type StorageOp } from "../src/engine/index.js";
import { FakeServer, ManualClock, SeededRandom } from "../src/fake/index.js";
import { FileStorage, FileStorageProvider } from "../src/platform/file-storage.js";
import { InProcessLocks } from "../src/platform/locks.js";
import { NodeDevice, createNodeHost } from "../src/platform/node.js";
import { pack, parse, stringify, unpack } from "../src/platform/serialize.js";
import { KS, MANIFEST } from "./helpers/engine.js";

let dir: string;
beforeEach(() => {
  dir = mkdtempSync(path.join(tmpdir(), "abpc-platform-"));
});
afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

const entry = (id: string, n = 1): OutboxEntry => ({
  mutationId: id,
  keyspaceId: "ks",
  lane: "SEQUENCED",
  opType: "Set",
  content: new Uint8Array([1, 2, 3, n]),
  author: { issuer: "i", sub: "s", deviceId: "d" },
  ordinal: n,
  authoredServerMs: 1_700_000_000_000 + n,
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
  outcome: { kind: "appended", seq: 123456789012345678n, logEpoch: 2, overwroteNewer: [1, 2] },
});

describe("the JSON form of stored data", () => {
  it("keeps bigint, bytes, undefined members, -0 and the numbers that JSON has no text for", () => {
    const v = { a: 1n << 62n, b: new Uint8Array([0, 255, 7]), c: undefined, d: [undefined, 1, null, "x"], e: -0, f: NaN, g: Infinity, h: { nested: new Uint8Array(0) } };
    const back = parse(stringify(v)) as typeof v;
    expect(back.a).toBe(1n << 62n);
    expect([...back.b]).toEqual([0, 255, 7]);
    expect("c" in back).toBe(false);
    expect(back.d).toEqual([null, 1, null, "x"]);
    expect(Object.is(back.e, -0)).toBe(true);
    expect(back.f).toBeNaN();
    expect(back.g).toBe(Infinity);
    expect(back.h.nested).toBeInstanceOf(Uint8Array);
  });

  it("never reads what a server wrote as a tag: a map that has a key named like one comes back as it was", () => {
    const hostile = { details: { $big: "not a number", $u8: 5, $obj: { $big: "1" }, $num: "NaN", ok: "fine" }, only: { $big: "7" }, $typeName: "abpc.v2.Reject" };
    expect(unpack(JSON.parse(JSON.stringify(pack(hostile))))).toEqual(hostile);
    expect(parse(stringify({ $obj: { $obj: 5 } }))).toEqual({ $obj: { $obj: 5 } });
  });
});

describe("the in-process lock", () => {
  it("grants one holder at a time, the waiters in order, later than the release, and honours an abort", async () => {
    const locks = new InProcessLocks();
    const first = await locks.tryAcquire("l");
    expect(first?.held).toBe(true);
    expect(await locks.tryAcquire("l")).toBeUndefined();
    const order: string[] = [];
    const second = locks.acquire("l").then((l) => (order.push("second"), l));
    const ac = new AbortController();
    const aborted = locks.acquire("l", ac.signal);
    const third = locks.acquire("l").then((l) => (order.push("third"), l));
    expect(locks.waiting("l")).toBe(3);
    ac.abort();
    await expect(aborted).rejects.toMatchObject({ name: "AbortError" });
    expect(locks.waiting("l")).toBe(2);
    await first?.release();
    expect(order).toEqual([]); // the waiter is called back later than the release
    const s = await second;
    expect(order).toEqual(["second"]);
    expect(first?.held).toBe(false);
    await s.release();
    await (await third).release();
    expect(await locks.tryAcquire("l")).toBeDefined();
    await expect(locks.acquire("x", AbortSignal.abort())).rejects.toMatchObject({ name: "AbortError" });
  });
});

describe("the file storage", () => {
  const putAll = (ids: string[]): StorageOp[] => ids.map((id, i) => ({ op: "putEntry", entry: entry(id, i + 1) }));

  it("keeps a commit across a restart, byte for byte, and a purge", async () => {
    const d = path.join(dir, "p");
    let s = await FileStorage.open(d);
    expect(s.mode).toBe("persisted");
    await s.commit([...putAll(["a", "b"]), { op: "setMeta", key: "horizon", value: 123n }, { op: "setMeta", key: "n", value: 7 }]);
    await s.commit([{ op: "deleteEntry", mutationId: "a" }]);
    await s.close();
    s = await FileStorage.open(d);
    const st = await s.load();
    expect(st.entries.map((e) => e.mutationId)).toEqual(["b"]);
    expect(st.entries[0]).toEqual(entry("b", 2));
    expect(st.meta).toEqual({ horizon: 123n, n: 7 });
    await s.commit([{ op: "purgeAll" }]);
    await s.close();
    const empty = await FileStorage.open(d);
    expect((await empty.load()).entries).toEqual([]);
    await empty.close();
  });

  it("applies a commit whole or not at all: a refused commit writes nothing, a line cut by a crash is not part of the state", async () => {
    const d = path.join(dir, "q");
    let s = await FileStorage.open(d, { maxOutbox: 2 });
    await s.commit(putAll(["a", "b"]));
    expect(s.full()).toEqual(["outbox"]);
    const before = readFileSync(path.join(d, "journal.jsonl"), "utf8");
    await expect(s.commit(putAll(["c"]))).rejects.toBeInstanceOf(StoreFull);
    expect(readFileSync(path.join(d, "journal.jsonl"), "utf8")).toBe(before);
    await s.close();
    // a crash in the middle of a commit: the journal ends in a part of a line, which has no end of line (a line that has its end of line and fails its
    // check is damage, not a crash's, and the partition is refused: see platform-file.test.ts)
    appendFileSync(path.join(d, "journal.jsonl"), '12345678 {"half');
    s = await FileStorage.open(d, { maxOutbox: 2 });
    expect((await s.load()).entries.map((e) => e.mutationId).sort()).toEqual(["a", "b"]);
    await s.commit([{ op: "deleteEntry", mutationId: "a" }]); // the next commit does not follow garbage
    await s.close();
    s = await FileStorage.open(d);
    expect((await s.load()).entries.map((e) => e.mutationId)).toEqual(["b"]);
    await s.close();
  });

  it("stores a persisted replica as the engine writes it, and serves the same state as MemoryStorage after a restart", async () => {
    const ops: StorageOp[] = [
      { op: "putEntry", entry: entry("x") },
      {
        op: "replicaBegin",
        keyspaceId: "ks",
        head: { logEpoch: 1, seq: 5n, watermark: 5n, lastRecordSeq: 5n, manifestVersion: 1, kind: "team", lifecycle: 1, successors: [], interest: [], lastSyncMs: 99 },
        base: { manifestVersion: 1, logEpoch: 1, checkpointSeq: 5n, entries: [] },
      },
    ];
    const mem = new MemoryStorage();
    const file = await FileStorage.open(path.join(dir, "r"));
    await mem.commit(ops);
    await file.commit(ops);
    await file.close();
    const again = await FileStorage.open(path.join(dir, "r"));
    expect(await again.load()).toEqual(await mem.load());
    await again.close();
  });

  it("is one process's: a partition that another live process has open is refused, and the lock of a process that is gone is taken over", async () => {
    const provider = new FileStorageProvider(path.join(dir, "root"));
    const key = { origin: "o", appId: "a", partition: "p" };
    const s = await provider.open(key);
    expect(await provider.open(key)).toBe(s); // two opens in one process are one store
    const second = new FileStorageProvider(path.join(dir, "root"));
    await expect(second.open(key)).rejects.toThrow(/open in this process under another provider/);
    expect((await provider.list("o", "a")).map((k) => k.partition)).toEqual(["p"]);
    expect(await provider.list("o", "other")).toEqual([]);
    await provider.close();
    // a lock whose holder is another live process (this one's parent)
    const lock = path.join(dir, "root", readdirOnly(path.join(dir, "root")), "LOCK");
    writeFileSync(lock, String(process.ppid));
    await expect(new FileStorageProvider(path.join(dir, "root")).open(key)).rejects.toThrow(/another process/);
    // a lock whose holder is gone
    writeFileSync(lock, "2147483646");
    const taken = new FileStorageProvider(path.join(dir, "root"));
    await taken.open(key);
    await taken.destroy(key);
    expect(existsSync(path.join(dir, "root", readdirOnly(path.join(dir, "root"), true)))).toBe(false);
    expect(await taken.list("o", "a")).toEqual([]);
  });

  it("rewrites a journal that has grown to a snapshot of its state", async () => {
    const d = path.join(dir, "big");
    const s = await FileStorage.open(d);
    const big = (n: number): StorageOp => ({ op: "putEntry", entry: { ...entry("a"), content: new Uint8Array(512 * 1024).fill(n) } });
    for (let i = 0; i < 24; i++) await s.commit([big(i)]); // 24 versions of one 512 KiB entry: 17 MB of journal, if it were never rewritten, for 0.7 MB of state
    const size = readFileSync(path.join(d, "journal.jsonl")).length;
    expect(size).toBeLessThan(8 * 1024 * 1024);
    await s.close();
    const again = await FileStorage.open(d);
    expect((await again.load()).entries[0]?.content[0]).toBe(23);
    await again.close();
  }, 30_000);
});

function readdirOnly(root: string, allowNone = false): string {
  const names = existsSyncDirs(root);
  if (names.length === 0 && allowNone) return "gone";
  expect(names).toHaveLength(1);
  return names[0] as string;
}
function existsSyncDirs(root: string): string[] {
  return readdirSync(root);
}

describe("two engines of one process on one NodeDevice", () => {
  /** An engine on a tab of the device, over the fake server; the manual clock never moves, so only what needs no timer is asserted. */
  async function open(device: NodeDevice, server: FakeServer, clock: ManualClock, manifests: StaticManifests, seed: number) {
    const principal = server.principal("ana", "ana-device");
    const tab = device.newTab();
    const engine = await Engine.create({
      appId: "app",
      principal: { issuer: principal.issuer, sub: principal.sub, deviceId: principal.deviceId },
      transport: server.transport(principal),
      manifests,
      clock,
      random: new SeededRandom(seed),
      origin: "https://abpc.test",
      tab,
    });
    await engine.addKeyspace(KS);
    engine.start();
    return { tab, engine };
  }
  const until = async (cond: () => boolean, what: string): Promise<void> => {
    const end = Date.now() + 10_000;
    while (!cond()) {
      if (Date.now() > end) throw new Error(`timed out: ${what}`);
      await new Promise((r) => setTimeout(r, 10));
    }
  };

  it("elect one flusher by the lock, carry the follower's action over BroadcastChannel, and keep the outbox in files across a restart", async () => {
    const clock = new ManualClock();
    const manifests = new StaticManifests(MANIFEST);
    const server = new FakeServer({ manifests, clock, heartbeatMs: 15_000 });
    server.createKeyspaceNow(KS);
    const device = new NodeDevice({ directory: path.join(dir, "device") });
    const a = await open(device, server, clock, manifests, 1);
    const b = await open(device, server, clock, manifests, 2);
    await until(() => a.engine.status().role === "flusher" && b.engine.status().role === "follower", "one flusher and one follower");
    expect(a.engine.status().storageMode).toBe("persisted");
    // the follower's action is the flusher's to author and to store: it reaches it over the channel, and both see the confirmed value
    const w = await b.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/x", "from the follower")] });
    expect(w.mutationId).toBeTruthy();
    await until(() => a.engine.confirmed(KS).has("settings/x"), "the flusher committed the follower's action");
    await until(() => b.engine.view(KS).has("settings/x"), "the follower sees it");
    expect(new TextDecoder().decode(a.engine.confirmed(KS).get("settings/x")?.value)).toBe(new TextDecoder().decode(jsonBytes("from the follower")));
    // the flusher stops: the lock goes to the follower, which becomes the flusher
    await a.engine.stop();
    await until(() => b.engine.status().role === "flusher", "the follower took the lock");
    await b.engine.stop();
    a.tab.close();
    b.tab.close();
    await device.close();
    // a restart: a new device on the same directory has the replica that the engines persisted
    const device2 = new NodeDevice({ directory: path.join(dir, "device") });
    const c = await open(device2, server, clock, manifests, 3);
    await until(() => c.engine.status().role === "flusher", "the restarted engine is the flusher");
    expect(c.engine.status().storageMode).toBe("persisted");
    expect(c.engine.keyspaceInfo(KS)?.loaded).toBe(true); // restored from the files, before any load
    expect(c.engine.confirmed(KS).has("settings/x")).toBe(true);
    await c.engine.stop();
    c.tab.close();
    await device2.close();
  }, 30_000);

  it("a host made alone is a tab of its own device, with the storage in memory when it has no directory", () => {
    const h = createNodeHost();
    expect(h.locks.reliable).toBe(true);
    expect(h.visible()).toBe(true);
    expect(h.tabId).toMatch(/^[0-9a-f-]{36}$/);
    const ch1 = h.channel("c");
    const got: unknown[] = [];
    const other = createNodeHost().channel("c");
    other.onMessage((m) => got.push(m));
    ch1.post({ hello: 1n });
    ch1.close();
    other.close();
    expect(createNodeHost({ tabId: "t1" }).tabId).toBe("t1");
  });
});
