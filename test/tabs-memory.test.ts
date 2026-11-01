// The in-memory tab platform (`engine/tabs-memory.ts`): the test double that simulates two tabs or processes on one device. What the engine's
// suites of 14 §14.8 assume of it holds here: one exclusive lock per name, granted in order and later than the release; a crashed tab
// frees its lock; a broadcast reaches the other tabs only, later, as a copy; storage is one store per partition.

import { describe, expect, it } from "vitest";
import { MemoryDevice, lockName, partitionId, sameKey, type PartitionKey } from "../src/engine/index.js";

const key = (partition: string, extra: Partial<PartitionKey> = {}): PartitionKey => ({ origin: "https://x.test", appId: "app", partition, ...extra });
const flush = (): Promise<void> => new Promise((r) => setImmediate(r));

describe("14 [CLI-61]: the cross-tab lock", () => {
  it("is exclusive: a tab that asks while another holds it waits, and `tryAcquire` says no", async () => {
    const d = new MemoryDevice();
    const a = d.newTab();
    const b = d.newTab();
    const la = await a.locks.tryAcquire("n");
    expect(la?.held).toBe(true);
    expect(await b.locks.tryAcquire("n")).toBeUndefined();
    let got = false;
    void b.locks.acquire("n").then(() => (got = true));
    await flush();
    expect(got).toBe(false);
    expect(d.lockManager.holder("n")).toBe(a.tabId);
    expect(d.lockManager.waiting("n")).toBe(1);
    await la?.release();
    expect(la?.held).toBe(false);
    expect(got).toBe(false); // granted later than the release, never inside it
    await flush();
    expect(got).toBe(true);
    expect(d.lockManager.holder("n")).toBe(b.tabId);
  });

  it("grants the waiting tabs in the order that they asked; separate names are separate locks", async () => {
    const d = new MemoryDevice();
    const [a, b, c, e] = [d.newTab(), d.newTab(), d.newTab(), d.newTab()];
    const first = await a.locks.acquire("n");
    const order: string[] = [];
    const lb = b.locks.acquire("n").then((l) => (order.push("b"), l));
    const lc = c.locks.acquire("n").then((l) => (order.push("c"), l));
    expect(await e.locks.tryAcquire("other")).toBeDefined();
    await first.release();
    await flush();
    expect(order).toEqual(["b"]);
    await (await lb).release();
    await flush();
    expect(order).toEqual(["b", "c"]);
    await (await lc).release();
  });

  it("frees the lock of a tab that crashes, and drops its requests; the next tab in line is granted it", async () => {
    const d = new MemoryDevice();
    const [a, b, c] = [d.newTab(), d.newTab(), d.newTab()];
    const la = await a.locks.acquire("n");
    let bErr: unknown;
    void b.locks.acquire("n").catch((e: unknown) => (bErr = e));
    let cGot = false;
    void c.locks.acquire("n").then(() => (cGot = true));
    await flush();
    b.crash(); // a waiting tab goes away: its request is dropped
    await flush();
    expect((bErr as Error).name).toBe("AbortError");
    a.crash(); // the holder crashes: nothing is released by it
    expect(la.held).toBe(false);
    await flush();
    expect(cGot).toBe(true);
    expect(d.lockManager.holder("n")).toBe(c.tabId);
    await expect(a.locks.acquire("n")).rejects.toMatchObject({ name: "AbortError" });
    expect(await a.locks.tryAcquire("m")).toBeUndefined(); // a crashed tab gets nothing
  });

  it("a request can be aborted before it is granted", async () => {
    const d = new MemoryDevice();
    const [a, b] = [d.newTab(), d.newTab()];
    await a.locks.acquire("n");
    const ac = new AbortController();
    const p = b.locks.acquire("n", ac.signal);
    ac.abort();
    await expect(p).rejects.toMatchObject({ name: "AbortError" });
    expect(d.lockManager.waiting("n")).toBe(0);
    await expect(b.locks.acquire("n", ac.signal)).rejects.toMatchObject({ name: "AbortError" }); // already aborted
  });

  it("a platform with no reliable lock says so", () => {
    expect(new MemoryDevice().newTab({ reliableLocks: false }).locks.reliable).toBe(false);
    expect(new MemoryDevice().newTab().locks.reliable).toBe(true);
  });
});

describe("the broadcast between the tabs of a device", () => {
  it("reaches every other open tab of the same channel, later, as a copy, and never the sender", async () => {
    const d = new MemoryDevice();
    const [a, b, c] = [d.newTab(), d.newTab(), d.newTab()];
    const ca = a.channel("ch");
    const cb = b.channel("ch");
    const cc = c.channel("other");
    const got: string[] = [];
    const sent = { n: 1, big: 10n };
    ca.onMessage(() => got.push("a"));
    cb.onMessage((m) => {
      got.push("b");
      expect(m).toEqual(sent);
      expect(m).not.toBe(sent);
    });
    cc.onMessage(() => got.push("c"));
    ca.post(sent);
    expect(got).toEqual([]); // asynchronous
    await flush();
    expect(got).toEqual(["b"]);
    cb.close();
    ca.post({});
    await flush();
    expect(got).toEqual(["b"]); // a closed channel hears nothing
  });

  it("a crashed tab's channels close with it", async () => {
    const d = new MemoryDevice();
    const [a, b] = [d.newTab(), d.newTab()];
    const ca = a.channel("ch");
    const cb = b.channel("ch");
    let heard = 0;
    cb.onMessage(() => heard++);
    a.crash();
    ca.post({});
    await flush();
    expect(heard).toBe(0);
  });

  it("a tab's visibility is its own, with listeners", () => {
    const d = new MemoryDevice();
    const a = d.newTab({ visible: false });
    expect(a.visible()).toBe(false);
    const seen: boolean[] = [];
    const off = a.onVisibility((v) => seen.push(v));
    a.setVisible(true);
    a.setVisible(true);
    off();
    a.setVisible(false);
    expect(seen).toEqual([true]);
  });
});

describe("14 [CLI-59], [CLI-74]: storage is one store per partition of an origin and an app", () => {
  it("two opens of one key are one store; keys differ by origin, app, partition and tab; `list` and `destroy` follow", async () => {
    const d = new MemoryDevice();
    const s1 = await d.storage.open(key("u1"));
    const s1b = await d.storage.open(key("u1"));
    const s2 = await d.storage.open(key("u2"));
    const o2 = await d.storage.open(key("u1", { origin: "https://y.test" }));
    const t1 = await d.storage.open(key("u1", { tab: "tab7" }));
    expect(s1b).toBe(s1);
    expect(new Set([s1, s2, o2, t1]).size).toBe(4);
    expect((await d.storage.list("https://x.test", "app")).map((k) => `${k.partition}${k.tab ?? ""}`).sort()).toEqual(["u1", "u1tab7", "u2"]);
    await d.storage.destroy(key("u1"));
    expect((await d.storage.list("https://x.test", "app")).map((k) => `${k.partition}${k.tab ?? ""}`).sort()).toEqual(["u1tab7", "u2"]);
    expect(await d.storage.open(key("u1"))).not.toBe(s1); // a new, empty store
  });

  it("names: a partition id and a lock name are keyed by origin, app and partition, and the key comparison includes the tab", () => {
    expect(partitionId(key("u"))).not.toBe(partitionId(key("u", { tab: "t" })));
    expect(lockName(key("u"))).toBe(lockName(key("u", { tab: "t" }))); // one lock for the tabs of one partition: a tab with an outbox of its own has none
    expect(lockName(key("u"))).not.toBe(lockName(key("v")));
    expect(sameKey(key("u"), key("u"))).toBe(true);
    expect(sameKey(key("u"), key("u", { tab: "t" }))).toBe(false);
  });
});
