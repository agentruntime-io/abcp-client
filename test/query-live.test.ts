// The local query layer (C4): live queries. Each update is a set of rows added, changed and removed, computed from the engine's change feed (engine/view-feed.ts):
// 14 [CLI-6] (the view is recomputed when confirmed state advances or the outbox changes), [CLI-2] (confirmed state is fed by loads and records), 05 [STATE-43] (a subtree
// deletion), 14 §14.10 [CLI-71], [CLI-72] (the interest), [CLI-66] (a purge), [CLI-13] (a CRDT key's view document is rebuilt), 09 [READ-54] (heartbeats).

import { describe, expect, it } from "vitest";
import * as Y from "yjs";
import { create } from "@bufbuild/protobuf";
import { Code, ChangeKind, KeyChangeSchema } from "../src/gen/abpc/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { del, set } from "../src/engine/index.js";
import { goOffline, goOnline, kc, rejectResult } from "./helpers/engine.js";
import { createQueries, type QueryEngine } from "../src/query/index.js";
import { drop, jsonOf, keysOf, KS, put, qrig, watch, type QRig } from "./helpers/query.js";

describe("the first update and the snapshot", () => {
  it("the listener gets every row added at once, and the snapshot is what a one-shot query gives", async () => {
    const r = await qrig();
    put(r, { "item/b": 2, "item/a": 1 });
    await r.engine.settle();
    const query = { keyspaceId: KS, select: { prefix: "item/" } } as const;
    const w = watch(r, query);
    expect(w.updates).toHaveLength(1);
    expect(w.updates[0]?.cause).toBe("initial");
    expect(w.updates[0]?.added.map((x) => x.key)).toEqual(["item/a", "item/b"]);
    expect(w.live.snapshot().rows).toEqual(r.q.run(query).rows);
    expect(w.updates[0]?.result.rows).toEqual(r.q.run(query).rows);
    expect(w.live.stats).toMatchObject({ fullRuns: 1, keyEvals: 0, updates: 1 });
  });

  it("a keyspace that is not followed yet says so, and the rows come when it is (14 [CLI-70])", async () => {
    const r = await qrig({ keyspaces: [], start: false });
    r.server.createKeyspaceNow(KS);
    put(r, { "item/a": 1 });
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    expect(w.live.snapshot().keyspace.status).toBe("unfollowed");
    expect(w.live.snapshot().rows).toEqual([]);
    await r.engine.addKeyspace(KS);
    expect(w.live.snapshot().keyspace.status).toBe("loading"); // followed, not loaded: nothing is known, nothing is absent
    r.engine.start();
    await r.engine.settle();
    expect(w.live.snapshot().keyspace.status).toBe("ready");
    expect(w.updates.at(-1)?.added.map((x) => x.key)).toEqual(["item/a"]);
    expect(w.updates.at(-1)?.cause).toBe("refresh");
  });
});

describe("added, changed and removed rows, from records that arrive", () => {
  it("another principal's writes: a key in scope is added, then changed, then removed; before and after are the rows", async () => {
    const r = await qrig();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    put(r, { "item/a": 1 });
    await r.engine.settle();
    put(r, { "item/a": 2 });
    await r.engine.settle();
    drop(r, "item/a");
    await r.engine.settle();
    expect(w.log()).toEqual(["+item/a", "~item/a", "-item/a"]);
    const [add, chg, rem] = w.after();
    expect(jsonOf(add?.added[0])).toBe(1n);
    expect(chg?.changed[0] && [jsonOf(chg.changed[0].before), jsonOf(chg.changed[0].after)]).toEqual([1n, 2n]);
    expect(chg?.changed[0]?.after.version).toBeGreaterThan(chg?.changed[0]?.before.version ?? 0n);
    expect(jsonOf(rem?.removed[0])).toBe(2n);
    expect(w.updates.every((u) => u.cause === "initial" || u.cause === "change")).toBe(true);
  });

  it("one record that changes several keys is one update", async () => {
    const r = await qrig();
    put(r, { "item/a": 1, "item/b": 1 });
    await r.engine.settle();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    r.server.write(KS, [kc("item/a", "set", 2), kc("item/b", "delete"), kc("item/c", "set", 3)]);
    await r.engine.settle();
    expect(w.after()).toHaveLength(1);
    expect(w.log().sort()).toEqual(["+item/c", "-item/b", "~item/a"]);
  });

  it("a template query follows its keys and their bindings", async () => {
    const r = await qrig();
    const w = watch(r, { keyspaceId: KS, select: { template: "issue/{i}/title" } });
    put(r, { "issue/1/title": "one", "issue/1/meta": 5, "issue/2/title": "two" });
    await r.engine.settle();
    expect(w.updates[1]?.added.map((x) => [x.key, x.bindings])).toEqual([
      ["issue/1/title", { i: "1" }],
      ["issue/2/title", { i: "2" }],
    ]);
    put(r, { "issue/1/meta": 6 }); // a key that the template does not match
    await r.engine.settle();
    expect(w.after()).toHaveLength(1);
  });

  it("a key query follows its keys, and tells of one that arrives", async () => {
    const r = await qrig();
    const w = watch(r, { keyspaceId: KS, select: { keys: ["item/a", "item/b"] } });
    put(r, { "item/b": 1, "item/z": 1 });
    await r.engine.settle();
    expect(w.log()).toEqual(["+item/b"]);
  });

  it("a row whose value stops passing the filter is removed, and one that starts passing is added", async () => {
    const r = await qrig();
    put(r, { "item/a": { n: 1 }, "item/b": { n: 5 } });
    await r.engine.settle();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" }, filter: { path: "n", op: "gte", value: 3 } });
    expect(w.updates[0]?.added.map((x) => x.key)).toEqual(["item/b"]);
    put(r, { "item/a": { n: 4 }, "item/b": { n: 0 } });
    await r.engine.settle();
    expect(w.log().sort()).toEqual(["+item/a", "-item/b"]);
    // a change that keeps the row out is not an update
    put(r, { "item/b": { n: 1 } });
    await r.engine.settle();
    expect(w.after()).toHaveLength(1);
  });

  it("an update arrives in the order of the query: the snapshot is sorted, ascending or descending", async () => {
    const r = await qrig();
    const asc = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    const desc = watch(r, { keyspaceId: KS, select: { prefix: "item/" }, order: "desc" });
    for (const k of ["m", "c", "x", "a", "q"]) {
      put(r, { [`item/${k}`]: 1 });
      await r.engine.settle();
    }
    drop(r, "item/m");
    await r.engine.settle();
    expect(keysOf(asc.live.snapshot())).toEqual(["item/a", "item/c", "item/q", "item/x"]);
    expect(keysOf(desc.live.snapshot())).toEqual(["item/x", "item/q", "item/c", "item/a"]);
  });
});

describe("no spurious updates, and no work for a change that touches no key of the query", () => {
  it("a change to keys outside the query's scopes costs it nothing: no update, no key judged, no evaluation", async () => {
    const r = await qrig();
    put(r, { "item/a": 1, "setting/x": 1 });
    await r.engine.settle();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    const t = watch(r, { keyspaceId: KS, select: { template: "issue/{i}/title" } });
    const before = { ...w.live.stats };
    const tBefore = { ...t.live.stats };
    put(r, { "setting/x": 2, "setting/y": 1 });
    drop(r, "setting/y");
    await r.engine.settle();
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("setting/z", 1)] });
    await r.engine.settle();
    expect(w.live.stats).toEqual(before);
    expect(t.live.stats).toEqual(tBefore);
    expect(w.after()).toEqual([]);
    expect(t.after()).toEqual([]);
  });

  it("a key in scope that the query does not select is judged and no more: the filter keeps it out, and nothing is told", async () => {
    const r = await qrig();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" }, filter: { path: "n", op: "eq", value: 1 } });
    put(r, { "item/a": { n: 2 } });
    await r.engine.settle();
    expect(w.after()).toEqual([]);
    expect(w.live.stats.fullRuns).toBe(1);
    expect(w.live.stats.keyEvals).toBe(1);
  });

  it("a change judged one key at a time never evaluates the whole query again", async () => {
    const r = await qrig();
    const many: Record<string, number> = {};
    for (let i = 0; i < 200; i++) many[`item/k${i}`] = i;
    put(r, many);
    await r.engine.settle();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    for (let i = 0; i < 10; i++) {
      put(r, { [`item/k${i}`]: 1000 + i });
      await r.engine.settle();
    }
    expect(w.live.stats.fullRuns).toBe(1);
    expect(w.live.stats.keyEvals).toBe(10);
    expect(w.after()).toHaveLength(10);
    expect(w.live.snapshot().rows).toHaveLength(200);
  });

  it("heartbeats and delivery watermarks move the cursor and tell nothing (09 [READ-54])", async () => {
    const r = await qrig({ heartbeatMs: 1000 });
    put(r, { "item/a": 1 });
    await r.engine.settle();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    const before = { ...w.live.stats };
    await r.clock.advance(10_000);
    await r.engine.settle();
    expect(w.live.stats).toEqual(before);
    expect(w.after()).toEqual([]);
  });

  it("a pending action is no part of a confirmed query: it is told nothing when it is submitted, and one change when the record is confirmed", async () => {
    const r = await qrig();
    put(r, { "item/a": "old" });
    await r.engine.settle();
    await goOffline(r);
    const conf = watch(r, { keyspaceId: KS, mode: "confirmed", select: { prefix: "item/" } });
    const pred = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("item/a", "mine"), set("item/b", "new")] });
    expect(conf.after()).toEqual([]);
    expect(conf.live.stats.keyEvals).toBe(0);
    expect(pred.log().sort()).toEqual(["+item/b", "~item/a"]);
    await goOnline(r);
    await r.engine.settle();
    expect(conf.log().sort()).toEqual(["+item/b", "~item/a"]);
    const confRow = r.q.get(KS, "item/a", { mode: "confirmed" });
    expect(confRow.status === "present" && jsonOf(confRow.row)).toBe("mine");
  });

  it("a submit that changes nothing a query shows (a rejected one, on a key it cannot see) is no update", async () => {
    const r = await qrig();
    await goOffline(r);
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("setting/a", 1)] });
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.mutationId === a.mutationId ? rejectResult(m, mk(Code.RANGE_EXCEEDED)) : undefined) });
    await goOnline(r);
    await r.engine.settle();
    expect(w.after()).toEqual([]);
    expect(w.live.stats.keyEvals).toBe(0);
  });
});

describe("a subtree deletion removes every row under it (05 [STATE-43], 14 [CLI-14])", () => {
  it("a record that deletes a marker key removes the rows of its subtree from every kind of query, in one update", async () => {
    const r = await qrig();
    put(r, { "issue/1": {}, "issue/1/title": "t", "issue/1/comment/2": {}, "issue/1/comment/2/text": "c", "issue/2": {}, "issue/2/title": "u" });
    await r.engine.settle();
    const pre = watch(r, { keyspaceId: KS, select: { prefix: "issue/1/" } });
    const tpl = watch(r, { keyspaceId: KS, select: { template: "issue/{i}/comment/{c}/text" } });
    const ent = watch(r, { keyspaceId: KS, select: { entities: "issue/{i}", subtree: true } });
    const keys = watch(r, { keyspaceId: KS, select: { keys: ["issue/1/title", "issue/2/title"] } });
    drop(r, "issue/1");
    await r.engine.settle();
    expect(pre.after()).toHaveLength(1);
    expect(pre.after()[0]?.removed.map((x) => x.key)).toEqual(["issue/1/comment/2", "issue/1/comment/2/text", "issue/1/title"]);
    expect(tpl.log()).toEqual(["-issue/1/comment/2/text"]);
    expect(ent.log()).toEqual(["-issue/1"]);
    expect(keys.log()).toEqual(["-issue/1/title"]);
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "" } }))).toEqual(["issue/2", "issue/2/title"]);
  });

  it("a pending DELETE of a marker key hides the subtree from a predicted query and not from a confirmed one; taking it back shows the rows again", async () => {
    const r = await qrig();
    put(r, { "issue/1": {}, "issue/1/title": "t", "issue/2": {} });
    await r.engine.settle();
    await goOffline(r);
    const pred = watch(r, { keyspaceId: KS, select: { prefix: "issue/" } });
    const conf = watch(r, { keyspaceId: KS, mode: "confirmed", select: { prefix: "issue/" } });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [del("issue/1")] });
    expect(pred.after()[0]?.removed.map((x) => x.key).sort()).toEqual(["issue/1", "issue/1/title"]);
    expect(conf.after()).toEqual([]);
    await r.engine.cancel(a.mutationId);
    expect(pred.after()[1]?.added.map((x) => x.key).sort()).toEqual(["issue/1", "issue/1/title"]);
    expect(conf.after()).toEqual([]);
    expect(keysOf(pred.live.snapshot())).toEqual(["issue/1", "issue/1/title", "issue/2"]);
  });

  it("an entity query with a subtree changes its row when a key below changes, and not when another entity's does", async () => {
    const r = await qrig();
    put(r, { "issue/1": {}, "issue/1/title": "t", "issue/2": {}, "issue/2/title": "u" });
    await r.engine.settle();
    const w = watch(r, { keyspaceId: KS, select: { entities: "issue/{i}", subtree: true }, filter: { at: "title", op: "eq", value: "t" } });
    expect(w.updates[0]?.added.map((x) => [x.key, x.subtree?.map((s) => s.key)])).toEqual([["issue/1", ["issue/1/title"]]]);
    put(r, { "issue/1/meta": 1 });
    await r.engine.settle();
    expect(w.log()).toEqual(["~issue/1"]);
    put(r, { "issue/2/title": "t" }); // the other entity's title now passes the filter
    await r.engine.settle();
    expect(w.log()).toEqual(["~issue/1", "+issue/2"]);
    put(r, { "issue/1/title": "zz" });
    await r.engine.settle();
    expect(w.log()).toEqual(["~issue/1", "+issue/2", "-issue/1"]);
  });
});

describe("a live query with a page limit holds a window", () => {
  const seed = async (): Promise<QRig> => {
    const r = await qrig();
    put(r, { "item/b": 2, "item/d": 4, "item/f": 6, "item/h": 8 });
    await r.engine.settle();
    return r;
  };

  it("a key before the window's end pushes the last row out, a removal brings the next in, and a change after a full window is not even looked at", async () => {
    const r = await seed();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" }, limit: 2 });
    expect(keysOf(w.live.snapshot())).toEqual(["item/b", "item/d"]);
    expect(w.live.snapshot().next).toBe("item/d");
    const runs = w.live.stats.fullRuns;
    put(r, { "item/z": 1, "item/g": 1 }); // after the window: nothing
    await r.engine.settle();
    expect(w.live.stats.fullRuns).toBe(runs);
    expect(w.after()).toEqual([]);
    put(r, { "item/c": 3 }); // inside the window
    await r.engine.settle();
    expect(w.log().sort()).toEqual(["+item/c", "-item/d"]);
    expect(keysOf(w.live.snapshot())).toEqual(["item/b", "item/c"]);
    drop(r, "item/b");
    await r.engine.settle();
    expect(keysOf(w.live.snapshot())).toEqual(["item/c", "item/d"]);
    expect(w.live.snapshot().next).toBe("item/d");
  });

  it("a page after a cursor ignores the keys before it, and a window that is not full takes what is added after it", async () => {
    const r = await seed();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" }, limit: 3, after: "item/d" });
    expect(keysOf(w.live.snapshot())).toEqual(["item/f", "item/h"]);
    expect(w.live.snapshot().next).toBeUndefined();
    const runs = w.live.stats.fullRuns;
    put(r, { "item/a": 1, "item/c": 1 }); // before the cursor
    await r.engine.settle();
    expect(w.live.stats.fullRuns).toBe(runs);
    put(r, { "item/x": 1 });
    await r.engine.settle();
    expect(keysOf(w.live.snapshot())).toEqual(["item/f", "item/h", "item/x"]);
    expect(w.log()).toEqual(["+item/x"]);
  });

  it("descending, the window is the other end", async () => {
    const r = await seed();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" }, limit: 2, order: "desc" });
    expect(keysOf(w.live.snapshot())).toEqual(["item/h", "item/f"]);
    put(r, { "item/g": 1 });
    await r.engine.settle();
    expect(keysOf(w.live.snapshot())).toEqual(["item/h", "item/g"]);
    expect(w.log().sort()).toEqual(["+item/g", "-item/f"]);
  });
});

describe("interest (14 §14.10, [CLI-71], [CLI-72])", () => {
  const withInterest = async (interest: string[]): Promise<QRig> => {
    const r = await qrig({ keyspaces: [], start: false });
    r.server.createKeyspaceNow(KS);
    put(r, { "issue/1": {}, "issue/1/title": "one", "issue/2": {}, "issue/2/title": "two", "setting/a": "a" });
    await r.engine.addKeyspace(KS, { interest });
    r.engine.start();
    await r.engine.settle();
    return r;
  };

  it("a key outside the interest is not loaded, never absent; the result says how much of the query the interest covers", async () => {
    const r = await withInterest(["issue/1"]);
    expect(r.q.get(KS, "issue/2/title")).toEqual({ status: "notLoaded" });
    expect(r.q.get(KS, "setting/a")).toEqual({ status: "notLoaded" });
    expect(r.q.get(KS, "issue/1/title").status).toBe("present");
    expect(r.q.get(KS, "issue/1/nothing")).toEqual({ status: "absent" });
    const keys = r.q.run({ keyspaceId: KS, select: { keys: ["issue/1/title", "issue/2/title", "setting/a", "issue/1/x"] } });
    expect(keys.rows.map((x) => x.key)).toEqual(["issue/1/title"]);
    expect(keys.notLoaded).toEqual(["issue/2/title", "setting/a"]);
    expect(keys.coverage).toBe("partial");
    const cov = (select: Parameters<typeof r.q.run>[0]["select"]) => r.q.run({ keyspaceId: KS, select }).coverage;
    expect(cov({ prefix: "issue/1/" })).toBe("full");
    expect(cov({ prefix: "issue/" })).toBe("partial");
    expect(cov({ prefix: "" })).toBe("partial");
    expect(cov({ prefix: "setting/" })).toBe("none");
    expect(cov({ template: "issue/{i}/title" })).toBe("partial");
    expect(cov({ template: "issue/{i}/title", bind: { i: "1" } })).toBe("full");
    expect(cov({ template: "issue/{i}/title", bind: { i: "2" } })).toBe("none");
    expect(cov({ template: "setting/{name}" })).toBe("none");
    expect(cov({ entities: "issue/{i}" })).toBe("partial");
    expect(r.q.run({ keyspaceId: KS, select: { prefix: "issue/" } }).rows.map((x) => x.key)).toEqual(["issue/1", "issue/1/title"]);
  });

  it("the whole keyspace is covered when the interest is empty", async () => {
    const r = await withInterest([]);
    expect(r.q.run({ keyspaceId: KS, select: { prefix: "" } }).coverage).toBe("full");
    expect(r.q.get(KS, "setting/a").status).toBe("present");
  });

  it("an interest prefix that is an ancestor covers the key, and one that is the key's own subtree covers it", async () => {
    const r = await withInterest(["issue"]);
    expect(r.q.get(KS, "issue/2/title").status).toBe("present");
    expect(r.q.run({ keyspaceId: KS, select: { prefix: "issue/2/" } }).coverage).toBe("full");
    expect(r.q.run({ keyspaceId: KS, select: { template: "issue/{i}/title" } }).coverage).toBe("full");
  });

  it("loadNotLoaded asks the engine for the keys outside the interest, and they stay not loaded: their versions are for authoring (14 [CLI-73], 09 [READ-26])", async () => {
    const r = await withInterest(["issue/1"]);
    const res = r.q.run({ keyspaceId: KS, select: { keys: ["issue/2/title", "setting/a"] } });
    expect(res.notLoaded).toEqual(["issue/2/title", "setting/a"]);
    const loads = r.server.calls.filter((c) => c.method === "loadState").length;
    expect(await r.q.loadNotLoaded(res)).toBe(true);
    expect(r.server.calls.filter((c) => c.method === "loadState").length).toBeGreaterThan(loads);
    const asked = r.server.calls.filter((c) => c.method === "loadState").flatMap((c) => (c.request as { cursor?: { interestPrefixes: string[] } }).cursor?.interestPrefixes ?? []);
    expect(asked).toEqual(expect.arrayContaining(["issue/2/title", "setting/a"]));
    expect(r.q.get(KS, "issue/2/title")).toEqual({ status: "notLoaded" });
    expect(await r.q.loadNotLoaded(r.q.run({ keyspaceId: KS, select: { keys: ["issue/1/title"] } }))).toBe(true); // nothing to load
  });

  it("widening the interest brings the rows in, and a live query is told, with its status; narrowing takes them out again", async () => {
    const r = await withInterest(["issue/1"]);
    const keys = watch(r, { keyspaceId: KS, select: { keys: ["issue/1/title", "issue/2/title"] } });
    const pre = watch(r, { keyspaceId: KS, select: { prefix: "issue/" } });
    expect(keys.live.snapshot().notLoaded).toEqual(["issue/2/title"]);
    expect(pre.live.snapshot().coverage).toBe("partial");
    await r.engine.setInterest(KS, ["issue/1", "issue/2"]);
    await r.engine.settle();
    expect(keys.after().at(-1)?.added.map((x) => x.key)).toEqual(["issue/2/title"]);
    expect(keys.after().some((u) => u.statusChanged)).toBe(true);
    expect(keys.live.snapshot().notLoaded).toEqual([]);
    expect(pre.live.snapshot().rows.map((x) => x.key)).toEqual(["issue/1", "issue/1/title", "issue/2", "issue/2/title"]);
    expect(pre.live.snapshot().coverage).toBe("partial");
    await r.engine.setInterest(KS, ["issue"]);
    await r.engine.settle();
    expect(pre.live.snapshot().coverage).toBe("full");
    await r.engine.setInterest(KS, ["issue/2"]);
    await r.engine.settle();
    expect(pre.live.snapshot().rows.map((x) => x.key)).toEqual(["issue/2", "issue/2/title"]);
    expect(pre.after().at(-1)?.removed.map((x) => x.key)).toEqual(["issue/1", "issue/1/title"]);
    expect(keys.live.snapshot().notLoaded).toEqual(["issue/1/title"]);
    expect(keys.live.snapshot().rows.map((x) => x.key)).toEqual(["issue/2/title"]);
  });

  it("a record that names keys outside the interest does not show them (09 [READ-26]), and a live query is told only of the keys inside", async () => {
    const r = await withInterest(["issue/1"]);
    const w = watch(r, { keyspaceId: KS, select: { prefix: "" } });
    r.server.write(KS, [kc("issue/1/title", "set", "uno"), kc("issue/2/title", "set", "dos")]);
    await r.engine.settle();
    expect(w.log()).toEqual(["~issue/1/title"]);
  });
});

describe("a purge (14 [CLI-66])", () => {
  it("a revocation purges the copy: live queries lose every row, the layer holds nothing of the keyspace, and a query says purged", async () => {
    const r = await qrig();
    put(r, { "item/a": 1, "item/b": 2, "issue/1": {} });
    await r.engine.settle();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "" } });
    expect(r.q.holds(KS).keys).toBe(3);
    r.server.revokeKeyspaces(KS);
    await r.engine.settle();
    expect(r.engine.status().stopped[KS]).toBe("ACCESS_REVOKED");
    expect(w.updates.at(-1)?.removed.map((x) => x.key)).toEqual(["issue/1", "item/a", "item/b"]);
    expect(w.live.snapshot().rows).toEqual([]);
    expect(w.live.snapshot().keyspace.status).toBe("purged");
    expect(w.updates.at(-1)?.statusChanged).toBe(true);
    expect(r.q.holds(KS)).toEqual({ keys: 0, live: 1 });
    const res = r.q.run({ keyspaceId: KS, select: { prefix: "item/" } });
    expect(res.rows).toEqual([]);
    expect(res.keyspace.status).toBe("purged");
    expect(res.coverage).toBe("none");
    expect(r.q.get(KS, "item/a")).toEqual({ status: "unavailable", reason: "purged" });
    expect(r.q.run({ keyspaceId: KS, select: { keys: ["item/a"] } }).notLoaded).toEqual(["item/a"]); // not known: never absent
    expect(r.q.holds(KS).keys).toBe(0);
  });

  it("the purge hook alone purges: with the change feed cut off, the engine's call of the hook drops the rows, the index and the results (14 [CLI-66])", async () => {
    const r = await qrig();
    put(r, { "item/a": 1, "item/b": 2 });
    await r.engine.settle();
    const e = r.engine;
    const wrapped: QueryEngine = {
      view: (k) => e.view(k),
      confirmed: (k) => e.confirmed(k),
      keyspaceInfo: (k) => e.keyspaceInfo(k),
      loadKeys: (k, keys) => e.loadKeys(k, keys),
      crdt: (k, key) => e.crdt(k, key),
      on: (l) => e.on(l),
      onViewChange: () => () => undefined, // the feed tells this layer nothing
      registerPurgeHook: (h) => e.registerPurgeHook(h),
      flushViewChanges: () => undefined,
      manifests: e.manifests,
    };
    const q = createQueries(wrapped);
    const seen: string[] = [];
    const live = q.watch({ keyspaceId: KS, select: { prefix: "" } }, (u) => seen.push(`${u.cause}:${u.removed.length}`));
    expect(q.holds(KS).keys).toBe(2);
    r.server.revokeKeyspaces(KS);
    await r.engine.settle();
    expect(seen).toEqual(["initial:0", "purged:2"]);
    expect(live.snapshot()).toMatchObject({ rows: [], keyspace: { status: "purged" } });
    expect(q.holds(KS).keys).toBe(0);
    q.close();
  });

  it("the layer registered a purge hook with the engine; a live query that starts after the purge shows nothing", async () => {
    const r = await qrig();
    put(r, { "item/a": 1 });
    await r.engine.settle();
    r.server.revokeKeyspaces(KS);
    await r.engine.settle();
    const late = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    expect(late.updates[0]?.added).toEqual([]);
    expect(late.live.snapshot().keyspace.status).toBe("purged");
  });

  it("closing a live query stops its updates, and closing the layer closes them all", async () => {
    const r = await qrig();
    const a = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    const b = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    a.live.close();
    expect(a.live.closed).toBe(true);
    expect(r.q.holds(KS).live).toBe(1);
    put(r, { "item/x": 1 });
    await r.engine.settle();
    expect(a.after()).toEqual([]);
    expect(b.log()).toEqual(["+item/x"]);
    r.q.close();
    expect(b.live.closed).toBe(true);
    expect(() => r.q.run({ keyspaceId: KS, select: { prefix: "" } })).toThrow();
    put(r, { "item/y": 1 });
    await r.engine.settle();
    expect(b.log()).toEqual(["+item/x"]);
  });
});

describe("the copy is replaced, or the engine stops following it", () => {
  it("a load that replaces the state (a restore, 14 [CLI-51]) evaluates the query again, and reports only the rows that really differ", async () => {
    const r = await qrig();
    r.server.write(KS, [kc("item/a", "set", 1)]);
    r.server.write(KS, [kc("item/b", "set", 2)]);
    r.server.write(KS, [kc("item/c", "set", 3)]);
    await r.engine.settle();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    const reloads = r.events.filter((e) => e.type === "reloaded").length;
    r.server.restore(KS, 2n); // the third record is lost
    await r.engine.settle();
    expect(r.events.filter((e) => e.type === "reloaded").length).toBeGreaterThan(reloads);
    expect(w.log()).toEqual(["-item/c"]); // a and b are the same rows: nothing is told of them
    expect(w.after()[0]?.cause).toBe("refresh");
    expect(keysOf(w.live.snapshot())).toEqual(["item/a", "item/b"]);
  });

  it("GRANT_CHANGED with the default policy keeps the copy, and the live query says that the engine stopped following it (14 [CLI-67])", async () => {
    const r = await qrig();
    put(r, { "item/a": 1 });
    await r.engine.settle();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    r.server.denyRead("ana", KS);
    await r.engine.settle();
    expect(r.engine.status().stopped[KS]).toBe("GRANT_CHANGED");
    expect(w.live.snapshot().keyspace).toMatchObject({ status: "ready", stopped: "GRANT_CHANGED" });
    expect(keysOf(w.live.snapshot())).toEqual(["item/a"]);
    expect(w.after().some((u) => u.statusChanged)).toBe(true);
  });

  it("a copy purged by the app's policy leaves nothing; when the engine follows the keyspace again, a cold load brings what the server holds now and none of the purged rows (14 [CLI-66], [CLI-70])", async () => {
    const r = await qrig({ cfg: { copyPolicy: "purge" } });
    put(r, { "item/a": 1, "item/b": 2 });
    await r.engine.settle();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    r.server.denyRead("ana", KS);
    await r.engine.settle();
    expect(w.live.snapshot()).toMatchObject({ rows: [], keyspace: { status: "purged" } });
    expect(r.q.holds(KS).keys).toBe(0);
    // meanwhile the server changes: item/a is deleted, item/c is created
    drop(r, "item/a");
    put(r, { "item/c": 3 });
    r.server.allowRead("ana", KS);
    await r.engine.addKeyspace(KS);
    await r.engine.settle();
    await r.clock.advance(5000);
    await r.engine.settle();
    expect(w.live.snapshot().keyspace.status).toBe("ready");
    expect(keysOf(w.live.snapshot())).toEqual(["item/b", "item/c"]);
    expect(w.updates.at(-1)?.cause).toBe("refresh");
    expect(w.updates.at(-1)?.added.map((x) => x.key)).toEqual(["item/b", "item/c"]);
    expect(r.q.get(KS, "item/a")).toEqual({ status: "absent" });
  });
});

describe("a CRDT key in a live query (14 [CLI-13])", () => {
  it("a confirmed update from another principal is a change of the row; the document is the live view document", async () => {
    const r = await qrig();
    const h = r.engine.crdt(KS, "doc/d1");
    h.doc.getText("text").insert(0, "hello");
    await h.flush("DocEdit");
    await r.engine.settle();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "doc/" } });
    expect(w.updates[0]?.added.map((x) => x.key)).toEqual(["doc/d1"]);
    const server = r.server.ydoc(KS, "doc/d1") as Y.Doc;
    const other = new Y.Doc();
    Y.applyUpdate(other, Y.encodeStateAsUpdate(server));
    const sv = Y.encodeStateVector(other);
    other.getText("text").insert(5, "!");
    r.server.write(KS, [create(KeyChangeSchema, { key: "doc/d1", kind: ChangeKind.CRDT_UPDATE, update: Y.encodeStateAsUpdate(other, sv) })], { sub: "bob" });
    await r.engine.settle();
    expect(w.log()).toEqual(["~doc/d1"]);
    const row = w.live.snapshot().rows[0];
    expect(row?.value.kind === "crdt" && row.value.doc).toBe(h.doc);
    expect(h.doc.getText("text").toString()).toBe("hello!");
  });

  it("a rebuilt view document is a change of the row, whatever else it reads the same (14 [CLI-13], `rebuilt`)", async () => {
    const r = await qrig();
    const h = r.engine.crdt(KS, "doc/d1");
    h.doc.getText("text").insert(0, "base");
    await h.flush("DocEdit");
    await r.engine.settle();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "doc/" } });
    const before = w.live.snapshot().rows[0];
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, mk(Code.SCHEMA_INVALID)) });
    const oldView = h.doc;
    h.doc.getText("text").insert(4, "A");
    await h.flush("DocEdit");
    await r.engine.settle();
    expect(h.doc).not.toBe(oldView); // the rejected update rebuilt the view document
    const row = w.live.snapshot().rows[0];
    expect(row?.value.kind === "crdt" && row.value.doc).toBe(h.doc);
    // the row reads as it did before the action; it is reported as changed all the same, for the document was replaced
    expect(row).toMatchObject({ version: before?.version, pending: [], predicted: false });
    expect(w.log()).toEqual(["~doc/d1", "~doc/d1"]); // pending, then rebuilt
  });
});
