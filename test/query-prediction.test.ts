// The local query layer (C4): the predicted view against the confirmed state, and what a pending change looks like to a query until it is committed, reverted or corrected.
// 14 [CLI-6] (the view), [CLI-7] (retirement), [CLI-10] to [CLI-16] (prediction), [CLI-14] (a pending DELETE of a marker key hides its subtree), [CLI-38] and [CLI-39]
// (the events of an outcome), [CLI-11] (a CAS that was predicted to have no effect).

import { describe, expect, it } from "vitest";
import { Code } from "../src/gen/abcp/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { cas, del, incr, jsonBytes, set } from "../src/engine/index.js";
import { goOffline, goOnline, kc, kcCas, ofType, rejectResult, types } from "./helpers/engine.js";
import { jsonOf, keysOf, KS, put, qrig, watch } from "./helpers/query.js";

describe("the predicted view and the confirmed state (14 [CLI-6])", () => {
  it("a pending SET shows in a predicted query, marked pending and predicted, and not in a confirmed one", async () => {
    const r = await qrig();
    put(r, { "item/a": "old" });
    await r.engine.settle();
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("item/a", "new"), set("item/b", "created")] });
    const p = r.q.run({ keyspaceId: KS, select: { prefix: "item/" } });
    expect(p.rows.map((x) => [x.key, jsonOf(x), x.pending, x.isPending, x.predicted])).toEqual([
      ["item/a", "new", [a.mutationId], true, true],
      ["item/b", "created", [a.mutationId], true, true],
    ]);
    const c = r.q.run({ keyspaceId: KS, mode: "confirmed", select: { prefix: "item/" } });
    // the confirmed state alone: the pending action is no part of it, and marks nothing; the key that it creates is not there
    expect(c.rows.map((x) => [x.key, jsonOf(x), x.pending, x.predicted])).toEqual([["item/a", "old", [], false]]);
    expect(r.q.get(KS, "item/b", { mode: "confirmed" }).status).toBe("absent");
  });

  it("a pending DELETE shows the key's absence; a pending DELETE of a marker key hides its subtree (14 [CLI-10], [CLI-14])", async () => {
    const r = await qrig();
    put(r, { "issue/1": {}, "issue/1/title": "t", "issue/1/comment/2": {}, "issue/1/comment/2/text": "c", "issue/2": {}, "item/x": 1 });
    await r.engine.settle();
    await goOffline(r);
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [del("issue/1")] });
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [del("item/x")] });
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "" } }))).toEqual(["issue/2"]);
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}", subtree: true } }))).toEqual(["issue/2"]);
    expect(r.q.get(KS, "issue/1/comment/2/text").status).toBe("absent");
    expect(keysOf(r.q.run({ keyspaceId: KS, mode: "confirmed", select: { prefix: "" } }))).toEqual(["issue/1", "issue/1/comment/2", "issue/1/comment/2/text", "issue/1/title", "issue/2", "item/x"]);
  });

  it("an action of an op type that is not optimistic shows the key as pending only, with no predicted value (14 [CLI-16])", async () => {
    const r = await qrig();
    put(r, { "item/a": "old" });
    await r.engine.settle();
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Quiet", changes: [set("item/a", "new")] });
    const row = r.q.get(KS, "item/a");
    expect(row.status === "present" && [jsonOf(row.row), row.row.pending, row.row.predicted]).toEqual(["old", [a.mutationId], false]);
  });

  it("a CAS whose expectation does not hold has no predicted effect; its key is not shown (14 [CLI-11])", async () => {
    const r = await qrig();
    await goOffline(r);
    await r.engine.submit({ keyspaceId: KS, opType: "CasSet", changes: [cas("note/n1", "mine", { value: jsonBytes("other") })] });
    expect(r.q.get(KS, "note/n1").status).toBe("absent");
    await r.engine.submit({ keyspaceId: KS, opType: "CasSet", changes: [cas("note/n2", "mine", { absent: true })] });
    const n2 = r.q.get(KS, "note/n2");
    expect(n2.status === "present" && [jsonOf(n2.row), n2.row.predicted]).toEqual(["mine", true]);
  });
});

describe("a pending change is committed, reverted or corrected, and a live query is told", () => {
  it("committed: the row is marked pending and predicted until the record is confirmed; then it is neither, at the committed version", async () => {
    const r = await qrig();
    put(r, { "item/a": "old" });
    await r.engine.settle();
    await goOffline(r);
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("item/a", "mine")] });
    expect(w.log()).toEqual(["~item/a"]);
    const pending = w.updates[1]?.changed[0];
    expect(pending?.before).toMatchObject({ pending: [], predicted: false });
    expect(pending?.after).toMatchObject({ pending: [a.mutationId], predicted: true, version: pending?.before.version });
    await goOnline(r);
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["committed"]);
    const done = w.updates.at(-1)?.changed[0];
    expect(done?.after).toMatchObject({ pending: [], predicted: false });
    expect(done?.after.version).toBeGreaterThan(done?.before.version ?? 0n);
    expect(jsonOf(done?.after)).toBe("mine");
    expect(w.log()).toEqual(["~item/a", "~item/a"]); // pending, then committed: two changes of one row, nothing else
    // the confirmed query changed once, when the record was confirmed
    const conf = r.q.get(KS, "item/a", { mode: "confirmed" });
    expect(conf.status === "present" && jsonOf(conf.row)).toBe("mine");
  });

  it("reverted: a rejected action leaves the view, and the row goes back to what the confirmed state holds", async () => {
    const r = await qrig();
    put(r, { "item/a": "old" });
    await r.engine.settle();
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("item/a", "mine"), set("item/new", "x")] });
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    expect(w.updates[0]?.added.map((x) => x.key)).toEqual(["item/a", "item/new"]);
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.mutationId === a.mutationId ? rejectResult(m, mk(Code.RANGE_EXCEEDED)) : undefined) });
    await goOnline(r);
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["conflict", "rejected"]);
    expect(w.log().sort()).toEqual(["-item/new", "~item/a"]);
    const after = r.q.run({ keyspaceId: KS, select: { prefix: "item/" } });
    expect(after.rows.map((x) => [x.key, jsonOf(x), x.pending, x.predicted])).toEqual([["item/a", "old", [], false]]);
  });

  it("cancelled: a pending change that the app takes back leaves the view (14 [CLI-81])", async () => {
    const r = await qrig();
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("item/a", "mine")] });
    const w = watch(r, { keyspaceId: KS, select: { prefix: "item/" } });
    await r.engine.cancel(a.mutationId);
    expect(w.log()).toEqual(["-item/a"]);
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "item/" } }))).toEqual([]);
  });

  it("corrected: a CAS that was predicted to have no effect appears when it commits, and the event says so (14 [CLI-11], [CLI-38])", async () => {
    const r = await qrig();
    await r.engine.settle();
    await goOffline(r);
    const c = await r.engine.submit({ keyspaceId: KS, opType: "CasSet", changes: [cas("note/n1", "new", { value: jsonBytes("y") })] });
    const w = watch(r, { keyspaceId: KS, select: { prefix: "note/" } });
    expect(w.updates[0]?.added).toEqual([]);
    // another principal makes the expectation hold before the action is sent
    r.server.write(KS, [kcCas("note/n1", "y")]);
    r.server.faults.paused = true;
    await goOnline(r);
    r.server.resumeDelivery();
    await r.engine.settle();
    expect(types(r, c.mutationId)).toEqual(["corrected"]);
    const row = r.q.get(KS, "note/n1");
    expect(row.status === "present" && [jsonOf(row.row), row.row.pending]).toEqual(["new", []]);
    expect(w.log()).toContain("+note/n1");
  });

  it("corrected: the effects of a GATED action are placeholders, so a commit with effects adds rows that were never predicted (14 [CLI-15])", async () => {
    const r = await qrig();
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("setting/requested", 1)] });
    await r.engine.settle();
    const w = watch(r, { keyspaceId: KS, select: { prefix: "setting/" } });
    expect(w.updates[0]?.added.map((x) => [x.key, x.pending, x.predicted])).toEqual([["setting/requested", [a.mutationId], true]]);
    r.server.decide(KS, a.mutationId, { status: "COMMITTED", effects: [kc("setting/effect", "set", "e")] });
    await r.engine.settle();
    expect(ofType(r, "corrected").map((e) => e.keys)).toEqual([["setting/effect"]]);
    const res = r.q.run({ keyspaceId: KS, select: { prefix: "setting/" } });
    expect(res.rows.map((x) => [x.key, x.pending, x.predicted])).toEqual([["setting/effect", [], false], ["setting/requested", [], false]]);
    expect(w.log().sort()).toEqual(["+setting/effect", "~setting/requested"]);
  });

  it("a predicted INCR is shown as the sum, and then as the committed counter (14 [CLI-12])", async () => {
    const r = await qrig();
    r.server.write(KS, [kc("count/c", "incr", 10)]);
    await r.engine.settle();
    await goOffline(r);
    const w = watch(r, { keyspaceId: KS, select: { template: "count/{n}" } });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [incr("count/c", 5)] });
    expect(w.updates[1]?.changed[0]?.after).toMatchObject({ value: { kind: "counter", value: 15n }, pending: [a.mutationId], predicted: true });
    await goOnline(r);
    await r.engine.settle();
    expect(w.updates.at(-1)?.changed[0]?.after).toMatchObject({ value: { kind: "counter", value: 15n }, pending: [], predicted: false });
    expect(w.log()).toEqual(["~count/c", "~count/c"]);
  });
});
