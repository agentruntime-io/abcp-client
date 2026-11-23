// The local query layer (C4): scenarios over the fake server. Two clients, where one's committed write appears in the other's live query (14 [CLI-62]: tabs and devices of
// one user are concurrent writers, and there is no read-your-writes across devices until the write's confirmed seq is observed); a follower tab, which reads through the
// flusher and holds no view documents (14 [CLI-61]).

import { describe, expect, it } from "vitest";
import { jsonBytes, LocalRefusal, set, del } from "../src/engine/index.js";
import { create } from "@bufbuild/protobuf";
import { ChangeKind, KeyChangeSchema } from "../src/gen/abcp/v2/types_pb.js";
import { createQueries } from "../src/query/index.js";
import { goOffline, goOnline, KS, ofType, types } from "./helpers/engine.js";
import { jsonOf, keysOf, qrig, watch } from "./helpers/query.js";
import { settleAll, tabsRig } from "./helpers/tabs.js";

describe("two clients and one fake server", () => {
  it("one client's committed write appears in the other's live query, as a row added, then changed, then removed; its own query saw it pending first", async () => {
    const ana = await qrig({ sub: "ana", seed: 1 });
    const bob = await qrig({ server: ana.server, manifests: ana.manifests, clock: ana.clock, sub: "bob", seed: 2 });
    const anaView = watch(ana, { keyspaceId: KS, select: { entities: "issue/{i}", subtree: true } });
    const bobView = watch(bob, { keyspaceId: KS, select: { entities: "issue/{i}", subtree: true } });
    const bobTitles = watch(bob, { keyspaceId: KS, select: { template: "issue/{i}/title" }, mode: "confirmed" });

    // ana creates an issue: she sees it at once, pending; bob sees it when the record arrives
    const created = await ana.engine.submit({ keyspaceId: KS, opType: "Create", changes: [set("issue/1", { n: 1 }), set("issue/1/title", "first")] });
    expect(anaView.updates[1]?.added[0]).toMatchObject({ key: "issue/1", pending: [created.mutationId], predicted: true });
    expect(bobView.after()).toEqual([]); // no read-your-writes across devices: bob has seen nothing yet
    await ana.engine.settle();
    await bob.engine.settle();
    expect(types(ana, created.mutationId)).toEqual(["committed"]);
    expect(anaView.live.snapshot().rows[0]).toMatchObject({ pending: [], predicted: false });
    expect(bobView.log()).toEqual(["+issue/1"]);
    const seen = bobView.after()[0]?.added[0];
    expect(seen).toMatchObject({ key: "issue/1", pending: [], predicted: false, bindings: { i: "1" } });
    expect(seen?.subtree?.map((s) => [s.key, jsonOf(s)])).toEqual([["issue/1/title", "first"]]);
    expect(bobTitles.log()).toEqual(["+issue/1/title"]);

    // bob edits the title: ana sees it change
    await bob.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("issue/1/title", "second")] });
    await bob.engine.settle();
    await ana.engine.settle();
    expect(anaView.log()).toEqual(["+issue/1", "~issue/1", "~issue/1"]); // pending, committed, and bob's edit
    expect(anaView.after().at(-1)?.changed[0]?.after.subtree?.map((s) => jsonOf(s))).toEqual(["second"]);
    expect(bobTitles.log()).toEqual(["+issue/1/title", "~issue/1/title"]); // a confirmed query: bob's own edit is told once, when it is confirmed

    // ana deletes the issue: bob's rows under it go in one update
    await ana.engine.submit({ keyspaceId: KS, opType: "Set", changes: [del("issue/1")] });
    await ana.engine.settle();
    await bob.engine.settle();
    expect(bobView.log()).toEqual(["+issue/1", "~issue/1", "~issue/1", "-issue/1"]); // created by ana; bob's edit pending, then confirmed; deleted by ana
    expect(bobTitles.log()).toEqual(["+issue/1/title", "~issue/1/title", "-issue/1/title"]);
    expect(keysOf(bob.q.run({ keyspaceId: KS, select: { prefix: "" } }))).toEqual([]);
    expect(ofType(bob, "rejected")).toEqual([]);
  });

  it("a client that was offline sees the other's writes when it reconnects, as one update for the records it missed, and its pending change is marked until the server answers", async () => {
    const ana = await qrig({ sub: "ana", seed: 1 });
    const bob = await qrig({ server: ana.server, manifests: ana.manifests, clock: ana.clock, sub: "bob", seed: 2 });
    await goOffline(bob);
    const w = watch(bob, { keyspaceId: KS, select: { prefix: "item/" } });
    const mine = await bob.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("item/b", "bob")] });
    await ana.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("item/a", "ana")] });
    await ana.engine.settle();
    expect(w.log()).toEqual(["+item/b"]);
    expect(w.live.snapshot().rows[0]).toMatchObject({ key: "item/b", pending: [mine.mutationId], predicted: true });
    await goOnline(bob);
    await bob.engine.settle();
    expect(keysOf(w.live.snapshot())).toEqual(["item/a", "item/b"]);
    expect(w.live.snapshot().rows.map((x) => [x.key, x.pending, x.predicted])).toEqual([["item/a", [], false], ["item/b", [], false]]);
    expect(types(bob, mine.mutationId)).toEqual(["committed"]);
  });

  it("a counter that both clients increment is the sum, in both live queries", async () => {
    const ana = await qrig({ sub: "ana", seed: 1 });
    const bob = await qrig({ server: ana.server, manifests: ana.manifests, clock: ana.clock, sub: "bob", seed: 2 });
    const wa = watch(ana, { keyspaceId: KS, select: { template: "count/{n}" } });
    const wb = watch(bob, { keyspaceId: KS, select: { template: "count/{n}" } });
    for (const [c, n] of [[ana, 3], [bob, 4], [ana, 5]] as const) {
      await c.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [{ key: "count/c", kind: ChangeKind.INCR, delta: BigInt(n) }] });
      await ana.engine.settle();
      await bob.engine.settle();
    }
    expect(wa.live.snapshot().rows[0]?.value).toEqual({ kind: "counter", value: 12n });
    expect(wb.live.snapshot().rows[0]?.value).toEqual({ kind: "counter", value: 12n });
  });
});

describe("a follower tab (14 [CLI-61])", () => {
  it("reads through the flusher: its live query sees the flusher's confirmed state and the shared outbox's prediction; the CRDT row has no document here", async () => {
    const r = tabsRig();
    const a = await r.open();
    const b = await r.open();
    expect(b.engine.status().role).toBe("follower");
    const qa = createQueries(a.engine);
    const qb = createQueries(b.engine);
    const wb: string[] = [];
    const live = qb.watch({ keyspaceId: KS, select: { prefix: "settings/" } }, (u) => wb.push(...u.added.map((x) => `+${x.key}`), ...u.changed.map((x) => `~${x.after.key}`), ...u.removed.map((x) => `-${x.key}`)));
    r.server.write(KS, [create(KeyChangeSchema, { key: "settings/s", kind: ChangeKind.SET, value: jsonBytes(5), encoding: "json/v1" })]);
    await settleAll(a.engine, b.engine);
    expect(wb).toEqual(["+settings/s"]);
    // a pending action of the shared outbox shows in the follower's predicted query
    r.server.disconnect();
    await settleAll(a.engine, b.engine);
    const x = await a.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/p", 9)] });
    await settleAll(a.engine, b.engine);
    const p = qb.get(KS, "settings/p");
    expect(p.status === "present" && [jsonOf(p.row), p.row.pending, p.row.predicted]).toEqual([9n, [x.mutationId], true]);
    expect(qb.get(KS, "settings/p", { mode: "confirmed" }).status).toBe("absent");
    expect(wb).toEqual(["+settings/s", "+settings/p"]);
    // the flusher's tab holds the view documents; the follower's row has none
    const h = a.engine.crdt(KS, "docs/d1");
    h.doc.getText("text").insert(0, "hi");
    await h.flush("DocEdit");
    r.server.reconnect();
    await r.clock.advance(2000);
    await settleAll(a.engine, b.engine);
    const onA = qa.get(KS, "docs/d1");
    const onB = qb.get(KS, "docs/d1");
    expect(onA.status === "present" && onA.row.value.kind === "crdt" && onA.row.value.doc).toBe(h.doc);
    expect(onB.status === "present" && onB.row.value.kind).toBe("crdt");
    expect(onB.status === "present" && onB.row.value.kind === "crdt" && onB.row.value.doc).toBeUndefined();
    expect(() => b.engine.crdt(KS, "docs/d1")).toThrow(LocalRefusal);
    expect(wb).toEqual(["+settings/s", "+settings/p", "~settings/p"]); // the pending action committed; the docs are under another prefix
    live.close();
    qa.close();
    qb.close();
  });
});
