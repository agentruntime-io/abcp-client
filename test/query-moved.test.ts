// The local query layer (C4): moved and staged entities, 14 [CLI-14] with 11 [COORD-73] (the moved key and its statuses), [COORD-76] (what a client shows), [COORD-104] (the
// freeze) and [COORD-111] (a staged entity is not yet present). The moved key of an entity is `abcp.saga.v1/moved/` and its marker key; the run key of its saga is
// `abcp.saga.v1/run/{saga_id}`.

import { describe, expect, it } from "vitest";
import { drop, jsonOf, keysOf, KS, put, qrig, watch, type QRig } from "./helpers/query.js";

const TO = { keyspace_id: "ks-target", key: "issue/1" };
const moved = (status: string, extra: Record<string, unknown> = {}): Record<string, unknown> => ({ status, saga_id: "s1", saga: "MoveIssue", ...extra });

const seed = async (): Promise<QRig> => {
  const r = await qrig();
  put(r, { "issue/1": { n: 1 }, "issue/1/title": "one", "issue/1/comment/5": {}, "issue/1/comment/5/text": "c", "issue/2": { n: 2 }, "issue/2/title": "two" });
  await r.engine.settle();
  return r;
};

const setMoved = async (r: QRig, marker: string, value: Record<string, unknown>): Promise<void> => {
  put(r, { [`abcp.saga.v1/moved/${marker}`]: value });
  await r.engine.settle();
};

describe("an entity whose moved key says `moving` or `moved` is hidden with its subtree, and its pointer is shown in its place (14 [CLI-14])", () => {
  for (const status of ["moving", "moved"] as const) {
    it(`${status}: the entity listing shows the pointer, and every other query shows no row of the entity`, async () => {
      const r = await seed();
      await setMoved(r, "issue/1", moved(status, { to: TO, ...(status === "moved" ? { at: "2026-01-01T00:00:00Z" } : { copy_seq: 7 }) }));
      const list = r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}", subtree: true } });
      expect(list.rows.map((x) => [x.key, x.value.kind, x.subtree?.length])).toEqual([
        ["issue/1", "moved", undefined],
        ["issue/2", "json", 1],
      ]);
      const pointer = list.rows[0]?.value;
      expect(pointer).toEqual({ kind: "moved", status, to: { keyspaceId: "ks-target", key: "issue/1" }, sagaId: "s1", runKey: "abcp.saga.v1/run/s1", run: undefined, failed: false });
      expect(list.rows[0]?.bindings).toEqual({ i: "1" });
      // no subtree, no row below it, in any kind of query
      expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "issue/1/" } }))).toEqual([]);
      expect(keysOf(r.q.run({ keyspaceId: KS, select: { template: "issue/{i}/title" } }))).toEqual(["issue/2/title"]);
      expect(keysOf(r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}/comment/{c}" } }))).toEqual([]);
      expect(r.q.get(KS, "issue/1/title")).toEqual({ status: "absent" });
      expect(r.q.get(KS, "issue/1/comment/5/text")).toEqual({ status: "absent" });
      expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "issue/" } }))).toEqual(["issue/1", "issue/2", "issue/2/title"]);
      // the marker key read by key is the pointer, in its place
      const g = r.q.get(KS, "issue/1");
      expect(g.status === "present" && g.row.value.kind).toBe("moved");
    });
  }

  it("the moved key and the run key are plain keys, and are listed as such", async () => {
    const r = await seed();
    await setMoved(r, "issue/1", moved("moving", { to: TO, copy_seq: 7 }));
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "abcp.saga.v1/" } }))).toEqual(["abcp.saga.v1/moved/issue/1"]);
  });

  it("a filter reads no pointer: it is no json/v1 value", async () => {
    const r = await seed();
    await setMoved(r, "issue/1", moved("moved", { to: TO, at: "2026-01-01T00:00:00Z" }));
    const res = r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}" }, filter: { path: "n", op: "gte", value: 1 } });
    expect(keysOf(res)).toEqual(["issue/2"]);
  });
});

describe("a staged entity is not yet present (14 [CLI-14], 11 [COORD-111])", () => {
  it("`importing` hides the entity and its subtree, with no pointer; `active` shows it as any other", async () => {
    const r = await seed();
    await setMoved(r, "issue/1", { status: "importing", saga_id: "s1", saga: "MoveIssue", from: TO, chunk: 0, chunks: 2 });
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}", subtree: true } }))).toEqual(["issue/2"]);
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "issue/" } }))).toEqual(["issue/2", "issue/2/title"]);
    expect(r.q.get(KS, "issue/1")).toEqual({ status: "absent" });
    expect(r.q.get(KS, "issue/1/title")).toEqual({ status: "absent" });
    await setMoved(r, "issue/1", { status: "active", saga_id: "s1", saga: "MoveIssue", from: TO });
    const res = r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}", subtree: true } });
    expect(res.rows.map((x) => [x.key, x.value.kind, x.subtree?.length])).toEqual([["issue/1", "json", 3], ["issue/2", "json", 1]]);
    expect(r.q.get(KS, "issue/1/title").status).toBe("present");
  });
});

describe("a moved key that says `purged` (14 [CLI-14], 11 [COORD-104])", () => {
  it("the pointer is shown while the entity's marker is absent; an entity created again under that marker is shown as it is", async () => {
    const r = await seed();
    await setMoved(r, "issue/1", moved("moved", { to: TO, at: "2026-01-01T00:00:00Z" }));
    drop(r, "issue/1"); // the purge deletes the marker, and with it the subtree (05 [STATE-43])
    await r.engine.settle();
    await setMoved(r, "issue/1", moved("purged", { to: TO, at: "2026-01-01T00:00:00Z" }));
    const list = r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}" } });
    expect(list.rows.map((x) => [x.key, x.value.kind, x.value.kind === "moved" && x.value.status])).toEqual([
      ["issue/1", "moved", "purged"],
      ["issue/2", "json", false],
    ]);
    const g = r.q.get(KS, "issue/1");
    expect(g.status === "present" && g.row.value.kind === "moved" && g.row.value.to).toEqual({ keyspaceId: "ks-target", key: "issue/1" });
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "issue/" } }))).toEqual(["issue/2", "issue/2/title"]); // a prefix lists present keys: the marker is absent
    // created again under that marker: no longer frozen, and shown as it is
    put(r, { "issue/1": { n: 99 }, "issue/1/title": "again" });
    await r.engine.settle();
    const again = r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}", subtree: true } });
    expect(again.rows.map((x) => [x.key, x.value.kind])).toEqual([["issue/1", "json"], ["issue/2", "json"]]);
    expect(jsonOf(again.rows[0])).toEqual({ n: 99n });
    expect(again.rows[0]?.subtree?.map((s) => s.key)).toEqual(["issue/1/title"]);
  });
});

describe("a failed move is shown as failed, with both sides kept as they are (14 [CLI-14], 11 [COORD-114])", () => {
  it("the run key of the saga, when the copy holds it, says so; the pointer's run follows it", async () => {
    const r = await seed();
    await setMoved(r, "issue/1", moved("moving", { to: TO, copy_seq: 7 }));
    expect(r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}" } }).rows[0]?.value).toMatchObject({ failed: false, run: undefined });
    put(r, { "abcp.saga.v1/run/s1": { status: "running", saga: "MoveIssue" } });
    await r.engine.settle();
    expect(r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}" } }).rows[0]?.value).toMatchObject({ failed: false, run: { status: "running" } });
    put(r, { "abcp.saga.v1/run/s1": { status: "failed", cause: "EPOCH" } });
    await r.engine.settle();
    expect(r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}" } }).rows[0]?.value).toMatchObject({ kind: "moved", status: "moving", failed: true, run: { status: "failed", cause: "EPOCH" } });
  });
});

describe("live queries follow the moved keys", () => {
  it("a moved key that arrives replaces the entity's row by the pointer and takes the rows below it out of every query, in one update each", async () => {
    const r = await seed();
    const ent = watch(r, { keyspaceId: KS, select: { entities: "issue/{i}", subtree: true } });
    const pre = watch(r, { keyspaceId: KS, select: { prefix: "issue/1/" } });
    const tpl = watch(r, { keyspaceId: KS, select: { template: "issue/{i}/title" } });
    await setMoved(r, "issue/1", moved("moving", { to: TO, copy_seq: 7 }));
    expect(ent.log()).toEqual(["~issue/1"]);
    expect(ent.after()[0]?.changed[0]).toMatchObject({ before: { value: { kind: "json" } }, after: { value: { kind: "moved", status: "moving" } } });
    expect(pre.after()).toHaveLength(1);
    expect(pre.log().sort()).toEqual(["-issue/1/comment/5", "-issue/1/comment/5/text", "-issue/1/title"]);
    expect(tpl.log()).toEqual(["-issue/1/title"]);
    // moving -> moved: the pointer changes, nothing else
    await setMoved(r, "issue/1", moved("moved", { to: TO, at: "2026-01-01T00:00:00Z" }));
    expect(ent.log()).toEqual(["~issue/1", "~issue/1"]);
    expect(ent.after()[1]?.changed[0]?.after.value).toMatchObject({ status: "moved" });
    expect(pre.after()).toHaveLength(1);
    expect(tpl.after()).toHaveLength(1);
    // the entity is moved back (the moved key is deleted: a compensation, 11 [COORD-75]): everything is shown again
    drop(r, "abcp.saga.v1/moved/issue/1");
    await r.engine.settle();
    expect(ent.log()).toEqual(["~issue/1", "~issue/1", "~issue/1"]);
    expect(pre.log().sort()).toEqual(["+issue/1/comment/5", "+issue/1/comment/5/text", "+issue/1/title", "-issue/1/comment/5", "-issue/1/comment/5/text", "-issue/1/title"]);
    expect(tpl.log()).toEqual(["-issue/1/title", "+issue/1/title"]);
  });

  it("a staged entity appears when it is activated; a run key that says failed changes the pointer", async () => {
    const r = await seed();
    await setMoved(r, "issue/3", { status: "importing", saga_id: "s2", saga: "MoveIssue", from: TO, chunk: 0, chunks: 1 });
    put(r, { "issue/3": { n: 3 }, "issue/3/title": "three" });
    await r.engine.settle();
    const ent = watch(r, { keyspaceId: KS, select: { entities: "issue/{i}" } });
    expect(ent.updates[0]?.added.map((x) => x.key)).toEqual(["issue/1", "issue/2"]); // issue/3 is staged
    await setMoved(r, "issue/3", { status: "active", saga_id: "s2", saga: "MoveIssue", from: TO });
    expect(ent.log()).toEqual(["+issue/3"]);
    await setMoved(r, "issue/1", moved("moving", { to: TO, copy_seq: 1 }));
    put(r, { "abcp.saga.v1/run/s1": { status: "failed", cause: "EPOCH" } });
    await r.engine.settle();
    expect(ent.log()).toEqual(["+issue/3", "~issue/1", "~issue/1"]);
    expect(ent.after()[2]?.changed[0]?.after.value).toMatchObject({ failed: true });
  });

  it("a purged pointer appears for an absent marker, and goes when the entity is created again", async () => {
    const r = await seed();
    const ent = watch(r, { keyspaceId: KS, select: { entities: "issue/{i}" } });
    drop(r, "issue/1");
    await r.engine.settle();
    expect(ent.log()).toEqual(["-issue/1"]);
    await setMoved(r, "issue/1", moved("purged", { to: TO, at: "2026-01-01T00:00:00Z" }));
    expect(ent.log()).toEqual(["-issue/1", "+issue/1"]);
    expect(ent.after()[1]?.added[0]?.value).toMatchObject({ kind: "moved", status: "purged" });
    put(r, { "issue/1": { n: 5 } });
    await r.engine.settle();
    expect(ent.log()).toEqual(["-issue/1", "+issue/1", "~issue/1"]);
    expect(ent.after()[2]?.changed[0]?.after.value.kind).toBe("json");
  });
});

describe("the interest decides whether moves can be seen (14 [CLI-14], [CLI-71])", () => {
  const withInterest = async (interest: string[]): Promise<QRig> => {
    const r = await qrig({ keyspaces: [], start: false });
    r.server.createKeyspaceNow(KS);
    put(r, { "issue/1": {}, "issue/1/title": "one", "abcp.saga.v1/moved/issue/1": moved("moved", { to: TO, at: "2026-01-01T00:00:00Z" }) });
    await r.engine.addKeyspace(KS, { interest });
    r.engine.start();
    await r.engine.settle();
    return r;
  };

  it("with the moved keys in the interest the pointer is shown and movesTracked says so; without them the entity is shown as any other, and movesTracked says it cannot know", async () => {
    const a = await withInterest(["issue", "abcp.saga.v1"]);
    const ra = a.q.run({ keyspaceId: KS, select: { entities: "issue/{i}" } });
    expect(ra.rows[0]?.value.kind).toBe("moved");
    expect(ra.movesTracked).toBe(true);
    const b = await withInterest(["issue"]);
    const rb = b.q.run({ keyspaceId: KS, select: { entities: "issue/{i}" } });
    expect(rb.rows[0]?.value.kind).toBe("json");
    expect(rb.movesTracked).toBe(false);
    expect(rb.coverage).toBe("full");
    const c = await withInterest([]);
    expect(c.q.run({ keyspaceId: KS, select: { entities: "issue/{i}" } }).movesTracked).toBe(true);
  });
});
