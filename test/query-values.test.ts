// The local query layer (C4): values decoded by encoding. 05 [STATE-7] (json/v1, exact integers), [STATE-26] (counters), 05 §5.3 and [STATE-14] (null), [STATE-82] (the
// redacted state), 14 [CLI-5] (undecodable values), [CLI-13] (CRDT keys as documents), 09 [READ-9] (tombstones).

import { describe, expect, it } from "vitest";
import { create } from "@bufbuild/protobuf";
import * as Y from "yjs";
import { ChangeKind, KeyChangeSchema } from "../src/gen/abcp/v2/types_pb.js";
import { incr, set } from "../src/engine/index.js";
import { utf8Encode } from "../src/rules/bytes.js";
import { jsonOf, KS, put, qrig, rawSet, type QRig } from "./helpers/query.js";

const row = (r: QRig, key: string, mode: "predicted" | "confirmed" = "predicted") => {
  const x = r.q.get(KS, key, { mode });
  if (x.status !== "present") throw new Error(`${key}: ${x.status}`);
  return x.row;
};

describe("json/v1 values (05 [STATE-7])", () => {
  it("integers are bigint, other numbers are numbers, and the rest is as it reads", async () => {
    const r = await qrig();
    put(r, { "item/a": { n: 7, neg: -3, f: 1.5, big: 9007199254740991, e: 1e3, zero: 0, s: "text", t: true, nil: null, arr: [1, 2.5, "x"] } });
    await r.engine.settle();
    expect(jsonOf(row(r, "item/a"))).toEqual({ n: 7n, neg: -3n, f: 1.5, big: 9007199254740991n, e: 1000n, zero: 0n, s: "text", t: true, nil: null, arr: [1n, 2.5, "x"] });
  });

  it("a number that is no integer stays a number; an integer past ±(2^53−1) is no json/v1 value at all (05 [STATE-7]), so it shows as undecodable", async () => {
    const r = await qrig();
    r.server.write(KS, [rawSet("item/frac", utf8Encode("0.5"), "json/v1"), rawSet("item/big", utf8Encode("9007199254740993"), "json/v1")]);
    await r.engine.settle();
    expect(jsonOf(row(r, "item/frac"))).toBe(0.5);
    expect(row(r, "item/big").value.kind).toBe("undecodable");
  });

  it("objects have no prototype and are frozen, so that a member named __proto__ is a member and nothing is changed by the reader", async () => {
    const r = await qrig();
    r.server.write(KS, [rawSet("item/p", utf8Encode('{"__proto__":1,"a":{"b":[1]}}'), "json/v1")]);
    await r.engine.settle();
    const v = jsonOf(row(r, "item/p")) as Record<string, unknown>;
    expect(Object.getPrototypeOf(v)).toBeNull();
    expect(Object.hasOwn(v, "__proto__")).toBe(true);
    expect(v["__proto__"]).toBe(1n);
    expect(Object.isFrozen(v)).toBe(true);
    expect(Object.isFrozen(v["a"])).toBe(true);
    expect(Object.isFrozen((v["a"] as { b: unknown[] }).b)).toBe(true);
    expect(({} as Record<string, unknown>)["a"]).toBeUndefined(); // nothing leaked into Object.prototype
  });
});

describe("a counter key (05 [STATE-26])", () => {
  it("is shown as its integer, a bigint", async () => {
    const r = await qrig();
    r.server.write(KS, [create(KeyChangeSchema, { key: "count/c", kind: ChangeKind.INCR, delta: 5n })]);
    r.server.write(KS, [create(KeyChangeSchema, { key: "count/c", kind: ChangeKind.INCR, delta: 9007199254740986n })]);
    await r.engine.settle();
    expect(row(r, "count/c").value).toEqual({ kind: "counter", value: 9007199254740991n });
  });
});

describe("a bytes/v1 value", () => {
  it("is shown as bytes, a copy that a reader may change", async () => {
    const r = await qrig();
    r.server.write(KS, [rawSet("raw/a", new Uint8Array([9, 8, 7]), "bytes/v1")]);
    await r.engine.settle();
    const v = row(r, "raw/a").value;
    expect(v.kind).toBe("bytes");
    if (v.kind !== "bytes") return;
    expect([...v.value]).toEqual([9, 8, 7]);
    v.value[0] = 0;
    const again = row(r, "raw/a").value;
    expect(again.kind === "bytes" && [...again.value]).toEqual([9, 8, 7]);
  });
});

describe("a value that cannot be decoded is shown as undecodable (14 [CLI-5])", () => {
  it("json/v1 text that does not parse keeps its bytes and shows as no value; it is present, counted and matches no filter", async () => {
    const r = await qrig();
    r.server.write(KS, [rawSet("item/u", utf8Encode("{not json"), "json/v1")]);
    put(r, { "item/ok": 1 });
    await r.engine.settle();
    const u = row(r, "item/u").value;
    expect(u.kind === "undecodable" && u.encoding).toBe("json/v1");
    expect(u.kind === "undecodable" && new TextDecoder().decode(u.bytes)).toBe("{not json");
    expect(r.q.run({ keyspaceId: KS, select: { prefix: "item/" } }).rows.map((x) => x.value.kind)).toEqual(["json", "undecodable"]);
    expect(r.q.run({ keyspaceId: KS, select: { prefix: "item/" }, filter: { op: "ne", value: 1 } }).rows).toEqual([]);
    expect(r.engine.status().skippedChanges).toBe(1); // the engine counts it (14 [CLI-5])
  });
});

describe("null and absent (05 §5.3, [STATE-18])", () => {
  it("an explicit null is a present key with no value; a key that was never set, or deleted, is absent, and nothing is shown for its tombstone", async () => {
    const r = await qrig();
    r.server.write(KS, [create(KeyChangeSchema, { key: "item/n", kind: ChangeKind.SET, isNull: true, encoding: "json/v1" })]);
    put(r, { "item/d": 1 });
    r.server.write(KS, [create(KeyChangeSchema, { key: "item/d", kind: ChangeKind.DELETE })]);
    await r.engine.settle();
    expect(row(r, "item/n").value).toEqual({ kind: "null" });
    expect(r.q.get(KS, "item/d").status).toBe("absent");
    expect(r.q.get(KS, "item/never").status).toBe("absent");
    expect(r.q.run({ keyspaceId: KS, select: { prefix: "item/" } }).rows.map((x) => [x.key, x.value.kind])).toEqual([["item/n", "null"]]);
  });

  it("a pending explicit null is a present null, and a pending delete is absent (14 [CLI-10])", async () => {
    const r = await qrig();
    put(r, { "item/a": 1 });
    await r.engine.settle();
    r.server.faults.paused = true;
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [{ key: "item/a", kind: ChangeKind.SET, isNull: true, encoding: "json/v1" }] });
    expect(row(r, "item/a").value).toEqual({ kind: "null" });
    expect(row(r, "item/a", "confirmed").value).toEqual({ kind: "json", value: 1n });
  });
});

describe("a key in the redacted state (05 [STATE-82])", () => {
  it("is present, holds no value, and is never shown as one; a later write gives it a value again", async () => {
    const r = await qrig();
    put(r, { "item/a": "secret", "item/b": "open" });
    await r.engine.settle();
    r.server.write(KS, [rawSet("item/a", new Uint8Array(0), "redacted/v1")]);
    await r.engine.settle();
    const a = row(r, "item/a");
    expect(a.value).toEqual({ kind: "redacted" });
    expect(JSON.stringify(a, (_k, v: unknown) => (typeof v === "bigint" ? Number(v) : v))).not.toContain("secret");
    const res = r.q.run({ keyspaceId: KS, select: { prefix: "item/" } });
    expect(res.rows.map((x) => [x.key, x.value.kind])).toEqual([["item/a", "redacted"], ["item/b", "json"]]);
    // no filter reads a redacted row
    expect(r.q.run({ keyspaceId: KS, select: { prefix: "item/" }, filter: { op: "ne", value: "x" } }).rows.map((x) => x.key)).toEqual(["item/b"]);
    put(r, { "item/a": "new" });
    await r.engine.settle();
    expect(jsonOf(row(r, "item/a"))).toBe("new");
  });
});

describe("a CRDT key is its Yjs document, not bytes (14 [CLI-13])", () => {
  it("a predicted query gives the view document that the user edits, a confirmed one the confirmed document", async () => {
    const r = await qrig();
    const h = r.engine.crdt(KS, "doc/d1");
    h.doc.getText("text").insert(0, "hello");
    await h.flush("DocEdit");
    await r.engine.settle();
    const p = row(r, "doc/d1");
    expect(p.value.kind).toBe("crdt");
    if (p.value.kind !== "crdt") return;
    expect(p.value.doc).toBe(h.doc);
    expect(p.value.doc?.getText("text").toString()).toBe("hello");
    const c = row(r, "doc/d1", "confirmed");
    expect(c.value.kind === "crdt" && c.value.doc).toBe(h.confirmedDoc);
    // the user's unsent typing is in the view document and not in the confirmed one
    h.doc.getText("text").insert(5, " world");
    expect((row(r, "doc/d1").value as { doc: Y.Doc }).doc.getText("text").toString()).toBe("hello world");
    expect((row(r, "doc/d1", "confirmed").value as { doc: Y.Doc }).doc.getText("text").toString()).toBe("hello");
  });

  it("listing CRDT keys opens no document: the document is fetched when it is read", async () => {
    const r = await qrig();
    const h = r.engine.crdt(KS, "doc/d1");
    h.doc.getText("text").insert(0, "x");
    await h.flush("DocEdit");
    await r.engine.settle();
    let opened = 0;
    const real = r.engine.crdt.bind(r.engine);
    r.engine.crdt = ((ks: string, key: string) => {
      opened++;
      return real(ks, key);
    }) as typeof r.engine.crdt;
    const res = r.q.run({ keyspaceId: KS, select: { prefix: "doc/" } });
    expect(res.rows).toHaveLength(1);
    expect(opened).toBe(0);
    expect((res.rows[0]?.value as { doc: Y.Doc }).doc).toBeDefined();
    expect(opened).toBe(1);
  });

  it("a CRDT key that the user has edited and not sent is shown too, and a pending update marks it pending", async () => {
    const r = await qrig();
    r.server.faults.paused = true;
    const h = r.engine.crdt(KS, "doc/d2");
    h.doc.getText("text").insert(0, "draft");
    const sub = await h.flush("DocEdit");
    await r.engine.settle();
    const x = row(r, "doc/d2");
    expect(x.value.kind).toBe("crdt");
    expect(x.pending).toEqual([sub?.mutationId]);
    expect(x.isPending).toBe(true);
    expect(x.predicted).toBe(true);
    expect(r.q.get(KS, "doc/d2", { mode: "confirmed" }).status).toBe("absent");
  });
});

describe("values of a pending action (14 [CLI-10], [CLI-12])", () => {
  it("a pending INCR shows the sum, as a counter", async () => {
    const r = await qrig();
    r.server.write(KS, [create(KeyChangeSchema, { key: "count/c", kind: ChangeKind.INCR, delta: 10n })]);
    await r.engine.settle();
    r.server.faults.paused = true;
    await r.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [incr("count/c", 5)] });
    await r.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [incr("count/c", 2)] });
    expect(row(r, "count/c").value).toEqual({ kind: "counter", value: 17n });
    expect(row(r, "count/c", "confirmed").value).toEqual({ kind: "counter", value: 10n });
    // an INCR of a counter that does not exist yet
    await r.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [incr("count/new", 3)] });
    expect(row(r, "count/new").value).toEqual({ kind: "counter", value: 3n });
  });

  it("a pending SET shows its value, decoded", async () => {
    const r = await qrig();
    r.server.faults.paused = true;
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("item/x", { a: 1, b: [2] })] });
    expect(jsonOf(row(r, "item/x"))).toEqual({ a: 1n, b: [2n] });
  });
});
