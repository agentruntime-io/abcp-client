// The local query layer (C4): the kinds of query, ordering, filters on json/v1 values, and paging. 14 [CLI-6] (the view), 03 [ID-14] and [ID-17] (prefixes and templates),
// 05 §5.3 (absent and null), §5.6 and [STATE-43] (entities), 02 [ARC-62] (byte order of keys), 09 [READ-9] (tombstones are never keys).

import { describe, expect, it } from "vitest";
import { create } from "@bufbuild/protobuf";
import { ChangeKind, KeyChangeSchema } from "../src/gen/abcp/v2/types_pb.js";
import { QueryError } from "../src/query/index.js";
import { drop, jsonOf, keysOf, KS, put, qrig, rawSet } from "./helpers/query.js";

describe("a query by key (14 [CLI-6], 05 §5.3)", () => {
  it("reads a present key, a key that is absent, and an explicit null, which is present (05 [STATE-18])", async () => {
    const r = await qrig();
    put(r, { "item/a": "x" });
    r.server.write(KS, [create(KeyChangeSchema, { key: "item/n", kind: ChangeKind.SET, isNull: true, encoding: "json/v1" })]);
    await r.engine.settle();
    const a = r.q.get(KS, "item/a");
    expect(a.status).toBe("present");
    if (a.status === "present") {
      expect(a.row.value).toEqual({ kind: "json", value: "x" });
      expect(a.row.version).toBeGreaterThan(0n);
      expect(a.row.isPending).toBe(false);
      expect(a.row.predicted).toBe(false);
    }
    expect(r.q.get(KS, "item/none")).toEqual({ status: "absent" });
    const n = r.q.get(KS, "item/n");
    expect(n.status === "present" && n.row.value).toEqual({ kind: "null" });
  });

  it("a key that was deleted is absent, and its tombstone is not a key (09 [READ-9])", async () => {
    const r = await qrig();
    put(r, { "item/a": 1, "item/b": 2 });
    drop(r, "item/a");
    await r.engine.settle();
    expect(r.q.get(KS, "item/a")).toEqual({ status: "absent" });
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "item/" } }))).toEqual(["item/b"]);
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "" } }))).toEqual(["item/b"]);
  });

  it("several keys: one row for each that is present, in order, without repeats", async () => {
    const r = await qrig();
    put(r, { "item/a": 1, "item/c": 3 });
    await r.engine.settle();
    const res = r.q.run({ keyspaceId: KS, select: { keys: ["item/c", "item/b", "item/a", "item/c"] } });
    expect(keysOf(res)).toEqual(["item/a", "item/c"]);
    expect(res.notLoaded).toEqual([]);
  });
});

describe("a query by key prefix (03 [ID-14])", () => {
  it("matches the keys below the prefix and not the key without the final slash", async () => {
    const r = await qrig();
    put(r, { "issue/1": {}, "issue/1/title": "a", "issue/1/meta": 1, "issue/12": {}, "issue/12/title": "b", "item/x": 0 });
    await r.engine.settle();
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "issue/1/" } }))).toEqual(["issue/1/meta", "issue/1/title"]);
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "issue/" } }))).toEqual(["issue/1", "issue/1/meta", "issue/1/title", "issue/12", "issue/12/title"]);
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "" } })).length).toBe(6);
  });

  it("refuses what is no key prefix", async () => {
    const r = await qrig();
    for (const bad of ["issue", "issue//", "/issue/", "issue/a b/", "issue/../"]) {
      expect(() => r.q.run({ keyspaceId: KS, select: { prefix: bad } }), bad).toThrow(QueryError);
    }
    expect(() => r.q.run({ keyspaceId: KS, select: { keys: ["issue/"] } })).toThrow(QueryError);
  });
});

describe("a query by template with variable bindings (03 [ID-16], [ID-17])", () => {
  it("matches the keys that have the template's shape and binds each variable to the segment it faces", async () => {
    const r = await qrig();
    put(r, { "issue/1": {}, "issue/1/title": "one", "issue/2/title": "two", "issue/2/meta": 7, "issue/2/comment/9": {}, "issue/2/comment/9/text": "hi" });
    await r.engine.settle();
    const res = r.q.run({ keyspaceId: KS, select: { template: "issue/{i}/title" } });
    expect(res.rows.map((x) => [x.key, x.bindings])).toEqual([
      ["issue/1/title", { i: "1" }],
      ["issue/2/title", { i: "2" }],
    ]);
    // a variable matches exactly one whole segment: the comment's text has more segments
    const text = r.q.run({ keyspaceId: KS, select: { template: "issue/{i}/comment/{c}/text" } });
    expect(text.rows.map((x) => x.bindings)).toEqual([{ i: "2", c: "9" }]);
  });

  it("bind fixes variables; a template that binds every variable names one key", async () => {
    const r = await qrig();
    put(r, { "issue/1/title": "one", "issue/2/title": "two", "issue/2/comment/9/text": "a", "issue/2/comment/8/text": "b", "issue/3/comment/9/text": "c" });
    await r.engine.settle();
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { template: "issue/{i}/title", bind: { i: "2" } } }))).toEqual(["issue/2/title"]);
    const res = r.q.run({ keyspaceId: KS, select: { template: "issue/{i}/comment/{c}/text", bind: { c: "9" } } });
    expect(res.rows.map((x) => x.key)).toEqual(["issue/2/comment/9/text", "issue/3/comment/9/text"]);
    expect(res.rows.map((x) => x.bindings)).toEqual([{ i: "2", c: "9" }, { i: "3", c: "9" }]);
    const one = r.q.run({ keyspaceId: KS, select: { template: "issue/{i}/title", bind: { i: "1" } } });
    expect(one.rows.map((x) => [x.key, x.bindings])).toEqual([["issue/1/title", { i: "1" }]]);
  });

  it("refuses a template that is none, a binding of a variable it has not, and a binding that is no segment", async () => {
    const r = await qrig();
    expect(() => r.q.run({ keyspaceId: KS, select: { template: "issue/{i}/{i}" } })).toThrow(QueryError);
    expect(() => r.q.run({ keyspaceId: KS, select: { template: "issue/t_{id}" } })).toThrow(QueryError);
    expect(() => r.q.run({ keyspaceId: KS, select: { template: "issue/{i}", bind: { z: "1" } } })).toThrow(QueryError);
    expect(() => r.q.run({ keyspaceId: KS, select: { template: "issue/{i}", bind: { i: "a/b" } } })).toThrow(QueryError);
  });
});

describe("an entity listing (05 §5.6, [STATE-43])", () => {
  it("lists the present markers of an entity template, and with `subtree` every key below each", async () => {
    const r = await qrig();
    put(r, { "issue/1": { n: 1 }, "issue/1/title": "one", "issue/1/comment/5": {}, "issue/1/comment/5/text": "c", "issue/2": { n: 2 }, "issue/2/title": "two", "item/x": 0 });
    await r.engine.settle();
    const plain = r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}" } });
    expect(plain.rows.map((x) => [x.key, x.bindings, x.subtree])).toEqual([
      ["issue/1", { i: "1" }, undefined],
      ["issue/2", { i: "2" }, undefined],
    ]);
    const deep = r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}", subtree: true } });
    expect(deep.rows.map((x) => [x.key, x.subtree?.map((s) => s.key)])).toEqual([
      ["issue/1", ["issue/1/comment/5", "issue/1/comment/5/text", "issue/1/title"]],
      ["issue/2", ["issue/2/title"]],
    ]);
    // nested entities
    const comments = r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}/comment/{c}", subtree: true } });
    expect(comments.rows.map((x) => [x.key, x.bindings, x.subtree?.map((s) => s.key)])).toEqual([["issue/1/comment/5", { i: "1", c: "5" }, ["issue/1/comment/5/text"]]]);
  });

  it("a deleted marker is absent, with every row under it (05 [STATE-43]); its tombstone is no entity", async () => {
    const r = await qrig();
    put(r, { "issue/1": {}, "issue/1/title": "one", "issue/1/comment/5": {}, "issue/1/comment/5/text": "c", "issue/2": {}, "issue/2/title": "two" });
    await r.engine.settle();
    drop(r, "issue/1");
    await r.engine.settle();
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}" } }))).toEqual(["issue/2"]);
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "issue/1/" } }))).toEqual([]);
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "" } }))).toEqual(["issue/2", "issue/2/title"]);
    expect(r.q.get(KS, "issue/1/comment/5/text")).toEqual({ status: "absent" });
  });

  it("refuses a template that the manifest declares and that is no entity template", async () => {
    const r = await qrig();
    expect(() => r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}/title" } })).toThrow(QueryError);
    expect(() => r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}" } })).not.toThrow();
  });
});

describe("ordering by key, in the byte order of UTF-8 (02 [ARC-62])", () => {
  it("is ascending by default, descending on request, and is not the order of a locale", async () => {
    const r = await qrig();
    // byte order: "-y" 0x2D, "." 0x2E, digits, upper case, "_" 0x5F, lower case
    put(r, { "item/a": 1, "item/B": 2, "item/_x": 3, "item/-y": 4, "item/1": 5, "item/.z": 6, "item/Z": 7 });
    await r.engine.settle();
    const asc = ["item/-y", "item/.z", "item/1", "item/B", "item/Z", "item/_x", "item/a"];
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "item/" } }))).toEqual(asc);
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "item/" }, order: "desc" }))).toEqual([...asc].reverse());
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { template: "item/{x}" } }))).toEqual(asc);
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { keys: [...asc].reverse() } }))).toEqual(asc);
  });

  it("a key is before the keys that extend it, and the slash orders as its byte", async () => {
    const r = await qrig();
    put(r, { "issue/1": {}, "issue/1-b": {}, "issue/1.b": {}, "issue/1/title": "t", "issue/10": {} });
    await r.engine.settle();
    // "/" is 0x2F: after "-" and "." and before the digits
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "issue/" } }))).toEqual(["issue/1", "issue/1-b", "issue/1.b", "issue/1/title", "issue/10"]);
  });
});

describe("filters on decoded json/v1 values", () => {
  const seed = async (): Promise<Awaited<ReturnType<typeof qrig>>> => {
    const r = await qrig();
    put(r, {
      "item/a": { n: 1, tag: "x", tags: ["p", "q"], inner: { k: 5 } },
      "item/b": { n: 2, tag: "y", tags: [], inner: { k: 7 } },
      "item/c": { n: 3, tag: "x", price: 1.5 },
      "item/d": { n: 9007199254740991 },
      "item/e": "plain text",
      "item/f": true,
    });
    r.server.write(KS, [create(KeyChangeSchema, { key: "item/g", kind: ChangeKind.SET, isNull: true, encoding: "json/v1" })]);
    await r.engine.settle();
    return r;
  };
  const run = (r: Awaited<ReturnType<typeof qrig>>, filter: Parameters<typeof r.q.run>[0]["filter"]): string[] => keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "item/" }, ...(filter !== undefined ? { filter } : {}) }));

  it("equality and comparison on a JSON path, and on the whole value", async () => {
    const r = await seed();
    expect(run(r, { path: "tag", op: "eq", value: "x" })).toEqual(["item/a", "item/c"]);
    expect(run(r, { path: "$.tag", op: "ne", value: "x" })).toEqual(["item/b", "item/d", "item/e", "item/f"]); // a json/v1 value with no `tag` is not equal to "x"; the explicit null has no value
    expect(run(r, { path: "n", op: "gt", value: 1 })).toEqual(["item/b", "item/c", "item/d"]);
    expect(run(r, { path: "n", op: "lte", value: 2 })).toEqual(["item/a", "item/b"]);
    expect(run(r, { path: "inner.k", op: "gte", value: 7 })).toEqual(["item/b"]);
    expect(run(r, { path: ["inner", "k"], op: "eq", value: 5 })).toEqual(["item/a"]);
    expect(run(r, { path: "tags[1]", op: "eq", value: "q" })).toEqual(["item/a"]);
    expect(run(r, { op: "eq", value: "plain text" })).toEqual(["item/e"]);
    expect(run(r, { op: "eq", value: true })).toEqual(["item/f"]);
    expect(run(r, { op: "gt", value: "m" })).toEqual(["item/e"]); // strings by UTF-8 byte order; other kinds are not comparable
    expect(run(r, { path: "tags", op: "eq", value: ["p", "q"] })).toEqual(["item/a"]);
    expect(run(r, { path: "inner", op: "eq", value: { k: 7 } })).toEqual(["item/b"]);
    expect(run(r, { path: "price", op: "lt", value: 2 })).toEqual(["item/c"]);
    expect(run(r, { path: "tag", op: "in", values: ["y", "z"] })).toEqual(["item/b"]);
  });

  it("exists, and and, or and not", async () => {
    const r = await seed();
    expect(run(r, { path: "price", op: "exists" })).toEqual(["item/c"]);
    expect(run(r, { path: "tag", op: "exists", value: false })).toEqual(["item/d", "item/e", "item/f"]);
    expect(run(r, { and: [{ path: "tag", op: "eq", value: "x" }, { path: "n", op: "gt", value: 1 }] })).toEqual(["item/c"]);
    expect(run(r, { or: [{ path: "n", op: "eq", value: 1 }, { path: "n", op: "eq", value: 3 }] })).toEqual(["item/a", "item/c"]);
    expect(run(r, { not: { path: "tag", op: "exists" } })).toEqual(["item/d", "item/e", "item/f", "item/g"]); // not a leaf: a row with no json/v1 value matches it
    expect(run(r, { and: [] })).toEqual(["item/a", "item/b", "item/c", "item/d", "item/e", "item/f", "item/g"]);
    expect(run(r, { or: [] })).toEqual([]);
  });

  it("integers are exact: a json/v1 integer is a bigint, and 2^53-1 compares as itself (05 [STATE-7])", async () => {
    const r = await seed();
    const d = r.q.get(KS, "item/d");
    expect(d.status === "present" && jsonOf(d.row)).toEqual({ n: 9007199254740991n });
    const c = r.q.get(KS, "item/c");
    expect(c.status === "present" && jsonOf(c.row)).toEqual({ n: 3n, tag: "x", price: 1.5 });
    expect(run(r, { path: "n", op: "eq", value: 9007199254740991n })).toEqual(["item/d"]);
    expect(run(r, { path: "n", op: "eq", value: 9007199254740991 })).toEqual(["item/d"]);
    expect(run(r, { path: "n", op: "gt", value: 9007199254740990n })).toEqual(["item/d"]);
    expect(run(r, { path: "n", op: "lt", value: 9007199254740991n })).toEqual(["item/a", "item/b", "item/c"]);
    // a bigint and a number are compared by their values
    expect(run(r, { path: "n", op: "gt", value: 2.5 })).toEqual(["item/c", "item/d"]);
    expect(run(r, { path: "price", op: "gt", value: 1n })).toEqual(["item/c"]);
    expect(run(r, { path: "n", op: "eq", value: 3n })).toEqual(["item/c"]);
  });

  it("a row with no json/v1 value (null, bytes) matches no leaf", async () => {
    const r = await seed();
    r.server.write(KS, [rawSet("raw/b", new Uint8Array([1, 2, 3]), "bytes/v1")]);
    await r.engine.settle();
    expect(run(r, { op: "ne", value: 1 })).toEqual(["item/a", "item/b", "item/c", "item/d", "item/e", "item/f"]); // item/g is an explicit null: no value
    expect(run(r, { op: "exists" })).toEqual(["item/a", "item/b", "item/c", "item/d", "item/e", "item/f"]);
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "raw/" }, filter: { op: "ne", value: 1 } }))).toEqual([]);
    expect(keysOf(r.q.run({ keyspaceId: KS, select: { prefix: "raw/" }, filter: { not: { op: "eq", value: 1 } } }))).toEqual(["raw/b"]);
  });

  it("refuses a filter that is none", async () => {
    const r = await seed();
    const bad = [{ path: "a..b", op: "eq", value: 1 }, { path: "a[x]", op: "eq", value: 1 }, { op: "like", value: 1 }, { path: "a", op: "eq", value: Number.NaN }, { and: [{ op: "eq", value: undefined }] }];
    for (const f of bad) expect(() => run(r, f as never), JSON.stringify(f)).toThrow(QueryError);
    expect(() => r.q.run({ keyspaceId: KS, select: { prefix: "item/" }, filter: { at: "title", op: "eq", value: "x" } })).toThrow(QueryError);
  });
});

describe("limit and cursor paging", () => {
  it("pages in key order, each key once, with the cursor of the next page; descending the same", async () => {
    const r = await qrig();
    const all: Record<string, number> = {};
    for (let i = 0; i < 23; i++) all[`item/k${String(i).padStart(2, "0")}`] = i;
    put(r, all);
    await r.engine.settle();
    for (const order of ["asc", "desc"] as const) {
      const got: string[] = [];
      let after: string | undefined;
      let pages = 0;
      for (;;) {
        const res = r.q.run({ keyspaceId: KS, select: { prefix: "item/" }, order, limit: 5, ...(after !== undefined ? { after } : {}) });
        pages++;
        got.push(...keysOf(res));
        expect(res.rows.length).toBeLessThanOrEqual(5);
        if (res.next === undefined) break;
        expect(res.next).toBe(res.rows[res.rows.length - 1]?.key);
        after = res.next;
      }
      const expected = Object.keys(all).sort();
      expect(got, order).toEqual(order === "asc" ? expected : expected.reverse());
      expect(pages).toBe(5);
    }
  });

  it("a page holds the rows that pass the filter, and a page that is exactly full has no next", async () => {
    const r = await qrig();
    put(r, { "item/a": { v: 1 }, "item/b": { v: 2 }, "item/c": { v: 1 }, "item/d": { v: 2 }, "item/e": { v: 1 } });
    await r.engine.settle();
    const f = { path: "v", op: "eq", value: 1 } as const;
    const p1 = r.q.run({ keyspaceId: KS, select: { prefix: "item/" }, filter: f, limit: 2 });
    expect(keysOf(p1)).toEqual(["item/a", "item/c"]);
    expect(p1.next).toBe("item/c");
    const p2 = r.q.run({ keyspaceId: KS, select: { prefix: "item/" }, filter: f, limit: 2, after: p1.next as string });
    expect(keysOf(p2)).toEqual(["item/e"]);
    expect(p2.next).toBeUndefined();
    const exact = r.q.run({ keyspaceId: KS, select: { prefix: "item/" }, filter: f, limit: 3 });
    expect(keysOf(exact)).toEqual(["item/a", "item/c", "item/e"]);
    expect(exact.next).toBeUndefined();
    expect(() => r.q.run({ keyspaceId: KS, select: { prefix: "item/" }, limit: 0 })).toThrow(QueryError);
  });

  it("an entity query pages by marker key, and a key query by its keys", async () => {
    const r = await qrig();
    put(r, { "issue/1": {}, "issue/2": {}, "issue/3": {}, "issue/2/title": "t" });
    await r.engine.settle();
    const p1 = r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}" }, limit: 2 });
    expect(keysOf(p1)).toEqual(["issue/1", "issue/2"]);
    const p2 = r.q.run({ keyspaceId: KS, select: { entities: "issue/{i}" }, limit: 2, after: p1.next as string });
    expect(keysOf(p2)).toEqual(["issue/3"]);
    const k = r.q.run({ keyspaceId: KS, select: { keys: ["issue/1", "issue/2", "issue/3"] }, limit: 2, order: "desc" });
    expect(keysOf(k)).toEqual(["issue/3", "issue/2"]);
    expect(k.next).toBe("issue/2");
  });
});
