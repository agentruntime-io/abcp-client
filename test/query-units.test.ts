// The local query layer (C4): the small pieces. The key index (02 [ARC-62], 03 [ID-14]: the keys below a prefix are a range in byte order), the scope index of the live
// queries, paths into json/v1 values, the order of scalars, and what the interest covers (14 [CLI-71], 09 [READ-22]).

import { describe, expect, it } from "vitest";
import { compareUtf8 } from "../src/rules/bytes.js";
import { KeyIndex, mergeSorted, ScopeIndex } from "../src/query/key-index.js";
import { compareScalar, deepEqual, getAt, normalize, parsePath, toQueryJson } from "../src/query/json.js";
import { coverageOfPrefix, firstSegments, inInterest } from "../src/query/select.js";
import { parse } from "../src/rules/jsonv1/index.js";
import { QueryError } from "../src/query/index.js";

const sorted = (xs: string[]): string[] => [...xs].sort(compareUtf8);

describe("the key index", () => {
  const keys = sorted(["a", "a-b", "a.b", "a/b", "a/b/c", "a/c", "a0", "b", "ab/c", "issue/1", "issue/1/title", "issue/10", "issue/2"]);

  it("a prefix is a contiguous range, found by search, and it is not the key without the slash", () => {
    const idx = new KeyIndex([...keys]);
    const inRange = (p: string): string[] => {
      const [lo, hi] = idx.range(p);
      return idx.slice(lo, hi);
    };
    expect(inRange("a/")).toEqual(["a/b", "a/b/c", "a/c"]);
    expect(inRange("a/b/")).toEqual(["a/b/c"]);
    expect(inRange("issue/1/")).toEqual(["issue/1/title"]);
    expect(inRange("issue/")).toEqual(["issue/1", "issue/1/title", "issue/10", "issue/2"]);
    expect(inRange("zz/")).toEqual([]);
    expect(inRange("")).toEqual(keys);
    expect(idx.hasPrefix("issue/1/")).toBe(true);
    expect(idx.hasPrefix("issue/3/")).toBe(false);
  });

  it("add and remove keep the order, and ignore a repeat", () => {
    const idx = new KeyIndex(sorted(["b", "d"]));
    idx.add("c");
    idx.add("a");
    idx.add("c");
    idx.remove("d");
    idx.remove("zz");
    expect(idx.slice(0, idx.size)).toEqual(["a", "b", "c"]);
    expect(idx.has("c")).toBe(true);
    expect(idx.has("d")).toBe(false);
    expect(idx.lowerBound("b")).toBe(1);
    expect(idx.upperBound("b")).toBe(2);
  });

  it("mergeSorted merges two lists without a repeat", () => {
    expect(mergeSorted(["a", "c", "e"], ["b", "c", "f"])).toEqual(["a", "b", "c", "e", "f"]);
    expect(mergeSorted([], ["x"])).toEqual(["x"]);
  });
});

describe("the scope index", () => {
  it("a key finds the scopes that contain it: itself, and the prefixes of its path", () => {
    const s = new ScopeIndex<string>();
    s.add("", "all");
    s.add("issue/", "issues");
    s.add("issue/1/", "issue1");
    s.add("issue/1/title", "title");
    s.add("issue/2/", "issue2");
    s.add("item/", "items");
    expect([...s.forKey("issue/1/title")].sort()).toEqual(["all", "issue1", "issues", "title"]);
    expect([...s.forKey("issue/1")].sort()).toEqual(["all", "issues"]); // the key is not below its own prefix
    expect([...s.forKey("setting/a")]).toEqual(["all"]);
    s.remove("", "all");
    expect([...s.forKey("setting/a")]).toEqual([]);
  });

  it("a subtree meets the scopes that contain it and those inside it", () => {
    const s = new ScopeIndex<string>();
    s.add("issue/", "issues");
    s.add("issue/1/", "issue1");
    s.add("issue/1/comment/", "comments");
    s.add("issue/1/title", "title");
    s.add("issue/12/", "issue12");
    s.add("issue/2/", "issue2");
    expect([...s.forSubtree("issue/1")].sort()).toEqual(["comments", "issue1", "issues", "title"]); // issue/12 is no part of the subtree of issue/1
    expect([...s.forSubtree("issue/2")].sort()).toEqual(["issue2", "issues"]);
    expect([...s.forSubtree("item/x")]).toEqual([]);
    s.remove("issue/1/", "issue1");
    expect([...s.forSubtree("issue/1")].sort()).toEqual(["comments", "issues", "title"]);
  });
});

describe("paths into json/v1 values", () => {
  it("reads member names and indexes, with an optional leading $", () => {
    expect(parsePath("a.b[0].c")).toEqual(["a", "b", 0, "c"]);
    expect(parsePath("$.a[12]")).toEqual(["a", 12]);
    expect(parsePath("$")).toEqual([]);
    expect(parsePath("")).toEqual([]);
    expect(parsePath("[3]")).toEqual([3]);
    expect(parsePath(["a.b", 2])).toEqual(["a.b", 2]);
    for (const bad of ["a..b", "a.", ".a.", "a[x]", "a[", "a[-1]", "a b[0]c"]) expect(() => parsePath(bad), bad).toThrow(QueryError);
    expect(() => parsePath([1.5])).toThrow(QueryError);
  });

  it("finds the value at a path, and tells a null from a path that is not there", () => {
    const v = toQueryJson(parse(new TextEncoder().encode('{"__proto__":3,"a":{"b":[10,{"c":null}]}}')));
    expect(getAt(v, ["a", "b", 0])).toBe(10n);
    expect(getAt(v, ["a", "b", 1, "c"])).toBeNull();
    expect(getAt(v, ["a", "b", 2])).toBeUndefined();
    expect(getAt(v, ["a", "x"])).toBeUndefined();
    expect(getAt(v, ["a", 0])).toBeUndefined(); // an index is for arrays, a name for objects
    expect(getAt(v, ["a", "b", "0"])).toBeUndefined();
    expect(getAt(v, ["__proto__"])).toBe(3n);
    expect(getAt(v, ["toString"])).toBeUndefined(); // nothing is read from a prototype
    expect(getAt(v, [])).toBe(v);
  });
});

describe("values and their order", () => {
  it("numbers compare as numbers, bigint with number exactly; strings by UTF-8 bytes; booleans false first; anything else does not compare", () => {
    expect(compareScalar(1n, 2)).toBe(-1);
    expect(compareScalar(2.5, 2n)).toBe(1);
    expect(compareScalar(9007199254740991n, 9007199254740991)).toBe(0);
    expect(compareScalar("a", "b")).toBe(-1);
    expect(compareScalar("￮", "\u{1f600}")).toBe(-1); // code point order: U+FFEE before U+1F600, as their UTF-8 bytes say (UTF-16 code units say the opposite)
    expect(compareScalar(false, true)).toBe(-1);
    expect(compareScalar("1", 1n)).toBeUndefined();
    expect(compareScalar(null, null)).toBeUndefined();
    expect(compareScalar(true, 1n)).toBeUndefined();
  });

  it("deep equality, and a filter value normalised as the decoder would give it", () => {
    expect(deepEqual(normalize({ a: [1, 2, { b: null }] }), toQueryJson(parse(new TextEncoder().encode('{"a":[1,2,{"b":null}]}'))))).toBe(true);
    expect(deepEqual(normalize([1, 2]), normalize([2, 1]))).toBe(false);
    expect(deepEqual(normalize({ a: 1 }), normalize({ a: 1, b: 2 }))).toBe(false);
    expect(deepEqual(normalize(1), normalize(1.5))).toBe(false);
    expect(normalize(3)).toBe(3n);
    expect(normalize(3.5)).toBe(3.5);
    expect(normalize(2 ** 60)).toBe(2 ** 60); // not an exact integer of json/v1: it stays a number
    expect(() => normalize(undefined)).toThrow(QueryError);
    expect(() => normalize(Number.POSITIVE_INFINITY)).toThrow(QueryError);
    expect(() => normalize(() => 1)).toThrow(QueryError);
  });
});

describe("the interest (14 [CLI-71])", () => {
  it("a key is covered by an interest prefix that is its own path or an ancestor's", () => {
    expect(inInterest([], "x/y")).toBe(true);
    expect(inInterest(["issue/1"], "issue/1")).toBe(true);
    expect(inInterest(["issue/1"], "issue/1/title")).toBe(true);
    expect(inInterest(["issue/1"], "issue/10")).toBe(false);
    expect(inInterest(["issue/1"], "issue")).toBe(false);
  });

  it("a prefix is covered in full by an interest prefix on its path, in part by one inside it", () => {
    expect(coverageOfPrefix([], "issue/")).toBe("full");
    expect(coverageOfPrefix(["issue"], "issue/1/")).toBe("full");
    expect(coverageOfPrefix(["issue/1"], "issue/1/")).toBe("full");
    expect(coverageOfPrefix(["issue/1"], "issue/")).toBe("partial");
    expect(coverageOfPrefix(["issue/1"], "")).toBe("partial");
    expect(coverageOfPrefix(["issue/1"], "issue/2/")).toBe("none");
    expect(coverageOfPrefix(["issue2"], "issue/")).toBe("none");
  });

  it("the first segments of a key", () => {
    expect(firstSegments("a/b/c/d", 2)).toBe("a/b");
    expect(firstSegments("a/b", 2)).toBeUndefined();
    expect(firstSegments("a/b", 1)).toBe("a");
  });
});
