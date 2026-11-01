// Filters on the decoded json/v1 values of rows (types.ts, `Filter`): compiled once per query into a predicate.

import { isSegment } from "../rules/fold/index.js";
import { QueryError } from "./errors.js";
import { compareScalar, deepEqual, getAt, normalize, parsePath, type PathSegment } from "./json.js";
import type { Filter, QueryJson, RowValue } from "./types.js";

/** What a predicate reads: the row's own value, and the value of a key below the entity (an entity query's `at`). */
export interface FilterInput {
  self: RowValue;
  at(relative: string): RowValue | undefined;
}

export type Predicate = (input: FilterInput) => boolean;

export interface Compiled {
  test: Predicate;
  /** Some leaf reads a key below the entity (`at`). */
  usesAt: boolean;
}

/** The JSON value that a row holds, for a filter to read: a decoded json/v1 value or a counter's integer; none for any other kind of row. */
function jsonOf(v: RowValue | undefined): QueryJson | undefined {
  if (v === undefined) return undefined;
  if (v.kind === "json") return v.value;
  if (v.kind === "counter") return v.value;
  return undefined;
}

/** Compiles a filter; throws a `QueryError` for one that is none. */
export function compileFilter(f: Filter): Compiled {
  let usesAt = false;
  const build = (x: Filter, depth: number): Predicate => {
    if (depth > 32) throw new QueryError("INVALID_FILTER", "filters nested deeper than 32");
    if (typeof x !== "object" || x === null) throw new QueryError("INVALID_FILTER", "a filter is an object");
    if ("and" in x) {
      const ps = x.and.map((y) => build(y, depth + 1));
      return (i) => ps.every((p) => p(i));
    }
    if ("or" in x) {
      const ps = x.or.map((y) => build(y, depth + 1));
      return (i) => ps.some((p) => p(i));
    }
    if ("not" in x) {
      const p = build(x.not, depth + 1);
      return (i) => !p(i);
    }
    if (!("op" in x)) throw new QueryError("INVALID_FILTER", "a filter is a comparison (with `op`), `and`, `or` or `not`");
    const path: readonly PathSegment[] = x.path === undefined ? [] : parsePath(x.path);
    let at: string | undefined;
    if (x.at !== undefined) {
      const parts = x.at.split("/");
      if (parts.length === 0 || !parts.every(isSegment)) throw new QueryError("INVALID_FILTER", `"${x.at}" is not a key path below an entity`);
      at = x.at;
      usesAt = true;
    }
    const read = (i: FilterInput): QueryJson | undefined => {
      const base = jsonOf(at === undefined ? i.self : i.at(at));
      return base === undefined ? undefined : getAt(base, path);
    };
    switch (x.op) {
      case "eq": {
        const want = normalize(x.value);
        return (i) => {
          const got = read(i);
          return got !== undefined && deepEqual(got, want);
        };
      }
      case "ne": {
        const want = normalize(x.value);
        return (i) => {
          if (jsonOf(at === undefined ? i.self : i.at(at)) === undefined) return false; // a row with no json/v1 value matches no leaf
          const got = read(i);
          return got === undefined || !deepEqual(got, want);
        };
      }
      case "in": {
        const wants = x.values.map((v) => normalize(v));
        return (i) => {
          const got = read(i);
          return got !== undefined && wants.some((w) => deepEqual(got, w));
        };
      }
      case "lt":
      case "lte":
      case "gt":
      case "gte": {
        const want = normalize(x.value);
        const op = x.op;
        return (i) => {
          const got = read(i);
          if (got === undefined) return false;
          const c = compareScalar(got, want);
          if (c === undefined) return false;
          return op === "lt" ? c < 0 : op === "lte" ? c <= 0 : op === "gt" ? c > 0 : c >= 0;
        };
      }
      case "exists": {
        const want = x.value ?? true;
        return (i) => {
          if (jsonOf(at === undefined ? i.self : i.at(at)) === undefined) return false;
          return (read(i) !== undefined) === want;
        };
      }
      default:
        throw new QueryError("INVALID_FILTER", `the operator "${String((x as { op: unknown }).op)}" does not exist`);
    }
  };
  const test = build(f, 0);
  return { test, usesAt };
}
