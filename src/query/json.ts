// Decoding json/v1 for a reader (05 [STATE-7]): `rules/jsonv1` reads the text strictly; here its values become what an app compares and shows. An integer
// within ±(2^53−1) is a `bigint` (every such integer is exact in binary64, and a `bigint` never passes 2^53 unnoticed, as counters do); any other number is a
// `number`. Objects are frozen and have no prototype.

import { compareUtf8 } from "../rules/bytes.js";
import { isInteger, parse, type JsonValue } from "../rules/jsonv1/index.js";
import { QueryError } from "./errors.js";
import type { JsonPath, QueryJson } from "./types.js";

/** One value's decoding is kept for as long as its bytes are: the bytes of a row in the replica are never changed, and a re-evaluation reads the same ones. */
const memo = new WeakMap<Uint8Array, QueryJson>();

/** The reader's form of a parsed json/v1 value. */
export function toQueryJson(v: JsonValue): QueryJson {
  if (v === null) return null;
  switch (typeof v) {
    case "number":
      return isInteger(v) ? BigInt(v) : v;
    case "string":
    case "boolean":
      return v;
    default:
      break;
  }
  if (Array.isArray(v)) return Object.freeze(v.map(toQueryJson));
  const o: Record<string, QueryJson> = Object.create(null) as Record<string, QueryJson>;
  for (const [k, x] of v) o[k] = toQueryJson(x);
  return Object.freeze(o);
}

/** Decodes json/v1 text; throws the `JsonV1Error` of the reader for text that is none. */
export function decodeJson(bytes: Uint8Array): QueryJson {
  const have = memo.get(bytes);
  if (have !== undefined) return have;
  const v = toQueryJson(parse(bytes));
  memo.set(bytes, v);
  return v;
}

/** A filter's value as the decoder would give it: an integer within ±(2^53−1) is a `bigint`. Refuses what json/v1 has no value for. */
export function normalize(v: unknown, depth = 0): QueryJson {
  if (depth > 33) throw new QueryError("INVALID_FILTER", "a filter value nested deeper than json/v1 allows");
  if (v === null) return null;
  switch (typeof v) {
    case "bigint":
    case "string":
    case "boolean":
      return v;
    case "number":
      if (!Number.isFinite(v)) throw new QueryError("INVALID_FILTER", `${String(v)} is not a json/v1 number`);
      return isInteger(v) ? BigInt(v) : v;
    case "object":
      break;
    default:
      throw new QueryError("INVALID_FILTER", `a ${typeof v} is not a json/v1 value`);
  }
  if (Array.isArray(v)) return v.map((x) => normalize(x, depth + 1));
  const o: Record<string, QueryJson> = Object.create(null) as Record<string, QueryJson>;
  for (const k of Object.keys(v as object)) o[k] = normalize((v as Record<string, unknown>)[k], depth + 1);
  return o;
}

/** Equality of two values of one decoding (a number is never equal to a `bigint`: an integer is always a `bigint`). Objects and arrays are compared deeply. */
export function deepEqual(a: QueryJson, b: QueryJson): boolean {
  if (a === b) return true;
  if (typeof a !== "object" || typeof b !== "object" || a === null || b === null) return false;
  if (Array.isArray(a)) {
    if (!Array.isArray(b) || a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) if (!deepEqual(a[i] as QueryJson, (b as readonly QueryJson[])[i] as QueryJson)) return false;
    return true;
  }
  if (Array.isArray(b)) return false;
  const ka = Object.keys(a);
  if (ka.length !== Object.keys(b).length) return false;
  for (const k of ka) {
    if (!Object.hasOwn(b, k)) return false;
    if (!deepEqual((a as Record<string, QueryJson>)[k] as QueryJson, (b as Record<string, QueryJson>)[k] as QueryJson)) return false;
  }
  return true;
}

/**
 * The order of two scalars: numbers (`bigint` or `number`, compared exactly), strings (by the byte order of their UTF-8, which is that of their code
 * points), booleans (false first). Undefined for two values that are not of one of these kinds together.
 */
export function compareScalar(a: QueryJson, b: QueryJson): number | undefined {
  const na = typeof a === "bigint" || typeof a === "number";
  const nb = typeof b === "bigint" || typeof b === "number";
  if (na && nb) return a < b ? -1 : a > b ? 1 : 0;
  if (typeof a === "string" && typeof b === "string") return compareUtf8(a, b);
  if (typeof a === "boolean" && typeof b === "boolean") return a === b ? 0 : a ? 1 : -1;
  return undefined;
}

// ---------------------------------------------------------------------------------------------------------------------
// Paths

export type PathSegment = string | number;

/** Reads a path: `a.b[0].c`, a leading `$` allowed, or the segments themselves. Throws a `QueryError` for text that is no path. */
export function parsePath(p: JsonPath): PathSegment[] {
  if (typeof p !== "string") {
    for (const s of p) {
      if (typeof s === "number" ? !Number.isInteger(s) || s < 0 : typeof s !== "string") throw new QueryError("INVALID_PATH", "a path segment is a member name or an array index");
    }
    return [...p];
  }
  const out: PathSegment[] = [];
  let i = p.startsWith("$") ? 1 : 0;
  let first = i === 0;
  while (i < p.length) {
    const c = p.charAt(i);
    if (c === "[") {
      const end = p.indexOf("]", i);
      const digits = end < 0 ? "" : p.slice(i + 1, end);
      if (!/^\d{1,15}$/.test(digits)) throw new QueryError("INVALID_PATH", `"${p}": an index in [] is a number`);
      out.push(Number(digits));
      i = end + 1;
    } else {
      if (c === ".") i++;
      else if (!first) throw new QueryError("INVALID_PATH", `"${p}": a member name follows a "." or starts the path`);
      let j = i;
      while (j < p.length && p.charAt(j) !== "." && p.charAt(j) !== "[") j++;
      if (j === i) throw new QueryError("INVALID_PATH", `"${p}": an empty member name`);
      out.push(p.slice(i, j));
      i = j;
    }
    first = false;
  }
  return out;
}

/** The value at the path, or undefined when the path is not there (a `null` at the end is a value). */
export function getAt(v: QueryJson, path: readonly PathSegment[]): QueryJson | undefined {
  let cur: QueryJson | undefined = v;
  for (const seg of path) {
    if (cur === null || typeof cur !== "object") return undefined;
    if (Array.isArray(cur)) {
      if (typeof seg !== "number") return undefined;
      cur = (cur as readonly QueryJson[])[seg];
    } else {
      if (typeof seg !== "string" || !Object.hasOwn(cur, seg)) return undefined;
      cur = (cur as Record<string, QueryJson>)[seg];
    }
  }
  return cur;
}
