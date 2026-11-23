// Small builders for the changes of an action (`ChangeInput`).
//
// Every json/v1 value that the engine authors is written by the canonical writer of `rules/jsonv1` (RFC 8785, 05 [STATE-7]):
// `JSON.stringify` keeps the members of an object in the order of their insertion (and the integer-like ones first, in numeric
// order), where RFC 8785 sorts them by the UTF-16 code units of their names, so its text is often not canonical, and the server
// refuses a value that is not canonical (`SCHEMA_INVALID`, reason `not-canonical`). A value that is not valid json/v1 (14 [CLI-22]:
// `undefined`, `NaN` and `Infinity` among them, a lone surrogate, nesting over 32, a bigint) is refused here, with a typed local
// error, before anything is stored or shown.

import { ChangeKind } from "../gen/abcp/v2/types_pb.js";
import { canonical, JsonV1Error, validate, MAX_DEPTH, type JsonValue } from "../rules/jsonv1/index.js";
import { LocalRefusal } from "./errors.js";
import type { ChangeInput } from "./types.js";

/** Reads an ordinary JavaScript value as a json/v1 value. Throws a `LocalRefusal` for one that is none (14 [CLI-22]). */
function toJson(v: unknown, depth: number): JsonValue {
  if (v === null) return null;
  switch (typeof v) {
    case "boolean":
    case "string":
      return v;
    case "number":
      if (!Number.isFinite(v)) throw new LocalRefusal("INVALID", `${String(v)} is not a json/v1 number (14 [CLI-22])`);
      return v;
    case "undefined":
      throw new LocalRefusal("INVALID", "undefined is not a json/v1 value (14 [CLI-22])");
    case "bigint":
      throw new LocalRefusal("INVALID", "a bigint is not a json/v1 value: write it as a number within ±(2^53−1) or as a string (14 [CLI-22])");
    case "object":
      break;
    default:
      throw new LocalRefusal("INVALID", `a ${typeof v} is not a json/v1 value (14 [CLI-22])`);
  }
  if (depth > MAX_DEPTH) throw new LocalRefusal("INVALID", `nesting deeper than ${MAX_DEPTH} (05 [STATE-7])`);
  if (Array.isArray(v)) return v.map((x) => toJson(x, depth + 1));
  const proto = Object.getPrototypeOf(v) as unknown;
  if (v instanceof Map) {
    const m = new Map<string, JsonValue>();
    for (const [k, x] of v) {
      if (typeof k !== "string") throw new LocalRefusal("INVALID", "an object member name is a string (14 [CLI-22])");
      m.set(k, toJson(x, depth + 1));
    }
    return m;
  }
  if (proto !== Object.prototype && proto !== null) throw new LocalRefusal("INVALID", "only plain objects, arrays, strings, numbers, booleans and null are json/v1 values (14 [CLI-22])");
  const m = new Map<string, JsonValue>();
  for (const k of Object.keys(v as object)) m.set(k, toJson((v as Record<string, unknown>)[k], depth + 1));
  return m;
}

/**
 * The bytes of a JSON value as json/v1 text, in the RFC 8785 canonical form that 05 [STATE-7] requires, checked against the limits of
 * json/v1. Throws a `LocalRefusal` (code `INVALID`) for a value that is not valid json/v1 (14 [CLI-22]).
 */
export function jsonBytes(v: unknown): Uint8Array {
  const j = toJson(v, 1);
  let bytes: Uint8Array;
  try {
    bytes = canonical(j);
  } catch (e) {
    if (e instanceof RangeError) throw new LocalRefusal("INVALID", e.message);
    throw e;
  }
  const bad = validate(bytes);
  if (bad !== null) throw new LocalRefusal("INVALID", `not a json/v1 value: ${bad.message}`);
  return bytes;
}

/**
 * Checks bytes that an app supplies as a json/v1 value (a `SET`, `CAS`, `OVERRIDE` value or a `CAS` expectation): valid json/v1 in
 * the canonical form, which the server requires (05 [STATE-7]). Returns the reason, or undefined when it is one.
 */
export function checkJsonV1Bytes(b: Uint8Array): string | undefined {
  const bad: JsonV1Error | null = validate(b);
  return bad === null ? undefined : bad.message;
}

/** `SET` of an `lww` key to a JSON value. */
export function set(key: string, value: unknown, encoding = "json/v1"): ChangeInput {
  return { key, kind: ChangeKind.SET, value: encoding === "json/v1" ? jsonBytes(value) : (value as Uint8Array), encoding };
}

/** `SET` of an explicit null (05 [STATE-14]). */
export function setNull(key: string): ChangeInput {
  return { key, kind: ChangeKind.SET, isNull: true, encoding: "json/v1" };
}

export function del(key: string): ChangeInput {
  return { key, kind: ChangeKind.DELETE };
}

/** `INCR` of a counter. */
export function incr(key: string, delta: bigint | number): ChangeInput {
  return { key, kind: ChangeKind.INCR, delta: BigInt(delta) };
}

/** `CAS` of a `cas` key; `expected` omitted: the key's version in the confirmed state (14 [CLI-17]). */
export function cas(key: string, value: unknown, expected?: ChangeInput["expected"]): ChangeInput {
  return { key, kind: ChangeKind.CAS, value: jsonBytes(value), encoding: "json/v1", expected: expected ?? {} };
}

/** `CRDT_UPDATE` of a CRDT key with a Yjs update. */
export function crdtUpdate(key: string, update: Uint8Array): ChangeInput {
  return { key, kind: ChangeKind.CRDT_UPDATE, update };
}
