// Counters (05 [STATE-26] to [STATE-28]): a counter's value is a json/v1 integer within the range of its template, and an
// INCR adds a delta to it. Every integer here is a bigint, so that nothing passes 2^53 unnoticed.

import { JsonV1Error, valueError } from "./errors.js";
import { parse } from "./parse.js";
import { DEFAULT_RANGE, type Range } from "./schema.js";
import { MAX_SAFE_INTEGER, isInteger } from "./value.js";

const MAX = BigInt(MAX_SAFE_INTEGER);

/** True when v lies within the range, and within ±(2^53−1) whatever the range says. */
export function rangeContains(r: Range, v: bigint): boolean {
  return v >= r.min && v <= r.max && v >= -MAX && v <= MAX;
}

/** The canonical json/v1 text of a counter's value, which a server stores (05 [STATE-26]): decimal, no fraction, no exponent, no sign on zero. */
export function counterText(c: bigint): Uint8Array {
  return new TextEncoder().encode(c.toString(10));
}

function rangeError(base: bigint, delta: bigint): JsonV1Error {
  return new JsonV1Error("RANGE_EXCEEDED", "range", "STATE-28", `the counter ${base} with the delta ${delta} leaves the range`);
}

/**
 * Applies an INCR of delta to the counter `cur` (05 [STATE-27], [STATE-28]) and returns its new value. A null `cur` is an
 * absent counter, which starts from 0 and becomes present: an INCR of 0 on it creates the counter at 0 (and gets
 * RANGE_EXCEEDED when the range excludes 0), and an INCR of 0 on a present counter leaves the value. A delta of
 * magnitude above 2^53−1 gets SCHEMA_INVALID (reason `delta-range`); a result outside the range gets RANGE_EXCEEDED
 * (reason `range`). Both are thrown as a JsonV1Error.
 */
export function applyIncr(cur: bigint | null, delta: bigint, range: Range = DEFAULT_RANGE): bigint {
  if (delta > MAX || delta < -MAX) {
    throw valueError("delta-range", "STATE-27", `the magnitude of delta ${delta} is above 2^53-1`);
  }
  const base = cur ?? 0n;
  const res = base + delta; // a bigint never overflows
  if (!rangeContains(range, res)) throw rangeError(base, delta);
  return res;
}

/**
 * Reads a counter value from its json/v1 text (05 [STATE-26]): a json/v1 value (05 [STATE-7]) that is an integer. A value that
 * is valid json/v1 but not an integer, such as 1.5, 2^53 or a string, has reason `not-integer`. The range of the template
 * is not applied. Throws a JsonV1Error.
 */
export function parseCounter(text: Uint8Array): bigint {
  const v = parse(text);
  if (typeof v !== "number" || !isInteger(v)) {
    throw valueError("not-integer", "STATE-26", "a counter value is an integer within ±(2^53-1)");
  }
  return BigInt(v);
}

/**
 * Applies an OVERRIDE of a counter with the json/v1 text of the new value (05 [STATE-26]). The value must be a json/v1
 * integer within the range: any other gets SCHEMA_INVALID, never RANGE_EXCEEDED (a value outside the range has reason
 * `range`). The reason of a value that is not valid json/v1 is the reason of `validate`.
 */
export function applyOverride(text: Uint8Array, range: Range = DEFAULT_RANGE): bigint {
  const c = parseCounter(text);
  if (!rangeContains(range, c)) throw valueError("range", "STATE-26", `the value ${c} is outside the range of the template`);
  return c;
}
