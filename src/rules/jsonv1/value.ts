// The parsed form of a JSON value, and the limits of 05 [STATE-7] and 04 [MAN-25], [MAN-4].

/**
 * A parsed JSON value. A number is the binary64 value that its text denotes (05 [STATE-7]), so every integer within
 * ±(2^53−1) is exact in it. An object is a `Map` that keeps the order of the text and cannot be polluted by a member
 * named `__proto__`; a value that `parse` returned has no duplicate member.
 */
export type JsonValue = null | boolean | number | string | JsonValue[] | JsonObject;
export type JsonObject = Map<string, JsonValue>;

/** The deepest nesting of a json/v1 value (05 [STATE-7]): a top-level object or array has depth 1. */
export const MAX_DEPTH = 32;
/** The most members of one object of a json/v1 value (05 [STATE-7]). */
export const MAX_MEMBERS = 1024;
/** 2^53−1, the bound of a json/v1 integer and of a counter (05 [STATE-7], [STATE-27], [STATE-28]). */
export const MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER;
/** The deepest nesting of a value schema (04 [MAN-25]): a value_schema has depth 1. */
export const MAX_SCHEMA_DEPTH = 8;
/**
 * Bounds the nesting of a manifest document and of any value that `canonical` writes. A manifest is not a json/v1 value, so
 * [STATE-7]'s 32 does not apply to it; this bound only keeps untrusted input from recursing without limit (02 [ARC-46]).
 *
 * SPEC-QUESTION: 04 [MAN-4]: the text bounds a manifest's size but not its nesting; this bound is the Go server's own
 * (jsonv1.MaxDocumentDepth) and not the spec's.
 */
export const MAX_DOCUMENT_DEPTH = 128;
/** The largest manifest document as published: 4 MiB (04 [MAN-4]). */
export const MAX_DOCUMENT_BYTES = 4 << 20;
/** The largest canonical form of a manifest: 1 MiB (04 [MAN-4]). */
export const MAX_CANONICAL_DOCUMENT_BYTES = 1 << 20;

/** True when f is an integer of 05 [STATE-7]: a binary64 value with zero fraction within ±(2^53−1). −0 is one (it is 0). */
export function isInteger(f: number): boolean {
  return Number.isInteger(f) && Math.abs(f) <= MAX_SAFE_INTEGER;
}

/** Returns the member of an object value, or undefined. */
export function getMember(v: JsonValue | undefined, name: string): JsonValue | undefined {
  return v instanceof Map ? v.get(name) : undefined;
}
