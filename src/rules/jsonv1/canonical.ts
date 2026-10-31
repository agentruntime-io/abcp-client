// The RFC 8785 (JCS) serialization of a JSON value, the canonical form of 05 [STATE-7] and [STATE-86]: no whitespace,
// object members sorted by the UTF-16 code units of their names, strings written as UTF-8 with only the escapes of
// RFC 8785 §3.2.2.2, numbers by ECMAScript Number::toString.

import { compareUtf16, utf8Encode } from "../bytes.js";
import { numberText } from "./number.js";
import { type JsonValue, MAX_DOCUMENT_DEPTH } from "./value.js";

/**
 * Returns the RFC 8785 serialization of v as UTF-8 octets. It refuses a value that has no such text with a RangeError: a
 * number that is not finite, a kind that is not a JSON kind, or nesting deeper than MAX_DOCUMENT_DEPTH. A value that
 * `parse` or `parseDocument` returned never fails. It does not check the limits of json/v1 (`validate` does that).
 */
export function canonical(v: JsonValue): Uint8Array {
  const out: string[] = [];
  write(out, v, 1);
  return utf8Encode(out.join(""));
}

function write(out: string[], v: JsonValue, depth: number): void {
  if (v === null) {
    out.push("null");
  } else if (typeof v === "boolean") {
    out.push(v ? "true" : "false");
  } else if (typeof v === "number") {
    out.push(numberText(v));
  } else if (typeof v === "string") {
    // JSON.stringify writes exactly the escapes of RFC 8785 §3.2.2.2: the two-character escapes for \b \t \n \f \r " and \,
    // \u00xx in lowercase for the other controls, and every other character as itself.
    out.push(JSON.stringify(v));
  } else if (Array.isArray(v)) {
    if (depth > MAX_DOCUMENT_DEPTH) throw new RangeError(`nesting deeper than ${MAX_DOCUMENT_DEPTH}`);
    out.push("[");
    v.forEach((it, i) => {
      if (i > 0) out.push(",");
      write(out, it, depth + 1);
    });
    out.push("]");
  } else if (v instanceof Map) {
    if (depth > MAX_DOCUMENT_DEPTH) throw new RangeError(`nesting deeper than ${MAX_DOCUMENT_DEPTH}`);
    const members = [...v.entries()].sort((a, b) => compareUtf16(a[0], b[0]));
    out.push("{");
    members.forEach(([name, value], i) => {
      if (i > 0) out.push(",");
      out.push(JSON.stringify(name), ":");
      write(out, value, depth + 1);
    });
    out.push("}");
  } else {
    throw new RangeError("not a JSON value");
  }
}
