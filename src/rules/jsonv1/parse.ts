// The strict json/v1 reader of 05 [STATE-7], [STATE-15]: one pass over the octets of a value, checking I-JSON (RFC 7493),
// duplicate members, the limits of [STATE-7], the numbers, RFC 8785 canonical form, and, when given, the value schema of
// 04 §4.4.2. General JSON libraries make none of these checks on their own (JSON.parse accepts deeper nesting, more
// members, noncharacters, a duplicate member and any number text), so this package carries its own parser. It recurses
// only to an explicit depth limit and never throws anything but a JsonV1Error (02 [ARC-46]).

import { isValidUtf8, utf8DecodeValid } from "../bytes.js";
import { echo, JsonV1Error, quote, valueError } from "./errors.js";
import { denote, numberText } from "./number.js";
import {
  fitNumber,
  fitString,
  schemaLengthWithin,
  type Schema,
} from "./schema.js";
import {
  type JsonValue,
  MAX_DEPTH,
  MAX_CANONICAL_DOCUMENT_BYTES,
  MAX_DOCUMENT_BYTES,
  MAX_DOCUMENT_DEPTH,
  MAX_DOCUMENT_VALUES,
  MAX_MEMBERS,
} from "./value.js";

/**
 * Checks b as a json/v1 value (05 [STATE-7], [STATE-15]) and, when schema is given, against the template's value schema
 * (04 §4.4.2, [MAN-26], [MAN-27]). No schema is a template with no `value_schema`: any I-JSON value within the limits of
 * STATE-7 (MAN-27). Returns null for a valid value and a JsonV1Error (code SCHEMA_INVALID, the vector's reason)
 * otherwise. The value is stored as received and never re-serialized (05 [STATE-3]).
 *
 * The explicit null of a key is not a value of this check: it is a change with `is_null` set and an empty value
 * (05 [STATE-14]), and the bytes `null` are never a stored value (05 [STATE-15], reason `literal-null`).
 *
 * Several defects can hold at once; the one reported is the first that this order finds: bytes that are not UTF-8, then
 * the first syntax, duplicate-member, surrogate, noncharacter, number or depth/member-limit defect in text order, then
 * U+0000, then a text that is not in canonical form, then the literal null, then the schema. (The reference models of the
 * vectors report in this order.)
 */
export function validate(b: Uint8Array, schema?: Schema): JsonV1Error | null {
  try {
    new Parser(b, false, false, MAX_DEPTH, MAX_MEMBERS).run(schema);
    return null;
  } catch (e) {
    if (e instanceof JsonV1Error) return e;
    throw e;
  }
}

/** Checks b as `validate` does with no schema and returns its value. Every integer in it is exact. Throws a JsonV1Error. */
export function parse(b: Uint8Array): JsonValue {
  return new Parser(b, false, true, MAX_DEPTH, MAX_MEMBERS).run(undefined);
}

/**
 * Reads a manifest document of encoding json/v1 as 04 [MAN-3] says. It is I-JSON (RFC 7493) in UTF-8 with no byte order
 * mark, so it refuses bytes that are not UTF-8, a text that is not JSON, a duplicate member, an unpaired surrogate escape,
 * a noncharacter, a number that does not denote a finite binary64 value (05 [STATE-7]: 1e400, and 1e-400, which rounds to
 * zero), and a string or member name that holds U+0000. A document is not a stored value: any whitespace, member order,
 * number text and escape is accepted (`canonicalDocument` gives its canonical form), and the depth and member limits of a
 * json/v1 value do not apply (nesting is bounded by MAX_DOCUMENT_DEPTH only so that untrusted input cannot recurse without
 * limit). The document is at most 4 MiB (04 [MAN-4]). Every error has code INVALID_ENVELOPE (04 [MAN-77]).
 *
 * 04 [MAN-3] (Q4, decided by 05 [STATE-7]): 1e-400 is refused like 1e400, and so is a string with a noncharacter, as the Go
 * server and the reference models do.
 */
export function parseDocument(b: Uint8Array): JsonValue {
  const p = new Parser(b, true, true, MAX_DOCUMENT_DEPTH, 0);
  if (b.length > MAX_DOCUMENT_BYTES) {
    throw new JsonV1Error(
      "INVALID_ENVELOPE",
      "json-limits",
      "MAN-4",
      `the document is ${b.length} octets, over the limit of ${MAX_DOCUMENT_BYTES}`,
    );
  }
  if (b.length >= 3 && b[0] === 0xef && b[1] === 0xbb && b[2] === 0xbf) {
    throw p.err("json-syntax", "the document begins with U+FEFF");
  }
  return p.run(undefined);
}

/**
 * Checks one JSON text of a CRDT update (05 [STATE-71], Strings): one I-JSON text (RFC 7493) with no duplicate object member,
 * whose numbers are valid as 05 [STATE-7] says (finite as binary64, and zero only when every digit is zero), within the
 * depth, member and U+0000 limits of [STATE-7], and not necessarily in canonical form. b is valid UTF-8 (the update was read
 * strictly). Returns null for a valid text and a JsonV1Error otherwise (reason `json-limits` for a limit, else another).
 *
 * U+0000 in these texts is refused as [STATE-7] says, as the Go server does (Q6, decided by the text: the reference models apply the
 * limit to them since draft 14, and the vectors `crdt-str-020` and `crdt-str-021` hold it).
 */
export function checkJsonText(b: Uint8Array): JsonV1Error | null {
  try {
    new Parser(b, false, false, MAX_DEPTH, MAX_MEMBERS, true).run(undefined);
    return null;
  } catch (e) {
    if (e instanceof JsonV1Error) return e;
    throw e;
  }
}

const isDigit = (c: number): boolean => c >= 0x30 && c <= 0x39;

/** True when r is a Unicode noncharacter, U+FDD0 to U+FDEF or U+nFFFE and U+nFFFF: I-JSON does not allow one in a string. */
function isNonchar(r: number): boolean {
  return (r >= 0xfdd0 && r <= 0xfdef) || (r & 0xfffe) === 0xfffe;
}

class Parser {
  private pos = 0;
  private notCanonical = false; // the text differs from the RFC 8785 form of its value
  private hasNUL = false; // a string or member name holds U+0000
  private topNull = false; // the whole text is the literal null
  private values = 0; // the values read so far (a manifest document only)
  private schemaErr: JsonV1Error | null = null; // the first value-schema failure

  constructor(
    private readonly b: Uint8Array,
    private readonly doc: boolean, // a manifest document (MAN-3): no canonical form, no json/v1 limits, U+0000 refused
    private readonly build: boolean,
    private readonly maxDepth: number,
    private readonly maxMembers: number, // 0: no limit
    private readonly text = false, // a JSON text inside a CRDT update (05 [STATE-71]): I-JSON within the limits, any canonical form, null allowed
  ) {}

  run(schema: Schema | undefined): JsonValue {
    if (!isValidUtf8(this.b)) throw this.err("utf8", "not valid UTF-8");
    const v = this.value(0, schema, true);
    this.skipWS();
    if (this.pos !== this.b.length) throw this.err("json-syntax", "bytes after the value");
    if (this.hasNUL) throw this.err("json-limits", "a string or an object member name holds U+0000");
    if (this.doc || this.text) return v;
    if (this.notCanonical) throw this.err("not-canonical", "the text is not in RFC 8785 canonical form");
    if (this.topNull) throw this.err("literal-null", "the literal null is never a stored value; null is a change with is_null");
    if (this.schemaErr !== null) throw this.schemaErr;
    return v;
  }

  /** The refusal of a text that cannot be accepted: SCHEMA_INVALID for a json/v1 value, INVALID_ENVELOPE for a manifest document. */
  err(reason: JsonV1Error["reason"], message: string): JsonV1Error {
    const msg = reason === "utf8" || reason === "not-canonical" || reason === "literal-null" ? message : `${message} (byte ${this.pos})`;
    if (this.doc) return new JsonV1Error("INVALID_ENVELOPE", reason, "MAN-3", msg);
    return valueError(reason, reason === "literal-null" ? "STATE-15" : "STATE-7", msg);
  }

  /** Records the first failure against the value schema; the pass goes on, since a syntax defect later in the text takes precedence. */
  private failSchema(rule: string, message: string): void {
    if (this.schemaErr === null) this.schemaErr = valueError("schema", rule, message);
  }

  /** Skips JSON whitespace; any of it makes the text non-canonical. */
  private skipWS(): void {
    const b = this.b;
    const start = this.pos;
    while (this.pos < b.length) {
      const c = b[this.pos] as number;
      if (c === 0x20 || c === 0x09 || c === 0x0a || c === 0x0d) this.pos++;
      else break;
    }
    if (this.pos > start) this.notCanonical = true;
  }

  private peek(): number {
    return this.pos >= this.b.length ? -1 : (this.b[this.pos] as number);
  }

  /** Reads one value. depth is the depth of the container that holds it (0 at the top). */
  private value(depth: number, sch: Schema | undefined, top: boolean): JsonValue {
    if (this.doc && ++this.values > MAX_DOCUMENT_VALUES) {
      // MAN-4: a document of V values has a canonical form of at least 2V-1 octets, so one of more than MAX_DOCUMENT_VALUES cannot be within
      // 1 MiB. Refused here, before the tree is built (V1 SHOULD-6).
      throw new JsonV1Error(
        "INVALID_ENVELOPE",
        "json-limits",
        "MAN-4",
        `more than ${MAX_DOCUMENT_VALUES} values: the canonical form is over the limit of ${MAX_CANONICAL_DOCUMENT_BYTES} octets`,
      );
    }
    this.skipWS();
    if (this.pos >= this.b.length) throw this.err("json-syntax", "unexpected end of the text");
    const c = this.b[this.pos] as number;
    if (c === 0x7b) return this.object(depth + 1, sch); // {
    if (c === 0x5b) return this.array(depth + 1, sch); // [
    if (c === 0x22) {
      const s = this.str();
      if (sch !== undefined) {
        const why = fitString(sch, s);
        if (why !== "") this.failSchema("STATE-10", why);
      }
      return s;
    }
    if (c === 0x74 && this.literal("true")) {
      if (sch !== undefined && sch.type !== "bool") this.failSchema("STATE-10", `expected ${sch.type}, got bool`);
      return true;
    }
    if (c === 0x66 && this.literal("false")) {
      if (sch !== undefined && sch.type !== "bool") this.failSchema("STATE-10", `expected ${sch.type}, got bool`);
      return false;
    }
    if (c === 0x6e && this.literal("null")) {
      if (top) {
        // 05 [STATE-15]: at the top the literal null is never a value, whatever the schema says (04 [MAN-27]).
        this.topNull = true;
      } else if (sch !== undefined && !sch.nullable) {
        // 04 [MAN-27]: in a nested schema, null is allowed as that member or item only with nullable: true.
        this.failSchema("MAN-27", "null where the schema is not nullable");
      }
      return null;
    }
    return this.number(sch);
  }

  /** Consumes word when the text has it at the current position. */
  private literal(word: string): boolean {
    if (this.b.length - this.pos < word.length) return false;
    for (let i = 0; i < word.length; i++) {
      if (this.b[this.pos + i] !== word.charCodeAt(i)) return false;
    }
    this.pos += word.length;
    return true;
  }

  private object(depth: number, schema: Schema | undefined): JsonValue {
    if (depth > this.maxDepth) throw this.err("json-limits", `nesting deeper than ${this.maxDepth}`); // 05 [STATE-7]
    let sch = schema;
    if (sch !== undefined && sch.type !== "object") {
      this.failSchema("STATE-10", `expected ${sch.type}, got object`);
      sch = undefined;
    }
    this.pos++; // {
    const v: JsonValue = this.build ? new Map<string, JsonValue>() : null;
    this.skipWS();
    if (this.peek() === 0x7d) {
      this.pos++;
      this.checkRequired(sch, 0);
      return v;
    }
    const names = new Set<string>();
    let last: string | undefined;
    let required = 0;
    for (;;) {
      this.skipWS();
      if (this.peek() !== 0x22) throw this.err("json-syntax", "an object member name was expected");
      const name = this.str();
      if (names.has(name)) throw this.err("json-duplicate", `duplicate member ${quote(name)}`); // 05 [STATE-7]
      if (this.maxMembers > 0 && names.size >= this.maxMembers) {
        throw this.err("json-limits", `more than ${this.maxMembers} members`); // 05 [STATE-7]
      }
      if (last !== undefined && last > name) this.notCanonical = true; // RFC 8785 §3.2.3: members sorted by UTF-16 code units
      names.add(name);
      last = name;
      this.skipWS();
      if (this.peek() !== 0x3a) throw this.err("json-syntax", "a colon was expected");
      this.pos++;
      let child: Schema | undefined;
      if (sch !== undefined) {
        // 04 [MAN-26]: objects are closed; an object schema with no fields allows only {}.
        const f = sch.fields?.get(name);
        if (f !== undefined) {
          child = f;
          if (sch.requiredSet.has(name)) required++;
        } else {
          this.failSchema("MAN-26", `member ${quote(name)} is not a field of the schema`);
          sch = undefined;
        }
      }
      const cv = this.value(depth, child, false);
      if (v instanceof Map) v.set(name, cv);
      this.skipWS();
      const c = this.peek();
      if (c === 0x2c) {
        this.pos++;
        continue;
      }
      if (c === 0x7d) {
        this.pos++;
        this.checkRequired(sch, required);
        return v;
      }
      throw this.err("json-syntax", "a comma or } was expected");
    }
  }

  /** Fails the schema when an object that fits sch lacks a required field (04 [MAN-26], §4.4.2). */
  private checkRequired(sch: Schema | undefined, seen: number): void {
    // the count is that of the distinct names (V1 SHOULD-9): a Schema that a caller built with a name listed twice still counts it once
    if (sch !== undefined && seen < sch.requiredSet.size) this.failSchema("MAN-26", "a required field is missing");
  }

  private array(depth: number, schema: Schema | undefined): JsonValue {
    if (depth > this.maxDepth) throw this.err("json-limits", `nesting deeper than ${this.maxDepth}`); // 05 [STATE-7]
    let sch = schema;
    let items: Schema | undefined; // an array schema with no items allows items of any value, null included (04 [MAN-26])
    if (sch !== undefined) {
      if (sch.type !== "array") {
        this.failSchema("STATE-10", `expected ${sch.type}, got array`);
        sch = undefined;
      } else {
        items = sch.items;
      }
    }
    this.pos++; // [
    const v: JsonValue[] | null = this.build ? [] : null;
    let count = 0;
    this.skipWS();
    if (this.peek() === 0x5d) {
      this.pos++;
    } else {
      for (;;) {
        const cv = this.value(depth, items, false);
        count++;
        if (v !== null) v.push(cv);
        this.skipWS();
        const c = this.peek();
        if (c === 0x2c) {
          this.pos++;
          continue;
        }
        if (c === 0x5d) {
          this.pos++;
          break;
        }
        throw this.err("json-syntax", "a comma or ] was expected");
      }
    }
    if (sch !== undefined && !schemaLengthWithin(sch, count)) {
      this.failSchema("STATE-10", `an array of ${count} items is outside the length bounds`);
    }
    return v;
  }

  /** Reads a number token as RFC 8259 §6 writes it, applies the number rule of 05 [STATE-7], and checks the text is the canonical text of the value it denotes. */
  private number(sch: Schema | undefined): JsonValue {
    const b = this.b;
    const start = this.pos;
    let i = start;
    if (i < b.length && b[i] === 0x2d) i++;
    if (i >= b.length) throw this.err("json-syntax", "a value was expected");
    const first = b[i] as number;
    if (first === 0x30) {
      i++;
    } else if (first >= 0x31 && first <= 0x39) {
      while (i < b.length && isDigit(b[i] as number)) i++;
    } else {
      throw this.err("json-syntax", "a value was expected");
    }
    if (i + 1 < b.length && b[i] === 0x2e && isDigit(b[i + 1] as number)) {
      i += 2;
      while (i < b.length && isDigit(b[i] as number)) i++;
    }
    if (i < b.length && (b[i] === 0x65 || b[i] === 0x45)) {
      let j = i + 1;
      if (j < b.length && (b[j] === 0x2b || b[j] === 0x2d)) j++;
      if (j < b.length && isDigit(b[j] as number)) {
        while (j < b.length && isDigit(b[j] as number)) j++;
        i = j;
      }
    }
    let tok = "";
    for (let k = start; k < i; k++) tok += String.fromCharCode(b[k] as number);
    this.pos = i;
    const d = denote(tok);
    if (d.status === "infinite") throw this.err("json-number", `${echo(tok)} is not a finite binary64 value`); // 05 [STATE-7]
    if (d.status === "underflow") throw this.err("json-number", `${echo(tok)} rounds to zero but its digits are not zero`); // 05 [STATE-7]
    if (d.plain) {
      // up to 15 digits: its canonical text is itself, except -0
      if (d.value === 0 && tok.charCodeAt(0) === 0x2d) this.notCanonical = true;
    } else if (numberText(d.value) !== tok) {
      this.notCanonical = true; // RFC 8785 §3.2.2.3
    }
    if (sch !== undefined) {
      const why = fitNumber(sch, d.value);
      if (why !== "") this.failSchema("STATE-10", why);
    }
    return d.value;
  }

  /**
   * Reads the string whose opening quote is at the current position and returns its characters, decoded. It applies the
   * I-JSON rules (no unpaired surrogate, no noncharacter), notes U+0000, and notes any escape that RFC 8785 would not write.
   */
  private str(): string {
    const b = this.b;
    let i = this.pos + 1;
    let start = i;
    const parts: string[] = [];
    for (;;) {
      if (i >= b.length) {
        this.pos = b.length;
        throw this.err("json-syntax", "unterminated string");
      }
      const c = b[i] as number;
      if (c === 0x22) {
        this.pos = i + 1;
        if (parts.length === 0) return utf8DecodeValid(b.subarray(start, i));
        parts.push(utf8DecodeValid(b.subarray(start, i)));
        return parts.join("");
      }
      if (c < 0x20) {
        this.pos = i;
        throw this.err("json-syntax", "a control character in a string must be escaped");
      }
      if (c === 0x5c) {
        parts.push(utf8DecodeValid(b.subarray(start, i)));
        if (i + 1 >= b.length) {
          this.pos = b.length;
          throw this.err("json-syntax", "unterminated string");
        }
        const { text, width } = this.escape(i);
        parts.push(text);
        i += width;
        start = i;
        continue;
      }
      if (c >= 0x80) {
        // valid UTF-8: run() checked
        let r: number;
        let w: number;
        if (c < 0xe0) {
          r = ((c & 0x1f) << 6) | ((b[i + 1] as number) & 0x3f);
          w = 2;
        } else if (c < 0xf0) {
          r = ((c & 0x0f) << 12) | (((b[i + 1] as number) & 0x3f) << 6) | ((b[i + 2] as number) & 0x3f);
          w = 3;
        } else {
          r =
            ((c & 0x07) << 18) |
            (((b[i + 1] as number) & 0x3f) << 12) |
            (((b[i + 2] as number) & 0x3f) << 6) |
            ((b[i + 3] as number) & 0x3f);
          w = 4;
        }
        if (isNonchar(r)) {
          this.pos = i;
          throw this.err("json-nonchar", `the noncharacter U+${r.toString(16).toUpperCase().padStart(4, "0")}`);
        }
        i += w;
        continue;
      }
      i++;
    }
  }

  /** Decodes the escape sequence that starts at b[i] == '\\' and returns its text and its length in octets. */
  private escape(i: number): { text: string; width: number } {
    const b = this.b;
    const e = b[i + 1] as number;
    switch (e) {
      case 0x22: // "
      case 0x5c: // \
        return { text: String.fromCharCode(e), width: 2 }; // the escapes of RFC 8785 §3.2.2.2
      case 0x62:
        return { text: "\b", width: 2 };
      case 0x66:
        return { text: "\f", width: 2 };
      case 0x6e:
        return { text: "\n", width: 2 };
      case 0x72:
        return { text: "\r", width: 2 };
      case 0x74:
        return { text: "\t", width: 2 };
      case 0x2f: // valid JSON, but RFC 8785 writes the solidus as itself
        this.notCanonical = true;
        return { text: "/", width: 2 };
      case 0x75: {
        // \u
        let cp = hex4(b, i + 2);
        if (cp < 0) {
          this.pos = i;
          throw this.err("json-syntax", "a bad \\u escape");
        }
        let width = 6;
        let canonical = false;
        if (cp >= 0xd800 && cp <= 0xdbff) {
          const lo = hex4(b, i + 8);
          if (lo < 0 || b[i + 6] !== 0x5c || b[i + 7] !== 0x75 || lo < 0xdc00 || lo > 0xdfff) {
            this.pos = i;
            throw this.err("json-surrogate", "an unpaired high surrogate escape"); // I-JSON
          }
          cp = 0x10000 + ((cp - 0xd800) << 10) + (lo - 0xdc00);
          width = 12; // a pair is never canonical: RFC 8785 writes the character itself
        } else if (cp >= 0xdc00 && cp <= 0xdfff) {
          this.pos = i;
          throw this.err("json-surrogate", "an unpaired low surrogate escape"); // I-JSON
        } else {
          // Only a control character other than \b \f \n \r \t is written as an escape, \u00xx in lowercase.
          canonical =
            cp < 0x20 && cp !== 0x08 && cp !== 0x09 && cp !== 0x0a && cp !== 0x0c && cp !== 0x0d && lowerHex(b, i + 2);
        }
        if (isNonchar(cp)) {
          this.pos = i;
          throw this.err("json-nonchar", `the noncharacter U+${cp.toString(16).toUpperCase().padStart(4, "0")}`);
        }
        if (cp === 0) this.hasNUL = true; // 05 [STATE-7]: no string and no member name holds U+0000
        if (!canonical) this.notCanonical = true;
        return { text: String.fromCodePoint(cp), width };
      }
      default:
        this.pos = i;
        throw this.err("json-syntax", "a bad escape");
    }
  }
}

/** Reads four hex digits at b[j..], in either case, or returns -1. */
function hex4(b: Uint8Array, j: number): number {
  if (j < 0 || j + 4 > b.length) return -1;
  let v = 0;
  for (let k = 0; k < 4; k++) {
    const c = b[j + k] as number;
    let d: number;
    if (c >= 0x30 && c <= 0x39) d = c - 0x30;
    else if (c >= 0x61 && c <= 0x66) d = c - 0x61 + 10;
    else if (c >= 0x41 && c <= 0x46) d = c - 0x41 + 10;
    else return -1;
    v = (v << 4) | d;
  }
  return v;
}

function lowerHex(b: Uint8Array, j: number): boolean {
  for (let k = 0; k < 4; k++) {
    const c = b[j + k] as number;
    if (c >= 0x41 && c <= 0x46) return false;
  }
  return true;
}
