// The checks that 07 [MUT-45] and 02 [ARC-61] make on received bytes, before a runtime decodes (and repairs) them.
//
// protobuf-es and protobuf.js replace invalid octets with U+FFFD as they decode, and read a tag, a length or a varint
// that is not in its shortest form without comment, so a party built on them checks the octets itself (02 [ARC-61], note).
// The checker walks the bytes by the message descriptors, without decoding them.

import type { DescField, DescMessage, Message } from "@bufbuild/protobuf";
import { reflect, type ReflectList, type ReflectMap, type ReflectMessage } from "@bufbuild/protobuf/reflect";
import { isValidUtf8 } from "../bytes.js";
import { CanonError, MAX_DEPTH, malformed } from "./errors.js";
import {
  cachedFieldInfo,
  fieldsByNumber,
  type FieldInfo,
  MAX_FIELD_NUMBER,
  WT_FIXED32,
  WT_FIXED64,
  WT_LEN,
  WT_VARINT,
} from "./wire.js";

/**
 * Checks received bytes of a message of type `schema` before they are decoded or hashed, and throws the first refusal as a
 * CanonError. It looks for, in this order of precedence:
 *  - bytes that are not a protobuf message of the type: kind `malformed`;
 *  - a string field, at any depth, that is not valid UTF-8 (02 [ARC-61]): kind `invalid-utf8`;
 *  - a tag, a length or a varint value, of a known field or of an unknown one, that is not in its shortest form
 *    (07 [MUT-45]): kind `not-shortest`. The inside of an unknown length-delimited field is not looked at: its bytes remain
 *    the sender's duty (07 [MUT-45]).
 */
export function checkEncoding(schema: DescMessage, bytes: Uint8Array): void {
  new Checker(true, true).run(schema, bytes);
}

/**
 * The UTF-8 check of `checkEncoding` alone. It serves a request that holds many messages: a string that is not valid UTF-8
 * fails the whole request (02 [ARC-61]), whereas an encoding not in its shortest form is refused per mutation (07 [MUT-45]).
 */
export function checkUtf8(schema: DescMessage, bytes: Uint8Array): void {
  new Checker(true, false).run(schema, bytes);
}

/** The shortest-form check of `checkEncoding` alone (07 [MUT-45]). */
export function checkShortest(schema: DescMessage, bytes: Uint8Array): void {
  new Checker(false, true).run(schema, bytes);
}

/** Reads the varint at b[p..end) and returns its value (approximate above 2^53) and the position after it, or null. */
function readVarint(b: Uint8Array, p: number, end: number): { v: number; next: number } | null {
  let v = 0;
  let mul = 1;
  for (let i = 0; i < 10; i++) {
    if (p + i >= end) return null;
    const x = b[p + i] as number;
    if (i === 9 && x > 1) return null; // more than 64 bits
    v += (x & 0x7f) * mul;
    mul *= 128;
    if (x < 0x80) return { v, next: p + i + 1 };
  }
  return null;
}

class Checker {
  private path: string[] = [];
  private badUTF8: CanonError | null = null;
  private notShortest: CanonError | null = null;

  constructor(
    private readonly doUtf8: boolean,
    private readonly doShortest: boolean,
  ) {}

  run(schema: DescMessage, b: Uint8Array): void {
    this.message(lookupOf(schema), b, 0, b.length, 0);
    if (this.badUTF8 !== null) throw this.badUTF8;
    if (this.notShortest !== null) throw this.notShortest;
  }

  /** Reads a varint and notes, for 07 [MUT-45], one that is not in its shortest form: more than one octet, the last of them zero. */
  private varint(b: Uint8Array, p: number, end: number, what: string): { v: number; next: number } {
    const r = readVarint(b, p, end);
    if (r === null) throw malformed(`bad ${what} at byte ${p}`);
    if (this.doShortest && this.notShortest === null && r.next - p > 1 && b[r.next - 1] === 0) {
      this.notShortest = new CanonError("not-shortest", this.path.join("."), `${what} at byte ${p}`);
    }
    return r;
  }

  private message(lookup: (num: number) => FieldInfo | undefined, b: Uint8Array, start: number, end: number, depth: number): void {
    if (depth > MAX_DEPTH) throw malformed(`nested deeper than ${MAX_DEPTH}`);
    let p = start;
    while (p < end) {
      const t = this.varint(b, p, end, "tag");
      p = t.next;
      const num = Math.floor(t.v / 8);
      const wt = t.v % 8;
      if (num === 0 || num > MAX_FIELD_NUMBER || t.v >= 2 ** 32) throw malformed(`bad field number in the tag at byte ${p}`);
      const fi = lookup(num);
      if (fi !== undefined) this.path.push(fi.name);
      switch (wt) {
        case WT_VARINT:
          p = this.varint(b, p, end, "varint").next;
          break;
        case WT_FIXED64:
          if (end - p < 8) throw malformed(`truncated fixed64 at byte ${p}`);
          p += 8;
          break;
        case WT_FIXED32:
          if (end - p < 4) throw malformed(`truncated fixed32 at byte ${p}`);
          p += 4;
          break;
        case WT_LEN: {
          const l = this.varint(b, p, end, "length");
          p = l.next;
          if (l.v > end - p) throw malformed(`truncated field ${num} at byte ${p}`);
          if (fi !== undefined) this.bytesField(fi, b, p, p + l.v, depth);
          p += l.v;
          break;
        }
        default:
          // SPEC-QUESTION: 07 [MUT-45] (SPEC-QUESTIONS Q2) says nothing of an unknown field with the wire type of a group (3 or
          // 4), whose inside is a sequence of fields. proto3 has no groups, and both vector models fail on one, so it is
          // refused here as bytes that do not parse, as the Go server does, and no hash is taken of it.
          throw malformed(`wire type ${wt} at byte ${p}`);
      }
      if (fi !== undefined) this.path.pop();
    }
  }

  /**
   * Looks inside the payload of a length-delimited occurrence of a known field. A field whose kind has another wire type is
   * unknown to the decoder, and its payload is left alone.
   */
  private bytesField(fi: FieldInfo, b: Uint8Array, start: number, end: number, depth: number): void {
    switch (fi.kind) {
      case "string":
        if (this.doUtf8 && this.badUTF8 === null && !isValidUtf8(b, start, end)) {
          this.badUTF8 = new CanonError("invalid-utf8", this.path.join("."), "");
        }
        return;
      case "message":
        this.message(lookupOf(fi.message as DescMessage), b, start, end, depth + 1);
        return;
      case "map": {
        const entry = fi.entry as { key: FieldInfo; value: FieldInfo };
        // a map entry is a message too: key 1 and value 2
        this.message((num) => (num === 1 ? entry.key : num === 2 ? entry.value : undefined), b, start, end, depth + 1);
        return;
      }
      case "varint": {
        if (!fi.list) return; // a singular field sent as bytes is unknown to the decoder
        // a packed repeated field
        let p = start;
        while (p < end) p = this.varint(b, p, end, "packed varint").next;
        return;
      }
      default:
        return;
    }
  }
}

function lookupOf(desc: DescMessage): (num: number) => FieldInfo | undefined {
  const fields = fieldsByNumber(desc);
  return (num) => {
    const f = fields.get(num);
    return f === undefined ? undefined : cachedFieldInfo(f);
  };
}

// ---- 02 [ARC-61]: Timestamps ----

const MIN_TIMESTAMP_SECONDS = -62135596800n;
const MAX_TIMESTAMP_SECONDS = 253402300799n;

/** True when seconds and nanos form a Timestamp that 02 [ARC-61] allows: 0001-01-01T00:00:00Z to 9999-12-31T23:59:59.999999999Z. */
export function timestampInRange(seconds: bigint, nanos: number): boolean {
  return nanos >= 0 && nanos <= 999999999 && seconds >= MIN_TIMESTAMP_SECONDS && seconds <= MAX_TIMESTAMP_SECONDS;
}

const TIMESTAMP = "google.protobuf.Timestamp";

/**
 * Throws a CanonError of kind `timestamp-range` for the first google.protobuf.Timestamp of the message, at any depth, that
 * breaks 02 [ARC-61]; `path` names its field. The caller picks the answer: an `authored_at` of a Mutation is
 * INVALID_ENVELOPE for that mutation alone (`CanonError.invalidEnvelope`), every other Timestamp fails the request as a whole.
 */
export function checkTimestamps(schema: DescMessage, message: Message): void {
  walkTimestamps(reflect(schema, message, false), [], 0);
}

function walkTimestamps(m: ReflectMessage, path: string[], depth: number): void {
  if (depth > MAX_DEPTH) throw malformed(`nested deeper than ${MAX_DEPTH}`);
  const md = m.desc;
  if (md.typeName === TIMESTAMP) {
    const seconds = m.get(md.fields.find((f) => f.name === "seconds") as DescField) as bigint;
    const nanos = m.get(md.fields.find((f) => f.name === "nanos") as DescField) as number;
    if (!timestampInRange(seconds, nanos)) throw new CanonError("timestamp-range", path.join("."), "seconds or nanos out of range");
    return;
  }
  for (const f of m.fields) {
    if (!m.isSet(f)) continue;
    path.push(f.name);
    if (f.fieldKind === "message") {
      walkTimestamps(m.get(f) as ReflectMessage, path, depth + 1);
    } else if (f.fieldKind === "list" && f.listKind === "message") {
      for (const item of m.get(f) as ReflectList) walkTimestamps(item as ReflectMessage, path, depth + 1);
    } else if (f.fieldKind === "map" && f.mapKind === "message") {
      for (const [, v] of m.get(f) as ReflectMap) walkTimestamps(v as ReflectMessage, path, depth + 1);
    }
    path.pop();
  }
}
