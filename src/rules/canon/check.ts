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
  WT_EGROUP,
  WT_FIXED32,
  WT_FIXED64,
  WT_LEN,
  WT_SGROUP,
  WT_VARINT,
} from "./wire.js";

/**
 * Checks received bytes of a message of type `schema` before they are decoded or hashed, and throws the first refusal as a
 * CanonError. It looks for, in this order of precedence:
 *  - bytes that are not a protobuf message of the type: kind `malformed`;
 *  - a string field, at any depth, that is not valid UTF-8 (02 [ARC-61]): kind `invalid-utf8`;
 *  - a tag, a length or a varint value, of a known field or of an unknown one, that is not in its shortest form
 *    (07 [MUT-45]): kind `not-shortest`. The inside of an unknown length-delimited field is not looked at: its bytes remain
 *    the sender's duty (07 [MUT-45]);
 *  - a known field that arrives with a wire type that its kind does not have, and any field, known or unknown, with wire type
 *    3 or 4 (the group tags) (07 [MUT-45], round 16): kind `wire-type`, refused in the same way and at the same place as an
 *    encoding that is not in its shortest form. Of the two, the one that comes first in the bytes is reported.
 */
export function checkEncoding(schema: DescMessage, bytes: Uint8Array): void {
  new Checker({ utf8: true, shortest: true, wire: true, skipGroups: false }).run(schema, bytes);
}

/**
 * The UTF-8 check of `checkEncoding` alone. It serves a request that holds many messages: a string that is not valid UTF-8
 * fails the whole request (02 [ARC-61]), whereas an encoding not in its shortest form is refused per mutation (07 [MUT-45]).
 * A group (wire type 3 or 4) is refused here too, as `wire-type`, since the walk cannot go on past one; see `checkUtf8Request`.
 */
export function checkUtf8(schema: DescMessage, bytes: Uint8Array): void {
  new Checker({ utf8: true, shortest: false, wire: false, skipGroups: false }).run(schema, bytes);
}

/**
 * `checkUtf8` for the bytes of a MutateRequest, whose mutations are each checked on their own bytes by `checkShortest`: a
 * well-formed group is passed over and left to that per-mutation check, which refuses the mutation with INVALID_ENVELOPE
 * (07 [MUT-45], Table 7-1 step 4). Bytes that do not parse, a group that is not closed included, are still refused as a whole.
 */
export function checkUtf8Request(schema: DescMessage, bytes: Uint8Array): void {
  new Checker({ utf8: true, shortest: false, wire: false, skipGroups: true }).run(schema, bytes);
}

/**
 * The shortest-form check of `checkEncoding` alone, with the wire-type check that is refused in the same way (07 [MUT-45]):
 * it serves a Mutation's bytes and an AdminService request's.
 */
export function checkShortest(schema: DescMessage, bytes: Uint8Array): void {
  new Checker({ utf8: false, shortest: true, wire: true, skipGroups: false }).run(schema, bytes);
}

/**
 * The wire-type check of `checkShortest` alone (07 [MUT-45], round 16): a known field with a wire type that its kind does not
 * have, and a group tag on any field. It serves a request that 07 [MUT-45] does not hold to the shortest form but refuses so (a
 * SyncService request that is no MutateRequest).
 */
export function checkWireTypes(schema: DescMessage, bytes: Uint8Array): void {
  new Checker({ utf8: false, shortest: false, wire: true, skipGroups: false }).run(schema, bytes);
}

interface CheckerOptions {
  utf8: boolean;
  shortest: boolean;
  wire: boolean;
  /** Pass over a well-formed group instead of refusing it. */
  skipGroups: boolean;
}

/** Ends a walk that cannot go on past the field it has just refused (an end-group tag with no start): the refusals found so far stand. */
const STOP = Symbol("stop");

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
  /** The first encoding not in its shortest form or wire type that does not fit, whichever comes first. */
  private refused: CanonError | null = null;
  private readonly doUtf8: boolean;
  private readonly doShortest: boolean;
  private readonly doWire: boolean;
  private readonly skipGroups: boolean;

  constructor(o: CheckerOptions) {
    this.doUtf8 = o.utf8;
    this.doShortest = o.shortest;
    this.doWire = o.wire;
    this.skipGroups = o.skipGroups;
  }

  run(schema: DescMessage, b: Uint8Array): void {
    if (this.message(lookupOf(schema), b, 0, b.length, 0) === STOP) {
      // the walk ended at a refusal; what was found before it stands
    }
    if (this.badUTF8 !== null) throw this.badUTF8;
    if (this.refused !== null) throw this.refused;
  }

  /** Reads a varint and notes, for 07 [MUT-45], one that is not in its shortest form: more than one octet, the last of them zero. */
  private varint(b: Uint8Array, p: number, end: number, what: string): { v: number; next: number } {
    const r = readVarint(b, p, end);
    if (r === null) throw malformed(`bad ${what} at byte ${p}`);
    if (this.doShortest && this.refused === null && r.next - p > 1 && b[r.next - 1] === 0) {
      this.refused = new CanonError("not-shortest", this.path.join("."), `${what} at byte ${p}`);
    }
    return r;
  }

  private message(lookup: (num: number) => FieldInfo | undefined, b: Uint8Array, start: number, end: number, depth: number): typeof STOP | undefined {
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
      // 07 [MUT-45] (round 16): a known field with a wire type that its kind does not have is malformed in the same way as an
      // encoding that is not in its shortest form. The value is passed over as the wire type has it.
      if (this.doWire && this.refused === null && fi !== undefined && !wireFits(fi, wt)) {
        this.refused = new CanonError("wire-type", this.path.join("."), `field ${fi.name} arrives with wire type ${wt}, which its kind does not have`);
      }
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
          if (fi !== undefined && this.bytesField(fi, b, p, p + l.v, depth) === STOP) return STOP;
          p += l.v;
          break;
        }
        case WT_SGROUP:
        case WT_EGROUP: {
          // 07 [MUT-45] (round 16): a field of wire type 3 or 4, known or unknown, is malformed in the same way as an
          // encoding that is not in its shortest form: a Mutation gets INVALID_ENVELOPE at the shortest-form check of step 4,
          // any other request InvalidArgument with Reject{INVALID_ENVELOPE}. proto3 writes no group.
          const refusal = new CanonError("wire-type", this.path.join("."), `a group tag (wire type ${wt}) at byte ${p}`);
          if (wt === WT_EGROUP) {
            // an end tag with no start: nothing to pass over, and no group that parses
            if (!this.doWire) {
              if (this.skipGroups) throw malformed(`end group tag with no start at byte ${p}`);
              throw refusal;
            }
            if (this.refused === null) this.refused = refusal;
            return STOP;
          }
          // pass over the group with its inside; one that is not closed is bytes that do not parse
          p = skipGroup(b, p, end, num);
          if (this.doWire) {
            if (this.refused === null) this.refused = refusal;
          } else if (!this.skipGroups) {
            throw refusal;
          }
          break;
        }
        default:
          // a wire type that no protobuf message has (6, 7): the bytes do not parse
          throw malformed(`wire type ${wt} at byte ${p}`);
      }
      if (fi !== undefined) this.path.pop();
    }
    return undefined;
  }

  /**
   * Looks inside the payload of a length-delimited occurrence of a known field. A field whose kind has another wire type is
   * unknown to the decoder, and its payload is left alone.
   */
  private bytesField(fi: FieldInfo, b: Uint8Array, start: number, end: number, depth: number): typeof STOP | undefined {
    switch (fi.kind) {
      case "string":
        if (this.doUtf8 && this.badUTF8 === null && !isValidUtf8(b, start, end)) {
          this.badUTF8 = new CanonError("invalid-utf8", this.path.join("."), "");
        }
        return undefined;
      case "message":
        return this.message(lookupOf(fi.message as DescMessage), b, start, end, depth + 1);
      case "map": {
        const entry = fi.entry as { key: FieldInfo; value: FieldInfo };
        // a map entry is a message too: key 1 and value 2
        return this.message((num) => (num === 1 ? entry.key : num === 2 ? entry.value : undefined), b, start, end, depth + 1);
      }
      case "varint": {
        if (!fi.list) return undefined; // a singular field sent as bytes is unknown to the decoder
        // a packed repeated field
        let p = start;
        while (p < end) p = this.varint(b, p, end, "packed varint").next;
        return undefined;
      }
      default:
        return undefined;
    }
  }
}

/**
 * Whether a field of this kind can arrive with wire type wt (07 [MUT-45]): a varint kind as a varint, a fixed-width kind as a
 * fixed32 or a fixed64, and a string, bytes, message or map as length-delimited. A repeated numeric, bool or enum field is
 * read packed or not, so it may also arrive length-delimited.
 */
function wireFits(fi: FieldInfo, wt: number): boolean {
  return wt === fi.wire || (fi.packable && wt === WT_LEN);
}

/**
 * Passes over a group whose start tag (field number `num`) has just been read, at p: its inside is a sequence of fields that
 * ends at the end tag of the same number. Returns the position after the end tag. Bounded by MAX_DEPTH; throws `malformed` for
 * a group that is not closed or whose inside does not parse.
 */
function skipGroup(b: Uint8Array, p: number, end: number, num: number): number {
  const open: number[] = [num];
  while (open.length > 0) {
    if (p >= end) throw malformed(`group not closed at byte ${p}`);
    const t = readVarint(b, p, end);
    if (t === null) throw malformed(`bad tag in a group at byte ${p}`);
    p = t.next;
    const n = Math.floor(t.v / 8);
    const wt = t.v % 8;
    if (n === 0 || n > MAX_FIELD_NUMBER || t.v >= 2 ** 32) throw malformed(`bad field number in the tag at byte ${p}`);
    switch (wt) {
      case WT_VARINT: {
        const r = readVarint(b, p, end);
        if (r === null) throw malformed(`bad varint in a group at byte ${p}`);
        p = r.next;
        break;
      }
      case WT_FIXED64:
        if (end - p < 8) throw malformed(`truncated fixed64 at byte ${p}`);
        p += 8;
        break;
      case WT_FIXED32:
        if (end - p < 4) throw malformed(`truncated fixed32 at byte ${p}`);
        p += 4;
        break;
      case WT_LEN: {
        const l = readVarint(b, p, end);
        if (l === null) throw malformed(`bad length in a group at byte ${p}`);
        p = l.next;
        if (l.v > end - p) throw malformed(`truncated field ${n} at byte ${p}`);
        p += l.v;
        break;
      }
      case WT_SGROUP:
        if (open.length >= MAX_DEPTH) throw malformed(`groups nested deeper than ${MAX_DEPTH}`);
        open.push(n);
        break;
      case WT_EGROUP:
        if (open[open.length - 1] !== n) throw malformed(`end group tag of field ${n} does not match its start at byte ${p}`);
        open.pop();
        break;
      default:
        throw malformed(`wire type ${wt} at byte ${p}`);
    }
  }
  return p;
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
