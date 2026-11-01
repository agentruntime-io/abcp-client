// A strict decoder for received messages, in both codecs (02 [ARC-61], 07 [MUT-45], 15 [COMPAT-38]).
//
// `unmarshal` checks the octets first (`checkEncoding`), then decodes them with a reader of this package. It does what
// protobuf-es's `fromBinary` does, with the differences that matter to a party that hashes and stores what it reads:
//  - a string is never repaired: invalid UTF-8 is a CanonError, and a leading U+FEFF is an ordinary character (protobuf-es
//    replaces invalid octets with U+FFFD and drops a leading byte-order mark);
//  - a known field that arrives with a wire type its kind does not have is kept as an unknown field, as Go's decoder keeps
//    it, where protobuf-es reads it as if it had the right one. `unmarshal` refuses such a message first (07 [MUT-45], round
//    16: `checkEncoding`, kind `wire-type`); `decode`, which runs no check, keeps the field as received;
//  - nothing but a CanonError is thrown for bad bytes (02 [ARC-46]).
// Unknown fields are kept as received, in `$unknown`, so that the canonical encoding can write them at their numbers.

import { fromJson, ScalarType, type DescField, type DescMessage, type JsonValue, type Message } from "@bufbuild/protobuf";
import { reflect, type ReflectList, type ReflectMap, type ReflectMessage } from "@bufbuild/protobuf/reflect";
import { isValidUtf8, utf8DecodeValid, hasLoneSurrogate } from "../bytes.js";
import { checkEncoding, checkTimestamps } from "./check.js";
import { CanonError, MAX_DEPTH, malformed } from "./errors.js";
import { MAX_FIELD_NUMBER, scalarPackable, scalarWire, WT_FIXED32, WT_FIXED64, WT_LEN, WT_VARINT } from "./wire.js";

type Unknown = { no: number; wireType: number; data: Uint8Array };

/**
 * Checks b with `checkEncoding` and decodes it as a message of type `schema` (binary codec). Unknown fields stay as
 * received. It does not check Timestamps: see `checkTimestamps`, since an authored_at out of range refuses one mutation,
 * not the request that carries it (02 [ARC-61]). Throws a CanonError.
 */
export function unmarshal(schema: DescMessage, b: Uint8Array): Message {
  checkEncoding(schema, b);
  return decode(schema, b);
}

/** Decodes b as a message of type `schema` without the encoding checks (the caller has run them). Throws a CanonError. */
export function decode(schema: DescMessage, b: Uint8Array): Message {
  const m = reflect(schema, undefined, false);
  new Decoder(b).readMessage(m, 0, b.length, 0);
  return m.message;
}

/**
 * Decodes a message received in the JSON codec, the proto3 JSON mapping (02 [ARC-53], [ARC-61]). Text that is not valid
 * UTF-8, a string escape of an unpaired surrogate (RFC 8259 §8.2), text that the mapping refuses and a Timestamp out of the
 * range of 02 [ARC-61] fail the whole request: every refusal here is a CanonError whose `invalidEnvelope` is false. Unknown
 * members are accepted and dropped, as 15 [COMPAT-38] has the JSON codec do.
 *
 * The message then hashes as its binary form does, since every hash of this package is taken on the canonical binary
 * encoding (07 [MUT-45]).
 */
export function unmarshalJson(schema: DescMessage, text: Uint8Array): Message {
  if (!isValidUtf8(text)) throw new CanonError("invalid-utf8", "", "JSON text");
  let parsed: unknown;
  try {
    parsed = JSON.parse(utf8DecodeValid(text));
  } catch (e) {
    throw malformed(`not JSON: ${e instanceof Error ? e.message : String(e)}`);
  }
  if (jsonHasLoneSurrogate(parsed, 0)) throw new CanonError("invalid-utf8", "", "a string escape of an unpaired surrogate");
  let msg: Message;
  try {
    msg = fromJson(schema, parsed as JsonValue, { ignoreUnknownFields: true });
  } catch (e) {
    throw malformed(`the proto3 JSON mapping refuses it: ${e instanceof Error ? e.message : String(e)}`);
  }
  try {
    checkTimestamps(schema, msg);
  } catch (e) {
    // the JSON codec cannot carry it: the whole request fails, authored_at included
    if (e instanceof CanonError) throw new CanonError(e.kind, "", e.detail);
    throw e;
  }
  return msg;
}

/** True when a string of the parsed JSON, a member name included, holds an unpaired surrogate. The depth is bounded by JSON.parse's own. */
function jsonHasLoneSurrogate(v: unknown, depth: number): boolean {
  if (depth > 10000) throw malformed("JSON nested too deeply");
  if (typeof v === "string") return hasLoneSurrogate(v);
  if (Array.isArray(v)) return v.some((x) => jsonHasLoneSurrogate(x, depth + 1));
  if (typeof v === "object" && v !== null) {
    for (const [k, x] of Object.entries(v)) {
      if (hasLoneSurrogate(k) || jsonHasLoneSurrogate(x, depth + 1)) return true;
    }
  }
  return false;
}

class Decoder {
  constructor(private readonly b: Uint8Array) {}

  private varint(p: number, end: number): { v: number; next: number } {
    let v = 0;
    let mul = 1;
    for (let i = 0; i < 10; i++) {
      if (p + i >= end) throw malformed(`truncated varint at byte ${p}`);
      const x = this.b[p + i] as number;
      if (i === 9 && x > 1) throw malformed(`varint over 64 bits at byte ${p}`);
      v += (x & 0x7f) * mul;
      mul *= 128;
      if (x < 0x80) return { v, next: p + i + 1 };
    }
    throw malformed(`varint over 64 bits at byte ${p}`);
  }

  private varintBig(p: number, end: number): { v: bigint; next: number } {
    let v = 0n;
    for (let i = 0; i < 10; i++) {
      if (p + i >= end) throw malformed(`truncated varint at byte ${p}`);
      const x = this.b[p + i] as number;
      if (i === 9 && x > 1) throw malformed(`varint over 64 bits at byte ${p}`);
      v |= BigInt(x & 0x7f) << BigInt(7 * i);
      if (x < 0x80) return { v, next: p + i + 1 };
    }
    throw malformed(`varint over 64 bits at byte ${p}`);
  }

  /** Skips one value of the given wire type at p and returns its raw octets (its length prefix included for a length-delimited one). */
  private skip(wt: number, p: number, end: number): { next: number; data: Uint8Array } {
    let next: number;
    switch (wt) {
      case WT_VARINT:
        next = this.varint(p, end).next;
        break;
      case WT_FIXED64:
        if (end - p < 8) throw malformed(`truncated fixed64 at byte ${p}`);
        next = p + 8;
        break;
      case WT_FIXED32:
        if (end - p < 4) throw malformed(`truncated fixed32 at byte ${p}`);
        next = p + 4;
        break;
      case WT_LEN: {
        const l = this.varint(p, end);
        if (l.v > end - l.next) throw malformed(`truncated field at byte ${l.next}`);
        next = l.next + l.v;
        break;
      }
      default:
        // a group (3, 4) or a wire type that does not exist: refused as bytes that do not parse; `checkEncoding` refuses a
        // group first, as a wire type that does not fit (07 [MUT-45], round 16)
        throw malformed(`wire type ${wt} at byte ${p}`);
    }
    return { next, data: this.b.slice(p, next) };
  }

  readMessage(m: ReflectMessage, start: number, end: number, depth: number): void {
    if (depth > MAX_DEPTH) throw malformed(`nested deeper than ${MAX_DEPTH}`);
    const unknown: Unknown[] = [...(m.getUnknown() ?? [])];
    let p = start;
    while (p < end) {
      const t = this.varint(p, end);
      p = t.next;
      const no = Math.floor(t.v / 8);
      const wt = t.v % 8;
      if (no === 0 || no > MAX_FIELD_NUMBER || t.v >= 2 ** 32) throw malformed(`bad field number in the tag at byte ${p}`);
      const f = m.findNumber(no);
      const next = f === undefined ? -1 : this.readField(m, f, wt, p, end, depth);
      if (next >= 0) {
        p = next;
        continue;
      }
      // unknown to the type, or known with a wire type that its kind does not have
      const s = this.skip(wt, p, end);
      unknown.push({ no, wireType: wt, data: s.data });
      p = s.next;
    }
    if (unknown.length > 0) m.setUnknown(unknown);
  }

  /** Reads a known field's value at p. Returns the position after it, or -1 when the wire type is not one that the field's kind has. */
  private readField(m: ReflectMessage, f: DescField, wt: number, p: number, end: number, depth: number): number {
    switch (f.fieldKind) {
      case "scalar": {
        if (wt !== scalarWire(f.scalar)) return -1;
        const r = this.readScalar(f.scalar, p, end, f.name);
        m.set(f, r.value);
        return r.next;
      }
      case "enum": {
        if (wt !== WT_VARINT) return -1;
        const r = this.varintBig(p, end);
        m.set(f, Number(BigInt.asIntN(32, r.v)));
        return r.next;
      }
      case "message": {
        if (f.delimitedEncoding || wt !== WT_LEN) return -1;
        const l = this.varint(p, end);
        if (l.v > end - l.next) throw malformed(`truncated field ${f.number} at byte ${l.next}`);
        const sub = m.isSet(f) ? (m.get(f) as ReflectMessage) : reflect((f as DescField & { fieldKind: "message" }).message, undefined, false);
        this.readMessage(sub, l.next, l.next + l.v, depth + 1);
        m.set(f, sub);
        return l.next + l.v;
      }
      case "list": {
        const list = m.get(f) as ReflectList;
        if (f.listKind === "message") {
          if (f.delimitedEncoding || wt !== WT_LEN) return -1;
          const l = this.varint(p, end);
          if (l.v > end - l.next) throw malformed(`truncated field ${f.number} at byte ${l.next}`);
          const sub = reflect(f.message, undefined, false);
          this.readMessage(sub, l.next, l.next + l.v, depth + 1);
          list.add(sub);
          return l.next + l.v;
        }
        const t = f.listKind === "enum" ? ScalarType.INT32 : f.scalar;
        if (f.listKind === "scalar" && !scalarPackable(t)) {
          if (wt !== WT_LEN) return -1; // a repeated string or bytes: one length-delimited element per field
          const r = this.readScalar(t, p, end, f.name);
          list.add(r.value);
          return r.next;
        }
        if (wt === WT_LEN) {
          // packed
          const l = this.varint(p, end);
          if (l.v > end - l.next) throw malformed(`truncated field ${f.number} at byte ${l.next}`);
          const stop = l.next + l.v;
          let q = l.next;
          while (q < stop) {
            const r = f.listKind === "enum" ? this.readEnumValue(q, stop) : this.readScalar(t, q, stop, f.name);
            list.add(r.value);
            q = r.next;
          }
          return stop;
        }
        if (wt !== scalarWire(t)) return -1;
        const r = f.listKind === "enum" ? this.readEnumValue(p, end) : this.readScalar(t, p, end, f.name);
        list.add(r.value);
        return r.next;
      }
      case "map": {
        if (wt !== WT_LEN) return -1;
        const l = this.varint(p, end);
        if (l.v > end - l.next) throw malformed(`truncated field ${f.number} at byte ${l.next}`);
        this.readMapEntry(m.get(f) as ReflectMap, f, l.next, l.next + l.v, depth);
        return l.next + l.v;
      }
    }
  }

  private readEnumValue(p: number, end: number): { value: unknown; next: number } {
    const r = this.varintBig(p, end);
    return { value: Number(BigInt.asIntN(32, r.v)), next: r.next };
  }

  /** Reads a map entry, a message with the key as field 1 and the value as field 2. A missing key or value is the zero value; other fields are dropped. */
  private readMapEntry(map: ReflectMap, f: DescField & { fieldKind: "map" }, start: number, end: number, depth: number): void {
    if (depth + 1 > MAX_DEPTH) throw malformed(`nested deeper than ${MAX_DEPTH}`);
    let key: unknown;
    let val: unknown;
    let p = start;
    while (p < end) {
      const t = this.varint(p, end);
      p = t.next;
      const no = Math.floor(t.v / 8);
      const wt = t.v % 8;
      if (no === 0 || no > MAX_FIELD_NUMBER || t.v >= 2 ** 32) throw malformed(`bad field number in the tag at byte ${p}`);
      if (no === 1 && wt === scalarWire(f.mapKey)) {
        const r = this.readScalar(f.mapKey, p, end, f.name);
        key = r.value;
        p = r.next;
      } else if (no === 2 && f.mapKind === "scalar" && wt === scalarWire(f.scalar)) {
        const r = this.readScalar(f.scalar, p, end, f.name);
        val = r.value;
        p = r.next;
      } else if (no === 2 && f.mapKind === "enum" && wt === WT_VARINT) {
        const r = this.readEnumValue(p, end);
        val = r.value;
        p = r.next;
      } else if (no === 2 && f.mapKind === "message" && wt === WT_LEN) {
        const l = this.varint(p, end);
        if (l.v > end - l.next) throw malformed(`truncated map value at byte ${l.next}`);
        const sub = val === undefined ? reflect(f.message, undefined, false) : (val as ReflectMessage);
        this.readMessage(sub, l.next, l.next + l.v, depth + 2);
        val = sub;
        p = l.next + l.v;
      } else {
        p = this.skip(wt, p, end).next;
      }
    }
    if (key === undefined) key = zeroScalar(f.mapKey);
    if (val === undefined) {
      if (f.mapKind === "scalar") val = zeroScalar(f.scalar);
      else if (f.mapKind === "enum") val = 0;
      else val = reflect(f.message, undefined, false);
    }
    map.set(key, val);
  }

  private readScalar(t: ScalarType, p: number, end: number, name: string): { value: unknown; next: number } {
    switch (t) {
      case ScalarType.BOOL: {
        const r = this.varintBig(p, end);
        return { value: r.v !== 0n, next: r.next };
      }
      case ScalarType.INT32: {
        const r = this.varintBig(p, end);
        return { value: Number(BigInt.asIntN(32, r.v)), next: r.next };
      }
      case ScalarType.UINT32: {
        const r = this.varintBig(p, end);
        return { value: Number(BigInt.asUintN(32, r.v)), next: r.next };
      }
      case ScalarType.INT64: {
        const r = this.varintBig(p, end);
        return { value: BigInt.asIntN(64, r.v), next: r.next };
      }
      case ScalarType.UINT64: {
        const r = this.varintBig(p, end);
        return { value: r.v, next: r.next };
      }
      case ScalarType.SINT32: {
        const r = this.varintBig(p, end);
        const z = BigInt.asUintN(32, r.v);
        return { value: Number((z >> 1n) ^ -(z & 1n)), next: r.next };
      }
      case ScalarType.SINT64: {
        const r = this.varintBig(p, end);
        return { value: BigInt.asIntN(64, (r.v >> 1n) ^ -(r.v & 1n)), next: r.next };
      }
      case ScalarType.FIXED32:
        return { value: this.view(p, end, 4).getUint32(0, true), next: p + 4 };
      case ScalarType.SFIXED32:
        return { value: this.view(p, end, 4).getInt32(0, true), next: p + 4 };
      case ScalarType.FLOAT:
        return { value: this.view(p, end, 4).getFloat32(0, true), next: p + 4 };
      case ScalarType.FIXED64:
        return { value: this.view(p, end, 8).getBigUint64(0, true), next: p + 8 };
      case ScalarType.SFIXED64:
        return { value: this.view(p, end, 8).getBigInt64(0, true), next: p + 8 };
      case ScalarType.DOUBLE:
        return { value: this.view(p, end, 8).getFloat64(0, true), next: p + 8 };
      case ScalarType.STRING:
      case ScalarType.BYTES: {
        const l = this.varint(p, end);
        if (l.v > end - l.next) throw malformed(`truncated field at byte ${l.next}`);
        const stop = l.next + l.v;
        if (t === ScalarType.BYTES) return { value: this.b.slice(l.next, stop), next: stop };
        // 02 [ARC-61]: never repaired, never normalized
        if (!isValidUtf8(this.b, l.next, stop)) throw new CanonError("invalid-utf8", name, "");
        return { value: utf8DecodeValid(this.b.subarray(l.next, stop)), next: stop };
      }
    }
  }

  private view(p: number, end: number, n: number): DataView {
    if (end - p < n) throw malformed(`truncated fixed-width value at byte ${p}`);
    return new DataView(this.b.buffer, this.b.byteOffset + p, n);
  }
}

function zeroScalar(t: ScalarType): unknown {
  switch (t) {
    case ScalarType.STRING:
      return "";
    case ScalarType.BYTES:
      return new Uint8Array(0);
    case ScalarType.BOOL:
      return false;
    case ScalarType.INT64:
    case ScalarType.UINT64:
    case ScalarType.SINT64:
    case ScalarType.FIXED64:
    case ScalarType.SFIXED64:
      return 0n;
    default:
      return 0;
  }
}
