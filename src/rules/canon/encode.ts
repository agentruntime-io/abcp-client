// The canonical protobuf encoding of 07 [MUT-45], over any message through its descriptor.
//
// It is not `toBinary`, which writes the unknown fields of a message after its known ones and does not sort map entries by
// the bytes of their keys. [MUT-45] puts every unknown field at the place of its number, and sorts map entries by the bytes
// of their keys.

import { ScalarType, type DescField, type DescMessage, type Message } from "@bufbuild/protobuf";
import { reflect, type ReflectList, type ReflectMap, type ReflectMessage } from "@bufbuild/protobuf/reflect";
import { ByteWriter, compareBytes, hasLoneSurrogate, utf8Encode } from "../bytes.js";
import { CanonError, MAX_DEPTH, malformed } from "./errors.js";
import { scalarPackable, scalarWire, WT_LEN, WT_VARINT } from "./wire.js";

/**
 * Says which parts of a message an encoding leaves out. The hashes of 06 [LOG-71], 07 [MUT-45] and 12 [ADM-94] are all the
 * canonical encoding of a message with some fields cleared, which a view gives without a copy of the message. An absent
 * function keeps everything.
 */
export interface EncodeView {
  /** Whether a populated field of a message of type md is left out, as if it were cleared. */
  skipField?(md: DescMessage, fd: DescField): boolean;
  /** Whether the unknown fields of a message of type md are left out. */
  skipUnknown?(md: DescMessage): boolean;
  /**
   * skipField for a decision that reads the message itself and not only its type: the `key` of a change whose kind and key the
   * request digest judges (12 [ADM-94]).
   */
  skipFieldIn?(m: ReflectMessage, fd: DescField): boolean;
}

/**
 * Returns the canonical encoding of message (07 [MUT-45]): every field in increasing order of its number, unknown fields at
 * the place of their numbers (a known field first when a number is both, several unknown fields of one number in the order
 * received); a field without explicit presence omitted at its proto3 default; a message field, an `optional` field and a
 * `oneof` member written whenever it is set; repeated numeric and enum fields packed, others one per element; map entries
 * in increasing order of their keys' bytes; every tag, length and varint in its shortest form.
 *
 * A string field that is not valid UTF-8 (an unpaired surrogate in the JavaScript string) is a CanonError (02 [ARC-61]: a
 * party never holds such a value), and so is a message nested deeper than MAX_DEPTH.
 */
export function encode(schema: DescMessage, message: Message, view?: EncodeView): Uint8Array {
  const w = new ByteWriter();
  encodeMessage(w, reflect(schema, message, false), view, 0);
  return w.finish();
}

function writeFixed(w: ByteWriter, bytes: number, set: (v: DataView) => void): void {
  const b = new Uint8Array(bytes);
  set(new DataView(b.buffer));
  w.bytes(b);
}

function encodeMessage(w: ByteWriter, m: ReflectMessage, view: EncodeView | undefined, depth: number): void {
  if (depth > MAX_DEPTH) throw malformed(`nested deeper than ${MAX_DEPTH}`);
  const md = m.desc;
  // 07 [MUT-45] "A field that the encoding party does not know is written as the wire bytes it was received with". A known
  // field that arrived with a wire type that its kind does not have (a string field as a varint) is kept by `decode` as an
  // unknown field, as Go's decoder does, and is written here at its number, after the known field of that number, if one is
  // set. Since round 16 the text refuses such a message (`checkShortest`, kind `wire-type`), so a received message that passes
  // the checks holds none; a message that a caller built with one is still encoded as received.
  //
  // 07 [MUT-45] puts each unknown field at the place of its number, several of one number in the order received. A sender that writes
  // the canonical encoding writes them in increasing order of their numbers already, which one pass checks with no allocation; the
  // fields are then written as the cursor meets them. Otherwise the indexes of the fields are ordered by (number, index) in one typed
  // array, so that the cost of a message of unknown fields is that and not an object for each of them, and two copies of the list: an
  // 8 MiB mutation of 3-octet fields held 2.8 million fields, and the first version of the Go twin, which built a struct for each and
  // sorted them, allocated 543 MiB to hash it (V1 SHOULD-1).
  let unknown = m.getUnknown();
  let order: Uint32Array | null = null; // null: the fields are in the order of their numbers, and are written as they stand
  if (unknown !== undefined && unknown.length > 0 && view?.skipUnknown?.(md) !== true) {
    const list = unknown;
    let sorted = true;
    for (let i = 1; i < list.length; i++) {
      if ((list[i] as { no: number }).no < (list[i - 1] as { no: number }).no) {
        sorted = false;
        break;
      }
    }
    if (!sorted) {
      order = new Uint32Array(list.length);
      for (let i = 0; i < list.length; i++) order[i] = i;
      // the index is part of the key, so the order within a number is the order received
      order.sort((a, b) => (list[a] as { no: number }).no - (list[b] as { no: number }).no || a - b);
    }
  } else {
    unknown = undefined;
  }
  let ui = 0;
  const writeUnknownBelow = (no: number): void => {
    while (unknown !== undefined && ui < unknown.length) {
      const f = unknown[order === null ? ui : (order[ui] as number)] as { no: number; wireType: number; data: Uint8Array };
      if (f.no >= no) break;
      writeUnknown(w, f);
      ui++;
    }
  };
  for (const fd of m.sortedFields) {
    if (!m.isSet(fd)) continue;
    if (view?.skipField?.(md, fd) === true || view?.skipFieldIn?.(m, fd) === true) continue;
    writeUnknownBelow(fd.number);
    try {
      encodeField(w, fd, m, view, depth);
    } catch (e) {
      if (e instanceof CanonError) throw new CanonError(e.kind, e.path === "" ? fd.name : `${fd.name}.${e.path}`, e.detail);
      throw e;
    }
  }
  writeUnknownBelow(Number.MAX_SAFE_INTEGER);
}

function writeUnknown(w: ByteWriter, u: { no: number; wireType: number; data: Uint8Array }): void {
  w.varint(u.no * 8 + u.wireType);
  w.bytes(u.data);
}

function encodeField(w: ByteWriter, fd: DescField, m: ReflectMessage, view: EncodeView | undefined, depth: number): void {
  switch (fd.fieldKind) {
    case "map":
      encodeMap(w, fd, m.get(fd) as ReflectMap, view, depth);
      return;
    case "list":
      encodeList(w, fd, m.get(fd) as ReflectList, view, depth);
      return;
    case "scalar":
      w.varint(fd.number * 8 + scalarWire(fd.scalar));
      encodeScalar(w, fd.scalar, m.get(fd));
      return;
    case "enum":
      w.varint(fd.number * 8 + WT_VARINT);
      encodeEnum(w, m.get(fd) as number);
      return;
    case "message":
      w.varint(fd.number * 8 + WT_LEN);
      encodeNested(w, m.get(fd) as ReflectMessage, view, depth);
      return;
  }
}

/** A repeated numeric or enum field is written packed, and every other repeated field as one field per element, in order. */
function encodeList(w: ByteWriter, fd: DescField & { fieldKind: "list" }, list: ReflectList, view: EncodeView | undefined, depth: number): void {
  if (list.size === 0) return;
  const packable = fd.listKind === "enum" || (fd.listKind === "scalar" && scalarPackable(fd.scalar));
  if (packable) {
    const body = new ByteWriter();
    for (const item of list) {
      if (fd.listKind === "enum") encodeEnum(body, item as number);
      else encodeScalar(body, fd.scalar as ScalarType, item);
    }
    w.varint(fd.number * 8 + WT_LEN);
    w.lbytesVarint(body.finish());
    return;
  }
  for (const item of list) {
    if (fd.listKind === "scalar") {
      w.varint(fd.number * 8 + scalarWire(fd.scalar));
      encodeScalar(w, fd.scalar, item);
    } else {
      w.varint(fd.number * 8 + WT_LEN);
      encodeNested(w, item as ReflectMessage, view, depth);
    }
  }
}

/**
 * Writes the entries in increasing order of their keys' bytes (UTF-8 for a string key), each as a message with the key as
 * field 1 and the value as field 2, both always written.
 *
 * SPEC-QUESTION: 07 [MUT-45] "map entries in increasing order of their keys' bytes (UTF-8 for string keys)". For a key that
 * is no string, "bytes" is read as the key's encoding on the wire, as both vector models and the Go server do, and not as
 * its big-endian value. Every map of abpc/v2 has string keys, so no vector decides it.
 */
function encodeMap(w: ByteWriter, fd: DescField & { fieldKind: "map" }, map: ReflectMap, view: EncodeView | undefined, depth: number): void {
  if (map.size === 0) return;
  const entries: { key: Uint8Array; k: unknown; v: unknown }[] = [];
  for (const [k, v] of map) {
    let kb: Uint8Array;
    if (fd.mapKey === ScalarType.STRING) {
      if (typeof k !== "string" || hasLoneSurrogate(k)) throw new CanonError("invalid-utf8", "", "map key");
      kb = utf8Encode(k);
    } else {
      const kw = new ByteWriter();
      encodeScalar(kw, fd.mapKey, k);
      kb = kw.finish();
    }
    entries.push({ key: kb, k, v });
  }
  entries.sort((a, b) => compareBytes(a.key, b.key));
  for (const en of entries) {
    const body = new ByteWriter();
    body.varint(1 * 8 + scalarWire(fd.mapKey));
    encodeScalar(body, fd.mapKey, en.k);
    if (fd.mapKind === "scalar") {
      body.varint(2 * 8 + scalarWire(fd.scalar));
      encodeScalar(body, fd.scalar, en.v);
    } else if (fd.mapKind === "enum") {
      body.varint(2 * 8 + WT_VARINT);
      encodeEnum(body, en.v as number);
    } else {
      body.varint(2 * 8 + WT_LEN);
      encodeNested(body, en.v as ReflectMessage, view, depth);
    }
    w.varint(fd.number * 8 + WT_LEN);
    w.lbytesVarint(body.finish());
  }
}

/** Writes the length of a nested message, a varint in its shortest form, and its canonical encoding. */
function encodeNested(w: ByteWriter, m: ReflectMessage, view: EncodeView | undefined, depth: number): void {
  const body = new ByteWriter();
  encodeMessage(body, m, view, depth + 1);
  w.lbytesVarint(body.finish());
}

function encodeEnum(w: ByteWriter, v: number): void {
  w.varintBig(BigInt.asUintN(64, BigInt(v))); // a negative value takes ten bytes
}

/** Writes one scalar value without its tag: a varint, a fixed-width number, or a length and the bytes. */
function encodeScalar(w: ByteWriter, t: ScalarType, v: unknown): void {
  switch (t) {
    case ScalarType.BOOL:
      w.u8(v === true ? 1 : 0);
      return;
    case ScalarType.INT32:
      w.varintBig(BigInt.asUintN(64, BigInt(v as number))); // a negative value takes ten bytes
      return;
    case ScalarType.INT64:
      w.varintBig(BigInt.asUintN(64, BigInt(v as bigint)));
      return;
    case ScalarType.UINT32:
      w.varint(v as number);
      return;
    case ScalarType.UINT64:
      w.varintBig(BigInt(v as bigint));
      return;
    case ScalarType.SINT32: {
      const n = BigInt(v as number);
      w.varintBig(BigInt.asUintN(32, (n << 1n) ^ (n >> 31n)));
      return;
    }
    case ScalarType.SINT64: {
      const n = BigInt(v as bigint);
      w.varintBig(BigInt.asUintN(64, (n << 1n) ^ (n >> 63n)));
      return;
    }
    case ScalarType.FIXED32:
      writeFixed(w, 4, (d) => d.setUint32(0, v as number, true));
      return;
    case ScalarType.SFIXED32:
      writeFixed(w, 4, (d) => d.setInt32(0, v as number, true));
      return;
    case ScalarType.FIXED64:
      writeFixed(w, 8, (d) => d.setBigUint64(0, BigInt(v as bigint), true));
      return;
    case ScalarType.SFIXED64:
      writeFixed(w, 8, (d) => d.setBigInt64(0, BigInt(v as bigint), true));
      return;
    case ScalarType.FLOAT:
      writeFixed(w, 4, (d) => d.setFloat32(0, v as number, true));
      return;
    case ScalarType.DOUBLE:
      writeFixed(w, 8, (d) => d.setFloat64(0, v as number, true));
      return;
    case ScalarType.STRING: {
      const s = v as string;
      if (hasLoneSurrogate(s)) throw new CanonError("invalid-utf8", "", "");
      w.lbytesVarint(utf8Encode(s)); // copied unchanged, with no Unicode normalization
      return;
    }
    case ScalarType.BYTES:
      w.lbytesVarint(v as Uint8Array);
      return;
  }
}
