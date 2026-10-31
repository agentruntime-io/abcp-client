// Wire types and field descriptors for the canonical encoder, the checker and the decoder.

import { ScalarType, type DescField, type DescMessage } from "@bufbuild/protobuf";

export const WT_VARINT = 0;
export const WT_FIXED64 = 1;
export const WT_LEN = 2;
export const WT_FIXED32 = 5;

/** The largest valid field number (2^29 − 1). */
export const MAX_FIELD_NUMBER = 536870911;

/** The wire type that a scalar kind is written with. */
export function scalarWire(t: ScalarType): number {
  switch (t) {
    case ScalarType.STRING:
    case ScalarType.BYTES:
      return WT_LEN;
    case ScalarType.DOUBLE:
    case ScalarType.FIXED64:
    case ScalarType.SFIXED64:
      return WT_FIXED64;
    case ScalarType.FLOAT:
    case ScalarType.FIXED32:
    case ScalarType.SFIXED32:
      return WT_FIXED32;
    default:
      return WT_VARINT;
  }
}

/** True for the scalar kinds that a repeated field writes packed: every numeric and bool kind, never string or bytes. */
export function scalarPackable(t: ScalarType): boolean {
  return t !== ScalarType.STRING && t !== ScalarType.BYTES;
}

/** What the checker needs to know about a field. */
export interface FieldInfo {
  /** The proto name of the field, for paths. */
  name: string;
  kind: "string" | "message" | "map" | "varint" | "other";
  /** A repeated field (never a map). */
  list: boolean;
  /** kind "message": the message type. */
  message?: DescMessage;
  /** kind "map": the entry's key (field 1) and value (field 2). */
  entry?: { key: FieldInfo; value: FieldInfo };
}

function scalarInfo(name: string, t: ScalarType, list: boolean): FieldInfo {
  if (t === ScalarType.STRING) return { name, kind: "string", list };
  if (scalarWire(t) === WT_VARINT) return { name, kind: "varint", list };
  return { name, kind: "other", list };
}

/** Describes one field of a message for the checker. */
export function fieldInfo(f: DescField): FieldInfo {
  switch (f.fieldKind) {
    case "scalar":
      return scalarInfo(f.name, f.scalar, false);
    case "enum":
      return { name: f.name, kind: "varint", list: false };
    case "message":
      return { name: f.name, kind: "message", list: false, message: f.message };
    case "list":
      if (f.listKind === "scalar") return scalarInfo(f.name, f.scalar, true);
      if (f.listKind === "enum") return { name: f.name, kind: "varint", list: true };
      return { name: f.name, kind: "message", list: true, message: f.message };
    case "map": {
      const key = scalarInfo("key", f.mapKey, false);
      let value: FieldInfo;
      if (f.mapKind === "scalar") value = scalarInfo("value", f.scalar, false);
      else if (f.mapKind === "enum") value = { name: "value", kind: "varint", list: false };
      else value = { name: "value", kind: "message", list: false, message: f.message };
      return { name: f.name, kind: "map", list: false, entry: { key, value } };
    }
  }
}

const byNumber = new WeakMap<DescMessage, Map<number, DescField>>();

/** The fields of a message by field number (cached per descriptor). */
export function fieldsByNumber(desc: DescMessage): Map<number, DescField> {
  let m = byNumber.get(desc);
  if (m === undefined) {
    m = new Map(desc.fields.map((f) => [f.number, f]));
    byNumber.set(desc, m);
  }
  return m;
}

const infoCache = new WeakMap<DescField, FieldInfo>();

export function cachedFieldInfo(f: DescField): FieldInfo {
  let i = infoCache.get(f);
  if (i === undefined) {
    i = fieldInfo(f);
    infoCache.set(f, i);
  }
  return i;
}
