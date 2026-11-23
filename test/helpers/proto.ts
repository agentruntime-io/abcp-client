// Building protobuf-es messages from the JSON form that the conformance vectors use (conformance/vectors/README.md,
// "Canonical bytes"), and extending a message type with fields, as the vectors of an "upgraded server" do.

import {
  clone,
  create,
  createFileRegistry,
  createRegistry,
  ScalarType,
  type DescField,
  type DescFile,
  type DescMessage,
  type Message,
  type Registry,
} from "@bufbuild/protobuf";
import {
  FieldDescriptorProto_Label,
  FieldDescriptorProto_Type,
  FieldDescriptorProtoSchema,
  FileDescriptorProtoSchema,
  FileDescriptorSetSchema,
  type FileDescriptorProto,
} from "@bufbuild/protobuf/wkt";
import { reflect, type ReflectList, type ReflectMap, type ReflectMessage } from "@bufbuild/protobuf/reflect";
import { file_abcp_v2_admin } from "../../src/gen/abcp/v2/admin_pb.js";
import { file_abcp_v2_host } from "../../src/gen/abcp/v2/host_pb.js";
import { file_abcp_v2_sync } from "../../src/gen/abcp/v2/sync_pb.js";
import { file_abcp_v2_types } from "../../src/gen/abcp/v2/types_pb.js";
import { fromHex } from "../../src/rules/bytes.js";
import { expandBytes } from "./vectors.js";

export const registry: Registry = createRegistry(file_abcp_v2_types, file_abcp_v2_sync, file_abcp_v2_admin, file_abcp_v2_host);

export function messageDesc(name: string, reg: Registry = registry): DescMessage {
  const d = reg.getMessage(name);
  if (d === undefined) throw new Error(`no message type ${name}`);
  return d;
}

function numberText(v: unknown): string {
  return typeof v === "string" ? v : String(v);
}

/**
 * Fills a new message of type `desc` from the JSON form of the vectors: field names of the .proto files; strings as JSON
 * strings; bytes as {"hex"} or the other compact forms; 64-bit integers as decimal strings or numbers; enums as numbers;
 * repeated fields as arrays (a repeated string may be {"gen": ...}); map fields as arrays of [key, value] pairs; and
 * "$unknown": ["<hex of tag and payload>", ...] for fields that the type does not know, in received order.
 */
export function fromVectorJson(desc: DescMessage, json: unknown): Message {
  const m = reflect(desc, undefined, false);
  fill(m, json);
  return m.message;
}

function fill(m: ReflectMessage, json: unknown): void {
  if (typeof json !== "object" || json === null || Array.isArray(json)) throw new Error(`${m.desc.typeName}: not an object`);
  for (const [name, val] of Object.entries(json)) {
    if (name === "$unknown") {
      const unknown = [...(m.getUnknown() ?? [])];
      for (const h of val as string[]) {
        const b = fromHex(h);
        let i = 0;
        let tag = 0;
        let mul = 1;
        for (;;) {
          const x = b[i++] as number;
          tag += (x & 0x7f) * mul;
          mul *= 128;
          if (x < 0x80) break;
        }
        unknown.push({ no: Math.floor(tag / 8), wireType: tag % 8, data: b.slice(i) });
      }
      m.setUnknown(unknown);
      continue;
    }
    const fd = m.desc.fields.find((f) => f.name === name);
    if (fd === undefined) throw new Error(`${m.desc.typeName} has no field ${name}`);
    setField(m, fd, val);
  }
}

function scalar(t: ScalarType, v: unknown): unknown {
  switch (t) {
    case ScalarType.STRING:
      return v as string;
    case ScalarType.BYTES:
      return expandBytes(v);
    case ScalarType.BOOL:
      return v as boolean;
    case ScalarType.INT64:
    case ScalarType.SINT64:
    case ScalarType.SFIXED64:
    case ScalarType.UINT64:
    case ScalarType.FIXED64:
      return BigInt(numberText(v));
    case ScalarType.FLOAT:
    case ScalarType.DOUBLE:
      return Number(numberText(v));
    default:
      return Number(numberText(v));
  }
}

function setField(m: ReflectMessage, fd: DescField, val: unknown): void {
  switch (fd.fieldKind) {
    case "map": {
      const map = m.get(fd) as ReflectMap;
      for (const [k, v] of val as [unknown, unknown][]) {
        const key = scalar(fd.mapKey, k);
        if (fd.mapKind === "message") {
          const sub = reflect(fd.message, undefined, false);
          fill(sub, v);
          map.set(key, sub);
        } else if (fd.mapKind === "enum") {
          map.set(key, Number(numberText(v)));
        } else {
          map.set(key, scalar(fd.scalar, v));
        }
      }
      m.set(fd, map);
      return;
    }
    case "list": {
      const list = m.get(fd) as ReflectList;
      let items: unknown[];
      if (!Array.isArray(val)) {
        const gen = (val as { gen: { prefix: string; width: number; count: number } }).gen;
        items = Array.from({ length: gen.count }, (_, i) => gen.prefix + String(i).padStart(gen.width, "0"));
      } else {
        items = val;
      }
      for (const it of items) {
        if (fd.listKind === "message") {
          const sub = reflect(fd.message, undefined, false);
          fill(sub, it);
          list.add(sub);
        } else if (fd.listKind === "enum") {
          list.add(Number(numberText(it)));
        } else {
          list.add(scalar(fd.scalar, it));
        }
      }
      return;
    }
    case "message": {
      const sub = reflect(fd.message, undefined, false);
      fill(sub, val);
      m.set(fd, sub);
      return;
    }
    case "enum":
      m.set(fd, Number(numberText(val)));
      return;
    case "scalar":
      m.set(fd, scalar(fd.scalar, val));
      return;
  }
}

export interface FieldAddition {
  name: string;
  id: number;
  type: string;
  repeated?: boolean;
}

const SCALAR_TYPES: Record<string, FieldDescriptorProto_Type> = {
  string: FieldDescriptorProto_Type.STRING,
  bytes: FieldDescriptorProto_Type.BYTES,
  bool: FieldDescriptorProto_Type.BOOL,
  int32: FieldDescriptorProto_Type.INT32,
  int64: FieldDescriptorProto_Type.INT64,
  uint32: FieldDescriptorProto_Type.UINT32,
  uint64: FieldDescriptorProto_Type.UINT64,
  sint32: FieldDescriptorProto_Type.SINT32,
  sint64: FieldDescriptorProto_Type.SINT64,
};

/**
 * Returns a message type that is the top-level message type `base` with the fields in `add`: the type that an upgraded
 * server holds, which knows fields that the generated type keeps as unknown bytes.
 */
export function extendMessage(base: DescMessage, add: FieldAddition[]): DescMessage {
  // every file that the base's file depends on, dependencies first
  const protos: FileDescriptorProto[] = [];
  const seen = new Set<string>();
  const visit = (f: DescFile, replace?: FileDescriptorProto): void => {
    if (seen.has(f.name)) return;
    seen.add(f.name);
    for (const dep of f.dependencies) visit(dep);
    protos.push(replace ?? f.proto);
  };
  const proto = clone(FileDescriptorProtoSchema, base.file.proto);
  const target = proto.messageType.find((mt) => mt.name === base.name);
  if (target === undefined) throw new Error(`${base.typeName} is not a top-level message`);
  for (const a of add) {
    const type = SCALAR_TYPES[a.type];
    if (type === undefined) throw new Error(`field type ${a.type}`);
    target.field.push(
      create(FieldDescriptorProtoSchema, {
        name: a.name,
        number: a.id,
        label: a.repeated === true ? FieldDescriptorProto_Label.REPEATED : FieldDescriptorProto_Label.OPTIONAL,
        type,
        jsonName: a.name,
      }),
    );
  }
  visit(base.file, proto);
  const reg = createFileRegistry(create(FileDescriptorSetSchema, { file: protos }));
  const d = reg.getMessage(base.typeName);
  if (d === undefined) throw new Error("extended type not found");
  return d;
}
