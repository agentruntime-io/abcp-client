// Value schemas (04 §4.4.2): reading a `value_schema` from a manifest and judging a value against it.

import { definitionError, echoName, JsonV1Error, quote, valueError } from "./errors.js";
import {
  type JsonValue,
  MAX_SCHEMA_DEPTH,
  isInteger,
} from "./value.js";
import { parseDocument } from "./parse.js";

export type SchemaType = "string" | "int" | "number" | "bool" | "bytes" | "object" | "array";

const TYPES: readonly SchemaType[] = ["string", "int", "number", "bool", "bytes", "object", "array"];

/**
 * A value schema as `parseSchema` returns it: well formed at every depth (04 [MAN-25], [MAN-26]), with `type` always set
 * (a schema with only an `enum` has the type of its values). Treat it as read-only.
 */
export interface Schema {
  readonly type: SchemaType;
  /** A string enum (type string), distinct, none of them excluded by the length bounds. */
  readonly enum?: readonly string[];
  /** An integer enum (type int), distinct, none of them excluded by min and max. */
  readonly enumInt?: readonly number[];
  /** string (code points), bytes (octets) and array (items). */
  readonly minLen?: number;
  readonly maxLen?: number;
  /** int and number, inclusive; for int always an integer. */
  readonly min?: number;
  readonly max?: number;
  /** object: the fields it may have. With none, the object allows only {} (04 [MAN-26]). */
  readonly fields?: ReadonlyMap<string, Schema>;
  /** object: fields it must have, distinct, each of them in `fields`. */
  readonly required: readonly string[];
  /** array: the schema of every item. With none, any value, null included (04 [MAN-26]). */
  readonly items?: Schema;
  /** Whether an explicit null (05 §5.3) or, nested, the JSON null is allowed (04 [MAN-27]). */
  readonly nullable: boolean;
  /** @internal the set of `required`, built with the schema. */
  readonly requiredSet: ReadonlySet<string>;
}

/** What a value schema is for: 04 [MAN-18] fits the attributes of a schema to the merge rule and the encoding of its template. */
export type SchemaUse =
  | "json" // an lww or cas template of encoding json/v1: no `type: bytes` at any depth
  | "bytes" // a bytes/v1 template: `type: bytes` with no attribute but min_len, max_len and nullable
  | "counter"; // a counter template: `type: int` with min, max and no `nullable: true`, and no other attribute

/** The inclusive range of a counter template (04 [MAN-18]): always within ±(2^53−1) (05 [STATE-28]). */
export interface Range {
  readonly min: bigint;
  readonly max: bigint;
}

export const DEFAULT_RANGE: Range = {
  min: -BigInt(Number.MAX_SAFE_INTEGER),
  max: BigInt(Number.MAX_SAFE_INTEGER),
};

/** The range that the schema of a counter template gives: its `min` and `max`, and ±(2^53−1) where it gives none. */
export function intRange(s: Schema | undefined): Range {
  if (s === undefined) return DEFAULT_RANGE;
  return {
    min: s.min !== undefined ? BigInt(s.min) : DEFAULT_RANGE.min,
    max: s.max !== undefined ? BigInt(s.max) : DEFAULT_RANGE.max,
  };
}

/** True when the schema has an `enum`. */
export function hasEnum(s: Schema): boolean {
  return (s.enum?.length ?? 0) > 0 || (s.enumInt?.length ?? 0) > 0;
}

/**
 * Whether an explicit null (05 [STATE-14]) is allowed on a template whose rule is not counter and not crdt:*, which
 * never allow it: only when it has no schema, or its top level has `nullable: true` (04 [MAN-27]).
 */
export function allowsNull(s: Schema | undefined): boolean {
  return s === undefined || s.nullable;
}

function lengthWithin(s: Schema, n: number): boolean {
  return (s.minLen === undefined || n >= s.minLen) && (s.maxLen === undefined || n <= s.maxLen);
}

function boundsWithin(s: Schema, f: number): boolean {
  return (s.min === undefined || f >= s.min) && (s.max === undefined || f <= s.max);
}

/** The number of code points of a string without an unpaired surrogate. */
export function codePointLength(s: string): number {
  let n = 0;
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c < 0xdc00 || c > 0xdfff) n++; // a low surrogate continues the pair that its high one began
  }
  return n;
}

/** @internal Returns "" when a string value fits the schema, and else why it does not. Length counts code points. */
export function fitString(s: Schema, str: string): string {
  if (s.type !== "string") return `expected ${s.type}, got string`;
  if (s.enum !== undefined && s.enum.length > 0 && !s.enum.includes(str)) {
    return "the string is not one of the enum values";
  }
  if ((s.minLen !== undefined || s.maxLen !== undefined) && !lengthWithin(s, codePointLength(str))) {
    return "the string length is outside the length bounds";
  }
  return "";
}

/** @internal Returns "" when a number value fits the schema, and else why it does not. An int is an integer of 05 [STATE-7], so 1e21 is a number and not an int. */
export function fitNumber(s: Schema, f: number): string {
  switch (s.type) {
    case "int":
      if (!isInteger(f)) return "the number is not an integer within ±(2^53−1)";
      if (s.enumInt !== undefined && s.enumInt.length > 0 && !s.enumInt.includes(f === 0 ? 0 : f)) {
        return "the integer is not one of the enum values";
      }
      break;
    case "number":
      break;
    default:
      return `expected ${s.type}, got number`;
  }
  return boundsWithin(s, f) ? "" : "the number is outside the bounds";
}

/** @internal */
export function schemaLengthWithin(s: Schema, n: number): boolean {
  return lengthWithin(s, n);
}

/**
 * Checks a bytes/v1 value against the template's schema (05 [STATE-8], 04 [MAN-18], [MAN-28]): with no schema any octets
 * are valid; with one (`type: bytes`) the number of octets lies within its length bounds. The size limit in force
 * (04 [MAN-19]) and the explicit null (`allowsNull`) are the caller's. Returns null for a valid value.
 */
export function validateBytes(b: Uint8Array, s: Schema | undefined): JsonV1Error | null {
  if (s === undefined) return null;
  if (s.type !== "bytes") return valueError("schema", "STATE-8", `expected ${s.type}, got bytes`);
  if (!lengthWithin(s, b.length)) return valueError("schema", "STATE-8", `${b.length} octets are outside the length bounds`);
  return null;
}

/**
 * Reads a value schema written as JSON (the `value_schema` of a template of a json/v1 manifest, 04 §4.4.2) and checks it
 * for the given use. Every refusal is a JsonV1Error with code INVALID_ENVELOPE (04 [MAN-77], row 9), the rule that it
 * breaks, and the attribute path inside the schema.
 */
export function parseSchema(text: Uint8Array, use: SchemaUse): Schema {
  return schemaFromValue(parseDocument(text), use);
}

/**
 * Reads a value schema from an already parsed value, as `parseSchema` does. It enforces 04 [MAN-25]: only the attributes
 * of the table, each with the types that allow it and of its own type and range; `type` required unless `enum` is given;
 * depth at most 8, at most 256 fields and 1,024 enum values; and no schema that allows no value. It enforces 04 [MAN-26]
 * for names (`^[A-Za-z_][A-Za-z0-9_]{0,63}$`, not `__proto__`) and for `required`, and the use (04 [MAN-18]).
 */
export function schemaFromValue(v: JsonValue, use: SchemaUse): Schema {
  const s = readNode(v, 1, "", use);
  switch (use) {
    case "bytes":
      if (s.type !== "bytes") throw definitionError("MAN-18", "type", "the value_schema of a bytes/v1 template must have type bytes");
      break;
    case "counter":
      if (s.type !== "int") throw definitionError("MAN-18", "type", "the value_schema of a counter template must have type int");
      if (hasEnum(s)) throw definitionError("MAN-25", "enum", "the schema of a counter template has no enum");
      if (s.minLen !== undefined || s.maxLen !== undefined) throw definitionError("MAN-25", "", "the schema of a counter template has no length bound");
      if (s.nullable) throw definitionError("MAN-27", "nullable", "a counter never allows null");
      break;
    default:
      break;
  }
  return s;
}

function childPath(path: string, name: string): string {
  name = echoName(name); // a path names a place, and never repeats a name of any length (V1 SHOULD-8)
  return path === "" ? name : `${path}.${name}`;
}

interface MutableSchema {
  type: SchemaType;
  enum?: string[];
  enumInt?: number[];
  minLen?: number;
  maxLen?: number;
  min?: number;
  max?: number;
  fields?: Map<string, Schema>;
  required: string[];
  items?: Schema;
  nullable: boolean;
  requiredSet: Set<string>;
}

function readNode(v: JsonValue, depth: number, path: string, use: SchemaUse): Schema {
  if (depth > MAX_SCHEMA_DEPTH) throw definitionError("MAN-25", path, `a value schema nests deeper than ${MAX_SCHEMA_DEPTH}`);
  if (!(v instanceof Map)) throw definitionError("MAN-25", path, "a value schema is an object");
  const s: MutableSchema = { type: "string", required: [], nullable: false, requiredSet: new Set() };
  let hasType = false;
  let hasEnum_ = false;
  let hasFields = false;
  let hasRequired = false;
  const attrs = new Set<string>();
  for (const [name, val] of v) {
    const p = childPath(path, name);
    if (attrs.has(name)) throw definitionError("MAN-25", p, `the attribute ${quote(name)} is given twice`);
    attrs.add(name);
    switch (name) {
      case "type": {
        if (typeof val !== "string") throw definitionError("MAN-25", p, "type is a string");
        const t = TYPES.find((x) => x === val);
        if (t === undefined) throw definitionError("MAN-25", p, `${quote(val)} is not a type of a value schema`);
        s.type = t;
        hasType = true;
        break;
      }
      case "enum":
        hasEnum_ = true;
        readEnum(s, val, p);
        break;
      case "min_len":
      case "max_len": {
        if (typeof val !== "number" || !isInteger(val) || val < 0) {
          throw definitionError("MAN-25", p, `${name} is an integer of at least 0`);
        }
        if (name === "min_len") s.minLen = val;
        else s.maxLen = val;
        break;
      }
      case "min":
      case "max": {
        if (typeof val !== "number" || !Number.isFinite(val)) throw definitionError("MAN-25", p, `${name} is a number`);
        if (name === "min") s.min = val;
        else s.max = val;
        break;
      }
      case "fields":
        hasFields = true;
        readFields(s, val, depth, p, use);
        break;
      case "required": {
        hasRequired = true;
        if (!Array.isArray(val)) throw definitionError("MAN-25", p, "required is a list of field names");
        val.forEach((r, i) => {
          if (typeof r !== "string") throw definitionError("MAN-25", `${p}[${i}]`, "required is a list of field names");
          // 04 [MAN-26] (draft 14, Q5): "a name listed twice in `required` is accepted and means the same as one, and the
          // canonical form (MAN-4) keeps the list as published". Read as a set here, as the Go server does: a repeated name is
          // kept once in the schema; the manifest's canonical form is made from the document.
          if (!s.requiredSet.has(r)) {
            s.requiredSet.add(r);
            s.required.push(r);
          }
        });
        break;
      }
      case "items":
        s.items = readNode(val, depth + 1, p, use);
        break;
      case "nullable":
        if (typeof val !== "boolean") throw definitionError("MAN-25", p, "nullable is a boolean");
        s.nullable = val;
        break;
      default:
        throw definitionError("MAN-25", p, `${quote(name)} is not an attribute of a value schema`);
    }
  }

  // type: required unless enum is given; with enum alone the type is that of its values.
  if (!hasType) {
    if (!hasEnum_) throw definitionError("MAN-25", path, "type is required unless enum is given");
    s.type = s.enum !== undefined && s.enum.length > 0 ? "string" : "int";
  }
  const t = s.type;
  // Each attribute is allowed only with the types that the table of 04 §4.4.2 lists.
  const allow = (name: string, ok: boolean): void => {
    if (attrs.has(name) && !ok) throw definitionError("MAN-25", childPath(path, name), `${name} is not allowed with type ${t}`);
  };
  allow("enum", t === "string" || t === "int");
  allow("min_len", t === "string" || t === "bytes" || t === "array");
  allow("max_len", t === "string" || t === "bytes" || t === "array");
  allow("min", t === "int" || t === "number");
  allow("max", t === "int" || t === "number");
  allow("fields", t === "object");
  allow("required", t === "object");
  allow("items", t === "array");
  if (hasEnum_) {
    // an enum holds only values of the schema's own type
    if ((t === "string") !== (s.enum !== undefined && s.enum.length > 0)) {
      throw definitionError("MAN-25", childPath(path, "enum"), `the enum holds a value of a type other than ${t}`);
    }
  }
  if (t === "bytes" && use === "json") {
    throw definitionError("MAN-18", childPath(path, "type"), "a json/v1 value_schema has no type bytes at any depth");
  }
  if (t === "int") {
    if (s.min !== undefined && !isInteger(s.min)) throw definitionError("MAN-25", childPath(path, "min"), "min is an integer on a schema of type int");
    if (s.max !== undefined && !isInteger(s.max)) throw definitionError("MAN-25", childPath(path, "max"), "max is an integer on a schema of type int");
  }
  // no schema allows no value: the bounds are not empty, and an enum value is not excluded by them
  if (s.minLen !== undefined && s.maxLen !== undefined && s.minLen > s.maxLen) throw definitionError("MAN-25", path, "min_len is above max_len");
  if (s.min !== undefined && s.max !== undefined && s.min > s.max) throw definitionError("MAN-25", path, "min is above max");
  (s.enum ?? []).forEach((e, i) => {
    if (!lengthWithin(s, codePointLength(e))) {
      throw definitionError("MAN-25", `${childPath(path, "enum")}[${i}]`, `the enum value ${quote(e)} is excluded by the length bounds`);
    }
  });
  (s.enumInt ?? []).forEach((e, i) => {
    if (!boundsWithin(s, e)) {
      throw definitionError("MAN-25", `${childPath(path, "enum")}[${i}]`, `the enum value ${e} is excluded by min and max`);
    }
  });
  if (hasRequired) {
    for (const r of s.required) {
      if (!hasFields || s.fields === undefined || !s.fields.has(r)) {
        throw definitionError("MAN-26", childPath(path, "required"), `the required name ${quote(r)} is not in fields`);
      }
    }
  }
  return s;
}

/** An `enum`: a non-empty list of at most 1,024 distinct strings, or distinct integers. */
function readEnum(s: MutableSchema, v: JsonValue, path: string): void {
  if (!Array.isArray(v)) throw definitionError("MAN-25", path, "enum is a list of distinct strings or of distinct integers");
  if (v.length === 0) throw definitionError("MAN-25", path, "an enum is not empty");
  if (v.length > 1024) throw definitionError("MAN-25", path, "an enum has at most 1,024 values");
  const first = v[0];
  if (typeof first === "string") {
    const seen = new Set<string>();
    const out: string[] = [];
    v.forEach((it, i) => {
      if (typeof it !== "string") throw definitionError("MAN-25", `${path}[${i}]`, "an enum holds only strings or only integers");
      if (seen.has(it)) throw definitionError("MAN-25", `${path}[${i}]`, `the enum value ${quote(it)} is repeated`);
      seen.add(it);
      out.push(it);
    });
    s.enum = out;
  } else if (typeof first === "number") {
    const seen = new Set<number>();
    const out: number[] = [];
    v.forEach((it, i) => {
      if (typeof it !== "number" || !isInteger(it)) throw definitionError("MAN-25", `${path}[${i}]`, "an enum holds only strings or only integers");
      const n = it === 0 ? 0 : it; // −0 is 0
      if (seen.has(n)) throw definitionError("MAN-25", `${path}[${i}]`, `the enum value ${n} is repeated`);
      seen.add(n);
      out.push(n);
    });
    s.enumInt = out;
  } else {
    throw definitionError("MAN-25", `${path}[0]`, "an enum holds only strings or only integers");
  }
}

/** `fields`: at most 256 fields, names as 04 [MAN-26] gives them, each a schema one level deeper. */
function readFields(s: MutableSchema, v: JsonValue, depth: number, path: string, use: SchemaUse): void {
  if (!(v instanceof Map)) throw definitionError("MAN-25", path, "fields is a map from field name to value schema");
  if (v.size > 256) throw definitionError("MAN-25", path, "an object has at most 256 fields");
  const fields = new Map<string, Schema>();
  for (const [name, val] of v) {
    const p = childPath(path, name);
    if (!validFieldName(name)) {
      throw definitionError("MAN-26", p, `${quote(name)} is not a field name (^[A-Za-z_][A-Za-z0-9_]{0,63}$, not __proto__)`);
    }
    fields.set(name, readNode(val, depth + 1, p, use));
  }
  s.fields = fields;
}

/** 04 [MAN-26]: ^[A-Za-z_][A-Za-z0-9_]{0,63}$ and not __proto__. */
function validFieldName(name: string): boolean {
  return name !== "__proto__" && /^[A-Za-z_][A-Za-z0-9_]{0,63}$/.test(name);
}
