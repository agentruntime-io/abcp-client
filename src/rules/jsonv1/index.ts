// json/v1 values (05 [STATE-7], [STATE-15], [STATE-26] to [STATE-28], 04 §4.4.2): the strict I-JSON reader, the RFC 8785
// canonical form, value schemas and counters.

export { JsonV1Error, type ErrorCode, type Reason } from "./errors.js";
export {
  type JsonObject,
  type JsonValue,
  MAX_CANONICAL_DOCUMENT_BYTES,
  MAX_DEPTH,
  MAX_DOCUMENT_BYTES,
  MAX_DOCUMENT_DEPTH,
  MAX_DOCUMENT_VALUES,
  MAX_MEMBERS,
  MAX_SAFE_INTEGER,
  MAX_SCHEMA_DEPTH,
  getMember,
  isInteger,
} from "./value.js";
export { denote, numberText } from "./number.js";
export { checkJsonText, parse, parseDocument, validate } from "./parse.js";
export { checkLimits } from "./limits.js";
export { canonical } from "./canonical.js";
export {
  DEFAULT_RANGE,
  type Range,
  type Schema,
  type SchemaType,
  type SchemaUse,
  allowsNull,
  hasEnum,
  intRange,
  parseSchema,
  schemaFromValue,
  validateBytes,
} from "./schema.js";
export { applyIncr, applyOverride, counterText, parseCounter, rangeContains } from "./counter.js";
export { canonicalDocument, canonicalManifest, manifestHash } from "./manifest.js";
