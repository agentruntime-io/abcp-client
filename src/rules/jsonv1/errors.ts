// The refusals of json/v1: a value, a counter step, a value schema or a manifest document that breaks a rule.

/**
 * Why a value or a counter step was refused. The values are the `reason` members of the json-v1 and json-counter vectors
 * (conformance/vectors/README.md), so a vector compares them as they are.
 */
export type Reason =
  | "utf8" // the bytes are not UTF-8 (05 [STATE-7])
  | "json-syntax" // not a JSON text (RFC 8259): an empty value and a byte-order mark included
  | "json-duplicate" // two members of one object have the same name
  | "json-surrogate" // an unpaired surrogate, written as an escape (I-JSON)
  | "json-nonchar" // a string holds a Unicode noncharacter (I-JSON)
  | "json-number" // a number that is not a finite binary64 value, or rounds to zero
  | "json-limits" // depth over 32, over 1,024 members, or U+0000 in a string or a member name
  | "not-canonical" // I-JSON, but not in RFC 8785 form
  | "literal-null" // the bytes `null`: null has one representation, `is_null` (05 [STATE-15])
  | "schema" // the value breaks its template's value_schema (05 [STATE-10], 04 [MAN-26], [MAN-27])
  | "range" // an INCR result or an OVERRIDE value outside the template's range (05 [STATE-26], [STATE-28])
  | "delta-range" // an INCR delta of magnitude above 2^53-1 (05 [STATE-27])
  | "not-integer" // an OVERRIDE value that is valid json/v1 but not an integer (05 [STATE-26])
  | "definition"; // a refused value schema or manifest document (04 [MAN-3], [MAN-25]): refused at publish, not judged as a value

/** The wire code of a refusal (A.1): a value is SCHEMA_INVALID, a counter result RANGE_EXCEEDED, a definition INVALID_ENVELOPE. */
export type ErrorCode = "SCHEMA_INVALID" | "RANGE_EXCEEDED" | "INVALID_ENVELOPE";

/** The typed refusal of this package (02 [ARC-46]): nothing else is thrown for untrusted input. */
export class JsonV1Error extends Error {
  readonly code: ErrorCode;
  readonly reason: Reason;
  /** The rule that decides it, such as "STATE-7". */
  readonly rule: string;
  /** Value schemas only: the attribute path inside the schema, such as "fields.a.items". */
  readonly path: string;

  constructor(code: ErrorCode, reason: Reason, rule: string, message: string, path = "") {
    super(`${code} (${reason}, ${rule})${path ? ` at ${path}` : ""}: ${message}`);
    this.name = "JsonV1Error";
    this.code = code;
    this.reason = reason;
    this.rule = rule;
    this.path = path;
  }
}

export function valueError(reason: Reason, rule: string, message: string): JsonV1Error {
  return new JsonV1Error("SCHEMA_INVALID", reason, rule, message);
}

export function definitionError(rule: string, path: string, message: string): JsonV1Error {
  return new JsonV1Error("INVALID_ENVELOPE", "definition", rule, message, path);
}
