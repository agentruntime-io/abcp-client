// Why a CRDT update is refused or folded to nothing. The names are those of the conformance vectors
// (conformance/vectors/README.md, `crdt-fold`).

/** The reasons of a refused update, SCHEMA_INVALID at commit (05 [STATE-71]). */
export type RefusalReason =
  | "truncated" // Parse: the bytes end inside a field
  | "trailing-bytes" // Parse: a byte after the delete set
  | "varint-not-shortest" // Parse: a multi-byte varUint or varInt ends in 0x00
  | "int-range" // Integers: a client id, clock or clock+length too large
  | "utf8" // Strings
  | "json" // Strings: a JSON text that is not I-JSON, or a bad number
  | "json-limits" // Strings: a JSON text over the depth or member limit of 05 [STATE-7], or with U+0000
  | "any-tag" // Values: a lib0 any tag outside 116 to 127
  | "any-limits" // Values: depth over 32 or more than 1,024 members
  | "any-int-range" // Values: an integer above 2^53-1 in magnitude
  | "any-float" // Values: NaN or ±Infinity
  | "any-duplicate" // Values: a lib0 object that holds a member name twice (round 17)
  | "content-ref" // Parse: a content ref that is unknown
  | "content-json" // Parse: a ContentJSON (2)
  | "content-doc" // Parse: a ContentDoc (9)
  | "gc-skip-high-bits" // Parse: a GC or Skip info byte with a high bit set
  | "parent-info" // Parse: a parent-info number other than 0 or 1
  | "type-ref" // Parse: a ContentType with a type ref above 6
  | "empty-group" // No empty pieces
  | "zero-length" // No empty pieces
  | "group-order" // No empty pieces: client groups not strictly descending
  | "self-reference" // References
  | "cycle" // References
  | "root-name" // Roots: a root that crdt_roots does not list
  | "root-type"; // Roots: an item that does not fit its root's type

/** The reasons of an update that is a deterministic no-op for good (05 [STATE-76]). */
export type NoopReason =
  | "gap" // structs of a client start above its next clock
  | "partial" // a struct only partly below its client's next clock
  | "skip" // a Skip struct anywhere
  | "missing" // an origin, right origin, parent or deleted id that is not present
  | "non-type-parent" // a parent id that is not hidden and is not a ContentType item
  | "cross-parent" // an item whose origin and right origin are items with different parents, none hidden
  | "surrogate"; // the update would split a surrogate pair

export type Reason = RefusalReason | NoopReason;

/** A well-formedness failure of 05 [STATE-71]: the update is to be rejected with SCHEMA_INVALID. */
export class CrdtError extends Error {
  readonly reason: RefusalReason;
  readonly detail: string;

  constructor(reason: RefusalReason, detail = "") {
    super(`crdt: update is not well formed: ${reason}${detail ? `: ${detail}` : ""}`);
    this.name = "CrdtError";
    this.reason = reason;
    this.detail = detail;
  }
}

export function fail(reason: RefusalReason, detail = ""): CrdtError {
  return new CrdtError(reason, detail);
}
