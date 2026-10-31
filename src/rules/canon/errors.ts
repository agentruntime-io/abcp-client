// The refusals of 07 [MUT-45] and 02 [ARC-61] that are made on received bytes.

/** Names the rule that refused an input. */
export type CanonKind =
  /**
   * The bytes are not a protobuf message of the type (a truncated field, a wire type that proto3 has no use for, a
   * message nested deeper than MAX_DEPTH), or the text is not what the proto3 JSON mapping reads.
   */
  | "malformed"
  /** A tag, a length or a varint value of a known or an unknown field is not in its shortest form (07 [MUT-45]). */
  | "not-shortest"
  /** A string field, at any depth, is not valid UTF-8 (02 [ARC-61]). */
  | "invalid-utf8"
  /** A google.protobuf.Timestamp is out of the range of 02 [ARC-61]. */
  | "timestamp-range";

/**
 * The typed refusal of this package (02 [ARC-46]): untrusted bytes never throw anything else. A client engine that gets
 * one for a record, a frame or a response handles it as 16 [SEC-3] and 14 [CLI-50] say: it applies, stores and repairs
 * none of it, reports `protocol-error`, and stops for the keyspace.
 */
export class CanonError extends Error {
  readonly kind: CanonKind;
  /** Names the field that broke the rule, by the names of the fields from the root message, such as "changes.key". */
  readonly path: string;
  readonly detail: string;

  constructor(kind: CanonKind, path: string, detail: string) {
    super(`canon: ${describe(kind)}${path ? ` at ${path}` : ""}${detail ? `: ${detail}` : ""}`);
    this.name = "CanonError";
    this.kind = kind;
    this.path = path;
    this.detail = detail;
  }

  /**
   * Whether the refusal of a Mutation, received in the binary codec and checked as the root message, answers
   * INVALID_ENVELOPE for that mutation alone: an encoding not in its shortest form (07 [MUT-45], step 4 of Table 7-1), or an
   * authored_at out of range (02 [ARC-61]). Every other refusal fails the whole request with InvalidArgument.
   */
  get invalidEnvelope(): boolean {
    return this.kind === "not-shortest" || (this.kind === "timestamp-range" && this.path === "authored_at");
  }
}

function describe(kind: CanonKind): string {
  switch (kind) {
    case "malformed":
      return "malformed message";
    case "not-shortest":
      return "not in shortest form";
    case "invalid-utf8":
      return "string is not valid UTF-8";
    case "timestamp-range":
      return "timestamp out of range";
  }
}

export function malformed(detail: string, path = ""): CanonError {
  return new CanonError("malformed", path, detail);
}

/** Bounds the nesting of messages that the checks, the decoder and the encoder follow (02 [ARC-46]). No message of ABPC nests half as deep. */
export const MAX_DEPTH = 100;
