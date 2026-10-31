// The refusals of the fold of records (02 [ARC-26], [ARC-46], 06 [LOG-11]).

/** Says why the fold refused a record. */
export type FoldCode =
  /** The record is not the next one: a gap (02 [ARC-26], 06 [LOG-2]), or no seq at all. */
  | "seq"
  /** The record's kind is none of the four (06 [LOG-11]). A stop. */
  | "unknown-record-kind"
  /** The record lists a required feature that the fold does not support (06 [LOG-11]). A stop. */
  | "unsupported-feature"
  /** A SYSTEM record of a kind that the fold does not know has `must_understand` set or carries changes (06 [LOG-11], [LOG-41]). A stop. */
  | "unknown-system-kind"
  /** A change names a key that no template of the record's manifest version matches (03 [ID-19]). */
  | "no-template"
  /**
   * The record is not one that a server writes: a change of no known kind, a SYSTEM body that does not read, a CRDT update that
   * does not parse, a counter that leaves its range, two applied changes to one key.
   */
  | "malformed";

/**
 * The refusal of a record, a typed refusal (02 [ARC-46]). The state is unchanged: a record is applied whole or not at all
 * (02 [ARC-28]). A client that gets one stops applying records for the keyspace at the previous seq and holds it as 14 [CLI-4],
 * [CLI-5] and 16 [SEC-3] say (`stop` tells the three cases that 06 [LOG-11] names: the hold is UPGRADE_REQUIRED).
 */
export class FoldError extends Error {
  readonly code: FoldCode;
  /** The seq of the refused record. */
  readonly seq: bigint;
  /** The rule that refuses it, for example "LOG-11". */
  readonly rule: string;
  readonly detail: string;

  constructor(code: FoldCode, seq: bigint, rule: string, detail: string) {
    super(`fold: record ${seq}: ${code}: ${detail} (${rule})`);
    this.name = "FoldError";
    this.code = code;
    this.seq = seq;
    this.rule = rule;
    this.detail = detail;
  }

  /** Whether the refusal is one of the three that 06 [LOG-11] names: an unknown record kind, an unsupported required feature, a SYSTEM record of an unknown kind that must be understood. */
  get stop(): boolean {
    return this.code === "unknown-record-kind" || this.code === "unsupported-feature" || this.code === "unknown-system-kind";
  }
}

export function refuse(code: FoldCode, seq: bigint, rule: string, detail: string): FoldError {
  return new FoldError(code, seq, rule, detail);
}
