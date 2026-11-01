// The refusals of the fold of records (02 [ARC-26], [ARC-46], 06 [LOG-11], 15 [COMPAT-35]).

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
  /** A change of a kind that the fold does not know (15 [COMPAT-35]). A stop. */
  | "unknown-change-kind"
  /** A change carries an encoding that the fold does not know (15 [COMPAT-35]). A stop. */
  | "unknown-encoding"
  /** A DECISION record with a status that the fold does not know (15 Table 15.2: a reader stops at the record, as for an unknown record kind). A stop. */
  | "unknown-decision-status"
  /** A change names a key that no template of the record's manifest version matches (03 [ID-19]). */
  | "no-template"
  /**
   * The record is not one that a server writes: a SYSTEM body that does not read, a CRDT update that does not parse, a counter that
   * leaves its range, two applied changes to one key. Not a stop (15 [COMPAT-35]).
   */
  | "malformed";

/** The codes that 15 [COMPAT-35] and 06 [LOG-11] make a stop. */
const STOPS: ReadonlySet<FoldCode> = new Set<FoldCode>([
  "unknown-record-kind",
  "unsupported-feature",
  "unknown-system-kind",
  "unknown-change-kind",
  "unknown-encoding",
  "unknown-decision-status",
]);

/**
 * The refusal of a record, a typed refusal (02 [ARC-46]). The state is unchanged: a record is applied whole or not at all
 * (02 [ARC-28]).
 *
 * `stop` says whether the refusal is one of the cases that 15 [COMPAT-35] lists: a record that needs a later revision. A client that gets
 * such an error stops applying records for the keyspace at the previous seq and holds it as 14 [CLI-4] and 15 [COMPAT-36] say (the hold is
 * UPGRADE_REQUIRED). Any other FoldError is a structurally invalid record, one that no server writes and that the fold cannot apply: 15
 * [COMPAT-35] does not list it, so it is no case for UPGRADE_REQUIRED (16 [SEC-3]: 15 [COMPAT-36] and §15.5 are cited only for a record that
 * needs a later revision). It stops the engine for that keyspace and is reported, as 16 [SEC-3] and 02 [ARC-46] say, which the caller does:
 * the `protocol-error` event and the hold PROTOCOL_ERROR (14 [CLI-50], Table 14.1).
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

  /**
   * Whether the refusal is one of the cases of 15 [COMPAT-35]: a record kind that the fold does not know, a required feature that
   * it does not support, a SYSTEM record of an unknown kind that must be understood or carries changes (06 [LOG-11]), a change
   * kind or an encoding that it does not know, and a DECISION status that it does not know (15 Table 15.2).
   */
  get stop(): boolean {
    return STOPS.has(this.code);
  }
}

export function refuse(code: FoldCode, seq: bigint, rule: string, detail: string): FoldError {
  return new FoldError(code, seq, rule, detail);
}

/**
 * A change that a record of a known kind carries and that the fold does not apply, because its kind does not define it: the
 * changes of a known SYSTEM kind that has none, or that defines others (06 [LOG-39] table, [LOG-41]). 02 [ARC-46] has a reader
 * count and report it, and 15 [COMPAT-35] does not stop the keyspace for it.
 */
export interface Anomaly {
  /** The seq of the record. */
  seq: bigint;
  /** The change's index in the record. */
  index: number;
  /** The change's key. */
  key: string;
  /** The rule that makes the change ignored, for example "LOG-41". */
  rule: string;
  detail: string;
}
