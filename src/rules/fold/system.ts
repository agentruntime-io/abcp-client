// The SYSTEM records (06 §6.7): which of a record's changes the fold applies and what each kind does besides (05 [STATE-34]).

import type { KeyChange, Record as RecordMessage } from "../../gen/abpc/v2/types_pb.js";
import { ChangeKind, SystemKind } from "../../gen/abpc/v2/types_pb.js";
import { JsonV1Error } from "../jsonv1/errors.js";
import { type JsonValue, isInteger } from "../jsonv1/value.js";
import { parse } from "../jsonv1/parse.js";
import { refuse } from "./errors.js";
import { expandKeyTemplate, matchKeyTemplate, parseKeyTemplate } from "./template.js";
import type { ReplicaState } from "./state.js";

/** What a SYSTEM kind does besides applying its changes (06 §6.7, 05 [STATE-34]). */
export interface SystemEffect {
  /** The record's DELETEs leave tombstones that carry the redaction mark (a `REDACTED` record, 05 [STATE-37]). */
  mark: boolean;
  /**
   * The erased keys that the record takes: a nested marker key among them that was present just before the record gets a marked
   * entity tombstone (05 [STATE-37], 06 [LOG-44]).
   */
  markKeys: ReadonlySet<string>;
  /** For a `TEMPLATE_CONVERSION` record, gives the key that a change's key was converted from (05 [STATE-67]). */
  keep?: (c: KeyChange) => string | undefined;
  /** Runs after the record's changes. */
  finish?: (st: ReplicaState) => void;
}

/** A record's applied changes, and what its SYSTEM kind does besides. */
export interface Selection {
  /** The indexes of the changes that the record's kind applies (05 [STATE-34]). */
  applied: number[];
  /** A SYSTEM record of an unknown kind that the reader skips (06 [LOG-41]). */
  skip?: boolean;
  sys?: SystemEffect;
  /**
   * The indexes of the changes that a known SYSTEM kind carries and does not define: not applied, and reported with the rule and
   * the reason (02 [ARC-46], 06 [LOG-41]); the keyspace is not stopped (15 [COMPAT-35]).
   */
  ignored?: number[];
  ignoredRule?: string;
  ignoredWhy?: string;
}

const NO_KEYS: ReadonlySet<string> = new Set();

/** The encoding of every SYSTEM body (06 [LOG-39]). */
const BODY_ENCODING = "json/v1";

/**
 * Reads a SYSTEM record: the kinds of 06 §6.7 and their effects. SYSTEM records change keys only as the table of 06 [LOG-41]
 * says, and the fold applies the changes that a kind defines (05 [STATE-34]); the others are ignored and reported (Q13).
 */
export function selectSystem(rec: RecordMessage, all: () => number[]): Selection {
  const seq = rec.seq;
  const sys = rec.system;
  if (sys === undefined) throw refuse("malformed", seq, "LOG-39", "a SYSTEM record with no system event");
  // LOG-41: a SYSTEM record of this kind changes no key. A change that it carries nevertheless is not one that the kind defines,
  // so it is not applied (05 [STATE-34] "SYSTEM: the changes its kind defines"): it is ignored, counted and reported (02 [ARC-46]),
  // and the reader goes on (15 [COMPAT-35] does not list it).
  //
  // SPEC-QUESTION (ruled, Q13): 06 [LOG-39] table, "Changes: none" for this kind, and 05 [STATE-34] "SYSTEM: the changes its kind
  // defines". The text did not say what a reader that knows the kind does with changes that the kind does not define: (a) ignores
  // them, (b) refuses the record. Ruled by the coordinator (2026-10-31): ignore and report; do not stop. No server writes one.
  const noChanges = (what: string): Selection => {
    const idx = all();
    if (idx.length === 0) return { applied: [] };
    return { applied: [], ignored: idx, ignoredRule: "LOG-41", ignoredWhy: `a ${what} record defines no change` };
  };
  switch (sys.kind) {
    case SystemKind.MANIFEST_ACTIVATED:
      // LOG-42: no change; the version in force changes for the records after it, each of which names its own.
      return noChanges("MANIFEST_ACTIVATED");
    case SystemKind.SUBTREE_CLEANUP:
      // LOG-45, STATE-44: the physical removal of rows that the subtree deletion already hid: no logical change.
      return noChanges("SUBTREE_CLEANUP");
    case SystemKind.LIFECYCLE:
      return noChanges("LIFECYCLE");
    case SystemKind.EPOCH_ADVANCED:
      // LOG-48: the first record of a log epoch; the state takes the epoch from the record.
      return noChanges("EPOCH_ADVANCED");
    case SystemKind.FLOOR_RAISED:
      return noChanges("FLOOR_RAISED");
    case SystemKind.BLOB_DELETED:
      return noChanges("BLOB_DELETED");
    case SystemKind.PRUNE_FLOOR_RAISED:
      // LOG-80: the prune floor is a fact of the log, not of the state; no change.
      return noChanges("PRUNE_FLOOR_RAISED");
    case SystemKind.KEY_EXPIRED:
      // LOG-46: a reader applies the record's changes, the DELETEs of the named keys, whatever its cause. A change of another kind
      // is not one that LOG-41's table gives this kind: it is ignored and reported (Q13).
      return defined(rec, "KEY_EXPIRED", "DELETE", { applied: [], sys: { mark: false, markKeys: NO_KEYS } }, ChangeKind.DELETE);
    case SystemKind.TEMPLATE_CONVERSION:
      return selectConversion(rec);
    case SystemKind.REDACTED:
      return selectRedacted(rec);
    case SystemKind.TOMBSTONES_DISCARDED:
      return selectDiscard(rec, all);
    default:
      // LOG-41: a reader skips a SYSTEM record of a kind it does not know, unless the record must be understood or carries
      // changes; then LOG-11 stops it.
      if (sys.mustUnderstand || rec.changes.length > 0) {
        throw refuse("unknown-system-kind", seq, "LOG-11", `SYSTEM kind ${sys.kind} (must_understand ${sys.mustUnderstand}, ${rec.changes.length} changes)`);
      }
      return { applied: [], skip: true };
  }
}

/**
 * Completes sel with the changes of the record, as the table of 06 [LOG-41] gives them to the kind: the changes of one of the kinds
 * are applied, in the record's order, and every other change is ignored and reported, not applied (05 [STATE-34] "SYSTEM: the
 * changes its kind defines"; the Q13 ruling: ignore and report, and do not stop; 15 [COMPAT-35] does not count it as a stop). `what`
 * names the SYSTEM kind and `gives` the changes that the table gives it, for the report.
 *
 * SPEC-QUESTION (ruled, Q13): the table of 06 [LOG-41] names the changes of `KEY_EXPIRED` ("`DELETE` of every named key"),
 * `TEMPLATE_CONVERSION` ("`SET` of converted keys; `DELETE` of renamed-from keys") and `REDACTED` ("`DELETE` ...; `SET` ..."). The
 * text gives no rule for a change of another kind in such a record, an `INCR` in a conversion or a `SET` in a `KEY_EXPIRED`. The
 * ruling for a kind that defines no change covers it: the change is not one that "its kind defines". No server writes one.
 */
function defined(rec: RecordMessage, what: string, gives: string, sel: Selection, ...kinds: ChangeKind[]): Selection {
  const ignored: number[] = [];
  rec.changes.forEach((c, i) => {
    if (kinds.includes(c.kind)) sel.applied.push(i);
    else ignored.push(i);
  });
  if (ignored.length > 0) {
    sel.ignored = ignored;
    sel.ignoredRule = "LOG-41";
    sel.ignoredWhy = `a ${what} record defines ${gives} changes only`;
  }
  return sel;
}

/** Reads the body of a SYSTEM record. Readers ignore unknown members (06 [LOG-39]). */
function readBody(rec: RecordMessage): Map<string, JsonValue> {
  const sys = rec.system;
  if (sys === undefined || sys.encoding !== BODY_ENCODING) {
    throw refuse("malformed", rec.seq, "LOG-39", `SYSTEM body encoding "${sys?.encoding}"`);
  }
  let v: JsonValue;
  try {
    v = parse(sys.body);
  } catch (e) {
    if (e instanceof JsonV1Error) throw refuse("malformed", rec.seq, "LOG-39", `SYSTEM body: ${e.message}`);
    throw e;
  }
  if (!(v instanceof Map)) throw refuse("malformed", rec.seq, "LOG-39", "SYSTEM body is not an object");
  return v;
}

function stringMember(body: Map<string, JsonValue>, name: string, rec: RecordMessage): string {
  const v = body.get(name);
  if (v === undefined || v === null) return "";
  if (typeof v !== "string") throw refuse("malformed", rec.seq, "LOG-39", `SYSTEM body: ${name} is not a string`);
  return v;
}

function stringListMember(body: Map<string, JsonValue>, name: string, rec: RecordMessage): string[] {
  const v = body.get(name);
  if (v === undefined || v === null) return [];
  if (!Array.isArray(v) || v.some((x) => typeof x !== "string")) {
    throw refuse("malformed", rec.seq, "LOG-39", `SYSTEM body: ${name} is not a list of strings`);
  }
  return v as string[];
}

/**
 * Reads a `TEMPLATE_CONVERSION` record (06 [LOG-43], 04 [MAN-66]). Its SETs and DELETEs are applied (a change of another kind is
 * ignored and reported, Q13); a key that it writes keeps the changed_at and changed_by of the key that it converted from (05
 * [STATE-67]), which is the source template's key bound by the same variable values as the target key (04 [MAN-66]).
 */
function selectConversion(rec: RecordMessage): Selection {
  const body = readBody(rec);
  const fromText = stringMember(body, "from_template", rec);
  const toText = stringMember(body, "to_template", rec);
  const from = parseKeyTemplate(fromText);
  const to = parseKeyTemplate(toText);
  if (from === undefined || to === undefined) {
    throw refuse("malformed", rec.seq, "LOG-39", `TEMPLATE_CONVERSION templates "${fromText}" to "${toText}"`);
  }
  const keep = (c: KeyChange): string | undefined => {
    const b = matchKeyTemplate(to, c.key);
    return b === undefined ? undefined : expandKeyTemplate(from, b);
  };
  return defined(rec, "TEMPLATE_CONVERSION", "SET and DELETE", { applied: [], sys: { mark: false, markKeys: NO_KEYS, keep } }, ChangeKind.SET, ChangeKind.DELETE);
}

/**
 * Reads a `REDACTED` record (06 [LOG-44]). Its changes, the DELETEs of the redacted keys that were present and the SETs of run
 * keys, are applied; a change of another kind is ignored and reported (Q13). Its DELETEs leave tombstones with the redaction mark,
 * and every key of its `keys` that holds a key tombstone after it gets a reset seq (05 [STATE-37], [STATE-85]).
 */
function selectRedacted(rec: RecordMessage): Selection {
  const body = readBody(rec);
  const keys = stringListMember(body, "keys", rec);
  const erased = stringListMember(body, "erased_keys", rec);
  const markKeys = new Set<string>([...keys, ...erased]);
  const finish = (st: ReplicaState): void => {
    for (const k of keys) {
      // STATE-85: the key's version after the record: the record's seq for a key that it deleted, the conversion's seq for a
      // former path, which holds the tombstone that its conversion's DELETE gave it. Only a key tombstone carries a reset seq; a
      // key under an entity tombstone, or with no tombstone, gets none.
      const t = st.tombstone(k);
      if (t !== undefined && t.kind === "key") st.setResetSeq(k, t.version);
    }
  };
  return defined(rec, "REDACTED", "SET and DELETE", { applied: [], sys: { mark: true, markKeys, finish } }, ChangeKind.SET, ChangeKind.DELETE);
}

/**
 * Reads a `TOMBSTONES_DISCARDED` record (06 [LOG-84], 05 [STATE-83]): no change, and the tombstones at or below `floor_seq` are
 * discarded at the record. A change that it carries is ignored and reported, as LOG-41 says (Q13).
 */
function selectDiscard(rec: RecordMessage, all: () => number[]): Selection {
  const body = readBody(rec);
  const f = body.get("floor_seq");
  if (typeof f !== "number" || !isInteger(f) || f < 0 || BigInt(f) >= rec.seq) {
    // LOG-60: the floor is at or below the prune floor, which is below every later record.
    throw refuse("malformed", rec.seq, "LOG-84", "TOMBSTONES_DISCARDED floor_seq is missing or not below the record");
  }
  const floor = BigInt(f);
  const sel: Selection = { applied: [], sys: { mark: false, markKeys: NO_KEYS, finish: (st) => st.discardTombstones(floor) } };
  const idx = all();
  if (idx.length > 0) {
    sel.ignored = idx;
    sel.ignoredRule = "LOG-84";
    sel.ignoredWhy = "a TOMBSTONES_DISCARDED record defines no change";
  }
  return sel;
}
