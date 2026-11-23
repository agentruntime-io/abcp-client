// The fold of served records onto a client's confirmed state: 02 [ARC-26], 05 [STATE-33] to [STATE-38], [STATE-43], [STATE-45],
// [STATE-82], 06 §6.4 to §6.7, 14 [CLI-2], [CLI-4], [CLI-5]. The state of a keyspace at seq S is the result of applying, from a
// load (or from the empty state at seq 0), every record in seq order, each under the rules of the manifest version in
// `Record.manifest_version`. It is the fold that the Go server runs in its commit, in rebuilds and in checkpoints, written again.
//
// A record is applied whole or not at all (02 [ARC-28]): the fold first reads the record and settles what every change does, and
// only then changes the state, so a refusal leaves the state as it was.
//
// What the fold reads of a manifest is the small `Rules` interface. It reads no limit of the app and no gate: the commit judged them,
// and a record that the server wrote is applied as it says. It judges the record's structure as 16 [SEC-3] says (`judge.ts`): the
// envelope, the keys, the `json/v1` limits and the value schema of the manifest version that the record carries.

import {
  ChangeKind,
  DecisionStatus,
  RecordKind,
  type KeyChange,
  type Record as RecordMessage,
} from "../../gen/abcp/v2/types_pb.js";
import { CrdtError, Doc, parseUpdate, type Update } from "../crdt/index.js";
import { applyIncr, counterText, JsonV1Error, parseCounter } from "../jsonv1/index.js";
import { compareUtf8 } from "../bytes.js";
import { type Anomaly, refuse } from "./errors.js";
import { judgeChange, judgeChangeKey, judgeRecord, judgeRequestedChange } from "./judge.js";
import { type Rules, type Template, VersionTopology } from "./rules.js";
import { selectSystem, type Selection } from "./system.js";
import {
  CRDT_ENCODING,
  StateNull,
  StateRedacted,
  StateValue,
  type KeyRow,
  type ReplicaState,
  type Subtree,
} from "./state.js";
import { COUNTER_ENCODING, REDACTED_ENCODING } from "../hashes/index.js";

/** The feature ids that the fold supports (15 Table 15.1). */
export const DEFAULT_FEATURES: readonly string[] = ["escrow.v1"];

export interface FoldOptions {
  /**
   * Whether the folder supports a feature id. A record that lists one it does not support stops the fold (06 [LOG-11]). Default:
   * the features of DEFAULT_FEATURES.
   */
  supported?: (feature: string) => boolean;
  /**
   * Whether the `DELETE` of the marker key in the record leaves a staged tombstone instead of an entity tombstone (05 [STATE-45],
   * 11 [COORD-111]): the record is an action of the freeze template's own module and the entity is staged in the state before it.
   * The records alone do not tell, so the coordination milestone supplies it; absent means that no deletion leaves one.
   */
  staged?: (st: ReplicaState, rec: RecordMessage, key: string) => boolean;
  /**
   * Called with each change that a record the fold has applied carries and that its kind does not define (02 [ARC-46], 06 [LOG-41],
   * the Q13 ruling): the fold ignored it, and the caller counts and reports it. A record that the fold refuses reports none, and nor
   * does a record at or below the cursor (an overlapping delivery), which was reported when it moved the state. It runs inside
   * `applyRecord`, so it must not call back into the state.
   */
  report?: (a: Anomaly) => void;
}

/** What `applyRecord` did. */
export interface ApplyResult {
  /** The record moved the state on: its seq was the one after the state's last, so the cursor is at it now. False for a record at or below the cursor (an overlapping delivery). */
  moved: boolean;
  /** The changes that the version-gated test of 05 [STATE-38] skipped. */
  skipped: number;
}

function defaultSupported(feature: string): boolean {
  return DEFAULT_FEATURES.includes(feature);
}

/**
 * Applies the records in order, and stops at the first that `applyRecord` refuses: the refusal is thrown, and the records
 * before it are applied.
 */
export function applyRecords(st: ReplicaState, rules: Rules, recs: readonly RecordMessage[], opts: FoldOptions = {}): void {
  for (const rec of recs) applyRecord(st, rules, rec, opts);
}

/**
 * Applies one record to the state, whole or not at all (02 [ARC-28]): it first reads the record and settles what every change
 * does, and only then changes the state, so a refusal (a FoldError) leaves the state as it was. A client that gets one stops
 * applying records for the keyspace (14 [CLI-4], 16 [SEC-3]).
 *
 * The record is the one after the state's last (06 [LOG-2]), and the state is then at the record's seq, log epoch and manifest
 * version; or it is a record at or below the cursor, which a delivery that overlaps what the client holds gives (09 [READ-16]).
 * A record above the next one is a gap, which the client never infers across (14 [CLI-3]): it is refused with code `seq`.
 *
 * 05 [STATE-38]: a party that applies records onto a state it holds applies a change to key k from record seq s only if s is
 * greater than k's version (tombstones included; for a present CRDT key, the crdt_covered_seq of the base it holds) and greater
 * than the deletion seq of every deleted ancestor marker of k. A change that fails this test is skipped without error, so
 * duplicate and overlapping delivery is harmless: a counter is never counted twice (09 [READ-16]). The test reads the state as
 * it was before the record. A record at or below the cursor changes keys only by this test and does none of what a record does
 * besides: not the cursor, not a reset seq, not a tombstone floor.
 */
export function applyRecord(st: ReplicaState, rules: Rules, rec: RecordMessage, opts: FoldOptions = {}): ApplyResult {
  const seq = rec.seq;
  // ARC-26, LOG-2: seq order, no gap. (A seq of 0 is no record.)
  if (seq === 0n) throw refuse("malformed", seq, "LOG-2", "a record with seq 0");
  if (seq > st.seq + 1n) throw refuse("seq", seq, "LOG-2", `the state is at seq ${st.seq}, so the next record is ${st.seq + 1n}`);
  const isNew = seq === st.seq + 1n;
  understood(rec, opts);
  const sel = selectChanges(rec);
  if (sel.skip === true) {
    // ARC-28 holds trivially: an unknown SYSTEM kind with no change (06 [LOG-41])
    if (isNew) st.applied(seq, rec.logEpoch, rec.manifestVersion);
    return { moved: isNew, skipped: 0 };
  }
  // 16 [SEC-3]: the envelope of the record, judged before its changes (`plan` judges each change that the record applies)
  judgeRecord(rec);
  const p = plan(st, rules, rec, sel, opts, isNew);
  commit(st, rec, p);
  if (isNew) {
    st.applied(seq, rec.logEpoch, rec.manifestVersion);
    report(rec, sel, opts);
  }
  return { moved: isNew, skipped: p.skipped };
}

/** Tells `FoldOptions.report` of the changes that the record carried and the fold ignored (02 [ARC-46]). */
function report(rec: RecordMessage, sel: Selection, opts: FoldOptions): void {
  if (opts.report === undefined || sel.ignored === undefined) return;
  for (const i of sel.ignored) {
    opts.report({ seq: rec.seq, index: i, key: (rec.changes[i] as KeyChange).key, rule: sel.ignoredRule ?? "LOG-41", detail: sel.ignoredWhy ?? "" });
  }
}

/**
 * The stop of 15 [COMPAT-35] and 06 [LOG-11]: a record of unknown kind, a record with a required feature that the folder does not
 * support, a DECISION with a status that it does not know (15 Table 15.2), and a change of a kind or with an encoding that it does
 * not know. It reads every change of the record, applied or not: the reader meets them all. The SYSTEM kind that the fold does not
 * know is judged with the record's changes (`selectChanges`).
 *
 * A change of a known kind and encoding that the fold cannot apply (a counter that is no counter, a key that no template matches, a
 * CRDT update that is not a Yjs update) is not on this list: 15 [COMPAT-35] does not stop the keyspace for it, and 02 [ARC-46] has the
 * reader count and report it (`malformed`, `no-template`).
 */
function understood(rec: RecordMessage, opts: FoldOptions): void {
  switch (rec.kind) {
    case RecordKind.OP:
    case RecordKind.INTENT:
    case RecordKind.DECISION:
    case RecordKind.SYSTEM:
      break;
    default:
      // LOG-11: stop at the previous seq.
      throw refuse("unknown-record-kind", rec.seq, "LOG-11", `record kind ${rec.kind}`);
  }
  const supported = opts.supported ?? defaultSupported;
  for (const feature of rec.requiredFeatures) {
    if (!supported(feature)) {
      // LOG-11: stop at the previous seq.
      throw refuse("unsupported-feature", rec.seq, "LOG-11", `required feature "${feature}" is not supported`);
    }
  }
  if (rec.kind === RecordKind.DECISION) {
    const status = rec.decision?.status;
    if (status !== DecisionStatus.COMMITTED && status !== DecisionStatus.REJECTED) {
      // COMPAT-35 (Table 15.2, DecisionStatus): a reader stops at the record, as for an unknown record kind.
      throw refuse("unknown-decision-status", rec.seq, "COMPAT-35", `a DECISION record with status ${status ?? 0}`);
    }
  }
  rec.changes.forEach((c, i) => changeKnown(rec.seq, i, c));
}

/** The encodings of 05 Table 5.2.1. */
const KNOWN_ENCODINGS: ReadonlySet<string> = new Set<string>(["json/v1", "bytes/v1", CRDT_ENCODING, REDACTED_ENCODING]);

/**
 * The stop of 15 [COMPAT-35] for "a change kind or an encoding it does not know". The encoding is read for the kinds that carry one
 * (05 [STATE-20]); a change that carries none, and an empty encoding, name no encoding to know.
 */
function changeKnown(seq: bigint, i: number, c: KeyChange): void {
  switch (c.kind) {
    case ChangeKind.DELETE:
    case ChangeKind.INCR:
      return;
    case ChangeKind.SET:
    case ChangeKind.CAS:
    case ChangeKind.CRDT_UPDATE:
    case ChangeKind.OVERRIDE:
      if (c.encoding !== "" && !KNOWN_ENCODINGS.has(c.encoding)) {
        throw refuse("unknown-encoding", seq, "COMPAT-35", `change ${i} of key "${c.key}" has encoding "${c.encoding}"`);
      }
      return;
    default:
      throw refuse("unknown-change-kind", seq, "COMPAT-35", `change ${i} of key "${c.key}" has kind ${c.kind}`);
  }
}

/** Says which changes of the record the fold applies (05 [STATE-34], 02 §2.4), and reads the SYSTEM body. */
function selectChanges(rec: RecordMessage): Selection {
  const changes = rec.changes;
  const all = (): number[] => changes.map((_, i) => i);
  // the intent row: `abcp.intent/{mutation_id}`; for an INTENT or a DECISION the mutation id is the intent id (06 [LOG-22], [LOG-31])
  const rowOnly = (): number[] => {
    const row = `abcp.intent/${rec.mutationId}`;
    const idx: number[] = [];
    changes.forEach((c, i) => {
      if (c.key === row) idx.push(i);
    });
    return idx;
  };
  switch (rec.kind) {
    case RecordKind.OP:
      // LOG-20: every change of an OP record is applied.
      return { applied: all() };
    case RecordKind.INTENT:
      // LOG-23: only the intent row, which is the last change, is applied.
      return { applied: rowOnly() };
    case RecordKind.DECISION: {
      const status = rec.decision?.status;
      if (status === DecisionStatus.COMMITTED) {
        // LOG-32: the requested changes, the gate's effects and the intent row, all applied.
        return { applied: all() };
      }
      if (status === DecisionStatus.REJECTED) {
        // LOG-32: the intent row only; with log_intent false there is none, and nothing is applied.
        return { applied: rowOnly() };
      }
      // `understood` stops at any other status
      throw refuse("unknown-decision-status", rec.seq, "COMPAT-35", `a DECISION record with status ${status ?? 0}`);
    }
    default:
      // SYSTEM, which `understood` has let through
      return selectSystem(rec, all);
  }
}

interface Step {
  key: string;
  delete: boolean;
  /** A DELETE of a marker key. */
  subtree?: Subtree;
  /**
   * A change to a key of the subtree of a marker key that the same record deletes: a DELETE, or any other change (05 [STATE-43]). The
   * subtree deletion decides the key's state, and the step makes it absent with no tombstone.
   */
  covered: boolean;
  /** Any other change: the key it stores, and the CRDT update to integrate into its document, when it has one. */
  put?: KeyRow;
  update?: Update;
  /** `update` is a base that starts the document anew. */
  base: boolean;
}

/** A record's applied changes, settled. */
interface Plan {
  top: VersionTopology;
  steps: Step[];
  /** The record is a REDACTED record: the tombstones that its DELETEs leave carry the mark (05 [STATE-37]). */
  mark: boolean;
  /** The changes that 05 [STATE-38] skipped. */
  skipped: number;
  /** The SYSTEM kind's effect after the changes. */
  finish?: (st: ReplicaState) => void;
}

/** Reads the applied changes of the record against the state before it, and returns what to do. */
function plan(st: ReplicaState, rules: Rules, rec: RecordMessage, sel: Selection, opts: FoldOptions, isNew: boolean): Plan {
  const seq = rec.seq;
  const top = new VersionTopology(rules, rec.manifestVersion);
  const p: Plan = { top, steps: [], mark: false, skipped: 0 };
  if (sel.sys !== undefined) {
    p.mark = sel.sys.mark;
    // a record that the state has already passed (an overlapping delivery) does none of what a SYSTEM kind does besides its changes
    if (isNew) p.finish = sel.sys.finish;
  }

  // Pass 1: the shape of each applied change, the version-gated test, and the marker keys that the record deletes. The kind and the
  // encoding of every change of the record were judged by `understood` (15 [COMPAT-35]).
  interface Read {
    c: KeyChange;
    tpl: Template;
    index: number;
  }
  const reads: Read[] = [];
  const seen = new Set<string>();
  const deleted = new Set<string>(); // the marker keys that the record deletes
  const deletedList: string[] = []; // the same, in record order
  for (const i of sel.applied) {
    const c = rec.changes[i] as KeyChange;
    const key = c.key;
    // 16 [SEC-3]: the syntax of the key (03 [ID-1], [ID-2], [ID-13]) and the reserved namespace (03 [ID-47])
    judgeChangeKey(rec, i, key);
    // STATE-5, MUT-15: one action never changes a key twice, and the records of the server's own work do not either.
    if (seen.has(key)) throw refuse("malformed", seq, "STATE-5", `the record changes key "${key}" twice`);
    seen.add(key);
    const tpl = top.template(key);
    if (tpl === undefined) throw refuse("no-template", seq, "ID-19", `key "${key}" matches no template of manifest version ${rec.manifestVersion}`);
    // 16 [SEC-3]: the change against the envelope rules, and its value against the encoding and the value schema of the template of the manifest version that the record carries, and the `json/v1`
    // limits of 05 [STATE-7]; never against a limit of the app in force (07 §7.9)
    judgeChange(rec, i, c, tpl);
    // STATE-38, READ-16: a change whose seq is not above the key's version, or above the deletion seq of a deleted ancestor marker,
    // is skipped without error.
    if (seq <= st.gateVersion(top, key)) {
      p.skipped++;
      continue;
    }
    reads.push({ c, tpl, index: i });
    if (c.kind === ChangeKind.DELETE && tpl.entity) {
      deleted.add(key);
      deletedList.push(key);
    }
  }
  // R19 review, note 4 (16 [SEC-3]): the requested changes of an INTENT, and of a REJECTED decision, are not applied (06 [LOG-23], [LOG-32]); their keys and their envelopes are judged all the same
  if (rec.kind === RecordKind.INTENT || rec.kind === RecordKind.DECISION) {
    const applied = new Set(sel.applied);
    rec.changes.forEach((c, i) => {
      if (applied.has(i)) return;
      judgeRequestedChange(rec, i, c, (k) => top.template(k)); // the template is looked up after the key is judged: a key that breaks the syntax has none
    });
  }
  // A key is in the subtree of a marker key when that marker is among its ancestor markers (05 [STATE-42]), so the questions about
  // the record's own deletions are answered by the key's ancestors, a handful, and not by the record's changes: a record of 2,000
  // changes costs 2,000 chains, not 2,000 x 2,000 tests.
  const covered = (key: string): boolean => {
    // a marker key that the record deletes is an ancestor marker of the key
    if (deleted.size === 0) return false;
    return top.ancestors(key).some((a) => deleted.has(a));
  };
  // The outermost marker key that the record deletes among key and its ancestor markers.
  const outermostDeleted = (key: string): string => {
    let out = key;
    for (const a of top.ancestors(key)) if (deleted.has(a) && a.length < out.length) out = a;
    return out;
  };
  const nestedDeleted = new Map<string, string[]>(); // a deleted marker key -> the deleted marker keys of its subtree
  for (const other of deletedList) {
    for (const a of top.ancestors(other)) {
      if (!deleted.has(a)) continue;
      const list = nestedDeleted.get(a);
      if (list === undefined) nestedDeleted.set(a, [other]);
      else list.push(other);
    }
  }

  // The tombstone mark of 05 [STATE-37]: a REDACTED record's DELETEs leave marked tombstones, and the entity tombstone of a nested
  // marker key is marked when the key is one of the record's erased keys and was present just before it.
  const markNested = (key: string): boolean => sel.sys?.markKeys.has(key) === true && st.key(key) !== undefined;

  // Pass 2: each change.
  for (const r of reads) {
    const c = r.c;
    const key = c.key;
    const step: Step = { key, delete: false, covered: false, base: false };
    if (c.kind === ChangeKind.DELETE) {
      step.delete = true;
      if (r.tpl.entity) {
        // STATE-35, STATE-43, STATE-45 (S-02): a nested marker key that the same record deletes holds the tombstone that the
        // outermost deletion of the record gives its nested markers, whatever the order of the changes. Its own DELETE stays a
        // subtree deletion ([STATE-43] "because its own DELETE is a subtree deletion too", for the prefixes of [STATE-51]), but
        // its kind is the outermost one's. The server's fold does the same.
        const sub: Subtree = { root: key, markRoot: p.mark, staged: opts.staged?.(st, rec, outermostDeleted(key)) === true, nested: [] };
        // STATE-45: the nested marker keys that are present, or hold a tombstone, just before the record, and the ones that the
        // same record deletes, get an entity tombstone too.
        const nested = new Set<string>(st.nestedMarkers(top, key));
        for (const other of nestedDeleted.get(key) ?? []) nested.add(other);
        for (const n of [...nested].sort(compareUtf8)) sub.nested.push({ key: n, mark: markNested(n) });
        step.subtree = sub;
      } else if (covered(key)) {
        step.covered = true;
      }
      p.steps.push(step);
      continue;
    }
    // STATE-43, STATE-35, MUT-16: a change other than a DELETE to a key of the subtree of a marker key that the same record deletes. No
    // server writes one (07 [MUT-16] refuses the action), but a record of one is no malformed record: the subtree deletion decides the
    // key's state "whatever the order of the changes", so the key is absent at the record's seq with no tombstone of its own, as for a
    // covered DELETE, and its value is not read. 15 [COMPAT-35] does not stop a reader for it, and the change counts as applied for
    // the prefixes ([STATE-51]), as a covered DELETE does.
    //
    // SPEC-QUESTION (ruled, Q12): 05 [STATE-43] "When the same record also changes other keys of the subtree, the subtree deletion
    // decides their state at S, whatever the order of the changes in the record", with 07 [MUT-16]. The two readings were (a) any
    // change to a key of the subtree is overruled, and (b) the sentence is about DELETEs only, as the reference models read it (they
    // apply a SET by index order, so it survives when it follows the marker's DELETE). Ruled by the coordinator (2026-10-31): reading
    // (a). The reference models differ; no vector holds such a record.
    if (covered(key)) {
      step.delete = true;
      step.covered = true;
      p.steps.push(step);
      continue;
    }
    planPut(st, rec, sel, r.tpl, c, step);
    p.steps.push(step);
  }
  return p;
}

/** Settles the key that a change other than a DELETE stores (05 [STATE-22], [STATE-26] to [STATE-32], [STATE-82]). */
function planPut(st: ReplicaState, rec: RecordMessage, sel: Selection, tpl: Template, c: KeyChange, s: Step): void {
  const seq = rec.seq;
  const key = c.key;
  const t = rec.serverTime;
  const k: KeyRow = {
    state: StateValue,
    rule: tpl.rule,
    encoding: "",
    value: new Uint8Array(0),
    doc: null,
    version: seq,
    crdtCoveredSeq: 0n,
    changedAt: { seconds: t?.seconds ?? 0n, nanos: Math.max(t?.nanos ?? 0, 0) },
    changedBy: rec.principal?.sub ?? "",
    overrideSeq: 0n,
    resetSeq: 0n,
  };
  // STATE-60, STATE-85: the key keeps its override seq and reset seq; an OVERRIDE sets the override seq.
  const seqs = st.seqs(key);
  k.overrideSeq = seqs.overrideSeq;
  k.resetSeq = seqs.resetSeq;
  const kind = c.kind;
  if (kind === ChangeKind.OVERRIDE) k.overrideSeq = seq;
  const old = st.key(key);
  if (c.encoding === REDACTED_ENCODING) {
    // STATE-82: a blanked change, of any kind but DELETE, puts the key in the redacted state: an empty value, the version and
    // changed_at that the change gives it, and an empty changed_by.
    //
    // SPEC-QUESTION (ruled, Q11: the text decides, and the code follows it): 05 [STATE-82] (SPEC-QUESTIONS Q11) "Every party that folds records MUST apply a blanked change of any kind but
    // DELETE by putting the key in the redacted state". Both reference models test the change's kind before its encoding: a
    // blanked INCR is applied as an INCR of 0 and a blanked CRDT_UPDATE as an update of no bytes. No vector tells the readings
    // apart: every key that a blanked INCR or update names is deleted by the REDACTED record that follows. The code follows the text.
    k.state = StateRedacted;
    k.encoding = REDACTED_ENCODING;
    k.changedBy = "";
    if (tpl.rule === "crdt") k.crdtCoveredSeq = seq; // 09 [READ-14]: a CRDT key in the redacted state has crdt_covered_seq = version
  } else if (kind === ChangeKind.INCR) {
    // STATE-27: an INCR adds delta to the counter; on an absent key, a null key and a key in the redacted state it starts from 0
    // (STATE-82). The commit judged the template's range, so only the range of every counter is read.
    let cur: bigint | null = null;
    if (old !== undefined && old.state === StateValue) {
      try {
        cur = parseCounter(old.value);
      } catch (e) {
        if (e instanceof JsonV1Error) throw refuse("malformed", seq, "STATE-26", `INCR of key "${key}", which holds a value that is not a counter: ${e.message}`);
        throw e;
      }
    }
    let n: bigint;
    try {
      n = applyIncr(cur, c.delta);
    } catch (e) {
      if (e instanceof JsonV1Error) throw refuse("malformed", seq, "STATE-28", `INCR of key "${key}" by ${c.delta}: ${e.message}`);
      throw e;
    }
    k.state = StateValue;
    k.value = counterText(n);
    k.encoding = COUNTER_ENCODING;
  } else if (kind === ChangeKind.CRDT_UPDATE || (kind === ChangeKind.OVERRIDE && tpl.rule === "crdt")) {
    // STATE-31, STATE-32: an update, and the update of an OVERRIDE of a CRDT key, which is a normal CRDT transaction.
    s.update = parseCrdt(c.update, seq, key, "STATE-71", "update");
    k.state = StateValue;
    k.encoding = CRDT_ENCODING;
    if (old !== undefined && old.state === StateValue && old.doc !== null) {
      k.crdtCoveredSeq = old.crdtCoveredSeq;
      // an update that the state already passed (STATE-38 lets a CRDT key's records above its base through, and folds them to
      // no effect) never takes the key's version back
      if (old.version > seq) k.version = old.version;
    }
  } else if (kind === ChangeKind.SET && tpl.rule === "crdt" && !c.isNull) {
    // LOG-40: a SYSTEM SET of a CRDT key holds a base; it is the first of the key's integrated updates (STATE-41).
    s.update = parseCrdt(c.value, seq, key, "LOG-40", "base");
    s.base = true;
    k.state = StateValue;
    k.encoding = CRDT_ENCODING;
    k.crdtCoveredSeq = seq; // STATE-74: a verified base at crdt_covered_seq = the record's seq
  } else if (c.isNull) {
    // STATE-14: an explicit null is a present key with an empty value.
    k.state = StateNull;
    k.encoding = c.encoding;
  } else {
    // STATE-3: the value bytes exactly as the record gave them.
    k.state = StateValue;
    k.encoding = c.encoding;
    k.value = c.value.slice();
  }
  // STATE-67: a conversion keeps the changed_at and changed_by of the key that it converted from.
  //
  // STATE-82 with STATE-67: a blanked conversion SET puts the key in the redacted state with "the version and changed_at the change
  // would otherwise have given it", which for a conversion is the source key's changed_at, so the metadata hash (05 [STATE-84] covers
  // changed_at) through the blanked records equals the one of the rewritten checkpoint (06 [LOG-82]). Its changed_by stays empty: a key
  // in the redacted state has none (STATE-82, STATE-69).
  if (sel.sys?.keep !== undefined) {
    const src = sel.sys.keep(c);
    if (src !== undefined) {
      const o = st.key(src);
      if (o !== undefined) {
        k.changedAt = o.changedAt;
        if (k.state !== StateRedacted) k.changedBy = o.changedBy;
      }
    }
  }
  s.put = k;
}

function parseCrdt(b: Uint8Array, seq: bigint, key: string, rule: string, what: string): Update {
  try {
    return parseUpdate(b);
  } catch (e) {
    if (e instanceof CrdtError) throw refuse("malformed", seq, rule, `${what} of key "${key}": ${e.message}`);
    throw e;
  }
}

/** Changes the state. Everything that can fail was settled by `plan`, so it does not. */
function commit(st: ReplicaState, rec: RecordMessage, p: Plan): void {
  const seq = rec.seq;
  for (const s of p.steps) {
    if (s.subtree !== undefined) {
      // STATE-43, STATE-45: a subtree deletion decides the state of every key of its subtree at seq, whatever the order of the changes.
      st.deleteSubtree(p.top, seq, s.subtree);
    } else if (s.delete && s.covered) {
      // STATE-37, STATE-43: the key is absent at seq with no tombstone of its own, and the subtree deletion of the same record decides
      // it, "whatever the order of the changes in the record". `deleteSubtree` has removed, or will remove, every row and tombstone of
      // the subtree but the entity tombstones that it writes, so the step changes nothing but the prefixes (below). A `dropKey` here
      // would take the entity tombstone that the deletion gave a nested marker key, and only when the change came after the DELETE
      // (verifier V6, MUST-B).
    } else if (s.delete) {
      // STATE-17, STATE-37: a DELETE of the key itself, of an absent key too, leaves a key tombstone at seq.
      st.deleteKey(s.key, seq, p.mark);
    } else {
      const k = s.put as KeyRow;
      if (s.update !== undefined) {
        // STATE-31, STATE-76: the key's document after this update; a base, a record's SET, and an update after a key that is
        // absent, null or in the redacted state start a document anew (STATE-41, STATE-82).
        //
        // 02 [ARC-28], `ReplicaState.key`: the Doc that the state holds is shared with every reader of the row, so the update is applied to a
        // clone, which costs the key's clients and not its size, and the Doc that was read stays as it was.
        let doc: Doc | null = null;
        if (!s.base) {
          const old = st.key(s.key);
          if (old !== undefined && old.state === StateValue && old.doc !== null) doc = old.doc.clone();
        }
        if (doc === null) doc = new Doc();
        doc.applyUpdate(s.update);
        k.doc = doc;
      }
      st.putKey(s.key, k);
    }
    // STATE-51: a change that a record applies sets the version of every proper ancestor prefix of its key.
    st.touchPrefixes(s.key, seq);
  }
  p.finish?.(st);
}
