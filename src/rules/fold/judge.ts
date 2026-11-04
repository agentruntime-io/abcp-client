// The structural judgement of a record by a client engine: 16 [SEC-3].
//
//   "A client engine MUST check every record it applies against the same structural rules. A record is structurally invalid when it decodes under 02 [ARC-61] and breaks a rule of SEC-2 (the envelope
//   rules, the segment syntax and the reserved namespace, the op type's templates, the value schemas and the limits). A client engine judges a record against the envelope rules, the segment syntax,
//   the reserved namespace and the `json/v1` limits of 05 [STATE-7] always, against the templates and value schemas of the manifest version that the record carries, and never against the app's limits
//   in force (07 §7.9): a record that the server appended is not structurally invalid for a limit that was lowered later (04 [MAN-75])."
//
// What is judged here, for each change that the record applies (`INTENT` and a `REJECTED` decision apply only the intent row; their requested changes are audit):
//   - the record's own envelope: an OP record holds a change (07 Table 7-2 item 1); `causation_id` and `correlation_id`, when set, and each `blob_refs` entry have their syntax (03 [ID-23], Table 3.1);
//   - the key: 1 to 16 segments of 1 to 64 octets of `seg-char`, none of them `.`, `..` or `__proto__`, at most 512 octets (03 [ID-1], [ID-2], [ID-13]); a key of the reserved namespace is one that 03 §3.8
//     defines (03 [ID-47]);
//   - the envelope of the change: the fields that 05 [STATE-20] requires and forbids for its kind, and a `CAS`'s `expected` (05 [STATE-24]);
//   - the value: for a `json/v1` value the limits of 05 [STATE-7] (nesting over 32, an object of over 1,024 members, U+0000) and the literal null of [STATE-15], the encoding of the template
//     (05 [STATE-6]), its `value_schema` (05 [STATE-10]), the null of 05 [STATE-14], the magnitude of an `INCR`'s `delta` (05 [STATE-27]), and, for the OP and DECISION records, the change kind that
//     the merge rule of the template allows (05 [STATE-19]). The CRDT update of a change is judged by `rules/crdt` (05 [STATE-71]).
// The template of the key under the manifest version of the record is `plan`'s (03 [ID-19]).
//
// Never judged: a limit of 07 §7.9 or 04 [MAN-19] (`max_value_bytes`, `max_action_bytes`, `max_changes_per_action`, a template's `max_bytes`), which the server judged at append and may have lowered since
// (04 [MAN-75]). A change that a redaction blanked (encoding `redacted/v1`, 06 [LOG-76]) has no value to judge, and the server-written `SET` of a run key that a `REDACTED` record carries (03 [ID-19], 06 [LOG-44])
// is a `json/v1` value of a reserved key, which passes every rule here whatever the manifest in force.
//
// SPEC-QUESTION (16 [SEC-3] with 03 [ID-47]): "a key whose first segment is reserved but is not defined by this specification or by a module that the manifest in force declares" is refused by a server.
// A client reads no module list from the manifest, and a later revision adds modules and module versions (15 [COMPAT-25]). Taken: a reserved first segment is judged by the forms of 03 §3.8 that this
// revision defines (`abpc.intent`, `abpc.lease.v1`, `abpc.saga.v1`, `abpc.escrow.v1`, each with its shapes); a first segment of the form `abpc.<module>.v<N>` that is none of them is a module of
// another revision and is not judged; any other reserved first segment is invalid. Reading B: a module that the record's manifest version does not declare makes its keys invalid too.
//
// SPEC-QUESTION (16 [SEC-3] with 14 [CLI-5], 16 [SEC-4]): CLI-5 has a change "whose value a client cannot decode locally" enter the confirmed state and be shown as undecodable, "MUST NOT stop the engine
// for such a change", while SEC-3 judges a record against "the `json/v1` limits of 05 [STATE-7]" and stops the keyspace for a record that breaks them. Taken: the limits of STATE-7 are what its text calls
// "these limits" (nesting over 32, over 1,024 members in an object, U+0000 in a string or a member name), and a value that breaks them, or the literal null of [STATE-15], is structurally invalid; any other
// defect of a `json/v1` text (not JSON, a duplicate member, a number that is no binary64 value, not in canonical form) is a value that the client cannot decode, which CLI-5 keeps. Reading B: every
// condition of STATE-7 is a rule that SEC-3 judges, and CLI-5 covers only a value that passed the server's checks and that the client cannot display.

import { ChangeKind, RecordKind, type KeyChange, type Record as RecordMessage } from "../../gen/abpc/v2/types_pb.js";
import { REDACTED_ENCODING } from "../hashes/index.js";
import { allowsNull, validate, validateBytes, MAX_SAFE_INTEGER } from "../jsonv1/index.js";
import { refuse, type FoldCode } from "./errors.js";
import { isReservedKey, type Template } from "./rules.js";
import { isSegment } from "./template.js";

const CAUSATION = /^[A-Za-z0-9._:-]{1,128}$/;
const BLOB_ID = /^[A-Za-z0-9_-]{21,64}$/;
/** `abpc.<module>.v<N>`: the first segment of a module of 03 §3.8, whichever revision defines it. */
const MODULE_SEGMENT = /^abpc\.[a-z][a-z0-9-]*(\.[a-z][a-z0-9-]*)*\.v[0-9]+$/;

const MAX_DELTA = BigInt(MAX_SAFE_INTEGER);

function bad(code: FoldCode, rec: RecordMessage, rule: string, detail: string): never {
  throw refuse(code, rec.seq, rule, detail);
}

/** 07 Table 7-2 items 1 and 9, 03 [ID-23], as a record shows them: judged once for the record, before its changes. */
export function judgeRecord(rec: RecordMessage): void {
  if (rec.kind === RecordKind.OP && rec.changes.length === 0) bad("invalid-envelope", rec, "MUT-20", "an OP record with no change (07 Table 7-2 item 1)");
  if (rec.causationId !== "" && !CAUSATION.test(rec.causationId)) bad("invalid-envelope", rec, "ID-23", "the causation_id has no valid syntax (03 Table 3.1)");
  if (rec.correlationId !== "" && !CAUSATION.test(rec.correlationId)) bad("invalid-envelope", rec, "ID-23", "the correlation_id has no valid syntax (03 Table 3.1)");
  for (const id of rec.blobRefs) {
    if (!BLOB_ID.test(id)) bad("invalid-envelope", rec, "ID-23", "a blob_refs entry has no valid syntax (03 Table 3.1)");
  }
}

/** A lease class or an escrow class (03 Table 3.1). */
const CLASS = /^[a-z][a-z0-9_]{0,31}$/;
/** A saga step name (03 Table 3.1, [ID-54]), which is also a segment. */
const STEP = /^[a-z][a-z0-9_]*(\.[a-z0-9_]+)*$/;
const MAX_UINT64 = (1n << 64n) - 1n;

/** Decimal without leading zeros, `0` included, within a uint64 (03 §3.8: `{n}` and `{seq}`). */
function isDecimal(s: string): boolean {
  if (s.length === 0 || s.length > 20 || !/^(0|[1-9][0-9]*)$/.test(s)) return false;
  return BigInt(s) <= MAX_UINT64;
}

/**
 * True when the key of the reserved namespace is one of the forms that 03 §3.8 defines in the revision that this client knows (the form of the server's `ids.ClassifyReservedKey`, but for the
 * intent id and the ids that 11 defines), or belongs to a module of another revision (see the SPEC-QUESTION above). Not judged: that the manifest in force declares the module (03 [ID-49]).
 */
export function isDefinedReservedKey(key: string): boolean {
  const s = key.split("/");
  const first = s[0] as string;
  switch (first) {
    case "abpc.intent":
      return s.length === 2;
    case "abpc.lease.v1":
      return s.length >= 3 && CLASS.test(s[1] as string);
    case "abpc.saga.v1": {
      if (s.length < 3) return false;
      if (s[1] === "moved") return true; // `abpc.saga.v1/moved/` and the segments of the entity key
      if (s[1] !== "run") return false;
      if (s.length === 3) return true;
      if (s.length === 4) return s[3] === "open" || s[3] === "resolve";
      if (s.length === 5) return (s[3] === "target" && isDecimal(s[4] as string)) || (s[3] === "step" && (s[4] as string).length <= 64 && STEP.test(s[4] as string));
      return false;
    }
    case "abpc.escrow.v1": {
      if (s.length < 3) return false;
      if (s[1] === "series") return s.length === 4 && CLASS.test(s[2] as string) && s[2] !== "series";
      if (!CLASS.test(s[1] as string)) return false;
      if (s.length === 3) return true;
      if (s.length === 4) return s[3] === "open" || s[3] === "range" || s[3] === "release";
      return s.length === 5 && s[3] === "exception" && isDecimal(s[4] as string) && s[4] !== "0";
    }
    default:
      return MODULE_SEGMENT.test(first);
  }
}

/** The key of a change: 03 [ID-1], [ID-2], [ID-13] and, in the reserved namespace, [ID-47]. */
function judgeKey(rec: RecordMessage, i: number, key: string): void {
  const where = `change ${i} names the key "${key.length > 80 ? `${key.slice(0, 80)}...` : key}"`;
  if (key === "") bad("invalid-key", rec, "ID-13", `change ${i} has no key`);
  // an octet count: every octet of a key that passes the segment syntax is one character, and one that does not is refused at its first
  if (key.length > 512) bad("invalid-key", rec, "ID-13", `${where}, over 512 octets`);
  const segments = key.split("/");
  if (segments.length > 16) bad("invalid-key", rec, "ID-13", `${where}, which has more than 16 segments`);
  for (const s of segments) {
    if (!isSegment(s)) bad("invalid-key", rec, "ID-1", `${where}, which has a segment that breaks 03 [ID-1] or [ID-2]`);
  }
  if (isReservedKey(key) && !isDefinedReservedKey(key)) bad("invalid-key", rec, "ID-47", `${where}, a reserved key that 03 §3.8 does not define`);
}

/** 05 [STATE-20]: the fields that a change of the kind requires and forbids; 05 [STATE-24]: a `CAS`'s `expected`. */
function judgeEnvelope(rec: RecordMessage, i: number, c: KeyChange, crdtKey: boolean): void {
  const set = { value: c.value.length > 0, is_null: c.isNull, encoding: c.encoding !== "", delta: c.delta !== 0n, update: c.update.length > 0, expected: c.expected !== undefined };
  const name = `change ${i} (${ChangeKind[c.kind] ?? c.kind}) of "${c.key}"`;
  const forbid = (...fields: (keyof typeof set)[]): void => {
    for (const f of fields) if (set[f]) bad("invalid-envelope", rec, "STATE-20", `${name} sets ${f}, which its kind forbids`);
  };
  const need = (...fields: (keyof typeof set)[]): void => {
    for (const f of fields) if (!set[f]) bad("invalid-envelope", rec, "STATE-20", `${name} leaves ${f} unset, which its kind requires`);
  };
  switch (c.kind) {
    case ChangeKind.DELETE:
      forbid("value", "is_null", "encoding", "delta", "update", "expected");
      return;
    case ChangeKind.SET:
      need("encoding");
      forbid("delta", "update", "expected");
      return;
    case ChangeKind.CAS: {
      need("encoding", "expected");
      forbid("delta", "update");
      const e = (c.expected as NonNullable<KeyChange["expected"]>).expect;
      if (e.case === undefined) bad("invalid-envelope", rec, "STATE-24", `${name} has an expected that sets no member`);
      if (e.case === "absent" && !e.value) bad("invalid-envelope", rec, "STATE-24", `${name} has an expected whose absent is false`);
      return;
    }
    case ChangeKind.INCR:
      forbid("value", "is_null", "encoding", "update", "expected");
      return;
    case ChangeKind.CRDT_UPDATE:
      need("update", "encoding");
      forbid("value", "is_null", "delta", "expected");
      return;
    case ChangeKind.OVERRIDE:
      if (crdtKey) {
        need("update", "encoding");
        forbid("value", "is_null", "delta", "expected");
      } else {
        need("encoding");
        forbid("delta", "update", "expected");
      }
      return;
    default:
      return; // a kind that this client does not know is `understood`'s (15 [COMPAT-35])
  }
}

/** Whether a merge rule allows the change kind (05 §5.4, [STATE-19]). */
function kindFits(kind: ChangeKind, rule: Template["rule"]): boolean {
  switch (kind) {
    case ChangeKind.SET:
      return rule === "lww";
    case ChangeKind.CAS:
      return rule === "cas";
    case ChangeKind.INCR:
      return rule === "counter";
    case ChangeKind.CRDT_UPDATE:
      return rule === "crdt";
    case ChangeKind.DELETE:
      return rule !== "crdt";
    default:
      return true; // OVERRIDE fits every rule, and a kind that this client does not know is not judged here
  }
}

/** The value of a `SET`, `CAS` or `OVERRIDE`: 05 [STATE-6], [STATE-7], [STATE-10], [STATE-14], [STATE-15]. */
function judgeValue(rec: RecordMessage, i: number, c: KeyChange, tpl: Template): void {
  const name = `change ${i} of "${c.key}"`;
  if (c.isNull) {
    if (c.value.length > 0) bad("schema-invalid", rec, "STATE-15", `${name} sets is_null with a value`);
    if (tpl.rule === "counter" || tpl.rule === "crdt") bad("schema-invalid", rec, "STATE-14", `${name} is a null for a ${tpl.rule} template`);
    if (tpl.schema !== undefined && !allowsNull(tpl.schema)) bad("schema-invalid", rec, "STATE-14", `${name} is a null for a value_schema that does not allow one`);
    return;
  }
  if (tpl.encoding !== undefined && c.encoding !== tpl.encoding) bad("schema-invalid", rec, "STATE-6", `${name} has the encoding "${c.encoding}", not its template's "${tpl.encoding}"`);
  if (c.encoding === "json/v1") {
    const err = validate(c.value, tpl.schema !== undefined && tpl.schema.type !== "bytes" ? tpl.schema : undefined);
    // a value that cannot be read at all (not JSON, a duplicate member, a number that is no binary64 value, not canonical) is one that the client cannot decode: 14 [CLI-5] keeps it, as the SPEC-QUESTION
    // above says; the limits of [STATE-7], the literal null of [STATE-15] and the value schema are the structural rules of SEC-3
    if (err !== null && (err.reason === "json-limits" || err.reason === "literal-null" || err.reason === "schema")) bad("schema-invalid", rec, "STATE-7", `${name}: ${err.message}`);
  } else if (c.encoding === "bytes/v1") {
    const err = validateBytes(c.value, tpl.schema !== undefined && tpl.schema.type === "bytes" ? tpl.schema : undefined);
    if (err !== null) bad("schema-invalid", rec, "STATE-8", `${name}: ${err.message}`);
  }
}

/**
 * Judges one change that the record applies, against the template that its key matches under the record's manifest version. `tpl` is the template, never undefined here: a key that matches none is
 * `plan`'s `no-template` (03 [ID-19]).
 */
export function judgeChange(rec: RecordMessage, i: number, c: KeyChange, tpl: Template): void {
  const reserved = isReservedKey(c.key);
  const blanked = c.encoding === REDACTED_ENCODING && c.kind !== ChangeKind.DELETE;
  if (!blanked) judgeEnvelope(rec, i, c, tpl.rule === "crdt");
  // a SYSTEM change is the server's own: a conversion's `SET` of a CRDT key holds its base (06 [LOG-40]), whatever the kind that its template's rule allows; a reserved key has the one template of
  // 03 [ID-19] for the fold, while the module that owns it writes it with the kinds of its own rules (11 [COORD-106])
  if (rec.kind !== RecordKind.SYSTEM && !reserved && !blanked && !kindFits(c.kind, tpl.rule)) {
    bad("schema-invalid", rec, "STATE-19", `change ${i} is a ${ChangeKind[c.kind] ?? c.kind} of "${c.key}", whose template's merge rule is ${tpl.rule}`);
  }
  if (blanked) return;
  switch (c.kind) {
    case ChangeKind.SET:
    case ChangeKind.CAS:
      if (!(tpl.rule === "crdt")) judgeValue(rec, i, c, tpl);
      return;
    case ChangeKind.OVERRIDE:
      if (tpl.rule !== "crdt") judgeValue(rec, i, c, tpl);
      return;
    case ChangeKind.INCR: {
      const mag = c.delta < 0n ? -c.delta : c.delta;
      if (mag > MAX_DELTA) bad("schema-invalid", rec, "STATE-27", `change ${i} of "${c.key}" has a delta of magnitude ${mag}, over 2^53-1`);
      return;
    }
    default:
      return;
  }
}

/** The key of a change, before its template is looked up. */
export function judgeChangeKey(rec: RecordMessage, i: number, key: string): void {
  judgeKey(rec, i, key);
}
