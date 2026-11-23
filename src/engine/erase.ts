// What a redaction takes out of a local copy (14 [CLI-76]), as pure functions over the messages that a copy holds: a change, a record, a page entry, an action's content,
// a saga start document. The engine (`erasure.ts`) decides which copies there are and what to commit; this file says what each is after the erasure.
//
// The blanking is the server's (06 [LOG-76]), applied to the client's copy: a change keeps `key`, `kind` and `seen_version` and the encoding `redacted/v1` (a `DELETE` keeps its
// empty encoding), every value field is cleared and the unknown fields with them; a `previous` entry keeps `key`, `version`, `changed_at` and the encoding `redacted/v1`; a page entry
// that a redaction reaches becomes a key in the redacted state (05 [STATE-82]): its version, its `changed_at`, the encoding `redacted/v1`, an empty value and the redaction mark. A
// reader folds each of these as the server's fold does, so that a replica restored from blanked pages and records is the state that the unblanked ones gave (14 [CLI-2]).

import { create } from "@bufbuild/protobuf";
import { MutationSchema, type Mutation } from "../gen/abcp/v2/sync_pb.js";
import {
  ChangeKind,
  KeyChangeSchema,
  KeyEntrySchema,
  RecordKind,
  RecordSchema,
  SystemKind,
  type KeyChange,
  type KeyEntry,
  type Record as RecordMessage,
} from "../gen/abcp/v2/types_pb.js";
import { canonical, parse, type JsonValue } from "../rules/jsonv1/index.js";
import { bytesEqual } from "../rules/bytes.js";
import { CanonError, decodeContent, encodeContent } from "./entry.js";

/** The encoding of a blanked change and of a key in the redacted state (06 [LOG-76], 05 [STATE-82]). */
export const REDACTED_ENCODING = "redacted/v1";

/** A saga's run key (11 [COORD-61]): its value holds the start document (11 [COORD-58]). */
export const isRunKey = (key: string): boolean => /^abcp\.saga\.v1\/run\/[^/]+$/.test(key);

/** The ways that a client learns of a redaction of a keyspace (14 [CLI-76]). */
export type RedactionSource = "record" | "mark" | "list";

/** One redaction of a keyspace's keys, as the client reads it. */
export interface Redaction {
  keyspaceId: string;
  /** The erased keys: `keys` and `erased_keys` of a `REDACTED` record (06 [LOG-44]), the keys of the load entries with the redaction mark, the keys of `LoadStateResponse.redactions`. */
  keys: ReadonlySet<string>;
  /** The seq of the `REDACTED` record; undefined for a mark or a listing, which name the keys and no record the client holds. */
  seq: bigint | undefined;
  /** The server-written `SET`s of run keys that the record carries: the run key and the value that holds the blanked start document (12 [ADM-65]). */
  runKeys: ReadonlyMap<string, Uint8Array>;
  /** The seqs of the Start INTENT records whose run-key `SET` the record blanks for a run key that is absent (06 [LOG-44]). */
  blankedIntents: ReadonlySet<bigint>;
  source: RedactionSource;
}

/** The redaction of a `REDACTED` record that the fold applied; undefined for any other record. */
export function readRedaction(keyspaceId: string, rec: RecordMessage): Redaction | undefined {
  if (rec.kind !== RecordKind.SYSTEM || rec.system?.kind !== SystemKind.REDACTED) return undefined;
  let body: JsonValue;
  try {
    body = parse(rec.system.body);
  } catch {
    return undefined; // the fold refuses such a record, and nothing of it is applied
  }
  if (!(body instanceof Map)) return undefined;
  const keys = new Set<string>();
  for (const name of ["keys", "erased_keys"]) {
    const v = body.get(name);
    if (Array.isArray(v)) for (const k of v) if (typeof k === "string") keys.add(k);
  }
  const blankedIntents = new Set<bigint>();
  const bi = body.get("blanked_intents");
  if (Array.isArray(bi)) for (const n of bi) if (typeof n === "number" && Number.isSafeInteger(n) && n >= 0) blankedIntents.add(BigInt(n));
  const runKeys = new Map<string, Uint8Array>();
  for (const c of rec.changes) if (c.kind === ChangeKind.SET && isRunKey(c.key)) runKeys.set(c.key, c.value);
  return { keyspaceId, keys, seq: rec.seq, runKeys, blankedIntents, source: "record" };
}

// ---------------------------------------------------------------------------------------------------------------------
// Changes and entries

const hasUnknown = (m: object): boolean => ((m as { $unknown?: unknown[] }).$unknown?.length ?? 0) > 0;

/** Whether the change holds a value (06 [LOG-44]): it is not a blanked change, and it is not a `DELETE`. A `DELETE` holds no value field. */
function holdsValue(c: KeyChange): boolean {
  if (c.kind === ChangeKind.DELETE) return false;
  return !isBlanked(c);
}

/** A change with no value field and the encoding `redacted/v1`: the form that blanking leaves (06 [LOG-76]). */
function isBlanked(c: KeyChange): boolean {
  return c.encoding === REDACTED_ENCODING && c.value.length === 0 && !c.isNull && c.delta === 0n && c.update.length === 0 && c.expected === undefined && !hasUnknown(c);
}

/** Whether blanking changes the change: it holds a value, or it is a `DELETE` with an unknown field. */
function needsBlanking(c: KeyChange): boolean {
  return c.kind === ChangeKind.DELETE ? hasUnknown(c) : !isBlanked(c);
}

/** 06 [LOG-76]: the change with every value field cleared (unknown fields included), and the encoding `redacted/v1` unless it is a `DELETE`. */
export function blankChange(c: KeyChange): KeyChange {
  return create(KeyChangeSchema, { key: c.key, kind: c.kind, seenVersion: c.seenVersion, encoding: c.kind === ChangeKind.DELETE ? "" : REDACTED_ENCODING });
}

/** 06 [LOG-76]: a `previous` entry, blanked: `key`, `version`, `changed_at` and the encoding `redacted/v1`, no mark. */
export function blankPrevious(e: KeyEntry): KeyEntry {
  return create(KeyEntrySchema, { key: e.key, version: e.version, ...(e.changedAt !== undefined ? { changedAt: e.changedAt } : {}), encoding: REDACTED_ENCODING });
}

/** 05 [STATE-82], 09 [READ-13]: a page entry that a redaction reaches is the key in the redacted state: its version and `changed_at`, an empty value, the encoding `redacted/v1` and the mark. */
export function redactedEntry(e: KeyEntry): KeyEntry {
  return create(KeyEntrySchema, {
    key: e.key,
    version: e.version,
    ...(e.changedAt !== undefined ? { changedAt: e.changedAt } : {}),
    encoding: REDACTED_ENCODING,
    redacted: true,
    crdtCoveredSeq: e.crdtCoveredSeq > 0n || e.encoding === "yjs/v1" ? e.version : 0n, // 09 [READ-14]
  });
}

/** Whether a page entry holds a value that a redaction takes: not a tombstone, not a key that is in the redacted state already. */
function entryHoldsValue(e: KeyEntry): boolean {
  return !e.deleted && !e.redacted;
}

// ---------------------------------------------------------------------------------------------------------------------
// Saga start documents (12 [ADM-65], 11 [COORD-58])

/**
 * The start document that a run-key `SET` of a `REDACTED` record holds, in the place of the one that a stored action holds: "replace the start document in every copy of the
 * `abcp.saga.v1.Start` of that run key ... with the blanked one that the `SET`'s value holds" (14 [CLI-76]). The record's `SET` is the run key's whole value, the start document's members and
 * the members that the module writes (11 [COORD-61]: `status`, `initiator`, ...); the stored Start holds the start document alone, so each member of the stored document is the record's.
 * Undefined when the stored document is already the blanked one, or when either is no JSON object.
 */
export function blankedStartDocument(stored: Uint8Array, fromRecord: Uint8Array): Uint8Array | undefined {
  let old: JsonValue;
  let rec: JsonValue;
  try {
    old = parse(stored);
    rec = parse(fromRecord);
  } catch {
    return undefined;
  }
  if (!(old instanceof Map) || !(rec instanceof Map)) return undefined;
  const out = new Map<string, JsonValue>();
  for (const name of old.keys()) {
    const v = rec.get(name);
    if (v !== undefined) out.set(name, v);
  }
  const next = canonical(out);
  return bytesEqual(next, stored) ? undefined : next;
}

// ---------------------------------------------------------------------------------------------------------------------
// An action's content (journal, dead-letter, export and rejection entries)

/** What an action or a record held, for the judgement of which blobs only erased values named (14 [CLI-76], `erasure.ts`). */
export interface Use {
  /** The blobs that it names (`blob_refs`): not a value field, so they stay with the copy (06 [LOG-71]). */
  blobRefs: readonly string[];
  /** It held a value of an erased key before the erasure: a blob that it names may be the one that value was about. */
  erasedAny: boolean;
  /** The bytes of those values (`value` or `update`), before the erasure. */
  erased: Uint8Array[];
  /** It holds a change of another key that is not a `DELETE`: a blob that it names may be that change's. */
  otherAny: boolean;
  /** The bytes of those values. */
  other: Uint8Array[];
}

const newUse = (blobRefs: readonly string[]): Use => ({ blobRefs, erasedAny: false, erased: [], otherAny: false, other: [] });
const valueBytes = (c: KeyChange): Uint8Array => (c.update.length > 0 ? c.update : c.value);

function noteErased(u: Use, c: KeyChange): void {
  if (!holdsValue(c)) return;
  u.erasedAny = true;
  const b = valueBytes(c);
  if (b.length > 0) u.erased.push(b);
}

function noteOther(u: Use, c: KeyChange): void {
  if (c.kind === ChangeKind.DELETE) return;
  u.otherAny = true;
  const b = valueBytes(c);
  if (b.length > 0) u.other.push(b);
}

/** What an action's content erasure found and did. */
export interface ContentErasure {
  /** The content after the erasure; the bytes it had when nothing was erased. */
  content: Uint8Array;
  /** The erasure changed the content. */
  changed: boolean;
  use: Use;
}

/**
 * Blanks, in the content of an action, every change of an erased key (06 [LOG-76]), and replaces the start document of a Start whose run key a `REDACTED` record set (14 [CLI-76]).
 * Undefined for a content that cannot be decoded (14 [CLI-29]): what it holds cannot be told, and the entry that holds it is left as it is.
 */
export function eraseContent(content: Uint8Array, keys: ReadonlySet<string>, runKeys: ReadonlyMap<string, Uint8Array>): ContentErasure | undefined {
  let m: Mutation;
  try {
    m = decodeContent(content);
  } catch (e) {
    if (e instanceof CanonError) return undefined;
    throw e;
  }
  let changed = false;
  const use = newUse(m.blobRefs);
  const changes = m.changes.map((c) => {
    if (keys.has(c.key)) {
      noteErased(use, c);
      if (!needsBlanking(c)) return c;
      changed = true;
      return blankChange(c);
    }
    const start = c.kind === ChangeKind.SET ? runKeys.get(c.key) : undefined;
    if (start !== undefined) {
      const next = blankedStartDocument(c.value, start);
      if (next !== undefined) {
        changed = true;
        use.erasedAny = true;
        use.erased.push(c.value);
        const blanked = create(KeyChangeSchema, { ...c, value: next });
        noteOther(use, blanked);
        return blanked;
      }
    }
    noteOther(use, c);
    return c;
  });
  if (!changed) return { content, changed: false, use };
  return { content: encodeContent(create(MutationSchema, { ...m, changes })), changed: true, use };
}

// ---------------------------------------------------------------------------------------------------------------------
// A persisted replica: the pages of a load and the records applied since (14 [CLI-2])

/** What an erasure takes out of the persisted pages and records of a keyspace. */
export interface ImageErasure {
  keys: ReadonlySet<string>;
  runKeys: ReadonlyMap<string, Uint8Array>;
  blankedIntents: ReadonlySet<bigint>;
  /** A `REDACTED` record erases the changes below it: a record at or above this seq is not blanked. Undefined: every record is (a mark or a listing names no record). */
  below?: bigint;
  /**
   * A key that the redaction did not delete, and that holds a value now, keeps the change that gave it that value (a former path that is present again, 06 [LOG-44]): true for the
   * change at `(key, seq)`, or the page entry of `key` at version `seq`, that must stay.
   */
  keeps?: (key: string, seq: bigint) => boolean;
}

/** The record after the erasure of every spec; undefined when nothing of it is erased. `use`, when given, is told what the record held. */
export function eraseRecord(rec: RecordMessage, specs: readonly ImageErasure[], use?: (u: Use) => void): RecordMessage | undefined {
  let changes: KeyChange[] | undefined;
  let previous: KeyEntry[] | undefined;
  const u = newUse(rec.blobRefs);
  rec.changes.forEach((c, i) => {
    let blank = false;
    for (const s of specs) {
      if (s.below !== undefined && rec.seq >= s.below) continue;
      const named = s.keys.has(c.key);
      const run = (s.runKeys.has(c.key) || (s.blankedIntents.has(rec.seq) && rec.kind === RecordKind.INTENT)) && c.kind === ChangeKind.SET && isRunKey(c.key);
      if (!named && !run) continue;
      if (named && s.keeps?.(c.key, rec.seq) === true) continue;
      blank = true;
      break;
    }
    if (blank) noteErased(u, c);
    else noteOther(u, c);
    if (!blank || !needsBlanking(c)) return;
    changes ??= [...rec.changes];
    changes[i] = blankChange(c);
  });
  rec.previous.forEach((p, i) => {
    for (const s of specs) {
      if (s.below !== undefined && rec.seq >= s.below) continue;
      if ((s.keys.has(p.key) && s.keeps?.(p.key, rec.seq) !== true) || s.runKeys.has(p.key)) {
        if (p.encoding === REDACTED_ENCODING && p.value.length === 0 && !p.isNull && p.changedBy === "" && !p.deleted && !p.redacted && !p.staged && p.crdtCoveredSeq === 0n && !hasUnknown(p)) return;
        previous ??= [...rec.previous];
        previous[i] = blankPrevious(p);
        return;
      }
    }
  });
  use?.(u);
  if (changes === undefined && previous === undefined) return undefined;
  return create(RecordSchema, { ...rec, changes: changes ?? rec.changes, previous: previous ?? rec.previous });
}

/** The page entry after the erasure; undefined when it holds nothing that a spec erases. */
export function erasePage(e: KeyEntry, specs: readonly ImageErasure[]): KeyEntry | undefined {
  if (!entryHoldsValue(e)) return undefined;
  for (const s of specs) {
    if (!(s.keys.has(e.key) || s.runKeys.has(e.key))) continue;
    if (s.keys.has(e.key) && s.keeps?.(e.key, e.version) === true) continue;
    return redactedEntry(e);
  }
  return undefined;
}
