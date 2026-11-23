// The confirmed state of one keyspace on a client (14 [CLI-1], 05 §5.5): for every key its value state, value, version,
// `changed_at`, `changed_by`, override seq and reset seq; the key and entity tombstones it has learned; the tombstone floor;
// its cursor (the seq and log epoch of the last record applied) and the manifest version of that record.
//
// It is a plain data structure with a read interface and the few write primitives that the fold needs. It decides nothing
// about records: which changes a record applies, what a change does to a key, which keys a subtree deletion reaches under a
// manifest version. The fold decides those, and calls the primitives here.
//
// Hidden subtrees. 05 [STATE-43] hides the subtree of a deleted marker key logically, at once, and 05 [STATE-44] has
// `SUBTREE_CLEANUP` records remove its rows physically later. A client has no use for the delay: a subtree deletion removes
// the rows of the subtree in the same call, so a `SUBTREE_CLEANUP` record changes nothing here. Nothing a reader or a hash
// sees depends on the choice: 05 [STATE-84] makes the hidden rows no entries of the metadata hash, and they are no present
// keys of the keyspace state hash.
//
// Prefix versions (05 [STATE-51]) are the server's: a client's confirmed state needs none (14 [CLI-1] lists what it keeps), so
// they are kept only when asked for (`trackPrefixes`), which the tests do to reach the metadata hash of 05 [STATE-84] and so
// check every tombstone, override seq and reset seq of the fold against the vectors.

import { compareUtf8 } from "../bytes.js";
import { Doc } from "../crdt/index.js";
import {
  COUNTER_ENCODING,
  EntryKind,
  keyStateHash,
  keyspaceStateHash,
  metadataHash,
  REDACTED_ENCODING,
  StateNull,
  StateRedacted,
  StateValue,
  type KeyState,
  type KeyValueState,
  type MetaEntry,
  type PrefixVersion,
  type Time,
} from "../hashes/index.js";
import { inSubtree, type Rule, type Topology } from "./rules.js";

export { StateNull, StateRedacted, StateValue };
export type { KeyValueState, Rule, Time };

/** The encoding of a CRDT key's stored value, which is a base (05 [STATE-20], [STATE-41]). */
export const CRDT_ENCODING = "yjs/v1";

/** A present key (05 [STATE-33]). Treat a row that the state returns as read-only; the fold replaces rows with `putKey`. */
export interface KeyRow {
  /** The value state: a value, an explicit null (05 [STATE-14]) or the redacted state (05 [STATE-82]). */
  state: KeyValueState;
  /** The merge rule of the key's template when the key was written. */
  rule: Rule;
  /**
   * The stored encoding: the change's, except `json/v1` for a counter and `yjs/v1` for a CRDT key, and `redacted/v1` for a key in
   * the redacted state.
   */
  encoding: string;
  /** The stored value bytes, exactly as the record gave them (05 [STATE-3]). Empty for null and for the redacted state, and for a CRDT key, whose value is `doc`. */
  value: Uint8Array;
  /** The integrated updates of a CRDT key that holds a value (05 [STATE-31], [STATE-41]); null for any other. */
  doc: Doc | null;
  /** The seq of the last record that changed the key (05 [STATE-36]). */
  version: bigint;
  /**
   * For a CRDT key that holds a value: the seq that the base it holds covers (05 [STATE-73], [STATE-75]); 0 for a key with no base.
   * Records at or below it are already in the base, so 05 [STATE-38] skips them.
   */
  crdtCoveredSeq: bigint;
  /** The server_time of the record that last changed the key (05 [STATE-66]); the converted-from key's after a conversion ([STATE-67]). */
  changedAt: Time;
  /** That record's principal.sub; empty in the redacted state (05 [STATE-69]). */
  changedBy: string;
  /** The seq of the last OVERRIDE that changed the key, and the reset seq that a redaction gave it; 0n when the key holds none (05 [STATE-60], [STATE-85]). */
  overrideSeq: bigint;
  resetSeq: bigint;
}

/** The kind of a retained tombstone (05 [STATE-37], [STATE-45]). */
export type TombstoneKind =
  /** The tombstone of a key other than a marker key, or of a marker key that no subtree deletion deleted. */
  | "key"
  /** The tombstone that a subtree deletion leaves a marker key (05 [STATE-45]); it covers the subtree and blocks the marker's re-creation (05 [STATE-47]). */
  | "entity"
  /** The tombstone of a marker key of a staged entity (05 [STATE-45]); it covers the subtree and blocks nothing. */
  | "staged";

/** A retained tombstone (05 [STATE-37]). */
export interface Tombstone {
  kind: TombstoneKind;
  /** The deletion seq: the version of the key (05 [STATE-36]). */
  version: bigint;
  /** The redaction mark of 05 [STATE-37]. */
  mark: boolean;
  /** The key's override seq and reset seq; only a key tombstone holds them (05 [STATE-43], [STATE-45], [STATE-60], [STATE-85]). */
  overrideSeq: bigint;
  resetSeq: bigint;
}

/** Whether the tombstone covers the keys of its marker's subtree (05 [STATE-36]). */
export function covers(t: Tombstone): boolean {
  return t.kind === "entity" || t.kind === "staged";
}

/** A nested marker key of a deleted subtree that gets an entity tombstone (05 [STATE-45]). */
export interface NestedMarker {
  key: string;
  /**
   * The new tombstone carries the redaction mark, because the key is an erased key of the `REDACTED` record that deletes the
   * subtree and was present just before it (05 [STATE-37]). A mark that the key's tombstone carried before is kept whatever this says.
   */
  mark: boolean;
}

/** One subtree deletion of a record (05 [STATE-43], [STATE-45]). */
export interface Subtree {
  /** The marker key whose `DELETE` it is. */
  root: string;
  /** The root's tombstone carries the redaction mark (a `DELETE` of a `REDACTED` record, 05 [STATE-37]). */
  markRoot: boolean;
  /** The deletion leaves staged tombstones instead of entity tombstones (05 [STATE-45], 11 [COORD-111]). */
  staged: boolean;
  /**
   * The nested marker keys of the subtree that are present, or hold a tombstone, just before the record, and those that the same
   * record deletes: the keys that get a tombstone besides `root`. `nestedMarkers` lists the first.
   */
  nested: NestedMarker[];
}

export interface StateOptions {
  /** Keep the version of every key prefix (05 [STATE-51]), which only the metadata hash reads. Default false. */
  trackPrefixes?: boolean;
}

/**
 * The names of one of the state's maps in an order that makes a subtree a range, so that the keys of a subtree, which all start with
 * `root/`, are read and removed at the cost of the subtree and a search, not of the keyspace (05 [STATE-17]: a `DELETE` of an absent marker
 * key is a valid subtree deletion, and a record can hold 2,000 of them; scanning every name for each of them took 3.6 s on 300,000 keys in
 * the Go server before its keys went into an ordered index, V1 MUST-3). The order is that of the UTF-16 code units, which any prefix is a
 * range of; the listings that are hashed keep their own order, the bytes of UTF-8 (`keyNames`).
 *
 * The map is the truth and the index follows it: a name that was added since the last range query waits in `pending` and is merged in
 * by the next one, a name that left the map stays in `sorted` until a query skips it or the index is rebuilt, and a query reads only
 * the names that the map still holds. A query costs the range, a search, and the merge of what was added since the last one, which is
 * never more than the scan that it replaces.
 */
class NameIndex {
  private sorted: string[] = [];
  private pending: string[] = [];
  private stale = 0;

  constructor(private readonly live: ReadonlyMap<string, unknown>) {}

  /** A name that was not in the map has been added to it. */
  added(name: string): void {
    this.pending.push(name);
  }

  /** A name that was in the map has been removed from it. */
  removed(): void {
    this.stale++;
  }

  private settle(): void {
    if (this.stale > Math.max(256, this.sorted.length >> 1)) {
      // most of the index is names that left the map: rebuild it from the map
      this.sorted = [...this.live.keys()].sort();
      this.pending = [];
      this.stale = 0;
      return;
    }
    if (this.pending.length === 0) return;
    const add = [...new Set(this.pending)].sort();
    this.pending = [];
    const out: string[] = [];
    let i = 0;
    let j = 0;
    while (i < this.sorted.length || j < add.length) {
      const a = this.sorted[i];
      const b = add[j];
      if (b === undefined || (a !== undefined && a < b)) {
        out.push(a as string);
        i++;
      } else if (a === undefined || b < a) {
        out.push(b);
        j++;
      } else {
        out.push(a); // a name that is in both: it left the map and came back
        i++;
        j++;
      }
    }
    this.sorted = out;
  }

  /** The index of the first name that is not below `prefix`. */
  private lowerBound(prefix: string): number {
    let lo = 0;
    let hi = this.sorted.length;
    while (lo < hi) {
      const m = (lo + hi) >>> 1;
      if ((this.sorted[m] as string) < prefix) lo = m + 1;
      else hi = m;
    }
    return lo;
  }

  /** The names that the map holds and that start with `prefix`. */
  withPrefix(prefix: string): string[] {
    this.settle();
    const out: string[] = [];
    for (let i = this.lowerBound(prefix); i < this.sorted.length; i++) {
      const n = this.sorted[i] as string;
      if (!n.startsWith(prefix)) break;
      if (this.live.has(n)) out.push(n);
    }
    return out;
  }

  /** Whether the map holds a name that starts with `prefix`. */
  hasPrefix(prefix: string): boolean {
    this.settle();
    for (let i = this.lowerBound(prefix); i < this.sorted.length; i++) {
      const n = this.sorted[i] as string;
      if (!n.startsWith(prefix)) return false;
      if (this.live.has(n)) return true;
    }
    return false;
  }
}

/** The confirmed state of a keyspace at a seq (05 [STATE-33], 14 [CLI-1]). */
export class ReplicaState {
  private readonly keys = new Map<string, KeyRow>();
  private readonly tombs = new Map<string, Tombstone>();
  private readonly prefixes: Map<string, bigint> | null;
  // the ordered indexes of the three maps' names (see NameIndex); every write of a map goes through the helpers below
  private readonly keyIdx = new NameIndex(this.keys);
  private readonly tombIdx = new NameIndex(this.tombs);
  private readonly prefixIdx: NameIndex | null;
  private tombFloor = 0n;
  private seq_ = 0n;
  private logEpoch_ = 0;
  private manifest_ = 0;

  constructor(options: StateOptions = {}) {
    this.prefixes = options.trackPrefixes === true ? new Map() : null;
    this.prefixIdx = this.prefixes === null ? null : new NameIndex(this.prefixes);
  }

  // ---- the writers of the three maps, which keep the indexes in step; no other code of the class writes a map ----

  private setKeyRow(key: string, row: KeyRow): void {
    if (!this.keys.has(key)) this.keyIdx.added(key);
    this.keys.set(key, row);
  }

  private delKeyRow(key: string): void {
    if (this.keys.delete(key)) this.keyIdx.removed();
  }

  private setTomb(key: string, t: Tombstone): void {
    if (!this.tombs.has(key)) this.tombIdx.added(key);
    this.tombs.set(key, t);
  }

  private delTomb(key: string): void {
    if (this.tombs.delete(key)) this.tombIdx.removed();
  }

  private setPrefix(prefix: string, version: bigint): void {
    const m = this.prefixes as Map<string, bigint>;
    if (!m.has(prefix)) (this.prefixIdx as NameIndex).added(prefix);
    m.set(prefix, version);
  }

  private delPrefix(prefix: string): void {
    if ((this.prefixes as Map<string, bigint>).delete(prefix)) (this.prefixIdx as NameIndex).removed();
  }

  /** The seq of the last record applied: the cursor. 0 for a keyspace with none (05 [STATE-39]). */
  get seq(): bigint {
    return this.seq_;
  }

  /** The log epoch of the last record applied; 0 before the first. */
  get logEpoch(): number {
    return this.logEpoch_;
  }

  /** The manifest version of the last record applied, the keyspace's effective version (06 [LOG-8]); 0 before the first. */
  get manifestVersion(): number {
    return this.manifest_;
  }

  /** The tombstone floor: the `floor_seq` of the last `TOMBSTONES_DISCARDED` record (05 [STATE-83], 06 [LOG-84]); 0 for a keyspace that has none. */
  get tombstoneFloor(): bigint {
    return this.tombFloor;
  }

  /** The number of present keys. */
  get size(): number {
    return this.keys.size;
  }

  /** Notes that the record at seq, in logEpoch, under manifestVersion, has been applied. The fold calls it once per record, after the record's changes. */
  applied(seq: bigint, logEpoch: number, manifestVersion: number): void {
    this.seq_ = seq;
    this.logEpoch_ = logEpoch;
    this.manifest_ = manifestVersion;
  }

  /** Sets the cursor without applying a record, as a load does (09 [READ-15]). */
  setCursor(seq: bigint, logEpoch: number, manifestVersion: number): void {
    this.applied(seq, logEpoch, manifestVersion);
  }

  /** The present key, or undefined when the key is absent (05 [STATE-2]). A key in the redacted state is present. */
  key(key: string): Readonly<KeyRow> | undefined {
    return this.keys.get(key);
  }

  /** The retained tombstone of the key, or undefined when it holds none. */
  tombstone(key: string): Readonly<Tombstone> | undefined {
    return this.tombs.get(key);
  }

  /** The key's override seq and reset seq: those of the present key, or of its key tombstone; 0n, 0n for a key that holds neither (05 [STATE-60], [STATE-85]). */
  seqs(key: string): { overrideSeq: bigint; resetSeq: bigint } {
    const k = this.keys.get(key);
    if (k !== undefined) return { overrideSeq: k.overrideSeq, resetSeq: k.resetSeq };
    const t = this.tombs.get(key);
    if (t !== undefined && t.kind === "key") return { overrideSeq: t.overrideSeq, resetSeq: t.resetSeq };
    return { overrideSeq: 0n, resetSeq: 0n };
  }

  /** The version of a key prefix (05 [STATE-51]), when the state tracks them; 0n otherwise. */
  prefixVersion(prefix: string): bigint {
    return this.prefixes?.get(prefix) ?? 0n;
  }

  /** The present keys in ascending byte order. */
  keyNames(): string[] {
    return [...this.keys.keys()].sort(compareUtf8);
  }

  /** The keys that hold a retained tombstone, in ascending byte order. */
  tombstoneKeys(): string[] {
    return [...this.tombs.keys()].sort(compareUtf8);
  }

  /**
   * The version of a key whether it is present or not (05 [STATE-36]): its own, a present key's version or its key tombstone's;
   * otherwise the seq of the last subtree deletion that covers it, for as long as the marker key of that deletion holds the
   * tombstone that it left; the higher of the two for a key that holds a tombstone of its own under a covering one. A key with
   * neither has version 0: one never changed, one whose tombstone a tombstone floor discarded, and one whose covering
   * tombstone ended because its marker became present again.
   */
  effectiveVersion(top: Topology, key: string): bigint {
    const k = this.keys.get(key);
    if (k !== undefined) return k.version; // a present key is not covered: a change that makes it present came after any deletion
    let v = this.tombs.get(key)?.version ?? 0n;
    for (const a of top.ancestors(key)) {
      const t = this.tombs.get(a);
      if (t !== undefined && covers(t) && t.version > v) v = t.version;
    }
    return v;
  }

  /**
   * The version that 05 [STATE-38] compares a change's seq with: the key's version, tombstones included, and the deletion seq of
   * every deleted ancestor marker ({@link effectiveVersion}); for a present CRDT key that holds a base, the `crdt_covered_seq` of
   * the base it holds.
   *
   * SPEC-QUESTION (S-01, A2-S2): 05 [STATE-38] "MUST apply a change to key k from record seq s only if s is greater than k's
   * version (tombstones included; for a present CRDT key, the `crdt_covered_seq` of the base it holds, [STATE-75])". Reading (a):
   * for a present CRDT key the test is against `crdt_covered_seq` alone. A key that the fold built from records has covered 0
   * until a SYSTEM `SET` gives it a base, so under (a) every older record passes: a redelivered update of an expired document
   * is integrated into the new one, a redelivered DELETE deletes the key, and a redelivered OVERRIDE moves its attribution back
   * (09 [READ-16] overlapping delivery). Reading (b): the parenthesis adds a floor and does not replace the version, so the test
   * is against max(version, crdt_covered_seq). The code takes (b): a load gives `crdt_covered_seq` equal to the version
   * ([STATE-75], 09 [READ-14]), so nothing that must apply is skipped, and records at or below the key's version are already in
   * it.
   */
  gateVersion(top: Topology, key: string): bigint {
    const k = this.keys.get(key);
    if (k !== undefined && k.rule === "crdt" && k.state === StateValue && k.doc !== null) {
      return k.crdtCoveredSeq > k.version ? k.crdtCoveredSeq : k.version;
    }
    return this.effectiveVersion(top, key);
  }

  // ---- the hashes of 05 [STATE-41] and [STATE-84] ----

  /** The key as its per-key state hash sees it (05 [STATE-41]), or undefined when the key is absent. */
  keyState(key: string): KeyState | undefined {
    const k = this.keys.get(key);
    return k === undefined ? undefined : toKeyState(key, k);
  }

  /** The per-key state hash of a present key (05 [STATE-41]), or undefined when the key is absent. */
  keyStateHash(key: string): Uint8Array | undefined {
    const ks = this.keyState(key);
    return ks === undefined ? undefined : keyStateHash(ks);
  }

  /** Every present key as its per-key state hash sees it, in ascending byte order of the keys. */
  keyStates(): KeyState[] {
    return this.keyNames().map((key) => toKeyState(key, this.keys.get(key) as KeyRow));
  }

  /** The keyspace state hash (05 [STATE-41]) over every present key, the reserved `abcp.*` keys included. */
  keyspaceStateHash(): Uint8Array {
    return keyspaceStateHash(this.keyStates());
  }

  /** The entries of K in the metadata hash (05 [STATE-84]): every present key and every retained tombstone. */
  metaEntries(): MetaEntry[] {
    const out: MetaEntry[] = [];
    for (const [key, k] of this.keys) {
      out.push({
        key,
        kind: EntryKind.Present,
        version: k.version,
        overrideSeq: k.overrideSeq,
        resetSeq: k.resetSeq,
        changedAt: k.changedAt,
      });
    }
    for (const [key, t] of this.tombs) {
      const e: MetaEntry = {
        key,
        kind: t.kind === "key" ? EntryKind.KeyTombstone : t.kind === "entity" ? EntryKind.EntityTombstone : EntryKind.StagedTombstone,
        mark: t.mark,
        version: t.version,
      };
      if (t.kind === "key") {
        e.overrideSeq = t.overrideSeq;
        e.resetSeq = t.resetSeq;
      }
      out.push(e);
    }
    return out;
  }

  /** The prefix versions (05 [STATE-51]); empty when the state does not track them. */
  prefixVersions(): PrefixVersion[] {
    return this.prefixes === null ? [] : [...this.prefixes.entries()].map(([prefix, version]) => ({ prefix, version }));
  }

  /** The metadata hash (05 [STATE-84]). It equals the server's only when the state tracks prefix versions. */
  metadataHash(): Uint8Array {
    return metadataHash(this.metaEntries(), this.prefixVersions());
  }

  // ---- the write primitives of the fold: each is one rule of 05 §5.5 to §5.7 and nothing more ----

  /**
   * Stores a present key, whatever it was before: present, or absent with or without a tombstone. Its own tombstone is dropped,
   * since the key is present again (05 [STATE-37]). The key's override seq and reset seq are those of the row; the caller takes
   * them from `seqs` when the change keeps them (05 [STATE-60], [STATE-85]).
   */
  putKey(key: string, row: KeyRow): void {
    this.setKeyRow(key, row);
    this.delTomb(key);
  }

  /** Stores a tombstone for an absent key, and makes sure that no present key holds the name. It serves a loader of tombstone entries. */
  putTombstone(key: string, t: Tombstone): void {
    this.delKeyRow(key);
    this.setTomb(key, t);
  }

  /** Sets the tombstone floor without discarding anything; `discardTombstones` is the rule of 05 [STATE-83]. It serves a loader. */
  setTombstoneFloor(floor: bigint): void {
    this.tombFloor = floor;
  }

  /** Sets the version of a prefix; a version of 0 removes it (05 [STATE-51]). A state that does not track prefixes ignores it. */
  setPrefixVersion(prefix: string, version: bigint): void {
    if (this.prefixes === null) return;
    if (version === 0n) this.delPrefix(prefix);
    else this.setPrefix(prefix, version);
  }

  /**
   * Makes the key absent by a `DELETE` of the key itself at seq (05 [STATE-17], [STATE-37]), the `DELETE` of a key that is absent
   * included: the key gets a key tombstone at seq, a later one moving it. Its override seq and reset seq stay with the tombstone
   * (05 [STATE-60], [STATE-85]). With mark, the tombstone carries the redaction mark, and it keeps a mark that it carried
   * before, whatever mark says (05 [STATE-37]). A marker key is deleted by `deleteSubtree`.
   */
  deleteKey(key: string, seq: bigint, mark: boolean): void {
    const t: Tombstone = { kind: "key", version: seq, mark, overrideSeq: 0n, resetSeq: 0n };
    const k = this.keys.get(key);
    if (k !== undefined) {
      // a key is never present and tombstoned at once
      t.overrideSeq = k.overrideSeq;
      t.resetSeq = k.resetSeq;
      this.delKeyRow(key);
    } else {
      const prev = this.tombs.get(key);
      if (prev !== undefined) {
        t.mark = t.mark || prev.mark;
        if (prev.kind === "key") {
          t.overrideSeq = prev.overrideSeq;
          t.resetSeq = prev.resetSeq;
        }
      }
    }
    this.setTomb(key, t);
  }

  /**
   * Makes the key absent with no tombstone of its own, and no override seq or reset seq: a key whose state a subtree deletion of
   * the same record decides (05 [STATE-37], [STATE-43]).
   */
  dropKey(key: string): void {
    this.delKeyRow(key);
    this.delTomb(key);
  }

  /** Gives the key's key tombstone the reset seq of 05 [STATE-85], and reports whether the key holds one. A key with no key tombstone gets none. */
  setResetSeq(key: string, seq: bigint): boolean {
    const t = this.tombs.get(key);
    if (t === undefined || t.kind !== "key") return false;
    this.tombs.set(key, { ...t, resetSeq: seq }); // the name is held already: the index stays as it is
    return true;
  }

  /** Records a change to key at seq: it sets the version of every proper ancestor prefix of the key to seq (05 [STATE-51]). The proper ancestor prefixes of `a/b/c` are `a/` and `a/b/`. */
  touchPrefixes(key: string, seq: bigint): void {
    if (this.prefixes === null) return;
    for (let i = 0; i < key.length; i++) {
      if (key[i] === "/") this.setPrefix(key.slice(0, i + 1), seq);
    }
  }

  /**
   * The marker keys of the subtree of root, root excluded, that are present or hold a tombstone now, in ascending byte order
   * (05 [STATE-45]). The fold calls it before it applies any change of the record, so that the answer is the state "just before"
   * the record.
   */
  nestedMarkers(top: Topology, root: string): string[] {
    const out: string[] = [];
    const prefix = `${root}/`;
    const visit = (key: string): void => {
      if (top.isMarker(key) && inSubtree(top, root, key)) out.push(key);
    };
    // the subtree is the range of the names that start with `root/`: the cost is that of the range and a search, whatever the size of the
    // keyspace (a key is never present and tombstoned at once, so the two ranges hold no name twice)
    for (const key of this.keyIdx.withPrefix(prefix)) visit(key);
    for (const key of this.tombIdx.withPrefix(prefix)) {
      if (!this.keys.has(key)) visit(key);
    }
    return out.sort(compareUtf8);
  }

  /**
   * Applies a subtree deletion at seq (05 [STATE-43], [STATE-45], [STATE-51]): every key of the subtree of root becomes absent at
   * seq, nested marker keys included, and loses its key tombstone, its override seq and its reset seq; the root and every nested
   * marker key get an entity tombstone (or a staged tombstone) at seq; the prefix `root/` and every prefix below it that has a
   * version get the version seq. The rows of the subtree are removed at once, as the comment of this file says.
   *
   * The ancestor prefixes of root get their version from `touchPrefixes`, which the fold calls for every change.
   */
  deleteSubtree(top: Topology, seq: bigint, d: Subtree): void {
    const kind: TombstoneKind = d.staged ? "staged" : "entity";
    // 05 [STATE-37]: a tombstone keeps its redaction mark for as long as it is kept, also when a subtree deletion moves it; the
    // marks of the tombstones that this deletion moves are read before it removes them.
    const rootMark = d.markRoot || (this.tombs.get(d.root)?.mark ?? false);
    const nestedMark = d.nested.map((n) => n.mark || (this.tombs.get(n.key)?.mark ?? false));

    const prefix = `${d.root}/`;
    const gone: string[] = [];
    for (const key of this.keyIdx.withPrefix(prefix)) {
      if (inSubtree(top, d.root, key)) gone.push(key);
    }
    for (const key of this.tombIdx.withPrefix(prefix)) {
      if (inSubtree(top, d.root, key)) gone.push(key);
    }
    for (const key of gone) {
      this.delKeyRow(key);
      this.delTomb(key);
    }
    this.delKeyRow(d.root);

    this.setTomb(d.root, { kind, version: seq, mark: rootMark, overrideSeq: 0n, resetSeq: 0n });
    d.nested.forEach((n, i) => {
      if (n.key !== d.root) this.setTomb(n.key, { kind, version: seq, mark: nestedMark[i] as boolean, overrideSeq: 0n, resetSeq: 0n });
    });

    // 05 [STATE-51]: the prefix `root/` and every prefix below it that has a version
    if (this.prefixes !== null) {
      const below = (this.prefixIdx as NameIndex).withPrefix(prefix);
      this.setPrefix(prefix, seq);
      for (const p of below) this.setPrefix(p, seq);
    }
  }

  /**
   * Applies a `TOMBSTONES_DISCARDED` record with floor_seq at (05 [STATE-83]): every tombstone, of any kind, whose deletion seq is
   * at or below floor is discarded with its redaction mark, its override seq and its reset seq, and every prefix whose version
   * is at or below floor and below which every key then has version 0 loses its version (05 [STATE-51]). It changes nothing
   * else, and raises the tombstone floor to floor.
   */
  discardTombstones(floor: bigint): void {
    for (const [key, t] of [...this.tombs]) {
      if (t.version <= floor) this.delTomb(key);
    }
    this.tombFloor = floor;
    if (this.prefixes === null) return;
    // A prefix keeps its version while some key below it has a version above 0: a present key, a key tombstone that is left,
    // or a key covered by the tombstone of a marker key above the prefix.
    for (const [p, v] of [...this.prefixes]) {
      if (v > floor) continue;
      if (this.keyIdx.hasPrefix(p) || this.tombIdx.hasPrefix(p)) continue;
      if (this.coveredPrefix(p)) continue;
      this.delPrefix(p);
    }
  }

  /** Whether a marker key holds a covering tombstone and p lies below its subtree prefix: some key of that subtree under p then has the tombstone's version. */
  private coveredPrefix(p: string): boolean {
    for (let i = 0; i < p.length; i++) {
      if (p[i] !== "/") continue;
      const t = this.tombs.get(p.slice(0, i));
      if (t !== undefined && covers(t)) return true;
    }
    return false;
  }
}

function toKeyState(key: string, k: KeyRow): KeyState {
  return {
    key,
    state: k.state,
    counter: k.rule === "counter",
    encoding: k.encoding,
    content: k.state === StateValue ? (k.doc !== null ? k.doc.canonical() : k.value) : undefined,
    version: k.version,
  };
}

export { COUNTER_ENCODING, REDACTED_ENCODING };
