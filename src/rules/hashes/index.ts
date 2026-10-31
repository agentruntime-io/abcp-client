// The state hashes of 05 [STATE-41] and [STATE-84]: the per-key state hash, the keyspace state hash and the metadata hash.
// They are plain functions of plain values and know nothing of manifests, stores or the wire.
//
// Notation, as in [STATE-41]: u32(n) and u64(n) are n as a 32-bit and as a 64-bit big-endian unsigned integer, i64(n) is n
// as a 64-bit big-endian two's-complement integer, L(b) is u32(len(b)) || b, and || is concatenation.

import { ByteWriter, compareUtf8 } from "../bytes.js";
import { sha256 } from "../crypto.js";

/** The state byte of a present key in its per-key state hash (05 [STATE-41]). */
export const StateValue = 0x01; // the key holds a value
export const StateNull = 0x02; // the key holds an explicit null
export const StateRedacted = 0x03; // the key is in the redacted state (05 [STATE-82])
export type KeyValueState = typeof StateValue | typeof StateNull | typeof StateRedacted;

/** The encoding of a key in the redacted state, whatever its rule (05 [STATE-41]). */
export const REDACTED_ENCODING = "redacted/v1";
/** The encoding of a counter, although an `INCR` carries no encoding (05 [STATE-20], [STATE-41]). */
export const COUNTER_ENCODING = "json/v1";

/** A present key, as its per-key state hash sees it (05 [STATE-41]). */
export interface KeyState {
  /** The key's bytes (as a string, hashed as its UTF-8). */
  key: string;
  state: KeyValueState;
  /** The key's template has the `counter` rule: it hashes as `json/v1`, whatever `encoding` says. */
  counter?: boolean;
  /** The key's stored encoding. A key in the redacted state hashes as `redacted/v1`, whatever its rule. */
  encoding: string;
  /**
   * The stored value bytes; for a CRDT key that holds a value, its canonical form SV || DS, which the caller computes from
   * the key's integrated updates. Ignored for null and for the redacted state, whose content is empty.
   */
  content?: Uint8Array;
  /** The key's version. */
  version: bigint | number;
}

/** Returns the bytes that the per-key state hash of k digests: L(key) || state || L(encoding) || L(content) || u64(version). */
export function keyStatePreimage(k: KeyState): Uint8Array {
  let encoding = k.encoding;
  if (k.state === StateRedacted) encoding = REDACTED_ENCODING;
  else if (k.counter === true) encoding = COUNTER_ENCODING;
  const content = k.state === StateValue ? (k.content ?? new Uint8Array(0)) : new Uint8Array(0);
  return new ByteWriter().lstring(k.key).u8(k.state).lstring(encoding).lbytes(content).u64(k.version).finish();
}

/** The per-key state hash of a present key (05 [STATE-41]): SHA-256 of `keyStatePreimage`. */
export function keyStateHash(k: KeyState): Uint8Array {
  return sha256(keyStatePreimage(k));
}

/**
 * The keyspace state hash (05 [STATE-41]): SHA-256 over the per-key state hashes of all present keys, reserved `abpc.*`
 * keys included, concatenated in ascending byte order of the keys; for a keyspace with no present key, the SHA-256 of the
 * empty string. `keys` may come in any order and is not changed; each key must occur once.
 */
export function keyspaceStateHash(keys: readonly KeyState[]): Uint8Array {
  const sorted = [...keys].sort((a, b) => compareUtf8(a.key, b.key));
  const w = new ByteWriter();
  for (const k of sorted) w.bytes(keyStateHash(k));
  return sha256(w.finish());
}

// ---- 05 [STATE-84]: the metadata hash ----

/** The kind byte of an entry of K (05 [STATE-84]). */
export const EntryKind = {
  Present: 0x00, // a present key
  KeyTombstone: 0x01, // a key tombstone other than a staged tombstone
  EntityTombstone: 0x02, // an entity tombstone
  StagedTombstone: 0x03, // a staged tombstone (05 [STATE-45])
} as const;
export type EntryKindValue = (typeof EntryKind)[keyof typeof EntryKind];

/** A point in time as the fields of a Timestamp give it. */
export interface Time {
  seconds: bigint | number;
  nanos: number;
}

/** A key that is present or holds a retained tombstone, as an entry of K sees it (05 [STATE-84]). */
export interface MetaEntry {
  key: string;
  kind: EntryKindValue;
  /** The tombstone carries the redaction mark (05 [STATE-37]). It is part of the hash only for a tombstone. */
  mark?: boolean;
  /** The key's version, which for a tombstone is its deletion seq. */
  version: bigint | number;
  /** The key's override seq and reset seq (05 [STATE-60], [STATE-85]); 0 when the key holds none. */
  overrideSeq?: bigint | number;
  resetSeq?: bigint | number;
  /** The key's changed_at for a present key; ignored for a tombstone, which hashes zero time. */
  changedAt?: Time;
}

/** A key prefix with its version (05 [STATE-51]). */
export interface PrefixVersion {
  prefix: string;
  version: bigint | number;
}

/**
 * K of 05 [STATE-84]: u32(n) followed, for each of the n entries in ascending byte order of the keys, by
 * L(key) || kind || mark || u64(version) || u64(override seq) || u64(reset seq) || i64(seconds) || u32(nanos). `entries`
 * may come in any order; a key occurs once. The caller leaves out the keys that a deleted subtree hides.
 */
export function metadataK(entries: readonly MetaEntry[]): Uint8Array {
  const sorted = [...entries].sort((a, b) => compareUtf8(a.key, b.key));
  const w = new ByteWriter().u32(sorted.length);
  for (const e of sorted) {
    const present = e.kind === EntryKind.Present;
    // SPEC-QUESTION: 05 [STATE-84] "`mark` is one byte: `0x01` for a tombstone that carries the redaction mark, and `0x00`
    // otherwise". A present key therefore hashes mark 0x00, as the Go server does. The JavaScript vector model writes the
    // mark of any entry; no vector has a present key with a mark.
    const mark = !present && e.mark === true ? 1 : 0;
    const at: Time = present && e.changedAt !== undefined ? e.changedAt : { seconds: 0, nanos: 0 };
    w.lstring(e.key)
      .u8(e.kind)
      .u8(mark)
      .u64(e.version)
      .u64(e.overrideSeq ?? 0)
      .u64(e.resetSeq ?? 0)
      .i64(at.seconds)
      .u32(at.nanos);
  }
  return w.finish();
}

/**
 * P of 05 [STATE-84]: u32(m) followed, for each of the m prefixes with a non-zero version, in ascending byte order of the
 * prefixes, by L(prefix) || u64(version). A prefix with version 0 is not an entry.
 */
export function metadataP(prefixes: readonly PrefixVersion[]): Uint8Array {
  const ps = prefixes.filter((p) => BigInt(p.version) !== 0n).sort((a, b) => compareUtf8(a.prefix, b.prefix));
  const w = new ByteWriter().u32(ps.length);
  for (const p of ps) w.lstring(p.prefix).u64(p.version);
  return w.finish();
}

/**
 * The metadata hash of a keyspace (05 [STATE-84]): SHA-256 over K || P. It covers the state, other than values, that
 * decides whether a change is accepted: tombstones, override seqs, reset seqs, prefix versions and `changed_at`.
 */
export function metadataHash(entries: readonly MetaEntry[], prefixes: readonly PrefixVersion[]): Uint8Array {
  return sha256(new ByteWriter().bytes(metadataK(entries)).bytes(metadataP(prefixes)).finish());
}
