// The hashes built on the canonical encoding: the content hash of a mutation (07 [MUT-45]), the value digests, the content
// digest C and the chain hash of a record (06 [LOG-71]), and the digest of an AdminService request (12 [ADM-94]).
//
// The hashes work by message name, so that a dynamic type with more fields than this version's (as the conformance vectors
// build for an "upgraded server") hashes the same way.

import type { DescField, DescMessage, Message } from "@bufbuild/protobuf";
import { ByteWriter, concat } from "../bytes.js";
import { hmacSha256, sha256 } from "../crypto.js";
import { MutationSchema } from "../../gen/abpc/v2/sync_pb.js";
import {
  KeyChangeSchema,
  KeyEntrySchema,
  RecordSchema,
  type KeyChange,
  type KeyEntry,
  type Record as RecordMessage,
} from "../../gen/abpc/v2/types_pb.js";
import { type EncodeView, encode } from "./encode.js";

const MUTATION = "abpc.v2.Mutation";
const KEY_CHANGE = "abpc.v2.KeyChange";
const RECORD = "abpc.v2.Record";
const OWNER_CONTACT = "abpc.v2.OwnerContact";

/** The size of the salt of a value digest, in octets (06 [LOG-71]). */
export const SALT_SIZE = 16;

// ---- 07 [MUT-45]: the content hash ----

/** True when the Mutation field called name is one of the stamping fields of 07 [MUT-45]. */
function stamping(name: string): boolean {
  return name === "client_id" || name === "client_seq" || name === "after_client_seq";
}

const contentHashView: EncodeView = {
  skipField: (md, fd) => md.typeName === MUTATION && stamping(fd.name),
};

/**
 * The canonical encoding of a copy of the Mutation in which the stamping fields client_id, client_seq and after_client_seq
 * are cleared (07 [MUT-45]). Unknown fields are included as received, in the place of their numbers: a message received in
 * the JSON codec holds none, since that codec drops them (15 [COMPAT-38]). `schema` defaults to this version's Mutation; a
 * dynamic type is hashed by its name.
 */
export function contentHashInput(m: Message, schema: DescMessage = MutationSchema): Uint8Array {
  if (schema.typeName !== MUTATION) throw new RangeError(`content hash of a ${schema.typeName}, want ${MUTATION}`);
  return encode(schema, m, contentHashView);
}

/**
 * The content hash of an action (07 [MUT-45]): HMAC-SHA-256, under the app's content-hash key, of `contentHashInput`. The
 * hash of a message that arrived in the JSON codec is the hash of the same message that arrived in the binary codec, since
 * both are taken on the canonical binary form (02 [ARC-61]). `key` is the per-app key that only ABPC holds, distinct from
 * the record MAC key (06 [LOG-73]); an empty key is refused with a RangeError.
 */
export function contentHash(m: Message, key: Uint8Array, schema: DescMessage = MutationSchema): Uint8Array {
  if (key.length === 0) throw new RangeError("no content-hash key");
  return hmacSha256(key, contentHashInput(m, schema));
}

// ---- 06 [LOG-71]: value digests, E, C and the record hash ----

/** True when the KeyChange field called name is a non-value field: exactly key, kind and seen_version (06 [LOG-71]). */
function nonValue(name: string): boolean {
  return name === "key" || name === "kind" || name === "seen_version";
}

function saltedDigest(salt: Uint8Array, schema: DescMessage, m: Message, view?: EncodeView): Uint8Array {
  if (salt.length !== SALT_SIZE) throw new RangeError(`salt of ${salt.length} octets, want ${SALT_SIZE}`);
  return sha256(concat(salt, encode(schema, m, view)));
}

const changeValueView: EncodeView = {
  skipField: (md, fd) => md.typeName === KEY_CHANGE && nonValue(fd.name),
};

/**
 * The value digest of a change (06 [LOG-71]): SHA-256 over the salt followed by the canonical encoding of a KeyChange that
 * holds only the change's value fields, its unknown fields included (every field but key, kind and seen_version).
 */
export function changeValueDigest(salt: Uint8Array, c: KeyChange): Uint8Array {
  return saltedDigest(salt, KeyChangeSchema, c, changeValueView);
}

/** The value digest of an entry of `previous` (06 [LOG-71]): SHA-256 over the salt followed by the canonical encoding of the whole entry. */
export function entryValueDigest(salt: Uint8Array, k: KeyEntry): Uint8Array {
  return saltedDigest(salt, KeyEntrySchema, k);
}

/**
 * The value digests of a record's changes, in index order, and then those of its `previous` entries in order. `salts` holds
 * one salt for each change, `previousSalts` one for each entry.
 */
export function valueDigests(r: RecordMessage, salts: readonly Uint8Array[], previousSalts: readonly Uint8Array[]): Uint8Array[] {
  if (salts.length !== r.changes.length || previousSalts.length !== r.previous.length) {
    throw new RangeError(
      `${salts.length} salts for ${r.changes.length} changes and ${previousSalts.length} for ${r.previous.length} previous entries`,
    );
  }
  return [
    ...r.changes.map((c, i) => changeValueDigest(salts[i] as Uint8Array, c)),
    ...r.previous.map((k, j) => entryValueDigest(previousSalts[j] as Uint8Array, k)),
  ];
}

/**
 * The view of E (06 [LOG-71]): the Record with seq, mac (field 18), record_hash (field 20), overwrote_newer and previous
 * cleared, and the value fields of every change cleared, a change's unknown fields included. The unknown fields of the
 * Record itself stay.
 */
const recordEView: EncodeView = {
  skipField: (md, fd) => {
    switch (md.typeName) {
      case RECORD:
        return fd.name === "seq" || fd.name === "mac" || fd.name === "record_hash" || fd.name === "overwrote_newer" || fd.name === "previous";
      case KEY_CHANGE:
        return !nonValue(fd.name);
      default:
        return false;
    }
  },
  skipUnknown: (md) => md.typeName === KEY_CHANGE,
};

/**
 * Returns E and the content digest C of a record (06 [LOG-71]): E is the canonical encoding of the record as `recordEView`
 * says, and C is SHA-256 over E followed by the value digests, those of the changes in index order and then those of the
 * `previous` entries in order. After a redaction the digests are the ones kept with the record, and E and C do not change,
 * since the blanked fields are outside E.
 */
export function contentDigest(r: RecordMessage, digests: readonly Uint8Array[]): { e: Uint8Array; c: Uint8Array } {
  const e = encode(RecordSchema, r, recordEView);
  return { e, c: sha256(concat(e, ...digests)) };
}

/** W of 06 [LOG-71]: the count of overwrote_newer entries followed by each entry, all as 32-bit big-endian integers. */
export function overwroteNewerBytes(overwroteNewer: readonly number[]): Uint8Array {
  const w = new ByteWriter().u32(overwroteNewer.length);
  for (const i of overwroteNewer) w.u32(i);
  return w.finish();
}

/**
 * The chain hash of 06 [LOG-71]: SHA-256(prev_hash || log_epoch || seq || C || W), where log_epoch is a 32-bit and seq a
 * 64-bit big-endian integer. `prevHash` is the record_hash of the record at seq − 1 of the timeline that the record
 * continues: the previous record of its epoch, the record at the base seq for the first record of an epoch (the source's
 * record at S for a split successor), and 32 zero bytes when the base seq is 0.
 */
export function recordHash(
  prevHash: Uint8Array,
  logEpoch: number,
  seq: bigint,
  c: Uint8Array,
  overwroteNewer: readonly number[],
): Uint8Array {
  return sha256(new ByteWriter().bytes(prevHash).u32(logEpoch).u64(seq).bytes(c).bytes(overwroteNewerBytes(overwroteNewer)).finish());
}

/** The hashes that 06 [LOG-71] has a server compute for a record. */
export interface RecordHashes {
  /** The changes' in index order, then the `previous` entries'. */
  valueDigests: Uint8Array[];
  e: Uint8Array;
  c: Uint8Array;
  w: Uint8Array;
  recordHash: Uint8Array;
}

/**
 * Computes every hash of 06 [LOG-71] for r from its salts: one for each change and one for each `previous` entry. It reads
 * seq, log_epoch and overwrote_newer from the record, and `prevHash` is the chain hash of the record before it.
 */
export function hashRecord(
  r: RecordMessage,
  salts: readonly Uint8Array[],
  previousSalts: readonly Uint8Array[],
  prevHash: Uint8Array,
): RecordHashes {
  return hashRecordFromDigests(r, valueDigests(r, salts, previousSalts), prevHash);
}

/** `hashRecord` for a record whose salts are gone, as after a redaction: it takes the value digests kept with the record (06 [LOG-76]). */
export function hashRecordFromDigests(r: RecordMessage, digests: readonly Uint8Array[], prevHash: Uint8Array): RecordHashes {
  const { e, c } = contentDigest(r, digests);
  return {
    valueDigests: [...digests],
    e,
    c,
    w: overwroteNewerBytes(r.overwroteNewer),
    recordHash: recordHash(prevHash, r.logEpoch, r.seq, c, r.overwroteNewer),
  };
}

// ---- 12 [ADM-94]: the audit request digest ----

/**
 * The view of the request digest (12 [ADM-94]): every value field cleared first, which is the value fields of every
 * KeyChange (06 [LOG-71]), unknown fields included, and the `address` of every owner contact. Unknown fields of every other
 * message stay as received.
 */
const requestView: EncodeView = {
  skipField: (md: DescMessage, fd: DescField) => {
    switch (md.typeName) {
      case KEY_CHANGE:
        return !nonValue(fd.name);
      case OWNER_CONTACT:
        return fd.name === "address";
      default:
        return false;
    }
  },
  skipUnknown: (md) => md.typeName === KEY_CHANGE,
};

/**
 * Returns the canonical encoding that 12 [ADM-94] digests, and the digest: SHA-256 over the canonical encoding of the
 * request with every value field cleared, so that no digest of a value remains after a `Redact`, and none of a contact ever.
 * The request keeps its unknown fields as received (07 [MUT-45]). For the audit entry of a `Mutate/abpc.Override`, m is the
 * override's Mutation. An entry that has no request, and one of a request that was refused before it could be decoded
 * (02 [ARC-61]), carries an empty digest, which this function is not for.
 */
export function requestDigest(schema: DescMessage, m: Message): { digest: Uint8Array; canonical: Uint8Array } {
  const canonical = encode(schema, m, requestView);
  return { digest: sha256(canonical), canonical };
}
