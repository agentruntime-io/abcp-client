// The hashes built on the canonical encoding: the content hash of a mutation (07 [MUT-45]), the value digests, the content
// digest C and the chain hash of a record (06 [LOG-71]), and the digest of an AdminService request (12 [ADM-94]).
//
// The hashes work by message name, so that a dynamic type with more fields than this version's (as the conformance vectors
// build for an "upgraded server") hashes the same way.

import type { DescField, DescMessage, Message } from "@bufbuild/protobuf";
import type { ReflectMessage } from "@bufbuild/protobuf/reflect";
import { ByteWriter, concat } from "../bytes.js";
import { hmacSha256, sha256 } from "../crypto.js";
import { MutationSchema } from "../../gen/abpc/v2/sync_pb.js";
import {
  ChangeKind,
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
const REDACT_REQUEST = "abpc.v2.RedactRequest";
/** A rule of a `SplitKeyspaceRequest` and a successor of an `ArchiveKeyspaceRequest` (the one request that holds a SuccessorRule): 12 [ADM-94] clears their `prefix`. */
const SPLIT_RULE = "abpc.v2.SplitRule";
const SUCCESSOR_RULE = "abpc.v2.SuccessorRule";
/** ChangeKind `OVERRIDE` (types.proto), compared by number so that a dynamic type works too. */
const OVERRIDE_KIND = ChangeKind.OVERRIDE;

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
  // LOG-71: one digest for each change and one for each `previous` entry. Another count is a caller's mistake, which would give a wrong
  // record_hash with no sign of it, so it is refused (V1 SHOULD-3).
  const want = r.changes.length + r.previous.length;
  if (digests.length !== want) {
    throw new RangeError(`${digests.length} value digests for ${r.changes.length} changes and ${r.previous.length} previous entries, want ${want}`);
  }
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
 * KeyChange (06 [LOG-71]), unknown fields included, the `address` of every owner contact, `RedactRequest.keys` (round 18)
 * and, since round 19 (J7), the `prefix` of every rule of a `SplitKeyspaceRequest` and of every successor of an
 * `ArchiveKeyspaceRequest`, whatever it is, so that the digest of a `Redact`, a split or an archive holds no key path of a
 * `personal` template. Unknown fields of every other message stay as received. With `personal`, the `key` of each
 * `OVERRIDE` change for which it holds is cleared too (change by change: the key of another change is kept).
 */
function requestViewFor(personal?: (key: string) => boolean): EncodeView {
  const view: EncodeView = {
    skipField: (md: DescMessage, fd: DescField) => {
      switch (md.typeName) {
        case KEY_CHANGE:
          return !nonValue(fd.name);
        case OWNER_CONTACT:
          return fd.name === "address";
        case REDACT_REQUEST:
          return fd.name === "keys"; // no key path of a personal template, so no key path of a Redact
        case SPLIT_RULE:
        case SUCCESSOR_RULE:
          return fd.name === "prefix"; // J7: no prefix of a split or an archive, whatever it is
        default:
          return false;
      }
    },
    skipUnknown: (md) => md.typeName === KEY_CHANGE,
  };
  if (personal === undefined) return view;
  return {
    ...view,
    skipFieldIn: (m: ReflectMessage, fd: DescField) => {
      if (m.desc.typeName !== KEY_CHANGE || fd.name !== "key") return false;
      const kind = m.desc.fields.find((f) => f.name === "kind");
      return kind !== undefined && m.get(kind) === OVERRIDE_KIND && personal(m.get(fd) as string);
    },
  };
}

const requestView = requestViewFor();

/**
 * Returns the canonical encoding that 12 [ADM-94] digests, and the digest: SHA-256 over the canonical encoding of the
 * request with every value field cleared (and `RedactRequest.keys` and every split and archive `prefix`, J7), so that no digest of a value remains after a `Redact`,
 * none of a contact ever, and none of a key path of a `personal` template. The request keeps its unknown fields as received
 * (07 [MUT-45]). An entry that has no request, and one of a request that was refused before it could be decoded (02
 * [ARC-61]), carries an empty digest, which this function is not for.
 *
 * For the audit entry of a `Mutate/abpc.Override`, m is the override's Mutation and `personal` says whether a key matches a
 * template that is `personal` in the manifest version in force for the keyspace at the commit, the version under which 07
 * Table 7-2 item 3 judges the override and that the override's record carries as `manifest_version` (06 [LOG-8]; the
 * keyspace's effective version); the key of each `OVERRIDE` change for which it holds is cleared with the value fields,
 * and the key of another change of the same override is kept, so that such a change keeps its kind and
 * `seen_version`. A later change of `personal` in a newer version does not change the digest (A.7: a client that recomputes it
 * clears the same fields under the same version). Without `personal` every key stays.
 */
export function requestDigest(
  schema: DescMessage,
  m: Message,
  personal?: (key: string) => boolean,
): { digest: Uint8Array; canonical: Uint8Array } {
  const canonical = encode(schema, m, personal === undefined ? requestView : requestViewFor(personal));
  return { digest: sha256(canonical), canonical };
}
