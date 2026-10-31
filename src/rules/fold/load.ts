// Installing the entries of a load (09 [READ-9], [READ-13], [READ-18]) into the confirmed state: a key with its value, a key in
// the redacted state, or a tombstone. A load is the other source of confirmed state besides records (14 [CLI-2]).

import type { KeyEntry } from "../../gen/abpc/v2/types_pb.js";
import { CrdtError, Doc, parseUpdate } from "../crdt/index.js";
import { COUNTER_ENCODING, REDACTED_ENCODING } from "../hashes/index.js";
import { refuse } from "./errors.js";
import { isReservedKey, RESERVED_TEMPLATE, type Rules } from "./rules.js";
import { CRDT_ENCODING, StateNull, StateRedacted, StateValue, type KeyRow, type ReplicaState, type Tombstone } from "./state.js";

/**
 * Installs one entry of a `LoadStateResponse` into the state, replacing whatever the state held for the key (09 [READ-13]):
 *  - a tombstone entry (`deleted`) is a tombstone: an entity tombstone for a marker key, a staged tombstone when `staged` is set,
 *    and a key tombstone for any other key, the kind taken from the key and `staged`, never from the key alone; with the
 *    redaction mark when `redacted` is set. It is never shown as a key;
 *  - an entry with `redacted` set and no `deleted` is a key in the redacted state (05 [STATE-82]), kept with its version;
 *  - an explicit null is a present key with an empty value (05 [STATE-14]);
 *  - a CRDT key's value is a base, the first of the key's integrated updates, covering `crdt_covered_seq` (05 [STATE-74], [STATE-75]);
 *  - any other entry is a key with its value bytes and encoding exactly as served (05 [STATE-3]).
 *
 * `manifestVersion` is the version under which the template of the key is read: its merge rule (a counter hashes as `json/v1`, a
 * CRDT key by its canonical form) and whether it is a marker. A load does not carry override seqs or reset seqs, which a
 * client does not use, so they are 0. Throws a FoldError (code `malformed`) for a CRDT base that is not a Yjs update.
 */
export function installEntry(st: ReplicaState, e: KeyEntry, rules: Rules, manifestVersion: number): void {
  const tpl = isReservedKey(e.key) ? RESERVED_TEMPLATE : rules.template(manifestVersion, e.key);
  if (e.deleted) {
    const t: Tombstone = {
      kind: tpl?.entity === true ? (e.staged ? "staged" : "entity") : "key",
      version: e.version,
      mark: e.redacted,
      overrideSeq: 0n,
      resetSeq: 0n,
    };
    st.putTombstone(e.key, t);
    return;
  }
  const rule = tpl?.rule ?? (e.encoding === CRDT_ENCODING ? "crdt" : "lww");
  const k: KeyRow = {
    state: StateValue,
    rule,
    encoding: e.encoding,
    value: new Uint8Array(0),
    doc: null,
    version: e.version,
    crdtCoveredSeq: 0n,
    changedAt: { seconds: e.changedAt?.seconds ?? 0n, nanos: Math.max(e.changedAt?.nanos ?? 0, 0) },
    changedBy: e.changedBy,
    overrideSeq: 0n,
    resetSeq: 0n,
  };
  if (e.redacted) {
    k.state = StateRedacted;
    k.encoding = REDACTED_ENCODING;
    k.changedBy = "";
    if (rule === "crdt") k.crdtCoveredSeq = e.version; // 09 [READ-14]
  } else if (e.isNull) {
    k.state = StateNull;
  } else if (rule === "crdt") {
    let u;
    try {
      u = parseUpdate(e.value);
    } catch (err) {
      if (err instanceof CrdtError) throw refuse("malformed", e.version, "READ-14", `base of key "${e.key}": ${err.message}`);
      throw err;
    }
    const doc = new Doc();
    doc.applyUpdate(u);
    k.doc = doc;
    k.encoding = CRDT_ENCODING;
    k.crdtCoveredSeq = e.crdtCoveredSeq;
  } else {
    if (rule === "counter") k.encoding = COUNTER_ENCODING;
    k.value = e.value.slice();
  }
  st.putKey(e.key, k);
}
