// The validity of a candidate base (05 [STATE-74]). A client never builds a base, but a rename's SYSTEM `SET` (06 [LOG-40])
// carries one that a party can judge with the same fold, and the vectors of 17 [CONF-9] check the verdicts.

import { CrdtError, type Reason } from "./errors.js";
import { Analysis, Doc, noOpForGood } from "./doc.js";
import { type ClientClock, type ClientRanges, parseUpdate } from "./parse.js";
import { checkRoots, type Roots } from "./roots.js";

/** Which condition of 05 [STATE-74] a candidate base fails. */
export type BaseFault =
  | "not-well-formed" // it is not well formed (05 [STATE-71]); reason is the failed check
  | "pending" // it has pending structs (05 [STATE-76]); reason is the no-op rule
  | "state-vector" // its state vector is not the key's at crdt_covered_seq
  | "delete-set" // its normalized delete set is not the key's canonical one
  | "gc-not-allowed"; // a GC stands in place of an id whose parent is not hidden

/** Why a candidate base is not valid (05 [STATE-74]). */
export interface BaseError {
  fault: BaseFault;
  /** for "not-well-formed" and "pending" */
  reason?: Reason;
  /** The verdict as the vectors write it: `not-well-formed:<code>`, `pending:<reason>`, `state-vector`, `delete-set` or `gc-not-allowed`. */
  code: string;
}

function baseError(fault: BaseFault, reason?: Reason): BaseError {
  return { fault, reason, code: reason !== undefined ? `${fault}:${reason}` : fault };
}

/**
 * Judges a candidate base against the fold of the key's integrated updates at the seq that the base covers, `key`
 * (05 [STATE-74]). Returns null when the base is valid, and a BaseError when it is not. The base is valid when, read as one
 * Yjs update:
 *  - it is well formed (05 [STATE-71]), under `roots`, the crdt_roots of the key that holds it;
 *  - it has no pending structs: applied alone to an empty document it is not a no-op for good (05 [STATE-76]);
 *  - its state vector equals the key's;
 *  - its delete set, normalized (05 [STATE-41]), equals the key's canonical delete set;
 *  - and a GC stands only in place of an id that a GC first defines in the covered updates, or of an id whose parent is
 *    hidden, which is the part of the content rule that the key's fold can check.
 *
 * The content rule beyond the GC rule (each id holds the content of its first definition in seq order, or a ContentDeleted in
 * its place for an id that the delete set holds) needs the covered updates themselves; it is no part of the validity
 * conditions here. For the value of a SYSTEM `SET` (06 [LOG-40]), `key` is the fold of the renamed-from key.
 */
export function validateBase(base: Uint8Array, roots: Roots, key: Doc): BaseError | null {
  let u;
  try {
    u = parseUpdate(base);
    checkRoots(u, roots);
  } catch (e) {
    if (e instanceof CrdtError) return baseError("not-well-formed", e.reason);
    throw e;
  }
  const empty = new Doc();
  const pending = noOpForGood(empty, new Analysis(empty, u));
  if (pending !== undefined) return baseError("pending", pending);
  if (!sameStateVector(u.stateVector(), key.stateVector())) return baseError("state-vector");
  if (!sameDeleteSet(u.deletions(), key.deleteSet())) return baseError("delete-set");

  // A GC may stand only in place of an id that a GC first defines, or whose parent is hidden. The model reads the parent that
  // the first definition encodes, so an item that takes its parent from an origin that is a GC does not allow one.
  //
  // SPEC-QUESTION: 05 [STATE-74] (SPEC-QUESTIONS Q7): "or a `GC` in its place for an id whose parent is hidden", with
  // [STATE-76]: "an item that encodes none has the parent of its origin when it has an origin (hidden when the origin is a
  // `GC`)". An item whose origin is a GC has a hidden parent, so the text allows a GC in its place. Both reference models read
  // only the parent id that the definition carries, which such an item has not, and refuse the GC; no vector has the case. The
  // code follows the models and the Go server, which is the stricter reading (the base is refused).
  const a = new Analysis(key, null);
  for (const g of u.groups) {
    for (const s of g.structs) {
      if (s.kind !== "gc") continue;
      const defs = key.defs.get(s.client) ?? [];
      for (const d of defs) {
        if (d.end <= s.clock || d.start >= s.clock + s.len) continue;
        if (!(d.kind === "gc" || (d.hasParent && d.parentId !== null && a.hidden(d.parentId)))) return baseError("gc-not-allowed");
      }
    }
  }
  return null;
}

function sameStateVector(a: readonly ClientClock[], b: readonly ClientClock[]): boolean {
  return a.length === b.length && a.every((x, i) => x.client === (b[i] as ClientClock).client && x.clock === (b[i] as ClientClock).clock);
}

function sameDeleteSet(a: readonly ClientRanges[], b: readonly ClientRanges[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    const x = a[i] as ClientRanges;
    const y = b[i] as ClientRanges;
    if (x.client !== y.client || x.ranges.length !== y.ranges.length) return false;
    for (let j = 0; j < x.ranges.length; j++) {
      const r = x.ranges[j] as (typeof x.ranges)[number];
      const t = y.ranges[j] as (typeof y.ranges)[number];
      if (r.clock !== t.clock || r.len !== t.len) return false;
    }
  }
  return true;
}
