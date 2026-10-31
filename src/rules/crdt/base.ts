// The validity of a candidate base (05 [STATE-74]). A client never builds a base, but a rename's SYSTEM `SET` (06 [LOG-40])
// carries one that a party can judge with the same fold, and the vectors of 17 [CONF-9] check the verdicts.

import { CrdtError, type Reason } from "./errors.js";
import { Analysis, type Def, Doc, noOpForGood } from "./doc.js";
import { type ClientClock, type ClientRanges, type Range, type Struct, type Update, parseUpdate, ContentRef } from "./parse.js";
import { checkRoots, type Roots } from "./roots.js";

/** Which condition of 05 [STATE-74] a candidate base fails. */
export type BaseFault =
  | "not-well-formed" // it is not well formed (05 [STATE-71]); reason is the failed check
  | "pending" // it has pending structs (05 [STATE-76]); reason is the no-op rule
  | "state-vector" // its state vector is not the key's at crdt_covered_seq
  | "delete-set" // its normalized delete set is not the key's canonical one
  | "gc-not-allowed" // a GC stands in place of an id whose parent is not hidden
  | "content"; // an id does not hold what its first definition in the key's fold holds

/** Why a candidate base is not valid (05 [STATE-74]). */
export interface BaseError {
  fault: BaseFault;
  /** for "not-well-formed" and "pending" */
  reason?: Reason;
  /** The verdict as the vectors write it: `not-well-formed:<code>`, `pending:<reason>`, `state-vector`, `delete-set`, `gc-not-allowed` or `content`. */
  code: string;
}

function baseError(fault: BaseFault, reason?: Reason): BaseError {
  return { fault, reason, code: reason !== undefined ? `${fault}:${reason}` : fault };
}

/**
 * Judges a candidate base against the fold of the key's integrated updates at the seq that the base covers, `key`
 * (05 [STATE-74]). Returns null when the base is valid, and a BaseError when it is not. The base is valid when, read as one
 * Yjs update:
 *  - it is well formed (05 [STATE-71]), under `roots`, the crdt_roots of the key that holds it. (The root conditions are judged under
 *    the roots that the caller passes: they only grow, so the caller passes those of the version in force at or after the covered seq.);
 *  - it has no pending structs: applied alone to an empty document it is not a no-op for good (05 [STATE-76]);
 *  - its state vector equals the key's;
 *  - its delete set, normalized (05 [STATE-41]), equals the key's canonical delete set;
 *  - a GC stands only in place of an id that a GC first defines in the covered updates, or of an id whose parent is hidden, which is
 *    the part of the content rule that the key's fold can check (05 [STATE-74], Q7 ruled: an item whose origin is a GC has a hidden
 *    parent, so a GC may stand in place of it);
 *  - and the rest of that rule, as far as the key's fold can check it: each id holds the kind, the content ref and the parent of its
 *    first definition (05 [STATE-76]: first definition wins), or a ContentDeleted in its place for an id that the canonical delete set
 *    holds. A base that has an item where the key's first definition is a GC, or another content ref or another parent than the first
 *    definition has, gives a party that loads it other hidden ids and other parents than every party that folds the updates ("whatever
 *    base it holds", 05 [STATE-76]).
 *
 * The fold keeps of a first definition its kind, content ref and parent, not its bytes, so that two items that differ only in their text
 * or values are not told apart here. A server MAY compare the content itself, by loading the base and the covered updates with one Yjs
 * implementation (05 [STATE-74]); that is no part of the spec's validity conditions and is not done here. For the value of a SYSTEM
 * `SET` (06 [LOG-40]), `key` is the fold of the renamed-from key.
 */
export function validateBase(base: Uint8Array, roots: Roots, key: Doc): BaseError | null {
  let u: Update;
  try {
    u = parseUpdate(base);
    checkRoots(u, roots);
  } catch (e) {
    if (e instanceof CrdtError) return baseError("not-well-formed", e.reason);
    throw e;
  }
  const empty = new Doc();
  const own = new Analysis(empty, u); // the base read alone: the parents that its items take
  const pending = noOpForGood(empty, own);
  if (pending !== undefined) return baseError("pending", pending);
  if (!sameStateVector(u.stateVector(), key.stateVector())) return baseError("state-vector");
  if (!sameDeleteSet(u.deletions(), key.deleteSet())) return baseError("delete-set");
  return checkBaseContent(u, own, key);
}

/**
 * Checks each struct of the base against the first definitions of the key's fold (05 [STATE-74], the content rule): the GC rule, and
 * the kind, content ref and parent of the others. The state vectors are equal, so every client of the base is the key's.
 */
function checkBaseContent(u: Update, own: Analysis, key: Doc): BaseError | null {
  const hid = new Analysis(key, null); // hidden ids, in the key
  for (const g of u.groups) {
    const defs = key.defs.get(g.client);
    if (defs === undefined) continue;
    const ds = key.ds.get(g.client) ?? [];
    for (const s of g.structs) {
      const lo = s.clock;
      const hi = s.clock + s.len;
      // the definitions of the key that hold a clock of [lo, hi): they are contiguous and ascending, so the first is a search away
      for (let i = firstEndingAbove(defs, lo); i < defs.length; i++) {
        const d = defs[i] as Def;
        if (d.start >= hi) break;
        if (s.kind === "gc") {
          // 05 [STATE-74]: a GC stands in place of an id that a GC defines, or whose parent is hidden. An item whose origin is a GC has
          // a hidden parent (05 [STATE-76]: "hidden when the origin is a `GC`"), so it allows one too.
          //
          // SPEC-QUESTION (ruled, Q7): 05 [STATE-74] "or a `GC` in its place for an id whose parent is hidden", with [STATE-76]: "an item
          // that encodes none has the parent of its origin when it has an origin (hidden when the origin is a `GC`)". Both reference
          // models (runner-yjs/lib/crdt.mjs gcAllowed, runner-yrs/abpc/crdt.py check_base) read only the parent id that the definition
          // carries, which an item whose origin is a GC has not, and refuse the GC; no vector has the case. Ruled by the coordinator
          // (2026-10-31): reading (a), the text. The reference models will be fixed in spec round 16.
          if (!(d.kind === "gc" || d.gcParent || (d.hasParent && d.parentId !== null && hid.hidden(d.parentId)))) return baseError("gc-not-allowed");
        } else if (!baseItemHolds(own, hid, s, d, ds, Math.max(lo, d.start), Math.min(hi, d.end))) {
          // an item of the base, over a definition of the key: only the overlap of the two is judged
          return baseError("content");
        }
      }
    }
  }
  return null;
}

/** The index of the first definition whose end is above clock (the definitions are contiguous and ascending). */
function firstEndingAbove(defs: readonly Def[], clock: number): number {
  let lo = 0;
  let hi = defs.length;
  while (lo < hi) {
    const m = (lo + hi) >>> 1;
    if ((defs[m] as Def).end > clock) hi = m;
    else lo = m + 1;
  }
  return lo;
}

/**
 * Whether the item s of a base holds, over the ids [lo, hi) that the key's definition d covers, what d holds (05 [STATE-74]): the same
 * content ref, or a ContentDeleted where the canonical delete set holds every id, and the same parent. An item over a GC does not.
 * `own` answers for the base, `hid` for the key.
 */
function baseItemHolds(own: Analysis, hid: Analysis, s: Struct, d: Def, ds: readonly Range[], lo: number, hi: number): boolean {
  if (d.kind === "gc") return false; // the id's first definition is a GC, which hides it: an item there shows it
  if (s.content !== d.content && !(s.content === ContentRef.Deleted && rangesHold(ds, lo, hi))) return false;
  // The parent that the item takes, against the parent of the first definition (05 [STATE-76]: the same for every party, whatever base
  // it holds). A parent that a GC origin hides on both sides is the same hidden parent whichever item the chain ends at. On one side
  // only, the item is hidden for the party that folds the updates and shown for the party that loads the base, or the other way round,
  // unless the parent that the other side names is hidden too: then both hide the item, and it matters no more which hidden parent it has.
  const p = own.parentOf(s);
  if (p.gc && d.gcParent) return true;
  if (p.gc) return d.hasParent && d.parentId !== null && hid.hidden(d.parentId);
  if (d.gcParent) return p.hasId && p.id !== null && hid.hidden(p.id);
  if (p.hasId !== d.hasParent) return false;
  return !p.hasId || (p.id !== null && d.parentId !== null && p.id.client === d.parentId.client && p.id.clock === d.parentId.clock);
}

/** Whether the normalized ranges hold every clock of [lo, hi). */
function rangesHold(rs: readonly Range[], lo: number, hi: number): boolean {
  let a = 0;
  let b = rs.length;
  while (a < b) {
    const m = (a + b) >>> 1;
    const r = rs[m] as Range;
    if (r.clock + r.len > lo) b = m;
    else a = m + 1;
  }
  const r = rs[a];
  return r !== undefined && r.clock <= lo && hi <= r.clock + r.len;
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
