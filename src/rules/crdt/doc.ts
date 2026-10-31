// The fold state of one CRDT key (05 §5.10): what the key's integrated updates define (05 [STATE-76]), the canonical form
// SV || DS of 05 [STATE-41], and the verdict of each committed update.
//
// Nothing here reads a document's content: no root name, no text and no JSON form of a document enters the canonical form,
// so the state hash of a key is the same whatever base holds it. A client that shows the document keeps a Yjs document of
// its own (14 [CLI-13]); this fold is what the per-key state hash and the version-gated apply read.

import { ByteWriter } from "../bytes.js";
import { CrdtError, type Reason } from "./errors.js";
import { checkRoots, type Roots } from "./roots.js";
import {
  type ClientClock,
  type ClientRanges,
  type Id,
  normalizeRanges,
  type Range,
  type StructKind,
  type Update,
  parseUpdate,
  ParentKind,
  type Struct,
} from "./parse.js";

/** What a committed CRDT update comes to (05 [STATE-71], [STATE-76]; the vector README, `crdt-fold`). */
export type Verdict =
  /** The update is not well formed (05 [STATE-71]): rejected at commit, never appended; the fold does not see it. */
  | "SCHEMA_INVALID"
  /** The update is committed and never integrated (05 [STATE-76]). Nothing of it is kept, then or later. */
  | "NOOP_FOR_GOOD"
  /** The update is integrated and leaves the canonical form unchanged. */
  | "NO_EFFECT"
  /** The update is integrated and changes the canonical form. */
  | "APPLIED";

/** Whether the update entered the key's integrated updates. */
export function integrated(v: Verdict): boolean {
  return v === "NO_EFFECT" || v === "APPLIED";
}

/**
 * The outcome of folding one update. `reason` names the failed check of a SCHEMA_INVALID update (05 [STATE-71]) and the rule
 * that made a NOOP_FOR_GOOD update a no-op (05 [STATE-76]); it is absent otherwise. `error` is the refusal of a SCHEMA_INVALID update.
 */
export interface Result {
  verdict: Verdict;
  reason?: Reason;
  error?: CrdtError;
}

/** The first definition of a run of ids: one struct of an integrated update, whole. */
export interface Def {
  /** the ids [start, end) */
  start: number;
  end: number;
  /** an Item or a GC */
  kind: StructKind;
  content: number;
  /** the units of a ContentString that holds a surrogate pair */
  units: Uint16Array | null;
  /** the id of the parent item, when hasParent */
  parentId: Id | null;
  /** the parent that the struct takes is an item */
  hasParent: boolean;
  /** the item takes its parent from an origin that is a GC, so its parent is hidden (05 [STATE-76]) */
  gcParent: boolean;
}

export class Doc {
  /** Whether the key has received a committed update, a no-op for good included (05 [STATE-76]). */
  present = false;
  /** client → next clock */
  readonly sv = new Map<number, number>();
  readonly ds = new Map<number, Range[]>();
  /** client → the first definitions below sv, in ascending clock order */
  readonly defs = new Map<number, Def[]>();

  /** The next clock of a client: the highest clock that a struct of the client in an integrated update covers, plus 1. */
  next(client: number): number {
    return this.sv.get(client) ?? 0;
  }

  /** The definition that holds id, which must be below the client's next clock. */
  defAt(id: Id): Def | undefined {
    const ds = this.defs.get(id.client);
    if (ds === undefined) return undefined;
    let lo = 0;
    let hi = ds.length;
    while (lo < hi) {
      const m = (lo + hi) >>> 1;
      if ((ds[m] as Def).end > id.clock) hi = m;
      else lo = m + 1;
    }
    const d = ds[lo];
    return d === undefined || d.start > id.clock ? undefined : d;
  }

  /** Whether the canonical delete set holds id. */
  inDeleteSet(id: Id): boolean {
    const rs = this.ds.get(id.client);
    if (rs === undefined) return false;
    let lo = 0;
    let hi = rs.length;
    while (lo < hi) {
      const m = (lo + hi) >>> 1;
      const r = rs[m] as Range;
      if (r.clock + r.len > id.clock) hi = m;
      else lo = m + 1;
    }
    const r = rs[lo];
    return r !== undefined && r.clock <= id.clock;
  }

  /** The key's state vector over its integrated updates (05 [STATE-41]), in ascending order of client id. */
  stateVector(): ClientClock[] {
    return [...this.sv.entries()].map(([client, clock]) => ({ client, clock })).sort((a, b) => a.client - b.client);
  }

  /**
   * The key's canonical delete set (05 [STATE-41]): the normalized union of the delete sets of its integrated updates, in
   * ascending order of client id, with no client that has no range.
   */
  deleteSet(): ClientRanges[] {
    return [...this.ds.entries()]
      .filter(([, rs]) => rs.length > 0)
      .map(([client, rs]) => ({ client, ranges: rs.map((r) => ({ ...r })) }))
      .sort((a, b) => a.client - b.client);
  }

  /** The canonical form of the key, SV || DS (05 [STATE-41]): the content of its per-key state hash. */
  canonical(): Uint8Array {
    return canonicalForm(this.stateVector(), this.deleteSet());
  }

  /**
   * The commit of an update: checks b as 05 [STATE-71] says, under `roots` (the template's crdt_roots under the manifest
   * version in force for the record; none skips the root conditions), and folds it as 05 [STATE-76] says. A malformed update
   * gives the verdict SCHEMA_INVALID, with the refusal, and leaves the Doc as it was: such an update is rejected at commit and
   * never reaches the fold. Every other update, a no-op for good included, makes the key present.
   */
  apply(b: Uint8Array, roots?: Roots): Result {
    let u: Update;
    try {
      u = parseUpdate(b);
      if (roots !== undefined) checkRoots(u, roots);
    } catch (e) {
      if (e instanceof CrdtError) return { verdict: "SCHEMA_INVALID", reason: e.reason, error: e };
      throw e;
    }
    return this.applyUpdate(u);
  }

  /**
   * Folds the update of a record that is already committed onto the Doc: it reads b as `parseUpdate` does and applies it as
   * `applyUpdate` does. It checks no root, since the root conditions of 05 [STATE-71] are judged once, at commit, under the
   * manifest version in force for that record, and a later fold of the log, a rebuild or a client's must not judge them again
   * under another manifest. An update that does not parse was never committed: the result is SCHEMA_INVALID and the Doc is as it was.
   */
  fold(b: Uint8Array): Result {
    return this.apply(b, undefined);
  }

  /**
   * Folds an update that `parseUpdate` has read, and whose roots the caller has checked at commit, onto the Doc.
   *
   * 05 [STATE-76]: the update is checked against the state vector SV of the key's integrated updates. It is a no-op for good
   * when it has a gap, a partly present struct, a Skip, a missing dependency, a parent that is not a type, or when it would
   * split a surrogate pair; a no-op for good integrates no part of itself. Otherwise it is integrated so that the first
   * definition wins: a struct wholly below its client's next clock is ignored. The canonical form of 05 [STATE-41] then grows
   * by the update's state vector and delete set, and the verdict is APPLIED when the form changed and NO_EFFECT when it did not.
   */
  applyUpdate(u: Update): Result {
    this.present = true;
    const a = new Analysis(this, u);
    const reason = noOpForGood(this, a);
    if (reason !== undefined) return { verdict: "NOOP_FOR_GOOD", reason };

    // The first definitions, with the parents resolved against the state before the update.
    const news: { client: number; def: Def }[] = [];
    for (const s of u.all) {
      if (s.clock + s.len <= this.next(s.client)) continue; // wholly below the next clock: the ids keep their earlier definition
      const p = a.parentOf(s);
      news.push({
        client: s.client,
        def: {
          start: s.clock,
          end: s.clock + s.len,
          kind: s.kind,
          content: s.content,
          units: s.units,
          parentId: p.id,
          hasParent: p.hasId,
          gcParent: p.gc,
        },
      });
    }
    for (const n of news) {
      let ds = this.defs.get(n.client);
      if (ds === undefined) {
        ds = [];
        this.defs.set(n.client, ds);
      }
      ds.push(n.def);
    }

    let changed = false;
    for (const g of u.groups) {
      const last = g.structs[g.structs.length - 1] as Struct;
      const end = last.clock + last.len;
      if (end > this.next(g.client)) {
        this.sv.set(g.client, end);
        changed = true;
      }
    }
    for (const dg of u.deletes) {
      const { union, changed: ch } = unionRanges(this.ds.get(dg.client) ?? [], dg.ranges);
      if (ch) {
        this.ds.set(dg.client, union);
        changed = true;
      }
    }
    return { verdict: changed ? "APPLIED" : "NO_EFFECT" };
  }
}

/**
 * Encodes SV || DS. SV is u32(n) and, for each client in ascending order, u64(client) || u64(next clock). DS is u32(m) and,
 * for each client in ascending order, u64(client) || u32(r) and r ranges of u64(clock) || u64(length).
 */
export function canonicalForm(sv: readonly ClientClock[], ds: readonly ClientRanges[]): Uint8Array {
  const w = new ByteWriter().u32(sv.length);
  for (const e of sv) w.u64(e.client).u64(e.clock);
  w.u32(ds.length);
  for (const c of ds) {
    w.u64(c.client).u32(c.ranges.length);
    for (const r of c.ranges) w.u64(r.clock).u64(r.len);
  }
  return w.finish();
}

/** The normalized union of `cur`, which is normalized, and `add`, which is any list, and whether it differs from `cur`. */
export function unionRanges(cur: readonly Range[], add: readonly Range[]): { union: Range[]; changed: boolean } {
  const a = normalizeRanges(add);
  const union: Range[] = [];
  let i = 0;
  let j = 0;
  const push = (r: Range): void => {
    const last = union[union.length - 1];
    if (last !== undefined && r.clock <= last.clock + last.len) {
      const end = r.clock + r.len;
      if (end > last.clock + last.len) last.len = end - last.clock;
      return;
    }
    union.push({ clock: r.clock, len: r.len });
  };
  while (i < cur.length || j < a.length) {
    if (j === a.length || (i < cur.length && (cur[i] as Range).clock <= (a[j] as Range).clock)) {
      push(cur[i] as Range);
      i++;
    } else {
      push(a[j] as Range);
      j++;
    }
  }
  if (union.length !== cur.length) return { union, changed: true };
  for (let k = 0; k < union.length; k++) {
    const u = union[k] as Range;
    const c = cur[k] as Range;
    if (u.clock !== c.clock || u.len !== c.len) return { union, changed: true };
  }
  return { union: cur as Range[], changed: false };
}

// ---- the questions of 05 [STATE-76] ----

/** The parent that a struct takes: an item (hasId), or a root or none. */
interface ParentRef {
  id: Id | null;
  hasId: boolean;
  /** the parent is hidden because it comes from an origin that is a GC */
  gc: boolean;
}

const NO_PARENT: ParentRef = { id: null, hasId: false, gc: false };

/**
 * Answers the questions of 05 [STATE-76] about one update against the state of a Doc. The questions are asked in the order of
 * the rules, and each answer is kept, so that the work of one update stays linear in its size.
 */
export class Analysis {
  private readonly implied: ParentRef[];
  private readonly known: boolean[];
  private readonly hmemo = new Map<string, boolean>();

  /** u is null when the questions are about the Doc alone. */
  constructor(
    private readonly doc: Doc,
    readonly u: Update | null,
  ) {
    this.implied = u === null ? [] : new Array<ParentRef>(u.all.length).fill(NO_PARENT);
    this.known = u === null ? [] : new Array<boolean>(u.all.length).fill(false);
  }

  /**
   * The parent that s takes (05 [STATE-76]): the one it encodes; for an item that encodes none, the parent of its origin when it
   * has an origin, and otherwise that of its right origin (hidden when that struct is a GC); the right origin of an item that
   * has an origin takes no part. A GC has none. The chain of origins is followed without recursion, and every struct on it
   * takes the same answer.
   */
  parentOf(s: Struct): ParentRef {
    if (s.kind !== "item") return NO_PARENT;
    if (s.parentKind === ParentKind.Item) return { id: s.parentId, hasId: true, gc: false };
    if (s.parentKind === ParentKind.Root) return NO_PARENT;
    if (this.known[s.idx] === true) return this.implied[s.idx] as ParentRef;
    const u = this.u as Update;
    const path: number[] = [];
    let res: ParentRef = NO_PARENT;
    let cur = s;
    for (;;) {
      path.push(cur.idx);
      const via = cur.hasOrigin ? cur.origin : cur.right;
      if (via.clock < this.doc.next(via.client)) {
        const d = this.doc.defAt(via);
        if (d !== undefined) res = { id: d.parentId, hasId: d.hasParent, gc: d.kind === "gc" || d.gcParent };
        break;
      }
      const t = u.find(via);
      if (t === undefined) break;
      if (t.kind === "gc") {
        res = { id: null, hasId: false, gc: true };
        break;
      }
      if (t.parentKind === ParentKind.Item) {
        res = { id: t.parentId, hasId: true, gc: false };
        break;
      }
      if (t.parentKind === ParentKind.Root) break;
      if (this.known[t.idx] === true) {
        res = this.implied[t.idx] as ParentRef;
        break;
      }
      if (path.length > u.all.length) break; // no chain of an acyclic update is longer than the update
      cur = t;
    }
    for (const i of path) {
      this.implied[i] = res;
      this.known[i] = true;
    }
    return res;
  }

  /** The first definition of id: among the key's integrated updates when id is below the next clock of its client, and otherwise in the update. */
  defOf(id: Id): Def | undefined {
    if (id.clock < this.doc.next(id.client)) return this.doc.defAt(id);
    if (this.u === null) return undefined;
    const s = this.u.find(id);
    if (s === undefined) return undefined;
    const p = this.parentOf(s);
    return {
      start: s.clock,
      end: s.clock + s.len,
      kind: s.kind,
      content: s.content,
      units: s.units,
      parentId: p.id,
      hasParent: p.hasId,
      gcParent: p.gc,
    };
  }

  /**
   * Whether id is hidden (05 [STATE-76]): the canonical delete set holds it, a GC defines it, or the item that defines it has a
   * hidden parent. It follows the chain of parent items without recursion, and keeps its answers.
   */
  hidden(id: Id): boolean {
    const chain: string[] = [];
    let res = false;
    let cur = id;
    for (;;) {
      const key = `${cur.client}:${cur.clock}`;
      const v = this.hmemo.get(key);
      if (v !== undefined) {
        res = v;
        break;
      }
      this.hmemo.set(key, false); // provisional, so that a chain that came back to itself answers false
      chain.push(key);
      if (this.doc.inDeleteSet(cur)) {
        res = true;
        break;
      }
      const d = this.defOf(cur);
      if (d === undefined) break;
      if (d.kind === "gc" || d.gcParent) {
        res = true;
        break;
      }
      if (!d.hasParent || d.parentId === null) break;
      cur = d.parentId;
    }
    for (const k of chain) this.hmemo.set(k, res);
    return res;
  }

  /**
   * The UTF-16 code unit at id when a ContentString defines it and the text holds a surrogate pair. For any other id no unit is
   * a surrogate, and it returns undefined.
   */
  unitAt(id: Id): number | undefined {
    const d = this.defOf(id);
    if (d === undefined || d.kind !== "item" || d.content !== 4 || d.units === null) return undefined;
    return d.units[id.clock - d.start];
  }
}

const isHigh = (u: number | undefined): boolean => u !== undefined && u >= 0xd800 && u <= 0xdbff;
const isLow = (u: number | undefined): boolean => u !== undefined && u >= 0xdc00 && u <= 0xdfff;

/** The reason that the update is a deterministic no-op for good (05 [STATE-76]), or undefined. The rules are tried in the order that the text lists them. */
export function noOpForGood(d: Doc, a: Analysis): Reason | undefined {
  const u = a.u as Update;
  // Its structs of some client start above that client's next clock.
  for (const g of u.groups) {
    if (g.clock > d.next(g.client)) return "gap";
  }
  // One of its structs is only partly below its client's next clock.
  for (const s of u.all) {
    const next = d.next(s.client);
    if (s.clock < next && next < s.clock + s.len) return "partial";
  }
  // It holds a Skip struct, wherever the Skip stands.
  for (const s of u.all) {
    if (s.kind === "skip") return "skip";
  }
  // The origin, right origin or parent item of one of its items, or an id in a range of its delete set, is not present. An id is
  // present when it is below its client's next clock, or when the update defines it by an earlier struct of the same group or by
  // a struct of another group.
  for (const s of u.all) {
    if (s.kind !== "item") continue;
    const ids: Id[] = [];
    if (s.hasOrigin) ids.push(s.origin);
    if (s.hasRight) ids.push(s.right);
    if (s.parentKind === ParentKind.Item) ids.push(s.parentId);
    for (const id of ids) {
      if (id.clock < d.next(id.client)) continue;
      const t = u.find(id);
      if (t === undefined || (id.client === s.client && t.clock >= s.clock)) return "missing";
    }
  }
  for (const dg of u.deletes) {
    let top = d.next(dg.client); // ids below top are present
    const gi = u.groupIndex(dg.client);
    if (gi >= 0) {
      const g = u.groups[gi] as (typeof u.groups)[number];
      const last = g.structs[g.structs.length - 1] as Struct;
      const end = last.clock + last.len;
      if (end > top && g.clock <= top) top = end;
    }
    for (const r of dg.ranges) {
      if (r.clock + r.len > top) return "missing";
    }
  }
  // One of its items names as its parent an id that is not hidden and that a struct other than a ContentType item defines.
  for (const s of u.all) {
    if (s.kind !== "item" || s.parentKind !== ParentKind.Item) continue;
    const pd = a.defOf(s.parentId);
    if (!(pd !== undefined && pd.kind === "item" && pd.content === 7) && !a.hidden(s.parentId)) return "non-type-parent";
  }
  // It would split a surrogate pair whose ids are not hidden: the origin of one of its items is the first code unit of the pair,
  // the right origin of one of its items is the second, or a range of its delete set starts at the second or ends at the first.
  for (const s of u.all) {
    if (s.kind !== "item") continue;
    if (s.hasOrigin && isHigh(a.unitAt(s.origin)) && !a.hidden(s.origin)) return "surrogate";
    if (s.hasRight && isLow(a.unitAt(s.right)) && !a.hidden(s.right)) return "surrogate";
  }
  for (const dg of u.deletes) {
    for (const r of dg.ranges) {
      const first: Id = { client: dg.client, clock: r.clock };
      const last: Id = { client: dg.client, clock: r.clock + r.len - 1 };
      if (isLow(a.unitAt(first)) && !a.hidden(first)) return "surrogate";
      if (isHigh(a.unitAt(last)) && !a.hidden(last)) return "surrogate";
    }
  }
  return undefined;
}
