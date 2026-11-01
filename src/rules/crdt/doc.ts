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

/**
 * The first definition of a run of ids: one or more structs of an integrated update with the same kind, content ref and parent.
 * Adjacent structs that agree are one definition, so that a run of GCs, or of the items of one text, costs one (02 [ARC-46]: a key's
 * memory follows the shape of its document, not the number of its structs). A Def is never changed once a Doc holds it.
 */
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
  /**
   * the item takes its parent from a GC: its origin or right origin is a GC, an item whose parent is hidden, or an item that takes its
   * parent from a GC (05 [STATE-76]); it is hidden
   */
  gcParent: boolean;
  /** the name of the root that the parent is, when it is a root */
  root: string | null;
}

/** Whether e, which starts where d ends, can join d: the ids agree on everything that a reader of the definitions asks. */
function sameRun(d: Def, e: Def): boolean {
  return (
    d.end === e.start &&
    d.units === null &&
    e.units === null &&
    d.kind === e.kind &&
    d.content === e.content &&
    d.hasParent === e.hasParent &&
    d.gcParent === e.gcParent &&
    d.root === e.root &&
    sameId(d.parentId, e.parentId)
  );
}

function sameId(a: Id | null, b: Id | null): boolean {
  return a === null || b === null ? a === b : a.client === b.client && a.clock === b.clock;
}

export class Doc {
  /** Whether the key has received a committed update, a no-op for good included (05 [STATE-76]). */
  present = false;
  /** client → next clock */
  readonly sv: Map<number, number>;
  /** client → the canonical delete ranges (05 [STATE-41]); an array is replaced, never edited, so a clone shares it */
  readonly ds: Map<number, Range[]>;
  /** client → the first definitions below sv, in ascending clock order */
  readonly defs: Map<number, Def[]>;
  /** The clients whose `defs` array this Doc made, and may change in place; the arrays of the others are shared with a clone and are copied before a write. */
  private owned = new Set<number>();

  constructor() {
    this.sv = new Map();
    this.ds = new Map();
    this.defs = new Map();
  }

  /**
   * Returns a Doc that holds what this one holds and that changes independently of it: an update applied to either does not show in
   * the other (02 [ARC-28]: the state that a reader holds does not move under it). It costs the key's clients, not the size of its
   * document: the delete ranges and the definitions of a client are shared, and the definitions of a client are copied only when an
   * update adds to them. The fold clones the Doc of a key before it applies an update to it, so that the Doc that a reader of the
   * state holds stays as it was.
   */
  clone(): Doc {
    const c = new Doc();
    c.present = this.present;
    for (const [client, next] of this.sv) c.sv.set(client, next);
    for (const [client, rs] of this.ds) c.ds.set(client, rs);
    for (const [client, ds] of this.defs) c.defs.set(client, ds);
    this.owned = new Set(); // the arrays that the clone shares are no longer this Doc's to change in place
    return c;
  }

  /** Adds the first definition of a run of ids at the end of the client's list, or extends the one before it when the two agree. */
  private addDef(client: number, d: Def): void {
    let list = this.defs.get(client);
    if (list === undefined) {
      list = [];
      this.defs.set(client, list);
      this.owned.add(client);
    } else if (!this.owned.has(client)) {
      list = list.slice();
      this.defs.set(client, list);
      this.owned.add(client);
    }
    const last = list[list.length - 1];
    if (last !== undefined && sameRun(last, d)) list[list.length - 1] = { ...last, end: d.end };
    else list.push(d);
  }

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
   * when it has a gap, a partly present struct, a Skip, a missing dependency, a parent that is not a type, an item whose origin and
   * right origin are items with different parents, or when it would split a surrogate pair; a no-op for good integrates no part of itself. Otherwise it is integrated so that the first
   * definition wins: a struct wholly below its client's next clock is ignored. The canonical form of 05 [STATE-41] then grows
   * by the update's state vector and delete set, and the verdict is APPLIED when the form changed and NO_EFFECT when it did not.
   */
  applyUpdate(u: Update): Result {
    this.present = true;
    const a = new Analysis(this, u);
    const reason = noOpForGood(this, a);
    if (reason !== undefined) return { verdict: "NOOP_FOR_GOOD", reason };

    // The first definitions, with the parents resolved against the state before the update. Adjacent structs of a client that agree
    // are one definition, so a run of GCs is one entry however many structs it has.
    const news: { client: number; def: Def }[] = [];
    for (const s of u.all) {
      if (s.clock + s.len <= this.next(s.client)) continue; // wholly below the next clock: the ids keep their earlier definition
      const p = a.parentOf(s);
      const def: Def = {
        start: s.clock,
        end: s.clock + s.len,
        kind: s.kind,
        content: s.content,
        units: s.units,
        parentId: p.id,
        hasParent: p.hasId,
        gcParent: p.gc,
        root: p.root,
      };
      const last = news[news.length - 1];
      if (last !== undefined && last.client === s.client && sameRun(last.def, def)) last.def.end = def.end; // last.def is this call's own
      else news.push({ client: s.client, def });
    }
    for (const n of news) this.addDef(n.client, n.def);

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

/** The parent that a struct takes: an item (hasId), a root, or none. */
interface ParentRef {
  id: Id | null;
  hasId: boolean;
  /** the name of the root, when the parent is a root */
  root: string | null;
  /** the item takes its parent from a GC (05 [STATE-76]): it takes none, and is hidden */
  gc: boolean;
}

const NO_PARENT: ParentRef = { id: null, hasId: false, root: null, gc: false };
const GC_PARENT: ParentRef = { id: null, hasId: false, root: null, gc: true };

/**
 * Whether two parents are the same one in the sense of 05 [STATE-76]: the same root, or the same item. A struct that takes no parent
 * counts as having the same parent as any other, since the other rules of the no-op decide it.
 */
function sameParent(a: ParentRef, b: ParentRef): boolean {
  if ((!a.hasId && a.root === null) || (!b.hasId && b.root === null)) return true;
  if (a.root !== null || b.root !== null) return a.root === b.root;
  return a.id !== null && b.id !== null && a.id.client === b.id.client && a.id.clock === b.id.clock;
}

/** What 05 [STATE-76] asks of the struct that the origin or the right origin of an item names. */
interface Neighbour {
  /** the parent that it takes */
  parent: ParentRef;
  /** it is a GC, an item whose parent is hidden, or an item that takes its parent from a GC: the three count alike */
  hides: boolean;
  /** some struct defines the id */
  found: boolean;
}

const NOT_FOUND: Neighbour = { parent: NO_PARENT, hides: false, found: false };
const GC_NEIGHBOUR: Neighbour = { parent: NO_PARENT, hides: true, found: true };

/** The states of an item of the update in `Analysis.state`. */
const ITEM_KNOWN = 1;
const ITEM_ON_PATH = 2;

/** An item that `resolve` is working on: the ids that it names, and how many of them it has looked at. */
interface ResolveFrame {
  s: Struct;
  ids: Id[];
  k: number;
}

/**
 * Answers the questions of 05 [STATE-76] about one update against the state of a Doc. The questions are asked in the order of
 * the rules, and each answer is kept, so that the work of one update stays linear in its size.
 */
export class Analysis {
  private readonly implied: ParentRef[];
  private readonly state: number[];
  private readonly hmemo = new Map<string, boolean>();

  /** u is null when the questions are about the Doc alone. */
  constructor(
    private readonly doc: Doc,
    readonly u: Update | null,
  ) {
    this.implied = u === null ? [] : new Array<ParentRef>(u.all.length).fill(NO_PARENT);
    this.state = u === null ? [] : new Array<number>(u.all.length).fill(0);
  }

  /**
   * The parent that s takes (05 [STATE-76]): the one it encodes. An item that encodes none takes none, and is hidden, when its origin
   * or its right origin is a GC, an item whose parent is hidden, or an item that takes its parent from a GC. Otherwise it takes the
   * parent of its origin when it has one, and else that of its right origin; an item that has both takes the parent that they share,
   * since one whose origin and right origin have different parents is a no-op for good (noOpForGood finds it, and the update is
   * folded no further). A GC has none.
   */
  parentOf(s: Struct): ParentRef {
    if (s.kind !== "item") return NO_PARENT;
    if (s.parentKind === ParentKind.Item) return { id: s.parentId, hasId: true, root: null, gc: false };
    if (s.parentKind === ParentKind.Root) return { id: null, hasId: false, root: s.root, gc: false };
    if (((this.state[s.idx] as number) & ITEM_KNOWN) === 0) this.resolve(s);
    return this.implied[s.idx] as ParentRef;
  }

  /**
   * Finds the parent of the item s and of each item that it depends on and that is not known yet: the items that s names (its origin,
   * its right origin and its parent), those that they name, and so on, each before the item that names it. That is every item whose
   * definition the answer for s reads: the parent of a neighbour, and whether the neighbour's parent is hidden, which follows the
   * parents up. The references of an update have no cycle (05 [STATE-71], References), so the order exists. The walk keeps its own
   * stack, so that a chain as long as the update costs the update's time and no recursion (02 [ARC-46]).
   */
  private resolve(s: Struct): void {
    const u = this.u as Update;
    const stack: ResolveFrame[] = [];
    const push = (t: Struct): void => {
      this.state[t.idx] = (this.state[t.idx] as number) | ITEM_ON_PATH;
      const ids: Id[] = [];
      if (t.hasOrigin) ids.push(t.origin);
      if (t.hasRight) ids.push(t.right);
      if (t.parentKind === ParentKind.Item) ids.push(t.parentId);
      stack.push({ s: t, ids, k: 0 });
    };
    push(s);
    while (stack.length > 0) {
      const top = stack[stack.length - 1] as ResolveFrame;
      if (top.k < top.ids.length) {
        const id = top.ids[top.k++] as Id;
        if (id.clock >= this.doc.next(id.client)) {
          // below it, the Doc's definition of the id says it all
          const t = u.find(id);
          if (t !== undefined && t.kind === "item" && this.state[t.idx] === 0) push(t);
        }
        continue;
      }
      this.implied[top.s.idx] = this.compute(top.s);
      this.state[top.s.idx] = ITEM_KNOWN;
      stack.pop();
    }
  }

  /** `parentOf` for an item whose neighbours' answers are known. */
  private compute(s: Struct): ParentRef {
    if (s.parentKind === ParentKind.Item) return { id: s.parentId, hasId: true, root: null, gc: false };
    if (s.parentKind === ParentKind.Root) return { id: null, hasId: false, root: s.root, gc: false };
    let via = NO_PARENT; // the parent of the origin, else of the right origin
    let hides = false;
    if (s.hasOrigin) {
      const n = this.neighbourAt(s.origin);
      via = n.parent;
      hides = n.hides;
    }
    if (s.hasRight) {
      const n = this.neighbourAt(s.right);
      if (!s.hasOrigin) via = n.parent;
      hides = hides || n.hides;
    }
    return hides ? GC_PARENT : via;
  }

  /**
   * The neighbour that the id names: its definition among the key's integrated updates when id is below the next clock of its client,
   * and otherwise the struct of the update, whose parent is known when this is called by the walk of `resolve` or after it.
   */
  neighbourAt(id: Id): Neighbour {
    if (id.clock < this.doc.next(id.client)) {
      const d = this.doc.defAt(id);
      if (d === undefined) return NOT_FOUND;
      if (d.kind === "gc") return GC_NEIGHBOUR;
      return {
        found: true,
        parent: { id: d.parentId, hasId: d.hasParent, root: d.root, gc: false },
        hides: d.gcParent || (d.hasParent && d.parentId !== null && this.hidden(d.parentId)),
      };
    }
    if (this.u === null) return NOT_FOUND;
    const t = this.u.find(id);
    if (t === undefined) return NOT_FOUND;
    if (t.kind === "gc") return GC_NEIGHBOUR;
    const p = this.parentOf(t);
    return { found: true, parent: p, hides: p.gc || (p.hasId && p.id !== null && this.hidden(p.id)) };
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
      root: p.root,
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
  // One of its items has both an origin and a right origin that are items with different parents, none of them a GC, an item whose
  // parent is hidden, or an item that takes its parent from a GC (the three count alike, so that the answer is the same whatever base a
  // party holds, 05 [STATE-74]). Such an item is no part of any document.
  for (const s of u.all) {
    if (s.kind !== "item" || !s.hasOrigin || !s.hasRight) continue;
    const o = a.neighbourAt(s.origin);
    if (!o.found || o.hides) continue;
    const r = a.neighbourAt(s.right);
    if (r.found && !r.hides && !sameParent(o.parent, r.parent)) return "cross-parent";
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
