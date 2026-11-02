// The document length of a CRDT key (05 [STATE-74], round 17, D-AP (a)): "the length in octets of the key's document at that seq, the
// Yjs v1 update that the following steps give from the key's integrated updates at that seq alone, whatever base the key holds". The
// steps are Groups, Content, Cuts, Merges and Delete set; this file is the whole of the counting (the twin of the Go
// server/internal/crdt/measure.go and content.go, without their caches: the client measures a key's document when it is asked, from
// the first definitions that its Doc keeps, and keeps no measure of its own).
//
// What a Doc keeps for it, per client, in the order of the clocks (doc.ts):
//   - the first definitions as the document sees them (MRun): one struct of an integrated update, or the structs that the Merges step
//     joins and that the Doc keeps as one, with the octets of the header of the first and of the content of all, in the canonical form;
//   - for a run of text or of values whose ids take different numbers of octets, their profile (Profile), so that a cut inside it, at the
//     edge of a delete range, is measured without the text.
//
// The cover of an id, which decides whether the document holds it as a GC, as ContentDeleted or as its content, is a function of the
// definitions and of the delete set: a GC where a GC first defines it, where it takes its parent from a GC or where its parent is hidden
// (05 [STATE-76]); ContentDeleted where the canonical delete set holds it.

import type { Id, Range } from "./parse.js";

// ---- the sizes of lib0 values ----

/** The length of the lib0 varUint of n. */
export function uvSize(n: number): number {
  if (n < 128) return 1;
  let s = 1;
  while (n > 127) {
    n = Math.floor(n / 128);
    s++;
  }
  return s;
}

/** The length of an id in an update: its client and its clock, each a varUint. */
export function idSize(id: Id): number {
  return uvSize(id.client) + uvSize(id.clock);
}

/** The length of a lib0 string of n octets: a varUint of n and the octets. */
export function strSize(n: number): number {
  return uvSize(n) + n;
}

/**
 * The length of the lib0 varInt of a number of magnitude mag, at most 2^31-1 (the sign takes no octet of its own: it is a bit of the
 * first): 6 bits in the first octet, then 7 in each.
 */
function varIntSize(mag: number): number {
  let s = 1;
  for (mag = mag >>> 6; mag > 0; mag >>>= 7) s++;
  return s;
}

/**
 * The octets that Yjs's writeAny writes for the number f (05 [STATE-74], Content): by the value, and not by the tag that the update used.
 * An integer of magnitude at most 2^31-1 (a negative zero too, which has the integer's tag) goes under 125, a varInt; another number that a
 * float32 holds exactly under 124; every other under 123.
 */
export function anyNumberSize(f: number): number {
  if (Number.isInteger(f) && Math.abs(f) <= 2147483647) return 1 + varIntSize(Math.abs(f));
  if (Math.fround(f) === f) return 5;
  return 9;
}

/**
 * Runs of ids that each take a number of octets of content in the document: the profile of a text of characters of different widths, and of
 * a ContentAny. They are kept flat, as pairs (n, bpu) of a count of ids and the octets of each, so that a text of a million width changes
 * costs two numbers for each and not an object (02 [ARC-46]).
 */
export type SizeRuns = number[];

/** Adds a run of n ids of bpu octets each to runs, joining the last when it has the same width. */
export function appendRun(runs: SizeRuns, n: number, bpu: number): void {
  const k = runs.length;
  if (k > 0 && runs[k - 1] === bpu) runs[k - 2] = (runs[k - 2] as number) + n;
  else runs.push(n, bpu);
}

/**
 * The octets of each UTF-16 code unit of the valid UTF-8 text raw, as runs: a BMP character takes 1, 2 or 3 octets, and a pair of
 * surrogates is two units of 2 octets each (so that a cut between the two units, which Yjs makes, has both halves of the pair).
 */
export function utf8Profile(raw: Uint8Array): SizeRuns {
  const runs: SizeRuns = [];
  for (let i = 0; i < raw.length; ) {
    const c = raw[i] as number;
    if (c < 0x80) {
      appendRun(runs, 1, 1);
      i++;
    } else if (c < 0xe0) {
      appendRun(runs, 1, 2);
      i += 2;
    } else if (c < 0xf0) {
      appendRun(runs, 1, 3);
      i += 3;
    } else {
      appendRun(runs, 2, 2);
      i += 4;
    }
  }
  return runs;
}

// ---- the profile of a client ----

/**
 * The profile of a client: the stretches of its ids whose every id takes the same number of octets of content, in ascending order, flat as
 * triples (start, bcum, bpu): the first id of the stretch, the octets of all the ids of the profile before it, and the octets of each id of
 * it; and where the last ends. A Doc's own, never one that a clone shares, is changed in place.
 */
export interface Profile {
  segs: number[];
  end: number;
}

/** Adds the profile of the ids that start at clock `at`, which follow the profile or leave a gap that no profiled run asks about. */
export function pushRuns(p: Profile, at: number, runs: readonly number[]): void {
  let pos = at;
  for (let k = 0; k < runs.length; k += 2) {
    const n = runs[k] as number;
    const bpu = runs[k + 1] as number;
    const segs = p.segs;
    const last = segs.length - 3;
    if (last >= 0 && p.end === pos && segs[last + 2] === bpu) {
      // the same width as the stretch before it: it grows
    } else {
      const bcum = last < 0 ? 0 : (segs[last + 1] as number) + (p.end - (segs[last] as number)) * (segs[last + 2] as number);
      segs.push(pos, bcum, bpu);
    }
    pos += n;
    p.end = pos;
  }
}

/** The octets that the ids [lo, hi) of the profile take; they all lie in it. */
export function profileBytes(p: Profile | null, lo: number, hi: number): number {
  if (hi <= lo || p === null || p.segs.length === 0) return 0;
  const segs = p.segs;
  const count = segs.length / 3;
  /** The index of the last stretch that starts at or before the clock (the first when none does). */
  const find = (clock: number): number => {
    let a = 0;
    let b = count;
    while (a < b) {
      const m = (a + b) >>> 1;
      if ((segs[m * 3] as number) > clock) b = m;
      else a = m + 1;
    }
    return a === 0 ? 0 : a - 1;
  };
  const a = find(lo) * 3;
  const b = find(hi - 1) * 3;
  return (
    (segs[b + 1] as number) + (hi - (segs[b] as number)) * (segs[b + 2] as number) - ((segs[a + 1] as number) + (lo - (segs[a] as number)) * (segs[a + 2] as number))
  );
}

// ---- the first definitions as the document sees them ----

/** The kind of a struct as the document sees it: a GC, or an item by the cover that its content gives it. */
export const MKind = {
  GC: 1,
  Str: 2, // ContentString
  Any: 3, // ContentAny
  Del: 4, // ContentDeleted
  Other: 5, // ContentEmbed, ContentFormat, ContentType, ContentBinary: one id each, never merged
} as const;
export type MKind = (typeof MKind)[keyof typeof MKind];

/**
 * The first definition of a run of ids as the document measure reads it: one struct of an integrated update, or the structs that the
 * Merges step of 05 [STATE-74] joins. The `applyUpdate` that made it may extend it in place; once the call is over a Doc that is cloned shares it, and it is never changed.
 */
export interface MRun {
  /** the ids [start, end) */
  start: number;
  end: number;
  kind: MKind;
  /** the origin and the right origin of the first struct, or null */
  origin: Id | null;
  right: Id | null;
  /** the parent that the items take, when it is an item */
  parentId: Id | null;
  /** the items take their parent from a GC (05 [STATE-76]); they are hidden */
  gcParent: boolean;
  /** a text: its UTF-8 octets; a ContentAny: its values' octets; ContentEmbed, ContentFormat, ContentType, ContentBinary: the whole content; else 0 */
  bytes: number;
  /** the octets of the header of the first struct: its info byte, origin, right origin and, with neither, parent and parentSub */
  hdr: number;
  /** the ids take different numbers of octets, which the client's profile holds */
  prof: boolean;
}

function sameOptId(a: Id | null, b: Id | null): boolean {
  return a === null || b === null ? a === b : a.client === b.client && a.clock === b.clock;
}

/**
 * Whether n can join the run l, which ends where n starts: the Merges step of 05 [STATE-74] ("both are `GC`s, or both are items of the
 * same cover kind among `ContentString`, `ContentAny` and `ContentDeleted`, the origin of the second is the last id of the first, and the
 * two have the same right origin"), for items that also take their parent from a GC alike, so that what a client's cover asks of the run is
 * true of each of its structs (the parent of the second is that of its origin, the last id of the first).
 */
export function chains(client: number, l: MRun, n: MRun): boolean {
  if (l.end !== n.start || l.kind !== n.kind) return false;
  switch (l.kind) {
    case MKind.GC:
      return true;
    case MKind.Str:
    case MKind.Any:
    case MKind.Del:
      return (
        n.origin !== null &&
        n.origin.client === client &&
        n.origin.clock === l.end - 1 &&
        (l.parentId !== null) === (n.parentId !== null) &&
        l.gcParent === n.gcParent &&
        l.prof === n.prof &&
        sameOptId(l.right, n.right)
      );
  }
  return false;
}

/** The run that joining n to l gives: l with n's ids and octets after its own. */
export function joined(l: MRun, n: MRun): MRun {
  return { ...l, end: n.end, bytes: l.bytes + n.bytes };
}

// ---- the Cuts and Merges over the first definitions of one client ----

/** A struct of the document: the pieces that the Merges step joined. */
interface Cell {
  kind: MKind;
  start: number;
  end: number;
  hdr: number;
  right: Id | null;
  bytes: number;
}

/** The octets of the struct in the document. */
function cellSize(c: Cell): number {
  const n = c.end - c.start;
  switch (c.kind) {
    case MKind.GC:
      return 1 + uvSize(n);
    case MKind.Str:
      return c.hdr + strSize(c.bytes);
    case MKind.Any:
      return c.hdr + uvSize(n) + c.bytes;
    case MKind.Del:
      return c.hdr + uvSize(n);
  }
  return c.hdr + c.bytes;
}

/** Walks a client's canonical delete ranges in step with the ids that the fold reaches, which ascend. */
export class DsCursor {
  private i = 0;

  constructor(private readonly rs: readonly Range[]) {}

  /** Says whether the delete set holds the id pos, and where the stretch with that answer ends, at most limit. */
  at(pos: number, limit: number): { held: boolean; end: number } {
    while (this.i < this.rs.length && (this.rs[this.i] as Range).clock + (this.rs[this.i] as Range).len <= pos) this.i++;
    if (this.i === this.rs.length) return { held: false, end: limit };
    const r = this.rs[this.i] as Range;
    if (r.clock <= pos) return { held: true, end: Math.min(limit, r.clock + r.len) };
    return { held: false, end: Math.min(limit, r.clock) };
  }
}

/**
 * The Cuts and Merges of 05 [STATE-74] over the first definitions of one client, in clock order: it takes the pieces and keeps the number
 * and the octets of the structs that it has closed, and the one that is still open, which the next piece may join.
 */
export class CellFold {
  private n = 0;
  private sum = 0;
  private open: Cell | null = null;

  constructor(readonly client: number) {}

  /** Takes the next piece, which starts where the last ended. `origin` is the piece's origin, or null when it has none. */
  private add(kind: MKind, start: number, end: number, hdr: number, origin: Id | null, right: Id | null, bytes: number): void {
    const o = this.open;
    if (o !== null && o.kind === kind && o.end === start) {
      let join = false;
      switch (kind) {
        case MKind.GC:
          join = true;
          break;
        case MKind.Str:
        case MKind.Any:
        case MKind.Del:
          join = origin !== null && origin.client === this.client && origin.clock === o.end - 1 && sameOptId(o.right, right);
          break;
      }
      if (join) {
        o.end = end;
        o.bytes += bytes;
        return;
      }
    }
    if (o !== null) {
      this.n++;
      this.sum += cellSize(o);
    }
    this.open = { kind, start, end, hdr, right, bytes };
  }

  /**
   * Takes the first definition m, in clock order, with the cover that its parent gives it (cover: it is a GC) and the client's delete set.
   * 05 [STATE-74] Content: "Each id has the content of its first definition in seq order ([STATE-76]), except that an id that a `GC` first
   * defines, or that is hidden because its parent is hidden or because it takes its parent from a `GC`, is covered by a `GC`; and any other
   * id that the canonical delete set holds is covered by a `ContentDeleted`." `prof` is the client's profile.
   */
  feed(m: MRun, cover: boolean, ds: DsCursor, prof: Profile | null): void {
    const client = this.client;
    const part = (kind: MKind, lo: number, hi: number): void => {
      let hdr: number;
      let origin: Id | null;
      if (lo === m.start) {
        hdr = m.hdr;
        origin = m.origin;
      } else {
        // Cuts: the right part has the last id of the left part as its origin and keeps the item's right origin, as Yjs cuts an item
        origin = { client, clock: lo - 1 };
        hdr = 1 + idSize(origin) + (m.right !== null ? idSize(m.right) : 0);
      }
      let bytes = 0;
      switch (kind) {
        case MKind.Str:
        case MKind.Any:
          bytes = lo === m.start && hi === m.end ? m.bytes : m.prof ? profileBytes(prof, lo, hi) : hi - lo;
          break;
        case MKind.Other:
          bytes = m.bytes;
          break;
      }
      this.add(kind, lo, hi, hdr, origin, m.right, bytes);
    };
    if (m.kind === MKind.GC || cover) {
      this.add(MKind.GC, m.start, m.end, 0, null, null, 0);
    } else if (m.kind === MKind.Del) {
      part(MKind.Del, m.start, m.end);
    } else if (m.kind === MKind.Other) {
      part(ds.at(m.start, m.end).held ? MKind.Del : MKind.Other, m.start, m.end);
    } else {
      // a text or a ContentAny: cut at each edge of the delete set
      for (let pos = m.start; pos < m.end; ) {
        const { held, end } = ds.at(pos, m.end);
        part(held ? MKind.Del : m.kind, pos, end);
        pos = end;
      }
    }
  }

  /** The octets of the client's group in the document: its struct count, its id, its first clock (0) and its structs. */
  term(): number {
    let n = this.n;
    let sum = this.sum;
    if (this.open !== null) {
      n++;
      sum += cellSize(this.open);
    }
    return uvSize(n) + uvSize(this.client) + 1 + sum;
  }
}

/** The octets of a client's group of the delete set: its id, its number of ranges, and each range's clock and length. */
export function dsTermOf(client: number, rs: readonly Range[]): number {
  let n = uvSize(client) + uvSize(rs.length);
  for (const r of rs) n += uvSize(r.clock) + uvSize(r.len);
  return n;
}
