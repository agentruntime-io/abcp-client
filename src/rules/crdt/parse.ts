// The strict reader of a Yjs update in format v1 (05 [STATE-71]): a struct section followed by a delete set, checked against
// the first six conditions of the well-formedness rule. The seventh, the root conditions, needs the template's `crdt_roots`
// and is `Update.checkRoots`; `checkWellFormed` does both.
//
// The work and the memory are bounded by the update's length: no length field sizes an allocation, and the recursion that
// nesting could drive (a lib0 any value, a JSON text) is cut at the depth limit of 05 [STATE-7].

import { utf8DecodeValid } from "../bytes.js";
import { canonical } from "../jsonv1/canonical.js";
import { checkJsonText, parseDocument } from "../jsonv1/parse.js";
import { fail } from "./errors.js";
import { anyNumberSize, appendRun, strSize, type SizeRuns, uvSize, utf8Profile } from "./measure.js";
import { Reader } from "./reader.js";

/** 05 [STATE-71] (Integers). */
const MAX_CLIENT_ID = Number.MAX_SAFE_INTEGER; // a client id
const MAX_CLOCK = 2 ** 32 - 1; // a clock, a struct's clock plus its length, a delete range's clock plus its length
const MAX_ANY_INT = Number.MAX_SAFE_INTEGER; // the magnitude of an integer inside a lib0 any value
/** The limits of 05 [STATE-7] that bound a lib0 any value in an update. */
const MAX_DEPTH = 32;
const MAX_MEMBERS = 1024;

/** The id of one clock of one client: the id of a struct, or of a part of one. */
export interface Id {
  client: number;
  clock: number;
}

export type StructKind = "item" | "gc" | "skip";

/** The content refs that an item may hold. A ContentJSON (2) and a ContentDoc (9) are refused, so they are not listed. */
export const ContentRef = {
  Deleted: 1,
  Binary: 3,
  String: 4,
  Embed: 5,
  Format: 6,
  Type: 7,
  Any: 8,
} as const;

/** What an item encodes as its parent. An item that has an origin or a right origin encodes none and takes it from them (05 [STATE-76]). */
export const ParentKind = {
  Implied: 0,
  Root: 1, // a root name
  Item: 2, // the id of a ContentType item
} as const;
export type ParentKind = (typeof ParentKind)[keyof typeof ParentKind];

/** One struct of an update: an Item, a GC or a Skip, as Yjs calls them. */
export interface Struct {
  kind: StructKind;
  /** items only */
  content: number;
  client: number;
  clock: number;
  /** The length of 05 [STATE-41]: UTF-16 code units of a text, the encoded length of a deleted, GC or Skip, the number of values of a ContentAny, else 1. */
  len: number;
  hasOrigin: boolean;
  origin: Id;
  hasRight: boolean;
  right: Id;
  parentKind: ParentKind;
  /** when parentKind is Item */
  parentId: Id;
  /** when parentKind is Root */
  root: string;
  /** the item encodes a parentSub */
  hasSub: boolean;
  /** the UTF-16 units of a ContentString, kept only when the text holds a surrogate pair */
  units: Uint16Array | null;
  /**
   * What the document length of 05 [STATE-74] counts of the item. `hdr`: the octets of its header (the info byte, the origin, the right origin
   * and, when it has neither, the parent and the parentSub; the content follows). `bytes`: a ContentString's UTF-8 octets, a ContentAny's values
   * in their canonical forms, a ContentEmbed's and a ContentFormat's content with its JSON text in the RFC 8785 form, a ContentType's and a
   * ContentBinary's whole content, and 0 for a ContentDeleted. `prof`: for a ContentString whose characters are not all one octet, and for a
   * ContentAny, the octets of each id, as pairs (count of ids, octets of each) (SizeRuns). A GC and a Skip have 0, 0 and null.
   */
  hdr: number;
  bytes: number;
  prof: SizeRuns | null;
  /** position in Update.all */
  idx: number;
  /** position of the struct's group in Update.groups */
  grp: number;
  /** the struct's encoded bytes are data[off, end) */
  off: number;
  end: number;
}

/** The run of structs that the encoding gives under one client id. */
export interface ClientGroup {
  client: number;
  /** the clock of the first struct */
  clock: number;
  structs: Struct[];
  /** index of the first struct in Update.all */
  first: number;
  n: number;
}

/** A run of `len` ids starting at `clock`. */
export interface Range {
  clock: number;
  len: number;
}

/** The run of delete ranges that the encoding gives under one client id. */
export interface DeleteGroup {
  client: number;
  ranges: Range[];
}

export interface ClientClock {
  client: number;
  clock: number;
}

export interface ClientRanges {
  client: number;
  ranges: Range[];
}

/**
 * A Yjs update in format v1 that `parseUpdate` has read and checked: a struct section, then a delete set. It is read-only.
 * The groups of both sections are in the strictly descending order of client id that the encoding requires.
 */
export class Update {
  readonly groups: ClientGroup[] = [];
  readonly deletes: DeleteGroup[] = [];
  readonly all: Struct[] = [];

  constructor(readonly data: Uint8Array) {}

  /** The struct of the update that defines id, or undefined. A Skip covers clocks but defines no id. */
  find(id: Id): Struct | undefined {
    const gi = this.groupIndex(id.client);
    if (gi < 0) return undefined;
    const ss = (this.groups[gi] as ClientGroup).structs;
    // the first struct whose end is above the clock
    let lo = 0;
    let hi = ss.length;
    while (lo < hi) {
      const m = (lo + hi) >>> 1;
      const s = ss[m] as Struct;
      if (s.clock + s.len > id.clock) hi = m;
      else lo = m + 1;
    }
    const s = ss[lo];
    if (s === undefined || s.clock > id.clock || s.kind === "skip") return undefined;
    return s;
  }

  /** The position of the client's group in `groups` (descending client order), or -1. */
  groupIndex(client: number): number {
    let lo = 0;
    let hi = this.groups.length;
    while (lo < hi) {
      const m = (lo + hi) >>> 1;
      if ((this.groups[m] as ClientGroup).client > client) lo = m + 1;
      else hi = m;
    }
    return lo < this.groups.length && (this.groups[lo] as ClientGroup).client === client ? lo : -1;
  }

  /** The state vector of the update read alone: the next clock of each client over its structs, in ascending order of client id (05 [STATE-74]). */
  stateVector(): ClientClock[] {
    const out: ClientClock[] = [];
    for (let i = this.groups.length - 1; i >= 0; i--) {
      const g = this.groups[i] as ClientGroup;
      const last = g.structs[g.structs.length - 1] as Struct;
      out.push({ client: g.client, clock: last.clock + last.len });
    }
    return out;
  }

  /** The delete set of the update read alone, normalized as 05 [STATE-41] says, in ascending order of client id. */
  deletions(): ClientRanges[] {
    const out: ClientRanges[] = [];
    for (let i = this.deletes.length - 1; i >= 0; i--) {
      const dg = this.deletes[i] as DeleteGroup;
      out.push({ client: dg.client, ranges: normalizeRanges(dg.ranges) });
    }
    return out;
  }
}

/** Returns ranges sorted by clock, with overlapping and adjacent ranges merged (05 [STATE-41], DS). */
export function normalizeRanges(rs: readonly Range[]): Range[] {
  const sorted = rs.map((r) => ({ ...r })).sort((a, b) => a.clock - b.clock);
  const out: Range[] = [];
  for (const r of sorted) {
    const last = out[out.length - 1];
    if (last !== undefined && r.clock <= last.clock + last.len) {
      const end = r.clock + r.len;
      if (end > last.clock + last.len) last.len = end - last.clock;
      continue;
    }
    out.push(r);
  }
  return out;
}

const NO_ID: Id = { client: 0, clock: 0 };

/**
 * A GC or a Skip: it names no id and holds no content, so it keeps only what it has, and the rest of a Struct is read from the
 * prototype. An update of 4 MiB can hold two million of them (02 [ARC-46]: the memory of a read is bounded by the update's length, and
 * a struct of seventeen fields for each of two million GCs was 240 MB retained, and the check of references another hundred).
 */
class BareStruct implements Struct {
  declare readonly content: number;
  declare readonly hasOrigin: boolean;
  declare readonly origin: Id;
  declare readonly hasRight: boolean;
  declare readonly right: Id;
  declare readonly parentKind: ParentKind;
  declare readonly parentId: Id;
  declare readonly root: string;
  declare readonly hasSub: boolean;
  declare readonly units: Uint16Array | null;
  declare readonly hdr: number;
  declare readonly bytes: number;
  declare readonly prof: SizeRuns | null;
  idx = 0;
  grp = 0;
  off = 0;
  end = 0;

  constructor(
    readonly kind: StructKind,
    readonly client: number,
    readonly clock: number,
    public len: number,
  ) {}
}
Object.assign(BareStruct.prototype, {
  content: 0,
  hasOrigin: false,
  origin: NO_ID,
  hasRight: false,
  right: NO_ID,
  parentKind: ParentKind.Implied,
  parentId: NO_ID,
  root: "",
  hasSub: false,
  units: null,
  hdr: 0,
  bytes: 0,
  prof: null,
});

/**
 * Reads b as a Yjs update in format v1 and checks it against the first six conditions of 05 [STATE-71]: Parse, Strings,
 * Values, Integers, No empty pieces and References. Throws a CrdtError with the vector's reason name.
 */
export function parseUpdate(b: Uint8Array): Update {
  const r = new Reader(b);
  const u = new Update(b);

  const nGroups = r.varUint();
  let prevClient = 0;
  for (let g = 0; g < nGroups; g++) {
    const nStructs = r.varUint();
    const client = readClient(r, "client id");
    let clock = readClock(r, "clock");
    if (nStructs === 0) throw fail("empty-group", `client ${client} has an empty client group`);
    if (g > 0 && client >= prevClient) throw fail("group-order", "struct client groups are not in strictly descending client id order");
    prevClient = client;
    const grp: ClientGroup = { client, clock, structs: [], first: u.all.length, n: 0 };
    for (let i = 0; i < nStructs; i++) {
      const s = readStruct(r, client, clock);
      s.idx = u.all.length;
      s.grp = u.groups.length;
      clock += s.len;
      u.all.push(s);
      grp.structs.push(s);
    }
    grp.n = grp.structs.length;
    u.groups.push(grp);
  }

  const nDS = r.varUint();
  for (let i = 0; i < nDS; i++) {
    const client = readClient(r, "delete set client id");
    const n = r.varUint();
    if (n === 0) throw fail("empty-group", `client ${client} has an empty delete-set group`);
    if (i > 0 && client >= prevClient) throw fail("group-order", "delete-set client groups are not in strictly descending client id order");
    prevClient = client;
    const dg: DeleteGroup = { client, ranges: [] };
    for (let j = 0; j < n; j++) {
      const clock = readClock(r, "delete range clock");
      const len = r.varUint();
      if (len === 0) throw fail("zero-length", "delete range of length 0");
      if (len > MAX_CLOCK - clock) throw fail("int-range", "delete range clock + length above 2^32-1");
      dg.ranges.push({ clock, len });
    }
    u.deletes.push(dg);
  }
  if (r.p !== b.length) throw fail("trailing-bytes", `${b.length - r.p} bytes after the delete set`);
  checkReferences(u);
  return u;
}

function readClient(r: Reader, what: string): number {
  const v = r.varUint();
  if (v > MAX_CLIENT_ID) throw fail("int-range", `${what} above 2^53-1`);
  return v;
}

function readClock(r: Reader, what: string): number {
  const v = r.varUint();
  if (v > MAX_CLOCK) throw fail("int-range", `${what} above 2^32-1`);
  return v;
}

function readId(r: Reader, what: string): Id {
  const client = readClient(r, `${what} client`);
  const clock = readClock(r, `${what} clock`);
  return { client, clock };
}

/** Reads one struct of the group of `client`, whose clock is the struct's. */
function readStruct(r: Reader, client: number, clock: number): Struct {
  const start = r.p;
  const info = r.u8();
  const ref0 = info & 0x1f;
  if (ref0 === 0 || ref0 === 10) {
    // 05 [STATE-71] (Parse): the info byte of a GC or a Skip has none of its three high bits set.
    if ((info & 0xe0) !== 0) throw fail("gc-skip-high-bits", `info byte 0x${info.toString(16).padStart(2, "0")}`);
    const len = r.varUint();
    if (len === 0) throw fail("zero-length", `struct of client ${client} at clock ${clock} has length 0`);
    if (len > MAX_CLOCK - clock) throw fail("int-range", "struct clock + length above 2^32-1");
    const b = new BareStruct(ref0 === 10 ? "skip" : "gc", client, clock, len);
    b.off = start;
    b.end = r.p;
    return b;
  }
  const s: Struct = {
    kind: "item",
    content: 0,
    client,
    clock,
    len: 0,
    hasOrigin: false,
    origin: NO_ID,
    hasRight: false,
    right: NO_ID,
    parentKind: ParentKind.Implied,
    parentId: NO_ID,
    root: "",
    hasSub: false,
    units: null,
    hdr: 0,
    bytes: 0,
    prof: null,
    idx: 0,
    grp: 0,
    off: 0,
    end: 0,
  };
  const ref = ref0;
  if (ref > 10) throw fail("content-ref", `content ref ${ref}`);
  if (ref === 2) throw fail("content-json", "ContentJSON is refused");
  if (ref === 9) throw fail("content-doc", "ContentDoc is refused");
  s.kind = "item";
  s.content = ref;
  if ((info & 0x80) !== 0) {
    s.hasOrigin = true;
    s.origin = readId(r, "origin");
  }
  if ((info & 0x40) !== 0) {
    s.hasRight = true;
    s.right = readId(r, "right origin");
  }
  if ((info & 0xc0) === 0) {
    // Only an item with neither origin nor right origin encodes its parent, and its parentSub with it.
    const pi = r.varUint();
    if (pi === 1) {
      s.parentKind = ParentKind.Root;
      s.root = utf8DecodeValid(r.strBytes());
    } else if (pi === 0) {
      s.parentKind = ParentKind.Item;
      s.parentId = readId(r, "parent");
    } else {
      throw fail("parent-info", `parent-info number ${pi}`);
    }
    if ((info & 0x20) !== 0) {
      s.hasSub = true;
      r.strBytes();
    }
  }
  s.hdr = r.p - start;
  readContent(r, s);
  if (s.len === 0) throw fail("zero-length", `struct of client ${client} at clock ${clock} has length 0`);
  if (s.len > MAX_CLOCK - clock) throw fail("int-range", "struct clock + length above 2^32-1");
  s.off = start;
  s.end = r.p;
  return s;
}

/** Reads the content of the item s and sets its length and what the document length counts of it (`bytes`, `prof`). */
function readContent(r: Reader, s: Struct): void {
  const c0 = r.p;
  switch (s.content) {
    case ContentRef.Deleted:
      s.len = r.varUint();
      return;
    case ContentRef.Binary: {
      const n = r.varUint();
      r.take(n);
      s.len = 1;
      s.bytes = r.p - c0; // a length and the octets, as Yjs v1 writes them (a varUint is read in its shortest form only)
      return;
    }
    case ContentRef.String: {
      const raw = r.strBytes();
      const text = utf8DecodeValid(raw);
      s.len = text.length; // UTF-16 code units
      s.bytes = raw.length;
      // a text whose characters are not all one octet and one unit keeps the octets of each unit (05 [STATE-74]: a cut inside it is measured)
      if (raw.length !== text.length) s.prof = utf8Profile(raw);
      // keep the units only when the text holds a surrogate pair (no unit of any other text is a surrogate)
      if (/[\ud800-\udfff]/.test(text)) {
        const units = new Uint16Array(text.length);
        for (let i = 0; i < text.length; i++) units[i] = text.charCodeAt(i);
        s.units = units;
      }
      return;
    }
    case ContentRef.Embed: {
      const raw = r.strBytes();
      s.len = 1;
      checkJSON(raw);
      s.bytes = strSize(canonicalJsonLength(raw));
      return;
    }
    case ContentRef.Format: {
      const key = r.strBytes(); // the key
      const raw = r.strBytes();
      s.len = 1;
      checkJSON(raw);
      s.bytes = strSize(key.length) + strSize(canonicalJsonLength(raw));
      return;
    }
    case ContentRef.Type: {
      const t = r.varUint();
      if (t > 6) throw fail("type-ref", `contentType ref ${t}`);
      if (t === 3 || t === 5) r.strBytes(); // XmlElement and XmlHook carry a node or hook name
      s.len = 1;
      s.bytes = r.p - c0; // the type ref and the name, as Yjs v1 writes them
      return;
    }
    case ContentRef.Any: {
      const n = r.varUint();
      if (n > r.remaining()) throw fail("truncated", "contentAny holds more values than the update has bytes"); // every value takes at least one byte
      const prof: SizeRuns = [];
      let total = 0;
      for (let i = 0; i < n; i++) {
        const size = readAny(r, 0);
        total += size;
        appendRun(prof, 1, size);
      }
      s.len = n;
      s.bytes = total;
      s.prof = prof;
      return;
    }
    default:
      throw fail("content-ref", `content ref ${s.content}`);
  }
}

/** Checks one JSON text of an update (05 [STATE-71], Strings), mapping the refusal to the reason of the vectors. */
function checkJSON(text: Uint8Array): void {
  const err = checkJsonText(text);
  if (err === null) return;
  throw fail(err.reason === "json-limits" ? "json-limits" : "json", err.message);
}

/**
 * The length in octets of the RFC 8785 form of the JSON text of an embed or a format value (05 [STATE-74], Content), which `checkJSON` has
 * passed. A text that does not read as a document (it should not happen) counts as the octets that the update holds.
 */
function canonicalJsonLength(text: Uint8Array): number {
  try {
    return canonical(parseDocument(text)).length;
  } catch {
    return text.length;
  }
}

/**
 * Reads one lib0 any value (05 [STATE-71], Values); depth is the depth of the container that holds it. It returns the octets that Yjs's
 * writeAny writes for the value (05 [STATE-74], Content): a number by its value, and not by the tag that the update used.
 */
function readAny(r: Reader, depth: number): number {
  const tag = r.u8();
  switch (tag) {
    case 127: // undefined
    case 126: // null
    case 121: // false
    case 120: // true
      return 1;
    case 125: {
      // integer, a varInt
      const { neg, mag } = r.varInt();
      if (mag > MAX_ANY_INT) throw fail("any-int-range", "varInt above 2^53-1");
      return anyNumberSize(neg ? -mag : mag);
    }
    case 124: {
      // float32
      const b = r.take(4);
      const bits = (((b[0] as number) << 24) | ((b[1] as number) << 16) | ((b[2] as number) << 8) | (b[3] as number)) >>> 0;
      if ((bits & 0x7f800000) >>> 0 === 0x7f800000) throw fail("any-float", "float32 NaN or infinite");
      return anyNumberSize(new DataView(b.buffer, b.byteOffset, 4).getFloat32(0, false));
    }
    case 123: {
      // float64
      const b = r.take(8);
      if ((((b[0] as number) & 0x7f) << 4 | ((b[1] as number) >> 4)) === 0x7ff) throw fail("any-float", "float64 NaN or infinite");
      return anyNumberSize(new DataView(b.buffer, b.byteOffset, 8).getFloat64(0, false));
    }
    case 122: {
      // bigint, an int64: it stays under tag 122
      const b = r.take(8);
      const v = new DataView(b.buffer, b.byteOffset, 8).getBigInt64(0, false);
      const max = BigInt(MAX_ANY_INT);
      if (v > max || v < -max) throw fail("any-int-range", "bigint above 2^53-1");
      return 9;
    }
    case 119: // string
      return 1 + strSize(r.strBytes().length);
    case 118: {
      // object
      if (depth + 1 > MAX_DEPTH) throw fail("any-limits", "depth over 32");
      const n = r.varUint();
      if (n > MAX_MEMBERS) throw fail("any-limits", "more than 1,024 members");
      // 05 [STATE-71] (Values), round 17: a lib0 object holds each member name once (I-JSON, RFC 7493, as a JSON text does). The names are
      // compared as strings, which is as octets for valid UTF-8; a small object compares them pairwise, a large one by a set.
      const names: string[] = [];
      let seen: Set<string> | null = null;
      let size = 1 + uvSize(n);
      for (let i = 0; i < n; i++) {
        const raw = r.strBytes();
        const name = utf8DecodeValid(raw);
        if (seen === null) {
          if (names.includes(name)) throw fail("any-duplicate", `a lib0 object holds the member name ${JSON.stringify(name)} twice`);
          names.push(name);
          if (names.length >= 8) seen = new Set(names);
        } else {
          if (seen.has(name)) throw fail("any-duplicate", `a lib0 object holds the member name ${JSON.stringify(name)} twice`);
          seen.add(name);
        }
        size += strSize(raw.length) + readAny(r, depth + 1);
      }
      return size;
    }
    case 117: {
      // array
      if (depth + 1 > MAX_DEPTH) throw fail("any-limits", "depth over 32");
      const n = r.varUint();
      if (n > r.remaining()) throw fail("truncated", "array longer than the update");
      let size = 1 + uvSize(n);
      for (let i = 0; i < n; i++) size += readAny(r, depth + 1);
      return size;
    }
    case 116: {
      // Uint8Array
      const n = r.varUint();
      r.take(n);
      return 1 + strSize(n);
    }
    default:
      throw fail("any-tag", `lib0 any tag ${tag}`);
  }
}

/** The ids that an item names: its origin, right origin and parent item, in that order. */
export function named(s: Struct): Id[] {
  if (s.kind !== "item") return [];
  const out: Id[] = [];
  if (s.hasOrigin) out.push(s.origin);
  if (s.hasRight) out.push(s.right);
  if (s.parentKind === ParentKind.Item) out.push(s.parentId);
  return out;
}

/** Checks the References condition of 05 [STATE-71]. */
function checkReferences(u: Update): void {
  // No item names an id of its own client at or above its own clock, which is an id of itself or of a later struct.
  for (const s of u.all) {
    if (s.kind !== "item") continue;
    if (s.hasOrigin && s.origin.client === s.client && s.origin.clock >= s.clock) throw selfReference(s, s.origin);
    if (s.hasRight && s.right.client === s.client && s.right.clock >= s.clock) throw selfReference(s, s.right);
    if (s.parentKind === ParentKind.Item && s.parentId.client === s.client && s.parentId.clock >= s.clock) throw selfReference(s, s.parentId);
  }
  // The reference graph has no cycle. Its nodes are the structs. Its edges run from each item to the struct that defines an id the item
  // names, and from each struct to the struct before it in its group. The search is iterative, and it allocates no edge list: the edges
  // of a node are four slots (the struct before it, then its origin, right origin and parent), read where they stand, so that the memory
  // of the check is a few octets for each struct and not an array for each (02 [ARC-46]).
  const FRESH = 0;
  const ON_PATH = 1;
  const DONE = 2;
  const n = u.all.length;
  const state = new Uint8Array(n);
  const edge = (v: number, k: number): number => {
    const s = u.all[v] as Struct;
    switch (k) {
      case 0:
        return s.idx > (u.groups[s.grp] as ClientGroup).first ? s.idx - 1 : -1;
      case 1:
        return s.kind === "item" && s.hasOrigin ? (u.find(s.origin)?.idx ?? -1) : -1;
      case 2:
        return s.kind === "item" && s.hasRight ? (u.find(s.right)?.idx ?? -1) : -1;
      default:
        return s.kind === "item" && s.parentKind === ParentKind.Item ? (u.find(s.parentId)?.idx ?? -1) : -1;
    }
  };
  const stackV: number[] = [];
  const stackK: number[] = [];
  for (let start = 0; start < n; start++) {
    if (state[start] !== FRESH) continue;
    state[start] = ON_PATH;
    stackV.push(start);
    stackK.push(0);
    while (stackV.length > 0) {
      const top = stackV.length - 1;
      const k = stackK[top] as number;
      if (k >= 4) {
        state[stackV[top] as number] = DONE;
        stackV.pop();
        stackK.pop();
        continue;
      }
      stackK[top] = k + 1;
      const w = edge(stackV[top] as number, k);
      if (w < 0) continue;
      if (state[w] === ON_PATH) throw fail("cycle", "the reference graph of the update has a cycle");
      if (state[w] === FRESH) {
        state[w] = ON_PATH;
        stackV.push(w);
        stackK.push(0);
      }
    }
  }
}

function selfReference(s: Struct, id: Id): Error {
  return fail("self-reference", `item ${s.client}:${s.clock} names ${id.client}:${id.clock}`);
}
