// The rules that the Go crdt package gained after the client's first port (C1): Q7 (ruled, reading a), V2 S2 and V6 S-3 (the content rule
// of a base), V2 SHOULD S3 (a Doc a reader holds never moves), V2 MUST-1 / V6 MUST-A (the memory of a key follows the shape of its
// document, not its number of structs or clients, 02 [ARC-46]), and, from spec draft 14, the parent of an item that has both an origin and
// a right origin (05 [STATE-76], Y9, Z3, Q33). The tests are the twins of server/internal/crdt/clone_test.go, resource_test.go,
// clients_test.go and parents_test.go.

import * as v8 from "node:v8";
import * as vm from "node:vm";
import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { ChangeKind, KeyChangeSchema, RecordKind, RecordSchema } from "../src/gen/abpc/v2/types_pb.js";
import { Doc, MAX_NESTING, parseUpdate, validateBase, type BaseError, type Roots, type Verdict } from "../src/rules/crdt/index.js";
import { Analysis } from "../src/rules/crdt/doc.js";
import { applyRecord, ReplicaState } from "../src/rules/fold/index.js";

// ---- a small encoder for Yjs updates, format v1 (the helpers of server/internal/crdt/crdt_test.go) ----

function uv(n: number): number[] {
  const out: number[] = [];
  while (n > 127) {
    out.push((n % 128) | 128);
    n = Math.floor(n / 128);
  }
  out.push(n);
  return out;
}
const lstr = (s: string): number[] => {
  const b = new TextEncoder().encode(s);
  return [...uv(b.length), ...b];
};
const cat = (...parts: number[][]): number[] => parts.flat();
/** A client group: the struct count, the client, the first clock, and the structs. */
const group = (client: number, clock: number, ...structs: number[][]): number[] => cat(uv(structs.length), uv(client), uv(clock), ...structs);
/** A delete-set client group: the client, the range count, and the ranges as clock, length pairs. */
const ds = (client: number, ...ranges: number[]): number[] => cat(uv(client), uv(ranges.length / 2), ...ranges.map(uv));
/** Groups, in descending client order, and delete-set groups, in the same order. */
const update = (groups: number[][], ...dsGroups: number[][]): Uint8Array =>
  Uint8Array.from(cat(uv(groups.length), ...groups, uv(dsGroups.length), ...dsGroups));
const rootText = (root: string, text: string): number[] => cat([0x04, 1], lstr(root), lstr(text));
const textAfter = (client: number, clock: number, text: string): number[] => cat([0x84], uv(client), uv(clock), lstr(text));
const anyAfter = (client: number, clock: number): number[] => cat([0x88], uv(client), uv(clock), uv(1), [127]);
const gcOf = (n: number): number[] => cat([0], uv(n));
const textRoots: Roots = new Map([["t", "text"]]);

function mustApply(d: Doc, b: Uint8Array, want: Verdict): void {
  const r = d.apply(b, textRoots);
  expect(r.verdict, `${r.reason ?? ""} ${r.error?.message ?? ""}`).toBe(want);
}

const code = (d: Doc, b: Uint8Array): string => validateBase(b, textRoots, d)?.code ?? "";

describe("05 [STATE-74] with [STATE-76], Q7 (reading a, now the text): a GC may stand in place of an item whose origin is a GC", () => {
  it("the base that holds the GC, the base that holds the item, and a GC for live text", () => {
    const key = new Doc();
    mustApply(key, update([group(1, 0, gcOf(1))]), "APPLIED");
    mustApply(key, update([group(2, 0, anyAfter(1, 0))]), "APPLIED"); // its origin is a GC, so it is hidden

    const gc = update([group(2, 0, gcOf(1)), group(1, 0, gcOf(1))]);
    expect(code(key, gc), "a GC in place of the item whose origin is a GC").toBe("");
    // the item itself, with its origin a GC, is a valid base too
    expect(code(key, update([group(2, 0, anyAfter(1, 0)), group(1, 0, gcOf(1))]))).toBe("");
    // and a GC for an item whose parent is not hidden is not
    const live = new Doc();
    mustApply(live, update([group(5, 0, rootText("t", "a"), textAfter(5, 0, "b"))]), "APPLIED");
    expect(code(live, update([group(5, 0, rootText("t", "a"), gcOf(1))]))).toBe("gc-not-allowed");
  });
});

describe("05 [STATE-74] (V2 S2): a base holds the first definition of every id", () => {
  const typeUnderT = cat([0x07, 1], lstr("t"), uv(0)); // 5:0, a ContentType (array) under the root
  const textInType = cat([0x04, 0], uv(5), uv(0), lstr("z")); // 7:0, text whose parent is the item 5:0
  const textUnderRoot = rootText("t", "z"); // 7:0, text whose parent is the root

  it("another parent, another content ref, an item where a GC is the first definition", () => {
    const key = new Doc();
    mustApply(key, update([group(7, 0, textInType), group(5, 0, typeUnderT)]), "APPLIED");
    const gcKey = new Doc();
    mustApply(gcKey, update([group(5, 0, gcOf(1))]), "APPLIED");
    const cases: [string, Doc, Uint8Array, string][] = [
      ["the first definitions", key, update([group(7, 0, textInType), group(5, 0, typeUnderT)]), ""],
      ["another parent", key, update([group(7, 0, textUnderRoot), group(5, 0, typeUnderT)]), "content"],
      ["another content ref", key, update([group(7, 0, cat([0x08, 0], uv(5), uv(0), uv(1), [127])), group(5, 0, typeUnderT)]), "content"],
      ["an item where a GC is the first definition", gcKey, update([group(5, 0, rootText("t", "a"))]), "content"],
      ["the GC itself", gcKey, update([group(5, 0, gcOf(1))]), ""],
    ];
    for (const [name, doc, base, want] of cases) expect(code(doc, base), name).toBe(want);
  });

  it("a ContentDeleted stands in place of an id that the canonical delete set holds, and only of one", () => {
    const key = new Doc();
    mustApply(key, update([group(5, 0, rootText("t", "ab"))], ds(5, 0, 1)), "APPLIED"); // 5:0 deleted, 5:1 not
    // 5:0..1 as a ContentString "ab": the same content as the key's first definition
    expect(code(key, update([group(5, 0, rootText("t", "ab"))], ds(5, 0, 1)))).toBe("");
    // 5:0..2 as ContentDeleted of length 2: the delete set does not hold 5:1, so it is not what the first definition holds
    expect(code(key, update([group(5, 0, cat([0x01, 1], lstr("t"), uv(2)))], ds(5, 0, 1)))).toBe("content");
    // with both ids deleted in the key, a ContentDeleted of length 2 is the content of a deleted ContentString
    const both = new Doc();
    mustApply(both, update([group(5, 0, rootText("t", "ab"))], ds(5, 0, 2)), "APPLIED");
    expect(code(both, update([group(5, 0, cat([0x01, 1], lstr("t"), uv(2)))], ds(5, 0, 2)))).toBe("");
  });
});

describe("05 [STATE-76] (V6 S-3): the parent of the first definition, hidden by a GC origin on one side and visible on the other, is another document", () => {
  const rootAny = cat([0x08, 1], lstr("t"), uv(1), [127]); // 2:0, ContentAny, parent: the root t (visible)
  const typeUnderT = cat([0x07, 1], lstr("t"), uv(0)); // 5:0, a ContentType under the root
  const anyInType = cat([0x08, 0], uv(5), uv(0), uv(1), [127]);

  // The key's 2:0 is hidden (its origin is a GC); a base that gives it the root as parent shows it.
  const hiddenKey = new Doc();
  mustApply(hiddenKey, update([group(1, 0, gcOf(1))]), "APPLIED");
  mustApply(hiddenKey, update([group(2, 0, anyAfter(1, 0))]), "APPLIED");
  // The key's 2:0 is visible (the root is its parent); a base that hides it, by a GC origin, hides what the key shows.
  const visibleKey = new Doc();
  mustApply(visibleKey, update([group(2, 0, rootAny), group(1, 0, gcOf(1))]), "APPLIED");
  const baseOf = (deleted: boolean, six: number[]): Uint8Array =>
    update([group(6, 0, six), group(5, 0, typeUnderT), group(1, 0, gcOf(1))], ...(deleted ? [ds(5, 0, 1)] : []));
  // The key's 6:0 has the parent 5:0, which is deleted (hidden) in one key and not in the other; or it has a GC origin.
  const build = (deleted: boolean, six: number[]): Doc => {
    const d = new Doc();
    mustApply(d, baseOf(deleted, six), "APPLIED");
    return d;
  };
  const parentDeleted = build(true, anyInType);
  const parentLive = build(false, anyInType);
  const originDeleted = build(true, anyAfter(1, 0));
  const originLive = build(false, anyAfter(1, 0));

  const cases: [string, Doc, Uint8Array, string][] = [
    ["hidden in the key, the same item in the base", hiddenKey, update([group(2, 0, anyAfter(1, 0)), group(1, 0, gcOf(1))]), ""],
    ["hidden in the key, visible in the base", hiddenKey, update([group(2, 0, rootAny), group(1, 0, gcOf(1))]), "content"],
    ["visible in the key, hidden in the base", visibleKey, update([group(2, 0, anyAfter(1, 0)), group(1, 0, gcOf(1))]), "content"],
    ["visible in the key, the same item in the base", visibleKey, update([group(2, 0, rootAny), group(1, 0, gcOf(1))]), ""],
    ["a GC origin in the base, a deleted parent in the key: hidden on both sides", parentDeleted, baseOf(true, anyAfter(1, 0)), ""],
    ["a GC origin in the base, a live parent in the key", parentLive, baseOf(false, anyAfter(1, 0)), "content"],
    ["a deleted parent in the base, a GC origin in the key: hidden on both sides", originDeleted, baseOf(true, anyInType), ""],
    ["a live parent in the base, a GC origin in the key", originLive, baseOf(false, anyInType), "content"],
    ["a live parent, the same item", parentLive, baseOf(false, anyInType), ""],
  ];
  for (const [name, doc, base, want] of cases) {
    it(name, () => {
      expect(code(doc, base)).toBe(want);
    });
  }

  it("the content fault is a BaseError with the code the vectors write", () => {
    const err = validateBase(update([group(2, 0, rootAny), group(1, 0, gcOf(1))]), textRoots, hiddenKey) as BaseError;
    expect(err).toMatchObject({ fault: "content", code: "content" });
  });
});

/** The definitions of a client, as plain data. */
const defsOf = (d: Doc, client: number): string => JSON.stringify(d.defs.get(client) ?? []);
/** Everything that a Doc holds: its canonical form and the first definitions of every client, so that two Docs that agree fold every later update alike. */
const digest = (d: Doc): string => {
  const clients = [...d.defs.keys()].sort((a, b) => a - b);
  return `${Buffer.from(d.canonical()).toString("hex")}|${clients.map((c) => `${c}:${defsOf(d, c)}`).join(";")}|${d.present}`;
};

describe("05 [STATE-40] (V2 S3): a Doc that a reader holds never moves under a later update", () => {
  it("an update applied to a Doc after clone() changes the clone not at all, and an update applied to the clone changes the Doc not at all", () => {
    const live = new Doc();
    mustApply(live, update([group(5, 0, rootText("t", "ab"), textAfter(5, 1, "cd")), group(3, 0, anyAfter(5, 0))], ds(5, 1, 1)), "APPLIED");
    const before = digest(live);
    const c = live.clone();
    expect(digest(c)).toBe(before);
    // the same client gets a definition that does not join the one before it, and a new client appears
    mustApply(c, update([group(9, 0, rootText("t", "z")), group(5, 4, anyAfter(5, 3))]), "APPLIED");
    expect(digest(live), "an update applied to the clone moved the Doc").toBe(before);
    expect(digest(c)).not.toBe(before);
    // and the other way round
    const copyDigest = digest(c);
    mustApply(live, update([group(7, 0, rootText("t", "q")), group(5, 4, anyAfter(5, 3))], ds(3, 0, 1)), "APPLIED");
    expect(digest(c), "an update applied to the Doc moved the clone").toBe(copyDigest);
    expect(live.next(7)).toBe(1);
    expect(c.next(7)).toBe(0);
    expect(c.next(9)).toBe(1);
    expect(live.next(9)).toBe(0);
  });

  it("the definitions and delete ranges of a client that an update leaves alone are shared, and those that it changes are copied", () => {
    const d = new Doc();
    mustApply(d, update([group(5, 0, rootText("t", "a")), group(3, 0, rootText("t", "b"))], ds(3, 0, 1)), "APPLIED");
    const c = d.clone();
    expect(c.defs.get(3)).toBe(d.defs.get(3));
    mustApply(c, update([group(5, 1, anyAfter(5, 0))]), "APPLIED");
    expect(c.defs.get(3), "a client that the update did not touch").toBe(d.defs.get(3));
    expect(c.ds.get(3)).toBe(d.ds.get(3));
    expect(c.defs.get(5)).not.toBe(d.defs.get(5));
    expect(d.defs.get(5)).toHaveLength(1);
  });

  it("the fold of a key's record gives a reader's Doc that stays as it was (the twin of TestSTATE40_AReadersCRDTDocDoesNotMoveUnderLaterUpdates)", () => {
    // the Doc that the state held before the record, read after the record: through apply.ts, which clones before it applies
    const rules = { template: () => ({ rule: "crdt" as const, entity: false, parent: "" }) };
    const st = new ReplicaState();
    const rec = (seq: number, u: Uint8Array) =>
      create(RecordSchema, {
        seq: BigInt(seq),
        logEpoch: 1,
        kind: RecordKind.OP,
        manifestVersion: 1,
        serverTime: { seconds: 1000n, nanos: 0 },
        changes: [create(KeyChangeSchema, { key: "d/doc", kind: ChangeKind.CRDT_UPDATE, update: u, encoding: "yjs/v1" })],
      });
    applyRecord(st, rules, rec(1, update([group(5, 0, rootText("t", "a"))])));
    const held = st.key("d/doc");
    const heldDoc = held?.doc as Doc;
    const before = digest(heldDoc);
    applyRecord(st, rules, rec(2, update([group(5, 1, textAfter(5, 0, "b")), group(2, 0, rootText("t", "x"))])));
    expect(digest(heldDoc), "a Doc that a reader held moved under the next record").toBe(before);
    expect(st.key("d/doc")?.doc).not.toBe(heldDoc);
    expect(digest(st.key("d/doc")?.doc as Doc)).not.toBe(before);
  });
});

describe("02 [ARC-46], 05 [STATE-76]: the first definitions of a run of structs that agree are one definition", () => {
  it("a run of text items, a run of GCs, and a string that holds a surrogate pair", () => {
    const d = new Doc();
    // 5:0 'a', then items after one another under the same root: one definition, and across updates too
    mustApply(d, update([group(5, 0, rootText("t", "a"), textAfter(5, 0, "b"), textAfter(5, 1, "cd"))]), "APPLIED");
    mustApply(d, update([group(5, 4, textAfter(5, 3, "e"))]), "APPLIED");
    expect(d.defs.get(5), "a run of text items").toHaveLength(1);
    // a GC joins a GC before it, and a text item does not join a GC
    mustApply(d, update([group(5, 5, gcOf(2), gcOf(3), textAfter(5, 0, "x"))]), "APPLIED");
    expect(d.defs.get(5), "text, a run of GCs, text").toHaveLength(3);
    // a string that holds a surrogate pair keeps its own definition, with its units
    const d2 = new Doc();
    mustApply(d2, update([group(5, 0, rootText("t", "😀"), textAfter(5, 1, "b"))]), "APPLIED");
    expect(d2.defs.get(5), "a pair and a letter").toHaveLength(2);
  });

  it("the merged definitions answer every question that the separate ones did", () => {
    // a base over merged runs: the definition that holds an id covers the run, and a GC in a base over a run of GCs is valid
    const key = new Doc();
    mustApply(key, update([group(5, 0, gcOf(1), gcOf(1), gcOf(2))]), "APPLIED");
    expect(key.defs.get(5)).toHaveLength(1);
    expect(key.defAt({ client: 5, clock: 3 })).toMatchObject({ start: 0, end: 4, kind: "gc" });
    expect(code(key, update([group(5, 0, gcOf(4))]))).toBe("");
    // a struct that is partly below the next clock is still partial, whatever the definitions look like
    expect(key.apply(update([group(5, 2, gcOf(4))]), textRoots).verdict).toBe("NOOP_FOR_GOOD");
  });
});

// ---- the memory of a key (02 [ARC-46]) ----

/** A garbage collection, or null when the runtime has none to call. */
function gcFn(): (() => void) | null {
  try {
    v8.setFlagsFromString("--expose-gc");
    const f = vm.runInNewContext("gc") as unknown;
    return typeof f === "function" ? (f as () => void) : null;
  } catch {
    return null;
  }
}
const gc = gcFn();
const heap = (): number => {
  gc?.();
  gc?.();
  return process.memoryUsage().heapUsed;
};
const MB = 1 << 20;

describe("02 [ARC-46] (V2 MUST-1, V6 MUST-A): the memory of a read and of a key is bounded by the update's length, per client and per struct", () => {
  it.skipIf(gc === null)("an update of 2 million GC structs (4 MiB) leaves a Doc of one definition, and its parse is a fraction of what a struct of seventeen fields cost", () => {
    // the update of V2 M1: one client, 2,097,136 GC structs of length 1, two octets each, in a legal 4 MiB update
    const n = 2_097_136;
    const head = [1, ...uv(n), ...uv(7), 0];
    const b = new Uint8Array(head.length + 2 * n + 1);
    b.set(head);
    let p = head.length;
    for (let i = 0; i < n; i++) {
      b[p++] = 0;
      b[p++] = 1;
    }
    b[p++] = 0;
    expect(b.length).toBeLessThanOrEqual(4 * MB);

    const base = heap();
    let u: ReturnType<typeof parseUpdate> | null = parseUpdate(b);
    const parsed = heap() - base;
    const doc = new Doc();
    expect(doc.applyUpdate(u).verdict).toBe("APPLIED");
    u = null;
    const kept = heap() - base;
    expect(doc.defs.get(7)).toHaveLength(1);
    expect(doc.next(7)).toBe(n);
    // The update, read, held 500 MB before the struct of a GC lost its idle fields (about 240 B for each); the Doc held 275 MB more, a
    // definition for each struct. The bounds leave a wide margin over what they hold now and cut off both.
    expect(parsed / MB, `the read update retains ${Math.round(parsed / MB)} MB`).toBeLessThan(350);
    expect(kept / MB, `the Doc retains ${Math.round(kept / MB)} MB`).toBeLessThan(20);
  });

  it.skipIf(gc === null)("20,000 clients of one struct each cost about a record of each, not a preallocated block (27 KB a client before V6 MUST-A)", () => {
    const n = 20_000;
    const groups: number[][] = [];
    for (let c = n; c >= 1; c--) groups.push(group(c, 0, gcOf(1))); // descending client ids, one GC each: 7 octets a group
    const b = update(groups);
    expect(b.length).toBeLessThan(200_000);
    const base = heap();
    let u: ReturnType<typeof parseUpdate> | null = parseUpdate(b);
    const parsed = heap() - base;
    const doc = new Doc();
    doc.applyUpdate(u);
    u = null;
    const kept = heap() - base;
    expect(doc.sv.size).toBe(n);
    // about 400 B a client in each; a block of a few hundred definitions for each client would be 27 KB
    expect(parsed / n, `the read update retains ${Math.round(parsed / n)} B a client`).toBeLessThan(2048);
    expect(kept / n, `the Doc retains ${Math.round(kept / n)} B a client`).toBeLessThan(2048);
    // and a clone costs the clients, not a block for each: it shares the arrays
    const c = doc.clone();
    expect(c.defs.get(1)).toBe(doc.defs.get(1));
    expect(heap() - base - kept).toBeLessThan(10 * MB);
    expect(c.sv.size).toBe(n);
  });

  it("many small updates of many clients fold in time proportional to their size (the per-update constants are small)", () => {
    // V6 S-2: a 10-octet update cost 90 KB in Go after M1's fix. Here a tiny update costs a few small objects.
    const d = new Doc();
    mustApply(d, update([group(1, 0, rootText("t", "a"))]), "APPLIED");
    const start = performance.now();
    let cur = d;
    for (let i = 0; i < 2000; i++) {
      const u = update([group(1, 1 + i, anyAfter(1, i))]);
      cur = cur.clone();
      expect(cur.apply(u, textRoots).verdict).toBe("APPLIED");
    }
    expect(performance.now() - start).toBeLessThan(5000);
    expect(cur.next(1)).toBe(2001);
    expect(cur.defs.get(1)).toHaveLength(2); // the text item and a run of 2,000 items that share a definition
  });
});

// ---- 05 [STATE-76], Y9 and Z3 (Q33): the parent of an item is read from both its neighbours (the twin of server/internal/crdt/parents_test.go) ----

const parentRoots: Roots = new Map([
  ["t", "text"],
  ["u", "text"],
  ["a", "array"],
]);
/** A YText (type ref 2) under the root, and one that takes its parent from its origin. */
const rootType = (root: string): number[] => cat([0x07, 1], lstr(root), uv(2));
const typeAfter = (client: number, clock: number): number[] => cat([0x87], uv(client), uv(clock), uv(2));
/** A ContentString item whose parent is the item at (client, clock). */
const textIn = (client: number, clock: number, text: string): number[] => cat([0x04, 0], uv(client), uv(clock), lstr(text));
/** A ContentString item with an origin and a right origin, and one with a right origin only. */
const textBetween = (oc: number, ok: number, rc: number, rk: number, text: string): number[] =>
  cat([0xc4], uv(oc), uv(ok), uv(rc), uv(rk), lstr(text));
const textBefore = (client: number, clock: number, text: string): number[] => cat([0x44], uv(client), uv(clock), lstr(text));

function applyIn(d: Doc, b: Uint8Array, want: Verdict, reason?: string): void {
  const r = d.apply(b, parentRoots);
  expect({ verdict: r.verdict, reason: r.reason }, `${r.error?.message ?? ""}`).toEqual({ verdict: want, reason });
}
const hiddenIn = (d: Doc, client: number, clock: number): boolean => new Analysis(d, null).hidden({ client, clock });
const hasClient = (d: Doc, client: number): boolean => d.stateVector().some((e) => e.client === client);

// The nested texts P1 = 1:0 (kept) and P2 = 1:1 (deleted), the items O = 2:0 in P1 and R = 2:1 in P2, X = 3:0 a surrogate pair with origin O
// and right origin R, and an insert between the units of X's pair.
const pair = "\u{1F600}";
const parTypes = update([group(1, 0, rootType("a"), typeAfter(1, 0))]);
const parInner = update([group(2, 0, textIn(1, 0, "p"), textIn(1, 1, "q"))]);
const parDelP2 = update([], ds(1, 1, 1));
const parX = update([group(3, 0, textBetween(2, 0, 2, 1, pair))]);
const parSplit = update([group(4, 0, textAfter(3, 0, "z"))]);

describe("05 [STATE-76] (Y9, Q33): neighbours with different parents are a no-op for good", () => {
  it("X while P2 is live is a no-op, keeps nothing, and the later deletion of P2 does not integrate it", () => {
    const d = new Doc();
    applyIn(d, parTypes, "APPLIED");
    applyIn(d, parInner, "APPLIED");
    const before = d.canonical();
    applyIn(d, parX, "NOOP_FOR_GOOD", "cross-parent");
    expect(hasClient(d, 3)).toBe(false);
    expect(d.canonical()).toEqual(before);
    applyIn(d, parSplit, "NOOP_FOR_GOOD", "missing"); // nothing of it is kept: an insert that names it names an id that is not present
    applyIn(d, parDelP2, "APPLIED");
    expect(hasClient(d, 3)).toBe(false);
    // sent again, it finds R an item whose parent is hidden: integrated, hidden
    applyIn(d, parX, "APPLIED");
    expect(hiddenIn(d, 3, 0)).toBe(true);
  });

  it("items under different roots are different parents, and the names are what is compared", () => {
    const d = new Doc();
    applyIn(d, update([group(1, 0, rootText("t", "p"))]), "APPLIED");
    applyIn(d, update([group(2, 0, rootText("u", "q"))]), "APPLIED");
    applyIn(d, update([group(3, 0, textBetween(1, 0, 2, 0, "x"))]), "NOOP_FOR_GOOD", "cross-parent");
    expect(hasClient(d, 3)).toBe(false);
    applyIn(d, update([group(4, 0, rootText("t", "r"))]), "APPLIED");
    applyIn(d, update([group(5, 0, textBetween(1, 0, 4, 0, "x"))]), "APPLIED");
    expect(hiddenIn(d, 5, 0)).toBe(false);
    const c = d.clone();
    applyIn(c, update([group(6, 0, textBetween(2, 0, 4, 0, "x"))]), "NOOP_FOR_GOOD", "cross-parent");
    applyIn(c, update([group(6, 0, textBetween(4, 0, 1, 0, "x"))]), "APPLIED");
  });
});

describe("05 [STATE-76] (Z3): a neighbour that is a GC, an item whose parent is hidden or an item that takes its parent from a GC hides the item", () => {
  for (const [name, x] of [
    ["right origin", parX],
    ["origin", update([group(3, 0, textBetween(2, 1, 2, 0, pair))])],
  ] as const) {
    it(`an item whose ${name} is an item whose parent is hidden is integrated hidden (crdt-int-par-004, -005)`, () => {
      const d = new Doc();
      for (const b of [parTypes, parInner, parDelP2]) applyIn(d, b, "APPLIED");
      applyIn(d, x, "APPLIED");
      expect(hiddenIn(d, 3, 0) && hiddenIn(d, 3, 1)).toBe(true);
      const def = d.defAt({ client: 3, clock: 0 });
      expect(def).toMatchObject({ gcParent: true, hasParent: false, root: null });
      applyIn(d, parSplit, "APPLIED"); // the pair is hidden, so an insert inside it integrates
    });
  }

  it("a GC as the right origin of an item with a live origin, and as the only neighbour (crdt-int-par-009, -007)", () => {
    const d = new Doc();
    applyIn(d, update([group(1, 0, rootText("t", "p"))]), "APPLIED");
    applyIn(d, update([group(2, 0, gcOf(1))]), "APPLIED");
    applyIn(d, update([group(3, 0, textBetween(1, 0, 2, 0, pair))]), "APPLIED");
    expect(hiddenIn(d, 3, 0)).toBe(true);
    applyIn(d, parSplit, "APPLIED");
    const e = new Doc();
    applyIn(e, update([group(2, 0, gcOf(1))]), "APPLIED");
    applyIn(e, update([group(3, 0, textBefore(2, 0, pair))]), "APPLIED");
    expect(hiddenIn(e, 3, 0)).toBe(true);
  });

  it("an item whose origin or right origin takes its parent from a GC is hidden, not a no-op, though its other neighbour is visible (crdt-int-par-011)", () => {
    const d = new Doc();
    applyIn(d, update([group(1, 0, gcOf(1))]), "APPLIED");
    applyIn(d, update([group(2, 0, textAfter(1, 0, "v"))]), "APPLIED");
    applyIn(d, update([group(3, 0, rootText("t", "w"))]), "APPLIED");
    applyIn(d, update([group(4, 0, textBetween(2, 0, 3, 0, "y"))]), "APPLIED");
    applyIn(d, update([group(5, 0, textBetween(3, 0, 2, 0, pair))]), "APPLIED");
    for (const [c, k] of [[2, 0], [4, 0], [5, 0], [5, 1]] as const) expect(hiddenIn(d, c, k), `${c}:${k}`).toBe(true);
    applyIn(d, update([group(6, 0, textAfter(5, 0, "q"))]), "APPLIED");
    expect(hiddenIn(d, 3, 0)).toBe(false);
  });
});

describe("05 [STATE-74] (Z3, Q33, Q53): the base rules that follow", () => {
  const baseCode = (key: Doc, b: Uint8Array): string => validateBase(b, parentRoots, key)?.code ?? "";
  const folded = (...updates: Uint8Array[]): Doc => {
    const d = new Doc();
    for (const b of updates) applyIn(d, b, "APPLIED");
    return d;
  };

  it("R as a GC and X as first defined is a valid base (crdt-base-021); R kept as an item of the deleted P2 is pending when read alone", () => {
    const key = folded(parTypes, parInner, parDelP2, parX);
    const hand = update(
      [
        group(3, 0, textBetween(2, 0, 2, 1, pair)),
        group(2, 0, textIn(1, 0, "p"), gcOf(1)),
        group(1, 0, rootType("a"), cat([0x81], uv(1), uv(0), uv(1))),
      ],
      ds(1, 1, 1),
    );
    expect(baseCode(key, hand)).toBe("");
    const kept = update(
      [group(3, 0, textBetween(2, 0, 2, 1, pair)), group(2, 0, textIn(1, 0, "p"), textIn(1, 1, "q")), group(1, 0, rootType("a"), typeAfter(1, 0))],
      ds(1, 1, 1),
    );
    expect(baseCode(key, kept)).toBe("pending:cross-parent");
  });

  it("a GC in place of an item whose origin is a GC, or that takes its parent from one, is valid (crdt-base-022)", () => {
    const key = folded(
      update([group(1, 0, gcOf(1))]),
      update([group(2, 0, textAfter(1, 0, "v"))]),
      update([group(3, 0, textAfter(2, 0, "w"))]),
    );
    expect(baseCode(key, update([group(3, 0, gcOf(1)), group(2, 0, gcOf(1)), group(1, 0, gcOf(1))]))).toBe("");
  });

  it("a ContentDeleted over an id that a GC first defines is refused, whatever its parent (Q53)", () => {
    const key = folded(update([group(1, 0, gcOf(2))], ds(1, 0, 2)));
    expect(baseCode(key, update([group(1, 0, gcOf(2))], ds(1, 0, 2)))).toBe("");
    // 1:1 a ContentDeleted whose origin is the GC 1:0: it takes its parent from a GC, a hidden one
    expect(baseCode(key, update([group(1, 0, gcOf(1), cat([0x81], uv(1), uv(0), uv(1)))], ds(1, 0, 2)))).toBe("content");
    // 1:0 a ContentDeleted under the root t: a visible parent
    expect(baseCode(key, update([group(1, 0, cat([0x01, 1], lstr("t"), uv(1)), gcOf(1))], ds(1, 0, 2)))).toBe("content");
  });

  it("an item under another root than its first definition's is another content", () => {
    const key = folded(update([group(1, 0, rootText("t", "p"))]));
    expect(baseCode(key, update([group(1, 0, rootText("t", "p"))]))).toBe("");
    expect(baseCode(key, update([group(1, 0, rootText("u", "p"))]))).toBe("content");
  });
});

describe("02 [ARC-46]: the walk that reads the parent of an item through its neighbours", () => {
  it("a chain of neighbours across 20,000 client groups costs the update's time and no stack", () => {
    const n = 20_000;
    const chain = (first: number[]): Uint8Array => {
      const groups: number[][] = [];
      for (let c = n; c >= 1; c--) groups.push(group(c, 0, c === 1 ? first : textAfter(c - 1, 0, "a")));
      return update(groups);
    };
    const start = performance.now();
    const d = new Doc();
    applyIn(d, chain(rootText("t", "a")), "APPLIED");
    expect(performance.now() - start).toBeLessThan(3000);
    expect(d.defAt({ client: n, clock: 0 })).toMatchObject({ gcParent: false, hasParent: false, root: "t" });
    const e = new Doc();
    applyIn(e, chain(gcOf(1)), "APPLIED");
    expect(e.defAt({ client: n, clock: 0 })?.gcParent).toBe(true);
    expect(hiddenIn(e, n, 0)).toBe(true);
  });
});

// ---- 05 [STATE-71] (Values), round 17: a lib0 object holds each member name once (the twin of TestSTATE71_AnyObjectMemberNamesOccurOnce) ----
//
// No conformance vector covers it (the two reference models count a duplicate once; the text rejects it: a recorded spec question), so these are the tests.

describe("05 [STATE-71] (Values, round 17): the member names of a lib0 any object occur once", () => {
  /** One ContentAny item of one value under the root a, as the Go test builds it: info 0x08, parent-info 1, the root name, a count of 1, the value. */
  const anyItem = (...vals: number[][]): Uint8Array => update([group(5, 0, cat([0x08, 1], lstr("a"), uv(1), ...vals))]);
  /** An object whose members are the names, each with the integer 1. */
  const obj = (...members: string[]): number[] => cat([118], uv(members.length), ...members.map((m) => cat(lstr(m), [125, 1])));
  const reasonOf = (b: Uint8Array): string => {
    try {
      parseUpdate(b);
      return "";
    } catch (e) {
      return (e as { reason?: string }).reason ?? String(e);
    }
  };

  for (const [name, members, ok] of [
    ["none", [], true],
    ["one", ["a"], true],
    ["two", ["a", "b"], true],
    ["the same twice", ["a", "a"], false],
    ["apart", ["a", "b", "c", "a"], false],
    ["nine, the last twice", ["a", "b", "c", "d", "e", "f", "g", "h", "h"], false],
    ["nine, the ninth the first", ["a", "b", "c", "d", "e", "f", "g", "h", "a"], false],
    ["ten distinct", ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"], true],
    ["names of other lengths", ["a", "ab", "abc", "b", "ba"], true],
    ["empty names twice", ["", ""], false],
    ["a name that is not ASCII, twice", ["é", "x", "é"], false],
    ["two names that differ only in the case", ["a", "A"], true],
  ] as [string, string[], boolean][]) {
    it(`${name}: ${ok ? "well formed" : "any-duplicate"}`, () => {
      const b = anyItem(obj(...members));
      expect(reasonOf(b)).toBe(ok ? "" : "any-duplicate");
      const r = new Doc().fold(b);
      if (ok) expect(r.verdict).toBe("APPLIED");
      else expect(r).toMatchObject({ verdict: "SCHEMA_INVALID", reason: "any-duplicate" });
    });
  }

  it("1,024 distinct names are within the member limit, a duplicate among 1,024 is a duplicate, and 1,025 are over the limit", () => {
    const names = Array.from({ length: 1024 }, (_, i) => `k${i}`);
    expect(reasonOf(anyItem(obj(...names)))).toBe("");
    expect(reasonOf(anyItem(obj(...names.slice(0, 1023), "k0")))).toBe("any-duplicate");
    expect(reasonOf(anyItem(obj(...names, "k1024")))).toBe("any-limits");
  });

  it("a nested object has its own names, and a duplicate inside a nested object or an array is found", () => {
    const outer = (a: number[], b: number[]): number[] => cat([118, 2], lstr("a"), a, lstr("b"), b);
    expect(reasonOf(anyItem(outer(obj("a"), obj("a", "a"))))).toBe("any-duplicate");
    expect(reasonOf(anyItem(outer(obj("a"), obj("a"))))).toBe("");
    expect(reasonOf(anyItem(cat([117, 2], obj("a", "a"), obj("a"))))).toBe("any-duplicate");
    expect(reasonOf(anyItem(cat([117, 2], obj("a"), obj("a"))))).toBe("");
  });

  it("the check runs in the order of the octets: a duplicate name before a fault of the same object, and a fault before a later duplicate", () => {
    // {a: 1, a: <tag 99>}: the second name is the duplicate, and its value is never read
    expect(reasonOf(anyItem(cat([118, 2], lstr("a"), [125, 1], lstr("a"), [99])))).toBe("any-duplicate");
    // {a: <tag 99>, a: 1}: the first value is the fault
    expect(reasonOf(anyItem(cat([118, 2], lstr("a"), [99], lstr("a"), [125, 1])))).toBe("any-tag");
  });
});

// ---- 05 [STATE-74] (round 17, D-AP (a)): the document length, beyond the vectors and the Yjs store (the twin of server/internal/crdt/measure_test.go) ----

describe("05 [STATE-74] document length: each content in its one canonical form", () => {
  const roots: Roots = new Map([
    ["a", "array"],
    ["t", "text"],
  ]);
  const lengthOf = (...updates: Uint8Array[]): number => {
    const d = new Doc();
    for (const b of updates) {
      const r = d.apply(b, roots);
      expect(r.verdict, `${r.reason ?? ""} ${r.error?.message ?? ""}`).toBe("APPLIED");
    }
    return d.documentLength();
  };
  /** The lib0 varInt of a number: 6 bits in the first octet and the sign in 0x40, then 7 in each. */
  const varInt = (mag: number, neg = false): number[] => {
    const out: number[] = [];
    let first = mag % 64;
    mag = Math.floor(mag / 64);
    if (neg) first |= 0x40;
    if (mag > 0) first |= 0x80;
    out.push(first);
    while (mag > 0) {
      let b = mag % 128;
      mag = Math.floor(mag / 128);
      if (mag > 0) b |= 0x80;
      out.push(b);
    }
    return out;
  };
  const f64 = (x: number): number[] => [...new Uint8Array(new Float64Array([x]).buffer)].reverse();
  const f32 = (x: number): number[] => [...new Uint8Array(new Float32Array([x]).buffer)].reverse();
  /** An update of one ContentAny item under the root a: a header of 4 (info, parent-info, the name "a"), the count and the values. */
  const anyOf = (...vals: number[][]): Uint8Array => update([group(5, 0, cat([0x08, 1], lstr("a"), uv(vals.length), ...vals))]);
  /** The document of one client's group of one struct of `struct` octets: the count of clients, the group's count, id and first clock, the struct, the empty delete set. */
  const docOf = (struct: number): number => 1 + 3 + struct + 1;

  it("the empty document is of 2 octets, and a GC is a byte and a varUint of its length", () => {
    expect(new Doc().documentLength()).toBe(2);
    expect(lengthOf(update([group(5, 0, gcOf(1))]))).toBe(docOf(2));
    expect(lengthOf(update([group(5, 0, gcOf(300))]))).toBe(docOf(1 + 2));
  });

  const values: [string, number[], number][] = [
    ["an integer 1 under tag 125", [125, 1], 2],
    ["a negative zero under tag 125", [125, 0x40], 2],
    ["64, which takes two octets as a varInt", [125, ...varInt(64)], 3],
    ["-64", [125, ...varInt(64, true)], 3],
    ["8191, two octets", [125, ...varInt(8191)], 3],
    ["8192, three octets", [125, ...varInt(8192)], 4],
    ["2^31-1, five octets as a varInt", [125, ...varInt(2147483647)], 6],
    ["-(2^31-1)", [125, ...varInt(2147483647, true)], 6],
    ["2^31 under tag 125: another number, which a float32 holds", [125, ...varInt(2147483648)], 5],
    ["3000000001 under tag 125: no float32 holds it", [125, ...varInt(3000000001)], 9],
    ["2^53-1 under tag 125", [125, ...varInt(9007199254740991)], 9],
    ["1 under tag 123 (a float64): an integer goes under 125", [123, ...f64(1)], 2],
    ["-0 under tag 123", [123, ...f64(-0)], 2],
    ["0.5 under tag 123: a float32 holds it", [123, ...f64(0.5)], 5],
    ["0.1 under tag 123", [123, ...f64(0.1)], 9],
    ["1e300", [123, ...f64(1e300)], 9],
    ["2^32 under tag 123: a float32 holds it", [123, ...f64(2 ** 32)], 5],
    ["2^32+256 under tag 123: a float32 does not", [123, ...f64(2 ** 32 + 256)], 9],
    ["16777217.5, which no float32 holds", [123, ...f64(16777217.5)], 9],
    ["3 under tag 124 (a float32): an integer goes under 125", [124, ...f32(3)], 2],
    ["0.5 under tag 124", [124, ...f32(0.5)], 5],
    ["an int64 of 5 under tag 122 stays under 122", [122, 0, 0, 0, 0, 0, 0, 0, 5], 9],
    ["a string of two octets", [119, 2, 104, 105], 4],
    ["a string that is not ASCII: the octets of its UTF-8", [119, ...lstr("é€")], 1 + 1 + 5],
    ["null", [126], 1],
    ["undefined", [127], 1],
    ["true", [120], 1],
    ["false", [121], 1],
    ["a Uint8Array of two octets", [116, 2, 1, 2], 4],
    ["an array of an integer and a string", [117, 2, 125, 1, 119, 1, 120], 1 + 1 + 2 + 3],
    ["an object with one member", cat([118, 1], lstr("k"), [125, 1]), 1 + 1 + 2 + 2],
    ["an empty object", [118, 0], 2],
  ];
  for (const [name, enc, size] of values) {
    it(`a ContentAny value is counted by its value: ${name}: ${size} octets`, () => {
      expect(lengthOf(anyOf(enc))).toBe(docOf(4 + 1 + size));
    });
  }

  it("a ContentAny of several values counts their octets and its count", () => {
    const some = values.slice(0, 12);
    expect(lengthOf(anyOf(...some.map(([, enc]) => enc)))).toBe(docOf(4 + 1 + some.reduce((n, [, , size]) => n + size, 0)));
  });

  it("a ContentString is its UTF-8, and a count of more than 127 octets takes two", () => {
    expect(lengthOf(update([group(5, 0, rootText("t", "abc"))]))).toBe(docOf(4 + 1 + 3));
    expect(lengthOf(update([group(5, 0, rootText("t", "é€😀"))]))).toBe(docOf(4 + 1 + 2 + 3 + 4)); // 9 octets
    expect(lengthOf(update([group(5, 0, rootText("t", "a".repeat(200)))]))).toBe(docOf(4 + 2 + 200));
  });

  it("a ContentEmbed and a ContentFormat count the RFC 8785 form of their JSON text, whatever the update holds", () => {
    // {"b": 1, "a": [1.0, "x"]} is {"a":[1,"x"],"b":1}, 19 octets
    const embed = update([group(5, 0, cat([0x05, 1], lstr("t"), lstr('{"b": 1, "a":  [1.0, "x"]}')))]);
    expect(lengthOf(embed)).toBe(docOf(4 + 1 + 19));
    const sorted = update([group(5, 0, cat([0x05, 1], lstr("t"), lstr('{"a":[1,"x"],"b":1}')))]);
    expect(lengthOf(sorted)).toBe(docOf(4 + 1 + 19));
    // the key "bold" and the value " true " (canonical: true)
    const format = update([group(5, 0, cat([0x06, 1], lstr("t"), lstr("bold"), lstr(" true ")))]);
    expect(lengthOf(format)).toBe(docOf(4 + 5 + 5));
    // 1e2 is 100, 1.50 is 1.5, and an escape that has a shorter form is written as that
    const num = update([group(5, 0, cat([0x05, 1], lstr("t"), lstr('[1e2, 1.50, "\\u0041"]')))]);
    expect(lengthOf(num)).toBe(docOf(4 + 1 + '[100,1.5,"A"]'.length));
  });

  it("a ContentBinary and a ContentType count their own octets, and the name of a type that has one", () => {
    expect(lengthOf(update([group(5, 0, cat([0x03, 1], lstr("a"), [2, 1, 2]))]))).toBe(docOf(4 + 3));
    expect(lengthOf(update([group(5, 0, cat([0x07, 1], lstr("a"), uv(0)))]))).toBe(docOf(4 + 1)); // a YArray
    expect(lengthOf(update([group(5, 0, cat([0x07, 1], lstr("a"), uv(3), lstr("div")))]))).toBe(docOf(4 + 1 + 4)); // an XmlElement named div
  });

  it("the groups are in descending order of client, and the counts of clients and of delete-set clients are varUints", () => {
    // 130 clients of one GC each: clients 1 to 127 have a one-octet id and 128 to 130 a two-octet one
    const groups: number[][] = [];
    for (let c = 130; c >= 1; c--) groups.push(group(c, 0, gcOf(1)));
    // the count of clients (130: two octets), 130 groups of (a count, the id, the clock, a GC of 2), and the empty delete set
    expect(lengthOf(update(groups))).toBe(2 + 127 * 5 + 3 * 6 + 1);
    // with every id deleted a GC stays a GC, and the delete set holds 130 groups of (client, one range, clock, length)
    const dsGroups: number[][] = [];
    for (let c = 130; c >= 1; c--) dsGroups.push(ds(c, 0, 1));
    expect(lengthOf(update(groups, ...dsGroups))).toBe(2 + 127 * 5 + 3 * 6 + 2 + 127 * 4 + 3 * 5);
  });
});

describe("05 [STATE-74] document length: Cuts, Merges and the cover of a hidden parent", () => {
  const roots: Roots = new Map([
    ["t", "text"],
    ["a", "array"],
  ]);

  it("deleting a ContentType item hides what has it as its parent, in every client: the children are GCs (the twin of the Go test)", () => {
    const typ = cat([0x07, 1], lstr("a"), uv(0)); // 5:0, an array type under the root a
    const child = textIn(5, 0, "ab"); // 5:1..2, text whose parent is the type 5:0
    const other = textIn(5, 0, "xyz"); // 6:0..2
    const doc = new Doc();
    expect(doc.apply(update([group(6, 0, other), group(5, 0, typ, child)]), roots).verdict).toBe("APPLIED");
    // 2 clients; group 6: (1 + 1 + 1) + (header 4 + count 1 + 3); group 5: (1 + 1 + 1) + (the type: 4 + 1) + (the child: 4 + 1 + 2); the empty delete set
    const before = 1 + (3 + 8) + (3 + 5 + 7) + 1;
    expect(doc.documentLength()).toBe(before);
    const next = doc.clone();
    expect(next.apply(update([], ds(5, 0, 1)), roots).verdict).toBe("APPLIED");
    // 5:0 is a ContentDeleted (header 4, length 1), 5:1..2 and 6:0..2 are GCs (2 octets each); the delete set: 1 client, 1 range
    const after = 1 + (3 + 2) + (3 + 5 + 2) + (1 + 4);
    expect(next.documentLength()).toBe(after);
    expect(doc.documentLength(), "the Doc that was cloned does not move under its clone").toBe(before);
    expect(next.clone().documentLength()).toBe(after);
  });

  it("a type that is deleted after a dozen clients have put text under it, on Docs that a reader measured at each step", () => {
    const typ = cat([0x07, 1], lstr("a"), uv(0));
    let doc = new Doc();
    doc.apply(update([group(5, 0, typ)]), roots);
    for (let c = 0; c < 12; c++) {
      const next = doc.clone();
      expect(next.apply(update([group(100 + c, 0, textIn(5, 0, "k".repeat(c + 1)))]), roots).verdict).toBe("APPLIED");
      doc = next;
      doc.documentLength(); // a reader measures every Doc
    }
    const live = doc.documentLength();
    const hidden = doc.clone();
    hidden.apply(update([], ds(5, 0, 1)), roots);
    // each child is a GC of its own client's group (a count, the id, the clock, 2 octets); the type is a ContentDeleted; the delete set has one range
    expect(hidden.documentLength()).toBe(1 + 12 * (3 + 2) + (3 + 5) + (1 + 4));
    expect(doc.documentLength()).toBe(live);
    expect(live).toBeGreaterThan(hidden.documentLength());
  });

  it("a text typed one character at a time is one struct, however many updates and clones it took (Merges)", () => {
    const n = 3000;
    let doc = new Doc();
    for (let i = 0; i < n; i++) {
      const next = doc.clone();
      const b = i === 0 ? update([group(7, 0, rootText("t", "a"))]) : update([group(7, i, textAfter(7, i - 1, "a"))]);
      expect(next.apply(b, roots).verdict).toBe("APPLIED");
      doc = next;
    }
    // one struct: a header of 4, the count of 3000 octets (two octets), the text; the group's count, client 7 and first clock
    const one = 1 + (1 + 1 + 1 + 4 + 2 + n) + 1;
    expect(doc.documentLength()).toBe(one);
    expect(doc.defs.get(7)).toHaveLength(1);
    // another client puts one character between the 2nd and the 3rd: [STATE-74] cuts only where the cover changes, so client 7 stays one struct
    const mid = doc.clone();
    expect(mid.apply(update([group(8, 0, cat([0xc4], uv(7), uv(1), uv(7), uv(2), lstr("b")))]), roots).verdict).toBe("APPLIED");
    const eight = 1 + 2 + 2 + 1 + 1; // the info byte, the origin 7:1, the right origin 7:2, the text "b" (a count and an octet)
    expect(mid.documentLength()).toBe(1 + (1 + 1 + 1 + eight) + (1 + 1 + 1 + 4 + 2 + n) + 1);
    expect(doc.documentLength(), "the Doc that was cloned").toBe(one);
  });

  it("a clone that extends a run of GCs, or a chained text, in place leaves the Doc that it was cloned from as it was (02 [ARC-28])", () => {
    const d = new Doc();
    expect(d.apply(update([group(1, 0, gcOf(2))]), roots).verdict).toBe("APPLIED");
    expect(d.apply(update([group(2, 0, rootText("t", "ab"))]), roots).verdict).toBe("APPLIED");
    // clients 2; group 2: (1+1+1) + (header 4 + count 1 + 2); group 1: (1+1+1) + a GC of 2; the empty delete set
    const before = 1 + (3 + 7) + (3 + 2) + 1;
    expect(d.documentLength()).toBe(before);
    // 130 GCs of one id each, in one group: they are one run, and join the GC that the Doc has; "c" and "d" typed one by one join "ab"
    const gcs = Array.from({ length: 130 }, () => gcOf(1));
    const c = d.clone();
    expect(c.apply(update([group(2, 2, textAfter(2, 1, "c"), textAfter(2, 2, "d")), group(1, 2, ...gcs)]), roots).verdict).toBe("APPLIED");
    // the GC is of 132 ids (a varUint of two octets: 3); the text is of 4 octets
    const after = 1 + (3 + 4 + 1 + 4) + (3 + 3) + 1;
    expect(c.documentLength()).toBe(after);
    expect(d.documentLength(), "the Doc that was cloned").toBe(before);
    // a clone of the clone does it again, and neither of the two before it moves
    const e = c.clone();
    expect(e.apply(update([group(2, 4, textAfter(2, 3, "e")), group(1, 132, gcOf(200))]), roots).verdict).toBe("APPLIED");
    expect(e.documentLength()).toBe(1 + (3 + 4 + 1 + 5) + (3 + 3) + 1);
    expect(c.documentLength()).toBe(after);
    expect(d.documentLength()).toBe(before);
  });

  it("a text that is deleted in the middle is three structs: the text, a ContentDeleted whose origin is the last id before it, the text again", () => {
    const d = new Doc();
    expect(d.apply(update([group(7, 0, rootText("t", "abcdefghij"))], ds(7, 3, 4)), roots).verdict).toBe("APPLIED");
    // "abc" (header 4, count 1, 3), a ContentDeleted of 4 (info, origin 7:2 = 2, length 1 = 4), "hij" (info, origin 7:6 = 2, count 1, 3 = 7)
    // the group: a count of 3, the client, the clock; the delete set: a count of 1 and (client, 1 range, clock 3, length 4)
    expect(d.documentLength()).toBe(1 + (3 + (4 + 1 + 3) + (1 + 2 + 1) + (1 + 2 + 1 + 3)) + (1 + 4));
  });
});

// ---- the document length against a count made id by id: a text and a ContentAny of one client, cut at random ----

describe("05 [STATE-74] document length: a text and a ContentAny cut by random deletions, against a count made id by id", () => {
  const roots: Roots = new Map([
    ["t", "text"],
    ["a", "array"],
  ]);
  const client = 300; // a two-octet client id
  const uvl = (n: number): number => uv(n).length;

  let seed = 20260511;
  const rnd = (n: number): number => {
    seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
    return Math.floor((seed / 2 ** 32) * n);
  };

  /**
   * The length that the steps of 05 [STATE-74] give for the ids of one client, typed as one struct (its header is `hdr0`) of which `deleted`
   * are deleted, counted one id at a time: one struct for each run of ids of one cover, a struct after the first has an info byte and the last
   * id before it as its origin; a live run counts its octets (and for a ContentAny its count), a deleted one its length; then the delete set.
   */
  function perId(octets: number[], deleted: boolean[], hdr0: number, isAny: boolean): number {
    let structs = 0;
    let sum = 0;
    const ranges: [number, number][] = [];
    for (let i = 0; i < octets.length; ) {
      let j = i;
      while (j < octets.length && deleted[j] === deleted[i]) j++;
      const hdr = i === 0 ? hdr0 : 1 + uvl(client) + uvl(i - 1);
      if (deleted[i]) {
        sum += hdr + uvl(j - i);
        ranges.push([i, j - i]);
      } else {
        let b = 0;
        for (let k = i; k < j; k++) b += octets[k] as number;
        sum += isAny ? hdr + uvl(j - i) + b : hdr + uvl(b) + b;
      }
      structs++;
      i = j;
    }
    const group = uvl(structs) + uvl(client) + 1 + sum;
    let dsn = 0;
    if (ranges.length > 0) dsn = uvl(client) + uvl(ranges.length) + ranges.reduce((n, [c, l]) => n + uvl(c) + uvl(l), 0);
    return 1 + group + 1 + dsn; // one client, its group, the count of delete-set clients (0 or 1: one octet), its group
  }

  /** Applies one to three updates of deletions, each on a clone, to doc: the Doc that has them, and the ids that they hold. */
  function deleteSome(doc: Doc, bounds: number[], n: number): { doc: Doc; deleted: boolean[] } {
    const deleted = new Array<boolean>(n).fill(false);
    let cur = doc;
    for (let k = 1 + rnd(3); k > 0; k--) {
      const ranges: number[] = [];
      for (let r = 1 + rnd(4); r > 0; r--) {
        const a = rnd(bounds.length - 1); // a boundary between code points, or between values
        const b = a + 1 + rnd(Math.min(6, bounds.length - 1 - a));
        const lo = bounds[a] as number;
        const hi = bounds[b] as number;
        ranges.push(lo, hi - lo);
        for (let i = lo; i < hi; i++) deleted[i] = true;
      }
      const next = cur.clone();
      const res = next.apply(update([], ds(client, ...ranges)), roots);
      expect(["APPLIED", "NO_EFFECT"], `${res.reason ?? ""} ${res.error?.message ?? ""}`).toContain(res.verdict);
      cur = next;
    }
    return { doc: cur, deleted };
  }

  const none = (n: number): boolean[] => new Array<boolean>(n).fill(false);
  const flags = (d: boolean[]): string => d.map((x) => (x ? 1 : 0)).join("");
  /** The octets of each UTF-16 unit of a string, and the unit offset of each code point boundary. */
  function unitsOf(chars: string[]): { octets: number[]; bounds: number[] } {
    const octets: number[] = [];
    const bounds: number[] = [0];
    for (const ch of chars) {
      const cp = ch.codePointAt(0) as number;
      if (cp >= 0x10000) octets.push(2, 2);
      else octets.push(cp < 0x80 ? 1 : cp < 0x800 ? 2 : 3);
      bounds.push(octets.length);
    }
    return { octets, bounds };
  }

  it("a text of characters of one to four octets: 1, 2, 3, and a pair of surrogates (two units of two octets each)", () => {
    const alphabet = ["a", "b", "é", "ß", "€", "中", "😀", "𝒳"];
    for (let n = 0; n < 400; n++) {
      const chars = Array.from({ length: 1 + rnd(30) }, () => alphabet[rnd(alphabet.length)] as string);
      const { octets, bounds } = unitsOf(chars);
      const text = chars.join("");
      const base = new Doc();
      expect(base.apply(update([group(client, 0, rootText("t", text))]), roots).verdict).toBe("APPLIED");
      expect(base.documentLength(), JSON.stringify(text)).toBe(perId(octets, none(octets.length), 4, false));
      const { doc, deleted } = deleteSome(base, bounds, octets.length);
      expect(doc.documentLength(), `${JSON.stringify(text)} deleted ${flags(deleted)}`).toBe(perId(octets, deleted, 4, false));
      expect(base.documentLength(), "the Doc that was cloned").toBe(perId(octets, none(octets.length), 4, false));
    }
  });

  it("a ContentAny of values of different sizes", () => {
    const vals: [number[], number][] = [
      [[125, 1], 2],
      [[125, 0x80, 0x01], 3],
      [[123, 0x3f, 0xb9, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a], 9],
      [[124, 0x3f, 0, 0, 0], 5],
      [[119, 3, 97, 98, 99], 5],
      [[126], 1],
      [[116, 2, 1, 2], 4],
      [[118, 1, 1, 107, 125, 1], 6],
    ];
    for (let n = 0; n < 400; n++) {
      const picked = Array.from({ length: 1 + rnd(24) }, () => vals[rnd(vals.length)] as [number[], number]);
      const octets = picked.map(([, size]) => size);
      const bounds = octets.map((_, i) => i).concat([octets.length]);
      const base = new Doc();
      const item = cat([0x08, 1], lstr("a"), uv(picked.length), ...picked.map(([enc]) => enc));
      expect(base.apply(update([group(client, 0, item)]), roots).verdict).toBe("APPLIED");
      expect(base.documentLength()).toBe(perId(octets, none(octets.length), 4, true));
      const { doc, deleted } = deleteSome(base, bounds, octets.length);
      expect(doc.documentLength(), `deleted ${flags(deleted)}`).toBe(perId(octets, deleted, 4, true));
    }
  });

  it("a text typed in pieces by several updates, whose characters are of different widths (the pieces chain), and cut across the pieces", () => {
    const alphabet = ["a", "é", "€", "😀"];
    for (let n = 0; n < 200; n++) {
      const pieces: string[] = [];
      for (let p = 1 + rnd(5); p > 0; p--) pieces.push(Array.from({ length: 1 + rnd(5) }, () => alphabet[rnd(alphabet.length)] as string).join(""));
      let doc = new Doc();
      let clock = 0;
      for (const piece of pieces) {
        const b = clock === 0 ? update([group(client, 0, rootText("t", piece))]) : update([group(client, clock, textAfter(client, clock - 1, piece))]);
        const next = doc.clone();
        expect(next.apply(b, roots).verdict).toBe("APPLIED");
        doc = next;
        clock += unitsOf(Array.from(piece)).octets.length;
      }
      const { octets, bounds } = unitsOf(Array.from(pieces.join("")));
      // the pieces are one struct, as one update of the whole text would be (the header of the first piece, the octets of all)
      expect(doc.documentLength(), JSON.stringify(pieces)).toBe(perId(octets, none(octets.length), 4, false));
      const { doc: cut, deleted } = deleteSome(doc, bounds, octets.length);
      expect(cut.documentLength(), `${JSON.stringify(pieces)} deleted ${flags(deleted)}`).toBe(perId(octets, deleted, 4, false));
    }
  });
});

describe("05 [STATE-74] document length: its cost follows the shape of the document, not the number of its updates", () => {
  const roots: Roots = new Map([["t", "text"]]);

  it("a text of 20,000 characters typed by two clients in turns (no two chain), then every third id of one client deleted", () => {
    const n = 20_000;
    let doc = new Doc();
    const next: [number, number] = [0, 0]; // the next clock of clients 1 and 2
    let prev: [number, number] | null = null; // the id of the character before
    for (let i = 0; i < n; i++) {
      const c = i % 2 === 0 ? 2 : 1; // client 2 types the first character (the root's), then 1, 2, 1, ...
      const clock = next[c - 1] as number;
      const body = prev === null ? rootText("t", "a") : textAfter(prev[0], prev[1], i % 3 === 0 ? "é" : "a");
      const cl = doc.clone();
      expect(cl.apply(update([group(c, clock, body)]), roots).verdict).toBe("APPLIED");
      doc = cl;
      next[c - 1] = clock + 1;
      prev = [c, clock];
    }
    const t0 = performance.now();
    const len = doc.documentLength();
    const ms = performance.now() - t0;
    // each character is its own struct (a header, an origin of the other client), so the document is about 7 octets for each
    expect(len).toBeGreaterThan(n * 5);
    expect(ms, `${ms} ms for the document length of ${n} structs`).toBeLessThan(1000);
    // a delete of every third id of client 1 changes the cover at each of them
    const ranges: number[] = [];
    for (let k = 0; k < 9000; k += 3) ranges.push(k, 1);
    const d2 = doc.clone();
    expect(d2.apply(update([], ds(1, ...ranges)), roots).verdict).toBe("APPLIED");
    const t1 = performance.now();
    const len2 = d2.documentLength();
    expect(performance.now() - t1).toBeLessThan(1000);
    expect(len2).not.toBe(len);
    expect(doc.documentLength(), "the Doc that was cloned").toBe(len);
  });
});

// ---- 05 [STATE-71], Nesting (round 18, H2; round 19, m1): the twins of server/internal/crdt/nesting_test.go ----
//
// The depth of a ContentType item is the number of ContentType items on the chain of its parents, itself included, with the parents of 05 [STATE-76], the chain
// ending at a hidden id, which counts as no type; depth 32 is accepted and depth 33 is refused with SCHEMA_INVALID (the reason `nesting`), judged with the key's
// integrated updates and before the fold, and the chain is walked without recursion. The vectors `crdt-type-depth-001` to `-022` fix the cases of the text; these
// tests build the shapes that they do not.

/** A ContentType item (a YText, type ref 2) whose parent is the item at (client, clock). */
const typeIn = (client: number, clock: number): number[] => cat([0x07, 0], uv(client), uv(clock), uv(2));
const skipOf = (n: number): number[] => cat([10], uv(n));

/** An update of one client group of many structs (a spread of 300,000 arguments would overflow the stack of the test, not of the code under test). */
function bigUpdate(client: number, from: number, n: number, struct: (clock: number) => number[]): Uint8Array {
  const out: number[] = [...uv(1), ...uv(n), ...uv(client), ...uv(from)];
  for (let i = 0; i < n; i++) for (const b of struct(from + i)) out.push(b);
  out.push(0); // an empty delete set
  return Uint8Array.from(out);
}

/** The update of one client group, clocks from `from`, that holds n ContentType items: the first under the root "t" when from is 0, each other under the one before it. */
const nestUpdate = (client: number, from: number, n: number): Uint8Array =>
  bigUpdate(client, from, n, (clock) => (clock === 0 ? rootType("t") : typeIn(client, clock - 1)));

/** The update of `client`, from clock 0, that holds n ContentType items: the first under the item at (parentClient, parentClock), each other under the one before it. */
const nestUnder = (client: number, parentClient: number, parentClock: number, n: number): Uint8Array =>
  update([group(client, 0, typeIn(parentClient, parentClock), ...Array.from({ length: n - 1 }, (_, i) => typeIn(client, i)))]);

function wantNesting(d: Doc, b: Uint8Array): void {
  const before = Buffer.from(d.canonical()).toString("hex");
  const present = d.present;
  const r = d.apply(b, textRoots);
  expect({ verdict: r.verdict, reason: r.reason }).toEqual({ verdict: "SCHEMA_INVALID", reason: "nesting" });
  expect(r.error?.reason).toBe("nesting");
  expect(d.present, "a refused update leaves the key as it was").toBe(present);
  expect(Buffer.from(d.canonical()).toString("hex")).toBe(before);
}

describe("05 [STATE-71] Nesting: depth 32 is accepted and depth 33 is refused", () => {
  it("in one update and across two: the refused update leaves the key as it was", () => {
    const d = new Doc();
    mustApply(d, nestUpdate(5, 0, MAX_NESTING), "APPLIED");
    for (const c of [0, 15, 31]) expect(d.defAt({ client: 5, clock: c })?.depth, `the depth stored at clock ${c}`).toBe(c + 1);
    wantNesting(d, nestUpdate(5, MAX_NESTING, 1)); // a 33rd level, in a second update
    mustApply(d, update([group(6, 0, textIn(5, MAX_NESTING - 1, "x"))]), "APPLIED");

    const fresh = new Doc();
    wantNesting(fresh, nestUpdate(5, 0, MAX_NESTING + 1));
    expect(fresh.present).toBe(false);
    const u = parseUpdate(nestUpdate(5, 0, MAX_NESTING + 1));
    expect(() => fresh.checkNesting(u)).toThrow(/nesting/);
    expect(() => fresh.checkNesting(parseUpdate(nestUpdate(5, 0, MAX_NESTING)))).not.toThrow();
  });

  it("ends the chain at a hidden item (round 19, m1): a type under a deleted string below the last level of a nest is at depth 1", () => {
    for (const levels of [MAX_NESTING - 1, MAX_NESTING]) {
      const d = new Doc();
      mustApply(d, nestUpdate(5, 0, levels), "APPLIED");
      mustApply(d, update([group(6, 0, textIn(5, levels - 1, "s"))]), "APPLIED");
      mustApply(d, update([], ds(6, 0, 1)), "APPLIED"); // the string is deleted: a hidden parent that is no type
      mustApply(d, update([group(7, 0, typeIn(6, 0))]), "APPLIED");
      expect(d.defAt({ client: 7, clock: 0 })?.depth, `${levels} levels`).toBe(1);
    }
  });

  it("counts only its own levels for a nest placed under a deleted or a hidden level 32 (crdt-type-depth-014, -016)", () => {
    const cases: [string, (d: Doc) => void, number, boolean][] = [
      ["level 32 deleted, 32 under it", (d) => mustApply(d, update([], ds(5, MAX_NESTING - 1, 1)), "APPLIED"), MAX_NESTING, true],
      ["level 32 deleted, 33 under it", (d) => mustApply(d, update([], ds(5, MAX_NESTING - 1, 1)), "APPLIED"), MAX_NESTING + 1, false],
      ["level 1 deleted, 32 under level 32", (d) => mustApply(d, update([], ds(5, 0, 1)), "APPLIED"), MAX_NESTING, true],
      ["level 1 deleted, 33 under level 32", (d) => mustApply(d, update([], ds(5, 0, 1)), "APPLIED"), MAX_NESTING + 1, false],
    ];
    for (const [name, hide, levels, accept] of cases) {
      const d = new Doc();
      mustApply(d, nestUpdate(5, 0, MAX_NESTING), "APPLIED");
      hide(d);
      expect(new Analysis(d, null).hidden({ client: 5, clock: MAX_NESTING - 1 }), `${name}: level 32 is hidden`).toBe(true);
      const x = nestUnder(6, 5, MAX_NESTING - 1, levels);
      if (accept) mustApply(d, x, "APPLIED");
      else wantNesting(d, x);
    }
  });

  it("does not bound the levels under a hidden type (crdt-type-depth-022; D-AT is open with the owner)", () => {
    const d = new Doc();
    mustApply(d, nestUpdate(5, 0, 1), "APPLIED");
    mustApply(d, update([], ds(5, 0, 1)), "APPLIED"); // level 1 is deleted: everything below it is hidden
    let parent: [number, number] = [5, 0];
    for (let round = 0; round < 6; round++) {
      const client = 10 + round;
      mustApply(d, nestUnder(client, parent[0], parent[1], MAX_NESTING), "APPLIED");
      parent = [client, MAX_NESTING - 1];
    }
    wantNesting(d, nestUnder(30, parent[0], parent[1], MAX_NESTING + 1)); // 33 levels in one update are refused below a hidden type, as anywhere
  });

  it("judges an update that is a no-op for good like any other, and gives a struct that 05 [STATE-76] ignores no depth (crdt-type-depth-008, -009)", () => {
    const d = new Doc();
    mustApply(d, nestUpdate(5, 0, MAX_NESTING), "APPLIED");
    wantNesting(d, update([group(6, 0, typeIn(5, MAX_NESTING - 1), skipOf(3))])); // a Skip: a no-op for good, and a 33rd level
    mustApply(d, update([group(5, MAX_NESTING, textIn(5, MAX_NESTING - 1, "x"))]), "APPLIED");
    mustApply(d, update([group(5, MAX_NESTING, typeIn(5, MAX_NESTING - 1))]), "NO_EFFECT"); // wholly below the next clock: ignored
  });

  it("stops at a GC and at a parent that is not present", () => {
    const d = new Doc();
    mustApply(d, update([group(5, 0, gcOf(1))]), "APPLIED");
    mustApply(d, update([group(6, 0, typeIn(5, 0))]), "APPLIED");
    expect(d.defAt({ client: 6, clock: 0 })?.depth, "a type under a GC").toBe(1);
    mustApply(d, update([group(7, 0, typeIn(9, 9))]), "NOOP_FOR_GOOD");
  });
});

describe("02 [ARC-46], 05 [STATE-71] Nesting: the chain is walked without recursion, and each item once", () => {
  it("a nest of 300,000 levels in one update is refused, and a fold of the log (which does not judge the bound again) saturates its depths", () => {
    const n = 300000;
    const b = nestUpdate(5, 0, n);
    const t0 = performance.now();
    wantNesting(new Doc(), b);
    expect(performance.now() - t0).toBeLessThan(20000);
    const d = new Doc();
    mustApply(d, nestUpdate(5, 0, MAX_NESTING), "APPLIED");
    wantNesting(d, nestUpdate(5, MAX_NESTING, n - MAX_NESTING));
    const old = new Doc();
    expect(old.fold(b).verdict).toBe("APPLIED");
    expect(old.defAt({ client: 5, clock: n - 1 })?.depth, "saturated").toBe(255);
    wantNesting(old, update([group(6, 0, typeIn(5, n - 1))]));
  });

  it("a type under the end of a long chain of hidden items costs one walk of the chain for each check, which no recursion makes", () => {
    // 05 [STATE-71]: "a server computes each id's parent and depth once per check, keeping them for that check only and never between checks, so the cost is
    // linear in the structs of the update and the ids it reaches". A hidden id counts as no type, so the check asks whether the parent is hidden, which walks
    // the chain up to the delete range; this client keeps no memory of the answer between checks, as the server's hidCache does.
    const chain = 60000;
    const checks = 30;
    const d = new Doc();
    expect(d.fold(update([group(5, 0, rootType("t"), textIn(5, 0, "s"))])).verdict).toBe("APPLIED"); // the type 5:0 and the string 5:1 under it
    expect(d.fold(update([], ds(5, 1, 1))).verdict).toBe("APPLIED"); // the string is deleted: everything below it is hidden
    // 5:2.., each the child of the one before it
    expect(d.fold(bigUpdate(5, 2, chain, (clock) => textIn(5, clock - 1, "x"))).verdict).toBe("APPLIED");
    const tail = chain + 1;
    expect(new Analysis(d, null).hidden({ client: 5, clock: tail }), "the tail of the chain is hidden").toBe(true);
    const us = Array.from({ length: checks }, (_, i) => parseUpdate(update([group(6, i, typeIn(5, tail))])));
    const t0 = performance.now();
    for (const u of us) d.checkNesting(u);
    const ms = performance.now() - t0;
    expect(ms, `${checks} checks under a chain of ${chain} hidden items took ${ms} ms`).toBeLessThan(2000);
    mustApply(d, update([group(6, 0, typeIn(5, tail))]), "APPLIED");
    expect(d.defAt({ client: 6, clock: 0 })?.depth, "a type under the hidden tail counts only itself").toBe(1);
  });
});
