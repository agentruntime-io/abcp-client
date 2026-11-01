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
import { Doc, parseUpdate, validateBase, type BaseError, type Roots, type Verdict } from "../src/rules/crdt/index.js";
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
