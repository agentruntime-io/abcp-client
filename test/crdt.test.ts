// CRDT vectors (conformance/vectors/crdt.json): the strict reader (05 [STATE-71]), the fold (05 [STATE-76]), the canonical
// form and per-key hash (05 [STATE-41]), the validity of a base (05 [STATE-74]) and the rename of a CRDT key (06 [LOG-40]).
// Not run here, by design: `expect.content` and `library.*`, the checks of a library's document (the vector README).

import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { fromHex, toHex } from "../src/rules/bytes.js";
import { Doc, integrated, parseUpdate, validateBase, type RootType, type Roots } from "../src/rules/crdt/index.js";
import { keyStatePreimage, keyStateHash, StateValue, type KeyValueState } from "../src/rules/hashes/index.js";
import { mergeUpdates } from "./helpers/yjs-merge.js";
import { loadVectors, type Vector } from "./helpers/vectors.js";

const all = loadVectors("crdt.json");
const ofKind = (k: string): Vector[] => all.filter((v) => v.kind === k);

const toRoots = (m: Record<string, string>): Roots => new Map(Object.entries(m) as [string, RootType][]);

interface Step {
  update: string;
  expect: string;
  reason?: string;
  seq?: number;
}

interface Folded {
  seq: number;
  /** the updates that were integrated, in seq order */
  integrated: Uint8Array[];
  /** every committed update, a no-op for good included */
  committed: Uint8Array[];
}

/** Folds the steps onto doc under roots, as the vector README says, and checks each verdict, its reason and its seq. */
function runSteps(doc: Doc, roots: Roots, steps: readonly Step[], seq0: number): Folded {
  const f: Folded = { seq: seq0, integrated: [], committed: [] };
  steps.forEach((st, i) => {
    const b = fromHex(st.update);
    const res = doc.apply(b, roots);
    if (st.expect === "BASE") {
      // 05 [STATE-74]: a verified base at crdt_covered_seq = seq, integrated as the first update of the key
      expect(integrated(res.verdict), `step ${i}: the base is ${res.verdict} (${res.reason})`).toBe(true);
      f.seq = st.seq as number;
      f.integrated.push(b);
      f.committed.push(b);
      return;
    }
    expect(res.verdict, `step ${i} (${res.reason})`).toBe(st.expect);
    if (st.reason !== undefined) expect(res.reason, `step ${i}`).toBe(st.reason);
    if (res.verdict === "SCHEMA_INVALID") return;
    f.seq++;
    f.committed.push(b);
    if (integrated(res.verdict)) f.integrated.push(b);
    if (st.seq !== undefined && st.seq !== 0) expect(f.seq, `step ${i}: seq`).toBe(st.seq);
  });
  return f;
}

const svPairs = (doc: Doc): [number, number][] => doc.stateVector().map((e) => [e.client, e.clock]);
const dsEntries = (doc: Doc): [number, [number, number][]][] =>
  doc.deleteSet().map((c) => [c.client, c.ranges.map((r): [number, number] => [r.clock, r.len])]);

// the bases that Yjs JS built for the vectors that give `base_from` and no bytes, which the server's tests keep
const libraryBasesPath = fileURLToPath(new URL("../../server/internal/crdt/testdata/library-bases.json", import.meta.url));
const libraryBases: Record<string, { base_from: string; hex: string }> = existsSync(libraryBasesPath)
  ? (JSON.parse(readFileSync(libraryBasesPath, "utf8")) as { bases: Record<string, { base_from: string; hex: string }> }).bases
  : {};

/**
 * The candidate base of a vector: its bytes, the merge that `mergeUpdates` builds, or, for `doc`, the bytes that Yjs JS built
 * (the encoder of a library, not a rule of the spec). Returns undefined when a library's bytes are needed and not at hand.
 */
function builtBase(id: string, hexBase: string | undefined, from: string | undefined, f: Folded): Uint8Array | undefined {
  if (hexBase !== undefined) return fromHex(hexBase);
  const lib = libraryBases[id];
  if (from === "merge" || from === "merge-all") {
    const got = mergeUpdates(from === "merge-all" ? f.committed : f.integrated);
    if (lib !== undefined) expect(toHex(got), "mergeUpdates against Yjs's").toBe(lib.hex);
    return got;
  }
  if (from === "doc") return lib !== undefined ? fromHex(lib.hex) : undefined;
  throw new Error(`no base: base_from ${from}`);
}

describe("05 [STATE-71], [STATE-76], [STATE-41] CRDT fold (crdt-fold vectors)", () => {
  for (const v of ofKind("crdt-fold")) {
    it(`${v.id}: ${v.title}`, () => {
      const doc = new Doc();
      const f = runSteps(doc, toRoots(v.crdt_roots), v.steps, 0);
      expect(doc.present).toBe(v.expect.present);
      if (!v.expect.present) return;
      expect(f.seq).toBe(v.expect.version);
      expect(svPairs(doc)).toEqual(v.expect.state_vector);
      expect(dsEntries(doc)).toEqual(v.expect.delete_set);
      expect(toHex(doc.canonical())).toBe(v.expect.canonical);
      // the per-key state hash of the key at its version, a CRDT key hashing its canonical form
      const ks = { key: v.key as string, state: StateValue as KeyValueState, encoding: "yjs/v1", content: doc.canonical(), version: f.seq };
      expect(toHex(keyStatePreimage(ks))).toBe(v.expect.state_hash_preimage);
      expect(toHex(keyStateHash(ks))).toBe(v.expect.state_hash);
    });
  }
});

describe("05 [STATE-74] base validity (crdt-base vectors)", () => {
  for (const v of ofKind("crdt-base")) {
    it(`${v.id}: ${v.title}`, (ctx) => {
      const roots = toRoots(v.crdt_roots);
      const key = new Doc();
      const f = runSteps(key, roots, v.covered, 0);
      expect(f.seq).toBe(v.crdt_covered_seq);
      expect(toHex(key.canonical())).toBe(v.expect.canonical);
      // a crdt-base vector's hash is that of its covered fold, whether or not the candidate base is valid; the base holds the key at crdt_covered_seq
      const ks = { key: v.key as string, state: StateValue as KeyValueState, encoding: "yjs/v1", content: key.canonical(), version: v.crdt_covered_seq };
      expect(toHex(keyStateHash(ks))).toBe(v.expect.state_hash);

      const base = builtBase(v.id, v.base, v.base_from, f);
      if (base === undefined) return ctx.skip();
      const err = validateBase(base, roots, key);
      expect(err === null, `${err?.code}`).toBe(v.expect.valid);
      if (err !== null && v.expect.reason !== undefined) expect(err.code).toBe(v.expect.reason);
    });
  }
});

describe("06 [LOG-40] rename of a CRDT key (crdt-rename vectors)", () => {
  for (const v of ofKind("crdt-rename")) {
    it(`${v.id}: ${v.title.slice(0, 100)}`, (ctx) => {
      const roots = toRoots(v.crdt_roots);
      const src = new Doc();
      const f = runSteps(src, roots, v.source_steps, 0);
      // 06 [LOG-40], 04 [MAN-65]: the SYSTEM SET of the target key holds a base of the source key's integrated updates
      const base = builtBase(v.id, v.base, v.base_from, f);
      if (base === undefined) return ctx.skip();
      const err = validateBase(base, roots, src);
      expect(err === null, `${err?.code}`).toBe(v.expect.base_valid);
      if (err !== null) {
        if (v.expect.base_reason !== undefined) expect(err.code).toBe(v.expect.base_reason);
        return;
      }
      // 05 [STATE-41]: the base is the first of the target key's integrated updates, so its canonical form just after the SET
      // is the source key's just before the record
      const dst = new Doc();
      const res = dst.apply(base, roots);
      expect(integrated(res.verdict), `the base is ${res.verdict} (${res.reason})`).toBe(true);
      expect(toHex(src.canonical())).toBe(v.expect.canonical_at_conversion);
      expect(toHex(dst.canonical())).toBe(v.expect.canonical_at_conversion);
      const g = runSteps(dst, roots, v.later_steps, v.conversion_seq);
      expect(g.seq).toBe(v.expect.version);
      expect(toHex(dst.canonical())).toBe(v.expect.canonical);
      const ks = { key: v.key as string, state: StateValue as KeyValueState, encoding: "yjs/v1", content: dst.canonical(), version: g.seq };
      expect(toHex(keyStateHash(ks))).toBe(v.expect.state_hash);
    });
  }
});

describe("05 [STATE-71] strictness beyond the vectors", () => {
  it("never throws anything but a CrdtError, whatever the bytes (02 [ARC-46])", () => {
    let x = 7;
    const seed = [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x07, 0x08, 0x0a, 0x41, 0x7f, 0x80, 0x81, 0xff, 0x74, 0x75, 0x7b];
    for (let n = 0; n < 5000; n++) {
      const b = new Uint8Array((n % 40) + 1);
      for (let i = 0; i < b.length; i++) {
        x = (x * 1103515245 + 12345) & 0x7fffffff;
        b[i] = seed[x % seed.length] as number;
      }
      const doc = new Doc();
      const res = doc.fold(b);
      expect(["SCHEMA_INVALID", "NOOP_FOR_GOOD", "NO_EFFECT", "APPLIED"]).toContain(res.verdict);
    }
  });

  it("a varUint longer than 64 bits is a refusal and not an overflow", () => {
    const b = new Uint8Array([0x01, ...new Array(200).fill(0xff), 0x01, 0x00, 0x00]);
    expect(() => parseUpdate(b)).toThrow();
    expect(new Doc().fold(b).verdict).toBe("SCHEMA_INVALID");
  });

  it("a refused update leaves the key as it was and a no-op for good makes an absent key present (05 [STATE-76])", () => {
    const doc = new Doc();
    expect(doc.fold(new Uint8Array([0xff])).verdict).toBe("SCHEMA_INVALID");
    expect(doc.present).toBe(false);
    // client 5 inserts "a" with origin 9:0, which the update does not define and the key has not: a no-op for good
    const res = doc.fold(fromHex("01010500" + "84" + "0900" + "0161" + "00"));
    expect(res).toEqual({ verdict: "NOOP_FOR_GOOD", reason: "missing" });
    expect(doc.present).toBe(true);
    expect(toHex(doc.canonical())).toBe("0000000000000000"); // an empty document: SV and DS both empty
  });
});
