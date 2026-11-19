// CRDT vectors (conformance/vectors/crdt.json): the strict reader (05 [STATE-71]), the fold (05 [STATE-76]), the canonical
// form and per-key hash (05 [STATE-41]), the validity of a base (05 [STATE-74]) and the rename of a CRDT key (06 [LOG-40]).
//
// And the document check of 17 [CONF-9] ("Where a CRDT vector gives an expected document, the document that each of them folds MUST
// also equal it, root by root"), for every vector that has one (Q33, client side): the Yjs document that a client shows (14
// [CLI-13]: stock Yjs, which the engine uses) after the updates that the spec integrates is compared with `expect.content`, per root;
// for a base, the base loaded alone is compared with the covered updates folded (`expect.content_matches_fold`) and with `expect.content`;
// for a rename, the base and the later updates. Nothing is skipped: the vectors where a library differs from the spec are listed by id
// below (`KNOWN_LIBRARY_DIFFERENCES`: since draft 14 yrs only, stock Yjs agrees), and a difference anywhere else fails the run.

import { existsSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import * as Y from "yjs";
import { fromHex, toHex } from "../src/rules/bytes.js";
import { Doc, integrated, parseUpdate, validateBase, type RootType, type Roots } from "../src/rules/crdt/index.js";
import { keyStatePreimage, keyStateHash, StateValue, type KeyValueState } from "../src/rules/hashes/index.js";
import { mergeUpdates } from "./helpers/yjs-merge.js";
import { HAVE_VECTORS, loadVectors, type Vector } from "./helpers/vectors.js";
import { CRDT_TESTDATA_DIR } from "./helpers/server-dir.js";
import { describeNeeds } from "./helpers/describe-needs.js";

const all = loadVectors("crdt.json");
const ofKind = (k: string): Vector[] => all.filter((v) => v.kind === k);

const toRoots = (m: Record<string, string>): Roots => new Map(Object.entries(m) as [string, RootType][]);

// ---- the documents that a library shows (Q33, client side; the twin of runner-yjs/run.mjs) ----

/**
 * The vectors where a library document differs from the spec one, by id, and which library (the vector's `library.known`). Since draft 14
 * (rulings Y9 and Z3, Q33) the text follows Yjs: an item whose origin or right origin is a GC, an item whose parent is hidden or an item
 * that takes its parent from a GC is hidden, and an item whose origin and right origin are items with different parents is a no-op for
 * good (05 [STATE-76]), so stock Yjs, which this client shows (14 [CLI-13]), agrees with the text on every vector with a document, and the
 * vectors list yrs only: it shows the item of `crdt-int-par-004`, `-005`, `-009` and `-011` and of the base of `crdt-base-021` (a neighbour
 * that is a GC, or an item whose parent is deleted, does not hide the item for it), and aborts on `crdt-any-008`. An entry here is the one
 * place where a library document may differ, and it is a yrs one. `shows` is the document that a Yjs library gives, for an entry that
 * names yjs, so that a difference that moves fails too; no entry does now.
 */
const KNOWN_LIBRARY_DIFFERENCES: Record<string, { library: "yjs" | "yrs"; shows?: Record<string, unknown> }> = {
  "crdt-any-008": { library: "yrs" },
  "crdt-int-par-004": { library: "yrs" },
  "crdt-int-par-005": { library: "yrs" },
  "crdt-int-par-009": { library: "yrs" },
  "crdt-int-par-011": { library: "yrs" },
  "crdt-base-021": { library: "yrs" },
};

/**
 * The vectors of the pinned spec (draft 22, the v2.0-rc.5, omniview 715f597) that the client's rules do not pass yet, with the work that will
 * make them pass (the twin of the server's `vectors.SkipPending`). Remove an id when its work lands. Empty: the nesting vectors of round 18
 * (`crdt-type-depth-002`, `-003`, `-004`, `-006`, `-009`, R18-B1) and of round 19 (`-010` to `-022`, R19-B1) pass.
 */
const PENDING = new Map<string, string>([]);
const vit = (id: string) => (PENDING.has(id) ? it.skip : it);

/** Stable JSON for a comparison of documents: object keys sorted. */
const norm = (x: unknown): string =>
  JSON.stringify(x, (_k, v: unknown) =>
    v !== null && typeof v === "object" && !Array.isArray(v) ? Object.fromEntries(Object.entries(v as object).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) : v,
  );

const newYDoc = (roots: Record<string, string>): Y.Doc => {
  const d = new Y.Doc();
  for (const [n, t] of Object.entries(roots)) {
    if (t === "text") d.getText(n);
    else if (t === "map") d.getMap(n);
    else if (t === "array") d.getArray(n);
    else d.getXmlFragment(n);
  }
  return d;
};

/** The document of every root, as a library shows it; a text root is a delta when the expected content gives one. */
function contentOf(d: Y.Doc, roots: Record<string, string>, expected: Record<string, unknown> | undefined): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [n, t] of Object.entries(roots)) {
    if (t === "text") out[n] = Array.isArray(expected?.[n]) ? d.getText(n).toDelta() : d.getText(n).toString();
    else if (t === "map") out[n] = d.getMap(n).toJSON();
    else if (t === "array") out[n] = d.getArray(n).toJSON();
    else out[n] = d.getXmlFragment(n).toString();
  }
  return JSON.parse(JSON.stringify(out, (_k, x: unknown) => (typeof x === "bigint" ? Number(x) : x))) as Record<string, unknown>;
}

const pick = (obj: Record<string, unknown>, keys: string[]): Record<string, unknown> => Object.fromEntries(keys.map((k) => [k, obj[k]]));

/** The Yjs document that integrating the updates gives, in order. */
function yDocOf(roots: Record<string, string>, updates: readonly Uint8Array[], base?: Uint8Array): Y.Doc {
  const d = newYDoc(roots);
  if (base !== undefined) Y.applyUpdate(d, base);
  for (const u of updates) Y.applyUpdate(d, u);
  return d;
}

/**
 * 17 [CONF-9]: the document of a library after the updates equals `expect.content`, root by root. For a vector that lists the library as
 * a known difference it must be the document that the table gives, and not the spec one.
 */
function expectDocument(id: string, roots: Record<string, string>, d: Y.Doc, expected: Record<string, unknown>): void {
  const got = pick(contentOf(d, roots, expected), Object.keys(expected));
  const known = KNOWN_LIBRARY_DIFFERENCES[id];
  if (known?.library === "yjs") {
    expect(norm(got), `${id}: Yjs is listed as differing from the spec, and shows the document of the table`).toBe(norm(known.shows));
    expect(norm(got), `${id}: the difference is gone: remove the entry`).not.toBe(norm(expected));
  } else {
    expect(norm(got), `${id}: the Yjs document`).toBe(norm(expected));
  }
}

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
const libraryBasesPath = CRDT_TESTDATA_DIR === undefined ? undefined : CRDT_TESTDATA_DIR + "/library-bases.json";
const libraryBases: Record<string, { base_from: string; hex: string }> = libraryBasesPath !== undefined && existsSync(libraryBasesPath)
  ? (JSON.parse(readFileSync(libraryBasesPath, "utf8")) as { bases: Record<string, { base_from: string; hex: string }> }).bases
  : {};

/**
 * The candidate base of a vector: its bytes, the merge that `mergeUpdates` builds, or, for `doc`, the bytes that Yjs JS built
 * (the encoder of a library, not a rule of the spec).
 */
function builtBase(id: string, hexBase: string | undefined, from: string | undefined, f: Folded, roots: Record<string, string>): Uint8Array {
  if (hexBase !== undefined) return fromHex(hexBase);
  const lib = libraryBases[id];
  if (from === "merge" || from === "merge-all") {
    const got = mergeUpdates(from === "merge-all" ? f.committed : f.integrated);
    if (lib !== undefined) expect(toHex(got), "mergeUpdates against Yjs's").toBe(lib.hex);
    return got;
  }
  if (from === "doc") {
    // the encoded state of a Yjs document that integrated the updates (the encoder of a library, not a rule of the spec)
    const built = Y.encodeStateAsUpdate(yDocOf(roots, f.integrated));
    if (lib !== undefined) expect(toHex(built), "the base that Yjs builds against the one that the server's tests keep").toBe(lib.hex);
    return built;
  }
  throw new Error(`no base: base_from ${from}`);
}

describeNeeds(HAVE_VECTORS, "05 [STATE-71], [STATE-76], [STATE-41] CRDT fold (crdt-fold vectors)", () => {
  for (const v of ofKind("crdt-fold")) {
    vit(v.id)(`${v.id}: ${v.title}`, () => {
      const doc = new Doc();
      const f = runSteps(doc, toRoots(v.crdt_roots), v.steps, 0);
      expect(doc.present).toBe(v.expect.present);
      if (!v.expect.present) return;
      expect(f.seq).toBe(v.expect.version);
      expect(svPairs(doc)).toEqual(v.expect.state_vector);
      expect(dsEntries(doc)).toEqual(v.expect.delete_set);
      expect(toHex(doc.canonical())).toBe(v.expect.canonical);
      // 05 [STATE-74] (round 17, D-AP (a)): the length in octets of the key's document, when the vector states it (crdt-doclen-001 to -005)
      if (v.expect.document_length !== undefined) {
        expect(doc.documentLength(), `${v.id}: the document length`).toBe(v.expect.document_length);
        // it is a function of the key's state: a clone has it, and the Doc that was cloned keeps it when the clone goes on
        const copy = doc.clone();
        expect(copy.documentLength(), `${v.id}: the length of a clone`).toBe(v.expect.document_length);
      }
      // the per-key state hash of the key at its version, a CRDT key hashing its canonical form
      const ks = { key: v.key as string, state: StateValue as KeyValueState, encoding: "yjs/v1", content: doc.canonical(), version: f.seq };
      expect(toHex(keyStatePreimage(ks))).toBe(v.expect.state_hash_preimage);
      expect(toHex(keyStateHash(ks))).toBe(v.expect.state_hash);
      // 17 [CONF-9]: the document that a client shows after the updates that the spec integrates
      if (v.expect.content) expectDocument(v.id, v.crdt_roots, yDocOf(v.crdt_roots, f.integrated), v.expect.content);
    });
  }
});

describeNeeds(HAVE_VECTORS, "05 [STATE-74] document length (crdt-doclen vectors, expect.document_length)", () => {
  const withLength = ofKind("crdt-fold").filter((v) => v.expect?.document_length !== undefined);
  it("the vectors that state it are crdt-doclen-001 to -005, and every one is a crdt-fold vector that the loop above checks", () => {
    expect(withLength.map((v) => v.id)).toEqual(["crdt-doclen-001", "crdt-doclen-002", "crdt-doclen-003", "crdt-doclen-004", "crdt-doclen-005"]);
    expect(all.filter((v) => v.expect?.document_length !== undefined).length).toBe(withLength.length);
  });
  for (const v of withLength) {
    it(`${v.id}: ${v.expect.document_length} octets, at the end of the steps and after each step that has a length of its own`, () => {
      const doc = new Doc();
      const f = runSteps(doc, toRoots(v.crdt_roots), v.steps, 0);
      expect(doc.documentLength()).toBe(v.expect.document_length);
      // the Doc folded at once from the integrated updates (as a rebuild of the key does) has the same length
      const again = new Doc();
      for (const b of f.integrated) again.fold(b);
      expect(again.documentLength()).toBe(v.expect.document_length);
      // a Doc that is cloned before each update (as the fold does) has it too
      let cur = new Doc();
      for (const b of f.integrated) {
        const next = cur.clone();
        next.fold(b);
        cur = next;
      }
      expect(cur.documentLength()).toBe(v.expect.document_length);
    });
  }
  it("the empty key has the empty document, 0x00 0x00, of length 2, and a key whose only update is a no-op for good has it too", () => {
    const doc = new Doc();
    expect(doc.documentLength()).toBe(2);
    // client 5 inserts "a" with origin 9:0, which nothing defines: a no-op for good
    expect(doc.fold(fromHex("01010500" + "84" + "0900" + "0161" + "00")).verdict).toBe("NOOP_FOR_GOOD");
    expect(doc.documentLength()).toBe(2);
  });
});

describeNeeds(HAVE_VECTORS, "05 [STATE-74] base validity (crdt-base vectors)", () => {
  for (const v of ofKind("crdt-base")) {
    vit(v.id)(`${v.id}: ${v.title}`, () => {
      const roots = toRoots(v.crdt_roots);
      const key = new Doc();
      const f = runSteps(key, roots, v.covered, 0);
      expect(f.seq).toBe(v.crdt_covered_seq);
      expect(toHex(key.canonical())).toBe(v.expect.canonical);
      // a crdt-base vector's hash is that of its covered fold, whether or not the candidate base is valid; the base holds the key at crdt_covered_seq
      const ks = { key: v.key as string, state: StateValue as KeyValueState, encoding: "yjs/v1", content: key.canonical(), version: v.crdt_covered_seq };
      expect(toHex(keyStateHash(ks))).toBe(v.expect.state_hash);

      const base = builtBase(v.id, v.base, v.base_from, f, v.crdt_roots);
      const err = validateBase(base, roots, key);
      expect(err === null, `${err?.code}`).toBe(v.expect.valid);
      if (err !== null && v.expect.reason !== undefined) expect(err.code).toBe(v.expect.reason);
      if (err !== null) return;
      // 05 [STATE-74] (the MAY check): the base loaded alone against the covered updates folded, with the same library, and the document
      // that the vector gives
      let loaded: Record<string, unknown> | undefined;
      try {
        loaded = contentOf(yDocOf(v.crdt_roots, [], base), v.crdt_roots, v.expect.content);
      } catch {
        loaded = undefined; // the library cannot load it
      }
      const folded = contentOf(yDocOf(v.crdt_roots, f.integrated), v.crdt_roots, v.expect.content);
      if (v.expect.content_matches_fold !== undefined) {
        expect(loaded !== undefined && norm(loaded) === norm(folded), `${v.id}: the base loaded alone against the covered updates folded`).toBe(v.expect.content_matches_fold);
      }
      if (v.expect.content) {
        expect(norm(pick(loaded ?? {}, Object.keys(v.expect.content))), `${v.id}: the base loaded alone`).toBe(norm(v.expect.content));
      }
    });
  }
});

describeNeeds(HAVE_VECTORS, "06 [LOG-40] rename of a CRDT key (crdt-rename vectors)", () => {
  for (const v of ofKind("crdt-rename")) {
    vit(v.id)(`${v.id}: ${v.title.slice(0, 100)}`, () => {
      const roots = toRoots(v.crdt_roots);
      const src = new Doc();
      const f = runSteps(src, roots, v.source_steps, 0);
      // 06 [LOG-40], 04 [MAN-65]: the SYSTEM SET of the target key holds a base of the source key's integrated updates
      const base = builtBase(v.id, v.base, v.base_from, f, v.crdt_roots);
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
      // 17 [CONF-9]: the document of the target key, the base and the later updates
      if (v.expect.content) expectDocument(v.id, v.crdt_roots, yDocOf(v.crdt_roots, g.integrated, base), v.expect.content);
    });
  }
});

describeNeeds(HAVE_VECTORS, "17 [CONF-9], Q33: no vector hides a difference between the documents of a library and the spec", () => {
  // at draft 14 the text follows Yjs (Y9, Z3, Q33): the vectors list yrs differences only, and 76 give a document
  it("the vectors with a document are all checked, and the known library differences are exactly those that the vectors list", () => {
    const withContent = all.filter((v) => v.expect?.content !== undefined && v.expect.content !== null);
    expect(withContent.length, "vectors that give an expected document at draft 18").toBe(100);
    const listed = all.filter((v) => v.library?.known !== undefined).map((v) => [v.id, Object.keys(v.library.known as object).sort()] as const);
    expect(listed.map(([id]) => id).sort()).toEqual(Object.keys(KNOWN_LIBRARY_DIFFERENCES).sort());
    for (const [id, libs] of listed) expect(libs, id).toEqual([(KNOWN_LIBRARY_DIFFERENCES[id] as { library: string }).library]);
    // the library that this client shows (stock Yjs) agrees with the spec on every vector: no entry names it
    expect(Object.values(KNOWN_LIBRARY_DIFFERENCES).filter((k) => k.library === "yjs")).toEqual([]);
    // and the four items and the base that yrs shows differently, which the vectors name
    for (const id of ["crdt-int-par-004", "crdt-int-par-005", "crdt-int-par-009", "crdt-int-par-011", "crdt-base-021"]) {
      expect(KNOWN_LIBRARY_DIFFERENCES[id]?.library, id).toBe("yrs");
    }
  });
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
