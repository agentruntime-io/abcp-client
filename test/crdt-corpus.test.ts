// The client's CRDT rules against the JS reference model of the spec (spec/vectors/runner-yjs/lib/crdt.mjs) over the random update
// sequences that the server's tests keep (server/internal/crdt/testdata/model-corpus*.json, made by testdata/gen/gencorpus.mjs): the
// verdict and the reason of every update (05 [STATE-71], [STATE-76]), the canonical form after each sequence (05 [STATE-41]), and the
// verdict on each base that Yjs JS builds from the integrated updates (05 [STATE-74]). The sequences cover the combinations that the
// vectors fix one by one: since draft 14 (Y9, Z3, Q33) items with both an origin and a right origin under different parents, hidden
// parents and GCs (the `neighbours` profile). The twin of the server's TestModelCorpus.
//
// The model checks the GC rule of 05 [STATE-74] and not its first-definition rule, so a Yjs `mergeUpdates` base that keeps a struct that is
// not the first definition of its ids (a GC and an item that define the same ids, or a later struct that covers earlier ones) is valid for
// the model and `content` for the rules here; any other difference fails.

import { existsSync, readdirSync, readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { fromHex, toHex } from "../src/rules/bytes.js";
import { Doc, validateBase, type RootType, type Roots } from "../src/rules/crdt/index.js";
import { CRDT_TESTDATA_DIR } from "./helpers/server-dir.js";
import { describeNeeds } from "./helpers/describe-needs.js";

// `$ABPC_SERVER_DIR/server/internal/crdt/testdata/`; without a server checkout the suites below are skipped
const HAVE_CORPORA = CRDT_TESTDATA_DIR !== undefined;
const dir = CRDT_TESTDATA_DIR === undefined ? "" : CRDT_TESTDATA_DIR + "/";
const files = HAVE_CORPORA && existsSync(dir) ? readdirSync(dir).filter((f) => /^model-corpus.*\.json$/.test(f)) : [];

interface Corpus {
  roots: Record<string, string>;
  cases: {
    steps: { update: string; expect: string; reason: string }[];
    canonical: string;
    present: boolean;
    bases: { kind: string; base: string; valid: boolean; reason: string }[];
  }[];
}

describeNeeds(HAVE_CORPORA, "the model corpora of server/internal/crdt/testdata", () => {
  it("are there", () => {
    expect(files.length).toBeGreaterThanOrEqual(2);
  });
  for (const file of files) {
    it(`${file}: every verdict, canonical form and base verdict is the model's`, () => {
      const corpus = JSON.parse(readFileSync(dir + file, "utf8")) as Corpus;
      const roots: Roots = new Map(Object.entries(corpus.roots) as [string, RootType][]);
      let bases = 0;
      let stricter = 0;
      corpus.cases.forEach((c, ci) => {
        const doc = new Doc();
        c.steps.forEach((st, si) => {
          const res = doc.apply(fromHex(st.update), roots);
          expect({ verdict: res.verdict, reason: res.reason ?? "" }, `case ${ci} step ${si} (${st.update})`).toEqual({ verdict: st.expect, reason: st.reason });
        });
        expect(doc.present, `case ${ci}: present`).toBe(c.present);
        expect(toHex(doc.canonical()), `case ${ci}: canonical`).toBe(c.canonical);
        for (const b of c.bases) {
          const err = validateBase(fromHex(b.base), roots, doc);
          const got = { valid: err === null, reason: err?.code ?? "" };
          if (got.reason === "content" && b.valid && b.kind !== "doc") {
            stricter++;
            continue;
          }
          expect(got, `case ${ci}: ${b.kind} base ${b.base}`).toEqual({ valid: b.valid, reason: b.reason });
          bases++;
        }
      });
      expect(bases).toBeGreaterThan(100);
      expect(stricter).toBeLessThan(5);
    });
  }
});

// ---- the document length of 05 [STATE-74] (round 17, D-AP (a)), against a real Yjs store ----
//
// yjs-doclen.json (made by testdata/gen: the twin of the server's test over it) holds histories of updates and, after each, the length of the
// document that a real Yjs 13.6.33 store gives, its structs cut and merged as [STATE-74] says and each content counted in its canonical form.
// Yjs integrates what the spec does not (a no-op for good), so a history is compared up to its first such update, as the Go test does.

interface DocLenCorpus {
  histories: { roots: Record<string, string>; updates: string[]; lengths: number[] }[];
}

describeNeeds(HAVE_CORPORA, "05 [STATE-74] document length: histories measured on a real Yjs store (server/internal/crdt/testdata/yjs-doclen.json)", () => {
  const path = dir + "yjs-doclen.json";
  const rootsOf = (h: { roots: Record<string, string> }): Roots => new Map(Object.entries(h.roots) as [string, RootType][]);
  const load = (): DocLenCorpus => JSON.parse(readFileSync(path, "utf8")) as DocLenCorpus;

  it("is there", () => {
    expect(existsSync(path)).toBe(true);
    const corpus = load();
    expect(corpus.histories.length).toBeGreaterThanOrEqual(100);
    for (const h of corpus.histories) expect(h.lengths.length).toBe(h.updates.length);
  });

  it("the document length after every update of every history is the one that Yjs's store gives", () => {
    let compared = 0;
    let stopped = 0;
    let largest = 0;
    corpus: for (const [hi, h] of load().histories.entries()) {
      const roots = rootsOf(h);
      const doc = new Doc();
      expect(doc.documentLength(), `history ${hi}: the empty key`).toBe(2);
      for (let i = 0; i < h.updates.length; i++) {
        const res = doc.apply(fromHex(h.updates[i] as string), roots);
        if (res.verdict === "SCHEMA_INVALID") throw new Error(`history ${hi} update ${i}: ${res.reason}`);
        if (res.verdict === "NOOP_FOR_GOOD") {
          stopped++;
          continue corpus; // stock Yjs integrates what the spec does not: only the prefix is compared
        }
        expect(doc.documentLength(), `history ${hi} update ${i} (${h.updates[i]})`).toBe(h.lengths[i]);
        compared++;
        largest = Math.max(largest, doc.documentLength());
      }
    }
    expect(compared).toBeGreaterThan(7000);
    expect(stopped).toBeGreaterThan(0);
    expect(largest).toBeGreaterThan(500);
  });

  it("a clone that takes the update and the Doc that was cloned keep their own lengths (02 [ARC-28]): the Doc, the clone, and the one that goes on, by turns", () => {
    let compared = 0;
    corpus: for (const [hi, h] of load().histories.entries()) {
      const roots = rootsOf(h);
      let doc = new Doc();
      for (let i = 0; i < h.updates.length; i++) {
        const bytes = fromHex(h.updates[i] as string);
        const before = doc.documentLength();
        // the fold clones the Doc of a key before it applies an update to it, and a reader keeps the Doc that it holds
        const fork = doc.clone();
        const res = fork.apply(bytes, roots);
        if (res.verdict === "NOOP_FOR_GOOD") continue corpus;
        expect(res.verdict, `history ${hi} update ${i}`).not.toBe("SCHEMA_INVALID");
        expect(doc.documentLength(), `history ${hi} update ${i}: the length of the Doc that was cloned`).toBe(before);
        expect(fork.documentLength(), `history ${hi} update ${i} (${h.updates[i]}): the clone that took the update`).toBe(h.lengths[i]);
        // the Doc that was cloned takes the same update, in place, and is measured as the clone is, and the clone does not move
        const again = doc.clone();
        again.apply(bytes, roots);
        expect(again.documentLength(), `history ${hi} update ${i}: a second clone`).toBe(h.lengths[i]);
        expect(fork.documentLength(), `history ${hi} update ${i}: the first clone, after the second took the update`).toBe(h.lengths[i]);
        expect(doc.documentLength(), `history ${hi} update ${i}: the Doc, after its clones took the update`).toBe(before);
        compared++;
        // the history goes on from one of them, by turns: the clones share what the Doc that they came from made
        doc = i % 2 === 0 ? fork : again;
      }
    }
    expect(compared).toBeGreaterThan(7000);
  });
});
