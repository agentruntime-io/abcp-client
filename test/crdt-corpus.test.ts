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
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { fromHex, toHex } from "../src/rules/bytes.js";
import { Doc, validateBase, type RootType, type Roots } from "../src/rules/crdt/index.js";

const dir = fileURLToPath(new URL("../../server/internal/crdt/testdata/", import.meta.url));
const files = existsSync(dir) ? readdirSync(dir).filter((f) => /^model-corpus.*\.json$/.test(f)) : [];

interface Corpus {
  roots: Record<string, string>;
  cases: {
    steps: { update: string; expect: string; reason: string }[];
    canonical: string;
    present: boolean;
    bases: { kind: string; base: string; valid: boolean; reason: string }[];
  }[];
}

describe("the model corpora of server/internal/crdt/testdata", () => {
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
