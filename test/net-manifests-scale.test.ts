// C16b, V19 N4: the manifest that the client reads (src/net/manifests.ts). `parentOf` was O(templates x entity templates), `selected` O(patterns x templates) with an
// `includes`, and `find` a scan of the templates of a key's length, each matched by splitting the key again: fine for the manifest of an app, seconds for a
// server-sized one (04 [MAN-80] allows thousands of templates). The templates are indexed by their segments (03 [ID-16]: at most 16), so that what the client
// does with a manifest costs by the length of a key and not by the number of templates, and a differential test holds the answers to those of the plain scan.

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { GetManifestResponseSchema, type GetManifestResponse } from "../src/gen/abcp/v2/sync_pb.js";
import { readManifest } from "../src/net/index.js";
import { sha256 } from "../src/rules/crypto.js";
import { jsonv1 } from "../src/rules/index.js";
import { matchKeyTemplate, parseKeyTemplate, type KeyTemplate } from "../src/rules/fold/index.js";

function answer(doc: unknown, version = 1): GetManifestResponse {
  const manifest = jsonv1.canonicalDocument(new TextEncoder().encode(JSON.stringify(doc)));
  return create(GetManifestResponseSchema, { version, manifest, encoding: "json/v1", sha256: sha256(manifest) });
}

type Attr = { merge: string; entity?: boolean; fenced_by?: string };

/** `entities` groups of templates that no two overlap (03 [ID-18]): the entity `g{i}/{id}`, its children, a nested entity and a literal branch that a variable one backs up. */
function manifestOf(groups: number, ops: Record<string, unknown> = {}): { keys: Record<string, Attr>; doc: unknown } {
  const keys: Record<string, Attr> = {};
  for (let i = 0; i < groups; i++) {
    keys[`g${i}/{id}`] = { merge: "lww", entity: true };
    keys[`g${i}/{id}/title`] = { merge: "lww" };
    keys[`g${i}/{id}/flag`] = { merge: "lww", fenced_by: `claims/g${i}/{id}` };
    keys[`g${i}/{id}/note/{nid}`] = { merge: "lww", entity: true };
    keys[`g${i}/{id}/note/{nid}/body`] = { merge: "cas" };
    keys[`g${i}/special/title2`] = { merge: "lww" }; // `special` is a literal where the entity has a variable: `g/special/x` falls back to `g/{id}/x`
    keys[`g${i}/count/{c}`] = { merge: "counter" };
  }
  keys[`claims/g0/{id}`] = { merge: "lww" };
  return { keys, doc: { app: "big", required_features: [], keys, op_types: ops } };
}

/** The plain scan: what the client did before the index, from the templates alone. */
class Naive {
  readonly list: { text: string; tpl: KeyTemplate; entity: boolean }[] = [];
  constructor(keys: Record<string, Attr>) {
    // the canonical form of the manifest (04 [MAN-4]) orders the keys, and the client reads the templates in that order
    for (const [text, a] of Object.entries(keys).sort(([x], [y]) => (x < y ? -1 : 1))) {
      const tpl = parseKeyTemplate(text) as KeyTemplate;
      this.list.push({ text, tpl, entity: a.entity === true });
    }
  }
  find(key: string): { text: string; entity: boolean } | undefined {
    for (const c of this.list) if (matchKeyTemplate(c.tpl, key) !== undefined) return c;
    return undefined;
  }
  parent(text: string): string {
    const tpl = this.list.find((c) => c.text === text)?.tpl as KeyTemplate;
    for (let n = tpl.segments.length - 1; n >= 1; n--) {
      for (const e of this.list) {
        if (!e.entity || e.tpl.segments.length !== n) continue;
        if (shape(e.tpl, tpl, n)) return e.text;
      }
    }
    return "";
  }
  selected(patterns: string[]): string[] {
    const out: string[] = [];
    for (const p of patterns) {
      const form = p.endsWith("/**") ? "many" : p.endsWith("/*") ? "one" : "plain";
      const base = parseKeyTemplate(form === "many" ? p.slice(0, -3) : form === "one" ? p.slice(0, -2) : p);
      if (base === undefined) continue;
      const n = base.segments.length;
      for (const c of this.list) {
        const len = c.tpl.segments.length;
        if (form === "plain" ? len !== n : form === "one" ? len !== n + 1 : len <= n) continue;
        if (shape(base, c.tpl, n) && !out.includes(c.text)) out.push(c.text);
      }
    }
    return out;
  }
}

function shape(a: KeyTemplate, b: KeyTemplate, n: number): boolean {
  for (let i = 0; i < n; i++) {
    const x = a.segments[i] as KeyTemplate["segments"][number];
    const y = b.segments[i] as KeyTemplate["segments"][number];
    if ("variable" in x !== "variable" in y) return false;
    if ("literal" in x && "literal" in y && x.literal !== y.literal) return false;
  }
  return true;
}

describe("V19 N4: what the client does with a manifest costs by the length of a key, not by the number of templates", () => {
  it("answers what the plain scan answers: the template of a key (a literal that a variable backs up included), the parent of each template, and what each `writes` pattern selects", () => {
    const patterns = ["g1/{id}", "g1/{id}/*", "g2/**", "g1/{x}/note/*", "g3/special/*", "claims/**", "g0/{id}/note/{nid}/**", "g1/{id}", "nothing/*", "**"];
    const ops = { A: { dispatch: "SEQUENCED", writes: patterns } };
    const { keys, doc } = manifestOf(6, ops);
    const m = readManifest(answer(doc), 1, "big");
    const naive = new Naive(keys);
    const probes: string[] = [];
    for (let g = 0; g < 7; g++) {
      for (const tail of ["", "/x1", "/x1/title", "/x1/flag", "/x1/note/n1", "/x1/note/n1/body", "/special/title2", "/special/flag", "/special/note/n2", "/special", "/count/k", "/count", "/x1/note", "/x1/note/n1/body/z", "/x1/other"]) probes.push(`g${g}${tail}`);
    }
    probes.push("claims/g0/a", "claims/g1/a", "claims", "", "g1//x", "g1/x/title/", "a".repeat(600), "G1/x");
    for (const key of probes) {
      const want = naive.find(key);
      const got = m.template(1, key);
      if (want === undefined) expect(got, key).toBeUndefined();
      else {
        expect(got, key).toMatchObject({ rule: expect.any(String), entity: want.entity, parent: naive.parent(want.text) });
        expect(m.templatePattern(key), key).toBe(want.text);
      }
    }
    expect(m.fencedBy("g1/x/flag")).toBe("claims/g1/x");
    expect(m.fencedBy("g0/special/flag")).toBe("claims/g0/special"); // `special` is bound to the variable that the template has there
    expect(m.fencedBy("g1/x/title")).toBeUndefined();
    expect(m.opType("A")?.writes).toEqual(naive.selected(patterns));
  });

  it("builds a manifest of 14,000 templates (about the most that the 1 MiB of 04 [MAN-4] holds) and answers 60,000 lookups in a time that no scan of the templates could meet", () => {
    const writes = ["g7/{id}/*", "g8/**", "g9/{id}"];
    const { keys, doc } = manifestOf(2000, { A: { dispatch: "SEQUENCED", writes } });
    const started = performance.now();
    const m = readManifest(answer(doc), 1, "big");
    for (let i = 0; i < 60_000; i++) {
      const g = (i * 7919) % 2000;
      expect(m.template(1, `g${g}/k${i}/note/n${i}/body`)?.parent).toBe(`g${g}/{id}/note/{nid}`);
    }
    const ms = performance.now() - started;
    expect(ms).toBeLessThan(5000); // the plain scan needs about ten seconds here; the step counts below are the cost bound, this only catches a scan (5 s: a loaded run took 2.1 s)
    expect(m.opType("A")?.writes).toEqual(new Naive(keys).selected(writes));
    expect(m.opType("A")?.writes).toHaveLength(2 + 7 + 1);
  });

  it("the work is bounded by the segments of a key and a template: the index visits no more nodes than a small multiple of them, however many templates there are", () => {
    for (const groups of [10, 100, 1000]) {
      const { doc } = manifestOf(groups, { A: { dispatch: "SEQUENCED", writes: ["g1/{id}/*", "g2/**"] } });
      const m = readManifest(answer(doc), 1, "big");
      const templates = groups * 7 + 1;
      expect(m.indexSteps, `build, ${groups} groups`).toBeLessThanOrEqual(templates * 40); // linear: each template is put, and its parent is read, by at most 16 steps each
      const before = m.indexSteps;
      for (let i = 0; i < 100; i++) m.template(1, `g${i % groups}/x/note/y/body`);
      expect((m.indexSteps - before) / 100, `lookups, ${groups} groups`).toBeLessThanOrEqual(2 * 16); // by the length of the key, however many templates
    }
  });
});
