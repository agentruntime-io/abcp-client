// C16b, 14 [CLI-13] and Q67: "A client MUST NOT author an insert, a delete or a format whose bound falls between the two code units of a surrogate pair: it moves
// the bound to the boundary before the pair". The view document's guard covered the root `Y.Text` only; a `Y.Text` (or `Y.XmlText`) nested in a map or an array
// of the document, made by the app or brought by a confirmed update, is edited as the user edits a root text: the same bounds move the same way.

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import * as Y from "yjs";
import { StaticManifests } from "../src/engine/index.js";
import { ChangeKind, KeyChangeSchema } from "../src/gen/abcp/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { Code } from "../src/gen/abcp/v2/types_pb.js";
import { KS, MANIFEST, rejectResult, rig, types, type Rig } from "./helpers/engine.js";

const KEY = "docs/d1";
const PAIR = "a\u{1F600}b"; // "a", the pair at 1 and 2, "b"

/** The manifest of the suites, with a document that has a root text, a root map and a root array. */
const NESTED = new StaticManifests({ ...MANIFEST, templates: { ...MANIFEST.templates, "docs/{id}": { rule: "crdt", roots: { text: "text", m: "map", a: "array" } } } });

type Holder = Y.Map<unknown> | Y.Array<unknown>;
type Edit = (t: Y.Text) => void;

async function nestedRig(): Promise<Rig> {
  return rig({ manifests: NESTED });
}

const server = (r: Rig): Y.Doc => r.server.ydoc(KS, KEY) as Y.Doc;
const nestedIn = (d: Y.Doc, where: "map" | "array" | "deep"): Y.Text | Y.XmlText => {
  if (where === "map") return d.getMap("m").get("body") as Y.Text;
  if (where === "array") return d.getArray("a").get(0) as Y.Text;
  return (d.getMap("m").get("inner") as Y.Map<unknown>).get("body") as Y.Text;
};

/** The pair that the nested text of each document holds, and that the update that the edit made never split. */
function whole(s: string): boolean {
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdbff && !(s.charCodeAt(i + 1) >= 0xdc00 && s.charCodeAt(i + 1) <= 0xdfff)) return false;
    if (c >= 0xdc00 && c <= 0xdfff && !(i > 0 && s.charCodeAt(i - 1) >= 0xd800 && s.charCodeAt(i - 1) <= 0xdbff)) return false;
  }
  return true;
}

/**
 * Makes the document's nested text "a😀b" where the app puts it (`make`), commits it, then the user edits it with `edit`, and commits that.
 * The view, the confirmed document and the server must agree afterwards, and the second action must be committed, not corrected (the server did not
 * fold the update to no effect).
 */
async function userEdit(where: "map" | "array" | "deep", make: (doc: Y.Doc) => Y.Text | Y.XmlText, edit: Edit): Promise<{ view: string; confirmed: string; server: string; events: string[] }> {
  const r = await nestedRig();
  const h = r.engine.crdt(KS, KEY);
  make(h.doc);
  await h.flush("DocEdit");
  await r.engine.settle();
  expect(nestedIn(h.confirmedDoc, where).toString()).toBe(PAIR);
  edit(nestedIn(h.doc, where) as Y.Text);
  const res = await h.flush("DocEdit");
  await r.engine.settle();
  return {
    view: nestedIn(h.doc, where).toString(),
    confirmed: nestedIn(h.confirmedDoc, where).toString(),
    server: nestedIn(server(r), where).toString(),
    events: types(r, res?.mutationId),
  };
}

const inMap = (doc: Y.Doc): Y.Text => {
  const t = new Y.Text(PAIR);
  doc.getMap("m").set("body", t);
  return t;
};
const inArray = (doc: Y.Doc): Y.Text => {
  const t = new Y.Text(PAIR);
  doc.getArray("a").insert(0, [t]);
  return t;
};
const inDeepMap = (doc: Y.Doc): Y.Text => {
  const inner = new Y.Map<unknown>();
  doc.getMap("m").set("inner", inner);
  const t = new Y.Text(PAIR);
  inner.set("body", t);
  return t;
};
const inXml = (doc: Y.Doc): Y.XmlText => {
  const t = new Y.XmlText(PAIR);
  doc.getMap("m").set("body", t);
  return t;
};

const WHERE = [
  ["a map", "map", inMap],
  ["an array", "array", inArray],
  ["a map in a map", "deep", inDeepMap],
] as const;

describe.each(WHERE)("14 [CLI-13], Q67: a Y.Text nested in %s keeps every bound out of a surrogate pair", (_name, where, make) => {
  it("an insert at an index inside the pair is authored before the pair, and the view, the confirmed document and the server agree", async () => {
    const o = await userEdit(where, make, (t) => t.insert(2, "x"));
    expect([o.view, o.confirmed, o.server]).toEqual(["ax\u{1F600}b", "ax\u{1F600}b", "ax\u{1F600}b"]);
    expect(o.events).toEqual(["committed"]); // not `corrected`: the server did not fold the update to no effect
  });

  it("a delete whose start is inside a pair deletes the pair (SPEC-QUESTION Q67), and one whose end is inside it leaves the pair whole", async () => {
    const start = await userEdit(where, make, (t) => t.delete(2, 1));
    expect([start.view, start.confirmed, start.server, start.events]).toEqual(["ab", "ab", "ab", ["committed"]]);
    const end = await userEdit(where, make, (t) => t.delete(0, 2));
    expect([end.view, end.confirmed, end.server, end.events]).toEqual(["\u{1F600}b", "\u{1F600}b", "\u{1F600}b", ["committed"]]);
  });

  it("a format, an embed and a delta keep their bounds out of a pair too", async () => {
    const fmt = await userEdit(where, make, (t) => t.format(2, 1, { bold: true }));
    expect([fmt.view, fmt.confirmed, fmt.server, fmt.events]).toEqual([PAIR, PAIR, PAIR, ["committed"]]);
    const embed = await userEdit(where, make, (t) => t.insertEmbed(2, { image: "x" }));
    expect([embed.confirmed, embed.server, embed.events]).toEqual([PAIR, PAIR, ["committed"]]);
    const delta = await userEdit(where, make, (t) => t.applyDelta([{ retain: 2 }, { insert: "Q" }, { delete: 1 }]));
    expect([delta.view, delta.confirmed, delta.server, delta.events]).toEqual(["aQb", "aQb", "aQb", ["committed"]]);
  });

  it("an edit that has no bound inside a pair is made as it is asked", async () => {
    const o = await userEdit(where, make, (t) => {
      t.insert(1, "x");
      t.insert(5, "y");
      t.delete(2, 2);
    });
    expect([o.view, o.confirmed, o.server, o.events]).toEqual(["axby", "axby", "axby", ["committed"]]);
  });
});

describe("14 [CLI-13], Q67: the nested text that the guard covers", () => {
  it("a text that a confirmed update brought (another principal made it) is guarded when the user edits it", async () => {
    const r = await nestedRig();
    const h = r.engine.crdt(KS, KEY);
    const remote = new Y.Doc();
    remote.getMap("m").set("body", new Y.Text(PAIR));
    r.server.write(KS, [create(KeyChangeSchema, { key: KEY, kind: ChangeKind.CRDT_UPDATE, update: Y.encodeStateAsUpdate(remote), encoding: "yjs/v1" })], { sub: "bob" });
    await r.engine.settle();
    const t = h.doc.getMap("m").get("body") as Y.Text;
    expect(t.toString()).toBe(PAIR);
    t.insert(2, "x");
    const res = await h.flush("DocEdit");
    await r.engine.settle();
    expect(types(r, res?.mutationId)).toEqual(["committed"]);
    expect([t.toString(), (h.confirmedDoc.getMap("m").get("body") as Y.Text).toString(), (server(r).getMap("m").get("body") as Y.Text).toString()]).toEqual(["ax\u{1F600}b", "ax\u{1F600}b", "ax\u{1F600}b"]);
  });

  it("edits that the app makes to a text before it is in the document are queued by Yjs and made when it is: they are guarded then", async () => {
    const r = await nestedRig();
    const h = r.engine.crdt(KS, KEY);
    const t = new Y.Text();
    t.insert(0, PAIR);
    t.insert(2, "x"); // inside the pair, once the text is there
    h.doc.getMap("m").set("body", t);
    expect(t.toString()).toBe("ax\u{1F600}b");
    const res = await h.flush("DocEdit");
    await r.engine.settle();
    expect(types(r, res?.mutationId)).toEqual(["committed"]);
    expect((server(r).getMap("m").get("body") as Y.Text).toString()).toBe("ax\u{1F600}b");
  });

  it("a Y.XmlText nested in a map is a text too", async () => {
    const o = await userEdit("map", inXml, (t) => t.insert(2, "x"));
    expect([o.view, o.confirmed, o.server, o.events]).toEqual(["ax\u{1F600}b", "ax\u{1F600}b", "ax\u{1F600}b", ["committed"]]);
    const d = await userEdit("map", inXml, (t) => t.delete(2, 1));
    expect([d.view, d.confirmed, d.server]).toEqual(["ab", "ab", "ab"]);
  });

  it("the rebuilt view document (a rejected action) is guarded as well: its nested texts are new instances", async () => {
    const r = await nestedRig();
    const h = r.engine.crdt(KS, KEY);
    inMap(h.doc);
    await h.flush("DocEdit");
    await r.engine.settle();
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, mk(Code.SCHEMA_INVALID)) });
    h.doc.getText("text").insert(0, "z"); // rejected: the view document is rebuilt from the confirmed one
    await h.flush("DocEdit");
    await r.engine.settle();
    const t = h.doc.getMap("m").get("body") as Y.Text;
    expect(t.toString()).toBe(PAIR);
    t.insert(2, "x");
    expect(t.toString()).toBe("ax\u{1F600}b");
    const res = await h.flush("DocEdit");
    await r.engine.settle();
    expect(types(r, res?.mutationId)).toEqual(["committed"]);
    expect(whole((server(r).getMap("m").get("body") as Y.Text).toString())).toBe(true);
  });

  it("a text of a document that is not the engine's view document is edited as Yjs edits it (the guard is the view document's, not the library's)", () => {
    const d = new Y.Doc();
    const t = new Y.Text(PAIR);
    d.getMap("m").set("body", t);
    t.insert(2, "x");
    expect(t.toString()).toBe("a�x�b"); // split between the two units: Yjs does as it is asked, and writes U+FFFD for each half of the pair
    expect(t.toString()).not.toBe("ax\u{1F600}b");
  });
});

void [Y.Map, Y.Array];
export type { Holder };
