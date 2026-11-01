// 14 [CLI-13] CRDT keys: a confirmed document and a view document, with Yjs.

import { describe, expect, it } from "vitest";
import { create } from "@bufbuild/protobuf";
import * as Y from "yjs";
import { crdtUpdate, type EngineEvent } from "../src/engine/index.js";
import { snapToCodePoint } from "../src/engine/crdt-view.js";
import { ChangeKind, KeyChangeSchema, StaleCause, Code } from "../src/gen/abpc/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { goOffline, goOnline, KS, ofType, rejectResult, rig, tick, types, type Rig } from "./helpers/engine.js";

const KEY = "docs/d1";

/** A remote edit: an update that another principal makes to the server's document of the key. */
function remoteEdit(r: Rig, fn: (t: Y.Text) => void): Uint8Array {
  const server = r.server.ydoc(KS, KEY);
  const doc = new Y.Doc();
  if (server !== undefined) Y.applyUpdate(doc, Y.encodeStateAsUpdate(server));
  const sv = Y.encodeStateVector(doc);
  fn(doc.getText("text"));
  return Y.encodeStateAsUpdate(doc, sv);
}

function writeRemote(r: Rig, update: Uint8Array, kind: ChangeKind = ChangeKind.CRDT_UPDATE): bigint {
  return r.server.write(KS, [create(KeyChangeSchema, { key: KEY, kind, update })], { sub: "bob" });
}

const textOf = (d: Y.Doc): string => d.getText("text").toString();

describe("14 [CLI-13] a confirmed document and a view document", () => {
  it("the user types into the view document while an update is pending: the view document is never replaced, confirmed updates merge into both documents, and the pending update retires", async () => {
    const r = await rig();
    const h = r.engine.crdt(KS, KEY);
    const view = h.doc;
    view.getText("text").insert(0, "hello");
    expect(h.hasUnsent).toBe(true);
    r.server.faults.paused = true; // the first action stays pending
    const first = await h.flush("DocEdit");
    await r.engine.settle();
    expect(first).toBeDefined();
    expect(h.hasUnsent).toBe(false);
    expect(r.engine.outbox()).toHaveLength(1);
    // the user types on, while the first update is pending
    view.getText("text").insert(5, " world");
    expect(h.hasUnsent).toBe(true);
    // another principal edits too
    const remote = remoteEdit(r, (t) => t.insert(0, ">> "));
    writeRemote(r, remote);
    r.server.resumeDelivery();
    await r.engine.settle();
    // confirmed: the server's two updates; view: those plus the user's unsent typing
    expect(h.doc).toBe(view); // not replaced
    expect(textOf(h.confirmedDoc)).toBe(">> hello");
    expect(textOf(view)).toBe(">> hello world");
    expect(types(r, first?.mutationId)).toEqual(["committed"]);
    const second = await h.flush("DocEdit");
    await r.engine.settle();
    expect(types(r, second?.mutationId)).toEqual(["committed"]);
    expect(textOf(r.server.ydoc(KS, KEY) as Y.Doc)).toBe(">> hello world");
    expect(textOf(h.confirmedDoc)).toBe(">> hello world");
    expect(r.engine.status().deadLetters).toBe(0);
  });

  it("an update that is a no-op for good is never integrated, also when the missing update arrives later (05 [STATE-76])", async () => {
    const r = await rig();
    // u1 inserts "abc"; u2 inserts "X" after "b" (it depends on u1); the server's fold sees u2 first
    const author = new Y.Doc();
    const log: Uint8Array[] = [];
    author.on("update", (u: Uint8Array) => log.push(u));
    author.getText("text").insert(0, "abc");
    author.getText("text").insert(2, "X");
    const [u1, u2] = log as [Uint8Array, Uint8Array];
    writeRemote(r, u2); // missing dependency: no-op for good
    const h = r.engine.crdt(KS, KEY);
    await r.engine.settle();
    expect(textOf(h.confirmedDoc)).toBe("");
    writeRemote(r, u1); // the missing update arrives
    await r.engine.settle();
    expect(textOf(h.confirmedDoc)).toBe("abc"); // u2 stays out: it was never integrated
    expect(textOf(h.doc)).toBe("abc");
    // what stock Yjs does instead: it would keep the structs pending and integrate them
    const stock = new Y.Doc();
    Y.applyUpdate(stock, u2);
    Y.applyUpdate(stock, u1);
    expect(textOf(stock)).toBe("abXc");
  });

  it("a rejected pending update rebuilds the view document: a fresh Yjs client id, the confirmed document and the remaining pending updates, and the user's unsent edits re-applied once, as a diff, after `rebuilt`", async () => {
    const r = await rig();
    writeRemote(r, remoteEdit(r, (t) => t.insert(0, "base")));
    await r.engine.settle();
    const h = r.engine.crdt(KS, KEY);
    const oldView = h.doc;
    const oldClient = oldView.clientID;
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, mk(Code.SCHEMA_INVALID)) });
    oldView.getText("text").insert(4, "A"); // goes into the action that is rejected
    const a = await h.flush("DocEdit");
    oldView.getText("text").insert(5, "B"); // typed after, in no action yet
    expect(textOf(oldView)).toBe("baseAB");
    const order: string[] = [];
    r.engine.on((e) => order.push(e.type));
    await r.engine.settle();
    expect(types(r, a?.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(types(r).slice(-3)).toEqual(["rejected", "dead-lettered", "rebuilt"]); // once, after the rejection that caused it
    expect(ofType(r, "rebuilt")).toHaveLength(1);
    expect(ofType(r, "rebuilt")[0]).toMatchObject({ keyspaceId: KS, key: KEY, cause: a?.mutationId });
    const view = h.doc;
    expect(view).not.toBe(oldView);
    expect(view.clientID).not.toBe(oldClient); // never one used before in the keyspace
    expect(textOf(view)).toBe("baseB"); // the rejected text is gone, the unsent edit is kept, once
    expect(h.hasUnsent).toBe(true);
    // the edits that were in no action yet are sent only after `rebuilt`
    const rebuiltAt = r.events.findIndex((e) => e.type === "rebuilt");
    const sentBefore = r.server.calls.filter((c) => c.method === "mutate").length;
    const b = await h.flush("DocEdit");
    await r.engine.settle();
    expect(r.server.calls.filter((c) => c.method === "mutate").length).toBe(sentBefore + 1);
    expect(r.events.findIndex((e) => e.type === "committed" && e.mutationId === b?.mutationId)).toBeGreaterThan(rebuiltAt);
    expect(textOf(r.server.ydoc(KS, KEY) as Y.Doc)).toBe("baseB");
    expect(textOf(h.confirmedDoc)).toBe("baseB");
  });

  it("only the user's own edits become actions: an update that the engine integrates leaves nothing unsent", async () => {
    const r = await rig();
    const h = r.engine.crdt(KS, KEY);
    writeRemote(r, remoteEdit(r, (t) => t.insert(0, "remote text")));
    await r.engine.settle();
    expect(textOf(h.doc)).toBe("remote text");
    expect(h.hasUnsent).toBe(false);
    expect(await h.flush("DocEdit")).toBeUndefined();
    expect(r.engine.outbox()).toHaveLength(0);
  });

  it("every view document uses a fresh Yjs client id, drawn again while a document of the keyspace holds it", async () => {
    const r = await rig();
    const a = r.engine.crdt(KS, KEY).doc.clientID;
    expect(a).toBeGreaterThan(0);
    expect(Number.isSafeInteger(a)).toBe(true);
    expect(a).toBeLessThan(2 ** 53);
    // a document of the keyspace that holds the next id the random source gives: the client draws again
    const probe = new Y.Doc();
    probe.clientID = 12345;
    probe.getText("text").insert(0, "x");
    writeRemote(r, Y.encodeStateAsUpdate(probe));
    await r.engine.settle();
    expect(r.engine.crdt(KS, KEY).confirmedDoc.store.clients.has(12345)).toBe(true);
    for (let i = 0; i < 30; i++) expect(new Y.Doc().clientID).not.toBe(0);
  });

  it("an OVERRIDE of the key while the client has pending updates of it: `overwritten` for each pending action (14 [CLI-40])", async () => {
    const r = await rig();
    const h = r.engine.crdt(KS, KEY);
    await goOffline(r);
    h.doc.getText("text").insert(0, "mine");
    const a = await h.flush("DocEdit");
    writeRemote(r, remoteEdit(r, (t) => t.insert(0, "admin")), ChangeKind.OVERRIDE);
    await goOnline(r);
    const ow = ofType(r, "overwritten").filter((e) => e.mutationId === a?.mutationId);
    expect(ow).toHaveLength(1);
    expect(ow[0]).toMatchObject({ keys: [KEY], by: "bob" });
    expect(types(r, a?.mutationId)).toContain("committed");
  });

  it("a committed update that folded to no effect makes the action `corrected` with cause NO_EFFECT (14 [CLI-40])", async () => {
    const r = await rig();
    const h = r.engine.crdt(KS, KEY);
    h.doc.getText("text").insert(0, "once");
    const first = await h.flush("DocEdit");
    await r.engine.settle();
    expect(types(r, first?.mutationId)).toEqual(["committed"]);
    const same = r.server.records(KS).find((x) => x.mutationId === first?.mutationId)?.changes[0]?.update as Uint8Array;
    const again = await r.engine.submit({ keyspaceId: KS, opType: "DocEdit", changes: [crdtUpdate(KEY, same)] });
    await r.engine.settle();
    expect(types(r, again.mutationId)).toEqual(["corrected"]);
    expect(ofType(r, "corrected")[0]).toMatchObject({ cause: "NO_EFFECT", keys: [KEY] });
  });

  it("an update that the server rejected as not well formed is dead-lettered and the view is rebuilt without it", async () => {
    const r = await rig();
    const h = r.engine.crdt(KS, KEY);
    const bad = await r.engine.submit({ keyspaceId: KS, opType: "DocEdit", changes: [crdtUpdate(KEY, new Uint8Array([9, 9, 9]))] });
    await r.engine.settle();
    expect(types(r, bad.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(types(r).slice(-3)).toEqual(["rejected", "dead-lettered", "rebuilt"]);
    expect(r.engine.deadLetterEntries()[0]?.reject.code).toBe(Code.SCHEMA_INVALID);
    expect(h.doc.getText("text").toString()).toBe("");
    void StaleCause;
  });
});

describe("14 [CLI-13] no insert, delete or format bound falls between the two code units of a surrogate pair", () => {
  it("moves every bound to the boundary before the pair (draft 14: the start of a delete, its end and the position of an insert alike)", () => {
    const s = "a\u{1F600}b"; // "a", the pair at 1 and 2, "b"
    expect(snapToCodePoint(s, 2)).toBe(1); // between the two units: before the pair
    expect(snapToCodePoint(s, 1)).toBe(1);
    expect(snapToCodePoint(s, 3)).toBe(3);
    expect(snapToCodePoint(s, 0)).toBe(0);
    expect(snapToCodePoint(s, s.length)).toBe(s.length);
    expect(snapToCodePoint("\ude00x", 1)).toBe(1); // a lone low unit is no pair
    expect(snapToCodePoint("\ud83dx", 1)).toBe(1); // a lone high unit is no pair
  });

  it("a re-applied edit that starts inside a pair replaces the whole pair and keeps it whole: the inserted text begins with the unit that the start gave up", async () => {
    const r = await rig();
    writeRemote(r, remoteEdit(r, (t) => t.insert(0, "a\u{1F600}b")));
    await r.engine.settle();
    const h = r.engine.crdt(KS, KEY);
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, mk(Code.SCHEMA_INVALID)) });
    h.doc.getText("text").insert(4, "Z"); // after the "b"; rejected, so the view is rebuilt and the edits that are in no action yet are re-applied
    await h.flush("DocEdit");
    // the user's unsent edit replaces the character U+1F600 by U+1F601 (Yjs never splits the pair itself): the two share their high unit, so the
    // difference of the two texts, which counts UTF-16 units, starts at 2, inside the pair
    h.doc.getText("text").delete(1, 2);
    h.doc.getText("text").insert(1, "\u{1F601}");
    await r.engine.settle();
    const t = textOf(h.doc);
    for (let i = 0; i < t.length; i++) {
      const c = t.charCodeAt(i);
      if (c >= 0xd800 && c <= 0xdbff) expect(t.charCodeAt(i + 1) >= 0xdc00 && t.charCodeAt(i + 1) <= 0xdfff).toBe(true);
      if (c >= 0xdc00 && c <= 0xdfff) expect(t.charCodeAt(i - 1) >= 0xd800 && t.charCodeAt(i - 1) <= 0xdbff).toBe(true);
    }
    expect(t).toBe("a\u{1F601}b"); // the edit was re-applied, whole
  });

  it("the re-applied diff of a rebuild never splits a pair", async () => {
    const r = await rig();
    writeRemote(r, remoteEdit(r, (t) => t.insert(0, "😀")));
    await r.engine.settle();
    const h = r.engine.crdt(KS, KEY);
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, mk(Code.SCHEMA_INVALID)) });
    h.doc.getText("text").insert(2, "A"); // after the pair; it is rejected
    await h.flush("DocEdit");
    h.doc.getText("text").insert(3, "B"); // unsent, after the A
    await r.engine.settle();
    const t = textOf(h.doc);
    expect(t).toBe("😀B");
    for (let i = 0; i < t.length; i++) {
      const hi = t.charCodeAt(i) >= 0xd800 && t.charCodeAt(i) <= 0xdbff;
      if (hi) expect(t.charCodeAt(i + 1) >= 0xdc00 && t.charCodeAt(i + 1) <= 0xdfff).toBe(true);
    }
  });
});

void [tick, ({} as EngineEvent)];
