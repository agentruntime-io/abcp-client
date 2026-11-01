// C16b, V19 N5 on the IndexedDB storage (src/platform/idb-storage.ts): (1) the stores list their rows in key order, which is the order of the mutation ids, while the
// text puts the actions of every answer, and so of every list that the app reads, in the authoring order of the actions (14 [CLI-38], [CLI-77], [CLI-28]); the
// engine lists the dead letters and the export-hook entries in that order whatever the storage's. (2) A row that is not what the storage wrote (damage, a build
// that wrote another shape) threw at every open: it is left out and reported, and the rest of the partition opens (14 [CLI-29]: "one bad entry MUST NOT stop
// the flush of others").

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { set, type DeadLetterEntry, type ExportEntry, type OutboxEntry, type ReplicaHead } from "../src/engine/index.js";
import { Code, KeyEntrySchema, RecordSchema } from "../src/gen/abpc/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { IndexedDbStorage } from "../src/platform/idb-storage.js";
import { FakeIDBFactory, FakeKeyRange } from "./helpers/fake-idb.js";
import { KS, rejectResult, rig, tick, types } from "./helpers/engine.js";

const author = { issuer: "i", sub: "s", deviceId: "d" };
const entry = (id: string, ordinal: number): OutboxEntry => ({
  mutationId: id, keyspaceId: "ks", lane: "SEQUENCED", opType: "Set", content: new Uint8Array([9, ordinal]), author, ordinal, authoredServerMs: 1, dependsOn: [],
  clientId: "", clientSeq: 0, afterClientSeq: 0, everSent: false, sentInEpoch: 0, attempts: 0, registered: false, permissionRetried: false, actorMismatchRetried: false, spendResent: false,
});
const dead = (id: string, ordinal: number): DeadLetterEntry => ({
  mutationId: id, keyspaceId: "ks", opType: "Set", content: new Uint8Array([1]), author, reject: undefined as never, reason: "X", attempts: 1, firstFailureMs: 1, lastFailureMs: 1, failedDependents: [], ordinal,
});
const exported = (id: string, ordinal: number): ExportEntry => ({ mutationId: id, keyspaceId: "ks", opType: "Set", content: new Uint8Array([1]), author, reason: "X", atMs: 1, ordinal });
const head = (seq: bigint): ReplicaHead => ({ logEpoch: 1, seq, watermark: seq, lastRecordSeq: seq, manifestVersion: 1, kind: "team", lifecycle: 1, successors: [], interest: [], lastSyncMs: 5 });
const rec = (seq: bigint) => create(RecordSchema, { seq, logEpoch: 1, manifestVersion: 1, mutationId: `m${seq}` });
const kentry = (key: string) => create(KeyEntrySchema, { key, encoding: "json/v1", value: new Uint8Array([1]), version: 1n });
const stores = (f: FakeIDBFactory, db: string): Map<string, Map<string, unknown>> => f.dbs.get(db)?.stores as Map<string, Map<string, unknown>>;

describe("V19 N5: the engine lists dead letters and export-hook entries in the authoring order of their actions, whatever order the storage reads them in", () => {
  // ids whose key order is the reverse of the authoring order, so that a store that lists by key lists them backwards
  const ids = ["m-d", "m-c", "m-b", "m-a"];

  it("actions that end in another order than they were authored are listed in the order they were authored (a hold ends a never-sent action at its bound, a reject ends another at once)", async () => {
    const r = await rig({ keyspaces: [KS, "ks2"], serverOptions: { limits: { maxOfflineSeconds: 120 } } });
    r.server.inject("mutate", { kind: "result", result: (m) => (m.keyspaceId === "ks2" ? rejectResult(m, mk(Code.UPGRADE_REQUIRED)) : undefined) });
    await r.engine.submit({ keyspaceId: "ks2", opType: "Set", changes: [set("settings/w", 1)] }); // sent: the hold starts, and it is outcome unknown at the bound
    await r.engine.settle();
    const x = await r.engine.submit({ keyspaceId: "ks2", opType: "Set", changes: [set("settings/x", 1)] }); // never sent: dead-lettered at the bound
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(Code.SCHEMA_INVALID)) });
    const y = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/y", 1)] }); // rejected at once, after x was authored, before x ends
    await r.engine.settle();
    expect(types(r, y.mutationId)).toEqual(["rejected", "dead-lettered"]);
    await tick(r, 130_000);
    expect(types(r, x.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(r.events.filter((e) => e.type === "dead-lettered").map((e) => (e as { mutationId: string }).mutationId)).toEqual([y.mutationId, x.mutationId]); // they ended in this order
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual([x.mutationId, y.mutationId]); // and are listed in the order they were authored
  });

  it("the engine over such a storage: the list, the dead-lettered events of a sign-out and the exported ones come in authoring order", async () => {
    const f = new FakeIDBFactory();
    const s = await IndexedDbStorage.open(f, "p");
    await s.commit([
      ...ids.map((id, i) => ({ op: "putDeadLetter" as const, entry: { ...dead(id, i + 1), author: undefined as never } })),
      ...ids.map((id, i) => ({ op: "putExport" as const, entry: { ...exported(`x-${id}`, i + 1), author: undefined as never } })),
    ]);
    const r = await rig({ cfg: { storage: s }, start: false });
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual(ids); // authoring order: ordinal 1 is "m-d"
    expect(r.engine.exportEntries().map((x) => x.mutationId)).toEqual(ids.map((id) => `x-${id}`));
    const out = await r.engine.signOut();
    expect(out).toMatchObject({ done: true });
    expect(r.events.filter((e) => e.type === "dead-lettered").map((e) => (e as { mutationId: string }).mutationId)).toEqual(ids);
    expect(r.events.filter((e) => e.type === "exported").map((e) => (e as { mutationId: string }).mutationId)).toEqual(ids.map((id) => `x-${id}`));
  });

  it("a row that an earlier build wrote, with no ordinal, keeps its place (storage order) before the ones that have one", async () => {
    const f = new FakeIDBFactory();
    const s = await IndexedDbStorage.open(f, "p");
    const old = { ...dead("old", 0), author: undefined as never } as Partial<DeadLetterEntry>;
    delete old.ordinal;
    await s.commit([{ op: "putDeadLetter", entry: old as DeadLetterEntry }, { op: "putDeadLetter", entry: { ...dead("m-z", 5), author: undefined as never } }, { op: "putDeadLetter", entry: { ...dead("a-first", 2), author: undefined as never } }]);
    const r = await rig({ cfg: { storage: s }, start: false });
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual(["old", "a-first", "m-z"]);
  });
});

for (const mode of ["with IDBKeyRange", "without a key range"] as const) {
  const range = mode === "with IDBKeyRange" ? FakeKeyRange : undefined;
  const open = (f: FakeIDBFactory, name = "p"): Promise<IndexedDbStorage> => IndexedDbStorage.open(f, name, {}, undefined, range);

  describe(`V19 N5: a malformed row is left out and reported, and the rest of the partition opens (${mode})`, () => {
    it("a row of the outbox, the dead letters, the journal, the exports, the rejections, the drafts and the meta that is not what was written: load() and loadLight() return the others and list it", async () => {
      const f = new FakeIDBFactory();
      const a = await open(f);
      await a.commit([{ op: "putEntry", entry: entry("good", 1) }, { op: "putDeadLetter", entry: dead("dgood", 1) }, { op: "setMeta", key: "ok", value: 7 }]);
      const db = stores(f, "p");
      db.get("entries")?.set("null-row", null);
      db.get("entries")?.set("wrong-key", entry("other-id", 2)); // a row under a key that is not its mutation id
      db.get("entries")?.set("no-content", { ...entry("no-content", 3), content: "text" });
      db.get("deadLetters")?.set("number", 42);
      db.get("journal")?.set("array", []);
      db.get("exports")?.set("undef", undefined);
      db.get("rejections")?.set("plain", "string");
      db.get("drafts")?.set("d", null);
      db.get("meta")?.set("bad", { not: "a scalar" });
      const b = await open(f);
      for (const read of [() => b.load(), () => b.loadLight?.()]) {
        const s = await read();
        expect(s?.entries.map((e) => e.mutationId)).toEqual(["good"]);
        expect(s?.deadLetters.map((e) => e.mutationId)).toEqual(["dgood"]);
        expect(s?.journal).toEqual([]);
        expect(s?.exports).toEqual([]);
        expect(s?.rejections).toEqual([]);
        expect(s?.drafts).toEqual([]);
        expect(s?.meta).toEqual({ ok: 7 });
        const listed = (s?.unreadable ?? []).map((u) => `${u.store}/${u.key}`).sort();
        expect(listed).toEqual(["deadLetters/number", "drafts/d", "entries/no-content", "entries/null-row", "entries/wrong-key", "exports/undef", "journal/array", "meta/bad", "rejections/plain"]);
        expect((s?.unreadable ?? []).every((u) => u.why.length > 0)).toBe(true);
      }
    });

    it("a replica header that is not one, or a page or a tail row that is not one: that keyspace's image is not returned (a gap would be a wrong confirmed state), the others are", async () => {
      const f = new FakeIDBFactory();
      const a = await open(f);
      for (const ks of ["good", "badhead", "badpage", "badtail"]) {
        await a.commit([{ op: "replicaBegin", keyspaceId: ks, head: head(3n), base: { manifestVersion: 1, logEpoch: 1, checkpointSeq: 3n, entries: [kentry("x/1"), kentry("x/2")] } }]);
        await a.commit([{ op: "replicaAppend", keyspaceId: ks, head: head(5n), records: [rec(4n), rec(5n)] }]);
      }
      const db = stores(f, "p");
      db.get("replicas")?.set("badhead", { head: null, base: {}, entryCount: 2 });
      db.get("replicaEntries")?.set("badpage\u0000" + "0000000001", "not a key entry");
      db.get("replicaTail")?.set("badtail\u0000" + "00000000000000000004", null);
      const b = await open(f);
      const s = await b.load();
      expect(s.replicas.map((i) => i.keyspaceId)).toEqual(["good"]);
      expect(s.replicas[0]?.base.entries).toHaveLength(2);
      expect(s.replicas[0]?.tail.map((r) => r.seq)).toEqual([4n, 5n]);
      const listed = (s.unreadable ?? []).map((u) => `${u.store}/${u.key.replace("\u0000", "\\0")}`);
      expect(listed).toContain("replicas/badhead");
      expect(listed).toContain("replicaEntries/badpage\\00000000001");
      expect(listed).toContain("replicaTail/badtail\\000000000000000000004");
      const light = await b.loadLight?.();
      expect(light?.replicas.map((i) => i.keyspaceId).sort()).toEqual(["badpage", "badtail", "good"]); // the header of each is whole: the rows are read with the image
      expect(await b.loadImage?.("badpage")).toBeUndefined();
      expect(await b.loadImage?.("badtail")).toBeUndefined();
      expect((await b.loadImage?.("good"))?.tail).toHaveLength(2);
    });

    it("a partition with a malformed row opens an engine: the sync status counts what was left out, and the good rows are the engine's", async () => {
      const f = new FakeIDBFactory();
      const a = await open(f);
      await a.commit([{ op: "putDeadLetter", entry: { ...dead("keep", 1), author: undefined as never } }]);
      stores(f, "p").get("entries")?.set("junk", null);
      stores(f, "p").get("deadLetters")?.set("junk2", 7);
      const s = await open(f);
      const r = await rig({ cfg: { storage: s }, start: false });
      expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual(["keep"]);
      expect(r.engine.status().unreadableRows).toBe(2);
      expect(r.engine.unreadableRows().map((u) => `${u.store}/${u.key}`).sort()).toEqual(["deadLetters/junk2", "entries/junk"]);
    });
  });
}
