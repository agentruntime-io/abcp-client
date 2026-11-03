// 14 [CLI-76], with [CLI-13], [CLI-49], [CLI-28], [CLI-83], 09 [READ-13], [READ-100], 06 [LOG-44], [LOG-76]: what the engine does when it learns of a redaction. A test for each local copy
// that the rule names: the replica (and the pages and records that the persisted replica keeps), the dead-letter store, the export-hook entries, the journal, the `previous`
// entries of override records, the saga start documents, the cached blobs; a CRDT key's reset; and each of the three ways of learning: a `REDACTED` record, a load entry with the
// redaction mark, a key of `LoadStateResponse.redactions`. The fake server's `redact` drives them.

import { create } from "@bufbuild/protobuf";
import * as Y from "yjs";
import { describe, expect, it } from "vitest";
import { jsonBytes, set, type EngineEvent } from "../src/engine/index.js";
import { decodeContent, dependsOn, type DepView } from "../src/engine/entry.js";
import { MutationSchema } from "../src/gen/abpc/v2/sync_pb.js";
import { Code, ChangeKind, KeyChangeSchema, KeyEntrySchema, RecordKind, RetryReason, StaleCause, SystemKind, type KeyChange } from "../src/gen/abpc/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { createQueries } from "../src/query/index.js";
import { brig, bytesOf, drive } from "./helpers/presence.js";
import { goOffline, goOnline, kc, KS, mutateRequests, ofType, rejectResult, retryResult, rig, type Rig } from "./helpers/engine.js";

const SECRET = "ERASED-SECRET-VALUE-9876";
const OTHER = "KEPT-VALUE-1111";
const setK = (key: string, v: unknown): { keyspaceId: string; opType: string; changes: ReturnType<typeof set>[] } => ({ keyspaceId: KS, opType: "Set", changes: [set(key, v)] });

/** Whether the bytes of the value, anywhere inside it (a message, a record, a stored row), hold the text. */
function holds(v: unknown, needle: string, depth = 0): boolean {
  if (depth > 40 || v === null || v === undefined) return false;
  if (v instanceof Uint8Array) return Buffer.from(v).toString("latin1").includes(needle);
  if (typeof v === "string") return v.includes(needle);
  if (Array.isArray(v)) return v.some((x) => holds(x, needle, depth + 1));
  if (v instanceof Map) return [...v.values()].some((x) => holds(x, needle, depth + 1));
  if (typeof v === "object") return Object.values(v as object).some((x) => holds(x, needle, depth + 1));
  return false;
}

const contentChanges = (content: Uint8Array): KeyChange[] => decodeContent(content).changes;

/** The dead-letter entry that holds a change of the key, and its export entry, journal entry and rejection: one action each, for the stores that the rule names. */
async function fourStores(r: Rig): Promise<{ journal: string; dead: string; exported: string }> {
  // one injection for both: the fake serves the first one that matches
  r.server.inject("mutate", {
    kind: "result",
    recorded: true,
    result: (m) => (m.changes[0]?.key === "settings/dead" ? rejectResult(m, mk(Code.SCHEMA_INVALID)) : m.changes[0]?.key === "settings/old" ? rejectResult(m, mk(Code.OFFLINE_TOO_LONG, { details: { cause: "HORIZON" } })) : undefined),
  }, { times: 10 });
  const journal = (await r.engine.submit(setK("settings/j", SECRET))).mutationId;
  const dead = (await r.engine.submit(setK("settings/dead", SECRET))).mutationId;
  const exported = (await r.engine.submit(setK("settings/old", SECRET))).mutationId;
  await r.engine.settle();
  return { journal, dead, exported };
}

const dead = (r: Rig): ReturnType<Rig["engine"]["deadLetterEntries"]> => r.engine.deadLetterEntries();

describe("14 [CLI-76] a REDACTED record: every local copy of the values of its erased keys is replaced", () => {
  it("the journal, the dead-letter store, the export-hook entries and the rejections keep their place with the erased keys' changes blanked and the mark `redacted`; `dead-lettered` tells the app; what the entries hold of other keys stays", async () => {
    const r = await rig();
    const a = await fourStores(r);
    // an action with a change of an erased key and one of a key that stays: the second change is not touched
    const mixed = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/j2", SECRET), set("settings/kept", OTHER)] })).mutationId;
    await r.engine.settle();
    expect(r.engine.journalEntries().map((j) => j.mutationId).sort()).toEqual([a.journal, mixed].sort());
    expect(dead(r).map((d) => d.mutationId)).toEqual([a.dead]);
    expect(r.engine.exportEntries().map((d) => d.mutationId)).toEqual([a.exported]);
    expect(holds(r.engine.journalEntries(), SECRET)).toBe(true);
    const eventsBefore = r.events.length;

    // `settings/dead` and `settings/old` never held a value in the server's timeline: they are in `erased_keys`; the others are in `keys`
    const seq = r.server.redact(KS, ["settings/j", "settings/j2", "settings/dead", "settings/old"]);
    await r.engine.settle();

    // 06 [LOG-44]: the record names them as the fake server writes it
    const rec = r.server.records(KS).find((x) => x.seq === seq);
    expect(rec?.kind).toBe(RecordKind.SYSTEM);
    expect(rec?.system?.kind).toBe(SystemKind.REDACTED);
    const body = JSON.parse(new TextDecoder().decode(rec?.system?.body)) as { keys: string[]; erased_keys: string[] };
    expect(body.keys).toEqual(["settings/j", "settings/j2"]);
    expect(body.erased_keys).toEqual(["settings/dead", "settings/old"]);

    // the entries: no value of an erased key in any, in memory and in the storage (CLI-76: the journal, the dead-letter store, the export-hook entries)
    for (const stored of [{ j: r.engine.journalEntries(), d: dead(r), x: r.engine.exportEntries(), q: r.engine.rejectionEntries() }, await r.storage.load()]) {
      const s = stored as { j?: unknown; d?: unknown; x?: unknown; q?: unknown; journal?: unknown; deadLetters?: unknown; exports?: unknown; rejections?: unknown };
      expect(holds(s.j ?? s.journal, SECRET)).toBe(false);
      expect(holds(s.d ?? s.deadLetters, SECRET)).toBe(false);
      expect(holds(s.x ?? s.exports, SECRET)).toBe(false);
      expect(holds(s.q ?? s.rejections, SECRET)).toBe(false);
    }
    const j = r.engine.journalEntries().find((x) => x.mutationId === a.journal);
    expect(j?.redacted).toBe(true);
    // 06 [LOG-76]: the change keeps `key`, `kind`, `seen_version`, and has the encoding `redacted/v1` and no value field
    const [bc] = contentChanges(j?.content as Uint8Array);
    expect(bc).toMatchObject({ key: "settings/j", kind: ChangeKind.SET, encoding: "redacted/v1", isNull: false, delta: 0n });
    expect(bc?.value.length).toBe(0);
    // the change that stays
    const m = r.engine.journalEntries().find((x) => x.mutationId === mixed);
    const [c1, c2] = contentChanges(m?.content as Uint8Array);
    expect(c1).toMatchObject({ key: "settings/j2", encoding: "redacted/v1" });
    expect(c2?.encoding).toBe("json/v1");
    expect(holds(c2?.value, OTHER)).toBe(true);
    // the entries keep their place, with the mark; `redacted` in the stored rows too
    expect(dead(r).map((d) => [d.mutationId, d.redacted])).toEqual([[a.dead, true]]);
    expect(r.engine.exportEntries().map((d) => [d.mutationId, d.redacted])).toEqual([[a.exported, true]]);
    expect(r.engine.rejectionEntries().every((q) => q.redacted === true || q.mutationId === mixed)).toBe(true);
    const st = await r.storage.load();
    expect(st.deadLetters.map((d) => d.redacted)).toEqual([true]);
    expect(st.exports.map((d) => d.redacted)).toEqual([true]);
    expect(st.journal.find((x) => x.mutationId === a.journal)?.redacted).toBe(true);
    // §14.5: `dead-lettered` for a dead-letter entry and for an export entry that a redaction changed, with the cause
    const told = r.events.slice(eventsBefore).filter((e) => e.type === "dead-lettered") as Extract<EngineEvent, { type: "dead-lettered" }>[];
    expect(told.map((e) => e.mutationId).sort()).toEqual([a.dead, a.exported].sort());
    expect(told.every((e) => e.cause === "REDACTED")).toBe(true);
    // the erased keys are gone from the replica (the fold of the record), and show no value
    expect(r.engine.confirmed(KS).get("settings/j")).toBeUndefined();
  });

  it("the persisted replica holds no value of an erased key: the page that held it is a key in the redacted state, the records that set it are blanked, and a restart restores the state the server holds (CLI-2)", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/e", "set", SECRET), kc("settings/s", "set", OTHER)]);
    await r.engine.settle();
    // a restart now loads the pages; the record that set the key later is in the tail
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 9, start: false });
    await r2.engine.stop();
    const r3 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 10 });
    r3.server.write(KS, [kc("settings/e", "set", `${SECRET}-2`)]);
    await r3.engine.settle();
    const img = r3.storage.replica(KS);
    expect(holds(img, SECRET)).toBe(true); // the image holds it until the redaction
    const seq = r3.server.redact(KS, ["settings/e"]);
    await r3.engine.settle();
    const after = r3.storage.replica(KS);
    expect(after).toBeDefined();
    expect(holds(after, SECRET)).toBe(false);
    expect(holds(after, OTHER)).toBe(true); // what is not erased stays
    // the record is in the tail, and the cursor moved to it in the same transaction as the purge
    expect(after?.tail.map((x) => x.seq)).toContain(seq);
    expect(after?.head.seq).toBe(seq);
    // a restart folds the blanked pages and records: the key is absent, with the mark that a redaction gives its tombstone
    await r3.engine.stop();
    const r4 = await rig({ storage: r3.storage, server: r3.server, clock: r3.clock, seed: 11, start: false });
    expect(r4.engine.confirmed(KS).get("settings/e")).toBeUndefined();
    expect(r4.engine.confirmed(KS).json("settings/s")).toBe(OTHER);
    const state = (r4.engine as unknown as { follows: Map<string, { state: { tombstone(k: string): { mark: boolean } | undefined; key(k: string): unknown } }> }).follows.get(KS)?.state;
    expect(state?.tombstone("settings/e")?.mark).toBe(true);
  });

  it("a record that the client received before a key's erasure and a record after it: only what is below the REDACTED record is blanked, a value set again later stays", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/e", "set", SECRET)]);
    await r.engine.settle();
    r.server.redact(KS, ["settings/e"]);
    r.server.write(KS, [kc("settings/e", "set", OTHER)]); // the key is written again, after the erasure
    await r.engine.settle();
    const img = r.storage.replica(KS);
    expect(holds(img, SECRET)).toBe(false);
    expect(holds(img, OTHER)).toBe(true);
    expect(r.engine.confirmed(KS).json("settings/e")).toBe(OTHER);
  });

  it("`previous` entries of an override record are blanked with its change (06 [LOG-21], [LOG-76]), and the entry of another key is not", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/e", "set", SECRET), kc("settings/s", "set", OTHER)]);
    await r.engine.settle();
    const over = (key: string, v: string): KeyChange => create(KeyChangeSchema, { key, kind: ChangeKind.OVERRIDE, value: jsonBytes(v), encoding: "json/v1" });
    const prev = (key: string, v: string): ReturnType<typeof create<typeof KeyEntrySchema>> => create(KeyEntrySchema, { key, value: jsonBytes(v), encoding: "json/v1", version: 1n, changedBy: "someone" });
    r.server.write(KS, [over("settings/e", "NEW-E"), over("settings/s", "NEW-S")], { opType: "abpc.Override", previous: [prev("settings/e", SECRET), prev("settings/s", OTHER)] });
    await r.engine.settle();
    expect(holds(r.storage.replica(KS)?.tail, SECRET)).toBe(true); // the previous entry of the key holds its value in the client's copy
    r.server.redact(KS, ["settings/e"]);
    await r.engine.settle();
    const rec = r.storage.replica(KS)?.tail.find((x) => x.previous.length > 0);
    expect(holds(rec, SECRET)).toBe(false);
    expect(holds(rec, "NEW-E")).toBe(false);
    expect(rec?.previous[0]).toMatchObject({ key: "settings/e", encoding: "redacted/v1", changedBy: "", isNull: false });
    expect(rec?.previous[0]?.value.length).toBe(0);
    expect(rec?.previous[0]?.version).toBe(1n); // the version stays, as for a key in the redacted state
    expect(holds(rec?.previous[1], OTHER)).toBe(true);
    expect(holds(rec?.changes[1], "NEW-S")).toBe(true);
  });

  it("the outbox stays: a pending action is the user's new write, with its value, and the predicted view still shows it (CLI-76); nothing of it is blanked, and nothing is sent because of the redaction", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/e", "set", SECRET)]);
    await r.engine.settle();
    r.server.inject("mutate", { kind: "result", result: (m) => (m.changes[0]?.key === "settings/e" ? retryResult(m, RetryReason.CONTENTION, 3_600_000) : undefined) });
    const p = await r.engine.submit(setK("settings/e", OTHER)); // pending: the server holds it back
    await r.engine.settle();
    const sent = mutateRequests(r).length;
    r.server.redact(KS, ["settings/e"]);
    await r.engine.settle();
    const e = r.engine.outbox().find((x) => x.mutationId === p.mutationId);
    expect(e).toBeDefined();
    const [c] = contentChanges(e?.content as Uint8Array);
    expect(c).toMatchObject({ key: "settings/e", encoding: "json/v1" });
    expect(holds(e?.content, OTHER)).toBe(true);
    expect(holds((await r.storage.load()).entries, OTHER)).toBe(true);
    expect(r.engine.view(KS).json("settings/e")).toBe(OTHER); // the prediction of the user's new write
    expect(r.engine.confirmed(KS).get("settings/e")).toBeUndefined(); // the confirmed copy of the erased value is gone
    expect(mutateRequests(r)).toHaveLength(sent);
  });

  it("a saga's start document: the run-key SET of the record replaces the start document that a journal, a dead-letter and an export entry hold (CLI-76, 12 [ADM-65], 11 [COORD-58])", async () => {
    const r = await rig();
    const RUN = "abpc.saga.v1/run/saga-1";
    const b64 = Buffer.from(JSON.stringify(SECRET)).toString("base64");
    const steps = [{ name: "s1", base_seq: 0, base_log_epoch: 1, changes: [{ key: "settings/e", kind: "CHANGE_KIND_SET", value: b64, encoding: "json/v1", seenVersion: "0" }], compensate: [] }];
    const startDoc = (): Uint8Array => jsonBytes({ saga: "S", params: { n: 1 }, steps });
    const start = (opType: string): { keyspaceId: string; opType: string; changes: ReturnType<typeof set>[] } => ({ keyspaceId: KS, opType, changes: [{ ...set("x", 1), key: RUN, value: startDoc() }] });
    // a Start of that run key in each store that the rule names: the journal (it commits), the dead-letter store and the export hook (the server rejects it)
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.opType === "Create" ? rejectResult(m, mk(Code.SCHEMA_INVALID)) : m.opType === "Silent" ? rejectResult(m, mk(Code.OFFLINE_TOO_LONG, { details: { cause: "HORIZON" } })) : undefined) }, { times: 10 });
    const j = (await r.engine.submit(start("Set"))).mutationId;
    await r.engine.settle();
    const d = (await r.engine.submit(start("Create"))).mutationId;
    await r.engine.settle();
    const x = (await r.engine.submit(start("Silent"))).mutationId;
    await r.engine.settle();
    // the saga module then wrote its members into the run key
    r.server.write(KS, [create(KeyChangeSchema, { key: RUN, kind: ChangeKind.SET, encoding: "json/v1", value: jsonBytes({ saga: "S", params: { n: 1 }, steps, status: "running" }) })]);
    await r.engine.settle();
    expect(holds(r.engine.journalEntries().find((e) => e.mutationId === j)?.content, b64)).toBe(true);
    expect(holds(dead(r).find((e) => e.mutationId === d)?.content, b64)).toBe(true);
    expect(holds(r.engine.exportEntries().find((e) => e.mutationId === x)?.content, b64)).toBe(true);
    r.server.redact(KS, ["settings/e"]); // the key is erased in the start document: the record carries a server-written SET of the run key
    await r.engine.settle();
    const rec = r.server.records(KS).at(-1);
    expect(rec?.system?.kind).toBe(SystemKind.REDACTED);
    expect(rec?.changes.map((c) => [c.key, c.kind])).toEqual([[RUN, ChangeKind.SET]]);
    for (const content of [r.engine.journalEntries().find((e) => e.mutationId === j)?.content, dead(r).find((e) => e.mutationId === d)?.content, r.engine.exportEntries().find((e) => e.mutationId === x)?.content]) {
      const [c] = contentChanges(content as Uint8Array);
      expect(c?.key).toBe(RUN);
      expect(c?.kind).toBe(ChangeKind.SET); // the SET stays a SET of the run key, with the encoding of the start document
      expect(c?.encoding).toBe("json/v1");
      const stored = JSON.parse(new TextDecoder().decode(c?.value)) as { saga: string; steps: { changes: Record<string, unknown>[] }[]; status?: string };
      expect(stored.status).toBeUndefined(); // the module's members are not the Start's
      expect(stored.saga).toBe("S");
      expect(stored.steps[0]?.changes[0]).toEqual({ key: "settings/e", kind: "CHANGE_KIND_SET", seenVersion: "0", redacted: true });
      expect(holds(content, b64)).toBe(false);
    }
    // the record's SET is what the replica holds now, and the earlier changes that set the run key are blanked in the persisted records
    const run = JSON.parse(new TextDecoder().decode(r.engine.confirmed(KS).get(RUN)?.value)) as { status: string; steps: { changes: Record<string, unknown>[] }[] };
    expect(run.status).toBe("running");
    expect(run.steps[0]?.changes[0]?.["redacted"]).toBe(true);
    expect(holds(r.storage.replica(KS), b64)).toBe(false);
    expect(holds(await r.storage.load(), b64)).toBe(false);
  });
});

describe("14 [CLI-76] the live query layer and the change feed: what a query shows of an erased key goes", () => {
  it("a live query that held the key's row removes it when the record is applied, and its snapshot holds no value of it", async () => {
    const r = await rig();
    const q = createQueries(r.engine);
    r.server.write(KS, [kc("settings/e", "set", SECRET), kc("settings/s", "set", OTHER)]);
    await r.engine.settle();
    const seen: string[] = [];
    const live = q.watch({ keyspaceId: KS, select: { prefix: "settings/" } }, (u) => void seen.push(`${u.cause}:${u.removed.map((x) => x.key).join(",")}`));
    expect(holds(live.snapshot().rows, SECRET)).toBe(true);
    r.server.redact(KS, ["settings/e"]);
    await r.engine.settle();
    expect(holds(live.snapshot().rows, SECRET)).toBe(false);
    expect(live.snapshot().rows.map((x) => x.key)).toEqual(["settings/s"]);
    expect(seen.some((x) => x.includes("settings/e"))).toBe(true);
    live.close();
    q.close();
  });
});

describe("14 [CLI-76] a load entry with the redaction mark and `LoadStateResponse.redactions`: the copies that the client held before the load are purged", () => {
  /** The client is offline while the server writes and redacts, and the log is pruned: it resumes with a cold load (09 [READ-17]), which lists the redaction. */
  async function missedRedaction(r: Rig, keys: string[], then?: () => void): Promise<void> {
    await goOffline(r);
    r.server.redact(KS, keys);
    then?.();
    r.server.write(KS, [kc("settings/other", "set", 1)]);
    r.server.prune(KS, r.server.head(KS), { judgeHorizon: false }); // the client's cursor is below the floor: RESUME_TOO_OLD, a cold load that carries the redactions
    await goOnline(r);
    await r.engine.settle();
  }

  it("a key that `redactions` lists: the journal, dead-letter and export copies are purged, whatever the load's entry for the key; the load is a resume check, so it lists them", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/j", "set", SECRET)]);
    await r.engine.settle();
    const a = await fourStores(r);
    expect(holds(r.engine.journalEntries(), SECRET)).toBe(true);
    // the key is re-created after the redaction: the load has a live entry for it, which is no marked tombstone; `redactions` is what names the redaction (09 [READ-13])
    await missedRedaction(r, ["settings/j", "settings/dead", "settings/old"], () => r.server.write(KS, [kc("settings/j", "set", OTHER)]));
    const loads = r.server.calls.filter((c) => c.method === "loadState");
    expect(loads.some((c) => (c.request as { cursor?: { afterSeq: bigint } }).cursor?.afterSeq !== 0n)).toBe(true);
    expect(r.events.some((e) => e.type === "reloaded")).toBe(true);
    expect(r.engine.confirmed(KS).json("settings/j")).toBe(OTHER); // the load's value is the key's value now; nothing of the erased one
    expect(holds(r.engine.journalEntries(), SECRET)).toBe(false);
    expect(holds(dead(r), SECRET)).toBe(false);
    expect(holds(r.engine.exportEntries(), SECRET)).toBe(false);
    const st = await r.storage.load();
    expect(holds([st.journal, st.deadLetters, st.exports, st.rejections, st.replicas], SECRET)).toBe(false);
    expect(dead(r).map((d) => [d.mutationId, d.redacted])).toEqual([[a.dead, true]]);
    const told = ofType(r, "dead-lettered").filter((e) => e.cause === "REDACTED");
    expect(told.map((e) => e.mutationId).sort()).toEqual([a.dead, a.exported].sort());
  });

  it("a load entry with the mark (a tombstone that a redaction created) purges the same copies; an ordinary tombstone purges nothing (CLI-76, 09 [READ-9])", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/j", "set", SECRET), kc("settings/plain", "set", SECRET)]);
    await r.engine.settle();
    const j = (await r.engine.submit(setK("settings/j", SECRET))).mutationId;
    const plain = (await r.engine.submit(setK("settings/plain", SECRET))).mutationId;
    await r.engine.settle();
    expect(r.engine.journalEntries().map((x) => x.mutationId).sort()).toEqual([j, plain].sort());
    await goOffline(r);
    r.server.redact(KS, ["settings/j"]); // the tombstone of settings/j carries the mark
    r.server.write(KS, [kc("settings/plain", "delete")]); // an ordinary tombstone
    r.server.prune(KS, r.server.head(KS), { judgeHorizon: false });
    await goOnline(r);
    await r.engine.settle();
    // the cold load carries the marked tombstone and `redactions`; the journal entry of settings/j is blanked, the one of settings/plain is as it was
    const jj = r.engine.journalEntries().find((x) => x.mutationId === j);
    const pp = r.engine.journalEntries().find((x) => x.mutationId === plain);
    expect(jj?.redacted).toBe(true);
    expect(holds(jj, SECRET)).toBe(false);
    expect(pp?.redacted).toBeUndefined();
    expect(holds(pp?.content, "settings/plain")).toBe(true);
    expect(holds(pp?.content, SECRET)).toBe(true);
  });

  it("a REDACTED record reaches every interest (09 [READ-23]): the copies of a key outside the client's interest are purged too", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/j", "set", SECRET)]);
    await r.engine.settle();
    const j = (await r.engine.submit(setK("settings/j", SECRET))).mutationId;
    await r.engine.settle();
    await r.engine.setInterest(KS, ["notes"]); // narrowed: settings/j is outside the interest
    await r.engine.settle();
    r.server.redact(KS, ["settings/j"]);
    await r.engine.settle();
    expect(holds(r.engine.journalEntries(), SECRET)).toBe(false);
    expect(r.engine.journalEntries().find((x) => x.mutationId === j)?.redacted).toBe(true);
    expect(holds(await r.storage.load(), SECRET)).toBe(false);
  });

  it("a widening load that installs a marked tombstone purges the copies of the key too (\"from any load (a widening load included)\")", async () => {
    // the load of the widening is the one that names the tombstone: the transport adds the entry that a server that kept it would serve
    const r0 = await rig();
    r0.server.write(KS, [kc("settings/j", "set", SECRET)]);
    await r0.engine.settle();
    const j = (await r0.engine.submit(setK("settings/j", SECRET))).mutationId;
    await r0.engine.settle();
    await r0.engine.setInterest(KS, ["notes"]);
    await r0.engine.settle();
    expect(holds(r0.engine.journalEntries(), SECRET)).toBe(true);
    const inner = r0.config.transport;
    let widened = false;
    const marked = new Proxy(inner, {
      get(t, p, rcv): unknown {
        if (p !== "loadState") return Reflect.get(t, p, rcv) as unknown;
        return async (req: Parameters<typeof inner.loadState>[0]) => {
          const resp = await inner.loadState(req);
          if (req.cursor?.interestPrefixes.includes("settings") === true && req.cursor.afterSeq === 0n) {
            widened = true;
            return { ...resp, entries: [...resp.entries.filter((e) => e.key !== "settings/j"), create(KeyEntrySchema, { key: "settings/j", deleted: true, redacted: true, version: 9n })] };
          }
          return resp;
        };
      },
    });
    await r0.engine.stop();
    const r = await rig({ storage: r0.storage, server: r0.server, clock: r0.clock, seed: 41, start: false, cfg: { transport: marked } });
    r.engine.start();
    await r.engine.settle();
    await r.engine.setInterest(KS, ["notes", "settings"]);
    await r.engine.settle();
    expect(widened).toBe(true);
    expect(holds(r.engine.journalEntries(), SECRET)).toBe(false);
    expect(r.engine.journalEntries().find((x) => x.mutationId === j)?.redacted).toBe(true);
    expect(holds(await r.storage.load(), SECRET)).toBe(false);
  });
});

describe("14 [CLI-76], [CLI-13], [CLI-49] a CRDT key that a redaction erased is reset: its pending updates and unsent edits go to the draft, and the view is built again from the confirmed document alone", () => {
  const DOC = "docs/d1";
  const typed = (doc: Y.Doc, text: string): void => {
    doc.getText("text").insert(doc.getText("text").length, text);
  };
  /** The server holds an update of the key back (a `Retry`), so that it is pending when the record arrives. */
  const holdBack = (r: Rig, ids: () => string[]): void => {
    r.server.inject("mutate", { kind: "result", result: (m) => (ids().includes(m.mutationId) ? retryResult(m, RetryReason.CONTENTION, 3_600_000) : undefined) }, { times: 100 });
  };
  /** A key with confirmed text, and a handle on it. */
  async function opened(r: Rig, text = "confirmed text. "): Promise<ReturnType<Rig["engine"]["crdt"]>> {
    const h = r.engine.crdt(KS, DOC);
    typed(h.doc, text);
    await h.flush("DocEdit");
    await r.engine.settle();
    expect(r.engine.crdt(KS, DOC).confirmedDoc.getText("text").toString()).toBe(text);
    return r.engine.crdt(KS, DOC);
  }

  it("a REDACTED record: `rebuilt` once (cause REDACTED) with nothing re-applied; the pending update and the unsent edit are the draft; the pending action stays in the outbox", async () => {
    const r = await rig();
    const h = await opened(r);
    const held: string[] = [];
    holdBack(r, () => held);
    typed(h.doc, "PENDING-SECRET ");
    const pending = await h.flush("DocEdit"); // an action in the outbox, authored before the reset
    held.push(pending?.mutationId as string);
    await r.engine.settle();
    typed(r.engine.crdt(KS, DOC).doc, "UNSENT-SECRET"); // edits that are in no action yet
    const before = r.events.length;
    r.server.redact(KS, [DOC]);
    await r.engine.settle();
    const rebuilt = r.events.slice(before).filter((e) => e.type === "rebuilt") as Extract<EngineEvent, { type: "rebuilt" }>[];
    expect(rebuilt.filter((e) => e.key === DOC && e.cause === "REDACTED")).toHaveLength(1);
    const view = r.engine.crdt(KS, DOC).doc.getText("text").toString();
    expect(view).toBe(""); // the pending update left the view, the unsent edit was not re-applied, and the confirmed document (the erased history) is gone
    expect(r.engine.crdt(KS, DOC).confirmedDoc.getText("text").toString()).toBe("");
    expect(r.engine.crdt(KS, DOC).hasUnsent).toBe(false);
    // the draft: the Yjs updates of both (they are the user's text, which the reset keeps for the user)
    const draft = r.engine.draftEntries().find((d) => d.key === DOC);
    expect(draft?.reason).toBe("REDACTED");
    expect(holds(draft?.updates, "PENDING-SECRET")).toBe(true);
    expect(holds(draft?.updates, "UNSENT-SECRET")).toBe(true);
    expect(holds(draft?.updates, "confirmed text")).toBe(false); // the erased value is no part of it
    expect((await r.storage.load()).drafts.map((d) => d.key)).toEqual([DOC]);
    // the action stays in the outbox, and is not sent again by the reset; the server ends it with STALE{DOC_RESET} (05 [STATE-85]), which the existing path of [CLI-49] handles
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([pending?.mutationId]);
    expect(holds(r.engine.outbox()[0]?.content, "PENDING-SECRET")).toBe(true);
  });

  it("an update typed after the reset does not depend on the action that was authored before it (14 [CLI-30] item 1): the server ends that one with STALE{DOC_RESET} and nothing fails the new one with DEPENDENCY_FAILED", async () => {
    const r = await rig();
    const h = await opened(r, "base ");
    const held: string[] = [];
    holdBack(r, () => held);
    typed(h.doc, "OLD-PENDING");
    const a = await h.flush("DocEdit");
    held.push(a?.mutationId as string);
    await r.engine.settle();
    r.server.redact(KS, [DOC]);
    await r.engine.settle();
    const view = r.engine.crdt(KS, DOC);
    typed(view.doc, "NEW-TEXT");
    const b = await view.flush("DocEdit");
    await r.engine.settle();
    // B does not depend on A
    const eb = r.engine.outbox().find((e) => e.mutationId === b?.mutationId);
    expect(eb?.dependsOn).toEqual([]);
    // the server ends A with STALE{DOC_RESET} (05 [STATE-85]; the fake does not model the reset seq, so the test says it), and then takes B
    r.server.clearInjections();
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.mutationId === a?.mutationId ? rejectResult(m, mk(Code.STALE, { stale: { cause: StaleCause.DOC_RESET } })) : undefined) }, { times: 100 });
    await r.clock.advance(3_600_000);
    await r.engine.settle();
    await r.clock.advance(60_000); // B was sent behind A in the lane, and waits out the server's answer for it
    await r.engine.settle();
    expect(ofType(r, "rejected").some((e) => e.mutationId === b?.mutationId)).toBe(false);
    expect(r.engine.deadLetterEntries().some((e) => e.mutationId === b?.mutationId)).toBe(false);
    expect(r.engine.journalEntries().some((j) => j.mutationId === b?.mutationId)).toBe(true);
    expect(r.server.ydoc(KS, DOC)?.getText("text").toString()).toBe("NEW-TEXT"); // on the document that the reset left
  });

  it("the exception of 14 [CLI-30] item 1 is for the same CRDT key, authored across the reset, and for nothing else", () => {
    const upd = (id: string, key: string, kind = ChangeKind.CRDT_UPDATE): DepView => ({
      mutationId: id,
      opType: "DocEdit",
      keys: [key],
      mutation: create(MutationSchema, { mutationId: id, changes: [create(KeyChangeSchema, { key, kind, update: new Uint8Array([1]) })] }),
    });
    const ctx = { barrier: () => false, fencedBy: () => undefined, resetOrdinal: (k: string) => (k === "docs/d1" ? 5 : 0) };
    const none = new Set<string>();
    // before and after the reset (ordinals 4 and 5): no dependency through the key
    expect(dependsOn({ ...upd("b", "docs/d1"), ordinal: 5 }, { ...upd("a", "docs/d1"), ordinal: 4 }, none, ctx)).toBe(false);
    // both before it, or both after it: as always
    expect(dependsOn({ ...upd("b", "docs/d1"), ordinal: 4 }, { ...upd("a", "docs/d1"), ordinal: 3 }, none, ctx)).toBe(true);
    expect(dependsOn({ ...upd("b", "docs/d1"), ordinal: 7 }, { ...upd("a", "docs/d1"), ordinal: 6 }, none, ctx)).toBe(true);
    // a key that no redaction reset
    expect(dependsOn({ ...upd("b", "docs/d2"), ordinal: 5 }, { ...upd("a", "docs/d2"), ordinal: 4 }, none, ctx)).toBe(true);
    // an explicit link still holds
    expect(dependsOn({ ...upd("b", "docs/d1"), ordinal: 5 }, { ...upd("a", "docs/d1"), ordinal: 4 }, new Set(["a"]), ctx)).toBe(true);
    // a key below it (a parent) is not the same key
    expect(dependsOn({ ...upd("b", "docs/d1"), ordinal: 5 }, { ...upd("a", "docs"), ordinal: 4 }, none, ctx)).toBe(true);
  });

  it("the reset is kept across a restart: the pending update that was authored before it does not come back into the view, and the draft stays", async () => {
    const r = await rig();
    const h = await opened(r, "base ");
    const held: string[] = [];
    holdBack(r, () => held);
    typed(h.doc, "OLD-PENDING");
    held.push(((await h.flush("DocEdit")) as { mutationId: string }).mutationId);
    await r.engine.settle();
    r.server.redact(KS, [DOC]);
    await r.engine.settle();
    expect(r.engine.crdt(KS, DOC).doc.getText("text").toString()).toBe("");
    await r.engine.stop();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 33, start: false });
    const view = r2.engine.crdt(KS, DOC).doc.getText("text").toString();
    expect(view).not.toContain("OLD-PENDING");
    expect(holds(r2.engine.draftEntries().find((d) => d.key === DOC)?.updates, "OLD-PENDING")).toBe(true);
    expect(r2.engine.outbox()).toHaveLength(1); // the action is still the outbox's
  });

  it("a load that lists the key (a resume check that came back as a cold load) resets it the same way, whatever the load's entry for the key", async () => {
    const r = await rig();
    const h = await opened(r, "base ");
    const held: string[] = [];
    holdBack(r, () => held);
    typed(h.doc, "OLD-PENDING ");
    held.push(((await h.flush("DocEdit")) as { mutationId: string }).mutationId);
    await r.engine.settle();
    typed(r.engine.crdt(KS, DOC).doc, "UNSENT");
    await goOffline(r);
    r.server.redact(KS, [DOC]);
    r.server.write(KS, [kc("settings/other", "set", 1)]);
    r.server.prune(KS, r.server.head(KS), { judgeHorizon: false });
    const before = r.events.length;
    await goOnline(r);
    await r.engine.settle();
    expect(r.events.slice(before).filter((e) => e.type === "rebuilt" && e.key === DOC && e.cause === "REDACTED")).toHaveLength(1);
    expect(r.engine.crdt(KS, DOC).doc.getText("text").toString()).toBe("");
    const draft = r.engine.draftEntries().find((d) => d.key === DOC);
    expect(holds(draft?.updates, "UNSENT")).toBe(true);
    expect(holds(draft?.updates, "OLD-PENDING")).toBe(true);
  });
});

describe("14 [CLI-76] cached blob bytes that only erased values named are dropped (the reading of the R1 design, section 4)", () => {
  const photo = (id: string): unknown => ({ photo: id });

  it("the bytes of a downloaded blob that only an erased value names go; one that a key that stays also names stays, and so does one that no value of the keyspace names", async () => {
    const alice = await brig();
    const [p, q, u] = [await drive(alice, alice.engine.uploadBlob(KS, bytesOf(300, 1))), await drive(alice, alice.engine.uploadBlob(KS, bytesOf(300, 2))), await drive(alice, alice.engine.uploadBlob(KS, bytesOf(300, 3)))];
    const bob = await brig({ server: alice.server, clock: alice.clock, manifests: alice.manifests, sub: "bob", seed: 31 });
    alice.server.write(KS, [kc("settings/erased", "set", photo(p.blobId)), kc("settings/shared", "set", photo(q.blobId)), kc("settings/kept", "set", photo(q.blobId))]);
    await bob.engine.settle();
    for (const ref of [p, q, u]) await drive(bob, bob.engine.getBlob(KS, ref.blobId));
    expect([p, q, u].map((x) => bob.engine.blobCached(KS, x.blobId))).toEqual([true, true, true]);
    alice.server.redact(KS, ["settings/erased", "settings/shared"]);
    await bob.engine.settle();
    expect(bob.engine.blobCached(KS, p.blobId)).toBe(false); // only the erased value named it
    expect(bob.engine.blobCached(KS, q.blobId)).toBe(true); // `settings/kept` names it too
    expect(bob.engine.blobCached(KS, u.blobId)).toBe(true); // no erased value names it
  });

  it("the staged bytes of an action that went to the dead-letter store go with its erased values; those of a pending action stay (09 [READ-94])", async () => {
    const r = await brig();
    const dead = await r.engine.stageBlob(KS, bytesOf(100, 5));
    const pend = await r.engine.stageBlob(KS, bytesOf(100, 6));
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.changes[0]?.key === "settings/dead" ? rejectResult(m, mk(Code.SCHEMA_INVALID)) : retryResult(m, RetryReason.CONTENTION, 3_600_000)) }, { times: 20 });
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/dead", 1)], blobRefs: [dead.blobId] } as never);
    await r.engine.settle();
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/pend", 1)], blobRefs: [pend.blobId] } as never);
    await r.engine.settle();
    expect(r.engine.deadLetterEntries()).toHaveLength(1);
    expect((await r.engine.stagedBlobs()).map((s) => s.blobId).sort()).toEqual([dead.blobId, pend.blobId].sort());
    r.server.redact(KS, ["settings/dead", "settings/pend"]); // the pending action writes the erased key too: it is the user's new write, and its bytes stay
    await r.engine.settle();
    expect((await r.engine.stagedBlobs()).map((s) => s.blobId)).toEqual([pend.blobId]);
  });
});
