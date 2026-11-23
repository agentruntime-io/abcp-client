// 14 [CLI-76], [CLI-66], [CLI-77] on the durable storages and across failures: "the erased bytes are gone from disk" (the journal of the file storage is rewritten as a snapshot), no row of
// the IndexedDB database holds an erased value, a purge that fails is made again and the persisted cursor does not pass the record meanwhile, and the answers of Table 12.4 that a client
// acts on for a deleted keyspace.

import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Engine, LocalRefusal, set, StaticManifests, type EngineEvent } from "../src/engine/index.js";
import { BlobError } from "../src/engine/index.js";
import { FakeServer, ManualClock, reject as mk, SeededRandom } from "../src/fake/index.js";
import { Code } from "../src/gen/abcp/v2/types_pb.js";
import { FileStorage } from "../src/platform/file-storage.js";
import { createWebHost } from "../src/platform/web.js";
import { FakeIDBFactory, FakeKeyRange } from "./helpers/fake-idb.js";
import { kc, KS, MANIFEST, ofType, rejectResult, rig, types } from "./helpers/engine.js";
import { brig, bytesOf, drive } from "./helpers/presence.js";

const SECRET = "ERASED-SECRET-VALUE-9876";
const setK = (key: string, v: unknown): { keyspaceId: string; opType: string; changes: ReturnType<typeof set>[] } => ({ keyspaceId: KS, opType: "Set", changes: [set(key, v)] });

/** Every byte string that the file holds (the `$u8` members of its lines, decoded) and its text, for a search that does not depend on how the bytes are written. */
function diskBytes(dir: string): Buffer {
  const text = readFileSync(path.join(dir, "journal.jsonl"), "utf8");
  const parts: Buffer[] = [Buffer.from(text, "utf8")];
  for (const m of text.matchAll(/"\$u8":"([A-Za-z0-9+/=]*)"/g)) parts.push(Buffer.from(m[1] as string, "base64"));
  return Buffer.concat(parts);
}

function rowsOf(v: unknown, out: Buffer[] = [], depth = 0): Buffer[] {
  if (depth > 40 || v === null || v === undefined) return out;
  if (v instanceof Uint8Array) out.push(Buffer.from(v));
  else if (typeof v === "string") out.push(Buffer.from(v, "utf8"));
  else if (Array.isArray(v)) for (const x of v) rowsOf(x, out, depth + 1);
  else if (v instanceof Map) for (const x of v.values()) rowsOf(x, out, depth + 1);
  else if (typeof v === "object") for (const x of Object.values(v as object)) rowsOf(x, out, depth + 1);
  return out;
}

/** The file storage commits to a disk, which the engine's `settle` does not wait for: wait, in real time, until the condition holds. */
async function waitFor(r: { engine: { settle(): Promise<void> }; clock: { advance(ms: number): Promise<void> } }, cond: () => boolean | Promise<boolean>, what: string): Promise<void> {
  for (let i = 0; i < 300; i++) {
    await r.engine.settle();
    if (await cond()) return;
    await r.clock.advance(150); // an action that the server did not process in the request (a lane stops at a rejection) is sent again after its Retry
    await new Promise((x) => setTimeout(x, 10));
  }
  throw new Error(`never held: ${what}`);
}

let dir: string;
beforeEach(() => {
  dir = mkdtempSync(path.join(tmpdir(), "abcp-redact-"));
});
afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

describe("14 [CLI-76] on the file storage: the erased bytes are not on disk after the commit that erases them", () => {
  it("the journal file holds the value in the pages, the records, the journal, the dead-letter store, the export entries and the rejections before the redaction, and none of it after", async () => {
    const storage = await FileStorage.open(path.join(dir, "p"));
    const r = await rig({ cfg: { storage }, start: false });
    r.server.write(KS, [kc("settings/p", "set", SECRET)]); // the pages of the first load hold it
    r.engine.start();
    await waitFor(r, async () => (await storage.load()).replicas.length > 0, "the image");
    // one injection for both: the fake serves the first one that matches
    r.server.inject("mutate", {
      kind: "result",
      recorded: true,
      result: (m) => (m.changes[0]?.key === "settings/dead" ? rejectResult(m, mk(Code.SCHEMA_INVALID)) : m.changes[0]?.key === "settings/old" ? rejectResult(m, mk(Code.OFFLINE_TOO_LONG, { details: { cause: "HORIZON" } })) : undefined),
    }, { times: 10 });
    const ids = [await r.engine.submit(setK("settings/j", SECRET)), await r.engine.submit(setK("settings/dead", SECRET)), await r.engine.submit(setK("settings/old", SECRET))];
    expect(ids).toHaveLength(3);
    await waitFor(r, async () => {
      const s = await storage.load();
      return s.journal.length >= 1 && s.deadLetters.length >= 1 && s.exports.length >= 1;
    }, "an entry in each store");
    r.server.write(KS, [kc("settings/p", "set", `${SECRET}-2`)]); // a record in the tail of the persisted replica
    await waitFor(r, async () => ((await storage.load()).replicas[0]?.tail.length ?? 0) > 0, "the record in the tail");
    const before = diskBytes(path.join(dir, "p"));
    expect(before.includes(SECRET)).toBe(true);
    // every place that the rule names holds it now
    const st = await storage.load();
    expect(st.journal.length + st.deadLetters.length + st.exports.length).toBeGreaterThanOrEqual(3);
    expect(st.replicas[0]?.base.entries.some((e) => Buffer.from(e.value).includes(SECRET)) || st.replicas[0]?.tail.some((x) => x.changes.some((c) => Buffer.from(c.value).includes(SECRET)))).toBe(true);

    r.server.redact(KS, ["settings/p", "settings/j", "settings/dead", "settings/old"]);
    await waitFor(r, () => !diskBytes(path.join(dir, "p")).includes(SECRET), "the erasure on disk");
    const after = diskBytes(path.join(dir, "p"));
    expect(after.includes(SECRET)).toBe(false); // not a byte of it, in any line, as text or as bytes
    expect(Buffer.from(readFileSync(path.join(dir, "p", "journal.jsonl"))).includes(Buffer.from(SECRET).toString("base64"))).toBe(false);
    // the journal was rewritten as the snapshot, the one line of the state, which holds what is not erased and what the redaction left (the mark, the blanked changes)
    const lines = readFileSync(path.join(dir, "p", "journal.jsonl"), "utf8").trimEnd().split("\n");
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain("redacted/v1");
    const st2 = await storage.load();
    expect(st2.deadLetters[0]?.redacted).toBe(true);
    expect(st2.exports[0]?.redacted).toBe(true);
    // a restart of the process reads the file: the state is what the server holds, with nothing of the value
    await r.engine.stop();
    await storage.close();
    const again = await FileStorage.open(path.join(dir, "p"));
    const r2 = await rig({ cfg: { storage: again }, server: r.server, clock: r.clock, seed: 12, start: false });
    expect(r2.engine.confirmed(KS).get("settings/p")).toBeUndefined();
    expect(r2.engine.deadLetterEntries()[0]?.redacted).toBe(true);
    expect(diskBytes(path.join(dir, "p")).includes(SECRET)).toBe(false);
    await r2.engine.stop();
    await again.close();
  });

  it("an erasure that touches no replica (a keyspace whose image is not kept) still rewrites the journal file: the entries that it replaced are not in an earlier line", async () => {
    const storage = await FileStorage.open(path.join(dir, "q"));
    const r = await rig({ cfg: { storage, persistReplica: false } });
    const j = (await r.engine.submit(setK("settings/j", SECRET))).mutationId;
    await waitFor(r, async () => (await storage.load()).journal.length === 1, "the journal entry");
    expect(diskBytes(path.join(dir, "q")).includes(SECRET)).toBe(true);
    r.server.redact(KS, ["settings/j"]);
    await waitFor(r, () => !diskBytes(path.join(dir, "q")).includes(SECRET), "the erasure on disk");
    expect(r.engine.journalEntries().find((x) => x.mutationId === j)?.redacted).toBe(true);
    expect(diskBytes(path.join(dir, "q")).includes(SECRET)).toBe(false);
    await r.engine.stop();
    await storage.close();
  });
});

describe("14 [CLI-76] on the IndexedDB storage: no row of the partition's database holds an erased value", () => {
  it("the rows of the entries, the journal, the dead-letter store and the replica's pages and records are replaced; the database holds the mark and the blanked changes", async () => {
    const clock = new ManualClock();
    const manifests = new StaticManifests(MANIFEST);
    const server = new FakeServer({ manifests, clock, heartbeatMs: 15_000 });
    server.createKeyspaceNow(KS);
    const idb = new FakeIDBFactory();
    const host = createWebHost({ env: { indexedDB: idb, IDBKeyRange: FakeKeyRange }, tabId: "t1" });
    const principal = server.principal("ana");
    server.write(KS, [kc("settings/p", "set", SECRET)]); // the pages of the first load hold it
    const engine = await Engine.create({
      appId: "app",
      principal: { issuer: principal.issuer, sub: principal.sub, deviceId: principal.deviceId },
      transport: server.transport(principal),
      manifests,
      clock,
      random: new SeededRandom(7),
      tab: host,
    });
    const events: EngineEvent[] = [];
    engine.on((e) => events.push(e));
    await engine.addKeyspace(KS);
    engine.start();
    await engine.settle();
    server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.changes[0]?.key === "settings/dead" ? rejectResult(m, mk(Code.SCHEMA_INVALID)) : undefined) });
    await engine.submit(setK("settings/j", SECRET));
    await engine.submit(setK("settings/dead", SECRET));
    await engine.settle();
    server.write(KS, [kc("settings/p", "set", `${SECRET}-2`)]);
    await engine.settle();
    const db = [...idb.dbs.keys()].find((k) => k.startsWith("abcp:") && k !== "abcp:registry") as string;
    const scan = (): { store: string; key: string }[] => {
      const hits: { store: string; key: string }[] = [];
      for (const [store, rows] of idb.dbs.get(db)?.stores ?? []) for (const [key, v] of rows) if (rowsOf(v).some((b) => b.includes(SECRET))) hits.push({ store, key });
      return hits;
    };
    const holding = scan().map((h) => h.store);
    for (const s of ["journal", "deadLetters", "rejections", "replicaEntries", "replicaTail"]) expect(holding).toContain(s);

    server.redact(KS, ["settings/p", "settings/j", "settings/dead"]);
    await engine.settle();
    expect(scan()).toEqual([]);
    const rows = idb.dbs.get(db)?.stores.get("replicaEntries") ?? new Map();
    expect([...rows.values()].every((e) => !rowsOf(e).some((b) => b.includes(SECRET)))).toBe(true);
    expect((await engine.storage.load()).deadLetters[0]?.redacted).toBe(true);
    expect(events.some((e) => e.type === "dead-lettered" && e.cause === "REDACTED")).toBe(true);
    await engine.stop();
  });
});

describe("14 [CLI-76] a purge that cannot be made durable is made again; the persisted cursor does not pass the record meanwhile", () => {
  it("the transaction fails: the image is dropped, the storage holds nothing of the cursor past the record, and the entries are made good again with the backoff; a restart then loads the keyspace", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/p", "set", SECRET)]);
    const j = (await r.engine.submit(setK("settings/j", SECRET))).mutationId;
    await r.engine.settle();
    const cursorBefore = r.storage.replica(KS)?.head.seq as bigint;
    expect(cursorBefore).toBeGreaterThan(0n);
    r.storage.failNext = 1; // the transaction that carries the REDACTED record and the purge
    const seq = r.server.redact(KS, ["settings/p", "settings/j"]);
    await r.engine.settle();
    // memory is purged at once; the storage still holds what the failed transaction did not replace, but no image that moved past the record
    expect(r.engine.journalEntries().find((x) => x.mutationId === j)?.redacted).toBe(true);
    const img = r.storage.replica(KS);
    expect(img === undefined || img.head.seq < seq).toBe(true);
    // the client backoff passes: the entries are made good again, and the image is gone for good
    await r.clock.advance(60_000);
    await r.engine.settle();
    const st = await r.storage.load();
    expect(JSON.stringify(st, (_k, v: unknown) => (typeof v === "bigint" ? v.toString() : v instanceof Uint8Array ? Buffer.from(v).toString("latin1") : v))).not.toContain(SECRET);
    expect(st.journal.find((x) => x.mutationId === j)?.redacted).toBe(true);
    expect(st.replicas).toEqual([]);
    // a restart finds no image: it cold-loads, and shows the state of the server, which holds nothing of the erased keys
    await r.engine.stop();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 77 });
    expect(r2.engine.confirmed(KS).get("settings/p")).toBeUndefined();
    expect(types(r2).includes("reloaded")).toBe(true);
  });
});

describe("12 Table 12.4, 14 [CLI-77]: what a client does with the answers for a deleted keyspace", () => {
  it("the calls that name the keyspace answer KEYSPACE_DELETED, final and once (a blob call: BlobError); an action cannot be authored into it; the sent action asks GetMutationResult, which answers with no error", async () => {
    const r = await brig();
    const ref = await drive(r, r.engine.uploadBlob(KS, bytesOf(100)));
    const calls = (m: string): number => r.server.calls.filter((c) => c.method === m).length;
    r.server.deleteKeyspace(KS);
    await r.engine.settle();
    expect(r.engine.status().stopped[KS]).toBe("DELETED");
    // 09 [READ-4]: GetBlob, CreateUpload, DeleteBlob (its row says `KEYSPACE_DELETED` when no data is retained)
    const before = [calls("getBlob"), calls("createUpload"), calls("deleteBlob")];
    const blobCalls: (() => Promise<unknown>)[] = [() => r.engine.getBlob(KS, ref.blobId), () => r.engine.uploadBlob(KS, bytesOf(10)), () => r.engine.deleteBlob(KS, ref.blobId, "x")];
    for (const call of blobCalls) {
      const e = await drive(r, call()).catch((x: unknown) => x);
      expect(e).toBeInstanceOf(BlobError);
      expect((e as BlobError).code).toBe("KEYSPACE_DELETED");
    }
    expect([calls("getBlob") - (before[0] as number), calls("createUpload") - (before[1] as number), calls("deleteBlob") - (before[2] as number)]).toEqual([1, 1, 1]);
    // an action is authored into no keyspace that the engine stopped following (14 [CLI-69]); the answer of Mutate would be KEYSPACE_DELETED
    await expect(r.engine.submit(setK("settings/a", 1))).rejects.toBeInstanceOf(LocalRefusal);
    expect(calls("mutate")).toBe(0);
  });

  it("a client that was away when the keyspace was deleted is told by its load: KEYSPACE_DELETED purges the copy as [CLI-77] says, and an action that was never sent is dead-lettered with it", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.changes[0]?.key === "settings/dead" ? rejectResult(m, mk(Code.SCHEMA_INVALID)) : undefined) });
    const dead = (await r.engine.submit(setK("settings/dead", SECRET))).mutationId;
    await r.engine.settle();
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual([dead]);
    await r.engine.stop();
    r.server.deleteKeyspace(KS);
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 51, start: false });
    const queued = (await r2.engine.submit(setK("settings/queued", 1)).catch(() => undefined))?.mutationId; // authored offline into the copy that the restart restored
    r2.engine.start();
    await r2.engine.settle();
    expect(r2.engine.status().stopped[KS]).toBe("DELETED");
    expect(r2.storage.replica(KS)).toBeUndefined();
    // the entry that the app did not acknowledge is handed to it once more (`dead-lettered`) and purged; what the deletion itself writes stays
    expect(ofType(r2, "dead-lettered").some((e) => e.mutationId === dead)).toBe(true);
    expect(r2.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual(queued === undefined ? [] : [queued]);
    if (queued !== undefined) expect(r2.engine.deadLetterEntries()[0]?.reason).toBe("KEYSPACE_DELETED");
  });
});

