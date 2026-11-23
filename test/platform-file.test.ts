// V19 MUST-3, SHOULD-5, SHOULD-6, SHOULD-7 and N5 on the file storage (src/platform/file-storage.ts): what a purge removes is not in the journal after the
// commit that purges it; a line that is damaged refuses the partition instead of emptying it; a snapshot that cannot be written does not close the storage; the
// lock is never read half made; a commit whose line cannot be built is not kept in memory.

import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { create } from "@bufbuild/protobuf";
import { KeyEntrySchema } from "../src/gen/abcp/v2/types_pb.js";
import type { OutboxEntry, ReplicaHead, StorageOp } from "../src/engine/index.js";
import { FileStorage, FileStorageProvider } from "../src/platform/file-storage.js";
import { parse, stringify } from "../src/platform/serialize.js";

let dir: string;
beforeEach(() => {
  dir = mkdtempSync(path.join(tmpdir(), "abcp-file-"));
});
afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

const entry = (id: string, n = 1, size = 4): OutboxEntry => ({
  mutationId: id, keyspaceId: "ks", lane: "SEQUENCED", opType: "Set", content: new Uint8Array(size).fill(n),
  author: { issuer: "i", sub: "s", deviceId: "d" }, ordinal: n, authoredServerMs: 1, dependsOn: [], clientId: "", clientSeq: 0,
  afterClientSeq: 0, everSent: false, sentInEpoch: 0, attempts: 0, registered: false, permissionRetried: false,
  actorMismatchRetried: false, spendResent: false,
});
const head = (seq: bigint): ReplicaHead => ({ logEpoch: 1, seq, watermark: seq, lastRecordSeq: seq, manifestVersion: 1, kind: "team", lifecycle: 1, successors: [], interest: [], lastSyncMs: 5 });
const check = (t: string): string => createHash("sha256").update(t).digest("hex").slice(0, 8);
const secret = new TextEncoder().encode("ERASED-VALUE-1234");
const b64 = Buffer.from(secret).toString("base64");
const kentry = (key: string, value = secret) => create(KeyEntrySchema, { key, encoding: "json/v1", value, version: 1n });
const journal = (d: string): string => readFileSync(path.join(d, "journal.jsonl"), "utf8");
const begin = (ks: string, value = secret) => ({ op: "replicaBegin" as const, keyspaceId: ks, head: head(3n), base: { manifestVersion: 1, logEpoch: 1, checkpointSeq: 3n, entries: [kentry("x/1", value)] } });

describe("V19 MUST-3: a purge leaves nothing of what it removed in the journal (14 [CLI-66], [CLI-76])", () => {
  it("deleteReplica: the pages of the replica are not in the file after the commit, and the state across a restart is right", async () => {
    const d = path.join(dir, "a");
    const s = await FileStorage.open(d);
    await s.commit([begin("ks"), { op: "putEntry", entry: entry("keep") }]);
    await s.commit([{ op: "setMeta", key: "note", value: "kept" }]);
    expect(journal(d)).toContain(b64); // the journal holds it while the replica is held: that is the journal
    await s.commit([{ op: "deleteReplica", keyspaceId: "ks" }]);
    expect(journal(d)).not.toContain(b64);
    expect((await s.load()).replicas).toEqual([]);
    await s.close();
    const again = await FileStorage.open(d);
    const st = await again.load();
    expect(st.replicas).toEqual([]);
    expect(st.entries.map((e) => e.mutationId)).toEqual(["keep"]);
    expect(st.meta).toEqual({ note: "kept" });
    await again.close();
  });

  it("purgeAll: nothing of the partition is in the file", async () => {
    const d = path.join(dir, "b");
    const s = await FileStorage.open(d);
    await s.commit([begin("ks"), { op: "setMeta", key: "note", value: "SIGNED-OUT-USER-DATA" }, { op: "putEntry", entry: entry("mine", 7, 64) }]);
    await s.commit([{ op: "purgeAll" }]);
    const t = journal(d);
    expect(t).not.toContain(b64);
    expect(t).not.toContain("SIGNED-OUT-USER-DATA");
    expect(t).not.toContain('"mine"');
    expect(t).toBe("");
    await s.close();
  });

  it("a replica that a new load replaces: the values of the old image are not in the file", async () => {
    const d = path.join(dir, "c");
    const s = await FileStorage.open(d);
    await s.commit([begin("ks")]);
    await s.commit([begin("ks", new TextEncoder().encode("NEW-IMAGE-VALUE"))]);
    expect(journal(d)).not.toContain(b64);
    expect(journal(d)).toContain(Buffer.from("NEW-IMAGE-VALUE").toString("base64"));
    await s.close();
  });

  it("a commit that purges nothing is appended: the journal is not rewritten for it", async () => {
    const d = path.join(dir, "d");
    const s = await FileStorage.open(d);
    await s.commit([begin("ks")]);
    await s.commit([{ op: "putEntry", entry: entry("a") }]);
    await s.commit([{ op: "putEntry", entry: entry("b") }]);
    expect(journal(d).split("\n").filter(Boolean)).toHaveLength(3);
    await s.close();
  });

  it("a snapshot that cannot be written refuses the purge, and the storage works on with memory as the disk holds it (V19 SHOULD-7)", async () => {
    const d = path.join(dir, "e");
    const s = await FileStorage.open(d);
    await s.commit([begin("ks")]);
    mkdirSync(path.join(d, "journal.jsonl.tmp")); // the snapshot's file cannot be opened for writing (stands for ENOSPC, EPERM, EBUSY)
    await expect(s.commit([{ op: "deleteReplica", keyspaceId: "ks" }])).rejects.toThrow(/journal cannot be written|snapshot/);
    expect((await s.load()).replicas.map((r) => r.keyspaceId)).toEqual(["ks"]); // refused: the replica is still held, in memory as on disk
    await s.commit([{ op: "putEntry", entry: entry("after") }]); // not closed
    expect((await s.load()).entries.map((e) => e.mutationId)).toEqual(["after"]);
    rmSync(path.join(d, "journal.jsonl.tmp"), { recursive: true });
    await s.commit([{ op: "deleteReplica", keyspaceId: "ks" }]); // and the purge goes through when the disk allows it
    expect(journal(d)).not.toContain(b64);
    await s.close();
  });
});

describe("V19 SHOULD-6: damage refuses the partition; only a line that a crash cut is dropped", () => {
  const line = (ops: unknown): string => {
    const b = stringify(ops);
    return `${check(b)} ${b}\n`;
  };
  const put = (id: string, n: number): StorageOp[] => [{ op: "putEntry", entry: entry(id, n) }];

  it("a byte flipped in a line that other lines follow: the partition is not opened and the file is left as it is", async () => {
    const d = path.join(dir, "f");
    const s = await FileStorage.open(d);
    await s.commit(put("a", 1));
    await s.commit(put("b", 2));
    await s.close();
    const j = path.join(d, "journal.jsonl");
    const flipped = journal(d).replace('"mutationId":"a"', '"mutationId":"A"');
    writeFileSync(j, flipped);
    await expect(FileStorage.open(d)).rejects.toThrow(/damaged at line 1 \(its check does not hold\)/);
    expect(readFileSync(j, "utf8")).toBe(flipped);
  });

  it("the same in the last line that has its end of line, and in the only line", async () => {
    const d = path.join(dir, "g");
    const s = await FileStorage.open(d);
    await s.commit(put("a", 1));
    await s.close();
    const j = path.join(d, "journal.jsonl");
    const flipped = journal(d).replace('"mutationId":"a"', '"mutationId":"A"');
    writeFileSync(j, flipped);
    await expect(FileStorage.open(d)).rejects.toThrow(/damaged at line 1/);
    expect(readFileSync(j, "utf8")).toBe(flipped);
  });

  it("a line whose check holds and whose text does not parse, or does not apply, is damage too: the lines after it are not cut away", async () => {
    const d = path.join(dir, "h");
    mkdirSync(d, { recursive: true });
    const j = path.join(d, "journal.jsonl");
    const badTag = '[{"op":"setMeta","key":"k","value":{"$big":"zz"}}]';
    writeFileSync(j, line(put("a", 1)) + `${check(badTag)} ${badTag}\n` + line(put("b", 2)));
    await expect(FileStorage.open(d)).rejects.toThrow(/damaged at line 2 \(it does not parse\)/);
    writeFileSync(j, line(put("a", 1)) + line([{ op: "replicaAppend", keyspaceId: "none", head: head(1n), records: [] }]) + line(put("b", 2)));
    await expect(FileStorage.open(d)).rejects.toThrow(/damaged at line 2 \(it does not apply\)/);
  });

  it("a line that a crash cut (no end of line) is dropped, and the next commit does not follow it", async () => {
    const d = path.join(dir, "i");
    mkdirSync(d, { recursive: true });
    const j = path.join(d, "journal.jsonl");
    writeFileSync(j, line(put("a", 1)) + line(put("b", 2)).slice(0, 40));
    const s = await FileStorage.open(d);
    expect((await s.load()).entries.map((e) => e.mutationId)).toEqual(["a"]);
    await s.commit(put("c", 3));
    await s.close();
    const again = await FileStorage.open(d);
    expect((await again.load()).entries.map((e) => e.mutationId)).toEqual(["a", "c"]);
    await again.close();
  });
});

describe("V19 SHOULD-7: a commit whose line cannot be built is refused and memory never holds it", () => {
  it("a value that has no text: the commit rejects, the state does not have it, a restart does not either", async () => {
    const d = path.join(dir, "j");
    const s = await FileStorage.open(d);
    const e = entry("x") as OutboxEntry & { self?: unknown };
    e.self = e; // stands for any value that `stringify` cannot write
    await expect(s.commit([{ op: "putEntry", entry: e }])).rejects.toThrow(/cannot be written/);
    expect((await s.load()).entries).toEqual([]);
    await s.commit([{ op: "putEntry", entry: entry("y") }]);
    await s.close();
    const again = await FileStorage.open(d);
    expect((await again.load()).entries.map((x) => x.mutationId)).toEqual(["y"]);
    await again.close();
  });
});

describe("V19 SHOULD-5: the lock of a partition is never read half made, and a stale one is taken over by one process only", () => {
  const key = { origin: "o", appId: "a", partition: "p" };
  const lockOf = (root: string): string => path.join(root, readdirSync(root).find((n) => !n.endsWith(".json")) as string, "LOCK");

  it("a lock that names no pid (made by a process that has not written it yet) is waited for, not removed at once; one that stays so is taken over", async () => {
    const root = path.join(dir, "root");
    const first = new FileStorageProvider(root);
    await first.open(key);
    const lock = lockOf(root);
    await first.close();
    writeFileSync(lock, ""); // what the creation by another process leaves between its create and its write
    const t0 = Date.now();
    const second = new FileStorageProvider(root);
    await second.open(key);
    expect(Date.now() - t0).toBeGreaterThanOrEqual(100); // it waited
    expect(readFileSync(lock, "utf8")).toBe(String(process.pid));
    await second.close();
  });

  it("a lock that names a live process is kept, empty or not made by this process: a process that is alive is never taken over", async () => {
    const root = path.join(dir, "root");
    const first = new FileStorageProvider(root);
    await first.open(key);
    const lock = lockOf(root);
    await first.close();
    writeFileSync(lock, String(process.ppid));
    await expect(new FileStorageProvider(root).open(key)).rejects.toThrow(/another process/);
    expect(readFileSync(lock, "utf8")).toBe(String(process.ppid));
  });

  it("two openers of a lock that a dead process left: exactly one takes it", async () => {
    const root = path.join(dir, "root");
    const first = new FileStorageProvider(root);
    await first.open(key);
    const lock = lockOf(root);
    await first.close();
    writeFileSync(lock, "2147483646");
    const providers = [new FileStorageProvider(root), new FileStorageProvider(root), new FileStorageProvider(root)];
    const results = await Promise.allSettled(providers.map((p) => p.open(key)));
    expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
    expect(readFileSync(lock, "utf8")).toBe(String(process.pid));
    expect(readdirSync(path.dirname(lock)).filter((n) => n.startsWith("LOCK."))).toEqual([]); // no temporary or stale file is left
    for (const p of providers) await p.close();
  });
});

describe("N5: the JSON form keeps a member named __proto__", () => {
  it("an own member named __proto__ survives the round trip as one, and sets no prototype", () => {
    const details = JSON.parse('{"__proto__":"x","moved_to":"y"}') as Record<string, string>;
    const back = parse(stringify({ details })) as { details: Record<string, string> };
    expect(Object.keys(back.details)).toEqual(["__proto__", "moved_to"]);
    const hostile = parse('{"mutationId":"m","__proto__":{"everSent":true,"polluted":1}}') as Record<string, unknown>;
    expect(hostile["everSent"]).toBeUndefined();
    expect(({} as Record<string, unknown>)["polluted"]).toBeUndefined();
    expect(Object.keys(hostile)).toEqual(["mutationId", "__proto__"]);
    expect(existsSync(dir)).toBe(true);
  });
});
