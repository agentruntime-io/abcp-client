// The outbox storage of a Node process in files (14 §14.3, [CLI-21], [CLI-74]): a directory for each partition, each holding a journal of the
// transactions that the engine committed, one line for each, written whole and flushed to disk before `commit` resolves.
//
//   <root>/<partition>/partition.json   the PartitionKey, for `list`
//   <root>/<partition>/journal.jsonl    one line for each commit: `<check> <packed ops>`, check = the first 8 hex digits of the SHA-256 of the text
//   <root>/<partition>/LOCK             the pid of the process that has the partition open
//
// All or none (14 [CLI-21], [CLI-28]): a commit is one line. A line that was cut by a crash, or whose check fails, ends the journal when it is
// read: what came before is the state, and the file is cut there so that the next commit does not follow garbage. A line is applied, in memory,
// by the same `MemoryStorage` that the tests use, so the bounds of 14 [CLI-84] and the all-or-none apply behave the same.
//
// The journal is rewritten as one line, the snapshot of the state, when the partition is opened and when it has grown to many times what the
// state needs (written to a file of its own, flushed, then renamed over the journal).
//
// One process at a time: `LOCK` names its holder, and a second process that opens the partition is refused (the lock of a process that is gone is
// taken over). Two engines of one process that open one key share one storage, as two tabs share one IndexedDB database.

import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { StorageError } from "../engine/errors.js";
import { MemoryStorage, type MemoryStorageOptions } from "../engine/memory-storage.js";
import { partitionId, sameKey, type PartitionKey, type StorageProvider } from "../engine/tabs.js";
import type { OutboxStorage, StorageMode, StorageOp, StoredState, StoreName } from "../engine/storage.js";
import { parse, stringify } from "./serialize.js";

const JOURNAL = "journal.jsonl";
/** A journal this much larger than a snapshot of its state is rewritten (and never one under COMPACT_MIN_BYTES). */
const COMPACT_FACTOR = 8;
const COMPACT_MIN_BYTES = 4 * 1024 * 1024;

const check = (text: string): string => createHash("sha256").update(text).digest("hex").slice(0, 8);

/** The operations that rebuild a state: what a snapshot is. */
export function snapshotOps(s: StoredState): StorageOp[] {
  const ops: StorageOp[] = [];
  for (const entry of s.entries) ops.push({ op: "putEntry", entry });
  for (const entry of s.deadLetters) ops.push({ op: "putDeadLetter", entry });
  for (const entry of s.journal) ops.push({ op: "putJournal", entry });
  for (const entry of s.exports) ops.push({ op: "putExport", entry });
  for (const entry of s.rejections) ops.push({ op: "putRejection", entry });
  for (const entry of s.drafts) ops.push({ op: "putDraft", entry });
  for (const img of s.replicas) {
    ops.push({ op: "replicaBegin", keyspaceId: img.keyspaceId, head: img.head, base: img.base });
    if (img.tail.length > 0) ops.push({ op: "replicaAppend", keyspaceId: img.keyspaceId, head: img.head, records: img.tail });
  }
  for (const [key, value] of Object.entries(s.meta)) ops.push({ op: "setMeta", key, value });
  return ops;
}

export class FileStorage implements OutboxStorage {
  readonly mode: StorageMode = "persisted";
  private inner: MemoryStorage;
  private fh: fsp.FileHandle | undefined;
  private chain: Promise<void> = Promise.resolve();
  private journalBytes = 0;
  private snapshotBytes = 0;
  private closed = false;

  private constructor(
    private readonly dir: string,
    private readonly bounds: MemoryStorageOptions,
  ) {
    this.inner = new MemoryStorage({});
  }

  /** Opens the partition directory (made when it does not exist) and reads its journal. */
  static async open(dir: string, bounds: MemoryStorageOptions = {}): Promise<FileStorage> {
    await fsp.mkdir(dir, { recursive: true });
    const s = new FileStorage(dir, bounds);
    await s.recover();
    return s;
  }

  private get journalPath(): string {
    return path.join(this.dir, JOURNAL);
  }

  /** Replays the journal into memory, cuts a torn tail, and writes a snapshot in place of what it replayed. */
  private async recover(): Promise<void> {
    let text = "";
    try {
      text = await fsp.readFile(this.journalPath, "utf8");
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw new StorageError(`cannot read the journal: ${(e as Error).message}`);
    }
    const inner = new MemoryStorage({}); // no bounds while replaying: a bound that was lowered since must not lose what is stored
    let good = 0;
    let at = 0;
    while (at < text.length) {
      const nl = text.indexOf("\n", at);
      if (nl < 0) break; // no end of line: a torn line
      const line = text.slice(at, nl);
      const sp = line.indexOf(" ");
      const body = sp < 0 ? "" : line.slice(sp + 1);
      if (sp !== 8 || check(body) !== line.slice(0, 8)) break;
      let ops: StorageOp[];
      try {
        ops = parse(body) as StorageOp[];
      } catch {
        break;
      }
      try {
        await inner.commit(ops);
      } catch {
        break; // a batch that does not apply is not part of the state (it did not when it was written)
      }
      at = nl + 1;
      good = at;
    }
    this.inner = inner;
    Object.assign(this.inner.options, this.bounds);
    const goodBytes = Buffer.byteLength(text.slice(0, good), "utf8");
    if (good < text.length) await fsp.truncate(this.journalPath, goodBytes).catch(() => undefined);
    this.journalBytes = goodBytes;
    await this.compact(); // also opens the journal for appending
  }

  /** Rewrites the journal as one line, the snapshot of the state, through a file of its own that is renamed over it. */
  private async compact(): Promise<void> {
    await this.fh?.close().catch(() => undefined);
    this.fh = undefined;
    const ops = snapshotOps(await this.inner.load());
    const body = stringify(ops);
    const line = ops.length === 0 ? "" : `${check(body)} ${body}\n`;
    const tmp = `${this.journalPath}.tmp`;
    const out = await fsp.open(tmp, "w");
    try {
      await out.write(line);
      await out.sync();
    } finally {
      await out.close();
    }
    await fsp.rename(tmp, this.journalPath);
    this.journalBytes = Buffer.byteLength(line, "utf8");
    this.snapshotBytes = this.journalBytes;
    this.fh = await fsp.open(this.journalPath, "a");
  }

  load(): Promise<StoredState> {
    return this.inner.load();
  }

  full(): StoreName[] {
    return this.inner.full();
  }

  commit(ops: readonly StorageOp[]): Promise<void> {
    const run = this.chain.then(() => this.commitNow(ops));
    this.chain = run.catch(() => undefined);
    return run;
  }

  private async commitNow(ops: readonly StorageOp[]): Promise<void> {
    if (this.closed || this.fh === undefined) throw new StorageError("the storage is closed");
    await this.inner.commit(ops); // the bounds, and all or none, in memory; a refusal here wrote nothing
    const body = stringify(ops);
    const line = `${check(body)} ${body}\n`;
    try {
      await this.fh.write(line);
      await this.fh.sync();
      this.journalBytes += Buffer.byteLength(line, "utf8");
    } catch (e) {
      // what memory holds is not on disk: memory is made what the disk holds, and the commit is refused (14 [CLI-21])
      await this.reload().catch(() => undefined);
      throw new StorageError(`the journal cannot be written: ${(e as Error).message}`);
    }
    if (this.journalBytes > COMPACT_MIN_BYTES && this.journalBytes > COMPACT_FACTOR * Math.max(1, this.snapshotBytes)) await this.compact().catch(() => undefined);
  }

  private async reload(): Promise<void> {
    await this.fh?.close().catch(() => undefined);
    this.fh = undefined;
    await this.recover();
  }

  async close(): Promise<void> {
    this.closed = true;
    await this.chain;
    await this.fh?.close().catch(() => undefined);
    this.fh = undefined;
  }
}

/** The `StorageProvider` of a directory: a partition of each key, as the layout above says. */
export class FileStorageProvider implements StorageProvider {
  private readonly open_ = new Map<string, { key: PartitionKey; storage: FileStorage; unlock: () => void }>();
  private readonly opening = new Map<string, Promise<FileStorage>>();

  constructor(
    readonly root: string,
    private readonly bounds: MemoryStorageOptions = {},
  ) {}

  private dirOf(key: PartitionKey): string {
    return path.join(this.root, createHash("sha256").update(partitionId(key)).digest("hex").slice(0, 32));
  }

  open(key: PartitionKey): Promise<OutboxStorage> {
    const id = partitionId(key);
    const have = this.open_.get(id);
    if (have !== undefined) return Promise.resolve(have.storage);
    const pending = this.opening.get(id);
    if (pending !== undefined) return pending;
    const p = this.openNow(key, id).finally(() => this.opening.delete(id));
    this.opening.set(id, p);
    return p;
  }

  private async openNow(key: PartitionKey, id: string): Promise<FileStorage> {
    const dir = this.dirOf(key);
    await fsp.mkdir(dir, { recursive: true });
    const unlock = await lockDirectory(dir);
    try {
      await fsp.writeFile(path.join(dir, "partition.json"), JSON.stringify(key));
      const storage = await FileStorage.open(dir, this.bounds);
      this.open_.set(id, { key: { ...key }, storage, unlock });
      return storage;
    } catch (e) {
      unlock();
      throw e;
    }
  }

  async list(origin: string, appId: string): Promise<PartitionKey[]> {
    let names: string[];
    try {
      names = await fsp.readdir(this.root);
    } catch {
      return [];
    }
    const out: PartitionKey[] = [];
    for (const n of names) {
      try {
        const k = JSON.parse(await fsp.readFile(path.join(this.root, n, "partition.json"), "utf8")) as PartitionKey;
        if (k.origin === origin && k.appId === appId) out.push(k);
      } catch {
        // not a partition directory
      }
    }
    return out;
  }

  async destroy(key: PartitionKey): Promise<void> {
    for (const [id, o] of [...this.open_]) {
      if (!sameKey(o.key, key)) continue;
      await o.storage.close();
      o.unlock();
      this.open_.delete(id);
    }
    await fsp.rm(this.dirOf(key), { recursive: true, force: true });
  }

  /** Closes every open partition and frees its lock (the process is done with them). */
  async close(): Promise<void> {
    for (const [id, o] of [...this.open_]) {
      await o.storage.close();
      o.unlock();
      this.open_.delete(id);
    }
  }
}

function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    return (e as NodeJS.ErrnoException).code === "EPERM";
  }
}

/** Takes the directory's `LOCK` for this process; throws a StorageError when a process that is alive has it. Returns the function that frees it. */
async function lockDirectory(dir: string): Promise<() => void> {
  const file = path.join(dir, "LOCK");
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      await fsp.writeFile(file, String(process.pid), { flag: "wx" });
      const release = (): void => {
        try {
          if (fs.readFileSync(file, "utf8") === String(process.pid)) fs.unlinkSync(file);
        } catch {
          // already gone
        }
        process.removeListener("exit", release);
      };
      process.once("exit", release);
      return release;
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code !== "EEXIST") throw new StorageError(`cannot lock the partition: ${(e as Error).message}`);
      const holder = Number.parseInt(await fsp.readFile(file, "utf8").catch(() => ""), 10);
      if (holder === process.pid) throw new StorageError("the partition is open in this process under another provider");
      if (Number.isFinite(holder) && alive(holder)) throw new StorageError(`the partition is open in another process (pid ${holder})`);
      await fsp.rm(file, { force: true }); // its holder is gone: the lock is taken over
    }
  }
  throw new StorageError("cannot lock the partition");
}
