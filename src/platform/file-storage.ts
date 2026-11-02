// The outbox storage of a Node process in files (14 §14.3, [CLI-21], [CLI-74]): a directory for each partition, each holding a journal of the
// transactions that the engine committed, one line for each, written whole and flushed to disk before `commit` resolves.
//
//   <root>/<partition>/partition.json   the PartitionKey, for `list`
//   <root>/<partition>/journal.jsonl    one line for each commit: `<check> <packed ops>`, check = the first 8 hex digits of the SHA-256 of the text
//   <root>/<partition>/LOCK             the pid of the process that has the partition open
//
// All or none (14 [CLI-21], [CLI-28]): a commit is one line, written with its end of line in one write. A line that a crash cut has no end of line: it
// ends the journal when it is read, what came before is the state, and the file is cut there so that the next commit does not follow garbage. A line
// that has its end of line and fails its check, does not parse or does not apply is not a crash's: it is damage, and the partition is refused with an
// error that names the line, never read as an empty one (V19 SHOULD-6): the file is left as it is. A line is applied, in memory, by the same
// `MemoryStorage` that the tests use, so the bounds of 14 [CLI-84] and the all-or-none apply behave the same.
//
// The journal is rewritten as one line, the snapshot of the state, when the partition is opened, when it has grown to many times what the state needs, and
// in the commit of every purge (V19 MUST-3): what 14 [CLI-66] and [CLI-76] say a client purges is not on disk after the commit that purges it, not in a line
// of the journal that the purge came after. The snapshot is written to a file of its own, flushed, then renamed over the journal; a snapshot that cannot be
// written leaves the journal as it was, and the storage working.
//
// One process at a time: `LOCK` names its holder, and a second process that opens the partition is refused (the lock of a process that is gone is
// taken over). The lock is made whole, with the pid in it, by a hard link, so that no process reads it half written. Two engines of one process that open
// one key share one storage, as two tabs share one IndexedDB database.

import { createHash, randomBytes } from "node:crypto";
import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import type { BlobStash } from "../engine/blob-store.js";
import { StorageError } from "../engine/errors.js";
import { MemoryStorage, type MemoryStorageOptions } from "../engine/memory-storage.js";
import { partitionId, sameKey, type PartitionKey, type StorageProvider } from "../engine/tabs.js";
import type { OutboxStorage, ReplicaImage, StorageMode, StorageOp, StoredLight, StoredState, StoreName } from "../engine/storage.js";
import { FileBlobStash } from "./file-blob-stash.js";
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
  /** A rewrite of the journal failed at this size: the next is tried when the journal is twice as long. */
  private compactAfter = 0;

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
    let at = 0;
    let lineNo = 0;
    while (at < text.length) {
      const nl = text.indexOf("\n", at);
      if (nl < 0) break; // no end of line: a line that a crash cut, which is not part of the state
      lineNo++;
      const line = text.slice(at, nl);
      const sp = line.indexOf(" ");
      const body = sp < 0 ? "" : line.slice(sp + 1);
      const damaged = (why: string): StorageError =>
        new StorageError(`the journal ${this.journalPath} is damaged at line ${lineNo} (${why}): the partition is not opened, and the file is left as it is`);
      if (sp !== 8 || check(body) !== line.slice(0, 8)) throw damaged("its check does not hold");
      let ops: StorageOp[];
      try {
        ops = parse(body) as StorageOp[];
      } catch {
        throw damaged("it does not parse");
      }
      try {
        await inner.commit(ops);
      } catch {
        throw damaged("it does not apply"); // a commit is written only after it was applied, in memory, to the state that the journal before it gives
      }
      at = nl + 1;
    }
    this.inner = inner;
    Object.assign(this.inner.options, this.bounds);
    const goodBytes = Buffer.byteLength(text.slice(0, at), "utf8");
    if (at < text.length) await fsp.truncate(this.journalPath, goodBytes).catch(() => undefined);
    this.journalBytes = goodBytes;
    try {
      await this.compact(); // also opens the journal for appending
    } catch {
      // the snapshot could not be written: the journal as it is, which holds the state, is appended to
      this.fh ??= await fsp.open(this.journalPath, "a");
    }
  }

  /**
   * Rewrites the journal as one line, the snapshot of the state, through a file of its own that is renamed over it. The journal is not given up until
   * the snapshot is on disk: a snapshot that cannot be written (no space, a file that cannot be replaced, as Windows refuses one that is open) leaves
   * the journal as it was and its handle open, and the error is the caller's (V19 SHOULD-7).
   */
  private async compact(): Promise<void> {
    const ops = snapshotOps(await this.inner.load());
    const body = stringify(ops);
    const line = ops.length === 0 ? "" : `${check(body)} ${body}\n`;
    const tmp = `${this.journalPath}.tmp`;
    try {
      const out = await fsp.open(tmp, "w");
      try {
        await out.write(line);
        await out.sync();
      } finally {
        await out.close();
      }
    } catch (e) {
      await fsp.rm(tmp, { force: true }).catch(() => undefined);
      throw new StorageError(`the snapshot of the journal cannot be written: ${(e as Error).message}`);
    }
    // the append handle is closed for the rename, which some platforms refuse for a file that is open
    const had = this.fh !== undefined;
    await this.fh?.close().catch(() => undefined);
    this.fh = undefined;
    try {
      await fsp.rename(tmp, this.journalPath);
    } catch (e) {
      await fsp.rm(tmp, { force: true }).catch(() => undefined);
      if (had) this.fh = await fsp.open(this.journalPath, "a").catch(() => undefined); // the journal as it was, still the state
      throw new StorageError(`the snapshot of the journal cannot replace it: ${(e as Error).message}`);
    }
    await syncDirectory(this.dir);
    this.journalBytes = Buffer.byteLength(line, "utf8");
    this.snapshotBytes = this.journalBytes;
    this.fh = await fsp.open(this.journalPath, "a");
  }

  load(): Promise<StoredState> {
    return this.inner.load();
  }

  loadLight(): Promise<StoredLight> {
    return this.inner.loadLight();
  }

  loadImage(keyspaceId: string, opts?: { entries?: boolean; tailAfter?: bigint }): Promise<ReplicaImage | undefined> {
    return this.inner.loadImage(keyspaceId, opts);
  }

  full(): StoreName[] {
    return this.inner.full();
  }

  commit(ops: readonly StorageOp[]): Promise<void> {
    const run = this.chain.then(() => this.commitNow(ops));
    this.chain = run.catch(() => undefined);
    return run;
  }

  /** Whether the commit purges: what it removes is not to stay in a line of the journal (14 [CLI-66], [CLI-76]). */
  private purges(ops: readonly StorageOp[]): boolean {
    const held = this.inner.replicaKeyspaces();
    return ops.some((o) => o.op === "purgeAll" || o.op === "deleteReplica" || (o.op === "replicaBegin" && held.includes(o.keyspaceId)));
  }

  private async commitNow(ops: readonly StorageOp[]): Promise<void> {
    if (this.closed || this.fh === undefined) throw new StorageError("the storage is closed");
    // the line is built before memory changes: a commit whose line cannot be built (a value that has no text) is refused, and memory never holds it (V19 SHOULD-7)
    let line: string;
    try {
      const body = stringify(ops);
      line = `${check(body)} ${body}\n`;
    } catch (e) {
      throw new StorageError(`the commit cannot be written: ${(e as Error).message}`);
    }
    const purging = this.purges(ops);
    await this.inner.commit(ops); // the bounds, and all or none, in memory; a refusal here wrote nothing
    try {
      if (purging) {
        // the snapshot of the state after the commit replaces the journal: the rename is the commit, and nothing that the commit removed is left in the file
        await this.compact();
        return;
      }
      await this.fh.write(line);
      await this.fh.sync();
      this.journalBytes += Buffer.byteLength(line, "utf8");
    } catch (e) {
      // what memory holds is not on disk: memory is made what the disk holds, and the commit is refused (14 [CLI-21])
      await this.reload().catch(() => undefined);
      throw new StorageError(`the journal cannot be written: ${(e as Error).message}`);
    }
    if (this.journalBytes > COMPACT_MIN_BYTES && this.journalBytes > COMPACT_FACTOR * Math.max(1, this.snapshotBytes) && this.journalBytes > this.compactAfter) {
      await this.compact().catch(() => {
        this.compactAfter = this.journalBytes * 2; // the storage works on: only the rewrite failed (V19 SHOULD-7)
      });
    }
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

/** Makes the rename durable where the platform can (a directory is not opened for reading on Windows). */
async function syncDirectory(dir: string): Promise<void> {
  try {
    const h = await fsp.open(dir, "r");
    try {
      await h.sync();
    } finally {
      await h.close();
    }
  } catch {
    // not possible here: the file itself is on disk
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

  /**
   * The stash of the bytes that the partition's actions name (09 [READ-94], 14 [CLI-44], [CLI-21]): files in `blobs` under the partition's directory, as durable as its outbox. `destroy` removes it
   * with the partition (14 [CLI-65]). The engine takes it as its default; an app's own stash goes before it.
   */
  blobStash(key: PartitionKey): BlobStash {
    return new FileBlobStash(path.join(this.dirOf(key), "blobs"));
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

const pause = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** The pid that a LOCK names; undefined for a file that names none (empty, or not a number: a lock that is being made, or one that was not made whole). */
async function holderOf(file: string): Promise<number | undefined | null> {
  let text: string;
  try {
    text = await fsp.readFile(file, "utf8");
  } catch (e) {
    return (e as NodeJS.ErrnoException).code === "ENOENT" ? null : undefined; // null: there is none
  }
  const n = /^\s*(\d+)\s*$/.test(text) ? Number.parseInt(text, 10) : Number.NaN;
  return Number.isFinite(n) && n > 0 ? n : undefined;
}

/** Makes the lock file with the pid in it, whole, and nothing else can see it half made: a file of its own, linked into place. False when a lock is there. */
async function createLock(dir: string, file: string, pid: string): Promise<boolean> {
  const tmp = path.join(dir, `LOCK.${process.pid}.${randomBytes(6).toString("hex")}.tmp`);
  await fsp.writeFile(tmp, pid);
  try {
    await fsp.link(tmp, file);
    return true;
  } catch (e) {
    const code = (e as NodeJS.ErrnoException).code;
    if (code === "EEXIST") return false;
    // a file system that has no hard links: the lock is made by its exclusive creation, which a reader can see before the pid is in it (it waits for it)
    try {
      await fsp.writeFile(file, pid, { flag: "wx" });
      return true;
    } catch (e2) {
      if ((e2 as NodeJS.ErrnoException).code === "EEXIST") return false;
      throw new StorageError(`cannot lock the partition: ${(e2 as Error).message}`);
    }
  } finally {
    await fsp.rm(tmp, { force: true }).catch(() => undefined);
  }
}

/**
 * Takes the lock of a holder that is gone. The file is moved aside first, and only the process whose rename succeeds goes on; it looks at what it moved
 * and, when that is not the lock that it judged (another process took the lock over between the read and the rename), puts it back (V19 SHOULD-5).
 */
async function takeOver(dir: string, file: string, judged: number | undefined): Promise<void> {
  const aside = path.join(dir, `LOCK.stale.${process.pid}.${randomBytes(6).toString("hex")}`);
  try {
    await fsp.rename(file, aside);
  } catch {
    return; // another process was first, or the holder released it
  }
  const moved = await holderOf(aside);
  if (moved !== judged && moved !== null && moved !== undefined) {
    // the lock of a live holder that was made after the read: it is put back, or, when another lock stands there now, left to be found
    await fsp.link(aside, file).catch(() => undefined);
  }
  await fsp.rm(aside, { force: true }).catch(() => undefined);
}

/** Takes the directory's `LOCK` for this process; throws a StorageError when a process that is alive has it. Returns the function that frees it. */
async function lockDirectory(dir: string): Promise<() => void> {
  const file = path.join(dir, "LOCK");
  const mine = String(process.pid);
  let unreadable = 0;
  for (let attempt = 0; attempt < 40; attempt++) {
    if (await createLock(dir, file, mine)) {
      const release = (): void => {
        try {
          if (fs.readFileSync(file, "utf8") === mine) fs.unlinkSync(file);
        } catch {
          // already gone
        }
        process.removeListener("exit", release);
      };
      process.once("exit", release);
      return release;
    }
    const holder = await holderOf(file);
    if (holder === null) continue; // released between the two calls
    if (holder === process.pid) throw new StorageError("the partition is open in this process under another provider");
    if (holder !== undefined && alive(holder)) throw new StorageError(`the partition is open in another process (pid ${holder})`);
    if (holder === undefined && unreadable++ < 5) {
      await pause(25); // a lock that names no pid is being made by a process that has not written it yet: it is waited for before it is taken for dead
      continue;
    }
    await takeOver(dir, file, holder);
  }
  throw new StorageError("cannot lock the partition");
}
