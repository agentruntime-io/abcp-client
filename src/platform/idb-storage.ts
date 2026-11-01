// The outbox storage of a browser in IndexedDB (14 §14.3, [CLI-21], [CLI-74], [CLI-75]), written to the standard API. One database for each partition;
// `commit` is one `readwrite` transaction over the stores that its operations touch, which IndexedDB applies whole or not at all (14 [CLI-21], [CLI-28]).
//
//   entries, deadLetters, journal, exports, rejections   keyed by mutation id
//   drafts                                                keyed by `draftKey`
//   meta                                                  keyed by name
//   replicas        keyspace id -> { head, base: the load's header, entryCount }
//   replicaEntries  `<keyspace>\0<n, 10 digits>` -> a KeyEntry of the load's pages, in the order they were served
//   replicaTail     `<keyspace>\0<seq, 20 digits>` -> a record applied since, in seq order
//
// The registry database `abpc:registry` lists the partitions, for `list` (`indexedDB.databases()` is not in every browser).
//
// Another tab writes to the same database (the tab that is the flusher changes with the handover, 14 [CLI-61]), so nothing that this storage knows of it
// is trusted past the call that read it (V19 MUST-2): a commit reads the headers of the replicas that it touches, and the keys of the rows that it
// removes, from the database just before its transaction, and the bounds of 14 [CLI-84] are judged on the keys of the stores that it grows, read the
// same way; `load` reads the keys that `full` judges on. A purge (14 [CLI-66]) deletes the rows that the database holds, whoever wrote them.
// `mode` is `best-effort` until `navigator.storage.persist()` is granted (14 [CLI-75]).
//
// A connection yields to a request to delete or upgrade the database (`versionchange`): it closes, so that the sign-out of another tab is not kept
// waiting for it (V19 SHOULD-8); a storage whose connection was closed so refuses every call with a StorageError.
//
// Untested against a real IndexedDB (Node has none): `test/platform-idb.test.ts` runs it against a small in-memory double of the API subset that it uses.

import { StorageError } from "../engine/errors.js";
import {
  draftKey,
  MAX_UNREADABLE_LISTED,
  StoreFull,
  type DeadLetterEntry,
  type DraftEntry,
  type ExportEntry,
  type JournalEntry,
  type OutboxEntry,
  type OutboxStorage,
  type RejectionRecord,
  type ReplicaBase,
  type ReplicaHead,
  type ReplicaImage,
  type StorageMode,
  type StorageOp,
  type StoredLight,
  type StoredState,
  type StoreName,
  type UnreadableRow,
} from "../engine/storage.js";
import type { MemoryStorageOptions } from "../engine/memory-storage.js";
import { partitionId, sameKey, type PartitionKey, type StorageProvider } from "../engine/tabs.js";
import type { KeyEntry, Record as RecordMessage } from "../gen/abpc/v2/types_pb.js";

// ---- the part of the IndexedDB API that is used ---------------------------------------------------------------------

export interface IDBRequestLike<T = unknown> {
  result: T;
  error: unknown;
  onsuccess: ((ev?: unknown) => void) | null;
  onerror: ((ev?: unknown) => void) | null;
}
export interface IDBOpenRequestLike extends IDBRequestLike<IDBDatabaseLike> {
  onupgradeneeded: ((ev?: unknown) => void) | null;
  onblocked: ((ev?: unknown) => void) | null;
}
export interface IDBObjectStoreLike {
  put(value: unknown, key: string): IDBRequestLike;
  delete(key: string): IDBRequestLike;
  clear(): IDBRequestLike;
  get(key: string): IDBRequestLike;
  /** `range` is an `IDBKeyRange` (see `KeyRangeFactory`). */
  getAll(range?: unknown): IDBRequestLike<unknown[]>;
  getAllKeys(range?: unknown): IDBRequestLike<string[]>;
}
/** `IDBKeyRange`, for reading the rows of one keyspace without reading the others'. A platform without it reads the keys and filters. */
export interface KeyRangeFactory {
  bound(lower: unknown, upper: unknown): unknown;
}
export interface IDBTransactionLike {
  objectStore(name: string): IDBObjectStoreLike;
  abort(): void;
  error: unknown;
  oncomplete: ((ev?: unknown) => void) | null;
  onerror: ((ev?: unknown) => void) | null;
  onabort: ((ev?: unknown) => void) | null;
}
export interface IDBDatabaseLike {
  /** Another connection asks to delete or upgrade the database. */
  onversionchange?: ((ev?: unknown) => void) | null;
  createObjectStore(name: string): unknown;
  objectStoreNames: { contains(name: string): boolean };
  transaction(names: string[], mode: "readonly" | "readwrite"): IDBTransactionLike;
  close(): void;
}
export interface IDBDeleteRequestLike extends IDBRequestLike {
  /** Another connection keeps the database open: the deletion waits for it. */
  onblocked?: ((ev?: unknown) => void) | null;
}
export interface IDBFactoryLike {
  open(name: string, version?: number): IDBOpenRequestLike;
  deleteDatabase(name: string): IDBDeleteRequestLike;
}
/** `navigator.storage`. */
export interface StorageManagerLike {
  persist(): Promise<boolean>;
  persisted?(): Promise<boolean>;
}

const STORES = ["entries", "deadLetters", "journal", "exports", "rejections", "drafts", "meta", "replicas", "replicaEntries", "replicaTail"] as const;
type StoreId = (typeof STORES)[number];
const REGISTRY = "abpc:registry";

const wait = <T>(r: IDBRequestLike<T>): Promise<T> =>
  new Promise((resolve, reject) => {
    r.onsuccess = (): void => resolve(r.result);
    r.onerror = (): void => reject(r.error instanceof Error ? r.error : new StorageError(`an IndexedDB request failed: ${String(r.error)}`));
  });

function done(tx: IDBTransactionLike): Promise<void> {
  return new Promise((resolve, reject) => {
    tx.oncomplete = (): void => resolve();
    tx.onabort = (): void => reject(new StorageError(`the transaction was aborted: ${tx.error instanceof Error ? tx.error.message : String(tx.error ?? "")}`));
    tx.onerror = (): void => reject(new StorageError(`the transaction failed: ${tx.error instanceof Error ? tx.error.message : String(tx.error ?? "")}`));
  });
}

async function openDb(factory: IDBFactoryLike, name: string, stores: readonly string[], onClosed?: () => void): Promise<IDBDatabaseLike> {
  const req = factory.open(name, 1);
  req.onupgradeneeded = (): void => {
    const db = req.result;
    for (const s of stores) if (!db.objectStoreNames.contains(s)) db.createObjectStore(s);
  };
  const db = await wait(req);
  // another tab deletes the database (a sign-out, 14 [CLI-65]) or upgrades it: this connection closes at once, or that tab waits for it for ever
  db.onversionchange = (): void => {
    try {
      db.close();
    } catch {
      // closed already
    }
    onClosed?.();
  };
  return db;
}

const pad = (n: bigint | number, width: number): string => n.toString().padStart(width, "0");
const rowKey = (keyspace: string, n: string): string => `${keyspace}\u0000${n}`;
const keyspaceOf = (row: string): string => row.slice(0, row.indexOf("\u0000"));

interface ReplicaHeader {
  head: ReplicaHead;
  base: Omit<ReplicaBase, "entries">;
  entryCount: number;
}

type Write = { store: StoreId; key: string; value: unknown } | { store: StoreId; key: string; delete: true } | { store: StoreId; clear: true };

type Bounded = "entries" | "deadLetters" | "journal" | "exports";

const BOUNDED: { name: StoreName; store: Bounded; opts: "maxOutbox" | "maxDeadLetters" | "maxJournal" | "maxExports" }[] = [
  { name: "outbox", store: "entries", opts: "maxOutbox" },
  { name: "deadLetters", store: "deadLetters", opts: "maxDeadLetters" },
  { name: "journal", store: "journal", opts: "maxJournal" },
  { name: "exports", store: "exports", opts: "maxExports" },
];

// ---- rows that are not what this storage wrote (V19 N5) -------------------------------------------------------------
//
// A row that was damaged, or written by another build in another shape, used to make every read throw, so that the partition never opened again. Each row
// is checked for what the engine reads of it; one that fails is left out of what the read returns and listed in `unreadable` (14 [CLI-29]: "one bad entry MUST
// NOT stop the flush of others"). The checks are of the shape, not of the meaning: the engine judges the content of an action as it always has.

/** The keys and the rows of one store, in key order. */
type Rows = [string[], unknown[]];

/** Why a row is not what the storage wrote, or undefined when it is. */
type Check = (key: string, row: unknown) => string | undefined;

const isObject = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
/** Bytes, whichever realm made them (a worker, a frame): `instanceof` would call a row of another realm malformed. */
const isBytes = (v: unknown): v is Uint8Array => Object.prototype.toString.call(v) === "[object Uint8Array]";

/** The rows of the stores that hold an action: the id is the key, and the engine reads the keyspace, the op type, the content and the author. */
const actionRow: Check = (key, row) => {
  if (!isObject(row)) return "the row is not an object";
  if (row["mutationId"] !== key) return "its mutation id is not its key";
  if (typeof row["keyspaceId"] !== "string" || typeof row["opType"] !== "string") return "it names no keyspace or no op type";
  if (!isBytes(row["content"])) return "its content is not bytes";
  if (row["author"] !== undefined && !isObject(row["author"])) return "its author is not an object";
  return undefined;
};

const entryRow: Check = (key, row) => {
  const why = actionRow(key, row);
  if (why !== undefined) return why;
  const r = row as Record<string, unknown>;
  if (typeof r["ordinal"] !== "number" || !Number.isFinite(r["ordinal"])) return "its ordinal is not a number";
  if (r["lane"] !== "SEQUENCED" && r["lane"] !== "GATED") return "its lane is not one";
  if (!Array.isArray(r["dependsOn"])) return "its dependencies are not a list";
  return undefined;
};

const draftRow: Check = (key, row) => {
  if (!isObject(row)) return "the row is not an object";
  if (typeof row["keyspaceId"] !== "string" || typeof row["key"] !== "string") return "it names no keyspace or no key";
  if (draftKey(row as unknown as DraftEntry) !== key) return "its key is not the key of its draft";
  return undefined;
};

const replicaHeaderRow: Check = (_key, row) => {
  if (!isObject(row)) return "the row is not an object";
  const head = row["head"];
  const base = row["base"];
  if (!isObject(head) || typeof head["seq"] !== "bigint" || !Array.isArray(head["interest"])) return "its head is not a replica head";
  if (!isObject(base) || typeof base["checkpointSeq"] !== "bigint") return "its base is not a replica base";
  if (typeof row["entryCount"] !== "number") return "it holds no count of its pages";
  return undefined;
};

const pageRow: Check = (_key, row) => (isObject(row) && typeof row["key"] === "string" ? undefined : "the row is not a key entry");
const tailRow: Check = (_key, row) => (isObject(row) && typeof row["seq"] === "bigint" ? undefined : "the row is not a record");

/** The rows that a read left out, as many as are listed (`MAX_UNREADABLE_LISTED`). */
class Unreadable {
  readonly rows: UnreadableRow[] = [];
  add(store: string, key: string, why: string): void {
    if (this.rows.length < MAX_UNREADABLE_LISTED) this.rows.push({ store, key, why });
  }
}

/** The rows of a store that pass `check`, with their keys; `onBad` hears the key of each that does not. */
function good<T>(store: StoreId, [keys, rows]: Rows, check: Check, into: Unreadable, onBad?: (key: string) => void): [string, T][] {
  const out: [string, T][] = [];
  keys.forEach((k, i) => {
    const why = check(k, rows[i]);
    if (why === undefined) out.push([k, rows[i] as T]);
    else {
      into.add(store, k, why);
      onBad?.(k);
    }
  });
  return out;
}

const values = <T>(rows: [string, T][]): T[] => rows.map(([, v]) => v);

/** The replica headers that are whole, by keyspace. */
function readHeaders(rows: Rows, into: Unreadable): Map<string, ReplicaHeader> {
  return new Map(good<ReplicaHeader>("replicas", rows, replicaHeaderRow, into));
}

/** The meta rows whose value is a scalar. */
function metaOf(rows: Rows, into: Unreadable): Record<string, string | number | bigint> {
  const meta: Record<string, string | number | bigint> = {};
  rows[0].forEach((k, i) => {
    const v = rows[1][i];
    if (typeof v === "string" || typeof v === "number" || typeof v === "bigint") meta[k] = v;
    else into.add("meta", k, "its value is not a string, a number or a bigint");
  });
  return meta;
}

/** What a commit reads of the database before it writes. */
interface Fresh {
  headers: Map<string, ReplicaHeader | undefined>;
  entryKeys: Map<string, string[]>;
  tailKeys: Map<string, string[]>;
  ids: Partial<Record<Bounded, Set<string>>>;
}

export class IndexedDbStorage implements OutboxStorage {
  mode: StorageMode = "best-effort";
  /** The ids of the bounded stores, as of the last read of them (`load`, or a commit that grows one): the bounds are judged on them (14 [CLI-84]). */
  private ids: Record<Bounded, Set<string>> = { entries: new Set(), deadLetters: new Set(), journal: new Set(), exports: new Set() };
  private chain: Promise<void> = Promise.resolve();
  /** The connection was closed by another tab's request to delete or upgrade the database. */
  private closed = false;

  private constructor(
    private readonly db: IDBDatabaseLike,
    private readonly bounds: MemoryStorageOptions,
    private readonly manager: StorageManagerLike | undefined,
    private readonly range: KeyRangeFactory | undefined,
  ) {}

  static async open(factory: IDBFactoryLike, name: string, bounds: MemoryStorageOptions = {}, manager?: StorageManagerLike, range?: KeyRangeFactory): Promise<IndexedDbStorage> {
    let self: IndexedDbStorage | undefined;
    const db = await openDb(factory, name, STORES, () => {
      if (self !== undefined) self.closed = true;
    });
    const s = new IndexedDbStorage(db, bounds, manager, range);
    self = s;
    await s.readIds();
    if (manager?.persisted !== undefined && (await manager.persisted().catch(() => false))) s.mode = "persisted";
    return s;
  }

  /** Whether another tab's request to delete or upgrade the database closed this connection. */
  get isClosed(): boolean {
    return this.closed;
  }

  private connection(): IDBDatabaseLike {
    if (this.closed) throw new StorageError("the database is closed (another tab deleted or upgraded it, or this storage was closed)");
    return this.db;
  }

  /** The keys of the bounded stores, from the database now. */
  private async readIds(): Promise<void> {
    const names: Bounded[] = ["entries", "deadLetters", "journal", "exports"];
    const tx = this.connection().transaction(names, "readonly");
    const finished = done(tx);
    const reads = names.map((n) => wait(tx.objectStore(n).getAllKeys()));
    void Promise.allSettled(reads); // a failed transaction fails its requests too: `finished` reports it
    await finished;
    for (const [i, n] of names.entries()) this.ids[n] = new Set(await (reads[i] as Promise<string[]>));
  }

  /** The key range of the rows of one keyspace in `replicaEntries` and `replicaTail`: `<keyspace>\0` up to `<keyspace>\1`. */
  private rangeOf(keyspace: string): unknown {
    return this.range?.bound(`${keyspace}\u0000`, `${keyspace}\u0001`);
  }

  /** The keys of a store of rows that belong to a keyspace (a read that a key range narrowed holds no other). */
  private ownKeys(keys: string[], keyspace: string): string[] {
    return this.range === undefined ? keys.filter((k) => keyspaceOf(k) === keyspace) : keys;
  }

  async load(): Promise<StoredState> {
    await this.chain;
    const tx = this.connection().transaction([...STORES], "readonly");
    const finished = done(tx); // listened to from the start: the transaction can complete before the requests' promises are awaited
    // every request is made at once, while the transaction is active; the rows of a store are read with their keys, which name a row that is not one
    const rowsOf = (s: StoreId): Promise<Rows> => Promise.all([wait(tx.objectStore(s).getAllKeys()), wait(tx.objectStore(s).getAll())]);
    const [entries, deadLetters, journal, exports, rejections, drafts, meta, headers, pages, tail] = await Promise.all([
      rowsOf("entries"),
      rowsOf("deadLetters"),
      rowsOf("journal"),
      rowsOf("exports"),
      rowsOf("rejections"),
      rowsOf("drafts"),
      rowsOf("meta"),
      rowsOf("replicas"),
      rowsOf("replicaEntries"),
      rowsOf("replicaTail"),
    ]);
    await finished;
    // what this read saw is what the bounds are judged on from here: a tab that became the flusher reads before it writes (V19 MUST-2); a row that is
    // left out below is still a row of the database
    this.ids = { entries: new Set(entries[0]), deadLetters: new Set(deadLetters[0]), journal: new Set(journal[0]), exports: new Set(exports[0]) };
    const unreadable = new Unreadable();
    const images = new Map<string, ReplicaImage>();
    const headerOf = readHeaders(headers, unreadable);
    // V19 N5: a page or a tail row of a keyspace that is not one leaves the image out whole: an image with a gap would show a wrong confirmed state
    const damaged = new Set<string>();
    const pageRows = good<KeyEntry>("replicaEntries", pages, pageRow, unreadable, (k) => damaged.add(keyspaceOf(k)));
    const tailRows = good<RecordMessage>("replicaTail", tail, tailRow, unreadable, (k) => damaged.add(keyspaceOf(k)));
    for (const [ks, h] of headerOf) {
      if (damaged.has(ks)) unreadable.add("replicas", ks, "a row of its pages or its tail is unreadable, so its image is not used");
      else images.set(ks, { keyspaceId: ks, head: h.head, base: { ...h.base, entries: [] }, tail: [] });
    }
    for (const [k, e] of pageRows) images.get(keyspaceOf(k))?.base.entries.push(e);
    for (const [k, r] of tailRows) images.get(keyspaceOf(k))?.tail.push(r);
    const state: StoredState = {
      entries: values(good<OutboxEntry>("entries", entries, entryRow, unreadable)),
      deadLetters: values(good<DeadLetterEntry>("deadLetters", deadLetters, actionRow, unreadable)),
      journal: values(good<JournalEntry>("journal", journal, actionRow, unreadable)),
      exports: values(good<ExportEntry>("exports", exports, actionRow, unreadable)),
      rejections: values(good<RejectionRecord>("rejections", rejections, actionRow, unreadable)),
      drafts: values(good<DraftEntry>("drafts", drafts, draftRow, unreadable)),
      replicas: [...images.values()],
      meta: metaOf(meta, unreadable),
    };
    if (unreadable.rows.length > 0) state.unreadable = unreadable.rows;
    return state;
  }

  async loadLight(): Promise<StoredLight> {
    await this.chain;
    const names: StoreId[] = ["entries", "deadLetters", "journal", "exports", "rejections", "drafts", "meta", "replicas"];
    const tx = this.connection().transaction(names, "readonly");
    const finished = done(tx);
    const rowsOf = (s: StoreId): Promise<Rows> => Promise.all([wait(tx.objectStore(s).getAllKeys()), wait(tx.objectStore(s).getAll())]);
    const [entries, deadLetters, journal, exports, rejections, drafts, meta, headers] = await Promise.all(names.map(rowsOf));
    await finished;
    const unreadable = new Unreadable();
    const light: StoredLight = {
      entries: values(good<OutboxEntry>("entries", entries as Rows, entryRow, unreadable)),
      deadLetters: values(good<DeadLetterEntry>("deadLetters", deadLetters as Rows, actionRow, unreadable)),
      journal: values(good<JournalEntry>("journal", journal as Rows, actionRow, unreadable)),
      exports: values(good<ExportEntry>("exports", exports as Rows, actionRow, unreadable)),
      rejections: values(good<RejectionRecord>("rejections", rejections as Rows, actionRow, unreadable)),
      drafts: values(good<DraftEntry>("drafts", drafts as Rows, draftRow, unreadable)),
      replicas: [...readHeaders(headers as Rows, unreadable)].map(([keyspaceId, h]) => ({ keyspaceId, head: h.head, base: h.base, entryCount: h.entryCount })),
      meta: metaOf(meta as Rows, unreadable),
    };
    if (unreadable.rows.length > 0) light.unreadable = unreadable.rows;
    return light;
  }

  /** The image of one keyspace; undefined when there is none, and when a row of it is not readable (`load` lists the rows, V19 N5). */
  async loadImage(keyspaceId: string, opts: { entries?: boolean; tailAfter?: bigint } = {}): Promise<ReplicaImage | undefined> {
    await this.chain;
    const tx = this.connection().transaction(["replicas", "replicaEntries", "replicaTail"], "readonly");
    const finished = done(tx);
    const withEntries = opts.entries !== false;
    const range = this.rangeOf(keyspaceId);
    const header = wait(tx.objectStore("replicas").get(keyspaceId));
    // without a key range the rows of every keyspace come back, and their keys tell them apart
    const entryKeys = withEntries ? wait(tx.objectStore("replicaEntries").getAllKeys(range)) : Promise.resolve([] as string[]);
    const entryRows = withEntries ? wait(tx.objectStore("replicaEntries").getAll(range)) : Promise.resolve([] as unknown[]);
    const tailKeys = wait(tx.objectStore("replicaTail").getAllKeys(range));
    const tailRows = wait(tx.objectStore("replicaTail").getAll(range));
    void Promise.allSettled([header, entryKeys, entryRows, tailKeys, tailRows]);
    await finished;
    const h = (await header) as unknown;
    if (h === undefined) return undefined;
    const unreadable = new Unreadable();
    if (replicaHeaderRow(keyspaceId, h) !== undefined) return undefined;
    const mine = (keys: string[], rows: unknown[]): Rows => {
      if (this.range !== undefined) return [keys, rows];
      const idx = keys.map((_, i) => i).filter((i) => keyspaceOf(keys[i] as string) === keyspaceId);
      return [idx.map((i) => keys[i] as string), idx.map((i) => rows[i])];
    };
    const pages = good<KeyEntry>("replicaEntries", mine(await entryKeys, await entryRows), pageRow, unreadable);
    const tail = good<RecordMessage>("replicaTail", mine(await tailKeys, await tailRows), tailRow, unreadable);
    if (unreadable.rows.length > 0) return undefined;
    const header_ = h as ReplicaHeader;
    const after = opts.tailAfter;
    const records = values(tail);
    return {
      keyspaceId,
      head: header_.head,
      base: { ...header_.base, entries: withEntries ? values(pages) : [] },
      tail: after === undefined ? records : records.filter((r) => r.seq > after),
    };
  }

  full(): StoreName[] {
    const out: StoreName[] = [];
    for (const b of BOUNDED) {
      const max = this.bounds[b.opts];
      if (max !== undefined && this.ids[b.store].size >= max) out.push(b.name);
    }
    return out;
  }

  async requestPersistence(): Promise<boolean> {
    if (this.manager === undefined) return this.mode === "persisted";
    try {
      if (await this.manager.persist()) this.mode = "persisted";
    } catch {
      // not granted
    }
    return this.mode === "persisted";
  }

  commit(ops: readonly StorageOp[]): Promise<void> {
    const run = this.chain.then(() => this.commitNow(ops));
    this.chain = run.catch(() => undefined);
    return run;
  }

  /**
   * What the operations need of the database, read in one transaction before the one that writes: the headers of the replicas that they read or
   * write, the keys of the rows that they remove, and the keys of the bounded stores that they grow when a bound is set.
   */
  private async readFresh(touched: ReadonlySet<string>, drops: ReadonlySet<string>, grows: ReadonlySet<Bounded>): Promise<Fresh> {
    const fresh: Fresh = { headers: new Map(), entryKeys: new Map(), tailKeys: new Map(), ids: {} };
    if (touched.size === 0 && drops.size === 0 && grows.size === 0) return fresh;
    const names: StoreId[] = [...grows];
    if (touched.size > 0 || drops.size > 0) names.push("replicas");
    if (drops.size > 0) names.push("replicaEntries", "replicaTail");
    const tx = this.connection().transaction(names, "readonly");
    const finished = done(tx);
    const reads: Promise<void>[] = [];
    for (const ks of touched) reads.push(wait(tx.objectStore("replicas").get(ks)).then((h) => void fresh.headers.set(ks, h as ReplicaHeader | undefined)));
    for (const ks of drops) {
      const range = this.rangeOf(ks);
      reads.push(wait(tx.objectStore("replicaEntries").getAllKeys(range)).then((k) => void fresh.entryKeys.set(ks, this.ownKeys(k, ks))));
      reads.push(wait(tx.objectStore("replicaTail").getAllKeys(range)).then((k) => void fresh.tailKeys.set(ks, this.ownKeys(k, ks))));
    }
    for (const g of grows) reads.push(wait(tx.objectStore(g).getAllKeys()).then((k) => void (fresh.ids[g] = new Set(k))));
    void Promise.allSettled(reads);
    await finished;
    await Promise.all(reads);
    return fresh;
  }

  private async commitNow(ops: readonly StorageOp[]): Promise<void> {
    // what the commit needs to know of the database (V19 MUST-2): another tab wrote since this one read, so none of it is kept from an earlier read
    const touched = new Set<string>();
    const drops = new Set<string>();
    const grows = new Set<Bounded>();
    for (const op of ops) {
      switch (op.op) {
        case "putEntry":
          if (this.bounds.maxOutbox !== undefined) grows.add("entries");
          break;
        case "putDeadLetter":
          if (this.bounds.maxDeadLetters !== undefined) grows.add("deadLetters");
          break;
        case "putJournal":
          if (this.bounds.maxJournal !== undefined) grows.add("journal");
          break;
        case "putExport":
          if (this.bounds.maxExports !== undefined) grows.add("exports");
          break;
        case "replicaBegin":
        case "deleteReplica":
          touched.add(op.keyspaceId);
          drops.add(op.keyspaceId);
          break;
        case "replicaAppend":
        case "replicaHead":
        case "replicaExtend":
          touched.add(op.keyspaceId);
          break;
        default:
          break;
      }
    }
    const fresh = await this.readFresh(touched, drops, grows);
    const baseline: Record<Bounded, Set<string>> = { entries: fresh.ids.entries ?? this.ids.entries, deadLetters: fresh.ids.deadLetters ?? this.ids.deadLetters, journal: fresh.ids.journal ?? this.ids.journal, exports: fresh.ids.exports ?? this.ids.exports };
    const ids: Record<Bounded, Set<string>> = { entries: new Set(baseline.entries), deadLetters: new Set(baseline.deadLetters), journal: new Set(baseline.journal), exports: new Set(baseline.exports) };
    const headers = new Map(fresh.headers);
    const writes: Write[] = [];
    /** The rows that this commit put for a keyspace: a later drop of the keyspace in the same commit removes them too. */
    const putRows = new Map<string, { store: StoreId; key: string }[]>();
    const put = (store: StoreId, key: string, value: unknown): void => void writes.push({ store, key, value });
    const del = (store: StoreId, key: string): void => void writes.push({ store, key, delete: true });
    const putRow = (store: "replicaEntries" | "replicaTail", ks: string, key: string, value: unknown): void => {
      put(store, key, value);
      const l = putRows.get(ks) ?? [];
      l.push({ store, key });
      putRows.set(ks, l);
    };
    const dropReplica = (ks: string): void => {
      for (const k of fresh.entryKeys.get(ks) ?? []) del("replicaEntries", k);
      for (const k of fresh.tailKeys.get(ks) ?? []) del("replicaTail", k);
      for (const r of putRows.get(ks) ?? []) del(r.store, r.key);
      putRows.delete(ks);
      del("replicas", ks);
      headers.set(ks, undefined);
    };
    for (const op of ops) {
      switch (op.op) {
        case "putEntry":
          ids.entries.add(op.entry.mutationId);
          put("entries", op.entry.mutationId, op.entry);
          break;
        case "deleteEntry":
          ids.entries.delete(op.mutationId);
          del("entries", op.mutationId);
          break;
        case "putDeadLetter":
          ids.deadLetters.add(op.entry.mutationId);
          put("deadLetters", op.entry.mutationId, op.entry);
          break;
        case "ackDeadLetter":
          ids.deadLetters.delete(op.mutationId);
          del("deadLetters", op.mutationId);
          break;
        case "putJournal":
          ids.journal.add(op.entry.mutationId);
          put("journal", op.entry.mutationId, op.entry);
          break;
        case "deleteJournal":
          ids.journal.delete(op.mutationId);
          del("journal", op.mutationId);
          break;
        case "putExport":
          ids.exports.add(op.entry.mutationId);
          put("exports", op.entry.mutationId, op.entry);
          break;
        case "ackExport":
          ids.exports.delete(op.mutationId);
          del("exports", op.mutationId);
          break;
        case "putRejection":
          put("rejections", op.entry.mutationId, op.entry);
          break;
        case "ackRejection":
          del("rejections", op.mutationId);
          break;
        case "putDraft":
          put("drafts", draftKey(op.entry), op.entry);
          break;
        case "deleteDraft":
          del("drafts", draftKey(op));
          break;
        case "setMeta":
          put("meta", op.key, op.value);
          break;
        case "replicaBegin": {
          dropReplica(op.keyspaceId);
          const h: ReplicaHeader = { head: op.head, base: { manifestVersion: op.base.manifestVersion, logEpoch: op.base.logEpoch, checkpointSeq: op.base.checkpointSeq }, entryCount: op.base.entries.length };
          op.base.entries.forEach((e, i) => putRow("replicaEntries", op.keyspaceId, rowKey(op.keyspaceId, pad(i, 10)), e));
          put("replicas", op.keyspaceId, h);
          headers.set(op.keyspaceId, h);
          break;
        }
        case "replicaAppend": {
          const h = headers.get(op.keyspaceId);
          if (h === undefined) throw new StorageError(`no replica image of ${op.keyspaceId} to append to`);
          const next: ReplicaHeader = { ...h, head: op.head };
          for (const r of op.records) putRow("replicaTail", op.keyspaceId, rowKey(op.keyspaceId, pad(r.seq, 20)), r);
          put("replicas", op.keyspaceId, next);
          headers.set(op.keyspaceId, next);
          break;
        }
        case "replicaHead": {
          const h = headers.get(op.keyspaceId);
          if (h === undefined) break;
          const next: ReplicaHeader = { ...h, head: op.head };
          put("replicas", op.keyspaceId, next);
          headers.set(op.keyspaceId, next);
          break;
        }
        case "replicaExtend": {
          const h = headers.get(op.keyspaceId);
          if (h === undefined) throw new StorageError(`no replica image of ${op.keyspaceId} to extend`);
          let count = h.entryCount;
          for (const e of op.entries) putRow("replicaEntries", op.keyspaceId, rowKey(op.keyspaceId, pad(count++, 10)), e);
          const next: ReplicaHeader = { ...h, head: op.head, entryCount: count };
          put("replicas", op.keyspaceId, next);
          headers.set(op.keyspaceId, next);
          break;
        }
        case "deleteReplica":
          dropReplica(op.keyspaceId);
          break;
        case "purgeAll":
          writes.length = 0;
          for (const s of STORES) writes.push({ store: s, clear: true });
          for (const k of Object.keys(ids)) ids[k as Bounded].clear();
          headers.clear();
          putRows.clear();
          break;
      }
    }
    // 14 [CLI-84]: a commit that would grow a full store beyond its bound is refused whole
    for (const b of BOUNDED) {
      const max = this.bounds[b.opts];
      if (max !== undefined && ids[b.store].size > baseline[b.store].size && ids[b.store].size > max) throw new StoreFull(b.name);
    }
    if (writes.length > 0) {
      const names = [...new Set(writes.map((w) => w.store))];
      let tx: IDBTransactionLike;
      try {
        tx = this.connection().transaction(names, "readwrite");
      } catch (e) {
        throw new StorageError(`cannot open a transaction: ${e instanceof Error ? e.message : String(e)}`);
      }
      const finished = done(tx);
      try {
        for (const w of writes) {
          const store = tx.objectStore(w.store);
          if ("clear" in w) store.clear();
          else if ("delete" in w) store.delete(w.key);
          else store.put(w.value, w.key);
        }
      } catch (e) {
        try {
          tx.abort();
        } catch {
          // already finished
        }
        await finished.catch(() => undefined);
        throw new StorageError(`a write failed: ${e instanceof Error ? e.message : String(e)}`);
      }
      await finished;
    }
    this.ids = ids;
  }

  close(): void {
    this.closed = true;
    this.db.close();
  }
}

/** What `IndexedDbStorageProvider` takes besides the factory and the bounds. */
export interface IndexedDbOptions {
  /** `IDBKeyRange`: the rows of one keyspace are read and removed by range. */
  keyRange?: KeyRangeFactory;
  /** How long `destroy` waits for another tab that keeps the database open before it fails; default 10 s (real time). */
  blockedWaitMs?: number;
}

/** The `StorageProvider` of a browser: a database for each partition, and the registry of them. */
export class IndexedDbStorageProvider implements StorageProvider {
  private readonly open_ = new Map<string, { key: PartitionKey; storage: IndexedDbStorage; name: string }>();
  private readonly opening = new Map<string, Promise<IndexedDbStorage>>();
  private registry: Promise<IDBDatabaseLike> | undefined;

  constructor(
    private readonly factory: IDBFactoryLike,
    private readonly bounds: MemoryStorageOptions = {},
    private readonly manager?: StorageManagerLike,
    private readonly extra: IndexedDbOptions = {},
  ) {}

  private reg(): Promise<IDBDatabaseLike> {
    this.registry ??= openDb(this.factory, REGISTRY, ["partitions"], () => {
      this.registry = undefined; // closed for another tab's request: the next use opens it again
    });
    return this.registry;
  }

  private nameOf(key: PartitionKey): string {
    return `abpc:${partitionId(key)}`;
  }

  open(key: PartitionKey): Promise<OutboxStorage> {
    const id = partitionId(key);
    const have = this.open_.get(id);
    if (have !== undefined && !have.storage.isClosed) return Promise.resolve(have.storage);
    const pending = this.opening.get(id);
    if (pending !== undefined) return pending;
    const p = (async (): Promise<IndexedDbStorage> => {
      const name = this.nameOf(key);
      const storage = await IndexedDbStorage.open(this.factory, name, this.bounds, this.manager, this.extra.keyRange);
      const rdb = await this.reg();
      const tx = rdb.transaction(["partitions"], "readwrite");
      tx.objectStore("partitions").put({ ...key }, id);
      await done(tx);
      this.open_.set(id, { key: { ...key }, storage, name });
      return storage;
    })().finally(() => this.opening.delete(id));
    this.opening.set(id, p);
    return p;
  }

  async list(origin: string, appId: string): Promise<PartitionKey[]> {
    const rdb = await this.reg();
    const tx = rdb.transaction(["partitions"], "readonly");
    const finished = done(tx);
    const rows = (await wait(tx.objectStore("partitions").getAll())) as PartitionKey[];
    await finished;
    return rows.filter((k) => k.origin === origin && k.appId === appId);
  }

  /** Deletes the database; another tab that keeps it open is asked to close it (`versionchange`), and waited for, for a while. */
  private deleteDatabase(name: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const req = this.factory.deleteDatabase(name);
      let timer: ReturnType<typeof setTimeout> | undefined;
      req.onsuccess = (): void => {
        if (timer !== undefined) clearTimeout(timer);
        resolve();
      };
      req.onerror = (): void => {
        if (timer !== undefined) clearTimeout(timer);
        reject(req.error instanceof Error ? req.error : new StorageError(`the database cannot be deleted: ${String(req.error)}`));
      };
      req.onblocked = (): void => {
        // a connection that does not close on `versionchange` (a tab of another build) keeps the data: the sign-out is told, never left waiting
        timer ??= setTimeout(() => reject(new StorageError(`another tab keeps the database ${name} open: it is deleted when that tab closes it`)), this.extra.blockedWaitMs ?? 10_000);
      };
    });
  }

  async destroy(key: PartitionKey): Promise<void> {
    for (const [id, o] of [...this.open_]) {
      if (!sameKey(o.key, key)) continue;
      if (!o.storage.isClosed) o.storage.close();
      this.open_.delete(id);
    }
    await this.deleteDatabase(this.nameOf(key));
    const rdb = await this.reg();
    const tx = rdb.transaction(["partitions"], "readwrite");
    tx.objectStore("partitions").delete(partitionId(key));
    await done(tx);
  }
}
