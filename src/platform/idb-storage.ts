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
// The bounds of 14 [CLI-84] are judged before the transaction from the ids that the storage holds in memory (read once at open), so a commit that
// would grow a full store is refused without a write. `mode` is `best-effort` until `navigator.storage.persist()` is granted (14 [CLI-75]).
//
// Untested against a real IndexedDB (Node has none): `test/platform-idb.test.ts` runs it against a small in-memory double of the API subset that it uses.

import { StorageError } from "../engine/errors.js";
import {
  draftKey,
  StoreFull,
  type OutboxStorage,
  type ReplicaBase,
  type ReplicaHead,
  type ReplicaImage,
  type StorageMode,
  type StorageOp,
  type StoredState,
  type StoreName,
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
  getAll(): IDBRequestLike<unknown[]>;
  getAllKeys(): IDBRequestLike<string[]>;
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
  createObjectStore(name: string): unknown;
  objectStoreNames: { contains(name: string): boolean };
  transaction(names: string[], mode: "readonly" | "readwrite"): IDBTransactionLike;
  close(): void;
}
export interface IDBFactoryLike {
  open(name: string, version?: number): IDBOpenRequestLike;
  deleteDatabase(name: string): IDBRequestLike;
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

async function openDb(factory: IDBFactoryLike, name: string, stores: readonly string[]): Promise<IDBDatabaseLike> {
  const req = factory.open(name, 1);
  req.onupgradeneeded = (): void => {
    const db = req.result;
    for (const s of stores) if (!db.objectStoreNames.contains(s)) db.createObjectStore(s);
  };
  return wait(req);
}

const pad = (n: bigint | number, width: number): string => n.toString().padStart(width, "0");
const rowKey = (keyspace: string, n: string): string => `${keyspace}\u0000${n}`;
const keyspaceOf = (row: string): string => row.slice(0, row.indexOf("\u0000"));

interface ReplicaHeader {
  head: ReplicaHead;
  base: Omit<ReplicaBase, "entries">;
  entryCount: number;
}
interface Mirror extends ReplicaHeader {
  tail: bigint[];
}

type Write = { store: StoreId; key: string; value: unknown } | { store: StoreId; key: string; delete: true } | { store: StoreId; clear: true };

const BOUNDED: { name: StoreName; store: StoreId; opts: "maxOutbox" | "maxDeadLetters" | "maxJournal" | "maxExports" }[] = [
  { name: "outbox", store: "entries", opts: "maxOutbox" },
  { name: "deadLetters", store: "deadLetters", opts: "maxDeadLetters" },
  { name: "journal", store: "journal", opts: "maxJournal" },
  { name: "exports", store: "exports", opts: "maxExports" },
];

export class IndexedDbStorage implements OutboxStorage {
  mode: StorageMode = "best-effort";
  private ids: Record<"entries" | "deadLetters" | "journal" | "exports", Set<string>> = { entries: new Set(), deadLetters: new Set(), journal: new Set(), exports: new Set() };
  private replicas = new Map<string, Mirror>();
  private chain: Promise<void> = Promise.resolve();

  private constructor(
    private readonly db: IDBDatabaseLike,
    private readonly bounds: MemoryStorageOptions,
    private readonly manager: StorageManagerLike | undefined,
  ) {}

  static async open(factory: IDBFactoryLike, name: string, bounds: MemoryStorageOptions = {}, manager?: StorageManagerLike): Promise<IndexedDbStorage> {
    const db = await openDb(factory, name, STORES);
    const s = new IndexedDbStorage(db, bounds, manager);
    await s.index();
    if (manager?.persisted !== undefined && (await manager.persisted().catch(() => false))) s.mode = "persisted";
    return s;
  }

  /** Reads the ids and the replica headers into memory. */
  private async index(): Promise<void> {
    const tx = this.db.transaction(["entries", "deadLetters", "journal", "exports", "replicas", "replicaTail"], "readonly");
    const finished = done(tx);
    const [entries, dead, journal, exports, headers, tailKeys] = [
      wait(tx.objectStore("entries").getAllKeys()),
      wait(tx.objectStore("deadLetters").getAllKeys()),
      wait(tx.objectStore("journal").getAllKeys()),
      wait(tx.objectStore("exports").getAllKeys()),
      wait(tx.objectStore("replicas").getAllKeys()),
      wait(tx.objectStore("replicaTail").getAllKeys()),
    ];
    const hdrReq = tx.objectStore("replicas").getAll();
    const hdrs = wait(hdrReq);
    await finished;
    this.ids = { entries: new Set(await entries), deadLetters: new Set(await dead), journal: new Set(await journal), exports: new Set(await exports) };
    const keys = await headers;
    const values = (await hdrs) as ReplicaHeader[];
    this.replicas = new Map();
    keys.forEach((k, i) => this.replicas.set(k, { ...(values[i] as ReplicaHeader), tail: [] }));
    for (const row of await tailKeys) this.replicas.get(keyspaceOf(row))?.tail.push(BigInt(row.slice(row.indexOf("\u0000") + 1)));
  }

  async load(): Promise<StoredState> {
    await this.chain;
    const tx = this.db.transaction([...STORES], "readonly");
    const finished = done(tx); // listened to from the start: the transaction can complete before the requests' promises are awaited
    const all = (s: StoreId): Promise<unknown[]> => wait(tx.objectStore(s).getAll());
    const keys = (s: StoreId): Promise<string[]> => wait(tx.objectStore(s).getAllKeys());
    const [entries, deadLetters, journal, exports, rejections, drafts, metaKeys, metaValues, headerKeys, headers, entryKeys, entryValues, tailKeys, tailValues] = await Promise.all([
      all("entries"),
      all("deadLetters"),
      all("journal"),
      all("exports"),
      all("rejections"),
      all("drafts"),
      keys("meta"),
      all("meta"),
      keys("replicas"),
      all("replicas"),
      keys("replicaEntries"),
      all("replicaEntries"),
      keys("replicaTail"),
      all("replicaTail"),
    ]);
    await finished;
    const meta: Record<string, string | number | bigint> = {};
    metaKeys.forEach((k, i) => (meta[k] = metaValues[i] as string | number | bigint));
    const images = new Map<string, ReplicaImage>();
    headerKeys.forEach((k, i) => {
      const h = headers[i] as ReplicaHeader;
      images.set(k, { keyspaceId: k, head: h.head, base: { ...h.base, entries: [] }, tail: [] });
    });
    entryKeys.forEach((k, i) => images.get(keyspaceOf(k))?.base.entries.push(entryValues[i] as KeyEntry));
    tailKeys.forEach((k, i) => images.get(keyspaceOf(k))?.tail.push(tailValues[i] as RecordMessage));
    return {
      entries: entries as StoredState["entries"],
      deadLetters: deadLetters as StoredState["deadLetters"],
      journal: journal as StoredState["journal"],
      exports: exports as StoredState["exports"],
      rejections: rejections as StoredState["rejections"],
      drafts: drafts as StoredState["drafts"],
      replicas: [...images.values()],
      meta,
    };
  }

  full(): StoreName[] {
    const out: StoreName[] = [];
    for (const b of BOUNDED) {
      const max = this.bounds[b.opts];
      if (max !== undefined && this.ids[b.store as keyof typeof this.ids].size >= max) out.push(b.name);
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

  private async commitNow(ops: readonly StorageOp[]): Promise<void> {
    const ids = { entries: new Set(this.ids.entries), deadLetters: new Set(this.ids.deadLetters), journal: new Set(this.ids.journal), exports: new Set(this.ids.exports) };
    const mirrors = new Map(this.replicas);
    const own = new Set<string>(); // the mirrors that this commit has copied, and may change in place
    const mirror = (ks: string): Mirror | undefined => {
      const m = mirrors.get(ks);
      if (m === undefined) return undefined;
      if (!own.has(ks)) {
        own.add(ks);
        const c: Mirror = { ...m, tail: [...m.tail] };
        mirrors.set(ks, c);
        return c;
      }
      return m;
    };
    const writes: Write[] = [];
    const put = (store: StoreId, key: string, value: unknown): void => void writes.push({ store, key, value });
    const del = (store: StoreId, key: string): void => void writes.push({ store, key, delete: true });
    const header = (m: Mirror): ReplicaHeader => ({ head: m.head, base: m.base, entryCount: m.entryCount });
    const dropReplica = (ks: string): void => {
      const m = mirrors.get(ks);
      if (m === undefined) return;
      for (let i = 0; i < m.entryCount; i++) del("replicaEntries", rowKey(ks, pad(i, 10)));
      for (const s of m.tail) del("replicaTail", rowKey(ks, pad(s, 20)));
      del("replicas", ks);
      mirrors.delete(ks);
      own.delete(ks);
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
          const m: Mirror = { head: op.head, base: { manifestVersion: op.base.manifestVersion, logEpoch: op.base.logEpoch, checkpointSeq: op.base.checkpointSeq }, entryCount: op.base.entries.length, tail: [] };
          op.base.entries.forEach((e, i) => put("replicaEntries", rowKey(op.keyspaceId, pad(i, 10)), e));
          put("replicas", op.keyspaceId, header(m));
          mirrors.set(op.keyspaceId, m);
          own.add(op.keyspaceId);
          break;
        }
        case "replicaAppend": {
          const m = mirror(op.keyspaceId);
          if (m === undefined) throw new StorageError(`no replica image of ${op.keyspaceId} to append to`);
          m.head = op.head;
          for (const r of op.records) {
            put("replicaTail", rowKey(op.keyspaceId, pad(r.seq, 20)), r);
            m.tail.push(r.seq);
          }
          put("replicas", op.keyspaceId, header(m));
          break;
        }
        case "replicaHead": {
          const m = mirror(op.keyspaceId);
          if (m === undefined) break;
          m.head = op.head;
          put("replicas", op.keyspaceId, header(m));
          break;
        }
        case "replicaExtend": {
          const m = mirror(op.keyspaceId);
          if (m === undefined) throw new StorageError(`no replica image of ${op.keyspaceId} to extend`);
          m.head = op.head;
          for (const e of op.entries) put("replicaEntries", rowKey(op.keyspaceId, pad(m.entryCount++, 10)), e);
          put("replicas", op.keyspaceId, header(m));
          break;
        }
        case "deleteReplica":
          dropReplica(op.keyspaceId);
          break;
        case "purgeAll":
          for (const s of STORES) writes.push({ store: s, clear: true });
          for (const k of Object.keys(ids)) ids[k as keyof typeof ids].clear();
          mirrors.clear();
          break;
      }
    }
    // 14 [CLI-84]: a commit that would grow a full store beyond its bound is refused whole
    for (const b of BOUNDED) {
      const key = b.store as keyof typeof ids;
      const max = this.bounds[b.opts];
      if (max !== undefined && ids[key].size > this.ids[key].size && ids[key].size > max) throw new StoreFull(b.name);
    }
    if (writes.length > 0) {
      const names = [...new Set(writes.map((w) => w.store))];
      let tx: IDBTransactionLike;
      try {
        tx = this.db.transaction(names, "readwrite");
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
    this.replicas = mirrors;
  }

  close(): void {
    this.db.close();
  }
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
  ) {}

  private reg(): Promise<IDBDatabaseLike> {
    this.registry ??= openDb(this.factory, REGISTRY, ["partitions"]);
    return this.registry;
  }

  private nameOf(key: PartitionKey): string {
    return `abpc:${partitionId(key)}`;
  }

  open(key: PartitionKey): Promise<OutboxStorage> {
    const id = partitionId(key);
    const have = this.open_.get(id);
    if (have !== undefined) return Promise.resolve(have.storage);
    const pending = this.opening.get(id);
    if (pending !== undefined) return pending;
    const p = (async (): Promise<IndexedDbStorage> => {
      const name = this.nameOf(key);
      const storage = await IndexedDbStorage.open(this.factory, name, this.bounds, this.manager);
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

  async destroy(key: PartitionKey): Promise<void> {
    for (const [id, o] of [...this.open_]) {
      if (!sameKey(o.key, key)) continue;
      o.storage.close();
      this.open_.delete(id);
    }
    await wait(this.factory.deleteDatabase(this.nameOf(key)));
    const rdb = await this.reg();
    const tx = rdb.transaction(["partitions"], "readwrite");
    tx.objectStore("partitions").delete(partitionId(key));
    await done(tx);
  }
}
