// A small in-memory double of the part of IndexedDB that src/platform/idb-storage.ts uses: databases of object stores with out-of-line keys, versioned
// open with `onupgradeneeded`, transactions that apply whole or not at all (a copy of the touched stores that replaces them when the transaction
// completes, and is dropped when it aborts), requests that answer later than they are made, structured-clone copies of what is stored, and keys read
// in ascending order. It is not IndexedDB: it shows that the storage's logic (the rows, the order, the bounds, the all-or-none) is right against
// the semantics that the code relies on, and nothing of what a browser does beyond them.

import type { IDBDatabaseLike, IDBFactoryLike, IDBObjectStoreLike, IDBOpenRequestLike, IDBRequestLike, IDBTransactionLike } from "../../src/platform/idb-storage.js";

type Rows = Map<string, unknown>;

/** `IDBKeyRange.bound(lower, upper)`, both ends closed. */
export class FakeKeyRange {
  constructor(
    readonly lower: string,
    readonly upper: string,
  ) {}
  static bound(lower: unknown, upper: unknown): FakeKeyRange {
    return new FakeKeyRange(lower as string, upper as string);
  }
  includes(key: string): boolean {
    return key >= this.lower && key <= this.upper;
  }
}
type Db = { version: number; stores: Map<string, Rows> };

class Request<T> implements IDBRequestLike<T> {
  result = undefined as T;
  error: unknown = undefined;
  onsuccess: ((ev?: unknown) => void) | null = null;
  onerror: ((ev?: unknown) => void) | null = null;
}

export class FakeIDBFactory implements IDBFactoryLike {
  readonly dbs = new Map<string, Db>();
  /** The next `readwrite` transactions abort when they would complete. */
  failTransactions = 0;
  /** Every database that was opened and not closed. */
  open_ = 0;
  /** The connections that are open, by database name: a deletion asks them to close (`versionchange`) and waits for them. */
  readonly handles = new Map<string, Set<Database>>();
  /** Whether a connection obeys `versionchange` by closing (a connection that was given no handler never does, as one of a build that predates it). */
  private readonly waiting = new Map<string, (() => void)[]>();

  open(name: string, version = 1): IDBOpenRequestLike {
    const req = new Request<IDBDatabaseLike>() as Request<IDBDatabaseLike> & IDBOpenRequestLike;
    req.onupgradeneeded = null;
    req.onblocked = null;
    queueMicrotask(() => {
      let db = this.dbs.get(name);
      const fresh = db === undefined;
      if (db === undefined) {
        db = { version, stores: new Map() };
        this.dbs.set(name, db);
      }
      const handle = new Database(this, db, name);
      const set = this.handles.get(name) ?? new Set();
      set.add(handle);
      this.handles.set(name, set);
      req.result = handle;
      if (fresh) req.onupgradeneeded?.();
      this.open_++;
      req.onsuccess?.();
    });
    return req;
  }

  deleteDatabase(name: string): IDBRequestLike & { onblocked: ((ev?: unknown) => void) | null } {
    const req = new Request<undefined>() as Request<undefined> & { onblocked: ((ev?: unknown) => void) | null };
    req.onblocked = null;
    queueMicrotask(() => {
      const finish = (): void => {
        this.dbs.delete(name);
        req.onsuccess?.();
      };
      // the connections are asked to close; the deletion waits for those that do not, and says so (`blocked`)
      for (const h of [...(this.handles.get(name) ?? [])]) h.onversionchange?.();
      if ((this.handles.get(name)?.size ?? 0) === 0) return finish();
      req.onblocked?.();
      const list = this.waiting.get(name) ?? [];
      list.push(() => {
        if ((this.handles.get(name)?.size ?? 0) === 0) finish();
      });
      this.waiting.set(name, list);
    });
    return req;
  }

  /** A connection closed: a deletion that waited for it may go on. */
  closed(name: string, handle: Database): void {
    this.handles.get(name)?.delete(handle);
    for (const f of this.waiting.get(name) ?? []) f();
  }
}

class Database implements IDBDatabaseLike {
  readonly objectStoreNames: { contains(name: string): boolean };
  onversionchange: ((ev?: unknown) => void) | null = null;
  private open = true;
  constructor(
    readonly factory: FakeIDBFactory,
    readonly db: Db,
    readonly name: string,
  ) {
    this.objectStoreNames = { contains: (n) => db.stores.has(n) };
  }
  createObjectStore(name: string): unknown {
    this.db.stores.set(name, new Map());
    return {};
  }
  transaction(names: string[], mode: "readonly" | "readwrite"): IDBTransactionLike {
    for (const n of names) if (!this.db.stores.has(n)) throw new Error(`NotFoundError: no object store ${n}`);
    return new Transaction(this.factory, this.db, names, mode);
  }
  close(): void {
    if (!this.open) return;
    this.open = false;
    this.factory.open_--;
    this.factory.closed(this.name, this);
  }
}

class Transaction implements IDBTransactionLike {
  error: unknown = undefined;
  oncomplete: ((ev?: unknown) => void) | null = null;
  onerror: ((ev?: unknown) => void) | null = null;
  onabort: ((ev?: unknown) => void) | null = null;
  private readonly work = new Map<string, Rows>();
  private pending = 0;
  private finished = false;
  private scheduled = false;

  constructor(
    private readonly factory: FakeIDBFactory,
    private readonly db: Db,
    names: string[],
    private readonly mode: "readonly" | "readwrite",
  ) {
    for (const n of names) this.work.set(n, new Map(this.db.stores.get(n)));
    this.settleSoon();
  }

  private settleSoon(): void {
    if (this.scheduled || this.finished) return;
    this.scheduled = true;
    queueMicrotask(() => {
      this.scheduled = false;
      if (this.finished) return;
      if (this.pending > 0) return this.settleSoon();
      this.finished = true;
      if (this.mode === "readwrite" && this.factory.failTransactions > 0) {
        this.factory.failTransactions--;
        this.error = new Error("QuotaExceededError (injected)");
        this.onabort?.();
        return;
      }
      if (this.mode === "readwrite") for (const [n, rows] of this.work) this.db.stores.set(n, rows);
      this.oncomplete?.();
    });
  }

  request<T>(run: (rows: Rows) => T, store: string, write: boolean): IDBRequestLike<T> {
    if (this.finished) throw new Error("TransactionInactiveError");
    if (write && this.mode !== "readwrite") throw new Error("ReadOnlyError");
    const rows = this.work.get(store);
    if (rows === undefined) throw new Error(`NotFoundError: ${store} is not in the transaction`);
    const req = new Request<T>();
    const result = run(rows);
    this.pending++;
    queueMicrotask(() => {
      req.result = result;
      req.onsuccess?.();
      this.pending--;
      this.settleSoon();
    });
    return req;
  }

  objectStore(name: string): IDBObjectStoreLike {
    const sorted = (rows: Rows, range?: unknown): string[] => [...rows.keys()].sort().filter((k) => range === undefined || (range as FakeKeyRange).includes(k));
    return {
      put: (value, key) => this.request((rows) => (rows.set(key, structuredClone(value)), key), name, true),
      delete: (key) => this.request((rows) => void rows.delete(key), name, true),
      clear: () => this.request((rows) => void rows.clear(), name, true),
      get: (key) => this.request((rows) => structuredClone(rows.get(key)), name, false),
      getAll: (range?: unknown) => this.request((rows) => sorted(rows, range).map((k) => structuredClone(rows.get(k))), name, false),
      getAllKeys: (range?: unknown) => this.request((rows) => sorted(rows, range), name, false),
    } as IDBObjectStoreLike;
  }

  abort(): void {
    if (this.finished) throw new Error("InvalidStateError");
    this.finished = true;
    queueMicrotask(() => this.onabort?.());
  }
}
