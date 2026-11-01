// A small in-memory double of the part of IndexedDB that src/platform/idb-storage.ts uses: databases of object stores with out-of-line keys, versioned
// open with `onupgradeneeded`, transactions that apply whole or not at all (a copy of the touched stores that replaces them when the transaction
// completes, and is dropped when it aborts), requests that answer later than they are made, structured-clone copies of what is stored, and keys read
// in ascending order. It is not IndexedDB: it shows that the storage's logic (the rows, the order, the bounds, the all-or-none) is right against
// the semantics that the code relies on, and nothing of what a browser does beyond them.

import type { IDBDatabaseLike, IDBFactoryLike, IDBObjectStoreLike, IDBOpenRequestLike, IDBRequestLike, IDBTransactionLike } from "../../src/platform/idb-storage.js";

type Rows = Map<string, unknown>;
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
      const handle = new Database(this, db);
      req.result = handle;
      if (fresh) req.onupgradeneeded?.();
      this.open_++;
      req.onsuccess?.();
    });
    return req;
  }

  deleteDatabase(name: string): IDBRequestLike {
    const req = new Request<undefined>();
    queueMicrotask(() => {
      this.dbs.delete(name);
      req.onsuccess?.();
    });
    return req;
  }
}

class Database implements IDBDatabaseLike {
  readonly objectStoreNames: { contains(name: string): boolean };
  constructor(
    readonly factory: FakeIDBFactory,
    readonly db: Db,
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
    this.factory.open_--;
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
    const sorted = (rows: Rows): string[] => [...rows.keys()].sort();
    return {
      put: (value, key) => this.request((rows) => (rows.set(key, structuredClone(value)), key), name, true),
      delete: (key) => this.request((rows) => void rows.delete(key), name, true),
      clear: () => this.request((rows) => void rows.clear(), name, true),
      getAll: () => this.request((rows) => sorted(rows).map((k) => structuredClone(rows.get(k))), name, false),
      getAllKeys: () => this.request((rows) => sorted(rows), name, false),
    } as IDBObjectStoreLike;
  }

  abort(): void {
    if (this.finished) throw new Error("InvalidStateError");
    this.finished = true;
    queueMicrotask(() => this.onabort?.());
  }
}
