// The part of the IndexedDB API that the platform's storages use, and the calls that open and delete a database (14 §14.3): shared by the outbox storage (`idb-storage.ts`) and the stash of
// the bytes that actions name (`idb-blob-stash.ts`), so that both treat `versionchange` and `blocked` alike. A connection yields to another tab's request to delete or upgrade the
// database (`versionchange`), so that a sign-out is not kept waiting for it (V19 SHOULD-8); a deletion that a connection which does not yield blocks is reported after a wait.

import { StorageError } from "../engine/errors.js";

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

export const wait = <T>(r: IDBRequestLike<T>): Promise<T> =>
  new Promise((resolve, reject) => {
    r.onsuccess = (): void => resolve(r.result);
    r.onerror = (): void => reject(r.error instanceof Error ? r.error : new StorageError(`an IndexedDB request failed: ${String(r.error)}`));
  });

export function done(tx: IDBTransactionLike): Promise<void> {
  return new Promise((resolve, reject) => {
    tx.oncomplete = (): void => resolve();
    tx.onabort = (): void => reject(new StorageError(`the transaction was aborted: ${tx.error instanceof Error ? tx.error.message : String(tx.error ?? "")}`));
    tx.onerror = (): void => reject(new StorageError(`the transaction failed: ${tx.error instanceof Error ? tx.error.message : String(tx.error ?? "")}`));
  });
}

export async function openDb(factory: IDBFactoryLike, name: string, stores: readonly string[], onClosed?: () => void): Promise<IDBDatabaseLike> {
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

/**
 * Deletes a database; another tab that keeps it open is asked to close it (`versionchange`), and waited for for `blockedWaitMs` real ms (default 10 s) before the deletion is reported as
 * failed (the browser still deletes it when that tab closes it). A sign-out is never left waiting for ever by a tab that does not yield (V19 SHOULD-8).
 */
export function deleteDatabase(factory: IDBFactoryLike, name: string, blockedWaitMs = 10_000): Promise<void> {
  return new Promise((resolve, reject) => {
    const req = factory.deleteDatabase(name);
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
      timer ??= setTimeout(() => reject(new StorageError(`another tab keeps the database ${name} open: it is deleted when that tab closes it`)), blockedWaitMs);
    };
  });
}

