// A `BlobStash` in IndexedDB, for a browser (09 [READ-94], 14 [CLI-44]): "A client keeps a blob's bytes locally until the action that references it is confirmed, and re-uploads on
// `BLOB_MISSING`". The outbox is durable (`idb-storage.ts`), so an action that names a blob outlives the page; the bytes that it needs, when the server answers `BLOB_MISSING` or
// `Retry{BLOB_PENDING}`, or before its first send, must too. A reload finds them here.
//
// One database for each partition, `abcp-blobs:<partition>`, beside the partition's own (the provider names and deletes both). Two stores keyed by `<keyspace>\0<blob>`:
//
//   bytes   the octets of the blob
//   facts   { keyspaceId, blobId, size, sha256, contentType, stagedAtMs }
//
// `put` and `delete` write both in one `readwrite` transaction, which IndexedDB applies whole or not at all: a blob is staged when its facts are there, and there is no moment between
// the bytes and the facts that a crash can leave (a file stash has one, `file-blob-stash.ts`). `list` and `has` read the facts only, so that the staged blobs of a page are counted
// without reading their bytes; `get` reads both, and a row that is not what `put` wrote (its bytes are not as many as its facts say) is a blob that is not staged: the upload then
// fails with `BLOB_MISSING`, which 14 [CLI-44] makes the action's resend meet again, and no wrong upload is ever made (the store also refuses a body whose checksum differs, 09 [READ-87]).
//
// The tabs of a partition share the database (the tab that is the flusher changes with the handover, 14 [CLI-61]), so a new flusher finds the bytes of the old one, which a stash in
// memory loses. A connection yields to a request to delete or upgrade the database (`versionchange`): it closes, so that the sign-out of another tab is not kept waiting for it
// (V19 SHOULD-8), and a stash whose connection was closed so refuses every call with a `StorageError`, as the storage does. The database is opened by the first call that needs it.
//
// Untested against a real IndexedDB (Node has none): `test/platform-idb-blob-stash.test.ts` runs it against the double of `test/helpers/fake-idb.ts`.

import { digestSha256, sameBytes, type BlobStash, type StagedBlob } from "../engine/blob-store.js";
import { StorageError } from "../engine/errors.js";
import { done, openDb, wait, type IDBDatabaseLike, type IDBFactoryLike, type IDBObjectStoreLike } from "./idb.js";

const BYTES = "bytes";
const FACTS = "facts";
const STORES = [BYTES, FACTS] as const;

interface Facts {
  keyspaceId: string;
  blobId: string;
  size: number;
  sha256: Uint8Array;
  contentType: string;
  stagedAtMs: number;
}

const isObject = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
/** Bytes, whichever realm made them (a worker, a frame): `instanceof` would call a row of another realm malformed. */
const isBytes = (v: unknown): v is Uint8Array => Object.prototype.toString.call(v) === "[object Uint8Array]";
const rowKey = (keyspaceId: string, blobId: string): string => `${keyspaceId}\u0000${blobId}`;

/** The facts of a row that this stash wrote, or undefined for a row that is not one (it is not a staged blob). */
function factsOf(row: unknown): Facts | undefined {
  if (!isObject(row)) return undefined;
  const { keyspaceId, blobId, size, sha256, contentType, stagedAtMs } = row;
  if (typeof keyspaceId !== "string" || typeof blobId !== "string" || typeof size !== "number" || !Number.isSafeInteger(size) || size < 0) return undefined;
  if (!isBytes(sha256) || sha256.length !== 32 || typeof contentType !== "string" || typeof stagedAtMs !== "number") return undefined;
  return { keyspaceId, blobId, size, sha256, contentType, stagedAtMs };
}

export class IndexedDbBlobStash implements BlobStash {
  /** The bytes are in a database: they outlive the page. */
  readonly durable = true;
  private db: Promise<IDBDatabaseLike> | undefined;
  /** The connection was closed by another tab's request to delete or upgrade the database, or by `close`. */
  private closed = false;

  /** `name` is the database of the partition's blobs; it is created by the first call. */
  constructor(
    private readonly factory: IDBFactoryLike,
    readonly name: string,
  ) {}

  /** Whether another tab's request to delete or upgrade the database closed this connection, or `close` did. */
  get isClosed(): boolean {
    return this.closed;
  }

  /** Closes the connection; the stash refuses every call after it. */
  close(): void {
    this.closed = true;
    const db = this.db;
    this.db = undefined;
    void db?.then((d) => d.close()).catch(() => undefined);
  }

  private connect(): Promise<IDBDatabaseLike> {
    if (this.closed) return Promise.reject(new StorageError(`the blob database ${this.name} is closed (another tab deleted or upgraded it, or this stash was closed)`));
    if (this.db === undefined) {
      // another tab deletes the database (a sign-out, 14 [CLI-65]) or upgrades it: this connection closes at once (`openDb`), or that tab waits for it for ever
      const p = openDb(this.factory, this.name, STORES, () => {
        this.closed = true;
      }).catch((e: unknown) => {
        if (this.db === p) this.db = undefined; // an open that failed is made again by the next call
        throw e instanceof StorageError ? e : new StorageError(`the blob database ${this.name} cannot be opened: ${e instanceof Error ? e.message : String(e)}`);
      });
      this.db = p;
    }
    return this.db;
  }

  /** One transaction over both stores: `work` makes its requests at once, while the transaction is active, and answers with what they gave. */
  private async transact<T>(mode: "readonly" | "readwrite", work: (stores: { bytes: IDBObjectStoreLike; facts: IDBObjectStoreLike }) => Promise<T>): Promise<T> {
    const db = await this.connect();
    if (this.closed) throw new StorageError(`the blob database ${this.name} is closed`);
    let tx;
    try {
      tx = db.transaction([...STORES], mode);
    } catch (e) {
      throw new StorageError(`cannot open a transaction: ${e instanceof Error ? e.message : String(e)}`);
    }
    const finished = done(tx); // listened to from the start: the transaction can complete before the requests' promises are awaited
    let out: Promise<T>;
    try {
      out = work({ bytes: tx.objectStore(BYTES), facts: tx.objectStore(FACTS) });
    } catch (e) {
      try {
        tx.abort();
      } catch {
        // already finished
      }
      await finished.catch(() => undefined);
      throw new StorageError(`a request failed: ${e instanceof Error ? e.message : String(e)}`);
    }
    void out.catch(() => undefined); // a failed transaction fails its requests too: `finished` reports it
    await finished;
    return out;
  }

  async put(blob: StagedBlob): Promise<void> {
    const key = rowKey(blob.keyspaceId, blob.blobId);
    const facts: Facts = { keyspaceId: blob.keyspaceId, blobId: blob.blobId, size: blob.size, sha256: blob.sha256, contentType: blob.contentType, stagedAtMs: blob.stagedAtMs };
    await this.transact("readwrite", (s) => Promise.all([wait(s.bytes.put(blob.bytes, key)), wait(s.facts.put(facts, key))]));
  }

  async get(keyspaceId: string, blobId: string): Promise<StagedBlob | undefined> {
    const key = rowKey(keyspaceId, blobId);
    const [factsRow, bytes] = await this.transact("readonly", (s) => Promise.all([wait(s.facts.get(key)), wait(s.bytes.get(key))]));
    const facts = factsOf(factsRow);
    if (facts === undefined || facts.keyspaceId !== keyspaceId || facts.blobId !== blobId) return undefined;
    if (!isBytes(bytes) || bytes.length !== facts.size) return undefined; // damaged: not staged
    // a row of the right length with other bytes in it is damaged too: shown as the blob, its upload would be refused by the store at every repair (09 [READ-87]) and its action would loop on `BLOB_MISSING`
    if (!sameBytes(await digestSha256(bytes), facts.sha256)) return undefined;
    return { keyspaceId, blobId, bytes: new Uint8Array(bytes), sha256: new Uint8Array(facts.sha256), size: facts.size, contentType: facts.contentType, stagedAtMs: facts.stagedAtMs };
  }

  async has(keyspaceId: string, blobId: string): Promise<boolean> {
    const facts = factsOf(await this.transact("readonly", (s) => wait(s.facts.get(rowKey(keyspaceId, blobId)))));
    return facts !== undefined && facts.keyspaceId === keyspaceId && facts.blobId === blobId;
  }

  async delete(keyspaceId: string, blobId: string): Promise<void> {
    const key = rowKey(keyspaceId, blobId);
    await this.transact("readwrite", (s) => Promise.all([wait(s.bytes.delete(key)), wait(s.facts.delete(key))]));
  }

  async list(): Promise<{ keyspaceId: string; blobId: string; size: number; stagedAtMs: number }[]> {
    const rows = await this.transact("readonly", (s) => wait(s.facts.getAll()));
    const out: { keyspaceId: string; blobId: string; size: number; stagedAtMs: number }[] = [];
    for (const row of rows) {
      const f = factsOf(row);
      if (f !== undefined) out.push({ keyspaceId: f.keyspaceId, blobId: f.blobId, size: f.size, stagedAtMs: f.stagedAtMs });
    }
    return out;
  }

  /** 14 [CLI-65]: both stores are emptied in one transaction, whoever staged the rows (another tab of the partition included) and whatever they hold. */
  async clear(): Promise<void> {
    await this.transact("readwrite", (s) => Promise.all([wait(s.bytes.clear()), wait(s.facts.clear())]));
  }
}
