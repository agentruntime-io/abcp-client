// A `BlobStash` in files, for a Node process (09 [READ-94]): "A client keeps a blob's bytes locally until the action that references it is confirmed". The outbox is durable, so an action that
// names a blob outlives the process; the bytes that it needs, when the server answers `BLOB_MISSING` or `Retry{BLOB_PENDING}` (14 [CLI-44]), must too.
//
// One directory, two files for each blob: `<name>.bin` holds the bytes and `<name>.json` the facts of the blob (keyspace, id, size, digest, content type). `<name>` is the hex of the SHA-256 of
// the keyspace id and the blob id, so that no id is ever a path. A file is written beside its final name and renamed into place, the bytes before the facts: a blob is staged when its `.json`
// exists, and a crash between the two leaves bytes that no facts name, which `list` ignores and the next `put` of the blob replaces. A read checks the bytes against the size and the digest
// that the facts hold, so that a damaged file is a blob that is not staged (the upload then fails with `BLOB_MISSING`, which 14 [CLI-44] makes the action's resend meet again), never a wrong upload.
//
// The directory is the stash's, for one partition: a sign-out that purges the partition erases the staged bytes too (`clear`, 14 [CLI-65]), and only the files that this stash wrote. The Node host
// (`FileStorageProvider.blobStash`) gives each partition one in `<partition directory>/blobs`, which is the engine's default (09 [READ-94], 14 [CLI-21]: the outbox is durable, so are the bytes that its
// actions name), and which `destroy` removes with the partition; an app's own (`EngineConfig.blobs.stash`) goes before it, and is one partition's: two principals never share a stash.
//
// A file is written to its end and flushed to the disk before it is renamed into place, so that a power loss never leaves a name over bytes that were not written (the check of `get` would catch
// them, but an upload of the bytes would then be lost for good).

import { createHash } from "node:crypto";
import { access, mkdir, open, readdir, readFile, rename, rm } from "node:fs/promises";
import * as path from "node:path";
import type { BlobStash, StagedBlob } from "../engine/blob-store.js";

interface Facts {
  keyspaceId: string;
  blobId: string;
  size: number;
  sha256: string;
  contentType: string;
  stagedAtMs: number;
}

const hex = (b: Uint8Array): string => Buffer.from(b).toString("hex");
/** The files of this stash: the hex of a SHA-256, then `.bin` or `.json`, and `.tmp` for a file that is being written. */
const OWN_FILE = /^[0-9a-f]{64}\.(bin|json)(\.tmp)?$/;

export class FileBlobStash implements BlobStash {
  /** The bytes are in files: they outlive the process. */
  readonly durable = true;
  private ready: Promise<void> | undefined;

  constructor(private readonly dir: string) {}

  private init(): Promise<void> {
    this.ready ??= mkdir(this.dir, { recursive: true }).then(() => undefined);
    return this.ready;
  }

  private name(keyspaceId: string, blobId: string): string {
    return createHash("sha256").update(`${keyspaceId}\u0000${blobId}`).digest("hex");
  }

  private async writeAtomic(file: string, data: Uint8Array | string): Promise<void> {
    const tmp = `${file}.tmp`;
    const h = await open(tmp, "w");
    try {
      await h.writeFile(data);
      await h.sync(); // on the disk before the name is
    } finally {
      await h.close();
    }
    await rename(tmp, file);
  }

  async put(blob: StagedBlob): Promise<void> {
    await this.init();
    const base = path.join(this.dir, this.name(blob.keyspaceId, blob.blobId));
    const facts: Facts = { keyspaceId: blob.keyspaceId, blobId: blob.blobId, size: blob.size, sha256: hex(blob.sha256), contentType: blob.contentType, stagedAtMs: blob.stagedAtMs };
    await this.writeAtomic(`${base}.bin`, blob.bytes);
    await this.writeAtomic(`${base}.json`, JSON.stringify(facts));
  }

  async get(keyspaceId: string, blobId: string): Promise<StagedBlob | undefined> {
    await this.init();
    const base = path.join(this.dir, this.name(keyspaceId, blobId));
    let facts: Facts;
    let bytes: Buffer;
    try {
      facts = JSON.parse(await readFile(`${base}.json`, "utf8")) as Facts;
      bytes = await readFile(`${base}.bin`);
    } catch {
      return undefined;
    }
    if (facts.keyspaceId !== keyspaceId || facts.blobId !== blobId || bytes.length !== facts.size) return undefined;
    const digest = createHash("sha256").update(bytes).digest("hex");
    if (digest !== facts.sha256) return undefined; // damaged: not staged
    return { keyspaceId, blobId, bytes: new Uint8Array(bytes), sha256: new Uint8Array(Buffer.from(facts.sha256, "hex")), size: facts.size, contentType: facts.contentType, stagedAtMs: facts.stagedAtMs };
  }

  async has(keyspaceId: string, blobId: string): Promise<boolean> {
    await this.init();
    try {
      await access(`${path.join(this.dir, this.name(keyspaceId, blobId))}.json`); // the facts name a staged blob (`put` writes them last); `get` checks the bytes
      return true;
    } catch {
      return false;
    }
  }

  async delete(keyspaceId: string, blobId: string): Promise<void> {
    await this.init();
    const base = path.join(this.dir, this.name(keyspaceId, blobId));
    await rm(`${base}.json`, { force: true });
    await rm(`${base}.bin`, { force: true });
  }

  /**
   * 14 [CLI-65]: every file of the stash goes, the bytes of a blob whose facts were never written and the files that a crash left beside their names included. A file that this stash did
   * not write (its name is not the hex of a digest, with `.bin`, `.json` or `.tmp` after it) is left alone: the directory is the app's.
   */
  async clear(): Promise<void> {
    await this.init();
    for (const f of await readdir(this.dir)) if (OWN_FILE.test(f)) await rm(path.join(this.dir, f), { force: true });
  }

  async list(): Promise<{ keyspaceId: string; blobId: string; size: number; stagedAtMs: number }[]> {
    await this.init();
    const out: { keyspaceId: string; blobId: string; size: number; stagedAtMs: number }[] = [];
    for (const f of await readdir(this.dir)) {
      if (!f.endsWith(".json")) continue;
      try {
        const facts = JSON.parse(await readFile(path.join(this.dir, f), "utf8")) as Facts;
        out.push({ keyspaceId: facts.keyspaceId, blobId: facts.blobId, size: facts.size, stagedAtMs: facts.stagedAtMs });
      } catch {
        // a file that is not one of this stash's facts is not a staged blob
      }
    }
    return out;
  }
}
