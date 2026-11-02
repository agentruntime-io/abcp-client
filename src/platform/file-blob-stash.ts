// A `BlobStash` in files, for a Node process (09 [READ-94]): "A client keeps a blob's bytes locally until the action that references it is confirmed". The outbox is durable, so an action that
// names a blob outlives the process; the bytes that it needs, when the server answers `BLOB_MISSING` or `Retry{BLOB_PENDING}` (14 [CLI-44]), must too.
//
// One directory, two files for each blob: `<name>.bin` holds the bytes and `<name>.json` the facts of the blob (keyspace, id, size, digest, content type). `<name>` is the hex of the SHA-256 of
// the keyspace id and the blob id, so that no id is ever a path. A file is written beside its final name and renamed into place, the bytes before the facts: a blob is staged when its `.json`
// exists, and a crash between the two leaves bytes that no facts name, which `list` ignores and the next `put` of the blob replaces. A read checks the bytes against the size and the digest
// that the facts hold, so that a damaged file is a blob that is not staged (the upload then fails with `BLOB_MISSING`, which 14 [CLI-44] makes the action's resend meet again), never a wrong upload.

import { createHash } from "node:crypto";
import { mkdir, readdir, readFile, rename, rm, writeFile } from "node:fs/promises";
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

export class FileBlobStash implements BlobStash {
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
    await writeFile(tmp, data);
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

  async delete(keyspaceId: string, blobId: string): Promise<void> {
    await this.init();
    const base = path.join(this.dir, this.name(keyspaceId, blobId));
    await rm(`${base}.json`, { force: true });
    await rm(`${base}.bin`, { force: true });
  }

  async list(): Promise<{ keyspaceId: string; blobId: string; size: number }[]> {
    await this.init();
    const out: { keyspaceId: string; blobId: string; size: number }[] = [];
    for (const f of await readdir(this.dir)) {
      if (!f.endsWith(".json")) continue;
      try {
        const facts = JSON.parse(await readFile(path.join(this.dir, f), "utf8")) as Facts;
        out.push({ keyspaceId: facts.keyspaceId, blobId: facts.blobId, size: facts.size });
      } catch {
        // a file that is not one of this stash's facts is not a staged blob
      }
    }
    return out;
  }
}
