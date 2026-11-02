// C6: the bytes that an action's blobs need outlive the process (09 [READ-94], 14 [CLI-44]): `FileBlobStash` of the Node platform, and an engine that restarts with an action in its outbox
// whose blob was never uploaded.

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { digestSha256, MemoryBlobStash, set, type StagedBlob } from "../src/engine/index.js";
import { FileBlobStash } from "../src/platform/node.js";
import { goOffline, KS } from "./helpers/engine.js";
import { brig, bytesOf, drive } from "./helpers/presence.js";

const dirs: string[] = [];
const tmp = (): string => {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), "abpc-stash-"));
  dirs.push(d);
  return d;
};
afterEach(() => {
  for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
});

async function blob(id: string, n: number): Promise<StagedBlob> {
  const bytes = bytesOf(n);
  return { keyspaceId: KS, blobId: id, bytes, sha256: await digestSha256(bytes), size: n, contentType: "text/plain", stagedAtMs: 1 };
}

describe("09 [READ-94]: FileBlobStash keeps the bytes in files, checked on every read", () => {
  it("put, get, list and delete; an id is never a path", async () => {
    const dir = tmp();
    const s = new FileBlobStash(dir);
    const b = await blob("a".repeat(22), 1000);
    await s.put(b);
    await s.put(await blob("../../etc/passwd".padEnd(22, "x"), 3)); // an id that is no blob id is still no path
    expect(fs.readdirSync(dir).every((f) => /^[0-9a-f]{64}\.(bin|json)$/.test(f))).toBe(true);
    const got = await new FileBlobStash(dir).get(KS, b.blobId); // another instance: a restart
    expect(got).toMatchObject({ keyspaceId: KS, blobId: b.blobId, size: 1000, contentType: "text/plain", stagedAtMs: 1 });
    expect(Buffer.from(got?.bytes as Uint8Array).equals(Buffer.from(b.bytes))).toBe(true);
    expect((await s.list()).map((x) => x.size).sort()).toEqual([1000, 3]);
    await s.delete(KS, b.blobId);
    expect(await s.get(KS, b.blobId)).toBeUndefined();
    expect(await s.list()).toHaveLength(1);
    await s.delete(KS, b.blobId); // nothing to delete
  });

  it("a damaged file is a blob that is not staged: never a wrong upload", async () => {
    const dir = tmp();
    const s = new FileBlobStash(dir);
    const b = await blob("b".repeat(22), 500);
    await s.put(b);
    const bin = fs.readdirSync(dir).find((f) => f.endsWith(".bin")) as string;
    const buf = fs.readFileSync(path.join(dir, bin));
    buf[10] = (buf[10] as number) ^ 0xff;
    fs.writeFileSync(path.join(dir, bin), buf);
    expect(await s.get(KS, b.blobId)).toBeUndefined();
    fs.writeFileSync(path.join(dir, bin), buf.subarray(0, 100)); // a short file
    expect(await s.get(KS, b.blobId)).toBeUndefined();
  });
});

describe("14 [CLI-44], 09 [READ-94]: an engine that restarts with an action whose blob was never uploaded", () => {
  it("with a durable stash the restarted engine uploads the same bytes when the server answers BLOB_MISSING; with the default stash the bytes are gone", async () => {
    const dir = tmp();
    const first = await brig({ cfg: { blobs: { stash: new FileBlobStash(dir) } } });
    first.random.fixed = 0;
    await goOffline(first);
    const bytes = bytesOf(900);
    const ref = await first.engine.stageBlob(KS, bytes);
    const { mutationId } = await first.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)], blobRefs: [ref.blobId] });
    await first.engine.stop();
    // the process restarts: the outbox is the storage's, the bytes are the stash's
    const second = await brig({ server: first.server, clock: first.clock, manifests: first.manifests, storage: first.storage, seed: 12, cfg: { blobs: { stash: new FileBlobStash(dir) } } });
    second.random.fixed = 0;
    expect(second.engine.outbox().map((e) => e.mutationId)).toEqual([mutationId]);
    // the stash reads files, which the engine's clock does not wait for: the clock moves while the test waits in real time
    for (let i = 0; i < 100 && !second.events.some((e) => e.type === "committed"); i++) {
      await new Promise((r) => setTimeout(r, 10));
      await second.clock.advance(200);
      await second.engine.settle();
    }
    expect(second.events.some((e) => e.type === "committed" && e.mutationId === mutationId)).toBe(true);
    expect(Buffer.from(first.server.blobs.object(KS, ref.blobId) as Uint8Array).equals(Buffer.from(bytes))).toBe(true);
    // confirmed: the files are gone (the engine deletes them outside its queue: real file I/O)
    for (let i = 0; i < 100 && (await second.engine.stagedBlobs()).length > 0; i++) await new Promise((r) => setTimeout(r, 10));
    expect(await second.engine.stagedBlobs()).toEqual([]);
    expect(fs.readdirSync(dir)).toEqual([]);
    // the default stash is in memory: after a restart the action meets BLOB_MISSING with no bytes to upload, and is resent with the backoff
    const third = await brig();
    third.random.fixed = 0.5;
    await goOffline(third);
    const ref2 = await third.engine.stageBlob(KS, bytes);
    await third.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)], blobRefs: [ref2.blobId] });
    await third.engine.stop();
    const fourth = await brig({ server: third.server, clock: third.clock, manifests: third.manifests, storage: third.storage, seed: 13 });
    fourth.random.fixed = 0.5;
    await fourth.clock.advance(5000);
    await fourth.engine.settle();
    expect(fourth.engine.outbox()).toHaveLength(1);
    expect(await fourth.engine.stagedBlobs()).toEqual([]);
    expect(fourth.engine.status().holds).toEqual([]);
    void MemoryBlobStash;
    void drive;
  });
});
