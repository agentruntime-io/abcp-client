// V23 (client), the blob findings: 16 [SEC-45], 09 [READ-94], 14 [CLI-44], [CLI-45]. Each test is the scenario of a verifier's probe (V23-report.md, P1 and P4) or a missing test of V21 SHOULD-8 turned into a
// rule: the loopback test of a blob URL, a staged blob whose bytes do not read is a blob that the engine holds none of, and the directory of the file stash is flushed after a rename.

import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { blobUrlAllowed, MemoryBlobStash, set, type StagedBlob } from "../src/engine/index.js";
import { goOffline, goOnline, KS } from "./helpers/engine.js";
import { brig, bytesOf } from "./helpers/presence.js";

const syncs: { dir: string; names: string[] }[] = [];
vi.mock("../src/platform/fsync.js", async (orig) => {
  const real = await orig<typeof import("../src/platform/fsync.js")>();
  const { readdirSync } = await import("node:fs");
  return {
    ...real,
    // what the directory holds at the moment it is flushed: the rename must be done, so that the flush covers it
    syncDirectory: async (dir: string): Promise<void> => {
      syncs.push({ dir, names: readdirSync(dir).sort() });
      await real.syncDirectory(dir);
    },
  };
});
const { FileBlobStash } = await import("../src/platform/file-blob-stash.js");

describe("V23 SHOULD-2, 16 [SEC-45]: plain HTTP is allowed only on a loopback address", () => {
  it("every host of 127.0.0.0/8 is a loopback address; a name that only looks like one, and every other address, is not", () => {
    const ok = ["http://127.0.0.1/x", "http://127.0.0.2/x", "http://127.1.2.3/x", "http://127.255.255.254:8080/x", "http://localhost/x", "http://localhost:3000/x", "http://app.localhost/x", "http://[::1]/x"];
    const no = [
      "http://127.d.d.d/x",
      "http://127xdxdxd/x",
      "http://127.dd.ddd.d/x",
      "http://127a0b0c1/x",
      "http://localhost.evil.com/x",
      "http://127.0.0.1.nip.io/x",
      "http://0.0.0.0/x",
      "http://126.0.0.1/x",
      "http://128.0.0.1/x",
      "http://10.0.0.1/x",
      "http://[::ffff:127.0.0.1]/x",
      "http://evil.com/x",
    ];
    for (const u of ok) expect([u, blobUrlAllowed(u, false)]).toEqual([u, true]);
    for (const u of no) expect([u, blobUrlAllowed(u, false)]).toEqual([u, false]);
    // https is allowed anywhere, a URL with credentials never, and `allowInsecure` is the development deployment's own switch
    expect(blobUrlAllowed("https://evil.com/x", false)).toBe(true);
    expect(blobUrlAllowed("http://user:pw@127.0.0.1/x", false)).toBe(false);
    expect(blobUrlAllowed("http://evil.com/x", true)).toBe(true);
  });
});

describe("V23 SHOULD-3, 09 [READ-94], 14 [CLI-44], [CLI-45]: a staged entry whose bytes do not read is no bytes held: the hold for lost bytes starts", () => {
  /** A stash that lists the blob and cannot read it: the check of the bytes failed (a damaged file or row). */
  class Damaged extends MemoryBlobStash {
    override get(): Promise<StagedBlob | undefined> {
      return Promise.resolve(undefined);
    }
  }

  it("the probe P4: the action meets BLOB_MISSING, the repair finds no bytes to upload, and the action is held (BLOB_BYTES_LOST) instead of being sent again for ever", async () => {
    const r = await brig({ cfg: { blobs: { stash: new Damaged() } } });
    r.random.fixed = 0.5;
    await goOffline(r);
    const ref = await r.engine.stageBlob(KS, bytesOf(300));
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)], blobRefs: [ref.blobId] });
    await goOnline(r);
    await r.engine.settle();
    for (let i = 0; i < 24; i++) {
      await r.clock.advance(300_000);
      await r.engine.settle();
    }
    const mutates = r.server.calls.filter((c) => c.method === "mutate").length;
    const holds = r.engine.status().holds;
    expect(holds).toMatchObject([{ scope: "action", code: "BLOB_MISSING", mutationId: a.mutationId, blobIds: [ref.blobId] }]);
    expect(mutates).toBeLessThan(10); // the silent loop of V21 MUST-1 sent it 56 times in 2 h
    expect(r.engine.outbox().some((e) => e.mutationId === a.mutationId)).toBe(true); // the action stays, never sent while the hold lasts
  });

  it("the app stages the bytes again under the same id and the hold clears (a stash that reads them)", async () => {
    let damaged = true;
    class Healing extends MemoryBlobStash {
      override get(ks: string, id: string): Promise<StagedBlob | undefined> {
        return damaged ? Promise.resolve(undefined) : super.get(ks, id);
      }
    }
    const r = await brig({ cfg: { blobs: { stash: new Healing() } } });
    r.random.fixed = 0.5;
    await goOffline(r);
    const ref = await r.engine.stageBlob(KS, bytesOf(300));
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)], blobRefs: [ref.blobId] });
    await goOnline(r);
    for (let i = 0; i < 6; i++) {
      await r.clock.advance(60_000);
      await r.engine.settle();
    }
    expect(r.engine.status().holds.map((h) => h.code)).toEqual(["BLOB_MISSING"]);
    damaged = false;
    await r.engine.stageBlob(KS, bytesOf(300), { blobId: ref.blobId });
    for (let i = 0; i < 20 && r.engine.outbox().some((e) => e.mutationId === a.mutationId); i++) {
      await r.clock.advance(5_000);
      await r.engine.settle();
    }
    expect(r.engine.status().holds).toEqual([]);
    expect(r.events.some((e) => e.type === "committed" && e.mutationId === a.mutationId)).toBe(true);
  });
});

describe("V23 SHOULD-4, 09 [READ-94]: the file stash flushes its directory after the rename, as the outbox's storage does", () => {
  let dir: string;
  beforeEach(() => {
    dir = mkdtempSync(path.join(tmpdir(), "abpc-stash-"));
    syncs.length = 0;
  });
  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("put: after the bytes' file and after the facts' file is in place under its final name, the directory is flushed (so the name is on disk, not only the file)", async () => {
    const s = new FileBlobStash(path.join(dir, "stash"));
    const sha = new Uint8Array(32).fill(7);
    await s.put({ keyspaceId: "ks", blobId: "b".repeat(22), bytes: bytesOf(64), sha256: sha, size: 64, contentType: "x/y", stagedAtMs: 1 });
    expect(syncs.length).toBeGreaterThanOrEqual(2); // once for each rename: the bytes before the facts
    for (const x of syncs) expect(x.names.some((n) => n.endsWith(".tmp"))).toBe(false); // flushed after the rename, never with the file still under its temporary name
    expect(syncs[0]?.names.filter((n) => n.endsWith(".bin"))).toHaveLength(1); // the bytes' name was in place at the first flush
    expect(syncs[0]?.names.filter((n) => n.endsWith(".json"))).toHaveLength(0); // and the facts' not yet: the bytes come first
    expect(syncs.at(-1)?.names.filter((n) => n.endsWith(".json"))).toHaveLength(1);
    expect(syncs.every((x) => x.dir === path.join(dir, "stash"))).toBe(true);
  });

  it("delete and clear flush it too: a name that was removed to erase the bytes does not come back after a power loss", async () => {
    const s = new FileBlobStash(path.join(dir, "stash"));
    const sha = new Uint8Array(32).fill(7);
    await s.put({ keyspaceId: "ks", blobId: "b".repeat(22), bytes: bytesOf(64), sha256: sha, size: 64, contentType: "x/y", stagedAtMs: 1 });
    syncs.length = 0;
    await s.delete("ks", "b".repeat(22));
    expect(syncs).toHaveLength(1);
    expect(syncs[0]?.names).toEqual([]); // the files were removed before the flush
    await s.put({ keyspaceId: "ks", blobId: "c".repeat(22), bytes: bytesOf(8), sha256: sha, size: 8, contentType: "x/y", stagedAtMs: 1 });
    syncs.length = 0;
    await s.clear();
    expect(syncs).toHaveLength(1);
    expect(syncs[0]?.names).toEqual([]);
    expect(existsSync(path.join(dir, "stash"))).toBe(true);
  });
});
