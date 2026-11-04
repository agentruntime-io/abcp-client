// V21 (C6, C6b), the blob findings: 09 §9.9 [READ-94], 14 [CLI-21], [CLI-44], [CLI-45], 16 [SEC-1], [SEC-45]. Each test is the scenario of a verifier's probe (V21-report.md, B1 to B5) turned into a rule:
// the bytes of the actions of a durable outbox are durable, an action whose bytes are lost is held and says so, staged bytes are collected, a transfer that never answers ends, a follower's upload can
// be stopped and runs once, and what a call buffers is bounded.

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { create } from "@bufbuild/protobuf";
import { afterEach, describe, expect, it } from "vitest";
import { BlobError, digestSha256, Engine, LocalRefusal, MemoryBlobStash, MemoryStorage, STAGED_GRACE_MS, STAGED_SWEEP_MS, StaticManifests, set, type BlobHttp, type StagedBlob } from "../src/engine/index.js";
import { FakeServer, ManualClock, reject as mk, SeededRandom } from "../src/fake/index.js";
import { CreateUploadRequestSchema, MutationResultSchema } from "../src/gen/abpc/v2/sync_pb.js";
import { Code, RetryReason } from "../src/gen/abpc/v2/types_pb.js";
import { FileStorageProvider } from "../src/platform/file-storage.js";
import { FileBlobStash, NodeDevice } from "../src/platform/node.js";
import { goOffline, goOnline, KS, MANIFEST, ofType, rejectResult, retryResult } from "./helpers/engine.js";
import { brig, bytesOf, drive, until } from "./helpers/presence.js";
import { settleAll, tabsRig } from "./helpers/tabs.js";

const calls = (s: { calls: { method: string }[] }, m: string): number => s.calls.filter((c) => c.method === m).length;
const committed = (r: { events: { type: string; mutationId?: string }[] }, id: string): boolean => r.events.some((e) => e.type === "committed" && e.mutationId === id);
const named = (r: { engine: { submit: (a: never) => Promise<{ mutationId: string }> } }, refs: string[], key = "settings/a"): Promise<{ mutationId: string }> =>
  r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set(key, 1)], blobRefs: refs } as never);
const held = (r: { engine: { status(): { holds: unknown[] } } }): unknown[] => r.engine.status().holds;

const dirs: string[] = [];
const tmp = (): string => {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), "abpc-v21-"));
  dirs.push(d);
  return d;
};
afterEach(() => {
  for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------------------------------------------------
describe("V21 MUST-1, 09 [READ-94], 14 [CLI-21]: the bytes that a durable outbox's actions name are durable", () => {
  /** An engine on a tab of a Node device (files under `directory`), over the fake server; not started, so that nothing is uploaded until the test starts it. */
  async function nodeEngine(device: NodeDevice, server: FakeServer, clock: ManualClock, manifests: StaticManifests, seed: number, start: boolean, http: BlobHttp = server.blobs.blobHttp()) {
    const principal = server.principal("ana", "ana-device");
    const tab = device.newTab();
    const engine = await Engine.create({
      appId: "app",
      principal: { issuer: principal.issuer, sub: principal.sub, deviceId: principal.deviceId },
      transport: server.transport(principal),
      manifests,
      clock,
      random: new SeededRandom(seed),
      origin: "https://abpc.test",
      tab,
      blobs: { http },
    });
    await engine.addKeyspace(KS);
    if (start) engine.start();
    await engine.settle();
    return { tab, engine, clock, events: [...engine.eventLog()] };
  }

  it("the Node host with a directory keeps the staged bytes in files beside the outbox: a restart finds them, uploads them and the action commits (the probe B1)", async () => {
    const dir = tmp();
    const clock = new ManualClock();
    const manifests = new StaticManifests(MANIFEST);
    const server = new FakeServer({ manifests, clock, heartbeatMs: 15_000 });
    server.createKeyspaceNow(KS);
    const device1 = new NodeDevice({ directory: dir });
    // a blob store that never answers: the first engine stages the bytes and authors the actions, and none of them goes to the server
    const hang: BlobHttp = { put: () => new Promise(() => undefined), get: () => new Promise(() => undefined) };
    const a = await nodeEngine(device1, server, clock, manifests, 1, true, hang);
    expect(a.engine.status().blobStash).toBe("durable");
    expect(a.engine.status().storageMode).toBe("persisted");
    const bytes = bytesOf(900);
    const ref = await a.engine.stageBlob(KS, bytes);
    const x = await named(a, [ref.blobId], "settings/a");
    const y = await named(a, [], "settings/b");
    await a.engine.stop();
    a.tab.close();
    await device1.close();
    // the files of the stash are under the partition's directory, which holds the outbox
    const partitions = fs.readdirSync(dir);
    expect(partitions).toHaveLength(1);
    expect(fs.readdirSync(path.join(dir, partitions[0] as string, "blobs")).filter((f) => /\.(bin|json)$/.test(f))).toHaveLength(2);
    // the process restarts: a new device on the directory
    const device2 = new NodeDevice({ directory: dir });
    const b = await nodeEngine(device2, server, clock, manifests, 2, true);
    expect(b.engine.outbox().map((e) => e.mutationId).sort()).toEqual([x.mutationId, y.mutationId].sort());
    expect((await b.engine.stagedBlobs()).map((s) => s.blobId)).toEqual([ref.blobId]);
    const rigLike = { clock, engine: b.engine };
    await until(rigLike as never, () => b.engine.outbox().length === 0, "both actions committed", 200);
    expect(Buffer.from(server.blobs.object(KS, ref.blobId) as Uint8Array).equals(Buffer.from(bytes))).toBe(true);
    await until(rigLike as never, () => fs.readdirSync(path.join(dir, partitions[0] as string, "blobs")).length === 0, "the confirmed action's files are erased", 0);
    await b.engine.stop();
    b.tab.close();
    await device2.close();
  });

  it("a Node host without a directory has an in-memory outbox and an in-memory stash, and says so; destroying a partition of a directory removes its stash with it", async () => {
    const mem = new NodeDevice();
    const server = new FakeServer({ manifests: new StaticManifests(MANIFEST), clock: new ManualClock(), heartbeatMs: 15_000 });
    server.createKeyspaceNow(KS);
    const e = await nodeEngine(mem, server, new ManualClock(), new StaticManifests(MANIFEST), 3, false);
    expect(e.engine.status()).toMatchObject({ storageMode: "memory", blobStash: "memory" });
    expect(typeof (mem.storage as { blobStash?: unknown }).blobStash).toBe("undefined");
    await e.engine.stop();
    // the directory provider
    const root = tmp();
    const provider = new FileStorageProvider(root);
    const key = { origin: "https://abpc.test", appId: "app", partition: "i|ana" };
    await provider.open(key);
    const stash = provider.blobStash(key);
    expect(stash.durable).toBe(true);
    const bytes = bytesOf(10);
    await stash.put({ keyspaceId: KS, blobId: "a".repeat(22), bytes, sha256: new Uint8Array(32), size: 10, contentType: "", stagedAtMs: 1 });
    expect(fs.readdirSync(root)).toHaveLength(1);
    await provider.destroy(key);
    expect(fs.readdirSync(root)).toEqual([]); // the partition's directory, the stash in it too
    expect(await new FileBlobStash(path.join(root, "gone")).list()).toEqual([]);
  });

  it("14 [CLI-21]: a durable outbox with a stash in memory refuses to stage: no silent fall-back, a typed local error, and the status says what the stash is", async () => {
    for (const mode of ["persisted", "best-effort"] as const) {
      const r = await brig({ storage: new MemoryStorage({ mode }), cfg: { blobs: { stash: new MemoryBlobStash() } } });
      expect(r.engine.status()).toMatchObject({ storageMode: mode, blobStash: "memory" });
      await expect(r.engine.stageBlob(KS, bytesOf(10))).rejects.toMatchObject({ name: "LocalRefusal", code: "REFUSED" });
      await expect(r.engine.uploadBlob(KS, bytesOf(10))).rejects.toBeInstanceOf(LocalRefusal);
      expect(await r.engine.stagedBlobs()).toEqual([]);
      expect(calls(r.server, "createUpload")).toBe(0);
    }
    // a memory outbox with a memory stash is the engine's default and is reported; a durable stash with a durable outbox is accepted; so is a stash that does not say (the app's)
    const mem = await brig();
    expect(mem.engine.status()).toMatchObject({ storageMode: "memory", blobStash: "memory" });
    await mem.engine.stageBlob(KS, bytesOf(10));
    const plain: MemoryBlobStash = new MemoryBlobStash();
    const custom = Object.create(plain) as MemoryBlobStash & { durable?: boolean };
    Object.defineProperty(custom, "durable", { value: undefined });
    const ok = await brig({ storage: new MemoryStorage({ mode: "persisted" }), cfg: { blobs: { stash: custom } } });
    expect(ok.engine.status().blobStash).toBe("durable");
    await ok.engine.stageBlob(KS, bytesOf(10));
    const durable = await brig({ storage: new MemoryStorage({ mode: "persisted" }), cfg: { blobs: { stash: new FileBlobStash(tmp()) } } });
    await durable.engine.stageBlob(KS, bytesOf(10));
    expect(durable.engine.status().blobStash).toBe("durable");
  });

  it("the bytes of an action are lost (a stash that was cleared): the server answers BLOB_MISSING, the action is held loudly and resent with the hold backoff (14 Table 14.1), and the lane is not wedged for good", async () => {
    const r = await brig();
    r.random.fixed = 0.5;
    await goOffline(r);
    const bytes = bytesOf(300);
    const ref = await r.engine.stageBlob(KS, bytes);
    const a = await named(r, [ref.blobId], "settings/a");
    const b = await named(r, [], "settings/b");
    const c = await named(r, [], "settings/c");
    await r.engine.blobStash.clear(); // what a restart does to a memory stash
    await goOnline(r);
    await r.engine.settle();
    const sentWhenHeld = calls(r.server, "mutate");
    expect(ofType(r, "held")).toMatchObject([{ state: "start", scope: "action", code: "BLOB_MISSING", mutationId: a.mutationId, keyspaceId: KS, blobIds: [ref.blobId] }]);
    expect(held(r)).toEqual([{ scope: "action", code: "BLOB_MISSING", keyspaceId: KS, mutationId: a.mutationId, blobIds: [ref.blobId] }]);
    for (let i = 0; i < 24; i++) {
      await r.clock.advance(300_000); // two hours
      await r.engine.settle();
    }
    // 14 Table 14.1: "resends it with the hold backoff", 1 min x 2^n up to 1 h, drawn here at its middle (random.fixed = 0.5): 30 s, 1 min, 2, 4, 8, 16, 30 min, 30 min. Eight resends in two hours, a few
    // more than none and far fewer than the 57 that the client backoff (cap 5 min) made before the V21 fix
    expect(calls(r.server, "mutate") - sentWhenHeld).toBe(8);
    expect(held(r)).toHaveLength(1); // the resends do not start the hold again, or end it
    expect(ofType(r, "held").map((e) => e.state)).toEqual(["start"]);
    expect(r.engine.deadLetterEntries()).toEqual([]);
    expect(r.engine.outbox().map((e) => e.mutationId)).toContain(a.mutationId);
    // the app stages the same bytes again under the same blob id: the hold clears, the engine uploads them and sends the action, and the actions behind it follow
    const again = await r.engine.stageBlob(KS, bytes, { blobId: ref.blobId });
    expect(again.blobId).toBe(ref.blobId);
    await until(r, () => committed(r, a.mutationId) && committed(r, b.mutationId) && committed(r, c.mutationId), "the actions committed", 1000);
    expect(held(r)).toEqual([]);
    expect(ofType(r, "held").map((e) => e.state)).toEqual(["start", "stop"]);
    expect(Buffer.from(r.server.blobs.object(KS, ref.blobId) as Uint8Array).equals(Buffer.from(bytes))).toBe(true);
    expect(await r.engine.stagedBlobs()).toEqual([]); // confirmed: the bytes go
  });

  it("14 [CLI-45]: the hold of the lost bytes has the bound of every hold: a sent action is outcome unknown at max_offline", async () => {
    const r = await brig({ limits: { maxOfflineSeconds: 120 } });
    r.random.fixed = 0.5;
    const a = await named(r, ["q".repeat(22)]);
    await r.engine.settle();
    expect(held(r)).toHaveLength(1);
    await r.clock.advance(130_000);
    await r.engine.settle();
    expect(ofType(r, "outcome-unknown").find((e) => e.mutationId === a.mutationId)?.cause).toBe("HOLD_BOUND");
    expect(r.engine.outbox()).toEqual([]);
    expect(held(r)).toEqual([]);
  });

  it("the app may cancel an action that is held for lost bytes (the server recorded nothing): it ends as rejected CANCELLED, its slot is released and the actions behind it go", async () => {
    const r = await brig();
    r.random.fixed = 0.5;
    const a = await named(r, ["w".repeat(22)], "settings/a");
    await r.engine.settle();
    expect(held(r)).toHaveLength(1);
    const c = await named(r, [], "settings/c");
    await r.engine.settle();
    expect(committed(r, c.mutationId)).toBe(false); // behind the slot of the held action
    expect(await r.engine.cancel(a.mutationId)).toBe(true);
    await r.engine.settle();
    await r.clock.advance(5000);
    await r.engine.settle();
    expect(ofType(r, "rejected").find((e) => e.mutationId === a.mutationId)?.reject?.code).toBe(Code.CANCELLED);
    expect(held(r)).toEqual([]);
    expect(committed(r, c.mutationId)).toBe(true);
  });

  it("a blob that is not committed and whose bytes are held is repaired as before; one bytes of which are lost while another is held is held after the repair did not mend it", async () => {
    const r = await brig();
    r.random.fixed = 0.5;
    await goOffline(r);
    const ref = await r.engine.stageBlob(KS, bytesOf(100));
    const a = await named(r, [ref.blobId, "l".repeat(22)]);
    await goOnline(r);
    await r.engine.settle();
    for (let i = 0; i < 6; i++) {
      await r.clock.advance(5000);
      await r.engine.settle();
    }
    expect(r.server.blobs.row(KS, ref.blobId)?.state).toBe("committed"); // the bytes that it holds were uploaded
    expect(held(r)).toMatchObject([{ scope: "action", mutationId: a.mutationId, blobIds: ["l".repeat(22)] }]); // the other could not be
  });
});

// ---------------------------------------------------------------------------------------------------------------------
describe("V21 SHOULD-3, 09 [READ-94]: staged bytes that no held action names are collected; the stash is counted and bounded", () => {
  const staged = async (r: { engine: { stagedBlobs(): Promise<unknown[]> } }): Promise<number> => (await r.engine.stagedBlobs()).length;

  it("cancel erases the bytes of the action; so do the acknowledgement of a dead letter and of an export entry (the probe B2)", async () => {
    const r = await brig();
    await goOffline(r);
    const x = await r.engine.stageBlob(KS, bytesOf(100, 1));
    const ax = await named(r, [x.blobId], "settings/a");
    expect(await staged(r)).toBe(1);
    expect(await r.engine.cancel(ax.mutationId)).toBe(true);
    await r.engine.settle();
    await until(r, async () => (await staged(r)) === 0, "the bytes of the cancelled action are erased", 0);
    // a dead letter keeps them (the app may resubmit it) until it is acknowledged
    const y = await r.engine.stageBlob(KS, bytesOf(100, 2));
    const ay = await named(r, [y.blobId], "settings/b");
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.mutationId === ay.mutationId ? create(MutationResultSchema, { mutationId: m.mutationId, outcome: { case: "rejected", value: mk(Code.SCHEMA_INVALID) } }) : undefined) });
    await goOnline(r);
    await r.engine.settle();
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual([ay.mutationId]);
    expect(await staged(r)).toBe(1);
    await r.engine.ackDeadLetter(ay.mutationId);
    await until(r, async () => (await staged(r)) === 0, "the bytes of the acknowledged dead letter are erased", 0);
    expect(r.engine.status()).toMatchObject({ stagedBlobs: 0, stagedBytes: 0 });
  });

  it("an export entry keeps the bytes until the app acknowledges it", async () => {
    const r = await brig({ limits: { maxOfflineSeconds: 120 } });
    const x = await r.engine.stageBlob(KS, bytesOf(100, 3));
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(Code.UPGRADE_REQUIRED)) }, { times: 1 }); // a hold that the action reaches the bound of: outcome unknown, to the export hook
    const a = await named(r, [x.blobId], "settings/a");
    await r.engine.settle();
    await r.clock.advance(130_000);
    await r.engine.settle();
    expect(r.engine.exportEntries().map((e) => e.mutationId)).toEqual([a.mutationId]);
    expect(await staged(r)).toBe(1); // the app may resubmit it
    await r.engine.ackExport(a.mutationId);
    await until(r, async () => (await staged(r)) === 0, "the bytes of the acknowledged export entry are erased", 0);
  });

  it("a stage that no action follows is collected when it is old enough, by a running engine and at a restart; young bytes and bytes that an action names stay", async () => {
    const stash = new MemoryBlobStash();
    const r = await brig({ cfg: { blobs: { stash } } });
    await goOffline(r);
    const lonely = await r.engine.stageBlob(KS, bytesOf(50, 1));
    const used = await r.engine.stageBlob(KS, bytesOf(50, 2));
    const pending = await named(r, [used.blobId]);
    // the action stays in the outbox: the server asks for it to be sent again, and again
    r.server.inject("mutate", { kind: "result", recorded: false, result: (m) => (m.mutationId === pending.mutationId ? retryResult(m, RetryReason.NOT_PROCESSED, 60_000) : undefined) }, { times: 100_000 });
    await r.clock.advance(STAGED_GRACE_MS - 3_600_000);
    const young = await r.engine.stageBlob(KS, bytesOf(50, 3)); // staged now: a day from now it is old enough, an hour from now it is not
    await r.clock.advance(2 * STAGED_SWEEP_MS + 1000); // the lonely blob is older than the grace; the sweep of a running engine runs every STAGED_SWEEP_MS
    await r.engine.settle();
    await until(r, async () => !(await stash.has(KS, lonely.blobId)), "the old stage that no action follows is erased", 0);
    expect(await stash.has(KS, used.blobId)).toBe(true); // an action names it, however old
    expect(await stash.has(KS, young.blobId)).toBe(true); // not old enough
    // a restart: the engine that boots on the stash finds an old stage that no action names
    const old: StagedBlob = { keyspaceId: KS, blobId: "o".repeat(22), bytes: bytesOf(5), sha256: new Uint8Array(32), size: 5, contentType: "", stagedAtMs: r.clock.now() - STAGED_GRACE_MS - 1 };
    await stash.put(old);
    await r.engine.stop();
    const again = await brig({ server: r.server, clock: r.clock, manifests: r.manifests, storage: r.storage, seed: 13, cfg: { blobs: { stash } } });
    await until(again, async () => !(await stash.has(KS, old.blobId)), "the restart erased the old stage", 0);
    expect(await stash.has(KS, used.blobId)).toBe(true);
    expect(await stash.has(KS, young.blobId)).toBe(true);
  });

  it("the stash is counted in the sync status, and a blob that would pass maxStagedBytes is refused locally as a full quota", async () => {
    const r = await brig({ cfg: { blobs: { maxStagedBytes: 1000 } } });
    await r.engine.stageBlob(KS, bytesOf(400, 1));
    await r.engine.stageBlob(KS, bytesOf(400, 2));
    expect(r.engine.status()).toMatchObject({ stagedBlobs: 2, stagedBytes: 800 });
    await expect(r.engine.stageBlob(KS, bytesOf(400, 3))).rejects.toMatchObject({ name: "LocalRefusal", code: "STORAGE", cause: "QUOTA" });
    expect(r.engine.status()).toMatchObject({ stagedBlobs: 2, stagedBytes: 800 });
    // the same blob id staged again replaces its bytes, it does not count twice
    const first = (await r.engine.stagedBlobs())[0];
    await r.engine.stageBlob(KS, bytesOf(500, 4), { blobId: first?.blobId as string });
    expect(r.engine.status()).toMatchObject({ stagedBlobs: 2, stagedBytes: 900 });
    await r.engine.releaseBlob(KS, first?.blobId as string);
    expect(r.engine.status()).toMatchObject({ stagedBlobs: 1, stagedBytes: 400 });
  });

  it("a follower reports the flusher's count and the flusher's stash", async () => {
    const r = tabsRig();
    const t1 = await r.open();
    const t2 = await r.open();
    await settleAll(t1.engine, t2.engine);
    await t2.engine.stageBlob(KS, bytesOf(300));
    await settleAll(t1.engine, t2.engine);
    expect(t1.engine.status()).toMatchObject({ stagedBlobs: 1, stagedBytes: 300, blobStash: "memory" });
    expect(t2.engine.status()).toMatchObject({ role: "follower", stagedBlobs: 1, stagedBytes: 300, blobStash: "memory" });
  });
});

// ---------------------------------------------------------------------------------------------------------------------
describe("V21 SHOULD-4, 09 [READ-94]: the first confirmed action does not erase the bytes that another pending action names (the probe B2b)", () => {
  it("a and b name one blob; a commits while b waits on a Retry; the bytes stay until b is confirmed, and b can still re-upload on BLOB_MISSING", async () => {
    const r = await brig();
    r.random.fixed = 0;
    await goOffline(r);
    const bytes = bytesOf(100, 4);
    const x = await r.engine.stageBlob(KS, bytes);
    const a = await named(r, [x.blobId], "settings/a");
    const b = await named(r, [x.blobId], "settings/b");
    r.server.inject("mutate", { kind: "result", recorded: false, result: (m) => (m.mutationId === b.mutationId ? retryResult(m, RetryReason.NOT_PROCESSED, 60_000) : undefined) }, { times: 1 });
    await goOnline(r);
    await r.engine.settle();
    await until(r, () => committed(r, a.mutationId), "a committed", 0);
    await r.engine.settle();
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([b.mutationId]);
    expect((await r.engine.stagedBlobs()).map((s) => s.blobId)).toEqual([x.blobId]); // b still names it
    // a store that lost the blob (a restore): b meets BLOB_MISSING and re-uploads from the bytes that stayed
    r.server.blobs.rows.delete(`${KS}\u0000${x.blobId}`);
    r.server.blobs.dropObject(KS, x.blobId);
    await r.clock.advance(61_000);
    await until(r, () => committed(r, b.mutationId), "b committed", 1000);
    expect(Buffer.from(r.server.blobs.object(KS, x.blobId) as Uint8Array).equals(Buffer.from(bytes))).toBe(true);
    await until(r, async () => (await r.engine.stagedBlobs()).length === 0, "the bytes went when the last action named them was confirmed", 0);
  });
});

// ---------------------------------------------------------------------------------------------------------------------
describe("V21 SHOULD-5, 14 [CLI-44]: a transfer has a time bound, and the engine's transfers end when it stops (the probe B3)", () => {
  it("a PUT that never answers: after its time it is a failed attempt, the rounds are used up, and the action is sent (the repair of the answer follows)", async () => {
    const r0 = await brig();
    const real = r0.server.blobs.blobHttp();
    let puts = 0;
    const signals: (AbortSignal | undefined)[] = [];
    const hang: BlobHttp = {
      put: (_u, _h, _b, o) => {
        puts++;
        signals.push(o?.signal);
        return new Promise(() => undefined); // a `fetch` that is never answered and ignores its signal
      },
      get: (u, o) => real.get(u, o),
    };
    const r = await brig({ server: r0.server, clock: r0.clock, manifests: r0.manifests, seed: 9, cfg: { blobs: { http: hang } } });
    await goOffline(r);
    const x = await r.engine.stageBlob(KS, bytesOf(100, 5));
    const a = await named(r, [x.blobId]);
    await goOnline(r);
    for (let i = 0; i < 12; i++) {
      await r.clock.advance(300_000);
      await r.engine.settle();
    }
    const sentA = r.server.calls.filter((c) => c.method === "mutate" && JSON.stringify((c.request as { mutations: { mutationId: string }[] }).mutations.map((m) => m.mutationId)).includes(a.mutationId)).length;
    expect(sentA).toBeGreaterThan(0); // 0 in an hour before
    expect(puts).toBeGreaterThanOrEqual(4); // the attempts of the first round
    expect(signals.slice(0, 4).every((s) => s?.aborted === true)).toBe(true); // each attempt of the first round was given up: its signal aborted (the repair that the answer started has one under way)
    await r.engine.stop();
    expect(signals.every((s) => s?.aborted === true)).toBe(true); // and the one that was under way when the engine stopped
  });

  it("the time of a transfer grows with its size: 30 s and 32 KiB/s by default, and the app may set it", async () => {
    for (const [octets, expected] of [
      [0, 30_000],
      [32 * 1024, 31_000],
      [10 * 1024 * 1024, 30_000 + 320_000],
    ] as const) {
      const { DEFAULT_TRANSFER_TIMEOUT } = await import("../src/engine/index.js");
      expect(DEFAULT_TRANSFER_TIMEOUT(octets)).toBe(expected);
    }
    // a timeout that the app sets: 5 s, on the engine's clock
    const r0 = await brig();
    const real = r0.server.blobs.blobHttp();
    const signals: (AbortSignal | undefined)[] = [];
    const slow: BlobHttp = { put: (_u, _h, _b, o) => (signals.push(o?.signal), new Promise(() => undefined)), get: (u, o) => real.get(u, o) };
    const r = await brig({ server: r0.server, clock: r0.clock, manifests: r0.manifests, seed: 9, cfg: { blobs: { http: slow, transferTimeoutMs: 5000 } } });
    const p = r.engine.uploadBlob(KS, bytesOf(10)).catch((e: unknown) => e);
    for (let i = 0; i < 40 && signals.length === 0; i++) await r.clock.yield();
    expect(signals).toHaveLength(1);
    await r.clock.advance(4_900);
    expect(signals[0]?.aborted).toBe(false);
    await r.clock.advance(200);
    expect(signals[0]?.aborted).toBe(true);
    const err = await drive(r, p, 5000, 100);
    expect(err).toMatchObject({ code: "UPLOAD_FAILED" });
    expect(signals.length).toBe(4); // MAX_TRANSFER_ATTEMPTS
  });

  it("a GET that never answers fails the download after its attempts; stop() aborts a transfer that is under way", async () => {
    const r0 = await brig();
    const ref = await drive(r0, r0.engine.uploadBlob(KS, bytesOf(100)));
    const real = r0.server.blobs.blobHttp();
    const signals: (AbortSignal | undefined)[] = [];
    const hang: BlobHttp = { put: (u, h, b, o) => real.put(u, h, b, o), get: (_u, o) => (signals.push(o.signal), new Promise(() => undefined)) };
    const b = await brig({ server: r0.server, clock: r0.clock, manifests: r0.manifests, sub: "bob", seed: 31, cfg: { blobs: { http: hang, transferTimeoutMs: 5000 } } });
    await expect(drive(b, b.engine.getBlob(KS, ref.blobId), 5000, 100)).rejects.toMatchObject({ code: "DOWNLOAD_FAILED" });
    expect(signals.length).toBeGreaterThanOrEqual(4);
    expect(signals.every((s) => s?.aborted === true)).toBe(true);
    // a transfer under way when the engine stops
    const c = await brig({ server: r0.server, clock: r0.clock, manifests: r0.manifests, sub: "cat", seed: 41, cfg: { blobs: { http: hang } } });
    const before = signals.length;
    const p = c.engine.getBlob(KS, ref.blobId).catch((e: unknown) => e);
    for (let i = 0; i < 50 && signals.length === before; i++) await c.clock.yield();
    expect(signals.length).toBe(before + 1);
    expect(signals.at(-1)?.aborted).toBe(false);
    await c.engine.stop();
    expect(signals.at(-1)?.aborted).toBe(true);
    expect(await p).toMatchObject({ code: "ABORTED" });
  });
});

// ---------------------------------------------------------------------------------------------------------------------
describe("V21 SHOULD-6, 14 [CLI-44], [CLI-61]: a follower's upload or delete runs once on the flusher, and the app can stop it (the probe B5)", () => {
  it("a follower's uploadBlob of 1 MiB with a PUT that takes 60 s is staged once and uploaded once, not carried again every 5 s", async () => {
    const r = tabsRig();
    let stashPuts = 0;
    class CountingStash extends MemoryBlobStash {
      override put(b: StagedBlob): Promise<void> {
        stashPuts++;
        return super.put(b);
      }
    }
    const stash = new CountingStash();
    const real = r.server.blobs.blobHttp();
    const slow: BlobHttp = { put: (u, h, b, o) => new Promise((res) => r.clock.setTimer(60_000, () => void real.put(u, h, b, o).then(res))), get: (u, o) => real.get(u, o) };
    const t1 = await r.open({ cfg: { blobs: { stash, http: slow } } });
    const t2 = await r.open({ cfg: { blobs: { stash, http: slow } } });
    await settleAll(t1.engine, t2.engine);
    expect(t2.engine.status().role).toBe("follower");
    let done = false;
    const p = t2.engine.uploadBlob(KS, bytesOf(1 << 20)).then(() => (done = true));
    for (let i = 0; i < 80 && !done; i++) {
      await r.clock.advance(1000);
      await settleAll(t1.engine, t2.engine);
    }
    await p;
    expect(stashPuts).toBe(1); // 13 before
    expect(calls(r.server, "createUpload")).toBe(1);
    expect(t2.engine.callsWaiting()).toBe(0);
  }, 60_000);

  it("the app's signal reaches the flusher: the follower's call fails with ABORTED at once and the flusher stops the PUT; so for deleteBlob that keeps waiting on BLOB_WITHHELD", async () => {
    const r = tabsRig();
    const real = r.server.blobs.blobHttp();
    const signals: (AbortSignal | undefined)[] = [];
    const hang: BlobHttp = { put: (_u, _h, _b, o) => (signals.push(o?.signal), new Promise(() => undefined)), get: (u, o) => real.get(u, o) };
    const t1 = await r.open({ cfg: { blobs: { http: hang } } });
    const t2 = await r.open({ cfg: { blobs: { http: hang } } });
    await settleAll(t1.engine, t2.engine);
    const ac = new AbortController();
    const p = t2.engine.uploadBlob(KS, bytesOf(100), { signal: ac.signal }).catch((e: unknown) => e);
    for (let i = 0; i < 20 && signals.length === 0; i++) await settleAll(t1.engine, t2.engine);
    expect(signals).toHaveLength(1);
    expect(signals[0]?.aborted).toBe(false);
    ac.abort();
    const err = await p;
    expect(err).toBeInstanceOf(BlobError);
    expect(err).toMatchObject({ code: "ABORTED" });
    await settleAll(t1.engine, t2.engine);
    expect(signals[0]?.aborted).toBe(true); // stopped on the flusher
    expect(t2.engine.callsWaiting()).toBe(0);
    // deleteBlob: the blob is withheld, the call keeps asking (14 [CLI-44]) until the app stops it
    const uploaded = r.server.blobs.createUpload("ana", create(CreateUploadRequestSchema, { keyspaceId: KS, blobId: "d".repeat(22), sha256: await digestSha256(bytesOf(20)), size: 20n }));
    await real.put(uploaded.uploadUrl, uploaded.uploadHeaders, bytesOf(20));
    r.server.blobs.commitBlob({ keyspaceId: KS, blobId: "d".repeat(22) } as never);
    r.server.blobs.withhold(KS, "d".repeat(22));
    const waits: string[] = [];
    const ac2 = new AbortController();
    const d = t2.engine.deleteBlob(KS, "d".repeat(22), "done", { signal: ac2.signal, onWait: (w) => waits.push(w.reason) }).catch((e: unknown) => e);
    for (let i = 0; i < 6; i++) {
      await r.clock.advance(5000);
      await settleAll(t1.engine, t2.engine);
    }
    expect(waits.length).toBeGreaterThanOrEqual(2); // the flusher's waits reach the follower's onWait
    expect(new Set(waits)).toEqual(new Set(["BLOB_WITHHELD"]));
    const asked = calls(r.server, "deleteBlob");
    ac2.abort();
    expect(await d).toMatchObject({ code: "ABORTED" });
    await r.clock.advance(120_000);
    await settleAll(t1.engine, t2.engine);
    expect(calls(r.server, "deleteBlob")).toBe(asked); // the flusher asks no more
  });
});

// ---------------------------------------------------------------------------------------------------------------------
describe("V21 SHOULD-7, 16 [SEC-1]: getBlob buffers no more than the app allows", () => {
  it("a blob that the server declares larger than the cap is refused before a byte is read: the engine's default, and the call's own", async () => {
    const r = await brig({ cfg: { blobs: { maxDownloadBytes: 1000 } } });
    const ref = await drive(r, r.engine.uploadBlob(KS, bytesOf(5000)));
    const b = await brig({ server: r.server, clock: r.clock, manifests: r.manifests, sub: "bob", seed: 31, cfg: { blobs: { maxDownloadBytes: 1000 } } });
    const gets = (): number => r.server.blobs.http.filter((h) => h.method === "GET").length;
    await expect(drive(b, b.engine.getBlob(KS, ref.blobId))).rejects.toMatchObject({ code: "DOWNLOAD_TOO_LARGE" });
    expect(gets()).toBe(0);
    await expect(drive(b, b.engine.getBlob(KS, ref.blobId, { maxBytes: 4999 }))).rejects.toMatchObject({ code: "DOWNLOAD_TOO_LARGE" });
    expect(gets()).toBe(0);
    const got = await drive(b, b.engine.getBlob(KS, ref.blobId, { maxBytes: 5000 })); // the call may allow more than the engine's default
    expect(got.size).toBe(5000);
    expect(gets()).toBe(1);
    // the default is 100 MiB
    const c = await brig({ server: r.server, clock: r.clock, manifests: r.manifests, sub: "cat", seed: 41 });
    expect((await drive(c, c.engine.getBlob(KS, ref.blobId))).size).toBe(5000);
  });
});

// ---------------------------------------------------------------------------------------------------------------------
describe("V21 N1, 16 [SEC-45], 02 note after [ARC-56]: blob URLs are https; http only on the loopback or where the app allows it", () => {
  it("an upload URL or a download URL over http to another host is a protocol error; https and http on the loopback are used; allowInsecureUrls allows a development host", async () => {
    const { blobUrlAllowed } = await import("../src/engine/index.js");
    expect(blobUrlAllowed("https://blobs.example.test/x?sig=1", false)).toBe(true);
    expect(blobUrlAllowed("http://127.0.0.1:9000/x", false)).toBe(true);
    expect(blobUrlAllowed("http://localhost:9000/x", false)).toBe(true);
    expect(blobUrlAllowed("http://[::1]:9000/x", false)).toBe(true);
    expect(blobUrlAllowed("http://blobs.example.test/x", false)).toBe(false);
    expect(blobUrlAllowed("http://blobs.example.test/x", true)).toBe(true);
    expect(blobUrlAllowed("ftp://blobs.example.test/x", true)).toBe(false);
    expect(blobUrlAllowed("https://user:pw@blobs.example.test/x", true)).toBe(false);
    expect(blobUrlAllowed("javascript:alert(1)", true)).toBe(false);
    expect(blobUrlAllowed("not a url", true)).toBe(false);
    // in the engine: a server whose upload and download URLs are http to a public host
    const r0 = await brig();
    const wrap = <T extends object>(t: T, over: Record<string, unknown>): never => Object.assign(Object.create(t) as object, over) as never;
    const tr = r0.server.transport(r0.principal);
    const rewritten = wrap(tr, { createUpload: async (req: never) => ({ ...(await tr.createUpload(req)), uploadUrl: "http://blobs.public.test/up" }) });
    let sent = 0;
    const spy: BlobHttp = { put: () => (sent++, Promise.resolve({ status: 200 })), get: () => (sent++, Promise.resolve({ status: 200, bytes: new Uint8Array(0) })) };
    const insecure = await brig({ server: r0.server, clock: r0.clock, manifests: r0.manifests, seed: 5, cfg: { transport: rewritten, blobs: { http: spy } } });
    await expect(drive(insecure, insecure.engine.uploadBlob(KS, bytesOf(10)))).rejects.toMatchObject({ code: "PROTOCOL_ERROR" });
    expect(sent).toBe(0); // nothing was sent to the URL
    const ok = await drive(r0, r0.engine.uploadBlob(KS, bytesOf(10)));
    const bobT = r0.server.transport(r0.server.principal("bob"));
    const dl = await brig({ server: r0.server, clock: r0.clock, manifests: r0.manifests, sub: "bob", seed: 6, cfg: { transport: wrap(bobT, { getBlob: async (req: never) => ({ ...(await bobT.getBlob(req)), downloadUrl: "http://blobs.public.test/down" }) }), blobs: { http: spy } } });
    await expect(drive(dl, dl.engine.getBlob(KS, ok.blobId))).rejects.toMatchObject({ code: "PROTOCOL_ERROR" });
    expect(sent).toBe(0);
    // the app allows a development host: the URL is used (the spy answers it)
    const dev = await brig({ server: r0.server, clock: r0.clock, manifests: r0.manifests, seed: 8, // another seed: the same one would mint the blob id of the engine above, which is committed
       cfg: { transport: wrap(tr, { createUpload: async (req: never) => { return { ...(await tr.createUpload(req)), uploadUrl: "http://blobs.dev.test/up" }; } }), blobs: { http: spy, allowInsecureUrls: true } } });
    const p = dev.engine.uploadBlob(KS, bytesOf(10)).catch(() => undefined); // the spy answers 200 and CommitBlob then finds no object: the URL was used all the same
    await until(dev, () => sent > 0, "the engine used the development URL", 1000);
    await dev.engine.stop();
    await p;
  });
});

// ---------------------------------------------------------------------------------------------------------------------
describe("V21 N6, 09 [READ-94]: staging again under a blob id that a held action names does not replace its bytes", () => {
  it("other bytes under the same id are refused while an action names it; the same bytes are fine", async () => {
    const r = await brig();
    await goOffline(r);
    const x = await r.engine.stageBlob(KS, bytesOf(100, 1));
    await named(r, [x.blobId]);
    await expect(r.engine.stageBlob(KS, bytesOf(100, 2), { blobId: x.blobId })).rejects.toMatchObject({ name: "LocalRefusal", code: "REFUSED" });
    expect((await r.engine.blobStash.get(KS, x.blobId))?.bytes).toEqual(bytesOf(100, 1));
    await r.engine.stageBlob(KS, bytesOf(100, 1), { blobId: x.blobId }); // idempotent
    // a blob that no action names may be staged again with other bytes
    const free = await r.engine.stageBlob(KS, bytesOf(10, 1));
    await r.engine.stageBlob(KS, bytesOf(10, 2), { blobId: free.blobId });
    expect((await r.engine.blobStash.get(KS, free.blobId))?.bytes).toEqual(bytesOf(10, 2));
  });
});
