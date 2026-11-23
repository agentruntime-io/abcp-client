// C6b: the engine on the browser host (createWebHost over the double of IndexedDB): the bytes that an action names survive a restart (09 [READ-94], 14 [CLI-44]), and are erased with the
// partition at a sign-out and with a keyspace's copy at a purge (14 [CLI-65], [CLI-66]). The engine runs as it does in a page: its outbox is in IndexedDB, its stash is the host's, and a
// reload is a new host and a new engine on the same database.

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { Engine, set, StaticManifests, type EngineEvent, type ExportHook } from "../src/engine/index.js";
import { FakeServer, ManualClock, reject as mk, SeededRandom } from "../src/fake/index.js";
import { MutationResultSchema } from "../src/gen/abcp/v2/sync_pb.js";
import { Code } from "../src/gen/abcp/v2/types_pb.js";
import { createWebHost } from "../src/platform/web.js";
import { FakeIDBFactory, FakeKeyRange } from "./helpers/fake-idb.js";
import { KS, MANIFEST } from "./helpers/engine.js";
import { bytesOf } from "./helpers/presence.js";

const calls = (s: FakeServer, m: string): number => s.calls.filter((c) => c.method === m).length;
const putCount = (s: FakeServer): number => s.blobs.http.filter((h) => h.method === "PUT").length;
const blobDbs = (f: FakeIDBFactory): string[] => [...f.dbs.keys()].filter((k) => k.startsWith("abcp-blobs:"));
const partitionDbs = (f: FakeIDBFactory): string[] => [...f.dbs.keys()].filter((k) => k.startsWith("abcp:") && k !== "abcp:registry");

/** A page: a world of one server, one clock and one IndexedDB, in which engines are started and restarted. */
class World {
  readonly clock = new ManualClock();
  readonly manifests = new StaticManifests(MANIFEST);
  readonly server = new FakeServer({ manifests: this.manifests, clock: this.clock, heartbeatMs: 15_000 });
  readonly idb = new FakeIDBFactory();
  readonly random = new SeededRandom(7);

  constructor() {
    this.server.createKeyspaceNow(KS);
  }

  /** A page load: a new host (so a new provider and a new stash) on the same IndexedDB, and an engine on it, started (unless `start` is false: the page has read its state and not connected). */
  async load(o: { sub?: string; exportHook?: ExportHook; keyspaces?: string[]; start?: boolean } = {}): Promise<{ engine: Engine; events: EngineEvent[] }> {
    const principal = this.server.principal(o.sub ?? "ana");
    const host = createWebHost({ env: { indexedDB: this.idb, IDBKeyRange: FakeKeyRange }, tabId: "t1" });
    const engine = await Engine.create({
      appId: "app",
      principal: { issuer: principal.issuer, sub: principal.sub, deviceId: principal.deviceId },
      transport: this.server.transport(principal),
      manifests: this.manifests,
      clock: this.clock,
      random: this.random,
      tab: host,
      blobs: { http: this.server.blobs.blobHttp() },
      ...(o.exportHook !== undefined ? { exportHook: o.exportHook } : {}),
    });
    const events: EngineEvent[] = [];
    engine.on((e) => events.push(e));
    for (const k of o.keyspaces ?? [KS]) await engine.addKeyspace(k);
    if (o.start === false) return { engine, events };
    engine.start();
    await engine.settle();
    return { engine, events };
  }

  async offline(engine: Engine): Promise<void> {
    this.server.disconnect();
    await engine.settle();
  }

  async advance(engine: Engine, ms: number): Promise<void> {
    await this.clock.advance(ms);
    await engine.settle();
  }

  /**
   * Advances the clock by `ms` and settles until `done` holds, at most `rounds` times. `settle` returns when nothing has started or ended for a few turns of the event loop ("what is left waits
   * for time or for the outside"), and the digest of the staged bytes is work of the outside: it runs on the thread pool, so on a loaded machine `settle` can return before it has ended. A
   * test that waits for what that work leads to waits for the condition, and yields the event loop between rounds, rather than assuming a fixed number of advances is enough.
   */
  async advanceUntil(engine: Engine, ms: number, done: () => boolean, rounds = 400): Promise<void> {
    for (let i = 0; i < rounds && !done(); i++) {
      await this.advance(engine, ms);
      if (!done()) await new Promise<void>((r) => setTimeout(r, 2));
    }
  }
}

const submitWith = (e: Engine, refs: string[], key = "settings/a"): Promise<{ mutationId: string }> => e.submit({ keyspaceId: KS, opType: "Set", changes: [set(key, 1)], blobRefs: refs });
const committed = (events: EngineEvent[], id: string): boolean => events.some((e) => e.type === "committed" && e.mutationId === id);

describe("09 [READ-94], 14 [CLI-44]: on the browser host the bytes that an action names survive a restart", () => {
  it("an action authored offline that names a staged blob, then a reload: the new engine finds the bytes in IndexedDB, uploads them before it sends the action, and the action commits", async () => {
    const w = new World();
    const first = await w.load();
    await w.offline(first.engine);
    const bytes = bytesOf(900);
    const ref = await first.engine.stageBlob(KS, bytes, { contentType: "image/png" });
    const { mutationId } = await submitWith(first.engine, [ref.blobId]);
    expect(blobDbs(w.idb)).toHaveLength(1); // the default stash of the host
    await first.engine.stop();
    // the page is reloaded: nothing of the first engine is left but IndexedDB
    const second = await w.load({ start: false });
    expect(second.engine.outbox().map((e) => e.mutationId)).toEqual([mutationId]);
    expect(await second.engine.stagedBlobs()).toMatchObject([{ keyspaceId: KS, blobId: ref.blobId, size: 900 }]);
    second.engine.start();
    await w.advanceUntil(second.engine, 5000, () => committed(second.events, mutationId));
    expect(committed(second.events, mutationId)).toBe(true);
    // the bytes were uploaded before the action was sent: the server never had to answer BLOB_MISSING or BLOB_PENDING (14 [CLI-44])
    expect(w.server.calls.map((c) => c.method).filter((m) => ["createUpload", "commitBlob", "mutate"].includes(m))).toEqual(["createUpload", "commitBlob", "mutate"]);
    expect(Buffer.from(w.server.blobs.object(KS, ref.blobId) as Uint8Array).equals(Buffer.from(bytes))).toBe(true);
    expect(w.server.blobs.row(KS, ref.blobId)).toMatchObject({ state: "committed", contentType: "image/png" });
    // 09 [READ-94]: confirmed: the bytes go from IndexedDB
    for (let i = 0; i < 10 && (await second.engine.stagedBlobs()).length > 0; i++) await second.engine.settle();
    expect(await second.engine.stagedBlobs()).toEqual([]);
    await second.engine.stop();
  });

  it("a reload, and the server answers BLOB_MISSING to the action: the engine uploads the same bytes from IndexedDB again, and resends the identical action (14 [CLI-44])", async () => {
    const w = new World();
    const first = await w.load();
    await w.offline(first.engine);
    const bytes = bytesOf(300);
    const ref = await first.engine.stageBlob(KS, bytes);
    const { mutationId } = await submitWith(first.engine, [ref.blobId]);
    await first.engine.stop();
    const second = await w.load({ start: false });
    // the store lost the object after the commit (09 [READ-88]): the answer to the first send is BLOB_MISSING
    w.server.inject("mutate", {
      kind: "result",
      result: (m) => {
        if (m.mutationId !== mutationId) return undefined;
        w.server.blobs.rows.delete(`${KS}\u0000${ref.blobId}`);
        w.server.blobs.objects.delete(`${KS}\u0000${ref.blobId}`);
        return create(MutationResultSchema, { mutationId: m.mutationId, outcome: { case: "rejected", value: mk(Code.BLOB_MISSING) } });
      },
    });
    second.engine.start();
    await w.advanceUntil(second.engine, 5000, () => committed(second.events, mutationId));
    expect(committed(second.events, mutationId)).toBe(true);
    expect(calls(w.server, "createUpload")).toBe(2); // before the first send, and for the answer
    expect(putCount(w.server)).toBe(2);
    expect(Buffer.from(w.server.blobs.object(KS, ref.blobId) as Uint8Array).equals(Buffer.from(bytes))).toBe(true);
    const sent = w.server.calls.filter((c) => c.method === "mutate").flatMap((c) => (c.request as { mutations: { mutationId: string; blobRefs: string[] }[] }).mutations);
    expect(sent.filter((m) => m.mutationId === mutationId).map((m) => m.blobRefs)).toEqual([[ref.blobId], [ref.blobId]]); // the identical action
    expect(second.engine.status().holds).toEqual([]);
    await second.engine.stop();
  });

  it("the default of a platform that keeps nothing durable is the memory stash: the same restart loses the bytes (what the browser host fixes)", async () => {
    const w = new World();
    const principal = w.server.principal("ana");
    const mk_ = async (): Promise<Engine> => {
      const e = await Engine.create({ appId: "app", principal: { issuer: principal.issuer, sub: principal.sub, deviceId: principal.deviceId }, transport: w.server.transport(principal), manifests: w.manifests, clock: w.clock, random: w.random, blobs: { http: w.server.blobs.blobHttp() } });
      await e.addKeyspace(KS);
      e.start();
      await e.settle();
      return e;
    };
    const e = await mk_();
    await e.stageBlob(KS, bytesOf(10));
    expect(await e.stagedBlobs()).toHaveLength(1);
    expect(blobDbs(w.idb)).toEqual([]);
    expect(await (await mk_()).stagedBlobs()).toEqual([]);
  });
});

describe("14 [CLI-65], [CLI-66]: the staged bytes are erased with the partition and with the copy of a keyspace", () => {
  it("a sign-out that purges the partition deletes the database of its staged bytes with it", async () => {
    const w = new World();
    const a = await w.load();
    await a.engine.stageBlob(KS, bytesOf(50));
    await a.engine.stageBlob(KS, bytesOf(60));
    expect(await a.engine.stagedBlobs()).toHaveLength(2);
    expect(blobDbs(w.idb)).toHaveLength(1);
    expect(await a.engine.signOut()).toMatchObject({ done: true, outcome: "purged" });
    expect(blobDbs(w.idb)).toEqual([]);
    expect(partitionDbs(w.idb)).toEqual([]);
    // the next principal of the page finds none of it
    const b = await w.load({ sub: "bob" });
    expect(await b.engine.stagedBlobs()).toEqual([]);
  });

  it("a sign-out that discards unsynced actions erases the bytes that they carried, and hands the actions to the export hook", async () => {
    const w = new World();
    const exported: string[] = [];
    const a = await w.load({ exportHook: (x) => void exported.push(x.mutationId) });
    await w.offline(a.engine);
    const ref = await a.engine.stageBlob(KS, bytesOf(70));
    const { mutationId } = await submitWith(a.engine, [ref.blobId]);
    expect(blobDbs(w.idb)).toHaveLength(1);
    expect(await a.engine.signOut()).toMatchObject({ done: false, unsynced: 1 });
    expect(await a.engine.stagedBlobs()).toHaveLength(1); // nothing is erased until the app decides
    expect(await a.engine.signOut({ unsynced: "discard" })).toMatchObject({ done: true, outcome: "discarded" });
    expect(exported).toEqual([mutationId]);
    expect(blobDbs(w.idb)).toEqual([]);
  });

  it("the purge of a keyspace's copy erases the bytes staged in it that no action held by the engine names; those that an action names stay with it (09 [READ-94])", async () => {
    const w = new World();
    const a = await w.load();
    await w.offline(a.engine);
    const unnamed = await a.engine.stageBlob(KS, bytesOf(10));
    const named = await a.engine.stageBlob(KS, bytesOf(20));
    const { mutationId } = await submitWith(a.engine, [named.blobId]);
    // online again, with an upload that the server is slow to answer: the action waits for it, never sent, when the access is revoked
    w.server.inject("createUpload", { kind: "delay", ms: 3_600_000 });
    await w.advance(a.engine, 1000);
    expect(a.engine.status().connection).toBe("live");
    expect(calls(w.server, "mutate")).toBe(0);
    w.server.revokeKeyspaces(KS); // ACCESS_REVOKED: the purge list of 14 [CLI-66], and the quarantine of the unsent action
    await a.engine.settle();
    expect(a.engine.keyspaceInfo(KS)?.purged).toBe(true);
    expect(a.events.filter((e) => e.type === "exported").map((e) => (e as { mutationId: string }).mutationId)).toEqual([mutationId]);
    // 09 [READ-94], 14 [CLI-66]: the bytes of the action that was quarantined stay (it can be re-submitted); the others of the keyspace go
    for (let i = 0; i < 10 && (await a.engine.stagedBlobs()).length > 1; i++) await a.engine.settle();
    expect((await a.engine.stagedBlobs()).map((s) => s.blobId)).toEqual([named.blobId]);
    expect(await a.engine.stagedBlobs()).not.toContainEqual(expect.objectContaining({ blobId: unnamed.blobId }));
    await a.engine.stop();
  });
});
