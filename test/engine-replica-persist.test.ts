// 14 §14.10 Interest and storage: the persisted replica ([CLI-2], [CLI-3], [CLI-8], [CLI-74], [CLI-75]). A restart installs the pages of the
// last load and folds the persisted records again, asks whether it can resume from its cursor and the hash of the record at it, and
// is exactly the state that the server's fold gives.

import { create } from "@bufbuild/protobuf";
import * as Y from "yjs";
import { describe, expect, it } from "vitest";
import { MemoryStorage, set } from "../src/engine/index.js";
import { ChangeKind, KeyChangeSchema, KeyEntrySchema } from "../src/gen/abpc/v2/types_pb.js";
import { toHex } from "../src/rules/bytes.js";
import type { ReplicaState } from "../src/rules/fold/index.js";
import { contentOf, kc, KS, mutateRequests, ofType, rig, tick, types, type Rig } from "./helpers/engine.js";

/** Starts the engine again over the same storage, server and clock, as a restart of the process. */
async function restart(r: Rig, o: { start?: boolean; seed?: number; cfg?: Rig["config"] extends infer C ? Partial<C> : never } = {}): Promise<Rig> {
  await r.engine.stop();
  return rig({ storage: r.storage, server: r.server, clock: r.clock, seed: o.seed ?? 21, start: o.start ?? false, ...(o.cfg !== undefined ? { cfg: o.cfg } : {}) });
}

const stateOf = (r: Rig, ks = KS): ReplicaState => (r.engine as unknown as { follows: Map<string, { state: ReplicaState }> }).follows.get(ks)?.state as ReplicaState;
const loads = (r: Rig): { cursor: { afterSeq: bigint; logEpoch: number; afterRecordHash: Uint8Array; interestPrefixes: string[] } }[] =>
  r.server.calls.filter((c) => c.method === "loadState").map((c) => c.request as never);
const subscribes = (r: Rig): { keyspaces: { keyspaceId: string; afterSeq: bigint; logEpoch: number; afterRecordHash: Uint8Array }[]; clientId: string }[] =>
  r.server.calls.filter((c) => c.method === "subscribe").map((c) => c.request as never);

describe("14 [CLI-2], [CLI-8]: a restart resumes from the persisted replica", () => {
  it("the persisted confirmed state is shown before the engine connects, and authoring against it works offline, with its cursor and epoch as the base ([CLI-18])", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    r.server.write(KS, [kc("counters/c", "incr", 3)]);
    await r.engine.settle();
    const r2 = await restart(r);
    expect(r2.engine.cursor(KS)).toMatchObject({ loaded: true, seq: 2n, logEpoch: 1 });
    expect(r2.engine.view(KS).json("settings/a")).toBe(1);
    expect(r2.engine.status().connection).toBe("offline");
    const a = await r2.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] });
    const m = contentOf(r2, a.mutationId);
    expect(m.baseSeq).toBe(2n);
    expect(m.baseLogEpoch).toBe(1);
    expect(m.changes[0]?.seenVersion).toBe(1n); // taken from the persisted confirmed state ([CLI-17])
    expect(r2.engine.view(KS).json("settings/a")).toBe(2);
    r2.engine.start();
    await r2.engine.settle();
    expect(types(r2, a.mutationId)).toEqual(["committed"]);
  });

  it("it asks whether it can resume from the persisted cursor and the hash of the record at it, and subscribes from the same cursor: no cold load, no `reloaded`", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    r.server.write(KS, [kc("settings/b", "set", 1)]);
    await r.engine.settle();
    const hash = r.server.records(KS)[1]?.recordHash as Uint8Array;
    const r2 = await restart(r, { start: true });
    await r2.engine.settle();
    const load = loads(r2).at(-1)?.cursor;
    expect(load?.afterSeq).toBe(2n); // never after_seq = 0 (09 [READ-17])
    expect(load?.logEpoch).toBe(1);
    expect(toHex(load?.afterRecordHash as Uint8Array)).toBe(toHex(hash));
    const sub = subscribes(r2).at(-1)?.keyspaces[0];
    expect(sub).toMatchObject({ keyspaceId: KS, afterSeq: 2n, logEpoch: 1 });
    expect(toHex(sub?.afterRecordHash as Uint8Array)).toBe(toHex(hash));
    expect(ofType(r2, "reloaded")).toHaveLength(0);
    expect(r2.engine.status().connection).toBe("live");
  });

  it("records written while the engine was away are delivered from the cursor and applied once (a counter is never counted twice)", async () => {
    const r = await rig();
    r.server.write(KS, [kc("counters/c", "incr", 3)]);
    await r.engine.settle();
    await r.engine.stop();
    r.server.write(KS, [kc("counters/c", "incr", 2)]);
    r.server.write(KS, [kc("settings/n", "set", 7)]);
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 22 });
    expect(r2.engine.view(KS).json("counters/c")).toBe(5);
    expect(r2.engine.view(KS).json("settings/n")).toBe(7);
    expect(r2.engine.cursor(KS)?.seq).toBe(3n);
  });

  it("the restored state is the state that the server's fold gives: keys, tombstones of a deleted entity and its subtree, a counter and a CRDT document, byte for byte (the keyspace state hash)", async () => {
    const r = await rig();
    r.server.write(KS, [kc("issues/i1", "set", { t: 1 }), kc("issues/i1/title", "set", "a")]);
    r.server.write(KS, [kc("issues/i2", "set", { t: 1 }), kc("issues/i2/title", "set", "b")]);
    r.server.write(KS, [kc("counters/c", "incr", 4)]);
    r.server.write(KS, [kc("issues/i1", "delete")]); // an entity tombstone and the removal of its subtree
    r.server.write(KS, [kc("settings/x", "set", { b: 1, a: 2 })]);
    const d = new Y.Doc();
    const ups: Uint8Array[] = [];
    d.on("update", (u: Uint8Array) => ups.push(u));
    d.getText("text").insert(0, "hello");
    d.getText("text").insert(5, " world");
    for (const update of ups) r.server.write(KS, [create(KeyChangeSchema, { key: "docs/d1", kind: ChangeKind.CRDT_UPDATE, update, encoding: "yjs/v1" })]);
    await r.engine.settle();
    const live = toHex(stateOf(r).keyspaceStateHash());
    expect(live).toBe(toHex(r.server.stateOf(KS).keyspaceStateHash()));
    const r2 = await restart(r);
    expect(toHex(stateOf(r2).keyspaceStateHash())).toBe(live);
    expect(stateOf(r2).tombstone("issues/i1")?.kind).toBe("entity");
    expect(r2.engine.view(KS).has("issues/i1/title")).toBe(false);
    expect(r2.engine.view(KS).json("counters/c")).toBe(4);
    r2.engine.start();
    await r2.engine.settle();
    expect(r2.engine.crdt(KS, "docs/d1").doc.getText("text").toString()).toBe("hello world");
    expect(toHex(stateOf(r2).keyspaceStateHash())).toBe(live);
  });

  it("the image is the pages of the last load and the records since: a later load begins it again, with no tail", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    r.server.write(KS, [kc("settings/b", "set", 1)]);
    await r.engine.settle();
    const img = r.storage.replica(KS);
    expect(img?.base.checkpointSeq).toBe(0n);
    expect(img?.tail.map((x) => x.seq)).toEqual([1n, 2n]);
    expect(img?.head).toMatchObject({ seq: 2n, watermark: 2n, logEpoch: 1, interest: [] });
    r.server.restore(KS, 1n); // a new epoch: a cold load replaces the state and the image
    await r.engine.settle();
    const img2 = r.storage.replica(KS);
    expect(img2?.base).toMatchObject({ logEpoch: 2 });
    expect(img2?.tail.map((x) => x.seq)).toEqual([]);
    expect(img2?.head.logEpoch).toBe(2);
  });
});

describe("14 [CLI-51], [CLI-8]: a persisted replica that the server cannot resume is replaced by a load made as a resume check", () => {
  it("the epoch changed while the engine was away: `reloaded` with EPOCH_CHANGED, the load names the persisted cursor, and the state is the server's", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    r.server.write(KS, [kc("settings/b", "set", 1)]);
    await r.engine.settle();
    await r.engine.stop();
    r.server.restore(KS, 1n);
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 23 });
    expect(loads(r2).at(-1)?.cursor.afterSeq).toBe(2n);
    expect(ofType(r2, "reloaded").at(-1)).toMatchObject({ cause: "EPOCH_CHANGED", logEpoch: 2 });
    expect(r2.engine.view(KS).has("settings/b")).toBe(false);
    expect(r2.engine.view(KS).has("settings/a")).toBe(true);
    expect(r2.engine.cursor(KS)?.logEpoch).toBe(2);
  });

  it("the log was pruned above the cursor while the engine was away: `reloaded` with RESUME_TOO_OLD", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    await r.engine.stop();
    r.server.write(KS, [kc("settings/b", "set", 1)]);
    r.server.write(KS, [kc("settings/c", "set", 1)]);
    r.server.prune(KS, 3n, { judgeHorizon: false });
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 24 });
    expect(ofType(r2, "reloaded").at(-1)).toMatchObject({ cause: "RESUME_TOO_OLD", logEpoch: 1 });
    expect(r2.engine.view(KS).json("settings/c")).toBe(1);
  });

  it("a tail longer than the limit is not replayed: the keyspace is loaded as if the engine had no image (a cold load)", async () => {
    const r = await rig();
    for (let i = 0; i < 5; i++) r.server.write(KS, [kc(`settings/k${i}`, "set", i)]);
    await r.engine.settle();
    expect(r.storage.replica(KS)?.tail).toHaveLength(5);
    await r.engine.stop();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 25, cfg: { replicaTailLimit: 2 } });
    expect(ofType(r2, "reloaded").at(-1)).toMatchObject({ cause: "COLD_LOAD" });
    expect(loads(r2).at(-1)?.cursor.afterSeq).toBe(0n);
    expect(r2.engine.view(KS).json("settings/k4")).toBe(4);
    expect(r.storage.replica(KS)?.tail).toHaveLength(0); // the new load began the image again
  });

  it("a tail that grows past the limit while the engine runs is dropped: nothing is persisted until a load begins the image again, and a restart loads the keyspace", async () => {
    const r = await rig({ cfg: { replicaTailLimit: 3 } });
    for (let i = 0; i < 3; i++) r.server.write(KS, [kc(`settings/k${i}`, "set", i)]);
    await r.engine.settle();
    expect(r.storage.replica(KS)?.tail).toHaveLength(3);
    r.server.write(KS, [kc("settings/k3", "set", 3)]); // the fourth record passes the limit
    await r.engine.settle();
    expect(r.storage.replica(KS)).toBeUndefined();
    r.server.write(KS, [kc("settings/k4", "set", 4)]);
    await r.engine.settle();
    expect(r.storage.replica(KS)).toBeUndefined();
    expect(r.engine.view(KS).json("settings/k4")).toBe(4); // the engine itself goes on
    const r2 = await restart(r, { start: true });
    expect(ofType(r2, "reloaded").at(-1)).toMatchObject({ cause: "COLD_LOAD" });
    expect(r.storage.replica(KS)?.tail).toHaveLength(0);
  });

  it("an image that cannot be written is dropped, and a restart loads the keyspace again; the outbox is the durable truth, and is untouched", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/q", 1)] });
    await r.engine.settle();
    r.storage.failNext = 1; // the next commit is the one that appends the record to the image
    r.server.write(KS, [kc("settings/b", "set", 1)]);
    await r.engine.settle();
    expect(r.engine.internalErrorCount).toBeGreaterThan(0);
    expect(r.storage.replica(KS)).toBeUndefined();
    expect(r.engine.cursor(KS)?.seq).toBeGreaterThanOrEqual(2n); // the engine itself goes on
    r.server.write(KS, [kc("settings/c", "set", 1)]);
    await r.engine.settle();
    expect(r.storage.replica(KS)).toBeUndefined(); // nothing is persisted until a load begins the image again
    const r2 = await restart(r, { start: true });
    expect(ofType(r2, "reloaded").at(-1)).toMatchObject({ cause: "COLD_LOAD" });
    expect(r2.engine.view(KS).json("settings/c")).toBe(1);
    expect(types(r2, a.mutationId).length + types(r, a.mutationId).length).toBeGreaterThan(0);
  });

  it("an image that cannot be restored (its pages hold a base that is no Yjs update) is dropped without a crash, and the keyspace is loaded again; so is one whose head is behind its own tail", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    await r.engine.stop();
    const img = r.storage.replica(KS);
    if (img === undefined) throw new Error("no image");
    const bad = create(KeyEntrySchema, { key: "docs/bad", value: Uint8Array.of(1, 2, 3), encoding: "yjs/v1", version: 1n, crdtCoveredSeq: 1n });
    await r.storage.commit([{ op: "replicaBegin", keyspaceId: KS, head: img.head, base: { ...img.base, entries: [...img.base.entries, bad] } }]);
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 26 });
    expect(ofType(r2, "reloaded").at(-1)).toMatchObject({ cause: "COLD_LOAD" });
    expect(r2.engine.view(KS).json("settings/a")).toBe(1);
    expect(r2.engine.view(KS).has("docs/bad")).toBe(false);
    expect(r.storage.replica(KS)?.base.entries.some((e) => e.key === "docs/bad")).toBe(false); // the load began the image again
    // a head behind its own tail is not one that this engine wrote
    await r2.engine.stop();
    const good = r.storage.replica(KS);
    if (good === undefined) throw new Error("no image");
    r.server.write(KS, [kc("settings/b", "set", 1)]);
    const r3 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 27 });
    await r3.engine.stop();
    const tail = r.storage.replica(KS) as NonNullable<ReturnType<MemoryStorage["replica"]>>;
    expect(tail.tail.length).toBeGreaterThan(0);
    await r.storage.commit([{ op: "replicaHead", keyspaceId: KS, head: { ...tail.head, seq: 0n, watermark: 0n } }]);
    const r4 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 28 });
    expect(ofType(r4, "reloaded").at(-1)).toMatchObject({ cause: "COLD_LOAD" });
    expect(r4.engine.view(KS).json("settings/b")).toBe(1);
  });

  it("an action that retired after the engine saw its record is not retired again: the restart finds it in the journal and the image at its cursor", async () => {
    const r = await rig();
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["committed"]);
    const sent = mutateRequests(r).length;
    const r2 = await restart(r, { start: true });
    await r2.engine.settle();
    expect(types(r2, a.mutationId)).toEqual([]);
    expect(r2.engine.outbox()).toHaveLength(0);
    expect(r2.engine.journalEntries().map((j) => j.mutationId)).toEqual([a.mutationId]);
    expect(mutateRequests(r2)).toHaveLength(sent); // nothing was sent again
  });
});

describe("14 [CLI-74], [CLI-75]: storage keyed by origin, app and partition; the storage mode", () => {
  it("the storage mode is reported in the sync status: memory by default, and the persistence that the platform grants or refuses", async () => {
    const mem = await rig();
    expect(mem.engine.status().storageMode).toBe("memory");
    const best = await rig({ storage: new MemoryStorage({ mode: "best-effort" }) });
    expect(best.engine.status().storageMode).toBe("best-effort");
    expect((best.storage as MemoryStorage).persistenceRequests).toBe(1); // 14 [CLI-75]: the engine asks for persistent storage
    const granted = await rig({ storage: new MemoryStorage({ mode: "best-effort", grantPersistence: true }) });
    expect(granted.engine.status().storageMode).toBe("persisted");
    const refused = await rig({ storage: new MemoryStorage({ mode: "best-effort", grantPersistence: false }) });
    expect(refused.engine.status().storageMode).toBe("best-effort");
  });

  it("an action is accepted into memory in `memory` mode and reported so; it is never silently kept where nothing persists (14 [CLI-21], [CLI-75])", async () => {
    const r = await rig();
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    expect(r.engine.status().storageMode).toBe("memory");
    await tick(r, 0);
  });
});
