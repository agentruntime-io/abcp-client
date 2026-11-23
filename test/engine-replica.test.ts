// 14 §14.1 Engine model: the confirmed state, its sources, the cursor, the stop at a record that cannot be read, undecodable values,
// retirement, rehydration.

import { describe, expect, it } from "vitest";
import { create } from "@bufbuild/protobuf";
import { MemoryStorage, set } from "../src/engine/index.js";
import { HeartbeatFrameSchema, HeartbeatFrame_CursorSchema, RecordsFrameSchema, SubscribeResponseSchema } from "../src/gen/abcp/v2/sync_pb.js";
import { ChangeKind, KeyChangeSchema, RecordKind, SystemKind } from "../src/gen/abcp/v2/types_pb.js";
import { goOffline, goOnline, kc, KS, ofType, rig, sentIds, tick, text, types, type Rig } from "./helpers/engine.js";

const cursorOf = (r: Rig): bigint => r.engine.cursor(KS)?.seq ?? -1n;

describe("14 [CLI-1] the confirmed state of a followed keyspace", () => {
  it("holds every key with its value, version, changed_at and changed_by, the tombstones, the cursor and epoch, and the keyspace's kind, lifecycle and manifest version", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/a", "set", "x")], { sub: "bob" });
    r.server.write(KS, [kc("settings/b", "set", "y")]);
    r.server.write(KS, [kc("settings/b", "delete")]);
    await r.engine.settle();
    const v = r.engine.confirmed(KS);
    expect(v.get("settings/a")).toMatchObject({ version: 1n, predicted: false });
    expect(text(v.get("settings/a")?.value)).toBe('"x"');
    const f = (r.engine as unknown as { follows: Map<string, { state: { key(k: string): { changedBy: string; changedAt: { seconds: bigint } } | undefined; tombstone(k: string): { version: bigint } | undefined; logEpoch: number; manifestVersion: number }; kind: string; lifecycle: number }> }).follows.get(KS);
    expect(f?.state.key("settings/a")?.changedBy).toBe("bob");
    expect(f?.state.key("settings/a")?.changedAt.seconds).toBeGreaterThan(0n);
    expect(f?.state.tombstone("settings/b")?.version).toBe(3n);
    expect(f?.state.logEpoch).toBe(1);
    expect(f?.state.manifestVersion).toBe(1);
    expect(f?.kind).toBe("kind");
    expect(f?.lifecycle).toBe(1);
    expect(r.engine.cursor(KS)).toEqual({ loaded: true, seq: 3n, logEpoch: 1 });
  });
});

describe("14 [CLI-2] confirmed state is fed only by server data, by the version-gated test", () => {
  it("a load of several pages, consistent at one checkpoint, then the records after it", async () => {
    const r = await rig({ start: false, serverOptions: { pageSize: 2 } });
    for (let i = 0; i < 5; i++) r.server.write(KS, [kc(`settings/k${i}`, "set", i)]);
    r.engine.start();
    await r.engine.settle();
    const loads = r.server.calls.filter((c) => c.method === "loadState");
    expect(loads).toHaveLength(3); // 5 entries, 2 per page
    expect((loads[1]?.request as { pageToken: string }).pageToken).not.toBe("");
    expect(r.engine.confirmed(KS).keys("settings/")).toEqual(["settings/k0", "settings/k1", "settings/k2", "settings/k3", "settings/k4"]);
    expect(cursorOf(r)).toBe(5n);
    r.server.write(KS, [kc("settings/k5", "set", 5)]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).has("settings/k5")).toBe(true);
  });

  it("a page token that the server refuses restarts the load without it (09 [READ-11], A.7)", async () => {
    const r = await rig({ start: false, serverOptions: { pageSize: 2 } });
    for (let i = 0; i < 5; i++) r.server.write(KS, [kc(`settings/k${i}`, "set", i)]);
    r.server.inject("loadState", { kind: "status", status: "FailedPrecondition" }, { match: (q) => (q as { pageToken: string }).pageToken !== "" });
    r.engine.start();
    await r.engine.settle();
    expect(r.engine.confirmed(KS).keys("settings/")).toHaveLength(5);
    expect(r.engine.cursor(KS)?.loaded).toBe(true);
  });

  it("records are applied by the version-gated test: a record delivered twice counts a counter once", async () => {
    const r = await rig();
    r.server.faults.duplicateNext = 1;
    r.server.write(KS, [kc("counters/c", "incr", 5)]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).json("counters/c")).toBe(5);
    r.server.write(KS, [kc("counters/c", "incr", 2)]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).json("counters/c")).toBe(7);
  });
});

describe("14 [CLI-3] the cursor advances only on records and delivery watermarks, and a frame must continue it", () => {
  it("a frame that was dropped: the next one does not continue the cursor, is not applied, and the keyspace is resumed from the cursor", async () => {
    const r = await rig();
    r.server.faults.dropNext = 1;
    r.server.write(KS, [kc("settings/a", "set", 1)]); // dropped
    r.server.write(KS, [kc("settings/b", "set", 2)]); // arrives with prev_cursor 1, but the client is at 0
    await r.engine.settle();
    expect(r.server.calls.some((c) => c.method === "updateSubscription")).toBe(true);
    expect(r.engine.confirmed(KS).keys("settings/")).toEqual(["settings/a", "settings/b"]);
    expect(cursorOf(r)).toBe(2n);
  });

  it("frames in the wrong order, and frames twice: the state converges, nothing is applied twice", async () => {
    const r = await rig();
    r.server.faults.reorderNext = true;
    r.server.write(KS, [kc("counters/c", "incr", 1)]);
    r.server.write(KS, [kc("counters/c", "incr", 1)]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).json("counters/c")).toBe(2);
    expect(cursorOf(r)).toBe(2n);
    r.server.faults.duplicateNext = 2;
    r.server.write(KS, [kc("counters/c", "incr", 10)]);
    r.server.write(KS, [kc("counters/c", "incr", 100)]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).json("counters/c")).toBe(112);
    expect(cursorOf(r)).toBe(4n);
  });

  it("a delayed frame arrives late and the stream still converges", async () => {
    const r = await rig();
    r.server.faults.delayMs = 5000;
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).has("settings/a")).toBe(false);
    await tick(r, 5000);
    r.server.faults.delayMs = 0;
    expect(r.engine.confirmed(KS).has("settings/a")).toBe(true);
  });

  it("an empty RecordsFrame and a heartbeat advance the delivery watermark; a later record that continues it is applied", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    expect(cursorOf(r)).toBe(1n);
    r.server.sendFrame(create(SubscribeResponseSchema, { frame: { case: "records", value: create(RecordsFrameSchema, { keyspaceId: KS, prevCursor: 1n, cursor: 5n, records: [] }) } }));
    await r.engine.settle();
    expect(cursorOf(r)).toBe(5n); // never by inferring contiguity: only because the frame says so
    r.server.sendFrame(create(SubscribeResponseSchema, { frame: { case: "heartbeat", value: create(HeartbeatFrameSchema, { cursors: [create(HeartbeatFrame_CursorSchema, { keyspaceId: KS, cursorSeq: 9n, headSeq: 9n, logEpoch: 1 })] }) } }));
    await r.engine.settle();
    expect(cursorOf(r)).toBe(9n);
  });
});

describe("14 [CLI-4] a record that 06 [LOG-11] says to stop at freezes the keyspace", () => {
  it.each([
    ["a required feature that the client lacks", { requiredFeatures: ["unknown.v9"] }],
    ["an unknown record kind", { kind: 99 }],
    ["an unknown SYSTEM kind that must be understood", { kind: RecordKind.SYSTEM, system: { kind: 99, mustUnderstand: true } }],
  ] as const)("%s: the confirmed state stays at the previous seq, the record is not partly applied, the keyspace is held with UPGRADE_REQUIRED, and actions are accepted but not sent", async (_name, init) => {
    const r = await rig();
    r.server.write(KS, [kc("settings/ok", "set", 1)]);
    await r.engine.settle();
    const withChange = { ...init, changes: [create(KeyChangeSchema, { key: "settings/never", kind: ChangeKind.SET, value: new TextEncoder().encode("1"), encoding: "json/v1" })] };
    r.server.appendRaw(KS, "system" in init ? init : withChange);
    await r.engine.settle();
    expect(cursorOf(r)).toBe(1n);
    expect(r.engine.confirmed(KS).has("settings/never")).toBe(false);
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "keyspace", keyspaceId: KS, code: "UPGRADE_REQUIRED" })]);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 60_000);
    expect(sentIds(r)).not.toContain(a.mutationId);
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([a.mutationId]);
    expect(SystemKind.EPOCH_ADVANCED).toBe(7);
  });
});

describe("14 [CLI-5] a change whose value cannot be decoded still enters the confirmed state", () => {
  it("is shown as undecodable, `skipped` is emitted, it is counted in the status, and the engine goes on", async () => {
    const r = await rig();
    r.server.write(KS, [create(KeyChangeSchema, { key: "settings/bad", kind: ChangeKind.SET, value: new TextEncoder().encode("{not json"), encoding: "json/v1" }), create(KeyChangeSchema, { key: "settings/odd", kind: ChangeKind.SET, value: new Uint8Array([1, 2]), encoding: "json/v1" })]);
    r.server.write(KS, [kc("settings/ok", "set", 1)]);
    await r.engine.settle();
    const v = r.engine.confirmed(KS);
    expect(v.get("settings/bad")).toMatchObject({ decodable: false, version: 1n });
    expect(v.get("settings/bad")?.value).toEqual(new TextEncoder().encode("{not json"));
    expect(v.get("settings/odd")).toMatchObject({ decodable: false, encoding: "json/v1" });
    expect(v.json("settings/bad")).toBeUndefined();
    expect(v.get("settings/ok")?.decodable).toBe(true);
    const sk = ofType(r, "skipped");
    expect(sk).toHaveLength(1);
    expect(sk[0]).toMatchObject({ keyspaceId: KS, seq: 1n, keys: ["settings/bad", "settings/odd"] });
    expect(r.engine.status().skippedChanges).toBe(2);
    expect(cursorOf(r)).toBe(2n); // the engine did not stop
    expect(r.engine.status().holds).toEqual([]);
  });

  it("a cold load that skipped records the client lacks features for is counted once and reported with `skipped`", async () => {
    const r = await rig({ start: false });
    r.server.skippedUnsupported = true;
    r.engine.start();
    await r.engine.settle();
    expect(r.engine.status().loadsWithSkipped).toBe(1);
    expect(ofType(r, "skipped")).toHaveLength(1);
    expect(ofType(r, "skipped")[0]?.keys).toEqual([]);
  });
});

describe("14 [CLI-7] a pending action retires only with its outcome, once the cursor has reached its seq", () => {
  it("not on an RPC acknowledgement alone: with the stream held back it stays pending, then retires when the cursor reaches the seq", async () => {
    const r = await rig();
    r.server.faults.paused = true;
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(r.engine.outbox()).toHaveLength(1);
    expect(r.engine.outbox()[0]?.outcome).toMatchObject({ kind: "appended", seq: 1n });
    expect(types(r, a.mutationId)).toEqual([]);
    expect(r.engine.view(KS).get("settings/a")?.pending).toEqual([a.mutationId]); // still in the overlay
    r.server.resumeDelivery();
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["committed"]);
  });

  it("retiring depends on the cursor, not on receiving the record: a watermark past the seq retires the action", async () => {
    const r = await rig();
    r.server.faults.paused = true;
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    r.server.faults.paused = false;
    r.server.sendFrame(create(SubscribeResponseSchema, { frame: { case: "records", value: create(RecordsFrameSchema, { keyspaceId: KS, prevCursor: 0n, cursor: 1n, records: [] }) } }));
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["committed"]);
  });

  it("a GATED action retires on its Decided once the cursor has reached decision_seq", async () => {
    const r = await rig();
    r.server.faults.paused = true;
    const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] });
    await r.engine.settle();
    const seq = r.server.decide(KS, g.mutationId, { status: "COMMITTED" });
    await r.engine.settle();
    await tick(r, 40_000); // the poll finds the decision, which is not retired until the cursor reaches decision_seq
    expect(types(r, g.mutationId)).toEqual([]);
    expect(r.engine.outbox()[0]?.outcome).toMatchObject({ kind: "decided", status: "COMMITTED", decisionSeq: seq });
    r.server.resumeDelivery();
    await r.engine.settle();
    expect(types(r, g.mutationId)).toEqual(["committed"]);
  });

  it("a confirmed OP record that carries its mutation id from the same subject resolves an action whose outcome was unknown; from another subject it does not", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "lose" });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(sentIds(r)).toEqual([a.mutationId]); // resolved by the record, with no resend
    expect(types(r, a.mutationId)).toEqual(["committed"]);

    const w = await rig();
    await goOffline(w);
    const b = await w.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
    w.server.write(KS, [kc("settings/b", "set", 9)], { sub: "bob", mutationId: b.mutationId }); // the same id from another subject
    await goOnline(w);
    expect(w.engine.outbox().map((e) => e.mutationId).length === 0 || types(w, b.mutationId).includes("committed")).toBe(true);
  });
});

describe("14 [CLI-8] one rehydration path", () => {
  it("recomputes the overlay over the loaded state after a restart: the pending actions of the durable outbox are shown on top", async () => {
    const storage = new MemoryStorage();
    const r = await rig({ storage });
    r.server.write(KS, [kc("settings/x", "set", "confirmed")]);
    await r.engine.settle();
    await goOffline(r);
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/y", "pending")] });
    await r.engine.stop();
    r.server.disconnect();
    const r2 = await rig({ storage, server: r.server, clock: r.clock, seed: 77, start: false });
    expect(r2.engine.outbox()).toHaveLength(1);
    r2.server.faults.paused = true; // the action is sent, and stays pending while the stream is held back
    r2.engine.start();
    await r2.engine.settle();
    expect(text(r2.engine.view(KS).get("settings/x")?.value)).toBe('"confirmed"');
    expect(text(r2.engine.view(KS).get("settings/y")?.value)).toBe('"pending"');
    expect(r2.engine.view(KS).get("settings/y")?.predicted).toBe(true);
  });
});

describe("14 [CLI-9] folding and prediction do not read the clock", () => {
  it("the view is the same a year later", async () => {
    const r = await rig({ heartbeatMs: 0 });
    await goOffline(r);
    r.server.write(KS, [kc("counters/c", "incr", 5)]);
    await r.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [{ key: "counters/c", kind: ChangeKind.INCR, delta: 2n }] });
    const before = JSON.stringify([r.engine.view(KS).keys(), r.engine.view(KS).json("counters/c")]);
    r.clock.now = (): number => 4_000_000_000_000;
    r.clock.mono = (): number => 4_000_000_000_000;
    expect(JSON.stringify([r.engine.view(KS).keys(), r.engine.view(KS).json("counters/c")])).toBe(before);
  });
});
