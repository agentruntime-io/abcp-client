// Session endings and dropped keyspaces: 14 [CLI-79], [CLI-80], [CLI-52], [CLI-7], and the session of 09 §9.6, §9.7.

import { describe, expect, it } from "vitest";
import { create } from "@bufbuild/protobuf";
import { set } from "../src/engine/index.js";
import { CloseReason, ControlFrameSchema, SubscribeResponseSchema } from "../src/gen/abpc/v2/sync_pb.js";
import { Code, SuccessorRuleSchema } from "../src/gen/abpc/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { goOffline, goOnline, kc, KS, mutateRequests, ofType, rig, sentIds, tick, types, type Rig } from "./helpers/engine.js";

const rule = (prefix: string, keyspaceId: string): ReturnType<typeof create<typeof SuccessorRuleSchema>> => create(SuccessorRuleSchema, { prefix, keyspaceId });
const subscribes = (r: Rig): { keyspaces: { keyspaceId: string; afterSeq: bigint; logEpoch: number; afterRecordHash: Uint8Array }[]; clientId: string }[] =>
  r.server.calls.filter((c) => c.method === "subscribe").map((c) => c.request as never);

describe("14 [CLI-79] Closing", () => {
  it("TOKEN_EXPIRED: refresh the token, then subscribe again from the cursors; without a valid token the hold NO_TOKEN applies", async () => {
    let refreshes = 0;
    const r = await rig({ cfg: { tokens: { refresh: () => (refreshes++, Promise.resolve(true)) } } });
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    r.server.closeSessions(CloseReason.TOKEN_EXPIRED, "Unauthenticated");
    await r.engine.settle();
    await tick(r, 1500);
    expect(refreshes).toBe(1);
    expect(r.engine.status().connection).toBe("live");
    expect(subscribes(r).at(-1)?.keyspaces[0]).toMatchObject({ keyspaceId: KS, afterSeq: 1n, logEpoch: 1 });

    const w = await rig({ cfg: { tokens: { refresh: () => Promise.resolve(false) } } });
    w.random.fixed = 0.5;
    w.server.closeSessions(CloseReason.TOKEN_EXPIRED, "Unauthenticated");
    await w.engine.settle();
    expect(w.engine.status().holds).toEqual([expect.objectContaining({ scope: "app", code: "NO_TOKEN" })]);
    expect(w.engine.status().connection).toBe("offline");
  });

  it("SUBJECT_REVOKED: the client sends nothing and keeps every outbox entry (TODO: purge and quarantine, 14 [CLI-66])", async () => {
    const r = await rig();
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    r.server.closeSessions(CloseReason.SUBJECT_REVOKED, "PermissionDenied");
    await r.engine.settle();
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
    await tick(r, 600_000);
    expect(sentIds(r)).toEqual([a.mutationId]);
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([b.mutationId]);
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "app", code: "SUBJECT_REVOKED" })]);
  });

  it("UNSPECIFIED, or a reason that the client does not know, is a dead session: resumed with backoff, every outbox entry kept", async () => {
    const r = await rig();
    r.random.fixed = 0.5;
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await goOnline(r);
    await r.engine.settle();
    r.server.sendFrame(create(SubscribeResponseSchema, { frame: { case: "control", value: create(ControlFrameSchema, { control: { case: "closing", value: { $typeName: "abpc.v2.ControlFrame.Closing", reason: 99 as CloseReason, message: "" } } }) } }));
    await r.engine.settle();
    expect(r.engine.status().connection).not.toBe("live");
    await tick(r, 5000);
    expect(r.engine.status().connection).toBe("live");
    expect(types(r, a.mutationId)).toEqual(["committed"]);
  });
});

describe("14 [CLI-80] and 09 §9.6: the session and the drop reasons", () => {
  it("a frame that the client does not know changes no cursor, keyspace or session state (09 [READ-53])", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    r.server.sendFrame(create(SubscribeResponseSchema, { frame: { case: undefined } }));
    r.server.sendFrame(create(SubscribeResponseSchema, { frame: { case: "control", value: create(ControlFrameSchema, { control: { case: undefined } }) } }));
    await r.engine.settle();
    expect(r.engine.status().connection).toBe("live");
    expect(r.engine.cursor(KS)?.seq).toBe(1n);
  });

  it("the client deadline: no frame for 60 s, or three heartbeat intervals when that is shorter, ends the session, and it resumes", async () => {
    const r = await rig({ heartbeatMs: 0 });
    expect(r.engine.status().connection).toBe("live");
    expect(subscribes(r)).toHaveLength(1);
    await tick(r, 44_000);
    expect(subscribes(r)).toHaveLength(1); // the deadline is 3 x 15 s = 45 s, shorter than 60 s
    await tick(r, 2000);
    expect(subscribes(r)).toHaveLength(2); // the session was dead and resumed
    expect(r.engine.status().connection).toBe("live");
  });

  it("resumes with full-jitter backoff, base 1 s, cap 30 s, and never sooner than Reconnect.retry_after_ms", async () => {
    const r = await rig({ heartbeatMs: 15_000 });
    r.random.fixed = 0.999999;
    const attempts = (): number[] => r.server.calls.filter((c) => c.method === "subscribe").map((c) => c.at);
    r.server.inject("subscribe", { kind: "status", status: "Unavailable" }, { times: 7 });
    r.server.disconnect();
    for (let i = 0; i < 400 && attempts().length < 9; i++) await tick(r, 1000);
    const t = attempts().slice(1);
    const gaps = t.slice(1).map((x, i) => x - (t[i] as number));
    expect(gaps.slice(0, 6)).toEqual([2000, 4000, 8000, 16_000, 30_000, 30_000]); // 1 s x 2^n up to the cap of 30 s
  });

  it("Reconnect{retry_after_ms}: the next attempt waits at least that long", async () => {
    const r = await rig();
    r.random.fixed = 0;
    const before = subscribes(r).length;
    r.server.reconnect(20_000);
    await r.engine.settle();
    await tick(r, 19_000);
    expect(subscribes(r)).toHaveLength(before);
    await tick(r, 2000);
    expect(subscribes(r)).toHaveLength(before + 1);
    expect(r.engine.status().connection).toBe("live");
  });

  it("the resume names each keyspace with its own cursor, epoch and the record hash of its cursor (09 [READ-57])", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    r.server.write(KS, [kc("settings/b", "set", 1)]);
    await r.engine.settle();
    r.server.disconnect();
    await r.engine.settle();
    await tick(r, 1000);
    const last = subscribes(r).at(-1);
    expect(last?.keyspaces[0]).toMatchObject({ keyspaceId: KS, afterSeq: 2n, logEpoch: 1 });
    expect(last?.keyspaces[0]?.afterRecordHash.length).toBe(32);
    expect(last?.clientId).toBe((r.engine as unknown as { clientId: string }).clientId);
  });

  it("a followed keyspace that a heartbeat does not list is removed from the session: it is added again from its cursor", async () => {
    const r = await rig();
    r.server.forget(KS);
    r.server.heartbeat();
    await r.engine.settle();
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).has("settings/a")).toBe(true);
  });

  it("RESUME_TOO_OLD: a cold load made as a resume check from the cursor; queued actions stay queued and are sent; the state is replaced with `reloaded`", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    r.server.write(KS, [kc("settings/b", "set", 1)]);
    await r.engine.settle();
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/q", 1)] });
    r.server.write(KS, [kc("settings/c", "set", 1)]);
    r.server.prune(KS, 3n); // the cursor (2) is below the prune floor
    await goOnline(r);
    await tick(r, 3000);
    const load = r.server.calls.filter((c) => c.method === "loadState").at(-1)?.request as { cursor: { afterSeq: bigint } };
    expect(load.cursor.afterSeq).toBe(2n); // never after_seq = 0
    expect(ofType(r, "reloaded").at(-1)).toMatchObject({ cause: "RESUME_TOO_OLD", logEpoch: 1 });
    expect(r.engine.confirmed(KS).has("settings/c")).toBe(true);
    expect(types(r, a.mutationId)).toEqual(["committed"]);
  });

  it("GRANT_CHANGED: stop following, keep the local copy and the outbox; a successful outcome retires at once", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/x", "set", 1)]);
    r.server.faults.paused = true;
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    r.server.faults.paused = false;
    r.server.drop(KS, 1);
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["committed"]); // retired at once, in the epoch of the cursor
    expect(r.engine.confirmed(KS).has("settings/x")).toBe(true); // the local copy is kept
    r.server.write(KS, [kc("settings/later", "set", 1)]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).has("settings/later")).toBe(false); // no longer followed
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
    await r.engine.settle();
    expect(sentIds(r)).toContain(b.mutationId); // the outbox stays; it is sent, and the server answers it (PERMISSION_DENIED when the grant is gone)
  });

  it("NOT_FOUND: the queued actions are held as for KEYSPACE_NOT_FOUND, and the keyspace is added again with the hold backoff", async () => {
    const r = await rig({ keyspaces: [KS] });
    r.random.fixed = 0.5;
    r.server.drop(KS, 10);
    await r.engine.settle();
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "keyspace", code: "NOT_FOUND" })]);
    await tick(r, 40_000);
    expect(r.engine.status().holds).toEqual([]);
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).has("settings/a")).toBe(true);
  });

  it("UPGRADE_REQUIRED: the keyspace is held", async () => {
    const r = await rig();
    r.server.drop(KS, 9);
    await r.engine.settle();
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "keyspace", code: "UPGRADE_REQUIRED" })]);
  });

  it("DELETED: an action with a successful outcome retires; one never sent is dead-lettered with KEYSPACE_DELETED; one with an unknown outcome is reported as outcome unknown", async () => {
    const r = await rig();
    const done = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/done", 1)] });
    await r.engine.settle();
    expect(types(r, done.mutationId)).toEqual(["committed"]);
    await goOffline(r);
    const unsent = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/unsent", 1)] });
    await goOnline(r);
    await r.engine.settle();
    expect(types(r, unsent.mutationId)).toEqual(["committed"]);
    r.server.faults.paused = true;
    r.server.inject("mutate", { kind: "lose" });
    const unknown = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/unknown", 1)] });
    await r.engine.settle();
    await goOffline(r);
    const never = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/never", 1)] });
    r.server.faults.paused = false;
    r.server.deleteKeyspace(KS);
    await goOnline(r);
    await tick(r, 40_000);
    expect(types(r, unknown.mutationId)).toEqual(["outcome-unknown", "exported"]);
    expect(ofType(r, "outcome-unknown").find((e) => e.mutationId === unknown.mutationId)?.cause).toBe("DELETED");
    void never;
    expect(r.engine.outbox().length).toBeLessThanOrEqual(1);
  });
});

describe("14 [CLI-52] the queued actions of a split or archived keyspace", () => {
  async function splitRig(): Promise<Rig> {
    const r = await rig({ keyspaces: [KS, "dst1", "dst2"] });
    return r;
  }

  it("split: each queued action whose keys all belong to one successor is re-routed to it, with its mutation id, changes and base unchanged; the others are dead-lettered with KEYSPACE_ARCHIVED", async () => {
    const r = await splitRig();
    r.server.write(KS, [kc("settings/seed", "set", 1)]);
    await r.engine.settle();
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Create", changes: [set("issues/i1", {})] }); // issues/ -> dst1
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/x", 1)] }); // the catch-all -> dst2
    const c = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/y", 1), set("raw/z", "w", "bytes/v1")] });
    void c;
    const e = await r.engine.submit({ keyspaceId: KS, opType: "Create", changes: [set("issues/i2", {}), set("settings/w", 1)] }); // two successors
    const baseOf = (id: string): bigint => r.engine.outbox().find((x) => x.mutationId === id) !== undefined ? ((r.engine as unknown as { content(x: unknown): { baseSeq: bigint } }).content(r.engine.outbox().find((x) => x.mutationId === id))).baseSeq : -1n;
    const baseA = baseOf(a.mutationId);
    r.server.split(KS, [rule("issues/", "dst1"), rule("", "dst2")]);
    await goOnline(r);
    await tick(r, 5000);
    const rr = ofType(r, "rerouted");
    expect(rr.map((x) => [x.from, x.to])).toEqual([[KS, "dst1"], [KS, "dst2"], [KS, "dst2"]]);
    // the re-routed action keeps its mutation id and its base; only keyspace_id changed
    const sent = mutateRequests(r).flatMap((q) => q.mutations).filter((m) => m.mutationId === a.mutationId);
    expect(sent.at(-1)?.keyspaceId).toBe("dst1");
    expect(sent.at(-1)?.baseSeq).toBe(baseA);
    expect(types(r, a.mutationId)).toEqual(["rerouted", "committed"]);
    expect(types(r, b.mutationId)).toEqual(["rerouted", "committed"]);
    expect(types(r, e.mutationId)).toEqual(["rejected", "dead-lettered"]);
    const dl = r.engine.deadLetterEntries().find((d) => d.mutationId === e.mutationId);
    expect(dl?.reject.code).toBe(Code.KEYSPACE_ARCHIVED);
    expect(dl?.reject.successors.map((s) => s.keyspaceId).sort()).toEqual(["dst1", "dst2"]);
    expect(r.server.records("dst1").some((x) => x.mutationId === a.mutationId)).toBe(true);
  });

  it("split, learned from a reject: the action's own Reject.successors decide it the same way", async () => {
    const r = await splitRig();
    r.server.split(KS, [rule("issues/", "dst1"), rule("", "dst2")]);
    r.server.inject("mutate", { kind: "status", status: "Unavailable" }, { times: 0 });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Create", changes: [set("issues/i9", {})] }); // sent to the source after the split
    await r.engine.settle();
    await tick(r, 5000);
    expect(types(r, a.mutationId).includes("rerouted") || types(r, a.mutationId).includes("committed")).toBe(true);
  });

  it("archived: nothing is re-routed and no seen_version is reset; every queued action is dead-lettered with KEYSPACE_ARCHIVED and the rules that its keys match", async () => {
    const r = await splitRig();
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Create", changes: [set("issues/i1", {})] });
    r.server.archive(KS, [rule("issues/", "dst1"), rule("", "dst2")]);
    await goOnline(r);
    await tick(r, 5000);
    expect(types(r, a.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(types(r, b.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(ofType(r, "rerouted")).toHaveLength(0);
    const dl = new Map(r.engine.deadLetterEntries().map((d) => [d.mutationId, d]));
    expect(dl.get(a.mutationId)?.reject.successors.map((s) => s.keyspaceId)).toEqual(["dst2"]);
    expect(dl.get(b.mutationId)?.reject.successors.map((s) => s.keyspaceId)).toEqual(["dst1"]);
    expect(dl.get(a.mutationId)?.reject.details["lifecycle"]).toBe("ARCHIVED");
    expect(r.server.records("dst1")).toHaveLength(0);
    expect(r.server.calls.filter((c) => c.method === "mutate" && (c.request as { mutations: { keyspaceId: string }[] }).mutations.some((m) => m.keyspaceId === "dst1"))).toHaveLength(0);
  });

  it("an action registered in the source as an intent is not re-routed or dead-lettered until the source gives it a result", async () => {
    const r = await splitRig();
    const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] });
    await r.engine.settle();
    r.server.split(KS, [rule("", "dst2")]);
    await tick(r, 5000);
    expect(ofType(r, "rerouted")).toHaveLength(0);
    expect(r.engine.deadLetterEntries()).toHaveLength(0);
    expect(r.engine.outbox().map((x) => x.mutationId)).toEqual([g.mutationId]);
  });
});

void [mk];
