// 14 §14.6.2 HOLD (Table 14.1) and 14 §14.6.3 DEAD_LETTER ([CLI-46]).

import { describe, expect, it } from "vitest";
import { set, StaticManifests, type EngineEvent, type HeldEvent } from "../src/engine/index.js";
import { CloseReason } from "../src/gen/abpc/v2/sync_pb.js";
import { Code, ErrorClass } from "../src/gen/abpc/v2/types_pb.js";
import { reject as mk, rejectWith } from "../src/fake/index.js";
import { goOffline, KS, MANIFEST, mutateRequests, ofType, rejectResult, rig, sentIds, tick, types } from "./helpers/engine.js";

const held = (events: EngineEvent[]): HeldEvent[] => events.filter((e): e is HeldEvent => e.type === "held");
const lim = (maxOffline = 120, extra: Record<string, number> = {}): { limits: Record<string, number> } => ({ limits: { maxOfflineSeconds: maxOffline, ...extra } });

describe("14 [CLI-45] a hold keeps the action, stops sending in its scope, emits held, shows in the status, and is bounded", () => {
  it("UPGRADE_REQUIRED holds the keyspace: nothing is sent to it, other keyspaces go on; at the default bound a sent action is outcome unknown and one never sent is dead-lettered with the hold's code", async () => {
    const r = await rig({ keyspaces: [KS, "ks2"], serverOptions: lim() });
    r.server.inject("mutate", { kind: "result", result: (m) => (m.keyspaceId === KS ? rejectResult(m, mk(Code.UPGRADE_REQUIRED)) : undefined) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(held(r.events).map((e) => [e.state, e.scope, e.keyspaceId, e.code])).toEqual([["start", "keyspace", KS, "UPGRADE_REQUIRED"]]);
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "keyspace", keyspaceId: KS, code: "UPGRADE_REQUIRED" })]);
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] }); // accepted into the outbox, never sent
    const o = await r.engine.submit({ keyspaceId: "ks2", opType: "Set", changes: [set("settings/o", 1)] });
    await r.engine.settle();
    expect(types(r, o.mutationId)).toEqual(["committed"]);
    await tick(r, 60_000);
    expect(sentIds(r).filter((id) => id === b.mutationId)).toHaveLength(0);
    expect(sentIds(r).filter((id) => id === a.mutationId)).toHaveLength(1); // no resend while held: the keyspace is sent nothing
    await tick(r, 70_000); // past max_offline: the default bound
    expect(types(r, a.mutationId)).toEqual(["outcome-unknown", "exported"]);
    expect(ofType(r, "outcome-unknown").find((e) => e.mutationId === a.mutationId)?.cause).toBe("HOLD_BOUND");
    expect(types(r, b.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(ofType(r, "dead-lettered").find((e) => e.mutationId === b.mutationId)?.reason).toBe("UPGRADE_REQUIRED");
    expect(r.engine.outbox()).toHaveLength(0);
  });

  it("UNKNOWN_OP_TYPE: the client reloads the version that LoadState names; a type that it declares is resent with the hold backoff, one that is not declared is dead-lettered at once", async () => {
    const manifests = new StaticManifests(MANIFEST);
    const r = await rig({ manifests });
    r.random.fixed = 0.5;
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(Code.UNKNOWN_OP_TYPE)) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(manifests.reloads).toEqual([1]); // the version in force is the one that the client used, and it declares the type
    expect(held(r.events).map((e) => [e.state, e.scope, e.lane, e.code])).toEqual([["start", "lane", "SEQUENCED", "UNKNOWN_OP_TYPE"]]);
    expect(types(r, a.mutationId)).toEqual([]); // nothing is resent before the hold backoff has passed
    await tick(r, 29_000);
    expect(sentIds(r)).toEqual([a.mutationId]);
    await tick(r, 2000); // the first probe of the hold backoff: a bound from 1 min, drawn at 0.5
    expect(held(r.events).map((e) => [e.state, e.scope, e.lane, e.code])).toEqual([["start", "lane", "SEQUENCED", "UNKNOWN_OP_TYPE"], ["stop", "lane", "SEQUENCED", "UNKNOWN_OP_TYPE"]]);
    expect(types(r, a.mutationId)).toEqual(["committed"]); // a resend that gets another result clears the hold
    const r2 = await rig({ manifests: new StaticManifests(MANIFEST) });
    r2.manifests.reload = (v: number): Promise<boolean> => {
      void v;
      // the reload changes nothing; and the engine's own manifest no longer declares the type either
      (r2.manifests as unknown as { versions: Map<number, { ops: Map<string, unknown> }> }).versions.get(1)?.ops.delete("Set");
      return Promise.resolve(true);
    };
    r2.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(Code.UNKNOWN_OP_TYPE)) });
    const b = await r2.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r2.engine.settle();
    expect(types(r2, b.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(r2.engine.deadLetterEntries()[0]?.reason).toBe("UNKNOWN_OP_TYPE");
  });

  it("CLIENT_FORKED and CLIENT_UNKNOWN: the lane stops, the client re-identifies and re-stamps, and the hold clears at once", async () => {
    for (const code of [Code.CLIENT_FORKED, Code.CLIENT_UNKNOWN]) {
      const r = await rig();
      const before = (r.engine as unknown as { clientId: string }).clientId;
      r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(code)) });
      const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
      await r.engine.settle();
      await tick(r, 2000);
      expect(held(r.events).map((e) => [e.state, e.scope])).toEqual([["start", "lane"], ["stop", "lane"]]);
      expect((r.engine as unknown as { clientId: string }).clientId).not.toBe(before);
      expect(types(r, a.mutationId)).toEqual(["committed"]);
      // the mutation id and the content did not change; the slot is 1 of the new client instance
      const reqs = mutateRequests(r);
      expect(reqs[1]?.mutations[0]?.mutationId).toBe(a.mutationId);
      expect(reqs[1]?.mutations[0]?.clientId).not.toBe(reqs[0]?.mutations[0]?.clientId);
    }
  });

  it("KEYSPACE_NOT_FOUND: held for the wait of 07 [MUT-101] from the first answer; a resend that gets another result clears it, and at the bound a sent action is outcome unknown", async () => {
    const r = await rig({ serverOptions: lim(600, { keyspaceNotFoundWaitSeconds: 60 }) });
    r.random.fixed = 0.5;
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(Code.KEYSPACE_NOT_FOUND)) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(held(r.events)[0]).toMatchObject({ state: "start", scope: "keyspace", code: "KEYSPACE_NOT_FOUND" });
    expect(held(r.events)[0]?.bound).toBe(r.clock.now() + 60_000);
    await tick(r, 40_000); // the probe: a resend with the hold backoff
    expect(types(r, a.mutationId)).toEqual(["committed"]);
    expect(held(r.events).map((e) => e.state)).toEqual(["start", "stop"]);

    const w = await rig({ serverOptions: lim(600, { keyspaceNotFoundWaitSeconds: 60 }) });
    w.random.fixed = 0.999999;
    w.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(Code.KEYSPACE_NOT_FOUND)) }, { times: 1000 });
    const b = await w.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await w.engine.settle();
    await tick(r, 0);
    await tick(w, 65_000);
    expect(types(w, b.mutationId)).toEqual(["outcome-unknown", "exported"]);
    expect(ofType(w, "outcome-unknown")[0]?.cause).toBe("HOLD_BOUND");
  });

  it("Closing{CLIENT_TOO_OLD} holds every keyspace of the app: the client sends nothing", async () => {
    const r = await rig({ keyspaces: [KS, "ks2"], serverOptions: lim() });
    r.server.closeSessions(CloseReason.CLIENT_TOO_OLD, "FailedPrecondition");
    await r.engine.settle();
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "app", code: "CLIENT_TOO_OLD" })]);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const b = await r.engine.submit({ keyspaceId: "ks2", opType: "Set", changes: [set("settings/b", 1)] });
    const sent = mutateRequests(r).length;
    await r.engine.settle();
    expect(mutateRequests(r)).toHaveLength(sent);
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([a.mutationId, b.mutationId]);
  });

  it("UNSUPPORTED_FEATURE holds the lane: its head is resent unchanged with the hold backoff until a resend gets another result", async () => {
    const r = await rig();
    r.random.fixed = 0.999999;
    r.server.inject("mutate", { kind: "result", result: (m) => (m.changes[0]?.key === "settings/a" ? rejectResult(m, mk(Code.UNSUPPORTED_FEATURE)) : undefined) }, { times: 2 });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] }); // the GATED lane is not held
    await r.engine.settle();
    expect(sentIds(r)).toEqual([a.mutationId, g.mutationId]);
    const t0 = r.clock.now();
    await tick(r, 60_000);
    await tick(r, 120_000);
    const at = r.server.calls.filter((c) => c.method === "mutate" && (c.request as { mutations: { mutationId: string }[] }).mutations.some((m) => m.mutationId === a.mutationId)).map((c) => c.at - t0);
    expect(at.slice(1)).toEqual([60_000, 180_000]); // the hold backoff: bound from 1 min, doubling
    expect(types(r, a.mutationId)).toEqual(["committed"]);
    expect(held(r.events).filter((e) => e.code === "UNSUPPORTED_FEATURE").map((e) => e.state)).toEqual(["start", "stop"]);
  });

  it("HISTORY_DIVERGED holds the keyspace: it sends nothing, polls LoadState with the hold backoff, and cold-loads when the keyspace reopens in a new epoch", async () => {
    const r = await rig();
    r.random.fixed = 0.5;
    r.server.write(KS, [kcSet("settings/x", 1)]);
    await r.engine.settle();
    r.server.freeze(KS);
    await r.engine.settle();
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "keyspace", code: "HISTORY_DIVERGED" })]);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const loads = (): number => r.server.calls.filter((c) => c.method === "loadState").length;
    const l0 = loads();
    await tick(r, 40_000);
    expect(loads()).toBeGreaterThan(l0); // it learns of the reopening from a LoadState (09 [READ-70])
    expect(sentIds(r)).not.toContain(a.mutationId);
    r.server.reopen(KS);
    r.server.restore(KS, 1n);
    await tick(r, 130_000);
    expect(ofType(r, "reloaded").map((e) => e.cause)).toContain("EPOCH_CHANGED");
    expect(r.engine.status().holds.filter((h) => h.code === "HISTORY_DIVERGED")).toEqual([]);
    expect(r.engine.cursor(KS)?.logEpoch).toBe(2);
  });

  it("APP_SUSPENDED (a call's error detail, or Closing) holds every keyspace; the client keeps the outbox and retries with the hold backoff until ResumeApp", async () => {
    const r = await rig();
    r.random.fixed = 0.5;
    r.server.suspend();
    await r.engine.settle();
    expect(r.engine.status()).toMatchObject({ suspended: true, connection: "offline" });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await tick(r, 20_000);
    expect(sentIds(r)).toHaveLength(0);
    r.server.resume();
    await tick(r, 40_000);
    expect(r.engine.status().suspended).toBe(false);
    expect(types(r, a.mutationId)).toEqual(["committed"]);
    expect(held(r.events).filter((e) => e.code === "APP_SUSPENDED").map((e) => e.state)).toEqual(["start", "stop"]);
  });

  it("APP_SUSPENDED as the error of a Mutate call", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "status", status: "PermissionDenied", reject: mk(Code.APP_SUSPENDED) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "app", code: "APP_SUSPENDED" })]);
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([a.mutationId]);
  });

  it("QUOTA_EXCEEDED on Subscribe for the per-principal session cap holds the app; the client keeps its data and subscribes again once another session has ended", async () => {
    const first = await rig({ serverOptions: { maxSessions: 1 } });
    const second = await rig({ server: first.server, clock: first.clock, seed: 9, serverOptions: { maxSessions: 1 } });
    second.random.fixed = 0.5;
    expect(second.engine.status().holds).toEqual([expect.objectContaining({ scope: "app", code: "QUOTA_EXCEEDED" })]);
    const a = await second.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await second.engine.settle();
    expect(second.engine.outbox()).toHaveLength(1);
    await first.engine.stop();
    first.server.disconnect();
    await tick(second, 40_000);
    expect(second.engine.status().holds).toEqual([]);
    expect(types(second, a.mutationId)).toEqual(["committed"]);
  });

  it("Retry{AUTHORIZER_UNAVAILABLE} on Subscribe holds the app; a Subscribe that succeeds clears it", async () => {
    const r = await rig({ start: false });
    r.random.fixed = 0.5;
    r.server.inject("subscribe", { kind: "status", status: "Unavailable", retry: { $typeName: "abpc.v2.Retry", afterMs: 0, reason: 6 } as never });
    r.engine.start();
    await r.engine.settle();
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "app", code: "AUTHORIZER_UNAVAILABLE" })]);
    await tick(r, 40_000);
    expect(r.engine.status().holds).toEqual([]);
    expect(r.engine.status().connection).toBe("live");
  });

  it("a HOLD code that the client does not know, a reject of unknown class and one of class UNSPECIFIED hold the lane: the head is resent with the hold backoff and the app is told", async () => {
    for (const [code, cls, name] of [[900, ErrorClass.HOLD, "CODE_900"], [Code.STALE, 77, "CLASS_UNKNOWN"], [Code.SCHEMA_INVALID, ErrorClass.UNSPECIFIED, "CLASS_UNKNOWN"]] as const) {
      const r = await rig();
      r.random.fixed = 0.999999;
      r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, rejectWith(code, cls)) });
      const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
      await r.engine.settle();
      expect(held(r.events).map((e) => [e.state, e.scope, e.code])).toEqual([["start", "lane", name]]);
      await tick(r, 30_000);
      expect(mutateRequests(r)).toHaveLength(1);
      await tick(r, 40_000);
      expect(types(r, a.mutationId)).toEqual(["committed"]);
      expect(held(r.events).map((e) => e.state)).toEqual(["start", "stop"]);
    }
  });

  it("a hold on a class that the client does not know ends at its bound too: dead-lettered, or outcome unknown for a sent action", async () => {
    const r = await rig({ serverOptions: lim() });
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, rejectWith(900, 77)) }, { times: 1000 });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
    await tick(r, 200_000);
    expect(types(r, a.mutationId)).toEqual(["outcome-unknown", "exported"]);
    expect(types(r, b.mutationId)).toEqual(["rejected", "dead-lettered"]);
  });

  it("a DropReason that the client does not know: it stops following, keeps its copy, holds the queued actions, reports, and adds the keyspace again with the hold backoff (15 [COMPAT-42])", async () => {
    const r = await rig();
    r.random.fixed = 0.5;
    r.server.write(KS, [kcSet("settings/x", 1)]);
    await r.engine.settle();
    r.server.drop(KS, 99);
    await r.engine.settle();
    expect(held(r.events).map((e) => [e.state, e.scope, e.code])).toEqual([["start", "keyspace", "DROP_REASON_99"]]);
    expect(r.engine.confirmed(KS).has("settings/x")).toBe(true); // the local copy stays
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(sentIds(r)).toHaveLength(0);
    await tick(r, 40_000);
    expect(types(r, a.mutationId)).toEqual(["committed"]);
    expect(held(r.events).map((e) => e.state)).toEqual(["start", "stop"]);
  });

  it("NO_TOKEN: a second Unauthenticated for one request stops it and reports; the client asks for a token with the hold backoff", async () => {
    let refreshes = 0;
    const r = await rig({ cfg: { tokens: { refresh: () => (refreshes++, Promise.resolve(true)) } } });
    r.random.fixed = 0.5;
    r.server.inject("mutate", { kind: "status", status: "Unauthenticated" }, { times: 2 });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 1);
    expect(refreshes).toBe(1); // one refresh, then one resend (02 [ARC-51] item 1)
    expect(held(r.events).map((e) => [e.state, e.scope, e.code])).toEqual([["start", "app", "NO_TOKEN"]]);
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([a.mutationId]);
    await tick(r, 40_000);
    expect(refreshes).toBeGreaterThanOrEqual(2);
    expect(types(r, a.mutationId)).toEqual(["committed"]);
    expect(held(r.events).map((e) => e.state)).toEqual(["start", "stop"]);
  });
});

describe("14 [CLI-46] DEAD_LETTER: the special cases", () => {
  it("PERMISSION_DENIED: refresh the token once and resend before treating the result as final", async () => {
    let refreshes = 0;
    const r = await rig({ cfg: { tokens: { refresh: () => (refreshes++, Promise.resolve(true)) } } });
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(Code.PERMISSION_DENIED)) }, { times: 2 });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 1);
    expect(refreshes).toBe(1);
    expect(sentIds(r)).toEqual([a.mutationId, a.mutationId]);
    expect(types(r, a.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(r.engine.deadLetterEntries()[0]?.reason).toBe("PERMISSION_DENIED");
  });

  it("PERMISSION_DENIED that the refreshed token cures: the resend succeeds", async () => {
    const r = await rig({ cfg: { tokens: { refresh: () => Promise.resolve(true) } } });
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(Code.PERMISSION_DENIED)) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 1);
    expect(types(r, a.mutationId)).toEqual(["committed"]);
  });

  it("ACTOR_MISMATCH: a new client_id and a re-stamp, and the action is resent once under it before the result is final", async () => {
    const r = await rig();
    const before = (r.engine as unknown as { clientId: string }).clientId;
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(Code.ACTOR_MISMATCH)) }, { times: 2 });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 1);
    const reqs = mutateRequests(r);
    expect(reqs).toHaveLength(2);
    expect(reqs[1]?.mutations[0]?.clientId).not.toBe(before);
    expect(reqs[1]?.mutations[0]?.mutationId).toBe(a.mutationId);
    expect(types(r, a.mutationId)).toEqual(["rejected", "dead-lettered"]); // a second reject of the same code is final
  });

  it("OFFLINE_TOO_LONG: the action and its dependents go to the export hook, never to the dead-letter store", async () => {
    const exported: string[] = [];
    const r = await rig({ cfg: { exportHook: (e) => void exported.push(e.mutationId) } });
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] });
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, mk(Code.OFFLINE_TOO_LONG, { details: { cause: "HORIZON" } })) });
    await tick(r, 2000);
    expect(r.engine.status().deadLetters).toBe(0);
    expect(r.engine.exportEntries().map((e) => e.mutationId)).toEqual([a.mutationId, b.mutationId]);
    expect(exported).toEqual([a.mutationId, b.mutationId]);
    expect(types(r, a.mutationId)).toEqual(["outcome-unknown", "exported"]);
    expect(types(r, b.mutationId)).toEqual(["rejected", "exported"]);
    expect(r.engine.exportEntries()[1]?.dependencyOf).toBe(a.mutationId);
  });
});

import { kc } from "./helpers/engine.js";
import type { KeyChange } from "../src/gen/abpc/v2/types_pb.js";
function kcSet(key: string, v: unknown): KeyChange {
  return kc(key, "set", v);
}
