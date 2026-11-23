// C16b, round 16 on draft 14: the engine's identifiers. 03 [ID-35] (a new `client_id` at every flusher handover, kept in memory only), [ID-32] (an action's
// mutation id unchanged across resends, restarts, client-id rotation, re-stamping, fork handling and re-routing) and [ID-40] (a cursor `log_epoch` of 0
// is a value that matches no epoch, 09 [READ-65]).

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { set } from "../src/engine/index.js";
import { decodeContent } from "../src/engine/entry.js";
import { ControlFrame_KeyspaceAddedSchema, ControlFrameSchema, SubscribeResponseSchema } from "../src/gen/abcp/v2/sync_pb.js";
import { Code, KeyspaceCursorSchema, SuccessorRuleSchema } from "../src/gen/abcp/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { goOffline, goOnline, KS, mutateRequests, ofType, rejectResult, rig, tick, types } from "./helpers/engine.js";
import { settleAll, tabsRig } from "./helpers/tabs.js";

const V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const hidden = { handOverWhenHidden: true } as const;
const setA = (key: string, v: unknown = 1): { keyspaceId: string; opType: string; changes: ReturnType<typeof set>[] } => ({ keyspaceId: KS, opType: "Set", changes: [set(key, v)] });
const rule = (prefix: string, keyspaceId: string): ReturnType<typeof create<typeof SuccessorRuleSchema>> => create(SuccessorRuleSchema, { prefix, keyspaceId });

describe("03 [ID-35], 14 [CLI-63], 10 [AUTH-19]: a new client_id at every flusher handover, in memory only", () => {
  it("a tab that handed the role over holds no client_id: a follower has no client instance, and the id of its former life is not kept", async () => {
    const r = tabsRig();
    const a = await r.open({ cfg: hidden });
    const b = await r.open({ cfg: hidden });
    expect(a.engine.clientId).toMatch(V4);
    expect(b.engine.clientId).toBe("");
    a.tab.setVisible(false);
    await settleAll(a.engine, b.engine);
    expect(a.engine.status().role).toBe("follower");
    expect(b.engine.status().role).toBe("flusher");
    expect(a.engine.clientId).toBe(""); // not the id that it sent under: that instance is over, and a follower is none
    expect(b.engine.clientId).toMatch(V4);
  });

  it("every handover, back and forth between two tabs, mints an id that no flusher of the device has had, and each is a version-4 UUID", async () => {
    const r = tabsRig();
    const a = await r.open({ cfg: hidden });
    const b = await r.open({ cfg: hidden });
    const seen = [a.engine.clientId];
    for (let round = 0; round < 3; round++) {
      a.tab.setVisible(false);
      await settleAll(a.engine, b.engine);
      expect(b.engine.status().role).toBe("flusher");
      seen.push(b.engine.clientId);
      a.tab.setVisible(true);
      b.tab.setVisible(false);
      await settleAll(a.engine, b.engine);
      expect(a.engine.status().role).toBe("flusher");
      seen.push(a.engine.clientId);
      b.tab.setVisible(true);
    }
    expect(seen).toHaveLength(7);
    expect(new Set(seen).size).toBe(7);
    for (const id of seen) expect(id).toMatch(V4);
  });

  it("an id that the engine's source gives again (a hook, a poor random source) is not used a second time by the engine that minted it: the tab that takes the role back has a new one (10 [AUTH-19]: it MUST NOT be reused)", async () => {
    const r = tabsRig();
    const fixed = "11111111-1111-4111-8111-111111111111";
    let calls = 0;
    const a = await r.open({ cfg: { handOverWhenHidden: true, newClientId: (): string => (calls++ < 2 ? fixed : `22222222-2222-4222-8222-${String(calls).padStart(12, "0")}`) } });
    const b = await r.open({ cfg: hidden });
    expect(a.engine.clientId).toBe(fixed);
    a.tab.setVisible(false);
    await settleAll(a.engine, b.engine);
    expect(b.engine.status().role).toBe("flusher");
    a.tab.setVisible(true);
    b.tab.setVisible(false);
    await settleAll(a.engine, b.engine);
    expect(a.engine.status().role).toBe("flusher");
    expect(calls).toBeGreaterThanOrEqual(2); // the source was asked again, and gave the first id again
    expect(a.engine.clientId).not.toBe(fixed);
    expect(a.engine.clientId).toMatch(V4);
  });

  it("the client_id is never persisted: no stored row of the partition carries it, before or after a handover", async () => {
    const r = tabsRig();
    const a = await r.open({ cfg: hidden });
    const b = await r.open({ cfg: hidden });
    await settleAll(a.engine, b.engine);
    r.server.disconnect();
    await settleAll(a.engine, b.engine);
    await a.engine.submit(setA("settings/a"));
    const c1 = a.engine.clientId;
    a.tab.setVisible(false);
    await settleAll(a.engine, b.engine);
    const c2 = b.engine.clientId;
    const stored = await r.device.storage.storageOf(a.engine.partitionKey()).load();
    const text = JSON.stringify(stored, (_k, v: unknown) => (typeof v === "bigint" ? v.toString() : v instanceof Uint8Array ? Array.from(v) : v));
    expect(text).not.toContain(c1);
    expect(text).not.toContain(c2);
    expect(stored.entries.every((e) => e.clientId === "")).toBe(true);
  });
});

describe("03 [ID-32]: an action's mutation id is never changed, by any path that sends it again or sends it elsewhere", () => {
  /** Every mutation id that a `Mutate` request carried is the id of an action that the app submitted. */
  function onlyAuthored(r: Awaited<ReturnType<typeof rig>>, authored: string[]): void {
    const wire = new Set(mutateRequests(r).flatMap((q) => q.mutations.map((m) => m.mutationId)));
    for (const id of wire) expect(authored).toContain(id);
  }

  it("fork handling and client-id rotation: CLIENT_FORKED, then CLIENT_UNKNOWN, on two actions in a row; the ids on the wire are the authored ones, each action's every send names the same id", async () => {
    const r = await rig();
    let n = 0;
    r.server.inject("mutate", { kind: "result", result: (m) => (n++ < 3 ? rejectResult(m, mk(n % 2 === 0 ? Code.CLIENT_FORKED : Code.CLIENT_UNKNOWN)) : undefined) }, { times: 3 });
    const a = await r.engine.submit(setA("settings/a"));
    const b = await r.engine.submit(setA("settings/b"));
    await r.engine.settle();
    await tick(r, 5000);
    expect(types(r, a.mutationId)).toEqual(["committed"]);
    expect(types(r, b.mutationId)).toEqual(["committed"]);
    const clients = new Set(mutateRequests(r).flatMap((q) => q.mutations.map((m) => m.clientId)));
    expect(clients.size).toBeGreaterThan(1); // the instance was rotated
    onlyAuthored(r, [a.mutationId, b.mutationId]);
    expect(mutateRequests(r).flatMap((q) => q.mutations).filter((m) => m.mutationId === a.mutationId).length).toBeGreaterThan(1); // a was sent more than once
  });

  it("re-routing to the successors of a split: the id on the wire in the successor is the authored one, for the action that is sent once and the one that was sent before the split", async () => {
    const r = await rig({ keyspaces: [KS, "dst1", "dst2"] });
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Create", changes: [set("issues/i1", {})] });
    const b = await r.engine.submit(setA("settings/x"));
    r.server.split(KS, [rule("issues/", "dst1"), rule("", "dst2")]);
    await goOnline(r);
    await tick(r, 5000);
    expect(ofType(r, "rerouted").map((e) => e.mutationId).sort()).toEqual([a.mutationId, b.mutationId].sort());
    onlyAuthored(r, [a.mutationId, b.mutationId]);
    expect(r.server.records("dst1").map((x) => x.mutationId)).toContain(a.mutationId);
    expect(r.server.records("dst2").map((x) => x.mutationId)).toContain(b.mutationId);
    // the entry in the outbox kept the id through the move, and what was stored decodes to it (a restart reads it back)
    const sent = mutateRequests(r).flatMap((q) => q.mutations).filter((m) => m.mutationId === a.mutationId);
    expect(sent.at(-1)?.keyspaceId).toBe("dst1");
  });

  it("re-routing keeps the id in what is stored: the content of the re-routed action decodes to its own id and the successor's keyspace", async () => {
    const r = await rig({ keyspaces: [KS, "dst1", "dst2"] });
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Create", changes: [set("issues/i1", {})] });
    r.server.split(KS, [rule("issues/", "dst1"), rule("", "dst2")]);
    await goOnline(r);
    await r.engine.settle();
    expect(ofType(r, "rerouted").map((e) => e.mutationId)).toEqual([a.mutationId]);
    const kept = r.engine.journalEntries().find((j) => j.mutationId === a.mutationId);
    expect(kept).toBeDefined();
    const m = decodeContent(kept?.content as Uint8Array);
    expect([m.mutationId, m.keyspaceId]).toEqual([a.mutationId, "dst1"]);
    expect(r.engine.deadLetterEntries()).toEqual([]);
  });

  it("the replacement of a rejected action is a new action with a new id; the action it replaces keeps its own", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(Code.SCHEMA_INVALID)) });
    const a = await r.engine.submit(setA("settings/a"));
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["rejected", "dead-lettered"]);
    const b = await r.engine.submit(setA("settings/a", 2));
    await r.engine.settle();
    expect(b.mutationId).not.toBe(a.mutationId);
    onlyAuthored(r, [a.mutationId, b.mutationId]);
  });

  it("a dead letter that the user resubmits is a new action with a new id (02 [ARC-50]); the id of the dead letter is its own", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(Code.SCHEMA_INVALID)) });
    const a = await r.engine.submit(setA("settings/a"));
    await r.engine.settle();
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual([a.mutationId]);
    const again = await r.engine.resubmitDeadLetter(a.mutationId);
    await r.engine.settle();
    expect(again.mutationId).not.toBe(a.mutationId);
    expect(types(r, again.mutationId)).toEqual(["committed"]);
    onlyAuthored(r, [a.mutationId, again.mutationId]);
  });

  it("a restart (the stored outbox is re-stamped under a new client_id) sends the same ids", async () => {
    const r = await rig();
    await goOffline(r);
    const a = await r.engine.submit(setA("settings/a"));
    const b = await r.engine.submit(setA("settings/b"));
    await r.engine.stop();
    const r2 = await rig({ server: r.server, clock: r.clock, manifests: r.manifests, storage: r.storage, keyspaces: [KS] });
    await tick(r2, 5000);
    expect(types(r2, a.mutationId)).toEqual(["committed"]);
    onlyAuthored(r2, [a.mutationId, b.mutationId]);
  });
});

describe("03 [ID-40], 09 [READ-65]: a cursor log_epoch of 0 matches no epoch", () => {
  const added = (keyspaceId: string, afterSeq: bigint, logEpoch: number): ReturnType<typeof create<typeof SubscribeResponseSchema>> =>
    create(SubscribeResponseSchema, {
      frame: {
        case: "control",
        value: create(ControlFrameSchema, {
          control: { case: "keyspaceAdded", value: create(ControlFrame_KeyspaceAddedSchema, { cursor: create(KeyspaceCursorSchema, { keyspaceId, afterSeq, logEpoch }) }) },
        }),
      },
    });

  it("a KeyspaceAdded whose cursor has epoch 0 is an epoch mismatch: the keyspace is held and loaded again, as for EPOCH_CHANGED; it is not taken as 'no epoch given'", async () => {
    const r = await rig();
    expect(r.engine.cursor(KS)?.logEpoch).toBe(1);
    r.server.sendFrame(added(KS, 0n, 0));
    await r.engine.settle();
    const held = r.events.filter((e) => e.type === "held" && e.code === "EPOCH_RESOLUTION");
    expect(held.length).toBeGreaterThan(0);
    expect(ofType(r, "reloaded").map((e) => e.cause)).toContain("EPOCH_CHANGED");
  });

  it("a KeyspaceAdded of the epoch that the client follows is taken as it was: nothing is held or loaded again", async () => {
    const r = await rig();
    const before = r.events.length;
    r.server.sendFrame(added(KS, 0n, 1));
    await r.engine.settle();
    expect(r.events.slice(before).filter((e) => e.type === "held" || e.type === "reloaded")).toEqual([]);
  });
});
