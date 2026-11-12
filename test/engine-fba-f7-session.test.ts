// FBA batch F7, the session: B3-M1 (09 [READ-39]), B3-M2 (09 [READ-62]), B3-M3 (A.7, 09 [READ-35]).

import { describe, expect, it } from "vitest";
import type { Transport } from "../src/engine/index.js";
import { FakeServer, ManualClock, reject as mk } from "../src/fake/index.js";
import { Code } from "../src/gen/abpc/v2/types_pb.js";
import type { UpdateSubscriptionRequest } from "../src/gen/abpc/v2/sync_pb.js";
import { StaticManifests } from "../src/engine/index.js";
import { KS, MANIFEST, ofType, rig, tick } from "./helpers/engine.js";

const adds = (r: { server: FakeServer }, id: string): number =>
  r.server.calls.filter((c) => c.method === "updateSubscription" && (c.request as UpdateSubscriptionRequest).add.some((a) => a.keyspaceId === id)).length;

/** The transport of the principal, which counts the `UpdateSubscription` calls in flight. */
function counting(t: Transport, lateAnswers = false): { transport: Transport; max: () => number } {
  let now = 0;
  let max = 0;
  const transport = new Proxy(t, {
    get(target, prop, recv) {
      const v = Reflect.get(target, prop, recv) as unknown;
      if (prop !== "updateSubscription" || typeof v !== "function") return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(target) : v;
      return async (...args: unknown[]): Promise<unknown> => {
        now++;
        max = Math.max(max, now);
        try {
          const resp = await (v as (...a: unknown[]) => Promise<unknown>).apply(target, args);
          if (lateAnswers) await new Promise((res) => setTimeout(res, 5)); // the frames that the call caused are read first (09 [READ-39]: either order)
          return resp;
        } finally {
          now--;
        }
      };
    },
  });
  return { transport, max: () => max };
}

describe("B3-M1: 09 [READ-39] adds and drops are taken from frames, not from the unary response", () => {
  it("READ39_AKeyspaceDroppedForGoodIsNotAddedAgainOnEveryHeartbeat", async () => {
    // a Subscribe of one cursor: ks2 is named in an add on each new session (09 [READ-57]). The unary answer of an add comes after the frames that it causes.
    const clock = new ManualClock();
    const manifests = new StaticManifests(MANIFEST);
    const server = new FakeServer({ manifests, clock, heartbeatMs: 15_000 });
    for (const k of [KS, "ks2"]) server.createKeyspaceNow(k);
    const c = counting(server.transport(server.principal("ana")), true);
    const r = await rig({ server, clock, manifests, keyspaces: [KS, "ks2"], cfg: { transport: c.transport, maxSubscribeCursors: 1 } });
    r.server.disconnect();
    await tick(r);
    r.server.archive("ks2"); // the add is answered by a drop frame (ARCHIVED), which comes before the unary answer
    const before = adds(r, "ks2");
    const step = async (ms: number): Promise<void> => {
      await tick(r, ms);
      await new Promise((res) => setTimeout(res, 20)); // the late unary answers come in real time
      await tick(r);
    };
    await step(1000); // the new session, and its add of ks2
    expect(adds(r, "ks2")).toBe(before + 1);
    for (let i = 0; i < 5; i++) await step(15_000); // five heartbeats, none of which lists ks2
    expect(adds(r, "ks2")).toBe(before + 1);
  });
});

describe("B3-M2: 09 [READ-62] at most one UpdateSubscription in flight per session", () => {
  it("READ62_TheAddsOfASessionAreSentOneAfterTheOther", async () => {
    const clock = new ManualClock();
    const manifests = new StaticManifests(MANIFEST);
    const server = new FakeServer({ manifests, clock, heartbeatMs: 15_000 });
    for (const k of [KS, "ks2", "ks3", "ks4"]) server.createKeyspaceNow(k);
    const c = counting(server.transport(server.principal("ana")));
    server.inject("updateSubscription", { kind: "delay", ms: 500 }, { times: 20 });
    // a Subscribe of one cursor: the other keyspaces are named in adds on the new session (09 [READ-57])
    const r = await rig({ server, clock, manifests, keyspaces: [KS, "ks2", "ks3", "ks4"], cfg: { transport: c.transport, maxSubscribeCursors: 1 } });
    for (let i = 0; i < 10; i++) await tick(r, 500);
    for (const k of ["ks2", "ks3", "ks4"]) expect(adds(r, k)).toBeGreaterThanOrEqual(1);
    expect(c.max()).toBe(1);
    expect(r.engine.status().connection).toBe("live");
  });
});

describe("B3-M3: A.7, 09 [READ-35] the Subscribe is capped at limits.max_keyspaces_per_session, and a refusal for it is not resent unchanged", () => {
  it("READ35_TheSubscribeNamesNoMoreKeyspacesThanTheLimit", async () => {
    const clock = new ManualClock();
    const manifests = new StaticManifests(MANIFEST);
    const server = new FakeServer({ manifests, clock, heartbeatMs: 15_000, limits: { maxKeyspacesPerSession: 2 } });
    for (const k of [KS, "ks2", "ks3"]) server.createKeyspaceNow(k);
    const r = await rig({ server, clock, manifests, keyspaces: [KS, "ks2", "ks3"] });
    await tick(r, 1000);
    // reconnect: the second Subscribe knows the limit
    r.server.disconnect();
    await tick(r);
    await tick(r, 1000);
    const subs = r.server.calls.filter((c) => c.method === "subscribe").map((c) => (c.request as { keyspaces: unknown[] }).keyspaces.length);
    expect(subs.length).toBeGreaterThanOrEqual(2);
    expect(subs[subs.length - 1]).toBeLessThanOrEqual(2);
  });

  it("A7_ASubscribeRefusedForTheKeyspaceLimitStopsAndIsReported", async () => {
    const r = await rig();
    r.server.inject("subscribe", { kind: "status", status: "ResourceExhausted", reject: mk(Code.QUOTA_EXCEEDED, { details: { quota: "max_keyspaces_per_session" } }) }, { times: 1000 });
    r.server.disconnect();
    await tick(r);
    for (let i = 0; i < 20; i++) await tick(r, 30_000);
    const subs = r.server.calls.filter((c) => c.method === "subscribe").length;
    expect(subs).toBeLessThanOrEqual(3); // stopped, not resent unchanged every reconnect
    expect(ofType(r, "protocol-error").some((e) => /max_keyspaces_per_session/.test(`${e.what} ${e.detail}`))).toBe(true);
  });

  it("READ35_AnEngineOverTheLoweredLimitWaitsForTheAppAndResumesOnceItFollowsFewer", async () => {
    const clock = new ManualClock();
    const manifests = new StaticManifests(MANIFEST);
    const server = new FakeServer({ manifests, clock, heartbeatMs: 15_000, limits: { maxKeyspacesPerSession: 20 } });
    const ids = Array.from({ length: 21 }, (_, i) => `k${String(i).padStart(2, "0")}`);
    for (const k of ids) server.createKeyspaceNow(k);
    // the engine was told no limit yet and the test lifts its own cap: a Subscribe of 21 keyspaces is refused whole, and the fresh LoadState says 20
    const r = await rig({ server, clock, manifests, keyspaces: ids, cfg: { maxSubscribeCursors: 10_000 } });
    for (let i = 0; i < 5; i++) await tick(r, 30_000);
    expect(r.engine.status().connection).toBe("live"); // the next Subscribe named 20, the 21st by an add that the server rejects
  });
});
