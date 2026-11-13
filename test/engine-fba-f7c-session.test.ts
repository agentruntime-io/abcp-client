// FBA batch F7c, V48: A.7 ResourceExhausted / QUOTA_EXCEEDED "the keyspaces of one session (09 [READ-35])": "Stop and report; never resend unchanged
// in a loop." 09 [READ-35] "refused whole until its `Subscribe` names no more keyspaces than the `limits.max_keyspaces_per_session` of a fresh
// `LoadState` response"; 09 §9.2 "a keyspace that was dropped … is no longer followed, so it counts no more". FBA batch F7d adds V53 S-1 and S-2.

import { describe, expect, it } from "vitest";
import { StaticManifests } from "../src/engine/index.js";
import { FakeServer, ManualClock, reject as mk } from "../src/fake/index.js";
import { DropReason, type UpdateSubscriptionRequest } from "../src/gen/abpc/v2/sync_pb.js";
import { Code } from "../src/gen/abpc/v2/types_pb.js";
import { KS, MANIFEST, ofType, rig, tick, type Rig } from "./helpers/engine.js";

const adds = (r: { server: FakeServer }, id: string): number =>
  r.server.calls.filter((c) => c.method === "updateSubscription" && (c.request as UpdateSubscriptionRequest).add.some((a) => a.keyspaceId === id)).length;

const subscribes = (r: Rig): number => r.server.calls.filter((c) => c.method === "subscribe").length;

/** A limit of 2 and three followed keyspaces: two on the session, and one whose add was refused for the limit, shown `QUOTA_EXCEEDED`. */
async function overTheLimit(): Promise<{ r: Rig; all: string[]; over: string }> {
  const clock = new ManualClock();
  const manifests = new StaticManifests(MANIFEST);
  const server = new FakeServer({ manifests, clock, heartbeatMs: 15_000, limits: { maxKeyspacesPerSession: 2 } });
  const all = [KS, "ks2", "ks3"];
  for (const k of all) server.createKeyspaceNow(k);
  const r = await rig({ server, clock, manifests, keyspaces: all });
  await tick(r, 1000);
  r.server.disconnect();
  await tick(r);
  await tick(r, 1000);
  const over = all.find((k) => r.engine.status().stopped[k] === "QUOTA_EXCEEDED");
  expect(over).toBeDefined();
  return { r, all, over: over as string };
}

describe("V48 MUST 1: A.7, 09 [READ-35] the limit probe asks a keyspace that can be subscribed, and a refused probe is no loop", () => {
  it("READ35_A7_AProbeOfAStoppedKeyspaceIsNotTakenAndTheRefusalStopsAndIsReported", async () => {
    const r = await rig({ keyspaces: [KS, "ks2"] });
    await tick(r, 1000);
    // KS is dropped GRANT_CHANGED and its copy is kept: every LoadState of it is refused PERMISSION_DENIED
    r.server.drop(KS, DropReason.GRANT_CHANGED);
    await tick(r);
    expect(r.engine.keyspaceInfo(KS)?.stopped).toBe("GRANT_CHANGED");
    expect(r.engine.keyspaceInfo(KS)?.loaded).toBe(true);
    r.server.inject("loadState", { kind: "status", status: "PermissionDenied", reject: mk(Code.PERMISSION_DENIED) }, { times: 10_000, match: (req) => (req as { cursor?: { keyspaceId: string } }).cursor?.keyspaceId === KS });
    r.server.inject("subscribe", { kind: "status", status: "ResourceExhausted", reject: mk(Code.QUOTA_EXCEEDED, { details: { quota: "max_keyspaces_per_session" } }) }, { times: 10_000 });
    const subsBefore = r.server.calls.filter((c) => c.method === "subscribe").length;
    r.server.disconnect();
    await tick(r);
    for (let i = 0; i < 20; i++) await tick(r, 30_000);
    const subs = r.server.calls.filter((c) => c.method === "subscribe").length - subsBefore;
    expect(subs).toBeLessThanOrEqual(2); // stopped, not resent unchanged every reconnect
    expect(ofType(r, "protocol-error").some((e) => /max_keyspaces_per_session/.test(e.detail))).toBe(true); // and the app is told
  });

  // V53 S-2: the path of V48 MUST 1 itself. Every candidate can be subscribed, so each is asked; each is refused with a reject, which says nothing of
  // the limit and does not heal by itself: the next one is asked, and when none answers the engine stops and reports (A.7)
  it("READ35_A7_AProbeRefusedWithARejectAsksTheNextKeyspaceAndWhenNoneAnswersStopsAndReports", async () => {
    const r = await rig({ keyspaces: [KS, "ks2"] });
    await tick(r, 1000);
    const probed = (req: unknown): string | undefined => (req as { cursor?: { keyspaceId: string } }).cursor?.keyspaceId;
    r.server.inject("loadState", { kind: "status", status: "PermissionDenied", reject: mk(Code.PERMISSION_DENIED) }, { times: 10_000, match: (req) => probed(req) === KS || probed(req) === "ks2" });
    r.server.inject("subscribe", { kind: "status", status: "ResourceExhausted", reject: mk(Code.QUOTA_EXCEEDED, { details: { quota: "max_keyspaces_per_session" } }) }, { times: 10_000 });
    const subsBefore = subscribes(r);
    const callsBefore = r.server.calls.length;
    r.server.disconnect();
    await tick(r);
    for (let i = 0; i < 20; i++) await tick(r, 30_000);
    // the first probe is refused, the next keyspace is asked, and neither is asked again
    const probes = r.server.calls.slice(callsBefore).filter((c) => c.method === "loadState").map((c) => probed(c.request));
    expect(probes).toEqual([KS, "ks2"]);
    // stop: the refused `Subscribe` is not sent again, unchanged, on any reconnect; the session waits until what the engine follows changes
    expect(subscribes(r) - subsBefore).toBe(1);
    expect((r.engine as unknown as { sess: { overCap?: { followed: number } } }).sess.overCap).toEqual({ followed: 2 });
    // and report: once, naming the limit
    expect(ofType(r, "protocol-error").filter((e) => e.what === "Subscribe" && /max_keyspaces_per_session/.test(e.detail))).toHaveLength(1);
    expect(r.engine.status().connection).not.toBe("live");
  });
});

describe("V48 S1: 09 [READ-35] a keyspace that the server dropped counts no more against the limit", () => {
  it("READ35_ADropThatFreesASlotLetsTheKeyspaceOverTheLimitBeAdded", async () => {
    const { r, all, over } = await overTheLimit();
    const before = adds(r, over);
    // another keyspace is archived, and the app keeps it: the session holds one keyspace now
    const other = all.find((k) => k !== over) as string;
    r.server.archive(other);
    await tick(r);
    await tick(r, 1000);
    expect(r.engine.keyspaceInfo(other)?.stopped).toBe("ARCHIVED");
    expect(adds(r, over)).toBeGreaterThan(before);
    expect(r.engine.status().stopped[over]).toBeUndefined();
  });
});

// V53 S-1: a drop `HISTORY_DIVERGED` or `UPGRADE_REQUIRED` holds the keyspace instead of stopping it. 09 [READ-64]: "a keyspace that was dropped (READ-65) is
// no longer followed, so it counts no more": the server freed its slot, so the held keyspace counts no more either, and the keyspace over the limit is added.
describe("V53 S-1: 09 [READ-64] a keyspace that a drop holds counts no more against the limit", () => {
  const heldDropFreesASlot = async (dropIt: (r: Rig, id: string) => void, code: string): Promise<void> => {
    const { r, all, over } = await overTheLimit();
    const before = adds(r, over);
    const other = all.find((k) => k !== over) as string;
    dropIt(r, other);
    await tick(r);
    await tick(r, 1000);
    // the dropped keyspace is held, not stopped: the session holds one keyspace now
    expect(r.engine.keyspaceInfo(other)?.stopped).toBeUndefined();
    expect(r.engine.status().holds.some((h) => h.keyspaceId === other && h.code === code)).toBe(true);
    // the keyspace over the limit is added again, and the server takes it
    expect(adds(r, over)).toBeGreaterThan(before);
    expect(r.engine.status().stopped[over]).toBeUndefined();
  };

  it("READ64_AHistoryDivergedDropFreesASlotForTheKeyspaceOverTheLimit", async () => {
    // the keyspace is frozen: the session drops it HISTORY_DIVERGED, and the probe of the hold keeps finding it frozen
    await heldDropFreesASlot((r, id) => r.server.freeze(id), "HISTORY_DIVERGED");
  });

  it("READ64_AnUpgradeRequiredDropFreesASlotForTheKeyspaceOverTheLimit", async () => {
    await heldDropFreesASlot((r, id) => r.server.drop(id, DropReason.UPGRADE_REQUIRED), "UPGRADE_REQUIRED");
  });
});
