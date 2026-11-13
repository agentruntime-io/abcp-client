// FBA batch F7c, V48: A.7 ResourceExhausted / QUOTA_EXCEEDED "the keyspaces of one session (09 [READ-35])": "Stop and report; never resend unchanged
// in a loop." 09 [READ-35] "refused whole until its `Subscribe` names no more keyspaces than the `limits.max_keyspaces_per_session` of a fresh
// `LoadState` response"; 09 §9.2 "a keyspace that was dropped … is no longer followed, so it counts no more".

import { describe, expect, it } from "vitest";
import { StaticManifests } from "../src/engine/index.js";
import { FakeServer, ManualClock, reject as mk } from "../src/fake/index.js";
import { DropReason, type UpdateSubscriptionRequest } from "../src/gen/abpc/v2/sync_pb.js";
import { Code } from "../src/gen/abpc/v2/types_pb.js";
import { KS, MANIFEST, ofType, rig, tick } from "./helpers/engine.js";

const adds = (r: { server: FakeServer }, id: string): number =>
  r.server.calls.filter((c) => c.method === "updateSubscription" && (c.request as UpdateSubscriptionRequest).add.some((a) => a.keyspaceId === id)).length;

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
});

describe("V48 S1: 09 [READ-35] a keyspace that the server dropped counts no more against the limit", () => {
  it("READ35_ADropThatFreesASlotLetsTheKeyspaceOverTheLimitBeAdded", async () => {
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
    const id = all.find((k) => r.engine.status().stopped[k] === "QUOTA_EXCEEDED");
    expect(id).toBeDefined();
    const over = id as string;
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
