// FBA batch F7b, V43 MUST FIX 1 (B3-M3): 09 [READ-35] "an add over the limit is listed in `UpdateSubscriptionResponse.rejected` with
// `Reject{QUOTA_EXCEEDED}` … which keyspaces it stops following is the app's choice"; A.7 ResourceExhausted / QUOTA_EXCEEDED "Stop and report;
// never resend unchanged in a loop."

import { describe, expect, it } from "vitest";
import { StaticManifests } from "../src/engine/index.js";
import { FakeServer, ManualClock, reject as mk } from "../src/fake/index.js";
import { Code } from "../src/gen/abpc/v2/types_pb.js";
import type { UpdateSubscriptionRequest } from "../src/gen/abpc/v2/sync_pb.js";
import { KS, MANIFEST, ofType, rig, tick } from "./helpers/engine.js";

const adds = (r: { server: FakeServer }, id: string): number =>
  r.server.calls.filter((c) => c.method === "updateSubscription" && (c.request as UpdateSubscriptionRequest).add.some((a) => a.keyspaceId === id)).length;

describe("V43 MUST 1: 09 [READ-35], A.7 an add over max_keyspaces_per_session is reported, not dropped, and the app chooses", () => {
  it("READ35_A7_AnAddRejectedOverTheKeyspaceLimitIsReportedAndNotResentUntilTheAppFollowsFewer", async () => {
    const clock = new ManualClock();
    const manifests = new StaticManifests(MANIFEST);
    const server = new FakeServer({ manifests, clock, heartbeatMs: 15_000, limits: { maxKeyspacesPerSession: 2 } });
    for (const k of [KS, "ks2", "ks3"]) server.createKeyspaceNow(k);
    const r = await rig({ server, clock, manifests, keyspaces: [KS, "ks2", "ks3"] });
    await tick(r, 1000);
    // a session that knows the limit: two cursors in the Subscribe, the third named in an add, which the server rejects QUOTA_EXCEEDED
    r.server.disconnect();
    await tick(r);
    await tick(r, 1000);
    expect(r.engine.status().connection).toBe("live");
    const over = ["ks3", "ks2", KS].find((k) => r.engine.status().stopped[k] !== undefined);
    expect(over).toBeDefined(); // the sync status names the keyspace that does not sync
    const id = over as string;
    expect(r.engine.status().stopped[id]).toBe("QUOTA_EXCEEDED");
    expect(ofType(r, "protocol-error").some((e) => e.keyspaces.includes(id) && /QUOTA_EXCEEDED/.test(e.detail))).toBe(true); // the app is told
    // A.7: not resent unchanged, whatever number of new sessions
    const before = adds(r, id);
    for (let i = 0; i < 3; i++) {
      r.server.disconnect();
      await tick(r);
      await tick(r, 30_000);
    }
    expect(adds(r, id)).toBe(before);
    // the app chooses: it stops following another keyspace, and the one over the limit is added and syncs
    const other = [KS, "ks2", "ks3"].find((k) => k !== id) as string;
    expect(await r.engine.removeKeyspace(other)).toBe(true);
    await tick(r, 1000);
    await tick(r, 1000);
    expect(adds(r, id)).toBeGreaterThan(before);
    expect(r.engine.status().stopped[id]).toBeUndefined();
  });

  it("READ35_S1_AFailedLoadStateProbeAfterAKeyspaceLimitRefusalIsRetriedNotAStop", async () => {
    // V43 S1: 09 [READ-35] "refused whole until its `Subscribe` names no more keyspaces than the `limits.max_keyspaces_per_session` of a fresh `LoadState` response"
    const r = await rig();
    await tick(r, 1000);
    r.server.inject("subscribe", { kind: "status", status: "ResourceExhausted", reject: mk(Code.QUOTA_EXCEEDED, { details: { quota: "max_keyspaces_per_session" } }) }, { times: 1 });
    r.server.inject("loadState", { kind: "status", status: "Unavailable" }, { times: 1 }); // the probe meets a network blip
    r.server.disconnect();
    await tick(r);
    for (let i = 0; i < 5; i++) await tick(r, 30_000);
    expect(r.engine.status().connection).toBe("live"); // the session resumed, once a fresh LoadState could be had
  });
});
