// FBA batch F7b, V43 S2: 14 [CLI-7] "When the client stops following the keyspace (`ACCESS_REVOKED`, `GRANT_CHANGED`, `MOVED`, `ARCHIVED`, `DELETED` or
// `NOT_FOUND`)": an unknown drop reason is not one of them (15 [COMPAT-42] adds the keyspace again), so a lost spend action is resent once
// ([CLI-64]), not reported as outcome unknown, and the client re-identifies before it is sent to again.

import { describe, expect, it } from "vitest";
import { set } from "../src/engine/index.js";
import { KS, rig, tick, types } from "./helpers/engine.js";

describe("V43 S2: 14 [CLI-7], [CLI-64], [CLI-83] only [CLI-7]'s list stops the resend of a lost spend action", () => {
  it("CLI7_CLI64_AnUnknownDropReasonDuringEpochResolutionStillResendsTheSpendActionAndReidentifies", async () => {
    const r = await rig();
    r.server.faults.paused = true;
    const { mutationId } = await r.engine.submit({ keyspaceId: KS, opType: "Pay", changes: [set("settings/pay", 1)] });
    await r.engine.settle();
    expect(r.engine.outbox().find((e) => e.mutationId === mutationId)?.outcome?.kind).toBe("appended");
    const clientIds = (): Set<string> =>
      new Set(r.server.calls.filter((c) => c.method === "subscribe" || c.method === "updateSubscription").map((c) => (c.request as { clientId: string }).clientId).filter((x) => x !== ""));
    const clientsBefore = clientIds().size;
    r.server.inject("getMutationResults", { kind: "delay", ms: 10_000 }, { times: 5 });
    r.server.restore(KS, 0n, { rewindBindings: true }); // EPOCH_CHANGED: the outcome is lost, nothing voided
    await tick(r);
    r.server.disconnect();
    // the next session names the keyspace again (it is subscribable under EPOCH_RESOLUTION)
    const subs = (): number => r.server.calls.filter((c) => c.method === "subscribe").length;
    const before = subs();
    for (let i = 0; i < 30 && (subs() === before || r.engine.status().connection !== "live"); i++) await tick(r, 1_000);
    r.server.drop(KS, 99); // a drop reason that the client does not know (15 [COMPAT-42])
    await tick(r);
    expect(r.events.some((e) => e.type === "held" && e.code === "DROP_REASON_99")).toBe(true); // the drop came during the resolution
    for (let i = 0; i < 5; i++) await tick(r, 5_000);
    const t = types(r, mutationId);
    expect(t).not.toContain("outcome-unknown");
    expect(t).toContain("resent");
    expect(clientIds().size).toBeGreaterThan(clientsBefore); // 14 [CLI-64]: a new client_id before the keyspace is sent to again
  });
});
