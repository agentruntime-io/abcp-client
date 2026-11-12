// FBA batch F7, B3-M5: 14 [CLI-83] outcome unknown when "`GetMutationResult` answers `known = false` and `voided = false` after an epoch change ([CLI-53]),
// for an action other than a spend action, which is resent instead ([CLI-64]), or for an action of a keyspace that the client stopped following ([CLI-7])".

import { describe, expect, it } from "vitest";
import { set } from "../src/engine/index.js";
import { KS, rig, tick, types } from "./helpers/engine.js";

describe("B3-M5: 14 [CLI-83], [CLI-7] a spend action of a keyspace that the client stopped following is not resent after an epoch change", () => {
  it("CLI83_ASpendActionOfADeletedKeyspaceEndsAsOutcomeUnknown", async () => {
    const r = await rig();
    r.server.faults.paused = true; // the record of the action is held back: it stays pending with its epoch-1 `Appended`
    const { mutationId } = await r.engine.submit({ keyspaceId: KS, opType: "Pay", changes: [set("settings/pay", 1)] });
    await r.engine.settle();
    expect(r.engine.outbox().find((e) => e.mutationId === mutationId)?.outcome?.kind).toBe("appended");
    // the answers of the resolution are slow, and the keyspace is deleted meanwhile: every id answers known = false (07 [MUT-53])
    r.server.inject("getMutationResults", { kind: "delay", ms: 10_000 }, { times: 5 });
    r.server.restore(KS, 0n, { rewindBindings: true }); // EPOCH_CHANGED: the outcome of the action is lost, and nothing is voided
    await tick(r);
    // while the answers are on their way the session ends, and the next one names the keyspace again (it is subscribable under EPOCH_RESOLUTION): it is
    // dropped DELETED during the resolution
    r.server.disconnect();
    await tick(r);
    r.server.deleteKeyspace(KS);
    await tick(r, 1_000);
    expect(r.engine.keyspaceInfo(KS)?.stopped).toBe("DELETED");
    for (let i = 0; i < 5; i++) await tick(r, 5_000);
    const t = types(r, mutationId);
    expect(t).not.toContain("resent");
    expect(t).toContain("outcome-unknown");
    expect(r.engine.outbox().map((e) => e.mutationId)).not.toContain(mutationId);
  });
});
