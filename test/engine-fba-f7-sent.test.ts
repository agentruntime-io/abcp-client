// FBA batch F7, B3-M7: 14 [CLI-52] "An action with an unknown outcome, or one registered in the source as an intent, MUST NOT be re-routed or dead-lettered
// until the source gives it a result". A crash with a resend in flight leaves the outcome unknown, whatever `Retry` an earlier request got.

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { set } from "../src/engine/index.js";
import { RetryReason, SuccessorRuleSchema } from "../src/gen/abpc/v2/types_pb.js";
import { KS, retryResult, rig, tick } from "./helpers/engine.js";

describe("B3-M7: 14 [CLI-52], [CLI-24] a resend in flight at a crash is an unknown outcome", () => {
  it("CLI52_AStaleRetryDoesNotLetASplitMoveAnActionWhoseResendWasInFlight", async () => {
    const r = await rig({ keyspaces: [KS, "dst2"] });
    // the first request gets Retry{CONTENTION}; the resend leaves and is slow
    r.server.inject("mutate", { kind: "result", result: (m) => retryResult(m, RetryReason.CONTENTION, 1000) }, { times: 1 });
    const { mutationId } = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/x", 1)] });
    await r.engine.settle();
    expect(r.storage.entry(mutationId)?.lastResult?.kind).toBe("retry");
    r.server.inject("mutate", { kind: "delay", ms: 3_600_000 }, { times: 1 });
    await tick(r, 5_000);
    const sends = r.server.calls.filter((c) => c.method === "mutate").length;
    expect(sends).toBe(2); // the resend left
    const leftAtCrash = r.storage.entry(mutationId)?.lastResult?.kind;
    // the process dies; the keyspace is split; a new engine starts on the same storage
    await r.engine.stop();
    r.server.split(KS, [create(SuccessorRuleSchema, { prefix: "", keyspaceId: "dst2" })]);
    const r2 = await rig({ server: r.server, clock: r.clock, manifests: r.manifests, storage: r.storage, keyspaces: [KS, "dst2"], seed: 9 });
    await tick(r2, 1_000);
    // the source is asked before anything moves (14 [CLI-52])
    const asked = r2.server.calls.findIndex((c) => c.method === "getMutationResult" && (c.request as { mutationId: string }).mutationId === mutationId);
    const moved = r2.events.findIndex((e) => e.type === "rerouted" && "mutationId" in e && e.mutationId === mutationId);
    expect(asked).toBeGreaterThanOrEqual(0);
    if (moved >= 0) {
      const movedCall = r2.server.calls.findIndex((c) => c.method === "mutate" && (c.request as { mutations: { mutationId: string; keyspaceId: string }[] }).mutations.some((m) => m.mutationId === mutationId && m.keyspaceId === "dst2"));
      expect(movedCall).toBeGreaterThan(asked);
    }
    expect(leftAtCrash).toBe("sent"); // what a crash leaves: no answer, not the Retry of the first request
  });
});
