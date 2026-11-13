// FBA batch F7b, V43 MUST FIX 2 (B3-M7 regression): 14 [CLI-7] "an answer that leaves the outcome unknown (`known = false` and
// `voided = false`) … ends the action as outcome unknown ([CLI-83])"; 14 [CLI-52] "An action with an unknown outcome … MUST NOT be
// re-routed or dead-lettered until the source gives it a result". A request that left and was never answered (a crash) is such an
// action: its binding may have expired after it was applied (07 [MUT-51], 14 [CLI-83]), so `known = false` does not free it.

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { set } from "../src/engine/index.js";
import { RetryReason, SuccessorRuleSchema } from "../src/gen/abpc/v2/types_pb.js";
import { KS, retryResult, rig, tick, types, type Rig } from "./helpers/engine.js";

/** An action whose resend is in flight when the process dies: what storage keeps is `lastResult.kind = "sent"`. */
async function crashWithResendInFlight(): Promise<{ r: Rig; mutationId: string }> {
  const r = await rig({ keyspaces: [KS, "dst2"] });
  r.server.inject("mutate", { kind: "result", result: (m) => retryResult(m, RetryReason.CONTENTION, 1000) }, { times: 1 });
  const { mutationId } = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/x", 1)] });
  await r.engine.settle();
  r.server.inject("mutate", { kind: "delay", ms: 3_600_000 }, { times: 1 });
  await tick(r, 5_000);
  expect(r.storage.entry(mutationId)?.lastResult?.kind).toBe("sent");
  await r.engine.stop();
  return { r, mutationId };
}

function sentTo(r: Rig, mutationId: string, keyspaceId: string): boolean {
  return r.server.calls.some(
    (c) => c.method === "mutate" && (c.request as { mutations: { mutationId: string; keyspaceId: string }[] }).mutations.some((m) => m.mutationId === mutationId && m.keyspaceId === keyspaceId),
  );
}

describe("V43 MUST 2: 14 [CLI-52], [CLI-7], [CLI-83] an unanswered request ends as outcome unknown on known = false", () => {
  it("CLI52_CLI7_CLI83_AnUnansweredActionOfASplitKeyspaceEndsOutcomeUnknownAndIsNotRerouted", async () => {
    const { r, mutationId } = await crashWithResendInFlight();
    r.server.split(KS, [create(SuccessorRuleSchema, { prefix: "", keyspaceId: "dst2" })]);
    const r2 = await rig({ server: r.server, clock: r.clock, manifests: r.manifests, storage: r.storage, keyspaces: [KS, "dst2"], seed: 9 });
    await tick(r2, 1_000);
    await tick(r2, 5_000);
    expect(r2.server.calls.some((c) => c.method === "getMutationResult" && (c.request as { mutationId: string }).mutationId === mutationId)).toBe(true);
    const t = types(r2, mutationId);
    expect(t).not.toContain("rerouted");
    expect(sentTo(r2, mutationId, "dst2")).toBe(false); // never applied a second time in the successor
    expect(t).toContain("outcome-unknown");
  });

  it("CLI52_CLI7_CLI83_AnUnansweredActionOfAnArchivedKeyspaceEndsOutcomeUnknownNotRejected", async () => {
    const { r, mutationId } = await crashWithResendInFlight();
    r.server.archive(KS);
    const r2 = await rig({ server: r.server, clock: r.clock, manifests: r.manifests, storage: r.storage, keyspaces: [KS, "dst2"], seed: 9 });
    await tick(r2, 1_000);
    await tick(r2, 5_000);
    const t = types(r2, mutationId);
    expect(t).not.toContain("rejected"); // [CLI-83] "never as rejected or committed"
    expect(r2.engine.deadLetterEntries().some((d) => d.mutationId === mutationId && d.reason === "KEYSPACE_ARCHIVED")).toBe(false);
    expect(t).toContain("outcome-unknown");
  });
});
