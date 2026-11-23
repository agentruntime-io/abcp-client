// V19 SHOULD-3, 14 [CLI-34]: "a final rejection (... a `Decided` with `REJECTED`, whatever the class of its reject, an unknown one included ...)". A decision is
// stored: a resend of its intent gets the same decision again, so nothing of a REJECTED decision is a hold, a retry, or a resend after a refresh of the token.

import { describe, expect, it } from "vitest";
import { set } from "../src/engine/index.js";
import { Code, ErrorClass } from "../src/gen/abcp/v2/types_pb.js";
import { reject as mk, rejectWith } from "../src/fake/index.js";
import { KS, mutateRequests, rig, tick, type Rig } from "./helpers/engine.js";

/** An approval and an action that depends on it (it writes the same key), the approval decided REJECTED with `reject`, the server paused for the rest. */
async function decidedWith(reject: ReturnType<typeof mk>): Promise<{ r: Rig; g: string; dep: string }> {
  const r = await rig();
  const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] });
  await r.engine.settle();
  const dep = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/g", 2)] });
  await r.engine.settle();
  r.server.faults.paused = true;
  r.server.decide(KS, g.mutationId, { status: "REJECTED", reject });
  await tick(r, 60_000);
  await tick(r, 600_000);
  return { r, g: g.mutationId, dep: dep.mutationId };
}

const sends = (r: Rig, id: string): number => mutateRequests(r).filter((q) => q.mutations.some((m) => m.mutationId === id)).length;

describe("14 [CLI-34] (V19 SHOULD-3): a REJECTED decision is final whatever the class of its reject", () => {
  it("a reject of the known class HOLD with a code that the client does not know: the action is dead-lettered and its dependent fails, no hold, no resend", async () => {
    const { r, g, dep } = await decidedWith(rejectWith(901, ErrorClass.HOLD));
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId).sort()).toEqual([g, dep].sort());
    expect(r.engine.status().holds).toEqual([]);
    expect(r.engine.outbox()).toEqual([]);
    expect(sends(r, g)).toBe(1); // sent once; the decision came on the stream
  });

  it("a reject of the known HOLD class with a known HOLD code (UPGRADE_REQUIRED, KEYSPACE_NOT_FOUND): final too", async () => {
    for (const code of [Code.UPGRADE_REQUIRED, Code.KEYSPACE_NOT_FOUND, Code.APP_SUSPENDED]) {
      const { r, g } = await decidedWith(mk(code));
      expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toContain(g);
      expect(r.engine.status().holds).toEqual([]);
      expect(r.engine.outbox()).toEqual([]);
    }
  });

  it("a decided PERMISSION_DENIED or ACTOR_MISMATCH is not resent once after a refresh or a new client_id: the decision is recorded", async () => {
    for (const code of [Code.PERMISSION_DENIED, Code.ACTOR_MISMATCH]) {
      const { r, g } = await decidedWith(mk(code));
      expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toContain(g);
      expect(sends(r, g)).toBe(1);
    }
  });

  it("an undecided reject of the class HOLD is still a hold (the rule is for a decision only)", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "result", recorded: false, result: (m) => (m.changes[0]?.key === "settings/h" ? { $typeName: "abcp.v2.MutationResult", mutationId: m.mutationId, outcome: { case: "rejected", value: rejectWith(901, ErrorClass.HOLD) }, duplicate: false, intentSeq: 0n } as never : undefined) });
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/h", 1)] });
    await r.engine.settle();
    expect(r.engine.status().holds.map((h) => h.code)).toEqual(["CODE_901"]);
    expect(r.engine.outbox()).toHaveLength(1);
  });
});
