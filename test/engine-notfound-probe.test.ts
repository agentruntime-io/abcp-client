// Q61 (the coordinator, verifier V19: the text decides, 14 [CLI-80], Table 14.1): while the client holds actions for a keyspace that a session dropped with
// NOT_FOUND, the probe of the hold is a resend of the head action (`Mutate` needs no session); the keyspace is added again when the resend gets another
// result, and as the probe only when the client holds no action for it.

import { describe, expect, it } from "vitest";
import { set } from "../src/engine/index.js";
import { Code } from "../src/gen/abpc/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { kc, KS, mutateRequests, ofType, rejectResult, rig, tick, types } from "./helpers/engine.js";

const methodsAfter = (r: Awaited<ReturnType<typeof rig>>, mark: number): string[] => r.server.calls.slice(mark).map((c) => c.method);

describe("Q61: the probe of the hold of a keyspace that a session dropped with NOT_FOUND", () => {
  it("with an action held, the probe is the resend of the action, not the re-add: the first call after the backoff is a Mutate, and the keyspace is followed again once it gets an answer", async () => {
    const r = await rig();
    r.random.fixed = 0.5;
    r.server.drop(KS, 10); // DropReason NOT_FOUND
    await r.engine.settle();
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "keyspace", code: "NOT_FOUND" })]);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(mutateRequests(r)).toHaveLength(0); // held: nothing is sent before the probe is due
    const mark = r.server.calls.length;
    await tick(r, 40_000);
    const calls = methodsAfter(r, mark);
    expect(calls[0]).toBe("mutate"); // the resend of the head: the probe
    expect(types(r, a.mutationId)).toContain("committed");
    expect(r.engine.status().holds).toEqual([]);
    // the result cleared the hold, and the keyspace is followed again: what is written now reaches the engine
    r.server.write(KS, [kc("settings/later", "set", 1)]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).json("settings/later")).toBe(1);
  });

  it("a server that still answers KEYSPACE_NOT_FOUND is probed by resends, with the hold backoff, and the keyspace is not added again while the action is held", async () => {
    const r = await rig();
    r.random.fixed = 0.5;
    r.server.drop(KS, 10);
    await r.engine.settle();
    r.server.inject("mutate", { kind: "result", result: (m) => rejectResult(m, mk(Code.KEYSPACE_NOT_FOUND)) }, { times: 3 });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    const mark = r.server.calls.length;
    for (let i = 0; i < 40 && !types(r, a.mutationId).includes("committed"); i++) await tick(r, 60_000);
    const calls = methodsAfter(r, mark);
    // three resends that were refused, a fourth that got through; the keyspace is added again after it, never before
    const firstAdd = calls.findIndex((m) => m === "loadState" || m === "subscribe");
    const lastMutate = calls.lastIndexOf("mutate");
    expect(calls.filter((m) => m === "mutate").length).toBeGreaterThanOrEqual(4);
    expect(firstAdd === -1 || firstAdd > lastMutate).toBe(true);
    expect(types(r, a.mutationId)).toContain("committed");
    expect(r.engine.status().holds).toEqual([]);
  });

  it("with no action held, the probe re-adds the keyspace (09 [READ-65]); nothing is resent", async () => {
    const r = await rig();
    r.random.fixed = 0.5;
    r.server.drop(KS, 10);
    await r.engine.settle();
    const mark = r.server.calls.length;
    await tick(r, 40_000);
    expect(methodsAfter(r, mark)).not.toContain("mutate");
    expect(methodsAfter(r, mark).some((m) => m === "loadState" || m === "subscribe")).toBe(true);
    expect(r.engine.status().holds).toEqual([]);
    expect(ofType(r, "held").length).toBeGreaterThan(0);
  });
});
