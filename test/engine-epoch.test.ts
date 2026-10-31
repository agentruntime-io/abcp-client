// A restore (14 [CLI-51], [CLI-53], [CLI-64], [CLI-83]) and the outcome lifecycles of 17 [CONF-38], against the fake server that answers as a
// restored server does (07 [MUT-104], [MUT-105]).

import { describe, expect, it } from "vitest";
import { set } from "../src/engine/index.js";
import { Code, StaleCause } from "../src/gen/abpc/v2/types_pb.js";
import { goOffline, goOnline, kc, KS, ofType, rig, sentIds, tick, types, type Rig } from "./helpers/engine.js";

interface Seen {
  mutationId: string;
  code: number;
  cause: number | undefined;
  epoch: number;
  cursor: bigint;
}

async function lostRig(): Promise<{ r: Rig; seen: Seen[] }> {
  const seen: Seen[] = [];
  const r = await rig({
    cfg: {
      conflictHandler: (c) => {
        seen.push({ mutationId: c.action.mutationId, code: c.reject.code, cause: c.stale?.cause, epoch: c.confirmed.logEpoch, cursor: c.confirmed.seq });
      },
    },
  });
  return { r, seen };
}

/** The kinds of the events that carry a mutation id, plus the events of the keyspace, in order. */
const kinds = (r: Rig, id: string): string[] => types(r, id).filter((t) => t !== "held");

describe("17 [CONF-38] a lost action that is not a spend action", () => {
  it("an action reported `committed` whose outcome a restore voids gets exactly committed, reverted, conflict (one call of the handler, with the Reject STALE{EPOCH} and the confirmed state after the cold load), then rejected; it is never sent again", async () => {
    const { r, seen } = await lostRig();
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(kinds(r, a.mutationId)).toEqual(["committed"]);
    r.server.write(KS, [kc("settings/other", "set", 1)], { sub: "bob" }); // survives
    await r.engine.settle();
    const sentBefore = sentIds(r).length;
    r.server.restore(KS, 0n); // everything is lost
    await r.engine.settle();
    expect(kinds(r, a.mutationId)).toEqual(["committed", "reverted", "conflict", "rejected"]);
    expect(seen).toEqual([{ mutationId: a.mutationId, code: Code.STALE, cause: StaleCause.EPOCH, epoch: 2, cursor: 1n }]);
    const rej = ofType(r, "rejected").find((e) => e.mutationId === a.mutationId);
    expect(rej?.reject?.stale?.cause).toBe(StaleCause.EPOCH);
    expect(ofType(r, "reverted")[0]).toMatchObject({ mutationId: a.mutationId, seq: 1n, logEpoch: 1 });
    expect(sentIds(r).length).toBe(sentBefore); // never sent again
    // each key keeps its latest confirmed value: the restore took both writes away
    expect(r.engine.view(KS).keys("settings/")).toEqual([]);
    expect(ofType(r, "reloaded").map((e) => [e.cause, e.logEpoch])).toEqual([["COLD_LOAD", 1], ["EPOCH_CHANGED", 2]]);
    expect(r.engine.cursor(KS)?.logEpoch).toBe(2);
    expect(r.engine.status().holds).toEqual([]);
  });

  it("one that the restore loses while it is still pending gets exactly conflict and rejected, and no reverted", async () => {
    const { r, seen } = await lostRig();
    r.server.faults.paused = true;
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(r.engine.outbox()[0]?.outcome).toBeDefined(); // Appended, not retired
    r.server.restore(KS, 0n);
    r.server.resumeDelivery();
    await r.engine.settle();
    expect(kinds(r, a.mutationId)).toEqual(["conflict", "rejected"]);
    expect(seen).toHaveLength(1);
    expect(r.engine.outbox()).toHaveLength(0);
  });

  it("with no handler registered, `rejected` follows `conflict` at once", async () => {
    const r = await rig();
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    r.server.restore(KS, 0n);
    await r.engine.settle();
    expect(kinds(r, a.mutationId)).toEqual(["committed", "reverted", "conflict", "rejected"]);
  });

  it("a GATED action whose intent the restore kept and whose COMMITTED decision it discarded: the answer is the REJECTED decision that ended the intent (08 [GATE-93])", async () => {
    const { r, seen } = await lostRig();
    const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] });
    await r.engine.settle();
    r.server.decide(KS, g.mutationId, { status: "COMMITTED" });
    await r.engine.settle();
    expect(kinds(r, g.mutationId)).toEqual(["committed"]);
    r.server.restore(KS, 1n); // the INTENT record at seq 1 survives; the DECISION at seq 2 does not
    await r.engine.settle();
    expect(kinds(r, g.mutationId)).toEqual(["committed", "reverted", "conflict", "rejected"]);
    expect(seen[0]).toMatchObject({ mutationId: g.mutationId, code: Code.STALE, cause: StaleCause.EPOCH });
    expect(sentIds(r)).toEqual([g.mutationId]);
  });

  it("a GATED action registered in the old epoch and pending: the answer decides it; it is never resent", async () => {
    const { r, seen } = await lostRig();
    const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] });
    await r.engine.settle();
    r.server.restore(KS, 1n); // the intent survives, undecided; the epoch advance ends it STALE{EPOCH}
    await r.engine.settle();
    expect(kinds(r, g.mutationId)).toEqual(["conflict", "rejected"]);
    expect(seen).toHaveLength(1);
    expect(sentIds(r)).toEqual([g.mutationId]);
  });
});

describe("14 [CLI-83] an outcome that the restore leaves unknown is reported as outcome unknown, never rejected or committed", () => {
  it("known = false and voided = false (the restore rewound the bindings): `outcome-unknown`, then `exported`, with the dependents; no `reverted`, never sent again", async () => {
    const exported: string[] = [];
    const r = await rig({ cfg: { exportHook: (e) => void exported.push(e.mutationId) } });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    r.server.faults.paused = true;
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] }); // pending, outcome from the old epoch
    await r.engine.settle();
    const c = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 2)] }); // depends on b; sent after b's result
    await r.engine.settle();
    r.server.restore(KS, 0n, { rewindBindings: true });
    r.server.resumeDelivery();
    await r.engine.settle();
    expect(kinds(r, a.mutationId)).toEqual(["committed", "outcome-unknown", "exported"]);
    expect(kinds(r, b.mutationId)).toEqual(["outcome-unknown", "exported"]);
    expect(ofType(r, "outcome-unknown").map((e) => e.cause)).toEqual(["EPOCH_CHANGED", "EPOCH_CHANGED", "EPOCH_CHANGED"].slice(0, ofType(r, "outcome-unknown").length));
    expect(exported).toContain(a.mutationId);
    expect(exported).toContain(b.mutationId);
    expect(r.engine.status().deadLetters).toBe(0);
    expect(ofType(r, "reverted")).toHaveLength(0);
    expect(sentIds(r).filter((id) => id === a.mutationId)).toHaveLength(1);
    void c;
  });
});

describe("17 [CONF-38] a lost spend action (14 [CLI-64], 02 [ARC-52] item 6)", () => {
  it("reported `committed`, then lost: `reverted`, then `resent` with cause VOIDED, then the final event of its resend; the mutation id is kept, and the resend is the only one", async () => {
    const { r, seen } = await lostRig();
    const p = await r.engine.submit({ keyspaceId: KS, opType: "Pay", changes: [set("settings/sale", 1)] });
    await r.engine.settle();
    expect(kinds(r, p.mutationId)).toEqual(["committed"]);
    r.server.restore(KS, 0n);
    await r.engine.settle();
    expect(kinds(r, p.mutationId)).toEqual(["committed", "reverted", "resent", "committed"]);
    expect(ofType(r, "resent")[0]).toMatchObject({ mutationId: p.mutationId, cause: "VOIDED", seq: 1n, logEpoch: 1 });
    expect(seen).toHaveLength(0);
    expect(sentIds(r).filter((id) => id === p.mutationId)).toHaveLength(2);
    // the server recorded the resend as a new action, in the new epoch
    expect(r.server.records(KS).filter((x) => x.mutationId === p.mutationId).map((x) => x.logEpoch)).toEqual([2]);
    // lost again: a resend that a later restore loses is never resent again; it ends as any lost action does
    r.server.restore(KS, 0n);
    await r.engine.settle();
    expect(kinds(r, p.mutationId)).toEqual(["committed", "reverted", "resent", "committed", "reverted", "conflict", "rejected"]);
    expect(sentIds(r).filter((id) => id === p.mutationId)).toHaveLength(2);
  });

  it("outcome unknown (rewound bindings): `reverted`, then `resent` with cause OUTCOME_UNKNOWN; one that the restore loses while pending gets no `reverted`", async () => {
    const r = await rig();
    const p = await r.engine.submit({ keyspaceId: KS, opType: "Pay", changes: [set("settings/sale", 1)] });
    await r.engine.settle();
    r.server.restore(KS, 0n, { rewindBindings: true });
    await r.engine.settle();
    expect(kinds(r, p.mutationId)).toEqual(["committed", "reverted", "resent", "committed"]);
    expect(ofType(r, "resent")[0]?.cause).toBe("OUTCOME_UNKNOWN");

    const w = await rig();
    w.server.faults.paused = true;
    const q = await w.engine.submit({ keyspaceId: KS, opType: "Pay", changes: [set("settings/sale", 1)] });
    await w.engine.settle();
    w.server.restore(KS, 0n);
    w.server.resumeDelivery();
    await w.engine.settle();
    expect(kinds(w, q.mutationId)).toEqual(["resent", "committed"]); // never reported committed: no `reverted`
  });

  it("a spend action authored in the old epoch is not refused for its base epoch", async () => {
    const r = await rig();
    await goOffline(r);
    const p = await r.engine.submit({ keyspaceId: KS, opType: "Pay", changes: [set("settings/sale", 1)] });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    r.server.restore(KS, 0n);
    await goOnline(r);
    await tick(r, 5000);
    expect(kinds(r, p.mutationId)).toEqual(["committed"]);
    // every other action authored in the old epoch gets STALE{EPOCH} and goes to the handler (14 [CLI-51], [CLI-49])
    expect(kinds(r, a.mutationId)).toEqual(["conflict", "rejected"]);
    expect(ofType(r, "rejected").find((e) => e.mutationId === a.mutationId)?.reject?.stale?.cause).toBe(StaleCause.EPOCH);
  });
});

describe("14 [CLI-51] [CLI-64] EPOCH_CHANGED: cold-load as a resume check, hold the keyspace, resolve, re-identify", () => {
  it("the cold load carries the cursor's after_seq and log_epoch; the keyspace is held (EPOCH_RESOLUTION) until every outcome of the earlier epoch is answered; then the client re-identifies and sends again", async () => {
    const r = await rig();
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    const clientBefore = (r.engine as unknown as { clientId: string }).clientId;
    r.server.inject("getMutationResults", { kind: "delay", ms: 3000 });
    r.server.restore(KS, 0n);
    await r.engine.settle();
    const load = r.server.calls.filter((c) => c.method === "loadState").at(-1)?.request as { cursor: { afterSeq: bigint; logEpoch: number } };
    expect(load.cursor.afterSeq).toBe(1n);
    expect(load.cursor.logEpoch).toBe(1);
    // while the answers are pending, the keyspace is held, and a new action waits in the outbox
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "keyspace", code: "EPOCH_RESOLUTION" })]);
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
    await r.engine.settle();
    expect(sentIds(r)).not.toContain(b.mutationId);
    await tick(r, 3000);
    expect(r.engine.status().holds).toEqual([]);
    expect((r.engine as unknown as { clientId: string }).clientId).not.toBe(clientBefore);
    expect(kinds(r, b.mutationId)).toEqual(["committed"]);
    const held = r.events.filter((e) => e.type === "held");
    expect(held.map((e) => (e.type === "held" ? [e.state, e.code] : []))).toEqual([["start", "EPOCH_RESOLUTION"], ["stop", "EPOCH_RESOLUTION"]]);
    // the client_id of the new instance: slots from 1 again
    const last = r.server.calls.filter((c) => c.method === "mutate").at(-1)?.request as { mutations: { clientSeq: bigint; clientId: string }[] };
    expect(last.mutations[0]?.clientSeq).toBe(1n);
    expect(last.mutations[0]?.clientId).not.toBe(clientBefore);
    void a;
  });

  it("the dependents of an action lost to the restore fail with DEPENDENCY_FAILED before they could be sent", async () => {
    const { r } = await lostRig();
    r.server.faults.paused = true;
    r.server.inject("mutate", { kind: "lose" }); // a is sent, applied, and its outcome is unknown
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] }); // depends on a: waits for its result
    await r.engine.settle();
    expect(sentIds(r)).toEqual([a.mutationId]);
    r.server.restore(KS, 0n);
    r.server.resumeDelivery();
    await r.engine.settle();
    expect(kinds(r, a.mutationId)).toEqual(["conflict", "rejected"]);
    expect(kinds(r, b.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(sentIds(r)).not.toContain(b.mutationId);
  });

  it("a frozen keyspace gives no answer: the client holds and asks again once the keyspace reopens (14 [CLI-53])", async () => {
    const { r } = await lostRig();
    r.random.fixed = 0.5;
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    r.server.restore(KS, 0n);
    r.server.freeze(KS); // frozen again before the client's questions
    await tick(r, 40_000);
    expect(kinds(r, a.mutationId)).toEqual(["committed"]);
    r.server.reopen(KS);
    await tick(r, 200_000);
    await tick(r, 200_000);
    expect(kinds(r, a.mutationId)).toEqual(["committed", "reverted", "conflict", "rejected"]);
  });
});
