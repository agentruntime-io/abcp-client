// Round 16 (spec draft 14): the engine's changes. 14 [CLI-50], Table 14.1 and A.7/A.11 (a whole-call `Mutate` failure), [CLI-45] (the hold backoff
// and the reason of an action that a hold ends), [CLI-41] and [CLI-44] (a `Subscribe` retried after `KEYSPACE_SEEDING` is in the sync status),
// 09 [READ-57] (the reset of the reconnect backoff), [CLI-63] (the re-stamp of the actions that were sent), [CLI-28] (the dead letters of a
// revoked keyspace stay), 15 [COMPAT-39] and [COMPAT-40].

import { describe, expect, it } from "vitest";
import { set } from "../src/engine/index.js";
import { Code, RetryReason } from "../src/gen/abcp/v2/types_pb.js";
import { reject as mk, rejectWith } from "../src/fake/index.js";
import { goOffline, goOnline, KS, mutateRequests, ofType, rejectResult, retryResult, rig, tick, types, type Rig } from "./helpers/engine.js";

const setK = (key: string, v: unknown = 1): { keyspaceId: string; opType: string; changes: ReturnType<typeof set>[] } => ({ keyspaceId: KS, opType: "Set", changes: [set(key, v)] });
const approve = (key: string): { keyspaceId: string; opType: string; changes: ReturnType<typeof set>[] } => ({ keyspaceId: KS, opType: "Approve", changes: [set(key, 1)] });
const held = (r: Rig): { state: string; scope: string; lane?: string; code: string }[] =>
  r.events.filter((e) => e.type === "held").map((e) => e as unknown as { state: string; scope: string; lane?: string; code: string });

describe("14 [CLI-50], Table 14.1, A.7, A.11: a `Mutate` call that fails as a whole with a status and no ABCP detail", () => {
  it("holds each lane that the request carried, under the code PROTOCOL_ERROR, and emits one `protocol-error` with the RPC status and the lanes of the request", async () => {
    const r = await rig();
    await goOffline(r);
    await r.engine.submit(setK("settings/a"));
    await r.engine.submit(approve("settings/g"));
    r.server.inject("mutate", { kind: "status", status: "NotFound" });
    await goOnline(r);
    expect(mutateRequests(r)).toHaveLength(1);
    expect(mutateRequests(r)[0]?.mutations.map((m) => m.lane)).toHaveLength(2); // one request: the SEQUENCED action and the GATED one
    const pe = ofType(r, "protocol-error");
    expect(pe).toHaveLength(1);
    expect(pe[0]).toMatchObject({ what: "Mutate", status: "NotFound", keyspaces: [KS] });
    expect(pe[0]?.lanes?.map((l) => `${l.keyspaceId}/${l.lane}`).sort()).toEqual([`${KS}/GATED`, `${KS}/SEQUENCED`]);
    expect(held(r).map((h) => [h.state, h.scope, h.lane, h.code])).toEqual([
      ["start", "lane", "SEQUENCED", "PROTOCOL_ERROR"],
      ["start", "lane", "GATED", "PROTOCOL_ERROR"],
    ]);
    expect(r.engine.status().holds.map((h) => [h.scope, h.lane, h.code]).sort()).toEqual([
      ["lane", "GATED", "PROTOCOL_ERROR"],
      ["lane", "SEQUENCED", "PROTOCOL_ERROR"],
    ]);
  });

  it("the report comes once, when the hold starts: a resend of the head that fails again, as the hold backoff sends it, reports nothing", async () => {
    const r = await rig();
    r.random.fixed = 0.5;
    r.server.inject("mutate", { kind: "status", status: "FailedPrecondition" }, { times: 3 });
    await r.engine.submit(setK("settings/a"));
    await r.engine.settle();
    await tick(r, 31_000); // the first probe (0.5 of a minute)
    await tick(r, 61_000); // the second (0.5 of two minutes)
    expect(mutateRequests(r).length).toBeGreaterThanOrEqual(3);
    expect(ofType(r, "protocol-error")).toHaveLength(1);
    expect(held(r)).toHaveLength(1);
  });

  it("a resend that gets a result clears the hold; the outcome of the request's actions was unknown until then, and they commit", async () => {
    const r = await rig();
    r.random.fixed = 0.5;
    r.server.inject("mutate", { kind: "status", status: "AlreadyExists" });
    const a = await r.engine.submit(setK("settings/a"));
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual([]); // no `rejected`, no `outcome-unknown`: still pending
    await tick(r, 31_000);
    expect(types(r, a.mutationId)).toContain("committed");
    expect(held(r).map((h) => h.state)).toEqual(["start", "stop"]);
    expect(r.engine.status().holds).toEqual([]);
  });

  it("[CLI-45] the bound ends the actions: one that was sent is reported as outcome unknown, one that was never sent is dead-lettered with the reason PROTOCOL_ERROR", async () => {
    const r = await rig({ serverOptions: { limits: { maxOfflineSeconds: 3600 } } });
    r.server.inject("mutate", { kind: "status", status: "NotFound" }, { times: 1000 });
    const a = await r.engine.submit(setK("settings/a")); // sent: the request failed as a whole
    await r.engine.settle();
    const b = await r.engine.submit(setK("settings/b")); // behind it in the held lane: never sent
    await r.engine.settle();
    await tick(r, 2 * 3600 * 1000);
    expect(types(r, a.mutationId)).toContain("outcome-unknown");
    expect(r.events.find((e) => e.type === "outcome-unknown" && e.mutationId === a.mutationId)).toMatchObject({ cause: "HOLD_BOUND" });
    const dl = r.engine.deadLetterEntries().find((d) => d.mutationId === b.mutationId);
    expect(dl?.reason).toBe("PROTOCOL_ERROR"); // the code that its `held` event names (A.11)
    expect(held(r)[0]?.code).toBe("PROTOCOL_ERROR");
  });
});

describe("14 [CLI-45] the hold backoff: the n-th wait is drawn from [0, min(1 h, 1 min × 2^n)], n counted from 0, and n returns to 0 when the hold clears", () => {
  it("a second hold, after the first cleared, waits again at most a minute for its first probe", async () => {
    const r = await rig();
    r.random.fixed = 0.99;
    r.server.inject("mutate", { kind: "status", status: "NotFound" }, { times: 2 });
    await r.engine.submit(setK("settings/a"));
    await r.engine.settle();
    await tick(r, 59_000); // the first probe of the first hold: 0.99 × 1 min
    await tick(r, 1_000);
    expect(mutateRequests(r)).toHaveLength(2); // failed again: n is 1 now, the next wait is up to 2 minutes
    await tick(r, 60_000);
    expect(mutateRequests(r)).toHaveLength(2); // 0.99 × 2 min is not reached yet
    await tick(r, 60_000);
    expect(mutateRequests(r)).toHaveLength(3); // the third request commits: the hold clears
    expect(r.engine.status().holds).toEqual([]);
    // a new hold: its first probe is again within a minute
    r.server.inject("mutate", { kind: "status", status: "NotFound" }, { times: 1 });
    await r.engine.submit(setK("settings/b"));
    await r.engine.settle();
    const before = mutateRequests(r).length;
    await tick(r, 60_000);
    expect(mutateRequests(r).length).toBe(before + 1);
  });
});

describe("14 [CLI-41], [CLI-44]: a `Subscribe` that is being retried after `Retry{KEYSPACE_SEEDING}` is in the sync status", () => {
  it("shows the retry while the successor is not seeded, and no hold; clears it with the `Subscribed` frame", async () => {
    const r = await rig({ keyspaces: [KS, "dst1"] });
    r.random.fixed = 0.5;
    expect(r.engine.status().subscribeRetry).toBeUndefined();
    r.server.setSeeding("dst1", true);
    r.server.disconnect();
    await r.engine.settle();
    await tick(r, 1_500); // the reconnect: the `Subscribe` names the successor, and the whole call is refused
    expect(r.engine.status().subscribeRetry).toBe("KEYSPACE_SEEDING");
    expect(r.engine.status().holds).toEqual([]);
    await tick(r, 3 * 60 * 1000);
    expect(r.engine.status().subscribeRetry).toBe("KEYSPACE_SEEDING"); // still retried
    r.server.setSeeding("dst1", false);
    await tick(r, 6 * 60 * 1000);
    expect(r.engine.status().connection).toBe("live");
    expect(r.engine.status().subscribeRetry).toBeUndefined();
  });
});

describe("09 [READ-57]: n returns to 0 when a session has delivered its `Subscribed` frame and lived for at least 60 s", () => {
  it("a session that lasted 60 s without ever delivering `Subscribed` is a failed attempt: the next waits grow", async () => {
    // a heartbeat interval of 30 s makes the client's own deadline 60 s (09 [READ-56]: the shorter of 60 s and three heartbeat intervals)
    const r = await rig({ heartbeatMs: 30_000 });
    r.random.fixed = 0.99;
    r.server.inject("subscribe", { kind: "delay", ms: 600_000 }, { times: 2 }); // the call is not answered; the client's deadline ends it after 60 s
    r.server.disconnect();
    await r.engine.settle();
    await tick(r, 300_000);
    const t = r.server.calls.filter((c) => c.method === "subscribe").map((c) => c.at);
    expect(t.length).toBeGreaterThanOrEqual(4); // the first session, the two that hang, the one that is answered
    const gap = (i: number): number => (t[i + 1] as number) - (t[i] as number);
    // the first session lived no time (n = 0 after it: at most 1 s); each hanging one waited 60 s and delivered nothing, so it counts as a failed attempt:
    // the wait after the first is that of n = 1 (at most 2 s, 1,980 ms at 0.99), the wait after the second that of n = 2 (at most 4 s, 3,960 ms)
    expect(gap(1)).toBeGreaterThanOrEqual(60_000 + 1_900);
    expect(gap(2)).toBeGreaterThanOrEqual(60_000 + 3_900);
  });

  it("a session that delivered `Subscribed` and lived 60 s resets the count, whatever its end", async () => {
    const r = await rig();
    r.random.fixed = 0.99;
    r.server.disconnect();
    await r.engine.settle();
    await tick(r, 1_000);
    r.server.disconnect();
    await r.engine.settle();
    await tick(r, 2_000); // two failed attempts so far: the next wait would be that of n = 2
    await tick(r, 61_000); // this session lives for more than 60 s
    r.server.disconnect();
    await r.engine.settle();
    await tick(r, 1_000); // n is back at 0: the wait is at most 1 s
    expect(r.engine.status().connection).toBe("live");
  });
});

describe("14 [CLI-63]: after a new `client_id`, every SEQUENCED action that was sent and has no definitive result is re-stamped in authoring order; one never sent takes its first slot when it is first sent", () => {
  it("the sent actions take slots 1, 2 under the new client_id before anything is sent; an action that was never sent takes the next slot when it is first sent", async () => {
    const r = await rig();
    r.random.fixed = 0.5;
    await goOffline(r);
    const a = await r.engine.submit(setK("settings/a"));
    const b = await r.engine.submit(setK("settings/b"));
    r.server.inject("mutate", { kind: "status", status: "NotFound" }); // the request that carries a and b fails as a whole: both were sent, the lane is held
    await goOnline(r);
    const c = await r.engine.submit(setK("settings/c")); // never sent: the lane is held
    await r.engine.settle();
    expect(mutateRequests(r)).toHaveLength(1);
    const first = mutateRequests(r)[0]?.mutations[0]?.clientId;
    (r.engine as unknown as { reidentify(why: string): void }).reidentify("test");
    const stamp = (id: string): { clientId: string; clientSeq: number } => {
      const e = (r.engine as unknown as { entries: Map<string, { clientId: string; clientSeq: number }> }).entries.get(id);
      return { clientId: e?.clientId ?? "?", clientSeq: e?.clientSeq ?? -1 };
    };
    // re-stamped at once, in authoring order, with the new client_id
    expect(stamp(a.mutationId).clientSeq).toBe(1);
    expect(stamp(b.mutationId).clientSeq).toBe(2);
    expect(stamp(a.mutationId).clientId).not.toBe(first);
    expect(stamp(a.mutationId).clientId).toBe(stamp(b.mutationId).clientId);
    expect(stamp(c.mutationId)).toEqual({ clientId: "", clientSeq: 0 }); // never sent: no slot yet
    await tick(r, 31_000); // the hold's probe: the head is resent under the new client_id with its new slot
    await tick(r, 31_000);
    const sent = mutateRequests(r).slice(1).flatMap((q) => q.mutations.map((m) => [m.mutationId, Number(m.clientSeq), m.clientId] as const));
    expect(sent.map((x) => [x[0], x[1]])).toEqual([
      [a.mutationId, 1],
      [b.mutationId, 2],
      [c.mutationId, 3], // its first slot, when it is first sent
    ]);
    expect(new Set(sent.map((x) => x[2])).size).toBe(1);
  });
});

describe("14 [CLI-28]: the dead-letter entries of a revoked keyspace stay (the purge of [CLI-66] does not list them)", () => {
  it("a revocation of the keyspace purges the replica and the journal, and leaves the dead-letter store as it was", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, mk(Code.SCHEMA_INVALID)) });
    const a = await r.engine.submit(setK("settings/a"));
    await r.engine.settle();
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual([a.mutationId]);
    r.server.revokeKeyspaces(KS);
    await r.engine.settle();
    expect(r.engine.status().stopped[KS]).toBe("ACCESS_REVOKED");
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual([a.mutationId]);
    expect(r.engine.status().deadLetters).toBe(1);
    expect((await r.storage.load()).deadLetters.map((d) => d.mutationId)).toEqual([a.mutationId]);
  });
});

describe("15 [COMPAT-39], 14 [CLI-34]: a REJECTED decision is final whatever the class of its reject, and the answer of `GetMutationResult` is a decision too", () => {
  it("a decision that the client reads from a poll, with an unknown class, is dead-lettered and its dependents fail", async () => {
    const r = await rig();
    const g = await r.engine.submit(approve("settings/g"));
    await r.engine.settle();
    const dep = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/g", 2)] }); // depends on g
    await r.engine.settle();
    // the stream is cut, so that the decision comes from the poll
    r.server.faults.paused = true;
    r.server.decide(KS, g.mutationId, { status: "REJECTED", reject: rejectWith(901, 77) });
    await tick(r, 60_000);
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId).sort()).toEqual([dep.mutationId, g.mutationId].sort());
    expect(r.engine.status().holds).toEqual([]);
  });
});

describe("15 [COMPAT-40]: an unknown RetryReason is a Retry, after `Retry.after_ms`", () => {
  it("is resent unchanged, no sooner than after_ms, and holds nothing", async () => {
    const r = await rig();
    r.random.fixed = 0;
    r.server.inject("mutate", { kind: "result", result: (m) => retryResult(m, 999 as RetryReason, 20_000) }, { times: 1 });
    const a = await r.engine.submit(setK("settings/a"));
    await r.engine.settle();
    expect(mutateRequests(r)).toHaveLength(1);
    await tick(r, 19_000);
    expect(mutateRequests(r)).toHaveLength(1);
    await tick(r, 2_000);
    expect(mutateRequests(r)).toHaveLength(2);
    expect(r.engine.status().holds).toEqual([]);
    expect(types(r, a.mutationId)).toContain("committed");
  });
});
