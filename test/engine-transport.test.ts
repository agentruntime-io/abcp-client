// 14 [CLI-50] transport errors as A.7 defines them, 14 [CLI-82] offline_mode, and the statuses that A.7 gives no row (17 [CONF-33]).

import { describe, expect, it } from "vitest";
import { set, type RpcStatus } from "../src/engine/index.js";
import { Code, RetryReason, RetrySchema } from "../src/gen/abpc/v2/types_pb.js";
import { create } from "@bufbuild/protobuf";
import { reject as mk } from "../src/fake/index.js";
import { goOffline, goOnline, KS, mutateRequests, ofType, rig, sentIds, tick, types } from "./helpers/engine.js";

describe("14 [CLI-50] transport errors", () => {
  it("Unauthenticated refreshes the token, then resends once", async () => {
    let refreshes = 0;
    const r = await rig({ cfg: { tokens: { refresh: () => (refreshes++, Promise.resolve(true)) } } });
    r.server.inject("mutate", { kind: "status", status: "Unauthenticated" });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 1);
    expect(refreshes).toBe(1);
    expect(sentIds(r)).toEqual([a.mutationId, a.mutationId]);
    expect(types(r, a.mutationId)).toEqual(["committed"]);
  });

  it("Unauthenticated with the SUBJECT_REVOKED detail: the request is never resent (TODO: purge and quarantine, 14 [CLI-79])", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "status", status: "Unauthenticated", reject: mk(Code.PERMISSION_DENIED, { details: { cause: "SUBJECT_REVOKED" } }) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 600_000);
    expect(sentIds(r)).toEqual([a.mutationId]);
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "app", code: "SUBJECT_REVOKED" })]);
    expect(r.engine.outbox()).toHaveLength(1); // kept, never deleted unreported
  });

  it("ResourceExhausted with no detail splits the request; a single mutation that is still too large is dead-lettered with PAYLOAD_TOO_LARGE", async () => {
    const r = await rig();
    await goOffline(r);
    const ids: string[] = [];
    for (let i = 0; i < 3; i++) ids.push((await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set(`settings/k${i}`, i)] })).mutationId);
    const before = mutateRequests(r).length;
    r.server.inject("mutate", { kind: "status", status: "ResourceExhausted" });
    await goOnline(r);
    await tick(r, 3000);
    const reqs = mutateRequests(r).slice(before);
    expect(reqs[0]?.mutations).toHaveLength(3);
    expect(reqs.slice(1).map((q) => q.mutations.length)).toEqual([1, 1, 1]); // each alone
    ids.forEach((id) => expect(types(r, id)).toEqual(["committed"]));

    const w = await rig();
    w.server.inject("mutate", { kind: "status", status: "ResourceExhausted" });
    const big = await w.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/big", 1)] });
    await w.engine.settle();
    expect(types(w, big.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(w.engine.deadLetterEntries()[0]).toMatchObject({ reason: "PAYLOAD_TOO_LARGE" });
    expect(w.engine.deadLetterEntries()[0]?.reject.errorClass).toBe(3);
  });

  it("InvalidArgument for a whole MutateRequest, which recorded none of its mutations: each is sent alone once, and one that still gets InvalidArgument is dead-lettered with that status as its reason", async () => {
    const r = await rig();
    await goOffline(r);
    const good1 = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/g1", 1)] });
    const bad = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/bad", 1)] });
    const good2 = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/g2", 1)] });
    const bads = (q: unknown): boolean => (q as { mutations: { changes: { key: string }[] }[] }).mutations.some((m) => m.changes[0]?.key === "settings/bad");
    r.server.inject("mutate", { kind: "status", status: "InvalidArgument", reject: mk(Code.INVALID_ENVELOPE) }, { times: 10, match: (q) => bads(q) });
    await goOnline(r);
    await tick(r, 3000);
    expect(types(r, good1.mutationId)).toEqual(["committed"]);
    expect(types(r, good2.mutationId)).toEqual(["committed"]);
    expect(types(r, bad.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(r.engine.deadLetterEntries()[0]?.reason).toBe("InvalidArgument");
  });

  it("Unimplemented for an RPC of v2.0 stops sync with that server", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "status", status: "Unimplemented" });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 600_000);
    expect(r.engine.status().connection).toBe("stopped");
    expect(ofType(r, "protocol-error")).toHaveLength(1);
    expect(sentIds(r)).toEqual([a.mutationId]);
    expect(r.engine.outbox()).toHaveLength(1);
  });

  it("ResourceExhausted with Retry{RATE_LIMITED}: back off and resend", async () => {
    const r = await rig();
    r.random.fixed = 0;
    r.server.inject("mutate", { kind: "status", status: "ResourceExhausted", retry: create(RetrySchema, { reason: RetryReason.RATE_LIMITED, afterMs: 20_000 }) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 10_000);
    expect(sentIds(r)).toHaveLength(1);
    await tick(r, 11_000);
    expect(types(r, a.mutationId)).toEqual(["committed"]);
  });

  it("an unknown outcome of a sent action is resolved by [CLI-24]: the identical content, the client backoff", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "lose" });
    r.server.faults.paused = true;
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 3000);
    r.server.resumeDelivery();
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["committed"]);
    expect(r.server.records(KS).filter((x) => x.mutationId === a.mutationId)).toHaveLength(1);
  });
});

describe("17 [CONF-33] the statuses that A.7 gives no row are treated as Unavailable, for Mutate, LoadState and Subscribe", () => {
  const statuses: RpcStatus[] = ["Unknown", "Internal", "Cancelled", "DataLoss", "OutOfRange"];
  it.each(statuses)("%s with no ABPC detail: a sent action is resent with the identical content until the stored outcome comes; a load and a subscribe are repeated", async (status) => {
    const r = await rig({ start: false });
    r.server.inject("loadState", { kind: "status", status });
    r.server.inject("subscribe", { kind: "status", status });
    r.server.inject("mutate", { kind: "lose", status });
    r.engine.start();
    await r.engine.settle();
    expect(r.engine.status().connection).not.toBe("live");
    for (let i = 0; i < 6; i++) await tick(r, 2000);
    expect(r.engine.status().connection).toBe("live");
    expect(r.engine.cursor(KS)?.loaded).toBe(true);
    r.server.faults.paused = true;
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    for (let i = 0; i < 4; i++) await tick(r, 2000);
    r.server.resumeDelivery();
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["committed"]);
    expect(r.engine.status().deadLetters).toBe(0);
    expect(r.server.records(KS).filter((x) => x.mutationId === a.mutationId)).toHaveLength(1);
    const contents = mutateRequests(r).flatMap((q) => q.mutations.map((m) => Buffer.from(m.changes[0]?.value ?? new Uint8Array()).toString()));
    expect(new Set(contents)).toEqual(new Set(["1"]));
  });
});

describe("14 [CLI-82] offline_mode", () => {
  it("fail_fast: an action authored while offline is refused at once, committed to nothing, with `rejected` and the cause OFFLINE; online it is accepted", async () => {
    const r = await rig();
    const ok = await r.engine.submit({ keyspaceId: KS, opType: "Quick", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(types(r, ok.mutationId)).toEqual(["committed"]);
    await goOffline(r);
    await expect(r.engine.submit({ keyspaceId: KS, opType: "Quick", changes: [set("settings/b", 1)] })).rejects.toMatchObject({ name: "LocalRefusal", code: "OFFLINE" });
    const rej = ofType(r, "rejected");
    expect(rej).toHaveLength(1);
    expect(rej[0]?.cause).toBe("OFFLINE");
    expect(rej[0]?.reject).toBeUndefined();
    expect(r.engine.outbox()).toHaveLength(0);
    expect(r.engine.view(KS).get("settings/b")).toBeUndefined();
    expect(new Set(r.storage.log.flat().flatMap((o) => (o.op === "putEntry" ? [o.entry.mutationId] : []))).size).toBe(1); // only the first action was ever stored
  });

  it("the keyspace being held counts as offline for it", async () => {
    const r = await rig({ keyspaces: [KS, "ks2"] });
    r.server.freeze(KS);
    await r.engine.settle();
    await expect(r.engine.submit({ keyspaceId: KS, opType: "Quick", changes: [set("settings/b", 1)] })).rejects.toMatchObject({ code: "OFFLINE" });
    const ok = await r.engine.submit({ keyspaceId: "ks2", opType: "Quick", changes: [set("settings/b", 1)] });
    await r.engine.settle();
    expect(types(r, ok.mutationId)).toEqual(["committed"]);
    // queue_recheck, the default, is accepted into the outbox and sent once the hold clears
    const q = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/q", 1)] });
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([q.mutationId]);
  });

  it("applies only at authoring: an action accepted online stays queued if the client goes offline before it is sent", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "delay", ms: 1000 });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Quick", changes: [set("settings/a", 1)] });
    r.server.disconnect();
    await tick(r, 1000);
    await goOnline(r);
    await tick(r, 3000);
    expect(types(r, a.mutationId)).toEqual(["committed"]);
  });
});
