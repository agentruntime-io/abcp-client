// C16c, round 16 on draft 14: what the server batches left to the client. 02 [ARC-63] (the protocol maxima that a server refuses before it keeps the
// message: a `Mutation` over 2,000 changes, an interest entry over 1,000 prefixes: the engine never builds one; the refusal that does reach it is
// engine-transport.test.ts's InvalidArgument case, 14 [CLI-50]), 15 [COMPAT-8] and [COMPAT-34] (a
// mutation lists the feature that its op type depends on), 09 [READ-57] (the resume names what fits in 16 MiB in the `Subscribe` and the rest in
// `UpdateSubscription` adds), 16 [SEC-3] (a structurally invalid record stops the keyspace and is reported: not as a record that needs a later revision).

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { set, StaticManifests, type StaticManifestSpec } from "../src/engine/index.js";
import { ChangeKind, KeyChangeSchema } from "../src/gen/abcp/v2/types_pb.js";
import { SubscribeRequestSchema } from "../src/gen/abcp/v2/sync_pb.js";
import { encodeMessage } from "../src/engine/wire.js";
import { KS, MANIFEST, kc, mutateRequests, ofType, rig, tick, type Rig } from "./helpers/engine.js";

const MiB = 1 << 20;
const keys = (n: number): ReturnType<typeof set>[] => Array.from({ length: n }, (_, i) => set(`settings/k${i}`, i));
const protocolErrors = (r: Rig): string[] => ofType(r, "protocol-error").map((e) => e.what);

describe("02 [ARC-63]: a `Mutation` over 2,000 changes is refused by a server before it keeps the message: the engine never builds one, whatever limit the server advertises", () => {
  it("a server that advertises max_changes_per_action 5,000 (over its protocol maximum, 07 [MUT-75]) gets at most 2,000: 2,001 changes are refused locally, 2,000 are sent, and the engine reports the value", async () => {
    const r = await rig({ serverOptions: { limits: { maxChangesPerAction: 5_000, maxActionBytes: 8 * MiB, maxRequestBytes: 8 * MiB } } });
    await expect(r.engine.submit({ keyspaceId: KS, opType: "Set", changes: keys(2_001) })).rejects.toMatchObject({ name: "LocalRefusal", code: "INVALID" });
    expect(r.engine.outbox()).toEqual([]);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: keys(2_000) });
    await r.engine.settle();
    const sent = mutateRequests(r).flatMap((q) => q.mutations);
    expect(sent.map((m) => m.mutationId)).toEqual([a.mutationId]);
    expect(sent[0]?.changes).toHaveLength(2_000);
    expect(sent.every((m) => m.changes.length <= 2_000)).toBe(true);
    expect(protocolErrors(r)).toEqual(["AppLimits.max_changes_per_action"]);
  });

  it("a conforming server's value is used as it is: no report, and an action over it is still refused", async () => {
    const r = await rig({ serverOptions: { limits: { maxChangesPerAction: 150 } } });
    await expect(r.engine.submit({ keyspaceId: KS, opType: "Set", changes: keys(151) })).rejects.toMatchObject({ code: "INVALID" });
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: keys(150) });
    expect(protocolErrors(r)).toEqual([]);
  });

  it("an interest of 1,000 prefixes is the most that an `UpdateSubscription` entry may name: a server that advertises more (max_interest_prefixes 5,000) is held to 1,000", async () => {
    const r = await rig({ serverOptions: { limits: { maxInterestPrefixes: 5_000 } } });
    const prefixes = (n: number): string[] => Array.from({ length: n }, (_, i) => `p${i}`);
    await expect(r.engine.setInterest(KS, prefixes(1_001))).rejects.toMatchObject({ code: "INVALID" });
    await r.engine.setInterest(KS, prefixes(1_000));
    await r.engine.settle();
    expect(r.engine.interest(KS)).toHaveLength(1_000);
    const entries = r.server.calls.filter((c) => c.method === "updateSubscription").flatMap((c) => (c.request as { setInterest: { interestPrefixes: string[] }[] }).setInterest);
    expect(entries.every((e) => e.interestPrefixes.length <= 1_000)).toBe(true);
    expect(protocolErrors(r)).toEqual(["AppLimits.max_interest_prefixes"]);
  });
});

describe("15 [COMPAT-8], [COMPAT-34], 07 Table 7-4 sub-step 6: a mutation lists `required_features` for the feature that its op type depends on", () => {
  const spec: StaticManifestSpec = { ...MANIFEST, opTypes: { ...MANIFEST.opTypes, "abcp.escrow.v1.Grant": { lane: "GATED" }, "abcp.lease.v1.Acquire": { lane: "GATED" } } };
  const featuresOf = (r: Rig): Record<string, string[]> => Object.fromEntries(mutateRequests(r).flatMap((q) => q.mutations.map((m) => [m.opType, [...m.requiredFeatures]] as const)));

  it("an op type of the escrow module, and a spend op type, list `escrow.v1`; any other op type lists none", async () => {
    const r = await rig({ manifests: new StaticManifests(spec) });
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.submit({ keyspaceId: KS, opType: "Pay", changes: [set("settings/p", 1)] });
    await r.engine.submit({ keyspaceId: KS, opType: "abcp.escrow.v1.Grant", changes: [set("settings/g", 1)] });
    await r.engine.submit({ keyspaceId: KS, opType: "abcp.lease.v1.Acquire", changes: [set("settings/l", 1)] });
    await r.engine.settle();
    expect(featuresOf(r)).toEqual({ Set: [], Pay: ["escrow.v1"], "abcp.escrow.v1.Grant": ["escrow.v1"], "abcp.lease.v1.Acquire": [] });
  });

  it("the feature is also in the request's `supported_features`, which the same request carries (07 Table 7-3 item 3)", async () => {
    const r = await rig({ manifests: new StaticManifests(spec) });
    await r.engine.submit({ keyspaceId: KS, opType: "Pay", changes: [set("settings/p", 1)] });
    await r.engine.settle();
    const q = mutateRequests(r)[0];
    expect(q?.supportedFeatures).toContain("escrow.v1");
    expect(q?.mutations[0]?.requiredFeatures).toEqual(["escrow.v1"]);
  });
});

describe("09 [READ-57]: cursors that would make the `SubscribeRequest` longer than 16 MiB are split: some in the `Subscribe`, the rest in `UpdateSubscription` adds on the new session, each request within that size", () => {
  const KSS = ["ks1", "ks2", "ks3", "ks4", "ks5"];
  const bigInterest = (i: number): string[] => Array.from({ length: 8 }, (_, j) => `issues/i${i}p${j}${"x".repeat(50)}`);

  async function withLimit(maxBytes: number): Promise<Rig> {
    const r = await rig({ keyspaces: KSS, start: false, cfg: { maxSubscribeBytes: maxBytes } });
    for (const [i, k] of KSS.entries()) await r.engine.setInterest(k, bigInterest(i));
    r.engine.start();
    await r.engine.settle();
    await tick(r, 5_000);
    return r;
  }
  const subscribes = (r: Rig): { keyspaces: { keyspaceId: string }[] }[] => r.server.calls.filter((c) => c.method === "subscribe").map((c) => c.request as { keyspaces: { keyspaceId: string }[] });
  const adds = (r: Rig): string[] => r.server.calls.filter((c) => c.method === "updateSubscription").flatMap((c) => (c.request as { add: { keyspaceId: string }[] }).add.map((a) => a.keyspaceId));

  it("with a limit that two cursors fill, the Subscribe names two keyspaces and the other three are added on the live session; every keyspace ends up followed", async () => {
    const one = encodeMessage(SubscribeRequestSchema, create(SubscribeRequestSchema, { keyspaces: [{ keyspaceId: "ks1", interestPrefixes: bigInterest(0) }] as never, supportedFeatures: ["escrow.v1"], clientId: "x".repeat(36) })).length;
    const r = await withLimit(Math.floor(one * 2.5));
    const first = subscribes(r)[0];
    expect(first?.keyspaces.length).toBeGreaterThan(0);
    expect(first?.keyspaces.length).toBeLessThan(KSS.length);
    const named = new Set([...(first?.keyspaces ?? []).map((k) => k.keyspaceId), ...adds(r)]);
    expect([...named].sort()).toEqual(KSS);
    for (const s of subscribes(r)) expect(encodeMessage(SubscribeRequestSchema, s as never).length).toBeLessThanOrEqual(Math.floor(one * 2.5));
    expect(r.engine.status().holds).toEqual([]);
    // every keyspace is followed on the live session: a record that the server writes reaches the client
    for (const [i, k] of KSS.entries()) r.server.write(k, [kc(`${bigInterest(i)[0]}/title`, "set", 1)]);
    await r.engine.settle();
    for (const [i, k] of KSS.entries()) expect(r.engine.confirmed(k).has(`${bigInterest(i)[0]}/title`), k).toBe(true);
  });

  it("02 [ARC-63]: a Subscribe names at most `max_keyspaces_per_session`'s protocol maximum of cursors (a test lowers it to two); the rest are added on the live session, one entry each", async () => {
    const r = await rig({ keyspaces: KSS, cfg: { maxSubscribeCursors: 2 } });
    await tick(r, 5_000);
    expect(subscribes(r)[0]?.keyspaces.map((k) => k.keyspaceId)).toEqual(["ks1", "ks2"]);
    expect([...adds(r)].sort()).toEqual(["ks3", "ks4", "ks5"]);
    for (const c of r.server.calls.filter((x) => x.method === "updateSubscription")) expect((c.request as { add: unknown[] }).add.length).toBeLessThanOrEqual(1);
    // every keyspace is followed on the live session
    for (const k of KSS) r.server.write(k, [kc("settings/after", "set", 1)]);
    await r.engine.settle();
    for (const k of KSS) expect(r.engine.confirmed(k).has("settings/after"), k).toBe(true);
  });

  it("the default is the protocol maximum of 10,000 cursors, so one Subscribe names every keyspace of a few", async () => {
    const r = await rig({ keyspaces: KSS });
    expect(subscribes(r)[0]?.keyspaces).toHaveLength(KSS.length);
  });

  it("with the default limit (16 MiB) one Subscribe names every keyspace and nothing is added", async () => {
    const r = await rig({ keyspaces: KSS });
    expect(subscribes(r)[0]?.keyspaces).toHaveLength(KSS.length);
    expect(adds(r)).toEqual([]);
  });
});

describe("16 [SEC-3], 14 [CLI-50] and Table 14.1: a structurally invalid record stops the engine for that keyspace and is reported (`protocol-error`, the hold PROTOCOL_ERROR); it is not a record that needs a later revision, so the hold is not UPGRADE_REQUIRED", () => {
  it("a record that changes one key twice: the confirmed state stays at the previous seq, `protocol-error` is reported, the keyspace is held as PROTOCOL_ERROR, and another keyspace goes on", async () => {
    const r = await rig({ keyspaces: ["ks1", "ks2"] });
    r.server.write("ks1", [kc("settings/ok", "set", 1)]);
    await r.engine.settle();
    r.server.appendRaw("ks1", { changes: [create(KeyChangeSchema, { key: "settings/dup", kind: ChangeKind.SET, value: new TextEncoder().encode("1"), encoding: "json/v1" }), create(KeyChangeSchema, { key: "settings/dup", kind: ChangeKind.SET, value: new TextEncoder().encode("2"), encoding: "json/v1" })] });
    await r.engine.settle();
    expect(r.engine.confirmed("ks1").has("settings/dup")).toBe(false);
    expect(r.engine.confirmed("ks1").has("settings/ok")).toBe(true);
    expect(ofType(r, "protocol-error").map((e) => [e.what, e.keyspaces])).toEqual([["RecordsFrame", ["ks1"]]]);
    expect(r.engine.status().holds.map((h) => [h.keyspaceId, h.code])).toEqual([["ks1", "PROTOCOL_ERROR"]]);
    r.server.write("ks2", [kc("settings/two", "set", 2)]);
    await tick(r, 30_000); // the session ended with the violation and resumes with its backoff
    expect(r.engine.confirmed("ks2").has("settings/two")).toBe(true);
  });

  it("Table 14.1: the held keyspace is sent nothing (an action authored into it waits in the outbox), and the other keyspace still takes actions", async () => {
    const r = await rig({ keyspaces: ["ks1", "ks2"] });
    r.server.write("ks1", [kc("settings/ok", "set", 1)]);
    await r.engine.settle();
    r.server.appendRaw("ks1", { changes: [create(KeyChangeSchema, { key: "settings/dup", kind: ChangeKind.SET, value: new TextEncoder().encode("1"), encoding: "json/v1" }), create(KeyChangeSchema, { key: "settings/dup", kind: ChangeKind.SET, value: new TextEncoder().encode("2"), encoding: "json/v1" })] });
    await r.engine.settle();
    expect(r.engine.status().holds.map((h) => [h.keyspaceId, h.code])).toEqual([["ks1", "PROTOCOL_ERROR"]]);
    // 16 [SEC-3]: an action authored into the held keyspace waits in the outbox, and nothing is sent to the keyspace while the hold lasts
    const sentBefore = mutateRequests(r).flatMap((q) => q.mutations).length;
    await r.engine.submit({ keyspaceId: "ks1", opType: "Set", changes: [set("settings/after", 1)] });
    await r.engine.submit({ keyspaceId: "ks2", opType: "Set", changes: [set("settings/other", 1)] });
    await tick(r, 1_000);
    const sent = mutateRequests(r).flatMap((q) => q.mutations).slice(sentBefore);
    expect(sent.map((m) => m.keyspaceId ?? "")).not.toContain("ks1");
    expect(r.engine.outbox().filter((e) => e.keyspaceId === "ks1")).toHaveLength(1);
    expect(sent.map((m) => m.opType)).toContain("Set"); // the action of ks2 was sent
  });
});
