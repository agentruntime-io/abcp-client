// C6, presence on the engine's session: 09 §9.8 ([READ-71] to [READ-83]), 14 [CLI-61], [CLI-66], [CLI-78], 02 [ARC-51] item 4, 16 [SEC-1], A.7. Against the fake server, which answers as
// `server/internal/session/presence*.go` does on the points that a client's behaviour depends on (src/fake/presence.ts).

import { create, toBinary } from "@bufbuild/protobuf";
import { TimestampSchema } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "vitest";
import { BytesTransport, LocalRefusal, ProtocolViolation, StaticManifests, type PresenceEvent } from "../src/engine/index.js";
import { reject as mk } from "../src/fake/index.js";
import { PublishPresenceRequestSchema, PublishPresenceResponseSchema, SubscribeRequestSchema } from "../src/gen/abpc/v2/sync_pb.js";
import { Code, RetryReason, RetrySchema } from "../src/gen/abpc/v2/types_pb.js";
import { FakeRaw, breakUtf8 } from "./helpers/raw.js";
import { goOffline, goOnline, kc, rig } from "./helpers/engine.js";
import { DOC, frame, KS, PRESENCE_MANIFEST, plain, presenceManifests, prig, protocolErrors, publishedStates, requests, ROOM, second, stateOf } from "./helpers/presence.js";
import { FakeServer, ManualClock } from "../src/fake/index.js";
import { settleAll, tabsRig } from "./helpers/tabs.js";

const joins = (s: { calls: { method: string }[] }): number => s.calls.filter((c) => c.method === "joinPresence").length;
const publishes = (s: { calls: { method: string }[] }): number => s.calls.filter((c) => c.method === "publishPresence").length;
const leaves = (s: { calls: { method: string }[] }): number => s.calls.filter((c) => c.method === "leavePresence").length;

describe("09 [READ-73]: a join makes the session a member and the snapshot replaces the view", () => {
  it("the engine joins on its session once the session follows the keyspace, and the view is what the server sent", async () => {
    const a = await prig({ seed: 11 });
    const b = await second(a);
    await b.engine.joinPresence(KS, ROOM);
    await b.engine.publishPresence(KS, ROOM, { viewing: true });
    await b.engine.settle();
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    expect(joins(a.server)).toBe(2);
    // the join of A got the snapshot, which holds B's entry; it was the first frame of the topic
    const entries = a.engine.presenceEntries(KS, ROOM);
    expect(entries.map((e) => e.clientId)).toEqual([b.engine.clientId]);
    expect(stateOf((entries[0] as { state: Uint8Array }).state)).toEqual({ viewing: true });
    const first = a.server.presence.sent.find((s) => s.frame.topic === ROOM && a.server.presence.members(KS, ROOM).includes(s.session));
    expect(first?.frame.snapshot).toBe(true);
    // the requests named the session that Subscribe opened, never the client id
    const req = requests<{ sessionId: string; keyspaceId: string; topic: string }>(a.server, "joinPresence")[1];
    expect(req).toMatchObject({ keyspaceId: KS, topic: ROOM });
    expect(req?.sessionId).toMatch(/^session-/);
  });

  it("a tab that joins before the engine is online is registered, and the engine joins when the session follows the keyspace", async () => {
    const a = await prig({ seed: 11, start: false });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.publishPresence(KS, ROOM, { viewing: true });
    expect(joins(a.server)).toBe(0);
    expect(publishes(a.server)).toBe(0);
    a.engine.start();
    await a.engine.settle();
    expect(joins(a.server)).toBe(1);
    expect(publishedStates(a.server)).toEqual([{ viewing: true }]);
  });

  it("a repeated join sends the snapshot again, and the snapshot replaces the view: an entry that it lacks is gone", async () => {
    const a = await prig({ seed: 11 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    const now = a.clock.now();
    a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: "c-1", state: { viewing: true }, expiresMs: now + 20_000 }] }));
    await a.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM).map((e) => e.clientId)).toEqual(["c-1"]);
    a.server.sendFrame(frame({ topic: ROOM, snapshot: true, entries: [{ clientId: "c-2", state: { viewing: false }, expiresMs: now + 20_000 }] }));
    await a.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM).map((e) => e.clientId)).toEqual(["c-2"]);
  });
});

describe("02 [ARC-51] item 4, 09 [READ-73]: after a new session the engine joins its topics again and publishes its entries again", () => {
  it("a dropped session: the new one is joined, the entry is published again, and the other member sees it", async () => {
    const a = await prig({ seed: 11 });
    const b = await second(a);
    await b.engine.joinPresence(KS, ROOM);
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.publishPresence(KS, ROOM, { viewing: true });
    await a.engine.settle();
    expect(joins(a.server)).toBe(2);
    expect(publishes(a.server)).toBe(1);
    await goOffline(a);
    await goOnline(a);
    await a.clock.advance(2000);
    await b.engine.settle();
    expect(a.engine.status().connection).toBe("live");
    const ofAna = (m: string): number => a.server.calls.filter((c) => c.method === m && c.sub === "ana").length;
    expect(ofAna("joinPresence")).toBe(2); // A joined on its new session (the server dropped B's session too: B joined again as well)
    expect(ofAna("publishPresence")).toBe(2);
    expect(a.server.presence.entries(KS, ROOM).map((e) => e.clientId)).toEqual([a.engine.clientId]);
    expect(b.engine.presenceEntries(KS, ROOM).map((e) => e.clientId)).toEqual([a.engine.clientId]);
  });
});

describe("09 [READ-74]: one entry per topic for all the tabs, published through the engine's one session (14 [CLI-61])", () => {
  it("the entry is the latest state that one of the tabs set; the engine leaves the topic only when the last tab has left", async () => {
    const r = tabsRig({ manifests: new StaticManifests(PRESENCE_MANIFEST) });
    const t1 = await r.open();
    const t2 = await r.open();
    await settleAll(t1.engine, t2.engine);
    expect(t1.engine.status().role).toBe("flusher");
    expect(t2.engine.status().role).toBe("follower");
    await t1.engine.joinPresence(KS, ROOM);
    await t2.engine.joinPresence(KS, ROOM);
    await settleAll(t1.engine, t2.engine);
    expect(joins(r.server)).toBe(1); // one membership for the engine
    await t1.engine.publishPresence(KS, ROOM, { cursor: "one" });
    await settleAll(t1.engine, t2.engine);
    await r.clock.advance(1100);
    await t2.engine.publishPresence(KS, ROOM, { cursor: "two" });
    await settleAll(t1.engine, t2.engine);
    await r.clock.advance(1100);
    await settleAll(t1.engine, t2.engine);
    expect(publishedStates(r.server)).toEqual([{ cursor: "one" }, { cursor: "two" }]); // the latest state that a tab set
    expect(r.server.presence.entries(KS, ROOM)).toHaveLength(1);
    // one tab leaves: the entry stays, and no `left_client_ids` goes to the other members; the engine has not left
    await t1.engine.leavePresence(KS, ROOM);
    await settleAll(t1.engine, t2.engine);
    expect(leaves(r.server)).toBe(0);
    expect(r.server.presence.entries(KS, ROOM)).toHaveLength(1);
    // the last tab leaves: LeavePresence
    await t2.engine.leavePresence(KS, ROOM);
    await settleAll(t1.engine, t2.engine);
    expect(leaves(r.server)).toBe(1);
    expect(r.server.presence.entries(KS, ROOM)).toHaveLength(0);
    expect(t1.engine.presenceTopics()).toEqual([]);
  });

  it("a follower tab reads the view of the topic through the engine (14 [CLI-61])", async () => {
    const r = tabsRig({ manifests: new StaticManifests(PRESENCE_MANIFEST) });
    const t1 = await r.open();
    const t2 = await r.open();
    const bob = await rig({ manifests: r.manifests, server: r.server, clock: r.clock, sub: "bob", seed: 31 });
    await settleAll(t1.engine, t2.engine, bob.engine);
    await t2.engine.joinPresence(KS, ROOM);
    await bob.engine.joinPresence(KS, ROOM);
    await bob.engine.publishPresence(KS, ROOM, { viewing: true });
    await settleAll(t1.engine, t2.engine, bob.engine);
    expect(t2.engine.presenceEntries(KS, ROOM).map((e) => stateOf(e.state))).toEqual([{ viewing: true }]);
    const seen: PresenceEvent["type"][] = [];
    t2.engine.onPresence((e) => seen.push(e.type));
    await bob.engine.publishPresence(KS, ROOM, { viewing: false });
    await r.clock.advance(1100);
    await settleAll(t1.engine, t2.engine, bob.engine);
    expect(t2.engine.presenceEntries(KS, ROOM).map((e) => stateOf(e.state))).toEqual([{ viewing: false }]);
    expect(seen).toContain("changed");
  });

  it("a follower tab that joins a topic that the engine is in already gets the view that the engine holds", async () => {
    const r = tabsRig({ manifests: new StaticManifests(PRESENCE_MANIFEST) });
    const t1 = await r.open();
    const t2 = await r.open();
    const bob = await rig({ manifests: r.manifests, server: r.server, clock: r.clock, sub: "bob", seed: 31 });
    await settleAll(t1.engine, t2.engine, bob.engine);
    await t1.engine.joinPresence(KS, ROOM);
    await bob.engine.joinPresence(KS, ROOM);
    await bob.engine.publishPresence(KS, ROOM, { viewing: true });
    await settleAll(t1.engine, t2.engine, bob.engine);
    // a tab that opens later has heard nothing of the topic; when it joins, the engine is in the topic already: nothing is joined, and the view that the engine holds is what the tab reads
    const t3 = await r.open();
    await settleAll(t1.engine, t2.engine, t3.engine, bob.engine);
    expect(t3.engine.presenceEntries(KS, ROOM)).toEqual([]);
    await t3.engine.joinPresence(KS, ROOM);
    await settleAll(t1.engine, t2.engine, t3.engine, bob.engine);
    expect(t3.engine.presenceEntries(KS, ROOM).map((e) => stateOf(e.state))).toEqual([{ viewing: true }]);
    expect(joins(r.server)).toBe(2); // the engine joined once for its tabs, and bob once
  });

  it("when the flusher goes away the tab that takes the role joins and publishes again for the tabs that are in the topic (09 [READ-73], 14 [CLI-63])", async () => {
    const r = tabsRig({ manifests: new StaticManifests(PRESENCE_MANIFEST) });
    const t1 = await r.open();
    const t2 = await r.open();
    await t2.engine.joinPresence(KS, ROOM);
    await t2.engine.publishPresence(KS, ROOM, { cursor: "x" });
    await settleAll(t1.engine, t2.engine);
    expect(publishedStates(r.server)).toEqual([{ cursor: "x" }]);
    await t1.engine.stop();
    await settleAll(t2.engine);
    await r.clock.advance(2000);
    await settleAll(t2.engine);
    expect(t2.engine.status().role).toBe("flusher");
    expect(publishedStates(r.server)).toEqual([{ cursor: "x" }, { cursor: "x" }]);
    expect(r.server.presence.entries(KS, ROOM)).toHaveLength(1);
  });

  it("a state over limits.max_presence_state_bytes is refused before it is sent; any value that is not json/v1 is too (14 [CLI-22])", async () => {
    const a = await prig({ seed: 11 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    expect(a.engine.limits.maxPresenceStateBytes).toBe(4096);
    await expect(a.engine.publishPresence(KS, ROOM, { x: "y".repeat(5000) })).rejects.toBeInstanceOf(LocalRefusal);
    await expect(a.engine.publishPresence(KS, ROOM, { x: undefined })).rejects.toBeInstanceOf(LocalRefusal);
    expect(publishes(a.server)).toBe(0);
    await expect(a.engine.publishPresence(KS, "room/never-joined", { x: 1 })).rejects.toBeInstanceOf(LocalRefusal);
    await expect(a.engine.joinPresence("ks-not-followed", ROOM)).rejects.toBeInstanceOf(LocalRefusal);
    await expect(a.engine.joinPresence(KS, "room/")).rejects.toBeInstanceOf(LocalRefusal); // 09 [READ-72]: whole segments, no final "/"
  });

  it("the server's PAYLOAD_TOO_LARGE and SCHEMA_INVALID are reported and the state is not sent again; a new state is sent", async () => {
    const a = await prig({ seed: 11, serverOptions: {} });
    const events: PresenceEvent[] = [];
    a.engine.onPresence((e) => events.push(e));
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    a.server.inject("publishPresence", { kind: "status", status: "InvalidArgument", reject: mk(Code.PAYLOAD_TOO_LARGE) });
    await a.engine.publishPresence(KS, ROOM, { x: 1 });
    await a.engine.settle();
    expect(events.filter((e) => e.type === "refused")).toMatchObject([{ operation: "publish", code: "PAYLOAD_TOO_LARGE", status: "InvalidArgument" }]);
    await a.clock.advance(120_000);
    expect(publishes(a.server)).toBe(1); // not resent
    a.server.inject("publishPresence", { kind: "status", status: "InvalidArgument", reject: mk(Code.SCHEMA_INVALID) });
    await a.engine.publishPresence(KS, ROOM, { x: 2 });
    await a.engine.settle();
    expect(events.filter((e) => e.type === "refused").map((e) => (e as { code: string }).code)).toEqual(["PAYLOAD_TOO_LARGE", "SCHEMA_INVALID"]);
    await a.engine.publishPresence(KS, ROOM, { x: 3 }); // a new state is tried
    await a.clock.advance(1100);
    expect(publishedStates(a.server)).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
    expect(a.server.presence.entries(KS, ROOM).map((e) => stateOf(e.state))).toEqual([{ x: 3 }]);
  });
});

describe("09 [READ-76]: an entry expires presence_ttl_seconds after its last publish; the engine publishes again at about half the TTL", () => {
  it("the same state is published again at half the TTL, so that the entry never expires", async () => {
    const a = await prig({ seed: 11 });
    const b = await second(a);
    await b.engine.joinPresence(KS, ROOM);
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.publishPresence(KS, ROOM, { viewing: true });
    await a.engine.settle();
    expect(publishes(a.server)).toBe(1);
    await a.clock.advance(14_000);
    expect(publishes(a.server)).toBe(1);
    await a.clock.advance(1_500); // the TTL is 30 s: at 15 s
    await b.engine.settle();
    expect(publishes(a.server)).toBe(2);
    expect(publishedStates(a.server)).toEqual([{ viewing: true }, { viewing: true }]);
    await a.clock.advance(60_000);
    await b.engine.settle();
    expect(a.server.presence.entries(KS, ROOM)).toHaveLength(1);
    expect(b.engine.presenceEntries(KS, ROOM).map((e) => e.clientId)).toEqual([a.engine.clientId]);
  });

  it("a view drops an entry whose expires_at has passed even when no left_client_ids came (09 [READ-81])", async () => {
    const a = await prig({ seed: 11 });
    const seen: PresenceEvent[] = [];
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    a.engine.onPresence((e) => seen.push(e));
    a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: "c-1", state: { viewing: true }, expiresMs: a.clock.now() + 5_000 }] }));
    await a.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM)).toHaveLength(1);
    await a.clock.advance(6_000);
    await a.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM)).toHaveLength(0);
    expect(seen.filter((e) => e.type === "changed")).toHaveLength(2); // the upsert, and the expiry
  });

  it("an entry that comes expired, or with no expires_at, is not kept", async () => {
    const a = await prig({ seed: 11 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: "c-1", state: { x: 1 }, expiresMs: a.clock.now() - 1 }] }));
    await a.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM)).toHaveLength(0);
  });
});

describe("09 [READ-79]: the publish rate", () => {
  it("states set within the interval coalesce into the latest, sent once the interval has passed since the answer of the last publish", async () => {
    const a = await prig({ seed: 11 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.publishPresence(KS, ROOM, { n: 1 });
    await a.engine.settle();
    expect(a.engine.limits.presenceMinPublishIntervalMs).toBe(1000);
    await a.engine.publishPresence(KS, ROOM, { n: 2 });
    await a.engine.publishPresence(KS, ROOM, { n: 3 });
    await a.engine.settle();
    expect(publishedStates(a.server)).toEqual([{ n: 1 }]);
    await a.clock.advance(1000);
    expect(publishedStates(a.server)).toEqual([{ n: 1 }, { n: 3 }]);
    expect(a.server.presence.entries(KS, ROOM).map((e) => stateOf(e.state))).toEqual([{ n: 3 }]); // and the server refused none
  });

  it("a server that refuses a publish over the rate with Retry{RATE_LIMITED} is obeyed: the latest state is sent after the larger of after_ms and the backoff (14 [CLI-43])", async () => {
    const a = await prig({ seed: 11 });
    a.random.fixed = 0; // the backoff draws 0: the wait is after_ms
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    a.server.inject("publishPresence", { kind: "status", status: "ResourceExhausted", retry: create(RetrySchema, { reason: RetryReason.RATE_LIMITED, afterMs: 3000 }) });
    await a.engine.publishPresence(KS, ROOM, { n: 1 });
    await a.engine.settle();
    expect(publishes(a.server)).toBe(1);
    await a.engine.publishPresence(KS, ROOM, { n: 2 }); // set while it waits: coalesced
    await a.clock.advance(2900);
    expect(publishes(a.server)).toBe(1);
    await a.clock.advance(200);
    expect(publishes(a.server)).toBe(2);
    expect(publishedStates(a.server)).toEqual([{ n: 1 }, { n: 2 }]);
    expect(a.server.presence.entries(KS, ROOM)).toHaveLength(1);
  });

  it("04 Table 4.10a, 07 [MUT-75]: a 0 presence_min_publish_interval_ms is the default (1,000 ms) and no protocol error; a 0 TTL is one, and the floor of 10 s applies; a value above the maximum is held to it", async () => {
    const a = await prig({ seed: 11, serverOptions: { limits: { presenceMinPublishIntervalMs: 0, presenceTtlSeconds: 0 } } });
    expect(a.engine.limits.presenceMinPublishIntervalMs).toBe(1000);
    expect(a.engine.limits.presenceTtlMs).toBe(10_000);
    const errs = protocolErrors(a).map((e) => (e as { what: string }).what);
    expect(errs).toContain("AppLimits.presence_ttl_seconds");
    expect(errs).not.toContain("AppLimits.presence_min_publish_interval_ms");
    const c = await prig({ seed: 11, serverOptions: { limits: { presenceMinPublishIntervalMs: 9000, maxPresenceStateBytes: 1 << 20 } } });
    expect(c.engine.limits.presenceMinPublishIntervalMs).toBe(5000);
    expect(c.engine.limits.maxPresenceStateBytes).toBe(64 << 10);
    expect(protocolErrors(c).map((e) => (e as { what: string }).what)).toEqual(expect.arrayContaining(["AppLimits.presence_min_publish_interval_ms", "AppLimits.max_presence_state_bytes"]));
  });
});

describe("09 [READ-71], 16 [SEC-30]: presence is never stored", () => {
  it("the engine keeps the entries and its own state in memory only: nothing of them reaches the storage of the outbox, the replica or the meta", async () => {
    const a = await prig({ seed: 11 });
    const b = await second(a);
    await a.engine.joinPresence(KS, ROOM);
    await b.engine.joinPresence(KS, ROOM);
    await a.engine.publishPresence(KS, ROOM, { cursor: "zzz-a-unique-marker-zzz" });
    await b.engine.publishPresence(KS, ROOM, { cursor: "yyy-another-marker-yyy" });
    await a.engine.settle();
    await b.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM)).toHaveLength(1);
    expect(b.engine.presenceEntries(KS, ROOM)).toHaveLength(1);
    // the same server write that a restart would have to replay: write something to the keyspace so that the storage is used
    a.server.write(KS, [kc("settings/x", "set", 1)]);
    await a.engine.settle();
    for (const r of [a, b]) {
      const st = await r.storage.load();
      const dump = JSON.stringify(st, (_k, v: unknown) => (typeof v === "bigint" ? v.toString() : v instanceof Uint8Array ? Buffer.from(v).toString("latin1") : v));
      expect(dump).not.toContain("a-unique-marker");
      expect(dump).not.toContain("another-marker");
    }
  });
});

describe("09 [READ-77], 14 [CLI-78]: leaving, a drop, and a rebind of the client instance", () => {
  it("a keyspace that the app stops following is removed from the session: the topic is left with it, and the view and the registrations go (09 [READ-63])", async () => {
    const a = await prig({ seed: 11 });
    const b = await second(a);
    await b.engine.joinPresence(KS, ROOM);
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.publishPresence(KS, ROOM, { viewing: true });
    await a.engine.settle();
    expect(b.engine.presenceEntries(KS, ROOM)).toHaveLength(1);
    expect(await a.engine.removeKeyspace(KS)).toBe(true);
    await a.engine.settle();
    expect(a.engine.presenceTopics()).toEqual([]);
    expect(requests<{ remove: string[] }>(a.server, "updateSubscription").some((r) => r.remove.includes(KS))).toBe(true);
    expect(a.server.presence.entries(KS, ROOM)).toHaveLength(0); // the server ended the entry with the removal
    expect(b.engine.presenceEntries(KS, ROOM)).toHaveLength(0);
    await a.clock.advance(120_000);
    expect(publishes(a.server)).toBe(1);
  });

  it("LeavePresence ends the entry and the membership; the other members get left_client_ids", async () => {
    const a = await prig({ seed: 11 });
    const b = await second(a);
    await b.engine.joinPresence(KS, ROOM);
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.publishPresence(KS, ROOM, { viewing: true });
    await a.engine.settle();
    expect(b.engine.presenceEntries(KS, ROOM)).toHaveLength(1);
    await a.engine.leavePresence(KS, ROOM);
    await a.engine.settle();
    expect(leaves(a.server)).toBe(1);
    expect(a.server.presence.members(KS, ROOM)).toHaveLength(1);
    expect(a.server.presence.entries(KS, ROOM)).toHaveLength(0);
    expect(b.engine.presenceEntries(KS, ROOM)).toHaveLength(0);
    expect(a.engine.presenceTopics()).toEqual([]);
    await a.clock.advance(60_000);
    expect(publishes(a.server)).toBe(1); // no keepalive after the leave
  });

  it("a keyspace dropped on the session ends the topic there; it joins again when the keyspace is added again", async () => {
    const a = await prig({ seed: 11 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.publishPresence(KS, ROOM, { viewing: true });
    await a.engine.settle();
    a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: "c-1", state: { x: 1 }, expiresMs: a.clock.now() + 20_000 }] }));
    await a.engine.settle();
    expect(joins(a.server)).toBe(1);
    expect(a.engine.presenceEntries(KS, ROOM)).toHaveLength(1);
    a.server.drop(KS, 4 /* RESUME_TOO_OLD: the engine loads the keyspace again and adds it again */);
    await a.engine.settle();
    await a.clock.advance(1000);
    await a.engine.settle();
    expect(a.engine.presenceTopics()).toMatchObject([{ keyspaceId: KS, topic: ROOM, joined: true }]);
    expect(joins(a.server)).toBe(2);
    expect(publishes(a.server)).toBe(2); // the entry went with the drop (09 [READ-69]) and is published again
  });

  it("a keyspace that the engine stops following is not joined, and a view of it is gone (09 [READ-69])", async () => {
    const a = await prig({ seed: 11 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: "c-1", state: { x: 1 }, expiresMs: a.clock.now() + 20_000 }] }));
    await a.engine.settle();
    a.server.drop(KS, 1 /* GRANT_CHANGED */);
    await a.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM)).toHaveLength(0);
    expect(a.engine.presenceTopics()).toMatchObject([{ joined: false }]);
    await a.clock.advance(60_000);
    expect(joins(a.server)).toBe(1);
  });

  it("14 [CLI-78]: once the rebind of the session to a new client_id succeeds, the engine publishes its entry again at once, and the earlier instance's entry is gone", async () => {
    const a = await prig({ seed: 11 });
    const b = await second(a);
    await b.engine.joinPresence(KS, ROOM);
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.publishPresence(KS, ROOM, { viewing: true });
    await a.engine.settle();
    const before = a.engine.clientId;
    expect(b.engine.presenceEntries(KS, ROOM).map((e) => e.clientId)).toEqual([before]);
    (a.engine as unknown as { reidentify(why: string): void }).reidentify("test");
    await a.engine.settle();
    await b.engine.settle();
    const after = a.engine.clientId;
    expect(after).not.toBe(before);
    expect(requests<{ clientId: string }>(a.server, "updateSubscription").some((r) => r.clientId === after)).toBe(true);
    expect(publishes(a.server)).toBe(2); // at once: no interval is waited for, the new instance has published nothing
    expect(joins(a.server)).toBe(2); // the membership stays: no join
    expect(a.server.presence.entries(KS, ROOM).map((e) => e.clientId)).toEqual([after]);
    expect(b.engine.presenceEntries(KS, ROOM).map((e) => e.clientId)).toEqual([after]);
  });
});

describe("09 [READ-81], [READ-80]: frames are applied to the view of their topic, outside the log", () => {
  it("upserts replace whole entries, left_client_ids remove them, an upsert with an older expires_at is ignored, and no cursor moves", async () => {
    const a = await prig({ seed: 11 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    const cursor = a.engine.cursor(KS);
    const status = a.engine.status().cursors;
    const now = a.clock.now();
    a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: "c-1", state: { n: 1 }, expiresMs: now + 20_000 }, { clientId: "c-2", state: { n: 2 }, expiresMs: now + 20_000 }] }));
    await a.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM).map((e) => stateOf(e.state))).toEqual([{ n: 1 }, { n: 2 }]);
    a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: "c-1", state: { n: 9 }, expiresMs: now + 10_000 }] })); // older: ignored
    await a.engine.settle();
    expect(stateOf(a.engine.presenceEntries(KS, ROOM)[0]?.state as Uint8Array)).toEqual({ n: 1 });
    a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: "c-1", state: { n: 7 }, expiresMs: now + 25_000 }], left: ["c-2"] }));
    await a.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM).map((e) => [e.clientId, stateOf(e.state)])).toEqual([["c-1", { n: 7 }]]);
    expect(a.engine.cursor(KS)).toEqual(cursor);
    expect(a.engine.status().cursors).toEqual(status);
  });

  it("16 [SEC-1]: the view of a topic holds a bounded number of entries, whatever a server sends", async () => {
    const a = await prig({ seed: 11 });
    (a.engine as unknown as { maxViewEntries: number }).maxViewEntries = 3;
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    const now = a.clock.now();
    const many = (n: number): { clientId: string; state: unknown; expiresMs: number }[] => Array.from({ length: n }, (_, i) => ({ clientId: `c-${i}`, state: { n: i }, expiresMs: now + 20_000 }));
    a.server.sendFrame(frame({ topic: ROOM, snapshot: true, entries: many(10) }));
    await a.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM)).toHaveLength(3);
    a.server.sendFrame(frame({ topic: ROOM, entries: many(10).slice(5) })); // new client ids: over the bound
    await a.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM).map((e) => e.clientId)).toEqual(["c-0", "c-1", "c-2"]);
    a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: "c-1", state: { n: 99 }, expiresMs: now + 25_000 }] })); // one that it holds: replaced
    await a.engine.settle();
    expect(stateOf(a.engine.presenceEntries(KS, ROOM)[1]?.state as Uint8Array)).toEqual({ n: 99 });
  });

  it("a frame for a topic that the engine is not in changes nothing (09 [READ-80])", async () => {
    const a = await prig({ seed: 11 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    const seen: PresenceEvent[] = [];
    a.engine.onPresence((e) => seen.push(e));
    a.server.sendFrame(frame({ topic: "room/other", entries: [{ clientId: "c-1", state: { n: 1 }, expiresMs: a.clock.now() + 20_000 }] }));
    a.server.sendFrame(frame({ keyspaceId: "ks-other", topic: ROOM, entries: [{ clientId: "c-1", state: { n: 1 }, expiresMs: a.clock.now() + 20_000 }] }));
    await a.engine.settle();
    expect(seen).toEqual([]);
    expect(a.engine.presenceEntries(KS, ROOM)).toEqual([]);
    expect(a.engine.presenceEntries(KS, "room/other")).toEqual([]);
  });
});

describe("09 [READ-83]: Yjs awareness", () => {
  it("the state of the entry maps the id of each tab of the engine in the topic to that tab's state", async () => {
    const r = tabsRig({ manifests: new StaticManifests(PRESENCE_MANIFEST) });
    const t1 = await r.open();
    const t2 = await r.open();
    await settleAll(t1.engine, t2.engine);
    await t1.engine.joinPresence(KS, DOC);
    await t2.engine.joinPresence(KS, DOC);
    expect(t1.engine.presenceTopics()).toMatchObject([{ topic: DOC, awareness: true }]); // a key of a `crdt` template: the manifest says so
    await t1.engine.publishPresence(KS, DOC, { cursor: "a" });
    await t2.engine.publishPresence(KS, DOC, { cursor: "b" });
    await settleAll(t1.engine, t2.engine);
    await r.clock.advance(1100);
    await settleAll(t1.engine, t2.engine);
    const [entry] = r.server.presence.entries(KS, DOC);
    expect(Object.keys(plain(stateOf((entry as { state: Uint8Array }).state)) as object).sort()).toEqual([t1.tab.tabId, t2.tab.tabId].sort());
    expect(stateOf((entry as { state: Uint8Array }).state)).toEqual({ [t1.tab.tabId]: { cursor: "a" }, [t2.tab.tabId]: { cursor: "b" } });
    // one tab leaves: the entry is the map of the tabs that remain
    await t1.engine.leavePresence(KS, DOC);
    await settleAll(t1.engine, t2.engine);
    await r.clock.advance(1100);
    await settleAll(t1.engine, t2.engine);
    expect(stateOf((r.server.presence.entries(KS, DOC)[0] as { state: Uint8Array }).state)).toEqual({ [t2.tab.tabId]: { cursor: "b" } });
    expect(leaves(r.server)).toBe(0);
  });

  it("09 [READ-72], 04 [MAN-15]: a presence topic template that equals the crdt template names an awareness topic: the entry is still a map of tab id to state", async () => {
    // Q73.1, draft 16: the topic is both a topic template and a key of a `crdt:*` template, and the key of the `crdt:*` template decides
    const r = tabsRig({ manifests: new StaticManifests({ ...PRESENCE_MANIFEST, presence: { kind: ["room/{id}", "docs/{id}"] } }) });
    const t1 = await r.open();
    await settleAll(t1.engine);
    await t1.engine.joinPresence(KS, DOC);
    await t1.engine.joinPresence(KS, ROOM);
    expect(t1.engine.presenceTopics().map((t) => [t.topic, t.awareness]).sort()).toEqual([[DOC, true], [ROOM, false]]);
    await t1.engine.publishPresence(KS, DOC, { cursor: "a" });
    await settleAll(t1.engine);
    await r.clock.advance(1100);
    await settleAll(t1.engine);
    expect(stateOf((r.server.presence.entries(KS, DOC)[0] as { state: Uint8Array }).state)).toEqual({ [t1.tab.tabId]: { cursor: "a" } });
  });
});

describe("14 [CLI-66]: the presence state is on the purge list of a revoked keyspace", () => {
  it("ACCESS_REVOKED removes the view and the registrations of the keyspace, and the engine says that its presence ended", async () => {
    const a = await prig({ seed: 11 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: "c-1", state: { n: 1 }, expiresMs: a.clock.now() + 20_000 }] }));
    await a.engine.settle();
    const seen: PresenceEvent[] = [];
    a.engine.onPresence((e) => seen.push(e));
    a.server.revokeKeyspaces(KS);
    await a.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM)).toEqual([]);
    expect(a.engine.presenceTopics()).toEqual([]);
    expect(seen.map((e) => e.type)).toEqual(expect.arrayContaining(["changed", "ended"]));
    await a.clock.advance(120_000);
    expect(publishes(a.server)).toBe(0);
  });
});

describe("A.7, 09 [READ-73], [READ-74], [READ-79]: what the calls answer", () => {
  it("QUOTA_EXCEEDED on a join (the member cap) is reported and not resent in a loop", async () => {
    const a = await prig({ seed: 11, serverOptions: { limits: { maxPresenceMembersPerTopic: 1 } } });
    const b = await second(a);
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    const seen: PresenceEvent[] = [];
    b.engine.onPresence((e) => seen.push(e));
    await b.engine.joinPresence(KS, ROOM);
    await b.engine.settle();
    expect(seen).toMatchObject([{ type: "refused", operation: "join", code: "QUOTA_EXCEEDED", detail: "max_presence_members_per_topic" }]);
    await b.clock.advance(300_000);
    expect(requests(a.server, "joinPresence").filter((_, i) => i > 0)).toHaveLength(1);
  });

  it("a topic that no template matches is INVALID_KEY: reported, not resent", async () => {
    const a = await prig({ seed: 11 });
    const seen: PresenceEvent[] = [];
    a.engine.onPresence((e) => seen.push(e));
    await a.engine.joinPresence(KS, "other/x");
    await a.engine.settle();
    await a.clock.advance(300_000);
    expect(seen).toMatchObject([{ type: "refused", operation: "join", code: "INVALID_KEY" }]);
    expect(joins(a.server)).toBe(1);
  });

  it("no presence grant: the publish is PERMISSION_DENIED and reported; the engine still observes the topic", async () => {
    const a = await prig({ seed: 11 });
    a.server.denyPresence("ana", KS);
    const seen: PresenceEvent[] = [];
    a.engine.onPresence((e) => seen.push(e));
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.publishPresence(KS, ROOM, { n: 1 });
    await a.engine.settle();
    a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: "c-1", state: { n: 1 }, expiresMs: a.clock.now() + 20_000 }] }));
    await a.engine.settle();
    expect(seen.filter((e) => e.type === "refused")).toMatchObject([{ operation: "publish", code: "PERMISSION_DENIED", status: "PermissionDenied" }]);
    expect(a.engine.presenceEntries(KS, ROOM)).toHaveLength(1);
    await a.clock.advance(120_000);
    expect(publishes(a.server)).toBe(1);
  });

  it("FailedPrecondition with no detail (not a member): the engine joins again and publishes again", async () => {
    const a = await prig({ seed: 11 });
    a.random.fixed = 0;
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    a.server.inject("publishPresence", { kind: "status", status: "FailedPrecondition" });
    await a.engine.publishPresence(KS, ROOM, { n: 1 });
    await a.engine.settle();
    await a.clock.advance(1100);
    await a.engine.settle();
    expect(joins(a.server)).toBe(2);
    expect(publishedStates(a.server)).toEqual([{ n: 1 }, { n: 1 }]);
    expect(a.server.presence.entries(KS, ROOM)).toHaveLength(1);
  });

  it("an outcome that is unknown (Unavailable) is resent with the client backoff", async () => {
    const a = await prig({ seed: 11 });
    a.random.fixed = 0.5;
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    a.server.inject("publishPresence", { kind: "status", status: "Unavailable" }, { times: 2 });
    await a.engine.publishPresence(KS, ROOM, { n: 1 });
    await a.engine.settle();
    expect(publishes(a.server)).toBe(1);
    await a.clock.advance(500); // 0.5 × 1 s
    expect(publishes(a.server)).toBe(2);
    await a.clock.advance(1000); // 0.5 × 2 s
    expect(publishes(a.server)).toBe(3);
    expect(a.server.presence.entries(KS, ROOM)).toHaveLength(1);
  });

  it("NotFound with no detail (the session is not known): the engine subscribes again and joins its topics again", async () => {
    const a = await prig({ seed: 11 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    a.server.inject("publishPresence", { kind: "status", status: "NotFound" });
    await a.engine.publishPresence(KS, ROOM, { n: 1 });
    await a.engine.settle();
    await a.clock.advance(3000);
    await a.engine.settle();
    expect(requests(a.server, "subscribe")).toHaveLength(2);
    expect(joins(a.server)).toBe(2);
    expect(a.server.presence.entries(KS, ROOM)).toHaveLength(1);
  });

  it("Unauthenticated: the token is refreshed and the call resent once; a second one stops and reports", async () => {
    let refreshes = 0;
    const a = await prig({ seed: 11, cfg: { tokens: { refresh: () => Promise.resolve(++refreshes > 0) } as never } });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    const seen: PresenceEvent[] = [];
    a.engine.onPresence((e) => seen.push(e));
    a.server.inject("publishPresence", { kind: "status", status: "Unauthenticated" }, { times: 2 });
    await a.engine.publishPresence(KS, ROOM, { n: 1 });
    await a.engine.settle();
    expect(publishes(a.server)).toBe(2);
    expect(refreshes).toBe(1);
    expect(seen.filter((e) => e.type === "refused")).toMatchObject([{ operation: "publish", code: "UNAUTHENTICATED" }]);
  });
});

describe("02 [ARC-61], 16 [SEC-3]: what the server sends as presence is decoded strictly", () => {
  it("a presence frame whose topic is not valid UTF-8 is a protocol violation", async () => {
    const a = await prig({ seed: 11 });
    const raw = new FakeRaw(a.server.transport(a.server.principal("zed")));
    raw.tamper = (bytes, f) => (f.frame.case === "presence" ? breakUtf8(bytes, ROOM) : bytes);
    const it = new BytesTransport(raw).subscribe(create(SubscribeRequestSchema, {}))[Symbol.asyncIterator]();
    expect((await it.next()).done).toBe(false); // Subscribed
    a.server.sendFrame(frame({ topic: ROOM, entries: [] }));
    await expect(it.next()).rejects.toBeInstanceOf(ProtocolViolation);
  });

  it("an answer that breaks 02 [ARC-61] in a presence call is reported as protocol-error, and the keyspace is held (16 [SEC-3], 14 [CLI-50])", async () => {
    const clock = new ManualClock();
    const manifests = presenceManifests();
    const server = new FakeServer({ manifests, clock, heartbeatMs: 15_000 });
    server.createKeyspaceNow(KS);
    const raw = new FakeRaw(server.transport(server.principal("ana")));
    const bad = toBinary(PublishPresenceResponseSchema, create(PublishPresenceResponseSchema, { expiresAt: create(TimestampSchema, { seconds: 253402300800n, nanos: 0 }) }));
    raw.tamperUnary = (method, bytes) => (method === "PublishPresence" ? bad : bytes);
    const a = await rig({ manifests, server, clock, cfg: { transport: new BytesTransport(raw) } });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.publishPresence(KS, ROOM, { n: 1 });
    await a.engine.settle();
    expect(protocolErrors(a)).toMatchObject([{ what: "PublishPresenceResponse", keyspaces: [KS] }]);
    expect(a.engine.status().holds.some((h) => h.code === "PROTOCOL_ERROR")).toBe(true);
    expect(publishes(server)).toBe(1); // not resent in a loop
  });

  it("a Timestamp out of range in the answer of a publish is a protocol violation", async () => {
    const bad = toBinary(PublishPresenceResponseSchema, create(PublishPresenceResponseSchema, { expiresAt: create(TimestampSchema, { seconds: 253402300800n, nanos: 0 }) }));
    const t = new BytesTransport({ unary: () => Promise.resolve(bad), stream: () => (async function* (): AsyncGenerator<Uint8Array> {})() });
    await expect(t.publishPresence(create(PublishPresenceRequestSchema, { sessionId: "s", keyspaceId: KS, topic: ROOM }))).rejects.toBeInstanceOf(ProtocolViolation);
  });
});
