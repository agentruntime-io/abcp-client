// V21 (C6), the presence findings: 09 §9.8 [READ-74], [READ-75], [READ-76], [READ-79], [READ-81], [READ-83], 14 [CLI-61], 16 [SEC-1]. Each test is the scenario of a verifier's probe (V21-report.md,
// PP1 to PP6) turned into a rule: what the view keeps of what a server sends is bounded, a follower tab that closes keeps no presence, the entry of an engine holds the latest state of the tabs that are
// still in the topic, and a follower tells the user's own entry from another member's.

import { describe, expect, it } from "vitest";
import { StaticManifests, type PresenceEvent } from "../src/engine/index.js";
import { PresenceView } from "../src/query/presence.js";
import { rig } from "./helpers/engine.js";
import { DOC, frame, KS, plain, PRESENCE_MANIFEST, prig, protocolErrors, publishedStates, ROOM, stateOf } from "./helpers/presence.js";
import { settleAll, tabsRig } from "./helpers/tabs.js";

const leaves = (s: { calls: { method: string }[] }): number => s.calls.filter((c) => c.method === "leavePresence").length;

async function tick(r: { clock: { advance(ms: number): Promise<void> } }, engines: { settle(): Promise<void> }[], ms: number, step = 1000): Promise<void> {
  for (let t = 0; t < ms; t += step) {
    await r.clock.advance(step);
    for (const e of engines) await e.settle();
  }
}

const entryState = (r: { server: { presence: { entries(ks: string, topic: string): unknown[] } } }, topic: string): Record<string, unknown> =>
  plain(stateOf((r.server.presence.entries(KS, topic)[0] as { state: Uint8Array }).state)) as Record<string, unknown>;

// ---------------------------------------------------------------------------------------------------------------------
describe("V21 MUST-3, 16 [SEC-1], 09 [READ-74], [READ-79], [READ-76]: what the view keeps of the entries that a server sends is bounded", () => {
  it("an entry over max_presence_state_bytes is dropped and reported once as a protocol error, however many come (the probe PP5: 40 upserts of 2 MiB)", async () => {
    const a = await prig({ seed: 11 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    const big = `"${"a".repeat(2 << 20)}"`;
    for (let i = 0; i < 40; i++) {
      a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: `c-${i}`, state: big, expiresMs: a.clock.now() + 20_000 }] }));
      await a.engine.settle();
    }
    expect(a.engine.presenceEntries(KS, ROOM)).toEqual([]);
    expect(protocolErrors(a)).toMatchObject([{ type: "protocol-error", what: "PresenceFrame", keyspaces: [KS] }]); // once, not forty times
    // the state limit is the entry's own: one at the limit is kept, one octet over is not (a snapshot too)
    const limit = a.engine.limits.maxPresenceStateBytes;
    const text = (n: number): string => `"${"a".repeat(n - 2)}"`;
    a.server.sendFrame(frame({ topic: ROOM, snapshot: true, entries: [{ clientId: "fits", state: text(limit), expiresMs: a.clock.now() + 20_000 }, { clientId: "over", state: text(limit + 1), expiresMs: a.clock.now() + 20_000 }] }));
    await a.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM).map((e) => e.clientId)).toEqual(["fits"]);
    // and it holds nothing of what was dropped: no state of 2 MiB is in memory
    expect(a.engine.presenceEntries(KS, ROOM).reduce((n, e) => n + e.state.length, 0)).toBeLessThanOrEqual(limit);
  });

  it("the view holds at most max_presence_members_per_topic entries (its floor is 100), and the bytes that it holds are bounded too", async () => {
    const a = await prig({ seed: 11 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    const many = (n: number): { clientId: string; state: unknown; expiresMs: number }[] => Array.from({ length: n }, (_, i) => ({ clientId: `c-${String(i).padStart(4, "0")}`, state: { n: i }, expiresMs: a.clock.now() + 20_000 }));
    a.server.sendFrame(frame({ topic: ROOM, snapshot: true, entries: many(150) }));
    await a.engine.settle();
    expect(a.engine.limits.maxPresenceMembersPerTopic).toBe(100);
    expect(a.engine.presenceEntries(KS, ROOM)).toHaveLength(100);
    a.server.sendFrame(frame({ topic: ROOM, entries: many(150).slice(120) })); // upserts of clients that it does not hold: over the cap
    await a.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM)).toHaveLength(100);
    // a server that allows 60 KiB states and 1,000 members: 1,000 entries of 60 KiB are 60 MB, and the view holds a part of them (a byte total of 8 MiB)
    const b = await prig({ seed: 12, serverOptions: { limits: { maxPresenceStateBytes: 64 << 10, maxPresenceMembersPerTopic: 1000 } } });
    await b.engine.joinPresence(KS, ROOM);
    await b.engine.settle();
    expect(b.engine.limits).toMatchObject({ maxPresenceStateBytes: 64 << 10, maxPresenceMembersPerTopic: 1000 });
    const state = `"${"b".repeat(60 << 10)}"`;
    const entries = Array.from({ length: 300 }, (_, i) => ({ clientId: `c-${String(i).padStart(4, "0")}`, state, expiresMs: b.clock.now() + 20_000 }));
    b.server.sendFrame(frame({ topic: ROOM, snapshot: true, entries }));
    await b.engine.settle();
    const held = b.engine.presenceEntries(KS, ROOM);
    expect(held.length).toBeGreaterThan(100);
    expect(held.reduce((n, e) => n + e.state.length, 0)).toBeLessThanOrEqual(8 << 20);
    expect(held.length).toBeLessThan(300);
  });

  it("an entry that expires far from now (a year) is kept for the TTL, not for a year (the probe PP6)", async () => {
    const a = await prig({ seed: 11 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: "ghost", state: { x: 1 }, expiresMs: a.clock.now() + 365 * 86_400_000 }] }));
    await a.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM).map((e) => e.clientId)).toEqual(["ghost"]);
    const ttl = a.engine.limits.presenceTtlMs;
    expect(a.engine.presenceEntries(KS, ROOM)[0]?.expiresAtMs).toBeLessThanOrEqual(a.engine.serverNow() + ttl + 5_000);
    await a.clock.advance(ttl + 6_000);
    await a.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM)).toEqual([]);
    // an honest entry (the TTL from its publish) is not shortened
    a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: "honest", state: { x: 2 }, expiresMs: a.engine.serverNow() + ttl }] }));
    await a.engine.settle();
    await a.clock.advance(ttl - 1000);
    expect(a.engine.presenceEntries(KS, ROOM).map((e) => e.clientId)).toEqual(["honest"]);
  });

  it("a client id that is not a client id is not kept (09 [READ-75]: one is a UUID)", async () => {
    const a = await prig({ seed: 11 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: "x".repeat(1000), state: { x: 1 }, expiresMs: a.clock.now() + 20_000 }, { clientId: "ok", state: { x: 1 }, expiresMs: a.clock.now() + 20_000 }] }));
    await a.engine.settle();
    expect(a.engine.presenceEntries(KS, ROOM).map((e) => e.clientId)).toEqual(["ok"]);
  });
});

// ---------------------------------------------------------------------------------------------------------------------
describe("V21 MUST-4 (Q75.2), 09 [READ-74], [READ-83], 14 [CLI-61]: a follower tab that closes keeps no presence", () => {
  async function two(manifests = new StaticManifests(PRESENCE_MANIFEST)) {
    const r = tabsRig({ manifests });
    const t1 = await r.open();
    const t2 = await r.open();
    await settleAll(t1.engine, t2.engine);
    expect(t2.engine.status().role).toBe("follower");
    return { r, t1, t2 };
  }

  it("awareness: a follower tab crashes: its state leaves the entry at once, and is not republished (the probe PP2: ten minutes later it was still there)", async () => {
    const { r, t1, t2 } = await two();
    await t1.engine.joinPresence(KS, DOC);
    await t2.engine.joinPresence(KS, DOC);
    await t1.engine.publishPresence(KS, DOC, { cursor: "a" });
    await t2.engine.publishPresence(KS, DOC, { cursor: "b" });
    await settleAll(t1.engine, t2.engine);
    await r.clock.advance(1100);
    await settleAll(t1.engine, t2.engine);
    expect(Object.keys(entryState(r, DOC)).sort()).toEqual([t1.tab.tabId, t2.tab.tabId].sort());
    t2.tab.crash();
    await tick(r, [t1.engine], 5000);
    expect(t1.engine.presenceTopics()[0]?.tabs).toEqual([t1.tab.tabId]);
    await tick(r, [t1.engine], 120_000, 5000);
    expect(Object.keys(entryState(r, DOC))).toEqual([t1.tab.tabId]);
  });

  it("a plain topic that only the follower is in: the crash makes the engine leave the topic with LeavePresence, and the other members see the entry go (the probe PP3)", async () => {
    const r = tabsRig({ manifests: new StaticManifests(PRESENCE_MANIFEST) });
    const t1 = await r.open();
    const t2 = await r.open();
    const bob = await rig({ manifests: r.manifests, server: r.server, clock: r.clock, sub: "bob", seed: 31 });
    await settleAll(t1.engine, t2.engine, bob.engine);
    await bob.engine.joinPresence(KS, ROOM);
    await t2.engine.joinPresence(KS, ROOM);
    await t2.engine.publishPresence(KS, ROOM, { viewing: "issue 42" });
    await settleAll(t1.engine, t2.engine, bob.engine);
    expect(bob.engine.presenceEntries(KS, ROOM).map((e) => stateOf(e.state))).toEqual([{ viewing: "issue 42" }]);
    expect(leaves(r.server)).toBe(0);
    t2.tab.crash();
    await tick(r, [t1.engine, bob.engine], 3000);
    expect(leaves(r.server)).toBe(1);
    expect(t1.engine.inPresence(KS, ROOM)).toBe(false);
    expect(bob.engine.presenceEntries(KS, ROOM)).toEqual([]);
    await tick(r, [t1.engine, bob.engine], 300_000, 5000);
    expect(r.server.presence.entries(KS, ROOM)).toEqual([]); // nothing keeps it alive
  });

  it("a topic that another tab is still in is not left when a follower crashes (09 [READ-74]: the engine does not leave while a tab is in it)", async () => {
    const { r, t1, t2 } = await two();
    await t1.engine.joinPresence(KS, ROOM);
    await t2.engine.joinPresence(KS, ROOM);
    await t1.engine.publishPresence(KS, ROOM, { who: "t1" });
    await settleAll(t1.engine, t2.engine);
    t2.tab.crash();
    await tick(r, [t1.engine], 3000);
    expect(leaves(r.server)).toBe(0);
    expect(t1.engine.inPresence(KS, ROOM)).toBe(true);
    expect(t1.engine.presenceTopics()[0]?.tabs).toEqual([t1.tab.tabId]);
  });

  it("a follower tab that is told to go (pagehide, the hint that comes before the lock is freed) ends its registrations at once; a page that comes back registers again", async () => {
    const { r, t1, t2 } = await two();
    await t2.engine.joinPresence(KS, ROOM);
    await t2.engine.publishPresence(KS, ROOM, { viewing: 1 });
    await settleAll(t1.engine, t2.engine);
    expect(t1.engine.inPresence(KS, ROOM)).toBe(true);
    t2.tab.pageEvent("hide");
    await settleAll(t1.engine, t2.engine);
    expect(t1.engine.inPresence(KS, ROOM)).toBe(false);
    expect(leaves(r.server)).toBe(1);
    t2.tab.pageEvent("show"); // restored from the back-forward cache: the tab is in the topic again
    await tick(r, [t1.engine, t2.engine], 3000);
    expect(t1.engine.inPresence(KS, ROOM)).toBe(true);
    expect(entryState(r, ROOM)).toEqual({ viewing: 1 });
  });

  it("a follower tab that holds no lock (the platform refused it) is known by its hint only: a crash is not seen, the hint is", async () => {
    const r = tabsRig({ manifests: new StaticManifests(PRESENCE_MANIFEST) });
    const t1 = await r.open();
    const tab2 = r.device.newTab();
    const real = tab2.locks.tryAcquire;
    (tab2.locks as { tryAcquire: typeof real }).tryAcquire = (name: string) => (name.startsWith("abcp.tab") ? Promise.resolve(undefined) : real(name));
    const t2 = await r.open({ tab: tab2 });
    await settleAll(t1.engine, t2.engine);
    await t2.engine.joinPresence(KS, ROOM);
    await settleAll(t1.engine, t2.engine);
    tab2.pageEvent("hide");
    await settleAll(t1.engine, t2.engine);
    expect(t1.engine.inPresence(KS, ROOM)).toBe(false);
  });

  it("a tab that stops by stop() is gone as before: it leaves its topics, the flusher is not told twice", async () => {
    const { r, t1, t2 } = await two();
    await t2.engine.joinPresence(KS, ROOM);
    await settleAll(t1.engine, t2.engine);
    await t2.engine.stop();
    await tick(r, [t1.engine], 3000);
    expect(t1.engine.inPresence(KS, ROOM)).toBe(false);
    expect(leaves(r.server)).toBe(1);
  });

  it("where there is no reliable lock each tab is an engine of its own: there is no follower to lose, and a crashed tab's entry ends with its own session", async () => {
    const r = tabsRig({ manifests: new StaticManifests(PRESENCE_MANIFEST) });
    const t1 = await r.open({ tab: { reliableLocks: false } });
    const t2 = await r.open({ tab: { reliableLocks: false } });
    await settleAll(t1.engine, t2.engine);
    expect(t1.engine.status().role).toBe("flusher");
    expect(t2.engine.status().role).toBe("flusher");
    await t2.engine.joinPresence(KS, ROOM);
    await t2.engine.publishPresence(KS, ROOM, { here: true });
    await settleAll(t1.engine, t2.engine);
    expect(t1.engine.inPresence(KS, ROOM)).toBe(false); // the tabs share nothing
  });
});

// ---------------------------------------------------------------------------------------------------------------------
describe("V21 SHOULD-1 (Q75.1), 09 [READ-74]: the entry's state is the latest state that one of the tabs still in the topic set", () => {
  async function setup() {
    const r = tabsRig({ manifests: new StaticManifests(PRESENCE_MANIFEST) });
    const t1 = await r.open();
    const t2 = await r.open();
    await settleAll(t1.engine, t2.engine);
    return { r, t1, t2 };
  }

  it("t1 set {typing: false}, t2 set {typing: true} and leaves: the entry becomes {typing: false} and the keepalive keeps publishing it (the probe PP1)", async () => {
    const { r, t1, t2 } = await setup();
    await t1.engine.joinPresence(KS, ROOM);
    await t1.engine.publishPresence(KS, ROOM, { typing: false });
    await settleAll(t1.engine, t2.engine);
    await r.clock.advance(1100);
    await t2.engine.joinPresence(KS, ROOM);
    await t2.engine.publishPresence(KS, ROOM, { typing: true });
    await settleAll(t1.engine, t2.engine);
    await r.clock.advance(1100);
    await settleAll(t1.engine, t2.engine);
    expect(entryState(r, ROOM)).toEqual({ typing: true });
    await t2.engine.leavePresence(KS, ROOM);
    await settleAll(t1.engine, t2.engine);
    await tick(r, [t1.engine, t2.engine], 3000);
    expect(entryState(r, ROOM)).toEqual({ typing: false }); // a state that a tab in the topic set
    const before = publishedStates(r.server).length;
    await tick(r, [t1.engine, t2.engine], 120_000, 5000);
    const after = publishedStates(r.server).slice(before);
    expect(after.length).toBeGreaterThan(0);
    for (const s of after) expect(s).toEqual({ typing: false }); // 8 publishes of {typing: true} before: a state that no tab in the topic set
    expect(leaves(r.server)).toBe(0); // t1 is still in the topic: the entry stays
  });

  it("the latest of several: three tabs, the one that set last leaves, the entry takes the latest of the other two", async () => {
    const r = tabsRig({ manifests: new StaticManifests(PRESENCE_MANIFEST) });
    const t1 = await r.open();
    const t2 = await r.open();
    const t3 = await r.open();
    await settleAll(t1.engine, t2.engine, t3.engine);
    for (const [t, v] of [[t1, 1], [t2, 2], [t3, 3]] as const) {
      await t.engine.joinPresence(KS, ROOM);
      await t.engine.publishPresence(KS, ROOM, { v });
      await settleAll(t1.engine, t2.engine, t3.engine);
      await r.clock.advance(1100);
    }
    await t3.engine.leavePresence(KS, ROOM);
    await tick(r, [t1.engine, t2.engine, t3.engine], 3000);
    expect(entryState(r, ROOM)).toEqual({ v: 2 });
  });

  // Round 19 (draft 17), 09 [READ-74], cited by 14 [CLI-61]: "When the tab that set the entry's state leaves and no remaining tab has set one, the entry keeps the last state that was published, is kept
  // alive while any tab of the engine is in the topic, and is replaced by the next publish of a remaining tab. When the last of its tabs in the topic leaves, the engine MUST leave the topic with
  // `LeavePresence`." The text settles what Q75.1 left as a gap (marked SPEC-QUESTION before).
  it("when no tab that remains set a state the entry keeps the last state that was published (09 [READ-74]), is kept alive while a tab is in the topic, and is replaced by the next publish of a remaining tab", async () => {
    const { r, t1, t2 } = await setup();
    await t1.engine.joinPresence(KS, ROOM); // in the topic, sets nothing
    await t2.engine.joinPresence(KS, ROOM);
    await t2.engine.publishPresence(KS, ROOM, { typing: true });
    await settleAll(t1.engine, t2.engine);
    await t2.engine.leavePresence(KS, ROOM);
    await tick(r, [t1.engine, t2.engine], 3000);
    expect(entryState(r, ROOM)).toEqual({ typing: true }); // the last state that was published
    expect(t1.engine.presenceTopics()[0]).toMatchObject({ orphaned: true });
    expect(leaves(r.server)).toBe(0); // t1 is in the topic: the engine does not leave it, and the other members get no `left_client_ids`
    // kept alive: the entry would expire after the TTL (30 s here) if the engine did not publish it again, and it does, with the same state, for as long as a tab is in the topic
    const before = publishedStates(r.server).length;
    await tick(r, [t1.engine, t2.engine], 120_000, 5000);
    const kept = publishedStates(r.server).slice(before);
    expect(kept.length).toBeGreaterThanOrEqual(4); // about every half TTL
    for (const s of kept) expect(s).toEqual({ typing: true });
    expect(r.server.presence.entries(KS, ROOM)).toHaveLength(1); // the entry never expired
    expect(leaves(r.server)).toBe(0);
    // replaced by the next publish of a remaining tab
    await t1.engine.publishPresence(KS, ROOM, { typing: false });
    await tick(r, [t1.engine, t2.engine], 3000);
    expect(entryState(r, ROOM)).toEqual({ typing: false });
    expect(t1.engine.presenceTopics()[0]).toMatchObject({ orphaned: false });
    // and when the last tab in the topic leaves, the engine leaves it
    await t1.engine.leavePresence(KS, ROOM);
    await tick(r, [t1.engine, t2.engine], 3000);
    expect(leaves(r.server)).toBe(1);
    expect(r.server.presence.entries(KS, ROOM)).toEqual([]);
  });

  it("a state that a tab published and the engine has not sent yet (coalesced within the publish interval) is the last state that was published when that tab leaves: it is sent, and the entry keeps it", async () => {
    const { r, t1, t2 } = await setup();
    await t1.engine.joinPresence(KS, ROOM);
    await t2.engine.joinPresence(KS, ROOM);
    await t2.engine.publishPresence(KS, ROOM, { n: 1 });
    await settleAll(t1.engine, t2.engine);
    await t2.engine.publishPresence(KS, ROOM, { n: 2 }); // within the interval of the first: it waits for the next slot
    await t2.engine.leavePresence(KS, ROOM);
    await tick(r, [t1.engine, t2.engine], 3000);
    expect(entryState(r, ROOM)).toEqual({ n: 2 });
    expect(leaves(r.server)).toBe(0);
  });
});

// ---------------------------------------------------------------------------------------------------------------------
describe("V21 SHOULD-2, 14 [CLI-61], 09 [READ-74]: a follower tab shows the user's own entry as the user's, not as another member's (the probe PP4)", () => {
  it("the server sends the publisher its own entry: the flusher's view and the follower's both leave it out of others()", async () => {
    const r = tabsRig({ manifests: new StaticManifests(PRESENCE_MANIFEST) });
    const t1 = await r.open();
    const t2 = await r.open();
    await settleAll(t1.engine, t2.engine);
    await t2.engine.joinPresence(KS, ROOM);
    await t2.engine.publishPresence(KS, ROOM, { here: true });
    await settleAll(t1.engine, t2.engine);
    r.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: t1.engine.clientId, state: { here: true }, expiresMs: r.clock.now() + 30_000, sub: "ana" }] }));
    await settleAll(t1.engine, t2.engine);
    const v1 = new PresenceView(t1.engine, KS, ROOM);
    const v2 = new PresenceView(t2.engine, KS, ROOM);
    expect(t2.engine.clientId).toBe(""); // a follower has no client instance (03 [ID-35])
    expect(t2.engine.presenceSelfId).toBe(t1.engine.clientId);
    expect(v1.entries()).toHaveLength(1);
    expect(v2.entries()).toHaveLength(1);
    expect(v1.entries()[0]?.self).toBe(true);
    expect(v2.entries()[0]?.self).toBe(true);
    expect(v1.others()).toHaveLength(0);
    expect(v2.others()).toHaveLength(0);
    // another member is another member
    r.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: "someone-else", state: { here: false }, expiresMs: r.clock.now() + 30_000, sub: "zoe" }] }));
    await settleAll(t1.engine, t2.engine);
    expect(v2.others().map((e) => e.clientId)).toEqual(["someone-else"]);
  });

  it("a flusher that mints another client_id (a rebind) tells the followers of it", async () => {
    const r = tabsRig({ manifests: new StaticManifests(PRESENCE_MANIFEST) });
    const t1 = await r.open();
    const t2 = await r.open();
    await settleAll(t1.engine, t2.engine);
    const first = t1.engine.clientId;
    expect(t2.engine.presenceSelfId).toBe(first);
    await t1.engine.stop(); // the lock goes to t2, which mints its own
    await settleAll(t2.engine);
    expect(t2.engine.status().role).toBe("flusher");
    expect(t2.engine.presenceSelfId).toBe(t2.engine.clientId);
    expect(t2.engine.presenceSelfId).not.toBe(first);
  });
});

// ---------------------------------------------------------------------------------------------------------------------
describe("V21 N4, N5, 09 [READ-73], [READ-74]: a leave while the join is under way, and an entry that is over the limit", () => {
  it("a tab that leaves while its join is in flight: the server made the session a member, so the engine sends the LeavePresence when the join is answered (N4)", async () => {
    const a = await prig({ seed: 11 });
    a.server.inject("joinPresence", { kind: "delay", ms: 2000 }, { times: 1 }); // the join call is under way for two seconds
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    await a.engine.leavePresence(KS, ROOM);
    await tick(a, [a.engine], 5000);
    expect(a.engine.inPresence(KS, ROOM)).toBe(false);
    expect(leaves(a.server)).toBe(1);
    expect(a.server.presence.members(KS, ROOM)).toEqual([]); // the session did not stay a member of the topic until it ended
  });

  it("an awareness entry over max_presence_state_bytes is refused once, not at every pump, and no keepalive is armed for it (N5)", async () => {
    const a = await prig({ seed: 11 });
    const seen: PresenceEvent[] = [];
    a.engine.onPresence((e) => seen.push(e));
    await a.engine.joinPresence(KS, DOC);
    await a.engine.publishPresence(KS, DOC, { cursor: "a".repeat(5000) }); // an awareness state is judged as the whole entry: 5,000 octets are over the 4 KiB limit
    await a.engine.settle();
    expect(seen.filter((e) => e.type === "refused")).toHaveLength(1);
    for (let i = 0; i < 5; i++) {
      await tick(a, [a.engine], 60_000, 5000);
      await a.engine.joinPresence(KS, DOC); // a repeated join pumps the topic again
    }
    expect(seen.filter((e) => e.type === "refused")).toHaveLength(1);
    expect(a.server.calls.filter((c) => c.method === "publishPresence")).toHaveLength(0);
  });
});

