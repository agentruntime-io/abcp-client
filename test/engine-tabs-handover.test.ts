// 14 [CLI-61] and 10 [AUTH-25]: the flusher's handover (V14: MUST-1, MUST-3), a follower's call ([CLI-21], S1), the cost of a follower per frame (S4)
// and the outbox of a tab that has no reliable lock (S8).
//
// "Each outbox MUST have exactly one flusher" (10 [AUTH-25]); "Two flushers MUST NOT drain one outbox", and "the flusher MUST hand its role over to a
// visible tab of the engine, when there is one" (14 [CLI-61]).

import { describe, expect, it } from "vitest";
import { LocalRefusal, set, type Engine, type EngineEvent, type MemoryStorage } from "../src/engine/index.js";
import { COMMAND_TIMEOUT_MS, HANDOVER_PROBE_MS } from "../src/engine/index.js";
import type { MutateRequestSchema } from "../src/gen/abcp/v2/sync_pb.js";
import type { create } from "@bufbuild/protobuf";
import { KS } from "./helpers/engine.js";
import { ORIGIN, settleAll, tabsRig, typesOf, type TabsRig } from "./helpers/tabs.js";

const LOCK = `abcp.flusher\u0000${ORIGIN}\u0000app\u0000https://issuer.test|ana`;
const setA = (key = "settings/a", v: unknown = 1): { keyspaceId: string; opType: string; changes: ReturnType<typeof set>[] } => ({ keyspaceId: KS, opType: "Set", changes: [set(key, v)] });
const mutates = (r: TabsRig): ReturnType<typeof create<typeof MutateRequestSchema>>[] => r.server.calls.filter((c) => c.method === "mutate").map((c) => c.request as never);
const sendersOf = (r: TabsRig, id: string): string[] => mutates(r).flatMap((q) => q.mutations.filter((m) => m.mutationId === id).map((m) => m.clientId));
const subscribers = (r: TabsRig): string[] => r.server.calls.filter((c) => c.method === "subscribe").map((c) => (c.request as { clientId: string }).clientId);
const hidden = { handOverWhenHidden: true } as const;

async function offline(r: TabsRig, ...engines: Engine[]): Promise<void> {
  r.server.disconnect();
  await settleAll(...engines);
}

describe("14 [CLI-61], 10 [AUTH-25]: a handover ends the old flusher's work before the lock goes (V14 MUST-1)", () => {
  it("the old flusher's session and pump are over: the queued action is sent once, under the new client_id, by one session", async () => {
    const r = tabsRig();
    const a = await r.open({ cfg: hidden });
    const b = await r.open({ cfg: hidden });
    expect(a.engine.status().role).toBe("flusher");
    expect(r.server.sessionCount).toBe(1);
    await offline(r, a.engine, b.engine);
    const x = await a.engine.submit(setA("settings/x", 1));
    const c1 = a.engine.clientId;
    a.tab.setVisible(false); // the flusher's tab is hidden: it hands over to the visible follower
    await settleAll(a.engine, b.engine);
    expect(a.engine.status().role).toBe("follower");
    expect(b.engine.status().role).toBe("flusher");
    const c2 = b.engine.clientId;
    expect(c2).not.toBe(c1); // 14 [CLI-63]: a new client_id at every flusher handover
    r.server.reconnect();
    await r.clock.advance(5000);
    await settleAll(a.engine, b.engine);
    await r.clock.advance(5000);
    await settleAll(a.engine, b.engine);
    expect(sendersOf(r, x.mutationId)).toEqual([c2]); // never under the old client_id: "two flushers MUST NOT drain one outbox"
    expect(subscribers(r).filter((c) => c === c1)).toHaveLength(1); // the old session of the first start only
    expect(r.server.sessionCount).toBe(1);
    expect(a.engine.status().connection).toBe("live"); // a follower reports the flusher's runtime: the one session is the new flusher's
    expect(typesOf(b, x.mutationId)).toEqual(["committed"]);
    expect(typesOf(a, x.mutationId)).toEqual(["committed"]); // the follower reads through the flusher
  });

  it("the work that was in flight when the lock went is not applied by the old tab: a tab that is not the flusher writes nothing", async () => {
    const r = tabsRig();
    const a = await r.open({ cfg: hidden });
    const b = await r.open({ cfg: hidden });
    // the first request of the old flusher is slow: its answer comes after the handover; records are held back, so that the action is not retired
    r.server.inject("mutate", { kind: "delay", ms: 60_000 }, { times: 1 });
    const x = await a.engine.submit(setA("settings/slow", 1));
    await settleAll(a.engine, b.engine);
    expect(mutates(r)).toHaveLength(1);
    const c1 = a.engine.clientId;
    r.server.faults.paused = true;
    a.tab.setVisible(false);
    await settleAll(a.engine, b.engine);
    expect(b.engine.status().role).toBe("flusher");
    const store = r.device.storage.storageOf(a.engine.partitionKey()) as MemoryStorage;
    const kept = store.entry(x.mutationId);
    expect(kept?.outcome?.kind).toBe("appended"); // the new flusher got the stored result of its resend, and wrote it
    await r.clock.advance(61_000); // the old answer arrives (the server handled both requests, the second one first)
    await settleAll(a.engine, b.engine);
    // the answer that came late found a tab that writes nothing: it is of a client_id that is over (03 [ID-35]: the old flusher keeps none), so it is not applied
    // at all; where it would be (a GATED action has no slot to lose), the write is refused, and that is the only failure that the tab notes
    expect(a.engine.clientId).toBe("");
    expect(a.engine.internalErrors.filter((e) => !(e instanceof Error && /not the flusher/.test(e.message)))).toEqual([]);
    expect(store.entry(x.mutationId)?.outcome).toEqual(kept?.outcome);
    r.server.resumeDelivery();
    await r.clock.advance(5000);
    await settleAll(a.engine, b.engine);
    expect(sendersOf(r, x.mutationId).filter((c) => c === c1)).toHaveLength(1); // the old flusher sent it once, and never again
    expect(typesOf(b, x.mutationId)).toEqual(["committed"]); // the new flusher's one report
    expect(b.engine.outbox()).toHaveLength(0);
    expect(b.engine.journalEntries().filter((j) => j.mutationId === x.mutationId)).toHaveLength(1);
    expect(store.entry(x.mutationId)).toBeUndefined();
  });

  it("a hidden flusher keeps flushing when the tab becomes visible again before a tab answers (the offer is withdrawn)", async () => {
    const r = tabsRig();
    const a = await r.open({ cfg: hidden });
    const b = await r.open({ cfg: hidden });
    a.tab.setVisible(false);
    a.tab.setVisible(true);
    await settleAll(a.engine, b.engine);
    await r.clock.advance(HANDOVER_PROBE_MS * 2);
    await settleAll(a.engine, b.engine);
    expect(a.engine.status().role).toBe("flusher");
    expect(b.engine.status().role).toBe("follower");
    expect(r.device.lockManager.holder(LOCK)).toBe(a.tab.tabId);
  });
});

describe("14 [CLI-61]: the role goes to a tab that is alive and visible, and the tab that hands it over is a follower (V14 MUST-3)", () => {
  it("the next tab in line is hidden, a visible one waits behind it: the visible one is the flusher", async () => {
    const r = tabsRig();
    const a = await r.open({ cfg: hidden });
    const b = await r.open({ tab: { visible: false }, cfg: hidden }); // hidden, first in line
    const c = await r.open({ cfg: hidden }); // visible, second in line
    await settleAll(a.engine, b.engine, c.engine);
    a.tab.setVisible(false);
    await settleAll(a.engine, b.engine, c.engine);
    expect({ a: a.engine.status().role, b: b.engine.status().role, c: c.engine.status().role }).toEqual({ a: "follower", b: "follower", c: "flusher" });
    expect(r.device.lockManager.holder(LOCK)).toBe(c.tab.tabId);
    expect(r.device.lockManager.waiting(LOCK)).toBe(2); // the two that are followers wait for the lock again
    // nothing was lost on the way: the new flusher sends what is queued
    const x = await c.engine.submit(setA("settings/c", 1));
    await settleAll(a.engine, b.engine, c.engine);
    expect(typesOf(c, x.mutationId)).toEqual(["committed"]);
    expect(r.server.sessionCount).toBe(1);
  });

  it("a peer that crashed without `gone` is not a visible tab: the hidden flusher keeps the role, and the device still has a flusher", async () => {
    const r = tabsRig();
    const a = await r.open({ cfg: hidden });
    const b = await r.open({ cfg: hidden });
    await settleAll(a.engine, b.engine);
    b.tab.crash();
    b.engine.kill();
    await settleAll(a.engine);
    a.tab.setVisible(false);
    await settleAll(a.engine);
    await r.clock.advance(HANDOVER_PROBE_MS + 1); // no tab answered the offer
    await settleAll(a.engine);
    expect(a.engine.status().role).toBe("flusher");
    expect(r.device.lockManager.holder(LOCK)).toBe(a.tab.tabId);
    const x = await a.engine.submit(setA("settings/still", 1));
    await settleAll(a.engine);
    expect(typesOf(a, x.mutationId)).toEqual(["committed"]); // it goes on flushing, hidden
  });

  it("after a handover the hidden tab's engine is a follower, and stays one when it is visible again", async () => {
    const r = tabsRig();
    const a = await r.open({ cfg: hidden });
    const b = await r.open({ cfg: hidden });
    a.tab.setVisible(false);
    await settleAll(a.engine, b.engine);
    expect(a.events.filter((e) => e.type === "role").at(-1)).toMatchObject({ role: "follower", cause: "HANDED_OVER" });
    a.tab.setVisible(true);
    await settleAll(a.engine, b.engine);
    expect(a.engine.status().role).toBe("follower");
    expect(b.engine.status().role).toBe("flusher");
    // its engine works: it reads through the new flusher, and its calls go to it
    const x = await a.engine.submit(setA("settings/f", 1));
    await settleAll(a.engine, b.engine);
    expect(typesOf(a, x.mutationId)).toEqual(["committed"]);
    expect(sendersOf(r, x.mutationId)).toEqual([b.engine.clientId]);
  });

  it("the tab that handed the role over takes it back, as a tab that was never the flusher, when the new flusher leaves", async () => {
    const r = tabsRig();
    const a = await r.open({ cfg: hidden });
    const b = await r.open({ cfg: hidden });
    const c1 = a.engine.clientId;
    a.tab.setVisible(false);
    await settleAll(a.engine, b.engine);
    a.tab.setVisible(true);
    const c2 = b.engine.clientId;
    await b.engine.stop();
    await settleAll(a.engine);
    expect(a.engine.status().role).toBe("flusher");
    expect(a.engine.clientId).not.toBe(c1);
    expect(a.engine.clientId).not.toBe(c2);
    expect(a.engine.status().holds).toEqual([]);
    const x = await a.engine.submit(setA("settings/x", 1));
    await settleAll(a.engine);
    expect(sendersOf(r, x.mutationId)).toEqual([a.engine.clientId]);
    expect(typesOf(a, x.mutationId)).toEqual(["committed"]);
    expect(r.server.sessionCount).toBe(1);
  });

  it("the chosen tab goes away before it has the lock: the tabs ahead of it pass the lock on once, and the device still ends with one flusher", async () => {
    const r = tabsRig();
    const a = await r.open({ cfg: hidden });
    const b = await r.open({ tab: { visible: false }, cfg: hidden });
    const c = await r.open({ cfg: hidden });
    await settleAll(a.engine, b.engine, c.engine);
    const deliver = r.device.deliver.bind(r.device);
    r.device.deliver = (from, message): void => {
      deliver(from, message);
      if ((message as { t?: string }).t === "handoff") c.tab.crash(); // the tab that was chosen is killed as the announcement goes out
    };
    a.tab.setVisible(false);
    await settleAll(a.engine, b.engine);
    const roles = [a, b].map((t) => t.engine.status().role);
    expect(roles.filter((x) => x === "flusher")).toHaveLength(1); // one flusher, a hidden one: "when there is one"
    expect(r.device.lockManager.holder(LOCK)).toBeDefined();
    await r.clock.advance(1000);
    await settleAll(a.engine, b.engine);
    expect(r.server.sessionCount).toBe(1);
  });
});

describe("14 [CLI-21], [CLI-61]: a follower's call is never refused while the flusher may still commit it (V14 S1, Q54.1)", () => {
  const settled = (p: Promise<unknown>): { state: () => string; code: () => string | undefined } => {
    let s = "pending";
    let code: string | undefined;
    p.then(
      () => (s = "accepted"),
      (e: unknown) => {
        s = "refused";
        code = e instanceof LocalRefusal ? e.code : "other";
      },
    );
    return { state: () => s, code: () => code };
  };

  it("a flusher that does not answer leaves the call waiting, reported as such; the tab that is promoted runs it itself, once", async () => {
    const r = tabsRig();
    const a = await r.open();
    const b = await r.open();
    a.engine.kill(); // hung: its lock is not freed (yet)
    const call = b.engine.submit(setA("settings/waits", 1));
    const p = settled(call);
    await r.clock.advance(COMMAND_TIMEOUT_MS * 3);
    await settleAll(b.engine);
    expect(p.state()).toBe("pending"); // not refused: the flusher may still commit it
    expect(b.engine.status().pendingCalls).toBe(1);
    expect(b.engine.outbox()).toHaveLength(0);
    a.tab.crash(); // the platform frees the lock: the next tab in line is the flusher, and the call is its own
    await settleAll(b.engine);
    expect(p.state()).toBe("accepted");
    const got = await call;
    expect(b.engine.status().role).toBe("flusher");
    expect(b.engine.status().pendingCalls).toBe(0);
    await r.clock.advance(2000);
    await settleAll(b.engine);
    expect(typesOf(b, got.mutationId)).toEqual(["committed"]);
    expect(mutates(r).flatMap((q) => q.mutations.filter((m) => m.mutationId === got.mutationId))).toHaveLength(1);
  });

  it("a flusher that committed the action and went away before it answered: the call is answered with the action's id, which is authored once", async () => {
    const r = tabsRig();
    const a = await r.open();
    const b = await r.open();
    const storage = r.device.storage.storageOf(a.engine.partitionKey());
    const commit = storage.commit.bind(storage);
    let armed = true;
    const authored = new Set<string>();
    storage.commit = async (ops): Promise<void> => {
      await commit(ops);
      for (const o of ops) if (o.op === "putEntry" && o.entry.opType === "Set") authored.add(o.entry.mutationId);
      if (armed && ops.some((o) => o.op === "putEntry" && o.entry.opType === "Set")) {
        armed = false;
        a.tab.crash(); // the platform frees the lock, the channel closes, and the answer is never sent
        a.engine.kill();
        await new Promise<never>(() => undefined);
      }
    };
    const p = b.engine.submit(setA("settings/stored", 1));
    await settleAll(b.engine);
    const got = await p;
    expect(b.engine.status().role).toBe("flusher");
    expect([...authored]).toEqual([got.mutationId]); // the one action that was ever stored, under the id that the follower minted
    await settleAll(b.engine);
    await r.clock.advance(2000);
    await settleAll(b.engine);
    expect(typesOf(b, got.mutationId)).toEqual(["committed"]);
    expect(mutates(r).flatMap((q) => q.mutations.filter((m) => m.mutationId === got.mutationId))).toHaveLength(1);
  });

  it("a call that is asked again is answered once, whichever tab runs it", async () => {
    const r = tabsRig();
    const a = await r.open();
    const b = await r.open();
    r.server.faults.paused = true; // nothing retires: the outbox shows what was authored
    const p = b.engine.submit(setA("settings/twice", 1));
    // the flusher is slow: the call is carried again (the same mutation id), and the flusher runs both copies in turn
    await r.clock.advance(COMMAND_TIMEOUT_MS + 1);
    await settleAll(a.engine, b.engine);
    const got = await p;
    expect(a.engine.outbox().map((e) => e.mutationId)).toEqual([got.mutationId]);
    expect(mutates(r).flatMap((q) => q.mutations.filter((m) => m.mutationId === got.mutationId))).toHaveLength(1);
  });

  it("an engine that stops refuses its waiting calls with NO_FLUSHER; one that the storage holds is answered", async () => {
    const r = tabsRig();
    const a = await r.open();
    const b = await r.open();
    a.engine.kill();
    const p = settled(b.engine.submit(setA("settings/gone", 1)));
    await settleAll(b.engine);
    await b.engine.stop();
    expect(p.state()).toBe("refused");
    expect(p.code()).toBe("NO_FLUSHER");
  });
});

describe("14 [CLI-61]: a follower folds what the flusher applied since it read, not the whole persisted tail again (V14 S4)", () => {
  it("n frames cost the follower n folds, and what it shows is what the flusher confirmed", async () => {
    const r = tabsRig();
    const a = await r.open();
    const b = await r.open();
    let folds = 0;
    const eng = b.engine as unknown as { foldRecord: (f: unknown, rec: unknown) => unknown };
    const orig = eng.foldRecord.bind(b.engine);
    eng.foldRecord = (f, rec): unknown => {
      folds++;
      return orig(f, rec);
    };
    const n = 12;
    for (let i = 1; i <= n; i++) {
      r.server.write(KS, [{ key: `settings/k${i}`, kind: 1, value: new TextEncoder().encode(String(i)), encoding: "json/v1" } as never]);
      await settleAll(a.engine, b.engine);
    }
    expect(a.engine.cursor(KS)?.seq).toBe(BigInt(n));
    expect(b.engine.cursor(KS)?.seq).toBe(BigInt(n));
    expect(b.engine.confirmed(KS).json(`settings/k${n}`)).toBe(n);
    expect(folds).toBeLessThanOrEqual(n); // not n * (n + 1) / 2
  });

  it("it does not read the pages of the replicas on a message that moved none: a change of the outbox, a new frame, a heartbeat (V19: V14 S4 read the whole storage on every message)", async () => {
    const r = tabsRig();
    for (let i = 1; i <= 20; i++) r.server.write(KS, [{ key: `settings/seed${i}`, kind: 1, value: new TextEncoder().encode(String(i)), encoding: "json/v1" } as never]);
    const a = await r.open();
    const b = await r.open();
    await settleAll(a.engine, b.engine);
    await r.clock.advance(20_000);
    await settleAll(a.engine, b.engine);
    const storage = r.device.storage.storageOf(a.engine.partitionKey()) as MemoryStorage & { pagesRead: number };
    expect(b.engine.confirmed(KS).json("settings/seed20")).toBe(20);
    expect(storage.pagesRead).toBeGreaterThanOrEqual(20); // the follower's first read took the 20 pages of the image
    const before = storage.pagesRead;
    // the outbox changes: an action is submitted through the follower (the flusher stores it, the follower reads the stores again)
    await b.engine.submit(setA("settings/mine", 5));
    await settleAll(a.engine, b.engine);
    // a frame of records, and a heartbeat
    r.server.write(KS, [{ key: "settings/late", kind: 1, value: new TextEncoder().encode("7"), encoding: "json/v1" } as never]);
    await settleAll(a.engine, b.engine);
    await r.clock.advance(20_000);
    await settleAll(a.engine, b.engine);
    expect(storage.pagesRead - before).toBe(0); // the 20 pages of the keyspace were not read again
    expect(b.engine.confirmed(KS).json("settings/late")).toBe(7);
    expect(b.engine.confirmed(KS).json("settings/mine")).toBe(5);
    expect(b.engine.outbox()).toHaveLength(0);
  });

  it("it reads the pages when the image moved under it: the flusher cold-loads the keyspace again, and the follower shows the new state", async () => {
    const r = tabsRig();
    r.server.write(KS, [{ key: "settings/old", kind: 1, value: new TextEncoder().encode("1"), encoding: "json/v1" } as never]);
    const a = await r.open();
    const b = await r.open();
    await settleAll(a.engine, b.engine);
    const storage = r.device.storage.storageOf(a.engine.partitionKey()) as MemoryStorage & { pagesRead: number };
    const before = storage.pagesRead;
    r.server.write(KS, [{ key: "settings/new", kind: 1, value: new TextEncoder().encode("2"), encoding: "json/v1" } as never]);
    await a.engine.setInterest(KS, ["settings"]); // a narrowing: the head's interest changes, the follower builds its mirror from the image
    await settleAll(a.engine, b.engine);
    expect(storage.pagesRead).toBeGreaterThan(before);
    expect(b.engine.confirmed(KS).json("settings/new")).toBe(2);
    expect(b.engine.confirmed(KS).json("settings/old")).toBe(1);
  });
});

describe("14 [CLI-61]: where no reliable lock exists each tab keeps an outbox of its own, named by its tab id (V14 S8)", () => {
  it("the partitions of the origin and app list the outbox of another tab, which no engine adopts", async () => {
    const r = tabsRig();
    const a = await r.open({ tab: { reliableLocks: false } });
    const b = await r.open({ tab: { reliableLocks: false } });
    await offline(r, a.engine, b.engine);
    const x = await a.engine.submit(setA("settings/mine", 1));
    expect(b.engine.outbox()).toHaveLength(0);
    const keys = await b.engine.partitions();
    expect(keys.map((k) => k.tab).sort()).toEqual([a.tab.tabId, b.tab.tabId].sort());
    // a tab that comes back under another id (a host that does not keep the id) starts with an outbox of its own: the old one is still there to be found
    const c = await r.open({ tab: { reliableLocks: false } });
    expect(c.engine.outbox()).toHaveLength(0);
    expect((await c.engine.partitions()).some((k) => k.tab === a.tab.tabId)).toBe(true);
    expect(a.engine.outbox().map((e) => e.mutationId)).toEqual([x.mutationId]);
  });
});

export type { EngineEvent };
