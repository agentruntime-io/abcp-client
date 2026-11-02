// C6, the query layer's view of a presence topic (`src/query/presence.ts`): 09 §9.8 [READ-81], [READ-83], 16 [SEC-1], [SEC-28], 14 [CLI-61]. The engine applies the frames; the view decodes the states
// with the strict json/v1 reader and reports the changes.

import { describe, expect, it } from "vitest";
import { StaticManifests } from "../src/engine/index.js";
import { createQueries, type PresenceUpdate } from "../src/query/index.js";
import { rig } from "./helpers/engine.js";
import { DOC, frame, KS, PRESENCE_MANIFEST, prig, ROOM, second } from "./helpers/presence.js";
import { settleAll, tabsRig } from "./helpers/tabs.js";

describe("query layer: the view of a presence topic", () => {
  it("watch calls the listener at once with every entry, then with what came, what changed and what is gone; a state is decoded as json/v1 (integers are bigints)", async () => {
    const a = await prig({ seed: 11 });
    const b = await second(a);
    const qa = createQueries(a.engine);
    const qb = createQueries(b.engine);
    const view = qa.presence(KS, ROOM);
    await view.join();
    const updates: PresenceUpdate[] = [];
    const off = view.watch((u) => updates.push(u));
    expect(updates).toMatchObject([{ cause: "initial", entries: [], added: [], removed: [] }]);
    const bob = qb.presence(KS, ROOM);
    await bob.publish({ viewing: true, n: 7 }); // joins first
    await a.engine.settle();
    expect(updates.at(-1)).toMatchObject({ cause: "changed", removed: [] });
    expect(updates.at(-1)?.added.map((e) => e.clientId)).toEqual([b.engine.clientId]);
    const entry = view.entries()[0];
    expect(entry?.state).toEqual({ viewing: true, n: 7n });
    expect(entry?.principal?.sub).toBe("bob");
    expect(entry?.self).toBe(false);
    expect(entry?.undecodable).toBe(false);
    // a changed state
    await bob.publish({ viewing: false, n: 8 });
    await a.clock.advance(1100);
    await a.engine.settle();
    expect(updates.at(-1)?.changed.map((e) => e.state)).toEqual([{ viewing: false, n: 8n }]);
    expect(updates.at(-1)?.added).toEqual([]);
    // the same state published again changes nothing that the watcher is told (the keepalive of every member would be noise)
    const n = updates.length;
    await a.clock.advance(20_000);
    await a.engine.settle();
    expect(updates).toHaveLength(n);
    expect(view.entries()).toHaveLength(1);
    // a leave
    await bob.leave();
    await a.engine.settle();
    expect(updates.at(-1)?.removed).toEqual([b.engine.clientId]);
    expect(view.entries()).toEqual([]);
    off();
    await view.close();
    await bob.close();
    qa.close();
    qb.close();
  });

  it("an entry of this engine, sent back in a snapshot, is marked self, and `others` leaves it out", async () => {
    const a = await prig({ seed: 11 });
    const q = createQueries(a.engine);
    const view = q.presence(KS, ROOM);
    await view.join();
    await a.engine.settle();
    a.server.sendFrame(frame({ topic: ROOM, snapshot: true, entries: [{ clientId: a.engine.clientId, state: { me: true }, expiresMs: a.clock.now() + 20_000 }, { clientId: "c-9", state: { me: false }, expiresMs: a.clock.now() + 20_000 }] }));
    await a.engine.settle();
    expect(view.entries().filter((e) => e.self).map((e) => e.clientId)).toEqual([a.engine.clientId]);
    expect(view.entries()).toHaveLength(2);
    expect(view.others().map((e) => e.clientId)).toEqual(["c-9"]);
    q.close();
  });

  it("16 [SEC-1], [SEC-28]: a state that is not json/v1 is shown undecodable, and stops nothing", async () => {
    const a = await prig({ seed: 11 });
    const q = createQueries(a.engine);
    const view = q.presence(KS, ROOM);
    await view.join();
    await a.engine.settle();
    a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: "c-1", state: "{not json", expiresMs: a.clock.now() + 20_000 }, { clientId: "c-2", state: { ok: true }, expiresMs: a.clock.now() + 20_000 }] }));
    await a.engine.settle();
    const [bad, good] = view.entries();
    expect(bad).toMatchObject({ clientId: "c-1", undecodable: true, state: undefined });
    expect(good).toMatchObject({ clientId: "c-2", undecodable: false, state: { ok: true } });
    q.close();
  });

  it("09 [READ-83]: the state of an awareness topic maps the tab ids to the tabs' states, and the view reads it so", async () => {
    const r = tabsRig({ manifests: new StaticManifests(PRESENCE_MANIFEST) });
    const t1 = await r.open();
    const t2 = await r.open();
    const t1q = createQueries(t1.engine);
    const t2q = createQueries(t2.engine);
    const other = await rig({ manifests: r.manifests, server: r.server, clock: r.clock, sub: "bob", seed: 31 });
    const bq = createQueries(other.engine);
    const v1 = t1q.presence(KS, DOC);
    const v2 = t2q.presence(KS, DOC);
    const vb = bq.presence(KS, DOC);
    await v1.join();
    await v2.join();
    await vb.join();
    await v1.publish({ cursor: "a" });
    await v2.publish({ cursor: "b" });
    await settleAll(t1.engine, t2.engine, other.engine);
    await r.clock.advance(1100);
    await settleAll(t1.engine, t2.engine, other.engine);
    const [entry] = vb.entries();
    expect(entry?.tabs).toEqual({ [t1.tab.tabId]: { cursor: "a" }, [t2.tab.tabId]: { cursor: "b" } });
    // a follower tab sees the same through the engine (14 [CLI-61])
    const seen: PresenceUpdate[] = [];
    v2.watch((u) => seen.push(u));
    await other.engine.publishPresence(KS, DOC, { cursor: "z" });
    await r.clock.advance(1100);
    await settleAll(t1.engine, t2.engine, other.engine);
    expect(v2.entries().find((e) => e.principal?.sub === "bob")?.tabs).toBeDefined();
    t1q.close();
    t2q.close();
    bq.close();
  });

  it("close leaves the topic, and a closed view refuses a watch; closing the query layer closes its views", async () => {
    const a = await prig({ seed: 11 });
    const q = createQueries(a.engine);
    const view = q.presence(KS, ROOM);
    await view.publish({ n: 1 }); // joins first
    await a.engine.settle();
    expect(a.engine.presenceTopics()).toHaveLength(1);
    await view.close();
    await a.engine.settle();
    expect(a.engine.presenceTopics()).toEqual([]);
    expect(() => view.watch(() => undefined)).toThrow();
    const v2 = q.presence(KS, ROOM);
    await v2.join();
    q.close();
    await a.engine.settle();
    expect(a.engine.presenceTopics()).toEqual([]);
  });
});
