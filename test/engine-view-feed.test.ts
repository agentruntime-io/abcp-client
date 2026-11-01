// The engine's feed of changes to the view (engine/view-feed.ts), which the local query layer is driven by (C4): at the end of each task of the engine, one report for each
// keyspace whose confirmed state or prediction changed in it, with the keys that changed. 14 [CLI-6] (the view is recomputed when confirmed state advances or the outbox
// changes), [CLI-2] (confirmed state is fed by loads and records), 05 [STATE-43] (a subtree deletion), 14 [CLI-71], [CLI-72] (the interest).

import { describe, expect, it } from "vitest";
import { del, set, type ViewChange } from "../src/engine/index.js";
import { goOffline, goOnline, kc, KS, rig, tick, type Rig } from "./helpers/engine.js";

const sorted = (s: ReadonlySet<string>): string[] => [...s].sort();

/** The changes that the feed reports, as a plain list. */
function feed(r: Rig): ViewChange[] {
  const out: ViewChange[] = [];
  r.engine.onViewChange((c) => out.push(c));
  return out;
}

describe("the feed of changes to the view", () => {
  it("a record from another principal is one report: the keys that it names, and the subtree of each key that it deletes; the prediction is untouched", async () => {
    const r = await rig();
    const changes = feed(r);
    r.server.write(KS, [kc("settings/a", "set", 1), kc("issues/i1", "delete"), kc("settings/b", "set", 2)]);
    await r.engine.settle();
    expect(changes).toHaveLength(1);
    const c = changes[0] as ViewChange;
    expect(c).toMatchObject({ keyspaceId: KS, all: false });
    expect(sorted(c.confirmed.keys)).toEqual(["issues/i1", "settings/a", "settings/b"]);
    expect(sorted(c.confirmed.subtrees)).toEqual(["issues/i1"]);
    expect(sorted(c.overlay.keys)).toEqual([]);
  });

  it("a submit, a cancel and a commit report the keys of the pending action in the overlay, and only then the record that confirms it in the confirmed state", async () => {
    const r = await rig();
    await goOffline(r);
    const changes = feed(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/x", 1), del("issues/i9")] });
    expect(changes).toHaveLength(1);
    expect(sorted(changes[0]?.overlay.keys ?? new Set())).toEqual(["issues/i9", "settings/x"]);
    expect(sorted(changes[0]?.overlay.subtrees ?? new Set())).toEqual(["issues/i9"]); // a pending DELETE of a marker key hides its subtree
    expect(sorted(changes[0]?.confirmed.keys ?? new Set())).toEqual([]);
    await r.engine.cancel(a.mutationId);
    expect(changes).toHaveLength(2);
    expect(sorted(changes[1]?.overlay.keys ?? new Set())).toEqual(["issues/i9", "settings/x"]);
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/y", 2)] });
    changes.length = 0;
    await goOnline(r);
    await r.engine.settle();
    expect(b.mutationId).toBeDefined();
    // the record that confirms it, and the retirement of the action from the overlay
    expect(changes.some((c) => c.confirmed.keys.has("settings/y"))).toBe(true);
    expect(changes.some((c) => c.overlay.keys.has("settings/y"))).toBe(true);
    expect(changes.every((c) => !c.all)).toBe(true);
  });

  it("a keyspace's change that leaves a pending action's cell as it was is not reported: an overlay is compared cell by cell", async () => {
    const r = await rig();
    await goOffline(r);
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/x", 1)] });
    const changes = feed(r);
    // another action on another key: the cell of settings/x is the same before and after
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/z", 3)] });
    expect(changes).toHaveLength(1);
    expect(sorted(changes[0]?.overlay.keys ?? new Set())).toEqual(["settings/z"]);
  });

  it("a load that replaces the state, and a change of the interest, are reported as anything may have changed", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    r.server.write(KS, [kc("settings/b", "set", 2)]);
    await r.engine.settle();
    const changes = feed(r);
    r.server.restore(KS, 1n);
    await r.engine.settle();
    expect(changes.some((c) => c.all)).toBe(true);
    changes.length = 0;
    await r.engine.setInterest(KS, ["settings"]);
    await r.engine.settle();
    expect(changes.some((c) => c.all)).toBe(true);
  });

  it("a heartbeat that only moves the cursor reports nothing; a keyspace that the engine stops following reports it, though no key changed (14 [CLI-67])", async () => {
    const r = await rig({ heartbeatMs: 1000 });
    const changes = feed(r);
    await tick(r, 10_000);
    expect(changes).toEqual([]);
    r.server.denyRead("ana", KS);
    await r.engine.settle();
    expect(r.engine.keyspaceInfo(KS)?.stopped).toBe("GRANT_CHANGED");
    expect(changes.some((c) => c.all)).toBe(true);
  });

  it("nothing is recorded while no one listens: a listener that registers later hears of what happens after it, not of what came before", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/before", "set", 1)]);
    await r.engine.settle();
    const changes = feed(r);
    expect(changes).toEqual([]);
    r.server.write(KS, [kc("settings/after", "set", 1)]);
    await r.engine.settle();
    expect(changes).toHaveLength(1);
    expect(sorted(changes[0]?.confirmed.keys ?? new Set())).toEqual(["settings/after"]);
  });

  it("a listener that throws never stops the engine or the other listeners, and `flushViewChanges` reports what is pending at once, once", async () => {
    const r = await rig();
    const heard: ViewChange[] = [];
    r.engine.onViewChange(() => {
      throw new Error("a listener of the app");
    });
    r.engine.onViewChange((c) => heard.push(c));
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    expect(heard).toHaveLength(1);
    expect(r.engine.status().pending).toBe(0);
    r.engine.flushViewChanges();
    expect(heard).toHaveLength(1);
  });

  it("the keyspaceInfo of a copy: loaded, purged, stopped, its cursor and its interest", async () => {
    const r = await rig({ cfg: { copyPolicy: "purge" } });
    expect(r.engine.keyspaceInfo("nope")).toBeUndefined();
    expect(r.engine.keyspaceInfo(KS)).toMatchObject({ loaded: true, purged: false, stopped: undefined, interest: [], manifestVersion: 1 });
    r.server.denyRead("ana", KS);
    await r.engine.settle();
    expect(r.engine.keyspaceInfo(KS)).toMatchObject({ purged: true, stopped: "GRANT_CHANGED" });
  });
});
