// 14 [CLI-56] (V14 S6): the time since the last successful sync outlives a restart that has no replica image, so that the warning still comes;
// and 14 [CLI-50] (V14 N1): "stop and report" reports with `protocol-error` while it holds the lane.

import { describe, expect, it } from "vitest";
import { set } from "../src/engine/index.js";
import { kc, KS, ofType, rig, tick, type Rig } from "./helpers/engine.js";

const DAY = 24 * 3600 * 1000;
const setK = (key: string, v: unknown = 1): { keyspaceId: string; opType: string; changes: ReturnType<typeof set>[] } => ({ keyspaceId: KS, opType: "Set", changes: [set(key, v)] });
const limits = (days: number): { serverOptions: { limits: Record<string, number> }; heartbeatMs: number } => ({ serverOptions: { limits: { maxOfflineSeconds: days * 24 * 3600 } }, heartbeatMs: 0 });
const warnings = (r: Rig): { kind?: string }[] => ofType(r, "offline-horizon");

describe("14 [CLI-56]: the time of the last sync survives a restart that has no replica image (V14 S6)", () => {
  it("an engine that was last synced days ago and restarts offline, with no image to restore, warns that the sync is old", async () => {
    const r = await rig({ ...limits(10), cfg: { persistReplica: false } });
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    await r.engine.stop();
    expect(r.storage.replicaKeyspaces()).toEqual([]); // no image: only the followed list keeps the time
    r.server.suspend();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, start: false, seed: 4, keyspaces: [], cfg: { persistReplica: false } });
    r2.engine.start();
    await tick(r2, 8 * DAY + 60_000);
    expect(warnings(r2).map((e) => e.kind)).toEqual(["SYNC"]);
  });

  it("the time is kept while the session is live: a process that is killed, with no stop, loses at most the last 30 s of it", async () => {
    const r = await rig({ ...limits(10), cfg: { persistReplica: false } });
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    await r.engine.settle();
    expect(String(r.storage.metaValue("followed"))).toContain("lastSyncMs");
    r.engine.kill();
    r.server.suspend();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, start: false, seed: 4, keyspaces: [], cfg: { persistReplica: false } });
    r2.engine.start();
    await tick(r2, 8 * DAY + 60_000);
    expect(warnings(r2).map((e) => e.kind)).toEqual(["SYNC"]);
  });
});

describe("14 [CLI-50], A.7: \"stop and report\" is reported with `protocol-error` (V14 N1)", () => {
  for (const status of ["NotFound", "AlreadyExists", "FailedPrecondition"] as const) {
    it(`${status} with no detail: the lane is held, and \`protocol-error\` says so, once for the hold`, async () => {
      const r = await rig();
      r.server.inject("mutate", { kind: "status", status });
      await r.engine.submit(setK("settings/a"));
      await r.engine.settle();
      const pe = ofType(r, "protocol-error");
      expect(pe).toHaveLength(1);
      expect(pe[0]).toMatchObject({ what: "Mutate", keyspaces: [KS] });
      expect(pe[0]?.detail).toContain(status);
      expect(r.events.filter((e) => e.type === "held").map((e) => (e as { state: string }).state)).toEqual(["start"]);
      await r.engine.submit(setK("settings/b")); // a second action of the held lane: no second report
      await r.engine.settle();
      expect(ofType(r, "protocol-error")).toHaveLength(1);
    });
  }
});
