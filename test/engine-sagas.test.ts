// V19 MUST-5, 14 [CLI-14] "Following sagas": a client that shows a pointer of a moved entity, or that started a saga, follows the saga's run key
// `abpc.saga.v1/run/{saga_id}` in its home keyspace, through its interest or by loading the key, and emits `saga-ended` when the run key's `status` says that the
// saga ended or failed (14 §14.5), with the cause that the run key records (11 §11.4.4).

import { describe, expect, it } from "vitest";
import { MemoryStorage, set } from "../src/engine/index.js";
import { SAGA_POLL_MS } from "../src/engine/sagas.js";
import { kc, KS, ofType, rig, tick, type Rig } from "./helpers/engine.js";

const SAGA = "5c2e9f1a-6b7d-4c3e-8a9f-0d1e2f3a4b5c";
const RUN = `abpc.saga.v1/run/${SAGA}`;
const moved = (marker: string, status: string, saga = SAGA): ReturnType<typeof kc> => kc(`abpc.saga.v1/moved/${marker}`, "set", { status, saga_id: saga, saga: "move", to: { keyspace_id: "ks2", key: marker } });
const run = (status?: string, cause?: string): ReturnType<typeof kc> => kc(RUN, "set", { saga: "move", ...(status !== undefined ? { status } : {}), ...(cause !== undefined ? { cause } : {}) });
const open = (): ReturnType<typeof kc> => kc(`${RUN}/open`, "set", true);
const ended = (r: Rig): { sagaId: string; status: string; cause?: string; keyspaceId: string }[] => ofType(r, "saga-ended").map((e) => ({ keyspaceId: e.keyspaceId, sagaId: e.sagaId, status: e.status, ...(e.cause !== undefined ? { cause: e.cause } : {}) }));

describe("14 [CLI-14] (V19 MUST-5): the run key of a saga that the client shows a pointer of, or started, is followed, and `saga-ended` is emitted", () => {
  it("a pointer of a moving entity starts the follow; the run key's status completed ends it: one `saga-ended`, once", async () => {
    const r = await rig();
    r.server.write(KS, [moved("issues/i1", "moving"), run("running"), open()]);
    await r.engine.settle();
    expect(r.engine.followedSagas()).toMatchObject([{ keyspaceId: KS, sagaId: SAGA, pointers: ["issues/i1"], open: true }]);
    expect(ended(r)).toEqual([]);
    r.server.write(KS, [moved("issues/i1", "moved"), run("completed"), kc(`${RUN}/open`, "delete")]);
    await r.engine.settle();
    expect(ended(r)).toEqual([{ keyspaceId: KS, sagaId: SAGA, status: "completed" }]);
    // a later record, and a change of the pointer's own status, say nothing more
    r.server.write(KS, [kc("settings/x", "set", 1)]);
    await r.engine.settle();
    expect(ended(r)).toHaveLength(1);
    // the pointer is still shown: the watch stays, with what was said; it ends when the pointer goes
    expect(r.engine.followedSagas()).toMatchObject([{ sagaId: SAGA, endedStatus: "completed", open: false }]);
    r.server.write(KS, [kc("abpc.saga.v1/moved/issues/i1", "delete")]);
    await r.engine.settle();
    expect(r.engine.followedSagas()).toEqual([]);
  });

  it("a failed move: `saga-ended` with the cause of the run key (EPOCH); its resolution is another, with ENTITY_LOST; each said once", async () => {
    const r = await rig();
    r.server.write(KS, [moved("issues/i1", "moving"), run("running"), open()]);
    await r.engine.settle();
    r.server.write(KS, [run("failed", "EPOCH")]);
    await r.engine.settle();
    expect(ended(r)).toEqual([{ keyspaceId: KS, sagaId: SAGA, status: "failed", cause: "EPOCH" }]);
    expect(r.engine.followedSagas()).toHaveLength(1); // a failed run stays open until it is resolved: still followed
    r.server.write(KS, [kc("settings/y", "set", 2)]);
    await r.engine.settle();
    expect(ended(r)).toHaveLength(1);
    r.server.write(KS, [run("resolved", "ENTITY_LOST"), kc(`${RUN}/open`, "delete")]);
    await r.engine.settle();
    expect(ended(r)).toEqual([
      { keyspaceId: KS, sagaId: SAGA, status: "failed", cause: "EPOCH" },
      { keyspaceId: KS, sagaId: SAGA, status: "resolved", cause: "ENTITY_LOST" },
    ]);
  });

  it("every status that ends a saga: compensated, abandoned, completed", async () => {
    for (const status of ["compensated", "abandoned", "completed"]) {
      const r = await rig();
      r.server.write(KS, [moved("issues/i1", "moving"), run("running"), open()]);
      await r.engine.settle();
      r.server.write(KS, [run(status)]);
      await r.engine.settle();
      expect(ended(r).map((e) => e.status)).toEqual([status]);
    }
  });

  it("a run key that the interest does not cover is loaded: at the start of the follow, and every SAGA_POLL_MS while the saga is not over", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/s", "set", 1), kc("issues/i1", "set", "an issue")]);
    await r.engine.settle();
    // the interest holds the moved keys and the app's own, not the run keys
    await r.engine.setInterest(KS, ["abpc.saga.v1/moved", "settings"]);
    await r.engine.settle();
    r.server.write(KS, [moved("issues/i1", "moving"), run("running"), open()]);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).has(RUN)).toBe(false); // the run key is outside the interest
    expect(r.engine.followedSagas()).toMatchObject([{ sagaId: SAGA, open: true }]); // found by loading it
    const loads = (): number => r.server.calls.filter((c) => c.method === "loadState").length;
    const before = loads();
    r.server.write(KS, [run("completed"), kc(`${RUN}/open`, "delete")]);
    await r.engine.settle();
    expect(ended(r)).toEqual([]); // not seen yet: nothing delivers it
    await tick(r, SAGA_POLL_MS + 1000);
    expect(loads()).toBeGreaterThan(before);
    expect(ended(r)).toEqual([{ keyspaceId: KS, sagaId: SAGA, status: "completed" }]);
    await tick(r, SAGA_POLL_MS * 3); // and no more polling or saying once it is over and its pointer is shown with nothing left to follow
    expect(ended(r)).toHaveLength(1);
  });

  it("a saga that the client started: the Start's SET of its run key is followed, whatever the interest shows of it, and its end is said", async () => {
    const r = await rig();
    const start = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set(RUN, { saga: "steps", definition: "d" })] });
    await r.engine.settle();
    expect(start.mutationId).toBeTruthy();
    expect(r.engine.followedSagas()).toMatchObject([{ keyspaceId: KS, sagaId: SAGA, started: true, open: false, endedStatus: undefined }]);
    r.server.write(KS, [run("running"), open()]);
    await r.engine.settle();
    expect(ended(r)).toEqual([]);
    r.server.write(KS, [run("compensated"), kc(`${RUN}/open`, "delete")]);
    await r.engine.settle();
    expect(ended(r)).toEqual([{ keyspaceId: KS, sagaId: SAGA, status: "compensated" }]);
    expect(r.engine.followedSagas()).toEqual([]); // ended, no open key, no pointer: nothing left to follow
  });

  it("the sagas that were followed, and what was said of them, survive a restart: nothing is said twice, and a saga that ends after it is said once", async () => {
    const storage = new MemoryStorage();
    const r = await rig({ storage });
    r.server.write(KS, [moved("issues/i1", "moving"), run("running"), open()]);
    await r.engine.settle();
    r.server.write(KS, [moved("issues/i1", "moved"), run("completed"), kc(`${RUN}/open`, "delete")]);
    await r.engine.settle();
    expect(ended(r)).toHaveLength(1);
    await r.engine.stop();
    const r2 = await rig({ storage, server: r.server, clock: r.clock, seed: 9, keyspaces: [] });
    await r2.engine.settle();
    await tick(r2, 40_000);
    expect(ended(r2)).toEqual([]); // said before the restart
    expect(r2.engine.followedSagas()).toMatchObject([{ sagaId: SAGA, endedStatus: "completed" }]);
    // another saga, over a second entity, begins and ends after the restart
    const S2 = "11111111-2222-4333-8444-555555555555";
    r2.server.write(KS, [moved("issues/i2", "moving", S2), kc(`abpc.saga.v1/run/${S2}`, "set", { saga: "move", status: "failed", cause: "REDACTED" })]);
    await r2.engine.settle();
    expect(ended(r2)).toEqual([{ keyspaceId: KS, sagaId: S2, status: "failed", cause: "REDACTED" }]);
  });
});
