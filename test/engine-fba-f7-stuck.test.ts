// FBA batch F7, B3-M6: 14 [CLI-84] "an action due for the dead-letter store or the export hook stays in the outbox"; the action keeps the sink that it is
// due in, not that of the store that overflowed (a dependent's), and a journal entry is never put back into the outbox (14 [CLI-53]).

import { describe, expect, it } from "vitest";
import { MemoryStorage, set } from "../src/engine/index.js";
import { goOffline, KS, rig, tick, types } from "./helpers/engine.js";

const drain = (r: { engine: unknown }): Promise<void> => {
  const e = r.engine as { run<T>(fn: () => Promise<T>): Promise<T>; drainStuck(): Promise<void> };
  return e.run(() => e.drainStuck());
};

describe("B3-M6: 14 [CLI-84], [CLI-34], [CLI-38] a full store keeps the action's own sink", () => {
  it("CLI84_ACancelledActionWhoseDependentMeetsAFullDeadLetterStoreIsNeverDeadLettered", async () => {
    const storage = new MemoryStorage({ maxDeadLetters: 0 });
    const r = await rig({ storage });
    await goOffline(r);
    const a = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] })).mutationId;
    const b = (await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] })).mutationId;
    expect(r.engine.outbox().find((e) => e.mutationId === b)?.dependsOn).toContain(a);
    expect(await r.engine.cancel(a)).toBe(true); // sink none; its never-sent dependent B fails into the dead-letter store, which is full
    expect(storage.entry(a)?.stuck?.sink).toBe("none");
    storage.options.maxDeadLetters = undefined; // the app made room
    await drain(r);
    expect(r.engine.deadLetterEntries().map((d) => d.mutationId)).toEqual([b]);
    expect(types(r, a)).toEqual(["rejected"]); // exactly one final event (14 [CLI-38]), and no `dead-lettered`
    expect(types(r, b)).toEqual(["rejected", "dead-lettered"]);
  });

  it("CLI53_CLI84_AJournalledActionWhoseEndMeetsAFullExportStoreIsNeverPutBackIntoTheOutbox", async () => {
    const storage = new MemoryStorage({ maxExports: 0 });
    const r = await rig({ storage });
    const { mutationId } = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/j", 1)] });
    await r.engine.settle();
    expect(r.engine.journalEntries().map((j) => j.mutationId)).toEqual([mutationId]);
    r.server.restore(KS, 0n, { rewindBindings: true }); // EPOCH_CHANGED: known = false, voided = false: outcome unknown, due for the export hook, which is full
    for (let i = 0; i < 3; i++) await tick(r, 1_000);
    expect(storage.entry(mutationId)).toBeUndefined(); // never put back into the outbox (14 [CLI-53])
    expect(r.engine.outbox().map((e) => e.mutationId)).not.toContain(mutationId);
    expect(r.engine.journalEntries().map((j) => j.mutationId)).toEqual([mutationId]);
    storage.options.maxExports = undefined; // the app made room
    await drain(r);
    expect(r.engine.journalEntries()).toEqual([]);
    expect(r.engine.exportEntries().map((x) => x.mutationId)).toEqual([mutationId]);
    expect(types(r, mutationId).filter((t) => t === "outcome-unknown")).toHaveLength(1);
  });
});
