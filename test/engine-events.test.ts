// 14 §14.5 Events.

import { describe, expect, it } from "vitest";
import { MemoryStorage, set } from "../src/engine/index.js";
import { Code, RetryReason } from "../src/gen/abpc/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { goOffline, goOnline, kc, KS, ofType, rejectResult, retryResult, rig, tick, types } from "./helpers/engine.js";

describe("14 [CLI-38] exactly one of committed, corrected or rejected for each action, in the order that the text gives", () => {
  it("a retired action gets `committed`", async () => {
    const r = await rig();
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["committed"]);
    expect(ofType(r, "committed")[0]).toMatchObject({ mutationId: a.mutationId, keyspaceId: KS, seq: 1n });
  });

  it("a DEAD_LETTER result: `rejected`, then `dead-lettered`", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, mk(Code.SCHEMA_INVALID)) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(ofType(r, "rejected")[0]?.reject?.code).toBe(Code.SCHEMA_INVALID);
    expect(ofType(r, "dead-lettered")[0]?.reason).toBe("SCHEMA_INVALID");
  });

  it("a CONFLICT result: `conflict`, the handler, then `rejected`; with no handler `rejected` follows `conflict` at once", async () => {
    const seen: string[] = [];
    const r = await rig({ cfg: { conflictHandler: () => void seen.push("handler") } });
    r.engine.on((e) => seen.push(e.type));
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, mk(Code.STALE, { stale: { cause: 1, currentSeq: 0n } })) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["conflict", "rejected"]);
    expect(seen.filter((x) => x === "handler" || x === "conflict" || x === "rejected")).toEqual(["conflict", "handler", "rejected"]);

    const w = await rig();
    w.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, mk(Code.STALE, { stale: { cause: 1, currentSeq: 0n } })) });
    const b = await w.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await w.engine.settle();
    expect(types(w, b.mutationId)).toEqual(["conflict", "rejected"]);
  });

  it("an OFFLINE_TOO_LONG result for an action that was sent is outcome unknown: `outcome-unknown`, then `exported`", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, mk(Code.OFFLINE_TOO_LONG, { details: { cause: "SUPPORT_WINDOW" } })) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["outcome-unknown", "exported"]);
    expect(ofType(r, "outcome-unknown")[0]?.cause).toBe("SUPPORT_WINDOW");
    expect(r.engine.status().deadLetters).toBe(0);
    expect(r.engine.status().exports).toBe(1);
  });
});

describe("14 [CLI-39] a predicted value changes only with an event for the action", () => {
  it("a Retry changes nothing on screen; the final rejection gives `rejected` at the moment the value reverts", async () => {
    const r = await rig();
    const shown: (string | undefined)[] = [];
    r.engine.on((e) => {
      if (e.type === "rejected") shown.push(new TextDecoder().decode(r.engine.view(KS).get("settings/a")?.value ?? new Uint8Array()) || undefined);
    });
    r.server.inject("mutate", { kind: "result", result: (m) => retryResult(m, RetryReason.STORAGE_UNAVAILABLE, 1000) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", "mine")] });
    await r.engine.settle();
    expect(new TextDecoder().decode(r.engine.view(KS).get("settings/a")?.value)).toBe('"mine"'); // still predicted: no event
    expect(types(r)).not.toContain("rejected");
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, mk(Code.SCHEMA_INVALID)) });
    await tick(r, 5000);
    expect(types(r, a.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(shown).toEqual([undefined]); // at `rejected` the prediction is already gone
  });
});

describe("14 [CLI-40] `overwritten` once per mutation id", () => {
  it("for the client's own action, from the first of the result and the record, and not again when the record is delivered again", async () => {
    const r = await rig();
    await goOffline(r);
    // the action is authored against a key that does not exist yet; another principal then writes it
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", "mine")] });
    r.server.write(KS, [kc("settings/a", "set", "theirs")]);
    r.server.faults.duplicateNext = 1;
    await goOnline(r);
    const ow = ofType(r, "overwritten").filter((e) => e.mutationId === a.mutationId);
    expect(ow).toHaveLength(1);
    expect(ow[0]?.keys).toEqual(["settings/a"]);
    expect(types(r, a.mutationId)).toEqual(["overwritten", "committed"]);
  });

  it("for another principal's action, from the record, listing every index", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/a", "set", 1), kc("settings/b", "set", 1), kc("settings/c", "set", 1)], { sub: "bob", mutationId: "bob-1", overwroteNewer: [0, 2] });
    await r.engine.settle();
    await tick(r, 15_000);
    const ow = ofType(r, "overwritten");
    expect(ow).toHaveLength(1);
    expect(ow[0]).toMatchObject({ mutationId: "bob-1", keys: ["settings/a", "settings/c"], by: "bob" });
  });
});

describe("14 [CLI-41] the sync status is readable state, and unacknowledged events survive a restart", () => {
  it("reports pending, dead letters, exports, rejections, cursors, storage mode, connection and holds", async () => {
    const r = await rig();
    expect(r.engine.status()).toMatchObject({ pending: 0, deadLetters: 0, exports: 0, skippedChanges: 0, loadsWithSkipped: 0, unacknowledgedRejections: 0, storageMode: "memory", connection: "live", suspended: false, holds: [], fullStores: [] });
    expect(r.engine.status().cursors[KS]).toEqual({ seq: 0n, logEpoch: 1 });
    await goOffline(r);
    expect(r.engine.status().connection).toBe("offline");
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    expect(r.engine.status().pending).toBe(1);
  });

  it("an unacknowledged rejection, a dead-letter entry and an export-hook entry are derivable from the state after a restart", async () => {
    const storage = new MemoryStorage();
    const r = await rig({ storage });
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, mk(Code.SCHEMA_INVALID)) });
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, mk(Code.OFFLINE_TOO_LONG, { details: { cause: "HORIZON" } })) });
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
    await r.engine.settle();
    expect(r.engine.status()).toMatchObject({ deadLetters: 1, exports: 1, unacknowledgedRejections: 1 });
    await r.engine.stop();
    const r2 = await rig({ storage, server: r.server, clock: r.clock, seed: 5, start: false });
    expect(r2.engine.status()).toMatchObject({ deadLetters: 1, exports: 1, unacknowledgedRejections: 1 });
  });
});
