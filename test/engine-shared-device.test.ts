// 14 [CLI-59] (V14 S3): on a shared device the partition is the device's, and the confirmed replica, the rejections and the drafts of one principal are
// never shown to the next; 14 [CLI-28], [CLI-65], [CLI-77] (V14 S2): a sign-out that purges the partition hands the app the entries that it has not
// acknowledged first.

import { describe, expect, it } from "vitest";
import * as Y from "yjs";
import { crdtUpdate, draftKey, set, type ExportRecord } from "../src/engine/index.js";
import { Code, StaleCause } from "../src/gen/abpc/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { kc, KS, rejectResult } from "./helpers/engine.js";
import { settleAll, tabsRig, typesOf } from "./helpers/tabs.js";

const setA = (key = "settings/a", v: unknown = 1): { keyspaceId: string; opType: string; changes: ReturnType<typeof set>[] } => ({ keyspaceId: KS, opType: "Set", changes: [set(key, v)] });
const shared = { mode: "shared-device" } as const;
const stale = (cause: StaleCause, key: string): ReturnType<typeof mk> => mk(Code.STALE, { stale: { cause, key, currentSeq: 0n } });

describe("14 [CLI-59]: a separate confirmed replica, rejections and drafts per principal, on a shared device (V14 S3)", () => {
  it("the cache of the principal that used the device is not the next principal's: it is not restored, and goes", async () => {
    const r = tabsRig();
    const till1 = await r.open({ sub: "till-1", deviceId: "till", cfg: shared });
    r.server.write(KS, [kc("settings/s", "set", "till-1 saw this")]);
    await settleAll(till1.engine);
    const storage = r.device.storage.storageOf(till1.engine.partitionKey());
    expect(storage.replica(KS)?.head.author?.sub).toBe("till-1");
    await till1.engine.stop();
    const till2 = await r.open({ sub: "till-2", deviceId: "till", cfg: shared, noAdd: true, start: false });
    expect(till2.engine.cursor(KS)?.loaded).not.toBe(true); // nothing of till-1's copy is till-2's
    expect(till2.engine.confirmed(KS).has("settings/s")).toBe(false);
    expect(storage.replica(KS)).toBeUndefined(); // the other principal's cache is gone
    till2.engine.start();
    await settleAll(till2.engine);
    expect(till2.engine.confirmed(KS).json("settings/s")).toBe("till-1 saw this"); // its own load
    expect(storage.replica(KS)?.head.author?.sub).toBe("till-2");
    expect(till2.events.filter((e) => e.type === "reloaded").map((e) => (e as { cause: string }).cause)).toEqual(["COLD_LOAD"]);
  });

  it("an unacknowledged rejection and a draft are their author's: the next principal has none, and the author finds them again", async () => {
    const r = tabsRig();
    const till1 = await r.open({ sub: "till-1", deviceId: "till", cfg: { ...shared, conflictHandler: () => undefined } });
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.changes[0]?.key === "settings/bad" ? rejectResult(m, stale(StaleCause.VERSION_MISMATCH, "settings/bad")) : undefined) });
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.changes[0]?.key === "docs/d1" ? rejectResult(m, stale(StaleCause.DOC_RESET, "docs/d1")) : undefined) });
    const bad = await till1.engine.submit(setA("settings/bad"));
    const doc = new Y.Doc();
    doc.getText("text").insert(0, "till-1's words");
    await till1.engine.submit({ keyspaceId: KS, opType: "DocEdit", changes: [crdtUpdate("docs/d1", Y.encodeStateAsUpdate(doc))] });
    await settleAll(till1.engine);
    expect(till1.engine.rejectionEntries().map((x) => x.mutationId)).toContain(bad.mutationId);
    expect(till1.engine.rejectionEntries().every((x) => x.author?.sub === "till-1")).toBe(true);
    expect(till1.engine.draftEntries()).toHaveLength(1);
    expect(till1.engine.draftEntries()[0]?.author?.sub).toBe("till-1"); // 14 [CLI-59]: tagged with its author
    await till1.engine.stop();
    const till2 = await r.open({ sub: "till-2", deviceId: "till", cfg: shared, start: false });
    expect(till2.engine.rejectionEntries()).toEqual([]);
    expect(till2.engine.status().unacknowledgedRejections).toBe(0);
    expect(till2.engine.draftEntries()).toEqual([]);
    await till2.engine.stop();
    const again = await r.open({ sub: "till-1", deviceId: "till", cfg: shared, start: false });
    expect(again.engine.rejectionEntries().map((x) => x.mutationId)).toContain(bad.mutationId);
    expect(again.engine.draftEntries()).toHaveLength(1);
  });

  it("two principals' drafts of one key are two drafts", () => {
    const a = { issuer: "i", sub: "till-1", deviceId: "till" };
    const b = { issuer: "i", sub: "till-2", deviceId: "till" };
    expect(draftKey({ keyspaceId: KS, key: "docs/d1", author: a })).not.toBe(draftKey({ keyspaceId: KS, key: "docs/d1", author: b }));
    expect(draftKey({ keyspaceId: KS, key: "docs/d1", author: a })).toBe(draftKey({ keyspaceId: KS, key: "docs/d1", author: { ...a, deviceId: "other" } }));
  });
});

describe("14 [CLI-28], [CLI-65]: a sign-out that purges the partition hands the app what it has not acknowledged first (V14 S2)", () => {
  it("an unacknowledged dead-letter entry and export-hook entry are handed over once more, with `dead-lettered`, `exported` and the hook, and then go with the partition", async () => {
    const hook: ExportRecord[] = [];
    const r = tabsRig();
    const a = await r.open({ cfg: { exportHook: (x) => void hook.push(x) } });
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.changes[0]?.key === "settings/dead" ? rejectResult(m, mk(Code.SCHEMA_INVALID)) : undefined) });
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.changes[0]?.key === "settings/old" ? rejectResult(m, mk(Code.OFFLINE_TOO_LONG, { details: { cause: "HORIZON" } })) : undefined) });
    const dead = await a.engine.submit(setA("settings/dead"));
    const old = await a.engine.submit(setA("settings/old"));
    await settleAll(a.engine);
    expect(a.engine.deadLetterEntries().map((x) => x.mutationId)).toEqual([dead.mutationId]);
    expect(a.engine.exportEntries().map((x) => x.mutationId)).toEqual([old.mutationId]);
    expect(typesOf(a, dead.mutationId).filter((t) => t === "dead-lettered")).toHaveLength(1);
    expect(hook).toHaveLength(1);
    const res = await a.engine.signOut();
    expect(res).toEqual({ done: true, outcome: "purged", unsynced: 0 });
    expect(typesOf(a, dead.mutationId).filter((t) => t === "dead-lettered")).toHaveLength(2); // once more, before the purge
    expect(typesOf(a, old.mutationId).filter((t) => t === "exported")).toHaveLength(2);
    expect(hook.map((x) => x.mutationId)).toEqual([old.mutationId, old.mutationId]);
    expect(await r.device.storage.list("https://abpc.test", "app")).toEqual([]);
  });
});
