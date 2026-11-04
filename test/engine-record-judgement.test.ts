// Round 19 (spec draft 17), 16 [SEC-3], 17 [CONF-22] (the client case, m5): what a client engine judges a record against.
//
//   "A client engine judges a record against the envelope rules, the segment syntax, the reserved namespace and the `json/v1` limits of 05 [STATE-7] always, against the templates and value schemas of
//   the manifest version that the record carries, and never against the app's limits in force (07 §7.9): a record that the server appended is not structurally invalid for a limit that was lowered later
//   (04 [MAN-75])."
//
//   CONF-22: "a client engine judges a record that the server appended, whose size is above an app limit that was lowered later (04 [MAN-75]), by the rules of 16 [SEC-3] alone, so that the record is
//   applied and no hold or `skipped` event follows."
//
// The fold of the client reads no limit of the app (`rules/fold/apply.ts`, `judge.ts`); these are the cases that the suite asks for. The records that break the structural rules are tested in
// engine-record-structure.test.ts and engine-c16c.test.ts (the hold PROTOCOL_ERROR).

import { describe, expect, it } from "vitest";
import { LocalRefusal, set, StaticManifests } from "../src/engine/index.js";
import { goOffline, goOnline, kc, KS, MANIFEST, ofType, rig, tick, type Rig } from "./helpers/engine.js";

const KiB = 1024;

/** Everything that would show a record judged and refused or half applied: a hold, a protocol error, a skipped change. */
const trouble = (r: Rig): string[] => [...ofType(r, "held").map((e) => `held ${e.code}`), ...ofType(r, "protocol-error").map((e) => `protocol-error ${e.what}`), ...ofType(r, "skipped").map((e) => `skipped ${e.cause ?? ""}`)];

/** 150 small changes and one value of 200 KiB: over every limit that the test lowers below. */
const bigRecord = (tag: string): ReturnType<typeof kc>[] => [kc(`settings/big-${tag}`, "set", "x".repeat(200 * KiB)), ...Array.from({ length: 150 }, (_, i) => kc(`settings/${tag}-${i}`, "set", i))];

/** An engine that follows the keyspace under the limits that were in force when the big records were appended; the server then lowers them, and the engine learns the lowered ones. */
async function afterLowering(): Promise<Rig> {
  const r = await rig({ serverOptions: { limits: { maxValueBytes: 1 << 20, maxActionBytes: 2 << 20, maxChangesPerAction: 1000, maxRequestBytes: 4 << 20 } } });
  r.server.write(KS, bigRecord("first"));
  await r.engine.settle();
  expect(r.engine.confirmed(KS).has("settings/big-first")).toBe(true);
  // the app lowers its limits to their floors (07 §7.9, 04 [MAN-75]); a session that starts after it advertises them
  r.server.limits.maxValueBytes = 64 * KiB;
  r.server.limits.maxActionBytes = 256 * KiB;
  r.server.limits.maxChangesPerAction = 100;
  r.server.reconnect();
  await r.engine.settle();
  await tick(r, 40_000); // a session that lives under a minute reconnects with a growing backoff, 30 s at most (09 [READ-57])
  expect([r.engine.limits.maxValueBytes, r.engine.limits.maxActionBytes, r.engine.limits.maxChangesPerAction]).toEqual([64 * KiB, 256 * KiB, 100]);
  return r;
}

describe("16 [SEC-3], 17 [CONF-22]: a record that the server appended is judged by the rules of SEC-3 alone, never by an app limit that was lowered later", () => {
  it("a record over max_value_bytes, max_action_bytes and max_changes_per_action in force, delivered on the live stream, is applied: the state holds every change, and no hold, protocol-error or skipped follows", async () => {
    const r = await afterLowering();
    const seq = r.server.write(KS, bigRecord("live"));
    await r.engine.settle();
    expect(r.engine.confirmed(KS).has("settings/big-live")).toBe(true);
    expect(r.engine.confirmed(KS).has("settings/live-149")).toBe(true);
    expect((r.engine.confirmed(KS).json("settings/big-live") as string).length).toBe(200 * KiB);
    expect(r.engine.cursor(KS)?.seq).toBe(seq);
    expect(r.engine.status().holds).toEqual([]);
    expect(r.engine.status().skippedChanges).toBe(0);
    expect(trouble(r)).toEqual([]);
  });

  it("the same record delivered by a resume after the engine was offline is applied too (the catch-up of 09 [READ-16])", async () => {
    const r = await afterLowering();
    await goOffline(r);
    r.server.write(KS, bigRecord("late"));
    await goOnline(r);
    await tick(r, 40_000);
    expect(r.engine.confirmed(KS).has("settings/big-late")).toBe(true);
    expect(r.engine.confirmed(KS).has("settings/late-149")).toBe(true);
    expect(r.engine.status().holds).toEqual([]);
    expect(trouble(r)).toEqual([]);
  });

  it("the app's limits in force still bound what the engine authors: an action over them is refused locally, while a record over them is applied", async () => {
    const r = await afterLowering();
    await expect(r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/mine", "y".repeat(70 * KiB))] })).rejects.toBeInstanceOf(LocalRefusal);
    await expect(r.engine.submit({ keyspaceId: KS, opType: "Set", changes: Array.from({ length: 101 }, (_, i) => set(`settings/m${i}`, i)) })).rejects.toBeInstanceOf(LocalRefusal);
    r.server.write(KS, bigRecord("other"));
    await r.engine.settle();
    expect(r.engine.confirmed(KS).has("settings/big-other")).toBe(true);
    expect(trouble(r)).toEqual([]);
  });
});

describe("16 [SEC-3]: a record is judged against the templates of the manifest version that it carries, not those of the version in force", () => {
  const v1 = { ...MANIFEST, version: 1, templates: { ...MANIFEST.templates, "old/{id}": { rule: "lww" as const } } };
  const v2 = { ...MANIFEST, version: 2 }; // version 2 has no template for `old/{id}`

  it("a record under version 1 that changes a key of a template that only version 1 declares is applied after version 2 is in force; a record under version 2 that changes it stops the keyspace", async () => {
    const manifests = new StaticManifests(v1, v2);
    const r = await rig({ manifests });
    r.server.write(KS, [kc("old/a", "set", 1)]); // appended under version 1
    await r.engine.settle();
    r.server.activateManifest(KS, 2);
    await r.engine.settle();
    expect(r.engine.confirmed(KS).has("old/a")).toBe(true);
    expect(trouble(r)).toEqual([]);
    // a record of version 2, in which no template matches the key: structurally invalid under the version that the record carries (03 [ID-19])
    r.server.appendRaw(KS, { changes: [kc("old/b", "set", 2)] }); // the fake server's own fold is not asked to apply it
    await r.engine.settle();
    expect(r.engine.confirmed(KS).has("old/b")).toBe(false);
    expect(ofType(r, "protocol-error").map((e) => e.what)).toEqual(["RecordsFrame"]);
    expect(r.engine.status().holds.map((h) => [h.keyspaceId, h.code])).toEqual([[KS, "PROTOCOL_ERROR"]]);
  });
});
