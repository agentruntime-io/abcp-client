// 14 §14.2 Prediction: the view of a keyspace is its confirmed state with the prediction of every pending action on top.

import { describe, expect, it } from "vitest";
import { cas, del, incr, jsonBytes, LocalRefusal, set, StaticManifests, type Predictor } from "../src/engine/index.js";
import { Code, ChangeKind } from "../src/gen/abcp/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { goOffline, goOnline, kc, kcCas, KS, ofType, rejectResult, rig, text, types } from "./helpers/engine.js";

describe("14 [CLI-6] the view is confirmed state plus the pending actions, in outbox order", () => {
  it("shows pending changes at once, in outbox order, and recomputes when confirmed state advances", async () => {
    const r = await rig();
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] });
    const v = r.engine.view(KS).get("settings/a");
    expect(text(v?.value)).toBe("2"); // the later action is on top
    expect(v?.pending).toEqual([a.mutationId, b.mutationId]);
    expect(v?.predicted).toBe(true);
    // confirmed state advances (another principal writes): the overlay is recomputed over it
    r.server.write(KS, [kc("settings/other", "set", "x")]);
    await goOnline(r);
    expect(text(r.server.value(KS, "settings/a"))).toBe("2");
    expect(r.engine.view(KS).get("settings/other")?.predicted).toBe(false);
    expect(r.engine.view(KS).get("settings/a")?.pending).toEqual([]);
  });
});

describe("14 [CLI-10] lww", () => {
  it("a pending SET shows its value, and a pending DELETE the key's absence, until the action retires", async () => {
    const r = await rig();
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/x", "v1")] });
    await r.engine.settle();
    await goOffline(r);
    expect(text(r.engine.view(KS).get("settings/x")?.value)).toBe('"v1"');
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/x", "v2")] });
    expect(text(r.engine.view(KS).get("settings/x")?.value)).toBe('"v2"');
    expect(r.engine.view(KS).json("settings/x")).toBe("v2");
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [del("settings/x")] });
    expect(r.engine.view(KS).get("settings/x")).toBeUndefined();
    expect(r.engine.confirmed(KS).get("settings/x")).toBeDefined();
    await goOnline(r);
    expect(r.engine.view(KS).get("settings/x")).toBeUndefined();
    expect(r.server.value(KS, "settings/x")).toBeUndefined();
  });
});

describe("14 [CLI-11] cas", () => {
  it("is shown as pending, and its value is predicted only if its expectation holds against the view below it", async () => {
    const r = await rig();
    r.server.write(KS, [kcCas("notes/n1", "base")]); // notes/{id} is a cas template: the record carries a CAS (05 [STATE-19])
    await goOffline(r);
    await r.clock.advance(0);
    await r.engine.settle();
    const hold = await r.engine.submit({ keyspaceId: KS, opType: "CasSet", changes: [cas("notes/n2", "mine", { absent: true })] });
    let v = r.engine.view(KS).get("notes/n2");
    expect(text(v?.value)).toBe('"mine"'); // absent holds: predicted
    expect(v?.pending).toEqual([hold.mutationId]);
    // an expectation that does not hold: shown as pending, with no predicted effect
    const bad = await r.engine.submit({ keyspaceId: KS, opType: "CasSet", changes: [cas("notes/n3", "mine", { value: jsonBytes("something else") })] });
    v = r.engine.view(KS).get("notes/n3");
    expect(v).toBeUndefined();
    expect(r.engine.view(KS).pending("notes/n3")).toEqual([bad.mutationId]);
  });

  it("a CAS that was predicted to have no effect and commits is `corrected`", async () => {
    const r = await rig();
    await r.engine.settle();
    // the CAS expects the value "y", and the key holds nothing: no predicted effect
    await goOffline(r);
    const c = await r.engine.submit({ keyspaceId: KS, opType: "CasSet", changes: [cas("notes/n1", "new", { value: jsonBytes("y") })] });
    expect(r.engine.view(KS).get("notes/n1")).toBeUndefined();
    // meanwhile another principal sets the value to "y", so the expectation holds when the action is sent
    r.server.write(KS, [kcCas("notes/n1", "y")]);
    r.server.faults.paused = true; // the record of the other write, and the one of this action, arrive together after the result
    await goOnline(r);
    r.server.resumeDelivery();
    await r.engine.settle();
    expect(types(r, c.mutationId)).toEqual(["corrected"]);
    expect(ofType(r, "corrected")[0]?.keys).toEqual(["notes/n1"]);
  });
});

describe("14 [CLI-12] counter", () => {
  it("the view of a counter is its confirmed value plus the deltas of every pending INCR", async () => {
    const r = await rig();
    r.server.write(KS, [kc("counters/c", "incr", 5)]);
    await r.engine.settle();
    await r.clock.advance(0);
    await goOffline(r);
    await r.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [incr("counters/c", 2)] });
    await r.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [incr("counters/c", 3)] });
    expect(r.engine.view(KS).json("counters/c")).toBe(5 + 2 + 3);
    // a counter that no state holds yet starts from 0
    await r.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [incr("counters/new", 4)] });
    expect(r.engine.view(KS).json("counters/new")).toBe(4);
  });
});

describe("14 [CLI-14] entities", () => {
  it("a pending DELETE of a marker key hides its subtree in the view", async () => {
    const r = await rig();
    await r.engine.submit({ keyspaceId: KS, opType: "Create", changes: [set("issues/i1", { n: 1 }), set("issues/i1/title", "t")] });
    await r.engine.settle();
    expect(r.engine.view(KS).has("issues/i1/title")).toBe(true);
    await goOffline(r);
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [del("issues/i1")] });
    expect(r.engine.view(KS).has("issues/i1")).toBe(false);
    expect(r.engine.view(KS).has("issues/i1/title")).toBe(false);
    expect(r.engine.view(KS).keys("issues/")).toEqual([]);
    expect(r.engine.confirmed(KS).has("issues/i1/title")).toBe(true);
  });
});

describe("14 [CLI-15] GATED actions", () => {
  it("predicts the requested changes as pending; the effects are placeholders, so a commit with effects is `corrected`", async () => {
    const r = await rig();
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/requested", 1)] });
    await r.engine.settle();
    expect(r.engine.view(KS).get("settings/requested")?.pending).toContain(a.mutationId);
    expect(text(r.engine.view(KS).get("settings/requested")?.value)).toBe("1");
    expect(r.engine.view(KS).has("settings/effect")).toBe(false); // a placeholder: nothing is predicted for the effect
    r.server.decide(KS, a.mutationId, { status: "COMMITTED", effects: [kc("settings/effect", "set", "e")] });
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["corrected"]);
    expect(ofType(r, "corrected")[0]?.keys).toEqual(["settings/effect"]);
  });

  it("with a predictor that was right, the commit is `committed`; with one that was wrong, `corrected`", async () => {
    const right: Predictor = () => [set("settings/effect", "e")];
    const r = await rig({ cfg: { predictors: { Approve: right } } });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/requested", 1)] });
    await r.engine.settle();
    expect(text(r.engine.view(KS).get("settings/effect")?.value)).toBe('"e"'); // the predictor's effect
    r.server.decide(KS, a.mutationId, { status: "COMMITTED", effects: [kc("settings/effect", "set", "e")] });
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["committed"]);

    const w = await rig({ cfg: { predictors: { Approve: () => [set("settings/effect", "predicted")] } } });
    const b = await w.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/requested", 1)] });
    await w.engine.settle();
    w.server.decide(KS, b.mutationId, { status: "COMMITTED", effects: [kc("settings/effect", "set", "actual")] });
    await w.engine.settle();
    expect(types(w, b.mutationId)).toEqual(["corrected"]);
  });
});

describe("14 [CLI-16] optimistic: false", () => {
  it("has no predicted values; the key is shown as pending until the action retires", async () => {
    const r = await rig();
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Silent", changes: [set("settings/s", "v")] });
    expect(r.engine.view(KS).get("settings/s")).toBeUndefined();
    expect(r.engine.view(KS).pending("settings/s")).toEqual([a.mutationId]);
    await goOnline(r);
    expect(text(r.engine.view(KS).get("settings/s")?.value)).toBe('"v"');
    expect(r.engine.view(KS).pending("settings/s")).toEqual([]);
  });
});

describe("14 [CLI-17] seen_version comes from the confirmed state, tombstones included", () => {
  it("sets the key's confirmed version, the deletion seq of a tombstone, or 0", async () => {
    const r = await rig();
    const s1 = r.server.write(KS, [kc("settings/live", "set", 1)]);
    const s2 = r.server.write(KS, [kc("settings/gone", "set", 1)]);
    const s3 = r.server.write(KS, [kc("settings/gone", "delete")]);
    expect(s2 < s3 && s1 < s2).toBe(true);
    await r.engine.settle();
    await r.clock.advance(0);
    await goOffline(r);
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/live", 2), set("settings/gone", 2), set("settings/never", 2)] });
    // a pending action never counts: a second action on the same key still takes the confirmed version
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/live", 3)] });
    const out = r.engine.outbox();
    const seen = (i: number, k: string): bigint => {
      const entry = out[i];
      if (entry === undefined) throw new Error("no entry");
      const m = (r.engine as unknown as { content(e: unknown): { changes: { key: string; seenVersion: bigint }[] } }).content(entry);
      return m.changes.find((c) => c.key === k)?.seenVersion ?? -1n;
    };
    expect(seen(0, "settings/live")).toBe(s1);
    expect(seen(0, "settings/gone")).toBe(s3);
    expect(seen(0, "settings/never")).toBe(0n);
    expect(seen(1, "settings/live")).toBe(s1);
  });
});

describe("14 [CLI-18] the base and the preconditions come from the confirmed watermark and epoch", () => {
  it("sets base_seq and base_log_epoch, the epoch of every precondition, and unchanged_since to the watermark", async () => {
    const r = await rig();
    r.server.write(KS, [kc("settings/a", "set", 1)]);
    r.server.write(KS, [kc("settings/b", "set", 1)]);
    await r.engine.settle();
    await r.clock.advance(0);
    await goOffline(r);
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/c", 1)], preconditions: [{ key: "settings/a", unchangedSince: "confirmed" }, { prefix: "issues/", unchangedSince: 1n }] });
    const entry = r.engine.outbox()[0];
    const m = (r.engine as unknown as { content(e: unknown): { baseSeq: bigint; baseLogEpoch: number; preconditions: { logEpoch: number; check: { case: string; value: bigint } }[] } }).content(entry);
    expect(m.baseSeq).toBe(r.engine.cursor(KS)?.seq);
    expect(m.baseSeq).toBeGreaterThan(0n);
    expect(m.baseLogEpoch).toBe(1);
    expect(m.preconditions.map((p) => p.logEpoch)).toEqual([1, 1]);
    expect(m.preconditions[0]?.check).toMatchObject({ case: "unchangedSince", value: m.baseSeq });
    expect(m.preconditions[1]?.check).toMatchObject({ case: "unchangedSince", value: 1n });
  });

  it("an empty keyspace that was loaded gives watermark 0 and the keyspace's epoch, not 'not given'", async () => {
    const r = await rig();
    await goOffline(r);
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/c", 1)] });
    const m = (r.engine as unknown as { content(e: unknown): { baseSeq: bigint; baseLogEpoch: number } }).content(r.engine.outbox()[0]);
    expect(m.baseSeq).toBe(0n);
    expect(m.baseLogEpoch).toBe(1);
  });
});

describe("14 [CLI-19] a local check against the manifest", () => {
  it("refuses an action that is certain to be dead-lettered, and stores nothing", async () => {
    const r = await rig();
    const refused = async (input: Parameters<typeof r.engine.submit>[0]): Promise<string> => {
      try {
        await r.engine.submit(input);
      } catch (e) {
        expect(e).toBeInstanceOf(LocalRefusal);
        return (e as LocalRefusal).code;
      }
      return "accepted";
    };
    expect(await refused({ keyspaceId: KS, opType: "NoSuchOp", changes: [set("settings/a", 1)] })).toBe("INVALID");
    expect(await refused({ keyspaceId: KS, opType: "Set", changes: [set("nowhere/a", 1)] })).toBe("INVALID"); // no template
    expect(await refused({ keyspaceId: KS, opType: "Set", changes: [incr("settings/a", 1)] })).toBe("INVALID"); // INCR on an lww key
    expect(await refused({ keyspaceId: KS, opType: "Writes", changes: [set("raw/x", "v")] })).toBe("INVALID"); // outside `writes`
    expect(await refused({ keyspaceId: KS, opType: "Set", changes: [] })).toBe("INVALID");
    expect(await refused({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1), set("settings/a", 2)] })).toBe("INVALID");
    expect(await refused({ keyspaceId: "other", opType: "Set", changes: [set("settings/a", 1)] })).toBe("NOT_FOLLOWED"); // 14 [CLI-69]
    expect(r.engine.outbox()).toHaveLength(0);
    expect(await refused({ keyspaceId: KS, opType: "Writes", changes: [set("settings/a", 1)] })).toBe("accepted");
  });
});

describe("14 [CLI-12] the view of a counter is its confirmed value plus the deltas of every pending INCR (Q42.4, ruled)", () => {
  const ranged = (): StaticManifests =>
    new StaticManifests({
      version: 1,
      templates: { "counters/{id}": { rule: "counter", range: { min: 0n, max: 10n } }, "settings/{name}": { rule: "lww" } },
      opTypes: { Incr: {}, Set: {} },
    });

  it("an INCR that would leave the template's range is part of the sum, until the server's RANGE_EXCEEDED ends the action", async () => {
    const r = await rig({ manifests: ranged() });
    r.server.write(KS, [kc("counters/c", "incr", 8)]);
    await r.engine.settle();
    await goOffline(r);
    await r.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [incr("counters/c", 1)] }); // 9: in range
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [incr("counters/c", 5)] }); // 14: leaves the range
    await r.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [incr("counters/c", -2)] }); // 12
    expect(r.engine.view(KS).json("counters/c")).toBe(8 + 1 + 5 - 2); // the plain sum, not 8 + 1 - 2
    expect(r.engine.view(KS).pending("counters/c")).toHaveLength(3);
    // the server applies them in order: 9, then RANGE_EXCEEDED for the second (9 + 5 > 10, 05 [STATE-28]: none of its changes), and the
    // third depends on it (the same key) and fails with it ([CLI-34]). The fake server does not judge ranges, so the refusal is scripted.
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.mutationId === b.mutationId ? rejectResult(m, mk(Code.RANGE_EXCEEDED)) : undefined) });
    await goOnline(r);
    await r.engine.settle();
    expect(types(r, b.mutationId)).toEqual(["conflict", "rejected"]); // RANGE_EXCEEDED is class CONFLICT
    expect(r.engine.confirmed(KS).json("counters/c")).toBe(9);
    expect(r.engine.view(KS).json("counters/c")).toBe(9); // the confirmed value plus the deltas that remain: none
    expect(r.engine.view(KS).pending("counters/c")).toHaveLength(0);
  });

  it("a counter that has no confirmed value starts from 0, and the sum may leave the range of its template", async () => {
    const r = await rig({ manifests: ranged() });
    await goOffline(r);
    await r.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [incr("counters/new", 50)] }); // far above the range: still the sum
    expect(r.engine.view(KS).json("counters/new")).toBe(50);
  });
});

describe("14 [CLI-20] prediction is total", () => {
  it("data that the server accepted never makes prediction throw", async () => {
    const r = await rig();
    // a counter at its largest value: an INCR leaves its range, which the server answers with RANGE_EXCEEDED
    r.server.write(KS, [kc("counters/max", "incr", Number.MAX_SAFE_INTEGER)]);
    await r.engine.settle();
    await r.clock.advance(0);
    await goOffline(r);
    await r.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [incr("counters/max", 1)] });
    expect(() => r.engine.view(KS).get("counters/max")).not.toThrow();
    // 14 [CLI-12] (Q42.4, ruled): the view is the confirmed value plus the delta, an INCR that leaves the range included
    expect(r.engine.view(KS).json("counters/max")).toBe(Number.MAX_SAFE_INTEGER + 1);
    expect(r.engine.view(KS).pending("counters/max")).toHaveLength(1);
    // a SET over a key that holds a value of another encoding, and a predictor that throws
    const w = await rig({ cfg: { predictors: { Approve: () => { throw new Error("boom"); } } } });
    await w.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/a", 1)] });
    expect(() => w.engine.view(KS).get("settings/a")).not.toThrow();
    expect(w.engine.internalErrors).toEqual([]);
  });
});

describe("14 [CLI-11] cas: an expectation by version (the default of the `cas` builder) is read from the confirmed state, and holds against the view below it", () => {
  it("a pending CAS whose version is the confirmed one is predicted; one behind another pending action on the key is not, since that action's version does not exist yet", async () => {
    const r = await rig();
    r.server.write(KS, [kcCas("notes/n1", "base")]);
    await r.engine.settle();
    await goOffline(r);
    const first = await r.engine.submit({ keyspaceId: KS, opType: "CasSet", changes: [cas("notes/n1", "first")] }); // version 1: the confirmed one
    expect(text(r.engine.view(KS).get("notes/n1")?.value)).toBe('"first"');
    expect(r.engine.view(KS).get("notes/n1")?.predicted).toBe(true);
    const second = await r.engine.submit({ keyspaceId: KS, opType: "CasSet", changes: [cas("notes/n1", "second")] }); // version 1 again: it depends on the first, and cannot hold behind it
    expect(text(r.engine.view(KS).get("notes/n1")?.value)).toBe('"first"');
    expect(r.engine.view(KS).get("notes/n1")?.pending).toEqual([first.mutationId, second.mutationId]);
  });
});
