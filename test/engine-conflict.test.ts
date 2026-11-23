// 14 §14.6.4 CONFLICT and STALE causes.

import { describe, expect, it } from "vitest";
import { create } from "@bufbuild/protobuf";
import * as Y from "yjs";
import { del, set, crdtUpdate, type ConflictContext } from "../src/engine/index.js";
import { PrincipalSchema, Code, StaleCause, StaleSchema, type Reject } from "../src/gen/abcp/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { goOffline, goOnline, kc, KS, ofType, rejectResult, rig, sentIds, tick, types, type Rig } from "./helpers/engine.js";

function stale(cause: StaleCause, extra: { key?: string; currentSeq?: bigint; details?: Record<string, string>; holder?: string } = {}): Reject {
  const r = mk(Code.STALE, { stale: { cause, key: extra.key ?? "settings/a", currentSeq: extra.currentSeq ?? 0n }, ...(extra.details !== undefined ? { details: extra.details } : {}) });
  if (extra.holder !== undefined && r.stale !== undefined) r.stale.holder = create(PrincipalSchema, { sub: extra.holder });
  return r;
}

function injectFor(r: Rig, key: string, reject: Reject): void {
  r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.changes[0]?.key === key ? rejectResult(m, reject) : undefined) });
}

describe("14 [CLI-47] a CONFLICT-class result goes to the conflict handler", () => {
  it("calls the handler with the action, the Reject and the current confirmed state; the action's changes are not applied", async () => {
    const calls: ConflictContext[] = [];
    const r = await rig({ cfg: { conflictHandler: (c) => void calls.push(c) } });
    injectFor(r, "settings/a", stale(StaleCause.VERSION_MISMATCH));
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", "mine")] });
    await r.engine.settle();
    expect(calls).toHaveLength(1);
    expect(calls[0]?.action.mutationId).toBe(a.mutationId);
    expect(calls[0]?.action.opType).toBe("Set");
    expect(calls[0]?.action.changes.map((c) => c.key)).toEqual(["settings/a"]);
    expect(calls[0]?.reject.code).toBe(Code.STALE);
    expect(calls[0]?.stale?.cause).toBe(StaleCause.VERSION_MISMATCH);
    expect(calls[0]?.confirmed.has("settings/a")).toBe(false);
    expect(r.engine.view(KS).get("settings/a")).toBeUndefined(); // the view shows the latest confirmed values
    expect(types(r, a.mutationId)).toEqual(["conflict", "rejected"]);
  });

  it("waits until the cursor is at Stale.current_seq (the hold CONFLICT_WAIT) so that the handler sees the state that caused the conflict", async () => {
    const calls: { value: string | undefined }[] = [];
    const r = await rig({ cfg: { conflictHandler: (c) => void calls.push({ value: c.confirmed.json("settings/a") as string | undefined }) } });
    r.server.faults.paused = true;
    const seq = r.server.write(KS, [kc("settings/a", "set", "theirs")]);
    injectFor(r, "settings/a", stale(StaleCause.VERSION_MISMATCH, { currentSeq: seq }));
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", "mine")] });
    await r.engine.settle();
    expect(calls).toHaveLength(0);
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "action", code: "CONFLICT_WAIT", mutationId: a.mutationId })]);
    expect(types(r, a.mutationId)).toEqual(["conflict", "held"]);
    r.server.resumeDelivery();
    await r.engine.settle();
    expect(calls).toEqual([{ value: "theirs" }]);
    expect(r.events.filter((e) => e.type === "held").map((e) => (e.type === "held" ? [e.state, e.code] : []))).toEqual([["start", "CONFLICT_WAIT"], ["stop", "CONFLICT_WAIT"]]);
    expect(types(r, a.mutationId)).toEqual(["conflict", "held", "held", "rejected"]);
  });

  it("at the default bound the handler is called with the confirmed state that the client holds, whatever its cursor", async () => {
    let called = 0;
    const r = await rig({ serverOptions: { limits: { maxOfflineSeconds: 120 } }, cfg: { conflictHandler: () => void called++ } });
    r.server.faults.paused = true;
    const seq = r.server.write(KS, [kc("settings/a", "set", "theirs")]);
    injectFor(r, "settings/a", stale(StaleCause.VERSION_MISMATCH, { currentSeq: seq }));
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", "mine")] });
    await r.engine.settle();
    expect(called).toBe(0);
    await tick(r, 130_000);
    expect(called).toBe(1);
    expect(types(r, a.mutationId)).toEqual(["conflict", "held", "held", "rejected"]); // the action keeps its final rejection
  });

  it("the user may keep theirs (nothing), or keep mine or merge: a new action with a fresh mutation id, authored only because the handler chose it", async () => {
    let fresh = "";
    const r = await rig({
      cfg: {
        conflictHandler: async (c) => {
          const r2 = await c.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", "merged")] });
          fresh = r2.mutationId;
        },
      },
    });
    injectFor(r, "settings/a", stale(StaleCause.VERSION_MISMATCH));
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", "mine")] });
    await r.engine.settle();
    await tick(r, 3000);
    expect(fresh).not.toBe("");
    expect(fresh).not.toBe(a.mutationId);
    expect(types(r, a.mutationId)).toEqual(["conflict", "rejected"]);
    expect(types(r, fresh)).toEqual(["committed"]); // its own events (14 [CLI-23])
    expect(sentIds(r).filter((id) => id === a.mutationId)).toHaveLength(1); // the old one is never resent (14 [CLI-48])
  });

  it("replace: a new action that the old one's dependents move to; without a replacement they fail", async () => {
    const r = await rig({
      cfg: {
        conflictHandler: async (c) => {
          await c.replaceWith({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", "replacement")] });
        },
      },
    });
    await goOffline(r);
    injectFor(r, "settings/a", stale(StaleCause.VERSION_MISMATCH));
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] }); // depends on a
    await goOnline(r);
    await tick(r, 5000);
    expect(types(r, a.mutationId)).toEqual(["conflict", "rejected"]);
    expect(types(r, b.mutationId)).toContain("committed"); // b moved to the replacement, and was not failed
    expect(r.engine.deadLetterEntries()).toHaveLength(0);
    expect(r.server.value(KS, "settings/a")).toEqual(new TextEncoder().encode("2"));

    const w = await rig();
    await goOffline(w);
    injectFor(w, "settings/a", stale(StaleCause.VERSION_MISMATCH));
    const c = await w.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const d = await w.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 2)] });
    await goOnline(w);
    expect(types(w, d.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(w.engine.deadLetterEntries()[0]?.dependencyOf).toBe(c.mutationId);
  });

  it("a handler that throws has chosen nothing: the action keeps its final rejection, and `rejected` follows", async () => {
    const r = await rig({ cfg: { conflictHandler: () => { throw new Error("boom"); } } });
    injectFor(r, "settings/a", stale(StaleCause.VERSION_MISMATCH));
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["conflict", "rejected"]);
    expect(r.engine.internalErrors).toEqual([]);
  });

  it("[CLI-48] never resends a CONFLICT-class action with the same mutation id", async () => {
    const r = await rig();
    injectFor(r, "settings/a", stale(StaleCause.OVERRIDDEN));
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 600_000);
    expect(sentIds(r)).toEqual([a.mutationId]);
  });
});

describe("14 [CLI-49] the default reaction of each STALE cause", () => {
  const causes: [string, StaleCause | number][] = [
    ["VERSION_MISMATCH", StaleCause.VERSION_MISMATCH],
    ["PARENT_DELETED", StaleCause.PARENT_DELETED],
    ["MOVING", StaleCause.MOVING],
    ["LEASE", StaleCause.LEASE],
    ["OVERRIDDEN", StaleCause.OVERRIDDEN],
    ["REDACTED", StaleCause.REDACTED],
    ["DOC_RESET", StaleCause.DOC_RESET],
    ["EPOCH", StaleCause.EPOCH],
    ["an unknown cause (15 [COMPAT-41])", 99],
  ];
  it.each(causes)("%s: a visible conflict through the handler, with the events of 14 [CLI-38], never resent and never re-authored", async (_name, cause) => {
    const seen: ConflictContext[] = [];
    const r = await rig({ cfg: { conflictHandler: (c) => void seen.push(c) } });
    injectFor(r, "settings/a", stale(cause as StaleCause, { holder: "ana" }));
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    await tick(r, 600_000);
    expect(seen).toHaveLength(1);
    expect(seen[0]?.stale?.cause).toBe(cause);
    if (cause === StaleCause.LEASE) expect(seen[0]?.stale?.holder?.sub).toBe("ana"); // a visible conflict naming the holder
    expect(types(r, a.mutationId)).toEqual(["conflict", "rejected"]);
    expect(sentIds(r)).toEqual([a.mutationId]);
    expect(r.engine.outbox()).toHaveLength(0);
  });

  it("PARENT_DELETED: dependents fail, details.moved_to reaches the handler, and while the entity stays deleted nothing is authored under it", async () => {
    const seen: ConflictContext[] = [];
    const r = await rig({ cfg: { conflictHandler: (c) => void seen.push(c) } });
    await r.engine.submit({ keyspaceId: KS, opType: "Create", changes: [set("issues/i1", {})] });
    await r.engine.settle();
    r.server.write(KS, [kc("issues/i1", "delete")]); // another principal deletes the entity
    await r.engine.settle();
    await tick(r, 15_000);
    await goOffline(r);
    injectFor(r, "issues/i1/title", stale(StaleCause.PARENT_DELETED, { details: { moved_to: "ks9/issues/i1" } }));
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/ok", 1)] });
    void a;
    // the entity is deleted in the confirmed state: the client refuses to author a write under it
    await expect(r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("issues/i1/title", "again")] })).rejects.toMatchObject({ name: "LocalRefusal", code: "REFUSED" });
    // the server's own PARENT_DELETED (the client had not heard yet) is a conflict and its dependents fail
    const w = await rig({ cfg: { conflictHandler: (c) => void seen.push(c) } });
    await goOffline(w);
    injectFor(w, "issues/i2/title", stale(StaleCause.PARENT_DELETED, { details: { moved_to: "ks9/issues/i2" } }));
    const t = await w.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("issues/i2/title", "x")] });
    const u = await w.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("issues/i2/title", "y")] });
    await goOnline(w);
    expect(types(w, t.mutationId)).toEqual(["conflict", "rejected"]);
    expect(types(w, u.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(seen.at(-1)?.reject.details["moved_to"]).toBe("ks9/issues/i2");
  });

  it("DOC_RESET: the text of the key's pending update is kept as a local draft, outside the outbox, never sent", async () => {
    const r = await rig({ cfg: { conflictHandler: () => undefined } });
    const doc = new Y.Doc();
    doc.getText("text").insert(0, "my unsaved words");
    const update = Y.encodeStateAsUpdate(doc);
    injectFor(r, "docs/d1", stale(StaleCause.DOC_RESET, { key: "docs/d1" }));
    const a = await r.engine.submit({ keyspaceId: KS, opType: "DocEdit", changes: [crdtUpdate("docs/d1", update)] });
    await r.engine.settle();
    expect(types(r, a.mutationId)).toEqual(["conflict", "rejected"]);
    const drafts = r.engine.draftEntries();
    expect(drafts).toHaveLength(1);
    expect(drafts[0]).toMatchObject({ keyspaceId: KS, key: "docs/d1", reason: "DOC_RESET" });
    const restored = new Y.Doc();
    Y.applyUpdate(restored, drafts[0]?.updates[0] as Uint8Array);
    expect(restored.getText("text").toString()).toBe("my unsaved words");
    expect(r.engine.outbox()).toHaveLength(0);
  });

  it("EPOCH: the keyspace is cold-loaded as a resume check, then the handler", async () => {
    const order: string[] = [];
    const r = await rig({ cfg: { conflictHandler: () => void order.push("handler") } });
    injectFor(r, "settings/a", stale(StaleCause.EPOCH));
    const loads = (): number => r.server.calls.filter((c) => c.method === "loadState").length;
    const l0 = loads();
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(loads()).toBeGreaterThan(l0);
    expect(order).toEqual(["handler"]);
    expect(types(r, a.mutationId)).toEqual(["conflict", "rejected"]);
  });
});

describe("14 [CLI-49] the other CONFLICT codes go to the handler the same way, also in a REJECTED decision", () => {
  it.each([
    [Code.RANGE_EXCEEDED, {}],
    [Code.POLICY_CHANGED, { replacement: "NewOp" }],
    [Code.CONTENDED, { keys: "settings/a,settings/b" }],
  ] as const)("code %s", async (code, details) => {
    const seen: ConflictContext[] = [];
    const r = await rig({ cfg: { conflictHandler: (c) => void seen.push(c) } });
    injectFor(r, "settings/a", mk(code, { details: { ...details } }));
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    expect(seen[0]?.reject.details).toMatchObject(details);
    expect(types(r, a.mutationId)).toEqual(["conflict", "rejected"]);

    const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] });
    await r.engine.settle();
    r.server.decide(KS, g.mutationId, { status: "REJECTED", reject: mk(code, { details: { ...details } }) });
    await r.engine.settle();
    expect(seen).toHaveLength(2);
    expect(types(r, g.mutationId)).toEqual(["conflict", "rejected"]);
  });
});

void [ofType, del];
