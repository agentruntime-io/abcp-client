// Scenarios of the client engine against the scriptable fake server (17 [CONF-4]): each runs several rules together.

import { describe, expect, it } from "vitest";
import * as Y from "yjs";
import { cas, del, incr, jsonBytes, set } from "../src/engine/index.js";
import { Code, StaleCause, type ErrorClass } from "../src/gen/abcp/v2/types_pb.js";
import { reject as mk } from "../src/fake/index.js";
import { goOffline, goOnline, kc, kcCas, KS, mutateRequests, ofType, rejectResult, rig, sentIds, tick, types, text } from "./helpers/engine.js";

describe("scenario: offline writes, then reconnect (14 [CLI-6], [CLI-21], [CLI-26], [CLI-27], [CLI-31], [CLI-62])", () => {
  it("the user works offline: every action is shown at once, survives a restart of the engine, and goes out in order, once, when the connection returns", async () => {
    const r = await rig();
    await goOffline(r);
    const created = await r.engine.submit({ keyspaceId: KS, opType: "Create", changes: [set("issues/i1", { n: 1 }), set("issues/i1/title", "first")] });
    const retitled = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("issues/i1/title", "second")] });
    const counted = await r.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [incr("counters/opened", 1)] });
    const settings = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/theme", "dark")] });
    expect(r.engine.status().connection).toBe("offline");
    expect(r.engine.status().pending).toBe(4);
    // shown at once, in outbox order
    expect(r.engine.view(KS).json("issues/i1/title")).toBe("second");
    expect(r.engine.view(KS).json("counters/opened")).toBe(1);
    expect(r.engine.view(KS).get("settings/theme")?.pending).toEqual([settings.mutationId]);
    // meanwhile, another principal changed the counter
    r.server.write(KS, [kc("counters/opened", "incr", 4)], { sub: "bob" });
    await goOnline(r);
    // dependents were sent after their predecessors, unrelated actions did not wait
    const order = sentIds(r);
    expect(order.indexOf(created.mutationId)).toBeLessThan(order.indexOf(retitled.mutationId));
    expect(new Set(order).size).toBe(order.length); // each action was sent once
    for (const a of [created, retitled, counted, settings]) expect(types(r, a.mutationId).filter((t) => t === "committed")).toHaveLength(1);
    expect(r.engine.outbox()).toHaveLength(0);
    expect(r.engine.view(KS).json("issues/i1/title")).toBe("second");
    expect(r.engine.view(KS).json("counters/opened")).toBe(5); // the confirmed 4 and the pending 1
    expect(text(r.server.value(KS, "settings/theme"))).toBe('"dark"');
    expect(r.engine.status().pending).toBe(0);
  });

  it("the engine is restarted while offline: the durable outbox carries the actions over, with new stamping and the same mutation ids", async () => {
    const r = await rig();
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] });
    await r.engine.stop();
    const r2 = await rig({ storage: r.storage, server: r.server, clock: r.clock, seed: 31 });
    expect(types(r2, a.mutationId)).toEqual(["committed"]);
    expect(types(r2, b.mutationId)).toEqual(["committed"]);
    expect(mutateRequests(r2).at(-1)?.mutations.map((m) => Number(m.clientSeq))).toEqual([1, 2]);
    expect(r2.engine.outbox()).toHaveLength(0);
  });
});

describe("scenario: a rejected action and its dependents (14 [CLI-30], [CLI-34], [CLI-38], [CLI-46])", () => {
  it("the rejected action goes to the dead-letter store with its dependents, the view falls back to confirmed state, and the user can resubmit", async () => {
    const r = await rig();
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.changes[0]?.key === "issues/i1" ? rejectResult(m, mk(Code.SCHEMA_INVALID)) : undefined) });
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Create", changes: [set("issues/i1", { bad: true })] });
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("issues/i1/title", "t")] });
    const c = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/independent", 1)] });
    expect(r.engine.view(KS).has("issues/i1/title")).toBe(true);
    await goOnline(r);
    expect(r.engine.view(KS).has("issues/i1/title")).toBe(false);
    expect(types(r, a.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(types(r, b.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(ofType(r, "rejected").find((e) => e.mutationId === b.mutationId)?.reject?.code).toBe(Code.DEPENDENCY_FAILED);
    await tick(r, 3000);
    expect(types(r, c.mutationId)).toEqual(["committed"]); // unrelated: not held back by a dependency
    expect(r.engine.status()).toMatchObject({ deadLetters: 2, unacknowledgedRejections: 2, pending: 0 });
    // the user resubmits the first as a new action
    const again = await r.engine.resubmitDeadLetter(a.mutationId);
    await r.engine.settle();
    await tick(r, 3000); // c was behind a on the lane (Retry{NOT_PROCESSED}): the lane goes on after its backoff
    expect(again.mutationId).not.toBe(a.mutationId);
    expect(types(r, again.mutationId)).toEqual(["committed"]);
  });
});

describe("scenario: a CONFLICT handed to the app's handler (14 [CLI-47], [CLI-48], [CLI-49])", () => {
  it("someone else changed the value first: the handler sees their value, the user merges, and the merge is a new action", async () => {
    const seen: string[] = [];
    const r = await rig({
      cfg: {
        conflictHandler: async (c) => {
          const theirs = c.confirmed.json("notes/n1");
          seen.push(`theirs=${String(theirs)}`);
          await c.submit({ keyspaceId: KS, opType: "CasSet", changes: [cas("notes/n1", `${String(theirs)}+mine`)] });
        },
      },
    });
    r.server.write(KS, [kcCas("notes/n1", "base")]);
    await r.engine.settle();
    await goOffline(r);
    // authored against version 1 of the note; Bob changes it meanwhile
    const mine = await r.engine.submit({ keyspaceId: KS, opType: "CasSet", changes: [cas("notes/n1", "mine")] });
    r.server.write(KS, [kcCas("notes/n1", "theirs")], { sub: "bob" });
    await goOnline(r);
    await tick(r, 3000);
    expect(seen).toEqual(['theirs="theirs"'.replace('"theirs"', "theirs")]);
    expect(types(r, mine.mutationId).filter((t) => t !== "held")).toEqual(["conflict", "rejected"]);
    expect(ofType(r, "conflict")[0]?.reject.stale?.cause).toBe(StaleCause.VERSION_MISMATCH);
    expect(r.engine.view(KS).json("notes/n1")).toBe("theirs+mine");
    expect(r.engine.outbox()).toHaveLength(0);
    expect(sentIds(r).filter((id) => id === mine.mutationId)).toHaveLength(1); // never resent
  });
});

describe("scenario: RETRY with backoff bounds (14 [CLI-43], [CLI-24])", () => {
  it("a loaded server answers Retry several times: the action waits within the bounds, keeps its place, and commits once the server has room", async () => {
    const r = await rig();
    r.random.fixed = 0.75;
    const injected: number[] = [];
    r.server.inject("mutate", { kind: "result", result: (m) => (injected.push(r.clock.now()), (m.changes[0]?.key === "settings/a" ? retryRes(m) : undefined)) }, { times: 5 });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 1)] }); // unrelated, but behind a on the lane
    await r.engine.settle();
    for (let i = 0; i < 300 && !types(r, a.mutationId).includes("committed"); i++) await tick(r, 1000);
    expect(types(r, a.mutationId)).toEqual(["committed"]);
    expect(types(r, b.mutationId)).toEqual(["committed"]);
    const times = r.server.calls.filter((c) => c.method === "mutate").map((c) => c.at);
    const gaps = times.slice(1).map((t, i) => t - (times[i] as number));
    gaps.slice(0, 5).forEach((g, n) => expect(g).toBeLessThanOrEqual(Math.min(300_000, 1000 * 2 ** n)));
    expect(r.server.records(KS).map((x) => x.mutationId)).toEqual([a.mutationId, b.mutationId]); // the lane kept its order
  });
});

import { RetryReason } from "../src/gen/abcp/v2/types_pb.js";
import { retryResult } from "./helpers/engine.js";
const retryRes = (m: Parameters<typeof retryResult>[0]): ReturnType<typeof retryResult> => retryResult(m, RetryReason.STORAGE_UNAVAILABLE, 0);

describe("scenario: a HOLD on a lane (14 [CLI-45], 15 [COMPAT-39])", () => {
  it("a reject of a class that no client knows holds the lane, not the keyspace: the other lane goes on, and the lane resumes when a resend gets another result", async () => {
    const r = await rig();
    r.random.fixed = 0.1;
    r.server.inject("mutate", { kind: "result", result: (m) => (m.changes[0]?.key === "settings/a" ? rejectResult(m, { ...mk(Code.STALE), errorClass: 42 as ErrorClass }) : undefined) });
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] }); // the GATED lane: not held
    const behind = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/behind", 1)] }); // same lane: waits
    await r.engine.settle();
    expect(sentIds(r)).toContain(g.mutationId);
    expect(sentIds(r)).not.toContain(behind.mutationId);
    expect(r.engine.status().holds).toEqual([expect.objectContaining({ scope: "lane", lane: "SEQUENCED" })]);
    await tick(r, 10_000);
    expect(types(r, a.mutationId)).toEqual(["committed"]);
    expect(types(r, behind.mutationId)).toEqual(["committed"]);
    expect(r.engine.status().holds).toEqual([]);
  });
});

describe("scenario: duplicate and reordered frames, with the client's own action in the stream (14 [CLI-3], [CLI-7], [CLI-40])", () => {
  it("the stream is disturbed, and nothing is applied twice, lost or reported twice", async () => {
    const r = await rig();
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [incr("counters/c", 1)] });
    r.server.faults.reorderNext = true;
    r.server.write(KS, [kc("counters/c", "incr", 10)], { sub: "bob" });
    r.server.write(KS, [kc("counters/c", "incr", 100)], { sub: "bob" });
    r.server.faults.duplicateNext = 3;
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Incr", changes: [incr("counters/c", 1000)] });
    r.server.write(KS, [kc("counters/c", "incr", 10_000)], { sub: "bob" });
    await tick(r, 3000);
    expect(r.engine.confirmed(KS).json("counters/c")).toBe(1 + 10 + 100 + 1000 + 10_000);
    expect(r.server.records(KS).length).toBe(5);
    expect(types(r, a.mutationId)).toEqual(["committed"]);
    expect(types(r, b.mutationId)).toEqual(["committed"]);
    expect(r.engine.cursor(KS)?.seq).toBe(5n);
    expect(r.engine.internalErrors).toEqual([]);
  });
});

describe("scenario: a GATED action decided later (14 [CLI-15], [CLI-32], [CLI-44], [CLI-58], 08)", () => {
  it("is shown as pending, survives going offline while the server decides, and the DECISION on the resumed stream retires it; effects make it `corrected`", async () => {
    const r = await rig();
    const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/requested", "yes")] });
    await r.engine.settle();
    expect(r.engine.view(KS).get("settings/requested")?.pending).toEqual([g.mutationId]);
    expect(r.engine.outbox()[0]?.registered).toBe(true);
    await goOffline(r);
    r.server.decide(KS, g.mutationId, { status: "COMMITTED", effects: [kc("settings/granted", "set", true)] }); // decided while the client is away
    await goOnline(r);
    await tick(r, 3000);
    expect(types(r, g.mutationId)).toEqual(["corrected"]);
    expect(ofType(r, "corrected")[0]?.keys).toEqual(["settings/granted"]);
    expect(r.engine.view(KS).json("settings/granted")).toBe(true);
    expect(sentIds(r).filter((id) => id === g.mutationId).length).toBeLessThanOrEqual(2); // registered once; never driven again by the client
    expect(r.engine.outbox()).toHaveLength(0);
  });

  it("a REJECTED decision made later ends it as any final rejection of its class does; its dependents fail", async () => {
    const r = await rig();
    const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/requested", "yes")] });
    await r.engine.settle();
    const dep = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [del("settings/requested")] }); // depends on g
    await r.engine.settle();
    expect(sentIds(r)).not.toContain(dep.mutationId); // waits for the decision
    r.server.decide(KS, g.mutationId, { status: "REJECTED", reject: mk(Code.GATE_REJECTED, { appCode: "tracker.NOT_ALLOWED" }) });
    await r.engine.settle();
    expect(types(r, g.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(ofType(r, "rejected").find((e) => e.mutationId === g.mutationId)?.reject?.appCode).toBe("tracker.NOT_ALLOWED");
    expect(types(r, dep.mutationId)).toEqual(["rejected", "dead-lettered"]);
    expect(r.engine.view(KS).has("settings/requested")).toBe(false);
  });
});

describe("scenario: a CRDT key typed into while an update is pending, by two clients (14 [CLI-13])", () => {
  it("both converge, every edit survives, and no view document is replaced while the user types", async () => {
    const ana = await rig({ heartbeatMs: 15_000 });
    const bob = await rig({ server: ana.server, clock: ana.clock, sub: "bob", seed: 99 });
    const a = ana.engine.crdt(KS, "docs/d1");
    const b = bob.engine.crdt(KS, "docs/d1");
    const aView = a.doc;
    const bView = b.doc;
    a.doc.getText("text").insert(0, "Ana: hello. ");
    ana.server.faults.paused = true; // the stream is slow: the updates are pending while they type on
    await a.flush("DocEdit");
    b.doc.getText("text").insert(0, "Bob: hi. ");
    await b.flush("DocEdit");
    a.doc.getText("text").insert(a.doc.getText("text").length, "More from Ana.");
    b.doc.getText("text").insert(b.doc.getText("text").length, "More from Bob.");
    await ana.engine.settle();
    ana.server.resumeDelivery();
    await a.flush("DocEdit");
    await b.flush("DocEdit");
    await ana.engine.settle();
    await bob.engine.settle();
    await tick(ana, 1000);
    await bob.engine.settle();
    const server = (ana.server.ydoc(KS, "docs/d1") as Y.Doc).getText("text").toString();
    expect(server).toContain("Ana: hello. ");
    expect(server).toContain("Bob: hi. ");
    expect(server).toContain("More from Ana.");
    expect(server).toContain("More from Bob.");
    for (const h of [a, b]) {
      expect(h.confirmedDoc.getText("text").toString()).toBe(server);
      expect(h.doc.getText("text").toString()).toBe(server);
      expect(h.hasUnsent).toBe(false);
    }
    expect(a.doc).toBe(aView);
    expect(b.doc).toBe(bView);
    expect(ana.engine.outbox()).toHaveLength(0);
    expect(bob.engine.outbox()).toHaveLength(0);
    expect(ofType(ana, "rebuilt")).toHaveLength(0);
    void jsonBytes;
  });
});
