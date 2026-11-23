// Round 19 (spec draft 17), 14 Table 14.1, the row for `BLOB_MISSING` for a name in an action's `blob_refs`, when the client holds the blob's bytes in no stash (14 [CLI-44], [CLI-45], 09 [READ-94]):
//
//   Scope: the action. While held: "The client keeps the action, resends it with the hold backoff, and reports to the app that the blob's bytes are missing, so that the app can supply them".
//   Clears when: "The app supplies the bytes: the client uploads them and a resend gets another result". Bound: the default one.
//
// Before round 19 the text had no row for it, and the engine held the action and sent it nothing until the app staged the bytes again (a reading marked SPEC-QUESTION in `blobs.ts`, Q75.8); the resend
// with the hold backoff, and the clearing by a resend's result, are the change.

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { digestSha256, set } from "../src/engine/index.js";
import { reject as mk } from "../src/fake/index.js";
import { CommitBlobRequestSchema, CreateUploadRequestSchema } from "../src/gen/abcp/v2/sync_pb.js";
import { Code } from "../src/gen/abcp/v2/types_pb.js";
import { KS, ofType, rejectResult, type Rig } from "./helpers/engine.js";
import { brig, bytesOf, until } from "./helpers/presence.js";

const MINUTE = 60_000;
const ID = "m".repeat(22);

const submitNamed = (r: Rig, refs: string[], key = "settings/a", keyspaceId = KS): Promise<{ mutationId: string }> => r.engine.submit({ keyspaceId, opType: "Set", changes: [set(key, 1)], blobRefs: refs });

/** The times, on the engine's clock, at which `Mutate` reached the server. */
const mutateTimes = (r: Rig): number[] => r.server.calls.filter((c) => c.method === "mutate").map((c) => c.at);
const holds = (r: Rig): unknown[] => r.engine.status().holds;
const heldEvents = (r: Rig): string[] => ofType(r, "held").map((e) => e.state);
const committed = (r: Rig, id: string): boolean => r.events.some((e) => e.type === "committed" && e.mutationId === id);

/** The blob reaches the server by another way than the engine's stash: the app of another device, or the same app after a restart that lost its bytes. */
async function uploadElsewhere(r: Rig, blobId: string, bytes: Uint8Array): Promise<void> {
  const t = r.server.transport(r.principal);
  const up = await t.createUpload(create(CreateUploadRequestSchema, { keyspaceId: KS, blobId, sha256: await digestSha256(bytes), size: BigInt(bytes.length), contentType: "" }));
  const put = await r.server.blobs.blobHttp().put(up.uploadUrl, { ...up.uploadHeaders }, bytes, {});
  expect(put.status).toBe(200);
  await t.commitBlob(create(CommitBlobRequestSchema, { keyspaceId: KS, blobId }));
}

describe("14 Table 14.1 BLOB_MISSING (CLI-44, CLI-45): the client keeps the action and resends it with the hold backoff", () => {
  it("the resends come at 1 min x 2^n up to 1 h, each wait drawn uniformly (here at its middle), never at the client backoff of at most 5 min", async () => {
    const r = await brig();
    r.random.fixed = 0.5;
    const a = await submitNamed(r, [ID]);
    await r.engine.settle();
    const started = r.clock.now();
    expect(mutateTimes(r)).toEqual([started]); // the first send: the server answers BLOB_MISSING, and the client holds no bytes of the blob
    expect(holds(r)).toEqual([{ scope: "action", code: "BLOB_MISSING", keyspaceId: KS, mutationId: a.mutationId, blobIds: [ID] }]);
    for (let s = 0; s < 3 * 3600; s += 5) {
      await r.clock.advance(5_000);
      await r.engine.settle();
    }
    const at = mutateTimes(r);
    const gaps = at.slice(1).map((t, i) => t - (at[i] as number));
    // the n-th wait is drawn from [0, min(1 h, 1 min x 2^n)], n counted from 0 (04 Table 4.11); at the middle: 30 s, 1 min, 2, 4, 8, 16 min, then 30 min (the cap is 1 h)
    expect(gaps.slice(0, 7).map((g) => Math.round(g / 1000))).toEqual([30, 60, 120, 240, 480, 960, 1800]);
    expect(gaps.slice(7).every((g) => Math.abs(g - 1_800_000) < 5_000)).toBe(true);
    // the action was kept, unchanged, the whole time: the same mutation id every time, nothing dead-lettered, no server blob deleted
    expect(r.engine.outbox().map((e) => e.mutationId)).toEqual([a.mutationId]);
    expect(r.engine.deadLetterEntries()).toEqual([]);
    expect(r.server.calls.filter((c) => c.method === "deleteBlob")).toHaveLength(0);
    expect(heldEvents(r)).toEqual(["start"]); // one hold, reported once, whatever the number of resends
  });

  it("reports to the app that the bytes are missing: the `held` event and the sync status name the action, the code and the blobs, so that the app can supply them", async () => {
    const r = await brig();
    r.random.fixed = 0.5;
    const a = await submitNamed(r, [ID, "n".repeat(22)]);
    await r.engine.settle();
    expect(ofType(r, "held")).toMatchObject([{ type: "held", state: "start", scope: "action", code: "BLOB_MISSING", keyspaceId: KS, mutationId: a.mutationId, blobIds: [ID, "n".repeat(22)] }]);
    expect(holds(r)).toEqual([{ scope: "action", code: "BLOB_MISSING", keyspaceId: KS, mutationId: a.mutationId, blobIds: [ID, "n".repeat(22)] }]);
  });

  it("the hold is the action's alone: an action of another keyspace goes on, and the held one keeps its place", async () => {
    const r = await brig();
    r.random.fixed = 0.5;
    r.server.createKeyspaceNow("ks2");
    await r.engine.addKeyspace("ks2");
    await r.engine.settle();
    const a = await submitNamed(r, [ID]);
    const other = await submitNamed(r, [], "settings/b", "ks2");
    await r.engine.settle();
    expect(committed(r, other.mutationId)).toBe(true);
    expect(committed(r, a.mutationId)).toBe(false);
    expect(holds(r)).toMatchObject([{ scope: "action", mutationId: a.mutationId }]);
  });
});

describe("14 Table 14.1 BLOB_MISSING: the hold clears when the app supplies the bytes, the client uploads them and a resend gets another result", () => {
  it("a resend that gets another result clears it: the blob reached the server by another way, the next resend is appended, `held` stops, and the engine uploaded nothing", async () => {
    const r = await brig();
    r.random.fixed = 0.5;
    const a = await submitNamed(r, [ID]);
    await r.engine.settle();
    await r.clock.advance(100_000); // two resends in
    await r.engine.settle();
    expect(holds(r)).toHaveLength(1);
    await uploadElsewhere(r, ID, bytesOf(300));
    expect(holds(r)).toHaveLength(1); // the hold clears with the result of a resend, not with a fact that the client has not seen
    await r.clock.advance(10 * MINUTE);
    await r.engine.settle();
    expect(committed(r, a.mutationId)).toBe(true);
    expect(holds(r)).toEqual([]);
    expect(heldEvents(r)).toEqual(["start", "stop"]);
    expect(r.server.calls.filter((c) => c.method === "createUpload" && c.sub === "ana")).toHaveLength(1); // the one of `uploadElsewhere`: the engine held no bytes to upload
  });

  it("the app stages the bytes again under the same blob id: the client uploads them and resends at once, without waiting out the backoff, and the hold ends with that resend's result", async () => {
    const r = await brig();
    r.random.fixed = 0.5;
    const bytes = bytesOf(300);
    const a = await submitNamed(r, [ID]);
    await r.engine.settle();
    await r.clock.advance(8 * MINUTE); // the waits so far: 30 s, 1, 2, 4 min; the next is 8 min away
    await r.engine.settle();
    const sendsBefore = mutateTimes(r).length;
    expect(holds(r)).toHaveLength(1);
    const staged = r.clock.now();
    await r.engine.stageBlob(KS, bytes, { blobId: ID });
    await until(r, () => committed(r, a.mutationId), "the action committed after the bytes were staged", 0, 10_000);
    expect(r.clock.now() - staged).toBeLessThan(5_000); // not the next 8 or 16 minutes of the hold backoff
    expect(mutateTimes(r).length).toBe(sendsBefore + 1);
    expect(holds(r)).toEqual([]);
    expect(heldEvents(r)).toEqual(["start", "stop"]);
    expect(Buffer.from(r.server.blobs.object(KS, ID) as Uint8Array).equals(Buffer.from(bytes))).toBe(true); // uploaded from the staged bytes
    expect(r.server.blobs.row(KS, ID)?.state).toBe("committed");
  });

  it("the resend after the supply of the bytes gets BLOB_MISSING again, with the bytes now held: the row no longer applies, the hold stops, and the action is repaired and resent with the client backoff", async () => {
    const r = await brig();
    r.random.fixed = 0.5;
    const a = await submitNamed(r, [ID]);
    await r.engine.settle();
    // a server that answers BLOB_MISSING once more, though the blob is committed (a store that lost its object again, a restore)
    r.server.inject("mutate", { kind: "result", result: (m) => (m.mutationId === a.mutationId ? rejectResult(m, mk(Code.BLOB_MISSING)) : undefined) }, { times: 1 });
    await r.engine.stageBlob(KS, bytesOf(300), { blobId: ID });
    await until(r, () => committed(r, a.mutationId), "the action committed", 100, 20_000);
    expect(holds(r)).toEqual([]);
    expect(heldEvents(r)).toEqual(["start", "stop"]);
    const stopped = ofType(r, "held").find((e) => e.state === "stop");
    expect(stopped).toMatchObject({ scope: "action", code: "BLOB_MISSING", mutationId: a.mutationId });
  });

  it("the hold backoff restarts when the hold clears: the next hold's first wait is the first of the series again", async () => {
    const r = await brig();
    r.random.fixed = 0.5;
    const a = await submitNamed(r, [ID]);
    await r.engine.settle();
    await r.clock.advance(8 * MINUTE);
    await r.engine.settle();
    await uploadElsewhere(r, ID, bytesOf(100));
    await r.clock.advance(20 * MINUTE);
    await r.engine.settle();
    expect(committed(r, a.mutationId)).toBe(true);
    const id2 = "p".repeat(22);
    const b = await submitNamed(r, [id2], "settings/b");
    await r.engine.settle();
    const first = mutateTimes(r).at(-1) as number;
    await r.clock.advance(31_000);
    await r.engine.settle();
    expect(mutateTimes(r).at(-1)).toBeGreaterThan(first); // resent after 30 s, as a first wait of a new hold
    expect(holds(r)).toMatchObject([{ scope: "action", mutationId: b.mutationId }]);
  });
});

describe("14 Table 14.1 BLOB_MISSING: the bound is the default one (14 [CLI-45])", () => {
  it("a sent action that reaches max_offline in the hold is reported as outcome unknown, and leaves the outbox, with its resends stopped", async () => {
    const r = await brig({ limits: { maxOfflineSeconds: 3600 } });
    r.random.fixed = 0.5;
    const a = await submitNamed(r, [ID]);
    await r.engine.settle();
    expect(holds(r)).toHaveLength(1);
    await r.clock.advance(3600_000 + 1000);
    await r.engine.settle();
    expect(ofType(r, "outcome-unknown").find((e) => e.mutationId === a.mutationId)?.cause).toBe("HOLD_BOUND");
    expect(r.engine.outbox()).toEqual([]);
    expect(holds(r)).toEqual([]);
    const sends = mutateTimes(r).length;
    await r.clock.advance(2 * 3600_000);
    await r.engine.settle();
    expect(mutateTimes(r).length).toBe(sends); // no probe of a hold that ended
  });
});
