// R5d-P7e, 13 [PLC-27]: the `committed` event carries `logEpoch`, `readYourWritesToken` reads the token off it, and the progress-row test on sample rows.

import { describe, expect, it } from "vitest";
import { copyHasWrite, readYourWritesToken, set, type CommittedEvent } from "../src/engine/index.js";
import { KS, ofType, rig } from "./helpers/engine.js";

describe("13 [PLC-27]: the committed event carries logEpoch", () => {
  it("for an append", async () => {
    const r = await rig();
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await r.engine.settle();
    const ev = ofType(r, "committed").find((e) => e.mutationId === a.mutationId) as CommittedEvent;
    expect(ev.logEpoch).toBeGreaterThan(0);
    expect(ev.seq).toBe(1n);
    expect(readYourWritesToken(ev)).toEqual({ keyspaceId: KS, logEpoch: ev.logEpoch, seq: 1n });
  });

  it("for a decided intent (decision_seq)", async () => {
    const r = await rig();
    const g = await r.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("settings/g", 1)] });
    await r.engine.settle();
    r.server.decide(KS, g.mutationId, { status: "COMMITTED" });
    await r.engine.settle();
    const ev = ofType(r, "committed").find((e) => e.mutationId === g.mutationId) as CommittedEvent;
    expect(ev).toBeDefined();
    expect(ev.logEpoch).toBeGreaterThan(0);
    const t = readYourWritesToken(ev);
    expect(t).toEqual({ keyspaceId: KS, logEpoch: ev.logEpoch, seq: ev.seq });
    expect(t.seq).toBeGreaterThan(0n);
  });
});

describe("13 [PLC-27]: the token", () => {
  it("has the three values of the event", () => {
    const ev: CommittedEvent = { type: "committed", mutationId: "m", keyspaceId: "k9", seq: 42n, logEpoch: 3 };
    expect(readYourWritesToken(ev)).toEqual({ keyspaceId: "k9", logEpoch: 3, seq: 42n });
  });
});

describe("13 [PLC-27] progress-row test (copyHasWrite)", () => {
  const token = { keyspaceId: "k", logEpoch: 2, seq: 10n };
  it("epoch above: has the write", () => {
    expect(copyHasWrite(token, { epoch: 3, copiedSeq: 0n })).toBe("has-write");
  });
  it("epoch equal, copied_seq at or above seq: has the write", () => {
    expect(copyHasWrite(token, { epoch: 2, copiedSeq: 10n })).toBe("has-write");
    expect(copyHasWrite(token, { epoch: 2, copiedSeq: 11n })).toBe("has-write");
  });
  it("epoch equal, copied_seq below seq: not yet", () => {
    expect(copyHasWrite(token, { epoch: 2, copiedSeq: 9n })).toBe("not-yet");
  });
  it("epoch below: not yet", () => {
    expect(copyHasWrite(token, { epoch: 1, copiedSeq: 99n })).toBe("not-yet");
  });
  it("N8: a CRDT row absent or with crdt_covered_seq below seq is not yet", () => {
    const p = { epoch: 2, copiedSeq: 10n };
    expect(copyHasWrite(token, p, { covered: undefined })).toBe("not-yet");
    expect(copyHasWrite(token, p, { covered: 9n })).toBe("not-yet");
    expect(copyHasWrite(token, p, { covered: 10n })).toBe("has-write");
  });
  it("tombstone: purged with epoch 0 is a deleted keyspace", () => {
    expect(copyHasWrite(token, { epoch: 0, copiedSeq: 0n, purged: true })).toBe("deleted");
  });
});
