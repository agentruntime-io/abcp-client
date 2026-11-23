// FBA batch F7, FBA-28 (A8-M5): 14 [CLI-76] "A client MUST make the purge of every local copy that this item names ... in the local transaction that
// advances its persisted cursor to or past the record, or before that advance, so that a crash repeats the purge and never skips it." The staged bytes of a
// blob live in a stash of their own, outside the outbox's transaction, so the engine erases them before the transaction that moves the cursor.

import { describe, expect, it } from "vitest";
import { MemoryBlobStash, MemoryStorage, set, type StorageOp } from "../src/engine/index.js";
import { reject as mk } from "../src/fake/index.js";
import { Code, RetryReason } from "../src/gen/abcp/v2/types_pb.js";
import { brig, bytesOf } from "./helpers/presence.js";
import { KS, rejectResult, retryResult } from "./helpers/engine.js";

/** The order in which the stash erased bytes and the outbox committed an erasure. */
class LoggedStorage extends MemoryStorage {
  /** The erasure transactions to refuse, as a quota would. */
  failErasures = 0;
  constructor(private readonly trace: string[]) {
    super();
  }
  override commit(ops: readonly StorageOp[]): Promise<void> {
    if (ops.some((o) => o.op === "erase")) {
      if (this.failErasures > 0) {
        this.failErasures--;
        return Promise.reject(new Error("quota"));
      }
      this.trace.push("erasure-tx");
    }
    return super.commit(ops);
  }
}

class LoggedStash extends MemoryBlobStash {
  constructor(private readonly log: string[]) {
    super();
  }
  override async delete(keyspaceId: string, blobId: string): Promise<void> {
    await super.delete(keyspaceId, blobId);
    this.log.push(`stash-delete:${blobId}`);
  }
}

describe("FBA-28: 14 [CLI-76] the staged bytes of a blob that only erased values named go before the persisted cursor passes the REDACTED record", () => {
  it("CLI76_StagedBlobPurgeBeforeTheCursorAdvance", async () => {
    const log: string[] = [];
    const stash = new LoggedStash(log);
    const r = await brig({ storage: new LoggedStorage(log), cfg: { blobs: { stash } } });
    const dead = await r.engine.stageBlob(KS, bytesOf(100, 5));
    const pend = await r.engine.stageBlob(KS, bytesOf(100, 6));
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => (m.changes[0]?.key === "settings/dead" ? rejectResult(m, mk(Code.SCHEMA_INVALID)) : retryResult(m, RetryReason.CONTENTION, 3_600_000)) }, { times: 20 });
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/dead", 1)], blobRefs: [dead.blobId] } as never);
    await r.engine.settle();
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/pend", 1)], blobRefs: [pend.blobId] } as never);
    await r.engine.settle();
    log.length = 0;
    r.server.redact(KS, ["settings/dead", "settings/pend"]);
    await r.engine.settle();
    expect((await stash.list()).map((s) => s.blobId)).toEqual([pend.blobId]);
    const purged = log.indexOf(`stash-delete:${dead.blobId}`);
    const tx = log.indexOf("erasure-tx");
    expect(purged).toBeGreaterThanOrEqual(0);
    expect(tx).toBeGreaterThanOrEqual(0);
    expect(purged).toBeLessThan(tx); // a crash between the two repeats the purge: the cursor is still below the record
  });

  it("CLI76_AStashThatCannotEraseHoldsTheCursorBelowTheRecord", async () => {
    const log: string[] = [];
    let refuse = true;
    const stash = new (class extends LoggedStash {
      override async delete(keyspaceId: string, blobId: string): Promise<void> {
        if (refuse) throw new Error("the disk refused");
        await super.delete(keyspaceId, blobId);
      }
    })(log);
    const r = await brig({ storage: new LoggedStorage(log), cfg: { blobs: { stash } } });
    const dead = await r.engine.stageBlob(KS, bytesOf(100, 5));
    r.server.inject("mutate", { kind: "result", recorded: true, result: (m) => rejectResult(m, mk(Code.SCHEMA_INVALID)) }, { times: 5 });
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/dead", 1)], blobRefs: [dead.blobId] } as never);
    await r.engine.settle();
    log.length = 0;
    r.server.redact(KS, ["settings/dead"]);
    await r.engine.settle();
    expect(log).not.toContain("erasure-tx"); // the purge is not done: nothing moves the persisted cursor past the record
    refuse = false;
    for (let i = 0; i < 20 && !log.includes("erasure-tx"); i++) {
      await r.clock.advance(60_000);
      await r.engine.settle();
    }
    expect(log.indexOf(`stash-delete:${dead.blobId}`)).toBeGreaterThanOrEqual(0);
    expect(log.indexOf(`stash-delete:${dead.blobId}`)).toBeLessThan(log.indexOf("erasure-tx"));
    expect(await stash.list()).toEqual([]);
  });

  it("CLI76_B3S12_AFailedErasureKeepsTheImageBelowTheRecord", async () => {
    const log: string[] = [];
    const storage = new LoggedStorage(log);
    const r = await brig({ storage });
    await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/x", 1)] });
    await r.engine.settle();
    const before = (await storage.load()).replicas.find((i) => i.keyspaceId === KS);
    expect(before).toBeDefined();
    storage.failErasures = 1;
    r.server.redact(KS, ["settings/x"]);
    await r.engine.settle();
    // the erasure failed: the image stays as it was, with its cursor below the record, so that a crash before the retry repeats the purge
    const kept = (await storage.load()).replicas.find((i) => i.keyspaceId === KS);
    expect(kept?.head).toEqual(before?.head);
    for (let i = 0; i < 20 && !log.includes("erasure-tx"); i++) {
      await r.clock.advance(60_000);
      await r.engine.settle();
    }
    expect(log).toContain("erasure-tx"); // the retry made it durable
  });
});
