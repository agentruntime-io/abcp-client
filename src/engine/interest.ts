// Interest (14 §14.10): which keyspaces the engine follows and which keys of each.
//
//  - [CLI-70] the engine follows only the keyspaces that the app asks for, named in `Subscribe` and `UpdateSubscription`; the list is
//    kept with the stored state (`followed`), so that a restart or a flusher handover follows the same ones;
//  - [CLI-71] interest prefixes per keyspace; the confirmed state then holds only the keys under them (`replica-store.ts`);
//  - [CLI-72] widening: the load of the added prefixes (a widening load, with the cursor unchanged), installed with the ancestor test
//    of 05 [STATE-38]; then the keyspace is added again below its cursor with the new interest, so that the records between the load's
//    `checkpoint_seq` and the switch are delivered (09 [READ-28]: "A client SHOULD widen in one step"). Until the load is installed the
//    interest is the old one, so the changes of arriving records to keys under the added prefixes are ignored, and the re-add delivers
//    them again; the other changes of those records apply at once;
//  - [CLI-72] narrowing needs no load: the keys that left the interest are dropped, and `set_interest` tells the server;
//  - [CLI-73] a key outside the interest that the app is about to change is loaded first (`loadKeys`), so that its `seen_version` is
//    right.

import { create } from "@bufbuild/protobuf";
import { UpdateSubscriptionRequestSchema, UpdateSubscriptionRequest_InterestSchema, type LoadStateResponse } from "../gen/abpc/v2/sync_pb.js";
import { FoldError } from "../rules/fold/index.js";
import { LocalRefusal, ProtocolViolation } from "./errors.js";
import type { Follow } from "./follow.js";
import { HorizonLayer } from "./horizon.js";
import { resendDelay } from "./backoff.js";
import { checkInterest, covers } from "./replica-store.js";

/** The most keys outside the interest whose loaded version is remembered (14 [CLI-73]). */
const OUTSIDE_KEPT = 1000;

function same(a: readonly string[], b: readonly string[]): boolean {
  return a.length === b.length && a.every((x, i) => x === b[i]);
}

export abstract class InterestLayer extends HorizonLayer {
  private readonly widenings = new Set<string>();

  /** The keyspaces that the engine follows and the interest of each, kept with the stored state ([CLI-70]). */
  protected async persistFollowed(): Promise<void> {
    if (this.role !== "flusher") return;
    const followed: Record<string, { interest: string[] }> = {};
    for (const f of this.follows.values()) {
      if (f.stopped === "ACCESS_REVOKED" || f.stopped === "SUBJECT_REVOKED" || f.stopped === "DELETED") continue; // the app asks again
      followed[f.id] = { interest: f.widening?.to ?? f.interest };
    }
    try {
      await this.tx([{ op: "setMeta", key: "followed", value: JSON.stringify(followed) }]);
      this.touchPersisted();
    } catch (e) {
      this.noteInternalError(e);
    }
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Changing the interest (14 [CLI-71], [CLI-72])

  /**
   * Sets the interest prefixes of a followed keyspace; an empty list is the whole keyspace (14 [CLI-71]). A prefix that the interest
   * did not cover is loaded first, as 14 [CLI-72] says, and the interest is the new one once the load is installed (`interest-changed`);
   * a narrowing takes effect at once. Resolves when the change is accepted: a widening is made when the engine has a session.
   */
  setInterest(keyspaceId: string, prefixes: readonly string[]): Promise<void> {
    return this.run(() => this.setInterestNow(keyspaceId, prefixes));
  }

  /** `setInterest` inside a task of the queue (a task never waits for another task of the queue, `core.ts`). */
  protected async setInterestNow(keyspaceId: string, prefixes: readonly string[]): Promise<void> {
    {
      const f = this.follows.get(keyspaceId);
      if (f === undefined || f.purged || (f.stopped !== undefined && f.stopped !== "GRANT_CHANGED")) throw new LocalRefusal("NOT_FOLLOWED", `the keyspace ${keyspaceId} is not followed`);
      const next = [...new Set(prefixes)];
      const bad = checkInterest(next, this.limits.maxInterestPrefixes);
      if (bad !== undefined) throw new LocalRefusal("INVALID", bad);
      if (same(next, f.widening?.to ?? f.interest)) return;
      // the whole keyspace covers every prefix; a prefix that the current interest does not cover has to be loaded
      const wholeNext = next.length === 0;
      const added = wholeNext ? (f.interest.length === 0 ? [] : [""]) : next.filter((p) => !covers(f.interest, p));
      if (f.loaded && added.length > 0) {
        // widening to the whole keyspace names no prefix: an empty interest in the cursor of a load is the whole keyspace
        f.widening = { to: next, added: wholeNext ? [] : added };
        await this.persistFollowed();
        if (this.online() && f.onSession) this.detach(this.widenKeyspace(f));
        else this.kick();
        return;
      }
      f.widening = undefined;
      f.interest = next;
      if (!f.loaded) {
        // not loaded yet: the first load names the new interest
        await this.persistFollowed();
        return;
      }
      this.pruneAllOutside(f);
      await this.persistHead(f, 0, true);
      await this.persistFollowed();
      this.emit({ type: "interest-changed", keyspaceId, interest: [...next], cause: "NARROWED" });
      if (this.online() && f.onSession) {
        // 09 [READ-27]: the server announces the switch point with a `KeyspaceAdded`, before any frame under the new interest
        this.detach(this.sendSetInterest(f));
      }
    }
  }

  private async sendSetInterest(f: Follow): Promise<void> {
    try {
      await this.transport.updateSubscription(
        create(UpdateSubscriptionRequestSchema, { sessionId: this.sessionId(), setInterest: [create(UpdateSubscriptionRequest_InterestSchema, { keyspaceId: f.id, interestPrefixes: [...f.interest] })] }),
      );
    } catch (e) {
      this.noteRevoked(e);
      this.abortSession(); // the session resumes from the cursors, with the interest that the client holds now (09 [READ-57])
    }
  }

  protected kickWidenings(): void {
    for (const f of this.follows.values()) if (this.needsWidening(f)) this.detach(this.widenKeyspace(f));
  }

  protected needsWidening(f: Follow): boolean {
    return f.widening !== undefined && f.loaded && f.stopped === undefined && !f.purged && !this.widenings.has(f.id) && f.frozen === undefined;
  }

  /**
   * The widening of 14 [CLI-72], in one step ([READ-28]): the widening load names only the added prefixes and leaves the cursor as it
   * is; its `log_epoch` must be the one that the client follows, or the keyspace is treated as dropped with `EPOCH_CHANGED`; its entries
   * and ancestor tombstones are installed, and nothing else; the keyspace is then added again with the new interest and
   * `after_seq = min(H, cursor)`, so that the records in (H, cursor] reach the added prefixes too.
   */
  protected async widenKeyspace(f: Follow): Promise<void> {
    const w = f.widening;
    if (w === undefined || this.widenings.has(f.id)) return;
    this.widenings.add(f.id);
    try {
      let pages: LoadStateResponse[];
      try {
        // an empty interest in the cursor of a load is the whole keyspace: a widening to it loads the whole keyspace (the whole state replaces nothing here but is installed over it)
        pages = await this.fetchLoad(f, false, { keyspaceId: f.id, interestPrefixes: [...w.added] });
      } catch (e) {
        await this.run(() => this.onLoadError(f, e));
        // a load that failed is made again with the client backoff, on a live session; with none, the next session makes it (14 [CLI-43])
        const delay = resendDelay(f.loadFailures++, undefined, this.random);
        this.after(delay, () => {
          if (this.online() && this.needsWidening(f)) this.detach(this.widenKeyspace(f));
        });
        return;
      }
      await this.run(async () => {
        if (f.widening !== w || f.stopped !== undefined) return;
        const first = pages[0] as LoadStateResponse;
        if (first.logEpoch !== f.logEpoch) {
          // 14 [CLI-72], 09 [READ-28]: another epoch is the keyspace dropped with EPOCH_CHANGED
          this.onEpochMismatch(f);
          return;
        }
        const entries = pages.flatMap((p) => p.entries);
        try {
          this.installWidening(f, entries, first.manifestVersion);
        } catch (e) {
          if (e instanceof FoldError) {
            this.onProtocolViolation(new ProtocolViolation("LoadStateResponse", e.message, [f.id]), [f.id]);
            return;
          }
          throw e;
        }
        const h = first.checkpointSeq;
        f.interest = [...w.to];
        f.widening = undefined;
        f.lastSyncMs = this.serverNow();
        // the keys that the load now covers are no longer "outside the interest" ([CLI-73])
        for (const k of [...f.outside.keys()]) if (covers(f.interest, k)) f.outside.delete(k);
        // 09 [READ-28]: the cursor of the keyspace is unchanged; the delivery restarts below it, from min(H, cursor)
        if (h < f.watermark) f.watermark = h;
        f.invalidate();
        await this.persistExtend(f, entries);
        await this.persistFollowed();
        this.emit({ type: "interest-changed", keyspaceId: f.id, interest: [...f.interest], cause: "WIDENED" });
        if (this.online()) {
          f.resuming = true;
          this.addToSession(f);
        }
        this.schedulePump();
      });
    } finally {
      this.widenings.delete(f.id);
    }
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // A key outside the interest (14 [CLI-73])

  /**
   * Loads keys that the interest does not cover, before the app changes them, so that each action's `seen_version` is the key's version
   * and not 0 (14 [CLI-73], 05 §5.8): a change with an unknown `seen_version` may be flagged `overwrote_newer` or get
   * `STALE{OVERRIDDEN}`. A key that the keyspace does not hold has version 0, or its tombstone's. The versions are the app's to use for
   * authoring only: the keys are not shown as current ([READ-26]). Resolves false when the keyspace could not be asked.
   */
  async loadKeys(keyspaceId: string, keys: readonly string[]): Promise<boolean> {
    const f = this.follows.get(keyspaceId);
    if (f === undefined || !f.loaded || f.purged) throw new LocalRefusal("NOT_FOLLOWED", `the keyspace ${keyspaceId} is not loaded`);
    const outside = [...new Set(keys)].filter((k) => !covers(f.interest, k));
    const bad = checkInterest(outside, Number.MAX_SAFE_INTEGER);
    if (bad !== undefined) throw new LocalRefusal("INVALID", bad);
    const max = this.limits.maxInterestPrefixes;
    for (let i = 0; i < outside.length; i += max) {
      const chunk = outside.slice(i, i + max);
      let pages: LoadStateResponse[];
      try {
        pages = await this.fetchLoad(f, false, { keyspaceId: f.id, interestPrefixes: chunk });
      } catch (e) {
        await this.run(() => this.onLoadError(f, e));
        return false;
      }
      const ok = await this.run(() => {
        const first = pages[0] as LoadStateResponse;
        if (first.logEpoch !== f.logEpoch) {
          this.onEpochMismatch(f);
          return false;
        }
        const seen = new Map<string, bigint>();
        for (const p of pages) for (const e of p.entries) seen.set(e.key, e.version);
        for (const k of chunk) {
          // a key that the load names has its version (a tombstone's included); one that it does not is absent, with no tombstone: 0
          f.outside.delete(k);
          f.outside.set(k, seen.get(k) ?? 0n);
        }
        while (f.outside.size > OUTSIDE_KEPT) {
          const oldest = f.outside.keys().next();
          if (oldest.done === true) break;
          f.outside.delete(oldest.value);
        }
        return true;
      });
      if (!ok) return false;
    }
    return true;
  }

}
