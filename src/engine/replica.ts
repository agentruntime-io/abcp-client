// The confirmed state of each followed keyspace and what feeds it (14 §14.1): `LoadState` pages and the records of `Subscribe`,
// applied by the version-gated fold of C1 (`rules/fold`). Nothing else installs confirmed state ([CLI-2]).
//
//  - [CLI-3]: the cursor advances only on records and delivery watermarks, and a frame whose `prev_cursor` is not the cursor is
//    not applied: the keyspace is resumed from its cursor;
//  - [CLI-4]: a record that 06 [LOG-11] says to stop at freezes the keyspace's confirmed state at the previous seq, and holds the
//    keyspace with `UPGRADE_REQUIRED`;
//  - [CLI-5]: a change that cannot be decoded still enters the confirmed state; only its display is skipped, with `skipped`;
//  - [CLI-7]: an action retires when the cursor has reached the seq of its outcome, in the outcome's epoch;
//  - [CLI-40]: `overwritten` once per mutation id.

import { type RecordsFrame, type HeartbeatFrame, type LoadStateResponse, type LaneWatermark } from "../gen/abpc/v2/sync_pb.js";
import { Code, ErrorClass, Lane, RecordKind, ChangeKind, DecisionStatus, type KeyEntry, type Record as RecordMessage, type Reject, KeyspaceLifecycle } from "../gen/abpc/v2/types_pb.js";
import { makeReject } from "./endings.js";
import { applyRecord, FoldError, installEntry, ReplicaState, StateValue } from "../rules/fold/index.js";
import { ProtocolViolation } from "./errors.js";
import { EndingLayer } from "./endings.js";
import { appliedIndexes, planCrdt, type CrdtPlanItem } from "./crdt-view.js";
import type { Follow } from "./follow.js";
import { canDecode } from "./overlay.js";
import type { OutboxEntry, StoredOutcome } from "./storage.js";
import * as Y from "yjs";

/** What a load did to the confirmed state. */
export interface LoadOutcome {
  /** The load replaced the confirmed state (`resumed = false`, 09 [READ-18]). */
  replaced: boolean;
  /** The load answered a resume check with another epoch. */
  epochChanged: boolean;
  previousEpoch: number;
}

export abstract class ReplicaLayer extends EndingLayer {
  /** The mutation ids for which `overwritten` was emitted, so that a record delivered again emits none (14 [CLI-40]). */
  protected readonly overwrittenSeen = new Set<string>();
  /** The pending actions whose REJECTED DECISION a frame carried: their final rejection is handled after the frame is applied. */
  protected rejectedByRecord: { entry: OutboxEntry; reject: Reject }[] = [];

  protected abstract flushDirty(): Promise<void>;
  protected abstract onRejectResult(e: OutboxEntry, reject: Reject): Promise<void>;
  protected abstract resumeKeyspace(f: Follow): void;
  protected abstract onFoldStop(f: Follow, err: FoldError): void;
  protected abstract onProtocolViolation(v: ProtocolViolation, keyspaces?: readonly string[]): void;
  protected abstract needEpochResolution(f: Follow): void;
  protected abstract onEpochMismatch(f: Follow): void;

  // ---------------------------------------------------------------------------------------------------------------------
  // Lane watermarks (14 [CLI-78])

  /**
   * Takes the lane watermarks of a `MutateResponse`, a load or a heartbeat: the ones for the current `client_id` and the epoch
   * followed for the keyspace; a watermark never decreases ([CLI-78]).
   */
  protected noteWatermarks(wms: readonly LaneWatermark[]): void {
    for (const wm of wms) {
      const f = this.follows.get(wm.keyspaceId);
      if (f === undefined || !f.loaded) continue;
      if (wm.clientId !== this.clientId || wm.logEpoch !== f.logEpoch) continue;
      const lane = wm.lane === Lane.SEQUENCED ? f.lanes.SEQUENCED : wm.lane === Lane.GATED ? f.lanes.GATED : undefined;
      if (lane === undefined) continue;
      if (lane.raise(Number(wm.processedClientSeq))) {
        // 14 [CLI-78]: an action at a slot at or below the watermark has a definitive result; if the client lacks it, it gets it
        // with `GetMutationResult` or by resending the identical content, and never treats the action as unsent
        const laneName = wm.lane === Lane.SEQUENCED ? "SEQUENCED" : "GATED";
        for (const e of this.entriesOf(f.id)) {
          if (e.lane === laneName && e.everSent && e.outcome === undefined && e.clientId === this.clientId && e.clientSeq > 0 && e.clientSeq <= lane.watermark) {
            const rt = this.rt(e.mutationId);
            if (!rt.inflight) rt.nextAttemptAt = 0;
          }
        }
        this.schedulePump();
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Loads (09 §9.2, 14 [CLI-2], [CLI-8])

  /**
   * Installs the pages of a load. A load with `resumed = true` only moves the delivery watermark to the cursor; any other load
   * replaces the confirmed state of the keyspace, and the view documents of its CRDT keys are rebuilt (09 [READ-18], 14 [CLI-13]).
   * `cause` is the `reloaded` cause (A.11). Throws a ProtocolViolation for pages that break 02 [ARC-61] or that no fold can install.
   */
  protected installLoad(f: Follow, pages: readonly LoadStateResponse[], cause: string, resumeCheck = false): LoadOutcome {
    const first = pages[0] as LoadStateResponse;
    this.applyLimits(first.limits);
    const previousEpoch = f.logEpoch;
    f.kind = first.kind;
    f.lifecycle = first.lifecycle === KeyspaceLifecycle.UNSPECIFIED ? KeyspaceLifecycle.ACTIVE : first.lifecycle;
    f.successors = [...first.successors];
    if (first.resumed) {
      // 09 [READ-17]: the client resumes the tail from its cursor
      f.watermark = f.seq;
      this.noteWatermarks(first.laneWatermarks);
      f.invalidate();
      return { replaced: false, epochChanged: false, previousEpoch };
    }
    // 09 [READ-18]: a resume check that came back as a cold load applies the reactions of EPOCH_CHANGED when the epoch differs
    if (resumeCheck && previousEpoch !== 0 && first.logEpoch !== previousEpoch) cause = "EPOCH_CHANGED";
    const st = new ReplicaState();
    const entries: KeyEntry[] = [];
    for (const p of pages) for (const e of p.entries) entries.push(e);
    try {
      for (const e of entries) installEntry(st, e, this.manifests, first.manifestVersion);
    } catch (e) {
      if (e instanceof FoldError) throw new ProtocolViolation("LoadStateResponse", e.message, [f.id]);
      throw e;
    }
    st.setCursor(first.checkpointSeq, first.logEpoch, first.manifestVersion);

    // the CRDT documents of the load: a key that holds a view document keeps it and is rebuilt (14 [CLI-13])
    const manifest = this.manifests.get(first.manifestVersion);
    const isCrdt = (e: KeyEntry): boolean => this.manifests.template(first.manifestVersion, e.key)?.rule === "crdt" || e.encoding === "yjs/v1";
    const docs = f.crdt.install(entries, isCrdt);
    const rebuildKeys = f.crdt.replaceConfirmed(docs);
    const rebuilds = rebuildKeys.map((key) => {
      const roots = manifest?.crdtRoots(key);
      const ck = f.crdt.get(key);
      if (ck !== undefined && roots !== undefined) ck.roots = roots;
      return { key, pending: this.pendingUpdates(f.id, key, new Set()) };
    });

    f.state = st;
    f.loaded = true;
    f.watermark = first.checkpointSeq;
    f.lastRecordHash = undefined;
    f.lastRecordSeq = 0n;
    f.needsLoad = undefined;
    // the lane state belongs to a (client_id, epoch): a new epoch starts it over
    if (first.logEpoch !== previousEpoch && previousEpoch !== 0) {
      f.lanes.SEQUENCED.reset();
      f.lanes.GATED.reset();
    }
    f.invalidate();
    this.noteWatermarks(first.laneWatermarks);
    this.emit({ type: "reloaded", keyspaceId: f.id, cause, logEpoch: first.logEpoch, checkpointSeq: first.checkpointSeq });
    for (const r of rebuilds) {
      const ck = f.crdt.get(r.key);
      if (ck === undefined) continue;
      if (ck.rebuild(r.pending, r.pending, docs.get(r.key) ?? new Y.Doc())) this.emit({ type: "rebuilt", keyspaceId: f.id, key: r.key, cause });
    }
    if (first.skippedUnsupported) {
      f.loadsWithSkipped++;
      this.emit({ type: "skipped", keyspaceId: f.id, seq: first.checkpointSeq, keys: [] });
    }
    f.everLoaded = true;
    return { replaced: true, epochChanged: previousEpoch !== 0 && first.logEpoch !== previousEpoch, previousEpoch };
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Records (09 §9.5, 14 [CLI-2] to [CLI-5])

  /** The subject of a record's principal is this client's (07 [MUT-46], 14 [CLI-7]). */
  private isOurs(rec: RecordMessage): boolean {
    const p = rec.principal;
    return p !== undefined && p.appId === this.appId && p.sub === this.author.sub && p.issuer === this.author.issuer;
  }

  /** Reads what the prediction shows now, so that the records that arrive can be judged against it ([CLI-11], [CLI-38]). */
  private capturePredictions(f: Follow): void {
    if (this.entriesOf(f.id).length === 0) return;
    const v = this.viewOf(f);
    // sticky: an action that was shown as having no effect and then commits is `corrected` ([CLI-11], [CLI-38])
    for (const e of this.entriesOf(f.id)) for (const k of v.noEffectKeys(e.mutationId)) this.rt(e.mutationId).noEffect.add(k);
  }

  /**
   * Applies a `RecordsFrame` (09 [READ-42]): checks that it continues the cursor ([CLI-3]), applies each record by the fold
   * ([CLI-4]), updates the CRDT documents ([CLI-13]), matches the records to the pending actions ([CLI-7]), raises the events
   * ([CLI-5], [CLI-40]), moves the delivery watermark, and retires what the cursor has reached.
   */
  protected async applyFrame(f: Follow, frame: RecordsFrame): Promise<void> {
    if (!f.loaded || f.stopped !== undefined || f.frozen !== undefined || !f.onSession) return;
    if (frame.prevCursor !== f.watermark) {
      // 14 [CLI-3], 09 [READ-46]: the frame is not applied; the keyspace resumes from its own cursor
      if (!f.resuming) this.resumeKeyspace(f);
      return;
    }
    f.resuming = false;
    this.capturePredictions(f);
    for (const rec of frame.records) {
      if (rec.logEpoch !== f.logEpoch) {
        this.onEpochMismatch(f);
        return;
      }
      const ok = await this.applyOne(f, rec);
      if (!ok) return;
    }
    if (frame.cursor > f.watermark) f.watermark = frame.cursor;
    if (frame.cursor > f.state.seq) f.state.setCursor(frame.cursor, f.logEpoch, f.state.manifestVersion);
    await this.afterCursorMoved(f);
    const rejects = this.rejectedByRecord;
    this.rejectedByRecord = [];
    for (const x of rejects) {
      if (this.entries.has(x.entry.mutationId) && !this.ending.has(x.entry.mutationId)) await this.onRejectResult(x.entry, x.reject);
    }
    await this.flushDirty();
  }

  /** Applies one record; returns false when the keyspace stopped. */
  private async applyOne(f: Follow, rec: RecordMessage): Promise<boolean> {
    const st = f.state;
    // 09 [READ-42]: every record touches the interest; a watermark that passes seqs the client does not hold says they do not
    if (rec.seq > st.seq + 1n) st.setCursor(rec.seq - 1n, f.logEpoch, st.manifestVersion);
    const plan: CrdtPlanItem[] = planCrdt(st, this.manifests, rec);
    try {
      applyRecord(st, this.manifests, rec, { supported: (feature) => this.features.includes(feature) });
    } catch (e) {
      if (e instanceof FoldError) {
        if (e.stop) {
          this.onFoldStop(f, e); // 14 [CLI-4]
        } else if (e.code === "seq") {
          if (!f.resuming) this.resumeKeyspace(f);
        } else {
          this.onProtocolViolation(new ProtocolViolation("RecordsFrame", e.message, [f.id]), [f.id]);
        }
        return false;
      }
      throw e;
    }
    // the confirmed documents of the CRDT keys that the record changed ([CLI-13])
    f.crdt.apply(plan);
    f.crdt.reconcile(st);
    if (rec.recordHash.length > 0) {
      f.lastRecordHash = rec.recordHash;
      f.lastRecordSeq = rec.seq;
    }
    if (rec.serverTime !== undefined) this.observeServerTime(Number(rec.serverTime.seconds) * 1000 + Math.floor(rec.serverTime.nanos / 1e6));
    this.recordBookkeeping(f, rec, plan);
    return true;
  }

  /** The events and the matching of a record that the fold applied: `skipped`, `overwritten`, the pending action that it carries. */
  private recordBookkeeping(f: Follow, rec: RecordMessage, plan: readonly CrdtPlanItem[]): void {
    const st = f.state;
    const applied = appliedIndexes(rec);
    // 14 [CLI-5]: a change whose value cannot be decoded is in the confirmed state; its display is skipped
    const undecodable: string[] = [];
    for (const i of applied) {
      const c = rec.changes[i];
      if (c === undefined) continue;
      const row = st.key(c.key);
      if (row !== undefined && row.version === rec.seq && row.state === StateValue && row.rule !== "crdt" && row.rule !== "counter" && !canDecode(row.encoding, row.value, row)) {
        undecodable.push(c.key);
      }
    }
    if (undecodable.length > 0) {
      this.skippedChanges += undecodable.length;
      this.emit({ type: "skipped", keyspaceId: f.id, seq: rec.seq, keys: undecodable });
    }
    // 14 [CLI-40]: `overwritten`, once per mutation id, listing every index
    const subject = rec.principal?.sub ?? "";
    if (rec.overwroteNewer.length > 0 && rec.kind === RecordKind.OP) {
      const seenKey = `${f.id}/${rec.mutationId}`;
      if (!this.overwrittenSeen.has(seenKey)) {
        this.overwrittenSeen.add(seenKey);
        this.emit({ type: "overwritten", mutationId: rec.mutationId, keyspaceId: f.id, seq: rec.seq, keys: rec.overwroteNewer.map((i) => rec.changes[i]?.key ?? ""), by: subject });
      }
    }
    // 14 [CLI-40]: a confirmed OVERRIDE of a CRDT key, while the client has pending updates of it, which merge into it
    for (const i of applied) {
      const c = rec.changes[i];
      if (c === undefined || c.kind !== ChangeKind.OVERRIDE || c.update.length === 0) continue;
      for (const e of this.entriesOf(f.id)) {
        if (this.ending.has(e.mutationId)) continue;
        const m = this.content(e);
        if (m === undefined || !m.changes.some((x) => x.key === c.key && x.kind === ChangeKind.CRDT_UPDATE)) continue;
        const k = `${e.mutationId}@${rec.seq}`;
        if (this.overwrittenSeen.has(k)) continue;
        this.overwrittenSeen.add(k);
        this.emit({ type: "overwritten", mutationId: e.mutationId, keyspaceId: f.id, seq: rec.seq, keys: [c.key], by: subject });
      }
    }
    // 14 [CLI-7]: a confirmed DECISION that carries the action's mutation id is its `Decided`, and a confirmed OP that carries its
    // mutation id from the same subject resolves a SEQUENCED action whose outcome was unknown
    const e = this.entries.get(rec.mutationId);
    if (e !== undefined && (rec.kind === RecordKind.OP || rec.kind === RecordKind.DECISION)) {
      const rt = this.rt(e.mutationId);
      rt.confirmed = { seq: rec.seq, changes: rec.changes };
      for (const item of plan) {
        if (item.verdict !== "APPLIED") rt.crdtNoEffect.add(item.key);
      }
      if (e.outcome === undefined || e.outcome.logEpoch !== rec.logEpoch) {
        if (rec.kind === RecordKind.OP && this.isOurs(rec)) {
          this.setOutcome(e, { kind: "appended", seq: rec.seq, logEpoch: rec.logEpoch, overwroteNewer: [...rec.overwroteNewer] });
        } else if (rec.kind === RecordKind.DECISION && rec.decision !== undefined && e.lane === "GATED" && rec.decision.status !== DecisionStatus.COMMITTED) {
          // 14 [CLI-7]: a REJECTED decision is a final rejection, handled as 14 §14.6 says once the frame is applied
          this.rejectedByRecord.push({ entry: e, reject: rec.decision.reject ?? makeReject(Code.GATE_REJECTED, ErrorClass.DEAD_LETTER, "rejected") });
        } else if (rec.kind === RecordKind.DECISION && rec.decision !== undefined && e.lane === "GATED") {
          const d = rec.decision;
          const out: StoredOutcome = {
            kind: "decided",
            status: d.status === DecisionStatus.COMMITTED ? "COMMITTED" : "REJECTED",
            decisionSeq: rec.seq,
            logEpoch: rec.logEpoch,
            ...(d.reject !== undefined ? { reject: d.reject } : {}),
          };
          this.setOutcome(e, out);
        }
      }
    }
  }

  /** Records the outcome of an action ([CLI-7]); the action retires when the cursor reaches its seq. */
  protected setOutcome(e: OutboxEntry, o: StoredOutcome): void {
    e.outcome = o;
    e.attempts = 0;
    const rt = this.rt(e.mutationId);
    rt.polling = false;
    rt.nextAttemptAt = 0;
    rt.timer?.();
    rt.timer = undefined;
    this.follows.get(e.keyspaceId)?.invalidate();
  }

  /** After the cursor moved: retire what it reached, wake the conflict waits, and let the outbox go on. */
  protected async afterCursorMoved(f: Follow): Promise<void> {
    f.invalidate();
    await this.checkRetirements(f);
    this.wakeCursorWaiters(f);
    this.schedulePump();
  }

  /** Retires every action of the keyspace whose outcome the cursor has reached ([CLI-7]). */
  protected async checkRetirements(f: Follow): Promise<void> {
    for (const e of this.entriesOf(f.id)) {
      const o = e.outcome;
      if (o === undefined || e.stuck !== undefined || this.ending.has(e.mutationId)) continue;
      if (o.kind === "decided" && o.status !== "COMMITTED") continue; // a final rejection, handled as 14 §14.6 says
      if (!f.loaded) continue;
      if (o.logEpoch !== f.logEpoch) {
        // 14 [CLI-7]: an outcome of an earlier epoch counts only after `GetMutationResult` confirms it
        this.needEpochResolution(f);
        continue;
      }
      if (f.state.seq >= this.outcomeSeq(e)) await this.retire(e);
    }
  }

  /** A heartbeat (09 [READ-54]): server time, the delivery watermarks, the lane watermarks. */
  protected async applyHeartbeat(hb: HeartbeatFrame): Promise<void> {
    if (hb.serverTime !== undefined) this.observeServerTime(Number(hb.serverTime.seconds) * 1000 + Math.floor(hb.serverTime.nanos / 1e6));
    const listed = new Set<string>();
    for (const c of hb.cursors) {
      listed.add(c.keyspaceId);
      const f = this.follows.get(c.keyspaceId);
      if (f === undefined || !f.loaded || !f.onSession || f.stopped !== undefined || f.frozen !== undefined) continue;
      if (c.logEpoch !== f.logEpoch) continue;
      if (c.cursorSeq > f.watermark) f.watermark = c.cursorSeq;
      if (c.cursorSeq > f.state.seq) {
        f.state.setCursor(c.cursorSeq, f.logEpoch, f.state.manifestVersion);
        await this.afterCursorMoved(f);
      }
    }
    // 09 [READ-54]: a followed keyspace that a heartbeat does not list is removed from the session
    for (const f of this.follows.values()) {
      if (f.onSession && !listed.has(f.id)) {
        f.onSession = false;
        this.resumeKeyspace(f);
      }
    }
    this.noteWatermarks(hb.laneWatermarks);
    await this.purgeJournal().catch(() => undefined);
  }
}
