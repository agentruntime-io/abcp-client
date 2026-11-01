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
import { Code, ErrorClass, Lane, RecordKind, ChangeKind, DecisionStatus, type KeyChange, type KeyEntry, type Record as RecordMessage, type Reject, type SuccessorRule, KeyspaceLifecycle } from "../gen/abpc/v2/types_pb.js";
import { makeReject, sameBytes } from "./endings.js";
import { FoldError, installEntry, ReplicaState, StateValue, type Anomaly } from "../rules/fold/index.js";
import { matchSuccessor } from "./successors.js";
import { BoundedSet } from "./bounded.js";
import { ProtocolViolation } from "./errors.js";
import { ReplicaStoreLayer } from "./replica-store.js";
import { appliedIndexes, type CrdtPlanItem } from "./crdt-view.js";
import type { Follow } from "./follow.js";
import { canDecode } from "./overlay.js";
import type { OutboxEntry, StoredOutcome, Verdict } from "./storage.js";
import * as Y from "yjs";

/** What a load did to the confirmed state. */
export interface LoadOutcome {
  /** The load replaced the confirmed state (`resumed = false`, 09 [READ-18]). */
  replaced: boolean;
  /** The load answered a resume check with another epoch. */
  epochChanged: boolean;
  previousEpoch: number;
}

export abstract class ReplicaLayer extends ReplicaStoreLayer {
  /** The mutation ids for which `overwritten` was emitted, so that a record delivered again emits none (14 [CLI-40]). */
  protected readonly overwrittenSeen = new BoundedSet<string>(1024);
  /** The pending actions whose REJECTED DECISION a frame carried: their final rejection is handled after the frame is applied. */
  protected rejectedByRecord: { entry: OutboxEntry; reject: Reject }[] = [];

  protected abstract flushDirty(): Promise<void>;
  protected abstract markDirty(e: OutboxEntry): void;
  /** `decided`: the reject is that of a `Decided{REJECTED}` (or of the DECISION record), which is final whatever its class (15 [COMPAT-39], 14 [CLI-34]). */
  protected abstract onRejectResult(e: OutboxEntry, reject: Reject, decided?: boolean): Promise<void>;
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
      const rose = lane.raise(Number(wm.processedClientSeq));
      lane.settle(); // 14 [CLI-27]: a slot that an ended action left, and the reading leaves unconsumed, goes to the next action
      if (rose) {
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
      // 09 [READ-17]: the client resumes the tail from its cursor: from the watermark that it asked about (the cursor, but after a widening
      // that re-added the keyspace below it, 14 [CLI-72])
      f.watermark = first.checkpointSeq;
      f.needsLoad = undefined;
      f.lastSyncMs = this.serverNow();
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
    f.purged = false;
    f.outside.clear(); // the versions of keys outside the interest that were loaded belong to the state that was replaced
    f.revoked = false;
    f.lastSyncMs = this.serverNow();
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

  /**
   * Reads what the prediction shows now, so that the records that arrive can be judged against it ([CLI-11], [CLI-38]). What it learns
   * is kept in the outbox entry, which is written with the next transaction of its task.
   */
  protected capturePredictions(f: Follow): void {
    if (this.entriesOf(f.id).length === 0) return;
    const v = this.viewOf(f);
    // sticky: an action that was shown as having no effect and then commits is `corrected` ([CLI-11], [CLI-38])
    for (const e of this.entriesOf(f.id)) this.noteVerdict(e, { noEffect: v.noEffectKeys(e.mutationId) });
  }

  /** Adds to what the client has learned of how the applied result differs from the prediction, and marks the entry to be written. */
  protected noteVerdict(e: OutboxEntry, add: { noEffect?: readonly string[]; crdtNoEffect?: readonly string[]; effects?: readonly string[] }): void {
    const v: Verdict = e.verdict ?? { noEffect: [], crdtNoEffect: [] };
    let changed = e.verdict === undefined && (add.noEffect?.length ?? 0) + (add.crdtNoEffect?.length ?? 0) > 0;
    for (const k of add.noEffect ?? []) if (!v.noEffect.includes(k)) (v.noEffect.push(k), (changed = true));
    for (const k of add.crdtNoEffect ?? []) if (!v.crdtNoEffect.includes(k)) (v.crdtNoEffect.push(k), (changed = true));
    if (add.effects !== undefined) {
      const have = v.effects ?? [];
      for (const k of add.effects) if (!have.includes(k)) have.push(k);
      if (v.effects === undefined || have.length !== v.effects.length) changed = true;
      v.effects = have;
    }
    if (!changed) return;
    e.verdict = v;
    this.markDirty(e);
  }

  /**
   * The keys of a gate's effects that the prediction lacked or got wrong, from the DECISION record that made them (14 [CLI-15], [CLI-38]):
   * the changes after the requested ones, but the intent row; placeholders unless the app registered a predictor for the op type.
   */
  private effectKeysOf(e: OutboxEntry, changes: readonly KeyChange[]): string[] {
    const m = this.content(e);
    if (m === undefined) return [];
    const effects = changes.slice(m.changes.length).filter((c) => c.key !== `abpc.intent/${e.mutationId}`);
    const predictor = this.predictors[e.opType];
    let predicted: KeyChange[] = [];
    if (predictor !== undefined) {
      try {
        predicted = predictor(this.snapshot(e)).map((c) => this.toKeyChange(c));
      } catch {
        predicted = [];
      }
    }
    const out: string[] = [];
    for (const c of effects) {
      const p = predicted.find((x) => x.key === c.key);
      if (p === undefined || p.kind !== c.kind || !sameBytes(p.value, c.value) || p.delta !== c.delta) out.push(c.key);
    }
    return out;
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
    const applied: RecordMessage[] = [];
    for (const rec of frame.records) {
      if (rec.logEpoch !== f.logEpoch) {
        this.onEpochMismatch(f);
        return;
      }
      const ok = await this.applyOne(f, rec, applied);
      if (!ok) {
        await this.persistRecords(f, applied); // the records before the one that stopped are applied: they are persisted as they are
        return;
      }
    }
    if (frame.cursor > f.watermark) f.watermark = frame.cursor;
    if (frame.cursor > f.state.seq) f.state.setCursor(frame.cursor, f.logEpoch, f.state.manifestVersion);
    f.lastSyncMs = this.serverNow();
    // 14 [CLI-2]: the records join the persisted replica before the actions that they retire leave the outbox, so that a restart never
    // finds an action retired whose record it has not got
    await this.persistRecords(f, applied);
    await this.afterCursorMoved(f);
    const rejects = this.rejectedByRecord;
    this.rejectedByRecord = [];
    for (const x of rejects) {
      if (this.entries.has(x.entry.mutationId) && !this.ending.has(x.entry.mutationId)) await this.onRejectResult(x.entry, x.reject, true);
    }
    await this.flushDirty();
  }

  /** Applies one record; returns false when the keyspace stopped. */
  private async applyOne(f: Follow, rec: RecordMessage, applied: RecordMessage[]): Promise<boolean> {
    const st = f.state;
    let plan: CrdtPlanItem[];
    let anomalies: Anomaly[];
    try {
      ({ plan, anomalies } = this.foldRecord(f, rec));
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
    applied.push(rec);
    if (anomalies.length > 0) this.noteAnomalies(f, rec.seq, anomalies);
    if (rec.recordHash.length > 0) {
      f.lastRecordHash = rec.recordHash;
      f.lastRecordSeq = rec.seq;
    }
    if (rec.serverTime !== undefined) this.observeServerTime(Number(rec.serverTime.seconds) * 1000 + Math.floor(rec.serverTime.nanos / 1e6));
    this.recordBookkeeping(f, rec, plan);
    return true;
  }

  /**
   * The changes that a record carried and that their kind does not define (06 [LOG-41]) are counted per keyspace and reported to the app
   * (02 [ARC-46]): the engine handles them as 14 [CLI-5] says, with `skipped`, and they count among the skipped changes of the sync status.
   */
  private noteAnomalies(f: Follow, seq: bigint, anomalies: readonly Anomaly[]): void {
    f.anomalies += anomalies.length;
    this.skippedChanges += anomalies.length;
    this.emit({ type: "skipped", keyspaceId: f.id, seq, keys: anomalies.map((a) => a.key), cause: "ANOMALY", anomalies: anomalies.map((a) => ({ index: a.index, key: a.key, rule: a.rule, detail: a.detail })) });
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
    const recMs = rec.serverTime !== undefined ? Number(rec.serverTime.seconds) * 1000 + Math.floor(rec.serverTime.nanos / 1e6) : undefined;
    if (e !== undefined && e.outcome !== undefined && recMs !== undefined && (rec.kind === RecordKind.OP || rec.kind === RecordKind.DECISION) && e.outcome.logEpoch === rec.logEpoch) {
      // 14 [CLI-53]: the window of the journal runs from the outcome; the record's time is the outcome's, the result's arrival is not earlier
      e.outcome.atServerMs = Math.min(e.outcome.atServerMs ?? recMs, recMs);
    }
    if (e !== undefined && (rec.kind === RecordKind.OP || rec.kind === RecordKind.DECISION)) {
      // 14 [CLI-38], [CLI-40]: how the applied record differs from the prediction, kept with the entry
      this.noteVerdict(e, { crdtNoEffect: plan.filter((item) => item.verdict !== "APPLIED").map((item) => item.key) });
      if (rec.kind === RecordKind.DECISION && e.lane === "GATED" && rec.decision?.status === DecisionStatus.COMMITTED) this.noteVerdict(e, { effects: this.effectKeysOf(e, rec.changes) });
      if (e.outcome === undefined || e.outcome.logEpoch !== rec.logEpoch) {
        if (rec.kind === RecordKind.OP && this.isOurs(rec)) {
          this.setOutcome(e, { kind: "appended", seq: rec.seq, logEpoch: rec.logEpoch, overwroteNewer: [...rec.overwroteNewer], ...(recMs !== undefined ? { atServerMs: recMs } : {}) });
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
            ...(recMs !== undefined ? { atServerMs: recMs } : {}),
          };
          this.setOutcome(e, out);
        }
      }
    }
  }

  /** Records the outcome of an action ([CLI-7]); the action retires when the cursor reaches its seq. */
  protected setOutcome(e: OutboxEntry, o: StoredOutcome): void {
    e.outcome = o.atServerMs === undefined ? { ...o, atServerMs: this.serverNow() } : o;
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
    // 14 [CLI-7]: the actions of a split source retire once the successors that take their keys are loaded at their seq
    for (const src of this.follows.values()) {
      if (src !== f && src.stopped === "MOVED" && src.successors.some((r) => r.keyspaceId === f.id)) await this.checkRetirements(src);
    }
    this.wakeCursorWaiters(f);
    this.schedulePump();
  }

  /**
   * Whether a stop of the following, rather than the cursor, decides the retirement of `e` (14 [CLI-7]): "When the client stops
   * following the keyspace (`ACCESS_REVOKED`, `GRANT_CHANGED`, `MOVED`, `ARCHIVED`, `DELETED` or `NOT_FOUND`), its actions with a
   * successful outcome in the epoch of its cursor retire at once"; for `MOVED`, "each retires once every successor that the client
   * loads and whose rule takes any of its keys is loaded at a `checkpoint_seq` at or above its seq, and at once when there is no such
   * successor". The cursor of a stopped keyspace never moves, so an outcome that arrives after the stop (a poll, a resend) is judged
   * here. `undefined`: the keyspace is followed, and the cursor decides.
   */
  protected retiresOnStop(f: Follow, e: OutboxEntry): boolean | undefined {
    switch (f.stopped) {
      case "ACCESS_REVOKED":
      case "SUBJECT_REVOKED":
      case "GRANT_CHANGED":
      case "ARCHIVED":
      case "DELETED":
      case "NOT_FOUND":
        return true;
      case "MOVED": {
        const seq = this.outcomeSeq(e);
        const m = this.content(e);
        const keys: string[] = [];
        if (m !== undefined) {
          for (const c of m.changes) keys.push(c.key);
          for (const p of m.preconditions) if (p.target.case === "key") keys.push(p.target.value); // a precondition on a prefix names no key (12 [ADM-45])
        }
        const rules = new Set<SuccessorRule>();
        for (const k of keys) {
          const r = matchSuccessor(f.successors, k);
          if (r !== undefined) rules.add(r);
        }
        for (const r of rules) {
          const sf = this.follows.get(r.keyspaceId);
          if (sf === undefined) continue; // the client does not load this successor
          if (!sf.loaded || sf.state.seq < seq) return false;
        }
        return true;
      }
      default:
        return undefined;
    }
  }

  /** Retires every action of the keyspace whose outcome the cursor has reached, or that a stop of the following retires ([CLI-7]). */
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
      const onStop = this.retiresOnStop(f, e);
      if (onStop === true || (onStop === undefined && f.state.seq >= this.outcomeSeq(e))) await this.retire(e);
    }
  }

  /** A heartbeat (09 [READ-54]): server time, the delivery watermarks, the lane watermarks. */
  protected async applyHeartbeat(hb: HeartbeatFrame): Promise<void> {
    if (hb.serverTime !== undefined) this.observeServerTime(Number(hb.serverTime.seconds) * 1000 + Math.floor(hb.serverTime.nanos / 1e6));
    const listed = new Set<string>();
    for (const c of hb.cursors) {
      listed.add(c.keyspaceId);
      const f = this.follows.get(c.keyspaceId);
      if (f === undefined || !f.loaded || !f.onSession || f.stopped !== undefined || f.frozen !== undefined || f.resuming) continue; // a resuming keyspace chains from its own cursor
      if (c.logEpoch !== f.logEpoch) continue;
      f.lastSyncMs = this.serverNow();
      if (c.cursorSeq > f.watermark) f.watermark = c.cursorSeq;
      if (c.cursorSeq > f.state.seq) {
        f.state.setCursor(c.cursorSeq, f.logEpoch, f.state.manifestVersion);
        await this.persistHead(f, 30_000, true);
        await this.afterCursorMoved(f);
      } else await this.persistHead(f);
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
