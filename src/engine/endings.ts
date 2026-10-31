// How an action leaves the outbox: it retires with a successful outcome ([CLI-7]), or it ends with a final rejection, to the
// dead-letter store ([CLI-28], [CLI-46]), to the conflict handler ([CLI-47]), to the export hook ([CLI-57], [CLI-83]), or by the
// user's cancel ([CLI-81]); and what happens to the actions that depend on it ([CLI-34]).
//
// The events of one result come in the order of 14 [CLI-38]: `reverted`; `resent` or `conflict`; `rejected` or
// `outcome-unknown`; `dead-lettered` or `exported`. The callers emit the first two, `endAction` the rest. Every move to a store
// is one local transaction ([CLI-28], [CLI-53]); the events follow it.

import { create } from "@bufbuild/protobuf";
import { Code, ErrorClass, RejectSchema, ChangeKind, type KeyChange, type Reject, type Stale } from "../gen/abpc/v2/types_pb.js";
import { codeName } from "./codes.js";
import { EngineCore } from "./core.js";
import type { Follow } from "./follow.js";
import { StoreFull, type DeadLetterEntry, type ExportEntry, type JournalEntry, type OutboxEntry, type RejectionRecord, type StorageOp } from "./storage.js";
import type { ConflictContext, ExportRecord, SubmitOptions, SubmitResult, ActionInput } from "./types.js";

/** A reject that the client itself makes (`DEPENDENCY_FAILED`, a local `CANCELLED`, `PAYLOAD_TOO_LARGE` for a split that cannot go on). */
export function makeReject(code: Code, errorClass: ErrorClass, message: string, details: Record<string, string> = {}): Reject {
  return create(RejectSchema, { code, errorClass, message, details });
}

/** What happens to an action that ends (see `endAction`). */
export interface EndSpec {
  /** The store that it goes to: the dead-letter store, the export hook, or none (a `CONFLICT`, a cancel: the rejection record only). */
  sink: "deadLetter" | "export" | "none";
  reject: Reject | undefined;
  /** The reason that the app is told, and the store entry keeps. */
  reason: string;
  /**
   * What becomes of the slot of the ended action (14 [CLI-27]), which the lane's watermark decides and never the code of a result:
   * `release`: the server recorded nothing for the action (the client ended it itself, or the call never reached the server), so the
   * slot goes to the next action of the lane unless the watermark has passed it. By default the watermark that came with the result of
   * the same `Mutate` response decides; with no such watermark in hand the slot waits for the next reading.
   */
  slot?: "release";
  /** Report `outcome-unknown` with this cause (A.11) instead of `rejected` (14 [CLI-83]). */
  outcomeUnknown?: string;
  /** The actions that depend on it: they fail ([CLI-34]), are left alone, or move to the replacement that the user chose ([CLI-47]). */
  dependents: "fail" | "keep" | { replacedBy: string };
  /** The `rebuilt` cause of CRDT views; default the mutation id. */
  rebuildCause?: string;
  /** For a dependent: the action whose end made it fail. */
  dependencyOf?: string;
  /** The action is a journalled one that a restore lost (14 [CLI-53]): it is not in the outbox, and its journal entry goes in the same transaction. */
  journal?: boolean;
}

export abstract class EndingLayer extends EngineCore {
  /** Entries whose final rejection awaits the app's conflict handler: they no longer shape the view ([CLI-47]) and are never sent. */
  protected readonly ending = new Set<string>();
  /** The lane watermarks of the `Mutate` response that is being applied have been taken: they are after the results that it carries ([CLI-27]). */
  protected freshReading = false;
  /** The conflict flows under way (a handler may take as long as the user does), by mutation id: the epoch flow waits for one before it acts on the next answer (14 [CLI-53]). */
  protected readonly conflictFlows = new Map<string, Promise<void>>();
  /** Waiters for the cursor of a keyspace to reach a seq (the hold `CONFLICT_WAIT`). */
  protected readonly cursorWaiters = new Map<string, Set<{ need: bigint; wake: () => void }>>();

  protected isExcludedFromOverlay(e: OutboxEntry): boolean {
    return this.ending.has(e.mutationId) || e.stuck !== undefined;
  }

  protected abstract submitAction(input: ActionInput, options?: SubmitOptions): Promise<SubmitResult>;

  // ---------------------------------------------------------------------------------------------------------------------
  // Dependents (14 [CLI-34])

  /** The actions that depend on `a`, transitively, in outbox order (14 [CLI-30]). */
  protected dependentsOf(a: OutboxEntry): OutboxEntry[] {
    const closure = new Set<string>([a.mutationId]);
    const out: OutboxEntry[] = [];
    for (const b of this.entriesOf(a.keyspaceId)) {
      if (b.mutationId === a.mutationId) continue;
      if (b.dependsOn.some((d) => closure.has(d))) {
        closure.add(b.mutationId);
        out.push(b);
      }
    }
    return out;
  }

  /** An action that fails with `DEPENDENCY_FAILED`: never sent, no definitive result, not already ending ([CLI-34]). */
  protected failable(b: OutboxEntry): boolean {
    return !b.everSent && !b.registered && b.outcome === undefined && !this.ending.has(b.mutationId) && b.stuck === undefined;
  }

  protected lastResultText(e: OutboxEntry): string | undefined {
    return e.lastResult === undefined ? undefined : `${e.lastResult.kind}: ${e.lastResult.what}`;
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Ending an action

  /**
   * Ends an action: removes it from the outbox in one local transaction with its record in the store that it goes to, with its
   * dependents ([CLI-34]); then emits the events in the order of [CLI-38] and rebuilds the view documents of its CRDT keys
   * ([CLI-13]). When the store is full ([CLI-84]), the action stays in the outbox, final and never sent again.
   */
  protected async endAction(e: OutboxEntry, spec: EndSpec): Promise<void> {
    if (spec.journal !== true && !this.entries.has(e.mutationId)) return;
    const f = this.follows.get(e.keyspaceId);
    const now = this.serverNow();
    let deps: OutboxEntry[] = [];
    const ops: StorageOp[] = [];
    if (spec.dependents === "fail") {
      deps = this.dependentsOf(e).filter((b) => this.failable(b));
    } else if (typeof spec.dependents === "object") {
      // 14 [CLI-34], [CLI-47]: the dependents of an action that the user replaced depend on the replacement
      const repl = spec.dependents.replacedBy;
      for (const b of this.entriesOf(e.keyspaceId)) {
        if (b.mutationId !== repl && b.dependsOn.includes(e.mutationId)) {
          b.dependsOn = [...new Set(b.dependsOn.map((d) => (d === e.mutationId ? repl : d)))];
          ops.push(this.putOp(b));
        }
      }
    }
    const group = [e, ...deps];
    // the sink of a dependent is the one of the action, and a dependent that fails goes to the dead-letter store otherwise (14 [CLI-34])
    const depSink: "deadLetter" | "export" = spec.sink === "export" ? "export" : "deadLetter";
    const items: { entry: OutboxEntry; reject: Reject | undefined; reason: string; sink: "deadLetter" | "export" | "none"; dependencyOf?: string; unknown?: string }[] = [
      { entry: e, reject: spec.reject, reason: spec.reason, sink: spec.sink, ...(spec.outcomeUnknown !== undefined ? { unknown: spec.outcomeUnknown } : {}) },
      ...deps.map((d) => ({
        entry: d,
        reject: makeReject(Code.DEPENDENCY_FAILED, ErrorClass.DEAD_LETTER, `depends on ${e.mutationId}, which ended`, { dependency: e.mutationId }),
        reason: codeName(Code.DEPENDENCY_FAILED),
        sink: depSink,
        dependencyOf: e.mutationId,
      })),
    ];
    for (const it of items) {
      const en = it.entry;
      ops.push(it === items[0] && spec.journal === true ? { op: "deleteJournal", mutationId: en.mutationId } : { op: "deleteEntry", mutationId: en.mutationId });
      if (it.sink === "deadLetter") {
        const dl: DeadLetterEntry = {
          mutationId: en.mutationId,
          keyspaceId: en.keyspaceId,
          opType: en.opType,
          content: en.content,
          author: en.author,
          reject: it.reject ?? makeReject(Code.UNSPECIFIED, ErrorClass.DEAD_LETTER, it.reason),
          reason: it.reason,
          attempts: en.attempts,
          firstFailureMs: en.firstFailureMs ?? now,
          lastFailureMs: now,
          failedDependents: it === items[0] ? deps.map((d) => d.mutationId) : [],
          ...(it.dependencyOf !== undefined ? { dependencyOf: it.dependencyOf } : {}),
        };
        ops.push({ op: "putDeadLetter", entry: dl });
      } else if (it.sink === "export") {
        const ex: ExportEntry = {
          mutationId: en.mutationId,
          keyspaceId: en.keyspaceId,
          opType: en.opType,
          content: en.content,
          author: en.author,
          reason: it.reason,
          ...(it.reject !== undefined ? { reject: it.reject } : {}),
          ...(this.lastResultText(en) !== undefined ? { lastResult: this.lastResultText(en) as string } : {}),
          ...(it.dependencyOf !== undefined ? { dependencyOf: it.dependencyOf } : {}),
          atMs: now,
        };
        ops.push({ op: "putExport", entry: ex });
      }
      if (it.unknown === undefined) {
        const rej: RejectionRecord = {
          mutationId: en.mutationId,
          keyspaceId: en.keyspaceId,
          opType: en.opType,
          content: en.content,
          ...(it.reject !== undefined ? { reject: it.reject } : {}),
          atMs: now,
        };
        ops.push({ op: "putRejection", entry: rej });
      }
    }
    // the CRDT views to rebuild: the keys that the ending actions' updates touch (14 [CLI-13]); their lists before and after
    const rebuilds = f === undefined ? [] : this.planRebuilds(f, group);

    try {
      await this.tx(ops);
    } catch (err) {
      if (err instanceof StoreFull && err.store !== "outbox") {
        // 14 [CLI-84]: the entry stays in the outbox, final and never sent again, until the app acknowledges entries of that store
        e.stuck = { sink: err.store === "exports" ? "export" : "deadLetter", reason: spec.reason, ...(spec.reject !== undefined ? { reject: spec.reject } : {}), ...(spec.outcomeUnknown !== undefined ? { cause: spec.outcomeUnknown } : {}), ...(spec.dependencyOf !== undefined ? { dependencyOf: spec.dependencyOf } : {}) };
        this.ending.delete(e.mutationId);
        try {
          await this.tx([this.putOp(e)]);
        } catch {
          // the entry stays in memory with its marker; the next start finds it final through the server's stored outcome
        }
        f?.invalidate();
        return;
      }
      throw err;
    }
    // the transaction is done: mirror it, then tell the app
    const exported: ExportRecord[] = [];
    for (const it of items) {
      const en = it.entry;
      if (it === items[0] && spec.journal === true) this.journal.delete(en.mutationId);
      this.entries.delete(en.mutationId);
      this.ending.delete(en.mutationId);
      // 14 [CLI-27]: whether the slot is consumed is the watermark's to say, never the result's code: a slot at or below the watermark is
      // consumed; one just above the watermark, after the action got its result, recorded nothing and goes to the next action of the lane.
      // The watermark of the `Mutate` response that carried the result is in hand (`freshReading`), and so is the fact that a reject that
      // the client made itself recorded nothing; any other end waits for the next reading, and so does an action reported as outcome
      // unknown, which may have consumed its slot (14 [CLI-83]).
      this.settleSlot(en, spec.slot === "release" || (this.freshReading && it.unknown === undefined) ? "decide" : "wait");
      this.dropRuntime(en.mutationId);
    }
    for (const op of ops) {
      if (op.op === "putDeadLetter") this.deadLetters.set(op.entry.mutationId, op.entry);
      else if (op.op === "putExport") {
        this.exports.set(op.entry.mutationId, op.entry);
        exported.push({ mutationId: op.entry.mutationId, keyspaceId: op.entry.keyspaceId, opType: op.entry.opType, content: op.entry.content, reason: op.entry.reason, reject: op.entry.reject, lastResult: op.entry.lastResult, dependencyOf: op.entry.dependencyOf });
      } else if (op.op === "putRejection") this.rejections.set(op.entry.mutationId, op.entry);
    }
    f?.invalidate();
    for (const it of items) {
      const en = it.entry;
      if (it.unknown !== undefined) {
        const ev: import("./types.js").OutcomeUnknownEvent = { type: "outcome-unknown", mutationId: en.mutationId, keyspaceId: en.keyspaceId, cause: it.unknown };
        const lr = this.lastResultText(en);
        if (lr !== undefined) ev.lastResult = lr;
        this.emit(ev);
      } else {
        this.emit({ type: "rejected", mutationId: en.mutationId, keyspaceId: en.keyspaceId, ...(it.reject !== undefined ? { reject: it.reject } : {}) });
      }
      if (it.sink === "deadLetter") this.emit({ type: "dead-lettered", mutationId: en.mutationId, keyspaceId: en.keyspaceId, reason: it.reason });
      else if (it.sink === "export") this.emit({ type: "exported", mutationId: en.mutationId, keyspaceId: en.keyspaceId, reason: it.reason });
    }
    for (const r of rebuilds) this.performRebuild(f as Follow, r, spec.rebuildCause ?? e.mutationId);
    for (const x of exported) this.callExportHook(x);
    this.schedulePump();
  }

  protected callExportHook(x: ExportRecord): void {
    const hook = this.exportHook;
    if (hook === undefined) return;
    this.detach(
      (async () => {
        try {
          await hook(x);
        } catch {
          // the entry stays durable until the app acknowledges it (14 [CLI-83])
        }
      })(),
    );
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // CRDT views of ended actions (14 [CLI-13])

  /** The pending Yjs updates of a CRDT key, in outbox order, leaving out the given actions. */
  protected pendingUpdates(keyspaceId: string, key: string, exclude: ReadonlySet<string>): Uint8Array[] {
    const out: Uint8Array[] = [];
    for (const e of this.entriesOf(keyspaceId)) {
      if (exclude.has(e.mutationId)) continue;
      const m = this.content(e);
      if (m === undefined) continue;
      for (const c of m.changes) {
        if (c.key === key && (c.kind === ChangeKind.CRDT_UPDATE || (c.kind === ChangeKind.OVERRIDE && c.update.length > 0))) out.push(c.update);
      }
    }
    return out;
  }

  /** For each CRDT key with a view document that the ended actions touch: the pending updates before and after. */
  protected planRebuilds(f: Follow, ended: readonly OutboxEntry[]): { key: string; before: Uint8Array[]; after: Uint8Array[] }[] {
    const ids = new Set(ended.map((x) => x.mutationId));
    const keys = new Set<string>();
    for (const en of ended) {
      const m = this.content(en);
      if (m === undefined) continue;
      for (const c of m.changes) {
        if (c.kind === ChangeKind.CRDT_UPDATE || (c.kind === ChangeKind.OVERRIDE && c.update.length > 0)) keys.add(c.key);
      }
    }
    const out: { key: string; before: Uint8Array[]; after: Uint8Array[] }[] = [];
    for (const key of keys) {
      const ck = f.crdt.get(key);
      if (ck === undefined || ck.view === null) continue;
      out.push({ key, before: this.pendingUpdates(f.id, key, new Set()), after: this.pendingUpdates(f.id, key, ids) });
    }
    return out;
  }

  /** Rebuilds the view document of a key and emits `rebuilt` once ([CLI-13]). */
  protected performRebuild(f: Follow, r: { key: string; before: Uint8Array[]; after: Uint8Array[] }, cause: string): void {
    const ck = f.crdt.get(r.key);
    if (ck === undefined) return;
    if (ck.rebuild(r.before, r.after)) this.emit({ type: "rebuilt", keyspaceId: f.id, key: r.key, cause });
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Slots

  /** An action that leaves the outbox gives up its slot, when the slot was not consumed ([CLI-27]). */
  protected releaseSlot(e: OutboxEntry): void {
    if (e.clientSeq === 0) return;
    const f = this.follows.get(e.keyspaceId);
    f?.lanes[e.lane].release(e.clientSeq, e.mutationId);
  }

  /**
   * The slot of an action that left the outbox (14 [CLI-27]): at or below the lane's watermark it is consumed and nothing is left to do;
   * above it, `decide`: the watermark in hand is after the result, so the slot recorded nothing and is given up; `wait`: a later reading
   * of the watermark decides (`LaneState.settle`).
   */
  protected settleSlot(e: OutboxEntry, how: "decide" | "wait"): void {
    if (e.clientSeq === 0) return;
    const lane = this.follows.get(e.keyspaceId)?.lanes[e.lane];
    if (lane === undefined || e.clientSeq <= lane.watermark) return;
    if (how === "decide") lane.release(e.clientSeq, e.mutationId);
    else lane.defer(e.clientSeq, e.mutationId);
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Retiring (14 [CLI-7], [CLI-53])

  /** The seq that an outcome names, which the cursor must reach. */
  protected outcomeSeq(e: OutboxEntry): bigint {
    const o = e.outcome;
    if (o === undefined) return 0n;
    return o.kind === "appended" ? o.seq : o.decisionSeq;
  }

  /**
   * Retires an action with a successful outcome ([CLI-7]): it leaves the overlay, the journal gets its content in the same
   * transaction ([CLI-53]), and the app is told `committed`, or `corrected` when what was applied differs from the prediction.
   */
  protected async retire(e: OutboxEntry): Promise<void> {
    const o = e.outcome;
    if (o === undefined || !this.entries.has(e.mutationId)) return;
    if (o.kind === "decided" && o.status !== "COMMITTED") return;
    const f = this.follows.get(e.keyspaceId);
    const seq = this.outcomeSeq(e);
    // what was applied against what the prediction showed ([CLI-38], [CLI-40]): the verdict that the entry keeps, which survives a restart
    const keys = new Set<string>();
    for (const k of e.verdict?.noEffect ?? []) keys.add(k); // a CAS that was predicted to have no effect ([CLI-11])
    let cause: "NO_EFFECT" | undefined;
    for (const k of e.verdict?.crdtNoEffect ?? []) {
      keys.add(k); // a CRDT update that folded to no effect (05 [STATE-76], [CLI-40])
      cause = "NO_EFFECT";
    }
    // the gate's effects (14 [CLI-15]): from the DECISION that the client saw; for a decision that it never saw (it came while the engine
    // was stopped, and a load installed its result), the keys that the decision changed besides the requested ones
    const effects = e.verdict?.effects ?? (f !== undefined && o.kind === "decided" && o.status === "COMMITTED" ? this.effectKeysFromState(f, e, o.decisionSeq) : []);
    for (const k of effects) keys.add(k);
    const reported: "committed" | "corrected" = keys.size > 0 ? "corrected" : "committed";
    const journal: JournalEntry = {
      mutationId: e.mutationId,
      keyspaceId: e.keyspaceId,
      opType: e.opType,
      content: e.content,
      author: e.author,
      outcome: o,
      reported,
      retiredServerMs: this.serverNow(),
      outcomeServerMs: o.atServerMs ?? this.serverNow(),
      spend: this.opInfoFor(e)?.spend ?? false,
      spendResent: e.spendResent,
      lane: e.lane,
      ordinal: e.ordinal,
      verifiedEpoch: o.logEpoch,
    };
    try {
      await this.tx([{ op: "deleteEntry", mutationId: e.mutationId }, { op: "putJournal", entry: journal }]);
    } catch (err) {
      if (err instanceof StoreFull && err.store === "journal") {
        // 14 [CLI-84]: it stays in the outbox, final and never sent again, until its journal entry can be written
        e.stuck = { sink: "journal", reason: reported };
        f?.invalidate();
        this.reportRetired(e, reported, keys, cause, seq);
        try {
          await this.tx([this.putOp(e)]);
        } catch {
          // kept in memory
        }
        return;
      }
      throw err;
    }
    this.entries.delete(e.mutationId);
    this.journal.set(e.mutationId, journal);
    this.dropRuntime(e.mutationId); // the slot is consumed: it stays with the lane until the watermark passes it (14 [CLI-27])
    f?.invalidate();
    this.reportRetired(e, reported, keys, cause, seq);
    this.schedulePump();
  }

  /**
   * The keys of the effects of a GATED decision that the client never saw as a record (14 [CLI-15]): those that the confirmed state holds
   * at the version of the decision, other than the requested ones and the intent row, which are not the prediction's unless a predictor
   * of the op type predicted that key with that value. A key that a later record changed is not found, so the verdict is never too high.
   */
  private effectKeysFromState(f: Follow, e: OutboxEntry, decisionSeq: bigint): string[] {
    const m = this.content(e);
    if (m === undefined) return [];
    const requested = new Set(m.changes.map((c) => c.key));
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
    const st = f.state;
    for (const key of [...st.keyNames(), ...st.tombstoneKeys()]) {
      if (requested.has(key) || key === `abpc.intent/${e.mutationId}`) continue;
      const row = st.key(key);
      const version = row !== undefined ? row.version : (st.tombstone(key)?.version ?? 0n);
      if (version !== decisionSeq) continue;
      const p = predicted.find((x) => x.key === key);
      const same = p !== undefined && (row === undefined ? p.kind === ChangeKind.DELETE : p.kind === ChangeKind.SET && sameBytes(p.value, row.value));
      if (!same) out.push(key);
    }
    return out;
  }

  private reportRetired(e: OutboxEntry, reported: "committed" | "corrected", keys: Set<string>, cause: "NO_EFFECT" | undefined, seq: bigint): void {
    if (reported === "committed") this.emit({ type: "committed", mutationId: e.mutationId, keyspaceId: e.keyspaceId, seq });
    else this.emit({ type: "corrected", mutationId: e.mutationId, keyspaceId: e.keyspaceId, seq, keys: [...keys], ...(cause !== undefined ? { cause } : {}) });
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // The conflict handler (14 [CLI-47])

  /**
   * A `CONFLICT`-class result is the action's final outcome: `conflict`, then the handler, then `rejected` ([CLI-38], [CLI-47]).
   * The action no longer shapes the view; the handler waits for the cursor to reach `Stale.current_seq` (the hold `CONFLICT_WAIT`),
   * and runs outside the queue, so that it may call the API.
   */
  protected beginConflict(e: OutboxEntry, reject: Reject, opts: { journal?: boolean } = {}): void {
    if (this.ending.has(e.mutationId)) return;
    this.ending.add(e.mutationId);
    this.follows.get(e.keyspaceId)?.invalidate();
    const ev: import("./types.js").ConflictEvent = { type: "conflict", mutationId: e.mutationId, keyspaceId: e.keyspaceId, reject };
    if (reject.stale !== undefined) ev.stale = reject.stale;
    this.emit(ev);
    const id = e.mutationId;
    const flow: Promise<void> = this.conflictFlow(e, reject, opts.journal === true).finally(() => {
      if (this.conflictFlows.get(id) === flow) this.conflictFlows.delete(id);
    });
    this.conflictFlows.set(id, flow);
    this.detach(flow);
  }

  private async conflictFlow(e: OutboxEntry, reject: Reject, journal: boolean): Promise<void> {
    await this.waitForCursor(e, reject.stale);
    let replacedBy: string | undefined;
    const handler = this.conflictHandler;
    const f = this.follows.get(e.keyspaceId);
    if (handler !== undefined && f !== undefined) {
      const ctx: ConflictContext = {
        action: this.snapshot(e),
        reject,
        stale: reject.stale,
        confirmed: this.confirmedViewOf(f),
        submit: (input, options) => this.submitAction(input, options),
        replaceWith: async (input, options) => {
          const r = await this.submitAction(input, { ...options, replaces: e.mutationId });
          replacedBy = r.mutationId;
          return r;
        },
      };
      try {
        await handler(ctx);
      } catch {
        // a handler that throws has chosen nothing: the action keeps its final rejection
      }
    }
    await this.run(() => this.endAction(e, { sink: "none", reject, reason: codeName(reject.code), dependents: replacedBy !== undefined ? { replacedBy } : "fail", ...(journal ? { journal: true } : {}) }));
  }

  /**
   * The hold `CONFLICT_WAIT` (14 Table 14.1): wait until the cursor is at `Stale.current_seq`, at most to the default bound. The table
   * and [CLI-47] give two ways to wait ("The client loads the key (09 §9.2), or waits for its cursor"); this client takes the second, which
   * installs nothing that the stream does not, and so keeps the order of the records. The default bound is the end of the wait in either
   * way, at which the handler is called with the confirmed state that the client holds.
   */
  private async waitForCursor(e: OutboxEntry, stale: Stale | undefined): Promise<void> {
    const need = stale?.currentSeq ?? 0n;
    const f = this.follows.get(e.keyspaceId);
    if (f === undefined || need === 0n || f.seq >= need) return;
    const bound = e.authoredServerMs + this.limits.maxOfflineMs;
    await this.run(() => {
      this.startHold({ kind: "CONFLICT_WAIT", scope: "action", keyspaceId: e.keyspaceId, mutationId: e.mutationId, code: "CONFLICT_WAIT", boundMs: bound });
    });
    await new Promise<void>((resolve) => {
      const waiters = this.cursorWaiters.get(f.id) ?? new Set();
      this.cursorWaiters.set(f.id, waiters);
      const w = { need, wake: (): void => resolve() };
      waiters.add(w);
      const left = Math.max(0, bound - this.serverNow());
      const cancel = this.clock.setTimer(left, () => resolve());
      const prev = w.wake;
      w.wake = (): void => {
        cancel();
        prev();
      };
      // the cursor may have reached the seq while the hold started
      if (f.seq >= need) {
        waiters.delete(w);
        w.wake();
      }
    });
    await this.run(() => {
      this.stopHold("CONFLICT_WAIT", e.keyspaceId, undefined, e.mutationId);
      this.cursorWaiters.get(f.id)?.forEach((w) => {
        if (w.need <= f.seq) this.cursorWaiters.get(f.id)?.delete(w);
      });
    });
  }

  /** Wakes the conflict waiters of a keyspace whose cursor advanced. */
  protected wakeCursorWaiters(f: Follow): void {
    const ws = this.cursorWaiters.get(f.id);
    if (ws === undefined) return;
    for (const w of [...ws]) {
      if (f.seq >= w.need) {
        ws.delete(w);
        w.wake();
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // The acknowledgements of the app (14 [CLI-28], [CLI-41], [CLI-83])

  async ackDeadLetter(mutationId: string): Promise<void> {
    await this.run(async () => {
      if (!this.deadLetters.has(mutationId)) return;
      await this.tx([{ op: "ackDeadLetter", mutationId }]);
      this.deadLetters.delete(mutationId);
      await this.drainStuck();
    });
  }

  async ackExport(mutationId: string): Promise<void> {
    await this.run(async () => {
      if (!this.exports.has(mutationId)) return;
      await this.tx([{ op: "ackExport", mutationId }]);
      this.exports.delete(mutationId);
      await this.drainStuck();
    });
  }

  async ackRejection(mutationId: string): Promise<void> {
    await this.run(async () => {
      if (!this.rejections.has(mutationId)) return;
      await this.tx([{ op: "ackRejection", mutationId }]);
      this.rejections.delete(mutationId);
    });
  }

  /** Moves the entries that wait for a sink with room (14 [CLI-84]). */
  protected async drainStuck(): Promise<void> {
    for (const e of [...this.entries.values()].sort((a, b) => a.ordinal - b.ordinal)) {
      const s = e.stuck;
      if (s === undefined) continue;
      if (s.sink === "journal") {
        const o = e.outcome;
        if (o === undefined) continue;
        const j: JournalEntry = {
          mutationId: e.mutationId,
          keyspaceId: e.keyspaceId,
          opType: e.opType,
          content: e.content,
          author: e.author,
          outcome: o,
          reported: s.reason === "corrected" ? "corrected" : "committed",
          retiredServerMs: this.serverNow(),
          outcomeServerMs: o.atServerMs ?? this.serverNow(),
          spend: this.opInfoFor(e)?.spend ?? false,
          spendResent: e.spendResent,
          lane: e.lane,
          ordinal: e.ordinal,
          verifiedEpoch: o.logEpoch,
        };
        try {
          await this.tx([{ op: "deleteEntry", mutationId: e.mutationId }, { op: "putJournal", entry: j }]);
        } catch {
          continue;
        }
        this.entries.delete(e.mutationId);
        this.journal.set(e.mutationId, j);
        this.dropRuntime(e.mutationId);
      } else {
        delete e.stuck;
        try {
          await this.endAction(e, { sink: s.sink, reject: s.reject, reason: s.reason, ...(s.cause !== undefined ? { outcomeUnknown: s.cause } : {}), dependents: "fail", ...(s.dependencyOf !== undefined ? { dependencyOf: s.dependencyOf } : {}) });
        } catch {
          e.stuck = s;
        }
        if (this.entries.has(e.mutationId) && e.stuck === undefined) e.stuck = s;
      }
      this.follows.get(e.keyspaceId)?.invalidate();
    }
  }

  /**
   * Drops the journal entries whose window has passed: `max_offline` after the outcome, in server time (14 [CLI-53]). The window runs
   * from the outcome and not from the retirement: it is as long as the server keeps the outcome's binding (07 [MUT-51]), and a retirement
   * that waited for the cursor (a hold, a stop of the following, an outage) does not make the server keep the binding longer.
   */
  protected async purgeJournal(): Promise<void> {
    const now = this.serverNow();
    const ops: StorageOp[] = [];
    for (const j of this.journal.values()) {
      if (now - (j.outcomeServerMs ?? j.retiredServerMs) > this.limits.maxOfflineMs) ops.push({ op: "deleteJournal", mutationId: j.mutationId });
    }
    if (ops.length === 0) return;
    await this.tx(ops);
    for (const op of ops) if (op.op === "deleteJournal") this.journal.delete(op.mutationId);
  }
}

export function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

