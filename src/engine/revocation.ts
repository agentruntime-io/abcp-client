// Revocation and quarantine (14 §14.9), and what a keyspace that the engine stops following leaves behind (§14.10).
//
//  - [CLI-66] `ACCESS_REVOKED` for a keyspace: the **purge list** (the confirmed replica, cached pages, blob cache, presence state and
//    journal) goes, and the keyspace's outbox entries that [CLI-7] has not ended are **quarantined**: never sent automatically, never
//    deleted unreported, handed to the app's export hook for re-submission by an authorized principal, with `quarantined` for the
//    keyspace and then `exported` for each entry. The purge and the quarantine are two steps, in that order, each its own local
//    transaction, committed and retried until it is done, with the record that the keyspace is revoked, so that a restart finishes what a
//    crash left and never follows the keyspace again with the old token ([CLI-8]); the purge never waits for a store ([CLI-84]), and an action
//    that the export store has no room for stays in the outbox, final and never sent again, until the app acknowledges entries. An action that
//    was sent, or registered, or has an outcome that [CLI-7] has not confirmed in the epoch of the cursor, is not quarantined: [CLI-7] ends it,
//    by `GetMutationResult`, which needs no grant (07 [MUT-53]): a recorded outcome that survived retires it, a lost one is the final rejection
//    `STALE{EPOCH}`, and one that cannot be learned makes it outcome unknown (`outcome-unknown`, then `exported`, cause `ACCESS_REVOKED`, A.11);
//  - [CLI-79], [CLI-50] `Closing{SUBJECT_REVOKED}`, or an `Unauthenticated` that carries the `SUBJECT_REVOKED` detail (a device that
//    was offline when the revocation was made): the same for every keyspace of the subject, and the engine resumes only with a new
//    token (the hold `SUBJECT_REVOKED`, whose probe asks the app for one);
//  - [CLI-67] `GRANT_CHANGED`: the engine stops following; the outbox entries stay, and get `PERMISSION_DENIED` when sent (handled by
//    [CLI-46]); the local copy stays or is purged as the app's policy says (`copyPolicy`);
//  - [CLI-68] the token is refreshed in band before it expires; a failed refresh is no revocation (the hold `NO_TOKEN`);
//  - [CLI-69] nothing is authored into a keyspace that the engine cannot read: a purged or stopped keyspace refuses an action;
//  - [CLI-77] `DELETED`: the purge list, and the dead-letter and export entries of the keyspace, each handed to the app once more
//    first; the entries that [CLI-7] and the deletion itself write stay.
//
// SPEC-QUESTION (14 [CLI-66] with [CLI-79]): "emits `quarantined` for the keyspace" says nothing of a keyspace whose outbox has no
// entry that [CLI-7] has not ended. Reading A (taken): `quarantined` is emitted for every keyspace that the revocation purged, with an
// empty list of mutation ids when there was none, so that the app learns of the revocation and of the purge from one event. Reading B:
// only a keyspace with entries to quarantine gets one, and the app learns of an empty one from the `held` of the subject's revocation
// alone. B leaves a revoked reader of a quiet keyspace unaware that its copy is gone.

// SPEC-QUESTION (14 [CLI-66] with [CLI-7], 07 [MUT-53], 10 [AUTH-52], Q54.3, ruled): "quarantine the keyspace's outbox entries that [CLI-7] has not
// ended" does not say what becomes of (1) an action with an outcome from an earlier epoch that `GetMutationResult` has not confirmed yet, nor of (2) an
// action that was sent, whose `GetMutationResult` a token issued before a revocation of the subject cannot make (10 [AUTH-52] refuses it), although
// [CLI-7] says that call "needs no grant". The coordinator ruled on 2026-11-01 (V14): the text decides, against the first reading that this engine took:
// (1) retires only once `GetMutationResult` confirms the outcome ([CLI-7]), so it is asked about like (2), and a re-submission of an outcome that survived
// the restore would apply it twice; (2) is asked about, with the new token that a revoked subject resumes with, and ends as [CLI-7] and [CLI-83] say;
// until then it stays in the outbox, never sent.

import type { GetMutationResultResponse } from "../gen/abcp/v2/sync_pb.js";
import { Code, DecisionStatus, ErrorClass } from "../gen/abcp/v2/types_pb.js";
import { resendDelay } from "./backoff.js";
import { makeReject } from "./endings.js";
import { RpcError } from "./errors.js";
import { ReplicaState } from "../rules/fold/index.js";
import type { Follow } from "./follow.js";
import { SagaLayer } from "./sagas.js";
import { StoreFull, type ExportEntry, type OutboxEntry, type StorageOp, type StoredOutcome } from "./storage.js";
import type { ExportRecord } from "./types.js";

/** The app's policy for the local copy of a keyspace that it stops following without a revocation (14 [CLI-67], [CLI-77]). */
export type CopyPolicy = "keep" | "purge";

/** The latest a token is refreshed: this long before its expiry. 10 [AUTH-50]: the session ends within a 60 s leeway after `exp`. */
export const TOKEN_REFRESH_LEAD_MS = 60_000;

/** 10 [AUTH-48]: a client refreshes its token at about 80% of its lifetime, measured on its own clock from receipt. */
export const TOKEN_REFRESH_FRACTION = 0.8;

/** The least wait between two in-band refreshes of the token. */
export const TOKEN_REFRESH_MIN_GAP_MS = 5_000;

export type PurgeHook = (keyspaceId: string, cause: string) => void | Promise<void>;

export abstract class RevocationLayer extends SagaLayer {
  /** The app's policy for `GRANT_CHANGED` and `ARCHIVED`; the default keeps the local copy. */
  protected copyPolicy: CopyPolicy | ((keyspaceId: string, reason: "GRANT_CHANGED" | "ARCHIVED") => CopyPolicy) = "keep";
  private readonly purgeHooks = new Set<PurgeHook>();
  private tokenTimer: (() => void) | undefined;
  private tokenExpMs: number | undefined;
  /** The token whose refresh is armed: its expiry, and when (on the device clock) and with what lifetime it was received. */
  private tokenArmed: { exp: number; receivedAt: number; lifetime: number } | undefined;
  private revokingSubject: Promise<void> | undefined;

  protected resetRuntime(): void {
    super.resetRuntime();
    this.tokenTimer?.();
    this.tokenTimer = undefined;
    this.tokenExpMs = undefined;
    this.tokenArmed = undefined;
    this.revokingSubject = undefined;
  }

  /**
   * Registers what a component that keeps data of a keyspace outside the storage purges with it: the cached pages of the query layer,
   * the blob cache, the presence state (14 [CLI-66]). It is called with the keyspace and the cause, and awaited. Returns the function
   * that removes it.
   */
  registerPurgeHook(hook: PurgeHook): () => void {
    this.purgeHooks.add(hook);
    return () => this.purgeHooks.delete(hook);
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Purging the local copy (14 [CLI-66])

  /** The operations that purge a keyspace's replica and journal from the storage. */
  protected purgeOps(f: Follow): StorageOp[] {
    const ops: StorageOp[] = [{ op: "deleteReplica", keyspaceId: f.id }];
    const ids = new Set(f.purgeJournal);
    for (const j of this.journal.values()) if (j.keyspaceId === f.id) ids.add(j.mutationId);
    for (const id of ids) ops.push({ op: "deleteJournal", mutationId: id });
    return ops;
  }

  /** The in-memory part of the purge: the confirmed state, the documents, the journal, what was loaded outside the interest. */
  protected purgeMemory(f: Follow): void {
    const st = new ReplicaState();
    st.setCursor(f.state.seq, f.state.logEpoch, f.state.manifestVersion); // the cursor's epoch is what a send of a queued action records
    f.crdt.destroy();
    f.state = st;
    f.purged = true;
    f.outside.clear();
    f.widening = undefined;
    f.image = { ok: false, seqs: new Set(), headMs: 0 };
    f.lastRecordHash = undefined;
    f.lastRecordSeq = 0n;
    for (const [id, j] of [...this.journal]) if (j.keyspaceId === f.id) this.journal.delete(id);
    f.invalidate();
  }

  protected async runPurgeHooks(f: Follow, cause: string): Promise<void> {
    for (const h of [...this.purgeHooks]) {
      try {
        await h(f.id, cause);
      } catch (e) {
        this.noteInternalError(e); // a hook that fails purges nothing, and stops no other
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // ACCESS_REVOKED and SUBJECT_REVOKED (14 [CLI-66], [CLI-79])

  protected async onAccessRevoked(f: Follow, cause: "ACCESS_REVOKED" | "SUBJECT_REVOKED"): Promise<void> {
    if (f.revoked) return; // the drop frame came first, or the notice of the revocation is a second one: it is done
    f.revoked = true;
    f.stopped = cause === "ACCESS_REVOKED" ? "ACCESS_REVOKED" : "SUBJECT_REVOKED";
    // 14 [CLI-7]: the actions with a successful outcome in the epoch of the cursor retire at once (they write no journal: the copy is being purged)
    f.purged = true;
    await this.checkRetirements(f);
    // the memory is purged now: nothing of the copy is shown, and what the storage still holds of it goes in `finishRevocation`
    f.purgeJournal = [...this.journal.values()].filter((j) => j.keyspaceId === f.id).map((j) => j.mutationId);
    this.purgeMemory(f);
    this.touchPersisted();
    await this.finishRevocation(f);
  }

  /**
   * The durable part of a revocation, in the order of 14 [CLI-66]: the purge, then the quarantine; each step is committed and retried until it is
   * (`revocationDone` and the stored record say that both are), so that a restart finds the keyspace revoked and does what is left, and never
   * loads it again with the old token ([CLI-8]).
   *
   *  - the purge (the replica, the journal) is one transaction with the record that the keyspace is revoked, and it never waits for a store:
   *    "the purges ... by a revocation ... still run when a store is full" (14 [CLI-84]);
   *  - the quarantine moves each action that [CLI-7] has not ended and that was never sent to the export store, in a transaction of its own; one
   *    that the export store has no room for stays in the outbox, final and never sent again, until the app acknowledges entries
   *    (`stuck`, [CLI-84]), and is handed over then;
   *  - an action that was sent, or registered, or has an outcome that no cursor of the epoch retired, is not quarantined: [CLI-7] ends it by
   *    `GetMutationResult`, which needs no grant (07 [MUT-53]) (`askRevoked`).
   */
  protected async finishRevocation(f: Follow): Promise<void> {
    if (!f.revoked || this.role !== "flusher") return;
    const cause = f.stopped === "SUBJECT_REVOKED" ? "SUBJECT_REVOKED" : "ACCESS_REVOKED";
    let failed = false;
    if (!f.revocationPurged) {
      try {
        await this.tx([...this.followedOps(), ...this.purgeOps(f)]);
        f.revocationPurged = true;
        this.touchPersisted();
      } catch (e) {
        this.noteInternalError(e);
        failed = true;
      }
    }
    // the quarantine: what the purge left in the outbox of the keyspace that was never sent and has no outcome
    const now = this.serverNow();
    const exported: ExportEntry[] = [];
    const batch = this.entriesOf(f.id).filter((e) => !this.ending.has(e.mutationId) && e.stuck === undefined && this.quarantinable(e));
    for (const e of batch) {
      const r = await this.quarantineOne(e, cause, now);
      if (r === "retry") failed = true;
      else f.quarantinedIds.push(e.mutationId);
      if (r === "exported") exported.push(this.exports.get(e.mutationId) as ExportEntry);
    }
    f.exportedPending.push(...exported);
    if (failed) {
      // a transaction that failed applied nothing: it is made again, with the client backoff ([CLI-43])
      this.after(resendDelay(f.revocationAttempts++, undefined, this.random), () => this.finishRevocation(f));
    } else if (!f.revocationDone) {
      f.revocationDone = true;
      f.revocationAttempts = 0;
      await this.persistFollowed();
      // 14 [CLI-66]: `quarantined` for the keyspace, then `exported` for each entry (14 [CLI-79] for every keyspace of the subject)
      this.emit({ type: "quarantined", keyspaceId: f.id, cause, mutationIds: [...f.quarantinedIds] });
      f.quarantinedIds = [];
    }
    // 14 [CLI-66] (V19 SHOULD-2): "`quarantined` for the keyspace, then `exported` for each entry": what a failed step left moved is announced after the one
    // `quarantined` event, at the retry that completes the quarantine, never before it
    if (f.revocationDone) {
      for (const x of f.exportedPending.splice(0)) {
        this.emit({ type: "exported", mutationId: x.mutationId, keyspaceId: x.keyspaceId, reason: x.reason });
        this.callExportHook(this.exportRecord(x));
      }
    }
    if (!f.revocationAsked && this.startRequested) this.askRevoked(f);
    if (failed || !f.revocationDone) return;
    this.detach(this.runPurgeHooks(f, cause)); // outside the queue: a hook may call the engine
    this.schedulePump();
  }

  /** Whether the quarantine takes the action: [CLI-7] has not ended it, and it was never sent (the server may hold an outcome for any other). */
  private quarantinable(e: OutboxEntry): boolean {
    return e.outcome === undefined && !e.everSent && !e.registered;
  }

  /** The export entry of a quarantined action (14 [CLI-66]): its content for re-submission by an authorized principal, with what the client knows of its outcome. */
  private exportOf(e: OutboxEntry, cause: string, atMs: number): ExportEntry {
    const lastResult = this.outcomeText(e);
    return {
      mutationId: e.mutationId,
      keyspaceId: e.keyspaceId,
      opType: e.opType,
      content: e.content,
      author: e.author,
      reason: cause,
      ...(lastResult !== undefined ? { lastResult } : {}),
      atMs,
    };
  }

  protected exportRecord(x: ExportEntry): ExportRecord {
    return { mutationId: x.mutationId, keyspaceId: x.keyspaceId, opType: x.opType, content: x.content, reason: x.reason, reject: x.reject, lastResult: x.lastResult, dependencyOf: x.dependencyOf };
  }

  /**
   * Quarantines one action: it leaves the outbox for the export store in one transaction. Returns `exported`; `stuck` when the export store is
   * full, and the action stays in the outbox, final and never sent again ([CLI-84]); `retry` when the transaction failed and applied nothing.
   */
  protected async quarantineOne(e: OutboxEntry, cause: string, atMs: number): Promise<"exported" | "stuck" | "retry"> {
    const x = this.exportOf(e, cause, atMs);
    try {
      await this.tx([{ op: "deleteEntry", mutationId: e.mutationId }, { op: "putExport", entry: x }]);
    } catch (err) {
      if (err instanceof StoreFull && err.store === "exports") {
        if (e.stuck?.quarantine !== true) {
          e.stuck = { sink: "export", reason: cause, quarantine: true };
          try {
            await this.tx([this.putOp(e)]);
          } catch (e2) {
            this.noteInternalError(e2); // the marker stays in memory; a restart finds the keyspace revoked and quarantines it again
          }
          this.follows.get(e.keyspaceId)?.invalidate();
        }
        return "stuck";
      }
      this.noteInternalError(err);
      return "retry";
    }
    this.entries.delete(e.mutationId);
    this.settleSlot(e, "wait");
    this.dropRuntime(e.mutationId);
    this.exports.set(x.mutationId, x);
    this.follows.get(e.keyspaceId)?.invalidate();
    return "exported";
  }

  /** The actions that the app acknowledged room for in the export store: the quarantined ones that waited are handed over now ([CLI-84]). */
  protected async drainStuck(): Promise<void> {
    for (const e of [...this.entries.values()].sort((a, b) => a.ordinal - b.ordinal)) {
      if (e.stuck?.quarantine !== true || !this.entries.has(e.mutationId)) continue;
      const r = await this.quarantineOne(e, e.stuck.reason, this.serverNow());
      if (r !== "exported") continue;
      const x = this.exports.get(e.mutationId) as ExportEntry;
      this.emit({ type: "exported", mutationId: x.mutationId, keyspaceId: x.keyspaceId, reason: x.reason });
      this.callExportHook(this.exportRecord(x));
    }
    await super.drainStuck();
  }

  /**
   * 14 [CLI-7], [CLI-8]: the actions of a revoked keyspace that the server may hold an outcome for are asked about with `GetMutationResult`, which
   * needs no grant: one that was sent or registered and has no outcome (`pollNow`), and one whose outcome is from an earlier epoch than the cursor's, or
   * that no cursor of the epoch retired (`askEarlier`: it retires only once the answer confirms the outcome). The answer ends each as 14 [CLI-53] and
   * [CLI-83] say, so that no such action stays pending. A restart asks again about those that are still in the outbox.
   */
  protected askRevoked(f: Follow): void {
    f.revocationAsked = true;
    for (const e of this.entriesOf(f.id)) {
      if (this.ending.has(e.mutationId) || e.stuck !== undefined) continue;
      if (e.outcome !== undefined) this.askEarlier(e);
      else if (e.everSent || e.registered) this.pollNow(e);
    }
  }

  /** Asks about an action of a revoked keyspace that has an outcome that [CLI-7] has not confirmed in the epoch of the cursor. */
  private askEarlier(e: OutboxEntry): void {
    const again = (): void => {
      this.after(resendDelay(this.rt(e.mutationId).authTries++, undefined, this.random), () => {
        if (this.entries.has(e.mutationId)) this.askEarlier(e);
      });
    };
    this.detach(
      (async () => {
        let resp: GetMutationResultResponse;
        try {
          resp = await this.transport.getMutationResult({ $typeName: "abcp.v2.GetMutationResultRequest", keyspaceId: e.keyspaceId, mutationId: e.mutationId });
        } catch (err) {
          this.noteRevoked(err);
          await this.run(() => {
            if (this.entries.has(e.mutationId)) again();
          });
          return;
        }
        await this.run(async () => {
          const f = this.follows.get(e.keyspaceId);
          if (f === undefined || !this.entries.has(e.mutationId) || this.ending.has(e.mutationId)) return;
          if (!(await this.answerRevoked(f, e, resp))) again();
          await this.flushDirty();
        });
      })(),
    );
  }

  /** Acts on the answer about an action of a revoked keyspace, as 14 [CLI-53] says; false when the answer is none (the server still drives the action). */
  protected async answerRevoked(f: Follow, e: OutboxEntry, ans: GetMutationResultResponse): Promise<boolean> {
    const spend = (this.opInfoFor(e)?.spend ?? false) && !e.spendResent;
    const cause = f.stopped ?? "ACCESS_REVOKED";
    if (ans.known && ans.result !== undefined) {
      const o = ans.result.outcome;
      if (o.case === "appended" || (o.case === "decided" && o.value.status === DecisionStatus.COMMITTED)) {
        // the outcome survived the restore (14 [CLI-7]): the action retires on it at once, since the keyspace is not followed any more
        const out: StoredOutcome =
          o.case === "appended"
            ? { kind: "appended", seq: o.value.seq, logEpoch: f.logEpoch, overwroteNewer: [...o.value.overwroteNewer] }
            : { kind: "decided", status: "COMMITTED", decisionSeq: (o.value as { decisionSeq: bigint }).decisionSeq, logEpoch: f.logEpoch };
        this.setOutcome(e, out);
        this.markDirty(e);
        await this.checkRetirements(f);
        return true;
      }
      if (o.case === "decided") {
        await this.onRejectResult(e, o.value.reject ?? makeReject(Code.GATE_REJECTED, ErrorClass.DEAD_LETTER, "rejected"), true);
        return true;
      }
      if (o.case === "rejected") {
        await this.onRejectResult(e, o.value);
        return true;
      }
      return false;
    }
    if (ans.voided) {
      if (!spend) {
        this.beginConflict(e, this.lostReject()); // the outcome was lost to a restore: the final rejection STALE{EPOCH}, through the conflict handler
        return true;
      }
      // a lost spend action is resent once after an epoch change (14 [CLI-64]); a keyspace that the engine cannot send to gets no resend: it is quarantined
      // with what is known of it, for an authorized principal to re-submit
      const r = await this.quarantineOne(e, cause, this.serverNow());
      if (r === "exported") {
        const x = this.exports.get(e.mutationId) as ExportEntry;
        this.emit({ type: "exported", mutationId: x.mutationId, keyspaceId: x.keyspaceId, reason: x.reason });
        this.callExportHook(this.exportRecord(x));
      }
      return true;
    }
    // known = false and voided = false: "for an action of a keyspace that the client stopped following" the outcome is unknown, a spend action too (14 [CLI-83]),
    // and no action stays pending (14 [CLI-7])
    await this.endAction(e, { sink: "export", reject: undefined, reason: cause, outcomeUnknown: cause, dependents: "fail" });
    return true;
  }

  /** What the client knows of the outcome of an action that is quarantined with an outcome it could not confirm (14 [CLI-53]). */
  private outcomeText(e: OutboxEntry): string | undefined {
    const o = e.outcome;
    if (o === undefined) return this.lastResultText(e);
    return o.kind === "appended" ? `appended: seq ${o.seq} in epoch ${o.logEpoch}` : `decided ${o.status}: seq ${o.decisionSeq} in epoch ${o.logEpoch}`;
  }

  /**
   * The subject is revoked (14 [CLI-79], [CLI-50]): the purge list is purged and the outboxes quarantined for every keyspace of the
   * subject, `quarantined` is emitted for each, and the engine holds the app until it has a new token: "resume only with a new token"
   * (09 [READ-60]). A keyspace that the drop frames already revoked is done; a second notice of the revocation (the detail that every
   * refusal of a device carries until it has recovered, 10 [AUTH-51]) changes nothing.
   */
  protected onSubjectRevoked(): Promise<void> {
    if (this.revokingSubject !== undefined) return this.revokingSubject;
    const p = (async (): Promise<void> => {
      for (const f of [...this.follows.values()]) await this.onAccessRevoked(f, "SUBJECT_REVOKED");
      // a second notice (every refused call carries it) does not start the hold again: its probe is on its own backoff
      if (!this.holds.has("SUBJECT_REVOKED")) this.holdApp("SUBJECT_REVOKED", "SUBJECT_REVOKED");
    })().finally(() => {
      this.revokingSubject = undefined;
    });
    this.revokingSubject = p;
    return p;
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // GRANT_CHANGED and ARCHIVED: the app's policy (14 [CLI-67], [CLI-77])

  protected async onCopyPolicy(f: Follow, reason: "GRANT_CHANGED" | "ARCHIVED"): Promise<void> {
    const policy = typeof this.copyPolicy === "function" ? this.copyPolicy(f.id, reason) : this.copyPolicy;
    if (policy !== "purge") return; // the copy stays: it is shown as it was, and nothing else of the keyspace changes
    await this.tx(this.purgeOps(f));
    this.purgeMemory(f);
    this.touchPersisted();
    this.detach(this.runPurgeHooks(f, reason));
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // DELETED (14 [CLI-77])

  protected async onKeyspaceDeletedPurge(f: Follow, before: { deadLetters: string[]; exports: string[] }): Promise<void> {
    const ops: StorageOp[] = [];
    const records: ExportRecord[] = [];
    // an entry that the app has not acknowledged is handed to it once more before it goes: the one case in which an entry goes before
    // the app acknowledges it (14 [CLI-28], [CLI-83], [CLI-84])
    for (const id of before.deadLetters) {
      const d = this.deadLetters.get(id);
      if (d === undefined) continue;
      this.emit({ type: "dead-lettered", mutationId: d.mutationId, keyspaceId: d.keyspaceId, reason: d.reason });
      ops.push({ op: "ackDeadLetter", mutationId: id });
    }
    for (const id of before.exports) {
      const x = this.exports.get(id);
      if (x === undefined) continue;
      this.emit({ type: "exported", mutationId: x.mutationId, keyspaceId: x.keyspaceId, reason: x.reason });
      records.push({ mutationId: x.mutationId, keyspaceId: x.keyspaceId, opType: x.opType, content: x.content, reason: x.reason, reject: x.reject, lastResult: x.lastResult, dependencyOf: x.dependencyOf });
      ops.push({ op: "ackExport", mutationId: id });
    }
    ops.push(...this.purgeOps(f));
    f.purged = true; // from here no journal entry of the keyspace is written
    await this.tx(ops);
    for (const id of before.deadLetters) this.deadLetters.delete(id);
    for (const id of before.exports) this.exports.delete(id);
    this.purgeMemory(f);
    this.touchPersisted();
    await this.persistFollowed();
    for (const r of records) this.callExportHook(r);
    this.detach(this.runPurgeHooks(f, "DELETED"));
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // The token, in band (14 [CLI-68])

  /**
   * Arms the refresh of the token before it expires, on the live session: the app is asked for a new token (`TokenProvider.refresh`),
   * and the session is told with `RefreshSession` (10 §10.7). The expiry is the token provider's (`expiresAt`), or the `expires_at`
   * of the last `RefreshSession` answer. A failed refresh is not a revocation: the data and the outbox stay, and the hold `NO_TOKEN`
   * applies (14 Table 14.1).
   */
  protected armTokenRefresh(): void {
    this.tokenTimer?.();
    this.tokenTimer = undefined;
    const exp = this.tokens?.expiresAt?.() ?? this.tokenExpMs;
    if (exp === undefined || !this.online()) return;
    this.tokenExpMs = exp;
    // 10 [AUTH-48]: about 80% of the lifetime, measured on the device clock from receipt: a token seen for the first time is
    // received now, with the lifetime it has left; re-arming for the same token keeps its receipt.
    if (this.tokenArmed?.exp !== exp) {
      this.tokenArmed = { exp, receivedAt: this.clock.now(), lifetime: Math.max(0, exp - this.serverNow()) };
    }
    const { receivedAt, lifetime } = this.tokenArmed;
    const due = receivedAt + Math.min(TOKEN_REFRESH_FRACTION * lifetime, lifetime - TOKEN_REFRESH_LEAD_MS);
    // never sooner than `TOKEN_REFRESH_MIN_GAP_MS`: a provider that reports an expiry that is already past, after a refresh, must not make a busy loop
    this.tokenTimer = this.after(Math.max(TOKEN_REFRESH_MIN_GAP_MS, due - this.clock.now()), () => this.refreshInBand());
  }

  private refreshInBand(): void {
    if (!this.online()) return;
    this.detach(
      (async () => {
        const ok = await this.refreshToken();
        if (!ok) {
          await this.run(() => {
            this.holdApp("NO_TOKEN", "NO_TOKEN");
          });
          return;
        }
        await this.sendRefreshSession();
      })(),
    );
  }

  protected afterTokenRefreshed(): void {
    if (this.online()) this.detach(this.sendRefreshSession());
  }

  private async sendRefreshSession(): Promise<void> {
    let resp;
    try {
      resp = await this.transport.refreshSession({ $typeName: "abcp.v2.RefreshSessionRequest", sessionId: this.sessionId() });
    } catch (e) {
      if (this.noteRevoked(e)) return;
      if (e instanceof RpcError && e.status === "Unauthenticated") {
        await this.run(() => {
          this.holdApp("NO_TOKEN", "NO_TOKEN");
        });
      }
      return; // a transport failure: the session carries on, and a failure of the session resumes it
    }
    await this.run(() => {
      // the `KeyspaceDropped{GRANT_CHANGED}` frames of a keyspace that the new token no longer grants are what the client acts on; the
      // `dropped_keyspace_ids` of the answer are informational (14 [CLI-67], 09 [READ-39])
      if (resp.expiresAt !== undefined) this.tokenExpMs = Number(resp.expiresAt.seconds) * 1000 + Math.floor(resp.expiresAt.nanos / 1e6);
      this.armTokenRefresh();
    });
  }
}
