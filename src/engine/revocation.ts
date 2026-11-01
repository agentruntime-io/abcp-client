// Revocation and quarantine (14 §14.9), and what a keyspace that the engine stops following leaves behind (§14.10).
//
//  - [CLI-66] `ACCESS_REVOKED` for a keyspace: the **purge list** (the confirmed replica, cached pages, blob cache, presence state and
//    journal) goes, and the keyspace's outbox entries that [CLI-7] has not ended are **quarantined**: never sent automatically, never
//    deleted unreported, handed to the app's export hook for re-submission by an authorized principal, with `quarantined` for the
//    keyspace and then `exported` for each entry. The purge and the quarantine are one local transaction, so a crash never leaves
//    the copy purged and the entries still queued. An action that was sent, or registered, and has no outcome yet is not quarantined:
//    [CLI-7] ends it, by `GetMutationResult`, which needs no grant (07 [MUT-53]): a recorded outcome retires it, and one that cannot
//    be learned makes it outcome unknown (`outcome-unknown`, then `exported`, cause `ACCESS_REVOKED`, A.11);
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

// SPEC-QUESTION (14 [CLI-66] with [CLI-7], 07 [MUT-53], 10 [AUTH-52]): "quarantine the keyspace's outbox entries that [CLI-7] has not ended" does not say
// what becomes of (1) an action with an outcome from an earlier epoch that `GetMutationResult` has not confirmed yet ([CLI-7]: it retires only once
// it does), nor of (2) an action that was sent, whose `GetMutationResult` a token issued before a revocation of the subject cannot make (10 [AUTH-52]
// refuses it), although [CLI-7] says that call "needs no grant". Reading A (taken): (1) is quarantined, with its outcome in the export entry
// (`lastResult`), so that the app knows it may have been applied; (2) is asked about, with the new token that a revoked subject resumes with, and ends
// as [CLI-7] and [CLI-83] say; until then it stays in the outbox, never sent. Reading B: (1) waits for the answer too, which needs the epoch flow of
// a keyspace whose copy is gone, and (2) is quarantined at once as unknown. B exports an action whose outcome the client could still learn.

import { RpcError } from "./errors.js";
import { ReplicaState } from "../rules/fold/index.js";
import type { Follow } from "./follow.js";
import { InterestLayer } from "./interest.js";
import type { ExportEntry, OutboxEntry, StorageOp } from "./storage.js";
import type { ExportRecord } from "./types.js";

/** The app's policy for the local copy of a keyspace that it stops following without a revocation (14 [CLI-67], [CLI-77]). */
export type CopyPolicy = "keep" | "purge";

/** What the engine does in band with the token: the lead before its expiry at which it is refreshed. 10 [AUTH-50]: the session ends within a 60 s leeway after `exp`. */
export const TOKEN_REFRESH_LEAD_MS = 60_000;

/** The least wait between two in-band refreshes of the token. */
export const TOKEN_REFRESH_MIN_GAP_MS = 5_000;

export type PurgeHook = (keyspaceId: string, cause: string) => void | Promise<void>;

export abstract class RevocationLayer extends InterestLayer {
  /** The app's policy for `GRANT_CHANGED` and `ARCHIVED`; the default keeps the local copy. */
  protected copyPolicy: CopyPolicy | ((keyspaceId: string, reason: "GRANT_CHANGED" | "ARCHIVED") => CopyPolicy) = "keep";
  private readonly purgeHooks = new Set<PurgeHook>();
  private tokenTimer: (() => void) | undefined;
  private tokenExpMs: number | undefined;
  private revokingSubject: Promise<void> | undefined;

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
    for (const j of this.journal.values()) if (j.keyspaceId === f.id) ops.push({ op: "deleteJournal", mutationId: j.mutationId });
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
    const live = this.entriesOf(f.id).filter((e) => !this.ending.has(e.mutationId) && e.stuck === undefined);
    // an action that was sent or registered and has no outcome is ended by `GetMutationResult` ([CLI-7]); the others are quarantined
    const asking = live.filter((e) => e.outcome === undefined && (e.everSent || e.registered));
    const quarantined = live.filter((e) => !asking.includes(e));
    const now = this.serverNow();
    const exported: ExportEntry[] = quarantined.map((e) => ({
      mutationId: e.mutationId,
      keyspaceId: e.keyspaceId,
      opType: e.opType,
      content: e.content,
      author: e.author,
      reason: cause,
      ...(this.outcomeText(e) !== undefined ? { lastResult: this.outcomeText(e) as string } : {}),
      atMs: now,
    }));
    const ops: StorageOp[] = [];
    for (const e of quarantined) ops.push({ op: "deleteEntry", mutationId: e.mutationId });
    for (const x of exported) ops.push({ op: "putExport", entry: x });
    ops.push(...this.purgeOps(f));
    await this.tx(ops);
    for (const e of quarantined) {
      this.entries.delete(e.mutationId);
      this.settleSlot(e, "wait");
      this.dropRuntime(e.mutationId);
    }
    for (const x of exported) this.exports.set(x.mutationId, x);
    this.purgeMemory(f);
    this.touchPersisted();
    await this.persistFollowed();
    // 14 [CLI-66]: `quarantined` for the keyspace, then `exported` for each entry
    this.emit({ type: "quarantined", keyspaceId: f.id, cause, mutationIds: exported.map((x) => x.mutationId) });
    const records: ExportRecord[] = [];
    for (const x of exported) {
      this.emit({ type: "exported", mutationId: x.mutationId, keyspaceId: x.keyspaceId, reason: x.reason });
      records.push({ mutationId: x.mutationId, keyspaceId: x.keyspaceId, opType: x.opType, content: x.content, reason: x.reason, reject: undefined, lastResult: x.lastResult, dependencyOf: undefined });
    }
    for (const r of records) this.callExportHook(r);
    for (const e of asking) this.pollNow(e);
    this.detach(this.runPurgeHooks(f, cause)); // outside the queue: a hook may call the engine
    this.schedulePump();
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
    // never sooner than `TOKEN_REFRESH_MIN_GAP_MS`: a provider that reports an expiry that is already past, after a refresh, must not make a busy loop
    this.tokenTimer = this.after(Math.max(TOKEN_REFRESH_MIN_GAP_MS, exp - this.serverNow() - TOKEN_REFRESH_LEAD_MS), () => this.refreshInBand());
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
      resp = await this.transport.refreshSession({ $typeName: "abpc.v2.RefreshSessionRequest", sessionId: this.sessionId() });
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
