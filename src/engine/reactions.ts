// Reactions by class and cause (14 §14.6): what the engine does with a `Reject` (by `error_class`, [CLI-42]), a `Retry`
// ([CLI-43], [CLI-44]), a hold ([CLI-45], Table 14.1), a `DEAD_LETTER` ([CLI-46]), a `CONFLICT` and its `STALE` cause ([CLI-47] to
// [CLI-49]), the queued actions of a split or archived keyspace ([CLI-52]), a restamp ([CLI-63], [CLI-64]) and a cancel ([CLI-81]).
//
// An action is never re-authored here. It is resent unchanged, or it ends and the app is told (02 [ARC-50], [ARC-51]).

import { create } from "@bufbuild/protobuf";
import { MutationSchema } from "../gen/abpc/v2/sync_pb.js";
import {
  ChangeKind,
  Code,
  ErrorClass,
  KeyspaceLifecycle,
  RejectSchema,
  RetryReason,
  StaleCause,
  type Reject,
  type Retry,
  type SuccessorRule,
} from "../gen/abpc/v2/types_pb.js";
import { fullJitter, HOLD_BACKOFF, resendDelay } from "./backoff.js";
import { codeName, isKnownClass, isKnownCode, reactionOf, retryReasonName } from "./codes.js";
import { encodeContent } from "./entry.js";
import { makeReject } from "./endings.js";
import type { Follow } from "./follow.js";
import type { Hold, HoldKind } from "./holds.js";
import { ReplicaLayer } from "./replica.js";
import { matchSuccessor } from "./successors.js";
import type { OutboxEntry } from "./storage.js";

/** The row of Table 14.1 that a reject falls under, or undefined for a reject that is no hold. */
export function holdKindOf(r: Reject): HoldKind | undefined {
  if (!isKnownClass(r.errorClass)) return "UNKNOWN_HOLD"; // 15 [COMPAT-39]: a class that is unknown or UNSPECIFIED is a HOLD
  if (r.errorClass !== ErrorClass.HOLD) return undefined;
  switch (r.code) {
    case Code.UNKNOWN_OP_TYPE:
      return "UNKNOWN_OP_TYPE";
    case Code.CLIENT_FORKED:
      return "CLIENT_FORKED";
    case Code.CLIENT_UNKNOWN:
      return "CLIENT_UNKNOWN";
    case Code.KEYSPACE_NOT_FOUND:
      return "KEYSPACE_NOT_FOUND";
    case Code.UPGRADE_REQUIRED:
      return "UPGRADE_REQUIRED";
    case Code.UNSUPPORTED_FEATURE:
      return "UNSUPPORTED_FEATURE";
    case Code.HISTORY_DIVERGED:
      return "HISTORY_DIVERGED";
    case Code.APP_SUSPENDED:
      return "APP_SUSPENDED";
    default:
      return "UNKNOWN_HOLD"; // a HOLD code that the client does not know (15 [COMPAT-39])
  }
}

export abstract class ReactionLayer extends ReplicaLayer {
  protected readonly dirty = new Set<string>();
  private readonly probeTimers = new Map<string, () => void>();
  private boundTimer: (() => void) | undefined;
  private pumpQueued = false;

  protected abstract online(): boolean;
  protected abstract applyResult(e: OutboxEntry, r: import("../gen/abpc/v2/sync_pb.js").MutationResult, source: "mutate" | "poll" | "cancel"): Promise<void>;
  protected abstract rebindSession(): void;
  protected abstract requestResumeCheck(f: Follow, cause: string): void;
  protected abstract whenLoaded(f: Follow): Promise<void>;
  protected abstract probeKeyspaceLoad(f: Follow): void;
  protected abstract readdKeyspace(f: Follow): void;
  protected abstract reconnectNow(): void;
  protected abstract retryEpochResolution(f: Follow): void;
  protected abstract followSuccessors(rules: readonly SuccessorRule[]): void;
  protected abstract pumpAll(): Promise<void>;
  /** The manifest version in force for the keyspace, as `LoadStateResponse.manifest_version` names it (14 Table 14.1, `UNKNOWN_OP_TYPE`). */
  protected abstract manifestVersionInForce(f: Follow): Promise<number>;
  protected abstract kick(f?: Follow): void;
  /** The subject is revoked (`Closing{SUBJECT_REVOKED}`, or the `Unauthenticated` detail): purge and quarantine (14 [CLI-79], [CLI-50]). */
  protected abstract onSubjectRevoked(): Promise<void>;
  /** A call failed with the revocation detail: the subject is revoked, whatever the call was (14 [CLI-50]). */
  protected abstract noteRevoked(error: unknown): boolean;
  /** The app gave a new token while a session is live: the session is told in band (14 [CLI-68]). */
  protected abstract afterTokenRefreshed(): void;

  // ---------------------------------------------------------------------------------------------------------------------
  // Persisting and scheduling

  protected markDirty(e: OutboxEntry): void {
    this.dirty.add(e.mutationId);
  }

  /** Writes the entries that changed during a task, in one transaction. */
  protected async flushDirty(): Promise<void> {
    if (this.dirty.size === 0) return;
    const ops = [...this.dirty].map((id) => this.entries.get(id)).filter((e): e is OutboxEntry => e !== undefined).map((e) => this.putOp(e));
    this.dirty.clear();
    if (ops.length > 0) await this.tx(ops);
  }

  protected schedulePump(): void {
    if (this.pumpQueued || this.stopped) return;
    this.pumpQueued = true;
    this.post(async () => {
      this.pumpQueued = false;
      await this.pumpAll();
    });
  }

  /** Holds an entry back until the delay has passed, on the engine's clock ([CLI-43]). */
  protected deferEntry(e: OutboxEntry, delayMs: number): void {
    const rt = this.rt(e.mutationId);
    rt.timer?.();
    rt.nextAttemptAt = this.clock.now() + delayMs;
    rt.timer = this.after(delayMs, () => {
      rt.timer = undefined;
      rt.nextAttemptAt = 0;
      this.schedulePump();
    });
  }

  /** The delay before the n-th consecutive resend: the larger of `after_ms` and the client backoff ([CLI-43]). */
  protected nextDelay(e: OutboxEntry, afterMs: number | undefined): number {
    return resendDelay(e.attempts++, afterMs, this.random);
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // `Retry` (14 [CLI-43], [CLI-44])

  protected async onRetryResult(e: OutboxEntry, retry: Retry, intentSeq: bigint): Promise<void> {
    const reason = retry.reason;
    e.lastResult = { kind: "retry", what: retryReasonName(reason), atMs: this.serverNow() };
    const rt = this.rt(e.mutationId);
    // 14 [CLI-44]: the server drives the retries of a registered intent; the client waits for the DECISION on the stream or polls
    const pending =
      e.lane === "GATED" &&
      (intentSeq > 0n || e.registered || reason === RetryReason.GATE_UNAVAILABLE || reason === RetryReason.GATE_TIMEOUT) &&
      reason !== RetryReason.LANE_NOT_READY &&
      reason !== RetryReason.RATE_LIMITED &&
      reason !== RetryReason.KEYSPACE_SEEDING &&
      reason !== RetryReason.BLOB_PENDING &&
      reason !== RetryReason.BLOB_WITHHELD &&
      reason !== RetryReason.AUTHORIZER_UNAVAILABLE &&
      reason !== RetryReason.STORAGE_UNAVAILABLE;
    if (pending) {
      e.registered = e.registered || intentSeq > 0n || reason === RetryReason.GATE_UNAVAILABLE || reason === RetryReason.GATE_TIMEOUT;
      this.markDirty(e);
      this.startPoll(e, retry.afterMs);
      return;
    }
    switch (reason) {
      case RetryReason.OUT_OF_ORDER: {
        // 14 [CLI-44]: first send the earlier actions of the lane that the server is missing; with none to send, re-identify
        const f = this.follows.get(e.keyspaceId);
        const lane = f?.lanes[e.lane];
        if (f !== undefined && lane !== undefined && e.clientSeq > lane.watermark + 1) {
          let missing = false;
          for (let s = lane.watermark + 1; s < e.clientSeq; s++) {
            const id = lane.assigned.get(s);
            if (id === undefined || !this.entries.has(id)) missing = true;
          }
          if (missing) {
            this.reidentify("OUT_OF_ORDER");
            return;
          }
        }
        this.deferEntry(e, this.nextDelay(e, retry.afterMs));
        break;
      }
      case RetryReason.NOT_PROCESSED:
      case RetryReason.LANE_NOT_READY:
      case RetryReason.CONTENTION:
      case RetryReason.KEYSPACE_SEEDING:
      case RetryReason.BLOB_PENDING:
      case RetryReason.BLOB_WITHHELD:
        // TODO(09 §9.9, 14 [CLI-44]): the blob uploads that BLOB_PENDING and BLOB_MISSING ask for come with the blob milestone;
        // the action is resent unchanged after the larger of `after_ms` and the client backoff
        this.deferEntry(e, this.nextDelay(e, retry.afterMs));
        break;
      default:
        // RATE_LIMITED, STORAGE_UNAVAILABLE, AUTHORIZER_UNAVAILABLE, COMMIT_DATABASE_ERROR, and a reason that the client does not
        // know: a retry after `after_ms` (15 [COMPAT-40])
        this.deferEntry(e, this.nextDelay(e, retry.afterMs));
    }
    this.markDirty(e);
  }

  /** Polls `GetMutationResult` for an action whose intent the server drives (14 [CLI-44]), with the client backoff. */
  protected startPoll(e: OutboxEntry, afterMs: number | undefined): void {
    const rt = this.rt(e.mutationId);
    rt.polling = true;
    rt.timer?.();
    rt.timer = this.after(this.nextDelay(e, afterMs), () => {
      rt.timer = undefined;
      this.pollNow(e);
    });
  }

  protected pollNow(e: OutboxEntry): void {
    if (!this.entries.has(e.mutationId) || e.outcome !== undefined) return;
    const rt = this.rt(e.mutationId);
    rt.polling = true;
    this.detach(
      (async () => {
        let resp;
        try {
          resp = await this.transport.getMutationResult({ $typeName: "abpc.v2.GetMutationResultRequest", keyspaceId: e.keyspaceId, mutationId: e.mutationId });
        } catch (err) {
          this.noteRevoked(err); // 14 [CLI-50]: the detail of a revoked subject, on whatever call it comes
          await this.run(() => {
            if (this.entries.has(e.mutationId)) this.startPoll(e, undefined);
          });
          return;
        }
        await this.run(async () => {
          if (!this.entries.has(e.mutationId) || e.outcome !== undefined) return;
          const f = this.follows.get(e.keyspaceId);
          if (resp.known && resp.result !== undefined) {
            rt.polling = false;
            await this.applyResult(e, resp.result, "poll");
            await this.flushDirty();
          } else if (f !== undefined && !resp.voided && this.mayMoveAfterAnswer(f, e)) {
            // 14 [CLI-52]: a GATED action whose last result is a Retry, and the source answers known = false and voided = false: it was
            // never registered, so it is handled as the other queued actions of the split or archived keyspace are
            rt.polling = false;
            if ((await this.moveQueued(f, e, undefined)) === "needWhole") {
              f.needsLoad = { cause: "RESUME_TOO_OLD", resume: f.loaded };
              this.kick(f);
            }
            this.followSuccessors(f.successors);
            this.schedulePump();
          } else if (f !== undefined && f.stopped !== undefined && f.stopped !== "GRANT_CHANGED" && f.stopped !== "UNKNOWN_DROP_REASON" && !resp.voided) {
            // 14 [CLI-7], [CLI-83]: the client stopped following the keyspace, and the answer leaves the outcome unknown: the action
            // ends as outcome unknown, so that no such action stays pending
            rt.polling = false;
            await this.endAction(e, { sink: "export", reject: undefined, reason: f.stopped, outcomeUnknown: f.stopped, dependents: "fail" });
          } else if (f !== undefined && (resp.voided || (e.sentInEpoch !== 0 && e.sentInEpoch < f.logEpoch))) {
            // an outcome of an earlier epoch is resolved as 14 [CLI-53] says
            rt.polling = false;
            this.needEpochResolution(f);
          } else {
            // 07 [MUT-53]: known = false proves nothing for an action sent in the current epoch: the client resends the identical action
            rt.polling = false;
            rt.nextAttemptAt = 0;
            this.schedulePump();
          }
        });
      })(),
    );
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // `Reject` (14 [CLI-42], [CLI-46] to [CLI-49])

  protected async onRejectResult(e: OutboxEntry, reject: Reject, decided = false): Promise<void> {
    const f = this.follows.get(e.keyspaceId);
    const now = this.serverNow();
    if (e.firstFailureMs === undefined) e.firstFailureMs = now;
    e.lastResult = { kind: "reject", what: codeName(reject.code), errorClass: reject.errorClass, atMs: now };
    this.markDirty(e);
    const known = isKnownClass(reject.errorClass);
    const reaction = reactionOf(reject.errorClass);

    // SPEC-QUESTION (ruled, Q42.2): 15 [COMPAT-39] says that a reject whose class is unknown or UNSPECIFIED "MUST be handled as HOLD", and
    // 14 [CLI-7] and [CLI-34] that a REJECTED decision is a final rejection. They meet in a `Decided{REJECTED}` whose reject has a class that
    // this client does not know. Reading A: the hold rule is the more specific one, the lane is held and the action resent with the hold
    // backoff, and at its bound it is reported as outcome unknown. Reading B: the decision is final, so the action ends at once as rejected,
    // to the dead-letter store, whatever the class; a resend of a decided intent can only get the same stored decision again, so reading A
    // reports a rejection that the server has made as an outcome that is unknown, and holds a lane that no resend can clear. Ruled by the
    // coordinator (until spec round 16 writes it into 15 [COMPAT-39]): reading B. A `Decided{REJECTED}` is final whatever its reject's
    // class (14 [CLI-34]); when the class is unknown or UNSPECIFIED it is handled as DEAD_LETTER (14 [CLI-46]), and the dead-letter entry
    // carries the reject with its class, which is how the app is told. The hold applies to a `Reject` result only.
    if (decided && !known) {
      await this.endAction(e, { sink: "deadLetter", reject, reason: codeName(reject.code), dependents: "fail" });
      return;
    }

    // the hold rows of Table 14.1 (a HOLD code, a class that the client does not know)
    const hk = holdKindOf(reject);
    if (hk !== undefined) {
      await this.enterHold(e, reject, hk);
      return;
    }
    if (known) {
      switch (reject.code) {
        case Code.PERMISSION_DENIED:
          // 14 [CLI-46]: refresh the token once and resend before treating the result as final
          if (!e.permissionRetried) {
            e.permissionRetried = true;
            this.markDirty(e);
            this.refreshThenResend(e);
            return;
          }
          break;
        case Code.ACTOR_MISMATCH:
          // 14 [CLI-46]: nothing was recorded; start a new client_id, re-stamp, and resend once under it
          if (!e.actorMismatchRetried) {
            e.actorMismatchRetried = true;
            this.markDirty(e);
            this.reidentify("ACTOR_MISMATCH");
            return;
          }
          break;
        case Code.OFFLINE_TOO_LONG: {
          // 14 [CLI-46], [CLI-57], [CLI-83]: to the export hook, with its dependents; outcome unknown when ever sent or registered
          const cause = reject.details["cause"] === "SUPPORT_WINDOW" ? "SUPPORT_WINDOW" : "HORIZON";
          const ever = e.everSent || e.registered;
          await this.endAction(e, {
            sink: "export",
            reject,
            reason: codeName(reject.code),
            dependents: "fail",
            ...(ever ? { outcomeUnknown: cause } : {}),
          });
          return;
        }
        case Code.KEYSPACE_ARCHIVED:
          if (f !== undefined) await this.keyspaceEnded(f, lifecycleOf(reject, f), f.successors, e, false, reject.successors);
          return;
        default:
          break;
      }
    }
    switch (reaction) {
      case "RETRY":
        // a `Reject` of class RETRY is `BLOB_MISSING`: upload again, then resend (TODO(09 §9.9)); resent unchanged with the backoff
        this.deferEntry(e, this.nextDelay(e, undefined));
        return;
      case "DEAD_LETTER":
        await this.endAction(e, { sink: "deadLetter", reject, reason: codeName(reject.code), dependents: "fail" });
        return;
      case "CONFLICT":
        await this.conflict(e, reject, f);
        return;
      default:
        await this.enterHold(e, reject, "UNKNOWN_HOLD");
    }
  }

  /** A `CONFLICT`-class result, a REJECTED decision included ([CLI-47], [CLI-49]). */
  protected async conflict(e: OutboxEntry, reject: Reject, f: Follow | undefined): Promise<void> {
    const cause = reject.stale?.cause;
    if (cause === StaleCause.DOC_RESET && f !== undefined) {
      // 14 [CLI-49]: the user's pending text for the key is kept as a local draft, outside the outbox, never sent or re-applied
      const m = this.content(e);
      if (m !== undefined) {
        const ops = [];
        for (const c of m.changes) {
          if (c.kind !== ChangeKind.CRDT_UPDATE || c.update.length === 0) continue;
          const k = `${f.id}\u0000${c.key}`;
          const have = this.drafts.get(k);
          const draft = { keyspaceId: f.id, key: c.key, updates: [...(have?.updates ?? []), c.update], reason: "DOC_RESET", atMs: this.serverNow() };
          this.drafts.set(k, draft);
          ops.push({ op: "putDraft" as const, entry: draft });
        }
        if (ops.length > 0) await this.tx(ops).catch(() => undefined);
      }
    }
    if (cause === StaleCause.EPOCH && f !== undefined) {
      // 14 [CLI-49]: cold-load the keyspace, then the conflict handler (an action authored in the old epoch)
      this.requestResumeCheck(f, "EPOCH_CHANGED");
    }
    this.beginConflict(e, reject);
  }

  /** Refreshes the token, then resends once (14 [CLI-46]); a refresh that fails starts the hold `NO_TOKEN` (14 [CLI-68]). */
  protected refreshThenResend(e: OutboxEntry): void {
    const rt = this.rt(e.mutationId);
    rt.nextAttemptAt = Number.MAX_SAFE_INTEGER; // not before the refresh is answered
    this.detach(
      (async () => {
        const ok = await this.refreshToken();
        await this.run(() => {
          rt.nextAttemptAt = 0;
          if (!ok) this.holdApp("NO_TOKEN", "NO_TOKEN");
          this.schedulePump();
        });
      })(),
    );
  }

  protected async refreshToken(): Promise<boolean> {
    const t = this.tokens;
    if (t === undefined) return true; // no token provider: the transport's token is the only one there is, and the resend is the one of 14 [CLI-46]
    try {
      return await t.refresh();
    } catch {
      return false;
    }
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Holds (14 [CLI-45], Table 14.1)

  protected holdApp(kind: HoldKind, code: string): Hold {
    const h = this.startHold({ kind, scope: "app", code });
    this.armProbe(h);
    return h;
  }

  protected holdKeyspace(kind: HoldKind, code: string, keyspaceId: string, boundMs?: number): Hold {
    const h = this.startHold({ kind, scope: "keyspace", keyspaceId, code, ...(boundMs !== undefined ? { boundMs } : {}) });
    this.armProbe(h);
    return h;
  }

  /**
   * A.7 "stop and report", for a `Mutate` call that failed with a status that has a row of its own and no ABPC detail: `PermissionDenied`
   * after the one refresh, `NotFound`, `AlreadyExists`, `FailedPrecondition`. The action is kept, never dropped unseen (02 [ARC-50]),
   * and the lane stops: it is held as 14 Table 14.1 holds a lane for a code that the client does not know, with the status as the hold's
   * code, which `held` and the sync status report. The head of the lane is resent with the hold backoff, which ends the hold when a
   * resend gets a result, and the default bound ends the actions (14 [CLI-45]).
   *
   * SPEC-QUESTION (Q42.5: the recommended reading stands until round 16 writes a Table 14.1 row): A.7 says "stop and report" and gives no Table 14.1 row for these statuses on a `Mutate` call. Reading A (taken): the
   * lane hold above, which reports, keeps the action, never loops (the hold backoff) and ends at the default bound as every hold does.
   * Reading B: the client stops sending for good, or dead-letters the action at once with the status as its reason. Reading B loses an
   * action to a status that a proxy or a deployment mistake can send, and has no way to resume; reading A needs no rule that the text lacks.
   */
  protected stopAndReport(entries: readonly OutboxEntry[], status: string): void {
    const lanes = new Set<string>();
    for (const e of entries) {
      e.lastResult = { kind: "rpc", what: status, atMs: this.serverNow() };
      this.markDirty(e);
      const key = `${e.keyspaceId}|${e.lane}`;
      if (lanes.has(key)) continue;
      lanes.add(key);
      const h = this.startHold({ kind: "UNKNOWN_HOLD", scope: "lane", keyspaceId: e.keyspaceId, lane: e.lane, code: status, probeId: e.mutationId });
      this.armProbe(h);
    }
  }

  /** The reaction to a `HOLD` result: keep the action, stop the scope, tell the app, and clear as the table says. */
  protected async enterHold(e: OutboxEntry, reject: Reject, kind: HoldKind): Promise<void> {
    const f = this.follows.get(e.keyspaceId);
    const rt = this.rt(e.mutationId);
    const code = kind === "UNKNOWN_HOLD" ? (isKnownClass(reject.errorClass) ? codeName(reject.code) : "CLASS_UNKNOWN") : codeName(reject.code);
    // the reject that starts a hold goes with it: an action that the hold ends before it was sent is dead-lettered with it (14 [CLI-45])
    const stamp = (h: Hold): Hold => {
      h.reject ??= reject;
      return h;
    };
    switch (kind) {
      case "UNKNOWN_OP_TYPE": {
        const h = stamp(this.startHold({ kind, scope: "lane", keyspaceId: e.keyspaceId, lane: e.lane, code, probeId: e.mutationId }));
        this.armProbe(h); // a resend of the lane's head with the hold backoff, which is all that bounds a server that keeps answering it
        if (f !== undefined) this.detach(this.reloadManifestFor(f, e, h));
        return;
      }
      case "CLIENT_FORKED":
      case "CLIENT_UNKNOWN": {
        // the lane stops; the engine re-identifies and re-stamps, which clears the hold at once (14 Table 14.1)
        this.startHold({ kind, scope: "lane", keyspaceId: e.keyspaceId, lane: e.lane, code });
        this.reidentify(code);
        this.stopHold(kind, e.keyspaceId, e.lane);
        return;
      }
      case "KEYSPACE_NOT_FOUND": {
        // the wait of 07 [MUT-101]: counted from the first KEYSPACE_NOT_FOUND, or from the end of the client's own CreateKeyspace
        const bound = this.serverNow() + this.limits.keyspaceNotFoundWaitMs;
        stamp(this.holdKeyspace(kind, code, e.keyspaceId, bound)).probeId = e.mutationId;
        return;
      }
      case "UPGRADE_REQUIRED":
        stamp(this.holdKeyspace(kind, code, e.keyspaceId));
        return;
      case "HISTORY_DIVERGED":
        stamp(this.holdKeyspace(kind, code, e.keyspaceId));
        return;
      case "UNSUPPORTED_FEATURE":
      case "UNKNOWN_HOLD": {
        const h = stamp(this.startHold({ kind, scope: "lane", keyspaceId: e.keyspaceId, lane: e.lane, code, probeId: e.mutationId }));
        this.armProbe(h);
        return;
      }
      case "APP_SUSPENDED":
        stamp(this.holdApp(kind, code));
        return;
      default:
        rt.nextAttemptAt = 0;
    }
  }

  /**
   * The hold `UNKNOWN_OP_TYPE` (14 Table 14.1): "The client reloads the keyspace's manifest version in force, the version that
   * `LoadStateResponse.manifest_version` names, with `GetManifest`". A `LoadState` says which version that is, and that version is
   * loaded again; the version that the client holds may be an older one, which declares the type that the version in force does not.
   * The type is not declared by it: "dead-lettered at once", since the server answers the code only after its duplicate lookup, so
   * nothing was recorded for the action. It is declared and the version is not the one the client used: the client resends at once.
   * It is declared by the version that the client already used: the server's answer contradicts the client's manifest, so the hold
   * stays and its probe resends the head with the hold backoff, which is what bounds a server that keeps answering it.
   */
  private async reloadManifestFor(f: Follow, e: OutboxEntry, h: Hold): Promise<void> {
    const used = f.state.manifestVersion;
    let version = 0;
    let loaded = false;
    try {
      version = await this.manifestVersionInForce(f);
      loaded = await this.manifests.reload(version);
    } catch {
      // the version, or its manifest, could not be had: the hold stays, and its probe resends the action, which brings the answer again
    }
    await this.run(async () => {
      if (!this.entries.has(e.mutationId)) {
        this.stopHold("UNKNOWN_OP_TYPE", h.keyspaceId, h.lane);
        return;
      }
      if (!loaded) return;
      const declared = this.manifests.get(version)?.opType(e.opType) !== undefined;
      if (declared && version === used) return; // the probe of the hold resends it
      this.stopHold("UNKNOWN_OP_TYPE", h.keyspaceId, h.lane);
      if (declared) {
        this.rt(e.mutationId).nextAttemptAt = 0;
        this.schedulePump();
      } else {
        // 14 Table 14.1: still unknown after the reload: dead-lettered at once; the server recorded nothing for the action
        const reject = makeReject(Code.UNKNOWN_OP_TYPE, ErrorClass.HOLD, "the op type is not declared by the manifest version in force");
        await this.endAction(e, { sink: "deadLetter", reject, reason: codeName(Code.UNKNOWN_OP_TYPE), dependents: "fail" });
      }
    });
  }

  /** Arms the next probe of a hold: a resend, a poll, a re-add, with the hold backoff ([CLI-45]). */
  protected armProbe(h: Hold): void {
    this.probeTimers.get(h.key)?.();
    if (h.kind === "UPGRADE_REQUIRED" || h.kind === "CONFLICT_WAIT" || h.kind === "CLIENT_TOO_OLD") {
      this.probeTimers.delete(h.key);
      return; // nothing is sent; the hold clears when the SDK is upgraded, or ends at its bound
    }
    const delay = fullJitter(h.attempts, HOLD_BACKOFF, this.random);
    h.probeDue = false;
    const cancel = this.after(delay, () => {
      this.probeTimers.delete(h.key);
      const live = this.holds.get(h.key);
      if (live === undefined) return;
      live.probeDue = true;
      this.onProbeDue(live);
    });
    this.probeTimers.set(h.key, cancel);
  }

  /** A probe is due: do what the hold's row says (14 Table 14.1). */
  protected onProbeDue(h: Hold): void {
    const f = h.keyspaceId !== undefined ? this.follows.get(h.keyspaceId) : undefined;
    switch (h.kind) {
      case "KEYSPACE_NOT_FOUND":
        if (f !== undefined && this.entriesOf(f.id).length === 0) {
          this.advanceProbe(h);
          this.readdKeyspace(f);
        } else this.schedulePump(); // the head is resent
        break;
      case "UNKNOWN_OP_TYPE":
      case "UNSUPPORTED_FEATURE":
      case "UNKNOWN_HOLD":
        this.schedulePump(); // the lane's head is resent, unchanged
        break;
      case "HISTORY_DIVERGED":
        if (f !== undefined) {
          this.advanceProbe(h);
          this.probeKeyspaceLoad(f);
        }
        break;
      case "PROTOCOL_ERROR":
      case "UNKNOWN_DROP_REASON":
        if (f !== undefined) {
          this.advanceProbe(h);
          this.readdKeyspace(f);
        }
        break;
      case "EPOCH_RESOLUTION":
        if (f !== undefined) {
          this.advanceProbe(h);
          this.retryEpochResolution(f);
        }
        break;
      case "NO_TOKEN":
      case "SUBJECT_REVOKED":
        // a revoked subject resumes only with a new token (09 [READ-60]): the app is asked for one with the hold backoff, as for NO_TOKEN
        this.advanceProbe(h);
        this.detach(
          (async () => {
            const ok = await this.refreshToken();
            await this.run(() => {
              if (ok) this.stopHold("NO_TOKEN"); // a revoked subject's hold ends with the `Subscribed` that a new token gets
              if (ok) this.afterTokenRefreshed();
              this.reconnectNow();
            });
          })(),
        );
        break;
      case "APP_SUSPENDED":
      case "SESSION_CAP":
      case "AUTHORIZER_UNAVAILABLE":
        this.advanceProbe(h);
        this.reconnectNow();
        break;
      default:
        break;
    }
  }

  /** A probe was made: the next waits longer, to the cap of the hold backoff. */
  protected advanceProbe(h: Hold): void {
    h.attempts++;
    h.probeDue = false;
    this.armProbe(h);
  }

  /** The probe of a lane or keyspace hold was spent by a resend of its head. */
  protected probeSpent(h: Hold): void {
    h.attempts++;
    h.probeDue = false;
    this.armProbe(h);
  }

  /** A result that is not the hold's own ends it (14 Table 14.1, "clears when"): a resend that gets another result. */
  protected clearProbeHolds(e: OutboxEntry, keep: HoldKind | undefined): void {
    for (const h of this.holds.all()) {
      if (h.kind !== "UNSUPPORTED_FEATURE" && h.kind !== "UNKNOWN_HOLD" && h.kind !== "KEYSPACE_NOT_FOUND" && h.kind !== "UNKNOWN_OP_TYPE") continue;
      if (h.keyspaceId !== e.keyspaceId || h.kind === keep) continue;
      if (h.scope === "lane" && h.lane !== e.lane) continue;
      this.stopHold(h.kind, h.keyspaceId, h.lane, h.mutationId);
    }
  }

  protected onHoldChanged(): void {
    for (const [key, cancel] of [...this.probeTimers]) {
      if (this.holds.get(key) === undefined) {
        cancel();
        this.probeTimers.delete(key);
      }
    }
    this.armBoundTimer();
    this.schedulePump();
  }

  // -- the bound of a hold --

  /** The server time at which an action blocked by the hold reaches its bound: the default bound, or the shorter one of the row. */
  private boundOf(h: Hold, e: OutboxEntry): number {
    const dflt = e.authoredServerMs + this.limits.maxOfflineMs;
    return h.boundMs !== undefined ? Math.min(h.boundMs, dflt) : dflt;
  }

  private heldEntries(h: Hold): OutboxEntry[] {
    const out: OutboxEntry[] = [];
    for (const e of this.entries.values()) {
      if (e.outcome !== undefined || this.ending.has(e.mutationId) || e.stuck !== undefined) continue;
      if (h.scope === "app" || (h.scope === "keyspace" && h.keyspaceId === e.keyspaceId) || (h.scope === "lane" && h.keyspaceId === e.keyspaceId && h.lane === e.lane)) out.push(e);
    }
    return out.sort((a, b) => a.ordinal - b.ordinal);
  }

  /** Arms the timer for the earliest bound among the held actions. */
  protected armBoundTimer(): void {
    this.boundTimer?.();
    this.boundTimer = undefined;
    let next = Infinity;
    for (const h of this.holds.all()) {
      if (h.kind === "CONFLICT_WAIT") continue; // its flow ends at the bound itself
      for (const e of this.heldEntries(h)) next = Math.min(next, this.boundOf(h, e));
    }
    if (next === Infinity) return;
    this.boundTimer = this.after(Math.max(0, next - this.serverNow()), () => this.enforceBounds());
  }

  /** Ends the actions that reached the bound of their hold: dead-lettered when never sent, outcome unknown when ever sent (14 [CLI-45]). */
  protected async enforceBounds(): Promise<void> {
    const now = this.serverNow();
    for (const h of this.holds.all()) {
      if (h.kind === "CONFLICT_WAIT") continue;
      for (const e of this.heldEntries(h)) {
        if (!this.entries.has(e.mutationId) || now < this.boundOf(h, e)) continue;
        if (e.everSent || e.registered) {
          await this.endAction(e, { sink: "export", reject: undefined, reason: "HOLD_BOUND", outcomeUnknown: "HOLD_BOUND", dependents: "fail" });
        } else {
          await this.endAction(e, { sink: "deadLetter", reject: holdReject(h), reason: h.code, dependents: "fail" });
        }
      }
    }
    this.armBoundTimer();
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Identity (14 [CLI-63], [CLI-64])

  /**
   * Starts a new `client_id` and re-stamps (14 [CLI-63]): every action without a definitive result loses its stamping, and takes a
   * fresh `client_seq` from 1 when it is next sent, in authoring order; the lane state starts over. Mutation ids and content never
   * change ([CLI-23]). A live session is rebound to the new id ([CLI-78]).
   */
  protected reidentify(_why: string): void {
    this.clientId = this.mintClientId();
    for (const f of this.follows.values()) {
      f.lanes.SEQUENCED.reset();
      f.lanes.GATED.reset();
    }
    for (const e of this.entries.values()) {
      if (e.outcome !== undefined) continue;
      e.clientId = "";
      e.clientSeq = 0;
      e.afterClientSeq = 0;
      this.markDirty(e);
      const rt = this.rt(e.mutationId);
      if (!rt.inflight) rt.nextAttemptAt = 0;
    }
    this.rebindSession();
    this.schedulePump();
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // The queued actions of a split or archived keyspace (14 [CLI-52])

  /**
   * Handles every queued action of a keyspace that was split or archived ([CLI-52]): only an action that was never sent, or whose
   * last result recorded nothing, is re-routed (a split) or dead-lettered (an archive); the others wait for the source to give them
   * a result. `trigger` is the entry whose reject told the client, when one did.
   */
  protected async keyspaceEnded(f: Follow, lifecycle: KeyspaceLifecycle, whole: readonly SuccessorRule[], trigger?: OutboxEntry, poll = true, own?: readonly SuccessorRule[]): Promise<void> {
    f.lifecycle = lifecycle;
    if (whole.length > 0) f.successors = [...whole]; // the whole map (a drop frame, a load); the rules of one reject are not it
    const entries = this.entriesOf(f.id);
    let needWhole = false;
    for (const e of entries) {
      if (!this.entries.has(e.mutationId) || e.outcome !== undefined || this.ending.has(e.mutationId) || e.stuck !== undefined) continue;
      if (this.rt(e.mutationId).inflight && e !== trigger) continue; // its answer is on the way, and decides it
      if (!this.mayMove(e, trigger)) {
        // an action with an unknown outcome, or registered as an intent, is not re-routed or dead-lettered until the source gives
        // it a result (14 [CLI-52], [CLI-24]); a GATED action whose last result is a Retry waits for the answer of
        // `GetMutationResult` as well, since the Retry may be the answer for an intent that is registered and pending, logged or not
        if (poll && !this.rt(e.mutationId).polling) this.pollNow(e);
        continue;
      }
      // the action's own reject holds the one or two rules that its keys match; another action needs the whole map, which a load gives (14 [CLI-52])
      if ((await this.moveQueued(f, e, e === trigger ? own : undefined)) === "needWhole") needWhole = true;
    }
    if (needWhole) {
      f.needsLoad = { cause: "RESUME_TOO_OLD", resume: f.loaded };
      this.kick(f);
    }
    this.followSuccessors(f.successors);
    this.schedulePump();
  }

  /**
   * Whether a queued action of a split or archived keyspace may be re-routed or dead-lettered now (14 [CLI-52]): the action whose
   * reject told the client; one that was never sent; a SEQUENCED action whose last result is a `Retry`; one whose last result is an
   * unrecorded reject. Not a GATED action whose last result is a `Retry`: "for which `GetMutationResult` from the source answers
   * `known = false` and `voided = false`", which `pollNow` asks and `mayMoveAfterAnswer` judges. An intent that is registered and
   * pending, logged or not, answers every call with a `Retry` and `intent_seq` only when it is logged (07 [MUT-8]), so a Retry says
   * nothing about registration (02 [ARC-50]: moving it would apply it twice, or end it that was decided).
   */
  private mayMove(e: OutboxEntry, trigger: OutboxEntry | undefined): boolean {
    if (e === trigger) return true;
    if (e.registered) return false;
    if (!e.everSent) return true;
    if (e.lastResult?.kind === "reject") return true;
    return e.lastResult?.kind === "retry" && e.lane === "SEQUENCED";
  }

  /** Whether the source's answer `known = false`, `voided = false` frees a GATED action whose last result was a Retry to be moved (14 [CLI-52]). */
  protected mayMoveAfterAnswer(f: Follow, e: OutboxEntry): boolean {
    return (f.stopped === "MOVED" || f.stopped === "ARCHIVED") && e.lane === "GATED" && !e.registered && e.lastResult?.kind === "retry";
  }

  /**
   * Re-routes (a split) or dead-letters (an archive, or a split action whose keys do not belong to one rule) a queued action that
   * [CLI-52] lets go. `needWhole`: the action's own reject is not given and the whole map of successors is not known yet; a load gives it.
   */
  protected async moveQueued(f: Follow, e: OutboxEntry, own: readonly SuccessorRule[] | undefined): Promise<"done" | "needWhole"> {
    const lifecycle = f.lifecycle;
    if (lifecycle !== KeyspaceLifecycle.SPLIT && lifecycle !== KeyspaceLifecycle.ARCHIVED) return "done";
    const m = this.content(e);
    const keys = new Set<string>();
    if (m !== undefined) {
      for (const c of m.changes) keys.add(c.key);
      for (const p of m.preconditions) if (p.target.case === "key") keys.add(p.target.value); // a precondition on a prefix names no key (12 [ADM-45])
    }
    const successors = own ?? f.successors;
    if (own === undefined && f.successors.length === 0) return "needWhole";
    const matched = [...keys].map((k) => matchSuccessor(successors, k));
    const rulesOf = (): SuccessorRule[] => {
      const out: SuccessorRule[] = [];
      for (const r of matched) if (r !== undefined && !out.includes(r)) out.push(r);
      return out;
    };
    if (lifecycle === KeyspaceLifecycle.SPLIT && m !== undefined && keys.size > 0 && matched.every((r) => r !== undefined && r === matched[0])) {
      const target = (matched[0] as SuccessorRule).keyspaceId;
      await this.reroute(e, m, target);
      return "done";
    }
    // archived, or a split action whose keys do not all belong to one rule: dead-lettered with KEYSPACE_ARCHIVED and the rules that
    // its keys match; nothing is re-routed and no seen_version is reset
    const reject = create(RejectSchema, {
      code: Code.KEYSPACE_ARCHIVED,
      errorClass: ErrorClass.DEAD_LETTER,
      message: lifecycle === KeyspaceLifecycle.SPLIT ? "split: the keys of the action do not belong to one successor" : "archived",
      details: { lifecycle: lifecycle === KeyspaceLifecycle.SPLIT ? "SPLIT" : "ARCHIVED" },
      successors: rulesOf(),
    });
    await this.endAction(e, { sink: "deadLetter", reject, reason: codeName(Code.KEYSPACE_ARCHIVED), dependents: "fail", slot: "release" });
    return "done";
  }

  /** Re-routes a queued action to the split successor of its keys: only `keyspace_id` changes, stored once ([CLI-22], [CLI-52]). */
  private async reroute(e: OutboxEntry, m: import("../gen/abpc/v2/sync_pb.js").Mutation, target: string): Promise<void> {
    const from = e.keyspaceId;
    const moved = create(MutationSchema, { ...m, keyspaceId: target });
    const content = encodeContent(moved);
    this.releaseSlot(e);
    const f = this.follows.get(from);
    const copy: OutboxEntry = { ...e, keyspaceId: target, content, clientId: "", clientSeq: 0, afterClientSeq: 0, ...(e.authoredIn === undefined ? { authoredIn: from } : {}) };
    await this.tx([this.putOp(copy)]);
    Object.assign(e, copy);
    this.decoded.set(e.mutationId, moved);
    f?.invalidate();
    this.emit({ type: "rerouted", mutationId: e.mutationId, from, to: target });
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Cancel (14 [CLI-81])

  /**
   * Cancels an action. One that was never sent leaves the outbox locally, with `rejected` and `CANCELLED`, and no RPC. A GATED
   * action that was sent gets `CancelIntent`, without a resend first, and the answer is its final outcome. Any other action that was
   * sent cannot be cancelled: its outcome may be recorded.
   */
  async cancel(mutationId: string): Promise<boolean> {
    return this.run(async () => {
      const e = this.entries.get(mutationId);
      if (e === undefined || this.ending.has(mutationId)) return false;
      if (!e.everSent && !e.registered) {
        await this.endAction(e, { sink: "none", reject: makeReject(Code.CANCELLED, ErrorClass.DEAD_LETTER, "cancelled by the app"), reason: codeName(Code.CANCELLED), dependents: "fail" });
        return true;
      }
      if (e.lane !== "GATED") return false;
      this.detach(
        (async () => {
          let resp;
          try {
            resp = await this.transport.cancelIntent({ $typeName: "abpc.v2.CancelIntentRequest", keyspaceId: e.keyspaceId, intentId: mutationId });
          } catch {
            return; // PERMISSION_DENIED cancels nothing: the action keeps the outcome it has, pending or unknown (14 [CLI-81])
          }
          await this.run(async () => {
            if (resp.result !== undefined && this.entries.has(mutationId)) {
              await this.applyResult(e, resp.result, "cancel");
              await this.flushDirty();
            }
          });
        })(),
      );
      return true;
    });
  }
}

/** The lifecycle that a `KEYSPACE_ARCHIVED` reject names; without one, ARCHIVED (14 [CLI-52]). */
function lifecycleOf(r: Reject, f: Follow): KeyspaceLifecycle {
  const l = r.details["lifecycle"];
  if (l === "SPLIT") return KeyspaceLifecycle.SPLIT;
  if (l === "ARCHIVED") return KeyspaceLifecycle.ARCHIVED;
  return f.lifecycle === KeyspaceLifecycle.SPLIT ? KeyspaceLifecycle.SPLIT : KeyspaceLifecycle.ARCHIVED;
}

/** The code of a hold's name, for the reject of a dead letter. */
function codeOfName(name: string): Code {
  const v = (Code as unknown as Record<string, number>)[name];
  return v !== undefined && isKnownCode(v) ? (v as Code) : Code.UNSPECIFIED;
}

/** The `Code` of a hold that has none of its own name: the code that its row of Table 14.1 stands for. */
const HOLD_KIND_CODE: Partial<Record<HoldKind, Code>> = {
  KEYSPACE_NOT_FOUND: Code.KEYSPACE_NOT_FOUND,
  UNKNOWN_OP_TYPE: Code.UNKNOWN_OP_TYPE,
  CLIENT_FORKED: Code.CLIENT_FORKED,
  CLIENT_UNKNOWN: Code.CLIENT_UNKNOWN,
  UPGRADE_REQUIRED: Code.UPGRADE_REQUIRED,
  UNSUPPORTED_FEATURE: Code.UNSUPPORTED_FEATURE,
  HISTORY_DIVERGED: Code.HISTORY_DIVERGED,
  APP_SUSPENDED: Code.APP_SUSPENDED,
  SESSION_CAP: Code.QUOTA_EXCEEDED,
};

/**
 * The reject that an action carries to the dead-letter store when the hold that stopped it reaches its bound before it was ever sent
 * (14 [CLI-45]: "dead-lettered with the hold's code"). The hold's code is `details["hold"]` whatever it is, since a hold's code may
 * be a drop reason or a condition (`NOT_FOUND`, `EPOCH_RESOLUTION`, `CLASS_UNKNOWN`) that no `Code` names; `code` is the reject that
 * started the hold, when one did (a code or a class that the client does not know included), or else the code of the hold's row.
 */
function holdReject(h: Hold): Reject {
  const message = `the hold ${h.code} reached its bound`;
  if (h.reject !== undefined) return create(RejectSchema, { ...h.reject, message, details: { ...h.reject.details, hold: h.code } });
  const named = codeOfName(h.code);
  return makeReject(named !== Code.UNSPECIFIED ? named : (HOLD_KIND_CODE[h.kind] ?? Code.UNSPECIFIED), ErrorClass.HOLD, message, { hold: h.code });
}
