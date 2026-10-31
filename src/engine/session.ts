// The session (09 §9.4 to §9.7, 14 [CLI-79], [CLI-80]): loads, the `Subscribe` stream and its frames, reconnecting, the drop and
// close reasons, and what a restore does to the client (14 [CLI-51], [CLI-53], [CLI-64]).
//
//  - one rehydration path ([CLI-8]): load confirmed state, read the outbox, resolve what is unknown, recompute the overlay;
//  - a load is made page by page, to one `checkpoint_seq` ([CLI-2]); a cold load as a resume check from the cursor after
//    `RESUME_TOO_OLD` or `EPOCH_CHANGED` ([CLI-51]);
//  - after `EPOCH_CHANGED` the keyspace is held (`EPOCH_RESOLUTION`) until every outcome of an earlier epoch is answered by
//    `GetMutationResult(s)`; then the client re-identifies and re-stamps ([CLI-53], [CLI-64]);
//  - a lost action is reported, never resubmitted, except a lost spend action, which is resent once ([CLI-38], [CLI-64]);
//  - the session ends and resumes with the backoffs of 09 [READ-57].
//
// TODO(14 §14.9, R/C3): `ACCESS_REVOKED` and `SUBJECT_REVOKED` purge and quarantine ([CLI-66], [CLI-79]); here the client holds the
// keyspace and keeps its outbox and its copy.

import { create } from "@bufbuild/protobuf";
import {
  GetMutationResultsRequestSchema,
  LoadStateRequestSchema,
  SubscribeRequestSchema,
  UpdateSubscriptionRequestSchema,
  type ControlFrame,
  type ControlFrame_KeyspaceAdded,
  type ControlFrame_KeyspaceDropped,
  type GetMutationResultResponse,
  type LoadStateResponse,
  type SubscribeResponse,
} from "../gen/abpc/v2/sync_pb.js";
import {
  Code,
  ErrorClass,
  KeyspaceCursorSchema,
  KeyspaceLifecycle,
  RejectSchema,
  RetryReason,
  StaleCause,
  StaleSchema,
  type KeyspaceCursor,
  type Reject,
  type SuccessorRule,
} from "../gen/abpc/v2/types_pb.js";
import { CloseReason, DropReason } from "../gen/abpc/v2/sync_pb.js";
import { CLIENT_BACKOFF, fullJitter, HOLD_BACKOFF, RECONNECT_BACKOFF, clampAfter, resendDelay } from "./backoff.js";
import { codeName } from "./codes.js";
import { makeReject } from "./endings.js";
import { ProtocolViolation, RpcError } from "./errors.js";
import { Follow } from "./follow.js";
import type { FoldError } from "../rules/fold/index.js";
import { SenderLayer } from "./sender.js";
import type { JournalEntry, OutboxEntry, StoredOutcome } from "./storage.js";
import type { Hold, HoldKind } from "./holds.js";

type SessionEnd =
  | { kind: "clean" }
  | { kind: "dead" }
  | { kind: "reconnect"; retryAfterMs: number }
  | { kind: "closing"; reason: CloseReason }
  | { kind: "error"; error: unknown };

/** A pending or journalled action that a restore may have lost (14 [CLI-53]). */
interface EarlierEpochItem {
  id: string;
  kind: "pending" | "journal";
  ordinal: number;
}

const SESSION_LIVE_RESET_MS = 60_000;

export abstract class SessionLayer extends SenderLayer {
  private running = false;
  private sess: {
    state: "idle" | "connecting" | "live";
    id: string;
    abort: AbortController | undefined;
    hbMs: number;
    watchdog: (() => void) | undefined;
    startedMono: number;
    closing: CloseReason | undefined;
    reconnectAfter: number | undefined;
    attempts: number;
    fatal: boolean;
  } = { state: "idle", id: "", abort: undefined, hbMs: 15_000, watchdog: undefined, startedMono: 0, closing: undefined, reconnectAfter: undefined, attempts: 0, fatal: false };
  private wakeSleep: (() => void) | undefined;
  private readonly epochFlows = new Set<string>();
  private readonly loadWaits = new Map<string, Promise<void>>();

  protected online(): boolean {
    return this.sess.state === "live";
  }

  /** The state of the connection for the sync status. */
  connection(): "live" | "connecting" | "offline" | "stopped" {
    if (this.stopped || this.sess.fatal) return "stopped";
    if (this.sess.state === "live") return "live";
    return this.sess.state === "connecting" ? "connecting" : "offline";
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Start and stop

  protected beginRunning(): void {
    if (this.running || this.stopped) return;
    this.running = true;
    this.busy++;
    void this.sessionLoop().then(
      () => {
        this.busy--;
      },
      (e: unknown) => {
        this.busy--;
        this.noteInternalError(e);
      },
    );
  }

  protected endRunning(): void {
    this.running = false;
    this.sess.abort?.abort();
    this.wake();
  }

  private wake(): void {
    const w = this.wakeSleep;
    this.wakeSleep = undefined;
    w?.();
  }

  protected reconnectNow(): void {
    if (this.sess.state === "live") return; // a live session needs no reconnect
    this.wake();
  }

  /** Sleeps on the engine's clock, or until woken. Not counted as work by `settle`. */
  private sleep(ms: number): Promise<void> {
    this.busy--;
    return new Promise<void>((resolve) => {
      let cancel: () => void = () => undefined;
      const done = (): void => {
        cancel();
        this.wakeSleep = undefined;
        this.busy++;
        resolve();
      };
      this.wakeSleep = done;
      cancel = this.clock.setTimer(Math.max(0, Math.floor(ms)), done);
    });
  }

  private hasAppHold(): Hold | undefined {
    return this.holds.all().find((h) => h.scope === "app");
  }

  private async sessionLoop(): Promise<void> {
    while (this.running && !this.stopped && !this.sess.fatal) {
      const started = this.clock.mono();
      let end: SessionEnd;
      try {
        end = await this.runSession();
      } catch (error) {
        end = { kind: "error", error };
      }
      if (!this.running || this.stopped) return;
      const lived = this.clock.mono() - started;
      await this.run(() => this.onSessionEnded(end, lived));
      if (this.sess.fatal || !this.running) return;
      const appHold = this.hasAppHold();
      if (appHold !== undefined) {
        // 14 [CLI-45]: the probe of an app-wide hold is the next subscribe, after the hold backoff (its own timer wakes the loop)
        await this.sleep(Number.MAX_SAFE_INTEGER);
      } else {
        const n = Math.max(0, this.sess.attempts - 1);
        const delay = Math.max(fullJitter(n, RECONNECT_BACKOFF, this.random), this.sess.reconnectAfter ?? 0);
        this.sess.reconnectAfter = undefined;
        await this.sleep(delay);
      }
    }
  }

  private followList(): Follow[] {
    return [...this.follows.values()];
  }

  private needsLoad(f: Follow): boolean {
    return (f.needsLoad !== undefined || !f.loaded) && f.stopped === undefined && !this.holds.has("PROTOCOL_ERROR", f.id) && !this.holds.has("HISTORY_DIVERGED", f.id) && !this.holds.has("UNKNOWN_DROP_REASON", f.id);
  }

  private subscribable(f: Follow): boolean {
    if (!f.loaded || f.stopped !== undefined || f.frozen !== undefined) return false;
    for (const h of this.holds.all()) {
      if (h.scope === "keyspace" && h.keyspaceId === f.id && h.kind !== "KEYSPACE_NOT_FOUND" && h.kind !== "EPOCH_RESOLUTION") return false;
    }
    return true;
  }

  /** One session: the loads, the `Subscribe`, its frames. Counted as work except while it waits for a frame. */
  private async runSession(): Promise<SessionEnd> {
    // the loads (09 §9.2): every followed keyspace that needs one, before it is followed
    for (const f of this.followList()) {
      if (!this.needsLoad(f)) continue;
      const ok = await this.syncKeyspace(f);
      if (!ok && !f.loaded) continue;
    }
    if (!this.running) return { kind: "clean" };
    const follows = this.followList().filter((f) => this.subscribable(f));
    const cursors: KeyspaceCursor[] = follows.map((f) => this.cursorOf(f));
    const ac = new AbortController();
    const s = this.sess;
    s.abort = ac;
    s.state = "connecting";
    s.closing = undefined;
    s.reconnectAfter = undefined;
    s.startedMono = this.clock.mono();
    const req = create(SubscribeRequestSchema, { keyspaces: cursors, supportedFeatures: [...this.features], clientId: this.clientId });
    for (const f of follows) {
      f.onSession = true;
      f.resuming = false;
      // 14 [CLI-24], [CLI-53], [CLI-64]: nothing is sent to the keyspace on this session until its `KeyspaceAdded` arrives with the
      // epoch that the client follows: the server sends `Subscribed` first and `KeyspaceDropped{EPOCH_CHANGED}` after it
      f.awaitingAdd = true;
    }
    const aborted = new Promise<{ done: true; value: undefined; aborted: true }>((resolve) => {
      ac.signal.addEventListener("abort", () => resolve({ done: true, value: undefined, aborted: true }), { once: true });
    });
    try {
      const it = this.transport.subscribe(req, { signal: ac.signal })[Symbol.asyncIterator]();
      this.armWatchdog(ac);
      for (;;) {
        this.busy--; // waiting for the next frame is not work
        let r: IteratorResult<SubscribeResponse>;
        try {
          r = (await Promise.race([it.next(), aborted])) as IteratorResult<SubscribeResponse>;
        } finally {
          this.busy++;
        }
        if (r.done === true) {
          void Promise.resolve(it.return?.()).catch(() => undefined);
          if (s.closing !== undefined) return { kind: "closing", reason: s.closing };
          if (s.reconnectAfter !== undefined) return { kind: "reconnect", retryAfterMs: s.reconnectAfter };
          return (r as { aborted?: boolean }).aborted === true ? { kind: "dead" } : { kind: "clean" };
        }
        this.armWatchdog(ac);
        const frame = r.value;
        // the manifests that the records name are loaded before the frame is applied, outside the queue (04 §4.7)
        if (frame.frame.case === "records") {
          for (const rec of frame.frame.value.records) {
            if (!(await this.manifests.ensure(rec.manifestVersion))) throw new ProtocolViolation("RecordsFrame", `no manifest version ${rec.manifestVersion}`, [frame.frame.value.keyspaceId]);
          }
        }
        await this.run(() => this.onFrame(frame));
        if (s.closing !== undefined) {
          ac.abort();
          void Promise.resolve(it.return?.()).catch(() => undefined);
          return { kind: "closing", reason: s.closing };
        }
        if (s.reconnectAfter !== undefined) {
          ac.abort();
          void Promise.resolve(it.return?.()).catch(() => undefined);
          return { kind: "reconnect", retryAfterMs: s.reconnectAfter };
        }
      }
    } catch (error) {
      return { kind: "error", error };
    } finally {
      s.watchdog?.();
      s.watchdog = undefined;
      s.state = "idle";
      s.id = "";
      for (const f of this.followList()) {
        f.onSession = false;
        f.awaitingAdd = false;
      }
      ac.abort();
    }
  }

  /** The client deadline of 09 [READ-56]: 60 s without a frame, or three heartbeat intervals when that is shorter, ends the session. */
  private armWatchdog(ac: AbortController): void {
    this.sess.watchdog?.();
    const ms = Math.min(60_000, 3 * this.sess.hbMs);
    this.sess.watchdog = this.clock.setTimer(ms, () => {
      this.sess.watchdog = undefined;
      ac.abort();
    });
  }

  private cursorOf(f: Follow): KeyspaceCursor {
    const useHash = f.lastRecordHash !== undefined && f.lastRecordSeq === f.watermark && f.watermark > 0n;
    return create(KeyspaceCursorSchema, {
      keyspaceId: f.id,
      afterSeq: f.watermark,
      logEpoch: f.logEpoch,
      ...(useHash ? { afterRecordHash: f.lastRecordHash as Uint8Array } : {}),
    });
  }

  /** What follows the end of a session (09 [READ-57]): the attempt count, and the keyspaces that the session no longer follows. */
  private onSessionEnded(end: SessionEnd, livedMs: number): void {
    const s = this.sess;
    if (livedMs >= SESSION_LIVE_RESET_MS) s.attempts = 0;
    else s.attempts++;
    for (const f of this.followList()) {
      f.onSession = false;
      f.resuming = false;
      f.awaitingAdd = false;
    }
    switch (end.kind) {
      case "reconnect":
        s.reconnectAfter = clampAfter(end.retryAfterMs);
        break;
      case "closing":
        break; // the holds were set when the frame came (14 [CLI-79])
      case "error":
        this.onSessionError(end.error);
        break;
      default:
        break;
    }
  }

  /** The stream ended with a status, or a frame broke 02 [ARC-61] (A.7, 14 [CLI-50]). */
  private onSessionError(error: unknown): void {
    if (error instanceof ProtocolViolation) {
      this.onProtocolViolation(error, error.keyspaces);
      return;
    }
    if (!(error instanceof RpcError)) return; // a transport failure: the session resumes with backoff
    const r = error.reject;
    switch (error.status) {
      case "Unauthenticated":
        if (r?.details["cause"] === "SUBJECT_REVOKED") {
          // TODO(14 [CLI-66], [CLI-79]): purge and quarantine
          this.holdApp("SUBJECT_REVOKED", "SUBJECT_REVOKED");
        } else this.noToken();
        break;
      case "PermissionDenied":
        if (r?.code === Code.APP_SUSPENDED) this.holdApp("APP_SUSPENDED", codeName(Code.APP_SUSPENDED));
        break;
      case "ResourceExhausted":
        if (r?.code === Code.QUOTA_EXCEEDED && r.details["quota"] === "max_sessions_per_principal") this.holdApp("SESSION_CAP", codeName(Code.QUOTA_EXCEEDED));
        break;
      case "Unavailable":
        if (error.retry?.reason === 6 /* AUTHORIZER_UNAVAILABLE */) this.holdApp("AUTHORIZER_UNAVAILABLE", "AUTHORIZER_UNAVAILABLE");
        else if (error.retry?.reason === RetryReason.KEYSPACE_SEEDING) {
          // 14 [CLI-44]: a `Subscribe` that names a successor that is not seeded yet is refused as a whole; the client sends the whole
          // `Subscribe` again, unchanged, with the backoff of [CLI-43]; a `Retry` has no age bound of its own, and no hold
          this.sess.reconnectAfter = Math.max(clampAfter(error.retry.afterMs), fullJitter(Math.max(0, this.sess.attempts - 1), CLIENT_BACKOFF, this.random));
        }
        break;
      case "Unimplemented":
        this.sess.fatal = true;
        this.emit({ type: "protocol-error", what: "Subscribe", detail: "Unimplemented: the server does not speak abpc.v2 (15 [COMPAT-6])", keyspaces: [] });
        break;
      default:
        break;
    }
  }

  /** The hold NO_TOKEN: the client has no valid token (14 [CLI-68]); it asks the app for one, again with the hold backoff. */
  private noToken(): void {
    this.holdApp("NO_TOKEN", "NO_TOKEN");
  }

  protected onUnimplemented(_f: Follow): void {
    this.sess.fatal = true;
    this.emit({ type: "protocol-error", what: "Mutate", detail: "Unimplemented: the server does not speak abpc.v2 (15 [COMPAT-6])", keyspaces: [] });
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Frames (09 §9.6)

  private async onFrame(frame: SubscribeResponse): Promise<void> {
    const body = frame.frame;
    switch (body.case) {
      case "records": {
        const f = this.follows.get(body.value.keyspaceId);
        if (f !== undefined) await this.applyFrame(f, body.value);
        return;
      }
      case "heartbeat":
        await this.applyHeartbeat(body.value);
        return;
      case "control":
        await this.onControl(body.value);
        return;
      default:
        return; // a frame that the client does not know, and presence, change no cursor, keyspace or session state (09 [READ-53])
    }
  }

  private async onControl(c: ControlFrame): Promise<void> {
    const body = c.control;
    switch (body.case) {
      case "subscribed": {
        const s = this.sess;
        s.state = "live";
        s.id = body.value.sessionId;
        s.hbMs = body.value.heartbeatIntervalMs > 0 ? body.value.heartbeatIntervalMs : 15_000;
        this.applyLimits(body.value.limits);
        // 14 Table 14.1: a Subscribe that succeeds clears the holds that waited for one
        for (const kind of ["SESSION_CAP", "AUTHORIZER_UNAVAILABLE", "APP_SUSPENDED", "NO_TOKEN", "CLIENT_TOO_OLD"] as HoldKind[]) this.stopHold(kind);
        this.armWatchdogFromSession();
        this.schedulePump();
        // the keyspaces added while connecting
        for (const f of this.followList()) if (f.loaded && !f.onSession && this.subscribable(f)) this.addToSession(f);
        return;
      }
      case "keyspaceAdded":
        await this.onKeyspaceAdded(body.value);
        return;
      case "keyspaceDropped":
        await this.onKeyspaceDropped(body.value);
        return;
      case "reconnect":
        this.sess.reconnectAfter = clampAfter(body.value.retryAfterMs);
        return;
      case "closing":
        await this.onClosing(body.value.reason);
        return;
      default:
        return;
    }
  }

  private armWatchdogFromSession(): void {
    const ac = this.sess.abort;
    if (ac !== undefined) this.armWatchdog(ac);
  }

  private async onKeyspaceAdded(a: ControlFrame_KeyspaceAdded): Promise<void> {
    const cur = a.cursor;
    if (cur === undefined) return;
    const f = this.follows.get(cur.keyspaceId);
    if (f === undefined || !f.loaded) return;
    f.awaitingAdd = false;
    if (cur.logEpoch !== 0 && cur.logEpoch !== f.logEpoch) {
      this.onEpochMismatch(f);
      return;
    }
    // 09 [READ-43]: the `after_seq` of a `KeyspaceAdded` is a delivery watermark; the client supplies the state up to it itself
    f.onSession = true;
    f.watermark = cur.afterSeq;
    f.resuming = false;
    this.schedulePump(); // the epoch is the one that the client follows: the keyspace may be sent to
    if (cur.afterSeq > f.state.seq) {
      f.state.setCursor(cur.afterSeq, f.logEpoch, f.state.manifestVersion);
      await this.afterCursorMoved(f);
    }
  }

  /** The drop reasons of A.6 (14 [CLI-51], [CLI-52], [CLI-80], 15 [COMPAT-42]). */
  private async onKeyspaceDropped(d: ControlFrame_KeyspaceDropped): Promise<void> {
    const f = this.follows.get(d.keyspaceId);
    if (f === undefined) return;
    f.onSession = false;
    f.resuming = false;
    f.awaitingAdd = false; // the drop reason decides what the client does with the keyspace now
    const successors: SuccessorRule[] = [...d.successors];
    switch (d.reason) {
      case DropReason.GRANT_CHANGED:
        // 14 [CLI-67]: stop following; the outbox stays; its actions get PERMISSION_DENIED when sent
        f.stopped = "GRANT_CHANGED";
        await this.retireAtOnce(f);
        return;
      case DropReason.ACCESS_REVOKED:
        // TODO(14 [CLI-66], R/C3 §14.9): purge the purge list and quarantine the outbox; the client only holds the keyspace here
        f.stopped = "ACCESS_REVOKED";
        this.holdKeyspace("ACCESS_REVOKED", "ACCESS_REVOKED", f.id);
        await this.retireAtOnce(f);
        return;
      case DropReason.MOVED:
        f.stopped = "MOVED";
        await this.keyspaceEnded(f, KeyspaceLifecycle.SPLIT, successors);
        await this.retireAtOnce(f);
        return;
      case DropReason.RESUME_TOO_OLD:
        f.needsLoad = { cause: "RESUME_TOO_OLD", resume: true };
        this.kick(f);
        return;
      case DropReason.EPOCH_CHANGED:
        this.startEpochChange(f);
        return;
      case DropReason.HISTORY_DIVERGED:
        this.holdKeyspace("HISTORY_DIVERGED", codeName(Code.HISTORY_DIVERGED), f.id);
        return;
      case DropReason.ARCHIVED:
        f.stopped = "ARCHIVED";
        await this.keyspaceEnded(f, KeyspaceLifecycle.ARCHIVED, successors);
        await this.retireAtOnce(f);
        return;
      case DropReason.DELETED:
        f.stopped = "DELETED";
        await this.keyspaceDeleted(f);
        return;
      case DropReason.UPGRADE_REQUIRED:
        this.holdKeyspace("UPGRADE_REQUIRED", "UPGRADE_REQUIRED", f.id);
        return;
      case DropReason.NOT_FOUND:
        f.stopped = "NOT_FOUND";
        this.holdKeyspace("KEYSPACE_NOT_FOUND", "NOT_FOUND", f.id, this.serverNow() + this.limits.keyspaceNotFoundWaitMs);
        await this.retireAtOnce(f);
        return;
      default:
        // 15 [COMPAT-42]: stop following, keep the local copy, hold the queued actions, report, and add the keyspace again with the hold backoff
        f.stopped = "UNKNOWN_DROP_REASON";
        this.holdKeyspace("UNKNOWN_DROP_REASON", `DROP_REASON_${d.reason}`, f.id);
    }
  }

  /** 14 [CLI-7]: when the client stops following a keyspace, an action with a successful outcome in the epoch of its cursor retires at once. */
  private async retireAtOnce(f: Follow): Promise<void> {
    await this.checkRetirements(f); // the same rule judges an outcome that is learned later (a poll, a resend)
  }

  /** `DELETED` (14 [CLI-7], [CLI-83]): the actions of the keyspace end; one whose outcome cannot be learned is reported as outcome unknown. */
  private async keyspaceDeleted(f: Follow): Promise<void> {
    await this.retireAtOnce(f);
    for (const e of this.entriesOf(f.id)) {
      if (!this.entries.has(e.mutationId) || e.outcome !== undefined || this.ending.has(e.mutationId)) continue;
      if (!e.everSent && !e.registered) {
        const reject = makeReject(Code.KEYSPACE_DELETED, ErrorClass.DEAD_LETTER, "the keyspace was deleted");
        await this.endAction(e, { sink: "deadLetter", reject, reason: codeName(Code.KEYSPACE_DELETED), dependents: "fail" });
      } else {
        this.pollNow(e); // `GetMutationResult` answers from bindings alone, whatever the keyspace's state (07 [MUT-53])
      }
    }
  }

  /** `Closing` (14 [CLI-79], A.8). */
  private async onClosing(reason: CloseReason): Promise<void> {
    this.sess.closing = reason;
    switch (reason) {
      case CloseReason.TOKEN_EXPIRED:
        // refresh the token, then subscribe again from the cursors; without a valid token the hold NO_TOKEN applies
        if (!(await this.refreshOutsideQueue())) this.noToken();
        return;
      case CloseReason.SUBJECT_REVOKED:
        // TODO(14 [CLI-66], [CLI-79]): purge the purge list and quarantine the outboxes of every keyspace of the subject
        this.holdApp("SUBJECT_REVOKED", "SUBJECT_REVOKED");
        return;
      case CloseReason.CLIENT_TOO_OLD:
        this.holdApp("CLIENT_TOO_OLD", "CLIENT_TOO_OLD");
        return;
      case CloseReason.APP_SUSPENDED:
        this.holdApp("APP_SUSPENDED", codeName(Code.APP_SUSPENDED));
        return;
      default:
        // UNSPECIFIED, or a value that the client does not know: a dead session, resumed with backoff (14 [CLI-79], 09 [READ-61])
        this.sess.closing = undefined;
        this.sess.reconnectAfter = 0;
    }
  }

  private async refreshOutsideQueue(): Promise<boolean> {
    if (this.tokens === undefined) return false;
    return this.refreshToken();
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Loads and adds (09 §9.2, §9.7)

  /** Wakes the loop, or syncs one keyspace on a live session. */
  protected kick(f?: Follow): void {
    if (!this.running) return;
    if (f !== undefined && this.sess.state === "live") {
      this.detach(this.syncAndAdd(f));
      return;
    }
    this.wake();
  }

  private addToSession(f: Follow): void {
    this.detach(this.addNow(f));
  }

  /**
   * Names the keyspace to the live session with `UpdateSubscription` and acts on the rejection of the add (09 [READ-62]). Nothing is
   * sent to the keyspace until its `KeyspaceAdded` has come (see `runSession`).
   */
  private async addNow(f: Follow): Promise<void> {
    f.awaitingAdd = true;
    let resp;
    try {
      resp = await this.transport.updateSubscription(create(UpdateSubscriptionRequestSchema, { sessionId: this.sess.id, add: [this.cursorOf(f)] }));
    } catch {
      this.sess.abort?.abort(); // the session resumes from the cursors (09 [READ-57])
      return;
    }
    await this.run(() => {
      for (const rej of resp.rejected) {
        if (rej.keyspaceId !== f.id) continue;
        if (rej.outcome.case === "retry") {
          // `KEYSPACE_SEEDING`, `AUTHORIZER_UNAVAILABLE`: a `Retry`, which has no age bound of its own (14 [CLI-44], 04 Table 4.11). It is no
          // hold of Table 14.1: the keyspace is added again, unchanged, after the larger of `after_ms` and the client backoff (14 [CLI-43])
          const delay = resendDelay(f.addFailures++, rej.outcome.value.afterMs, this.random);
          this.after(delay, () => {
            if (this.sess.state === "live" && !f.onSession && this.subscribable(f)) this.addToSession(f);
          });
        } else if (rej.outcome.case === "reject") {
          f.awaitingAdd = false;
          const code = rej.outcome.value.code;
          if (code === Code.PERMISSION_DENIED) f.stopped = "GRANT_CHANGED";
          else if (code === Code.KEYSPACE_NOT_FOUND) {
            f.stopped = "NOT_FOUND";
            this.holdKeyspace("KEYSPACE_NOT_FOUND", "NOT_FOUND", f.id, this.serverNow() + this.limits.keyspaceNotFoundWaitMs);
          }
        }
        return;
      }
      f.onSession = true;
      f.addFailures = 0;
    });
  }

  /** Loads a keyspace that needs it, then adds it to the live session. */
  private async syncAndAdd(f: Follow): Promise<void> {
    const ok = await this.syncKeyspace(f);
    if (!ok || this.sess.state !== "live" || !this.subscribable(f)) return;
    await this.addNow(f);
  }

  /** Re-adds a keyspace after a hold (14 [CLI-45]): a load as a resume check, then the add. */
  protected readdKeyspace(f: Follow): void {
    f.stopped = undefined;
    f.needsLoad = { cause: f.loaded ? "RESUME_TOO_OLD" : "COLD_LOAD", resume: f.loaded };
    this.kick(f);
  }

  protected probeKeyspaceLoad(f: Follow): void {
    // 09 [READ-70]: polling a frozen keyspace with LoadState; a load that is not refused with HISTORY_DIVERGED ends the hold
    f.needsLoad = { cause: "EPOCH_CHANGED", resume: f.loaded };
    this.kick(f);
  }

  protected requestResumeCheck(f: Follow, cause: string): void {
    f.needsLoad = { cause, resume: f.loaded };
    this.kick(f);
  }

  protected whenLoaded(f: Follow): Promise<void> {
    return this.loadWaits.get(f.id) ?? Promise.resolve();
  }

  protected followSuccessors(rules: readonly SuccessorRule[]): void {
    const added: Follow[] = [];
    for (const r of rules) {
      if (r.keyspaceId === "" || this.follows.has(r.keyspaceId)) continue;
      const nf = new Follow(r.keyspaceId, this.random);
      nf.needsLoad = { cause: "COLD_LOAD", resume: false };
      this.follows.set(r.keyspaceId, nf);
      added.push(nf);
    }
    // 09 [READ-70]: each successor is cold-loaded and added; on a live session at once, otherwise with the next session
    for (const nf of added) this.kick(nf);
    if (added.length === 0) this.kick();
  }

  protected resumeKeyspace(f: Follow): void {
    // 14 [CLI-3], 09 [READ-46]: resume the keyspace from the client's own cursor, by a re-add
    if (this.sess.state !== "live") return;
    f.resuming = true;
    this.addToSession(f);
  }

  protected rebindSession(): void {
    if (this.sess.state !== "live") return;
    this.detach(
      (async () => {
        try {
          await this.transport.updateSubscription(create(UpdateSubscriptionRequestSchema, { sessionId: this.sess.id, clientId: this.clientId }));
        } catch {
          this.sess.abort?.abort();
        }
        // TODO(09 §9.8, 14 [CLI-78]): publish presence again at once, in every topic where the client has an entry
      })(),
    );
  }

  /**
   * The manifest version in force for a keyspace: the `manifest_version` of a `LoadState` (14 Table 14.1). A resume check from the
   * cursor is the cheapest load (09 [READ-17]), a page of one entry bounds a cold one, and nothing of it is installed.
   */
  protected async manifestVersionInForce(f: Follow): Promise<number> {
    const cursor = create(KeyspaceCursorSchema, f.loaded ? this.cursorFields(f) : { keyspaceId: f.id });
    const resp = await this.transport.loadState(create(LoadStateRequestSchema, { cursor, clientId: this.clientId, pageToken: "", pageSize: 1, supportedFeatures: [...this.features] }));
    return resp.manifestVersion;
  }

  /** Loads a keyspace: the pages, then the install (14 [CLI-2], [CLI-18], 09 [READ-18]). Returns false when the load did not happen. */
  protected async syncKeyspace(f: Follow): Promise<boolean> {
    const need = f.needsLoad ?? (f.loaded ? undefined : { cause: "COLD_LOAD", resume: false });
    if (need === undefined) return true;
    if (f.loading) return (await this.loadWaits.get(f.id), f.needsLoad === undefined && f.loaded);
    f.loading = true;
    let release: () => void = () => undefined;
    this.loadWaits.set(f.id, new Promise<void>((r) => (release = r)));
    try {
      let pages: LoadStateResponse[];
      try {
        pages = await this.fetchLoad(f, need.resume && f.loaded);
      } catch (e) {
        await this.run(() => this.onLoadError(f, e));
        return false;
      }
      return await this.run(async () => {
        try {
          const out = this.installLoad(f, pages, need.cause, need.resume);
          f.loadFailures = 0;
          this.stopHold("PROTOCOL_ERROR", f.id);
          this.stopHold("HISTORY_DIVERGED", f.id);
          this.stopHold("UNKNOWN_DROP_REASON", f.id);
          this.stopHold("KEYSPACE_NOT_FOUND", f.id);
          f.stopped = f.stopped === "GRANT_CHANGED" ? f.stopped : undefined;
          const first = pages[0] as LoadStateResponse;
          if (first.lifecycle === KeyspaceLifecycle.ARCHIVED || first.lifecycle === KeyspaceLifecycle.SPLIT) {
            f.stopped = first.lifecycle === KeyspaceLifecycle.SPLIT ? "MOVED" : "ARCHIVED";
            await this.keyspaceEnded(f, first.lifecycle, first.successors);
          }
          this.checkReopenedKeyspaceHolds(f);
          if (out.epochChanged) this.startEpochChange(f, false);
          else this.needEpochResolution(f);
          await this.afterCursorMoved(f);
          return true;
        } catch (e) {
          if (e instanceof ProtocolViolation) {
            this.onProtocolViolation(e, [f.id]);
            return false;
          }
          throw e;
        }
      });
    } finally {
      f.loading = false;
      this.loadWaits.delete(f.id);
      release();
    }
  }

  /** The hold `EPOCH_RESOLUTION` is the epoch flow's own; a load in a new epoch clears nothing else here. */
  private checkReopenedKeyspaceHolds(_f: Follow): void {
    // a keyspace that was frozen and reopened is cold-loaded in its new epoch; `installLoad` made that load
  }

  /** The pages of a load, one `checkpoint_seq` and one epoch (09 [READ-7]); a refused page token restarts the load (A.7). */
  private async fetchLoad(f: Follow, resume: boolean): Promise<LoadStateResponse[]> {
    for (let restarts = 0; restarts < 3; restarts++) {
      const cursor = create(KeyspaceCursorSchema, resume ? this.cursorFields(f) : { keyspaceId: f.id });
      const pages: LoadStateResponse[] = [];
      let token = "";
      try {
        for (;;) {
          const resp = await this.transport.loadState(create(LoadStateRequestSchema, { cursor, clientId: this.clientId, pageToken: token, pageSize: 0, supportedFeatures: [...this.features] }));
          const first = pages[0];
          if (first !== undefined && (resp.checkpointSeq !== first.checkpointSeq || resp.logEpoch !== first.logEpoch)) {
            throw new ProtocolViolation("LoadStateResponse", "the pages of a load differ in checkpoint_seq or log_epoch (09 [READ-7])", [f.id]);
          }
          pages.push(resp);
          if (resp.nextPageToken === "") break;
          token = resp.nextPageToken;
        }
        const first = pages[0] as LoadStateResponse;
        if (!(await this.manifests.ensure(first.manifestVersion))) throw new ProtocolViolation("LoadStateResponse", `no manifest version ${first.manifestVersion}`, [f.id]);
        return pages;
      } catch (e) {
        // 09 [READ-11], A.7: a `page_token` that is refused restarts the load without it
        if (e instanceof RpcError && token !== "" && ((e.status === "FailedPrecondition" && e.reject === undefined) || (e.status === "InvalidArgument" && e.reject?.code === Code.INVALID_ENVELOPE))) continue;
        throw e;
      }
    }
    throw new RpcError("Unavailable", "the page tokens of the load kept expiring");
  }

  private cursorFields(f: Follow): { keyspaceId: string; afterSeq: bigint; logEpoch: number; afterRecordHash?: Uint8Array } {
    const useHash = f.lastRecordHash !== undefined && f.lastRecordSeq === f.watermark && f.watermark > 0n;
    return { keyspaceId: f.id, afterSeq: f.watermark, logEpoch: f.logEpoch, ...(useHash ? { afterRecordHash: f.lastRecordHash as Uint8Array } : {}) };
  }

  /** A load that failed (A.7, 14 [CLI-50]). */
  private onLoadError(f: Follow, e: unknown): void {
    if (e instanceof ProtocolViolation) {
      this.onProtocolViolation(e, [f.id]);
      return;
    }
    if (!(e instanceof RpcError)) return;
    const r = e.reject;
    switch (e.status) {
      case "FailedPrecondition":
        if (r?.code === Code.HISTORY_DIVERGED) this.holdKeyspace("HISTORY_DIVERGED", codeName(Code.HISTORY_DIVERGED), f.id);
        else if (r?.code === Code.UPGRADE_REQUIRED) this.holdKeyspace("UPGRADE_REQUIRED", "UPGRADE_REQUIRED", f.id);
        break;
      case "NotFound":
        if (r?.code === Code.KEYSPACE_NOT_FOUND || r === undefined) {
          f.stopped = "NOT_FOUND";
          this.holdKeyspace("KEYSPACE_NOT_FOUND", codeName(Code.KEYSPACE_NOT_FOUND), f.id, this.serverNow() + this.limits.keyspaceNotFoundWaitMs);
        } else if (r.code === Code.KEYSPACE_DELETED) {
          f.stopped = "DELETED";
          this.post(() => this.keyspaceDeleted(f));
        }
        break;
      case "PermissionDenied":
        if (r?.code === Code.APP_SUSPENDED) this.holdApp("APP_SUSPENDED", codeName(Code.APP_SUSPENDED));
        else if (r?.code === Code.PERMISSION_DENIED) f.stopped = "GRANT_CHANGED";
        break;
      case "Unauthenticated":
        this.noToken();
        break;
      case "Unimplemented":
        this.sess.fatal = true;
        this.emit({ type: "protocol-error", what: "LoadState", detail: "Unimplemented (15 [COMPAT-6])", keyspaces: [f.id] });
        break;
      default:
        // transient: the session loop tries again with the reconnect backoff, and a live session loads the keyspace again with the
        // client backoff (14 [CLI-43]): the load of a keyspace that is added to a live session is no part of the session loop
        this.sess.attempts = Math.max(this.sess.attempts, 1);
        if (this.sess.state === "live") {
          const delay = resendDelay(f.loadFailures++, undefined, this.random);
          this.after(delay, () => {
            if (this.needsLoad(f)) this.kick(f);
          });
        }
    }
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Stops: the fold, a message that breaks 02 [ARC-61] (14 [CLI-4], [CLI-50])

  protected onFoldStop(f: Follow, err: FoldError): void {
    // 14 [CLI-4]: the confirmed state stays at the previous seq; the record is not skipped or partly applied; the keyspace is held
    f.frozen = `${err.code}: ${err.detail}`;
    this.holdKeyspace("UPGRADE_REQUIRED", "UPGRADE_REQUIRED", f.id);
  }

  protected onProtocolViolation(v: ProtocolViolation, keyspaces: readonly string[] = []): void {
    const targets = keyspaces.length > 0 ? keyspaces : v.keyspaces.length > 0 ? v.keyspaces : this.followList().map((f) => f.id);
    this.emit({ type: "protocol-error", what: v.messageType, detail: v.message, keyspaces: [...targets] });
    for (const id of targets) {
      const f = this.follows.get(id);
      if (f === undefined) continue;
      f.onSession = false;
      f.needsLoad = { cause: "COLD_LOAD", resume: f.loaded }; // a re-add that gets good messages cold-loads the keyspace (14 [CLI-51])
      this.holdKeyspace("PROTOCOL_ERROR", "PROTOCOL_ERROR", id);
    }
    this.sess.abort?.abort();
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // A restore (14 [CLI-51], [CLI-53], [CLI-64])

  protected onEpochMismatch(f: Follow): void {
    this.startEpochChange(f);
  }

  /** `EPOCH_CHANGED`: hold the keyspace, cold-load it as a resume check, resolve the outcomes of the earlier epoch, re-identify. */
  protected startEpochChange(f: Follow, needLoad = true): void {
    f.onSession = false;
    this.startHold({ kind: "EPOCH_RESOLUTION", scope: "keyspace", keyspaceId: f.id, code: "EPOCH_RESOLUTION" });
    if (needLoad) f.needsLoad = { cause: "EPOCH_CHANGED", resume: f.loaded };
    this.epochFlows.delete(f.id);
    if (needLoad) {
      this.detach(
        (async () => {
          const ok = await this.syncKeyspace(f);
          if (!ok) return; // the probe of the hold tries again (14 Table 14.1)
          await this.run(() => this.beginEpochResolution(f));
        })(),
      );
    } else {
      this.beginEpochResolution(f);
    }
  }

  private earlierEpochItems(f: Follow): EarlierEpochItem[] {
    const out: EarlierEpochItem[] = [];
    const epoch = f.logEpoch;
    for (const e of this.entriesOf(f.id)) {
      if (e.stuck !== undefined || this.ending.has(e.mutationId)) continue;
      const o = e.outcome;
      const decidedRejected = o?.kind === "decided" && o.status === "REJECTED";
      if (o !== undefined && !decidedRejected && o.logEpoch < epoch) out.push({ id: e.mutationId, kind: "pending", ordinal: e.ordinal });
      else if (o === undefined && (e.registered || e.everSent) && e.sentInEpoch > 0 && e.sentInEpoch < epoch) out.push({ id: e.mutationId, kind: "pending", ordinal: e.ordinal });
    }
    for (const j of this.journal.values()) {
      if (j.keyspaceId === f.id && j.verifiedEpoch < epoch) out.push({ id: j.mutationId, kind: "journal", ordinal: j.ordinal });
    }
    return out.sort((a, b) => a.ordinal - b.ordinal);
  }

  protected needEpochResolution(f: Follow): void {
    if (this.epochFlows.has(f.id) || !f.loaded) return;
    if (this.earlierEpochItems(f).length === 0) return;
    this.startHold({ kind: "EPOCH_RESOLUTION", scope: "keyspace", keyspaceId: f.id, code: "EPOCH_RESOLUTION" });
    this.beginEpochResolution(f);
  }

  protected retryEpochResolution(f: Follow): void {
    this.epochFlows.delete(f.id);
    this.beginEpochResolution(f);
  }

  private beginEpochResolution(f: Follow): void {
    if (this.epochFlows.has(f.id)) return;
    this.epochFlows.add(f.id);
    const hold = this.startHold({ kind: "EPOCH_RESOLUTION", scope: "keyspace", keyspaceId: f.id, code: "EPOCH_RESOLUTION" });
    this.armProbe(hold);
    this.detach(this.resolveFlow(f));
  }

  /** Asks `GetMutationResult(s)` about every action whose outcome came from an earlier epoch, in the new epoch (14 [CLI-53]). */
  private async resolveFlow(f: Follow): Promise<void> {
    const items = await this.run(() => this.earlierEpochItems(f));
    const answers = new Map<string, GetMutationResultResponse>();
    try {
      for (let i = 0; i < items.length; i += 100) {
        const chunk = items.slice(i, i + 100);
        const resp = await this.transport.getMutationResults(create(GetMutationResultsRequestSchema, { keyspaceId: f.id, mutationIds: chunk.map((c) => c.id) }));
        if (resp.results.length !== chunk.length) throw new ProtocolViolation("GetMutationResultsResponse", "one answer for each id was expected", [f.id]);
        chunk.forEach((c, k) => answers.set(c.id, resp.results[k] as GetMutationResultResponse));
      }
    } catch (e) {
      await this.run(() => {
        this.epochFlows.delete(f.id);
        if (e instanceof ProtocolViolation) this.onProtocolViolation(e, [f.id]);
        else if (e instanceof RpcError && e.reject?.code === Code.HISTORY_DIVERGED) {
          // 14 [CLI-53]: while the keyspace is frozen this is no answer: the client holds the action and asks again once the keyspace reopens
          this.holdKeyspace("HISTORY_DIVERGED", codeName(Code.HISTORY_DIVERGED), f.id);
        }
        // any other failure: the hold EPOCH_RESOLUTION stays, and its probe asks again with the hold backoff
      });
      return;
    }
    // 14 [CLI-53]: the events of the answers come in the authoring order of the actions, journalled and pending alike, so an action's
    // events (`reverted`, `conflict`, the handler, `rejected`) all come before those of the next action: each answer is acted on as a task of
    // its own, and the conflict that it began is finished, outside the queue, before the next answer is acted on
    for (const it of items) {
      const ans = answers.get(it.id);
      if (ans === undefined) continue;
      await this.run(() => this.applyAnswer(f, it, ans));
      await this.conflictFlows.get(it.id)?.catch(() => undefined);
    }
    await this.run(async () => {
      await this.flushDirty();
      await this.checkRetirements(f);
      this.epochFlows.delete(f.id);
      // every answer is in: re-identify and re-stamp, then the keyspace is sent to again ([CLI-63], [CLI-64])
      this.reidentify("EPOCH_CHANGED");
      this.stopHold("EPOCH_RESOLUTION", f.id);
      if (this.sess.state === "live" && !f.onSession && this.subscribable(f)) this.addToSession(f);
      else this.kick();
    });
  }

  /** The reject that a voided outcome ends with (14 [CLI-53], A.1): `STALE` with cause `EPOCH`. */
  private lostReject(): Reject {
    return create(RejectSchema, {
      code: Code.STALE,
      errorClass: ErrorClass.CONFLICT,
      message: "the outcome of the action was lost to a restore",
      stale: create(StaleSchema, { key: "", currentSeq: 0n, cause: StaleCause.EPOCH }),
    });
  }

  /** Acts on one answer (14 [CLI-53], [CLI-38]); the answers are taken in the authoring order, journalled and pending alike. */
  private async applyAnswer(f: Follow, it: EarlierEpochItem, ans: GetMutationResultResponse): Promise<void> {
    if (it.kind === "pending") {
      const e = this.entries.get(it.id);
      if (e === undefined || this.ending.has(it.id)) return;
      await this.answerPending(f, e, ans);
    } else {
      const j = this.journal.get(it.id);
      if (j === undefined) return;
      await this.answerJournal(f, j, ans);
    }
    await this.flushDirty();
  }

  private async answerPending(f: Follow, e: OutboxEntry, ans: GetMutationResultResponse): Promise<void> {
    const spend = (this.opInfoFor(e)?.spend ?? false) && !e.spendResent;
    if (ans.known && ans.result !== undefined) {
      const o = ans.result.outcome;
      if (o.case === "appended" || (o.case === "decided" && o.value.status === 1)) {
        // the outcome survived the restore: the action retires on it once the cursor reaches its seq (14 [CLI-7])
        const out: StoredOutcome =
          o.case === "appended"
            ? { kind: "appended", seq: o.value.seq, logEpoch: f.logEpoch, overwroteNewer: [...o.value.overwroteNewer] }
            : { kind: "decided", status: "COMMITTED", decisionSeq: o.value.decisionSeq, logEpoch: f.logEpoch };
        this.setOutcome(e, out);
        this.markDirty(e);
      } else if (o.case === "decided") {
        await this.onRejectResult(e, o.value.reject ?? makeReject(Code.GATE_REJECTED, ErrorClass.DEAD_LETTER, "rejected"), true);
      } else if (o.case === "rejected") {
        await this.onRejectResult(e, o.value);
      } else if (o.case === "retry") {
        this.startPoll(e, o.value.afterMs);
      }
      return;
    }
    if (ans.voided) {
      if (spend) {
        await this.resendSpend(f, e, "VOIDED", e.outcome);
        return;
      }
      // lost: the final rejection STALE{EPOCH}, through the conflict handler (14 [CLI-38], [CLI-49])
      this.beginConflict(e, this.lostReject());
      return;
    }
    // known = false, voided = false: the outcome is unknown (14 [CLI-53], [CLI-83])
    if (spend) {
      await this.resendSpend(f, e, "OUTCOME_UNKNOWN", e.outcome);
      return;
    }
    await this.endAction(e, { sink: "export", reject: undefined, reason: "EPOCH_CHANGED", outcomeUnknown: "EPOCH_CHANGED", dependents: "fail" });
  }

  private stubOf(j: JournalEntry): OutboxEntry {
    return {
      mutationId: j.mutationId,
      keyspaceId: j.keyspaceId,
      lane: j.lane,
      opType: j.opType,
      content: j.content,
      author: j.author,
      ordinal: j.ordinal,
      authoredServerMs: j.retiredServerMs,
      dependsOn: [],
      clientId: "",
      clientSeq: 0,
      afterClientSeq: 0,
      everSent: true,
      sentInEpoch: j.outcome.logEpoch,
      attempts: 0,
      registered: j.lane === "GATED",
      permissionRetried: false,
      actorMismatchRetried: false,
      spendResent: j.spendResent,
      outcome: j.outcome,
    };
  }

  private seqOf(o: StoredOutcome): bigint {
    return o.kind === "appended" ? o.seq : o.decisionSeq;
  }

  private async answerJournal(f: Follow, j: JournalEntry, ans: GetMutationResultResponse): Promise<void> {
    const spend = j.spend && !j.spendResent;
    const stub = this.stubOf(j);
    const reverted = (): void => {
      this.emit({ type: "reverted", mutationId: j.mutationId, keyspaceId: j.keyspaceId, seq: this.seqOf(j.outcome), logEpoch: j.outcome.logEpoch });
    };
    if (ans.known && ans.result !== undefined) {
      const o = ans.result.outcome;
      if (o.case === "appended" || (o.case === "decided" && o.value.status === 1)) {
        // it survived the restore: a journalled action stays retired
        const nj: JournalEntry = { ...j, verifiedEpoch: f.logEpoch };
        await this.tx([{ op: "putJournal", entry: nj }]);
        this.journal.set(j.mutationId, nj);
        return;
      }
      // a REJECTED decision or a recorded Reject replaced the outcome: `reverted` first, then the rejection with the journalled content ([CLI-53])
      const reject = o.case === "decided" ? (o.value.reject ?? makeReject(Code.GATE_REJECTED, ErrorClass.DEAD_LETTER, "rejected")) : o.case === "rejected" ? o.value : this.lostReject();
      reverted();
      await this.endJournalledWith(stub, reject);
      return;
    }
    if (ans.voided) {
      reverted();
      if (spend) {
        await this.resendSpend(f, stub, "VOIDED", j.outcome, true);
        return;
      }
      this.beginConflict(stub, this.lostReject(), { journal: true });
      return;
    }
    // unknown: a spend action is reverted and resent; any other goes to the export hook as outcome unknown, with no `reverted` (14 [CLI-53], [CLI-83])
    if (spend) {
      reverted();
      await this.resendSpend(f, stub, "OUTCOME_UNKNOWN", j.outcome, true);
      return;
    }
    await this.endAction(stub, { sink: "export", reject: undefined, reason: "EPOCH_CHANGED", outcomeUnknown: "EPOCH_CHANGED", dependents: "fail", journal: true });
  }

  private async endJournalledWith(stub: OutboxEntry, reject: Reject): Promise<void> {
    const reaction = reject.errorClass;
    if (reaction === ErrorClass.CONFLICT) {
      this.beginConflict(stub, reject, { journal: true });
      return;
    }
    await this.endAction(stub, { sink: "deadLetter", reject, reason: codeName(reject.code), dependents: "fail", journal: true });
  }

  /**
   * The one resend of a lost spend action (14 [CLI-64], 02 [ARC-52] item 6): unchanged, with the same mutation id and content,
   * stamped in the new lanes, recorded as resent so that no restart or second tab sends it again. Events: `reverted` (done by the
   * caller for a journalled action), then `resent`.
   */
  private async resendSpend(f: Follow, e: OutboxEntry, cause: "VOIDED" | "OUTCOME_UNKNOWN", lost: StoredOutcome | undefined, fromJournal = false): Promise<void> {
    const copy: OutboxEntry = { ...e, outcome: undefined, everSent: false, sentInEpoch: 0, registered: false, attempts: 0, clientId: "", clientSeq: 0, afterClientSeq: 0, spendResent: true };
    const ops = [this.putOp(copy)];
    if (fromJournal) ops.push({ op: "deleteJournal", mutationId: e.mutationId });
    await this.tx(ops);
    if (fromJournal) {
      this.journal.delete(e.mutationId);
      this.entries.set(copy.mutationId, copy);
    } else {
      Object.assign(e, copy);
      delete e.outcome;
      this.rt(e.mutationId).nextAttemptAt = 0;
    }
    f.invalidate();
    this.emit({ type: "resent", mutationId: e.mutationId, keyspaceId: e.keyspaceId, cause, ...(lost !== undefined ? { seq: this.seqOf(lost), logEpoch: lost.logEpoch } : {}) });
    this.schedulePump();
  }

}
