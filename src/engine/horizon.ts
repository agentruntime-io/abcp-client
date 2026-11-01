// The offline horizon of the client (14 §14.7).
//
// The server decides what is too old (07 §7.10): the client never assigns `OFFLINE_TOO_LONG` itself and never refuses or drops an
// action for its age by its own clock ([CLI-57]). What the client does with the horizon is to warn, so that the app can tell the user
// before the server's answer ends the action:
//
//  - [CLI-54] it takes the horizon from `AppLimits.max_offline_seconds` (`core.ts`, `applyLimits`, 3,600 s until a value arrives,
//    07 [MUT-75], SPEC-QUESTIONS Q32) and keeps it with its stored state;
//  - [CLI-55] ages are measured in server time (`serverNow`): an action's age counts from the server time that the client estimated
//    when it authored the action, which its outbox entry keeps, and the estimate survives a restart (`core.ts`);
//  - [CLI-56] `offline-horizon` is emitted before the horizon is reached: when the age of the oldest unsynced action of a keyspace, or
//    the time since the last successful sync of the keyspace, reaches the warning threshold, 80% of `max_offline` unless the app set
//    another one below 100%.
//
// An unsynced action is one with no outcome yet: the server has not answered it with a result, whether it was ever sent or not. The
// time since the last successful sync counts from the last frame, heartbeat or load of the keyspace on a live session, and is 0 while
// the session is live. One warning is emitted for each cause, for each keyspace, in each episode: the episode of an action ends when no
// unsynced action of the keyspace is over the threshold, the one of a sync when the keyspace syncs again.

// SPEC-QUESTION (14 [CLI-56]): the text says when the warning is due (the age of the oldest unsynced action, or the time since the last successful sync of a
// keyspace, reaches the threshold) and not how often it repeats, nor what a successful sync is. Reading A (taken): one `offline-horizon` for each cause, for
// each keyspace, in each episode (an episode ends when the condition does); a keyspace is synced while it is on a live session, and the time since its sync
// counts from the last frame, heartbeat or load, or the end of its session. Reading B: the warning repeats (at 90% and 95%, or every interval), and a sync
// is a load only. B cannot be told from A by an app that keeps the first warning on screen, and it multiplies events while the user is away.

import type { Follow } from "./follow.js";
import { SessionLayer } from "./session.js";

/** The default warning threshold of 14 [CLI-56]: 80% of `max_offline`. */
export const DEFAULT_HORIZON_WARNING = 0.8;

export abstract class HorizonLayer extends SessionLayer {
  /** The share of `max_offline` at which the warning is emitted, below 1 (14 [CLI-56]). */
  protected horizonWarning = DEFAULT_HORIZON_WARNING;
  private horizonTimer: (() => void) | undefined;
  private horizonQueued = false;
  /** The warnings that were emitted in the episode that is going on: `ACTION|keyspace` and `SYNC|keyspace`. */
  private readonly horizonWarned = new Set<string>();

  /** Reads the app's threshold; it is a fraction above 0 and below 1 (14 [CLI-56]: "below 100% of `max_offline`"). */
  protected setHorizonWarning(w: number | undefined): void {
    if (w === undefined) return;
    if (!(w > 0 && w < 1)) throw new RangeError(`the offline-horizon warning threshold is a share of max_offline above 0 and below 1 (14 [CLI-56]), not ${String(w)}`);
    this.horizonWarning = w;
  }

  /** The limits changed (a new `max_offline`): the thresholds move, and a warning may be due at once. */
  protected onLimitsChanged(): void {
    this.armHorizon();
  }

  /** Something that the warnings read changed (the outbox, the session): they are looked at again after the current task. */
  protected armHorizonSoon(): void {
    if (this.horizonQueued || this.stopped) return;
    this.horizonQueued = true;
    this.post(() => {
      this.horizonQueued = false;
      this.armHorizon();
    });
  }

  protected schedulePump(): void {
    super.schedulePump();
    this.armHorizonSoon();
  }

  /** The server time at which the oldest unsynced action of the keyspace was authored, or undefined when there is none. */
  protected oldestUnsynced(f: Follow): { ms: number; mutationId: string } | undefined {
    let best: { ms: number; mutationId: string } | undefined;
    for (const e of this.entriesOf(f.id)) {
      if (e.outcome !== undefined || this.ending.has(e.mutationId)) continue;
      if (best === undefined || e.authoredServerMs < best.ms) best = { ms: e.authoredServerMs, mutationId: e.mutationId };
    }
    return best;
  }

  /** The time since the last successful sync of the keyspace, in ms: 0 while the keyspace is on a live session (14 [CLI-56]). */
  protected syncAge(f: Follow): number {
    if (this.online() && f.onSession) return 0;
    if (f.lastSyncMs === 0) return 0; // no sync yet: nothing to be out of date
    return Math.max(0, this.serverNow() - f.lastSyncMs);
  }

  private watched(f: Follow): boolean {
    return f.stopped === undefined && !f.purged;
  }

  /** The state of the warnings, for a test and the sync status: the keyspaces with a warning in force. */
  horizonState(): { keyspaceId: string; cause: "ACTION" | "SYNC" }[] {
    return [...this.horizonWarned].map((k) => {
      const [cause, keyspaceId] = k.split("|") as ["ACTION" | "SYNC", string];
      return { keyspaceId, cause };
    });
  }

  /**
   * Emits the warnings that are due, ends the episodes that are over, and arms the timer for the next threshold to be reached. The
   * thresholds are looked at in the engine's clock, in server time ([CLI-55]).
   */
  protected armHorizon(): void {
    this.horizonTimer?.();
    this.horizonTimer = undefined;
    if (this.stopped || this.role !== "flusher") return;
    const max = this.limits.maxOfflineMs;
    const threshold = max * this.horizonWarning;
    const now = this.serverNow();
    let nextAt = Infinity;
    for (const f of this.follows.values()) {
      if (!this.watched(f)) {
        this.horizonWarned.delete(`ACTION|${f.id}`);
        this.horizonWarned.delete(`SYNC|${f.id}`);
        continue;
      }
      // -- the oldest unsynced action --
      const oldest = this.oldestUnsynced(f);
      const actionKey = `ACTION|${f.id}`;
      if (oldest === undefined || now - oldest.ms < threshold) {
        this.horizonWarned.delete(actionKey);
        if (oldest !== undefined) nextAt = Math.min(nextAt, oldest.ms + threshold);
      } else if (!this.horizonWarned.has(actionKey)) {
        this.horizonWarned.add(actionKey);
        this.emit({ type: "offline-horizon", keyspaceId: f.id, ageMs: now - oldest.ms, maxOfflineMs: max, kind: "ACTION", oldestMutationId: oldest.mutationId });
      }
      // -- the last successful sync --
      const syncKey = `SYNC|${f.id}`;
      if (this.online() && f.onSession) {
        this.horizonWarned.delete(syncKey);
      } else if (f.lastSyncMs !== 0) {
        const age = now - f.lastSyncMs;
        if (age < threshold) {
          this.horizonWarned.delete(syncKey);
          nextAt = Math.min(nextAt, f.lastSyncMs + threshold);
        } else if (!this.horizonWarned.has(syncKey)) {
          this.horizonWarned.add(syncKey);
          this.emit({ type: "offline-horizon", keyspaceId: f.id, ageMs: age, maxOfflineMs: max, kind: "SYNC" });
        }
      }
    }
    if (nextAt === Infinity) return;
    this.horizonTimer = this.after(Math.max(1, nextAt - this.serverNow()), () => this.armHorizon());
  }
}
