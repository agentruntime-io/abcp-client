// Following sagas (14 [CLI-14], V19 MUST-5): "A client that shows such a pointer, or that started a saga, MUST follow the saga's run key,
// `abpc.saga.v1/run/{saga_id}` in its home keyspace (the source of a move), with the `saga_id` of the `moved` key or of the Start, through its interest or by
// loading the key, for as long as it shows the pointer or the saga's `open` key is present (11 §11.4.4). It MUST emit `saga-ended` when the run key's
// `status` says that the saga ended or failed (§14.5)."
//
//  - The sagas that the engine follows are its **watches**: one for each (home keyspace, saga id). A watch starts when the confirmed state shows a moved key
//    (`abpc.saga.v1/moved/<marker>`) that says `moving` or `moved` with a `saga_id` (a record, a load, or a restored image), and when the app's own action
//    starts a saga (its changes SET `abpc.saga.v1/run/{saga_id}`, 11 [COORD-58]), or the app says that it did (`Engine.followSaga`).
//  - The run key is read from the confirmed state when the keyspace's interest covers it (a record that changes it is the observation), and otherwise
//    **by loading the key**: a `LoadState` of the key's prefix, made when the watch starts and again every `SAGA_POLL_MS` on the engine's clock while the
//    session is live, until the saga has ended and nothing else keeps the watch (a failed run stays open until it is resolved).
//  - `saga-ended` is emitted once for each status that ends or fails a saga (`completed`, `compensated`, `failed`, `abandoned`, `resolved`), with the cause
//    that the run key records (`EPOCH`, `REDACTED`, `ENTITY_LOST`, 11 §11.4.4); a failed run that is resolved later is a second one. What has been emitted
//    is stored with the watch, so that a restart does not say it again.
//  - A watch ends when the saga has ended, its `open` key is absent, and no pointer of it is shown.

import type { LoadStateResponse } from "../gen/abpc/v2/sync_pb.js";
import { InterestLayer } from "./interest.js";
import type { Follow } from "./follow.js";
import { covers } from "./replica-store.js";
import type { StorageOp } from "./storage.js";

export const MOVED_PREFIX = "abpc.saga.v1/moved/";
export const RUN_PREFIX = "abpc.saga.v1/run/";
/** How often a run key that the interest does not cover is loaded while its saga is not over, on the engine's clock. */
export const SAGA_POLL_MS = 30_000;

/** The statuses of a run key that say that the saga ended or failed (11 §11.4.4). */
const ENDED = new Set(["completed", "compensated", "failed", "abandoned", "resolved"]);

interface SagaWatch {
  keyspaceId: string;
  sagaId: string;
  /** The app started the saga. */
  started: boolean;
  /** The marker keys whose moved key says `moving` or `moved` with this saga id: the pointers that the client shows. */
  pointers: Set<string>;
  /** The run key's `open` template holds `true`. */
  open: boolean;
  /** The last status that `saga-ended` was emitted for. */
  emitted: string | undefined;
  /** A load of the run key is under way. */
  peeking: boolean;
}

const key = (ks: string, id: string): string => `${ks}\u0000${id}`;

function jsonOf(bytes: Uint8Array): Record<string, unknown> | undefined {
  try {
    const v = JSON.parse(new TextDecoder().decode(bytes)) as unknown;
    return typeof v === "object" && v !== null && !Array.isArray(v) ? (v as Record<string, unknown>) : undefined;
  } catch {
    return undefined;
  }
}

export abstract class SagaLayer extends InterestLayer {
  private readonly watches = new Map<string, SagaWatch>();
  private cancelPoll: (() => void) | undefined;

  protected resetRuntime(): void {
    super.resetRuntime();
    this.watches.clear();
    this.cancelPoll = undefined;
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // The watches

  /** The sagas that this engine follows, with their state: for the app and the tests. */
  followedSagas(): { keyspaceId: string; sagaId: string; started: boolean; pointers: string[]; open: boolean; endedStatus: string | undefined }[] {
    return [...this.watches.values()].map((w) => ({ keyspaceId: w.keyspaceId, sagaId: w.sagaId, started: w.started, pointers: [...w.pointers], open: w.open, endedStatus: w.emitted }));
  }

  /**
   * The app started a saga (11 [COORD-58]) by a Start that this engine did not author (one that the engine authored is followed by itself): the run key
   * `abpc.saga.v1/run/{sagaId}` of the keyspace is followed (14 [CLI-14]).
   */
  followSaga(keyspaceId: string, sagaId: string): void {
    this.post(() => {
      if (!this.follows.has(keyspaceId)) return;
      this.watchOf(keyspaceId, sagaId, true);
    });
  }

  private watchOf(keyspaceId: string, sagaId: string, started = false): SagaWatch {
    const k = key(keyspaceId, sagaId);
    let w = this.watches.get(k);
    if (w === undefined) {
      w = { keyspaceId, sagaId, started, pointers: new Set(), open: false, emitted: undefined, peeking: false };
      this.watches.set(k, w);
      this.watchesChanged();
    } else if (started && !w.started) {
      w.started = true;
      this.watchesChanged();
    }
    this.readRun(w);
    return w;
  }

  private watchesChanged(): void {
    this.post(() => this.persistFollowed());
    this.armPoll();
  }

  /** The watches as they are stored with the followed keyspaces: what has been emitted, so that a restart does not say it again. */
  protected extraMetaOps(): StorageOp[] {
    const out = [...this.watches.values()].map((w) => ({ ks: w.keyspaceId, id: w.sagaId, started: w.started, emitted: w.emitted ?? null }));
    return [...super.extraMetaOps(), { op: "setMeta", key: this.metaKey("sagas"), value: JSON.stringify(out) }];
  }

  /** The watches that a stored state names, at an engine's start (the pointers are found again by `sagaScan`). */
  protected restoreSagas(v: string | number | bigint | undefined): void {
    if (typeof v !== "string") return;
    try {
      for (const x of JSON.parse(v) as { ks: string; id: string; started?: boolean; emitted?: string | null }[]) {
        const w: SagaWatch = { keyspaceId: x.ks, sagaId: x.id, started: x.started === true, pointers: new Set(), open: false, emitted: x.emitted ?? undefined, peeking: false };
        this.watches.set(key(x.ks, x.id), w);
      }
    } catch {
      // not what this engine wrote: no saga is followed because of it
    }
    this.armPoll();
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // What the confirmed state says

  /** A record changed a key of the saga state in the confirmed state of the keyspace (called by the fold). */
  protected sagaKeyChanged(f: Follow, k: string): void {
    if (this.role !== "flusher") return;
    if (k.startsWith(MOVED_PREFIX)) {
      this.readMoved(f, k);
    } else if (k.startsWith(RUN_PREFIX)) {
      const id = k.slice(RUN_PREFIX.length).split("/")[0] as string;
      const w = this.watches.get(key(f.id, id));
      if (w !== undefined) this.readRun(w);
    }
  }

  /** A load or a restored image installed state: the moved keys that it holds are looked at, and the run keys of the watches. */
  protected sagaScan(f: Follow): void {
    if (this.role !== "flusher") return;
    const names = f.state.keyNames();
    for (const k of names) if (k.startsWith(MOVED_PREFIX)) this.readMoved(f, k);
    for (const w of [...this.watches.values()]) if (w.keyspaceId === f.id) this.readRun(w);
  }

  /** The moved key of a marker key, as the confirmed state holds it: a pointer that the client shows while it says `moving` or `moved` (14 [CLI-14]). */
  private readMoved(f: Follow, movedKey: string): void {
    const marker = movedKey.slice(MOVED_PREFIX.length);
    const row = f.state.key(movedKey);
    const doc = row === undefined || row.encoding !== "json/v1" ? undefined : jsonOf(row.value);
    const status = doc?.["status"];
    const id = doc?.["saga_id"];
    const shown = (status === "moving" || status === "moved") && typeof id === "string" && id !== "";
    for (const w of this.watches.values()) {
      if (w.keyspaceId === f.id && w.pointers.has(marker) && !(shown && w.sagaId === id)) {
        w.pointers.delete(marker);
        this.settleWatch(w);
      }
    }
    if (shown) {
      const w = this.watchOf(f.id, id as string);
      if (!w.pointers.has(marker)) {
        w.pointers.add(marker);
        this.watchesChanged();
      }
    }
  }

  /** Whether the keyspace's interest holds the run key, so that the confirmed state shows it. */
  private covered(f: Follow, sagaId: string): boolean {
    return f.interest.length === 0 || covers(f.interest, `${RUN_PREFIX}${sagaId}`);
  }

  /** The run key as the confirmed state holds it, when the interest covers it. */
  private readRun(w: SagaWatch): void {
    const f = this.follows.get(w.keyspaceId);
    if (f === undefined || !f.loaded || f.purged) return;
    if (!this.covered(f, w.sagaId)) {
      this.peek(w);
      return;
    }
    const run = f.state.key(`${RUN_PREFIX}${w.sagaId}`);
    const doc = run === undefined || run.encoding !== "json/v1" ? undefined : jsonOf(run.value);
    const open = f.state.key(`${RUN_PREFIX}${w.sagaId}/open`) !== undefined;
    this.observe(w, typeof doc?.["status"] === "string" ? doc["status"] : undefined, typeof doc?.["cause"] === "string" ? doc["cause"] : undefined, open);
  }

  /** An observation of the run key: `saga-ended` for a status that ends or fails the saga that has not been said yet. */
  private observe(w: SagaWatch, status: string | undefined, cause: string | undefined, open: boolean): void {
    w.open = open;
    if (status !== undefined && ENDED.has(status) && w.emitted !== status) {
      w.emitted = status;
      this.emit({ type: "saga-ended", keyspaceId: w.keyspaceId, sagaId: w.sagaId, status, ...(cause !== undefined ? { cause } : {}) });
      this.watchesChanged();
    }
    this.settleWatch(w);
  }

  /** A watch that has nothing left to follow ends: the saga ended, its `open` key is absent, and no pointer of it is shown. */
  private settleWatch(w: SagaWatch): void {
    if (w.emitted !== undefined && !w.open && w.pointers.size === 0 && this.watches.get(key(w.keyspaceId, w.sagaId)) === w) {
      this.watches.delete(key(w.keyspaceId, w.sagaId));
      this.watchesChanged();
    }
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Loading the run key

  /** A run key that the interest does not cover is loaded: the keys under it, the `open` key included. */
  private peek(w: SagaWatch): void {
    if (w.peeking || !this.online() || this.role !== "flusher") return;
    const f = this.follows.get(w.keyspaceId);
    if (f === undefined || !f.loaded || f.purged || f.stopped !== undefined) return;
    w.peeking = true;
    const runKey = `${RUN_PREFIX}${w.sagaId}`;
    this.detach(
      (async () => {
        let pages: LoadStateResponse[];
        try {
          pages = await this.fetchLoad(f, false, { keyspaceId: f.id, interestPrefixes: [runKey] });
        } catch (e) {
          await this.run(() => {
            w.peeking = false;
            return this.onLoadError(f, e);
          });
          return;
        }
        await this.run(() => {
          w.peeking = false;
          if (this.watches.get(key(w.keyspaceId, w.sagaId)) !== w) return;
          const first = pages[0] as LoadStateResponse;
          if (first.logEpoch !== f.logEpoch) {
            this.onEpochMismatch(f);
            return;
          }
          let status: string | undefined;
          let cause: string | undefined;
          let open = false;
          for (const p of pages) {
            for (const e of p.entries) {
              if (e.deleted || e.isNull || e.redacted) continue;
              if (e.key === runKey) {
                const doc = e.encoding === "json/v1" ? jsonOf(e.value) : undefined;
                if (typeof doc?.["status"] === "string") status = doc["status"];
                if (typeof doc?.["cause"] === "string") cause = doc["cause"];
              } else if (e.key === `${runKey}/open`) open = true;
            }
          }
          this.observe(w, status, cause, open);
        });
      })(),
    );
  }

  /** While a saga whose run key the interest does not cover is not over, its run key is loaded every `SAGA_POLL_MS`. */
  private armPoll(): void {
    if (this.cancelPoll !== undefined || this.stopped || this.role !== "flusher") return;
    if (![...this.watches.values()].some((w) => this.needsPeek(w))) return;
    this.cancelPoll = this.after(SAGA_POLL_MS, () => {
      this.cancelPoll = undefined;
      for (const w of [...this.watches.values()]) if (this.needsPeek(w)) this.peek(w);
      this.armPoll();
    });
  }

  private needsPeek(w: SagaWatch): boolean {
    const f = this.follows.get(w.keyspaceId);
    return f !== undefined && f.loaded && !f.purged && !this.covered(f, w.sagaId);
  }
}
