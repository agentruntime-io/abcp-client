// The feed of what changed in the view of each followed keyspace, for the local query layer (`query/`, C4).
//
// The app reads a keyspace through `KeyspaceView` (14 [CLI-6]); a live query needs to be told when a read it made may now answer
// differently, and which keys that touches, so that a change to no key of a query costs the query nothing. The engine reports, at the end of
// each task of its queue (`core.ts`, `run`), every keyspace whose view changed in it, once, with:
//
//  - `confirmed`: the keys that applied records touched (14 [CLI-2]). A key that a record names is touched whether or not the fold applied
//    its change: a keyspace's rows are compared again by the reader, so a key reported without a change costs one comparison and shows
//    nothing. A `DELETE` also touches the subtree below its key (05 [STATE-43]: for a marker key it is a subtree deletion, and a reader
//    cannot know the markers);
//  - `overlay`: the keys whose predicted cell changed, and the marker keys whose pending `DELETE` came or went (14 [CLI-6], [CLI-14]),
//    found by comparing the overlay with the one of the last report: the cost is the pending actions, never the keyspace;
//  - `all`: anything may have changed (the confirmed state was replaced by a load or a purge, the interest changed, the copy was
//    removed, a follower mirrored the flusher again): the reader evaluates again from scratch.
//
// Nothing is kept or computed while no listener is registered.

import type { Follow } from "./follow.js";
import type { KeyspaceView, Overlay } from "./overlay.js";
import { overlayChange } from "./overlay.js";
import type { ReplicaState } from "../rules/fold/index.js";

/** The keys of one source of change: keys whose row may differ, and keys whose whole subtree may differ. */
export interface KeyTouch {
  readonly keys: ReadonlySet<string>;
  readonly subtrees: ReadonlySet<string>;
}

/** One keyspace's change in one task of the engine. */
export interface ViewChange {
  keyspaceId: string;
  /** Anything may have changed: evaluate again from scratch (`confirmed` and `overlay` are empty). */
  all: boolean;
  /** Keys that applied records touched. A predicted view is touched by these too. */
  confirmed: KeyTouch;
  /** Keys that the prediction of the pending actions changed. Only a predicted view is touched by these. */
  overlay: KeyTouch;
}

export type ViewChangeListener = (change: ViewChange) => void;

const NONE: KeyTouch = { keys: new Set(), subtrees: new Set() };

function allOf(keyspaceId: string): ViewChange {
  return { keyspaceId, all: true, confirmed: NONE, overlay: NONE };
}

/** What a follow showed at the last report: the overlay, and what makes a keyspace's copy another one. */
export interface Shown {
  overlay: Overlay;
  state: ReplicaState;
  fingerprint: string;
}

/** The parts of a follow whose change leaves nothing of the earlier rows valid: the interest, whether the copy is loaded or purged, the epoch. */
function fingerprintOf(f: Follow): string {
  return `${f.loaded ? 1 : 0}${f.purged ? 1 : 0}|${f.stopped ?? ""}|${f.logEpoch}|${f.interest.join("\u0000")}`;
}

/**
 * The engine's map of followed keyspaces. Every follow that enters it is wired to the feed, whichever of the engine's paths put it there (a
 * start, a rehydration, a follower's mirror), and one that leaves it is reported as a keyspace whose copy is gone.
 */
export class FollowMap extends Map<string, Follow> {
  constructor(private readonly feed: ViewFeed) {
    super();
  }

  override set(id: string, f: Follow): this {
    const had = super.get(id);
    f.feed = this.feed;
    super.set(id, f);
    if (had !== f) this.feed.mark(f);
    return this;
  }

  override delete(id: string): boolean {
    const had = super.delete(id);
    if (had) this.feed.gone(id);
    return had;
  }

  override clear(): void {
    for (const id of [...this.keys()]) this.feed.gone(id);
    super.clear();
  }
}

export class ViewFeed {
  private readonly listeners = new Set<ViewChangeListener>();
  private readonly dirty = new Set<string>();
  private readonly removed = new Set<string>();

  /** Whether anything listens: a follow records the keys it touches only then. */
  get active(): boolean {
    return this.listeners.size > 0;
  }

  /** The follow's view may have changed: it is reported at the end of the task. */
  mark(f: Follow): void {
    if (this.listeners.size > 0) this.dirty.add(f.id);
  }

  /** The follow of the keyspace left the engine's map (a follower mirrored the flusher without it). */
  gone(id: string): void {
    if (this.listeners.size > 0) this.removed.add(id);
  }

  /**
   * Registers a listener. `init` runs when it is the first: it takes, for every follow, the overlay that the first report is compared with.
   * Returns the function that removes it.
   */
  on(listener: ViewChangeListener, init: () => void): () => void {
    if (this.listeners.size === 0) {
      this.dirty.clear();
      this.removed.clear();
      init();
    }
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
      if (this.listeners.size === 0) {
        this.dirty.clear();
        this.removed.clear();
      }
    };
  }

  /** What a follow shows now, to be compared with at the next report. */
  shownOf(f: Follow, view: KeyspaceView): Shown {
    return { overlay: view.overlaySnapshot(), state: f.state, fingerprint: fingerprintOf(f) };
  }

  /** Reports the keyspaces that changed in the task that just ended. */
  flush(follows: ReadonlyMap<string, Follow>, viewOf: (f: Follow) => KeyspaceView): void {
    if (this.listeners.size === 0) {
      this.dirty.clear();
      this.removed.clear();
      return;
    }
    if (this.dirty.size === 0 && this.removed.size === 0) return;
    const out: ViewChange[] = [];
    for (const id of this.removed) if (!follows.has(id)) out.push(allOf(id));
    this.removed.clear();
    const ids = [...this.dirty];
    this.dirty.clear();
    for (const id of ids) {
      const f = follows.get(id);
      if (f === undefined) continue;
      const c = this.changeOf(f, viewOf(f));
      if (c !== undefined) out.push(c);
    }
    for (const c of out) {
      for (const l of [...this.listeners]) {
        try {
          l(c);
        } catch {
          // a listener never stops the engine
        }
      }
    }
  }

  private changeOf(f: Follow, view: KeyspaceView): ViewChange | undefined {
    const prev = f.shown;
    const now = this.shownOf(f, view);
    f.shown = now;
    const touched = f.takeTouched();
    if (prev === undefined || prev.fingerprint !== now.fingerprint || prev.state !== now.state) return allOf(f.id);
    const od = overlayChange(prev.overlay, now.overlay);
    if (touched.keys.size + touched.subtrees.size + od.keys.length + od.subtrees.length === 0) return undefined;
    return { keyspaceId: f.id, all: false, confirmed: touched, overlay: { keys: new Set(od.keys), subtrees: new Set(od.subtrees) } };
  }
}
