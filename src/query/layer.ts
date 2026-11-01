// The local query layer over the engine's replica (C4, 14 §14.10): queries over the state of one keyspace, predicted (14 [CLI-6]) or confirmed, and live queries
// that follow the engine.
//
// What it reads. A query sees only the interest-filtered copy of the keyspace (14 [CLI-71]): `view` (predicted) or `confirmed` of the engine. A key outside the interest
// is not loaded, which a result says; it is never absent (09 [READ-26]). `loadKeys` loads such a key's version for the app to author a change from (14 [CLI-73]), and
// still shows no value: to read it, the app widens the interest (`setInterest`).
//
// What drives it. The engine's change feed (`onViewChange`, engine/view-feed.ts) says which keys of which keyspace changed, in the confirmed state or in the prediction,
// and whether anything may have changed; the engine's events refresh what they replace (`reloaded`, `interest-changed`, `role`) and replace a CRDT view document
// (`rebuilt`); and a purge hook (`registerPurgeHook`, 14 [CLI-66]) drops what the layer holds of a keyspace whose copy is purged.
//
// What it keeps. Per keyspace, an index of its keys in byte order (key-index.ts) and the live queries, indexed by the scopes they read. Nothing of a keyspace outlives its purge.

import type { CrdtHandle, KeyspaceInfo } from "../engine/engine.js";
import type { Manifests } from "../engine/manifests.js";
import type { KeyspaceView } from "../engine/overlay.js";
import type { PurgeHook } from "../engine/revocation.js";
import type { EngineEvent, EngineEventListener } from "../engine/types.js";
import type { ViewChange, ViewChangeListener } from "../engine/view-feed.js";
import { QueryError } from "./errors.js";
import { evaluate, resultOf, stateOf, unavailableResult, type Ctx, type Reader } from "./eval.js";
import { KeyIndex, mergeSorted, ScopeIndex } from "./key-index.js";
import { Live, type Reading, type Services, type Touch } from "./live.js";
import { MOVED_PREFIX, Plan } from "./select.js";
import type { KeyRead, LiveListener, LiveQuery, Query, QueryMode, QueryResult } from "./types.js";

/** What the layer needs of the engine; `Engine` is one. */
export interface QueryEngine {
  view(keyspaceId: string): KeyspaceView;
  confirmed(keyspaceId: string): KeyspaceView;
  keyspaceInfo(keyspaceId: string): KeyspaceInfo | undefined;
  loadKeys(keyspaceId: string, keys: readonly string[]): Promise<boolean>;
  crdt(keyspaceId: string, key: string): CrdtHandle;
  on(listener: EngineEventListener): () => void;
  onViewChange(listener: ViewChangeListener): () => void;
  registerPurgeHook(hook: PurgeHook): () => void;
  flushViewChanges(): void;
  readonly manifests: Manifests;
}

/** What the layer keeps of one keyspace. */
class KsState {
  /** Every key present in the confirmed state or the predicted view: built when first read, kept in step by the change feed, dropped when anything may have changed. */
  index: KeyIndex | undefined;
  readonly lives = new Set<Live>();
  readonly scopes = new ScopeIndex<Live>();
}

export class Queries {
  private readonly states = new Map<string, KsState>();
  private readonly offs: (() => void)[] = [];
  private isClosed = false;
  private readonly reader: Reader;
  private readonly services: Services;

  constructor(private readonly engine: QueryEngine) {
    this.reader = {
      crdtDoc: (ks, key, mode) => {
        try {
          const h = engine.crdt(ks, key);
          return mode === "predicted" ? h.doc : h.confirmedDoc;
        } catch {
          return undefined; // a follower tab holds no view documents, and a purged copy none at all
        }
      },
    };
    this.services = { read: (ks, plan) => this.read(ks, plan.mode) };
    this.offs.push(engine.onViewChange((c) => this.onChange(c)));
    this.offs.push(engine.on((e) => this.onEvent(e)));
    this.offs.push(engine.registerPurgeHook((ks, cause) => this.purge(ks, cause)));
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Queries

  /** Evaluates a query now. Throws a `QueryError` for a query that is none; a keyspace that cannot answer gives a result that says so, with nothing claimed absent. */
  run(query: Query): QueryResult {
    this.assertOpen();
    this.engine.flushViewChanges();
    const plan = this.plan(query);
    const r = this.read(plan.keyspaceId, plan.mode);
    if (!r.ok) return unavailableResult(plan, r.state);
    const ev = evaluate(r.ctx, plan);
    return resultOf(plan, r.state, ev.rows, ev.next);
  }

  /** Reads one key. A key outside the interest is `notLoaded`, never `absent` (14 [CLI-71], 09 [READ-26]). */
  get(keyspaceId: string, key: string, options: { mode?: QueryMode } = {}): KeyRead {
    const res = this.run({ keyspaceId, select: { keys: [key] }, ...(options.mode !== undefined ? { mode: options.mode } : {}) });
    if (res.keyspace.status !== "ready") return { status: "unavailable", reason: res.keyspace.status };
    if (res.notLoaded.length > 0) return { status: "notLoaded" };
    const row = res.rows[0];
    return row === undefined ? { status: "absent" } : { status: "present", row };
  }

  /**
   * Starts a live query: the listener is called at once with the first answer (`cause: "initial"`, every row added), and then with each change of the result, in
   * rows added, changed and removed, from the engine's change feed. It is told nothing for a change that touches no key of the query, and nothing when a key it
   * touches reads the same. Close it when it is no longer shown.
   */
  watch(query: Query, listener: LiveListener): LiveQuery {
    this.assertOpen();
    this.engine.flushViewChanges();
    const plan = this.plan(query);
    const st = this.state(plan.keyspaceId);
    const live = new Live(query, plan, this.services, listener, (l) => this.unregister(l));
    st.lives.add(live);
    for (const s of plan.scopes) st.scopes.add(s, live);
    live.refresh("initial");
    return live;
  }

  /**
   * Asks the engine to load the keys of a key query that the interest does not cover (`loadKeys`, 14 [CLI-73]), so that a change that the app authors to them has
   * their versions. It makes them known to the engine for authoring, and shows no value: they stay `notLoaded` until the interest covers them.
   *
   * SPEC-QUESTION (14 [CLI-73] with 09 [READ-26]): [CLI-73] has a client load a key outside its interest "so that `seen_version` is correct", and [READ-26] says that a client "MUST
   * NOT present" a key outside its interest as current. Reading A (taken, as the engine's `loadKeys` keeps only the version): a loaded key is still outside the interest, and a
   * query reports it `notLoaded`; the app widens the interest (`setInterest`, [CLI-72]) to read it. Reading B: a key that the app asked to load is shown with the value of its load,
   * marked as outside the interest and as of the load, until a record or a later load replaces it.
   */
  async loadNotLoaded(result: QueryResult): Promise<boolean> {
    if (result.notLoaded.length === 0) return true;
    return this.engine.loadKeys(result.keyspaceId, result.notLoaded);
  }

  /** What the layer holds of a keyspace: the keys of its index, and its live queries; nothing for a keyspace whose copy was purged (14 [CLI-66]). */
  holds(keyspaceId: string): { keys: number; live: number } {
    const st = this.states.get(keyspaceId);
    return { keys: st?.index?.size ?? 0, live: st?.lives.size ?? 0 };
  }

  /** Stops every live query and drops everything the layer holds. */
  close(): void {
    if (this.isClosed) return;
    this.isClosed = true;
    for (const off of this.offs) off();
    for (const st of this.states.values()) for (const l of [...st.lives]) l.close();
    this.states.clear();
  }

  // -------------------------------------------------------------------------------------------------------------------

  private assertOpen(): void {
    if (this.isClosed) throw new QueryError("CLOSED", "the query layer was closed");
  }

  private state(keyspaceId: string): KsState {
    let st = this.states.get(keyspaceId);
    if (st === undefined) {
      st = new KsState();
      this.states.set(keyspaceId, st);
    }
    return st;
  }

  private unregister(l: Live): void {
    const st = this.states.get(l.keyspaceId);
    if (st === undefined) return;
    st.lives.delete(l);
    for (const s of l.plan.scopes) st.scopes.remove(s, l);
  }

  /** Builds the plan, and for an entity query checks the template against the manifest in force: a template that is declared and is no entity is refused (05 §5.6). */
  private plan(query: Query): Plan {
    const plan = Plan.build(query);
    if (plan.template !== undefined) {
      const info = this.engine.keyspaceInfo(plan.keyspaceId);
      const manifest = info === undefined ? undefined : this.engine.manifests.get(info.manifestVersion);
      if (info !== undefined && manifest !== undefined) {
        const sel = query.select as { bind?: Readonly<Record<string, string>> };
        const sample = plan.template.replace(/\{([a-z][a-z0-9_]*)\}/g, (_m, name: string) => sel.bind?.[name] ?? "x");
        const t = manifest.template(info.manifestVersion, sample);
        if (t !== undefined && !t.entity) throw new QueryError("NOT_ENTITY", `"${plan.template}" is declared by the manifest, and it is not an entity template (05 §5.6)`);
      }
    }
    return plan;
  }

  /** The reading of a keyspace for a mode: its view, its copy's state and its index; or why the copy cannot answer. */
  private read(keyspaceId: string, mode: QueryMode): Reading {
    const info = this.engine.keyspaceInfo(keyspaceId);
    if (info === undefined) return { ok: false, state: stateOf("unfollowed", undefined) };
    if (info.purged) return { ok: false, state: stateOf("purged", info) };
    if (!info.loaded) return { ok: false, state: stateOf("loading", info) };
    const st = this.state(keyspaceId);
    try {
      const view = mode === "predicted" ? this.engine.view(keyspaceId) : this.engine.confirmed(keyspaceId);
      if (st.index === undefined) st.index = new KeyIndex(mergeSorted(this.engine.confirmed(keyspaceId).keys(), this.engine.view(keyspaceId).keys()));
      const ctx: Ctx = { reader: this.reader, keyspaceId, mode, view, info, index: st.index, movedAny: st.index.hasPrefix(MOVED_PREFIX) };
      return { ok: true, ctx, state: stateOf("ready", info) };
    } catch {
      // the engine dropped the keyspace between the two calls
      st.index = undefined;
      return { ok: false, state: stateOf("purged", info) };
    }
  }

  // -------------------------------------------------------------------------------------------------------------------
  // What the engine tells

  private onChange(c: ViewChange): void {
    const st = this.states.get(c.keyspaceId);
    if (st === undefined) return;
    if (c.all) {
      st.index = undefined;
      for (const l of [...st.lives]) l.refresh("refresh");
      return;
    }
    if (st.index !== undefined) this.updateIndex(st, c);
    if (st.lives.size === 0) return;
    const per = new Map<Live, { keys: Set<string>; subtrees: Set<string> }>();
    const of = (l: Live): { keys: Set<string>; subtrees: Set<string> } => {
      let t = per.get(l);
      if (t === undefined) {
        t = { keys: new Set(), subtrees: new Set() };
        per.set(l, t);
      }
      return t;
    };
    const route = (keys: ReadonlySet<string>, subtrees: ReadonlySet<string>, confirmedOnly: boolean): void => {
      for (const k of keys) for (const l of st.scopes.forKey(k)) if (!confirmedOnly || l.plan.mode === "predicted") of(l).keys.add(k);
      // a moved key says whether an entity is hidden (14 [CLI-14]): its whole subtree may read differently
      const subs = new Set(subtrees);
      for (const k of keys) if (k.startsWith(MOVED_PREFIX)) subs.add(k.slice(MOVED_PREFIX.length));
      for (const m of subs) for (const l of st.scopes.forSubtree(m)) if (!confirmedOnly || l.plan.mode === "predicted") of(l).subtrees.add(m);
    };
    // the confirmed state touches both modes, the prediction only a predicted view
    route(c.confirmed.keys, c.confirmed.subtrees, false);
    route(c.overlay.keys, c.overlay.subtrees, true);
    for (const [l, t] of per) l.apply(t as Touch);
  }

  /** Keeps the index of the keyspace's keys as the feed says: a key is held while the confirmed state or the predicted view holds it. */
  private updateIndex(st: KsState, c: ViewChange): void {
    const idx = st.index as KeyIndex;
    let pv: KeyspaceView;
    let cv: KeyspaceView;
    try {
      pv = this.engine.view(c.keyspaceId);
      cv = this.engine.confirmed(c.keyspaceId);
    } catch {
      st.index = undefined;
      return;
    }
    const present = (k: string): boolean => cv.get(k) !== undefined || pv.get(k) !== undefined;
    const settle = (k: string): void => {
      if (present(k)) idx.add(k);
      else idx.remove(k);
    };
    for (const src of [c.confirmed, c.overlay]) {
      for (const k of src.keys) settle(k);
      for (const m of src.subtrees) {
        settle(m);
        const [lo, hi] = idx.range(`${m}/`);
        for (const k of idx.slice(lo, hi)) if (!present(k)) idx.remove(k);
      }
    }
  }

  private onEvent(e: EngineEvent): void {
    switch (e.type) {
      case "rebuilt": {
        // a CRDT key's view document was replaced (14 [CLI-13]): its row is reported as changed, whatever else it reads
        const st = this.states.get(e.keyspaceId);
        if (st === undefined) return;
        const forced: Touch = { keys: new Set(), subtrees: new Set(), forced: new Set([e.key]) };
        for (const l of st.scopes.forKey(e.key)) l.apply(forced);
        return;
      }
      case "reloaded":
      case "interest-changed": {
        const st = this.states.get(e.keyspaceId);
        if (st === undefined) return;
        st.index = undefined;
        for (const l of [...st.lives]) l.refresh("refresh");
        return;
      }
      case "role":
        // a tab's role decides whether the engine has view documents to give: the rows of CRDT keys read again
        for (const st of this.states.values()) for (const l of [...st.lives]) l.refresh("refresh");
        return;
      default:
        return;
    }
  }

  /** Revocation or deletion purged the keyspace's copy (14 [CLI-66], [CLI-77]): the layer keeps nothing of it. */
  private purge(keyspaceId: string, _cause: string): void {
    const st = this.states.get(keyspaceId);
    if (st === undefined) return;
    st.index = undefined;
    for (const l of [...st.lives]) l.purged();
  }
}

/** Creates the query layer over an engine. */
export function createQueries(engine: QueryEngine): Queries {
  return new Queries(engine);
}
