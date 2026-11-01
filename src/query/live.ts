// A live query: a query that tells its listener, row by row, what changed in its result (added, changed, removed), when the replica, the prediction or
// the interest changes.
//
// How an update is computed. The layer (layer.ts) is told by the engine's change feed which keys changed, and gives each live query only the keys that lie in
// the scopes it reads (an index by prefix), so a change to no key of a query costs the query nothing. For a query with no page limit, each touched key
// is judged alone: the row it concerns is evaluated again (`evalRow`) and compared with the row the query holds, which gives one added, changed or
// removed row, or nothing when the row is the same. A touched subtree (a `DELETE` of a marker key came, or went, 05 [STATE-43]) judges the rows of the
// subtree, and the rows that the query holds below it. A query with a page limit holds a window: a key after the window's last row (while it is full) or
// before its cursor concerns it not at all, and any other change evaluates the window again, which is at most a page. A change that says anything may have
// changed (a load, a purge, an interest change) evaluates the whole query again and compares: only the rows that really differ are reported.

import { bytesEqual } from "../rules/bytes.js";
import { deepEqual } from "./json.js";
import { candidates, evalRow, evaluate, resultOf, stateOf, unavailableResult, type Ctx } from "./eval.js";
import { MOVED_PREFIX, Plan, RUN_PREFIX } from "./select.js";
import type { KeyspaceState, LiveCause, LiveListener, LiveQuery, LiveStats, LiveUpdate, Query, QueryResult, Row, RowChange, RowValue } from "./types.js";

/** What a layer gives an evaluation of a keyspace, or why it has none. */
export type Reading = { ok: true; ctx: Ctx; state: KeyspaceState } | { ok: false; state: KeyspaceState };

export interface Services {
  read(keyspaceId: string, plan: Plan): Reading;
}

/** The keys of a change that lie in a live query's scopes. */
export interface Touch {
  keys: ReadonlySet<string>;
  subtrees: ReadonlySet<string>;
  /** Keys whose row is to be reported as changed although it reads the same: a CRDT key's view document was replaced (`rebuilt`, 14 [CLI-13]). */
  forced?: ReadonlySet<string>;
}

function sameValue(a: RowValue, b: RowValue): boolean {
  if (a.kind !== b.kind) return false;
  switch (a.kind) {
    case "json":
      return deepEqual(a.value, (b as typeof a).value);
    case "counter":
      return a.value === (b as typeof a).value;
    case "bytes":
      return bytesEqual(a.value, (b as typeof a).value);
    case "undecodable":
      return a.encoding === (b as typeof a).encoding && bytesEqual(a.bytes, (b as typeof a).bytes);
    case "moved": {
      const o = b as typeof a;
      return (
        a.status === o.status &&
        a.sagaId === o.sagaId &&
        a.failed === o.failed &&
        a.to?.keyspaceId === o.to?.keyspaceId &&
        a.to?.key === o.to?.key &&
        a.run?.status === o.run?.status &&
        a.run?.cause === o.run?.cause
      );
    }
    default:
      return true; // null, redacted, and a CRDT key: its document is live, and only a rebuild (forced) replaces it
  }
}

export function sameRow(a: Row, b: Row): boolean {
  if (a.key !== b.key || a.version !== b.version || a.predicted !== b.predicted || a.pending.length !== b.pending.length) return false;
  for (let i = 0; i < a.pending.length; i++) if (a.pending[i] !== b.pending[i]) return false;
  if (!sameValue(a.value, b.value)) return false;
  if ((a.subtree === undefined) !== (b.subtree === undefined)) return false;
  if (a.subtree !== undefined && b.subtree !== undefined) {
    if (a.subtree.length !== b.subtree.length) return false;
    for (let i = 0; i < a.subtree.length; i++) if (!sameRow(a.subtree[i] as Row, b.subtree[i] as Row)) return false;
  }
  return true;
}

export class Live implements LiveQuery {
  readonly stats: LiveStats = { fullRuns: 0, keyEvals: 0, updates: 0 };
  private rows: Row[] = [];
  private readonly byKey = new Map<string, Row>();
  private next: string | undefined;
  private state: KeyspaceState;
  private available = false;
  private statusKey = "";
  private isClosed = false;
  private cached: QueryResult | undefined;

  constructor(
    readonly query: Query,
    readonly plan: Plan,
    private readonly services: Services,
    private readonly listener: LiveListener,
    private readonly onClose: (l: Live) => void,
  ) {
    this.state = stateOf("unfollowed", undefined);
  }

  get closed(): boolean {
    return this.isClosed;
  }

  get keyspaceId(): string {
    return this.plan.keyspaceId;
  }

  snapshot(): QueryResult {
    if (this.cached === undefined) {
      this.cached = this.available ? resultOf(this.plan, this.state, this.rows.slice(), this.next) : unavailableResult(this.plan, this.state);
    }
    return this.cached;
  }

  close(): void {
    if (this.isClosed) return;
    this.isClosed = true;
    this.rows = [];
    this.byKey.clear();
    this.cached = undefined;
    this.onClose(this);
  }

  // -------------------------------------------------------------------------------------------------------------------

  /** Evaluates the whole query and reports what differs from what the query held. */
  refresh(cause: LiveCause): void {
    if (this.isClosed) return;
    this.stats.fullRuns++;
    const r = this.services.read(this.plan.keyspaceId, this.plan);
    let rows: Row[] = [];
    let next: string | undefined;
    this.available = r.ok;
    if (r.ok) {
      const ev = evaluate(r.ctx, this.plan);
      rows = ev.rows;
      next = ev.next;
    }
    const added: Row[] = [];
    const changed: RowChange[] = [];
    const removed: Row[] = [];
    const seen = new Set<string>();
    for (const row of rows) {
      seen.add(row.key);
      const had = this.byKey.get(row.key);
      if (had === undefined) added.push(row);
      else if (!sameRow(had, row)) changed.push({ before: had, after: row });
    }
    for (const [k, had] of this.byKey) if (!seen.has(k)) removed.push(had);
    this.rows = rows;
    this.byKey.clear();
    for (const row of rows) this.byKey.set(row.key, row);
    this.next = next;
    this.state = r.state;
    this.deliver(cause, added, changed, removed, true);
  }

  /** The keyspace's copy was purged (14 [CLI-66]): nothing is held and nothing is claimed. */
  purged(): void {
    if (this.isClosed) return;
    if (this.rows.length === 0 && !this.available) return;
    this.refresh("purged");
  }

  /** A change reached the scopes of the query. */
  apply(t: Touch): void {
    if (this.isClosed) return;
    const r = this.services.read(this.plan.keyspaceId, this.plan);
    if (!r.ok || !this.available) {
      this.refresh("refresh");
      return;
    }
    const ctx = r.ctx;
    if (this.plan.limit !== undefined) {
      const effect = this.windowEffect(t);
      if (effect === "rerun") this.refresh("change");
      else if (effect === "next") this.refreshNext(ctx);
      return;
    }
    const keys = new Set<string>();
    for (const k of t.keys) this.addRowKey(keys, k);
    for (const m of t.subtrees) this.addSubtreeRowKeys(keys, m, ctx);
    for (const k of t.forced ?? []) this.addRowKey(keys, k);
    for (const k of t.keys) if (k.startsWith(RUN_PREFIX)) this.addRunRows(keys, k);
    const added: Row[] = [];
    const changed: RowChange[] = [];
    const removed: Row[] = [];
    for (const k of keys) {
      this.stats.keyEvals++;
      const now = evalRow(ctx, this.plan, k);
      const had = this.byKey.get(k);
      if (now === undefined) {
        if (had === undefined) continue;
        this.drop(k);
        removed.push(had);
      } else if (had === undefined) {
        this.put(now);
        added.push(now);
      } else if (!sameRow(had, now) || t.forced?.has(k) === true) {
        this.put(now);
        changed.push({ before: had, after: now });
      }
    }
    this.state = r.state;
    this.deliver("change", added, changed, removed, false);
  }

  // -------------------------------------------------------------------------------------------------------------------

  private addRowKey(out: Set<string>, key: string): void {
    const r = this.plan.rowKeyOf(key);
    if (r !== undefined && this.plan.isAfterCursor(r)) out.add(r);
  }

  /** The pointer rows of the moves that a run key says something of (14 [CLI-14]: a failed move is shown as failed), whatever kind of query holds them. */
  private addRunRows(out: Set<string>, runKey: string): void {
    const saga = runKey.slice(RUN_PREFIX.length).split("/")[0];
    for (const [k, row] of this.byKey) if (row.value.kind === "moved" && row.value.sagaId === saga) out.add(k);
  }

  /** The rows that a touched subtree of `m` may concern: those the query holds in it, those the selection may now take from it, and the entity that `m` lies in. */
  private addSubtreeRowKeys(out: Set<string>, m: string, ctx: Ctx): void {
    const sub = `${m}/`;
    for (const k of this.byKey.keys()) if (k === m || k.startsWith(sub)) out.add(k);
    const p = this.plan;
    if (p.kind === "keys") {
      for (const k of p.keys) if (k === m || k.startsWith(sub)) this.addRowKey(out, k);
      return;
    }
    this.addRowKey(out, m);
    let lo = 0;
    let hi = 0;
    if (p.rangePrefix.startsWith(sub)) [lo, hi] = ctx.index.range(p.rangePrefix);
    else if (sub.startsWith(p.rangePrefix)) [lo, hi] = ctx.index.range(sub);
    for (let i = lo; i < hi; i++) this.addRowKey(out, ctx.index.at(i));
    if (p.kind === "entities" && ctx.movedAny) {
      // an absent marker whose moved key says `purged` stands as a pointer: its moved key is in the subtree's moved range
      const [mlo, mhi] = ctx.index.range(`${MOVED_PREFIX}${sub}`);
      for (let i = mlo; i < mhi; i++) this.addRowKey(out, ctx.index.at(i).slice(MOVED_PREFIX.length));
    }
  }

  /**
   * What a change does to the window of a query with a page limit: a key before the cursor does nothing; a key after a window that is full cannot change its rows,
   * and only whether a row follows it (`next`); any other change evaluates the window again.
   */
  private windowEffect(t: Touch): "none" | "next" | "rerun" {
    if (t.subtrees.size > 0) return "rerun";
    const limit = this.plan.limit as number;
    const last = this.rows.length >= limit ? (this.rows[this.rows.length - 1] as Row).key : undefined;
    const keys = new Set<string>();
    for (const k of t.keys) this.addRowKey(keys, k);
    for (const k of t.forced ?? []) this.addRowKey(keys, k);
    let beyond = false;
    for (const k of keys) {
      if (last === undefined || this.plan.compare(k, last) <= 0) return "rerun";
      beyond = true;
    }
    for (const k of t.keys) {
      if (!k.startsWith(RUN_PREFIX)) continue;
      const saga = k.slice(RUN_PREFIX.length).split("/")[0];
      for (const row of this.rows) if (row.value.kind === "moved" && row.value.sagaId === saga) return "rerun";
    }
    return beyond ? "next" : "none";
  }

  /** The window is full and a key after it changed: only the cursor of the next page can differ, by whether some row follows the window. */
  private refreshNext(ctx: Ctx): void {
    const last = (this.rows[this.rows.length - 1] as Row).key;
    let more = false;
    for (const k of candidates(ctx, this.plan, last)) {
      this.stats.keyEvals++;
      if (evalRow(ctx, this.plan, k) !== undefined) {
        more = true;
        break;
      }
    }
    const next = more ? last : undefined;
    if (next !== this.next) {
      this.next = next;
      this.cached = undefined;
    }
  }

  // -------------------------------------------------------------------------------------------------------------------

  private put(row: Row): void {
    if (this.byKey.has(row.key)) {
      const i = this.positionOf(row.key);
      this.rows[i] = row;
    } else {
      let lo = 0;
      let hi = this.rows.length;
      while (lo < hi) {
        const mid = (lo + hi) >>> 1;
        if (this.plan.compare((this.rows[mid] as Row).key, row.key) < 0) lo = mid + 1;
        else hi = mid;
      }
      this.rows.splice(lo, 0, row);
    }
    this.byKey.set(row.key, row);
  }

  private drop(key: string): void {
    this.rows.splice(this.positionOf(key), 1);
    this.byKey.delete(key);
  }

  private positionOf(key: string): number {
    let lo = 0;
    let hi = this.rows.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.plan.compare((this.rows[mid] as Row).key, key) < 0) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  private deliver(cause: LiveCause, added: Row[], changed: RowChange[], removed: Row[], full: boolean): void {
    this.cached = undefined;
    let statusChanged = false;
    if (full) {
      const key = this.statusSignature();
      statusChanged = cause !== "initial" && key !== this.statusKey;
      this.statusKey = key;
    }
    if (cause !== "initial" && added.length + changed.length + removed.length === 0 && !statusChanged) return;
    this.stats.updates++;
    const snapshot = (): QueryResult => this.snapshot();
    const update: LiveUpdate = {
      cause,
      added,
      changed,
      removed,
      get result(): QueryResult {
        return snapshot();
      },
      statusChanged,
    };
    try {
      this.listener(update);
    } catch {
      // a listener never stops the layer
    }
  }

  private statusSignature(): string {
    const snap = this.snapshot();
    return `${snap.keyspace.status}|${snap.keyspace.stopped ?? ""}|${snap.coverage}|${snap.movesTracked ? 1 : 0}|${snap.notLoaded.join("\u0000")}`;
  }
}

