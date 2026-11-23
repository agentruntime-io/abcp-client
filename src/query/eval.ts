// Evaluating a query against the view of one keyspace: what a key shows as (its value decoded, a moved or staged entity as 14 [CLI-14] says), which rows a
// plan selects, and the result with what it says of the copy and of the interest.

import { getMember, parse, parseCounter } from "../rules/jsonv1/index.js";
import { isReservedKey } from "../rules/fold/index.js";
import type { KeyspaceInfo } from "../engine/engine.js";
import type { KeyspaceView, ViewValue } from "../engine/overlay.js";
import { decodeJson } from "./json.js";
import { KeyIndex, mergeSorted } from "./key-index.js";
import { inInterest, MOVED_PREFIX, Plan, RUN_PREFIX, type Match } from "./select.js";
import type { CrdtRowValue, EntityRef, KeyspaceState, KeyspaceStatus, MovedRowValue, QueryMode, QueryResult, Row, RowValue } from "./types.js";

/** What evaluation reads of the engine (the layer's `QueryEngine`, narrowed). */
export interface Reader {
  /** The view document of a CRDT key (`Engine.crdt`): the user's view document, or the confirmed one; undefined where the engine has none to give. */
  crdtDoc(keyspaceId: string, key: string, mode: QueryMode): CrdtRowValue["doc"];
}

/** One evaluation's reading of a keyspace: the view of the query's mode, the copy's state, and the index of its keys. */
export interface Ctx {
  reader: Reader;
  keyspaceId: string;
  mode: QueryMode;
  view: KeyspaceView;
  info: KeyspaceInfo;
  index: KeyIndex;
  /** The copy holds some moved key (`abcp.saga.v1/moved/…`), so that moved and staged entities have to be looked for (14 [CLI-14]). */
  movedAny: boolean;
}

export function stateOf(status: KeyspaceStatus, info: KeyspaceInfo | undefined): KeyspaceState {
  return { status, stopped: info?.stopped, seq: info?.seq ?? 0n, logEpoch: info?.logEpoch ?? 0, interest: info === undefined ? [] : [...info.interest] };
}

// ---------------------------------------------------------------------------------------------------------------------
// Values

/** A CRDT value: the document is fetched when it is read, so that listing keys opens no document. */
function crdtValue(ctx: Ctx, key: string): CrdtRowValue {
  const { reader, keyspaceId, mode } = ctx;
  return {
    kind: "crdt",
    get doc(): CrdtRowValue["doc"] {
      return reader.crdtDoc(keyspaceId, key, mode);
    },
  };
}

/** What a key's value shows as, by its encoding and its state (05 §5.3, [STATE-82], 14 [CLI-5], [CLI-13]). Never throws. */
export function decodeValue(ctx: Ctx, key: string, v: ViewValue): RowValue {
  if (v.redacted) return { kind: "redacted" }; // 05 [STATE-82]: present, and never shown as a value
  if (v.crdt) return crdtValue(ctx, key);
  if (v.isNull) return { kind: "null" };
  const undecodable = (): RowValue => ({ kind: "undecodable", encoding: v.encoding, bytes: v.value.slice() });
  if (!v.decodable) return undecodable();
  try {
    switch (v.encoding) {
      case "json/v1":
        if (ctx.view.ruleOf(key) === "counter") return { kind: "counter", value: parseCounter(v.value) };
        return { kind: "json", value: decodeJson(v.value) };
      case "bytes/v1":
        return { kind: "bytes", value: v.value.slice() };
      default:
        return undecodable();
    }
  } catch {
    return undecodable();
  }
}

// ---------------------------------------------------------------------------------------------------------------------
// Moved and staged entities (14 [CLI-14], 11 [COORD-73], [COORD-76])

interface MovedKey {
  status: string;
  sagaId: string | undefined;
  to: EntityRef | undefined;
}

/** The moved key of a marker key, read from the copy; undefined when it holds none, or one that does not say a `status`. */
function movedKeyOf(ctx: Ctx, marker: string): MovedKey | undefined {
  const mv = ctx.view.get(`${MOVED_PREFIX}${marker}`);
  if (mv === undefined || mv.redacted || mv.isNull || mv.crdt || mv.encoding !== "json/v1" || !mv.decodable) return undefined;
  try {
    const doc = parse(mv.value);
    const status = getMember(doc, "status");
    if (typeof status !== "string") return undefined;
    const sagaId = getMember(doc, "saga_id");
    const to = getMember(doc, "to");
    const toKs = getMember(to, "keyspace_id");
    const toKey = getMember(to, "key");
    return {
      status,
      sagaId: typeof sagaId === "string" ? sagaId : undefined,
      to: typeof toKs === "string" && typeof toKey === "string" ? { keyspaceId: toKs, key: toKey } : undefined,
    };
  } catch {
    return undefined;
  }
}

/** What the run key `abcp.saga.v1/run/{saga_id}` says of the move, when the copy holds it. */
function runOf(ctx: Ctx, sagaId: string): { status: string; cause: string | undefined } | undefined {
  const rv = ctx.view.get(`${RUN_PREFIX}${sagaId}`);
  if (rv === undefined || rv.redacted || rv.isNull || rv.crdt || rv.encoding !== "json/v1" || !rv.decodable) return undefined;
  try {
    const doc = parse(rv.value);
    const status = getMember(doc, "status");
    if (typeof status !== "string") return undefined;
    const cause = getMember(doc, "cause");
    return { status, cause: typeof cause === "string" ? cause : undefined };
  } catch {
    return undefined;
  }
}

/**
 * Whether an ancestor marker of the key (05 §5.6, [STATE-42]: the key is in the entity's subtree) is an entity that is moving, moved or staged: it hides the
 * key (14 [CLI-14]).
 *
 * SPEC-QUESTION (14 [CLI-14] with 11 [COORD-76]): the text hides "an entity whose source `moved` key says `moving` or `moved`", and, for a `confirmed` read of the
 * state, says nothing: the rule is written for "the view". Reading A (taken): it is a rule of what a reader shows, driven by the moved keys of the confirmed state, and
 * applies to a `confirmed` query as to a `predicted` one, so that no query of the copy shows rows of an entity that the server freezes and hides. Reading B: only the
 * view (the prediction) hides it, and a `confirmed` query is the state as the fold holds it, the entity's rows included.
 */
function hiddenByAncestor(ctx: Ctx, key: string): boolean {
  for (const a of ctx.view.ancestors(key)) {
    const m = movedKeyOf(ctx, a);
    if (m !== undefined && (m.status === "moving" || m.status === "moved" || m.status === "importing")) return true;
  }
  return false;
}

/**
 * The pointer row of a moved entity.
 *
 * SPEC-QUESTION (14 [CLI-14], 11 [COORD-114]): "When a client holds that run key and it says `failed`, the client MUST show the move to the user as failed, with both sides
 * kept as they are, instead of the pointer." Reading A (taken): the row stays the moved entity's row, the entity and its subtree stay hidden (the first bullet hides an entity whose
 * moved key says `moving` or `moved`, whatever its run says), and `failed` is true with `to` kept, so that the app shows the failed move instead of "moved to X", and has the
 * target for `ResolveMove` (11 [COORD-105]). Reading B: "both sides kept as they are" says that the source entity is shown again as it is, beside the notice of the failed move,
 * and the pointer is dropped.
 */
function pointerRow(ctx: Ctx, key: string, m: MovedKey, status: "moving" | "moved" | "purged", v: ViewValue | undefined, bindings: Match["bindings"]): Row {
  const run = m.sagaId === undefined ? undefined : runOf(ctx, m.sagaId);
  const value: MovedRowValue = {
    kind: "moved",
    status,
    to: m.to,
    sagaId: m.sagaId,
    runKey: m.sagaId === undefined ? undefined : `${RUN_PREFIX}${m.sagaId}`,
    run,
    failed: run?.status === "failed",
  };
  const mv = ctx.view.get(`${MOVED_PREFIX}${key}`);
  return { keyspaceId: ctx.keyspaceId, key, bindings, value, version: v?.version ?? mv?.version ?? 0n, pending: v === undefined ? [] : [...v.pending], isPending: (v?.pending.length ?? 0) > 0, predicted: false };
}

// ---------------------------------------------------------------------------------------------------------------------
// Rows

/**
 * What the key shows as in the view, or undefined when it shows as nothing: it is absent (never set, deleted, below a deleted entity, 05 §5.3 and
 * [STATE-43]; a tombstone is no key, 09 [READ-9]), or it is hidden by an entity that is moving, moved or staged (14 [CLI-14]). A marker key whose entity
 * is moving or moved shows as the pointer; one whose moved key says `purged` shows as the pointer while its marker is absent (`absentPointer`).
 */
export function resolveRow(ctx: Ctx, key: string, bindings: Match["bindings"], absentPointer: boolean): Row | undefined {
  const v = ctx.view.get(key);
  if (ctx.movedAny && !isReservedKey(key)) {
    const m = movedKeyOf(ctx, key);
    if (m !== undefined) {
      // SPEC-QUESTION (14 [CLI-14]): "show in its place the key's `to` pointer" for an entity whose moved key says `moving` or `moved`; for `purged` the text says that the
      // pointer is shown "only while the entity's marker is absent". Reading A (taken): for `moving` and `moved` the pointer stands in the place of a marker that is
      // present, and a marker that is absent (a SYSTEM record, 11 [COORD-104], can delete a frozen one) shows nothing, as any absent key. Reading B: the pointer is shown
      // whenever the moved key says `moving` or `moved`, a marker that is absent included, as it is for `purged`.
      if (v !== undefined && (m.status === "moving" || m.status === "moved")) return pointerRow(ctx, key, m, m.status, v, bindings);
      if (v !== undefined && m.status === "importing") return undefined; // a staged entity is not yet present (11 [COORD-111])
      if (v === undefined && m.status === "purged" && absentPointer) return pointerRow(ctx, key, m, "purged", undefined, bindings);
    }
    if (v !== undefined && hiddenByAncestor(ctx, key)) return undefined;
  }
  if (v === undefined) return undefined;
  return { keyspaceId: ctx.keyspaceId, key, bindings, value: decodeValue(ctx, key, v), version: v.version, pending: [...v.pending], isPending: v.pending.length > 0, predicted: v.predicted };
}

/** The row of a plan's selection for one row key (a key, or an entity's marker), with the filter and the subtree applied; undefined when there is none. */
export function evalRow(ctx: Ctx, plan: Plan, rowKey: string): Row | undefined {
  const m = plan.match(rowKey);
  if (m === undefined) return undefined;
  const row = resolveRow(ctx, rowKey, m.bindings, plan.kind === "keys" || plan.kind === "entities" || (plan.kind === "template" && plan.keys.length === 1));
  if (row === undefined) return undefined;
  if (plan.filter !== undefined) {
    const entity = plan.kind === "entities";
    if (!plan.filter.test({ self: row.value, at: (rel) => (entity ? resolveRow(ctx, `${rowKey}/${rel}`, undefined, false)?.value : undefined) })) return undefined;
  }
  if (plan.kind === "entities" && plan.subtree && row.value.kind !== "moved") {
    // 05 [STATE-42]: the subtree of a marker is the keys that have it among their ancestor markers
    const [lo, hi] = ctx.index.range(`${rowKey}/`);
    const below: Row[] = [];
    for (let i = lo; i < hi; i++) {
      const k = ctx.index.at(i);
      if (!ctx.view.ancestors(k).includes(rowKey)) continue;
      const r = resolveRow(ctx, k, undefined, false);
      if (r !== undefined) below.push(r);
    }
    return { ...row, subtree: below };
  }
  return row;
}

// ---------------------------------------------------------------------------------------------------------------------
// Candidates

/** The keys of an index range in the query's order. */
function* walk(index: KeyIndex, lo: number, hi: number, order: "asc" | "desc"): Generator<string> {
  if (order === "asc") for (let i = lo; i < hi; i++) yield index.at(i);
  else for (let i = hi - 1; i >= lo; i--) yield index.at(i);
}

/**
 * The row keys that a plan may select, in its order and after its cursor (or after `cursor`, a row key: a window asking whether a row follows it): a superset
 * of the rows, each to be judged by `evalRow`.
 */
export function* candidates(ctx: Ctx, plan: Plan, cursor: string | undefined = plan.after): Generator<string> {
  if (plan.kind === "keys") {
    for (const k of plan.keys) if (cursor === undefined || plan.compare(k, cursor) > 0) yield k;
    return;
  }
  const rangeOf = (prefix: string): [number, number] => {
    let [lo, hi] = ctx.index.range(prefix);
    if (cursor !== undefined) {
      // a range of a marker's moved keys is cut by the cursor of the markers, which has the moved prefix put in front
      const cut = prefix.startsWith(MOVED_PREFIX) ? `${MOVED_PREFIX}${cursor}` : cursor;
      if (plan.order === "asc") lo = Math.max(lo, ctx.index.upperBound(cut));
      else hi = Math.min(hi, ctx.index.lowerBound(cut));
    }
    return [lo, Math.max(lo, hi)];
  };
  const [lo, hi] = rangeOf(plan.rangePrefix);
  if (plan.kind !== "entities" || !ctx.movedAny) {
    yield* walk(ctx.index, lo, hi, plan.order);
    return;
  }
  // an entity query also lists the markers that are absent and whose moved key says `purged`: their pointer stands in the marker's place
  const [mlo, mhi] = rangeOf(`${MOVED_PREFIX}${plan.rangePrefix}`);
  const markers: string[] = [];
  for (let i = mlo; i < mhi; i++) markers.push(ctx.index.at(i).slice(MOVED_PREFIX.length));
  const own = ctx.index.slice(lo, hi);
  const merged = mergeSorted(own, markers);
  if (plan.order === "desc") merged.reverse();
  yield* merged;
}

// ---------------------------------------------------------------------------------------------------------------------
// Whole queries

export interface Evaluated {
  rows: Row[];
  next: string | undefined;
}

/** Evaluates a plan from scratch: the page of rows it selects, and the cursor of the next page. */
export function evaluate(ctx: Ctx, plan: Plan): Evaluated {
  const rows: Row[] = [];
  let more = false;
  for (const k of candidates(ctx, plan)) {
    const row = evalRow(ctx, plan, k);
    if (row === undefined) continue;
    if (plan.limit !== undefined && rows.length === plan.limit) {
      more = true;
      break;
    }
    rows.push(row);
  }
  return { rows, next: more ? (rows[rows.length - 1] as Row).key : undefined };
}

/** A result for a keyspace that cannot answer: nothing is shown and nothing is claimed absent. */
export function unavailableResult(plan: Plan, state: KeyspaceState): QueryResult {
  return {
    keyspaceId: plan.keyspaceId,
    mode: plan.mode,
    rows: [],
    next: undefined,
    notLoaded: plan.kind === "keys" ? [...plan.keys] : [],
    coverage: "none",
    movesTracked: false,
    keyspace: state,
  };
}

/** The result of a plan over the rows that were selected. */
export function resultOf(plan: Plan, state: KeyspaceState, rows: readonly Row[], next: string | undefined): QueryResult {
  const interest = state.interest;
  return {
    keyspaceId: plan.keyspaceId,
    mode: plan.mode,
    rows,
    next,
    // 14 [CLI-71], 09 [READ-26]: a key outside the interest is not loaded: it is not shown, and it is not absent
    notLoaded: plan.kind === "keys" ? plan.keys.filter((k) => !inInterest(interest, k)) : [],
    coverage: plan.coverage(interest),
    movesTracked: plan.movesTracked(interest),
    keyspace: state,
  };
}

