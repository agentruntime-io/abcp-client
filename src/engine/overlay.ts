// The view of a keyspace (14 [CLI-6]): its confirmed state with the prediction of every pending action applied on top, in outbox
// order, by the fixed rules of 05 §5.4 (14 §14.2). The prediction only shows pending changes at once; the server's result
// always replaces it. Prediction is total (14 [CLI-20]): data that the server accepted never makes it throw, and it never reads
// the clock (14 [CLI-9]).
//
//  - `lww` (14 [CLI-10]): a pending `SET` or `DELETE` shows its value, or the key's absence;
//  - `cas` (14 [CLI-11]): a pending `CAS` is shown as pending, and its value is predicted only if its expectation holds against
//    the view below it;
//  - `counter` (14 [CLI-12]): the confirmed value plus the deltas of every pending `INCR`;
//  - CRDT keys (14 [CLI-13]): the view document (crdt-view.ts) holds the user's edits; here the key is only marked pending;
//  - entities (14 [CLI-14]): a pending `DELETE` of a marker key hides its subtree;
//  - GATED actions (14 [CLI-15]): the requested changes are predicted as pending; the gate's effects are placeholders unless
//    the app registered a predictor for the op type;
//  - `optimistic: false` (14 [CLI-16]): no predicted values; the keys are shown as pending until the action retires.
//
// TODO(14 [CLI-14]): the moved entities (the `moved` key of a built-in move hides the entity and shows the pointer) and the
// saga following come with the coordination milestone; only the pending DELETE of a marker key hides a subtree here.

import { ChangeKind, type KeyChange } from "../gen/abpc/v2/types_pb.js";
import { JsonV1Error, applyIncr, counterText, parse as parseJson, parseCounter, type JsonValue, type Range } from "../rules/jsonv1/index.js";
import { compareUtf8 } from "../rules/bytes.js";
import { StateNull, StateRedacted, StateValue, type KeyRow, type ReplicaState, type Rule, type Topology } from "../rules/fold/index.js";

/** An action as prediction reads it. */
export interface PendingAction {
  mutationId: string;
  /** False for an op type with `optimistic: false` (14 [CLI-16]). */
  optimistic: boolean;
  gated: boolean;
  changes: readonly KeyChange[];
  /** The predicted effects of a GATED op type, from the app's predictor (14 [CLI-15]); absent: placeholders. */
  effects?: readonly KeyChange[];
}

/** What the prediction needs besides the actions: the topology (markers, parents) and the counter ranges. */
export interface PredictContext {
  top: Topology;
  counterRange(key: string): Range | undefined;
}

type CellState = "value" | "null" | "deleted" | "counter" | "crdt" | "pending";

interface Cell {
  state: CellState;
  value: Uint8Array;
  encoding: string;
  counter: bigint | null;
  pending: string[];
  /** A prediction (not only a pending mark). */
  predicted: boolean;
}

/** One key as the view shows it. */
export interface ViewValue {
  /** Always true for what `get` returns; an absent key (or a hidden one) is `undefined`. */
  present: true;
  /** The value bytes; empty for a null, for a CRDT key (its document is the value) and for a key in the redacted state. */
  value: Uint8Array;
  isNull: boolean;
  redacted: boolean;
  encoding: string;
  /** The key's version in the confirmed state; 0n for a key that no confirmed state holds. */
  version: bigint;
  /** The pending actions that touch the key, in outbox order. */
  pending: readonly string[];
  /** The value is a prediction of a pending action. */
  predicted: boolean;
  /** The key is a CRDT key: its value is a Yjs document (`Engine.crdt`). */
  crdt: boolean;
  /** The value can be displayed: its encoding is known and a `json/v1` value parses (14 [CLI-5]). */
  decodable: boolean;
}

/** The prediction of the pending actions of a keyspace: the cells they change and the subtrees they hide. */
export class Overlay {
  readonly cells = new Map<string, Cell>();
  readonly hidden = new Set<string>();
  /** The changes of each action that were predicted to have no effect (a `CAS` whose expectation does not hold, 14 [CLI-11]). */
  readonly noEffect = new Map<string, Set<string>>();
}

function cell(o: Overlay, key: string): Cell {
  let c = o.cells.get(key);
  if (c === undefined) {
    c = { state: "pending", value: new Uint8Array(0), encoding: "", counter: null, pending: [], predicted: false };
    o.cells.set(key, c);
  }
  return c;
}

function equalBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

/** Whether key is hidden by a pending subtree deletion (14 [CLI-14]). */
function isHiddenBy(o: Overlay, top: Topology, key: string): boolean {
  if (o.hidden.size === 0) return false;
  if (o.hidden.has(key)) return true;
  for (const a of top.ancestors(key)) if (o.hidden.has(a)) return true;
  return false;
}

interface Below {
  present: boolean;
  value: Uint8Array;
  isNull: boolean;
  /** An earlier pending action changed the key: its version does not exist yet. */
  touched: boolean;
  counter: bigint | null;
  encoding: string;
}

function viewBelow(st: ReplicaState, o: Overlay, top: Topology, key: string): Below {
  const c = o.cells.get(key);
  if (c !== undefined && c.predicted) {
    if (c.state === "deleted") return { present: false, value: new Uint8Array(0), isNull: false, touched: true, counter: null, encoding: "" };
    return { present: true, value: c.value, isNull: c.state === "null", touched: true, counter: c.counter, encoding: c.encoding };
  }
  if (isHiddenBy(o, top, key)) return { present: false, value: new Uint8Array(0), isNull: false, touched: true, counter: null, encoding: "" };
  const row = st.key(key);
  const touched = c !== undefined;
  if (row === undefined) return { present: false, value: new Uint8Array(0), isNull: false, touched, counter: null, encoding: "" };
  return { present: true, value: row.value, isNull: row.state === StateNull, touched, counter: null, encoding: row.encoding };
}

/** Applies one pending change to the overlay (14 §14.2). Never throws (14 [CLI-20]). */
function predict(st: ReplicaState, o: Overlay, ctx: PredictContext, mutationId: string, c: KeyChange, optimistic: boolean): void {
  const key = c.key;
  const cl = cell(o, key);
  cl.pending.push(mutationId);
  if (!optimistic) return; // 14 [CLI-16]: shown as pending only
  try {
    switch (c.kind) {
      case ChangeKind.SET:
      case ChangeKind.OVERRIDE: {
        if (c.update.length > 0 && c.value.length === 0) {
          cl.state = "crdt"; // an OVERRIDE of a CRDT key: its document shows it
          cl.predicted = true;
        } else {
          cl.state = c.isNull ? "null" : "value";
          cl.value = c.isNull ? new Uint8Array(0) : c.value;
          cl.encoding = c.encoding;
          cl.counter = null;
          cl.predicted = true;
        }
        break;
      }
      case ChangeKind.DELETE: {
        cl.state = "deleted";
        cl.value = new Uint8Array(0);
        cl.predicted = true;
        // 14 [CLI-14]: a pending DELETE of a marker key hides its subtree
        if (ctx.top.isMarker(key)) o.hidden.add(key);
        break;
      }
      case ChangeKind.INCR: {
        const below = viewBelow(st, o, ctx.top, key);
        let cur: bigint | null = null;
        if (below.present && !below.isNull) cur = below.counter ?? parseCounter(below.value);
        const n = applyIncr(cur, c.delta, ctx.counterRange(key));
        cl.state = "counter";
        cl.counter = n;
        cl.value = counterText(n);
        cl.encoding = "json/v1";
        cl.predicted = true;
        break;
      }
      case ChangeKind.CAS: {
        // 14 [CLI-11]: predicted only if its expectation holds against the view below it
        const below = viewBelow(st, o, ctx.top, key);
        const ex = c.expected?.expect;
        let holds = false;
        if (ex?.case === "version") holds = !below.touched && st.effectiveVersion(ctx.top, key) === ex.value;
        else if (ex?.case === "value") holds = below.present && !below.isNull && equalBytes(below.value, ex.value);
        else if (ex?.case === "absent") holds = !below.present && st.tombstone(key)?.kind !== "entity";
        if (holds) {
          cl.state = c.isNull ? "null" : "value";
          cl.value = c.isNull ? new Uint8Array(0) : c.value;
          cl.encoding = c.encoding;
          cl.counter = null;
          cl.predicted = true;
        } else {
          let s = o.noEffect.get(mutationId);
          if (s === undefined) {
            s = new Set();
            o.noEffect.set(mutationId, s);
          }
          s.add(key);
        }
        break;
      }
      case ChangeKind.CRDT_UPDATE:
        cl.state = "crdt";
        cl.predicted = true;
        break;
      default:
        break;
    }
  } catch (e) {
    // 14 [CLI-20]: data that the server accepted never makes prediction throw. An INCR that leaves its range, a value that is not
    // a counter: the change has no predicted effect, and the key is shown as pending.
    // SPEC-QUESTION: 14 [CLI-12] says that "the view of a counter is its confirmed value plus the deltas of every pending `INCR`", and
    // 05 [STATE-28] that the server rejects an INCR whose result leaves the template's range with RANGE_EXCEEDED and applies none of its
    // changes. Reading A (taken): an INCR that would leave the range is no part of the predicted counter (its key is shown as pending), as a
    // CAS whose expectation fails has no predicted effect ([CLI-11]); the view never shows a value that the server will refuse.
    // Reading B: the view is the plain sum of every pending delta, an out-of-range value included, until the server's RANGE_EXCEEDED
    // ends that action and the view returns to the confirmed value plus the deltas that remain.
    if (!(e instanceof JsonV1Error)) {
      // any other failure is the same: no predicted effect
    }
  }
}

/** Builds the overlay of the pending actions, in outbox order (14 [CLI-6]). */
export function buildOverlay(st: ReplicaState, ctx: PredictContext, pending: readonly PendingAction[]): Overlay {
  const o = new Overlay();
  for (const a of pending) {
    try {
      for (const c of a.changes) predict(st, o, ctx, a.mutationId, c, a.optimistic);
      // 14 [CLI-15]: the effects of a GATED action are predicted only when the app registered a predictor
      if (a.gated && a.optimistic && a.effects !== undefined) {
        for (const c of a.effects) predict(st, o, ctx, a.mutationId, c, true);
      }
    } catch {
      // total: see predict
    }
  }
  return o;
}

const KNOWN_ENCODINGS = new Set(["json/v1", "bytes/v1", "yjs/v1", "redacted/v1"]);

const decodableMemo = new WeakMap<object, boolean>();

/** Whether a value can be displayed locally (14 [CLI-5]): a known encoding, and a `json/v1` value that parses. */
export function canDecode(encoding: string, value: Uint8Array, memo?: object): boolean {
  if (memo !== undefined) {
    const m = decodableMemo.get(memo);
    if (m !== undefined) return m;
  }
  let ok: boolean;
  if (!KNOWN_ENCODINGS.has(encoding)) ok = false;
  else if (encoding === "json/v1") {
    try {
      parseJson(value);
      ok = true;
    } catch {
      ok = false;
    }
  } else ok = true;
  if (memo !== undefined) decodableMemo.set(memo, ok);
  return ok;
}

/** The view of one keyspace: confirmed state plus the overlay of the pending actions. */
export class KeyspaceView {
  constructor(
    readonly keyspaceId: string,
    private readonly st: ReplicaState,
    private readonly overlay: Overlay,
    private readonly top: Topology,
  ) {}

  /** The confirmed cursor: the seq and the epoch of the confirmed state. */
  get seq(): bigint {
    return this.st.seq;
  }

  get logEpoch(): number {
    return this.st.logEpoch;
  }

  private confirmedRow(key: string): Readonly<KeyRow> | undefined {
    return this.st.key(key);
  }

  /** The key as the view shows it, or undefined when it is absent or hidden. */
  get(key: string): ViewValue | undefined {
    const c = this.overlay.cells.get(key);
    const row = this.confirmedRow(key);
    const pending = c?.pending ?? [];
    if (c !== undefined && c.predicted) {
      if (c.state === "deleted") return undefined;
      if (c.state === "crdt") {
        return { present: true, value: new Uint8Array(0), isNull: false, redacted: false, encoding: "yjs/v1", version: row?.version ?? 0n, pending, predicted: true, crdt: true, decodable: true };
      }
      return {
        present: true,
        value: c.value,
        isNull: c.state === "null",
        redacted: false,
        encoding: c.encoding,
        version: row?.version ?? 0n,
        pending,
        predicted: true,
        crdt: false,
        decodable: canDecode(c.encoding, c.value),
      };
    }
    if (isHiddenBy(this.overlay, this.top, key)) return undefined;
    if (row === undefined) return undefined;
    const isCrdt = row.rule === "crdt";
    return {
      present: true,
      value: isCrdt ? new Uint8Array(0) : row.value,
      isNull: row.state === StateNull,
      redacted: row.state === StateRedacted,
      encoding: row.encoding,
      version: row.version,
      pending,
      predicted: false,
      crdt: isCrdt,
      decodable: row.state === StateValue && !isCrdt ? canDecode(row.encoding, row.value, row) : true,
    };
  }

  has(key: string): boolean {
    return this.get(key) !== undefined;
  }

  /** The present keys that start with the prefix, in ascending order of their bytes, hidden subtrees left out. */
  keys(prefix = ""): string[] {
    const out = new Set<string>();
    for (const k of this.st.keyNames()) if (k.startsWith(prefix)) out.add(k);
    for (const [k, c] of this.overlay.cells) if (c.predicted && k.startsWith(prefix)) out.add(k);
    return [...out].filter((k) => this.get(k) !== undefined).sort(compareUtf8);
  }

  /** The json/v1 value of a key, or undefined when the key is absent, null, not json/v1, or cannot be decoded. */
  json(key: string): JsonValue | undefined {
    const v = this.get(key);
    if (v === undefined || v.isNull || v.encoding !== "json/v1" || !v.decodable) return undefined;
    try {
      return parseJson(v.value);
    } catch {
      return undefined;
    }
  }

  /** The pending actions that touch the key. */
  pending(key: string): readonly string[] {
    return this.overlay.cells.get(key)?.pending ?? [];
  }

  /** The keys of the action's changes that the prediction gave no effect, such as a `CAS` whose expectation does not hold (14 [CLI-11]). */
  noEffectKeys(mutationId: string): string[] {
    return [...(this.overlay.noEffect.get(mutationId) ?? [])];
  }

  /** The merge rule of the key's confirmed row, when it has one. */
  rule(key: string): Rule | undefined {
    return this.confirmedRow(key)?.rule;
  }
}
