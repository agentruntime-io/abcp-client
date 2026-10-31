// CRDT keys in the view (14 [CLI-13]): for each CRDT key it shows, the client keeps two Yjs documents: a **confirmed document**,
// built from the confirmed base and the confirmed updates, and a **view document**, which the user edits: the confirmed document
// with the user's pending updates applied.
//
// Confirmed updates are integrated by the fold of 05 [STATE-76]: an update that is a no-op for good is never integrated and
// leaves nothing pending, also when a missing update arrives later, so the verdict is read before the update is applied
// (`planCrdt` asks the fold's own questions of the state's own document, before the record is applied): stock Yjs would keep the
// structs of such an update pending.
//
// A transaction that the CRDT library starts while the client integrates an update (Yjs formatting cleanup) never becomes an
// action: only the user's own edits do (14 [CLI-13]). `CrdtKey.integrating` marks the window in which the view document's
// `update` events are ignored.
//
// A rebuild makes a fresh view document, with a fresh Yjs client id, from the confirmed document and the remaining pending
// updates, and re-applies once, as a diff, the user's edits that are in no action yet (02 [ARC-52] item 3).
//
// TODO(R2, 14 [CLI-13], [CLI-49], [CLI-76]): the redaction reset (a `REDACTED` record, a load entry with the redaction mark, a
// listed redaction): the rebuild that re-applies nothing, and the key's local draft. A DOC_RESET reject keeps the text of the
// rejected action as a draft in the dead-letter flow of the engine, but no reset is detected from the log yet.
// TODO(later): a diff of Y.XmlFragment roots, and of text formatting attributes; the diff here is of plain text, of map members
// and of array items.

import * as Y from "yjs";
import { ChangeKind, DecisionStatus, RecordKind, type KeyEntry, type Record as RecordMessage } from "../gen/abpc/v2/types_pb.js";
import { parseUpdate, type Roots } from "../rules/crdt/index.js";
import { Analysis, Doc as FoldDoc, noOpForGood, unionRanges } from "../rules/crdt/doc.js";
import { StateValue, VersionTopology, type ReplicaState, type Rules } from "../rules/fold/index.js";
import { selectSystem } from "../rules/fold/system.js";
import type { Random } from "./clock.js";
import { newYjsClientId } from "./ids.js";

/** The origin of every transaction that the engine itself starts on a document. */
export const ENGINE_ORIGIN = Symbol("abpc.engine");

/** Applies an update that came from storage or the server; one that Yjs cannot read is left out, never thrown (14 [CLI-20], [CLI-29]). */
function safeApply(doc: Y.Doc, update: Uint8Array, origin?: unknown): boolean {
  try {
    Y.applyUpdate(doc, update, origin);
    return true;
  } catch {
    return false;
  }
}

export type Verdict = "NOOP_FOR_GOOD" | "NO_EFFECT" | "APPLIED";

/** One CRDT update of a record that the fold applies, with the fold's verdict on it (05 [STATE-76]). */
export interface CrdtPlanItem {
  key: string;
  update: Uint8Array;
  /** A SYSTEM `SET` of a CRDT key: a base, which starts the key's document anew (05 [STATE-41], 06 [LOG-40]). */
  base: boolean;
  verdict: Verdict;
}

/** The verdict that the fold will give an update against the document of the key, read without changing the document. */
function verdictOf(doc: FoldDoc | null, update: Uint8Array): Verdict | undefined {
  let u;
  try {
    u = parseUpdate(update);
  } catch {
    return undefined; // not well formed: the fold refuses the record, and nothing is integrated
  }
  const d = doc ?? new FoldDoc();
  if (noOpForGood(d, new Analysis(d, u)) !== undefined) return "NOOP_FOR_GOOD";
  let changed = false;
  for (const g of u.groups) {
    const last = g.structs[g.structs.length - 1];
    if (last !== undefined && last.clock + last.len > d.next(g.client)) changed = true;
  }
  for (const dg of u.deletes) {
    if (unionRanges(d.ds.get(dg.client) ?? [], dg.ranges).changed) changed = true;
  }
  return changed ? "APPLIED" : "NO_EFFECT";
}

/** The indexes of the changes of a record that the fold applies (05 [STATE-34]), by the record's kind. */
export function appliedIndexes(rec: RecordMessage): number[] {
  const all = (): number[] => rec.changes.map((_, i) => i);
  switch (rec.kind) {
    case RecordKind.OP:
      return all();
    case RecordKind.DECISION:
      // 06 [LOG-32]: a COMMITTED decision applies every change; a REJECTED one only the intent row, which is no business key
      return rec.decision?.status === DecisionStatus.COMMITTED ? all() : [];
    case RecordKind.SYSTEM:
      try {
        const sel = selectSystem(rec, all);
        return sel.skip === true ? [] : sel.applied;
      } catch {
        return []; // the fold refuses the record
      }
    default:
      return []; // an INTENT applies only its intent row
  }
}

/**
 * The CRDT updates of a record that the fold is about to apply to `st`, with their verdicts. It reads the state as it is before
 * the record, applies nothing, and follows the same selection and the same version-gated test as the fold (05 [STATE-34],
 * [STATE-38]); a record that the fold refuses leaves it unused.
 */
export function planCrdt(st: ReplicaState, rules: Rules, rec: RecordMessage): CrdtPlanItem[] {
  const applied = appliedIndexes(rec);
  if (applied.length === 0) return [];
  const top = new VersionTopology(rules, rec.manifestVersion);
  const out: CrdtPlanItem[] = [];
  for (const i of applied) {
    const c = rec.changes[i];
    if (c === undefined) continue;
    const tpl = top.template(c.key);
    if (tpl === undefined || tpl.rule !== "crdt") continue;
    if (rec.seq <= st.gateVersion(top, c.key)) continue; // 05 [STATE-38]: the base already holds it, or the key was deleted since
    if (c.encoding === "redacted/v1") continue; // the key goes to the redacted state; `reconcile` drops its documents
    const isBase = c.kind === ChangeKind.SET && !c.isNull;
    const isUpdate = c.kind === ChangeKind.CRDT_UPDATE || c.kind === ChangeKind.OVERRIDE;
    if (!isBase && !isUpdate) continue;
    const bytes = isBase ? c.value : c.update;
    const old = st.key(c.key);
    const doc = !isBase && old !== undefined && old.state === StateValue && old.doc !== null ? old.doc : null;
    const verdict = isBase ? ("APPLIED" as const) : verdictOf(doc, bytes);
    if (verdict === undefined) continue;
    out.push({ key: c.key, update: bytes, base: isBase, verdict });
  }
  return out;
}

// ---------------------------------------------------------------------------------------------------------------------
// Snapshots and diffs of the content of a document's roots

type Snap =
  | { kind: "text"; text: string }
  | { kind: "map"; map: Map<string, unknown> }
  | { kind: "array"; items: unknown[] };

function sniff(doc: Y.Doc, name: string): "text" | "map" | "array" {
  const t = doc.share.get(name);
  if (t instanceof Y.Text) return "text";
  if (t instanceof Y.Map) return "map";
  if (t instanceof Y.Array) return "array";
  if (t !== undefined) {
    if (t._map.size > 0) return "map";
    let item = t._start;
    while (item !== null) {
      if (!item.deleted && item.content.constructor.name === "ContentString") return "text";
      item = item.right;
    }
  }
  return "array";
}

function rootKinds(doc: Y.Doc, roots: Roots | undefined): Map<string, "text" | "map" | "array"> {
  const out = new Map<string, "text" | "map" | "array">();
  if (roots !== undefined) {
    for (const [name, type] of roots) {
      if (type === "text" || type === "map" || type === "array") out.set(name, type);
    }
    return out;
  }
  for (const name of doc.share.keys()) out.set(name, sniff(doc, name));
  return out;
}

function snapshot(doc: Y.Doc, kinds: Map<string, "text" | "map" | "array">): Map<string, Snap> {
  const out = new Map<string, Snap>();
  for (const [name, kind] of kinds) {
    if (kind === "text") out.set(name, { kind, text: doc.getText(name).toString() });
    else if (kind === "map") out.set(name, { kind, map: new Map(Object.entries(doc.getMap(name).toJSON() as Record<string, unknown>)) });
    else out.set(name, { kind, items: doc.getArray(name).toJSON() as unknown[] });
  }
  return out;
}

function ensureRoots(doc: Y.Doc, kinds: Map<string, "text" | "map" | "array">): void {
  for (const [name, kind] of kinds) {
    if (kind === "text") doc.getText(name);
    else if (kind === "map") doc.getMap(name);
    else doc.getArray(name);
  }
}

interface Replace<T> {
  pos: number;
  del: number;
  ins: T;
}

/** The one replacement that turns a into b: the common prefix and the common suffix stay. */
function diffText(a: string, b: string): Replace<string> {
  let p = 0;
  const max = Math.min(a.length, b.length);
  while (p < max && a.charCodeAt(p) === b.charCodeAt(p)) p++;
  let s = 0;
  while (s < max - p && a.charCodeAt(a.length - 1 - s) === b.charCodeAt(b.length - 1 - s)) s++;
  return { pos: p, del: a.length - p - s, ins: b.slice(p, b.length - s) };
}

function diffArray(a: readonly unknown[], b: readonly unknown[]): Replace<unknown[]> {
  const same = (x: unknown, y: unknown): boolean => JSON.stringify(x) === JSON.stringify(y);
  let p = 0;
  const max = Math.min(a.length, b.length);
  while (p < max && same(a[p], b[p])) p++;
  let s = 0;
  while (s < max - p && same(a[a.length - 1 - s], b[b.length - 1 - s])) s++;
  return { pos: p, del: a.length - p - s, ins: b.slice(p, b.length - s) };
}

const isHigh = (u: number): boolean => u >= 0xd800 && u <= 0xdbff;
const isLow = (u: number): boolean => u >= 0xdc00 && u <= 0xdfff;

/**
 * Moves a bound that falls between the two code units of a surrogate pair to the code point boundary (14 [CLI-13]): `down`
 * moves it before the pair, otherwise after it. No update that the client authors splits a pair, which 05 [STATE-76] makes a
 * no-op for good.
 */
export function snapToCodePoint(s: string, i: number, down: boolean): number {
  if (i > 0 && i < s.length && isHigh(s.charCodeAt(i - 1)) && isLow(s.charCodeAt(i))) return down ? i - 1 : i + 1;
  return i;
}

/** Maps the user's edit, taken on `a`, onto `c`, where `c` is `a` with something taken out or replaced. */
function mapPos(edit: { pos: number; del: number }, rem: { pos: number; del: number; insLen: number }, cLen: number): { pos: number; del: number } {
  if (edit.pos + edit.del <= rem.pos) return { pos: edit.pos, del: edit.del };
  if (edit.pos >= rem.pos + rem.del) return { pos: Math.min(cLen, edit.pos + rem.insLen - rem.del), del: edit.del };
  // the edit overlaps what the rebuild took out: it goes where that was, and deletes nothing
  return { pos: Math.min(cLen, rem.pos), del: 0 };
}

/**
 * Re-applies to `view` the user's edits that were in no action: the difference of the roots of `before` (the old confirmed
 * document and every pending update, which the user had not yet edited beyond) to `edited` (the old view document), taken onto
 * the new view's content. The edits are the user's: the view document's `update` events record them as unsent.
 */
function reapply(view: Y.Doc, kinds: Map<string, "text" | "map" | "array">, before: Map<string, Snap>, edited: Map<string, Snap>): void {
  view.transact(() => {
    for (const [name, kind] of kinds) {
      const a = before.get(name);
      const b = edited.get(name);
      if (a === undefined || b === undefined || a.kind !== kind || b.kind !== kind) continue;
      if (kind === "text") {
        const at = (a as { text: string }).text;
        const bt = (b as { text: string }).text;
        const ytext = view.getText(name);
        const ct = ytext.toString();
        const edit = diffText(at, bt);
        if (edit.del === 0 && edit.ins === "") continue;
        const rem = diffText(at, ct);
        const m = mapPos(edit, { pos: rem.pos, del: rem.del, insLen: rem.ins.length }, ct.length);
        // the bounds move to code point boundaries: an insert, and the two ends of a delete
        const start = snapToCodePoint(ct, m.pos, true);
        const end = snapToCodePoint(ct, Math.min(ct.length, m.pos + m.del), false);
        if (end > start) ytext.delete(start, end - start);
        if (edit.ins !== "") ytext.insert(start, edit.ins);
      } else if (kind === "map") {
        const am = (a as { map: Map<string, unknown> }).map;
        const bm = (b as { map: Map<string, unknown> }).map;
        const ymap = view.getMap(name);
        for (const [k, v] of bm) {
          if (!am.has(k) || JSON.stringify(am.get(k)) !== JSON.stringify(v)) ymap.set(k, v);
        }
        for (const k of am.keys()) {
          if (!bm.has(k)) ymap.delete(k);
        }
      } else {
        const ai = (a as { items: unknown[] }).items;
        const bi = (b as { items: unknown[] }).items;
        const yarr = view.getArray(name);
        const ci = yarr.toJSON() as unknown[];
        const edit = diffArray(ai, bi);
        if (edit.del === 0 && edit.ins.length === 0) continue;
        const rem = diffArray(ai, ci);
        const m = mapPos(edit, { pos: rem.pos, del: rem.del, insLen: rem.ins.length }, ci.length);
        const end = Math.min(ci.length, m.pos + m.del);
        if (end > m.pos) yarr.delete(m.pos, end - m.pos);
        if (edit.ins.length > 0) yarr.insert(m.pos, edit.ins);
      }
    }
  });
}

// ---------------------------------------------------------------------------------------------------------------------
// One CRDT key

/** The documents of one CRDT key. */
export class CrdtKey {
  confirmed: Y.Doc = new Y.Doc();
  private view_: Y.Doc | null = null;
  /** The user's edits that are in no action yet: the `update` events of the view document, in order. */
  unsent: Uint8Array[] = [];
  /** True while the engine integrates an update into the view document: its transactions are not the user's. */
  integrating = false;
  roots: Roots | undefined;

  constructor(
    readonly key: string,
    private readonly store: CrdtStore,
  ) {}

  /** The view document, the one the user edits; null until the app opens the key. */
  get view(): Y.Doc | null {
    return this.view_;
  }

  /** True when there are edits in no action yet. */
  get hasUnsent(): boolean {
    return this.unsent.length > 0;
  }

  /** The user's unsent edits as one update, and they are then in an action (the caller authors it). */
  takeUnsent(): Uint8Array | undefined {
    if (this.unsent.length === 0) return undefined;
    const merged = this.unsent.length === 1 ? (this.unsent[0] as Uint8Array) : Y.mergeUpdates(this.unsent);
    this.unsent = [];
    return merged;
  }

  /** Integrates a confirmed update into both documents (14 [CLI-13]); the caller has read its verdict. */
  integrate(update: Uint8Array): void {
    Y.applyUpdate(this.confirmed, update, ENGINE_ORIGIN);
    if (this.view_ !== null) this.applyToView(update);
  }

  /** A confirmed base starts the key's document anew: the confirmed document is replaced, and the base is merged into the view. */
  replaceWithBase(base: Uint8Array): void {
    this.confirmed.destroy();
    this.confirmed = new Y.Doc();
    Y.applyUpdate(this.confirmed, base, ENGINE_ORIGIN);
    if (this.view_ !== null) this.applyToView(base);
  }

  private applyToView(update: Uint8Array): void {
    const v = this.view_ as Y.Doc;
    this.integrating = true;
    try {
      Y.applyUpdate(v, update, ENGINE_ORIGIN);
    } finally {
      this.integrating = false;
    }
  }

  private kinds(doc: Y.Doc): Map<string, "text" | "map" | "array"> {
    return rootKinds(doc, this.roots);
  }

  /** Opens the key for the user: a view document of the confirmed document and the pending updates, with a fresh Yjs client id. */
  open(pending: readonly Uint8Array[]): Y.Doc {
    if (this.view_ !== null) return this.view_;
    this.view_ = this.makeView(this.confirmed, pending);
    return this.view_;
  }

  private makeView(confirmed: Y.Doc, pending: readonly Uint8Array[]): Y.Doc {
    const v = new Y.Doc();
    v.clientID = this.store.freshClientId();
    this.integrating = true;
    try {
      Y.applyUpdate(v, Y.encodeStateAsUpdate(confirmed), ENGINE_ORIGIN);
      for (const u of pending) safeApply(v, u, ENGINE_ORIGIN);
    } finally {
      this.integrating = false;
    }
    const kinds = this.kinds(v);
    ensureRoots(v, kinds);
    v.on("update", (u: Uint8Array) => {
      if (this.integrating) return;
      this.unsent.push(u);
    });
    return v;
  }

  /**
   * Rebuilds the view document (14 [CLI-13]): a fresh document with a fresh Yjs client id, from the confirmed document and the
   * pending updates that remain, with the user's edits that were in no action re-applied once, as a diff. `pendingBefore` are the
   * pending updates as they were, `pendingAfter` those that remain; `confirmed`, when given, replaces the confirmed document
   * (a load with `resumed = false`, 09 [READ-18]). Returns false when the key has no view document, which has nothing to rebuild.
   */
  rebuild(pendingBefore: readonly Uint8Array[], pendingAfter: readonly Uint8Array[], confirmed?: Y.Doc): boolean {
    const old = this.view_;
    if (old === null) {
      if (confirmed !== undefined) {
        this.confirmed.destroy();
        this.confirmed = confirmed;
      }
      return false;
    }
    const kinds = this.kinds(old);
    // what the user had made of the document, and the document without those edits: the old confirmed document with every
    // pending update that the view held
    const base = new Y.Doc();
    Y.applyUpdate(base, Y.encodeStateAsUpdate(this.confirmed));
    for (const u of pendingBefore) safeApply(base, u);
    ensureRoots(base, kinds);
    const before = snapshot(base, kinds);
    const edited = snapshot(old, kinds);
    base.destroy();

    if (confirmed !== undefined) {
      this.confirmed.destroy();
      this.confirmed = confirmed;
    }
    this.unsent = [];
    old.destroy();
    this.view_ = this.makeView(this.confirmed, pendingAfter);
    reapply(this.view_, kinds, before, edited);
    return true;
  }

  /** Releases the documents. */
  destroy(): void {
    this.confirmed.destroy();
    this.view_?.destroy();
    this.view_ = null;
  }
}

// ---------------------------------------------------------------------------------------------------------------------
// The CRDT keys of one keyspace

export class CrdtStore {
  private readonly keys_ = new Map<string, CrdtKey>();

  constructor(private readonly random: Random) {}

  keys(): string[] {
    return [...this.keys_.keys()];
  }

  get(key: string): CrdtKey | undefined {
    return this.keys_.get(key);
  }

  /** A fresh Yjs client id: 53 random bits, never one that a document of the keyspace holds in its state vector (14 [CLI-13], 05 [STATE-76]). */
  freshClientId(): number {
    for (;;) {
      const id = newYjsClientId(this.random);
      let used = false;
      for (const k of this.keys_.values()) {
        if (k.confirmed.store.clients.has(id) || k.view?.store.clients.has(id) === true) {
          used = true;
          break;
        }
      }
      if (!used) return id;
    }
  }

  /** The document of a key, created empty when the key has none (a key that no confirmed state holds yet). */
  getOrCreate(key: string): CrdtKey {
    return this.ensure(key);
  }

  private ensure(key: string): CrdtKey {
    let k = this.keys_.get(key);
    if (k === undefined) {
      k = new CrdtKey(key, this);
      this.keys_.set(key, k);
    }
    return k;
  }

  /** The confirmed documents of a load (14 [CLI-2]): one per CRDT key that holds a base. Keys that hold a view document keep it (`rebuild`). */
  install(entries: readonly KeyEntry[], isCrdt: (e: KeyEntry) => boolean): Map<string, Y.Doc> {
    const docs = new Map<string, Y.Doc>();
    for (const e of entries) {
      if (e.deleted || e.redacted || e.isNull || !isCrdt(e)) continue;
      const d = new Y.Doc();
      try {
        Y.applyUpdate(d, e.value, ENGINE_ORIGIN);
      } catch {
        continue; // the fold's installEntry has refused the base as malformed
      }
      docs.set(e.key, d);
    }
    return docs;
  }

  /** Makes `docs` the confirmed documents, keeping the keys that hold a view document; returns the keys whose view must be rebuilt. */
  replaceConfirmed(docs: Map<string, Y.Doc>): string[] {
    const rebuild: string[] = [];
    for (const [key, k] of [...this.keys_]) {
      if (k.view === null) {
        k.destroy();
        this.keys_.delete(key);
      } else {
        rebuild.push(key);
      }
    }
    for (const [key, d] of docs) {
      const k = this.keys_.get(key);
      if (k === undefined) {
        const nk = new CrdtKey(key, this);
        nk.confirmed = d;
        this.keys_.set(key, nk);
      }
      // a key that holds a view document gets its new confirmed document in `rebuild`
    }
    return rebuild;
  }

  /** Integrates the updates of a record that the fold has applied (14 [CLI-13]): the verdict decides, and a no-op for good is never integrated. */
  apply(plan: readonly CrdtPlanItem[]): void {
    for (const item of plan) {
      if (item.verdict === "NOOP_FOR_GOOD") continue; // 05 [STATE-76]: nothing of it is kept, then or later
      const k = this.ensure(item.key);
      if (item.base) k.replaceWithBase(item.update);
      else k.integrate(item.update);
    }
  }

  /** Drops the documents of every key that the state no longer holds as a CRDT key that has a value (a delete, a subtree deletion, the redacted state). */
  reconcile(st: ReplicaState): string[] {
    const dropped: string[] = [];
    for (const [key, k] of [...this.keys_]) {
      const row = st.key(key);
      if (row === undefined || row.rule !== "crdt" || row.state !== StateValue) {
        // a view document that the user still holds stays readable; the key has no confirmed document any more
        if (k.view === null) k.destroy();
        this.keys_.delete(key);
        dropped.push(key);
      }
    }
    return dropped;
  }

  destroy(): void {
    for (const k of this.keys_.values()) k.destroy();
    this.keys_.clear();
  }
}
