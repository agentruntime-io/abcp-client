// What the engine does when it learns of a redaction (14 [CLI-76]): every local copy of the values of the erased keys is replaced, in the same local transaction that moves the
// persisted cursor to or past the `REDACTED` record, or the load, that carried the news, so that a crash repeats the purge and never skips it.
//
// The engine learns of a redaction in three ways, and each names the erased keys:
//   - a `REDACTED` record (`keys` and `erased_keys`, 06 [LOG-44]), through `redactionApplied`;
//   - a load entry with the redaction mark (09 [READ-13]), a tombstone that a redaction created or an entry in the redacted state, from any load, a widening one included;
//   - a key listed in `LoadStateResponse.redactions` of a load that answers a resume check (09 [READ-100]).
//
// The copies, and what is done to each (the persisted replica, whose pages and records are rewritten, is `replica-store.ts`'s):
//   - the replica in memory: the fold of the `REDACTED` record deletes the keys, or the load replaced the state;
//   - the journal, the dead-letter store, the export-hook entries and the unacknowledged rejections: the content of each entry that holds a change of an erased key is blanked
//     (`erase.ts`), the entry keeps its place and gets the mark `redacted`, and the app is told (`dead-lettered` for a dead-letter entry and for an export entry, 14 §14.5);
//   - the pending actions and the drafts are the user's new writes and stay (14 [CLI-76]); a CRDT key's pending updates leave the view and go to its draft, and the view document
//     is built again from the confirmed document alone (14 [CLI-13], [CLI-49]);
//   - the cached bytes of blobs that only erased values named (below);
//   - the cached pages of the query layer: the change feed tells it that each erased key changed, and a load tells it with `reloaded`.
//
// READING (14 [CLI-76], R1 design §4): "cached pages and blobs" is read as the persisted replica's pages and the bytes of blobs that the client keeps (the cache of downloads and the
// bytes that it staged for actions). A blob is "only erased values'" when the bytes of an erased value, or an action that held one, name it (its id is in the value, or in the action's
// `blob_refs`) and nothing that remains does: no present key's value, no pending action, no copy of an action that holds a change of another key, no record that the client keeps.
// The engine cannot know more of a value than its bytes, so a blob that an erased value named by other means stays; one that is dropped by mistake is one download again.
//
// SPEC-QUESTION (14 [CLI-76], [CLI-41], [CLI-28]): the unacknowledged rejections (`RejectionRecord`, which keep the content of an action that ended) are no store that [CLI-76] names, and
// they hold the same copy of the action as a dead-letter entry. Reading A (taken): they are erased like the dead-letter store, with no event. Reading B: they are left, as the list of
// [CLI-76] is taken to be complete.
//
// SPEC-QUESTION (14 [CLI-76], [CLI-53]): a journal entry whose content was erased is no longer the whole action. A lost spend action is resent from its journal entry ([CLI-64]); the engine
// does not resend an erased one, and ends it as an action of unknown outcome ([CLI-83]) instead.

import * as Y from "yjs";
import { ChangeKind } from "../gen/abpc/v2/types_pb.js";
import type { KeyRedaction } from "../gen/abpc/v2/types_pb.js";
import { StateValue } from "../rules/fold/index.js";
import { bytesEqual } from "../rules/bytes.js";
import { resendDelay } from "./backoff.js";
import { decodeContent } from "./entry.js";
import { CanonError } from "./entry.js";
import { EndingLayer } from "./endings.js";
import { eraseContent, type Redaction, type Use } from "./erase.js";
import type { Follow } from "./follow.js";
import { draftKey, type DeadLetterEntry, type DraftEntry, type ExportEntry, type JournalEntry, type RejectionRecord, type StorageOp, type StoredLight, type StoredState } from "./storage.js";

/** What redactions of a keyspace have erased and the local transaction has not yet made durable. */
export interface PendingErasure {
  redactions: Redaction[];
  /** The entries of the stores that were replaced in memory, by id: the operations are made from the engine's copies when the transaction is, so that an acknowledgement in between is not undone. */
  deadLetters: Set<string>;
  exports: Set<string>;
  journal: Set<string>;
  rejections: Set<string>;
  /** The drafts (`draftKey`) that the erasure wrote. */
  drafts: Set<string>;
  /** The reset marks of CRDT keys changed (`crdtResets`). */
  resets: boolean;
  /** For the judgement of blobs: the bytes of erased values, the blobs that actions with erased values name, and what still names a blob. */
  erased: Uint8Array[];
  refsErased: Set<string>;
  other: Uint8Array[];
  refsOther: Set<string>;
  /** The failed attempts to make it durable, the exponent of the client backoff before the next. */
  attempts: number;
}

export function newErasure(): PendingErasure {
  return { redactions: [], deadLetters: new Set(), exports: new Set(), journal: new Set(), rejections: new Set(), drafts: new Set(), resets: false, erased: [], refsErased: new Set(), other: [], refsOther: new Set(), attempts: 0 };
}

/** The erased keys of every redaction of the erasure. */
export function erasedKeys(er: PendingErasure): Set<string> {
  const out = new Set<string>();
  for (const r of er.redactions) for (const k of r.keys) out.add(k);
  return out;
}

/** The run keys that a `REDACTED` record set, with the values that hold the blanked start documents. */
export function erasedRunKeys(er: PendingErasure): Map<string, Uint8Array> {
  const out = new Map<string, Uint8Array>();
  for (const r of er.redactions) for (const [k, v] of r.runKeys) out.set(k, v);
  return out;
}

/** The redaction of a load: the keys of its entries that carry the mark and of its `redactions`, whatever the load's entry for them (14 [CLI-76], 09 [READ-13], [READ-100]). */
export function loadRedaction(keyspaceId: string, marked: Iterable<string>, listed: readonly KeyRedaction[]): Redaction | undefined {
  const keys = new Set<string>(marked);
  for (const l of listed) keys.add(l.key);
  if (keys.size === 0) return undefined;
  return { keyspaceId, keys, seq: undefined, runKeys: new Map(), blankedIntents: new Set(), source: listed.length > 0 ? "list" : "mark" };
}

const key = (ks: string, k: string): string => `${ks}\u0000${k}`;

export abstract class ErasureLayer extends EndingLayer {
  /**
   * The first ordinal from which the view of a CRDT key takes pending updates after a redaction reset (14 [CLI-13]: "the key's pending updates authored before the reset are not among
   * the remaining pending updates"): by keyspace and key. Kept with the stored state (`crdtResetsOp`), so that a restart does not put them back in the view.
   */
  protected crdtResets = new Map<string, number>();

  /** The name of a stored fact that belongs to the principal (`interest.ts`). */
  protected abstract metaKey(name: "followed" | "revoked" | "sagas" | "crdtResets"): string;
  /** The ids of the blobs of the keyspace that the engine holds bytes of: the cache of downloads and the staged bytes (`blobs.ts`). */
  protected abstract blobIdsHeld(keyspaceId: string): Promise<Set<string>>;
  /** Drops the cached and the staged bytes of the blobs, but those that a pending action names (`blobs.ts`). */
  protected abstract blobsErased(keyspaceId: string, ids: ReadonlySet<string>): Promise<void>;

  protected resetRuntime(): void {
    super.resetRuntime();
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // CRDT resets (14 [CLI-13])

  protected crdtResetOrdinal(keyspaceId: string, k: string): number {
    return this.crdtResets.get(key(keyspaceId, k)) ?? 0;
  }

  /** The reset marks that a stored state names, at an engine's start. */
  protected restoreCrdtResets(v: string | number | bigint | undefined): void {
    this.crdtResets.clear();
    if (typeof v !== "string") return;
    try {
      for (const x of JSON.parse(v) as { ks: string; key: string; ordinal: number }[]) {
        if (typeof x.ks === "string" && typeof x.key === "string" && typeof x.ordinal === "number") this.crdtResets.set(key(x.ks, x.key), x.ordinal);
      }
    } catch {
      // not what this engine wrote: no key is reset because of it
    }
  }

  /** The operation that keeps the reset marks. A mark that no action in the outbox is older than is of no use any more, and is not kept. */
  protected crdtResetsOp(): StorageOp {
    const live: { ks: string; key: string; ordinal: number }[] = [];
    for (const [id, ordinal] of [...this.crdtResets]) {
      const at = id.indexOf("\u0000");
      const ks = id.slice(0, at);
      const k = id.slice(at + 1);
      const older = this.entriesOf(ks).some((e) => e.ordinal < ordinal && this.writesCrdt(e.content, k));
      if (older) live.push({ ks, key: k, ordinal });
      else this.crdtResets.delete(id);
    }
    return { op: "setMeta", key: this.metaKey("crdtResets"), value: JSON.stringify(live) };
  }

  private writesCrdt(content: Uint8Array, k: string): boolean {
    try {
      return decodeContent(content).changes.some((c) => c.key === k && (c.kind === ChangeKind.CRDT_UPDATE || c.kind === ChangeKind.OVERRIDE));
    } catch (e) {
      if (e instanceof CanonError) return false;
      throw e;
    }
  }

  /**
   * The redaction reset of CRDT keys (14 [CLI-13], [CLI-49], [CLI-76]): for each key, the pending updates of the outbox, which were authored before the reset, and the user's edits
   * that are in no action yet become the key's draft, and the pending updates leave the view for good; the view document, when the key has one, is built again from the confirmed
   * document alone, re-applying no edit, and `rebuilt` is emitted once. The actions stay in the outbox until the server ends them with `STALE{DOC_RESET}`. `docs` are the confirmed
   * documents of a load that replaced the state; without them the key's confirmed document is the one that it has. Returns the keys whose view was rebuilt.
   */
  protected resetCrdtKeys(f: Follow, keys: Iterable<string>, docs?: ReadonlyMap<string, Y.Doc>): Set<string> {
    const er = this.erasureOf(f);
    const rebuilt = new Set<string>();
    for (const k of [...keys].sort()) {
      const ck = f.crdt.get(k);
      const pending = this.pendingUpdates(f.id, k, new Set());
      const unsent = ck?.unsent ?? [];
      const view = ck?.view ?? null;
      if (pending.length === 0 && unsent.length === 0 && view === null) continue;
      const updates = [...pending, ...unsent];
      if (updates.length > 0) this.putDraft(f, k, updates, er);
      if (pending.length > 0) {
        this.crdtResets.set(key(f.id, k), this.nextOrdinal);
        er.resets = true;
      }
      if (ck !== undefined && view !== null) {
        const confirmed = docs === undefined ? undefined : (docs.get(k) ?? new Y.Doc());
        if (ck.rebuild(pending, [], confirmed, false)) {
          rebuilt.add(k);
          this.emit({ type: "rebuilt", keyspaceId: f.id, key: k, cause: "REDACTED" });
        }
      }
    }
    return rebuilt;
  }

  /** Adds Yjs updates to the key's draft (14 [CLI-49], [CLI-59]): the ones that it holds already are not added again. */
  private putDraft(f: Follow, k: string, updates: readonly Uint8Array[], er: PendingErasure): void {
    const id = draftKey({ keyspaceId: f.id, key: k, author: this.author });
    const have = this.drafts.get(id);
    const known = have?.updates ?? [];
    const add = updates.filter((u) => !known.some((h) => bytesEqual(h, u)));
    if (add.length === 0 && have !== undefined) return;
    const draft: DraftEntry = { keyspaceId: f.id, key: k, author: { ...this.author }, updates: [...known, ...add], reason: have?.reason ?? "REDACTED", atMs: this.serverNow() };
    this.drafts.set(id, draft);
    er.drafts.add(id);
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // The copies of the stores

  protected erasureOf(f: Follow): PendingErasure {
    return (f.erasure ??= newErasure());
  }

  /** The values of the keys in the confirmed state, as they are before a redaction takes them out: for the judgement of blobs. */
  protected valuesOfKeys(f: Follow, keys: Iterable<string>): Uint8Array[] {
    const out: Uint8Array[] = [];
    for (const k of keys) {
      const row = f.state.key(k);
      if (row !== undefined && row.state === StateValue && row.value.length > 0) out.push(row.value);
    }
    return out;
  }

  /**
   * A redaction reached the confirmed state (a `REDACTED` record was folded, or a load installed): the stores' copies are replaced in memory now, and made durable with the next
   * transaction of the keyspace's image (`replica-store.ts`). `before` are the values that the keys held in the confirmed state. Emits the events of the entries that changed.
   */
  protected noteRedaction(f: Follow, r: Redaction, before: readonly Uint8Array[]): void {
    const er = this.erasureOf(f);
    er.redactions.push(r);
    for (const b of before) er.erased.push(b);
    this.eraseHeld(f, r, er);
  }

  /** A `REDACTED` record was applied (14 [CLI-76]): the stores' copies, and the CRDT keys that it erased. */
  protected redactionApplied(f: Follow, r: Redaction, before: readonly Uint8Array[]): void {
    f.touch(r.keys); // the change feed: a key that the record lists reads differently now, listed in its DELETEs or not
    this.noteRedaction(f, r, before);
    this.resetCrdtKeys(f, r.keys);
  }

  private noteUse(er: PendingErasure, u: Use): void {
    if (u.erasedAny) {
      for (const id of u.blobRefs) er.refsErased.add(id);
      for (const b of u.erased) er.erased.push(b);
    }
    if (u.otherAny) {
      for (const id of u.blobRefs) er.refsOther.add(id);
      for (const b of u.other) er.other.push(b);
    }
  }

  private eraseHeld(f: Follow, r: Redaction, er: PendingErasure): void {
    const told: { mutationId: string; keyspaceId: string; reason: string }[] = [];
    for (const d of this.authoringOrder(this.deadLetters.values())) {
      if (d.keyspaceId !== f.id) continue;
      const x = eraseContent(d.content, r.keys, r.runKeys);
      if (x === undefined) continue;
      this.noteUse(er, x.use);
      if (!x.changed) continue;
      const next: DeadLetterEntry = { ...d, content: x.content, redacted: true };
      this.deadLetters.set(d.mutationId, next);
      er.deadLetters.add(d.mutationId);
      told.push({ mutationId: d.mutationId, keyspaceId: d.keyspaceId, reason: d.reason });
    }
    for (const e of this.authoringOrder(this.exports.values())) {
      if (e.keyspaceId !== f.id) continue;
      const x = eraseContent(e.content, r.keys, r.runKeys);
      if (x === undefined) continue;
      this.noteUse(er, x.use);
      if (!x.changed) continue;
      const next: ExportEntry = { ...e, content: x.content, redacted: true };
      this.exports.set(e.mutationId, next);
      er.exports.add(e.mutationId);
      told.push({ mutationId: e.mutationId, keyspaceId: e.keyspaceId, reason: e.reason });
    }
    for (const j of [...this.journal.values()]) {
      if (j.keyspaceId !== f.id) continue;
      const x = eraseContent(j.content, r.keys, r.runKeys);
      if (x === undefined) continue;
      this.noteUse(er, x.use);
      if (!x.changed) continue;
      const next: JournalEntry = { ...j, content: x.content, redacted: true };
      this.journal.set(j.mutationId, next);
      er.journal.add(j.mutationId);
    }
    for (const q of [...this.rejections.values()]) {
      if (q.keyspaceId !== f.id) continue;
      const x = eraseContent(q.content, r.keys, r.runKeys);
      if (x === undefined) continue;
      this.noteUse(er, x.use);
      if (!x.changed) continue;
      const next: RejectionRecord = { ...q, content: x.content, redacted: true };
      this.rejections.set(q.mutationId, next);
      er.rejections.add(q.mutationId);
    }
    // 14 [CLI-76], §14.5: "report this change of the entry to the app through its store and with `dead-lettered`"
    for (const t of told) this.emit({ type: "dead-lettered", mutationId: t.mutationId, keyspaceId: t.keyspaceId, reason: t.reason, cause: "REDACTED" });
  }

  /** The operations of the erasure: the replaced entries and drafts, the reset marks, and the erasure mark that makes a storage with a history of commits rewrite it. */
  protected erasureOps(f: Follow, er: PendingErasure): StorageOp[] {
    const ops: StorageOp[] = [];
    for (const id of er.deadLetters) {
      const e = this.deadLetters.get(id);
      if (e !== undefined) ops.push({ op: "putDeadLetter", entry: e });
    }
    for (const id of er.exports) {
      const e = this.exports.get(id);
      if (e !== undefined) ops.push({ op: "putExport", entry: e });
    }
    for (const id of er.journal) {
      const e = this.journal.get(id);
      if (e !== undefined) ops.push({ op: "putJournal", entry: e });
    }
    for (const id of er.rejections) {
      const e = this.rejections.get(id);
      if (e !== undefined) ops.push({ op: "putRejection", entry: e });
    }
    for (const id of er.drafts) {
      const e = this.drafts.get(id);
      if (e !== undefined) ops.push({ op: "putDraft", entry: e });
    }
    if (er.resets) ops.push(this.crdtResetsOp());
    ops.push({ op: "erase", keyspaceId: f.id });
    return ops;
  }

  /**
   * The copies that the partition holds for other principals (a shared device, 14 [CLI-59]): the engine mirrors the entries of its own principal only, and a redaction erases the keyspace's
   * values for everyone. Their entries are replaced in the storage alone.
   */
  private async eraseOthers(f: Follow, er: PendingErasure): Promise<StorageOp[]> {
    let st: StoredState | StoredLight;
    try {
      st = this.storage.loadLight !== undefined ? await this.storage.loadLight() : await this.storage.load();
    } catch (e) {
      this.noteInternalError(e);
      return [];
    }
    const keys = erasedKeys(er);
    const runKeys = erasedRunKeys(er);
    const mine = (a: { issuer: string; sub: string } | undefined): boolean => a === undefined || (a.issuer === this.author.issuer && a.sub === this.author.sub);
    const ops: StorageOp[] = [];
    const redo = <T extends { keyspaceId: string; content: Uint8Array; author?: { issuer: string; sub: string } | undefined }>(rows: readonly T[], put: (e: T) => StorageOp): void => {
      for (const e of rows) {
        if (e.keyspaceId !== f.id || mine(e.author)) continue;
        const x = eraseContent(e.content, keys, runKeys);
        if (x?.changed === true) ops.push(put({ ...e, content: x.content, redacted: true } as T));
      }
    };
    redo(st.deadLetters, (entry) => ({ op: "putDeadLetter", entry: entry as unknown as DeadLetterEntry }));
    redo(st.exports, (entry) => ({ op: "putExport", entry: entry as unknown as ExportEntry }));
    redo(st.journal, (entry) => ({ op: "putJournal", entry: entry as unknown as JournalEntry }));
    redo(st.rejections, (entry) => ({ op: "putRejection", entry: entry as unknown as RejectionRecord }));
    return ops;
  }

  /** Commits the erasure with the operations that go in the same transaction. Throws when the storage cannot. */
  protected async erasureTx(f: Follow, er: PendingErasure, extra: readonly StorageOp[]): Promise<void> {
    await this.tx([...this.erasureOps(f, er), ...(await this.eraseOthers(f, er)), ...extra]);
  }

  /**
   * A transaction of the erasure failed and applied nothing: the keyspace's image no longer follows the state, and what the storage holds of the erased values is made good again with the
   * client backoff, until it is (14 [CLI-76]: "so that a crash repeats the purge and never skips it"; the persisted cursor never passes the record, since the image is gone with the
   * transaction that failed). The image is deleted with the entries, so that a restart finds nothing of the erased values in it.
   */
  protected retryErasure(f: Follow, er: PendingErasure): void {
    if (this.role !== "flusher" || this.stopped) return;
    f.image = { ok: false, seqs: new Set(), headMs: 0 };
    this.after(resendDelay(er.attempts++, undefined, this.random), async () => {
      if (this.role !== "flusher") return;
      try {
        await this.judgeBlobs(f, er); // FBA-28: the staged bytes go before the transaction, as on the first attempt
        await this.erasureTx(f, er, [{ op: "deleteReplica", keyspaceId: f.id }]);
      } catch (e) {
        this.noteInternalError(e);
        this.retryErasure(f, er);
      }
    });
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Blobs

  /**
   * Judges the blobs that the engine holds bytes of (14 [CLI-76], the reading in the header): those that erased values named and nothing that remains names are dropped.
   *
   * FBA-28: run before the transaction that moves the persisted cursor past the record (14 [CLI-76] "or before that advance, so that a crash repeats the purge and never skips
   * it"): the stash of staged bytes is no part of that transaction, and the candidates are known only in memory. Throws when the stash cannot be read or cannot erase: the caller
   * then does not move the cursor, and tries again. A crash before the transaction leaves the cursor below the record, so the restart applies it again and judges the blobs again.
   * It only frees what is held, so running it before the erasure is durable loses nothing.
   */
  protected async judgeBlobs(f: Follow, er: PendingErasure): Promise<void> {
    const held = await this.blobIdsHeld(f.id);
    if (held.size === 0) return;
    const dec = new TextDecoder();
    const erasedText = er.erased.map((b) => dec.decode(b)).join("\n");
    const candidates = [...held].filter((id) => er.refsErased.has(id) || erasedText.includes(id));
    if (candidates.length === 0) return;
    // what still names a blob: the values that remain in the confirmed state, the pending actions (they stay), the copies of actions that hold a change of another key, the records that the
    // engine keeps
    const remains: string[] = er.other.map((b) => dec.decode(b));
    const refs = new Set(er.refsOther);
    for (const k of f.state.keyNames()) {
      const row = f.state.key(k);
      if (row !== undefined && row.state === StateValue && row.value.length > 0) remains.push(dec.decode(row.value));
    }
    for (const e of [...this.entries.values(), ...this.lockedEntries]) {
      if (e.keyspaceId !== f.id && e.authoredIn !== f.id) continue;
      try {
        const m = decodeContent(e.content);
        for (const id of m.blobRefs) refs.add(id);
        for (const c of m.changes) if (c.kind !== ChangeKind.DELETE) remains.push(dec.decode(c.update.length > 0 ? c.update : c.value));
      } catch (err) {
        if (!(err instanceof CanonError)) throw err;
      }
    }
    const text = remains.join("\n");
    const gone = new Set(candidates.filter((id) => !refs.has(id) && !text.includes(id)));
    if (gone.size > 0) await this.blobsErased(f.id, gone);
  }
}
