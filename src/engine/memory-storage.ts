// The in-memory implementation of the outbox storage. It behaves as a durable store does, as far as a test can tell: it keeps
// copies (nothing the engine holds aliases what it stored), applies a commit whole or not at all, and can be made to fail a
// commit or to bound a store (14 [CLI-21], [CLI-84]).
//
// It is the engine's default and reports `mode: "memory"`, which the sync status shows, so that no one mistakes it for a durable
// store (14 [CLI-21]: a client MUST NOT fall back to memory-only storage silently).
//
// TODO(later): IndexedDB (browsers) and SQLite (node, react native) implementations of OutboxStorage.

import { StorageError } from "./errors.js";
import {
  StoreFull,
  type DeadLetterEntry,
  type DraftEntry,
  type ExportEntry,
  type JournalEntry,
  type OutboxEntry,
  type OutboxStorage,
  type RejectionRecord,
  type StorageOp,
  type StoredState,
  type StoreName,
} from "./storage.js";

export interface MemoryStorageOptions {
  /** Bounds of 14 [CLI-84]; an unset bound is none. */
  maxOutbox?: number;
  maxDeadLetters?: number;
  maxJournal?: number;
  maxExports?: number;
}

export class MemoryStorage implements OutboxStorage {
  readonly mode = "memory" as const;
  private entries = new Map<string, OutboxEntry>();
  private deadLetters = new Map<string, DeadLetterEntry>();
  private journal = new Map<string, JournalEntry>();
  private exports = new Map<string, ExportEntry>();
  private rejections = new Map<string, RejectionRecord>();
  private drafts = new Map<string, DraftEntry>();
  private meta = new Map<string, string | number | bigint>();

  /** A test hook: the next n commits fail with a StorageError and apply nothing (14 [CLI-21]). */
  failNext = 0;
  /** A test hook: every commit, in order, as it was applied. */
  readonly log: StorageOp[][] = [];

  constructor(readonly options: MemoryStorageOptions = {}) {}

  load(): Promise<StoredState> {
    const meta: Record<string, string | number | bigint> = {};
    for (const [k, v] of this.meta) meta[k] = v;
    return Promise.resolve({
      entries: [...this.entries.values()].map((e) => structuredClone(e)),
      deadLetters: [...this.deadLetters.values()].map((e) => structuredClone(e)),
      journal: [...this.journal.values()].map((e) => structuredClone(e)),
      exports: [...this.exports.values()].map((e) => structuredClone(e)),
      rejections: [...this.rejections.values()].map((e) => structuredClone(e)),
      drafts: [...this.drafts.values()].map((e) => structuredClone(e)),
      meta,
    });
  }

  commit(ops: readonly StorageOp[]): Promise<void> {
    if (this.failNext > 0) {
      this.failNext--;
      return Promise.reject(new StorageError("the transaction failed"));
    }
    // apply to copies of the maps, check the bounds, then swap: all or none
    const entries = new Map(this.entries);
    const deadLetters = new Map(this.deadLetters);
    const journal = new Map(this.journal);
    const exports = new Map(this.exports);
    const rejections = new Map(this.rejections);
    const drafts = new Map(this.drafts);
    const meta = new Map(this.meta);
    for (const op of ops) {
      switch (op.op) {
        case "putEntry":
          entries.set(op.entry.mutationId, structuredClone(op.entry));
          break;
        case "deleteEntry":
          entries.delete(op.mutationId);
          break;
        case "putDeadLetter":
          deadLetters.set(op.entry.mutationId, structuredClone(op.entry));
          break;
        case "ackDeadLetter":
          deadLetters.delete(op.mutationId);
          break;
        case "putJournal":
          journal.set(op.entry.mutationId, structuredClone(op.entry));
          break;
        case "deleteJournal":
          journal.delete(op.mutationId);
          break;
        case "putExport":
          exports.set(op.entry.mutationId, structuredClone(op.entry));
          break;
        case "ackExport":
          exports.delete(op.mutationId);
          break;
        case "putRejection":
          rejections.set(op.entry.mutationId, structuredClone(op.entry));
          break;
        case "ackRejection":
          rejections.delete(op.mutationId);
          break;
        case "putDraft":
          drafts.set(`${op.entry.keyspaceId}\u0000${op.entry.key}`, structuredClone(op.entry));
          break;
        case "deleteDraft":
          drafts.delete(`${op.keyspaceId}\u0000${op.key}`);
          break;
        case "setMeta":
          meta.set(op.key, op.value);
          break;
      }
    }
    const o = this.options;
    const check = (name: StoreName, before: number, after: number, max: number | undefined): void => {
      if (max !== undefined && after > before && after > max) throw new StoreFull(name);
    };
    try {
      check("outbox", this.entries.size, entries.size, o.maxOutbox);
      check("deadLetters", this.deadLetters.size, deadLetters.size, o.maxDeadLetters);
      check("journal", this.journal.size, journal.size, o.maxJournal);
      check("exports", this.exports.size, exports.size, o.maxExports);
    } catch (e) {
      return Promise.reject(e);
    }
    this.entries = entries;
    this.deadLetters = deadLetters;
    this.journal = journal;
    this.exports = exports;
    this.rejections = rejections;
    this.drafts = drafts;
    this.meta = meta;
    this.log.push(ops.map((x) => structuredClone(x)));
    return Promise.resolve();
  }

  full(): StoreName[] {
    const o = this.options;
    const out: StoreName[] = [];
    if (o.maxOutbox !== undefined && this.entries.size >= o.maxOutbox) out.push("outbox");
    if (o.maxDeadLetters !== undefined && this.deadLetters.size >= o.maxDeadLetters) out.push("deadLetters");
    if (o.maxJournal !== undefined && this.journal.size >= o.maxJournal) out.push("journal");
    if (o.maxExports !== undefined && this.exports.size >= o.maxExports) out.push("exports");
    return out;
  }

  // -- test conveniences --

  /** The stored outbox entry, or undefined. */
  entry(mutationId: string): OutboxEntry | undefined {
    const e = this.entries.get(mutationId);
    return e === undefined ? undefined : structuredClone(e);
  }
}
