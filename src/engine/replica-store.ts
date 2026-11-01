// The persisted replica and the interest of a followed keyspace (14 §14.10).
//
//  - [CLI-2]: the client MAY resume from its own persisted confirmed state, with that state's cursor and epoch. The persisted
//    replica of a keyspace is the pages of the load that began it, exactly as the server served them, and the records applied since
//    (`ReplicaImage`). A restart installs the pages and folds the records again by the fold of C1, the same code that applied them,
//    so the state that it restores is the state that the server's fold gives. (A client never builds a CRDT base, 05 [STATE-74]: it
//    would have to be one that every party's fold reads alike, and an update that Yjs encodes from a collected document is not always
//    one.) The image is a cache of the server's data: the outbox is the durable truth, so an image that cannot be written, read or
//    folded is dropped, and the keyspace is loaded again;
//  - [CLI-3], [CLI-71]: a restart resumes from the persisted watermark, with the hash of the record at it (`after_record_hash`);
//  - [CLI-71], [CLI-72]: the interest prefixes of a keyspace, the keys that the state holds (those under its interest), and a
//    widening. The server delivers whole records, so a record's changes to keys outside the interest arrive too: the fold applies the
//    record whole and the keys outside the interest are dropped at once ("MUST NOT present keys outside its interest as current",
//    09 [READ-26]); a deleted marker's tombstone stays, since the ancestor test of 05 [STATE-38] needs it.

import { ChangeKind, KeyspaceLifecycle, type KeyEntry, type Record as RecordMessage } from "../gen/abpc/v2/types_pb.js";
import type { LoadStateResponse } from "../gen/abpc/v2/sync_pb.js";
import { applyRecord, FoldError, installEntry, isReservedKey, ReplicaState, VersionTopology, type Anomaly } from "../rules/fold/index.js";
import { isSegment } from "../rules/fold/template.js";
import { EndingLayer } from "./endings.js";
import { planCrdt, appliedIndexes, type CrdtPlanItem } from "./crdt-view.js";
import { Follow } from "./follow.js";
import type { ReplicaHead, ReplicaImage } from "./storage.js";

/** Whether the key is under one of the interest prefixes (09 [READ-22]): equal to it, or below it. An empty interest is the whole keyspace. */
export function inInterest(interest: readonly string[], key: string): boolean {
  if (interest.length === 0) return true;
  for (const p of interest) if (key === p || key.startsWith(`${p}/`)) return true;
  return false;
}

/** Whether the prefix `p` is inside what the interest `interest` covers (an empty interest covers every prefix). */
export function covers(interest: readonly string[], p: string): boolean {
  return inInterest(interest, p);
}

/**
 * The refusal of an interest that no server accepts (09 [READ-22]): a prefix is a key path of one or more whole segments (03 §3.3), with
 * no trailing `/`; and no more than `max` prefixes. Returns the reason, or undefined.
 */
export function checkInterest(prefixes: readonly string[], max: number): string | undefined {
  if (prefixes.length > max) return `${prefixes.length} interest prefixes, over the limit of ${max} (09 [READ-22])`;
  for (const p of prefixes) {
    if (p.length === 0 || p.length > 512) return `the interest prefix "${p}" is empty or too long (09 [READ-22])`;
    const segs = p.split("/");
    if (segs.length > 16 || !segs.every(isSegment)) return `the interest prefix "${p}" is not a key path of whole segments with no trailing "/" (09 [READ-22])`;
  }
  return undefined;
}

/** The keys that a record's applied changes name (05 [STATE-34]). */
function appliedKeys(rec: RecordMessage): string[] {
  const out: string[] = [];
  for (const i of appliedIndexes(rec)) {
    const c = rec.changes[i];
    if (c !== undefined) out.push(c.key);
  }
  return out;
}

export abstract class ReplicaStoreLayer extends EndingLayer {
  /** Whether the engine keeps the persisted replica (§14.10). */
  protected persistReplica = true;
  /** The most records that a persisted tail may hold at a restart: a longer one is dropped and the keyspace is loaded again. */
  protected replicaTailLimit = 10_000;

  // ---------------------------------------------------------------------------------------------------------------------
  // Interest (14 [CLI-71])

  protected isInInterest(f: Follow, key: string): boolean {
    return inInterest(f.interest, key);
  }

  /** Drops the present keys among `keys` that are outside the keyspace's interest, and the documents of the CRDT keys among them. */
  protected pruneOutside(f: Follow, keys: Iterable<string>): void {
    if (f.interest.length === 0) return;
    const st = f.state;
    for (const k of keys) {
      if (st.key(k) !== undefined && !this.isInInterest(f, k)) st.dropKey(k);
    }
  }

  /** Drops every present key that is outside the interest (a narrowing, 14 [CLI-72]: "MAY remove keys outside the new interest"). */
  protected pruneAllOutside(f: Follow): void {
    if (f.interest.length === 0) return;
    this.pruneOutside(f, f.state.keyNames());
    f.crdt.reconcile(f.state);
    f.invalidate();
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Folding a record

  /**
   * Applies a record to the keyspace's confirmed state and its CRDT documents: the delivery watermark's jump over records that the
   * client does not hold (09 [READ-42]), the fold (14 [CLI-4]), the interest filter (14 [CLI-71]) and the confirmed documents (14 [CLI-13]).
   * Live delivery and the replay of a persisted tail both do exactly this. Throws the fold's `FoldError`; the state is then as it was.
   */
  protected foldRecord(f: Follow, rec: RecordMessage): { plan: CrdtPlanItem[]; anomalies: Anomaly[] } {
    const st = f.state;
    // 09 [READ-42]: every record touches the interest; a watermark that passes seqs the client does not hold says they do not
    if (rec.seq > st.seq + 1n) st.setCursor(rec.seq - 1n, f.logEpoch, st.manifestVersion);
    let plan: CrdtPlanItem[] = planCrdt(st, this.manifests, rec);
    const anomalies: Anomaly[] = [];
    applyRecord(st, this.manifests, rec, { supported: (feature) => this.features.includes(feature), report: (a) => anomalies.push(a) });
    // the change feed (view-feed.ts): every key that the record names may read differently now, and below a `DELETE`d key the whole subtree (05 [STATE-43])
    f.touch(
      rec.changes.map((c) => c.key),
      rec.changes.filter((c) => c.kind === ChangeKind.DELETE).map((c) => c.key),
    );
    if (f.interest.length > 0) {
      this.pruneOutside(f, appliedKeys(rec));
      plan = plan.filter((item) => this.isInInterest(f, item.key));
    }
    // the confirmed documents of the CRDT keys that the record changed (14 [CLI-13])
    f.crdt.apply(plan);
    f.crdt.reconcile(st);
    return { plan, anomalies };
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Persisting (14 [CLI-2], [CLI-74])

  protected headOf(f: Follow): ReplicaHead {
    return {
      logEpoch: f.logEpoch,
      seq: f.state.seq,
      watermark: f.watermark,
      ...(f.lastRecordHash !== undefined ? { lastRecordHash: f.lastRecordHash } : {}),
      lastRecordSeq: f.lastRecordSeq,
      manifestVersion: f.state.manifestVersion,
      kind: f.kind,
      lifecycle: f.lifecycle,
      successors: f.successors,
      interest: f.interest,
      lastSyncMs: f.lastSyncMs,
    };
  }

  /** A load installed confirmed state: the image of the keyspace is now the pages of the load (14 [CLI-2]). */
  protected async persistBegin(f: Follow, pages: readonly LoadStateResponse[]): Promise<void> {
    if (!this.persistReplica || this.role !== "flusher") return;
    const first = pages[0] as LoadStateResponse;
    const entries: KeyEntry[] = [];
    for (const p of pages) for (const e of p.entries) entries.push(e);
    try {
      await this.tx([{ op: "replicaBegin", keyspaceId: f.id, head: this.headOf(f), base: { manifestVersion: first.manifestVersion, logEpoch: first.logEpoch, checkpointSeq: first.checkpointSeq, entries } }]);
      f.image = { ok: true, seqs: new Set(), headMs: this.serverNow() };
      this.touchPersisted();
    } catch (e) {
      this.imageFailed(f, e);
    }
  }

  /** Records were applied: they join the image's tail with the new head, in one local transaction ([CLI-2]). */
  protected async persistRecords(f: Follow, recs: readonly RecordMessage[]): Promise<void> {
    if (!this.persistReplica || this.role !== "flusher" || !f.image.ok) return;
    const fresh = recs.filter((r) => !f.image.seqs.has(r.seq));
    const head = this.headOf(f);
    try {
      await this.tx([fresh.length > 0 ? { op: "replicaAppend", keyspaceId: f.id, head, records: [...fresh] } : { op: "replicaHead", keyspaceId: f.id, head }]);
      for (const r of fresh) f.image.seqs.add(r.seq);
      f.image.headMs = this.serverNow();
      this.touchPersisted();
      // a tail longer than `replicaTailLimit` is dropped at a restart (`restoreImage`): it is not kept to be dropped. The next load begins the image again
      // (a cold load, an epoch change, a `RESUME_TOO_OLD`); until then nothing is persisted, and the memory of the tail is free again
      if (f.image.seqs.size > this.replicaTailLimit) {
        f.image = { ok: false, seqs: new Set(), headMs: 0 };
        await this.tx([{ op: "deleteReplica", keyspaceId: f.id }]).catch(() => undefined);
      }
    } catch (e) {
      this.imageFailed(f, e);
    }
  }

  /** The head moved with no record (a delivery watermark, the time of a sync): written at most once in `everyMs`, unless `force`. */
  protected async persistHead(f: Follow, everyMs = 30_000, force = false): Promise<void> {
    if (!this.persistReplica || this.role !== "flusher" || !f.image.ok) return;
    if (!force && this.serverNow() - f.image.headMs < everyMs) return;
    try {
      await this.tx([{ op: "replicaHead", keyspaceId: f.id, head: this.headOf(f) }]);
      f.image.headMs = this.serverNow();
      this.touchPersisted();
    } catch (e) {
      this.imageFailed(f, e);
    }
  }

  /** The entries of a widening load join the pages of the image (14 [CLI-72]). */
  protected async persistExtend(f: Follow, entries: readonly KeyEntry[]): Promise<void> {
    if (!this.persistReplica || this.role !== "flusher" || !f.image.ok) return;
    try {
      await this.tx([{ op: "replicaExtend", keyspaceId: f.id, head: this.headOf(f), entries: [...entries] }]);
      f.image.headMs = this.serverNow();
      this.touchPersisted();
    } catch (e) {
      this.imageFailed(f, e);
    }
  }

  /** The image no longer follows the state: it is dropped, so that a restart loads the keyspace instead of restoring a state that is behind. */
  protected imageFailed(f: Follow, e: unknown): void {
    this.noteInternalError(e);
    f.image = { ok: false, seqs: new Set(), headMs: 0 };
    void this.tx([{ op: "deleteReplica", keyspaceId: f.id }]).catch(() => undefined);
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Restoring (14 [CLI-2], [CLI-8])

  /**
   * Builds the followed keyspace of a persisted image: the pages are installed, the records folded again, the cursor and the epoch
   * set. Returns undefined for an image that cannot be restored (a manifest that is gone, a record that no longer folds, a tail
   * that is too long): the caller drops it, and the keyspace is loaded as if it had none.
   */
  protected async restoreImage(img: ReplicaImage): Promise<Follow | undefined> {
    if (img.tail.length > this.replicaTailLimit) return undefined;
    try {
      if (!(await this.manifests.ensure(img.base.manifestVersion))) return undefined;
      for (const r of img.tail) if (!(await this.manifests.ensure(r.manifestVersion))) return undefined;
    } catch {
      return undefined;
    }
    const f = new Follow(img.keyspaceId, this.random);
    f.interest = [...img.head.interest];
    const st = new ReplicaState();
    try {
      for (const e of img.base.entries) installEntry(st, e, this.manifests, img.base.manifestVersion);
      st.setCursor(img.base.checkpointSeq, img.base.logEpoch, img.base.manifestVersion);
      const isCrdt = (e: KeyEntry): boolean => this.manifests.template(img.base.manifestVersion, e.key)?.rule === "crdt" || e.encoding === "yjs/v1";
      f.crdt.replaceConfirmed(f.crdt.install(img.base.entries, isCrdt));
      f.state = st;
      f.kind = img.head.kind;
      for (const rec of img.tail) {
        if (rec.logEpoch !== img.base.logEpoch) return undefined;
        this.foldRecord(f, rec);
        f.image.seqs.add(rec.seq);
      }
      if (img.head.seq < st.seq) return undefined; // a head behind its own tail: not one that this engine wrote
      st.setCursor(img.head.seq, img.head.logEpoch, img.head.manifestVersion);
      this.pruneAllOutside(f); // the pages may be those of a wider interest than the head names (a narrowing, 14 [CLI-72])
    } catch (e) {
      f.crdt.destroy();
      void (e instanceof FoldError); // a record that no longer folds, or a base that no longer installs: the image is of no use
      return undefined;
    }
    f.loaded = true;
    f.everLoaded = true;
    f.lifecycle = img.head.lifecycle === KeyspaceLifecycle.UNSPECIFIED ? KeyspaceLifecycle.ACTIVE : (img.head.lifecycle as KeyspaceLifecycle);
    f.successors = [...img.head.successors];
    f.watermark = img.head.watermark;
    f.lastRecordHash = img.head.lastRecordHash;
    f.lastRecordSeq = img.head.lastRecordSeq;
    f.lastSyncMs = img.head.lastSyncMs;
    f.image.ok = true;
    f.image.headMs = this.serverNow();
    return f;
  }

  /** The topology of the keyspace's manifest version in force, for the ancestor test of a widening load. */
  protected topologyOf(f: Follow): VersionTopology {
    return new VersionTopology(this.manifests, f.state.manifestVersion);
  }

  /**
   * Installs the entries of a widening load into the state (14 [CLI-72]): "the load's entries for the added prefixes and the ancestor
   * tombstones that it returns, and nothing else". An installed entry whose key fails the ancestor test of 05 [STATE-38] is absent: a
   * held entity or staged tombstone of one of its ancestor markers has a deletion seq at or above the entry's version, so that a
   * widening never brings a deleted entity back.
   */
  protected installWidening(f: Follow, entries: readonly KeyEntry[], manifestVersion: number): void {
    const st = f.state;
    for (const e of entries) installEntry(st, e, this.manifests, manifestVersion);
    const top = this.topologyOf(f);
    for (const e of entries) {
      if (e.deleted || isReservedKey(e.key)) continue;
      for (const a of top.ancestors(e.key)) {
        const t = st.tombstone(a);
        if (t !== undefined && (t.kind === "entity" || t.kind === "staged") && e.version <= t.version) {
          st.dropKey(e.key);
          break;
        }
      }
    }
    const isCrdt = (e: KeyEntry): boolean => this.manifests.template(manifestVersion, e.key)?.rule === "crdt" || e.encoding === "yjs/v1";
    for (const [key, doc] of f.crdt.install(entries, isCrdt)) {
      const row = st.key(key);
      if (row === undefined) {
        doc.destroy();
        continue;
      }
      f.crdt.setConfirmed(key, doc);
    }
    f.crdt.reconcile(st);
  }
}
