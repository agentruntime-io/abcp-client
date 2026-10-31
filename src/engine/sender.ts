// Sending (14 §14.3, §14.4): the durable outbox's front end (`submitAction`), the order and the dependencies of what is sent, the
// slots of the lanes, the request, and the handling of its answer.
//
//  - [CLI-21] an action is accepted only after the local storage transaction has completed;
//  - [CLI-22] every send repeats the stored content byte for byte, only the stamping fields change;
//  - [CLI-26] at most one `Mutate` in flight on a keyspace's SEQUENCED lane, batched in `client_seq` order;
//  - [CLI-27] `client_seq` per lane and keyspace, contiguous from 1, assigned when an action is first sent and recorded before the
//    request leaves; a slot is consumed when the lane's watermark says so, never by a result's code;
//  - [CLI-31], [CLI-33] an action waits for what it depends on, and for nothing else; [CLI-32] a GATED action carries
//    `after_client_seq`;
//  - [CLI-24], [CLI-50] an action with no definitive result is resent unchanged, never rebased, dead-lettered or dropped.

import { create } from "@bufbuild/protobuf";
import { MutateRequestSchema, type MutateResponse, type MutationResult, type Mutation } from "../gen/abpc/v2/sync_pb.js";
import { Code, DecisionStatus, ErrorClass, KeyspaceLifecycle, StaleCause, type Reject, type Retry } from "../gen/abpc/v2/types_pb.js";
import { VersionTopology } from "../rules/fold/index.js";
import { buildMutation, checkAction } from "./author.js";
import { codeName } from "./codes.js";
import { dependsOn, encodeContent, keysWritten, stamped, type DepView } from "./entry.js";
import { makeReject } from "./endings.js";
import { newUuid } from "./ids.js";
import { LocalRefusal, ProtocolViolation, RpcError, StorageError } from "./errors.js";
import type { Follow } from "./follow.js";
import type { Hold } from "./holds.js";
import { ReactionLayer } from "./reactions.js";
import type { OutboxEntry, StoredOutcome } from "./storage.js";
import type { ActionInput, SubmitOptions, SubmitResult } from "./types.js";

export abstract class SenderLayer extends ReactionLayer {
  /** The keyspaces with a SEQUENCED request in flight ([CLI-26]). */
  private readonly seqInflight = new Set<string>();
  /** Whether authoring of a SEQUENCED action may follow a dependency on the same lane without its result ([CLI-31], exception). */
  pipelineSequenced = false;

  // ---------------------------------------------------------------------------------------------------------------------
  // Authoring (14 [CLI-17], [CLI-18], [CLI-19], [CLI-21], [CLI-23], [CLI-82])

  protected isOffline(f: Follow): boolean {
    if (!this.online()) return true;
    return this.holds.blocking({ keyspaceId: f.id, lane: "SEQUENCED", mutationId: "" }).some((h) => h.scope === "app" || h.scope === "keyspace");
  }

  protected submitAction(input: ActionInput, options?: SubmitOptions): Promise<SubmitResult> {
    return this.run(async () => {
      const f = this.follows.get(input.keyspaceId);
      // 14 [CLI-69]: an action is authored only into a keyspace that the engine follows
      if (f === undefined) throw new LocalRefusal("NOT_FOLLOWED", `the keyspace ${input.keyspaceId} is not followed`);
      if (!f.loaded) throw new LocalRefusal("NOT_FOLLOWED", `the keyspace ${input.keyspaceId} is not loaded yet`);
      const version = f.state.manifestVersion;
      const manifest = this.manifests.get(version);
      if (manifest === undefined) throw new LocalRefusal("INVALID", `the manifest version ${version} is not loaded`);
      const op = manifest.opType(input.opType);
      if (op === undefined) throw new LocalRefusal("INVALID", `the manifest declares no op type ${input.opType}`);
      const id = newUuid(this.random); // 14 [CLI-23]: minted once, at authoring
      // 14 [CLI-82]: `fail_fast` while offline is refused at once, committed to nothing, with `rejected` and the cause OFFLINE
      if (op.offlineMode === "fail_fast" && this.isOffline(f)) {
        this.emit({ type: "rejected", mutationId: id, keyspaceId: f.id, cause: "OFFLINE" });
        throw new LocalRefusal("OFFLINE", `the op type ${op.name} is fail_fast and the client is offline`);
      }
      const top = new VersionTopology(this.manifests, version);
      const facts = {
        loaded: f.loaded,
        seq: f.state.seq,
        logEpoch: f.state.logEpoch,
        versionOf: (key: string): bigint => f.state.effectiveVersion(top, key), // 14 [CLI-17]: tombstones included
        manifestVersion: version,
      };
      const m = buildMutation(input, id, op, facts, this.clock.now());
      // 14 [CLI-49]: nothing is authored under an entity that stays deleted
      for (const c of m.changes) {
        for (const a of top.ancestors(c.key)) {
          if (f.state.tombstone(a)?.kind === "entity" && f.state.key(a) === undefined) throw new LocalRefusal("REFUSED", `the entity ${a} is deleted`);
        }
      }
      const content = encodeContent(m);
      const reason = checkAction(m, manifest, op, this.limits, content.length); // 14 [CLI-19]
      if (reason !== undefined) throw new LocalRefusal("INVALID", reason);

      // dependencies (14 [CLI-30]): on the earlier actions of the keyspace that are still in the outbox and not ended
      const explicit = new Set(options?.after ?? []);
      for (const x of explicit) if (!this.entries.has(x)) throw new LocalRefusal("REFUSED", `unknown action ${x} in the explicit links`);
      const bView: DepView = { mutationId: id, opType: input.opType, keys: keysWritten(m), mutation: m };
      const ctx = { barrier: (opType: string) => manifest.opType(opType)?.barrier ?? false, fencedBy: (key: string) => manifest.fencedBy(key) };
      const deps: string[] = [];
      // a replacement takes the place of the action it replaces: the actions that depend on that one move to it ([CLI-34], [CLI-47])
      const moving = new Set<string>();
      if (options?.replaces !== undefined) {
        const old = this.entries.get(options.replaces);
        if (old !== undefined) for (const d of this.dependentsOf(old)) moving.add(d.mutationId);
      }
      for (const a of this.entriesOf(f.id)) {
        if (this.ending.has(a.mutationId) || a.stuck !== undefined || moving.has(a.mutationId)) continue;
        const am = this.content(a);
        if (am === undefined) continue;
        if (dependsOn(bView, { mutationId: a.mutationId, opType: a.opType, keys: keysWritten(am), mutation: am }, explicit, ctx)) deps.push(a.mutationId);
      }

      const now = this.serverNow();
      const entry: OutboxEntry = {
        mutationId: id,
        keyspaceId: f.id,
        lane: op.lane,
        opType: input.opType,
        content,
        author: this.author,
        ordinal: this.nextOrdinal,
        authoredServerMs: now,
        dependsOn: deps,
        clientId: "",
        clientSeq: 0,
        afterClientSeq: 0,
        everSent: false,
        sentInEpoch: 0,
        attempts: 0,
        registered: false,
        permissionRetried: false,
        actorMismatchRetried: false,
        spendResent: false,
      };
      // 14 [CLI-21]: accepted only after the local transaction has completed; a failure refuses the action and nothing is shown
      try {
        await this.tx([this.putOp(entry), { op: "setMeta", key: "nextOrdinal", value: this.nextOrdinal + 1 }]);
      } catch (e) {
        throw new LocalRefusal("STORAGE", e instanceof Error ? e.message : String(e));
      }
      this.nextOrdinal++;
      this.entries.set(id, entry);
      this.decoded.set(id, m);
      f.invalidate();
      this.schedulePump();
      this.armBoundTimer();
      return { mutationId: id };
    });
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Planning (14 [CLI-26] to [CLI-33])

  protected async pumpAll(): Promise<void> {
    if (this.stopped) return;
    for (const f of [...this.follows.values()]) await this.pumpKeyspace(f);
  }

  private isDone(e: OutboxEntry): boolean {
    return e.outcome !== undefined || this.ending.has(e.mutationId) || e.stuck !== undefined;
  }

  /** The dependencies of `e` that have no final successful result yet ([CLI-31]). */
  private unresolvedDeps(e: OutboxEntry): OutboxEntry[] {
    const out: OutboxEntry[] = [];
    for (const id of e.dependsOn) {
      const a = this.entries.get(id);
      if (a === undefined) continue; // retired, or ended (then `e` failed)
      const ok = a.outcome !== undefined && !(a.outcome.kind === "decided" && a.outcome.status === "REJECTED");
      if (ok) continue;
      // 14 [CLI-31], exception: both SEQUENCED, B may follow A on the same lane (the server stops the lane at the first action not applied)
      if (this.pipelineSequenced && a.lane === "SEQUENCED" && e.lane === "SEQUENCED") continue;
      out.push(a);
    }
    return out;
  }

  /** Whether the hold that blocks `e` lets it through as the probe of the hold (a resend of the head, 14 [CLI-45]). */
  private probeFor(e: OutboxEntry): Hold | undefined {
    for (const h of this.holds.blocking(e)) {
      if (h.probeDue === true && h.probeId === e.mutationId && (h.scope === "lane" || h.scope === "keyspace")) return h;
    }
    return undefined;
  }

  private canSendNow(e: OutboxEntry): boolean {
    if (this.isDone(e)) return false;
    const rt = this.rt(e.mutationId);
    if (rt.inflight || rt.polling) return false;
    if (rt.nextAttemptAt > this.clock.now()) return false;
    const blocking = this.holds.blocking(e);
    if (blocking.length > 0) {
      // only the probe of a lane or keyspace hold goes through, and only when no other hold stops the action
      const others = blocking.filter((h) => !(h.probeDue === true && h.probeId === e.mutationId && (h.scope === "lane" || h.scope === "keyspace")));
      if (others.length > 0) return false;
    }
    return this.unresolvedDeps(e).length === 0;
  }

  protected async pumpKeyspace(f: Follow): Promise<void> {
    if (!this.online() || !f.loaded || f.frozen !== undefined) return;
    if (f.stopped !== undefined && f.stopped !== "GRANT_CHANGED") return;
    const entries = this.entriesOf(f.id).filter((e) => !this.isDone(e));
    if (entries.length === 0) return;
    const lim = this.limits;
    const batch: OutboxEntry[] = [];

    // -- the SEQUENCED lane: the assigned slots first, in order, then new actions in authoring order ([CLI-26], [CLI-27]) --
    if (!this.seqInflight.has(f.id)) {
      const lane = f.lanes.SEQUENCED;
      const seq = entries.filter((e) => e.lane === "SEQUENCED");
      const slotted = seq.filter((e) => e.clientId === this.clientId && e.clientSeq > 0 && lane.assigned.get(e.clientSeq) === e.mutationId).sort((a, b) => a.clientSeq - b.clientSeq);
      const fresh = seq.filter((e) => !slotted.includes(e));
      const room = (): boolean => batch.length < lim.maxMutationsPerRequest && batch.reduce((n, x) => n + x.content.length + 256, 0) < lim.maxRequestBytes;
      let blocked = false;
      let alone = false;
      for (const e of slotted) {
        if (!this.canSendNow(e)) {
          blocked = true;
          break; // the server processes the lane in client_seq order: nothing goes past an action that is not sent
        }
        if ((batch.length > 0 && !room()) || (this.rt(e.mutationId).alone && batch.length > 0)) {
          blocked = true;
          break;
        }
        batch.push(e);
        if (this.rt(e.mutationId).alone) {
          alone = true; // a request that was split: each of its mutations goes alone, in lane order (14 [CLI-50])
          blocked = true;
          break;
        }
      }
      if (!blocked && !alone) {
        for (const e of fresh) {
          if (!this.canSendNow(e)) continue; // an action with no unresolved dependency does not wait for unrelated actions ([CLI-33])
          if (batch.length > 0 && (!room() || this.rt(e.mutationId).alone)) break;
          batch.push(e);
          if (this.rt(e.mutationId).alone) break;
        }
      }
      // slots are assigned in this order, so that they are contiguous ([CLI-27])
      for (const e of batch) {
        if (!(e.clientId === this.clientId && e.clientSeq > 0 && lane.assigned.get(e.clientSeq) === e.mutationId)) e.clientSeq = lane.allocate(e.mutationId);
        e.clientId = this.clientId;
      }
    }

    // -- the GATED lane: independent of each other; each waits for the slot of the last earlier SEQUENCED action ([CLI-32], [CLI-33]) --
    const gatedLane = f.lanes.GATED;
    let gatedRoom = lim.maxInflightGated - entries.filter((e) => e.lane === "GATED" && e.everSent && !this.isDone(e) && (this.rt(e.mutationId).inflight || this.rt(e.mutationId).polling)).length;
    const earlierSeq = this.entriesOf(f.id).filter((e) => e.lane === "SEQUENCED" && !this.ending.has(e.mutationId) && e.stuck === undefined);
    const gated: OutboxEntry[] = [];
    for (const e of entries) {
      if (e.lane !== "GATED" || !this.canSendNow(e)) continue;
      if (gatedRoom <= 0) break;
      let after = 0;
      let waits = false;
      for (const s of earlierSeq) {
        if (s.ordinal >= e.ordinal) continue;
        if (s.clientId === this.clientId && s.clientSeq > 0) after = Math.max(after, s.clientSeq);
        else if (s.outcome === undefined) waits = true; // an earlier SEQUENCED action with no slot yet gives none to name ([CLI-32])
      }
      if (waits) continue;
      e.afterClientSeq = after;
      if (!(e.clientId === this.clientId && e.clientSeq > 0 && gatedLane.assigned.get(e.clientSeq) === e.mutationId)) e.clientSeq = gatedLane.allocate(e.mutationId);
      e.clientId = this.clientId;
      gated.push(e);
      gatedRoom--;
    }

    const all = [...batch, ...gated];
    if (all.length === 0) return;
    await this.dispatch(f, all, batch.length > 0);
  }

  /** Records the stamping before the request leaves ([CLI-27]), then sends. */
  private async dispatch(f: Follow, all: OutboxEntry[], hasSequenced: boolean): Promise<void> {
    const before = all.map((e) => ({ everSent: e.everSent, sentInEpoch: e.sentInEpoch }));
    all.forEach((e) => {
      e.everSent = true;
      e.sentInEpoch = f.logEpoch;
    });
    try {
      await this.tx(all.map((e) => this.putOp(e)));
    } catch (err) {
      // the stamping could not be recorded: the request does not leave, and the slots are given back
      all.forEach((e, i) => {
        e.everSent = (before[i] as { everSent: boolean }).everSent;
        e.sentInEpoch = (before[i] as { sentInEpoch: number }).sentInEpoch;
      });
      if (!(err instanceof StorageError) && !(err instanceof Error)) throw err;
      return;
    }
    const mutations: Mutation[] = [];
    for (const e of all) {
      const c = this.content(e);
      if (c === undefined) {
        // 14 [CLI-29]: an entry that cannot be decoded goes to the dead-letter store; it stops no other entry
        await this.endAction(e, { sink: "deadLetter", reject: makeReject(Code.INVALID_ENVELOPE, ErrorClass.DEAD_LETTER, "the stored content cannot be decoded"), reason: "UNDECODABLE", dependents: "fail" });
        continue;
      }
      mutations.push(stamped(c, e));
    }
    const live = all.filter((e) => this.entries.has(e.mutationId));
    if (live.length === 0) return;
    for (const e of live) {
      const rt = this.rt(e.mutationId);
      rt.inflight = true;
      // the probe of a hold is spent by this resend (14 [CLI-45])
      for (const h of this.holds.blocking(e)) if (h.probeDue === true && h.probeId === e.mutationId) this.probeSpent(h);
    }
    if (hasSequenced) this.seqInflight.add(f.id);
    const req = create(MutateRequestSchema, { mutations, supportedFeatures: [...this.features] });
    this.detach(this.sendRequest(f, live, req, hasSequenced));
  }

  /** The call, outside the queue: its answer comes back as a task. */
  private async sendRequest(f: Follow, batch: OutboxEntry[], req: import("../gen/abpc/v2/sync_pb.js").MutateRequest, hasSequenced: boolean): Promise<void> {
    let resp: MutateResponse;
    try {
      resp = await this.transport.mutate(req);
    } catch (err) {
      await this.run(async () => {
        if (hasSequenced) this.seqInflight.delete(f.id);
        await this.onMutateFailure(f, batch, err);
        await this.flushDirty();
      });
      return;
    }
    await this.run(async () => {
      if (hasSequenced) this.seqInflight.delete(f.id);
      await this.onMutateResponse(f, batch, resp);
      await this.flushDirty();
    });
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // The answer (14 [CLI-7], [CLI-27], [CLI-78])

  private async onMutateResponse(f: Follow, batch: OutboxEntry[], resp: MutateResponse): Promise<void> {
    for (const e of batch) {
      this.rt(e.mutationId).inflight = false;
      this.rt(e.mutationId).authTries = 0;
      this.rt(e.mutationId).alone = false; // the request that was split has been answered
    }
    if (resp.results.length !== batch.length || resp.results.some((r, i) => r.mutationId !== (batch[i] as OutboxEntry).mutationId)) {
      // the response does not answer its request: it breaks 02 [ARC-61]; the outcome of the actions stays unknown ([CLI-50], [CLI-24])
      this.onProtocolViolation(new ProtocolViolation("MutateResponse", "the results do not answer the request", [f.id]), [f.id]);
      return;
    }
    this.noteWatermarks(resp.watermarks);
    for (let i = 0; i < batch.length; i++) {
      const e = batch[i] as OutboxEntry;
      if (!this.entries.has(e.mutationId) || this.ending.has(e.mutationId)) continue;
      await this.applyResult(e, resp.results[i] as MutationResult, "mutate");
    }
    // 14 [CLI-52]: the rest of the queued actions of a keyspace that this answer showed to be split or archived
    if (f.lifecycle !== KeyspaceLifecycle.ACTIVE && f.lifecycle !== KeyspaceLifecycle.UNSPECIFIED) await this.keyspaceEnded(f, f.lifecycle, f.successors);
    this.armBoundTimer();
    this.schedulePump();
  }

  protected async applyResult(e: OutboxEntry, r: MutationResult, source: "mutate" | "poll" | "cancel"): Promise<void> {
    if (!this.entries.has(e.mutationId) || this.ending.has(e.mutationId)) return;
    const rt = this.rt(e.mutationId);
    rt.inflight = false;
    const f = this.follows.get(e.keyspaceId);
    if (e.lane === "GATED" && r.intentSeq > 0n) e.registered = true;
    const o = r.outcome;
    // a result that is not the hold's own ends the hold of a resend (14 Table 14.1)
    const rejectKind = o.case === "rejected" ? (await Promise.resolve(this.holdKindOfResult(o.value))) : undefined;
    if (source === "mutate" || source === "poll") this.clearProbeHolds(e, rejectKind);
    switch (o.case) {
      case "appended": {
        e.lastResult = { kind: "appended", what: `seq ${o.value.seq}`, atMs: this.serverNow() };
        const out: StoredOutcome = { kind: "appended", seq: o.value.seq, logEpoch: o.value.logEpoch, overwroteNewer: [...o.value.overwroteNewer] };
        if (e.outcome === undefined || e.outcome.logEpoch !== out.logEpoch) this.setOutcome(e, out);
        this.markDirty(e);
        // 14 [CLI-40]: `overwritten` for the client's own action, from the first of the result and the record that it gets
        if (o.value.overwroteNewer.length > 0 && f !== undefined) {
          const key = `${f.id}/${e.mutationId}`;
          if (!this.overwrittenSeen.has(key)) {
            this.overwrittenSeen.add(key);
            const m = this.content(e);
            this.emit({ type: "overwritten", mutationId: e.mutationId, keyspaceId: f.id, seq: o.value.seq, keys: o.value.overwroteNewer.map((i) => m?.changes[i]?.key ?? ""), by: this.author.sub });
          }
        }
        if (f !== undefined) await this.checkRetirements(f);
        return;
      }
      case "decided": {
        const d = o.value;
        e.lastResult = { kind: "decided", what: d.status === DecisionStatus.COMMITTED ? "COMMITTED" : "REJECTED", atMs: this.serverNow() };
        if (d.status === DecisionStatus.COMMITTED) {
          const out: StoredOutcome = { kind: "decided", status: "COMMITTED", decisionSeq: d.decisionSeq, logEpoch: d.logEpoch };
          if (e.outcome === undefined || e.outcome.logEpoch !== out.logEpoch) this.setOutcome(e, out);
          this.markDirty(e);
          if (f !== undefined) await this.checkRetirements(f);
        } else {
          // a REJECTED decision is a final rejection ([CLI-34]), of the class of its reject (A.1)
          const reject = d.reject ?? makeReject(Code.GATE_REJECTED, ErrorClass.DEAD_LETTER, "rejected");
          await this.onRejectResult(e, reject);
        }
        return;
      }
      case "rejected":
        await this.onRejectResult(e, o.value);
        return;
      case "retry":
        await this.onRetryResult(e, o.value, r.intentSeq);
        return;
      default:
        this.onProtocolViolation(new ProtocolViolation("MutationResult", "a result with no outcome", f !== undefined ? [f.id] : []), f !== undefined ? [f.id] : []);
    }
  }

  private holdKindOfResult(r: Reject): import("./holds.js").HoldKind | undefined {
    return holdKindOfReject(r);
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // A call that failed (14 [CLI-24], [CLI-50], A.7)

  private async onMutateFailure(f: Follow, batch: OutboxEntry[], err: unknown): Promise<void> {
    for (const e of batch) this.rt(e.mutationId).inflight = false;
    if (err instanceof ProtocolViolation) {
      // 16 [SEC-3]: nothing of it is applied; the actions that it answered stay unknown ([CLI-24])
      this.onProtocolViolation(err, err.keyspaces.length > 0 ? err.keyspaces : [f.id]);
      return;
    }
    const live = batch.filter((e) => this.entries.has(e.mutationId));
    const status = err instanceof RpcError ? err.status : "Unavailable";
    const detail = err instanceof RpcError ? err : undefined;
    const retryAfter = detail?.retry?.afterMs;
    switch (status) {
      case "Unauthenticated": {
        if (detail?.reject?.details["cause"] === "SUBJECT_REVOKED") {
          // TODO(14 [CLI-66], [CLI-79], R/C3 §14.9): purge and quarantine as for Closing{SUBJECT_REVOKED}; here the client only stops
          this.holdApp("SUBJECT_REVOKED", "SUBJECT_REVOKED");
          return;
        }
        // 14 [CLI-50]: refresh the token, then resend once; a second Unauthenticated for the request stops it (the hold NO_TOKEN)
        const second = live.every((e) => this.rt(e.mutationId).authTries >= 1);
        if (second) {
          this.holdApp("NO_TOKEN", "NO_TOKEN");
          return;
        }
        for (const e of live) {
          this.rt(e.mutationId).authTries++;
          this.refreshThenResend(e);
        }
        return;
      }
      case "ResourceExhausted": {
        if (detail?.retry !== undefined) {
          // Retry{RATE_LIMITED}: back off and resend
          for (const e of live) this.deferEntry(e, this.nextDelay(e, retryAfter));
          return;
        }
        // no detail: split the request; a single mutation that is still too large is dead-lettered (14 [CLI-50])
        await this.splitOrEnd(live, "ResourceExhausted", Code.PAYLOAD_TOO_LARGE);
        return;
      }
      case "InvalidArgument": {
        // 14 [CLI-50]: the request recorded none of its mutations; each is sent alone once, in lane order
        await this.splitOrEnd(live, "InvalidArgument", Code.INVALID_ENVELOPE);
        return;
      }
      case "PermissionDenied": {
        const r = detail?.reject;
        if (r !== undefined && r.code === Code.APP_SUSPENDED) {
          this.holdApp("APP_SUSPENDED", codeName(Code.APP_SUSPENDED));
          return;
        }
        if (r !== undefined) {
          for (const e of live) await this.onRejectResult(e, r);
          return;
        }
        for (const e of live) this.deferEntry(e, this.nextDelay(e, undefined));
        return;
      }
      case "Unimplemented":
        // 15 [COMPAT-6], [COMPAT-28]: an RPC of v2.0 that the server does not speak stops sync with that server
        this.onUnimplemented(f);
        return;
      case "NotFound":
      case "AlreadyExists":
      case "FailedPrecondition": {
        if (detail?.reject !== undefined) {
          for (const e of live) await this.onRejectResult(e, detail.reject);
          return;
        }
        for (const e of live) this.deferEntry(e, this.nextDelay(e, undefined));
        return;
      }
      default:
        // Unavailable, DeadlineExceeded, Aborted, Unknown, Internal, Cancelled, DataLoss, OutOfRange, and any other status: an unknown
        // outcome (14 [CLI-24], A.7's last row). The action is resent unchanged with the client backoff, never rebased, dead-lettered
        // or dropped.
        for (const e of live) {
          e.lastResult = { kind: "rpc", what: status, atMs: this.serverNow() };
          this.markDirty(e);
          this.deferEntry(e, this.nextDelay(e, retryAfter));
        }
    }
  }

  /** 14 [CLI-50]: split a request; one mutation that still fails alone is dead-lettered with the status as its reason. */
  private async splitOrEnd(live: OutboxEntry[], status: string, code: Code): Promise<void> {
    if (live.length === 1 || live.every((e) => this.rt(e.mutationId).alone)) {
      for (const e of live) {
        if (!this.rt(e.mutationId).alone && live.length > 1) continue;
        if (this.rt(e.mutationId).alone || live.length === 1) {
          const reject = makeReject(code, ErrorClass.DEAD_LETTER, `${status}: the call failed for the action alone`);
          await this.endAction(e, { sink: "deadLetter", reject, reason: status === "ResourceExhausted" ? codeName(Code.PAYLOAD_TOO_LARGE) : status, dependents: "fail" });
        }
      }
      return;
    }
    for (const e of live) {
      this.rt(e.mutationId).alone = true;
      this.rt(e.mutationId).nextAttemptAt = 0;
    }
    this.schedulePump();
  }

  protected abstract onUnimplemented(f: Follow): void;
}

/** Re-export for the sender: the hold row of a reject. */
import { holdKindOf as holdKindOfReject } from "./reactions.js";

export { StaleCause };
export type { Retry };
