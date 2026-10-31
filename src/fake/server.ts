// The scriptable fake server of 17 [CONF-4]: an in-memory ABPC v2 server that the client suites run against. It keeps keyspaces
// as the fold of C1 defines them (it reuses `rules/fold` for its own state), appends OP, INTENT, DECISION and SYSTEM records,
// binds mutation ids and lane slots as 07 §7.4 and §7.6 say, answers results, streams records and heartbeats, and lets a test
//
//  - drop, reorder, duplicate and delay frames, pause delivery, and disconnect a session;
//  - inject any `Reject` of any code and class, any `Retry`, and any RPC status, on any call, with or without an ABPC detail;
//  - lose a response after the action was applied (an unknown outcome);
//  - decide a GATED intent later, restore a keyspace (an epoch advance that voids bindings and ends intents, 07 [MUT-104]),
//    freeze, archive, split and delete a keyspace, suspend the app, and close a session with any reason.
//
// What it does not do: it is no reference of the checks of 07 Table 7-1 (values are not validated against schemas, grants are
// not judged); it checks what the engine's reactions depend on: the envelope, idempotency, lanes, the base epoch, preconditions,
// CAS, entity tombstones, and CRDT well-formedness.

import { create } from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import * as Y from "yjs";
import {
  AppendedSchema,
  ControlFrame_ClosingSchema,
  ControlFrame_KeyspaceAddedSchema,
  ControlFrame_KeyspaceDroppedSchema,
  ControlFrame_ReconnectSchema,
  ControlFrame_SubscribedSchema,
  ControlFrameSchema,
  CancelIntentResponseSchema,
  CreateKeyspaceResponseSchema,
  DecidedSchema,
  GetManifestResponseSchema,
  GetMutationResultResponseSchema,
  GetMutationResultsResponseSchema,
  HeartbeatFrame_CursorSchema,
  HeartbeatFrameSchema,
  LaneWatermarkSchema,
  LoadStateResponseSchema,
  MutateResponseSchema,
  MutationResultSchema,
  RecordsFrameSchema,
  RefreshSessionResponseSchema,
  SubscribeResponseSchema,
  UpdateSubscriptionResponseSchema,
  UpdateSubscriptionResponse_RejectionSchema,
  type CancelIntentRequest,
  type CancelIntentResponse,
  type CloseReason,
  type ControlFrame,
  type CreateKeyspaceRequest,
  type CreateKeyspaceResponse,
  type DropReason,
  type GetManifestRequest,
  type GetManifestResponse,
  type GetMutationResultRequest,
  type GetMutationResultResponse,
  type GetMutationResultsRequest,
  type GetMutationResultsResponse,
  type LoadStateRequest,
  type LoadStateResponse,
  type MutateRequest,
  type MutateResponse,
  type Mutation,
  type MutationResult,
  type RefreshSessionRequest,
  type RefreshSessionResponse,
  type SubscribeRequest,
  type SubscribeResponse,
  type UpdateSubscriptionRequest,
  type UpdateSubscriptionResponse,
  CloseReason as CloseReasonEnum,
} from "../gen/abpc/v2/sync_pb.js";
import {
  AppLimitsSchema,
  ChangeKind,
  Code,
  DecisionSchema,
  DecisionStatus,
  ErrorClass,
  KeyChangeSchema,
  KeyEntrySchema,
  KeyspaceCursorSchema,
  KeyspaceLifecycle,
  Lane,
  PrincipalKind,
  PrincipalSchema,
  RecordKind,
  RecordSchema,
  RejectSchema,
  RetryReason,
  RetrySchema,
  ServerInfoSchema,
  StaleCause,
  StaleSchema,
  SuccessorRuleSchema,
  SystemEventSchema,
  SystemKind,
  type AppLimits,
  type KeyChange,
  type KeyEntry,
  type KeyspaceCursor,
  type Principal,
  type Record as RecordMessage,
  type Reject,
  type Retry,
  type SuccessorRule,
} from "../gen/abpc/v2/types_pb.js";
import { checkWellFormed } from "../rules/crdt/index.js";
import { applyRecord, ReplicaState, StateNull, StateValue, VersionTopology, isReservedKey, type Rule } from "../rules/fold/index.js";
import { compareUtf8, toHex } from "../rules/bytes.js";
import { sha256 } from "../rules/crypto.js";
import { setHashProvider } from "../rules/crypto.js";
import { nodeHashProvider } from "../rules/node-crypto.js";
import { DEFAULT_CLASS } from "../engine/codes.js";
import { appliedIndexes, planCrdt } from "../engine/crdt-view.js";
import { encodeContent } from "../engine/entry.js";
import { RpcError, type RpcStatus } from "../engine/errors.js";
import type { Clock } from "../engine/clock.js";
import type { StaticManifests } from "../engine/manifests.js";
import type { CallOptions, Transport } from "../engine/transport.js";

setHashProvider(nodeHashProvider);

// ---------------------------------------------------------------------------------------------------------------------
// Scripting

export type Method = "mutate" | "getMutationResult" | "getMutationResults" | "cancelIntent" | "createKeyspace" | "loadState" | "subscribe" | "updateSubscription" | "refreshSession" | "getManifest";

/** What an injection does to a call (17 [CONF-4]). */
export type Behavior =
  /** The call fails with this status, and the ABPC detail if one is given, before the server does anything. */
  | { kind: "status"; status: RpcStatus; reject?: Reject; retry?: Retry; message?: string }
  /** The server handles the call, and the response is lost: the call fails with `status` (default Unavailable). The effect stays. */
  | { kind: "lose"; status?: RpcStatus }
  /** The server waits this long, on its clock, before it handles the call. */
  | { kind: "delay"; ms: number }
  /** For `mutate`: the result of a mutation, instead of processing it. `recorded`: the result is bound as the id's outcome and consumes its slot (07 §7.6). */
  | { kind: "result"; result: (m: Mutation) => MutationResult | undefined; recorded?: boolean };

interface Injection {
  method: Method;
  match: ((req: unknown, principal: Principal) => boolean) | undefined;
  times: number;
  behavior: Behavior;
}

/** What the test says about the frames of a session. */
export interface FrameFaults {
  /** Drop the next n records frames: the server's watermark moves, the client's does not (14 [CLI-3]). */
  dropNext: number;
  /** Send the next n records frames twice. */
  duplicateNext: number;
  /** Send the next two records frames in the other order. */
  reorderNext: boolean;
  /** Delay every records frame by this long, on the server's clock. */
  delayMs: number;
  /** Hold records frames until `resume()`. */
  paused: boolean;
}

export interface GateDecision {
  status: "COMMITTED" | "REJECTED";
  /** The effects of the gate, appended after the requested changes (COMMITTED). */
  effects?: KeyChange[];
  reject?: Reject;
}

export interface FakeOptions {
  manifests: StaticManifests;
  clock: Clock;
  appId?: string;
  limits?: Partial<Record<keyof AppLimitsInit, number>>;
  /** The heartbeat interval; 0 sends none on its own (a test calls `heartbeat()`). */
  heartbeatMs?: number;
  pageSize?: number;
  /** The per-principal session cap (09 [READ-35]); default none. */
  maxSessions?: number;
}

interface AppLimitsInit {
  maxOfflineSeconds: number;
  maxChangesPerAction: number;
  maxActionBytes: number;
  maxMutationsPerRequest: number;
  maxInflightGatedPerClient: number;
  maxRequestBytes: number;
  maxValueBytes: number;
  keyspaceNotFoundWaitSeconds: number;
}

const DEFAULT_LIMITS: AppLimitsInit = {
  maxOfflineSeconds: 30 * 24 * 3600,
  maxChangesPerAction: 1000,
  maxActionBytes: 2 << 20,
  maxMutationsPerRequest: 100,
  maxInflightGatedPerClient: 32,
  maxRequestBytes: 4 << 20,
  maxValueBytes: 1 << 20,
  keyspaceNotFoundWaitSeconds: 3600,
};

// ---------------------------------------------------------------------------------------------------------------------
// State

interface Binding {
  subject: string;
  /** The canonical content, as hex: what the content hash stands for (07 [MUT-45]). */
  content: string;
  opType: string;
  result: MutationResult;
  /** The outcome is final: the slot is consumed. A pending intent is bound and not consumed. */
  final: boolean;
  mutation?: Mutation;
  intentSeq?: bigint;
}

interface Voided {
  subject: string;
  opType: string;
}

interface LaneSlots {
  seq: Map<number, string>;
  gated: Map<number, string>;
}

class FakeKeyspace {
  state = new ReplicaState();
  log: RecordMessage[] = [];
  epoch = 1;
  kind = "";
  lifecycle: KeyspaceLifecycle = KeyspaceLifecycle.ACTIVE;
  successors: SuccessorRule[] = [];
  manifestVersion: number;
  frozen = false;
  deleted = false;
  seeding = false;
  /** The prune floor (06 [LOG-57]): a cursor below it cannot be resumed. */
  pruneFloor = 0n;
  bindings = new Map<string, Binding>();
  voided = new Map<string, Voided>();
  lanes = new Map<string, LaneSlots>();
  ydocs = new Map<string, Y.Doc>();
  lastWriter = new Map<string, string>();
  /** Intents that are registered and not decided. */
  pending = new Map<string, { mutation: Mutation; principal: Principal; intentSeq: bigint }>();

  constructor(
    readonly id: string,
    manifestVersion: number,
  ) {
    this.manifestVersion = manifestVersion;
  }

  get head(): bigint {
    return this.state.seq;
  }
}

interface FrameState {
  /** The server's delivery watermark: what it has examined and handed to the frame policy. */
  watermark: bigint;
  /** The watermark of the frames that were pushed to the stream: what a heartbeat may claim (09 [READ-55]). */
  pushed: bigint;
  epoch: number;
}

const END = Symbol("end");

class FakeSession {
  readonly queue: (SubscribeResponse | typeof END | { error: unknown })[] = [];
  private waiter: (() => void) | undefined;
  readonly follows = new Map<string, FrameState>();
  closed = false;
  heldFrames: SubscribeResponse[] = [];
  reorderHeld: SubscribeResponse | undefined;

  constructor(
    readonly id: string,
    readonly principal: Principal,
    public clientId: string,
  ) {}

  push(item: SubscribeResponse | typeof END | { error: unknown }): void {
    if (this.closed) return;
    this.queue.push(item);
    const w = this.waiter;
    this.waiter = undefined;
    w?.();
  }

  async next(signal: AbortSignal | undefined): Promise<SubscribeResponse | typeof END | { error: unknown }> {
    for (;;) {
      const item = this.queue.shift();
      if (item !== undefined) return item;
      if (this.closed || signal?.aborted === true) return END;
      await new Promise<void>((resolve) => {
        this.waiter = resolve;
        signal?.addEventListener("abort", () => resolve(), { once: true });
      });
    }
  }

  end(error?: unknown): void {
    this.queue.length = 0;
    this.push(error !== undefined ? { error } : END);
  }

  close(): void {
    this.closed = true;
    const w = this.waiter;
    this.waiter = undefined;
    w?.();
  }
}

const subjectOf = (p: Principal): string => `${p.issuer}|${p.sub}`;

function mkReject(code: Code, init: { message?: string; stale?: { key?: string; currentSeq?: bigint; cause: StaleCause }; details?: Record<string, string>; successors?: SuccessorRule[]; appCode?: string; changeIndex?: number } = {}): Reject {
  return create(RejectSchema, {
    code,
    errorClass: DEFAULT_CLASS.get(code) ?? ErrorClass.HOLD,
    message: init.message ?? "",
    appCode: init.appCode ?? "",
    details: init.details ?? {},
    successors: init.successors ?? [],
    ...(init.changeIndex !== undefined ? { changeIndex: init.changeIndex } : {}),
    ...(init.stale !== undefined ? { stale: create(StaleSchema, { key: init.stale.key ?? "", currentSeq: init.stale.currentSeq ?? 0n, cause: init.stale.cause }) } : {}),
  });
}

/** A reject with a class of the test's choosing, for the codes that a v2.x revision adds and the classes that no v2.0 client knows. */
export function rejectWith(code: number, errorClass: number, init: { message?: string; details?: Record<string, string>; appCode?: string } = {}): Reject {
  return create(RejectSchema, { code, errorClass, message: init.message ?? "", details: init.details ?? {}, appCode: init.appCode ?? "" });
}

export { mkReject as reject };

function resultOf(id: string, o: MutationResult["outcome"], extra: { duplicate?: boolean; intentSeq?: bigint } = {}): MutationResult {
  return create(MutationResultSchema, { mutationId: id, outcome: o, duplicate: extra.duplicate ?? false, intentSeq: extra.intentSeq ?? 0n });
}

const rejectedResult = (id: string, r: Reject, duplicate = false): MutationResult => resultOf(id, { case: "rejected", value: r }, { duplicate });
const retryResult = (id: string, reason: RetryReason, afterMs = 1000, intentSeq = 0n): MutationResult =>
  resultOf(id, { case: "retry", value: create(RetrySchema, { reason, afterMs }) }, { intentSeq });

/** Recorded rejects (07 §7.6): class DEAD_LETTER or CONFLICT, not PERMISSION_DENIED, ACTOR_MISMATCH, KEYSPACE_ARCHIVED, KEYSPACE_DELETED. */
function isRecorded(r: Reject): boolean {
  if (r.errorClass !== ErrorClass.DEAD_LETTER && r.errorClass !== ErrorClass.CONFLICT) return false;
  return r.code !== Code.PERMISSION_DENIED && r.code !== Code.ACTOR_MISMATCH && r.code !== Code.KEYSPACE_ARCHIVED && r.code !== Code.KEYSPACE_DELETED;
}

// ---------------------------------------------------------------------------------------------------------------------
// The server

export class FakeServer {
  readonly appId: string;
  readonly clock: Clock;
  readonly manifests: StaticManifests;
  readonly limits: AppLimits;
  readonly faults: FrameFaults = { dropNext: 0, duplicateNext: 0, reorderNext: false, delayMs: 0, paused: false };
  /** Every call that reached the server, in order: (method, principal sub). A test reads it. */
  readonly calls: { method: Method; sub: string; request: unknown; at: number }[] = [];
  private readonly keyspaces = new Map<string, FakeKeyspace>();
  private readonly clients = new Map<string, string>();
  private readonly sessions = new Set<FakeSession>();
  private injections: Injection[] = [];
  private readonly loads = new Map<string, { entries: KeyEntry[]; offset: number; checkpoint: bigint; epoch: number; manifestVersion: number }>();
  private nextSession = 1;
  private nextToken = 1;
  private suspended = false;
  private heartbeatCancel: (() => void) | undefined;
  private readonly pageSize: number;
  private readonly heartbeatMs: number;
  private readonly maxSessions: number | undefined;
  private principalCounter = 0;

  constructor(opts: FakeOptions) {
    this.appId = opts.appId ?? "app";
    this.clock = opts.clock;
    this.manifests = opts.manifests;
    this.limits = create(AppLimitsSchema, { ...DEFAULT_LIMITS, ...(opts.limits ?? {}) });
    this.pageSize = opts.pageSize ?? 1000;
    this.heartbeatMs = opts.heartbeatMs ?? 0;
    this.maxSessions = opts.maxSessions;
    if (this.heartbeatMs > 0) this.armHeartbeat();
  }

  /** A principal of this app. */
  principal(sub: string, deviceId = `${sub}-device`): Principal {
    return create(PrincipalSchema, { appId: this.appId, sub, kind: PrincipalKind.USER, deviceId, issuer: "https://issuer.test", actorChain: [] });
  }

  /** The `Transport` of a principal. */
  transport(principal: Principal): FakeTransport {
    return new FakeTransport(this, principal);
  }

  // -- scripting --

  /** Injects a behavior into the next `times` calls of a method that match (17 [CONF-4]). */
  inject(method: Method, behavior: Behavior, opts: { times?: number; match?: (req: unknown, principal: Principal) => boolean } = {}): void {
    this.injections.push({ method, match: opts.match, times: opts.times ?? 1, behavior });
  }

  clearInjections(): void {
    this.injections = [];
  }

  /** The next injection for a call; a `result` injection is used up by the mutations that it answers, not by the calls that it sees. */
  private take(method: Method, req: unknown, principal: Principal): Injection | undefined {
    for (const inj of this.injections) {
      if (inj.method !== method || inj.times <= 0) continue;
      if (inj.match !== undefined && !inj.match(req, principal)) continue;
      if (inj.behavior.kind !== "result") inj.times--;
      return inj;
    }
    return undefined;
  }

  /** The app is suspended (12 §12.2.3): every call fails with `PermissionDenied{APP_SUSPENDED}`, and sessions end. */
  suspend(): void {
    this.suspended = true;
    this.closeSessions(CloseReasonEnum.APP_SUSPENDED, "PermissionDenied", mkReject(Code.APP_SUSPENDED));
  }

  resume(): void {
    this.suspended = false;
  }

  // -- keyspaces --

  /** Creates a keyspace of the manifest's current version. */
  createKeyspaceNow(id: string, kind = "kind", manifestVersion = 1): void {
    const k = new FakeKeyspace(id, manifestVersion);
    k.kind = kind;
    this.keyspaces.set(id, k);
  }

  private ks(id: string): FakeKeyspace {
    const k = this.keyspaces.get(id);
    if (k === undefined) throw new Error(`the fake has no keyspace ${id}`);
    return k;
  }

  /** The keyspace's records, for a test. */
  records(id: string): readonly RecordMessage[] {
    return this.ks(id).log;
  }

  head(id: string): bigint {
    return this.ks(id).head;
  }

  epoch(id: string): number {
    return this.ks(id).epoch;
  }

  /** The keyspace's state, for a test. */
  stateOf(id: string): ReplicaState {
    return this.ks(id).state;
  }

  /** The value of a key as the server holds it. */
  value(id: string, key: string): Uint8Array | undefined {
    const row = this.ks(id).state.key(key);
    return row?.value;
  }

  /** The Yjs document that the server holds for a CRDT key. */
  ydoc(id: string, key: string): Y.Doc | undefined {
    return this.ks(id).ydocs.get(key);
  }

  /** Makes the manifest version in force for a keyspace, with a `MANIFEST_ACTIVATED` record. */
  activateManifest(id: string, version: number): void {
    const k = this.ks(id);
    k.manifestVersion = version;
    this.commit(k, this.record(k, RecordKind.SYSTEM, { system: create(SystemEventSchema, { kind: SystemKind.MANIFEST_ACTIVATED, mustUnderstand: false, body: new TextEncoder().encode(JSON.stringify({ version })), encoding: "json/v1" }), principal: this.principal("system") }));
  }

  /** A write by another principal, committed at once as an OP record (a remote edit that the client under test sees on its stream). */
  write(id: string, changes: KeyChange[], opts: { sub?: string; opType?: string; mutationId?: string; overwroteNewer?: number[] } = {}): bigint {
    const k = this.ks(id);
    const rec = this.record(k, RecordKind.OP, { principal: this.principal(opts.sub ?? "other"), mutationId: opts.mutationId ?? `other-${++this.principalCounter}`, opType: opts.opType ?? "op", changes, overwroteNewer: opts.overwroteNewer ?? [] });
    this.commit(k, rec);
    return rec.seq;
  }

  /**
   * Appends a record that the server's own fold is not asked to apply, and delivers it: one that a client must stop at (an
   * unknown kind, a required feature it lacks, an unknown SYSTEM kind that must be understood, 06 [LOG-11]). The server's state is
   * only moved to its seq.
   */
  appendRaw(id: string, init: { kind?: number; requiredFeatures?: readonly string[]; system?: { kind: number; mustUnderstand: boolean }; changes?: KeyChange[] }): bigint {
    const k = this.ks(id);
    const rec = this.record(k, (init.kind ?? RecordKind.OP) as RecordKind, {
      principal: this.principal("other"),
      mutationId: init.kind === RecordKind.SYSTEM || init.system !== undefined ? "" : `other-${++this.principalCounter}`,
      opType: "op",
      changes: init.changes ?? [],
      ...(init.system !== undefined ? { system: create(SystemEventSchema, { kind: init.system.kind as SystemKind, mustUnderstand: init.system.mustUnderstand, body: new Uint8Array(), encoding: "json/v1" }) } : {}),
    });
    rec.requiredFeatures = [...(init.requiredFeatures ?? [])];
    k.log.push(rec);
    k.state.setCursor(rec.seq, k.epoch, k.manifestVersion);
    this.deliver(k);
    return rec.seq;
  }

  /** The cold loads of this keyspace answer `skipped_unsupported = true` (09 [READ-18]). */
  skippedUnsupported = false;

  /** Decides a GATED intent (08 §8.4): appends the DECISION and binds the outcome, which consumes the slot. */
  decide(id: string, intentId: string, d: GateDecision): bigint {
    const k = this.ks(id);
    const p = k.pending.get(intentId);
    const b = k.bindings.get(intentId);
    if (p === undefined || b === undefined) throw new Error(`no pending intent ${intentId}`);
    const row = `abpc.intent/${intentId}`;
    const rowChange = create(KeyChangeSchema, { key: row, kind: ChangeKind.SET, value: new TextEncoder().encode(JSON.stringify({ status: d.status })), encoding: "json/v1" });
    const requested = p.mutation.changes;
    const committed = d.status === "COMMITTED";
    const rec = this.record(k, RecordKind.DECISION, {
      principal: this.principal("gate"),
      mutationId: intentId,
      opType: p.mutation.opType,
      changes: committed ? [...requested, ...(d.effects ?? []), rowChange] : [rowChange],
      decision: create(DecisionSchema, { intentId, status: committed ? DecisionStatus.COMMITTED : DecisionStatus.REJECTED, ...(d.reject !== undefined ? { reject: d.reject } : {}) }),
    });
    this.commit(k, rec);
    k.pending.delete(intentId);
    b.final = true;
    b.result = resultOf(intentId, { case: "decided", value: create(DecidedSchema, { status: committed ? DecisionStatus.COMMITTED : DecisionStatus.REJECTED, decisionSeq: rec.seq, logEpoch: k.epoch, ...(d.reject !== undefined ? { reject: d.reject } : {}) }) }, { intentSeq: b.intentSeq ?? 0n });
    return rec.seq;
  }

  /**
   * A restore of the keyspace to `baseSeq` (07 [MUT-104], 06 §6.9): the log above it is gone, the epoch advances, lane state is
   * dropped, bindings of outcomes above it are voided (or, with `rewindBindings`, simply gone), every intent without a DECISION at
   * or below it ends, and the sessions that follow the keyspace get `KeyspaceDropped{EPOCH_CHANGED}`.
   */
  restore(id: string, baseSeq: bigint, opts: { rewindBindings?: boolean } = {}): void {
    const k = this.ks(id);
    const keep = k.log.filter((r) => r.seq <= baseSeq);
    // rebuild the state and the documents from the kept prefix
    k.state = new ReplicaState();
    k.ydocs = new Map();
    k.lastWriter = new Map();
    k.log = [];
    for (const r of keep) this.replay(k, r);
    k.epoch++;
    k.lanes = new Map();
    const survivingIntents: { id: string; mutation: Mutation; subject: string; intentSeq: bigint }[] = [];
    for (const [mid, b] of [...k.bindings]) {
      const o = b.result.outcome;
      const decidedAtOrBelow = o.case === "decided" && o.value.decisionSeq <= baseSeq;
      if (o.case === "appended" && o.value.seq > baseSeq) {
        k.bindings.delete(mid);
        if (opts.rewindBindings !== true) k.voided.set(mid, { subject: b.subject, opType: b.opType });
      } else if (b.mutation?.lane === Lane.GATED && !decidedAtOrBelow) {
        k.bindings.delete(mid);
        const intentSeq = b.intentSeq ?? 0n;
        const survives = intentSeq !== 0n && intentSeq <= baseSeq && opts.rewindBindings !== true;
        if (survives && b.mutation !== undefined) survivingIntents.push({ id: mid, mutation: b.mutation, subject: b.subject, intentSeq });
        else if (opts.rewindBindings !== true) k.voided.set(mid, { subject: b.subject, opType: b.opType });
      }
    }
    k.pending.clear();
    // 06 [LOG-48]: the first record of the new epoch
    this.commit(k, this.record(k, RecordKind.SYSTEM, { principal: this.principal("system"), system: create(SystemEventSchema, { kind: SystemKind.EPOCH_ADVANCED, mustUnderstand: true, body: new TextEncoder().encode(JSON.stringify({ base_seq: baseSeq.toString() })), encoding: "json/v1" }) }));
    // 07 [MUT-104], 08 [GATE-93]: the intents that survived end with a REJECTED decision STALE{EPOCH}, in the new epoch
    for (const s of survivingIntents) {
      const reject = mkReject(Code.STALE, { stale: { cause: StaleCause.EPOCH } });
      const row = create(KeyChangeSchema, { key: `abpc.intent/${s.id}`, kind: ChangeKind.SET, value: new TextEncoder().encode(JSON.stringify({ status: "REJECTED" })), encoding: "json/v1" });
      const rec = this.record(k, RecordKind.DECISION, { principal: this.principal("gate"), mutationId: s.id, opType: s.mutation.opType, changes: [row], decision: create(DecisionSchema, { intentId: s.id, status: DecisionStatus.REJECTED, reject }) });
      this.commit(k, rec);
      k.bindings.set(s.id, {
        subject: s.subject,
        content: "",
        opType: s.mutation.opType,
        final: true,
        mutation: s.mutation,
        result: resultOf(s.id, { case: "decided", value: create(DecidedSchema, { status: DecisionStatus.REJECTED, decisionSeq: rec.seq, logEpoch: k.epoch, reject }) }),
      });
    }
    for (const s of this.sessions) {
      if (!s.follows.has(id)) continue;
      s.follows.delete(id);
      s.push(this.control({ case: "keyspaceDropped", value: create(ControlFrame_KeyspaceDroppedSchema, { keyspaceId: id, reason: 8 /* EPOCH_CHANGED */ }) }));
    }
  }

  private replay(k: FakeKeyspace, rec: RecordMessage): void {
    const plan = planCrdt(k.state, this.manifests, rec);
    applyRecord(k.state, this.manifests, rec, { supported: () => true });
    k.log.push(rec);
    this.integrate(k, plan);
    for (const i of appliedIndexes(rec)) {
      const c = rec.changes[i];
      if (c !== undefined) k.lastWriter.set(c.key, rec.mutationId);
    }
  }

  private integrate(k: FakeKeyspace, plan: ReturnType<typeof planCrdt>): void {
    for (const item of plan) {
      if (item.verdict === "NOOP_FOR_GOOD") continue;
      let d = k.ydocs.get(item.key);
      if (d === undefined || item.base) {
        d = new Y.Doc({ gc: false });
        k.ydocs.set(item.key, d);
      }
      Y.applyUpdate(d, item.update);
    }
  }

  /** Prunes the log up to seq: a cursor below it gets RESUME_TOO_OLD, and a resume check a cold load (09 [READ-17], [READ-65]). */
  prune(id: string, seq: bigint): void {
    this.ks(id).pruneFloor = seq;
  }

  /** Removes the keyspace from every session's follows without a frame: a following keyspace that heartbeats no longer list (09 [READ-54]). */
  forget(id: string): void {
    for (const s of this.sessions) s.follows.delete(id);
  }

  freeze(id: string): void {
    const k = this.ks(id);
    k.frozen = true;
    for (const s of this.sessions) {
      if (s.follows.delete(id)) s.push(this.control({ case: "keyspaceDropped", value: create(ControlFrame_KeyspaceDroppedSchema, { keyspaceId: id, reason: 5 /* HISTORY_DIVERGED */ }) }));
    }
  }

  reopen(id: string): void {
    this.ks(id).frozen = false;
  }

  /** Archives the keyspace (12 §12.4): no action is accepted; sessions get `KeyspaceDropped{ARCHIVED}`. */
  archive(id: string, successors: SuccessorRule[] = []): void {
    const k = this.ks(id);
    k.lifecycle = KeyspaceLifecycle.ARCHIVED;
    k.successors = successors;
    this.dropSessions(id, 6 /* ARCHIVED */, successors);
  }

  /** Splits the keyspace (12 §12.5): the successors exist and are seeded from this keyspace's state by `successors`' prefixes. */
  split(id: string, successors: SuccessorRule[]): void {
    const k = this.ks(id);
    k.lifecycle = KeyspaceLifecycle.SPLIT;
    k.successors = successors;
    this.dropSessions(id, 3 /* MOVED */, successors);
  }

  deleteKeyspace(id: string): void {
    const k = this.ks(id);
    k.deleted = true;
    k.bindings.clear();
    k.voided.clear();
    this.dropSessions(id, 7 /* DELETED */, []);
  }

  private dropSessions(id: string, reason: number, successors: SuccessorRule[]): void {
    for (const s of this.sessions) {
      if (s.follows.delete(id)) s.push(this.control({ case: "keyspaceDropped", value: create(ControlFrame_KeyspaceDroppedSchema, { keyspaceId: id, reason: reason as DropReason, successors }) }));
    }
  }

  /** Drops a keyspace from every session with any reason, a value that no client knows included. */
  drop(id: string, reason: number): void {
    this.dropSessions(id, reason, []);
  }

  // -- sessions --

  /** Ends every session cleanly, with `Reconnect`. */
  reconnect(retryAfterMs = 0): void {
    for (const s of this.sessions) {
      s.push(this.control({ case: "reconnect", value: create(ControlFrame_ReconnectSchema, { retryAfterMs }) }));
      s.push(END); // after the frames that are already queued: the stream ends with status OK (09 [READ-59])
    }
  }

  /** Ends every session: a hard disconnect; frames in flight are lost (17 [CONF-4]). */
  disconnect(error?: RpcError): void {
    for (const s of [...this.sessions]) s.end(error);
  }

  /** Ends every session with `Closing{reason}` and the stream status of 09 [READ-60]. */
  closeSessions(reason: CloseReason, status: RpcStatus, detail?: Reject): void {
    for (const s of [...this.sessions]) {
      s.push(this.control({ case: "closing", value: create(ControlFrame_ClosingSchema, { reason, message: "closed by the fake server" }) }));
      s.push({ error: new RpcError(status, "closing", detail !== undefined ? { reject: detail } : {}) });
    }
  }

  /** Sends a frame to every session, as it is. */
  sendFrame(frame: SubscribeResponse): void {
    for (const s of this.sessions) s.push(frame);
  }

  /** A control frame that no client knows, or any other that a test builds. */
  control(c: ControlFrame["control"]): SubscribeResponse {
    return create(SubscribeResponseSchema, { frame: { case: "control", value: create(ControlFrameSchema, { control: c }) } });
  }

  /** Releases the frames that `faults.paused` held, and clears it. */
  resumeDelivery(): void {
    this.faults.paused = false;
    for (const s of this.sessions) {
      const held = s.heldFrames;
      s.heldFrames = [];
      for (const f of held) {
        s.push(f);
        if (f.frame.case === "records") {
          const fs = s.follows.get(f.frame.value.keyspaceId);
          if (fs !== undefined && f.frame.value.cursor > fs.pushed) fs.pushed = f.frame.value.cursor;
        }
      }
    }
  }

  /** The number of live sessions. */
  get sessionCount(): number {
    return this.sessions.size;
  }

  /** Sends a heartbeat to every session now (09 [READ-54]). */
  heartbeat(): void {
    for (const s of this.sessions) s.push(this.heartbeatFrame(s));
  }

  private heartbeatFrame(s: FakeSession): SubscribeResponse {
    const cursors = [];
    const wms = [];
    for (const [ksId, fs] of s.follows) {
      const k = this.keyspaces.get(ksId);
      if (k === undefined) continue;
      cursors.push(create(HeartbeatFrame_CursorSchema, { keyspaceId: ksId, cursorSeq: fs.pushed, headSeq: k.head, logEpoch: fs.epoch }));
      if (s.clientId !== "") wms.push(...this.laneWatermarks(k, s.clientId));
    }
    return create(SubscribeResponseSchema, { frame: { case: "heartbeat", value: create(HeartbeatFrameSchema, { cursors, serverTime: timestampFromMs(this.clock.now()), laneWatermarks: wms }) } });
  }

  private armHeartbeat(): void {
    this.heartbeatCancel = this.clock.setTimer(this.heartbeatMs, () => {
      this.heartbeat();
      this.armHeartbeat();
    });
  }

  stop(): void {
    this.heartbeatCancel?.();
    for (const s of [...this.sessions]) s.close();
    this.sessions.clear();
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Records

  private record(
    k: FakeKeyspace,
    kind: RecordKind,
    init: { principal: Principal; mutationId?: string; opType?: string; changes?: KeyChange[]; overwroteNewer?: number[]; decision?: ReturnType<typeof create<typeof DecisionSchema>>; system?: ReturnType<typeof create<typeof SystemEventSchema>>; authoredAt?: Mutation["authoredAt"] },
  ): RecordMessage {
    const seq = k.head + 1n;
    const rec = create(RecordSchema, {
      seq,
      logEpoch: k.epoch,
      kind,
      mutationId: init.mutationId ?? "",
      opType: init.opType ?? "",
      principal: init.principal,
      serverTime: timestampFromMs(this.clock.now()),
      ...(init.authoredAt !== undefined ? { authoredAt: init.authoredAt } : {}),
      manifestVersion: k.manifestVersion,
      requiredFeatures: [],
      changes: init.changes ?? [],
      overwroteNewer: init.overwroteNewer ?? [],
      ...(init.decision !== undefined ? { decision: init.decision } : {}),
      ...(init.system !== undefined ? { system: init.system } : {}),
    });
    rec.recordHash = sha256(new TextEncoder().encode(`${k.epoch}:${seq}:${rec.mutationId}:${rec.kind}`));
    return rec;
  }

  private commit(k: FakeKeyspace, rec: RecordMessage): void {
    this.replay(k, rec);
    this.deliver(k);
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Delivery (09 §9.5)

  private emit(s: FakeSession, frame: SubscribeResponse): void {
    const records = frame.frame.case === "records";
    if (!records) {
      s.push(frame);
      return;
    }
    const push = (f: SubscribeResponse): void => {
      s.push(f);
      if (f.frame.case === "records") {
        const fs = s.follows.get(f.frame.value.keyspaceId);
        if (fs !== undefined && f.frame.value.cursor > fs.pushed) fs.pushed = f.frame.value.cursor;
      }
    };
    if (this.faults.paused) {
      s.heldFrames.push(frame);
      return;
    }
    if (this.faults.dropNext > 0) {
      this.faults.dropNext--;
      return;
    }
    if (this.faults.duplicateNext > 0) {
      this.faults.duplicateNext--;
      push(frame);
      push(frame);
      return;
    }
    if (this.faults.reorderNext) {
      if (s.reorderHeld === undefined) {
        s.reorderHeld = frame;
        return;
      }
      const held = s.reorderHeld;
      s.reorderHeld = undefined;
      this.faults.reorderNext = false;
      push(frame);
      push(held);
      return;
    }
    if (this.faults.delayMs > 0) {
      this.clock.setTimer(this.faults.delayMs, () => push(frame));
      return;
    }
    push(frame);
  }

  /** Sends the new records of a keyspace to every session that follows it. */
  private deliver(k: FakeKeyspace): void {
    for (const s of this.sessions) this.pump(s, k);
  }

  private pump(s: FakeSession, k: FakeKeyspace): void {
    const fs = s.follows.get(k.id);
    if (fs === undefined || fs.epoch !== k.epoch) return;
    const recs = k.log.filter((r) => r.seq > fs.watermark);
    if (recs.length === 0) return;
    const cursor = (recs[recs.length - 1] as RecordMessage).seq;
    const frame = create(SubscribeResponseSchema, { frame: { case: "records", value: create(RecordsFrameSchema, { keyspaceId: k.id, prevCursor: fs.watermark, cursor, records: recs }) } });
    fs.watermark = cursor;
    this.emit(s, frame);
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Calls

  private async prologue(method: Method, req: unknown, principal: Principal): Promise<Injection | undefined> {
    this.calls.push({ method, sub: principal.sub, request: req, at: this.clock.now() });
    const inj = this.take(method, req, principal);
    const b = inj?.behavior;
    if (b?.kind === "status") throw new RpcError(b.status, b.message ?? "injected", { ...(b.reject !== undefined ? { reject: b.reject } : {}), ...(b.retry !== undefined ? { retry: b.retry } : {}) });
    if (b?.kind === "delay") await new Promise<void>((r) => this.clock.setTimer(b.ms, r));
    if (this.suspended && method !== "getManifest") throw new RpcError("PermissionDenied", "the app is suspended", { reject: mkReject(Code.APP_SUSPENDED) });
    return inj;
  }

  async getManifest(principal: Principal, req: GetManifestRequest): Promise<GetManifestResponse> {
    await this.prologue("getManifest", req, principal);
    const version = req.version === 0 ? 1 : req.version;
    return create(GetManifestResponseSchema, { version, encoding: "json/v1" });
  }

  async createKeyspace(principal: Principal, req: CreateKeyspaceRequest): Promise<CreateKeyspaceResponse> {
    await this.prologue("createKeyspace", req, principal);
    const have = this.keyspaces.get(req.keyspaceId);
    if (have !== undefined) {
      if (have.kind !== req.kind) throw new RpcError("AlreadyExists", "another kind", { reject: mkReject(Code.IDEMPOTENCY_KEY_REUSED) });
      return create(CreateKeyspaceResponseSchema, { created: false, logEpoch: have.epoch });
    }
    this.createKeyspaceNow(req.keyspaceId, req.kind);
    return create(CreateKeyspaceResponseSchema, { created: true, logEpoch: 1 });
  }

  async refreshSession(principal: Principal, req: RefreshSessionRequest): Promise<RefreshSessionResponse> {
    await this.prologue("refreshSession", req, principal);
    return create(RefreshSessionResponseSchema, { expiresAt: timestampFromMs(this.clock.now() + 3600_000), droppedKeyspaceIds: [] });
  }

  // -- GetMutationResult(s) (07 [MUT-53]) --

  private resultFor(principal: Principal, keyspaceId: string, id: string): GetMutationResultResponse {
    const k = this.keyspaces.get(keyspaceId);
    const none = create(GetMutationResultResponseSchema, { known: false, voided: false });
    if (k === undefined || k.deleted) return none;
    const subject = subjectOf(principal);
    const b = k.bindings.get(id);
    if (k.frozen) {
      if ((b !== undefined && b.subject === subject) || k.voided.get(id)?.subject === subject) throw new RpcError("FailedPrecondition", "frozen", { reject: mkReject(Code.HISTORY_DIVERGED) });
      return none;
    }
    if (b !== undefined && b.subject === subject) return create(GetMutationResultResponseSchema, { known: true, result: create(MutationResultSchema, { ...b.result, duplicate: true }), voided: false });
    if (b === undefined && k.voided.get(id)?.subject === subject) return create(GetMutationResultResponseSchema, { known: false, voided: true });
    return none;
  }

  async getMutationResult(principal: Principal, req: GetMutationResultRequest): Promise<GetMutationResultResponse> {
    await this.prologue("getMutationResult", req, principal);
    return this.resultFor(principal, req.keyspaceId, req.mutationId);
  }

  async getMutationResults(principal: Principal, req: GetMutationResultsRequest): Promise<GetMutationResultsResponse> {
    await this.prologue("getMutationResults", req, principal);
    if (req.mutationIds.length === 0 || req.mutationIds.length > 100) throw new RpcError("InvalidArgument", "1 to 100 ids", { reject: mkReject(Code.INVALID_ENVELOPE) });
    return create(GetMutationResultsResponseSchema, { results: req.mutationIds.map((id) => this.resultFor(principal, req.keyspaceId, id)) });
  }

  async cancelIntent(principal: Principal, req: CancelIntentRequest): Promise<CancelIntentResponse> {
    await this.prologue("cancelIntent", req, principal);
    const k = this.ks(req.keyspaceId);
    const b = k.bindings.get(req.intentId);
    if (b !== undefined && b.subject === subjectOf(principal)) {
      if (!b.final && k.pending.has(req.intentId)) {
        this.decide(req.keyspaceId, req.intentId, { status: "REJECTED", reject: mkReject(Code.CANCELLED) });
      }
      return create(CancelIntentResponseSchema, { result: create(MutationResultSchema, { ...(k.bindings.get(req.intentId) as Binding).result, duplicate: true }) });
    }
    const reject = mkReject(Code.CANCELLED);
    k.bindings.set(req.intentId, { subject: subjectOf(principal), content: "", opType: "", result: rejectedResult(req.intentId, reject), final: true });
    return create(CancelIntentResponseSchema, { result: rejectedResult(req.intentId, reject) });
  }

  // -- LoadState (09 §9.2) --

  private entriesOf(k: FakeKeyspace): KeyEntry[] {
    const out: KeyEntry[] = [];
    for (const key of k.state.keyNames()) {
      const row = k.state.key(key);
      if (row === undefined) continue;
      const crdt = row.rule === "crdt" && row.state === StateValue;
      out.push(
        create(KeyEntrySchema, {
          key,
          value: crdt ? Y.encodeStateAsUpdate(k.ydocs.get(key) ?? new Y.Doc()) : row.value,
          isNull: row.state === StateNull,
          encoding: row.encoding,
          version: row.version,
          changedAt: timestampFromMs(Number(row.changedAt.seconds) * 1000 + Math.floor(row.changedAt.nanos / 1e6)),
          changedBy: row.changedBy,
          crdtCoveredSeq: crdt ? row.version : 0n,
        }),
      );
    }
    for (const key of k.state.tombstoneKeys()) {
      const t = k.state.tombstone(key);
      if (t === undefined) continue;
      out.push(create(KeyEntrySchema, { key, deleted: true, version: t.version, redacted: t.mark, staged: t.kind === "staged" }));
    }
    return out.sort((a, b) => compareUtf8(a.key, b.key));
  }

  private laneWatermarks(k: FakeKeyspace, clientId: string): ReturnType<typeof create<typeof LaneWatermarkSchema>>[] {
    const slots = k.lanes.get(clientId);
    if (slots === undefined) return [];
    const out = [];
    for (const [lane, map] of [[Lane.SEQUENCED, slots.seq], [Lane.GATED, slots.gated]] as const) {
      if (map.size === 0) continue;
      out.push(create(LaneWatermarkSchema, { keyspaceId: k.id, lane, processedClientSeq: BigInt(this.processed(k, map)), clientId, logEpoch: k.epoch }));
    }
    return out;
  }

  private processed(k: FakeKeyspace, map: Map<number, string>): number {
    let n = 0;
    for (;;) {
      const id = map.get(n + 1);
      if (id === undefined || k.bindings.get(id)?.final !== true) return n;
      n++;
    }
  }

  async loadState(principal: Principal, req: LoadStateRequest): Promise<LoadStateResponse> {
    await this.prologue("loadState", req, principal);
    const cur = req.cursor;
    const id = cur?.keyspaceId ?? "";
    const k = this.keyspaces.get(id);
    if (k === undefined) throw new RpcError("NotFound", "no such keyspace", { reject: mkReject(Code.KEYSPACE_NOT_FOUND) });
    if (k.deleted) throw new RpcError("NotFound", "deleted", { reject: mkReject(Code.KEYSPACE_DELETED) });
    if (k.frozen) throw new RpcError("FailedPrecondition", "frozen", { reject: mkReject(Code.HISTORY_DIVERGED) });
    const subject = subjectOf(principal);
    void subject;
    const base = { logEpoch: k.epoch, headSeq: k.head, manifestVersion: k.manifestVersion, limits: this.limits, kind: k.kind, lifecycle: k.lifecycle, successors: k.successors, serverInfo: create(ServerInfoSchema, { protocolName: "abpc.v2", revision: "2.0", features: [], build: "fake" }) };
    const wms = req.clientId !== "" ? this.laneWatermarks(k, req.clientId) : [];
    // a page of a load in progress
    if (req.pageToken !== "") {
      const ld = this.loads.get(req.pageToken);
      if (ld === undefined) throw new RpcError("FailedPrecondition", "the page token expired");
      const end = Math.min(ld.entries.length, ld.offset + (req.pageSize === 0 ? this.pageSize : Math.min(req.pageSize, 10000)));
      const entries = ld.entries.slice(ld.offset, end);
      const last = end >= ld.entries.length;
      const token = last ? "" : req.pageToken;
      ld.offset = end;
      if (last) this.loads.delete(req.pageToken);
      return create(LoadStateResponse_, { ...base, checkpointSeq: ld.checkpoint, logEpoch: ld.epoch, manifestVersion: ld.manifestVersion, entries, nextPageToken: token, resumed: false, laneWatermarks: wms, skippedUnsupported: this.skippedUnsupported });
    }
    const afterSeq = cur?.afterSeq ?? 0n;
    if (afterSeq > 0n) {
      const rec = k.log.find((r) => r.seq === afterSeq);
      const hashOk = cur?.afterRecordHash === undefined || cur.afterRecordHash.length === 0 || (rec !== undefined && toHex(rec.recordHash) === toHex(cur.afterRecordHash));
      if (cur?.logEpoch === k.epoch && afterSeq <= k.head && afterSeq >= k.pruneFloor && hashOk) {
        return create(LoadStateResponse_, { ...base, checkpointSeq: afterSeq, entries: [], nextPageToken: "", resumed: true, laneWatermarks: wms });
      }
    }
    // a cold load: the state at one checkpoint, as pages
    const all = this.entriesOf(k);
    const size = req.pageSize === 0 ? this.pageSize : Math.min(req.pageSize, 10000);
    const first = all.slice(0, size);
    const more = all.length > size;
    let token = "";
    if (more) {
      token = `page-${this.nextToken++}`;
      this.loads.set(token, { entries: all, offset: size, checkpoint: k.head, epoch: k.epoch, manifestVersion: k.manifestVersion });
    }
    return create(LoadStateResponse_, { ...base, checkpointSeq: k.head, entries: first, nextPageToken: token, resumed: false, laneWatermarks: wms, skippedUnsupported: this.skippedUnsupported });
  }

  // -- Subscribe (09 §9.4 to §9.7) --

  subscribe(principal: Principal, req: SubscribeRequest, o: CallOptions | undefined): AsyncIterable<SubscribeResponse> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;
    return {
      [Symbol.asyncIterator]: () => {
        const gen = self.stream(principal, req, o);
        return gen[Symbol.asyncIterator]();
      },
    };
  }

  private async *stream(principal: Principal, req: SubscribeRequest, o: CallOptions | undefined): AsyncGenerator<SubscribeResponse> {
    await this.prologue("subscribe", req, principal);
    if (this.maxSessions !== undefined && [...this.sessions].filter((s) => subjectOf(s.principal) === subjectOf(principal)).length >= this.maxSessions) {
      throw new RpcError("ResourceExhausted", "too many sessions", { reject: mkReject(Code.QUOTA_EXCEEDED, { details: { quota: "max_sessions_per_principal" } }) });
    }
    if (req.clientId !== "") {
      const bound = this.clients.get(req.clientId);
      if (bound !== undefined && bound !== subjectOf(principal)) throw new RpcError("PermissionDenied", "actor mismatch", { reject: mkReject(Code.ACTOR_MISMATCH) });
      this.clients.set(req.clientId, subjectOf(principal));
    }
    const session = new FakeSession(`session-${this.nextSession++}`, principal, req.clientId);
    this.sessions.add(session);
    const hb = this.heartbeatMs > 0 ? this.heartbeatMs : 15_000;
    session.push(this.control({ case: "subscribed", value: create(ControlFrame_SubscribedSchema, { sessionId: session.id, limits: this.limits, serverInfo: create(ServerInfoSchema, { protocolName: "abpc.v2" }), heartbeatIntervalMs: hb }) }));
    for (const c of req.keyspaces) this.addFollow(session, c);
    try {
      for (;;) {
        const item = await session.next(o?.signal);
        if (item === END) return;
        if ("error" in item) throw item.error;
        yield item;
      }
    } finally {
      session.close();
      this.sessions.delete(session);
    }
  }

  /** The start of a keyspace on a session (09 [READ-32], [READ-65]): added, or dropped with the reason that applies. */
  private addFollow(s: FakeSession, c: KeyspaceCursor): void {
    const drop = (reason: number, successors: SuccessorRule[] = []): void => {
      s.push(this.control({ case: "keyspaceDropped", value: create(ControlFrame_KeyspaceDroppedSchema, { keyspaceId: c.keyspaceId, reason: reason as DropReason, successors }) }));
    };
    const k = this.keyspaces.get(c.keyspaceId);
    if (k === undefined) return drop(10 /* NOT_FOUND */);
    if (k.deleted) return drop(7);
    if (k.frozen) return drop(5);
    if (k.lifecycle === KeyspaceLifecycle.ARCHIVED) return drop(6, k.successors);
    if (k.lifecycle === KeyspaceLifecycle.SPLIT) return drop(3, k.successors);
    if (c.logEpoch !== k.epoch) return drop(8);
    const rec = k.log.find((r) => r.seq === c.afterSeq);
    const hashOk = c.afterRecordHash.length === 0 || (rec !== undefined && toHex(rec.recordHash) === toHex(c.afterRecordHash));
    if (c.afterSeq > k.head || c.afterSeq < k.pruneFloor || !hashOk) return drop(4 /* RESUME_TOO_OLD */);
    s.follows.set(c.keyspaceId, { watermark: c.afterSeq, pushed: c.afterSeq, epoch: k.epoch });
    s.push(this.control({ case: "keyspaceAdded", value: create(ControlFrame_KeyspaceAddedSchema, { cursor: create(KeyspaceCursorSchema, { keyspaceId: c.keyspaceId, afterSeq: c.afterSeq, logEpoch: k.epoch }) }) }));
    this.pump(s, k);
  }

  async updateSubscription(principal: Principal, req: UpdateSubscriptionRequest): Promise<UpdateSubscriptionResponse> {
    await this.prologue("updateSubscription", req, principal);
    const s = [...this.sessions].find((x) => x.id === req.sessionId);
    if (s === undefined) throw new RpcError("NotFound", "no such session");
    if (req.clientId !== "") {
      const bound = this.clients.get(req.clientId);
      if (bound !== undefined && bound !== subjectOf(principal)) throw new RpcError("PermissionDenied", "actor mismatch", { reject: mkReject(Code.ACTOR_MISMATCH) });
      this.clients.set(req.clientId, subjectOf(principal));
      s.clientId = req.clientId;
    }
    for (const id of req.remove) s.follows.delete(id);
    const rejected = [];
    for (const c of req.add) {
      if (!this.keyspaces.has(c.keyspaceId)) {
        rejected.push(create(UpdateSubscriptionResponse_RejectionSchema, { keyspaceId: c.keyspaceId, outcome: { case: "reject", value: mkReject(Code.KEYSPACE_NOT_FOUND) } }));
        continue;
      }
      this.addFollow(s, c);
    }
    return create(UpdateSubscriptionResponseSchema, { rejected });
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Mutate (07)

  async mutate(principal: Principal, req: MutateRequest): Promise<MutateResponse> {
    const inj = await this.prologue("mutate", req, principal);
    const resp = this.doMutate(principal, req, inj);
    if (inj?.behavior.kind === "lose") throw new RpcError(inj.behavior.status ?? "Unavailable", "the response was lost");
    return resp;
  }

  private doMutate(principal: Principal, req: MutateRequest, inj: Injection | undefined): MutateResponse {
    const ms = req.mutations;
    const results: MutationResult[] = new Array<MutationResult>(ms.length);
    const passed = new Set<string>();
    // 07 [MUT-56], [MUT-38]: the SEQUENCED lanes in client_seq order first, then the GATED mutations in request order
    const groups = new Map<string, number[]>();
    const gated: number[] = [];
    ms.forEach((m, i) => {
      if (m.lane !== Lane.SEQUENCED) gated.push(i);
      else {
        const key = `${m.keyspaceId}|${m.clientId}`;
        const g = groups.get(key) ?? [];
        g.push(i);
        groups.set(key, g);
      }
    });
    for (const g of groups.values()) {
      g.sort((a, b) => Number((ms[a] as Mutation).clientSeq - (ms[b] as Mutation).clientSeq) || a - b);
      let stopped = false;
      for (const i of g) {
        const m = ms[i] as Mutation;
        if (stopped) {
          results[i] = retryResult(m.mutationId, RetryReason.NOT_PROCESSED, 100);
          continue;
        }
        const out = this.processOne(principal, m, inj);
        results[i] = out.result;
        if (out.passed) passed.add(`${m.keyspaceId}|${m.lane}|${m.clientId}`);
        const o = out.result.outcome;
        const cont = o.case === "appended" || (o.case === "rejected" && o.value.code === Code.QUOTA_EXCEEDED);
        if (!cont) stopped = true;
      }
    }
    for (const i of gated) {
      const m = ms[i] as Mutation;
      const out = this.processOne(principal, m, inj);
      results[i] = out.result;
      if (out.passed) passed.add(`${m.keyspaceId}|${m.lane}|${m.clientId}`);
    }
    // 07 [MUT-7]: one LaneWatermark for every lane the request touched
    const wms = [];
    const seen = new Set<string>();
    for (const m of ms) {
      if (m.clientId === "") continue;
      const key = `${m.keyspaceId}|${m.lane}|${m.clientId}`;
      if (seen.has(key)) continue;
      seen.add(key);
      const k = this.keyspaces.get(m.keyspaceId);
      let processed = 0;
      let epoch = 0;
      if (k !== undefined && passed.has(key)) {
        const slots = k.lanes.get(m.clientId);
        processed = slots === undefined ? 0 : this.processed(k, m.lane === Lane.SEQUENCED ? slots.seq : slots.gated);
        epoch = k.epoch;
      }
      wms.push(create(LaneWatermarkSchema, { keyspaceId: m.keyspaceId, lane: m.lane, processedClientSeq: BigInt(processed), clientId: m.clientId, logEpoch: epoch }));
    }
    return create(MutateResponseSchema, { results, watermarks: wms });
  }

  private slotsOf(k: FakeKeyspace, clientId: string): LaneSlots {
    let s = k.lanes.get(clientId);
    if (s === undefined) {
      s = { seq: new Map(), gated: new Map() };
      k.lanes.set(clientId, s);
    }
    return s;
  }

  /** Processes one mutation; `passed` says that the pair (keyspace, lane) passed the binding check (07 [MUT-7]). */
  private processOne(principal: Principal, m: Mutation, injection: Injection | undefined): { result: MutationResult; passed: boolean } {
    const inj = injection?.behavior;
    const id = m.mutationId;
    const subject = subjectOf(principal);
    // injected results (17 [CONF-4])
    if (inj?.kind === "result") {
      const injected = inj.result(m);
      if (injected !== undefined) {
        if (injection !== undefined) injection.times--;
        const k = this.keyspaces.get(m.keyspaceId);
        if (inj.recorded === true && k !== undefined) this.bindRecorded(k, principal, m, injected);
        return { result: injected, passed: k !== undefined };
      }
    }
    const k = this.keyspaces.get(m.keyspaceId);
    const rej = (r: Reject, passed = false): { result: MutationResult; passed: boolean } => ({ result: rejectedResult(id, r), passed });
    if (k === undefined) return rej(mkReject(Code.KEYSPACE_NOT_FOUND));
    if (k.deleted) return rej(mkReject(Code.KEYSPACE_DELETED));
    if (k.lifecycle !== KeyspaceLifecycle.ACTIVE) return rej(mkReject(Code.KEYSPACE_ARCHIVED, { details: { lifecycle: k.lifecycle === KeyspaceLifecycle.SPLIT ? "SPLIT" : "ARCHIVED" }, successors: this.successorsFor(k, m) }));
    if (k.seeding) return { result: retryResult(id, RetryReason.KEYSPACE_SEEDING, 1000), passed: false };
    if (k.frozen) return rej(mkReject(Code.HISTORY_DIVERGED));
    const manifest = this.manifests.get(k.manifestVersion);
    const op = manifest?.opType(m.opType);
    // 07 Table 7-1: the envelope
    if (m.mutationId === "") return rej(mkReject(Code.INVALID_ENVELOPE, { message: "no mutation id" }));
    if (m.clientId !== "" && m.clientSeq === 0n) return rej(mkReject(Code.INVALID_ENVELOPE, { message: "client_seq 0" }));
    if (op === undefined) return rej(mkReject(Code.UNKNOWN_OP_TYPE));
    if ((op.lane === "GATED") !== (m.lane === Lane.GATED)) return rej(mkReject(Code.INVALID_ENVELOPE, { message: "lane mismatch" }));
    // idempotency (07 §7.6)
    const content = toHex(encodeContent(m));
    const bound = k.bindings.get(id);
    const slots = m.clientId === "" ? undefined : this.slotsOf(k, m.clientId);
    const laneMap = slots === undefined ? undefined : m.lane === Lane.SEQUENCED ? slots.seq : slots.gated;
    // the client_id is bound to one subject (10 [AUTH-20])
    if (m.clientId !== "") {
      const owner = this.clients.get(m.clientId);
      if (owner !== undefined && owner !== subject) return rej(mkReject(Code.ACTOR_MISMATCH));
    }
    if (bound !== undefined) {
      if (bound.subject !== subject) return rej(mkReject(Code.ID_CONFLICT));
      if (bound.content !== "" && bound.content !== content) return rej(mkReject(Code.IDEMPOTENCY_KEY_REUSED));
      if (m.clientId !== "") this.clients.set(m.clientId, subject);
      // 07 [MUT-32], [MUT-33]: the lane checks come before the stored outcome
      if (laneMap !== undefined) {
        const at = laneMap.get(Number(m.clientSeq));
        if (at !== undefined && at !== id) return rej(mkReject(Code.CLIENT_FORKED), true);
        if (at === undefined) laneMap.set(Number(m.clientSeq), id); // 07 [MUT-50]: a duplicate binds its slot
      }
      const stored = create(MutationResultSchema, { ...bound.result, duplicate: true });
      return { result: stored, passed: true };
    }
    const voided = k.voided.get(id);
    if (voided !== undefined) {
      if (voided.subject !== subject) return rej(mkReject(Code.ID_CONFLICT));
      if (!(op.spend && voided.opType === m.opType)) {
        // 07 [MUT-105]: recorded as Reject{STALE, EPOCH}, never applied
        const reject = mkReject(Code.STALE, { stale: { cause: StaleCause.EPOCH } });
        k.voided.delete(id);
        this.bindRecorded(k, principal, m, rejectedResult(id, reject));
        return { result: rejectedResult(id, reject), passed: true };
      }
      k.voided.delete(id);
    }
    // 07 [MUT-30]: a client_id is bound at its first use
    if (m.clientId !== "") this.clients.set(m.clientId, subject);
    // 07 [MUT-31], [MUT-32], [MUT-33]: the lanes
    if (laneMap !== undefined && slots !== undefined) {
      if (m.lane === Lane.SEQUENCED) {
        if (slots.seq.size === 0 && slots.gated.size === 0 && m.clientSeq > 1n) return rej(mkReject(Code.CLIENT_UNKNOWN), false);
        const l = this.processed(k, slots.seq);
        const at = laneMap.get(Number(m.clientSeq));
        if (at !== undefined && at !== id) return rej(mkReject(Code.CLIENT_FORKED), true);
        if (Number(m.clientSeq) > l + 1) return { result: retryResult(id, RetryReason.OUT_OF_ORDER, 100), passed: true };
        if (Number(m.clientSeq) <= l && at === undefined) return rej(mkReject(Code.CLIENT_FORKED), true);
      } else {
        const at = laneMap.get(Number(m.clientSeq));
        if (at !== undefined && at !== id) return rej(mkReject(Code.CLIENT_FORKED), true);
      }
    }
    // the base epoch (05 [STATE-81]): not for a spend action
    if (m.baseLogEpoch !== 0 && m.baseLogEpoch !== k.epoch && !op.spend) {
      return this.final(k, principal, m, content, mkReject(Code.STALE, { stale: { cause: StaleCause.EPOCH, currentSeq: k.head } }));
    }
    const err = this.checkChanges(k, m, manifest);
    if (err !== undefined) return this.final(k, principal, m, content, err);
    const stale = this.checkPreconditions(k, m);
    if (stale !== undefined) return this.final(k, principal, m, content, stale);
    if (m.lane === Lane.GATED) return this.registerIntent(k, principal, m, content, laneMap);
    // 07 §7.8: the SEQUENCED commit
    return this.commitOp(k, principal, m, content, laneMap);
  }

  private successorsFor(k: FakeKeyspace, m: Mutation): SuccessorRule[] {
    const out: SuccessorRule[] = [];
    for (const c of m.changes) {
      let best: SuccessorRule | undefined;
      for (const r of k.successors) if (r.prefix !== "" && c.key.startsWith(r.prefix) && (best === undefined || r.prefix.length > best.prefix.length)) best = r;
      best ??= k.successors.find((r) => r.prefix === "");
      if (best !== undefined && !out.includes(best)) out.push(best);
    }
    return out.slice(0, 2);
  }

  private bindRecorded(k: FakeKeyspace, principal: Principal, m: Mutation, result: MutationResult): void {
    k.bindings.set(m.mutationId, { subject: subjectOf(principal), content: toHex(encodeContent(m)), opType: m.opType, result, final: true, mutation: m });
    if (m.clientId !== "") {
      const slots = this.slotsOf(k, m.clientId);
      (m.lane === Lane.SEQUENCED ? slots.seq : slots.gated).set(Number(m.clientSeq), m.mutationId);
    }
  }

  /** A final reject: recorded when 07 §7.6 says so (the slot is consumed), unrecorded otherwise. */
  private final(k: FakeKeyspace, principal: Principal, m: Mutation, _content: string, r: Reject): { result: MutationResult; passed: boolean } {
    const result = rejectedResult(m.mutationId, r);
    if (isRecorded(r)) this.bindRecorded(k, principal, m, result);
    return { result, passed: true };
  }

  private checkChanges(k: FakeKeyspace, m: Mutation, manifest: ReturnType<StaticManifests["get"]>): Reject | undefined {
    if (manifest === undefined) return mkReject(Code.UNKNOWN_OP_TYPE);
    const lim = this.limits;
    if (m.changes.length === 0) return mkReject(Code.INVALID_ENVELOPE, { message: "no change" });
    if (m.changes.length > lim.maxChangesPerAction) return mkReject(Code.PAYLOAD_TOO_LARGE);
    const seen = new Set<string>();
    const top = new VersionTopology(this.manifests, k.manifestVersion);
    const deleted: string[] = [];
    for (let i = 0; i < m.changes.length; i++) {
      const c = m.changes[i] as KeyChange;
      if (seen.has(c.key)) return mkReject(Code.INVALID_ENVELOPE, { message: "a key twice", changeIndex: i });
      seen.add(c.key);
      if (c.value.length > lim.maxValueBytes || c.update.length > lim.maxValueBytes) return mkReject(Code.PAYLOAD_TOO_LARGE, { changeIndex: i });
      if (isReservedKey(c.key)) continue;
      const tpl = manifest.template(k.manifestVersion, c.key);
      if (tpl === undefined) return mkReject(Code.INVALID_KEY, { changeIndex: i });
      if (!kindFits(tpl.rule, c.kind)) return mkReject(Code.SCHEMA_INVALID, { changeIndex: i });
      if (c.kind === ChangeKind.CRDT_UPDATE) {
        try {
          checkWellFormed(c.update, manifest.crdtRoots(c.key));
        } catch {
          return mkReject(Code.SCHEMA_INVALID, { changeIndex: i });
        }
      }
      if (c.kind === ChangeKind.DELETE && tpl.entity) deleted.push(c.key);
      // 05 [STATE-46]: a child of an entity that is deleted, or never was created
      if (c.kind !== ChangeKind.DELETE) {
        for (const a of top.ancestors(c.key)) {
          if (k.state.tombstone(a)?.kind === "entity" && k.state.key(a) === undefined && !deleted.includes(a)) {
            return mkReject(Code.STALE, { stale: { key: a, currentSeq: k.head, cause: StaleCause.PARENT_DELETED }, changeIndex: i });
          }
        }
        if (tpl.entity && k.state.tombstone(c.key)?.kind === "entity") return mkReject(Code.TOMBSTONED, { changeIndex: i });
      }
      if (c.kind === ChangeKind.CAS) {
        const ex = c.expected?.expect;
        const cur = k.state.key(c.key);
        let ok = true;
        if (ex?.case === "version") ok = k.state.effectiveVersion(top, c.key) === ex.value;
        else if (ex?.case === "value") ok = cur !== undefined && toHex(cur.value) === toHex(ex.value);
        else if (ex?.case === "absent") ok = cur === undefined;
        else return mkReject(Code.INVALID_ENVELOPE, { changeIndex: i });
        if (!ok) return mkReject(Code.STALE, { stale: { key: c.key, currentSeq: k.state.effectiveVersion(top, c.key), cause: StaleCause.VERSION_MISMATCH }, changeIndex: i });
      }
    }
    return undefined;
  }

  private checkPreconditions(k: FakeKeyspace, m: Mutation): Reject | undefined {
    const top = new VersionTopology(this.manifests, k.manifestVersion);
    for (const p of m.preconditions) {
      if (p.logEpoch !== 0 && p.logEpoch !== k.epoch) return mkReject(Code.STALE, { stale: { cause: StaleCause.EPOCH, currentSeq: k.head } });
      if (p.target.case === "key") {
        const key = p.target.value;
        const v = k.state.effectiveVersion(top, key);
        const present = k.state.key(key) !== undefined;
        const stale = (cause = StaleCause.VERSION_MISMATCH): Reject => mkReject(Code.STALE, { stale: { key, currentSeq: v, cause } });
        if (p.check.case === "unchangedSince" && v > p.check.value) return stale();
        if (p.check.case === "exists" && !present) return stale(StaleCause.PARENT_DELETED);
        if (p.check.case === "absent" && present) return stale();
        if (p.check.case === "expectedSeq" && v !== p.check.value) return stale();
        if (p.check.case === "afterMutation" && k.lastWriter.get(key) !== p.check.value) return stale();
      } else if (p.target.case === "prefix") {
        const prefix = p.target.value;
        if (p.check.case === "unchangedSince") {
          for (const key of [...k.state.keyNames(), ...k.state.tombstoneKeys()]) {
            if (key.startsWith(prefix) && k.state.effectiveVersion(top, key) > p.check.value) return mkReject(Code.STALE, { stale: { key: prefix, currentSeq: k.state.effectiveVersion(top, key), cause: StaleCause.VERSION_MISMATCH } });
          }
        }
      }
    }
    return undefined;
  }

  private registerIntent(k: FakeKeyspace, principal: Principal, m: Mutation, content: string, laneMap: Map<number, string> | undefined): { result: MutationResult; passed: boolean } {
    // 08 §8.2: registered, logged as an INTENT record with the intent row (06 [LOG-22])
    const row = create(KeyChangeSchema, { key: `abpc.intent/${m.mutationId}`, kind: ChangeKind.SET, value: new TextEncoder().encode(JSON.stringify({ status: "PENDING" })), encoding: "json/v1" });
    const rec = this.record(k, RecordKind.INTENT, { principal, mutationId: m.mutationId, opType: m.opType, changes: [...m.changes, row], authoredAt: m.authoredAt });
    this.commit(k, rec);
    const result = retryResult(m.mutationId, RetryReason.GATE_TIMEOUT, 1000, rec.seq);
    k.bindings.set(m.mutationId, { subject: subjectOf(principal), content, opType: m.opType, result, final: false, mutation: m, intentSeq: rec.seq });
    k.pending.set(m.mutationId, { mutation: m, principal, intentSeq: rec.seq });
    laneMap?.set(Number(m.clientSeq), m.mutationId);
    // 07 [MUT-37]: a lane wait that is not satisfied answers LANE_NOT_READY
    if (m.afterClientSeq > 0n && m.clientId !== "") {
      const slots = this.slotsOf(k, m.clientId);
      if (BigInt(this.processed(k, slots.seq)) < m.afterClientSeq) return { result: retryResult(m.mutationId, RetryReason.LANE_NOT_READY, 100, rec.seq), passed: true };
    }
    return { result, passed: true };
  }

  private commitOp(k: FakeKeyspace, principal: Principal, m: Mutation, content: string, laneMap: Map<number, string> | undefined): { result: MutationResult; passed: boolean } {
    const top = new VersionTopology(this.manifests, k.manifestVersion);
    const overwrote: number[] = [];
    m.changes.forEach((c, i) => {
      if ((c.kind === ChangeKind.SET || c.kind === ChangeKind.DELETE) && k.state.effectiveVersion(top, c.key) > c.seenVersion) overwrote.push(i);
    });
    const rec = this.record(k, RecordKind.OP, { principal, mutationId: m.mutationId, opType: m.opType, changes: [...m.changes], overwroteNewer: overwrote, authoredAt: m.authoredAt });
    this.commit(k, rec);
    const result = resultOf(m.mutationId, { case: "appended", value: create(AppendedSchema, { seq: rec.seq, overwroteNewer: overwrote, logEpoch: k.epoch }) });
    k.bindings.set(m.mutationId, { subject: subjectOf(principal), content, opType: m.opType, result, final: true, mutation: m });
    laneMap?.set(Number(m.clientSeq), m.mutationId);
    return { result, passed: true };
  }
}

// create() needs the schema object under a stable name here
const LoadStateResponse_ = LoadStateResponseSchema;

function kindFits(rule: Rule | undefined, kind: ChangeKind): boolean {
  switch (kind) {
    case ChangeKind.DELETE:
      return true;
    case ChangeKind.SET:
      return rule === "lww";
    case ChangeKind.INCR:
      return rule === "counter";
    case ChangeKind.CAS:
      return rule === "cas";
    case ChangeKind.CRDT_UPDATE:
      return rule === "crdt";
    default:
      return false;
  }
}

/** The transport of one principal over the fake server. */
export class FakeTransport implements Transport {
  constructor(
    readonly server: FakeServer,
    readonly principal: Principal,
  ) {}

  getManifest(req: GetManifestRequest): Promise<GetManifestResponse> {
    return this.server.getManifest(this.principal, req);
  }
  mutate(req: MutateRequest): Promise<MutateResponse> {
    return this.server.mutate(this.principal, req);
  }
  getMutationResult(req: GetMutationResultRequest): Promise<GetMutationResultResponse> {
    return this.server.getMutationResult(this.principal, req);
  }
  getMutationResults(req: GetMutationResultsRequest): Promise<GetMutationResultsResponse> {
    return this.server.getMutationResults(this.principal, req);
  }
  cancelIntent(req: CancelIntentRequest): Promise<CancelIntentResponse> {
    return this.server.cancelIntent(this.principal, req);
  }
  createKeyspace(req: CreateKeyspaceRequest): Promise<CreateKeyspaceResponse> {
    return this.server.createKeyspace(this.principal, req);
  }
  loadState(req: LoadStateRequest): Promise<LoadStateResponse> {
    return this.server.loadState(this.principal, req);
  }
  subscribe(req: SubscribeRequest, o?: CallOptions): AsyncIterable<SubscribeResponse> {
    return this.server.subscribe(this.principal, req, o);
  }
  updateSubscription(req: UpdateSubscriptionRequest): Promise<UpdateSubscriptionResponse> {
    return this.server.updateSubscription(this.principal, req);
  }
  refreshSession(req: RefreshSessionRequest): Promise<RefreshSessionResponse> {
    return this.server.refreshSession(this.principal, req);
  }
}
