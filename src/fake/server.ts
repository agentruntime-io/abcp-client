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
// It answers as the Go server does on the points that a client's reactions depend on, so that a test that passes here means something:
//
//  - the order of 07 Table 7-1 (`server/internal/mutate/one.go`): the identity fields, the duplicate lookup, the keyspace's existence
//    and freeze, the lane checks (a duplicate meets them too, `store/lanes.go`), the stored outcome of a duplicate, and only then
//    KEYSPACE_ARCHIVED, the op type and its lane, the base epoch, the changes and the preconditions;
//  - the intents of `store/gated.go`: `log_intent: false` logs no INTENT record and every result has `intent_seq` 0, a pending intent
//    answers `Retry{LANE_NOT_READY}` while its lane wait is not satisfied (also to a resend and to `GetMutationResult`), and the
//    in-flight cap of 07 [MUT-42] answers `Retry{RATE_LIMITED}` before registration;
//  - the limits: a request over `max_mutations_per_request` or `max_request_bytes` is ResourceExhausted with no detail (07 [MUT-4]),
//    an action over `max_action_bytes` is PAYLOAD_TOO_LARGE;
//  - the lifecycle hold (11 [COORD-96]): a keyspace with an open item (a pending intent, a present hold key) cannot be archived, split or
//    deleted; a split seeds the successors from the source at its split seq.
//
// Added for C3 (14 §14.7 to §14.10), after `server/internal/session` and `server/internal/auth`:
//
//  - revocation (10 [AUTH-51], [AUTH-52], 12 [ADM-87]): `revokeSubject` sends `KeyspaceDropped{ACCESS_REVOKED}` for every followed
//    keyspace of every session of the subject and then `Closing{SUBJECT_REVOKED}` with the stream status PermissionDenied
//    (`session.RevokeSubject`); a token issued before `not_before` (plus the 60 s leeway) is refused with Unauthenticated, and the refusal
//    carries `Reject{PERMISSION_DENIED}` with `details["cause"] = "SUBJECT_REVOKED"` until a token of the same device binding that is not
//    issued before it has been accepted (the recovery mark); `revokeKeyspaces` drops the matching keyspaces with ACCESS_REVOKED
//    (`session.RevokeKeyspaces`), and a token issued before `not_before` holds no grant on them: the answers for a keyspace that the caller
//    may not read, never ACCESS_REVOKED (09 [READ-32], [READ-64]);
//  - grants (09 [READ-39], 10 [AUTH-49]): `denyRead` drops a keyspace with GRANT_CHANGED at once or at the next `RefreshSession`, which lists
//    it in `dropped_keyspace_ids`; a keyspace that a principal may not read is dropped with GRANT_CHANGED at a `Subscribe` and an add, and its
//    loads and mutations are PermissionDenied;
//  - interest (09 §9.3, §9.7): records that do not touch a session's interest are not delivered, and the delivery watermark moves over
//    them; `KeyspaceAdded` echoes the interest; `set_interest` announces its switch point with a `KeyspaceAdded` (the seq examined under
//    the old interest, SPEC-QUESTIONS Q28); a cold load names its interest and returns the keys under it and the ancestor tombstones;
//  - the offline horizon (07 [MUT-84], [MUT-89]): an action whose base seq + 1 names a record older than `max_offline` is refused with the
//    recorded `OFFLINE_TOO_LONG`, `details["cause"] = "HORIZON"`.
//
// What it does not do: it is no reference of the other checks of 07 Table 7-1 (values are not validated against schemas, grants are
// not judged, key ownership and staged moves are not modelled); it checks what the engine's reactions depend on: the envelope,
// idempotency, lanes, the base epoch, preconditions, CAS, entity tombstones, and CRDT well-formedness.

import { create, toBinary } from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import * as Y from "yjs";
import {
  AppendedSchema,
  CommitBlobResponseSchema,
  DeleteBlobResponseSchema,
  JoinPresenceResponseSchema,
  LeavePresenceResponseSchema,
  PublishPresenceResponseSchema,
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
  MutateRequestSchema,
  MutateResponseSchema,
  MutationResultSchema,
  MutationSchema,
  RecordsFrameSchema,
  RefreshSessionResponseSchema,
  SubscribeResponseSchema,
  UpdateSubscriptionResponseSchema,
  UpdateSubscriptionResponse_RejectionSchema,
  type CancelIntentRequest,
  type CancelIntentResponse,
  type CloseReason,
  type CommitBlobRequest,
  type CommitBlobResponse,
  type ControlFrame,
  type CreateKeyspaceRequest,
  type CreateKeyspaceResponse,
  type CreateUploadRequest,
  type CreateUploadResponse,
  type DeleteBlobRequest,
  type DeleteBlobResponse,
  type DropReason,
  type GetBlobRequest,
  type GetBlobResponse,
  type JoinPresenceRequest,
  type JoinPresenceResponse,
  type LeavePresenceRequest,
  type LeavePresenceResponse,
  type PublishPresenceRequest,
  type PublishPresenceResponse,
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
  KeyRedactionSchema,
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
import { applyRecord, installEntry, ReplicaState, StateNull, StateRedacted, StateValue, VersionTopology, isReservedKey, type Rule } from "../rules/fold/index.js";
import { compareUtf8, toHex } from "../rules/bytes.js";
import { sha256 } from "../rules/crypto.js";
import { setHashProvider } from "../rules/crypto.js";
import { nodeHashProvider } from "../rules/node-crypto.js";
import { DEFAULT_CLASS } from "../engine/codes.js";
import { FakeBlobs } from "./blobs.js";
import { FakePresence, type FakePresenceOptions } from "./presence.js";
import { appliedIndexes, planCrdt } from "../engine/crdt-view.js";
import { matchSuccessor } from "../engine/successors.js";
import { encodeContent } from "../engine/entry.js";
import { jsonBytes } from "../engine/changes.js";
import { RpcError, type RpcStatus } from "../engine/errors.js";
import type { Clock } from "../engine/clock.js";
import type { StaticManifests } from "../engine/manifests.js";
import type { CallOptions, Transport } from "../engine/transport.js";

setHashProvider(nodeHashProvider);

// ---------------------------------------------------------------------------------------------------------------------
// Scripting

export type Method =
  | "mutate"
  | "getMutationResult"
  | "getMutationResults"
  | "cancelIntent"
  | "createKeyspace"
  | "loadState"
  | "subscribe"
  | "updateSubscription"
  | "refreshSession"
  | "getManifest"
  | "joinPresence"
  | "publishPresence"
  | "leavePresence"
  | "createUpload"
  | "commitBlob"
  | "getBlob"
  | "deleteBlob";

/** What an injection does to a call (17 [CONF-4]). */
export type Behavior =
  /** The call fails with this status, and the ABPC detail if one is given, before the server does anything. */
  | { kind: "status"; status: RpcStatus; reject?: Reject; retry?: Retry; message?: string }
  /** The server handles the call, and the response is lost: the call fails with `status` (default Unavailable). The effect stays. */
  | { kind: "lose"; status?: RpcStatus }
  /** The server waits this long, on its clock, before it handles the call. */
  | { kind: "delay"; ms: number }
  /** For `mutate`: the result of a mutation, instead of processing it. `recorded`: the result is bound as the id's outcome and consumes its slot (07 §7.6). */
  | { kind: "result"; result: (m: Mutation) => MutationResult | undefined; recorded?: boolean }
  /** For `loadState`: the server answers, then runs `fn`, and only then returns the answer: what `fn` writes is delivered to the sessions before the client reads the page (a load that is older than the stream). */
  | { kind: "after"; fn: () => void };

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
  /** Presence (09 §9.8): how a publish over the rate is answered, and the presence schema. */
  presence?: Pick<FakePresenceOptions, "mode" | "validate">;
  /** Blobs (09 §9.9): the validity of the signed URLs, in ms. */
  blobs?: { uploadValidityMs?: number; downloadValidityMs?: number };
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
  maxInterestPrefixes: number;
  maxPresenceStateBytes: number;
  maxPresenceMembersPerTopic: number;
  presenceTtlSeconds: number;
  presenceMinPublishIntervalMs: number;
  maxBlobBytes: number;
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
  maxInterestPrefixes: 64,
  maxPresenceStateBytes: 4 << 10,
  maxPresenceMembersPerTopic: 100,
  presenceTtlSeconds: 30,
  presenceMinPublishIntervalMs: 1000,
  maxBlobBytes: 100 << 20,
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
  /** The seq of the INTENT record; 0 for an intent of an op type with `log_intent: false` (07 [MUT-8]). */
  intentSeq?: bigint;
  /** The keyspace's head when the intent was accepted: the position a restore judges it by (08 [GATE-93]); the INTENT's seq when logged. */
  acceptedSeq?: bigint;
}

/** An intent that is registered and not decided (08 §8.2): what the Go store keeps in its intent entry. */
interface PendingIntent {
  mutation: Mutation;
  principal: Principal;
  intentSeq: bigint;
  acceptedSeq: bigint;
  /** The op type's `log_intent`. */
  logged: boolean;
  /** The lane wait (07 [MUT-37]): replaced by a resend while it is not satisfied (07 [MUT-39]), never unsatisfied again once it is. */
  wait: { clientId: string; after: bigint; satisfied: boolean };
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
  /** The floor was set by `prune` (a split successor's floor is its split seq, and is judged against its source, 07 [MUT-84]). */
  pruned = false;
  bindings = new Map<string, Binding>();
  voided = new Map<string, Voided>();
  lanes = new Map<string, LaneSlots>();
  ydocs = new Map<string, Y.Doc>();
  lastWriter = new Map<string, string>();
  /** Intents that are registered and not decided. */
  pending = new Map<string, PendingIntent>();
  /** The erasure ledger's redactions of the keyspace (12 §12.7.5), listed in a load that answers a resume check (09 [READ-100]). */
  redactions: { epoch: number; seq: bigint; keys: string[] }[] = [];

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
  /** The interest of the session in the keyspace (09 §9.3); empty is the whole keyspace. */
  interest: string[];
  /** The server's delivery watermark: what it has examined and handed to the frame policy. */
  watermark: bigint;
  /** The watermark of the frames that were pushed to the stream: what a heartbeat may claim (09 [READ-55]). */
  pushed: bigint;
  epoch: number;
}

const END = Symbol("end");

/** The keyspaces that a session follows: removing one ends the session's presence there (09 [READ-63], [READ-69], [READ-77]). */
class Follows extends Map<string, FrameState> {
  onEnd: ((keyspaceId: string) => void) | undefined;
  delete(key: string): boolean {
    const had = super.delete(key);
    if (had) this.onEnd?.(key);
    return had;
  }
}

class FakeSession {
  readonly queue: (SubscribeResponse | typeof END | { error: unknown })[] = [];
  private waiter: (() => void) | undefined;
  readonly follows = new Follows();
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

/**
 * 12 [ADM-65]: a run key's value with the changes of erased keys in its start document blanked (`steps[].changes` and `steps[].compensate`, proto3 JSON `KeyChange`s): a change that holds a
 * member other than `key`, `kind` and `seenVersion` becomes `{key, kind, seenVersion, redacted: true}`. Undefined when nothing is blanked.
 */
function blankStartDocument(value: Uint8Array, erased: ReadonlySet<string>): Uint8Array | undefined {
  let doc: unknown;
  try {
    doc = JSON.parse(new TextDecoder().decode(value));
  } catch {
    return undefined;
  }
  if (typeof doc !== "object" || doc === null || Array.isArray(doc)) return undefined;
  const steps = (doc as { steps?: unknown }).steps;
  if (!Array.isArray(steps)) return undefined;
  let changed = false;
  const dflt = (v: unknown): boolean => v === "" || v === 0 || v === "0" || v === false || v === null;
  for (const step of steps) {
    if (typeof step !== "object" || step === null) continue;
    const s = step as Record<string, unknown>;
    for (const field of ["changes", "compensate"]) {
      const list = s[field];
      if (!Array.isArray(list)) continue;
      s[field] = list.map((c: unknown) => {
        if (typeof c !== "object" || c === null) return c;
        const ch = c as Record<string, unknown>;
        if (typeof ch["key"] !== "string" || !erased.has(ch["key"]) || ch["redacted"] === true) return c;
        if (!Object.entries(ch).some(([m, v]) => m !== "key" && m !== "kind" && m !== "seenVersion" && !dflt(v))) return c;
        changed = true;
        return { key: ch["key"], ...(ch["kind"] !== undefined ? { kind: ch["kind"] } : {}), ...(ch["seenVersion"] !== undefined ? { seenVersion: ch["seenVersion"] } : {}), redacted: true };
      });
    }
  }
  return changed ? jsonBytes(doc) : undefined;
}

/** 07 [MUT-7]: the order of `LaneWatermark`s, by `keyspace_id` compared in bytes and then by `lane` (SEQUENCED, then GATED). */
function compareWatermarks(a: { keyspaceId: string; lane: number }, b: { keyspaceId: string; lane: number }): number {
  return compareUtf8(a.keyspaceId, b.keyspaceId) || a.lane - b.lane;
}

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
  /** The newest client_id bound to each subject (07 [MUT-39]). */
  private readonly newestClient = new Map<string, string>();
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
  /** The token of each transport's principal object: when it was issued (10 [AUTH-52]). */
  readonly tokens = new Map<Principal, { iat: number }>();
  private readonly revocations: { sub: string; notBefore: number; recorded: number }[] = [];
  private readonly keyspaceRevocations: { pattern: string; notBefore: number; recorded: number }[] = [];
  /** The recovery marks of 10 [AUTH-51]: the device binding (`sub|device_id`) and the recording time of the revocation that it has recovered from. */
  private readonly marks = new Map<string, number>();
  private readonly denied = new Set<string>();
  private readonly presenceDenied = new Set<string>();
  /** The presence of the fake (09 §9.8): its entries and frames are for a test to read. */
  readonly presence: FakePresence;
  /** The blobs of the fake (09 §9.9) and its blob store. */
  readonly blobs: FakeBlobs;

  constructor(opts: FakeOptions) {
    this.appId = opts.appId ?? "app";
    this.clock = opts.clock;
    this.manifests = opts.manifests;
    const init = { ...DEFAULT_LIMITS, ...(opts.limits ?? {}) };
    this.limits = create(AppLimitsSchema, { ...init, maxBlobBytes: BigInt(init.maxBlobBytes) });
    this.presence = new FakePresence({ clock: opts.clock, limits: () => this.limits, ...(opts.presence ?? {}) });
    this.blobs = new FakeBlobs({ clock: opts.clock, maxBlobBytes: () => this.limits.maxBlobBytes, ...(opts.blobs ?? {}) });
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

  // -- revocation and grants (10 §10.7) --

  /**
   * `RevokeAccess{subject}` (12 [ADM-87], 10 [AUTH-51]): every session of the subject gets `KeyspaceDropped{ACCESS_REVOKED}` for each
   * followed keyspace, then `Closing{SUBJECT_REVOKED}` and the stream status PermissionDenied; from then on a token issued before
   * `notBefore` (default: now) is refused. Returns the number of sessions that it closed.
   */
  revokeSubject(sub: string, opts: { notBefore?: number } = {}): number {
    const now = this.clock.now();
    this.revocations.push({ sub, notBefore: opts.notBefore ?? now, recorded: now });
    let n = 0;
    for (const s of [...this.sessions]) {
      if (s.principal.sub !== sub && !s.principal.actorChain.includes(sub)) continue;
      for (const id of [...s.follows.keys()]) {
        s.follows.delete(id);
        s.push(this.control({ case: "keyspaceDropped", value: create(ControlFrame_KeyspaceDroppedSchema, { keyspaceId: id, reason: 2 /* ACCESS_REVOKED */ }) }));
      }
      s.push(this.control({ case: "closing", value: create(ControlFrame_ClosingSchema, { reason: CloseReasonEnum.SUBJECT_REVOKED, message: "access was revoked" }) }));
      s.push({ error: new RpcError("PermissionDenied", "access was revoked") });
      n++;
    }
    return n;
  }

  /**
   * `RevokeAccess{keyspace_pattern}` (10 [AUTH-52]): every session that follows a matching keyspace gets `KeyspaceDropped{ACCESS_REVOKED}`
   * for it. A pattern is a keyspace id, or a prefix and a trailing `*`. Tokens issued before `notBefore` hold no grant on a matching keyspace.
   */
  revokeKeyspaces(pattern: string, opts: { notBefore?: number } = {}): number {
    const now = this.clock.now();
    this.keyspaceRevocations.push({ pattern, notBefore: opts.notBefore ?? now, recorded: now });
    let n = 0;
    for (const s of [...this.sessions]) {
      for (const id of [...s.follows.keys()]) {
        if (!matchesPattern(pattern, id)) continue;
        s.follows.delete(id);
        s.push(this.control({ case: "keyspaceDropped", value: create(ControlFrame_KeyspaceDroppedSchema, { keyspaceId: id, reason: 2 /* ACCESS_REVOKED */ }) }));
        n++;
      }
    }
    return n;
  }

  /**
   * The principal no longer holds `read` on the keyspace (10 [AUTH-49], [AUTH-54]). With `drop` (the default) the sessions that follow it get
   * `KeyspaceDropped{GRANT_CHANGED}` now, as after a re-check; without it the drop comes with the next `RefreshSession`.
   */
  denyRead(sub: string, keyspaceId: string, opts: { drop?: boolean } = {}): void {
    this.denied.add(`${sub}|${keyspaceId}`);
    if (opts.drop === false) return;
    for (const s of [...this.sessions]) {
      if (s.principal.sub !== sub || !s.follows.delete(keyspaceId)) continue;
      s.push(this.control({ case: "keyspaceDropped", value: create(ControlFrame_KeyspaceDroppedSchema, { keyspaceId, reason: 1 /* GRANT_CHANGED */ }) }));
    }
  }

  allowRead(sub: string, keyspaceId: string): void {
    this.denied.delete(`${sub}|${keyspaceId}`);
  }

  /** The principal holds no `presence` grant on the keyspace (10 §10.5): `PublishPresence` is PermissionDenied; observing still needs only `read`. */
  denyPresence(sub: string, keyspaceId: string): void {
    this.presenceDenied.add(`${sub}|${keyspaceId}`);
  }

  allowPresence(sub: string, keyspaceId: string): void {
    this.presenceDenied.delete(`${sub}|${keyspaceId}`);
  }

  /** Whether the token of the principal grants `read` on the keyspace now. */
  private readable(principal: Principal, keyspaceId: string): boolean {
    if (this.denied.has(`${principal.sub}|${keyspaceId}`)) return false;
    const iat = this.tokens.get(principal)?.iat ?? Number.MAX_SAFE_INTEGER;
    for (const r of this.keyspaceRevocations) {
      if (matchesPattern(r.pattern, keyspaceId) && iat < r.notBefore + 60_000) return false;
    }
    return true;
  }

  /**
   * The token of a call (10 [AUTH-51], [AUTH-52]): a token of a revoked subject that is issued before `not_before` (with the 60 s leeway) is
   * refused with Unauthenticated; the refusal carries the `SUBJECT_REVOKED` detail until a token of the same device binding that is not issued
   * before it has been accepted (the recovery mark), which this call sets when it is that token.
   */
  private authenticate(principal: Principal): void {
    const mine = this.revocations.filter((r) => r.sub === principal.sub || principal.actorChain.includes(r.sub));
    if (mine.length === 0) return;
    const latest = mine.reduce((a, b) => (b.recorded >= a.recorded ? b : a));
    const iat = this.tokens.get(principal)?.iat ?? Number.MAX_SAFE_INTEGER;
    const binding = `${principal.sub}|${principal.deviceId}`;
    if (iat < latest.notBefore + 60_000) {
      const marked = this.marks.get(binding);
      const detail = marked !== undefined && marked >= latest.recorded ? undefined : mkReject(Code.PERMISSION_DENIED, { details: { cause: "SUBJECT_REVOKED" } });
      throw new RpcError("Unauthenticated", "the token is refused: the subject was revoked", detail !== undefined ? { reject: detail } : {});
    }
    this.marks.set(binding, latest.recorded);
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
  write(id: string, changes: KeyChange[], opts: { sub?: string; opType?: string; mutationId?: string; overwroteNewer?: number[]; previous?: KeyEntry[]; blobRefs?: string[] } = {}): bigint {
    const k = this.ks(id);
    const rec = this.record(k, RecordKind.OP, { principal: this.principal(opts.sub ?? "other"), mutationId: opts.mutationId ?? `other-${++this.principalCounter}`, opType: opts.opType ?? "op", changes, overwroteNewer: opts.overwroteNewer ?? [] });
    if (opts.previous !== undefined) rec.previous = opts.previous; // an override-only action's `previous` entries (06 [LOG-21])
    if (opts.blobRefs !== undefined) rec.blobRefs = opts.blobRefs; // 06 [LOG-90]
    this.commit(k, rec);
    return rec.seq;
  }

  /**
   * Appends a record that the server's own fold is not asked to apply, and delivers it: one that a client must stop at (an
   * unknown kind, a required feature it lacks, an unknown SYSTEM kind that must be understood, 06 [LOG-11]). The server's state is
   * only moved to its seq.
   */
  appendRaw(
    id: string,
    init: {
      kind?: number;
      requiredFeatures?: readonly string[];
      system?: { kind: number; mustUnderstand: boolean; body?: Uint8Array };
      changes?: KeyChange[];
      /** The envelope fields of the record that a test breaks on purpose (16 [SEC-3]): the ids of 03 [ID-23]. */
      causationId?: string;
      correlationId?: string;
      blobRefs?: readonly string[];
    },
  ): bigint {
    const k = this.ks(id);
    const rec = this.record(k, (init.kind ?? RecordKind.OP) as RecordKind, {
      principal: this.principal("other"),
      mutationId: init.kind === RecordKind.SYSTEM || init.system !== undefined ? "" : `other-${++this.principalCounter}`,
      opType: "op",
      changes: init.changes ?? [],
      ...(init.system !== undefined ? { system: create(SystemEventSchema, { kind: init.system.kind as SystemKind, mustUnderstand: init.system.mustUnderstand, body: init.system.body ?? new Uint8Array(), encoding: "json/v1" }) } : {}),
    });
    rec.requiredFeatures = [...(init.requiredFeatures ?? [])];
    if (init.causationId !== undefined) rec.causationId = init.causationId;
    if (init.correlationId !== undefined) rec.correlationId = init.correlationId;
    if (init.blobRefs !== undefined) rec.blobRefs = [...init.blobRefs];
    k.log.push(rec);
    k.state.setCursor(rec.seq, k.epoch, k.manifestVersion);
    this.deliver(k);
    return rec.seq;
  }

  /** The cold loads of this keyspace answer `skipped_unsupported = true` (09 [READ-18]). */
  skippedUnsupported = false;

  /** The `decision_deadline` of the GATED intents that the fake registers, from their registration (08 [GATE-10]). */
  decisionDeadlineMs = 60_000;

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
    // 06 [LOG-32]: COMMITTED: the requested changes, the effects, then the intent row; REJECTED: the intent row only, or, with `log_intent: false`,
    // the requested changes for audit, none applied. An intent that logs no INTENT record has no intent row.
    const changes = committed ? [...requested, ...(d.effects ?? []), ...(p.logged ? [rowChange] : [])] : p.logged ? [rowChange] : [...requested];
    const rec = this.record(k, RecordKind.DECISION, {
      principal: this.principal("gate"),
      mutationId: intentId,
      opType: p.mutation.opType,
      changes,
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
    const survivingIntents: { id: string; mutation: Mutation; subject: string; logged: boolean }[] = [];
    for (const [mid, b] of [...k.bindings]) {
      const o = b.result.outcome;
      const decidedAtOrBelow = o.case === "decided" && o.value.decisionSeq <= baseSeq;
      if (o.case === "appended" && o.value.seq > baseSeq) {
        k.bindings.delete(mid);
        if (opts.rewindBindings !== true) k.voided.set(mid, { subject: b.subject, opType: b.opType });
      } else if (b.mutation?.lane === Lane.GATED && !decidedAtOrBelow) {
        k.bindings.delete(mid);
        // 08 [GATE-93]: an intent accepted at or below the restore point survives it, and ends with a REJECTED decision; one above it is voided
        const accepted = b.acceptedSeq ?? 0n;
        const survives = b.mutation?.lane === Lane.GATED && b.acceptedSeq !== undefined && accepted <= baseSeq && opts.rewindBindings !== true;
        if (survives && b.mutation !== undefined) survivingIntents.push({ id: mid, mutation: b.mutation, subject: b.subject, logged: (b.intentSeq ?? 0n) !== 0n });
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
      const rec = this.record(k, RecordKind.DECISION, { principal: this.principal("gate"), mutationId: s.id, opType: s.mutation.opType, changes: s.logged ? [row] : [...s.mutation.changes], decision: create(DecisionSchema, { intentId: s.id, status: DecisionStatus.REJECTED, reject }) });
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
  prune(id: string, seq: bigint, opts: { judgeHorizon?: boolean } = {}): void {
    const k = this.ks(id);
    k.pruneFloor = seq;
    // 07 [MUT-84], [MUT-87]: a record at or below the prune floor is older than the horizon, so an action whose base is below it gets OFFLINE_TOO_LONG;
    // a test that prunes young records for the sake of RESUME_TOO_OLD says so with `judgeHorizon: false`
    k.pruned = opts.judgeHorizon !== false;
  }

  /**
   * A redaction of keys of this keyspace (12 [ADM-62] to [ADM-66], 06 [LOG-44], [LOG-76]), as a client sees it. The named keys that are not `refused` (a legal floor, 12 [ADM-63]) are the erased keys:
   *
   *  - every earlier change of an erased key, in every record that the keyspace keeps, is blanked (06 [LOG-76]): its value fields cleared and the encoding `redacted/v1` (a `DELETE` stays), and
   *    so is each `previous` entry of it; and the earlier changes that set a run key whose start document the redaction blanks (12 [ADM-65]);
   *  - one `REDACTED` record is appended (06 [LOG-44]): its `keys` are the erased keys that held a value in the timeline (present, tombstoned, or changed with a value in a record), its
   *    `erased_keys` the others, its `min_seq` the first record that held a value of one, its `refused_keys` the refused ones; its changes are the `DELETE`s of the keys that were present and the
   *    server-written `SET` of each run key (`abpc.saga.v1/run/{id}`) whose start document holds a change of an erased key, with that change blanked (`{"key", "kind", "seenVersion", "redacted": true}`);
   *  - the ledger lists each erased key with the record's seq, for the load that answers a resume check (09 [READ-100]); a CRDT key's document is discarded (05 [STATE-78]).
   *
   * Not modelled: the closing of pending intents that request an erased key (12 [ADM-64]), the blobs (12 [ADM-103]; `blobs.deleteBlob` and `BLOB_DELETED` records are a test's own), the reach through
   * former paths, moves and splits, the checkpoints, and the grants. Returns the seq of the `REDACTED` record, or the head when nothing in the keyspace needed a record.
   */
  redact(id: string, named: readonly string[], opts: { refused?: readonly string[] } = {}): bigint {
    const k = this.ks(id);
    const refused = [...new Set(opts.refused ?? [])].sort(compareUtf8);
    const erased = [...new Set(named)].filter((x) => !refused.includes(x)).sort(compareUtf8);
    const erasedSet = new Set(erased);
    const blankedAlready = (c: KeyChange): boolean => c.encoding === "redacted/v1" && c.value.length === 0 && !c.isNull && c.delta === 0n && c.update.length === 0;
    const holdsValue = (c: KeyChange): boolean => c.kind !== ChangeKind.DELETE && !blankedAlready(c);
    const held = (key: string): boolean => k.state.key(key) !== undefined || k.state.tombstone(key) !== undefined || k.log.some((r) => r.changes.some((c) => c.key === key && holdsValue(c)));
    const redacted = erased.filter(held);
    const others = erased.filter((x) => !redacted.includes(x));
    let minSeq = 0n;
    for (const r of k.log) {
      if (r.changes.some((c) => erasedSet.has(c.key) && holdsValue(c))) {
        minSeq = r.seq;
        break;
      }
    }
    // 12 [ADM-65]: the run keys whose start document holds a change of an erased key get a server-written SET with the change blanked
    const runSets: KeyChange[] = [];
    for (const key of k.state.keyNames()) {
      const row = k.state.key(key);
      if (row === undefined || row.state !== StateValue || !/^abpc\.saga\.v1\/run\/[^/]+$/.test(key)) continue;
      const next = blankStartDocument(row.value, erasedSet);
      if (next !== undefined) runSets.push(create(KeyChangeSchema, { key, kind: ChangeKind.SET, value: next, encoding: "json/v1", seenVersion: row.version }));
    }
    const runKeys = new Set(runSets.map((c) => c.key));
    const deletes = redacted.flatMap((key) => {
      const row = k.state.key(key);
      return row === undefined ? [] : [create(KeyChangeSchema, { key, kind: ChangeKind.DELETE, seenVersion: row.version })];
    });
    // 06 [LOG-76]: the earlier records, replaced by their blanked copies (what a session has been sent is not changed)
    const blank = (c: KeyChange): KeyChange => create(KeyChangeSchema, { key: c.key, kind: c.kind, seenVersion: c.seenVersion, encoding: c.kind === ChangeKind.DELETE ? "" : "redacted/v1" });
    k.log = k.log.map((r) => {
      const changed = r.changes.some((c) => (erasedSet.has(c.key) || (runKeys.has(c.key) && c.kind === ChangeKind.SET)) && (c.kind === ChangeKind.DELETE ? false : !blankedAlready(c)));
      const prevChanged = r.previous.some((p) => erasedSet.has(p.key));
      if (!changed && !prevChanged) return r;
      return create(RecordSchema, {
        ...r,
        changes: r.changes.map((c) => ((erasedSet.has(c.key) || (runKeys.has(c.key) && c.kind === ChangeKind.SET)) && c.kind !== ChangeKind.DELETE && !blankedAlready(c) ? blank(c) : c)),
        previous: r.previous.map((p) => (erasedSet.has(p.key) ? create(KeyEntrySchema, { key: p.key, version: p.version, ...(p.changedAt !== undefined ? { changedAt: p.changedAt } : {}), encoding: "redacted/v1" }) : p)),
      });
    });
    if (redacted.length === 0 && others.length === 0 && runSets.length === 0) {
      k.redactions.push({ epoch: k.epoch, seq: k.head, keys: erased });
      return k.head;
    }
    const body: Record<string, unknown> = { redaction_id: `redaction-${++this.principalCounter}`, keys: redacted, min_seq: Number(minSeq) };
    if (others.length > 0) body["erased_keys"] = others;
    if (refused.length > 0) body["refused_keys"] = refused;
    const rec = this.record(k, RecordKind.SYSTEM, {
      principal: this.principal("system"),
      system: create(SystemEventSchema, { kind: SystemKind.REDACTED, mustUnderstand: true, body: jsonBytes(body), encoding: "json/v1" }),
      changes: [...deletes, ...runSets],
    });
    this.commit(k, rec);
    for (const key of erased) k.ydocs.delete(key);
    k.redactions.push({ epoch: k.epoch, seq: rec.seq, keys: erased });
    return rec.seq;
  }

  /** The redactions of the erasure ledger that a load answering a resume check lists (09 [READ-100]): the ones newer than the cursor, in increasing (epoch, seq) and then key order. */
  private redactionsSince(k: FakeKeyspace, cur: KeyspaceCursor | undefined): ReturnType<typeof create<typeof KeyRedactionSchema>>[] {
    const after = cur?.afterSeq ?? 0n;
    const epoch = cur?.logEpoch ?? 0;
    const out: ReturnType<typeof create<typeof KeyRedactionSchema>>[] = [];
    for (const r of [...k.redactions].sort((a, b) => a.epoch - b.epoch || (a.seq < b.seq ? -1 : a.seq > b.seq ? 1 : 0))) {
      if (!((r.epoch === epoch && r.seq > after) || r.epoch > epoch)) continue;
      for (const key of [...r.keys].sort(compareUtf8)) out.push(create(KeyRedactionSchema, { key, redactionSeq: r.seq, logEpoch: r.epoch }));
    }
    return out;
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

  /**
   * The open items of a keyspace (11 [COORD-95]), as the counts that 11 [COORD-97] gives: the PENDING intents, logged or not, and the
   * present keys of the hold templates (`abpc.saga.v1/run/{id}/open`, `abpc.escrow.v1/{class}/{id}/open`). The fake models no key
   * ownership (`owned_keys` is 0) and no staged move copies.
   */
  openItems(id: string): { pendingIntents: number; ownedKeys: number; holdKeys: number } {
    const k = this.ks(id);
    let holdKeys = 0;
    for (const key of k.state.keyNames()) {
      const parts = key.split("/");
      const saga = parts.length === 4 && parts[0] === "abpc.saga.v1" && parts[1] === "run" && parts[3] === "open";
      const escrow = parts.length === 4 && parts[0] === "abpc.escrow.v1" && parts[3] === "open";
      if (saga || escrow) holdKeys++;
    }
    return { pendingIntents: k.pending.size, ownedKeys: 0, holdKeys };
  }

  /** 11 [COORD-96], [COORD-97]: while a keyspace has an open item, `ArchiveKeyspace`, `SplitKeyspace` and `DeleteKeyspace` (but a frozen keyspace's) are refused and change nothing. */
  private refuseWhileOpen(k: FakeKeyspace): void {
    const o = this.openItems(k.id);
    if (o.pendingIntents + o.ownedKeys + o.holdKeys === 0) return;
    const reject = mkReject(Code.ADMIN_REFUSED, { message: "the keyspace has open items", details: { reason: "LIFECYCLE_HOLD", pending_intents: String(o.pendingIntents), owned_keys: String(o.ownedKeys), hold_keys: String(o.holdKeys) } });
    throw new RpcError("FailedPrecondition", "the keyspace has open items", { reject });
  }

  /** Archives the keyspace (12 §12.4): no action is accepted; sessions get `KeyspaceDropped{ARCHIVED}`. Refused while the keyspace has an open item (11 [COORD-96]). */
  archive(id: string, successors: SuccessorRule[] = []): void {
    const k = this.ks(id);
    this.refuseWhileOpen(k);
    k.lifecycle = KeyspaceLifecycle.ARCHIVED;
    k.successors = successors;
    this.dropSessions(id, 6 /* ARCHIVED */, successors);
  }

  /**
   * Splits the keyspace (12 §12.5): the successors that exist are seeded from this keyspace's state, each key to the successor whose
   * rule it matches (12 [ADM-45]), at a checkpoint at the split seq S, with the versions of the source (06 [LOG-67]) and the prune floor
   * S (13 [CONF-18]). Refused while the keyspace has an open item (11 [COORD-96]).
   */
  split(id: string, successors: SuccessorRule[]): void {
    const k = this.ks(id);
    this.refuseWhileOpen(k);
    const seed = this.entriesOf(k);
    k.lifecycle = KeyspaceLifecycle.SPLIT;
    k.successors = successors;
    for (const r of successors) {
      const t = this.keyspaces.get(r.keyspaceId);
      if (t === undefined) continue;
      const st = new ReplicaState();
      t.ydocs = new Map();
      for (const e of seed) {
        if (matchSuccessor(successors, e.key) !== r) continue;
        installEntry(st, e, this.manifests, k.manifestVersion);
        const doc = k.ydocs.get(e.key);
        if (doc !== undefined && !e.deleted) {
          const copy = new Y.Doc({ gc: false });
          Y.applyUpdate(copy, Y.encodeStateAsUpdate(doc));
          t.ydocs.set(e.key, copy);
        }
      }
      st.setCursor(k.head, t.epoch, k.manifestVersion);
      t.state = st;
      t.log = [];
      t.pruneFloor = k.head;
      t.manifestVersion = k.manifestVersion;
    }
    this.dropSessions(id, 3 /* MOVED */, successors);
  }

  /** Marks a keyspace as not seeded yet, or as seeded (12 [ADM-104]): its adds and its `Subscribe` get `Retry{KEYSPACE_SEEDING}`, and its mutations too. */
  setSeeding(id: string, seeding: boolean): void {
    this.ks(id).seeding = seeding;
  }

  /** Deletes the keyspace (12 §12.4). Refused while the keyspace has an open item, but for a frozen keyspace (11 [COORD-96], 12 [ADM-35]). */
  deleteKeyspace(id: string): void {
    const k = this.ks(id);
    if (!k.frozen) this.refuseWhileOpen(k);
    k.deleted = true;
    k.pending.clear(); // the intents that a frozen keyspace's deletion abandons no longer count against the in-flight cap (07 [MUT-42])
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
    // 07 [MUT-7]: in increasing order of (keyspace_id, lane), as in `MutateResponse.watermarks` and `LoadStateResponse.lane_watermarks`
    wms.sort(compareWatermarks);
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
    const examined = k.log.filter((r) => r.seq > fs.watermark);
    if (examined.length === 0) return;
    const cursor = (examined[examined.length - 1] as RecordMessage).seq;
    // 09 [READ-23], [READ-24]: the records that touch the interest are delivered whole; the watermark covers the others, examined and not sent
    const recs = fs.interest.length === 0 ? examined : examined.filter((r) => this.touches(k, r, fs.interest));
    const prev = fs.watermark;
    fs.watermark = cursor;
    if (recs.length === 0) {
      if (fs.pushed === prev) fs.pushed = cursor; // nothing of what the watermark passed is held back or lost: a heartbeat may claim it
      return;
    }
    const frame = create(SubscribeResponseSchema, { frame: { case: "records", value: create(RecordsFrameSchema, { keyspaceId: k.id, prevCursor: prev, cursor, records: recs }) } });
    this.emit(s, frame);
  }

  /** Whether a record touches an interest (09 [READ-23]): a change to a key under a prefix or an ancestor of one, a SYSTEM record, or a DECISION whose INTENT touches. */
  private touches(k: FakeKeyspace, rec: RecordMessage, interest: readonly string[]): boolean {
    if (rec.kind === RecordKind.SYSTEM) return true;
    const hit = (key: string): boolean => interest.some((p) => key === p || key.startsWith(`${p}/`) || p.startsWith(`${key}/`));
    if (rec.changes.some((c) => hit(c.key))) return true;
    if (rec.kind === RecordKind.DECISION) {
      const intent = k.log.find((r) => r.kind === RecordKind.INTENT && r.mutationId === rec.mutationId);
      if (intent !== undefined && intent.changes.some((c) => hit(c.key))) return true;
    }
    return false;
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Calls

  private async prologue(method: Method, req: unknown, principal: Principal): Promise<Injection | undefined> {
    this.calls.push({ method, sub: principal.sub, request: req, at: this.clock.now() });
    this.authenticate(principal); // 10 [AUTH-51], [AUTH-52]: before the body is read
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
    // 09 [READ-37], 10 [AUTH-49]: `read` is judged again on every followed keyspace against the new token; each one that it no longer allows is
    // dropped with GRANT_CHANGED, and listed, informationally, in the answer
    const s = [...this.sessions].find((x) => x.id === req.sessionId);
    if (s === undefined) throw new RpcError("NotFound", "no such session");
    const dropped: string[] = [];
    for (const id of [...s.follows.keys()]) {
      if (this.readable(principal, id)) continue;
      s.follows.delete(id);
      dropped.push(id);
      s.push(this.control({ case: "keyspaceDropped", value: create(ControlFrame_KeyspaceDroppedSchema, { keyspaceId: id, reason: 1 /* GRANT_CHANGED */ }) }));
    }
    return create(RefreshSessionResponseSchema, { expiresAt: timestampFromMs(this.clock.now() + 3600_000), droppedKeyspaceIds: dropped });
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
    if (b !== undefined && b.subject === subject) {
      // 08 [GATE-8], [GATE-57]: a pending intent answers a Retry with the current reason, `known = true`, never `known = false`
      const p = k.pending.get(id);
      const result = p !== undefined ? this.pendingResult(k, id, p) : b.result;
      return create(GetMutationResultResponseSchema, { known: true, result: create(MutationResultSchema, { ...result, duplicate: true }), voided: false });
    }
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
      const p = k.pending.get(req.intentId);
      const result = p !== undefined ? this.pendingResult(k, req.intentId, p) : (k.bindings.get(req.intentId) as Binding).result;
      return create(CancelIntentResponseSchema, { result: create(MutationResultSchema, { ...result, duplicate: true }) });
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
          crdtCoveredSeq: crdt || (row.state === StateRedacted && row.rule === "crdt") ? row.version : 0n,
          redacted: row.state === StateRedacted, // 05 [STATE-82]: a key in the redacted state, in a load below the `REDACTED` record (09 [READ-13])
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
    const inj = await this.prologue("loadState", req, principal);
    const resp = this.loadStateNow(principal, req);
    if (inj?.behavior.kind === "after") inj.behavior.fn();
    return resp;
  }

  private loadStateNow(principal: Principal, req: LoadStateRequest): LoadStateResponse {
    const cur = req.cursor;
    const id = cur?.keyspaceId ?? "";
    const k = this.keyspaces.get(id);
    if (k === undefined) throw new RpcError("NotFound", "no such keyspace", { reject: mkReject(Code.KEYSPACE_NOT_FOUND) });
    if (k.deleted) throw new RpcError("NotFound", "deleted", { reject: mkReject(Code.KEYSPACE_DELETED) });
    if (!this.readable(principal, id)) throw new RpcError("PermissionDenied", "no read grant", { reject: mkReject(Code.PERMISSION_DENIED) });
    if (k.frozen) throw new RpcError("FailedPrecondition", "frozen", { reject: mkReject(Code.HISTORY_DIVERGED) });
    const subject = subjectOf(principal);
    void subject;
    const interest = cur?.interestPrefixes ?? [];
    if (interest.length > Number(this.limits.maxInterestPrefixes)) throw new RpcError("ResourceExhausted", "too many interest prefixes", { reject: mkReject(Code.QUOTA_EXCEEDED, { details: { quota: "max_interest_prefixes" } }) });
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
    // a cold load: the state at one checkpoint, as pages; with an interest (09 [READ-9]): the keys under it, the tombstones that match it, and
    // the entity and staged tombstones of the ancestors of its prefixes
    const all = this.entriesOf(k).filter((e) => entryInInterest(e, interest));
    const size = req.pageSize === 0 ? this.pageSize : Math.min(req.pageSize, 10000);
    const first = all.slice(0, size);
    const more = all.length > size;
    let token = "";
    if (more) {
      token = `page-${this.nextToken++}`;
      this.loads.set(token, { entries: all, offset: size, checkpoint: k.head, epoch: k.epoch, manifestVersion: k.manifestVersion });
    }
    // 09 [READ-100]: the cold load that answers a resume check (`after_seq > 0`) carries the redactions newer than the cursor, once, on its first page
    const redactions = afterSeq > 0n ? this.redactionsSince(k, cur) : [];
    return create(LoadStateResponse_, { ...base, checkpointSeq: k.head, entries: first, nextPageToken: token, resumed: false, laneWatermarks: wms, skippedUnsupported: this.skippedUnsupported, redactions });
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
    // 09 [READ-4], [READ-62] (draft 14): the one order of the checks of a `Subscribe`. After the suspended app (the prologue) and the grants (a keyspace that
    // the caller may not read is dropped, [READ-32]) comes the identity check of the `client_id`, at the row for ACTOR_MISMATCH; then the rows below it, among them a
    // successor that is not seeded (the whole call is refused, 14 [CLI-44]); then the other checks of [READ-62] (a keyspace named twice); the sessions cap of
    // [READ-35] last. The `client_id` is bound only after every check has passed (10 [AUTH-20]).
    if (req.clientId !== "") {
      const bound = this.clients.get(req.clientId);
      if (bound !== undefined && bound !== subjectOf(principal)) throw new RpcError("PermissionDenied", "actor mismatch", { reject: mkReject(Code.ACTOR_MISMATCH) });
    }
    for (const c of req.keyspaces) {
      if (this.keyspaces.get(c.keyspaceId)?.seeding === true) throw new RpcError("Unavailable", "a successor is not seeded yet", { retry: create(RetrySchema, { reason: RetryReason.KEYSPACE_SEEDING, afterMs: 1000 }) });
    }
    if (new Set(req.keyspaces.map((c) => c.keyspaceId)).size !== req.keyspaces.length) {
      throw new RpcError("InvalidArgument", "a keyspace is named twice", { reject: mkReject(Code.INVALID_ENVELOPE) });
    }
    if (this.maxSessions !== undefined && [...this.sessions].filter((s) => subjectOf(s.principal) === subjectOf(principal)).length >= this.maxSessions) {
      throw new RpcError("ResourceExhausted", "too many sessions", { reject: mkReject(Code.QUOTA_EXCEEDED, { details: { quota: "max_sessions_per_principal" } }) });
    }
    if (req.clientId !== "") this.bindClient(req.clientId, subjectOf(principal));
    const session = new FakeSession(`session-${this.nextSession++}`, principal, req.clientId);
    session.follows.onEnd = (ks): void => this.presence.endKeyspace(session, ks);
    this.presence.carry(session); // 09 [READ-74]: the later session of a client instance carries its presence
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
      this.presence.endSession(session); // 09 [READ-77]: the memberships and the entries end with the session
    }
  }

  /** The start of a keyspace on a session (09 [READ-32], [READ-65]): added, or dropped with the reason that applies. */
  private addFollow(s: FakeSession, c: KeyspaceCursor): void {
    const drop = (reason: number, successors: SuccessorRule[] = []): void => {
      s.push(this.control({ case: "keyspaceDropped", value: create(ControlFrame_KeyspaceDroppedSchema, { keyspaceId: c.keyspaceId, reason: reason as DropReason, successors }) }));
    };
    const k = this.keyspaces.get(c.keyspaceId);
    if (!this.readable(s.principal, c.keyspaceId)) return drop(1 /* GRANT_CHANGED */); // 09 [READ-32]: whether or not the keyspace exists
    if (k === undefined) return drop(10 /* NOT_FOUND */);
    if (k.deleted) return drop(7);
    if (k.frozen) return drop(5);
    if (k.lifecycle === KeyspaceLifecycle.ARCHIVED) return drop(6, k.successors);
    if (k.lifecycle === KeyspaceLifecycle.SPLIT) return drop(3, k.successors);
    // 09 [READ-65]: `EPOCH_CHANGED` "if `after_seq > 0` and `cursor.log_epoch` differs from the keyspace's `log_epoch`": the epoch of a cursor is judged only then
    // (03 [ID-40]: in `KeyspaceCursor.log_epoch`, 0 is a value that matches no epoch)
    if (c.afterSeq > 0n && c.logEpoch !== k.epoch) return drop(8);
    const rec = k.log.find((r) => r.seq === c.afterSeq);
    const hashOk = c.afterRecordHash.length === 0 || (rec !== undefined && toHex(rec.recordHash) === toHex(c.afterRecordHash));
    if (c.afterSeq > k.head || c.afterSeq < k.pruneFloor || !hashOk) return drop(4 /* RESUME_TOO_OLD */);
    s.follows.set(c.keyspaceId, { interest: [...c.interestPrefixes], watermark: c.afterSeq, pushed: c.afterSeq, epoch: k.epoch });
    s.push(this.control({ case: "keyspaceAdded", value: create(ControlFrame_KeyspaceAddedSchema, { cursor: create(KeyspaceCursorSchema, { keyspaceId: c.keyspaceId, afterSeq: c.afterSeq, logEpoch: k.epoch, interestPrefixes: [...c.interestPrefixes] }) }) }));
    this.pump(s, k);
  }

  async updateSubscription(principal: Principal, req: UpdateSubscriptionRequest): Promise<UpdateSubscriptionResponse> {
    await this.prologue("updateSubscription", req, principal);
    const s = [...this.sessions].find((x) => x.id === req.sessionId);
    if (s === undefined) throw new RpcError("NotFound", "no such session");
    if (req.clientId !== "") {
      const bound = this.clients.get(req.clientId);
      if (bound !== undefined && bound !== subjectOf(principal)) throw new RpcError("PermissionDenied", "actor mismatch", { reject: mkReject(Code.ACTOR_MISMATCH) });
      this.bindClient(req.clientId, subjectOf(principal));
      const earlier = s.clientId;
      s.clientId = req.clientId;
      this.presence.rebind(s, earlier); // 09 [READ-77]: the entries of the earlier instance end, the memberships stay
    }
    for (const id of req.remove) s.follows.delete(id);
    const rejected = [];
    for (const c of req.add) {
      if (!this.readable(s.principal, c.keyspaceId)) {
        rejected.push(create(UpdateSubscriptionResponse_RejectionSchema, { keyspaceId: c.keyspaceId, outcome: { case: "reject", value: mkReject(Code.PERMISSION_DENIED) } }));
        continue;
      }
      if (c.interestPrefixes.length > Number(this.limits.maxInterestPrefixes)) {
        rejected.push(create(UpdateSubscriptionResponse_RejectionSchema, { keyspaceId: c.keyspaceId, outcome: { case: "reject", value: mkReject(Code.QUOTA_EXCEEDED, { details: { quota: "max_interest_prefixes" } }) } }));
        continue;
      }
      if (!this.keyspaces.has(c.keyspaceId)) {
        rejected.push(create(UpdateSubscriptionResponse_RejectionSchema, { keyspaceId: c.keyspaceId, outcome: { case: "reject", value: mkReject(Code.KEYSPACE_NOT_FOUND) } }));
        continue;
      }
      if (this.keyspaces.get(c.keyspaceId)?.seeding === true) {
        // 12 [ADM-104], 14 [CLI-44]: not seeded yet, a Retry and never KEYSPACE_NOT_FOUND
        rejected.push(create(UpdateSubscriptionResponse_RejectionSchema, { keyspaceId: c.keyspaceId, outcome: { case: "retry", value: create(RetrySchema, { reason: RetryReason.KEYSPACE_SEEDING, afterMs: 1000 }) } }));
        continue;
      }
      this.addFollow(s, c);
    }
    // 09 [READ-62], [READ-64]: `set_interest` after `add`; each entry is checked in order: the form, that the session follows the keyspace, the limit
    for (const i of req.setInterest) {
      const bad = i.interestPrefixes.some((p) => p === "" || p.endsWith("/") || p.split("/").some((seg) => !/^[A-Za-z0-9._-]{1,64}$/.test(seg) || seg === "." || seg === ".."));
      const fs = s.follows.get(i.keyspaceId);
      if (bad) rejected.push(create(UpdateSubscriptionResponse_RejectionSchema, { keyspaceId: i.keyspaceId, outcome: { case: "reject", value: mkReject(Code.INVALID_KEY) } }));
      else if (fs === undefined) rejected.push(create(UpdateSubscriptionResponse_RejectionSchema, { keyspaceId: i.keyspaceId, outcome: { case: "reject", value: mkReject(Code.INVALID_ENVELOPE) } }));
      else if (i.interestPrefixes.length > Number(this.limits.maxInterestPrefixes)) rejected.push(create(UpdateSubscriptionResponse_RejectionSchema, { keyspaceId: i.keyspaceId, outcome: { case: "reject", value: mkReject(Code.QUOTA_EXCEEDED, { details: { quota: "max_interest_prefixes" } }) } }));
      else {
        // 09 [READ-27]: the switch point is the delivery watermark when the change takes effect, announced before any frame under the new interest
        fs.interest = [...i.interestPrefixes];
        s.push(this.control({ case: "keyspaceAdded", value: create(ControlFrame_KeyspaceAddedSchema, { cursor: create(KeyspaceCursorSchema, { keyspaceId: i.keyspaceId, afterSeq: fs.watermark, logEpoch: fs.epoch, interestPrefixes: [...i.interestPrefixes] }) }) }));
      }
    }
    return create(UpdateSubscriptionResponseSchema, { rejected });
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Presence (09 §9.8)

  /** The session of a call that names one (09 [READ-34]): NotFound with no detail when it does not exist, has ended or belongs to another subject. */
  private sessionOf(principal: Principal, id: string): FakeSession {
    const s = [...this.sessions].find((x) => x.id === id && subjectOf(x.principal) === subjectOf(principal));
    if (s === undefined) throw new RpcError("NotFound", "no such session");
    return s;
  }

  async joinPresence(principal: Principal, req: JoinPresenceRequest): Promise<JoinPresenceResponse> {
    await this.prologue("joinPresence", req, principal);
    const s = this.sessionOf(principal, req.sessionId);
    if (!this.readable(s.principal, req.keyspaceId)) throw new RpcError("PermissionDenied", "no grant", { reject: mkReject(Code.PERMISSION_DENIED) }); // observing needs `read`
    if (!s.follows.has(req.keyspaceId)) throw new RpcError("FailedPrecondition", "the session does not follow the keyspace"); // 09 [READ-73]: no detail
    const k = this.ks(req.keyspaceId);
    const kind = this.manifests.get(k.manifestVersion)?.presenceTopicKind?.(k.kind, req.topic);
    if (kind === undefined) throw new RpcError("InvalidArgument", "the topic matches no presence topic of the kind", { reject: mkReject(Code.INVALID_KEY) }); // 09 [READ-72]
    this.presence.join(s, req.keyspaceId, req.topic, kind === "awareness");
    return create(JoinPresenceResponseSchema, {});
  }

  async publishPresence(principal: Principal, req: PublishPresenceRequest): Promise<PublishPresenceResponse> {
    const inj = await this.prologue("publishPresence", req, principal);
    const s = this.sessionOf(principal, req.sessionId);
    if (this.presenceDenied.has(`${s.principal.sub}|${req.keyspaceId}`) || !this.readable(s.principal, req.keyspaceId)) throw new RpcError("PermissionDenied", "no grant", { reject: mkReject(Code.PERMISSION_DENIED) });
    const expires = this.presence.publish(s, req.keyspaceId, req.topic, req.state);
    if (inj?.behavior.kind === "lose") throw new RpcError(inj.behavior.status ?? "Unavailable", "the response was lost");
    return create(PublishPresenceResponseSchema, { expiresAt: timestampFromMs(expires) });
  }

  async leavePresence(principal: Principal, req: LeavePresenceRequest): Promise<LeavePresenceResponse> {
    await this.prologue("leavePresence", req, principal);
    const s = this.sessionOf(principal, req.sessionId);
    this.presence.leave(s, req.keyspaceId, req.topic);
    return create(LeavePresenceResponseSchema, {});
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Blobs (09 §9.9)

  /** The rows of 09 [READ-4] above the state of the blob: the grant, then the keyspace (not found, deleted, archived or split for a call that writes, frozen but for `GetBlob`). */
  private blobKeyspace(principal: Principal, keyspaceId: string, call: "write" | "read"): FakeKeyspace {
    if (!this.readable(principal, keyspaceId)) throw new RpcError("PermissionDenied", "no grant", { reject: mkReject(Code.PERMISSION_DENIED) });
    const k = this.keyspaces.get(keyspaceId);
    if (k === undefined) throw new RpcError("NotFound", "no such keyspace", { reject: mkReject(Code.KEYSPACE_NOT_FOUND) });
    if (k.deleted) throw new RpcError("NotFound", "the keyspace was deleted", { reject: mkReject(Code.KEYSPACE_DELETED) });
    if (call === "write" && k.lifecycle !== KeyspaceLifecycle.ACTIVE) {
      throw new RpcError("FailedPrecondition", "the keyspace is archived or split", { reject: mkReject(Code.KEYSPACE_ARCHIVED, { details: { lifecycle: k.lifecycle === KeyspaceLifecycle.SPLIT ? "SPLIT" : "ARCHIVED" }, successors: k.successors }) });
    }
    if (call === "write" && k.frozen) throw new RpcError("FailedPrecondition", "the keyspace is frozen", { reject: mkReject(Code.HISTORY_DIVERGED) });
    return k;
  }

  async createUpload(principal: Principal, req: CreateUploadRequest): Promise<CreateUploadResponse> {
    await this.prologue("createUpload", req, principal);
    this.blobKeyspace(principal, req.keyspaceId, "write");
    return this.blobs.createUpload(principal.sub, req);
  }

  async commitBlob(principal: Principal, req: CommitBlobRequest): Promise<CommitBlobResponse> {
    await this.prologue("commitBlob", req, principal);
    const k = this.blobKeyspace(principal, req.keyspaceId, "read"); // the lifecycle of a commit is judged below, for a pending blob only (09 [READ-88])
    if (k.lifecycle !== KeyspaceLifecycle.ACTIVE && this.blobs.row(req.keyspaceId, req.blobId)?.state === "pending") {
      throw new RpcError("FailedPrecondition", "the keyspace is archived or split", { reject: mkReject(Code.KEYSPACE_ARCHIVED, { details: { lifecycle: k.lifecycle === KeyspaceLifecycle.SPLIT ? "SPLIT" : "ARCHIVED" }, successors: k.successors }) });
    }
    if (k.frozen) throw new RpcError("FailedPrecondition", "the keyspace is frozen", { reject: mkReject(Code.HISTORY_DIVERGED) });
    this.blobs.commitBlob(req);
    return create(CommitBlobResponseSchema, {});
  }

  async getBlob(principal: Principal, req: GetBlobRequest): Promise<GetBlobResponse> {
    await this.prologue("getBlob", req, principal);
    this.blobKeyspace(principal, req.keyspaceId, "read"); // a frozen keyspace serves `GetBlob` (09 [READ-90])
    return this.blobs.getBlob(req);
  }

  async deleteBlob(principal: Principal, req: DeleteBlobRequest): Promise<DeleteBlobResponse> {
    await this.prologue("deleteBlob", req, principal);
    const k = this.blobKeyspace(principal, req.keyspaceId, "read"); // an archived or split keyspace serves it (09 [READ-91])
    if (k.frozen) throw new RpcError("FailedPrecondition", "the keyspace is frozen", { reject: mkReject(Code.HISTORY_DIVERGED) });
    if (this.blobs.deleteBlob(principal.sub, req)) {
      // 09 [READ-91], 06 [LOG-79]: the record that names the deleted blob, which every client that follows the keyspace reads
      this.appendRaw(req.keyspaceId, { kind: RecordKind.SYSTEM, system: { kind: SystemKind.BLOB_DELETED, mustUnderstand: false, body: new TextEncoder().encode(JSON.stringify({ blob_id: req.blobId })) } });
    }
    return create(DeleteBlobResponseSchema, {});
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Mutate (07)

  async mutate(principal: Principal, req: MutateRequest): Promise<MutateResponse> {
    const inj = await this.prologue("mutate", req, principal);
    this.checkRequest(req);
    const resp = this.doMutate(principal, req, inj);
    if (inj?.behavior.kind === "lose") throw new RpcError(inj.behavior.status ?? "Unavailable", "the response was lost");
    return resp;
  }

  /**
   * Table 7-1 step 3, as the Go server makes it (`mutate/pipeline.go`): first the count and the size limits of 07 [MUT-4], the size
   * measured in the binary encoding, each ResourceExhausted with no detail and nothing processed; then a non-empty list, and one
   * `client_id` for all of it (07 [MUT-3]), each InvalidArgument.
   */
  private checkRequest(req: MutateRequest): void {
    const ms = req.mutations;
    if (ms.length > Number(this.limits.maxMutationsPerRequest)) throw new RpcError("ResourceExhausted", "too many mutations in the request");
    if (toBinary(MutateRequestSchema, req).byteLength > Number(this.limits.maxRequestBytes)) throw new RpcError("ResourceExhausted", "request too large");
    const invalid = (msg: string): RpcError => new RpcError("InvalidArgument", msg, { reject: mkReject(Code.INVALID_ENVELOPE) });
    if (ms.length === 0) throw invalid("no mutation");
    for (const m of ms) if (m.clientId !== (ms[0] as Mutation).clientId) throw invalid("the mutations carry more than one client_id");
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
    // 07 [MUT-7]: in increasing order of (keyspace_id, lane): the ids compared in bytes, then the lane by its enum number
    wms.sort(compareWatermarks);
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

  /** The client_id is bound to its subject at its first use (07 [MUT-30]); the newest id of a subject is the one a resend's lane wait may replace the wait for (07 [MUT-39]). */
  private bindClient(clientId: string, subject: string): void {
    if (this.clients.get(clientId) === subject) return;
    this.clients.set(clientId, subject);
    this.newestClient.set(subject, clientId);
  }

  /**
   * Processes one mutation in the order of 07 Table 7-1 as `mutate/one.go` makes it: the identity fields (step 4); the duplicate
   * lookup, voided markers included (step 6); the keyspace's existence and freeze (step 8); the lane checks (step 9), which a
   * duplicate meets too (`store/lanes.go`, JudgeLane); a duplicate's stored outcome; only then a keyspace that is not ACTIVE
   * (KEYSPACE_ARCHIVED), and the content checks (steps 10 to 16: the op type, its lane, the base epoch, the changes, the
   * preconditions) and the commit or the registration (step 17). `passed` says that the pair (keyspace, lane) passed the binding
   * check (07 [MUT-7]).
   */
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
    // step 4: the identity fields; none of their rejects is recorded
    if (m.mutationId === "") return rej(mkReject(Code.INVALID_ENVELOPE, { message: "no mutation id" }));
    if (m.lane !== Lane.SEQUENCED && m.lane !== Lane.GATED) return rej(mkReject(Code.INVALID_ENVELOPE, { message: "no lane" }));
    if (m.clientId !== "" && m.clientSeq === 0n) return rej(mkReject(Code.INVALID_ENVELOPE, { message: "client_seq 0" }));
    if (k === undefined) return rej(mkReject(Code.KEYSPACE_NOT_FOUND));
    // Table 7-1 step 7: no `write` without `read` (10 §10.5): the unrecorded PERMISSION_DENIED, whatever else is wrong with the action
    if (!this.readable(principal, m.keyspaceId)) return rej(mkReject(Code.PERMISSION_DENIED));
    const manifest = this.manifests.get(k.manifestVersion);
    const op = manifest?.opType(m.opType);
    // step 6: the duplicate lookup (07 §7.6)
    const content = toHex(encodeContent(m));
    const bound = k.bindings.get(id);
    const voided = k.voided.get(id);
    let duplicate = false;
    let voidedResend = false;
    if (bound !== undefined) {
      if (bound.subject !== subject) return rej(mkReject(Code.ID_CONFLICT));
      if (bound.content !== "" && bound.content !== content) return rej(mkReject(Code.IDEMPOTENCY_KEY_REUSED));
      duplicate = true;
    } else if (voided !== undefined) {
      if (voided.subject !== subject) return rej(mkReject(Code.ID_CONFLICT));
      // 07 [MUT-105]: a spend action lost to a restore is processed as a new action; every other resend gets the recorded STALE{EPOCH}, after the lane checks
      voidedResend = !(op?.spend === true && voided.opType === m.opType);
    }
    // step 8: the keyspace exists; a frozen keyspace answers a duplicate too
    if (k.deleted) return rej(mkReject(Code.KEYSPACE_DELETED));
    if (k.frozen) return rej(mkReject(Code.HISTORY_DIVERGED));
    // step 9: the lane checks, which a duplicate meets too (07 [MUT-31], [MUT-32], [MUT-33], store/lanes.go JudgeLane)
    const slots = m.clientId === "" ? undefined : this.slotsOf(k, m.clientId);
    const laneMap = slots === undefined ? undefined : m.lane === Lane.SEQUENCED ? slots.seq : slots.gated;
    let verdict: "proceed" | "stored" | "bind" = "proceed";
    let passed = false;
    if (m.clientId !== "" && slots !== undefined && laneMap !== undefined) {
      // 07 [MUT-30]: the binding of the client_id is judged first
      const owner = this.clients.get(m.clientId);
      if (owner !== undefined && owner !== subject) return rej(mkReject(Code.ACTOR_MISMATCH));
      passed = true;
      const dupLike = duplicate || voidedResend;
      const at = laneMap.get(Number(m.clientSeq));
      if (m.lane === Lane.SEQUENCED) {
        // 07 [MUT-31]: no lane state on either lane and a slot above 1
        if (slots.seq.size === 0 && slots.gated.size === 0 && m.clientSeq > 1n) return rej(mkReject(Code.CLIENT_UNKNOWN), true);
        const l = this.processed(k, slots.seq);
        const seq = Number(m.clientSeq);
        if (seq === l + 1) verdict = dupLike ? "bind" : "proceed";
        else if (seq > l + 1) return { result: retryResult(id, RetryReason.OUT_OF_ORDER, 100), passed: true };
        else if (at === id) verdict = "stored";
        else return rej(mkReject(Code.CLIENT_FORKED), true);
      } else if (at === undefined) verdict = dupLike ? "bind" : "proceed";
      else if (at === id) verdict = "stored";
      else return rej(mkReject(Code.CLIENT_FORKED), true);
      this.bindClient(m.clientId, subject); // 07 [MUT-30]: a client_id is bound at its first use
    }
    if (duplicate) {
      // 07 [MUT-50]: a duplicate at the next slot of its lane binds the slot; the stored outcome is returned
      if (verdict === "bind") laneMap?.set(Number(m.clientSeq), id);
      const p = k.pending.get(id);
      if (p !== undefined) return { result: this.pendingDuplicate(k, p, m), passed };
      return { result: create(MutationResultSchema, { ...(bound as Binding).result, duplicate: true }), passed };
    }
    if (voidedResend) {
      // 07 [MUT-105]: recorded as Reject{STALE, EPOCH}, never applied
      const reject = mkReject(Code.STALE, { stale: { cause: StaleCause.EPOCH } });
      this.bindRecorded(k, principal, m, rejectedResult(id, reject));
      return { result: rejectedResult(id, reject), passed };
    }
    // step 8, after the lane checks: an archived keyspace or a split source answers KEYSPACE_ARCHIVED, which is not recorded
    if (k.lifecycle !== KeyspaceLifecycle.ACTIVE) return rej(mkReject(Code.KEYSPACE_ARCHIVED, { details: { lifecycle: k.lifecycle === KeyspaceLifecycle.SPLIT ? "SPLIT" : "ARCHIVED" }, successors: this.successorsFor(k, m) }), passed);
    if (k.seeding) return { result: retryResult(id, RetryReason.KEYSPACE_SEEDING, 1000), passed: false };
    // steps 10 and 11: the envelope, then the op type and its lane (07 Tables 7-2, 7-3); a lane mismatch is a recorded reject
    if (m.changes.length === 0) return this.final(k, principal, m, content, mkReject(Code.INVALID_ENVELOPE, { message: "no change" }), passed);
    if (op === undefined || manifest === undefined) return rej(mkReject(Code.UNKNOWN_OP_TYPE), passed);
    if ((op.lane === "GATED") !== (m.lane === Lane.GATED)) return this.final(k, principal, m, content, mkReject(Code.INVALID_ENVELOPE, { message: "lane mismatch" }), passed);
    // the base epoch (05 [STATE-81]): not for a spend action
    if (m.baseLogEpoch !== 0 && m.baseLogEpoch !== k.epoch && !op.spend) {
      return this.final(k, principal, m, content, mkReject(Code.STALE, { stale: { cause: StaleCause.EPOCH, currentSeq: k.head } }), passed);
    }
    // step 12, after the base epoch: the offline horizon (07 [MUT-84]): the record at seq base_seq + 1 is older than `max_offline`, or at or below the prune floor
    {
      const n = m.baseSeq + 1n;
      const rec = k.log.find((r) => r.seq === n);
      const maxOffline = Number(this.limits.maxOfflineSeconds) * 1000;
      const recMs = rec?.serverTime === undefined ? undefined : Number(rec.serverTime.seconds) * 1000 + Math.floor(rec.serverTime.nanos / 1e6);
      if ((k.pruned && n <= k.pruneFloor) || (recMs !== undefined && recMs < this.clock.now() - maxOffline)) {
        return this.final(k, principal, m, content, mkReject(Code.OFFLINE_TOO_LONG, { details: { cause: "HORIZON" } }), passed);
      }
    }
    const err = this.checkChanges(k, m, manifest);
    if (err !== undefined) return this.final(k, principal, m, content, err, passed);
    const stale = this.checkPreconditions(k, m);
    if (stale !== undefined) return this.final(k, principal, m, content, stale, passed);
    // 07 Table 7-1 row 16, 09 [READ-89]: each id of `blob_refs` names a committed blob of the keyspace, the first failure in the order of the refs deciding
    if (m.blobRefs.length > 0) {
      const j = this.blobs.judgeRefs(k.id, m.blobRefs);
      if (j.retry !== undefined) return { result: retryResult(id, j.retry.reason, j.retry.afterMs), passed };
      if (j.reject !== undefined) return j.reject.code === Code.BLOB_DELETED ? this.final(k, principal, m, content, j.reject, passed) : rej(j.reject, passed);
    }
    if (m.lane === Lane.GATED) return this.registerIntent(k, principal, m, content, laneMap, op.logIntent);
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
    k.voided.delete(m.mutationId);
    k.bindings.set(m.mutationId, { subject: subjectOf(principal), content: toHex(encodeContent(m)), opType: m.opType, result, final: true, mutation: m });
    if (m.clientId !== "") {
      const slots = this.slotsOf(k, m.clientId);
      (m.lane === Lane.SEQUENCED ? slots.seq : slots.gated).set(Number(m.clientSeq), m.mutationId);
    }
  }

  /** A final reject: recorded when 07 §7.6 says so (the slot is consumed), unrecorded otherwise. */
  private final(k: FakeKeyspace, principal: Principal, m: Mutation, _content: string, r: Reject, passed = true): { result: MutationResult; passed: boolean } {
    const result = rejectedResult(m.mutationId, r);
    if (isRecorded(r)) this.bindRecorded(k, principal, m, result);
    return { result, passed };
  }

  private checkChanges(k: FakeKeyspace, m: Mutation, manifest: ReturnType<StaticManifests["get"]>): Reject | undefined {
    if (manifest === undefined) return mkReject(Code.UNKNOWN_OP_TYPE);
    const lim = this.limits;
    if (m.changes.length === 0) return mkReject(Code.INVALID_ENVELOPE, { message: "no change" });
    // 07 [MUT-76], Table 7-1 step 13: the action limits; the size of the action is its binary encoding (04 Table 4.10a)
    if (m.changes.length > lim.maxChangesPerAction) return mkReject(Code.PAYLOAD_TOO_LARGE);
    if (toBinary(MutationSchema, m).byteLength > Number(lim.maxActionBytes)) return mkReject(Code.PAYLOAD_TOO_LARGE);
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

  // -- GATED intents (08 §8.2) --

  /** Whether the lane wait of a pending intent is satisfied (07 [MUT-37]); once satisfied it stays so (`store/mem/gated.go`, waitSatisfied). */
  private waitSatisfied(k: FakeKeyspace, p: PendingIntent): boolean {
    if (p.wait.satisfied) return true;
    if (p.wait.after === 0n || p.wait.clientId === "") return (p.wait.satisfied = true);
    const slots = k.lanes.get(p.wait.clientId);
    if (slots !== undefined && BigInt(this.processed(k, slots.seq)) >= p.wait.after) return (p.wait.satisfied = true);
    return false;
  }

  /**
   * What a registration, a resend and `GetMutationResult` answer for a pending intent (`store/gated.go`, ResultFor and ReasonOf):
   * `Retry{LANE_NOT_READY}` while the lane wait is not satisfied, else `Retry{GATE_TIMEOUT}`; `intent_seq` only when the INTENT is
   * logged (07 [MUT-8]), so 0 for an op type with `log_intent: false`.
   */
  private pendingResult(k: FakeKeyspace, id: string, p: PendingIntent): MutationResult {
    return this.waitSatisfied(k, p) ? retryResult(id, RetryReason.GATE_TIMEOUT, 1000, p.intentSeq) : retryResult(id, RetryReason.LANE_NOT_READY, 100, p.intentSeq);
  }

  /** A resend of a pending intent: the lane wait of the newest client id of the subject replaces the old one while it is not satisfied (07 [MUT-39]). */
  private pendingDuplicate(k: FakeKeyspace, p: PendingIntent, m: Mutation): MutationResult {
    if (m.clientId !== "" && !this.waitSatisfied(k, p) && this.newestClient.get(subjectOf(p.principal)) === m.clientId) {
      p.wait = { clientId: m.clientId, after: m.afterClientSeq, satisfied: false };
    }
    return create(MutationResultSchema, { ...this.pendingResult(k, m.mutationId, p), duplicate: true });
  }

  /** The registered, undecided GATED actions of a client instance in every keyspace, or of a subject for a laneless action (07 [MUT-42]). */
  private inflight(m: Mutation, principal: Principal): number {
    let n = 0;
    for (const k of this.keyspaces.values()) {
      for (const p of k.pending.values()) {
        if (m.clientId !== "" ? p.mutation.clientId === m.clientId : p.mutation.clientId === "" && subjectOf(p.principal) === subjectOf(principal)) n++;
      }
    }
    return n;
  }

  private registerIntent(k: FakeKeyspace, principal: Principal, m: Mutation, content: string, laneMap: Map<number, string> | undefined, logIntent: boolean): { result: MutationResult; passed: boolean } {
    // 07 [MUT-42], 08 [GATE-7]: over the in-flight cap the mutation is not registered, binds no slot and is not logged
    if (this.inflight(m, principal) >= Number(this.limits.maxInflightGatedPerClient)) return { result: retryResult(m.mutationId, RetryReason.RATE_LIMITED, 1000), passed: true };
    // 08 §8.2: registered; with `log_intent` it is logged as an INTENT record with the intent row (06 [LOG-22]), without it nothing is logged
    let intentSeq = 0n;
    if (logIntent) {
      // 06 §6.5: a PENDING row holds `status`, `op_type` and `deadline`, in canonical order
      const row = create(KeyChangeSchema, { key: `abpc.intent/${m.mutationId}`, kind: ChangeKind.SET, value: new TextEncoder().encode(JSON.stringify({ deadline: new Date(this.clock.now() + this.decisionDeadlineMs).toISOString(), op_type: m.opType, status: "PENDING" })), encoding: "json/v1" });
      const rec = this.record(k, RecordKind.INTENT, { principal, mutationId: m.mutationId, opType: m.opType, changes: [...m.changes, row], authoredAt: m.authoredAt });
      this.commit(k, rec);
      intentSeq = rec.seq;
    }
    const p: PendingIntent = { mutation: m, principal, intentSeq, acceptedSeq: k.head, logged: logIntent, wait: { clientId: m.clientId, after: m.afterClientSeq, satisfied: false } };
    k.voided.delete(m.mutationId);
    k.pending.set(m.mutationId, p);
    const result = this.pendingResult(k, m.mutationId, p);
    k.bindings.set(m.mutationId, { subject: subjectOf(principal), content, opType: m.opType, result, final: false, mutation: m, intentSeq, acceptedSeq: p.acceptedSeq });
    laneMap?.set(Number(m.clientSeq), m.mutationId);
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
    k.voided.delete(m.mutationId);
    k.bindings.set(m.mutationId, { subject: subjectOf(principal), content, opType: m.opType, result, final: true, mutation: m });
    laneMap?.set(Number(m.clientSeq), m.mutationId);
    return { result, passed: true };
  }
}

// create() needs the schema object under a stable name here
const LoadStateResponse_ = LoadStateResponseSchema;

/** A keyspace pattern of `RevokeAccess` (03 §3.7): a keyspace id, or a prefix and a trailing `*`. */
function matchesPattern(pattern: string, id: string): boolean {
  return pattern.endsWith("*") ? id.startsWith(pattern.slice(0, -1)) : pattern === id;
}

/** Whether a load entry is in an interest (09 [READ-9]): under a prefix, or a tombstone entry of an entity or staged marker that is an ancestor of one. */
function entryInInterest(e: KeyEntry, interest: readonly string[]): boolean {
  if (interest.length === 0) return true;
  if (interest.some((p) => e.key === p || e.key.startsWith(`${p}/`))) return true;
  return e.deleted && interest.some((p) => p.startsWith(`${e.key}/`));
}

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
  readonly principal: Principal;

  constructor(
    readonly server: FakeServer,
    principal: Principal,
  ) {
    // each transport has a token of its own, issued now (10 [AUTH-52]): the server judges it against the revocations by its principal object
    this.principal = { ...principal };
    server.tokens.set(this.principal, { iat: server.clock.now() });
  }

  /** The app gets a new token: issued now (or at `iat`). A token issued before a revocation's `not_before` (plus 60 s) is refused. */
  renewToken(iat?: number): void {
    this.server.tokens.set(this.principal, { iat: iat ?? this.server.clock.now() });
  }

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
  joinPresence(req: JoinPresenceRequest): Promise<JoinPresenceResponse> {
    return this.server.joinPresence(this.principal, req);
  }
  publishPresence(req: PublishPresenceRequest): Promise<PublishPresenceResponse> {
    return this.server.publishPresence(this.principal, req);
  }
  leavePresence(req: LeavePresenceRequest): Promise<LeavePresenceResponse> {
    return this.server.leavePresence(this.principal, req);
  }
  createUpload(req: CreateUploadRequest): Promise<CreateUploadResponse> {
    return this.server.createUpload(this.principal, req);
  }
  commitBlob(req: CommitBlobRequest): Promise<CommitBlobResponse> {
    return this.server.commitBlob(this.principal, req);
  }
  getBlob(req: GetBlobRequest): Promise<GetBlobResponse> {
    return this.server.getBlob(this.principal, req);
  }
  deleteBlob(req: DeleteBlobRequest): Promise<DeleteBlobResponse> {
    return this.server.deleteBlob(this.principal, req);
  }
}
