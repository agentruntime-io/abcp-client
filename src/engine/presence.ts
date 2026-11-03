// Presence on the engine's session (09 §9.8, 14 [CLI-61], [CLI-78], 02 [ARC-51] item 4): joining a topic, publishing the entry of the engine into it, leaving it,
// and the view of what the other members publish.
//
//  - [READ-73] A topic is joined with `JoinPresence` on the session that `Subscribe` opened, once the session follows the keyspace (the `KeyspaceAdded` of the
//    keyspace has come). The server sends `PresenceFrame{snapshot: true}` before any other frame of the topic; the view of the topic is replaced by it. A repeated
//    join sends the snapshot again. "Memberships and entries belong to one session: after a new session (READ-57), a client MUST join its topics again and publish
//    its entries again, which is re-reading (02 [ARC-51] item 4)": a new `Subscribed` makes every topic unjoined and its entry stale.
//  - [READ-74] One entry per topic for all the tabs of the engine, published through the one session. The state of a plain topic is the latest state that one of the
//    tabs set; the engine does not leave the topic while one of its tabs is in it, and leaves it (`LeavePresence`) when the last one has left. The state is json/v1
//    (canonical, 05 [STATE-7]), refused locally when it is over `limits.max_presence_state_bytes`, and the server answers `PAYLOAD_TOO_LARGE` (before the schema) and
//    `SCHEMA_INVALID`, which are reported, not resent.
//  - [READ-83] Yjs awareness: the state of the entry is a JSON object that maps the id of each tab of the engine in the topic to that tab's state.
//  - [READ-75] `principal` and `client_id` of an entry are the server's: the engine reads them, it sends neither.
//  - [READ-76] An entry expires `limits.presence_ttl_seconds` after its last publish. The engine publishes again, with the same state, at about half the TTL; a view
//    drops an entry whose `expires_at` has passed (server time, 14 [CLI-55]), whether or not `left_client_ids` came ([READ-81]).
//  - [READ-77] The entry and the membership end with the session, with a `LeavePresence`, and when the keyspace is dropped on the session: after a `KeyspaceDropped`
//    the topic is unjoined and joins again when the keyspace is added again. A rebind of the session's `client_id` (14 [CLI-78]) ends the earlier instance's entries and
//    keeps the memberships: the engine publishes again at once, in every topic where it has an entry.
//  - [READ-79] The engine publishes at most once in `limits.presence_min_publish_interval_ms` per topic, counted from the answer of the last publish (which the server
//    applied before it answered), so that it is never over the rate; states set meanwhile coalesce into the latest. A server that refuses a publish over the rate with
//    `Retry{RATE_LIMITED}` is obeyed: the latest state is sent again after the larger of `after_ms` and the client backoff (14 [CLI-43]).
//  - [READ-81] A snapshot replaces the view of a topic, upserts replace whole entries, `left_client_ids` remove entries; an upsert whose `expires_at` is older than that of
//    the entry held for the same client instance is ignored. A presence frame never moves a cursor, a watermark or a lane watermark: it is applied outside the log.
//  - [READ-80] A frame is applied only to the topic that it names, in the keyspace that it names, and only when this engine has joined that topic.
//  - [SEC-1], [SEC-28] A presence state is untrusted input: this layer keeps the bytes as they came, and the view decodes them with the strict json/v1 reader. What a server sends is bounded as what it
//    promised: an entry over `limits.max_presence_state_bytes` ([READ-74]) or with a client id that is no client id is dropped (and reported once, `protocol-error`), the view of a topic holds at most
//    `limits.max_presence_members_per_topic` entries ([READ-79]) and `MAX_VIEW_BYTES` octets, and an entry expires `limits.presence_ttl_seconds` after it was published ([READ-76]), whatever `expires_at` says.
//  - [CLI-66] The purge of a keyspace (`ACCESS_REVOKED` and the revocation of the subject) removes the presence state with the rest of the purge list (`presencePurge`).
//
// Q75.2 (decided, V21 MUST-4): "When the last of its tabs in the topic leaves, the engine MUST leave the topic with `LeavePresence`", and a tab id is "not kept after the tab closes" ([READ-83]): a follower tab
// that closes without a completed `stop()` is a tab that left. The engine learns it from the tab's `gone` message, from a `pagehide` hint, and, for a crash or a close that sent nothing, from the lock that
// each tab holds and the flusher asks for (`tab-layer.ts`, `presenceTabGone`).
//
// Q75.1 (decided, V21 SHOULD-1): "When several of its tabs are in one topic, the entry's state is the latest state that one of them set": "them" are the tabs in the topic, so when a tab leaves, the entry
// takes the latest state that one of the tabs that remain set (`presenceLeave`); "its entry stays" is about the entry, not its state. When no tab that remains set a state there is nothing to take, and the
// entry stays as it is, marked as the state of a tab that left (`orphaned`): the one gap of the text.
//
// SPEC-QUESTION (09 [READ-74], residual of Q75.1): the text does not say what the entry holds when the only tab that set a state leaves and the tabs that remain set none. Taken: the entry keeps that
// state (a reading of the last state, marked `orphaned` in `presenceTopics`) and is kept alive with it, until a remaining tab sets one or the last tab leaves. Reading B: the entry is left to expire.
// For Yjs awareness the state is the map of the tabs that remain, and the engine publishes it ([READ-83]).

import { create } from "@bufbuild/protobuf";
import { timestampMs } from "@bufbuild/protobuf/wkt";
import { JoinPresenceRequestSchema, LeavePresenceRequestSchema, PublishPresenceRequestSchema, type PresenceEntry, type PresenceFrame } from "../gen/abpc/v2/sync_pb.js";
import { Code, RetryReason, type Principal } from "../gen/abpc/v2/types_pb.js";
import { canonical, parse, type JsonValue } from "../rules/jsonv1/index.js";
import { resendDelay } from "./backoff.js";
import { codeName } from "./codes.js";
import { LocalRefusal, ProtocolViolation, RpcError } from "./errors.js";
import { checkInterest } from "./replica-store.js";
import { SessionLayer } from "./session.js";

/** An entry of a topic as the server sent it (09 [READ-75]). `state` is the json/v1 text, untrusted until a reader decodes it. */
export interface PresenceEntryRaw {
  /** The key of the entry: the client instance that published it (09 [READ-75]), never a session id. */
  clientId: string;
  principal: Principal | undefined;
  state: Uint8Array;
  /** Server time, in ms since the epoch (14 [CLI-55]). */
  expiresAtMs: number;
}

/** What the presence layer tells its listeners (`Engine.onPresence`). */
export type PresenceEvent =
  /** The view of a topic changed: a snapshot, an upsert, `left_client_ids`, an expiry, a purge. */
  | { type: "changed"; keyspaceId: string; topic: string }
  /** The server made the session a member of the topic (09 [READ-73]). */
  | { type: "joined"; keyspaceId: string; topic: string }
  /** A publish was answered: `expiresAt` is server time (09 [READ-74]). */
  | { type: "published"; keyspaceId: string; topic: string; expiresAt: number }
  /**
   * A call was refused and is not resent as it is (A.7): `INVALID_KEY` (a topic that no template of the kind matches, or a kind with no presence), `SCHEMA_INVALID`,
   * `PAYLOAD_TOO_LARGE`, `PERMISSION_DENIED`, `QUOTA_EXCEEDED` (a join over the member cap), ...; `local` is true when the engine refused before it sent.
   */
  | { type: "refused"; keyspaceId: string; topic: string; operation: "join" | "publish"; code: string; status?: string; local?: boolean; detail?: string }
  /** The engine's presence in the keyspace ended for good: its purge (`ACCESS_REVOKED`, a deleted keyspace, a revoked subject, 14 [CLI-66], [CLI-77]). */
  | { type: "ended"; keyspaceId: string; topic: string; cause: string };

export type PresenceListener = (event: PresenceEvent) => void;

/** How the topics of a keyspace are judged (09 [READ-72], [READ-83]). */
export interface JoinOptions {
  /** The topic is a Yjs awareness topic, a key of a `crdt:*` template (09 [READ-83]): its state maps tab ids to states. Default: what the manifest says of the topic. */
  awareness?: boolean;
}

const sep = "\u0000";

/**
 * The most entries that the view of one topic holds. A topic's members are capped by the server (`max_presence_members_per_topic`, 09 [READ-79]), whose maximum is the 32-bit integer of
 * 04 Table 4.10a, and what a server sends is untrusted input (16 [SEC-1]): a frame of 16 MiB holds some hundreds of thousands of entries. The view keeps the first of them.
 */
export const MAX_VIEW_ENTRIES = 100_000;
/** The most octets (states and client ids) that the view of one topic holds (16 [SEC-1]): a server that allows 64 KiB states and many members does not make the client hold them all. */
export const MAX_VIEW_BYTES = 8 << 20;
/** A client id is a UUID (03 [ID-35]); one longer than this is not one. */
const MAX_CLIENT_ID_LENGTH = 128;
/** How far past `now + presence_ttl` an entry's `expires_at` may be, in server time: the estimate of the server's clock is not exact (14 [CLI-55]). */
const EXPIRY_SLACK_MS = 5_000;
const keyOf = (ks: string, topic: string): string => `${ks}${sep}${topic}`;

/**
 * The sizes of a principal that the text gives (V23 MUST-2, 16 [SEC-1]): a `sub` is at most 255 bytes (10 [AUTH-8]), an actor chain has at most 16 entries, each at most 255 octets
 * (10 [AUTH-13]), and a `device_id` is at most 128 bytes, or, for a DEVICE principal that takes it from its `sub`, as long as the `sub` is (10 [AUTH-26]): the one limit that holds for
 * both is 255. A token that breaks them is invalid, so a server that sends a principal over them sends what it never held.
 */
const MAX_SUB_OCTETS = 255;
const MAX_ACTOR_CHAIN_ENTRIES = 16;
const MAX_ACTOR_OCTETS = 255;
const MAX_DEVICE_ID_OCTETS = 255;

/** The octets of the text in UTF-8, without making the bytes. */
function octets(s: string): number {
  let n = 0;
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c < 0x80) n += 1;
    else if (c < 0x800) n += 2;
    else if (c >= 0xd800 && c <= 0xdbff && i + 1 < s.length && s.charCodeAt(i + 1) >= 0xdc00 && s.charCodeAt(i + 1) <= 0xdfff) {
      n += 4;
      i++;
    } else n += 3;
  }
  return n;
}

/** Whether the text is longer than `max` octets in UTF-8: a code unit is at most three of them, so a short text is not counted. */
const over = (s: string, max: number): boolean => s.length * 3 > max && octets(s) > max;

/** What a principal weighs in the view: every string that it carries (the issuer and the app id too, which the text gives no size, so the byte bound is what holds them). */
function principalSize(p: Principal | undefined): number {
  if (p === undefined) return 0;
  let n = octets(p.sub) + octets(p.issuer) + octets(p.appId) + octets(p.deviceId);
  for (const a of p.actorChain) n += octets(a);
  return n;
}

/** Why a principal is no one that a server could have verified (V23 MUST-2), or undefined. */
function principalProblem(p: Principal | undefined): string | undefined {
  if (p === undefined) return undefined;
  if (over(p.sub, MAX_SUB_OCTETS)) return `a sub of ${octets(p.sub)} octets, over the 255 of 10 [AUTH-8]`;
  if (p.actorChain.length > MAX_ACTOR_CHAIN_ENTRIES) return `an actor chain of ${p.actorChain.length} entries, over the 16 of 10 [AUTH-13]`;
  for (const a of p.actorChain) if (over(a, MAX_ACTOR_OCTETS)) return `an actor chain entry of ${octets(a)} octets, over the 255 of 10 [AUTH-13]`;
  if (over(p.deviceId, MAX_DEVICE_ID_OCTETS)) return `a device id of ${octets(p.deviceId)} octets, over the limit of 10 [AUTH-26]`;
  return undefined;
}

/** What an entry costs the view: its state, its client id, its principal and what the entry itself weighs. */
const entrySize = (e: PresenceEntryRaw): number => e.state.length + e.clientId.length + principalSize(e.principal) + 64;

/** The view of a topic: a map of the entries by client instance that knows the octets that it holds. */
class View {
  private readonly m = new Map<string, PresenceEntryRaw>();
  /** What each entry cost when it was put (`entrySize`), so that the bytes that the view holds are taken out as they were put in. */
  private readonly sizes = new Map<string, number>();
  bytes = 0;

  /** The octets that the entry of the client instance weighs in the view; 0 for none. */
  sizeOf(id: string): number {
    return this.sizes.get(id) ?? 0;
  }

  get size(): number {
    return this.m.size;
  }
  get(id: string): PresenceEntryRaw | undefined {
    return this.m.get(id);
  }
  has(id: string): boolean {
    return this.m.has(id);
  }
  set(id: string, e: PresenceEntryRaw): void {
    this.bytes -= this.sizeOf(id);
    const size = entrySize(e);
    this.m.set(id, e);
    this.sizes.set(id, size);
    this.bytes += size;
  }
  delete(id: string): boolean {
    if (!this.m.delete(id)) return false;
    this.bytes -= this.sizeOf(id);
    this.sizes.delete(id);
    return true;
  }
  values(): IterableIterator<PresenceEntryRaw> {
    return this.m.values();
  }
  [Symbol.iterator](): IterableIterator<[string, PresenceEntryRaw]> {
    return this.m[Symbol.iterator]();
  }
}

interface TabState {
  /** The json/v1 text of the state that the tab set; undefined: the tab joined and set none. */
  state: Uint8Array | undefined;
  /** When the tab set it, in the order of the topic's sets: the latest state of the tabs that are in the topic is the entry's (09 [READ-74]). */
  setSeq: number;
}

type Op = "join" | "publish";

/** What the engine keeps of one topic. */
class Topic {
  readonly tabs = new Map<string, TabState>();
  /** Plain topics: the latest state that a tab set (09 [READ-74]), among the tabs that are in the topic; when the tab that set it left and no remaining tab set one, the state of that tab (see `orphaned`). */
  latest: Uint8Array | undefined;
  /** The sets of the topic so far: `TabState.setSeq` is drawn from it. */
  setCounter = 0;
  /** `latest` is the state of a tab that left, and no tab that remains set one: the one gap of 09 [READ-74] (Q75.1). */
  orphaned = false;
  /** An entry of a frame was dropped as no entry of the server can be, and the engine said so (once for a session, not for every frame that a server floods). */
  droppedReported = false;
  /** The join was under way when the last tab left: the server made the session a member all the same, so the engine leaves when the join is answered. */
  leaveWhenJoined = false;
  /** The entry that was refused for its size, as text: it is not refused again until it changes. */
  refusedEntry: Uint8Array | undefined;
  /** The session id on which the server made this session a member; undefined: not a member (a new session, a drop, a refusal). */
  joined: string | undefined;
  /** A call is under way. */
  op: Op | undefined;
  /** The entry on the server is not the wanted one: a state was set, or the session is new. */
  stale = false;
  /** The keepalive is due (09 [READ-76]). */
  keepalive = false;
  /** The state that the server holds (the last publish that it answered), as the text of that publish. */
  published: Uint8Array | undefined;
  /** The monotonic time at which the last publish was answered: the interval of 09 [READ-79] runs from it. */
  answeredMono: number | undefined;
  /** Consecutive failures of the calls of this topic, the exponent of the client backoff (14 [CLI-43]). */
  failures = 0;
  authTries = 0;
  /** A refusal that no resend changes: the topic joins again at the next session, or when the app joins again. */
  blocked: string | undefined;
  /** The monotonic time before which no call of the topic is made again: the wait of a failed call (the client backoff, 14 [CLI-43]; `Retry.after_ms`, 09 [READ-79]). */
  retryAt = 0;
  waitTimer: (() => void) | undefined;
  keepTimer: (() => void) | undefined;
  /** The view: the entries that the server told this client of, by client instance. */
  view = new View();
  /** The topic was left: its calls that are under way end without effect. */
  closed = false;

  constructor(
    readonly keyspaceId: string,
    readonly topic: string,
    public awareness: boolean,
  ) {}
}

export abstract class PresenceLayer extends SessionLayer {
  private readonly topics = new Map<string, Topic>();
  /** The most entries that the view of a topic holds (`MAX_VIEW_ENTRIES`); a test lowers it. */
  protected maxViewEntries = MAX_VIEW_ENTRIES;
  /** A follower tab's copy of the views that the flusher holds. */
  private readonly mirror = new Map<string, PresenceEntryRaw[]>();
  private readonly presenceListeners = new Set<PresenceListener>();
  private expiryTimer: (() => void) | undefined;
  /** The client id that the entries of this engine were last published under: the entry of an earlier instance is dropped from the views at a rebind (09 [READ-77]). */
  private presenceClientId = "";

  protected resetRuntime(): void {
    super.resetRuntime();
    for (const t of this.topics.values()) this.closeTimers(t);
    this.topics.clear();
    this.mirror.clear();
    this.expiryTimer?.();
    this.expiryTimer = undefined;
    this.presenceClientId = "";
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Listeners and the view

  /** Registers a listener of the presence events. A listener that throws never stops the engine. Returns the function that removes it. */
  onPresence(listener: PresenceListener): () => void {
    this.presenceListeners.add(listener);
    return () => this.presenceListeners.delete(listener);
  }

  /** The engine tells its listeners, and the other tabs of the engine (`onPresenceEmitted`, tab-layer.ts). */
  protected emitPresence(e: PresenceEvent): void {
    this.deliverPresence(e);
    this.onPresenceEmitted(e);
  }

  /** Tells the listeners of this tab only. */
  protected deliverPresence(e: PresenceEvent): void {
    for (const l of [...this.presenceListeners]) {
      try {
        l(e);
      } catch (err) {
        this.noteInternalError(err);
      }
    }
  }

  /** A presence event of the flusher goes to the followers too (14 [CLI-61]: "their presence is published by the engine's session"; they read it through the engine). */
  protected onPresenceEmitted(_e: PresenceEvent): void {
    // nothing here: `TabLayer` overrides it
  }

  /** What the flusher told this follower tab of a topic (14 [CLI-61]): the entries that it holds, which `presenceEntries` reads here. */
  protected presenceMirrorSet(keyspaceId: string, topic: string, entries: PresenceEntryRaw[]): void {
    const key = keyOf(keyspaceId, topic);
    if (entries.length === 0) this.mirror.delete(key);
    else this.mirror.set(key, entries);
    this.deliverPresence({ type: "changed", keyspaceId, topic });
  }

  /** The entries of a topic that have not expired (09 [READ-81]), in the order of their client ids. A topic that the engine has not joined has none. */
  presenceEntries(keyspaceId: string, topic: string): PresenceEntryRaw[] {
    const now = this.serverNow();
    if (this.role === "follower") {
      return (this.mirror.get(keyOf(keyspaceId, topic)) ?? []).filter((e) => e.expiresAtMs > now);
    }
    const t = this.topics.get(keyOf(keyspaceId, topic));
    if (t === undefined) return [];
    return [...t.view.values()].filter((e) => e.expiresAtMs > now).sort((a, b) => (a.clientId < b.clientId ? -1 : a.clientId > b.clientId ? 1 : 0));
  }

  /** Whether the topic is a Yjs awareness topic (09 [READ-83]): the registration's, or what the manifest says of it. */
  presenceAwareness(keyspaceId: string, topic: string): boolean {
    return this.topics.get(keyOf(keyspaceId, topic))?.awareness ?? this.awarenessOf(keyspaceId, topic);
  }

  /** Whether this engine is in the topic (a tab joined it and none has left). */
  inPresence(keyspaceId: string, topic: string): boolean {
    return this.topics.has(keyOf(keyspaceId, topic));
  }

  /** The topics that this engine is in, for a test and for the sync status. */
  presenceTopics(): { keyspaceId: string; topic: string; joined: boolean; awareness: boolean; tabs: string[]; orphaned: boolean }[] {
    return [...this.topics.values()].map((t) => ({ keyspaceId: t.keyspaceId, topic: t.topic, joined: t.joined !== undefined, awareness: t.awareness, tabs: [...t.tabs.keys()], orphaned: t.orphaned }));
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Joining, publishing and leaving: what the tabs of the engine ask for (a task of the engine's queue)

  /** Whether a topic of the keyspace is a Yjs awareness topic of the kind (09 [READ-72], [READ-83]); the manifest decides. */
  protected awarenessOf(keyspaceId: string, topic: string): boolean {
    const f = this.follows.get(keyspaceId);
    if (f === undefined) return false;
    return this.manifests.get(f.state.manifestVersion)?.presenceTopicKind?.(f.kind, topic) === "awareness";
  }

  /** A tab joins a topic (09 [READ-73], [READ-74]). The engine joins it on the session as soon as the session follows the keyspace. */
  protected presenceJoin(tab: string, keyspaceId: string, topic: string, opts: JoinOptions = {}): void {
    const f = this.follows.get(keyspaceId);
    if (f === undefined || f.purged) throw new LocalRefusal("NOT_FOLLOWED", `the keyspace ${keyspaceId} is not followed`);
    const bad = checkInterest([topic], 1);
    if (bad !== undefined) throw new LocalRefusal("INVALID", bad.replace("interest prefix", "presence topic").replace("(09 [READ-22])", "(09 [READ-72])"));
    const key = keyOf(keyspaceId, topic);
    let t = this.topics.get(key);
    if (t === undefined) {
      t = new Topic(keyspaceId, topic, opts.awareness ?? this.awarenessOf(keyspaceId, topic));
      this.topics.set(key, t);
    }
    const fresh = !t.tabs.has(tab);
    if (fresh) t.tabs.set(tab, { state: undefined, setSeq: 0 });
    t.blocked = undefined; // the app asks again: a refusal of before is tried once more
    // a tab that joins a topic that the engine is in already has the view that the engine holds, which no frame brings it: the other tabs read it from the flusher (14 [CLI-61])
    if (fresh && t.view.size > 0) this.emitPresence({ type: "changed", keyspaceId, topic });
    this.pumpTopic(t);
  }

  /**
   * A tab sets its state in a topic it is in (09 [READ-74], [READ-83]): `state` is json/v1 text. A state over `limits.max_presence_state_bytes` is refused before it is
   * sent (the server answers `PAYLOAD_TOO_LARGE`, checked before the schema).
   */
  protected presenceSet(tab: string, keyspaceId: string, topic: string, state: Uint8Array): void {
    const t = this.topics.get(keyOf(keyspaceId, topic));
    const mine = t?.tabs.get(tab);
    if (t === undefined || mine === undefined) throw new LocalRefusal("REFUSED", `the tab is not in the presence topic ${topic} of ${keyspaceId}: join it first`);
    if (!t.awareness && state.length > this.limits.maxPresenceStateBytes) {
      throw new LocalRefusal("INVALID", `a presence state of ${state.length} octets, over max_presence_state_bytes (${this.limits.maxPresenceStateBytes}, 09 [READ-74])`);
    }
    mine.state = state;
    mine.setSeq = ++t.setCounter;
    t.latest = state;
    t.orphaned = false; // a tab that is in the topic set it
    t.stale = true;
    this.pumpTopic(t);
  }

  /** A tab leaves a topic. The engine leaves it (`LeavePresence`) when the last tab has left, and not before (09 [READ-74]). */
  protected presenceLeave(tab: string, keyspaceId: string, topic: string): void {
    const t = this.topics.get(keyOf(keyspaceId, topic));
    if (t === undefined || !t.tabs.delete(tab)) return;
    if (t.tabs.size === 0) {
      this.closeTopic(t, true);
      return;
    }
    // Yjs awareness: the state is the map of the tabs that remain
    if (t.awareness) {
      t.stale = true;
      this.pumpTopic(t);
      return;
    }
    // Q75.1, 09 [READ-74]: "the latest state that one of them set": the entry takes the latest state of the tabs that remain, which republishes it when it is not the one that the server holds
    let best: TabState | undefined;
    for (const s of t.tabs.values()) if (s.state !== undefined && (best === undefined || s.setSeq > best.setSeq)) best = s;
    if (best?.state !== undefined) {
      t.orphaned = false;
      if (!this.sameBytes(best.state, t.latest)) {
        t.latest = best.state;
        t.stale = true;
        this.pumpTopic(t);
      }
    } else if (t.latest !== undefined) {
      t.orphaned = true; // no tab that remains set a state: the entry stays as the tab that left made it (the one gap)
    }
  }

  /** A tab of the engine went away (it said so, 14 [CLI-61]): it leaves every topic that it is in. */
  protected presenceTabGone(tab: string): void {
    for (const t of [...this.topics.values()]) if (t.tabs.has(tab)) this.presenceLeave(tab, t.keyspaceId, t.topic);
  }

  /** [READ-77]: the entry and the membership end with this call; one that does not reach the server ends with the session or by its TTL. */
  private sendLeave(t: Topic, member: string): void {
    this.detach(
      this.transport.leavePresence(create(LeavePresenceRequestSchema, { sessionId: member, keyspaceId: t.keyspaceId, topic: t.topic })).catch((e: unknown) => {
        this.noteRevoked(e);
      }),
    );
  }

  private closeTimers(t: Topic): void {
    t.waitTimer?.();
    t.waitTimer = undefined;
    t.keepTimer?.();
    t.keepTimer = undefined;
  }

  private closeTopic(t: Topic, sendLeave: boolean): void {
    t.closed = true;
    this.closeTimers(t);
    this.topics.delete(keyOf(t.keyspaceId, t.topic));
    const member = t.joined;
    t.joined = undefined;
    const had = t.view.size > 0;
    t.view = new View();
    if (sendLeave && member !== undefined && member === this.sessionId()) this.sendLeave(t, member);
    else if (sendLeave && t.op === "join") t.leaveWhenJoined = true; // the join is under way: the server makes the session a member, and the answer finds the topic left
    if (had) this.emitPresence({ type: "changed", keyspaceId: t.keyspaceId, topic: t.topic });
    this.armExpiry();
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // The state of the entry

  /** The text of the entry that the engine wants the server to hold for a topic, or undefined when no tab has set a state. */
  private wanted(t: Topic): Uint8Array | undefined {
    if (!t.awareness) return t.latest;
    // [READ-83]: a JSON object that maps the id of each tab in the topic to that tab's state
    const members = new Map<string, JsonValue>();
    for (const [tab, s] of [...t.tabs].sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))) {
      if (s.state !== undefined) members.set(tab, parse(s.state));
    }
    return members.size === 0 ? undefined : canonical(members);
  }

  private sameBytes(a: Uint8Array | undefined, b: Uint8Array | undefined): boolean {
    if (a === b) return true;
    if (a === undefined || b === undefined || a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
    return true;
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // The loop of a topic: join, publish, keep alive

  /** The session follows the keyspace and the topic may be joined now (09 [READ-73]: "requires that the session follows the keyspace"). */
  private presenceReady(t: Topic): boolean {
    if (t.closed || t.blocked !== undefined || this.role !== "flusher" || !this.online() || this.sessionId() === "") return false;
    const f = this.follows.get(t.keyspaceId);
    return f !== undefined && f.loaded && !f.purged && f.stopped === undefined && f.frozen === undefined && f.onSession && !f.awaitingAdd;
  }

  /** What a topic does next: join, publish the entry, or wait for the interval, the keepalive or a retry. */
  private pumpTopic(t: Topic): void {
    t.waitTimer?.();
    t.waitTimer = undefined;
    if (!this.presenceReady(t) || t.op !== undefined) return;
    const wait = t.retryAt - this.clock.mono();
    if (wait > 0) {
      t.waitTimer = this.after(wait, () => this.pumpTopic(t));
      return;
    }
    if (t.joined !== this.sessionId()) {
      t.joined = undefined;
      this.startJoin(t);
      return;
    }
    let want: Uint8Array | undefined;
    try {
      want = this.wanted(t);
    } catch {
      want = undefined; // a tab state that does not read as json/v1: it was checked when it was set
    }
    if (want === undefined) return;
    if (!t.stale && !t.keepalive && this.sameBytes(want, t.published)) return;
    if (want.length > this.limits.maxPresenceStateBytes) {
      t.stale = false;
      t.keepalive = false; // nothing is published, so nothing is kept alive
      if (this.sameBytes(t.refusedEntry, want)) return; // refused already: it is said once, not at every pump
      t.refusedEntry = want;
      this.emitPresence({ type: "refused", keyspaceId: t.keyspaceId, topic: t.topic, operation: "publish", code: codeName(Code.PAYLOAD_TOO_LARGE), local: true, detail: `the entry of ${want.length} octets is over max_presence_state_bytes (${this.limits.maxPresenceStateBytes}, 09 [READ-74])` });
      return;
    }
    t.refusedEntry = undefined;
    // [READ-79]: not sooner than the interval after the answer of the last publish
    if (t.answeredMono !== undefined) {
      const wait = t.answeredMono + this.limits.presenceMinPublishIntervalMs - this.clock.mono();
      if (wait > 0) {
        t.waitTimer = this.after(wait, () => this.pumpTopic(t));
        return;
      }
    }
    this.startPublish(t, want);
  }

  private startJoin(t: Topic): void {
    const sid = this.sessionId();
    t.op = "join";
    this.detach(
      (async () => {
        try {
          await this.transport.joinPresence(create(JoinPresenceRequestSchema, { sessionId: sid, keyspaceId: t.keyspaceId, topic: t.topic }));
        } catch (e) {
          await this.run(() => this.presenceFailed(t, "join", e, sid));
          return;
        }
        await this.run(() => {
          if (t.op === "join") t.op = undefined;
          if (t.closed) {
            // N4: the last tab left while the join was under way, and the answer finds the topic left. V23 MUST-3, 09 [READ-74]: unless a tab joined it again meanwhile (the topic that the engine keeps now is
            // another one, and "The engine MUST NOT leave a topic while any of its tabs is still in it"): the membership that this join made is the session's, and the live topic uses it
            const live = this.topics.get(keyOf(t.keyspaceId, t.topic));
            if (t.leaveWhenJoined && this.sessionId() === sid && (live === undefined || live.closed)) this.sendLeave(t, sid);
            return;
          }
          t.failures = 0;
          t.authTries = 0;
          t.retryAt = 0;
          if (this.sessionId() !== sid) {
            this.pumpTopic(t); // the session ended while the call was under way: the membership was its own
            return;
          }
          t.joined = sid;
          // [READ-73]: after a join on a new session the entry is published again (02 [ARC-51] item 4)
          t.stale = true;
          t.published = undefined;
          this.emitPresence({ type: "joined", keyspaceId: t.keyspaceId, topic: t.topic });
          this.pumpTopic(t);
        });
      })(),
    );
  }

  private startPublish(t: Topic, want: Uint8Array): void {
    const sid = this.sessionId();
    t.op = "publish";
    t.stale = false;
    t.keepalive = false;
    this.detach(
      (async () => {
        let expiresAt: number | undefined;
        try {
          const r = await this.transport.publishPresence(create(PublishPresenceRequestSchema, { sessionId: sid, keyspaceId: t.keyspaceId, topic: t.topic, state: want }));
          expiresAt = r.expiresAt === undefined ? undefined : timestampMs(r.expiresAt);
        } catch (e) {
          t.stale = true; // the server may hold an older state: the latest is sent again, unless the failure says otherwise
          await this.run(() => this.presenceFailed(t, "publish", e, sid));
          return;
        }
        await this.run(() => {
          if (t.op === "publish") t.op = undefined;
          if (t.closed) return;
          t.failures = 0;
          t.authTries = 0;
          t.retryAt = 0;
          if (this.sessionId() !== sid) {
            t.stale = true;
            this.pumpTopic(t);
            return;
          }
          t.published = want;
          t.answeredMono = this.clock.mono();
          const exp = expiresAt ?? this.serverNow() + this.limits.presenceTtlMs;
          this.emitPresence({ type: "published", keyspaceId: t.keyspaceId, topic: t.topic, expiresAt: exp });
          // [READ-76]: publish again, with the same state, at about half the TTL
          t.keepTimer?.();
          t.keepTimer = this.after(Math.max(this.limits.presenceMinPublishIntervalMs, Math.floor(this.limits.presenceTtlMs / 2)), () => {
            t.keepTimer = undefined;
            t.keepalive = true;
            this.pumpTopic(t);
          });
          this.pumpTopic(t);
        });
      })(),
    );
  }

  /** What a failed call does (A.7, 09 [READ-73], [READ-74]). */
  private presenceFailed(t: Topic, op: Op, e: unknown, sid: string): void {
    if (t.op === op) t.op = undefined;
    if (t.closed) return;
    if (this.sessionId() !== sid) {
      this.pumpTopic(t); // the session that the call named is over: nothing of its answer applies to the new one
      return;
    }
    const base = { type: "refused", keyspaceId: t.keyspaceId, topic: t.topic, operation: op } as const;
    const retry = (afterMs?: number): void => {
      const delay = resendDelay(t.failures++, afterMs, this.random);
      t.retryAt = this.clock.mono() + delay; // a state that is set meanwhile waits too: the latest is sent when the wait is over
      t.waitTimer?.();
      t.waitTimer = this.after(delay, () => this.pumpTopic(t));
    };
    if (e instanceof ProtocolViolation) {
      // 16 [SEC-3], 14 [CLI-50]: what the server sent breaks 02 [ARC-61]: nothing of it is applied, and the keyspace is held as for any such message
      t.blocked = "PROTOCOL_ERROR";
      this.onProtocolViolation(e, [t.keyspaceId]);
      return;
    }
    if (!(e instanceof RpcError)) {
      retry(); // not a status: a transport that failed in its own way is as Unavailable (A.7)
      return;
    }
    const reject = e.reject;
    switch (e.status) {
      case "Unauthenticated":
        if (this.noteRevoked(e)) {
          t.blocked = "SUBJECT_REVOKED";
          return;
        }
        // A.7: refresh the token, then resend once; a second Unauthenticated stops and reports
        if (t.authTries++ >= 1) {
          t.blocked = "UNAUTHENTICATED";
          t.stale = false;
          this.emitPresence({ ...base, code: "UNAUTHENTICATED", status: e.status });
          return;
        }
        this.detach(
          (async () => {
            await this.refreshToken();
            await this.run(() => this.pumpTopic(t));
          })(),
        );
        return;
      case "PermissionDenied":
        if (reject?.code === Code.APP_SUSPENDED) return; // the session's own hold covers it: the topic waits for the next session
        t.blocked = codeName(reject?.code ?? Code.PERMISSION_DENIED);
        t.stale = false;
        this.emitPresence({ ...base, code: t.blocked, status: e.status });
        return;
      case "NotFound":
        if (reject === undefined) {
          // 09 [READ-4]: "The session does not exist, has ended, or belongs to another subject": the session is dead; the engine subscribes again
          this.abortSession();
          return;
        }
        t.blocked = codeName(reject.code);
        t.stale = false;
        this.emitPresence({ ...base, code: t.blocked, status: e.status });
        return;
      case "FailedPrecondition":
        if (reject === undefined) {
          // 09 [READ-73], [READ-74]: the session does not follow the keyspace, is not a member of the topic, or is an earlier session of its client instance: the
          // topic is joined again, with the backoff of the client
          t.joined = undefined;
          retry();
          return;
        }
        t.blocked = codeName(reject.code);
        t.stale = false;
        this.emitPresence({ ...base, code: t.blocked, status: e.status });
        return;
      case "ResourceExhausted":
        if (e.retry?.reason === RetryReason.RATE_LIMITED) {
          retry(e.retry.afterMs); // 09 [READ-79]: over the rate: the latest state is sent again after the larger of `after_ms` and the backoff
          return;
        }
        // `QUOTA_EXCEEDED` (a join over the member cap, 09 [READ-79]), or no detail: stop and report, never resend unchanged in a loop (A.7)
        t.blocked = reject !== undefined ? codeName(reject.code) : "RESOURCE_EXHAUSTED";
        t.stale = false;
        this.emitPresence({ ...base, code: t.blocked, status: e.status, ...(reject?.details["quota"] !== undefined ? { detail: reject.details["quota"] } : {}) });
        return;
      case "InvalidArgument":
      case "AlreadyExists":
      case "OutOfRange":
        // `INVALID_KEY`, `SCHEMA_INVALID`, `PAYLOAD_TOO_LARGE`, `INVALID_ENVELOPE`: a state or a topic that no resend changes
        if (op === "publish") {
          t.stale = false; // the state is not sent again; a new state is
        } else t.blocked = reject !== undefined ? codeName(reject.code) : "INVALID_ARGUMENT";
        this.emitPresence({ ...base, code: reject !== undefined ? codeName(reject.code) : "INVALID_ARGUMENT", status: e.status });
        return;
      case "Unimplemented":
        t.blocked = "UNIMPLEMENTED";
        t.stale = false;
        this.emitPresence({ ...base, code: "UNIMPLEMENTED", status: e.status });
        return;
      default:
        // Unavailable, DeadlineExceeded, Aborted, Unknown, Internal, Cancelled, DataLoss: outcome unknown, or transient: resend with the client backoff (A.7, 14 [CLI-43])
        retry(e.retry?.afterMs);
    }
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // Frames (09 [READ-81])

  /** A `PresenceFrame` of the stream. It changes no cursor, no watermark and no lane watermark (09 [READ-81]). */
  protected applyPresenceFrame(frame: PresenceFrame): void {
    const t = this.topics.get(keyOf(frame.keyspaceId, frame.topic));
    // 09 [READ-80]: only the members of the topic in the keyspace get its frames; a frame for a topic that this engine is not in (it left, a late frame) is none of its
    // (the snapshot of a join can reach the stream before the answer of the call reaches this task: a topic whose join is under way takes it)
    if (t === undefined || t.closed || (t.joined === undefined && t.op !== "join")) return;
    const now = this.serverNow();
    let changed = false;
    if (frame.snapshot) {
      const next = new View();
      for (const e of frame.entries) {
        const raw = this.entryOf(e, t, now);
        if (raw !== undefined && this.admits(next, raw)) next.set(raw.clientId, raw);
      }
      // 09 [READ-81]: "A client replaces its view of a topic on each snapshot"
      changed = next.size !== t.view.size || [...next].some(([id, e]) => !sameEntry(t.view.get(id), e));
      t.view = next;
    } else {
      for (const e of frame.entries) {
        const raw = this.entryOf(e, t, now);
        if (raw === undefined) continue;
        const have = t.view.get(raw.clientId);
        // 09 [READ-81]: "a client MUST ignore an upsert whose `expires_at` is older than that of the entry it holds for the same client instance"
        if (have !== undefined && raw.expiresAtMs < have.expiresAtMs) continue;
        if (!this.admits(t.view, raw)) continue; // 16 [SEC-1]: the member cap and the byte total of the view
        if (!sameEntry(have, raw)) changed = true;
        t.view.set(raw.clientId, raw);
      }
      for (const id of frame.leftClientIds) if (t.view.delete(id)) changed = true;
    }
    // 09 [READ-81]: "drops entries whose `expires_at` has passed even if no `left_client_ids` arrived"
    for (const [id, e] of [...t.view]) {
      if (e.expiresAtMs <= now) {
        t.view.delete(id);
        changed = true;
      }
    }
    this.armExpiry();
    if (changed || frame.snapshot) this.emitPresence({ type: "changed", keyspaceId: t.keyspaceId, topic: t.topic });
  }

  /** The most entries that the view of `t` holds: the member cap that the server advertised (09 [READ-79]), and the client's own (`maxViewEntries`). */
  private viewCap(): number {
    return Math.min(this.maxViewEntries, this.limits.maxPresenceMembersPerTopic);
  }

  /** Whether the view can take the entry: it is one it holds already, or there is room for another member, and the octets stay within `MAX_VIEW_BYTES` (16 [SEC-1]). */
  private admits(view: View, raw: PresenceEntryRaw): boolean {
    const have = view.get(raw.clientId);
    if (have === undefined && view.size >= this.viewCap()) return false;
    return view.bytes - view.sizeOf(raw.clientId) + entrySize(raw) <= MAX_VIEW_BYTES;
  }

  private entryOf(e: PresenceEntry, t: Topic, now: number): PresenceEntryRaw | undefined {
    // 09 [READ-75]: an entry has the client instance that published it; one with no `expires_at` cannot be judged for its expiry, and is not kept
    if (e.clientId === "" || e.expiresAt === undefined) return undefined;
    // 16 [SEC-1]: what a server sends is as bounded as what it promised: a client id is a UUID, a state is at most `max_presence_state_bytes` (09 [READ-74]: the server refuses a larger one with PAYLOAD_TOO_LARGE)
    if (e.clientId.length > MAX_CLIENT_ID_LENGTH || e.state.length > this.limits.maxPresenceStateBytes) {
      if (!t.droppedReported) {
        t.droppedReported = true;
        this.emit({
          type: "protocol-error",
          what: "PresenceFrame",
          detail: e.state.length > this.limits.maxPresenceStateBytes ? `an entry of ${e.state.length} octets, over max_presence_state_bytes (${this.limits.maxPresenceStateBytes}, 09 [READ-74]); it is not kept` : `a client id of ${e.clientId.length} characters (03 [ID-35]); the entry is not kept`,
          keyspaces: [t.keyspaceId],
        });
      }
      return undefined;
    }
    // V23 MUST-2, 16 [SEC-1]: nor is a principal that no token could carry: a `sub` of at most 255 bytes (10 [AUTH-8]), an actor chain of at most 16 entries of at most 255 octets ([AUTH-13]), a device id of at most 255 ([AUTH-26])
    const bad = principalProblem(e.principal);
    if (bad !== undefined) {
      if (!t.droppedReported) {
        t.droppedReported = true;
        this.emit({ type: "protocol-error", what: "PresenceFrame", detail: `an entry with ${bad}; it is not kept`, keyspaces: [t.keyspaceId] });
      }
      return undefined;
    }
    // 09 [READ-76]: "An entry expires `limits.presence_ttl_seconds` after its last publish": an `expires_at` further away than that is not believed
    const horizon = now + this.limits.presenceTtlMs + EXPIRY_SLACK_MS;
    return { clientId: e.clientId, principal: e.principal, state: e.state, expiresAtMs: Math.min(timestampMs(e.expiresAt), horizon) };
  }

  /** The timer that drops the entries as they expire (09 [READ-81]): one for the engine, armed for the earliest expiry. */
  private armExpiry(): void {
    this.expiryTimer?.();
    this.expiryTimer = undefined;
    let min: number | undefined;
    for (const t of this.topics.values()) for (const e of t.view.values()) if (min === undefined || e.expiresAtMs < min) min = e.expiresAtMs;
    if (min === undefined) return;
    this.expiryTimer = this.after(Math.max(1, min - this.serverNow() + 1), () => {
      this.expiryTimer = undefined;
      const now = this.serverNow();
      for (const t of this.topics.values()) {
        let changed = false;
        for (const [id, e] of [...t.view]) {
          if (e.expiresAtMs <= now) {
            t.view.delete(id);
            changed = true;
          }
        }
        if (changed) this.emitPresence({ type: "changed", keyspaceId: t.keyspaceId, topic: t.topic });
      }
      this.armExpiry();
    });
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // What the session tells the topics (called from session.ts)

  /** A new session is live (09 [READ-57], [READ-73]): the memberships and the entries of the earlier one are gone. */
  protected presenceSessionStarted(): void {
    for (const t of this.topics.values()) {
      t.joined = undefined;
      t.op = undefined;
      t.published = undefined;
      t.answeredMono = undefined;
      t.failures = 0;
      t.authTries = 0;
      t.retryAt = 0;
      t.blocked = undefined;
      t.stale = true;
      t.droppedReported = false;
      this.closeTimers(t);
    }
    // the clients of this engine hold the view of an earlier session until a snapshot replaces it
    this.presenceClientId = this.clientId;
    this.presenceKick();
  }

  /** The session ended (a drop of the stream, `Closing`): the server removed the memberships and the entries with it (09 [READ-77]). */
  protected presenceSessionEnded(): void {
    for (const t of this.topics.values()) {
      t.joined = undefined;
      t.op = undefined;
      t.published = undefined;
      this.closeTimers(t);
    }
  }

  /** A keyspace was added to the session, or a load that waited ended: the topics of the keyspace that wait for it join (09 [READ-73]). */
  protected presenceKick(keyspaceId?: string): void {
    for (const t of [...this.topics.values()]) if (keyspaceId === undefined || t.keyspaceId === keyspaceId) this.pumpTopic(t);
  }

  /** The keyspace was dropped on the session, or removed from it: its memberships and entries ended (09 [READ-69], [READ-77]); the view is not served any more. */
  protected presenceKeyspaceDropped(keyspaceId: string): void {
    for (const t of this.topics.values()) {
      if (t.keyspaceId !== keyspaceId) continue;
      t.joined = undefined;
      t.op = undefined;
      t.published = undefined;
      // (`answeredMono` stays: the interval of 09 [READ-79] runs per session and client instance, and a leave and a join do not start a new one)
      this.closeTimers(t);
      if (t.view.size > 0) {
        t.view = new View();
        this.emitPresence({ type: "changed", keyspaceId, topic: t.topic });
      }
    }
    this.armExpiry();
  }

  /** The session's client instance changed and the server accepted the rebind (14 [CLI-78], 09 [READ-77]): the entries of the earlier instance ended; publish again at once. */
  protected presenceRebound(): void {
    const earlier = this.presenceClientId;
    this.presenceClientId = this.clientId;
    for (const t of this.topics.values()) {
      if (earlier !== "" && earlier !== this.clientId && t.view.delete(earlier)) this.emitPresence({ type: "changed", keyspaceId: t.keyspaceId, topic: t.topic });
      // the interval of 09 [READ-79] counts per client instance: the new one has published nothing
      t.answeredMono = undefined;
      t.published = undefined;
      t.stale = true;
      t.failures = 0;
      t.retryAt = 0;
      this.pumpTopic(t);
    }
  }

  /** The purge of a keyspace (14 [CLI-66], [CLI-77]): its presence state goes, the registrations of its tabs with it. */
  protected presencePurge(keyspaceId: string, cause: string): void {
    for (const t of [...this.topics.values()]) {
      if (t.keyspaceId !== keyspaceId) continue;
      t.closed = true;
      this.closeTimers(t);
      this.topics.delete(keyOf(t.keyspaceId, t.topic));
      this.emitPresence({ type: "changed", keyspaceId, topic: t.topic });
      this.emitPresence({ type: "ended", keyspaceId, topic: t.topic, cause });
    }
    this.armExpiry();
  }

  protected onPresenceLimitsChanged(): void {
    // the TTL changed: the keepalive is armed again at the next publish; a shorter interval lets a waiting publish go
    for (const t of this.topics.values()) if (t.waitTimer !== undefined) this.pumpTopic(t);
  }
}

function sameEntry(a: PresenceEntryRaw | undefined, b: PresenceEntryRaw): boolean {
  if (a === undefined || a.expiresAtMs !== b.expiresAtMs || a.state.length !== b.state.length) return false;
  for (let i = 0; i < a.state.length; i++) if (a.state[i] !== b.state[i]) return false;
  return a.principal?.sub === b.principal?.sub;
}
