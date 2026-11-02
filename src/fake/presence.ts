// The presence of the fake server (09 §9.8), as `server/internal/session/presence*.go` answers it on the points that a client's behaviour depends on:
//
//  - [READ-73] a join makes the session a member of the topic and sends `PresenceFrame{snapshot: true}` before any other frame of it (the snapshot holds every current
//    entry, the member's own included); a repeated join sends it again;
//  - [READ-74] a publish of a member replaces the entry of its client instance, answers with `expires_at`, and sends the entry to the other members, not to the publisher; a
//    publish from a session that is not a member, or from an earlier session of its client instance, is FailedPrecondition with no detail; no `client_id` is
//    `INVALID_ENVELOPE`; a state over `max_presence_state_bytes` is `PAYLOAD_TOO_LARGE`, before the schema (`validate`, `SCHEMA_INVALID`);
//  - [READ-76] an entry expires `presence_ttl_seconds` after its last publish, and the members are told (`left_client_ids`) when it does;
//  - [READ-77] an entry and a membership end with a leave, the session, a keyspace dropped on the session; a rebind ends the entries of the earlier instance, not the
//    memberships;
//  - [READ-79] a join over `max_presence_members_per_topic` is `QUOTA_EXCEEDED`; a publish sooner than `presence_min_publish_interval_ms` after the member's last applied one
//    is refused with `Retry{RATE_LIMITED}` (default), or coalesced (`mode: "coalesce"`): the latest state is applied when the interval has passed, and counts as the entry's last
//    publish for its expiry.

import { create, type MessageInitShape } from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import { PresenceEntrySchema, PresenceFrameSchema, SubscribeResponseSchema, type SubscribeResponse } from "../gen/abpc/v2/sync_pb.js";
import { Code, ErrorClass, RejectSchema, RetryReason, RetrySchema, type AppLimits, type Principal } from "../gen/abpc/v2/types_pb.js";
import type { Clock } from "../engine/clock.js";
import { DEFAULT_CLASS } from "../engine/codes.js";
import { RpcError } from "../engine/errors.js";

/** What the presence needs of a session of the fake. */
export interface PresenceSession {
  id: string;
  principal: Principal;
  clientId: string;
  push(frame: SubscribeResponse): void;
}

interface Entry {
  clientId: string;
  principal: Principal;
  state: Uint8Array;
  expiresMs: number;
  owner: PresenceSession;
  cancelExpiry: () => void;
}

interface Topic {
  keyspaceId: string;
  topic: string;
  awareness: boolean;
  members: Set<PresenceSession>;
  entries: Map<string, Entry>;
}

interface Applied {
  atMs: number;
  /** A coalesced state that waits for the interval (`mode: "coalesce"`). */
  pending: Uint8Array | undefined;
  cancel: (() => void) | undefined;
}

const key = (ks: string, topic: string): string => `${ks}\u0000${topic}`;

export interface FakePresenceOptions {
  clock: Clock;
  limits: () => AppLimits;
  /** How a publish over the rate is answered: refused with `Retry{RATE_LIMITED}` (default) or coalesced (09 [READ-79]). */
  mode?: "refuse" | "coalesce";
  /** The presence schema of the kind, judged after the size: a state that it refuses is `SCHEMA_INVALID` (09 [READ-74]). */
  validate?: (state: Uint8Array, awareness: boolean) => boolean;
}

function reject(code: Code, details: Record<string, string> = {}): ReturnType<typeof create<typeof RejectSchema>> {
  return create(RejectSchema, { code, errorClass: DEFAULT_CLASS.get(code) ?? ErrorClass.DEAD_LETTER, details });
}

export class FakePresence {
  private readonly topics = new Map<string, Topic>();
  /** The latest session that carries each client instance (09 [READ-74]). */
  private readonly carriers = new Map<string, PresenceSession>();
  private readonly applied = new Map<string, Applied>();
  mode: "refuse" | "coalesce";
  validate: ((state: Uint8Array, awareness: boolean) => boolean) | undefined;
  /** Every presence frame that was sent, for a test: (session id, frame). */
  readonly sent: { session: string; frame: ReturnType<typeof create<typeof PresenceFrameSchema>> }[] = [];

  constructor(private readonly o: FakePresenceOptions) {
    this.mode = o.mode ?? "refuse";
    this.validate = o.validate;
  }

  private ttlMs(): number {
    return (Number(this.o.limits().presenceTtlSeconds) || 30) * 1000;
  }
  private intervalMs(): number {
    return Number(this.o.limits().presenceMinPublishIntervalMs) || 1000;
  }

  private send(s: PresenceSession, f: MessageInitShape<typeof PresenceFrameSchema>): void {
    const frame = create(PresenceFrameSchema, f);
    this.sent.push({ session: s.id, frame });
    s.push(create(SubscribeResponseSchema, { frame: { case: "presence", value: frame } }));
  }

  private entryMessage(e: Entry): ReturnType<typeof create<typeof PresenceEntrySchema>> {
    return create(PresenceEntrySchema, { principal: e.principal, clientId: e.clientId, state: e.state, expiresAt: timestampFromMs(e.expiresMs) });
  }

  /** The session binds a client instance: the later session of an instance carries its presence (09 [READ-74]). */
  carry(s: PresenceSession): void {
    if (s.clientId !== "") this.carriers.set(s.clientId, s);
  }

  join(s: PresenceSession, keyspaceId: string, topic: string, awareness: boolean): void {
    const k = key(keyspaceId, topic);
    let t = this.topics.get(k);
    if (t === undefined) {
      t = { keyspaceId, topic, awareness, members: new Set(), entries: new Map() };
      this.topics.set(k, t);
    }
    if (!t.members.has(s) && t.members.size >= (Number(this.o.limits().maxPresenceMembersPerTopic) || 100)) {
      throw new RpcError("ResourceExhausted", "the topic has as many members as the limit allows", { reject: reject(Code.QUOTA_EXCEEDED, { quota: "max_presence_members_per_topic" }) });
    }
    t.members.add(s);
    const now = this.o.clock.now();
    this.send(s, { keyspaceId, topic, snapshot: true, entries: [...t.entries.values()].filter((e) => e.expiresMs > now).map((e) => this.entryMessage(e)) });
  }

  isMember(s: PresenceSession, keyspaceId: string, topic: string): boolean {
    return this.topics.get(key(keyspaceId, topic))?.members.has(s) === true;
  }

  /** The checks and the effect of a publish; returns `expires_at` in ms. The caller has judged the grant. */
  publish(s: PresenceSession, keyspaceId: string, topic: string, state: Uint8Array): number {
    const t = this.topics.get(key(keyspaceId, topic));
    if (t === undefined || !t.members.has(s)) throw new RpcError("FailedPrecondition", "the session is not a member of the topic");
    if (s.clientId === "") throw new RpcError("InvalidArgument", "the session has no client_id", { reject: reject(Code.INVALID_ENVELOPE) });
    const carrier = this.carriers.get(s.clientId);
    if (carrier !== undefined && carrier !== s) throw new RpcError("FailedPrecondition", "a later session of the client instance publishes its presence");
    if (state.length > (Number(this.o.limits().maxPresenceStateBytes) || 4096)) throw new RpcError("InvalidArgument", "the state is over the limit", { reject: reject(Code.PAYLOAD_TOO_LARGE) });
    if (this.validate !== undefined && !this.validate(state, t.awareness)) throw new RpcError("InvalidArgument", "the state breaks the presence schema", { reject: reject(Code.SCHEMA_INVALID) });
    const now = this.o.clock.now();
    const ak = `${s.id}\u0000${key(keyspaceId, topic)}\u0000${s.clientId}`;
    const last = this.applied.get(ak);
    if (last !== undefined && now < last.atMs + this.intervalMs()) {
      const after = last.atMs + this.intervalMs() - now;
      if (this.mode === "refuse") {
        throw new RpcError("ResourceExhausted", "a publish sooner than the interval of the topic", { retry: create(RetrySchema, { reason: RetryReason.RATE_LIMITED, afterMs: after }) });
      }
      // coalesced: only the latest state is delivered, when the interval that runs from the last applied publish has passed; the entry's expiry counts the publish now
      last.pending = state;
      last.cancel?.();
      last.cancel = this.o.clock.setTimer(after, () => {
        const pend = last.pending;
        last.pending = undefined;
        last.cancel = undefined;
        if (pend !== undefined && this.topics.get(key(keyspaceId, topic))?.members.has(s) === true) this.apply(s, t, pend, false);
      });
      const e = t.entries.get(s.clientId);
      if (e !== undefined) this.touch(e, t);
      return (e?.expiresMs ?? now + this.ttlMs());
    }
    return this.apply(s, t, state, true);
  }

  private touch(e: Entry, t: Topic): void {
    e.cancelExpiry();
    e.expiresMs = this.o.clock.now() + this.ttlMs();
    e.cancelExpiry = this.o.clock.setTimer(this.ttlMs(), () => this.expire(e, t));
  }

  private apply(s: PresenceSession, t: Topic, state: Uint8Array, count: boolean): number {
    const now = this.o.clock.now();
    let e = t.entries.get(s.clientId);
    if (e === undefined) {
      e = { clientId: s.clientId, principal: s.principal, state, expiresMs: now + this.ttlMs(), owner: s, cancelExpiry: () => undefined };
      t.entries.set(s.clientId, e);
    }
    e.owner = s;
    e.state = state;
    e.principal = s.principal;
    this.touch(e, t);
    if (count) this.applied.set(`${s.id}\u0000${key(t.keyspaceId, t.topic)}\u0000${s.clientId}`, { atMs: now, pending: undefined, cancel: undefined });
    for (const m of t.members) if (m !== s) this.send(m, { keyspaceId: t.keyspaceId, topic: t.topic, entries: [this.entryMessage(e)] });
    return e.expiresMs;
  }

  private expire(e: Entry, t: Topic): void {
    if (t.entries.get(e.clientId) !== e) return;
    this.remove(e, t);
  }

  private remove(e: Entry, t: Topic): void {
    e.cancelExpiry();
    t.entries.delete(e.clientId);
    for (const m of t.members) if (m !== e.owner) this.send(m, { keyspaceId: t.keyspaceId, topic: t.topic, leftClientIds: [e.clientId] });
    if (t.members.size === 0 && t.entries.size === 0) this.topics.delete(key(t.keyspaceId, t.topic));
  }

  /** `LeavePresence`: the entry and the membership end; a topic that the session is not a member of changes nothing (09 [READ-77]). */
  leave(s: PresenceSession, keyspaceId: string, topic: string): void {
    const t = this.topics.get(key(keyspaceId, topic));
    if (t === undefined || !t.members.has(s)) return;
    this.leaveTopic(s, t);
  }

  private leaveTopic(s: PresenceSession, t: Topic): void {
    for (const e of [...t.entries.values()]) if (e.owner === s) this.remove(e, t);
    t.members.delete(s);
    if (t.members.size === 0 && t.entries.size === 0) this.topics.delete(key(t.keyspaceId, t.topic));
  }

  /** A keyspace is removed from the session or dropped on it: its memberships and entries end (09 [READ-63], [READ-69]). */
  endKeyspace(s: PresenceSession, keyspaceId: string): void {
    for (const t of [...this.topics.values()]) if (t.keyspaceId === keyspaceId && t.members.has(s)) this.leaveTopic(s, t);
  }

  /** The session ended: every membership and entry of it ends. */
  endSession(s: PresenceSession): void {
    for (const t of [...this.topics.values()]) if (t.members.has(s)) this.leaveTopic(s, t);
    for (const k of [...this.applied.keys()]) if (k.startsWith(`${s.id}\u0000`)) this.applied.delete(k);
    if (s.clientId !== "" && this.carriers.get(s.clientId) === s) this.carriers.delete(s.clientId);
  }

  /** The session's client instance changed (09 [READ-99]): the entries of the earlier instance end, the memberships stay (09 [READ-77]). */
  rebind(s: PresenceSession, earlier: string): void {
    if (earlier !== "" && earlier !== s.clientId) {
      for (const t of this.topics.values()) {
        const e = t.entries.get(earlier);
        if (e !== undefined && e.owner === s) this.remove(e, t);
      }
      if (this.carriers.get(earlier) === s) this.carriers.delete(earlier);
    }
    this.carry(s);
  }

  /** The entries of a topic that have not expired, for a test. */
  entries(keyspaceId: string, topic: string): { clientId: string; state: Uint8Array; expiresMs: number }[] {
    const t = this.topics.get(key(keyspaceId, topic));
    const now = this.o.clock.now();
    return t === undefined ? [] : [...t.entries.values()].filter((e) => e.expiresMs > now).map((e) => ({ clientId: e.clientId, state: e.state, expiresMs: e.expiresMs }));
  }

  /** The members of a topic (session ids), for a test. */
  members(keyspaceId: string, topic: string): string[] {
    return [...(this.topics.get(key(keyspaceId, topic))?.members ?? [])].map((s) => s.id);
  }
}
