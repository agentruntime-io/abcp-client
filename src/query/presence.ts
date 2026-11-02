// The view of a presence topic (09 §9.8, 14 [CLI-61]): the entries that the other members publish, decoded, with the changes that reach the app as they come, and the calls that this
// tab makes into the topic.
//
// What it reads. The engine applies the presence frames of its session to the view of each topic that it is in (`engine/presence.ts`: a snapshot replaces the view, an upsert replaces
// an entry, `left_client_ids` remove entries, an entry whose `expires_at` has passed is dropped, 09 [READ-81]); this layer reads that view through `presenceEntries` and the
// engine's presence events, and decodes each state with the strict json/v1 reader. A state is untrusted input (16 [SEC-1], [SEC-28]): one that does not read as json/v1 is shown as
// `undecodable`, with no `state`, and never stops the view.
//
// Yjs awareness (09 [READ-83]): the state of an entry of an awareness topic is a JSON object that maps the id of each tab of the member's engine to that tab's state; `tabs` is that
// map, so that several tabs on one document each show their own cursor.
//
// What it does not do: a presence frame is no part of the log (09 [READ-81]), so a view never changes a cursor, a query result or a key; it asks nothing of the replica.

import type { Principal } from "../gen/abpc/v2/types_pb.js";
import type { JoinOptions, PresenceEntryRaw, PresenceEvent } from "../engine/presence.js";
import { decodeJson } from "./json.js";
import type { QueryJson } from "./types.js";

/** What the view needs of the engine; `Engine` is one. */
export interface PresenceSource {
  joinPresence(keyspaceId: string, topic: string, opts?: JoinOptions): Promise<void>;
  publishPresence(keyspaceId: string, topic: string, state: unknown): Promise<void>;
  leavePresence(keyspaceId: string, topic: string): Promise<void>;
  presenceEntries(keyspaceId: string, topic: string): PresenceEntryRaw[];
  presenceAwareness(keyspaceId: string, topic: string): boolean;
  onPresence(listener: (event: PresenceEvent) => void): () => void;
  /** The `client_id` of the engine's session: the entry that carries it is this engine's own. Empty in a follower tab, which has none (03 [ID-35]). */
  readonly clientId: string;
}

/** One entry of a topic. */
export interface PresenceEntry {
  /** The client instance that published it (09 [READ-75]): one entry for each engine in a topic. */
  readonly clientId: string;
  /** The principal, as the server verified it (09 [READ-75]). */
  readonly principal: Principal | undefined;
  /** The decoded state; undefined when the bytes do not read as json/v1 (see `undecodable`). */
  readonly state: QueryJson | undefined;
  /** The state is not valid json/v1 (16 [SEC-1]). */
  readonly undecodable: boolean;
  /** The state as the server sent it. */
  readonly raw: Uint8Array;
  /** Awareness topics: the state of each tab of the member's engine, by tab id (09 [READ-83]). */
  readonly tabs: Readonly<Record<string, QueryJson>> | undefined;
  /** When the entry expires, in server time, ms since the epoch (09 [READ-76]). */
  readonly expiresAt: number;
  /** The entry is this engine's own (it was sent back in a snapshot). */
  readonly self: boolean;
}

export interface PresenceUpdate {
  /** `initial`: the first call of a `watch`; `changed`: a snapshot, an upsert, a `left_client_ids`, an expiry; `ended`: the engine's presence in the keyspace ended (a purge). */
  readonly cause: "initial" | "changed" | "ended";
  /** Every entry of the topic now. */
  readonly entries: readonly PresenceEntry[];
  /** The entries that were not in the topic before, or that changed their state. */
  readonly added: readonly PresenceEntry[];
  readonly changed: readonly PresenceEntry[];
  /** The client ids of the entries that are gone. */
  readonly removed: readonly string[];
}

export type PresenceWatcher = (update: PresenceUpdate) => void;

const sameBytes = (a: Uint8Array, b: Uint8Array): boolean => a.length === b.length && a.every((x, i) => x === b[i]);

export class PresenceView {
  private readonly watchers = new Map<PresenceWatcher, Map<string, Uint8Array>>();
  private off: (() => void) | undefined;
  private closed = false;
  private joined = false;

  constructor(
    private readonly engine: PresenceSource,
    readonly keyspaceId: string,
    readonly topic: string,
    private readonly opts: JoinOptions = {},
    private readonly onClose: (v: PresenceView) => void = () => undefined,
  ) {}

  private awareness(): boolean {
    return this.opts.awareness ?? this.engine.presenceAwareness(this.keyspaceId, this.topic);
  }

  private decode(e: PresenceEntryRaw, awareness: boolean): PresenceEntry {
    let state: QueryJson | undefined;
    let undecodable = false;
    try {
      state = decodeJson(e.state);
    } catch {
      undecodable = true;
    }
    let tabs: Record<string, QueryJson> | undefined;
    if (awareness && state !== undefined && state !== null && typeof state === "object" && !Array.isArray(state)) tabs = state as Record<string, QueryJson>;
    return { clientId: e.clientId, principal: e.principal, state, undecodable, raw: e.state, tabs, expiresAt: e.expiresAtMs, self: this.engine.clientId !== "" && e.clientId === this.engine.clientId };
  }

  /** Every entry of the topic that has not expired, in the order of the client ids. */
  entries(): PresenceEntry[] {
    const awareness = this.awareness();
    return this.engine.presenceEntries(this.keyspaceId, this.topic).map((e) => this.decode(e, awareness));
  }

  /** The entries of the other members: those that are not this engine's own. */
  others(): PresenceEntry[] {
    return this.entries().filter((e) => !e.self);
  }

  /**
   * Calls the listener at once with every entry (`cause: "initial"`), and then with each change of the topic: an entry that came, one whose state changed, one that is gone. A member that
   * publishes the same state again changes nothing here. Returns the function that stops it.
   */
  watch(listener: PresenceWatcher): () => void {
    if (this.closed) throw new Error("the presence view is closed");
    const known = new Map<string, Uint8Array>();
    this.watchers.set(listener, known);
    if (this.off === undefined) {
      this.off = this.engine.onPresence((ev) => {
        if (ev.keyspaceId !== this.keyspaceId || ev.topic !== this.topic) return;
        if (ev.type === "changed" || ev.type === "ended") this.notify(ev.type);
      });
    }
    const now = this.entries();
    for (const e of now) known.set(e.clientId, e.raw);
    call(listener, { cause: "initial", entries: now, added: now, changed: [], removed: [] });
    return () => {
      this.watchers.delete(listener);
      if (this.watchers.size === 0) {
        this.off?.();
        this.off = undefined;
      }
    };
  }

  private notify(cause: "changed" | "ended"): void {
    if (this.watchers.size === 0) return;
    const now = this.entries();
    for (const [listener, known] of [...this.watchers]) {
      const added: PresenceEntry[] = [];
      const changed: PresenceEntry[] = [];
      const seen = new Set<string>();
      for (const e of now) {
        seen.add(e.clientId);
        const before = known.get(e.clientId);
        if (before === undefined) added.push(e);
        else if (!sameBytes(before, e.raw)) changed.push(e);
      }
      const removed = [...known.keys()].filter((id) => !seen.has(id));
      if (added.length === 0 && changed.length === 0 && removed.length === 0 && cause === "changed") continue;
      known.clear();
      for (const e of now) known.set(e.clientId, e.raw);
      call(listener, { cause, entries: now, added, changed, removed });
    }
  }

  /** This tab joins the topic: the engine joins it on its session and keeps it joined (09 [READ-73]). */
  async join(): Promise<void> {
    if (this.closed) throw new Error("the presence view is closed");
    await this.engine.joinPresence(this.keyspaceId, this.topic, this.opts);
    this.joined = true;
  }

  /** Sets this tab's state in the topic (09 [READ-74]): any json/v1 value. For an awareness topic the entry of the engine maps the tab ids to their states (09 [READ-83]). */
  async publish(state: unknown): Promise<void> {
    if (this.closed) throw new Error("the presence view is closed");
    if (!this.joined) await this.join();
    await this.engine.publishPresence(this.keyspaceId, this.topic, state);
  }

  /** This tab leaves the topic; the engine leaves it when no tab is in it (09 [READ-74]). */
  async leave(): Promise<void> {
    if (!this.joined) return;
    this.joined = false;
    await this.engine.leavePresence(this.keyspaceId, this.topic);
  }

  /** Leaves the topic and stops every watcher. */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.watchers.clear();
    this.off?.();
    this.off = undefined;
    this.onClose(this);
    if (this.joined) await this.leave().catch(() => undefined);
  }
}

function call(l: PresenceWatcher, u: PresenceUpdate): void {
  try {
    l(u);
  } catch {
    // a listener that throws never stops the view
  }
}
