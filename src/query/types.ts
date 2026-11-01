// The public types of the local query layer (C4): what a query asks, what a row is, what a result says of the copy it was read from, and what a
// live query tells its listener. Rules cited in the code are those of 14 §14.1 (engine model), §14.2 (prediction), [CLI-14] (moved entities),
// §14.10 (interest), 05 §5.3 (absent and null), §5.6 (entities), [STATE-82] (redacted state) and 09 [READ-9] (tombstones).

import type * as Y from "yjs";

/** Which state a query reads: `predicted` is the view of 14 [CLI-6] (confirmed state with the pending actions on top, the default); `confirmed` is the confirmed state alone. */
export type QueryMode = "predicted" | "confirmed";

/**
 * A decoded json/v1 value (05 [STATE-7]). Every integer of it, within ±(2^53−1), is a `bigint`, so that nothing passes 2^53 unnoticed; any other
 * number is a `number`. An object is a frozen object with no prototype, so that a member named `__proto__` is a member; arrays are frozen.
 */
export type QueryJson = null | boolean | bigint | number | string | readonly QueryJson[] | { readonly [member: string]: QueryJson };

/** A path into a json/v1 value: `a.b[0].c` (a leading `$` is allowed), or its segments (a number is an array index). */
export type JsonPath = string | readonly (string | number)[];

/**
 * A filter on the decoded json/v1 values of the rows. A leaf compares the value at `path` (the whole value when it is left out) with `value`:
 * equality (`eq`, `ne`, `in`, deep for arrays and objects) and order (`lt`, `lte`, `gt`, `gte`, for numbers, for strings by UTF-8 byte order and
 * for booleans); `exists` asks whether the path is there. A number in a filter value that is an integer within ±(2^53−1) counts as that integer.
 * A row whose value is no json/v1 value (null, redacted, bytes, a CRDT document, undecodable) matches no leaf. `at`, in an entity query only, names a
 * key below the entity (relative to its marker) whose value the leaf reads instead of the marker's.
 */
export type Filter =
  | { path?: JsonPath; at?: string; op: "eq" | "ne" | "lt" | "lte" | "gt" | "gte"; value: QueryJson }
  | { path?: JsonPath; at?: string; op: "in"; values: readonly QueryJson[] }
  | { path?: JsonPath; at?: string; op: "exists"; value?: boolean }
  | { and: readonly Filter[] }
  | { or: readonly Filter[] }
  | { not: Filter };

/** A weak reference to a key of a keyspace (11 §11.4.8). */
export interface EntityRef {
  keyspaceId: string;
  key: string;
}

/** A value that is json/v1, decoded. */
export interface JsonRowValue {
  readonly kind: "json";
  readonly value: QueryJson;
}

/** A `counter` key (05 [STATE-26]): its integer. */
export interface CounterRowValue {
  readonly kind: "counter";
  readonly value: bigint;
}

/** A `bytes/v1` value. */
export interface BytesRowValue {
  readonly kind: "bytes";
  readonly value: Uint8Array;
}

/**
 * A CRDT key (14 [CLI-13]): its value is a Yjs document, never bytes. `doc` is the view document that the user edits (the confirmed document in a
 * `confirmed` query), opened when it is read for the first time; it is undefined where the engine has none to give, as in a follower tab, which holds
 * no view documents (14 [CLI-61]), and in a copy that was purged.
 */
export interface CrdtRowValue {
  readonly kind: "crdt";
  readonly doc: Y.Doc | undefined;
}

/** An explicit null (05 [STATE-14]): the key is present and holds no value. */
export interface NullRowValue {
  readonly kind: "null";
}

/** A key in the redacted state (05 [STATE-82]): present, and holding no value that a reader may show. */
export interface RedactedRowValue {
  readonly kind: "redacted";
}

/** A value that the client cannot decode (14 [CLI-5]): an encoding it does not know, or json/v1 text that does not parse. */
export interface UndecodableRowValue {
  readonly kind: "undecodable";
  readonly encoding: string;
  readonly bytes: Uint8Array;
}

/**
 * An entity that was moved or is being moved, shown in place of the entity (14 [CLI-14], 11 [COORD-73], [COORD-76]): `moving` ("this item is being
 * moved to X") and `moved` ("this item was moved to X") hide the entity and its subtree; `purged` shows the pointer only while the entity's marker is
 * absent, and an entity created again under that marker is shown as it is. An entity whose moved key says `importing` in the target is staged, and
 * shown as not yet present (no row at all).
 */
export interface MovedRowValue {
  readonly kind: "moved";
  readonly status: "moving" | "moved" | "purged";
  /** The `to` pointer of the moved key: the entity in the other keyspace; undefined when the moved key holds none a reader can read. */
  readonly to: EntityRef | undefined;
  readonly sagaId: string | undefined;
  /** The run key `abpc.saga.v1/run/{saga_id}`, which a client that shows this pointer follows (14 [CLI-14]); undefined without a saga id. */
  readonly runKey: string | undefined;
  /** What the run key says, when the copy holds it (it is in the interest): `failed` is the move to show as failed, with both sides kept as they are, instead of the pointer. */
  readonly run: { status: string; cause: string | undefined } | undefined;
  readonly failed: boolean;
}

export type RowValue =
  | JsonRowValue
  | CounterRowValue
  | BytesRowValue
  | CrdtRowValue
  | NullRowValue
  | RedactedRowValue
  | UndecodableRowValue
  | MovedRowValue;

/** One key of a result, or one entity. */
export interface Row {
  readonly keyspaceId: string;
  readonly key: string;
  /** The variables of the template that the key matched (a template or entity query); undefined for a key or prefix query. */
  readonly bindings: Readonly<Record<string, string>> | undefined;
  readonly value: RowValue;
  /** The key's version in the confirmed state; 0n for a key that no confirmed state holds (a predicted one). */
  readonly version: bigint;
  /** The pending actions that touch the key, in outbox order (14 [CLI-6]); empty for a key none touches. */
  readonly pending: readonly string[];
  readonly isPending: boolean;
  /** The value is a prediction of a pending action (14 §14.2). Always false in a `confirmed` query. */
  readonly predicted: boolean;
  /** For an entity query with `subtree`: every present key below the entity, in key order. */
  readonly subtree?: readonly Row[];
}

/** What a query reads. */
export type Select =
  /** These keys. Those outside the interest are reported as `notLoaded`, never as absent (14 [CLI-71], 09 [READ-26]). */
  | { keys: readonly string[] }
  /** The keys below a key prefix (03 [ID-14]): `""` (every key) or a key path followed by `/`; the prefix's own key is not among them. */
  | { prefix: string }
  /** The keys that match a key template (03 [ID-17]), `issue/{i}/title`; `bind` fixes some of its variables. Each row carries the bindings. */
  | { template: string; bind?: Readonly<Record<string, string>> }
  /** The present markers of an entity template (05 §5.6), `issue/{i}`, and with `subtree` every key below each. Moved and staged entities as 14 [CLI-14] says. */
  | { entities: string; bind?: Readonly<Record<string, string>>; subtree?: boolean };

export interface Query {
  keyspaceId: string;
  select: Select;
  /** Default `predicted`. */
  mode?: QueryMode;
  filter?: Filter;
  /** By key, in the byte order of its UTF-8 (02 [ARC-62]); default `asc`. For an entity query, by the marker key. */
  order?: "asc" | "desc";
  /** At most this many rows (a page). */
  limit?: number;
  /** Rows after this key in the order: the `next` of the page before. */
  after?: string;
}

/** How much of what a query asks lies in the interest of the keyspace (14 [CLI-71]). */
export type Coverage = "full" | "partial" | "none";

/** `ready`: the copy holds confirmed state; `loading`: the keyspace is followed and not loaded yet; `purged`: the copy was purged (14 [CLI-66]); `unfollowed`: the engine does not follow it. */
export type KeyspaceStatus = "ready" | "loading" | "purged" | "unfollowed";

/** The copy as a result saw it. */
export interface KeyspaceState {
  status: KeyspaceStatus;
  /** Why the engine no longer follows the keyspace, while its copy stays (14 [CLI-67]): `GRANT_CHANGED`, `ARCHIVED`, … */
  stopped: string | undefined;
  seq: bigint;
  logEpoch: number;
  /** The interest prefixes; empty is the whole keyspace. */
  interest: readonly string[];
}

export interface QueryResult {
  keyspaceId: string;
  mode: QueryMode;
  rows: readonly Row[];
  /** The `after` of the next page; undefined when there is none. */
  next: string | undefined;
  /** For a key query: the keys that the interest does not cover, or that no loaded copy answers for; they are not absent, they are not known (14 [CLI-71]). */
  notLoaded: readonly string[];
  /** Whether what the query asks lies in the interest. A result over a `partial` or `none` coverage says nothing of the keys outside it. */
  coverage: Coverage;
  /**
   * Whether the moved keys of what the query asks (`abpc.saga.v1/moved/…`) lie in the interest, so that a moved or staged entity is recognised
   * (14 [CLI-14]); where they do not, such an entity is shown as any other.
   */
  movesTracked: boolean;
  keyspace: KeyspaceState;
}

/** What a read of one key answers. */
export type KeyRead =
  | { status: "present"; row: Row }
  | { status: "absent" }
  | { status: "notLoaded" }
  | { status: "unavailable"; reason: Exclude<KeyspaceStatus, "ready"> };

export interface RowChange {
  before: Row;
  after: Row;
}

/** Why a live query is told something: its first answer; a change of the keyspace; a full evaluation again (the state was replaced, the interest changed); a purge (14 [CLI-66]). */
export type LiveCause = "initial" | "change" | "refresh" | "purged";

export interface LiveUpdate {
  cause: LiveCause;
  added: readonly Row[];
  changed: readonly RowChange[];
  removed: readonly Row[];
  /** The result after the update; built when it is read. */
  readonly result: QueryResult;
  /** The keyspace's status, its interest coverage or its not-loaded keys differ from what the last update said. */
  statusChanged: boolean;
}

export type LiveListener = (update: LiveUpdate) => void;

/** What a live query cost so far (a test reads it): evaluations of the whole query, of single keys, and updates delivered. */
export interface LiveStats {
  fullRuns: number;
  keyEvals: number;
  updates: number;
}

export interface LiveQuery {
  readonly query: Query;
  /** The result now. */
  snapshot(): QueryResult;
  readonly stats: LiveStats;
  readonly closed: boolean;
  /** Stops the updates and drops what the query holds. */
  close(): void;
}
