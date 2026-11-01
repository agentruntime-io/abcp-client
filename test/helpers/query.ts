// The harness of the query suites: the engine rig of helpers/engine.ts with a manifest that has entities, nested entities, counters, CRDT and byte keys, and the
// query layer over its engine.

import { create } from "@bufbuild/protobuf";
import { StaticManifests, type StaticManifestSpec } from "../../src/engine/index.js";
import { ChangeKind, KeyChangeSchema, type KeyChange } from "../../src/gen/abpc/v2/types_pb.js";
import { createQueries, type LiveQuery, type LiveUpdate, type Queries, type Query, type QueryResult, type Row } from "../../src/query/index.js";
import { jsonBytes } from "../../src/engine/index.js";
import { KS, rig, type Rig, type RigOptions } from "./engine.js";

export { KS };

export const QMANIFEST: StaticManifestSpec = {
  version: 1,
  templates: {
    "issue/{i}": { rule: "lww", entity: true },
    "issue/{i}/title": { rule: "lww" },
    "issue/{i}/meta": { rule: "lww" },
    "issue/{i}/comment/{c}": { rule: "lww", entity: true },
    "issue/{i}/comment/{c}/text": { rule: "lww" },
    "item/{x}": { rule: "lww" },
    "count/{n}": { rule: "counter" },
    "note/{n}": { rule: "cas" },
    "doc/{d}": { rule: "crdt", roots: { text: "text" } },
    "raw/{r}": { rule: "lww", encoding: "bytes/v1" },
    "setting/{name}": { rule: "lww" },
  },
  opTypes: {
    Set: {},
    Create: {},
    Incr: {},
    CasSet: {},
    DocEdit: {},
    Quiet: { optimistic: false },
    Approve: { lane: "GATED" },
  },
};

export interface QRig extends Rig {
  q: Queries;
}

/** The engine rig over `QMANIFEST` with a query layer on its engine. */
export async function qrig(opts: RigOptions = {}): Promise<QRig> {
  const r = await rig({ manifests: new StaticManifests(QMANIFEST), heartbeatMs: 0, ...opts });
  return Object.assign(r, { q: createQueries(r.engine) });
}

/** A server write (another principal's committed record) of json values: `put(r, { "item/a": 1 })`. */
export function put(r: Rig, values: Record<string, unknown>, ks = KS): bigint {
  return r.server.write(
    ks,
    Object.entries(values).map(([key, v]) => create(KeyChangeSchema, { key, kind: ChangeKind.SET, value: jsonBytes(v), encoding: "json/v1" })),
  );
}

export function drop(r: Rig, ...keys: string[]): bigint {
  return r.server.write(
    KS,
    keys.map((key) => create(KeyChangeSchema, { key, kind: ChangeKind.DELETE })),
  );
}

export function rawSet(key: string, value: Uint8Array, encoding: string): KeyChange {
  return create(KeyChangeSchema, { key, kind: ChangeKind.SET, value, encoding });
}

export const keysOf = (res: QueryResult): string[] => res.rows.map((x) => x.key);

/** The value of a json row, as the layer decodes it. */
export function jsonOf(row: Row | undefined): unknown {
  if (row === undefined || row.value.kind !== "json") throw new Error(`not a json row: ${row?.value.kind ?? "no row"}`);
  return row.value.value;
}

/** A live query whose updates are collected. */
export interface Watch {
  live: LiveQuery;
  updates: LiveUpdate[];
  /** The updates since the first (the initial one). */
  after(): LiveUpdate[];
  /** The keys of the updates since the first, as `+key`, `~key` and `-key`, in order. */
  log(): string[];
}

export function watch(r: QRig, query: Query): Watch {
  const updates: LiveUpdate[] = [];
  const live = r.q.watch(query, (u) => updates.push(u));
  const after = (): LiveUpdate[] => updates.slice(1);
  return {
    live,
    updates,
    after,
    log: () => after().flatMap((u) => [...u.added.map((x) => `+${x.key}`), ...u.changed.map((x) => `~${x.after.key}`), ...u.removed.map((x) => `-${x.key}`)]),
  };
}
