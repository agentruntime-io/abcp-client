// The harness of the engine suites: a fake server, a manual clock, a seeded random source, and an engine over them.

import { Engine, MemoryStorage, StaticManifests, type EngineConfig, type EngineEvent, type StaticManifestSpec } from "../../src/engine/index.js";
import { FakeServer, ManualClock, SeededRandom } from "../../src/fake/index.js";
import type { Principal } from "../../src/gen/abpc/v2/types_pb.js";

export const KS = "ks1";

/** A manifest with one template of each merge rule, an entity with a child, and the op types that the suites use. */
export const MANIFEST: StaticManifestSpec = {
  version: 1,
  templates: {
    "issues/{id}": { rule: "lww", entity: true },
    "issues/{id}/title": { rule: "lww" },
    "issues/{id}/status": { rule: "lww", fencedBy: "claims/{id}" },
    "claims/{id}": { rule: "lww" },
    "counters/{id}": { rule: "counter" },
    "notes/{id}": { rule: "cas" },
    "docs/{id}": { rule: "crdt", roots: { text: "text" } },
    "settings/{name}": { rule: "lww" },
    "raw/{id}": { rule: "lww", encoding: "bytes/v1" },
  },
  opTypes: {
    Set: {},
    Create: {},
    Incr: {},
    CasSet: {},
    DocEdit: {},
    Approve: { lane: "GATED" },
    ApproveQuiet: { lane: "GATED", logIntent: false },
    Fenced: {},
    Barrier: { barrier: true },
    Pay: { spend: true },
    PayGated: { lane: "GATED", spend: true },
    Quick: { offlineMode: "fail_fast" },
    Silent: { optimistic: false },
    Writes: { writes: ["settings/{name}"] },
  },
};

export interface Rig {
  clock: ManualClock;
  random: SeededRandom;
  manifests: StaticManifests;
  server: FakeServer;
  engine: Engine;
  storage: MemoryStorage;
  principal: Principal;
  events: EngineEvent[];
  config: EngineConfig;
}

export interface RigOptions {
  keyspaces?: string[];
  start?: boolean;
  sub?: string;
  heartbeatMs?: number;
  storage?: MemoryStorage;
  server?: FakeServer;
  clock?: ManualClock;
  manifests?: StaticManifests;
  cfg?: Partial<EngineConfig>;
  serverOptions?: { maxSessions?: number; pageSize?: number; limits?: Record<string, number> };
  seed?: number;
}

/** A fake server with the keyspaces, and an engine that follows them and is started and settled. */
export async function rig(opts: RigOptions = {}): Promise<Rig> {
  const clock = opts.clock ?? new ManualClock();
  const random = new SeededRandom(opts.seed ?? 7);
  const manifests = opts.manifests ?? new StaticManifests(MANIFEST);
  const server =
    opts.server ??
    new FakeServer({ manifests, clock, heartbeatMs: opts.heartbeatMs ?? 15_000, ...(opts.serverOptions?.maxSessions !== undefined ? { maxSessions: opts.serverOptions.maxSessions } : {}), ...(opts.serverOptions?.pageSize !== undefined ? { pageSize: opts.serverOptions.pageSize } : {}), ...(opts.serverOptions?.limits !== undefined ? { limits: opts.serverOptions.limits } : {}) });
  const keyspaces = opts.keyspaces ?? [KS];
  if (opts.server === undefined) for (const k of keyspaces) server.createKeyspaceNow(k);
  const principal = server.principal(opts.sub ?? "ana");
  const storage = opts.storage ?? new MemoryStorage();
  const config: EngineConfig = {
    appId: "app",
    principal: { issuer: principal.issuer, sub: principal.sub, deviceId: principal.deviceId },
    transport: server.transport(principal),
    manifests,
    storage,
    clock,
    random,
    ...(opts.cfg ?? {}),
  };
  const engine = await Engine.create(config);
  const events: EngineEvent[] = [];
  engine.on((e) => events.push(e));
  for (const k of keyspaces) await engine.addKeyspace(k);
  if (opts.start !== false) {
    engine.start();
    await engine.settle();
  }
  return { clock, random, manifests, server, engine, storage, principal, events, config };
}

/** The types of the events for one mutation id (or all events when none is given), in order. */
export function types(r: Rig, id?: string): string[] {
  return r.events.filter((e) => id === undefined || ("mutationId" in e && e.mutationId === id)).map((e) => e.type);
}

export function ofType<T extends EngineEvent["type"]>(r: Rig, type: T): Extract<EngineEvent, { type: T }>[] {
  return r.events.filter((e) => e.type === type) as Extract<EngineEvent, { type: T }>[];
}

/** Advances the clock and lets the engine settle. */
export async function tick(r: Rig, ms = 0): Promise<void> {
  if (ms > 0) await r.clock.advance(ms);
  await r.engine.settle();
}

export const text = (b: Uint8Array | undefined): string | undefined => (b === undefined ? undefined : new TextDecoder().decode(b));

// ---------------------------------------------------------------------------------------------------------------------

import { create } from "@bufbuild/protobuf";
import { ChangeKind, KeyChangeSchema, type KeyChange } from "../../src/gen/abpc/v2/types_pb.js";
import { jsonBytes } from "../../src/engine/index.js";

/** A KeyChange for a write that the server makes directly (`FakeServer.write`). */
export function kc(key: string, kind: "set" | "delete" | "incr", v?: unknown): KeyChange {
  if (kind === "delete") return create(KeyChangeSchema, { key, kind: ChangeKind.DELETE });
  if (kind === "incr") return create(KeyChangeSchema, { key, kind: ChangeKind.INCR, delta: BigInt(v as number) });
  return create(KeyChangeSchema, { key, kind: ChangeKind.SET, value: jsonBytes(v), encoding: "json/v1" });
}

/** Drops the session: the engine is offline until the clock moves (the reconnect backoff of 09 [READ-57]). */
export async function goOffline(r: Rig): Promise<void> {
  r.server.disconnect();
  await r.engine.settle();
}

/** Lets the reconnect backoff pass, and the engine reconnects and settles. */
export async function goOnline(r: Rig): Promise<void> {
  await r.clock.advance(1000);
  await r.engine.settle();
}

// ---------------------------------------------------------------------------------------------------------------------

import { MutateRequestSchema, MutationResultSchema, type Mutation, type MutationResult } from "../../src/gen/abpc/v2/sync_pb.js";
import { RetrySchema, type Reject, type RetryReason } from "../../src/gen/abpc/v2/types_pb.js";

/** The `Mutate` requests that reached the server, in order. */
export function mutateRequests(r: Rig): ReturnType<typeof create<typeof MutateRequestSchema>>[] {
  return r.server.calls.filter((c) => c.method === "mutate").map((c) => c.request as ReturnType<typeof create<typeof MutateRequestSchema>>);
}

/** The ids of the mutations of every `Mutate` request, in order. */
export function sentIds(r: Rig): string[] {
  return mutateRequests(r).flatMap((q) => q.mutations.map((m) => m.mutationId));
}

export function rejectResult(m: Mutation | string, reject: Reject): MutationResult {
  return create(MutationResultSchema, { mutationId: typeof m === "string" ? m : m.mutationId, outcome: { case: "rejected", value: reject } });
}

export function retryResult(m: Mutation | string, reason: RetryReason, afterMs = 0, intentSeq = 0n): MutationResult {
  return create(MutationResultSchema, { mutationId: typeof m === "string" ? m : m.mutationId, intentSeq, outcome: { case: "retry", value: create(RetrySchema, { reason, afterMs }) } });
}

/** The decoded content of an outbox entry. */
export function contentOf(r: Rig, mutationId: string): Mutation {
  const e = r.engine.outbox().find((x) => x.mutationId === mutationId);
  if (e === undefined) throw new Error(`no outbox entry ${mutationId}`);
  return (r.engine as unknown as { content(e: unknown): Mutation }).content(e);
}
