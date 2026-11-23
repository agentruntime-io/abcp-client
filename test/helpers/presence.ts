// The harness of the presence and blob suites (09 §9.8, §9.9): the engine rig of helpers/engine.ts over a manifest whose kind declares presence, a second engine on the same server and
// clock, and readers of what the fake server saw.

import { create } from "@bufbuild/protobuf";
import { StaticManifests, type EngineEvent, type PresenceEvent, type StaticManifestSpec } from "../../src/engine/index.js";
import { FakeServer, ManualClock } from "../../src/fake/index.js";
import { PresenceFrameSchema, PresenceEntrySchema, SubscribeResponseSchema, type PublishPresenceRequest } from "../../src/gen/abcp/v2/sync_pb.js";
import { PrincipalSchema } from "../../src/gen/abcp/v2/types_pb.js";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import { jsonBytes } from "../../src/engine/index.js";
import { parse } from "../../src/rules/jsonv1/index.js";
import { KS, MANIFEST, rig, type Rig, type RigOptions } from "./engine.js";

export { KS };

/** `MANIFEST` with presence for the kind `kind` of the fake's keyspaces: the topics `room/{id}`, and the keys of `docs/{id}` (a `crdt` template) as awareness topics. */
export const PRESENCE_MANIFEST: StaticManifestSpec = { ...MANIFEST, presence: { kind: ["room/{id}"] } };

export const ROOM = "room/r1";
export const DOC = "docs/d1";

export function presenceManifests(): StaticManifests {
  return new StaticManifests(PRESENCE_MANIFEST);
}

/** The engine rig over `PRESENCE_MANIFEST`. */
export function prig(opts: RigOptions = {}): Promise<Rig> {
  return rig({ manifests: presenceManifests(), ...opts });
}

/** A second engine (another principal) on the same server, clock and manifests as `a`. */
export function second(a: Rig, sub = "bob", opts: RigOptions = {}): Promise<Rig> {
  return rig({ manifests: a.manifests, server: a.server, clock: a.clock, sub, seed: 31, ...opts });
}

/** The presence events of an engine, as they come. */
export function presenceEvents(r: Rig): PresenceEvent[] {
  const out: PresenceEvent[] = [];
  r.engine.onPresence((e) => out.push(e));
  return out;
}

/** The requests of a method that reached the fake, in order. */
export function requests<T>(server: FakeServer, method: string): T[] {
  return server.calls.filter((c) => c.method === method).map((c) => c.request as T);
}

/** The states that `PublishPresence` requests carried, decoded from json/v1. */
export function publishedStates(server: FakeServer, sub?: string): unknown[] {
  return server.calls
    .filter((c) => c.method === "publishPresence" && (sub === undefined || c.sub === sub))
    .map((c) => plain(parse((c.request as PublishPresenceRequest).state)));
}

/** A json/v1 value as plain JavaScript (objects as objects, integers as numbers). */
export function plain(v: unknown): unknown {
  if (v instanceof Map) return Object.fromEntries([...v].map(([k, x]) => [k, plain(x)]));
  if (Array.isArray(v)) return v.map(plain);
  return v;
}

/** The state of an entry as plain JavaScript. */
export function stateOf(state: Uint8Array): unknown {
  return plain(parse(state));
}

/** A presence frame for the sessions of the server, as the server itself could send it. */
export function frame(init: { keyspaceId?: string; topic: string; snapshot?: boolean; entries?: { clientId: string; state: unknown; expiresMs: number; sub?: string }[]; left?: string[] }): ReturnType<typeof create<typeof SubscribeResponseSchema>> {
  return create(SubscribeResponseSchema, {
    frame: {
      case: "presence",
      value: create(PresenceFrameSchema, {
        keyspaceId: init.keyspaceId ?? KS,
        topic: init.topic,
        snapshot: init.snapshot ?? false,
        entries: (init.entries ?? []).map((e) =>
          create(PresenceEntrySchema, {
            principal: create(PrincipalSchema, { appId: "app", sub: e.sub ?? "zoe", issuer: "https://issuer.test" }),
            clientId: e.clientId,
            state: typeof e.state === "string" ? new TextEncoder().encode(e.state) : jsonBytes(e.state),
            expiresAt: timestampFromMs(e.expiresMs),
          }),
        ),
        leftClientIds: init.left ?? [],
      }),
    },
  });
}

export const protocolErrors = (r: Rig): EngineEvent[] => r.events.filter((e) => e.type === "protocol-error");

// ---------------------------------------------------------------------------------------------------------------------
// Blobs (09 §9.9)


/** The engine rig over a fake server whose blob store the engine uploads to and downloads from (`FakeBlobs.blobHttp`). */
export async function brig(opts: RigOptions & { limits?: Record<string, number> } = {}): Promise<Rig> {
  const clock = opts.clock ?? new ManualClock();
  const manifests = opts.manifests ?? new StaticManifests(MANIFEST);
  let server = opts.server;
  if (server === undefined) {
    server = new FakeServer({ manifests, clock, heartbeatMs: 15_000, ...(opts.limits !== undefined ? { limits: opts.limits } : {}) });
    server.createKeyspaceNow(KS);
  }
  const http = server.blobs.blobHttp();
  const { limits: _limits, ...rest } = opts;
  void _limits;
  return rig({ ...rest, manifests, clock, server, cfg: { ...(opts.cfg ?? {}), blobs: { http, ...(opts.cfg?.blobs ?? {}) } } });
}

/** Lets a call that waits for the clock run to its end: the clock moves by `stepMs` until it settles, and the result is what it gave, or its failure. */
export async function drive<T>(r: Rig, p: Promise<T>, stepMs = 1000, max = 300): Promise<T> {
  let done = false;
  let failed = false;
  let value: T | undefined;
  let err: unknown;
  void p.then(
    (v) => {
      done = true;
      value = v;
    },
    (e: unknown) => {
      done = true;
      failed = true;
      err = e;
    },
  );
  for (let i = 0; i < max && !done; i++) {
    await r.clock.yield();
    await r.engine.settle().catch(() => undefined);
    if (!done) await r.clock.advance(stepMs);
  }
  if (!done) throw new Error("the call did not finish");
  if (failed) throw err;
  return value as T;
}

/**
 * Waits, in real time, for a condition that real I/O (files, a database) makes true: the engine's clock does not wait for it, so each round lets the event loop run and the engine settle, and
 * moves the engine's clock by `stepMs` (a timer that the engine waits for passes). There is no assumption about how long the I/O takes: only a deadline, long enough for a loaded machine, after
 * which the test fails with `what`.
 */
export async function until(r: Rig, cond: () => boolean | Promise<boolean>, what: string, stepMs = 200, deadlineMs = 60_000): Promise<void> {
  const end = Date.now() + deadlineMs;
  while (!(await cond())) {
    if (Date.now() > end) throw new Error(`timed out waiting for: ${what}`);
    await new Promise((res) => setTimeout(res, 5));
    if (stepMs > 0) await r.clock.advance(stepMs);
    await r.engine.settle().catch(() => undefined);
  }
}

/** The bytes of a test blob: `n` octets of a pattern. */
export function bytesOf(n: number, seed = 7): Uint8Array {
  const out = new Uint8Array(n);
  for (let i = 0; i < n; i++) out[i] = (i * 31 + seed) & 0xff;
  return out;
}
