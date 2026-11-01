// The harness of the suites of 14 §14.8 to §14.10: one device (a `MemoryDevice`) with its tabs, one fake server, one clock, and engines
// over them. Each tab gets an engine of its own, which elects the flusher among them through the device's lock.

import { Engine, StaticManifests, type EngineConfig, type EngineEvent } from "../../src/engine/index.js";
import { MemoryDevice, MemoryTab, type MemoryTabOptions } from "../../src/engine/tabs-memory.js";
import { FakeServer, ManualClock, SeededRandom, type FakeTransport } from "../../src/fake/index.js";
import type { Principal } from "../../src/gen/abpc/v2/types_pb.js";
import { KS, MANIFEST } from "./engine.js";

export const ORIGIN = "https://abpc.test";

export interface TabEngine {
  tab: MemoryTab;
  engine: Engine;
  events: EngineEvent[];
  transport: FakeTransport;
  principal: Principal;
  random: SeededRandom;
}

export interface TabsRig {
  clock: ManualClock;
  manifests: StaticManifests;
  server: FakeServer;
  device: MemoryDevice;
  /** Opens a tab with an engine on it (which follows `keyspaces` and is started, unless `start` is false). */
  open(o?: OpenOptions): Promise<TabEngine>;
}

export interface OpenOptions {
  tab?: MemoryTab | MemoryTabOptions;
  sub?: string;
  deviceId?: string;
  keyspaces?: string[];
  start?: boolean;
  seed?: number;
  cfg?: Partial<EngineConfig>;
  origin?: string;
  /** Does not add the keyspaces: the engine follows only what its stored state names. */
  noAdd?: boolean;
}

export function tabsRig(opts: { keyspaces?: string[]; heartbeatMs?: number; serverLimits?: Record<string, number>; storageOptions?: ConstructorParameters<typeof MemoryDevice>[0] } = {}): TabsRig {
  const clock = new ManualClock();
  const manifests = new StaticManifests(MANIFEST);
  const server = new FakeServer({ manifests, clock, heartbeatMs: opts.heartbeatMs ?? 15_000, ...(opts.serverLimits !== undefined ? { limits: opts.serverLimits } : {}) });
  for (const k of opts.keyspaces ?? [KS]) server.createKeyspaceNow(k);
  const device = new MemoryDevice(opts.storageOptions);
  let seed = 100;
  const open = async (o: OpenOptions = {}): Promise<TabEngine> => {
    const tab = o.tab instanceof MemoryTab ? o.tab : device.newTab(o.tab);
    const principal = server.principal(o.sub ?? "ana", o.deviceId ?? `${o.sub ?? "ana"}-device`);
    const transport = server.transport(principal);
    const random = new SeededRandom(o.seed ?? seed++);
    const config: EngineConfig = {
      appId: "app",
      principal: { issuer: principal.issuer, sub: principal.sub, deviceId: principal.deviceId },
      transport,
      manifests,
      clock,
      random,
      origin: o.origin ?? ORIGIN,
      tab,
      ...(o.cfg ?? {}),
    };
    const engine = await Engine.create(config);
    const events: EngineEvent[] = [...engine.eventLog()];
    engine.on((e) => events.push(e));
    if (o.noAdd !== true) for (const k of o.keyspaces ?? opts.keyspaces ?? [KS]) await engine.addKeyspace(k);
    if (o.start !== false) engine.start();
    await settleAll(engine);
    return { tab, engine, events, transport, principal, random };
  };
  return { clock, manifests, server, device, open };
}

/** Lets the engines settle, and the messages between them travel and settle again. */
export async function settleAll(...engines: Engine[]): Promise<void> {
  for (let i = 0; i < 4; i++) for (const e of engines) await e.settle();
}

export const typesOf = (t: TabEngine, id?: string): string[] => t.events.filter((e) => id === undefined || ("mutationId" in e && e.mutationId === id)).map((e) => e.type);
