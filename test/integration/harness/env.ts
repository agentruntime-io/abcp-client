// The environment of an integration test: a test issuer, the Go server behind a cuttable proxy, and clients (an engine over the real
// transport and the real manifests adapter) that connect through the proxy.

import {
  Engine,
  systemRandom,
  type EngineConfig,
  type EngineEvent,
  type Random,
  type StorageProvider,
  type TabHost,
  type Transport,
  type OutboxStorage,
} from "../../../src/engine/index.js";
import { createNodeTransport, type NodeTransport } from "../../../src/net/node.js";
import { ConnectManifests, TokenStore, createWebTransport, type TokenGrant } from "../../../src/net/index.js";
import { IssuerServer, type Grant } from "./issuer.js";
import { APP, Proxy, startServer, type ServerProcess } from "./server.js";

export const WAIT_MS = 15_000;

/** Polls until the condition holds; fails with the message when it does not within the time. */
export async function waitFor(cond: () => boolean | Promise<boolean>, message: string, timeoutMs = WAIT_MS): Promise<void> {
  const end = Date.now() + timeoutMs;
  for (;;) {
    if (await cond()) return;
    if (Date.now() > end) throw new Error(`timed out after ${timeoutMs} ms waiting for: ${message}`);
    await new Promise((r) => setTimeout(r, 20));
  }
}

/** The engine's own jitter is shrunk so that a reconnect after an outage does not wait for a second; ids stay random. */
export const fastRandom: Random = { float: () => 0.02, bytes: (n) => systemRandom.bytes(n) };

export const ALL: Grant[] = [{ ks: "acme/**", actions: ["read", "write:*", "keyspace:create"] }];

export interface ClientOptions {
  sub: string;
  deviceId?: string;
  grants?: Grant[];
  /** The lifetime of the first token, in seconds. */
  lifetimeS?: number;
  /** What the app gives the engine when it asks for a new token. */
  refresh?: (n: number) => TokenGrant | string | undefined;
  /** The HTTP client: connect-node over HTTP/1.1 (default) or HTTP/2, or `fetch` (the browser's shape). */
  via?: "node-1.1" | "node-2" | "web";
  storage?: OutboxStorage;
  store?: StorageProvider;
  tab?: TabHost;
  /** The network of this client: a proxy of its own (`env.network()`) that the test can cut; default the environment's. */
  network?: Proxy;
  config?: Partial<EngineConfig>;
  start?: boolean;
}

export interface Calls {
  [method: string]: number;
}

export interface TestClient {
  engine: Engine;
  events: EngineEvent[];
  tokens: TokenStore;
  manifests: ConnectManifests;
  /** The Node transport's connections, when the client has them (not for `via: "web"`). */
  net: NodeTransport | undefined;
  /** How often each transport method was called. */
  calls: Calls;
  /** The answers of `RefreshSession`. */
  refreshed: { expiresAt: number | undefined; dropped: string[] }[];
  /** The number of tokens that the app gave the engine. */
  refreshes(): number;
  ofType<T extends EngineEvent["type"]>(type: T): Extract<EngineEvent, { type: T }>[];
  close(): Promise<void>;
}

export class Env {
  private readonly clients: TestClient[] = [];
  private readonly extraProxies: Proxy[] = [];

  private constructor(
    readonly issuers: IssuerServer,
    public server: ServerProcess,
    readonly proxy: Proxy,
    private readonly startOptions: { manifest?: unknown; config?: Record<string, unknown> } = {},
  ) {}

  static async start(opts: { manifest?: unknown; config?: Record<string, unknown> } = {}): Promise<Env> {
    const issuers = await IssuerServer.start(APP);
    let server: ServerProcess | undefined;
    try {
      server = await startServer({ issuers, ...opts });
      const proxy = await Proxy.start(server.port);
      return new Env(issuers, server, proxy, opts);
    } catch (e) {
      await server?.kill();
      await issuers.close();
      throw e;
    }
  }

  get url(): string {
    return this.proxy.url;
  }

  /** A network of its own to the server, which the test can cut without cutting the others. */
  async network(): Promise<Proxy> {
    const p = await Proxy.start(this.server.port);
    this.extraProxies.push(p);
    return p;
  }

  /** Kills the server and starts another one on the same configuration: the store is in memory, so nothing it held is left. */
  async restartServer(): Promise<void> {
    await this.server.kill();
    this.server = await startServer({ issuers: this.issuers, ...this.startOptions });
    this.proxy.target(this.server.port);
    for (const p of this.extraProxies) p.target(this.server.port);
  }

  token(o: { sub: string; deviceId?: string; grants?: Grant[]; lifetimeS?: number }): string {
    return this.issuers.user.mint({ sub: o.sub, ...(o.deviceId !== undefined ? { deviceId: o.deviceId } : {}), grants: o.grants ?? ALL, ...(o.lifetimeS !== undefined ? { lifetimeS: o.lifetimeS } : {}) });
  }

  async client(o: ClientOptions): Promise<TestClient> {
    const deviceId = o.deviceId ?? `dev-${o.sub}`;
    let refreshes = 0;
    const tokens = new TokenStore({
      initial: this.token({ sub: o.sub, deviceId, ...(o.grants !== undefined ? { grants: o.grants } : {}), ...(o.lifetimeS !== undefined ? { lifetimeS: o.lifetimeS } : {}) }),
      source: () => {
        refreshes++;
        return o.refresh !== undefined ? o.refresh(refreshes) : this.token({ sub: o.sub, deviceId, ...(o.grants !== undefined ? { grants: o.grants } : {}) });
      },
    });
    const baseUrl = (o.network ?? this.proxy).url;
    const net = o.via === "web" ? undefined : createNodeTransport({ baseUrl, token: tokens.reader, httpVersion: o.via === "node-2" ? "2" : "1.1" });
    const calls: Calls = {};
    const refreshed: TestClient["refreshed"] = [];
    const transport = spy(net?.transport ?? createWebTransport({ baseUrl, token: tokens.reader }), calls, refreshed);
    const manifests = new ConnectManifests({ transport, appId: APP });
    const events: EngineEvent[] = [];
    const engine = await Engine.create({
      appId: APP,
      principal: { issuer: this.issuers.user.url, sub: o.sub, deviceId },
      transport,
      manifests,
      tokens,
      random: fastRandom,
      origin: this.url,
      ...(o.storage !== undefined ? { storage: o.storage } : {}),
      ...(o.store !== undefined ? { store: o.store } : {}),
      ...(o.tab !== undefined ? { tab: o.tab } : {}),
      ...(o.config ?? {}),
    });
    engine.on((e) => events.push(e));
    const client: TestClient = {
      engine,
      events,
      tokens,
      manifests,
      net,
      calls,
      refreshed,
      refreshes: () => refreshes,
      ofType: <T extends EngineEvent["type"]>(type: T) => events.filter((e) => e.type === type) as Extract<EngineEvent, { type: T }>[],
      close: async () => {
        await engine.stop().catch(() => undefined);
        net?.close();
      },
    };
    this.clients.push(client);
    if (o.start !== false) engine.start();
    return client;
  }

  async close(): Promise<void> {
    await Promise.all(this.clients.map((c) => c.close().catch(() => undefined)));
    await this.proxy.close().catch(() => undefined);
    await Promise.all(this.extraProxies.map((p) => p.close().catch(() => undefined)));
    await this.server.kill().catch(() => undefined);
    await this.issuers.close().catch(() => undefined);
  }
}

/** A transport that counts its calls and notes the answers of `RefreshSession`. */
function spy(t: Transport, calls: Calls, refreshed: TestClient["refreshed"]): Transport {
  const count = <A extends unknown[], R>(name: string, f: (...a: A) => R): ((...a: A) => R) => {
    return (...a: A): R => {
      calls[name] = (calls[name] ?? 0) + 1;
      return f(...a);
    };
  };
  return {
    getManifest: count("getManifest", (r, o) => t.getManifest(r, o)),
    mutate: count("mutate", (r, o) => t.mutate(r, o)),
    getMutationResult: count("getMutationResult", (r, o) => t.getMutationResult(r, o)),
    getMutationResults: count("getMutationResults", (r, o) => t.getMutationResults(r, o)),
    cancelIntent: count("cancelIntent", (r, o) => t.cancelIntent(r, o)),
    createKeyspace: count("createKeyspace", (r, o) => t.createKeyspace(r, o)),
    loadState: count("loadState", (r, o) => t.loadState(r, o)),
    subscribe: count("subscribe", (r, o) => t.subscribe(r, o)),
    updateSubscription: count("updateSubscription", (r, o) => t.updateSubscription(r, o)),
    joinPresence: count("joinPresence", (r, o) => t.joinPresence(r, o)),
    publishPresence: count("publishPresence", (r, o) => t.publishPresence(r, o)),
    leavePresence: count("leavePresence", (r, o) => t.leavePresence(r, o)),
    createUpload: count("createUpload", (r, o) => t.createUpload(r, o)),
    commitBlob: count("commitBlob", (r, o) => t.commitBlob(r, o)),
    getBlob: count("getBlob", (r, o) => t.getBlob(r, o)),
    listKeyspaces: count("listKeyspaces", (r, o) => t.listKeyspaces(r, o)),
    deleteBlob: count("deleteBlob", (r, o) => t.deleteBlob(r, o)),
    refreshSession: count("refreshSession", async (r, o) => {
      const resp = await t.refreshSession(r, o);
      refreshed.push({
        expiresAt: resp.expiresAt === undefined ? undefined : Number(resp.expiresAt.seconds) * 1000,
        dropped: [...resp.droppedKeyspaceIds],
      });
      return resp;
    }),
  };
}

/** What a failed test needs to see: the engine's status and events, the calls it made, the server's log. */
export function dump(c: TestClient, env: Env): string {
  const j = (v: unknown): string => JSON.stringify(v, (_k, x) => (typeof x === "bigint" ? x.toString() : x), 1);
  return `status ${j(c.engine.status())}\nevents ${j(c.events)}\ncalls ${j(c.calls)}\nserver log:\n${env.server.log()}`;
}
