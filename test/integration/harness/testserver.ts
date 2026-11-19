// The test binary of the CONF-6 test profile as a process (17 [CONF-6], docs/S1-DESIGN.md §6, piece P5): `server/cmd/abpc-testserver` is built once into
// `client/.tmp/` (as `server.ts` builds the production binary: from the working tree, or from git HEAD with ABPC_SERVER_SOURCE=head), started on a configuration
// file that the harness writes (the production configuration plus a `test_profile` section), and driven through its control API, which this module gives as a
// connect-es client (`TestServer.control`, the generated `TestControlService` of `client/test/gen`). The binary writes a ready file when its deployment is up;
// that file, not a log, tells the harness where the nodes, the issuers and the control listener are.
//
// What the harness gives a suite:
//  - `startTestServer(options)`: a deployment of three nodes (by default) on the in-memory stores, with two user issuers (`user1`, `user2`), one admin issuer (`admin`)
//    and the manifest of `server.ts` (the app `tracker`), under the manual clock of the one time source (10 [AUTH-66]);
//  - `TestServer`: the control client, the node URLs and the issuers, the CA, the operator's secret, the log of the process, `now()`, `token()` (a token that the
//    test issuer signs, dated by the time source), `client()` (an engine on one node) and `close()`;
//  - `testServerAvailable`, `testServerBinary` and `removeTestServerBinary` (the global teardown calls it).
// `server.ts` and the integration tests of C5 stay as they are: they run the production binary.
//
// PostgreSQL is not wired in: the deployment is in memory.

import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import * as fs from "node:fs";
import * as http from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import { timestampDate } from "@bufbuild/protobuf/wkt";
import { createClient, type Client, type Interceptor } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-node";
import { Engine, systemRandom, type EngineConfig, type EngineEvent, type Random } from "../../../src/engine/index.js";
import { ConnectManifests, TokenStore, type TokenGrant } from "../../../src/net/index.js";
import { createNodeTransport, type NodeTransport } from "../../../src/net/node.js";
import { TestControlService, type GetDeploymentResponse, type MintTokenRequest } from "../../gen/abpctest/v1/control_pb.js";
import type { Grant } from "./issuer.js";
import { APP, ensureTmp, goAvailable, MANIFEST, SERVER_DIR, TMP_DIR } from "./server.js";
import { SKIP_NEEDS_SERVER } from "../../helpers/server-dir.js";

/** The control API of the test profile as a client. */
export type Control = Client<typeof TestControlService>;

/** Whether the test binary can be built here: a Go toolchain, and the source of the binary (a checkout that has none skips the suites that need it). */
export const testServerAvailable = goAvailable && SERVER_DIR !== "" && fs.existsSync(path.join(SERVER_DIR, "cmd", "abpc-testserver"));
export const TESTSERVER_SKIP_REASON =
  `integration tests against the test binary are skipped: ${SKIP_NEEDS_SERVER}; they also need a Go toolchain, cmd/abpc-testserver in the checkout, and ABPC_SKIP_INTEGRATION not 1`;

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

// ---- the build -----------------------------------------------------------------------------------------------------

const BINARY = path.join(TMP_DIR, os.platform() === "win32" ? "abpc-testserver.exe" : "abpc-testserver");
const LOCK = path.join(TMP_DIR, "build-testserver.lock");

function goBuild(cwd: string): { ok: boolean; output: string } {
  const r = spawnSync("go", ["build", "-o", BINARY, "./cmd/abpc-testserver"], { cwd, encoding: "utf8", timeout: 10 * 60_000 });
  return { ok: r.status === 0, output: `${r.stdout}\n${r.stderr}` };
}

/** Builds the test binary from the committed tree (`git archive HEAD server`), for when the working tree does not build. Read-only on the repository. */
function buildFromHead(): { ok: boolean; output: string } {
  const dir = path.join(TMP_DIR, "head-src-testserver"); // not the directory of server.ts: the two binaries may be built at the same time
  fs.rmSync(dir, { recursive: true, force: true });
  fs.mkdirSync(dir, { recursive: true });
  const repo = path.resolve(SERVER_DIR, "..");
  const tarball = spawnSync("git", ["-C", repo, "archive", "--format=tar", "HEAD", "server"], { maxBuffer: 256 << 20 });
  if (tarball.status !== 0) return { ok: false, output: `git archive failed: ${tarball.stderr}` };
  try {
    const x = spawnSync("tar", ["-x"], { cwd: dir, input: tarball.stdout, maxBuffer: 1 << 20 });
    if (x.status !== 0) return { ok: false, output: `tar failed: ${x.stderr}` };
    return goBuild(path.join(dir, "server"));
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

/** How long, and how often, a working tree that does not build at the moment is tried again (the server is under work by someone else). */
const BUILD_RETRY_MS = 20_000;
const BUILD_PATIENCE_MS = 8 * 60_000;

/** Which source the test binary of this run was built from, for the report of a run. */
export let testServerBuiltFrom: "worktree" | "head" | undefined;

/**
 * The path of the test binary, built on the first call of the run (the test files run in parallel, so the build is under a lock that the others wait on). The
 * working tree is built; when it does not build at the moment, the build is tried again every 20 s for up to 8 minutes and then fails with the compiler's words.
 * ABPC_SERVER_SOURCE=head builds the committed tree instead. The server is never edited from here.
 */
export async function testServerBinary(): Promise<string> {
  ensureTmp();
  const deadline = Date.now() + BUILD_PATIENCE_MS + 12 * 60_000;
  for (;;) {
    if (fs.existsSync(BINARY) && !fs.existsSync(LOCK)) return BINARY;
    let mine = false;
    try {
      fs.mkdirSync(LOCK);
      mine = true;
    } catch {
      // another worker builds; a lock that has not been touched for as long as a build may take is left by a run that was killed
      try {
        if (Date.now() - fs.statSync(LOCK).mtimeMs > BUILD_RETRY_MS * 3 + 11 * 60_000) fs.rmSync(LOCK, { recursive: true, force: true });
      } catch {
        // gone meanwhile
      }
    }
    if (mine) {
      try {
        if (!fs.existsSync(BINARY)) {
          if (process.env["ABPC_SERVER_SOURCE"] === "head") {
            const r = buildFromHead();
            if (!r.ok) throw new Error(`go build of the test binary (git HEAD) failed:\n${r.output}`);
            testServerBuiltFrom = "head";
          } else {
            const give = Date.now() + BUILD_PATIENCE_MS;
            for (let attempt = 1; ; attempt++) {
              const r = goBuild(SERVER_DIR);
              if (r.ok) break;
              if (Date.now() + BUILD_RETRY_MS > give) throw new Error(`go build of the test binary failed after ${attempt} attempts:\n${r.output}`);
              console.warn(`[integration] the test binary of the working tree does not build (attempt ${attempt}); trying again in ${BUILD_RETRY_MS / 1000} s:\n${r.output.slice(0, 600)}`);
              fs.utimesSync(LOCK, new Date(), new Date()); // the lock is alive
              await sleep(BUILD_RETRY_MS);
            }
            testServerBuiltFrom = "worktree";
          }
        }
        return BINARY;
      } finally {
        fs.rmSync(LOCK, { recursive: true, force: true });
      }
    }
    if (Date.now() > deadline) throw new Error("timed out waiting for the test binary to be built");
    await sleep(200);
  }
}

/** Deletes the test binary and its lock (the global teardown: the binary is 30 MB and the disk is small). */
export function removeTestServerBinary(): void {
  fs.rmSync(BINARY, { force: true });
  fs.rmSync(LOCK, { recursive: true, force: true });
  fs.rmSync(path.join(TMP_DIR, "head-src-testserver"), { recursive: true, force: true });
}

// ---- the ready file ------------------------------------------------------------------------------------------------

/** The ready file of the test binary (`server/internal/testprofile/deploy/ready.go`): where the deployment is and the secrets that reach it. */
export interface Ready {
  run_id: string;
  control_url: string;
  control_secret: string;
  nodes: { name: string; url: string }[];
  issuers: { name: string; url: string; jwks_uri: string; admin: boolean; algorithm: string }[];
  ca_pem: string;
  operator_secret: string;
  webhook_url: string;
  deployment_issuer_url: string;
  build: string;
  store: string;
  background: string;
}

// ---- the options ---------------------------------------------------------------------------------------------------

export interface TestServerOptions {
  /** The manifest of the app `tracker` (default: the manifest of `server.ts`). */
  manifest?: unknown;
  /** The number of server nodes; default 3 (17 [CONF-6]: at least three). */
  nodes?: number;
  /** The first reading of the manual clock (10 [AUTH-66]); default the real time of the start, so that the clients' own clocks and the server's agree until a test moves it. */
  clockStart?: Date;
  /** "scheduled" (the default: the nodes' loops run) or "manual" (no pass runs until RunPass; heartbeats stay). */
  background?: "scheduled" | "manual";
  /** More members of the `test_profile` section (`salt_seed`, `issuers`, ...) and of the configuration file. */
  profile?: Record<string, unknown>;
  config?: Record<string, unknown>;
}

/** The server's lease on a token: the default of `token()`. Below the 24 h of 10 [AUTH-8], above what a test moves the clock by. */
export const DEFAULT_LIFETIME_S = 12 * 3600;

/** The grants of a user of `acme/**`. */
export const ACME: Grant[] = [{ ks: "acme/**", actions: ["read", "write:*", "keyspace:create"] }];

export interface TokenOptions {
  /** The test issuer that signs: `user1` (default), `user2` or `admin`. */
  issuer?: string;
  sub: string;
  /** The audience; default the app. */
  aud?: string;
  deviceId?: string;
  grants?: Grant[];
  /** Seconds from the time source's reading (not the machine's) to `exp`; default 12 h. */
  lifetimeS?: number;
  /** Members of the claims, merged over the defaults. */
  claims?: Record<string, unknown>;
  /** Members of the MintTokenRequest (`headerJson`, `kid`, `unpublishedKey`, `rawHeader`, ...): the issuer signs what is given. */
  mint?: Partial<Omit<MintTokenRequest, "$typeName" | "issuer" | "claimsJson">>;
}

export interface ClientOptions {
  /** The node that the engine connects to: `n1` (default), `n2`, ... */
  node?: string;
  sub: string;
  issuer?: string;
  deviceId?: string;
  grants?: Grant[];
  lifetimeS?: number;
  /** The engine's jitter is shrunk so that a reconnect after an outage does not wait for seconds; ids stay random. */
  random?: Random;
  config?: Partial<EngineConfig>;
  /** Start the engine's sync; default true. */
  start?: boolean;
}

export interface ServerClient {
  engine: Engine;
  events: EngineEvent[];
  tokens: TokenStore;
  /** The node's transport: the raw calls (`subscribe`, `loadState`, ...) beside the engine. */
  net: NodeTransport;
  node: string;
  url: string;
  ofType<T extends EngineEvent["type"]>(type: T): Extract<EngineEvent, { type: T }>[];
  close(): Promise<void>;
}

const fastRandom: Random = { float: () => 0.02, bytes: (n) => systemRandom.bytes(n) };

// ---- the process ---------------------------------------------------------------------------------------------------

const running = new Set<ChildProcess>();
process.once("exit", () => {
  for (const c of running) c.kill();
});

export class TestServer {
  /** The control API: the RPCs of `proto/abpctest/v1/control.proto` (those that are not built answer Unimplemented). */
  readonly control: Control;

  /** @internal use `startTestServer` */
  constructor(
    readonly ready: Ready,
    private readonly child: ChildProcess,
    private readonly getLog: () => string,
    private readonly cleanup: () => Promise<void>,
    private readonly agent: http.Agent,
  ) {
    const bearer: Interceptor = (next) => (req) => {
      req.header.set("Authorization", `Bearer ${ready.control_secret}`);
      return next(req);
    };
    this.control = createClient(
      TestControlService,
      createConnectTransport({ baseUrl: ready.control_url, httpVersion: "1.1", useBinaryFormat: true, interceptors: [bearer], nodeOptions: { agent } }),
    );
  }

  get runId(): string {
    return this.ready.run_id;
  }
  /** The nodes' endpoints, `n1`, `n2`, ...: the same across a stop and a start. */
  get nodes(): { name: string; url: string }[] {
    return this.ready.nodes;
  }
  nodeUrl(name: string): string {
    const n = this.ready.nodes.find((x) => x.name === name);
    if (n === undefined) throw new Error(`no node ${name} (the nodes: ${this.ready.nodes.map((x) => x.name).join(", ")})`);
    return n.url;
  }
  get issuers(): Ready["issuers"] {
    return this.ready.issuers;
  }
  issuer(name: string): Ready["issuers"][number] {
    const i = this.ready.issuers.find((x) => x.name === name);
    if (i === undefined) throw new Error(`no test issuer ${name} (the issuers: ${this.ready.issuers.map((x) => x.name).join(", ")})`);
    return i;
  }
  /** The CA of the run (PEM): it signs the certificates of the issuers' and the webhook's endpoints. */
  get caPem(): string {
    return this.ready.ca_pem;
  }
  /** The deployment operator's bearer secret (12 §12.1.1). */
  get operatorSecret(): string {
    return this.ready.operator_secret;
  }
  /** What the process wrote to its log (stderr). */
  log(): string {
    return this.getLog();
  }

  /** The deployment as it is now: its nodes, issuers, clock and knobs. */
  deployment(): Promise<GetDeploymentResponse> {
    return this.control.getDeployment({});
  }

  /** The reading of the one time source (10 [AUTH-66]), the clock that dates every record and judges every token. */
  async now(): Promise<Date> {
    const d = await this.deployment();
    if (d.clock?.now === undefined) throw new Error("the time source cannot be read (it is failing)");
    return timestampDate(d.clock.now);
  }

  /**
   * A token that a test issuer signs for the app: `iat` is the time source's reading and `exp` is `lifetimeS` after it, so that a token follows the clock
   * that a test moves. The claims are those of 10 [AUTH-8]: the audience, the subject, the device, and the grants.
   */
  async token(o: TokenOptions): Promise<string> {
    const now = Math.floor((await this.now()).getTime() / 1000);
    const claims: Record<string, unknown> = {
      aud: o.aud ?? APP,
      sub: o.sub,
      iat: now,
      exp: now + (o.lifetimeS ?? DEFAULT_LIFETIME_S),
      abpc: { grants: (o.grants ?? ACME).map((g) => ({ ks: g.ks, actions: g.actions })) },
      ...(o.deviceId !== undefined ? { device_id: o.deviceId } : {}),
      ...(o.claims ?? {}),
    };
    const resp = await this.control.mintToken({ ...(o.mint ?? {}), issuer: o.issuer ?? "user1", claimsJson: JSON.stringify(claims) });
    return resp.token;
  }

  /** An engine on one node, over the real transport and manifests, with a token that is minted again when the engine asks for one. */
  async client(o: ClientOptions): Promise<ServerClient> {
    const node = o.node ?? "n1";
    const url = this.nodeUrl(node);
    const issuer = o.issuer ?? "user1";
    const deviceId = o.deviceId ?? `dev-${o.sub}`;
    const mint = (): Promise<string> =>
      this.token({ sub: o.sub, issuer, deviceId, ...(o.grants !== undefined ? { grants: o.grants } : {}), ...(o.lifetimeS !== undefined ? { lifetimeS: o.lifetimeS } : {}) });
    const tokens = new TokenStore({ initial: await mint(), source: async (): Promise<TokenGrant | string> => mint() });
    const net = createNodeTransport({ baseUrl: url, token: tokens.reader, httpVersion: "1.1" });
    const manifests = new ConnectManifests({ transport: net.transport, appId: APP });
    const events: EngineEvent[] = [];
    const engine = await Engine.create({
      appId: APP,
      principal: { issuer: this.issuer(issuer).url, sub: o.sub, deviceId },
      transport: net.transport,
      manifests,
      tokens,
      random: o.random ?? fastRandom,
      origin: url,
      ...(o.config ?? {}),
    });
    engine.on((e) => events.push(e));
    if (o.start !== false) engine.start();
    return {
      engine,
      events,
      tokens,
      net,
      node,
      url,
      ofType: <T extends EngineEvent["type"]>(type: T) => events.filter((e) => e.type === type) as Extract<EngineEvent, { type: T }>[],
      close: async () => {
        await engine.stop().catch(() => undefined);
        net.close();
      },
    };
  }

  /** Kills the process and deletes the run's directory. */
  async close(): Promise<void> {
    await this.cleanup();
  }

  /** Whether the process is still running. */
  get alive(): boolean {
    return this.child.exitCode === null && this.child.signalCode === null;
  }
}

/**
 * Builds the test binary (once for the run), starts a deployment of it on a configuration written for the test, waits for its ready file, and returns the harness's
 * view of it. The process is killed by `close()`, and at the exit of the test process.
 */
export async function startTestServer(o: TestServerOptions = {}): Promise<TestServer> {
  const bin = await testServerBinary();
  const dir = fs.mkdtempSync(path.join(TMP_DIR, "ts-run-"));
  const readyPath = path.join(dir, "ready.json");
  fs.writeFileSync(path.join(dir, "manifest.json"), JSON.stringify(o.manifest ?? MANIFEST));
  const profile: Record<string, unknown> = {
    nodes: o.nodes ?? 3,
    ...(o.clockStart !== undefined ? { clock: { mode: "manual", start: o.clockStart.toISOString().replace(/\.\d+Z$/, "Z") } } : {}),
    ...(o.background !== undefined ? { background: o.background } : {}),
    ...(o.profile ?? {}),
  };
  fs.writeFileSync(
    path.join(dir, "test.json"),
    JSON.stringify({
      development: true,
      fetch_timeout_seconds: 5,
      heartbeat_seconds: 5,
      gate_tick_ms: 100,
      apps: [
        {
          id: APP,
          manifest: "manifest.json",
          manifest_version: 1,
          issuers: [{ issuer_url: "test:user1" }, { issuer_url: "test:user2" }, { issuer_url: "test:admin", admin: true }],
        },
      ],
      test_profile: profile,
      ...(o.config ?? {}),
    }),
  );
  const child = spawn(bin, ["-config", path.join(dir, "test.json"), "-ready-file", readyPath], { cwd: dir, stdio: ["ignore", "ignore", "pipe"], windowsHide: true });
  running.add(child);
  let log = "";
  let exited: number | null | undefined;
  child.once("exit", (code) => {
    exited = code;
    running.delete(child);
  });
  child.stderr?.setEncoding("utf8");
  child.stderr?.on("data", (d: string) => {
    log += d;
  });
  const agent = new http.Agent({ keepAlive: false });
  const kill = async (): Promise<void> => {
    if (exited === undefined) {
      const done = new Promise<void>((r) => child.once("exit", () => r()));
      child.kill();
      await Promise.race([done, sleep(5000)]);
    }
    agent.destroy();
    fs.rmSync(dir, { recursive: true, force: true });
  };
  // ready when the ready file is there: the binary writes it whole (a temporary file that replaces it), when every node listens
  const deadline = Date.now() + 60_000;
  let ready: Ready | undefined;
  while (ready === undefined) {
    if (exited !== undefined) {
      await kill();
      throw new Error(`the test binary exited (${String(exited)}) before it was ready:\n${log}`);
    }
    if (fs.existsSync(readyPath)) {
      try {
        ready = JSON.parse(fs.readFileSync(readyPath, "utf8")) as Ready;
        break;
      } catch {
        // read again
      }
    }
    if (Date.now() > deadline) {
      await kill();
      throw new Error(`the test binary was not ready in time:\n${log}`);
    }
    await sleep(25);
  }
  return new TestServer(ready, child, () => log, kill, agent);
}

/** A node of the deployment as `GetDeployment` shows it. */
export function nodeOf(d: GetDeploymentResponse, name: string): GetDeploymentResponse["nodes"][number] {
  const n = d.nodes.find((x) => x.name === name);
  if (n === undefined) throw new Error(`the deployment has no node ${name}`);
  return n;
}
