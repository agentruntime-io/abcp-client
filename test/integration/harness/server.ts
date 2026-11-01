// The Go server as a process: built once from `server/cmd/abpc-server` into `client/.tmp/` (which keeps its own .gitignore, so that the
// repository's is not touched), started on a loopback port in development mode with a configuration written for the test, killed at the end.
// The binary is deleted by the global teardown (`../global-setup.ts`).
//
// Without a Go toolchain the integration tests are skipped with a message (`goAvailable`, `SKIP_REASON`).

import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import * as fs from "node:fs";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import type { IssuerServer } from "./issuer.js";

const here = path.dirname(fileURLToPath(import.meta.url));
export const CLIENT_DIR = path.resolve(here, "../../..");
export const SERVER_DIR = path.resolve(CLIENT_DIR, "../server");
export const TMP_DIR = path.join(CLIENT_DIR, ".tmp");
const BINARY = path.join(TMP_DIR, os.platform() === "win32" ? "abpc-server.exe" : "abpc-server");
const LOCK = path.join(TMP_DIR, "build.lock");

function hasGo(): boolean {
  if (process.env["ABPC_SKIP_INTEGRATION"] === "1") return false;
  try {
    return spawnSync("go", ["version"], { encoding: "utf8" }).status === 0 && fs.existsSync(path.join(SERVER_DIR, "cmd", "abpc-server"));
  } catch {
    return false;
  }
}

export const goAvailable = hasGo();
export const SKIP_REASON = "integration tests against the Go server are skipped: no Go toolchain was found on the PATH (or ABPC_SKIP_INTEGRATION=1)";

/** Makes `client/.tmp/`, with a .gitignore that ignores everything in it. */
export function ensureTmp(): void {
  fs.mkdirSync(TMP_DIR, { recursive: true });
  const ignore = path.join(TMP_DIR, ".gitignore");
  if (!fs.existsSync(ignore)) fs.writeFileSync(ignore, "*\n");
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** Which source the binary of this run was built from (`worktree` or `head`), for the report of a run. */
export let builtFrom: "worktree" | "head" | undefined;

function goBuild(cwd: string): { ok: boolean; output: string } {
  const r = spawnSync("go", ["build", "-o", BINARY, "./cmd/abpc-server"], { cwd, encoding: "utf8", timeout: 5 * 60_000 });
  return { ok: r.status === 0, output: `${r.stdout}
${r.stderr}` };
}

/**
 * Builds the server from the committed tree (`git archive HEAD server`, extracted under `client/.tmp/`), for when the working tree does not
 * build: the server is under work by someone else, and the client's tests need a server that is whole. Read-only on the repository.
 */
function buildFromHead(): { ok: boolean; output: string } {
  const dir = path.join(TMP_DIR, "head-src");
  fs.rmSync(dir, { recursive: true, force: true });
  fs.mkdirSync(dir, { recursive: true });
  const repo = path.resolve(SERVER_DIR, "..");
  const tarball = spawnSync("git", ["-C", repo, "archive", "--format=tar", "HEAD", "server"], { maxBuffer: 256 << 20 });
  if (tarball.status !== 0) return { ok: false, output: `git archive failed: ${tarball.stderr}` };
  try {
    // tar runs in the target directory: GNU tar on Windows reads a `C:\…` argument as a remote host.
    const x = spawnSync("tar", ["-x"], { cwd: dir, input: tarball.stdout, maxBuffer: 1 << 20 });
    if (x.status !== 0) return { ok: false, output: `tar failed: ${x.stderr}` };
    return goBuild(path.join(dir, "server"));
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

/** How long, and how often, the build of a working tree that does not build at the moment is tried again (the server is under work by someone else). */
const BUILD_RETRY_MS = 20_000;
const BUILD_PATIENCE_MS = 8 * 60_000;

/**
 * The path of the server binary, built on the first call of the run: the test files run in parallel, so the build is under a lock that the
 * others wait on. The working tree is built. When it does not build at the moment (another builder is in the middle of an edit of the server),
 * the build is tried again every 20 s for up to 8 minutes, and then fails with the compiler's words: the server is never edited from here, and
 * the committed tree is built only when ABPC_SERVER_SOURCE=head asks for it.
 */
export async function serverBinary(): Promise<string> {
  ensureTmp();
  const deadline = Date.now() + BUILD_PATIENCE_MS + 4 * 60_000;
  for (;;) {
    if (fs.existsSync(BINARY) && !fs.existsSync(LOCK)) return BINARY;
    let mine = false;
    try {
      fs.mkdirSync(LOCK);
      mine = true;
    } catch {
      // another worker builds; a lock that has not been touched for as long as a build may take is left by a run that was killed
      try {
        if (Date.now() - fs.statSync(LOCK).mtimeMs > BUILD_RETRY_MS * 3 + 5 * 60_000) fs.rmSync(LOCK, { recursive: true, force: true });
      } catch {
        // gone meanwhile
      }
    }
    if (mine) {
      try {
        if (!fs.existsSync(BINARY)) {
          if (process.env["ABPC_SERVER_SOURCE"] === "head") {
            const r = buildFromHead();
            if (!r.ok) throw new Error(`go build of the server (git HEAD) failed:
${r.output}`);
            builtFrom = "head";
          } else {
            const give = Date.now() + BUILD_PATIENCE_MS;
            for (let attempt = 1; ; attempt++) {
              const r = goBuild(SERVER_DIR);
              if (r.ok) break;
              if (Date.now() + BUILD_RETRY_MS > give) throw new Error(`go build of the server failed after ${attempt} attempts:
${r.output}`);
              console.warn(`[integration] the server of the working tree does not build (attempt ${attempt}); trying again in ${BUILD_RETRY_MS / 1000} s:
${r.output.slice(0, 600)}`);
              fs.utimesSync(LOCK, new Date(), new Date()); // the lock is alive
              await sleep(BUILD_RETRY_MS);
            }
            builtFrom = "worktree";
          }
        }
        return BINARY;
      } finally {
        fs.rmSync(LOCK, { recursive: true, force: true });
      }
    }
    if (Date.now() > deadline) throw new Error("timed out waiting for the server binary to be built");
    await sleep(200);
  }
}

/** Deletes what the build left in `client/.tmp/` (the global teardown). */
export function removeBinary(): void {
  fs.rmSync(BINARY, { force: true });
  fs.rmSync(LOCK, { recursive: true, force: true });
}

// ---- the manifest of the test app --------------------------------------------------------------------------------

export const APP = "tracker";

/**
 * SEQUENCED (`Set`, `Bump`, `DocEdit`), GATED decided by the built-in `always_commit` gate (`Approve`, 08 [GATE-86]), a counter, a CRDT key.
 */
export const MANIFEST = {
  app: APP,
  keyspace_kinds: {
    team: { keys: ["note/**", "count/**", "doc/**", "task/**"], op_types: ["Set", "Bump", "DocEdit", "Approve"], retention_class: "standard" },
  },
  keys: {
    "note/{id}": { merge: "lww", value_schema: { type: "string", max_len: 300 } },
    "count/{id}": { merge: "counter", value_schema: { type: "int", min: 0, max: 1000000 } },
    "doc/{id}": { merge: "crdt:yjs/v1", crdt_roots: { t: "text" } },
    "task/{id}": { merge: "lww", value_schema: { type: "string", max_len: 300 } },
  },
  op_types: {
    Set: { dispatch: "SEQUENCED", writes: ["note/{id}"] },
    Bump: { dispatch: "SEQUENCED", writes: ["count/{id}"] },
    DocEdit: { dispatch: "SEQUENCED", writes: ["doc/{id}"] },
    Approve: { dispatch: "GATED", gate: { builtin: "always_commit" }, decision_deadline: "1h", writes: ["task/{id}"] },
  },
};

// ---- the process -------------------------------------------------------------------------------------------------

const running = new Set<ChildProcess>();
process.once("exit", () => {
  for (const c of running) c.kill();
});

export interface ServerOptions {
  issuers: IssuerServer;
  /** The listen address; default a free loopback port. */
  port?: number;
  manifest?: unknown;
  /** Further members of the configuration file. */
  config?: Record<string, unknown>;
}

export interface ServerProcess {
  readonly port: number;
  readonly url: string;
  /** The text the server wrote to its log (stderr). */
  log(): string;
  /** Kills the process, as a crash does. */
  kill(): Promise<void>;
}

export async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const s = net.createServer();
    s.once("error", reject);
    s.listen(0, "127.0.0.1", () => {
      const port = (s.address() as net.AddressInfo).port;
      s.close(() => resolve(port));
    });
  });
}

export async function startServer(o: ServerOptions): Promise<ServerProcess> {
  const bin = await serverBinary();
  const dir = fs.mkdtempSync(path.join(TMP_DIR, "run-"));
  fs.writeFileSync(path.join(dir, "manifest.json"), JSON.stringify(o.manifest ?? MANIFEST));
  fs.writeFileSync(path.join(dir, "ca.pem"), o.issuers.certPem);
  const port = o.port ?? (await freePort());
  fs.writeFileSync(
    path.join(dir, "server.json"),
    JSON.stringify({
      development: true,
      listen: `127.0.0.1:${port}`,
      fetch_ca_file: "ca.pem",
      fetch_allow_hosts: ["127.0.0.1"],
      fetch_timeout_seconds: 5,
      heartbeat_seconds: 5,
      gate_tick_ms: 100,
      features: [],
      apps: [
        {
          id: APP,
          manifest: "manifest.json",
          issuers: [
            { issuer_url: o.issuers.user.url, algorithms: ["ES256"] },
            { issuer_url: o.issuers.admin.url, admin: true, algorithms: ["ES256"] },
          ],
        },
      ],
      ...(o.config ?? {}),
    }),
  );
  const child = spawn(bin, ["-config", path.join(dir, "server.json")], { cwd: dir, stdio: ["ignore", "ignore", "pipe"], windowsHide: true });
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
  // ready when the server says it listens, or the port accepts a connection
  const deadline = Date.now() + 20_000;
  for (;;) {
    if (exited !== undefined) throw new Error(`the server exited (${exited}) before it listened:\n${log}`);
    if (log.includes("abpc-server listening")) break;
    if (Date.now() > deadline) {
      child.kill();
      throw new Error(`the server did not start in time:\n${log}`);
    }
    await sleep(25);
  }
  const kill = async (): Promise<void> => {
    if (exited === undefined) {
      const done = new Promise<void>((r) => child.once("exit", () => r()));
      child.kill();
      await Promise.race([done, sleep(5000)]);
    }
    fs.rmSync(dir, { recursive: true, force: true });
  };
  return { port, url: `http://127.0.0.1:${port}`, log: () => log, kill };
}

// ---- a TCP proxy that a test can cut --------------------------------------------------------------------------------

/**
 * A loopback TCP proxy to the server: the clients connect to it, so that a test can cut the network (`down`: every connection is
 * destroyed and new ones are refused at once), bring it back (`up`), or point it at a server that was started again on another port
 * (`target`). It moves bytes and nothing else, so HTTP/1.1 and HTTP/2 pass through.
 */
export class Proxy {
  private readonly server: net.Server;
  private readonly sockets = new Set<net.Socket>();
  private upstream: number;
  private isDown = false;

  private constructor(server: net.Server, upstream: number) {
    this.server = server;
    this.upstream = upstream;
  }

  static async start(upstream: number): Promise<Proxy> {
    let proxy: Proxy | undefined;
    const server = net.createServer((client) => proxy?.accept(client));
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    proxy = new Proxy(server, upstream);
    return proxy;
  }

  get port(): number {
    return (this.server.address() as net.AddressInfo).port;
  }

  get url(): string {
    return `http://127.0.0.1:${this.port}`;
  }

  private accept(client: net.Socket): void {
    if (this.isDown) {
      client.destroy();
      return;
    }
    const up = net.connect(this.upstream, "127.0.0.1");
    for (const s of [client, up]) {
      this.sockets.add(s);
      s.once("close", () => this.sockets.delete(s));
      s.on("error", () => s.destroy());
    }
    client.on("close", () => up.destroy());
    up.on("close", () => client.destroy());
    client.pipe(up);
    up.pipe(client);
  }

  /** The network goes away: every open connection is destroyed, and a new one is refused at once. */
  down(): void {
    this.isDown = true;
    for (const s of [...this.sockets]) s.destroy();
  }

  up(): void {
    this.isDown = false;
  }

  /** Points the proxy at a server on another port; the open connections are cut, as a restarted server cuts them. */
  target(port: number): void {
    this.upstream = port;
    for (const s of [...this.sockets]) s.destroy();
  }

  async close(): Promise<void> {
    this.down();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}
