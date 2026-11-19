// The tests that need a checkout of the (private) abpc-server repo: the conformance vectors, the CRDT test data and the Go server that the
// integration tests build. The checkout is named by the environment variable ABPC_SERVER_DIR (a path, relative to the current directory
// or absolute). Without it, or when the path is no directory, those tests skip: they run in the server's release checks.

import { existsSync, statSync } from "node:fs";
import * as path from "node:path";

export const SKIP_NEEDS_SERVER = "needs an abpc-server checkout (ABPC_SERVER_DIR); runs in the server's release checks";

function resolveServerDir(): string | undefined {
  const raw = process.env["ABPC_SERVER_DIR"];
  if (raw === undefined || raw.trim() === "") return undefined;
  const dir = path.resolve(raw);
  try {
    return statSync(dir).isDirectory() ? dir : undefined;
  } catch {
    return undefined;
  }
}

/** The abpc-server checkout, absolute, or undefined when ABPC_SERVER_DIR is unset or names no directory. */
export const SERVER_DIR: string | undefined = resolveServerDir();

/** `$ABPC_SERVER_DIR/conformance/vectors`, or undefined when that folder is not there. */
export const VECTORS_DIR: string | undefined = SERVER_DIR !== undefined && existsSync(path.join(SERVER_DIR, "conformance", "vectors")) ? path.join(SERVER_DIR, "conformance", "vectors") : undefined;

/** `$ABPC_SERVER_DIR/server/internal/crdt/testdata`, or undefined when that folder is not there. */
export const CRDT_TESTDATA_DIR: string | undefined =
  SERVER_DIR !== undefined && existsSync(path.join(SERVER_DIR, "server", "internal", "crdt", "testdata")) ? path.join(SERVER_DIR, "server", "internal", "crdt", "testdata") : undefined;

/** `$ABPC_SERVER_DIR/server`, the Go server's source (the integration harness builds it), or undefined when that folder is not there. */
export const GO_SERVER_SRC: string | undefined = SERVER_DIR !== undefined && existsSync(path.join(SERVER_DIR, "server", "cmd")) ? path.join(SERVER_DIR, "server") : undefined;
