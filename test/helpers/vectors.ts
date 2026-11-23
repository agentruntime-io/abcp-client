// Loading the conformance vectors (conformance/vectors/*.json, spec 17 [CONF-9]) and the compact forms of their README.

import { readFileSync } from "node:fs";
import * as path from "node:path";
import { fromHex } from "../../src/rules/bytes.js";
import { VECTORS_DIR } from "./server-dir.js";

export interface Vector {
  id: string;
  group: string;
  kind: string;
  title: string;
  spec: string[];
  // the rest is per kind
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [key: string]: any;
}

/** True when the vectors are there (`$ABCP_SERVER_DIR/conformance/vectors`); the suites that run them are skipped otherwise. */
export const HAVE_VECTORS = VECTORS_DIR !== undefined;

/** Reads the `vectors` array of one vector file; empty when there is no abcp-server checkout (see `HAVE_VECTORS`). */
export function loadVectors(file: string): Vector[] {
  if (VECTORS_DIR === undefined) return [];
  const data = JSON.parse(readFileSync(path.join(VECTORS_DIR, file), "utf8")) as { vectors: Vector[] };
  return data.vectors;
}

export const utf8 = (s: string): Uint8Array => new TextEncoder().encode(s);

/**
 * Expands the compact forms of a `bytes` value (README, "Large values"): `{"hex"}`, `{"fill": "<ASCII char>", "len": N}`,
 * and `{"parts": [ ... ]}`, the concatenation of such forms in order.
 */
export function expandBytes(v: unknown): Uint8Array {
  if (typeof v !== "object" || v === null) throw new Error(`not a bytes form: ${JSON.stringify(v)}`);
  const o = v as { hex?: string; fill?: string; len?: number; parts?: unknown[] };
  if (o.parts !== undefined) {
    const chunks = o.parts.map(expandBytes);
    let n = 0;
    for (const c of chunks) n += c.length;
    const out = new Uint8Array(n);
    let at = 0;
    for (const c of chunks) {
      out.set(c, at);
      at += c.length;
    }
    return out;
  }
  if (o.fill !== undefined && o.len !== undefined) {
    return new Uint8Array(o.len).fill(o.fill.charCodeAt(0));
  }
  if (o.hex !== undefined) return fromHex(o.hex);
  throw new Error(`not a bytes form: ${JSON.stringify(v)}`);
}
