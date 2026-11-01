// The transport for Node: Connect over connect-node's `http`/`https` (HTTP/1.1) or `http2` client, with the binary codec and gzip
// (02 [ARC-53], [ARC-58]). One connection serves every call over HTTP/2, the `Subscribe` stream and the unary calls beside it, and a
// connection that breaks is made again by the next call; the engine's backoff (09 [READ-57]) decides when that is.

import * as http from "node:http";
import * as https from "node:https";
import type * as http2 from "node:http2";
import { compressionGzip, createNodeHttpClient, Http2SessionManager, type Http2SessionOptions } from "@connectrpc/connect-node";
import type { BytesTransport } from "../engine/wire.js";
import { createBytesTransport } from "./transport.js";

export interface NodeTransportOptions {
  /** The endpoint (02 [ARC-55]): `https://sync.example.com` or `https://sync.example.com/apps/tracker`; `http://` only in development (16 [SEC-45]). */
  baseUrl: string;
  /** The bearer token of the call that starts now (a `TokenStore`'s `reader`). */
  token?: () => string | undefined;
  /** `2` is HTTP/2 (ALPN for `https`, prior knowledge for `http`), `1.1` is HTTP/1.1. Default: `2` for `https`, `1.1` for `http`. */
  httpVersion?: "1.1" | "2";
  /** The trust roots and the other options of the TLS connection (`ca`, `rejectUnauthorized`, a client certificate). */
  tls?: { ca?: string | Buffer | (string | Buffer)[]; rejectUnauthorized?: boolean; servername?: string };
  /** The deadline of a unary call (02 [ARC-60]); default 30 s. */
  unaryTimeoutMs?: number;
  /** Keep-alive pings of an HTTP/2 connection (the options of connect-node's `Http2SessionManager`). */
  http2?: Http2SessionOptions;
}

/** A transport and the connections it holds. */
export interface NodeTransport {
  readonly transport: BytesTransport;
  /** Closes the connections: the streams that are open end with an error, and the process is free to exit. */
  close(): void;
}

export function createNodeTransport(o: NodeTransportOptions): NodeTransport {
  const secure = new URL(o.baseUrl).protocol === "https:";
  const version = o.httpVersion ?? (secure ? "2" : "1.1");
  const closers: (() => void)[] = [];
  let httpClient;
  if (version === "1.1") {
    const agent = secure ? new https.Agent({ keepAlive: true, ...(o.tls ?? {}) }) : new http.Agent({ keepAlive: true });
    closers.push(() => agent.destroy());
    httpClient = createNodeHttpClient({ httpVersion: "1.1", nodeOptions: { agent } });
  } else {
    // one session manager for each origin; it connects again when its connection has closed
    const sessions = new Map<string, Http2SessionManager>();
    const nodeOptions: http2.SecureClientSessionOptions = { ...(o.tls ?? {}) };
    httpClient = createNodeHttpClient({
      httpVersion: "2",
      sessionProvider: (url) => {
        const origin = new URL(url).origin;
        let s = sessions.get(origin);
        if (s === undefined) {
          s = new Http2SessionManager(origin, o.http2, nodeOptions);
          sessions.set(origin, s);
        }
        return s;
      },
    });
    closers.push(() => {
      for (const s of sessions.values()) s.abort(new Error("the transport was closed"));
      sessions.clear();
    });
  }
  const transport = createBytesTransport({
    baseUrl: o.baseUrl,
    http: httpClient,
    ...(o.token !== undefined ? { token: o.token } : {}),
    ...(o.unaryTimeoutMs !== undefined ? { unaryTimeoutMs: o.unaryTimeoutMs } : {}),
    acceptCompression: [compressionGzip],
    userAgent: true,
  });
  return { transport, close: () => closers.forEach((c) => c()) };
}
