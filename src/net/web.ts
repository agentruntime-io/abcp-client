// The transport for a browser, and for any platform with the standard `fetch` (Deno, Bun, a worker): Connect over `fetch`, the shape of
// connect-web, with the binary codec (02 [ARC-53]). It uses nothing but `fetch`, `Headers` and `ReadableStream`, so Node's own `fetch` runs it,
// which is how it is tested.
//
// What a browser does that the Node transport does not:
//  - it sets `Accept-Encoding` itself and decodes a compressed unary response before the page sees it (a script may not set the header),
//    so this transport asks for no compression and drops the `Content-Encoding` and `Content-Length` of a response that `fetch` has decoded;
//  - it sends the whole request body at once (a request body that streams needs HTTP/2 and `duplex: "half"`); a `Subscribe` request is one message;
//  - it reads the response as a stream (`Response.body`), which every current browser does;
//  - it may not set `User-Agent`.
// Over HTTP/1.1 a browser opens at most six connections to an origin and the `Subscribe` stream holds one: a deployment serves browsers over HTTP/2.

import type { UniversalClientFn } from "@connectrpc/connect/protocol";
import type { BytesTransport } from "../engine/wire.js";
import { createBytesTransport } from "./transport.js";

type FetchFn = (url: string, init: { method: string; headers: Headers; body?: Uint8Array; signal?: AbortSignal; cache?: "no-store"; credentials?: "omit" }) => Promise<{
  status: number;
  headers: Headers;
  body: ReadableStream<Uint8Array> | null;
}>;

/** An HTTP client of connect-es over `fetch` (the request body is read whole, the response body is streamed). */
export function fetchHttpClient(fetchFn?: FetchFn): UniversalClientFn {
  const f: FetchFn = fetchFn ?? (globalThis.fetch as unknown as FetchFn);
  return async (req) => {
    let body: Uint8Array | undefined;
    if (req.body !== undefined) {
      const parts: Uint8Array[] = [];
      let n = 0;
      for await (const c of req.body) {
        parts.push(c);
        n += c.length;
      }
      body = new Uint8Array(n);
      let at = 0;
      for (const p of parts) {
        body.set(p, at);
        at += p.length;
      }
    }
    const res = await f(req.url, {
      method: req.method,
      headers: req.header,
      ...(body !== undefined ? { body } : {}),
      ...(req.signal !== undefined ? { signal: req.signal } : {}),
      cache: "no-store",
      credentials: "omit", // the token is a bearer token in a header: no cookie is sent or kept
    });
    // `fetch` has decoded a compressed body, so the headers that describe the compressed one no longer fit
    const header = new Headers();
    res.headers.forEach((v, k) => {
      if (k !== "content-encoding" && k !== "content-length") header.append(k, v);
    });
    return { status: res.status, header, body: iterate(res.body), trailer: new Headers() };
  };
}

function iterate(body: ReadableStream<Uint8Array> | null): AsyncIterable<Uint8Array> {
  return {
    [Symbol.asyncIterator]() {
      const reader = body?.getReader();
      return {
        async next(): Promise<IteratorResult<Uint8Array>> {
          if (reader === undefined) return { done: true, value: undefined };
          const r = await reader.read();
          return r.done ? { done: true, value: undefined } : { done: false, value: r.value };
        },
        async return(): Promise<IteratorResult<Uint8Array>> {
          await reader?.cancel().catch(() => undefined);
          return { done: true, value: undefined };
        },
      };
    },
  };
}

export interface WebTransportOptions {
  /** The endpoint (02 [ARC-55]). A browser needs the deployment to allow its origin (16 [SEC-39]). */
  baseUrl: string;
  /** The bearer token of the call that starts now (a `TokenStore`'s `reader`). */
  token?: () => string | undefined;
  /** The `fetch` to use; default the global one. */
  fetch?: FetchFn;
  /** The deadline of a unary call (02 [ARC-60]); default 30 s. */
  unaryTimeoutMs?: number;
}

export function createWebTransport(o: WebTransportOptions): BytesTransport {
  return createBytesTransport({
    baseUrl: o.baseUrl,
    http: fetchHttpClient(o.fetch),
    ...(o.token !== undefined ? { token: o.token } : {}),
    ...(o.unaryTimeoutMs !== undefined ? { unaryTimeoutMs: o.unaryTimeoutMs } : {}),
    acceptCompression: [],
    userAgent: false,
  });
}
