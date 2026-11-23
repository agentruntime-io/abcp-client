// The Connect protocol (02 §2.9, [ARC-53], [ARC-55]) with the binary codec, as a `RawCaller`: the bytes of a request go out, the bytes of
// each response or stream message come back, and `BytesTransport` (engine/wire.ts) decodes them strictly (rules/canon, [ARC-61]). The
// HTTP is a `UniversalClientFn` of connect-es: connect-node's `http`/`http2` client in Node (net/node.ts), `fetch` in a browser (net/web.ts).
// The headers, the error JSON and the end-of-stream message are connect-es's own (`protocol-connect`); the framing of a stream is read here,
// with the bound of [ARC-59] on each message.
//
//  - unary: `POST <endpoint>/abcp.v2.SyncService/<Method>`, `application/proto`, `connect-protocol-version: 1`, `connect-timeout-ms` for the
//    call's deadline ([ARC-60]), `Authorization: Bearer <token>` ([ARC-56]). A status other than 200 carries the error as JSON, with the
//    ABCP detail in `details` ([ARC-57]).
//  - server-streaming (`Subscribe`): `application/connect+proto`, one request message in an envelope, then envelopes of messages and an
//    end-of-stream envelope (flag 2) that carries the status, and the detail, of a call that ended with one ([ARC-57]).
//
// A call never retries and never reconnects: the engine drives both, with the backoff of 09 [READ-57] and 14 [CLI-43]. A stream that ends
// without its end-of-stream envelope is a broken connection, `Unavailable`.

import { Code, ConnectError } from "@connectrpc/connect";
import { compressedFlag, type Compression, type UniversalClientFn } from "@connectrpc/connect/protocol";
import { endStreamFromJson, errorFromJsonBytes, requestHeaderWithCompression, trailerDemux, validateResponseWithCompression } from "@connectrpc/connect/protocol-connect";
import { ProtocolViolation, RpcError } from "../engine/errors.js";
import type { CallOptions } from "../engine/transport.js";
import type { RawCaller } from "../engine/wire.js";
import { toFailure, type Ending } from "./errors.js";

/** The flag of the end-of-stream envelope of the Connect protocol. */
const END_STREAM_FLAG = 2;
/** The largest message of a call, in the codec of its call (02 [ARC-59]); the bound on what a peer is read to send. */
export const MAX_MESSAGE_BYTES = 16 * 1024 * 1024;
/** The deadline of a unary call that has none of its own: longer than a GATED `Mutate` waits for its decision (08 [GATE-12]: at most 10 s). */
export const DEFAULT_UNARY_TIMEOUT_MS = 30_000;

export interface ConnectCallerOptions {
  /** The endpoint (02 [ARC-55]): the URL that `/abcp.v2.SyncService/<Method>` is appended to, such as `https://sync.example.com/apps/tracker`. */
  baseUrl: string;
  /** The HTTP client. */
  http: UniversalClientFn;
  /** The bearer token of the call that starts now (10 [AUTH-1]); read for each call, so that a token that was refreshed is used by the next one. */
  token?: () => string | undefined;
  /** The service whose methods are called; default `abcp.v2.SyncService`. */
  service?: string;
  /** The deadline of a unary call that sets none; default `DEFAULT_UNARY_TIMEOUT_MS`. 0 is none. */
  unaryTimeoutMs?: number;
  /** The largest message that is read; default `MAX_MESSAGE_BYTES`. */
  readMaxBytes?: number;
  /** The compressions that a response may use (02 [ARC-58]); gzip where the platform has it. A browser's `fetch` decodes a unary response itself. */
  acceptCompression?: Compression[];
  /** Whether to send `User-Agent` (a browser forbids it); default true. */
  userAgent?: boolean;
}

/** A call's deadline and abort, as one signal: which of the two ended it is `ending`. */
class CallScope {
  readonly controller = new AbortController();
  readonly ending: Ending = { deadline: false, aborted: false };
  private timer: ReturnType<typeof setTimeout> | undefined;
  private readonly relay: (() => void) | undefined;
  private readonly source: AbortSignal | undefined;

  constructor(signal: AbortSignal | undefined, timeoutMs: number | undefined) {
    this.source = signal;
    if (signal !== undefined) {
      if (signal.aborted) {
        this.ending.aborted = true;
        this.controller.abort();
      } else {
        this.relay = () => {
          this.ending.aborted = true;
          this.controller.abort();
        };
        signal.addEventListener("abort", this.relay, { once: true });
      }
    }
    if (timeoutMs !== undefined && timeoutMs > 0 && !this.controller.signal.aborted) {
      this.timer = setTimeout(() => {
        this.ending.deadline = true;
        this.controller.abort();
      }, timeoutMs);
    }
  }

  get signal(): AbortSignal {
    return this.controller.signal;
  }

  /** The call is over: the timer and the relay are not needed any more, and a connection that is still open is released. */
  close(abort: boolean): void {
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
    if (this.relay !== undefined) this.source?.removeEventListener("abort", this.relay);
    if (abort && !this.controller.signal.aborted) this.controller.abort();
  }
}

/** The octets that arrived and have not been used, read by the count. */
class ChunkQueue {
  private chunks: Uint8Array[] = [];
  length = 0;

  push(c: Uint8Array): void {
    if (c.length === 0) return;
    this.chunks.push(c);
    this.length += c.length;
  }

  /** The first n octets, which leave the queue (n is at most `length`). */
  take(n: number): Uint8Array {
    const out = new Uint8Array(n);
    let at = 0;
    while (at < n) {
      const head = this.chunks[0] as Uint8Array;
      const want = n - at;
      if (head.length <= want) {
        out.set(head, at);
        at += head.length;
        this.chunks.shift();
      } else {
        out.set(head.subarray(0, want), at);
        this.chunks[0] = head.subarray(want);
        at += want;
      }
    }
    this.length -= n;
    return out;
  }

  /** The first n octets, which stay (n is at most `length`). */
  peek(n: number): Uint8Array {
    const out = new Uint8Array(n);
    let at = 0;
    for (const c of this.chunks) {
      if (at >= n) break;
      const part = c.subarray(0, Math.min(c.length, n - at));
      out.set(part, at);
      at += part.length;
    }
    return out;
  }
}

async function readAll(body: AsyncIterable<Uint8Array>, max: number): Promise<Uint8Array> {
  const q = new ChunkQueue();
  for await (const c of body) {
    q.push(c);
    if (q.length > max) throw new ProtocolViolation("response", `a message over ${max} octets (02 [ARC-59])`);
  }
  return q.take(q.length);
}

async function* once(bytes: Uint8Array): AsyncGenerator<Uint8Array> {
  yield bytes;
}

/** One envelope of the Connect protocol for streams: a flags octet, a 4-octet big-endian length, the message. */
function envelope(flags: number, data: Uint8Array): Uint8Array {
  const out = new Uint8Array(5 + data.length);
  out[0] = flags;
  new DataView(out.buffer).setUint32(1, data.length, false);
  out.set(data, 5);
  return out;
}

async function* envelopes(body: AsyncIterable<Uint8Array>, max: number): AsyncGenerator<{ flags: number; data: Uint8Array }> {
  const q = new ChunkQueue();
  for await (const c of body) {
    q.push(c);
    for (;;) {
      if (q.length < 5) break;
      const h = q.peek(5);
      const size = new DataView(h.buffer, h.byteOffset, 5).getUint32(1, false);
      if (size > max) throw new ProtocolViolation("stream message", `a message of ${size} octets, over ${max} (02 [ARC-59])`);
      if (q.length < 5 + size) break;
      q.take(5);
      yield { flags: h[0] as number, data: q.take(size) };
    }
  }
  if (q.length > 0) throw new RpcError("Unavailable", "the stream ended inside a message");
}

export class ConnectCaller implements RawCaller {
  private readonly service: string;
  private readonly baseUrl: string;
  private readonly readMax: number;
  private readonly unaryTimeout: number;
  private readonly accept: Compression[];

  constructor(private readonly o: ConnectCallerOptions) {
    this.service = o.service ?? "abcp.v2.SyncService";
    this.baseUrl = o.baseUrl.replace(/\/+$/, "");
    this.readMax = o.readMaxBytes ?? MAX_MESSAGE_BYTES;
    this.unaryTimeout = o.unaryTimeoutMs ?? DEFAULT_UNARY_TIMEOUT_MS;
    this.accept = o.acceptCompression ?? [];
  }

  private url(method: string): string {
    return `${this.baseUrl}/${this.service}/${method}`;
  }

  private headers(kind: "unary" | "server_streaming", timeoutMs: number | undefined): Headers {
    const extra: Record<string, string> = {};
    const token = this.o.token?.();
    if (token !== undefined && token !== "") extra["authorization"] = `Bearer ${token}`;
    return requestHeaderWithCompression(kind, true, timeoutMs, extra, this.accept, null, this.o.userAgent ?? true);
  }

  async unary(method: string, request: Uint8Array, o?: CallOptions): Promise<Uint8Array> {
    const timeoutMs = o?.timeoutMs ?? this.unaryTimeout;
    const scope = new CallScope(o?.signal, timeoutMs);
    try {
      if (scope.signal.aborted) throw new RpcError("Cancelled", "the call was aborted before it began"); // it never goes out
      const res = await this.o.http({ url: this.url(method), method: "POST", header: this.headers("unary", timeoutMs > 0 ? timeoutMs : undefined), signal: scope.signal, body: once(request) });
      const { compression, isUnaryError, unaryError } = validateResponseWithCompression("unary", this.accept, true, res.status, res.header);
      const [header, trailer] = trailerDemux(res.header);
      let bytes = await readAll(res.body, this.readMax);
      if (compression !== undefined) bytes = await compression.decompress(bytes, this.readMax);
      if (isUnaryError) {
        // a status other than 200 whose body is a Connect error (02 [ARC-57]); one whose body is not falls back to the status of the HTTP code
        header.forEach((v, k) => trailer.append(k, v));
        throw errorFromJsonBytes(bytes, trailer, unaryError);
      }
      return bytes;
    } catch (e) {
      throw toFailure(e, scope.ending);
    } finally {
      scope.close(false);
    }
  }

  async *stream(method: string, request: Uint8Array, o?: CallOptions): AsyncGenerator<Uint8Array> {
    const scope = new CallScope(o?.signal, o?.timeoutMs);
    let ended = false;
    try {
      if (scope.signal.aborted) throw new RpcError("Cancelled", "the call was aborted before it began");
      const res = await this.o.http({
        url: this.url(method),
        method: "POST",
        header: this.headers("server_streaming", o?.timeoutMs),
        signal: scope.signal,
        body: once(envelope(0, request)),
      });
      if (res.status !== 200) {
        // a call that was refused before its stream began (the content type, the protocol version, a token): the error is a unary one
        const bytes = await readAll(res.body, this.readMax).catch(() => new Uint8Array(0));
        throw errorFromJsonBytes(bytes, res.header, new ConnectError(`HTTP ${res.status}`, httpCode(res.status), res.header));
      }
      const { compression } = validateResponseWithCompression("server_streaming", this.accept, true, res.status, res.header);
      for await (const env of envelopes(res.body, this.readMax)) {
        let data = env.data;
        if ((env.flags & compressedFlag) !== 0) {
          if (compression === undefined) throw new ProtocolViolation("stream message", "a compressed message with no encoding agreed");
          data = await compression.decompress(data, this.readMax);
        }
        if ((env.flags & END_STREAM_FLAG) !== 0) {
          if (ended) throw new ProtocolViolation("stream", "a second end-of-stream message");
          ended = true;
          const end = endStreamFromJson(data);
          if (end.error !== undefined) throw end.error;
          continue;
        }
        if (ended) throw new ProtocolViolation("stream", "a message after the end of the stream");
        yield data;
      }
      if (!ended) throw new RpcError("Unavailable", "the stream ended without its end-of-stream message");
    } catch (e) {
      throw toFailure(e, scope.ending);
    } finally {
      // the stream was left before its end (the caller stopped reading), or ended: the connection of the call is released either way
      scope.close(true);
    }
  }
}

/** The status of an HTTP status for a call that was refused before it began (the Connect protocol's table). */
function httpCode(status: number): Code {
  switch (status) {
    case 400:
      return Code.InvalidArgument;
    case 401:
      return Code.Unauthenticated;
    case 403:
      return Code.PermissionDenied;
    case 404:
      return Code.Unimplemented;
    case 408:
      return Code.DeadlineExceeded;
    case 429:
    case 502:
    case 503:
    case 504:
      return Code.Unavailable;
    default:
      return Code.Unknown;
  }
}
