// C5: the Connect transport (src/net) against a scripted HTTP client: what goes out (02 [ARC-53], [ARC-55], [ARC-56], [ARC-60]), what a response and a stream
// become (strict decoding, 02 [ARC-61]; the error and its one ABCP detail, [ARC-57]; A.7), and how a call ends (deadline, abort, a connection that breaks).

import { create, type DescMessage, type Message } from "@bufbuild/protobuf";
import { TimestampSchema } from "@bufbuild/protobuf/wkt";
import type { UniversalClientFn, UniversalClientRequest, UniversalClientResponse } from "@connectrpc/connect/protocol";
import { describe, expect, it } from "vitest";
import { BytesTransport, encodeMessage, ProtocolViolation, RpcError } from "../src/engine/index.js";
import {
  GetManifestRequestSchema,
  GetManifestResponseSchema,
  HeartbeatFrameSchema,
  SubscribeRequestSchema,
  SubscribeResponseSchema,
  type SubscribeResponse,
} from "../src/gen/abcp/v2/sync_pb.js";
import { Code, ErrorClass, RejectSchema, RetryReason, RetrySchema } from "../src/gen/abcp/v2/types_pb.js";
import { ConnectCaller, createBytesTransport, MAX_MESSAGE_BYTES } from "../src/net/index.js";
import { canon } from "../src/rules/index.js";

const enc = new TextEncoder();
const bytes = (schema: DescMessage, m: Message): Uint8Array => encodeMessage(schema, m);
const manifestResponse = (version = 1): Uint8Array => bytes(GetManifestResponseSchema, create(GetManifestResponseSchema, { version, encoding: "json/v1", manifest: enc.encode("{}") }));
const b64 = (b: Uint8Array): string => Buffer.from(b).toString("base64").replace(/=+$/, "");

/** A response whose body is the chunks, and that records whether the reader left it before its end. */
function response(status: number, header: Record<string, string>, chunks: Uint8Array[], gate?: { release: Promise<void>; signal?: AbortSignal }): UniversalClientResponse & { left: () => boolean } {
  let left = false;
  async function* body(): AsyncGenerator<Uint8Array> {
    try {
      for (const c of chunks) {
        yield c;
      }
      if (gate !== undefined) {
        // a body that has no more to give waits, and ends with an error when the request is aborted, as every HTTP client's does
        const abortError = (): Error => Object.assign(new Error("aborted"), { name: "AbortError" });
        if (gate.signal?.aborted === true) throw abortError();
        const aborted = new Promise<never>((_r, reject) => gate.signal?.addEventListener("abort", () => reject(abortError()), { once: true }));
        await Promise.race([gate.release, aborted]);
      }
    } finally {
      left = true;
    }
  }
  return { status, header: new Headers(header), body: body(), trailer: new Headers(), left: () => left };
}

const unaryOk = (body: Uint8Array): UniversalClientResponse => response(200, { "content-type": "application/proto" }, [body]);
const streamHeader = { "content-type": "application/connect+proto" };
function frame(flags: number, data: Uint8Array): Uint8Array {
  const out = new Uint8Array(5 + data.length);
  out[0] = flags;
  new DataView(out.buffer).setUint32(1, data.length, false);
  out.set(data, 5);
  return out;
}
const heartbeat = (ms: number): SubscribeResponse =>
  create(SubscribeResponseSchema, { frame: { case: "heartbeat", value: create(HeartbeatFrameSchema, { serverTime: create(TimestampSchema, { seconds: BigInt(ms) }) }) } });
const endStream = (error?: object, metadata?: object): Uint8Array => frame(2, enc.encode(JSON.stringify({ ...(error !== undefined ? { error } : {}), ...(metadata !== undefined ? { metadata } : {}) })));

function transport(http: UniversalClientFn, o: { token?: () => string | undefined; unaryTimeoutMs?: number; baseUrl?: string } = {}): BytesTransport {
  return createBytesTransport({ baseUrl: o.baseUrl ?? "https://sync.test/apps/tracker/", http, ...(o.token !== undefined ? { token: o.token } : {}), ...(o.unaryTimeoutMs !== undefined ? { unaryTimeoutMs: o.unaryTimeoutMs } : {}) });
}

describe("the request (02 [ARC-53], [ARC-55], [ARC-56], [ARC-60])", () => {
  it("is a Connect POST with the binary codec, the protocol version, the deadline and the bearer token, at the endpoint's path", async () => {
    const seen: UniversalClientRequest[] = [];
    const bodies: Uint8Array[] = [];
    const t = transport(
      async (req) => {
        seen.push(req);
        for await (const c of req.body ?? []) bodies.push(c);
        return unaryOk(manifestResponse());
      },
      { token: () => "tok-1" },
    );
    await t.getManifest(create(GetManifestRequestSchema, { version: 3 }), { timeoutMs: 1234 });
    expect(seen).toHaveLength(1);
    const r = seen[0] as UniversalClientRequest;
    expect(r.url).toBe("https://sync.test/apps/tracker/abcp.v2.SyncService/GetManifest"); // the final `/` of the endpoint is dropped
    expect(r.method).toBe("POST");
    expect(r.header.get("content-type")).toBe("application/proto");
    expect(r.header.get("connect-protocol-version")).toBe("1");
    expect(r.header.get("connect-timeout-ms")).toBe("1234");
    expect(r.header.get("authorization")).toBe("Bearer tok-1");
    expect(Buffer.concat(bodies)).toEqual(Buffer.from(bytes(GetManifestRequestSchema, create(GetManifestRequestSchema, { version: 3 }))));
  });

  it("reads the token for each call, sends none when there is none, and gives a unary call the default deadline", async () => {
    let token: string | undefined;
    const auths: (string | null)[] = [];
    const timeouts: (string | null)[] = [];
    const t = transport(
      async (req) => {
        auths.push(req.header.get("authorization"));
        timeouts.push(req.header.get("connect-timeout-ms"));
        return unaryOk(manifestResponse());
      },
      { token: () => token },
    );
    await t.getManifest(create(GetManifestRequestSchema, {}));
    token = "fresh";
    await t.getManifest(create(GetManifestRequestSchema, {}));
    expect(auths).toEqual([null, "Bearer fresh"]);
    expect(timeouts).toEqual(["30000", "30000"]);
  });

  it("opens a stream as application/connect+proto with the one request in an envelope and no deadline", async () => {
    let got: UniversalClientRequest | undefined;
    const sent: Uint8Array[] = [];
    const t = transport(async (req) => {
      got = req;
      for await (const c of req.body ?? []) sent.push(c);
      return response(200, streamHeader, [endStream()]);
    });
    const req = create(SubscribeRequestSchema, { clientId: "c1" });
    for await (const _ of t.subscribe(req)) void _;
    expect(got?.url).toBe("https://sync.test/apps/tracker/abcp.v2.SyncService/Subscribe");
    expect(got?.header.get("content-type")).toBe("application/connect+proto");
    expect(got?.header.get("connect-timeout-ms")).toBeNull();
    expect(Buffer.concat(sent)).toEqual(Buffer.from(frame(0, bytes(SubscribeRequestSchema, req))));
  });
});

describe("strict decoding of what the server sends (02 [ARC-61], rules/canon, never protobuf-es fromBinary alone)", () => {
  it("refuses a string that is not valid UTF-8 in a response, where fromBinary would repair it", async () => {
    // field 3 (`encoding`), a string, holding the two octets ff fe
    const bad = Uint8Array.from([0x1a, 0x02, 0xff, 0xfe]);
    await expect(transport(async () => unaryOk(bad)).getManifest(create(GetManifestRequestSchema, {}))).rejects.toMatchObject({ name: "ProtocolViolation", messageType: "GetManifestResponse" });
    expect(() => canon.unmarshal(GetManifestResponseSchema, bad)).toThrow(/UTF-8/i);
  });

  it("refuses an encoding that is not the shortest form (07 [MUT-45]), which fromBinary reads without a word", async () => {
    // field 1 (`version`, a varint) holding 1 in two octets, 81 00
    const bad = Uint8Array.from([0x08, 0x81, 0x00]);
    await expect(transport(async () => unaryOk(bad)).getManifest(create(GetManifestRequestSchema, {}))).rejects.toBeInstanceOf(ProtocolViolation);
  });

  it("refuses the same in a frame of a stream, after the frames before it were delivered", async () => {
    const good = bytes(SubscribeResponseSchema, heartbeat(1));
    const bad = Uint8Array.from([0x1a, 0x02, 0xff, 0xfe]); // SubscribeResponse field 3, a message (`control`), with bytes that are no message
    const t = transport(async () => response(200, streamHeader, [frame(0, good), frame(0, bad), endStream()]));
    const seen: SubscribeResponse[] = [];
    await expect(
      (async () => {
        for await (const f of t.subscribe(create(SubscribeRequestSchema, {}))) seen.push(f);
      })(),
    ).rejects.toBeInstanceOf(ProtocolViolation);
    expect(seen).toHaveLength(1);
  });

  it("refuses a response over 16 MiB and a stream message over it (02 [ARC-59])", async () => {
    const t = transport(async () => response(200, { "content-type": "application/proto" }, [new Uint8Array(MAX_MESSAGE_BYTES + 1)]));
    await expect(t.getManifest(create(GetManifestRequestSchema, {}))).rejects.toBeInstanceOf(ProtocolViolation);
    const header = new Uint8Array(5);
    new DataView(header.buffer).setUint32(1, MAX_MESSAGE_BYTES + 1, false);
    const s = transport(async () => response(200, streamHeader, [header]));
    await expect(
      (async () => {
        for await (const f of s.subscribe(create(SubscribeRequestSchema, {}))) void f;
      })(),
    ).rejects.toBeInstanceOf(ProtocolViolation);
  });
});

describe("a call that fails (02 [ARC-57], A.7)", () => {
  const rejectDetail = (code: Code, extra: { message?: string } = {}): { type: string; value: string } => ({
    type: "abcp.v2.Reject",
    value: b64(bytes(RejectSchema, create(RejectSchema, { code, errorClass: ErrorClass.DEAD_LETTER, message: extra.message ?? "", details: { cause: "SUBJECT_REVOKED" } }))),
  });
  const failing = (status: number, body: object | string, header: Record<string, string> = { "content-type": "application/json" }): UniversalClientFn =>
    async () => response(status, header, [enc.encode(typeof body === "string" ? body : JSON.stringify(body))]);

  it("is an RpcError with the status and the Reject that is its one detail", async () => {
    const t = transport(failing(401, { code: "unauthenticated", message: "no", details: [rejectDetail(Code.PERMISSION_DENIED)] }));
    const e = (await t.getManifest(create(GetManifestRequestSchema, {})).catch((x: unknown) => x)) as RpcError;
    expect(e).toBeInstanceOf(RpcError);
    expect(e.status).toBe("Unauthenticated");
    expect(e.reject?.code).toBe(Code.PERMISSION_DENIED);
    expect(e.reject?.details["cause"]).toBe("SUBJECT_REVOKED");
    expect(e.retry).toBeUndefined();
  });

  it("carries a Retry with its reason and delay, takes the type URL with its prefix, and ignores a detail of another type", async () => {
    const retry = { type: "type.googleapis.com/abcp.v2.Retry", value: b64(bytes(RetrySchema, create(RetrySchema, { reason: RetryReason.RATE_LIMITED, afterMs: 5000 }))) };
    const other = { type: "google.rpc.ErrorInfo", value: b64(enc.encode("whatever")) };
    const t = transport(failing(429, { code: "resource_exhausted", message: "slow down", details: [other, retry] }));
    const e = (await t.getManifest(create(GetManifestRequestSchema, {})).catch((x: unknown) => x)) as RpcError;
    expect(e.status).toBe("ResourceExhausted");
    expect(e.retry).toMatchObject({ reason: RetryReason.RATE_LIMITED, afterMs: 5000 });
    expect(e.reject).toBeUndefined();
  });

  it.each([
    ["canceled", "Cancelled"],
    ["unknown", "Unknown"],
    ["invalid_argument", "InvalidArgument"],
    ["deadline_exceeded", "DeadlineExceeded"],
    ["not_found", "NotFound"],
    ["already_exists", "AlreadyExists"],
    ["permission_denied", "PermissionDenied"],
    ["resource_exhausted", "ResourceExhausted"],
    ["failed_precondition", "FailedPrecondition"],
    ["aborted", "Aborted"],
    ["out_of_range", "OutOfRange"],
    ["unimplemented", "Unimplemented"],
    ["internal", "Internal"],
    ["unavailable", "Unavailable"],
    ["data_loss", "DataLoss"],
    ["unauthenticated", "Unauthenticated"],
  ])("maps the Connect code %s to the status %s", async (code, status) => {
    const e = (await transport(failing(400, { code, message: "m" })).getManifest(create(GetManifestRequestSchema, {})).catch((x: unknown) => x)) as RpcError;
    expect(e.status).toBe(status);
  });

  it("takes the status from the HTTP code when the body is not a Connect error, and is Unavailable when no response came", async () => {
    const e1 = (await transport(failing(503, "<html>bad gateway</html>", { "content-type": "application/json" })).getManifest(create(GetManifestRequestSchema, {})).catch((x: unknown) => x)) as RpcError;
    expect(e1).toMatchObject({ name: "RpcError", status: "Unavailable" });
    const e2 = (await transport(failing(502, "x", { "content-type": "text/html" })).getManifest(create(GetManifestRequestSchema, {})).catch((x: unknown) => x)) as RpcError;
    expect(e2.status).toBe("Unavailable");
    const refused = (await transport(async () => {
      throw Object.assign(new TypeError("fetch failed"), { cause: { code: "ECONNREFUSED" } });
    })
      .getManifest(create(GetManifestRequestSchema, {}))
      .catch((x: unknown) => x)) as RpcError;
    expect(refused).toMatchObject({ name: "RpcError", status: "Unavailable" });
  });

  it("is a ProtocolViolation when the one detail is a Reject whose bytes break 02 [ARC-61]", async () => {
    const badReject = { type: "abcp.v2.Reject", value: b64(Uint8Array.from([0x1a, 0x02, 0xff, 0xfe])) }; // Reject.message (field 3) is not UTF-8
    const t = transport(failing(409, { code: "failed_precondition", message: "m", details: [badReject] }));
    await expect(t.getManifest(create(GetManifestRequestSchema, {}))).rejects.toBeInstanceOf(ProtocolViolation);
  });

  it("ends a stream with the status and the detail of its end-of-stream message, after the frames before it", async () => {
    const hb = bytes(SubscribeResponseSchema, heartbeat(2));
    const t = transport(async () => response(200, streamHeader, [frame(0, hb), endStream({ code: "unauthenticated", message: "token expired", details: [rejectDetail(Code.PERMISSION_DENIED)] })]));
    const seen: SubscribeResponse[] = [];
    const e = (await (async () => {
      for await (const f of t.subscribe(create(SubscribeRequestSchema, {}))) seen.push(f);
    })().catch((x: unknown) => x)) as RpcError;
    expect(seen).toHaveLength(1);
    expect(e).toMatchObject({ name: "RpcError", status: "Unauthenticated" });
    expect(e.reject?.code).toBe(Code.PERMISSION_DENIED);
  });

  it("reads the error of a stream that was refused before it began (an HTTP status with a JSON body)", async () => {
    const t = transport(failing(401, { code: "unauthenticated", message: "no token", details: [rejectDetail(Code.PERMISSION_DENIED)] }));
    const e = (await (async () => {
      for await (const f of t.subscribe(create(SubscribeRequestSchema, {}))) void f;
    })().catch((x: unknown) => x)) as RpcError;
    expect(e).toMatchObject({ status: "Unauthenticated" });
    expect(e.reject?.code).toBe(Code.PERMISSION_DENIED);
  });
});

describe("the frames of a stream", () => {
  const drain = async (t: BytesTransport, signal?: AbortSignal): Promise<SubscribeResponse[]> => {
    const out: SubscribeResponse[] = [];
    for await (const f of t.subscribe(create(SubscribeRequestSchema, {}), signal !== undefined ? { signal } : {})) out.push(f);
    return out;
  };

  it("are read whole from chunks that cut them anywhere, a byte at a time included", async () => {
    const hbs = [heartbeat(1), heartbeat(2), heartbeat(3)].map((h) => frame(0, bytes(SubscribeResponseSchema, h)));
    const all = Buffer.concat([...hbs, endStream(undefined, { "x-note": ["1"] })]);
    const oneByOne = Array.from(all, (b) => Uint8Array.of(b));
    const t = transport(async () => response(200, streamHeader, oneByOne));
    expect(await drain(t)).toHaveLength(3);
    const odd = [all.subarray(0, 3), all.subarray(3, 9), all.subarray(9, all.length - 1), all.subarray(all.length - 1)].map((b) => new Uint8Array(b));
    expect(await drain(transport(async () => response(200, streamHeader, odd)))).toHaveLength(3);
  });

  it("end cleanly with the end-of-stream message; with none the connection broke and the call is Unavailable (the engine resumes with backoff)", async () => {
    expect(await drain(transport(async () => response(200, streamHeader, [endStream()])))).toEqual([]);
    const cut = (await drain(transport(async () => response(200, streamHeader, [frame(0, bytes(SubscribeResponseSchema, heartbeat(1)))]))).catch((x: unknown) => x)) as RpcError;
    expect(cut).toMatchObject({ name: "RpcError", status: "Unavailable" });
    const inside = frame(0, bytes(SubscribeResponseSchema, heartbeat(1)));
    const torn = (await drain(transport(async () => response(200, streamHeader, [inside.subarray(0, inside.length - 2)]))).catch((x: unknown) => x)) as RpcError;
    expect(torn.status).toBe("Unavailable");
  });

  it("refuse a compressed message that no encoding was agreed for, and a message after the end of the stream", async () => {
    const hb = bytes(SubscribeResponseSchema, heartbeat(1));
    await expect(drain(transport(async () => response(200, streamHeader, [frame(1, hb), endStream()])))).rejects.toBeInstanceOf(ProtocolViolation);
    await expect(drain(transport(async () => response(200, streamHeader, [endStream(), frame(0, hb)])))).rejects.toBeInstanceOf(ProtocolViolation);
    await expect(drain(transport(async () => response(200, streamHeader, [endStream(), endStream()])))).rejects.toBeInstanceOf(ProtocolViolation);
  });

  it("stop at an abort: the connection is released, and the call ends as Cancelled", async () => {
    let res: ReturnType<typeof response> | undefined;
    let release: () => void = () => undefined;
    const gate = { release: new Promise<void>((r) => (release = r)) };
    const t = transport(async (req) => (res = response(200, streamHeader, [frame(0, bytes(SubscribeResponseSchema, heartbeat(1)))], { ...gate, ...(req.signal !== undefined ? { signal: req.signal } : {}) })));
    const ac = new AbortController();
    const seen: SubscribeResponse[] = [];
    const run = (async () => {
      for await (const f of t.subscribe(create(SubscribeRequestSchema, {}), { signal: ac.signal })) {
        seen.push(f);
        ac.abort(); // the engine's watchdog, or its stop
      }
    })().catch((x: unknown) => x);
    const outcome = await Promise.race([run, new Promise((r) => setTimeout(() => r("hung"), 1000))]);
    release();
    expect(seen).toHaveLength(1);
    expect(outcome).toMatchObject({ name: "RpcError", status: "Cancelled" });
    await new Promise((r) => setTimeout(r, 10));
    expect(res?.left()).toBe(true);
  });

  it("stop when the reader leaves early: the request is aborted", async () => {
    let signal: AbortSignal | undefined;
    const hb = frame(0, bytes(SubscribeResponseSchema, heartbeat(1)));
    const t = transport(async (req) => {
      signal = req.signal;
      return response(200, streamHeader, [hb, hb, hb]);
    });
    for await (const f of t.subscribe(create(SubscribeRequestSchema, {}))) {
      void f;
      break;
    }
    expect(signal?.aborted).toBe(true);
  });
});

describe("deadlines and aborts (02 [ARC-60])", () => {
  const never: UniversalClientFn = (req) =>
    new Promise((_resolve, reject) => {
      req.signal?.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { name: "AbortError" })), { once: true });
    });

  it("a unary call that passes its deadline fails with DeadlineExceeded, and says so to the server in the header", async () => {
    let header: string | null = null;
    const t = transport((req) => ((header = req.header.get("connect-timeout-ms")), never(req)), { unaryTimeoutMs: 40 });
    await expect(t.getManifest(create(GetManifestRequestSchema, {}))).rejects.toMatchObject({ name: "RpcError", status: "DeadlineExceeded" });
    expect(header).toBe("40");
    await expect(t.getManifest(create(GetManifestRequestSchema, {}), { timeoutMs: 15 })).rejects.toMatchObject({ status: "DeadlineExceeded" });
  });

  it("a call that the caller aborts fails with Cancelled, one that was aborted before it began never goes out", async () => {
    const ac = new AbortController();
    const t = transport(never);
    const p = t.getManifest(create(GetManifestRequestSchema, {}), { signal: ac.signal });
    setTimeout(() => ac.abort(), 10);
    await expect(p).rejects.toMatchObject({ status: "Cancelled" });
    let went = false;
    const t2 = transport((req) => ((went = true), never(req)));
    await expect(t2.getManifest(create(GetManifestRequestSchema, {}), { signal: AbortSignal.abort() })).rejects.toMatchObject({ status: "Cancelled" });
    void went;
  });

  it("a ConnectCaller is the whole of the byte side: it moves the octets of the request and the response and decodes nothing", async () => {
    const caller = new ConnectCaller({ baseUrl: "https://x.test", http: async () => unaryOk(Uint8Array.of(1, 2, 3)) });
    expect([...(await caller.unary("Anything", Uint8Array.of(9)))]).toEqual([1, 2, 3]);
  });
});
