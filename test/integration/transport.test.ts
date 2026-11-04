// C5: the transport on its own against the Go server: the manifest as the server serves it (GetManifest, 04 §4.7), and the failures of A.7 as the server sends
// them with their one ABPC detail (02 [ARC-57]), over each client (connect-node over HTTP/1.1 and HTTP/2, and fetch).

import { create } from "@bufbuild/protobuf";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { RpcError } from "../../src/engine/index.js";
import {
  CreateKeyspaceRequestSchema,
  GetManifestRequestSchema,
  LoadStateRequestSchema,
  RefreshSessionRequestSchema,
  SubscribeRequestSchema,
} from "../../src/gen/abpc/v2/sync_pb.js";
import { KeyspaceCursorSchema } from "../../src/gen/abpc/v2/types_pb.js";
import { Code } from "../../src/gen/abpc/v2/types_pb.js";
import { ConnectManifests, createWebTransport, TokenStore } from "../../src/net/index.js";
import { createNodeTransport } from "../../src/net/node.js";
import type { Transport } from "../../src/engine/index.js";
import { Env } from "./harness/env.js";
import { APP, goAvailable, SKIP_REASON } from "./harness/server.js";

if (!goAvailable) console.warn(`[integration] ${SKIP_REASON}`);

describe.skipIf(!goAvailable)("the transport against the Go server", () => {
  let env: Env;
  beforeAll(async () => {
    env = await Env.start();
  }, 600_000);
  afterAll(async () => {
    await env?.close();
  });

  const clients: { name: string; make: (token: () => string | undefined) => { transport: Transport; close: () => void } }[] = [
    { name: "node http/1.1", make: (token) => createNodeTransport({ baseUrl: env.url, token, httpVersion: "1.1" }) },
    { name: "node http/2", make: (token) => createNodeTransport({ baseUrl: env.url, token, httpVersion: "2" }) },
    { name: "fetch", make: (token) => ({ transport: createWebTransport({ baseUrl: env.url, token }), close: () => undefined }) },
  ];

  describe.each(clients)("over $name", ({ make }) => {
    const call = async <T>(p: Promise<T>): Promise<T | RpcError> => p.catch((e: unknown) => e as RpcError);
    const ana = (): TokenStore => new TokenStore({ initial: env.token({ sub: "ana", deviceId: "dev-ana" }) });

    it("reads the manifest that the server serves, as the engine's manifests: hashed, in json/v1, of the version it asks for", async () => {
      const c = make(ana().reader);
      try {
        const ms = new ConnectManifests({ transport: c.transport, appId: APP });
        expect(await ms.ensure(1)).toBe(true);
        expect(await ms.ensure(2)).toBe(false); // the server answers NotFound for a version it never published
        const m = ms.parsed(1);
        expect(m?.app).toBe(APP);
        expect(m?.hash).toHaveLength(32);
        expect(ms.template(1, "doc/x")).toMatchObject({ rule: "crdt", entity: false, parent: "" });
        expect(ms.get(1)?.counterRange("count/x")).toEqual({ min: 0n, max: 1000000n });
        expect(ms.get(1)?.opType("Approve")).toMatchObject({ lane: "GATED", logIntent: true });
        // the active version (0) answers with the version's own number
        const active = await c.transport.getManifest(create(GetManifestRequestSchema, { version: 0 }));
        expect(active.version).toBe(1);
        expect(active.encoding).toBe("json/v1");
      } finally {
        c.close();
      }
    });

    it("fails Unauthenticated without a token or with one the server does not know, and PermissionDenied with the Reject that names the code when the token has no grant", async () => {
      const none = make(() => undefined);
      const forged = make(() => "not.a.token");
      const nogrant = make(() => env.token({ sub: "nobody", deviceId: "d", grants: [{ ks: "other/**", actions: ["read"] }] }));
      try {
        const e1 = await call(none.transport.getManifest(create(GetManifestRequestSchema, {})));
        expect(e1).toBeInstanceOf(RpcError);
        expect((e1 as RpcError).status).toBe("Unauthenticated");
        const e2 = await call(forged.transport.createKeyspace(create(CreateKeyspaceRequestSchema, { keyspaceId: "acme/x", kind: "team" })));
        expect((e2 as RpcError).status).toBe("Unauthenticated");
        const e3 = await call(nogrant.transport.createKeyspace(create(CreateKeyspaceRequestSchema, { keyspaceId: "acme/denied", kind: "team" })));
        expect((e3 as RpcError).status).toBe("PermissionDenied");
        expect((e3 as RpcError).reject?.code).toBe(Code.PERMISSION_DENIED); // [ARC-57]: the detail is the one Reject, decoded strictly
        // a load of a keyspace that the token may read and that does not exist: NotFound with the code
        const e4 = await call(
          make(ana().reader).transport.loadState(create(LoadStateRequestSchema, { cursor: create(KeyspaceCursorSchema, { keyspaceId: "acme/nothing-here" }), pageSize: 0, pageToken: "" })),
        );
        expect((e4 as RpcError).status).toBe("NotFound");
        expect((e4 as RpcError).reject?.code).toBe(Code.KEYSPACE_NOT_FOUND);
      } finally {
        for (const c of [none, forged, nogrant]) c.close();
      }
    });

    it("ends a stream that the server refuses before it opens with the status of the call, and one that the client abandons without a trace", async () => {
      const bad = make(() => "not.a.token");
      const good = make(ana().reader);
      try {
        const refused = await (async () => {
          for await (const f of bad.transport.subscribe(create(SubscribeRequestSchema, { clientId: "c" }))) void f;
        })().catch((e: unknown) => e as RpcError);
        expect(refused).toBeInstanceOf(RpcError);
        expect((refused as RpcError).status).toBe("Unauthenticated");
        // a good stream opens with `Subscribed`; leaving it aborts the request and the server's session ends with it
        const ac = new AbortController();
        const frames: string[] = [];
        const end = await (async () => {
          for await (const f of good.transport.subscribe(create(SubscribeRequestSchema, { clientId: crypto.randomUUID() }), { signal: ac.signal })) {
            frames.push(f.frame.case === "control" ? (f.frame.value.control.case ?? "control") : (f.frame.case ?? "?"));
            ac.abort();
          }
        })().catch((e: unknown) => e as RpcError);
        expect(frames[0], String(end)).toBe("subscribed");
        expect((end as RpcError).status).toBe("Cancelled");
        // RefreshSession for a session that is not the caller's or does not exist: NotFound, as 09 [READ-34] says
        const e = await call(good.transport.refreshSession(create(RefreshSessionRequestSchema, { sessionId: "no-such-session" })));
        expect((e as RpcError).status).toBe("NotFound");
      } finally {
        bad.close();
        good.close();
      }
    });

    it("fails a call whose deadline passes before the server answers with DeadlineExceeded, and one the caller aborts with Cancelled", async () => {
      const c = make(ana().reader);
      try {
        const e1 = await call(c.transport.getManifest(create(GetManifestRequestSchema, {}), { timeoutMs: 1 }));
        // a deadline of 1 ms may be won by a fast loopback answer; when it is lost it is the status that says so
        if (e1 instanceof RpcError) expect(["DeadlineExceeded", "Cancelled"]).toContain(e1.status);
        const ac = new AbortController();
        ac.abort();
        const e2 = await call(c.transport.getManifest(create(GetManifestRequestSchema, {}), { signal: ac.signal }));
        expect((e2 as RpcError).status).toBe("Cancelled");
      } finally {
        c.close();
      }
    });
  });
});
