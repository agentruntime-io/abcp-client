// `ListKeyspaces` (spec/09 §9.11, experimental, not part of v2.0): the request is encoded, the response is decoded strictly and its count is bounded
// by the request's `page_size` ([XKS-6]); the fake server answers Unimplemented (15 [COMPAT-28]).

import { describe, expect, it } from "vitest";
import { create } from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import { BytesTransport, ProtocolViolation, RpcError, type RawCaller } from "../src/engine/index.js";
import { ListKeyspacesRequestSchema, ListKeyspacesResponseSchema, ListedKeyspaceSchema, type ListKeyspacesRequest } from "../src/gen/abcp/v2/sync_pb.js";
import { KeyspaceLifecycle } from "../src/gen/abcp/v2/types_pb.js";
import { encode, unmarshal } from "../src/rules/canon/index.js";
import { FakeServer, ManualClock } from "../src/fake/index.js";
import { StaticManifests } from "../src/engine/index.js";
import { FakeRaw } from "./helpers/raw.js";
import { MANIFEST } from "./helpers/engine.js";

function listed(n: number) {
  return Array.from({ length: n }, (_, i) =>
    create(ListedKeyspaceSchema, { keyspaceId: `acme/k${String(i).padStart(5, "0")}`, kind: "doc", createdAt: timestampFromMs(1_700_000_000_000 + i), lifecycle: KeyspaceLifecycle.ACTIVE }),
  );
}

/** A RawCaller that records the request bytes and answers with a canned response. */
function canned(res: Uint8Array): RawCaller & { sent: { method: string; bytes: Uint8Array }[] } {
  const sent: { method: string; bytes: Uint8Array }[] = [];
  return {
    sent,
    async unary(method, request) {
      sent.push({ method, bytes: request });
      return res;
    },
    // eslint-disable-next-line require-yield
    async *stream() {
      throw new Error("no stream");
    },
  };
}

describe("spec/09 §9.11 ListKeyspaces over bytes (experimental)", () => {
  it("encodes the request, canonically, and decodes the response", async () => {
    const res = create(ListKeyspacesResponseSchema, { keyspaces: listed(3), nextPageToken: "tok" });
    const raw = canned(encode(ListKeyspacesResponseSchema, res));
    const t = new BytesTransport(raw);
    const req = create(ListKeyspacesRequestSchema, { kind: "doc", keyspacePattern: "acme/*", createdAfter: timestampFromMs(1_699_999_940_000), includeArchived: true, pageToken: "", pageSize: 10 });
    const got = await t.listKeyspaces(req);
    expect(got.keyspaces.map((k) => k.keyspaceId)).toEqual(["acme/k00000", "acme/k00001", "acme/k00002"]);
    expect(got.keyspaces[0]?.lifecycle).toBe(KeyspaceLifecycle.ACTIVE);
    expect(got.nextPageToken).toBe("tok");
    expect(raw.sent).toHaveLength(1);
    expect(raw.sent[0]?.method).toBe("ListKeyspaces");
    const back = unmarshal(ListKeyspacesRequestSchema, raw.sent[0]?.bytes as Uint8Array) as ListKeyspacesRequest;
    expect(back.kind).toBe("doc");
    expect(back.keyspacePattern).toBe("acme/*");
    expect(back.includeArchived).toBe(true);
    expect(back.pageSize).toBe(10);
    expect(Buffer.from(encode(ListKeyspacesRequestSchema, back)).equals(Buffer.from(raw.sent[0]?.bytes as Uint8Array))).toBe(true);
  });

  it("[XKS-6] refuses a response with more keyspaces than the page bound", async () => {
    const t = new BytesTransport(canned(encode(ListKeyspacesResponseSchema, create(ListKeyspacesResponseSchema, { keyspaces: listed(3) }))));
    await expect(t.listKeyspaces(create(ListKeyspacesRequestSchema, { pageSize: 2 }))).rejects.toBeInstanceOf(ProtocolViolation);
    // the page size 0 is 100
    const t100 = new BytesTransport(canned(encode(ListKeyspacesResponseSchema, create(ListKeyspacesResponseSchema, { keyspaces: listed(100) }))));
    await expect(t100.listKeyspaces(create(ListKeyspacesRequestSchema, {}))).resolves.toBeDefined();
    const t101 = new BytesTransport(canned(encode(ListKeyspacesResponseSchema, create(ListKeyspacesResponseSchema, { keyspaces: listed(101) }))));
    await expect(t101.listKeyspaces(create(ListKeyspacesRequestSchema, {}))).rejects.toBeInstanceOf(ProtocolViolation);
  });

  it("[XKS-6] a page size above 1,000 counts as 1,000", async () => {
    const ok = new BytesTransport(canned(encode(ListKeyspacesResponseSchema, create(ListKeyspacesResponseSchema, { keyspaces: listed(1000) }))));
    await expect(ok.listKeyspaces(create(ListKeyspacesRequestSchema, { pageSize: 5000 }))).resolves.toBeDefined();
    const over = new BytesTransport(canned(encode(ListKeyspacesResponseSchema, create(ListKeyspacesResponseSchema, { keyspaces: listed(1001) }))));
    await expect(over.listKeyspaces(create(ListKeyspacesRequestSchema, { pageSize: 5000 }))).rejects.toBeInstanceOf(ProtocolViolation);
  });

  it("15 [COMPAT-28] the fake server answers Unimplemented, directly and over bytes", async () => {
    const server = new FakeServer({ manifests: new StaticManifests(MANIFEST), clock: new ManualClock() });
    const ft = server.transport(server.principal("svc"));
    const req = create(ListKeyspacesRequestSchema, {});
    await expect(ft.listKeyspaces(req)).rejects.toMatchObject({ status: "Unimplemented" });
    await expect(ft.listKeyspaces(req)).rejects.toBeInstanceOf(RpcError);
    await expect(new BytesTransport(new FakeRaw(ft)).listKeyspaces(req)).rejects.toMatchObject({ status: "Unimplemented" });
  });
});
