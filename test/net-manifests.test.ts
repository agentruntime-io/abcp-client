// C5: the manifests of an app over `GetManifest` (src/net/manifests.ts): what the client reads of a canonical json/v1 manifest (04 §4.7, [MAN-4]) and how it
// fetches, checks and caches the versions.

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { ProtocolViolation, RpcError, StaticManifests } from "../src/engine/index.js";
import { GetManifestResponseSchema, type GetManifestRequest, type GetManifestResponse } from "../src/gen/abpc/v2/sync_pb.js";
import { ConnectManifests, MemoryManifestCache, readManifest } from "../src/net/index.js";
import { sha256 } from "../src/rules/crypto.js";
import { jsonv1 } from "../src/rules/index.js";

const DOC = {
  app: "tracker",
  required_features: ["escrow.v1"],
  keyspace_kinds: { team: { keys: ["issue/**", "count/**", "doc/**", "seq/**"], op_types: ["A", "B", "C", "Spend"] } },
  keys: {
    "issue/{id}": { merge: "lww", entity: true },
    "issue/{x}/title": { merge: "lww" },
    "issue/{id}/flag": { merge: "lww", fenced_by: "abpc.lease.v1/claim/{id}" },
    "issue/{id}/comment/{cid}": { merge: "lww", entity: true },
    "issue/{id}/comment/{cid}/body": { merge: "cas" },
    "count/{id}": { merge: "counter", value_schema: { type: "int", min: 0, max: 100 } },
    "seq/number": { merge: "counter" },
    "crate/{x}": { merge: "cas", entity: true },
    "crate/{y}/part/{id}": { merge: "lww" },
    "doc/{id}": { merge: "crdt:yjs/v1", crdt_roots: { t: "text", m: "map", odd: "something-new" } },
  },
  op_types: {
    A: { dispatch: "SEQUENCED", writes: ["issue/**"] },
    B: { dispatch: "GATED", gate: { builtin: "always_commit" }, writes: ["issue/{id}/*"], optimistic: false, barrier: true, offline_mode: "fail_fast", log_intent: false },
    C: { dispatch: "SEQUENCED", writes: ["count/{n}", "doc/{d}"] },
    Spend: { dispatch: "GATED", gate: { builtin: "always_commit" }, writes: ["count/{id}"], escrow: "tokens" },
  },
  coordination: { lease: { claim: { resource: "issue/{id}" } } },
};

function answer(doc: unknown, version = 1, over: Omit<Partial<GetManifestResponse>, "$typeName" | "$unknown"> = {}): GetManifestResponse {
  const manifest = jsonv1.canonicalDocument(new TextEncoder().encode(JSON.stringify(doc)));
  return create(GetManifestResponseSchema, { version, manifest, encoding: "json/v1", sha256: sha256(manifest), ...over });
}

describe("what the client reads of a manifest", () => {
  const m = readManifest(answer(DOC), 1, "tracker");

  it("gives the fold the merge rule, the entity marker and the parent of each template, the parent being the longest entity prefix compared as text, variable names included (04 [MAN-20], draft 15)", () => {
    expect(m.template(1, "issue/7")).toMatchObject({ rule: "lww", entity: true, parent: "" });
    // `issue/{x}/title` spells the enclosing entity's variable otherwise: its prefix `issue/{x}` is no entity template's text, so it has no parent
    expect(m.template(1, "issue/7/title")).toMatchObject({ rule: "lww", entity: false, parent: "" });
    expect(m.template(1, "issue/7/flag")).toMatchObject({ rule: "lww", entity: false, parent: "issue/{id}" });
    // fold-parent-014: `crate/{y}/part/{id}` has no parent beside the entity `crate/{x}`
    expect(m.template(1, "crate/c1/part/p1")).toMatchObject({ rule: "lww", entity: false, parent: "" });
    expect(m.template(1, "issue/7/comment/c1")).toMatchObject({ rule: "lww", entity: true, parent: "issue/{id}" });
    expect(m.template(1, "issue/7/comment/c1/body")).toMatchObject({ rule: "cas", entity: false, parent: "issue/{id}/comment/{cid}" });
    expect(m.template(1, "count/x")).toMatchObject({ rule: "counter", entity: false, parent: "" });
    expect(m.template(1, "doc/d1")).toMatchObject({ rule: "crdt", entity: false, parent: "" });
    expect(m.template(1, "seq/number")?.rule).toBe("counter");
    expect(m.template(1, "nothing/here")).toBeUndefined();
    expect(m.template(1, "issue/7/title/extra")).toBeUndefined();
    expect(m.templatePattern("issue/7/title")).toBe("issue/{x}/title");
  });

  it("gives the engine the attributes of the op types, with the defaults of 04 §4.5 and the declared templates that `writes` selects (03 [ID-21])", () => {
    expect(m.opType("A")).toMatchObject({ lane: "SEQUENCED", optimistic: true, barrier: false, offlineMode: "queue_recheck", spend: false, logIntent: false });
    expect([...(m.opType("A")?.writes ?? [])].sort()).toEqual(["issue/{id}", "issue/{id}/comment/{cid}", "issue/{id}/comment/{cid}/body", "issue/{id}/flag", "issue/{x}/title"].sort());
    expect(m.opType("B")).toMatchObject({ lane: "GATED", optimistic: false, barrier: true, offlineMode: "fail_fast", logIntent: false });
    expect([...(m.opType("B")?.writes ?? [])].sort()).toEqual(["issue/{id}/flag", "issue/{x}/title"]); // `T/*`: exactly one segment more than `issue/{id}`
    expect([...(m.opType("C")?.writes ?? [])].sort()).toEqual(["count/{id}", "doc/{id}"]); // a variable selects a variable, whatever its name
    expect(m.opType("Spend")).toMatchObject({ lane: "GATED", spend: true, logIntent: true });
    expect(m.opType("Nope")).toBeUndefined();
  });

  it("gives the roots of a CRDT template, the range of a counter, and the key that fences a key", () => {
    expect(new Map(m.crdtRoots("doc/d1"))).toEqual(
      new Map([
        ["t", "text"],
        ["m", "map"],
      ]),
    ); // a root type that this client does not know is left out
    expect(m.crdtRoots("count/x")).toBeUndefined();
    expect(m.counterRange("count/x")).toEqual({ min: 0n, max: 100n });
    expect(m.counterRange("seq/number")).toEqual({ min: -9007199254740991n, max: 9007199254740991n });
    expect(m.counterRange("issue/7")).toBeUndefined();
    expect(m.fencedBy("issue/7/flag")).toBe("abpc.lease.v1/claim/7");
    expect(m.fencedBy("issue/7/title")).toBeUndefined();
  });

  it("has the op types that no manifest declares: abpc.Override, and the client's op types of the modules that the coordination section enables (11 Table 11.1)", () => {
    expect(m.opType("abpc.Override")).toMatchObject({ lane: "SEQUENCED" });
    expect(m.opType("abpc.lease.v1.Acquire")).toMatchObject({ lane: "GATED" });
    expect(m.opType("abpc.lease.v1.Expire")).toBeUndefined(); // module only
    expect(m.opType("abpc.saga.v1.Start")).toBeUndefined(); // the saga module is not enabled
    expect(m.app).toBe("tracker");
    expect(m.requiredFeatures).toEqual(["escrow.v1"]);
  });

  it("agrees with the static description that the fake server and the tests use, for what the fold needs", () => {
    const s = new StaticManifests({
      version: 1,
      templates: {
        "issue/{id}": { rule: "lww", entity: true },
        "issue/{x}/title": { rule: "lww" }, // no parent: its prefix is no entity template's text (04 [MAN-20])
        "issue/{id}/flag": { rule: "lww" },
        "crate/{x}": { rule: "cas", entity: true },
        "crate/{y}/part/{id}": { rule: "lww" },
        "count/{id}": { rule: "counter" },
        "doc/{id}": { rule: "crdt", roots: { t: "text" } },
      },
    });
    for (const key of ["issue/1", "issue/1/title", "issue/1/flag", "crate/c1", "crate/c1/part/p1", "count/q", "doc/z"]) expect(m.template(1, key)).toMatchObject({ rule: s.template(1, key)?.rule, entity: s.template(1, key)?.entity, parent: s.template(1, key)?.parent, encoding: s.template(1, key)?.encoding });
  });
});

describe("what the client checks of the answer", () => {
  const bad = (a: GetManifestResponse, version = 1, app?: string): ProtocolViolation => {
    try {
      readManifest(a, version, app);
    } catch (e) {
      return e as ProtocolViolation;
    }
    throw new Error("the answer was accepted");
  };
  it("refuses an answer that is not a json/v1 manifest of the version asked for, whose hash is not its own, or that is another app's", () => {
    expect(bad(answer(DOC, 1, { encoding: "yaml/v1" })).message).toMatch(/not json\/v1/);
    expect(bad(answer(DOC, 2)).message).toMatch(/version 2/);
    expect(bad(answer(DOC, 0), 0).message).toMatch(/names no version/);
    expect(bad(answer(DOC, 1, { sha256: new Uint8Array(32) })).message).toMatch(/sha256/);
    expect(bad(answer(DOC, 1, { manifest: new TextEncoder().encode("{not json") })).message).toMatch(/sha256|json\/v1 document/);
    const garbage = new TextEncoder().encode('{"a":1,"a":2}'); // a duplicate member (04 [MAN-3])
    expect(bad(answer(DOC, 1, { manifest: garbage, sha256: sha256(garbage) })).message).toMatch(/json\/v1 document/);
    expect(bad(answer({ ...DOC, app: "other" }), 1, "tracker").message).toMatch(/"other"/);
    expect(bad(answer({ keys: {} })).message).toMatch(/no app/);
    expect(readManifest(answer(DOC, 5), 0).version).toBe(5); // version 0 asks for the active one, whatever its number
  });
});

describe("the manifests of an app by version", () => {
  const transport = (
    versions: Record<number, GetManifestResponse>,
    log: number[] = [],
    fail?: () => Error | undefined,
  ): { getManifest: (r: GetManifestRequest) => Promise<GetManifestResponse> } => ({
    getManifest: async (r) => {
      log.push(r.version);
      const e = fail?.();
      if (e !== undefined) throw e;
      const a = versions[r.version];
      if (a === undefined) throw new RpcError("NotFound", "the app has no such manifest version");
      return a;
    },
  });

  it("fetches a version once, however often it is asked for and however many ask at once, and answers false for a version that the server does not hold", async () => {
    const log: number[] = [];
    const ms = new ConnectManifests({ transport: transport({ 1: answer(DOC, 1), 2: answer(DOC, 2) }, log), appId: "tracker" });
    expect(ms.get(1)).toBeUndefined();
    expect(ms.template(1, "issue/1")).toBeUndefined(); // not loaded yet
    expect(await Promise.all([ms.ensure(1), ms.ensure(1), ms.ensure(1)])).toEqual([true, true, true]);
    expect(await ms.ensure(1)).toBe(true);
    expect(log).toEqual([1]);
    expect(ms.get(1)?.opType("A")).toBeDefined();
    expect(ms.template(1, "issue/1")?.rule).toBe("lww");
    expect(await ms.ensure(9)).toBe(false);
    expect(await ms.ensure(0)).toBe(false); // version 0 is never a version of a record
    expect(log).toEqual([1, 9]);
    expect(ms.parsed(1)?.hash).toEqual(sha256(answer(DOC).manifest));
  });

  it("throws what is not an absent version (the engine resumes with its backoff), and a violation for an answer that is wrong", async () => {
    let fail: Error | undefined = new RpcError("Unavailable", "down");
    const ms = new ConnectManifests({ transport: transport({ 1: answer(DOC, 1), 2: answer(DOC, 3) }, [], () => fail) });
    await expect(ms.ensure(1)).rejects.toMatchObject({ status: "Unavailable" });
    fail = undefined;
    expect(await ms.ensure(1)).toBe(true); // a failure is not remembered
    await expect(ms.ensure(2)).rejects.toBeInstanceOf(ProtocolViolation); // asked for 2, got 3
    expect(ms.get(2)).toBeUndefined();
  });

  it("reloads a version for the hold of UNKNOWN_OP_TYPE: it asks the server again and takes what it says now", async () => {
    const log: number[] = [];
    const versions: Record<number, GetManifestResponse> = { 1: answer({ ...DOC, op_types: { ...DOC.op_types, A: { dispatch: "SEQUENCED", writes: ["issue/{id}"] } } }, 1) };
    const ms = new ConnectManifests({ transport: transport(versions, log) });
    await ms.ensure(1);
    expect(ms.get(1)?.opType("D")).toBeUndefined();
    versions[1] = answer({ ...DOC, op_types: { ...DOC.op_types, D: { dispatch: "SEQUENCED", writes: ["issue/{id}"] } } }, 1);
    expect(await ms.reload(1)).toBe(true);
    expect(ms.get(1)?.opType("D")).toMatchObject({ lane: "SEQUENCED" });
    expect(log).toEqual([1, 1]);
    expect(await ms.reload(7)).toBe(false);
  });

  it("keeps the answers in a cache, so that an engine that starts without a network can restore its replicas; a bad cache entry is ignored", async () => {
    const cache = new MemoryManifestCache();
    const log: number[] = [];
    const first = new ConnectManifests({ transport: transport({ 1: answer(DOC, 1) }, log), cache, appId: "tracker" });
    expect(await first.ensure(1)).toBe(true);
    const offline = new ConnectManifests({ transport: transport({}, log, () => new RpcError("Unavailable", "no network")), cache, appId: "tracker" });
    expect(await offline.ensure(1)).toBe(true);
    expect(offline.get(1)?.opType("B")).toMatchObject({ lane: "GATED" });
    expect(log).toEqual([1]); // the second engine never asked
    await cache.put(2, Uint8Array.of(0xff, 0xff));
    const damaged = new ConnectManifests({ transport: transport({ 2: answer(DOC, 2) }, log), cache });
    expect(await damaged.ensure(2)).toBe(true); // the network answers
    expect(log).toEqual([1, 2]);
  });
});
