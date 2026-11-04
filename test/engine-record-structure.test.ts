// Round 19 (spec draft 17), 16 [SEC-3] and 17 [CONF-22]: a client engine judges every record it applies against the structural rules of 16 [SEC-2], and a record that breaks one stops the engine for that
// keyspace: a `protocol-error` event and the `PROTOCOL_ERROR` hold of 14 [CLI-50] and Table 14.1, nothing of the record applied (02 [ARC-28]).
//
//   "A client engine judges a record against the envelope rules, the segment syntax, the reserved namespace and the `json/v1` limits of 05 [STATE-7] always, against the templates and value schemas of
//   the manifest version that the record carries, and never against the app's limits in force (07 §7.9)."
//
// Each case is delivered live (the record reaches the engine on its stream) and on a resume (the engine was offline when it was appended, and reads it in the catch-up of its resume); the record that is
// well formed beside it (a twin) is applied both ways. The server-written `SET` of a run key that a `REDACTED` record carries (03 [ID-19], 06 [LOG-44]) breaks none of the rules, whatever the manifest.
// The fold vectors (fold.test.ts) are the other side of the check: no record that they accept is refused here.

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { jsonBytes, StaticManifests, type StaticManifestSpec } from "../src/engine/index.js";
import type { FakeServer } from "../src/fake/index.js";
import { CasExpectSchema, ChangeKind, KeyChangeSchema, RecordKind, SystemKind, type KeyChange } from "../src/gen/abpc/v2/types_pb.js";
import { fromHex } from "../src/rules/bytes.js";
import { parseSchema } from "../src/rules/jsonv1/index.js";
import { goOffline, goOnline, kc, KS, MANIFEST, ofType, rig, tick, type Rig } from "./helpers/engine.js";

type Init = Parameters<FakeServer["appendRaw"]>[1];

const J = (s: string): Uint8Array => new TextEncoder().encode(s);
const schema = (text: string, use: "json" | "bytes" = "json") => parseSchema(J(text), use);

const SPEC: StaticManifestSpec = {
  ...MANIFEST,
  templates: {
    ...MANIFEST.templates,
    "limits/{id}": { rule: "lww", schema: schema('{"type":"int","min":0,"max":10}') },
    "nn/{id}": { rule: "lww", schema: schema('{"type":"int"}') },
    "nu/{id}": { rule: "lww", schema: schema('{"type":"int","nullable":true}') },
    "blob/{id}": { rule: "lww", encoding: "bytes/v1", schema: schema('{"type":"bytes","max_len":4}', "bytes") },
  },
};

const sets = (key: string, text: string, encoding = "json/v1"): KeyChange => create(KeyChangeSchema, { key, kind: ChangeKind.SET, value: J(text), encoding });
const raw = (init: Partial<ConstructorParameters<typeof Object>[0]> & Record<string, unknown>): KeyChange => create(KeyChangeSchema, init as never);
const del = (key: string): KeyChange => create(KeyChangeSchema, { key, kind: ChangeKind.DELETE });
const casAbsent = (key: string, value: string): KeyChange => create(KeyChangeSchema, { key, kind: ChangeKind.CAS, value: J(value), encoding: "json/v1", expected: create(CasExpectSchema, { expect: { case: "absent", value: true } }) });
const ops = (...changes: KeyChange[]): Init => ({ changes });

const UPDATE = fromHex("01010501840500016200"); // a Yjs update that parses
const nest = (n: number): string => "[".repeat(n) + "]".repeat(n);
const members = (n: number): string => `{${Array.from({ length: n }, (_, i) => `"m${String(i).padStart(4, "0")}":0`).join(",")}}`;
const seg = (n: number): string => "a".repeat(n);

interface Case {
  /** What breaks, with its rule. */
  name: string;
  bad: Init;
  /** The record that is well formed beside it: applied. */
  good?: Init;
}

const cases: Case[] = [
  // -- the segment syntax (03 [ID-1], [ID-2], [ID-13]) --
  { name: "03 [ID-1]: a segment of 65 octets", bad: ops(sets(`settings/${seg(65)}`, "1")), good: ops(sets(`settings/${seg(64)}`, "1")) },
  { name: "03 [ID-2]: a segment `..`", bad: ops(sets("settings/..", "1")), good: ops(sets("settings/a.b", "1")) },
  { name: "03 [ID-2]: a segment `__proto__`", bad: ops(sets("settings/__proto__", "1")), good: ops(sets("settings/__proto", "1")) },
  { name: "03 [ID-1]: a segment with a space", bad: ops(sets("settings/a b", "1")), good: ops(sets("settings/a-b", "1")) },
  { name: "03 [ID-1]: an empty segment", bad: ops(sets("settings//a", "1")) },
  // a reserved key matches no template of the manifest, so that no template lookup judges its segments: the syntax of the key does
  { name: "03 [ID-1]: a segment of 65 octets in a reserved key", bad: ops(sets(`abpc.saga.v1/run/${seg(65)}`, "{}")), good: ops(sets(`abpc.saga.v1/run/${seg(64)}`, "{}")) },
  { name: "03 [ID-1]: a segment with a space in a reserved key", bad: ops(sets("abpc.saga.v1/run/a b", "{}")) },
  { name: "03 [ID-2]: a segment `..` in a reserved key", bad: ops(sets("abpc.lease.v1/claim/..", "{}")), good: ops(sets("abpc.lease.v1/claim/42", "{}")) },
  { name: "03 [ID-13]: a key of 17 segments", bad: ops(sets(Array.from({ length: 17 }, () => "a").join("/"), "1")) },
  { name: "03 [ID-13]: a key of over 512 octets", bad: ops(sets(Array.from({ length: 9 }, () => seg(64)).join("/"), "1")) },
  { name: "03 [ID-13]: no key", bad: ops(sets("", "1")) },
  // -- the reserved namespace (03 [ID-47]); the run key of a saga is one that 03 §3.8 defines --
  { name: "03 [ID-47]: a reserved key that nothing defines", bad: ops(sets("abpc.nothing/x", "1")), good: ops(sets("abpc.saga.v1/run/s1", "{}")) },
  { name: "03 [ID-47]: the first segment `abpc`", bad: ops(sets("abpc/x", "1")) },
  { name: "03 [ID-47]: an escrow key with a class that is no class", bad: ops(sets("abpc.escrow.v1/Till/b1", "{}")), good: ops(sets("abpc.escrow.v1/till/b1", "{}")) },
  { name: "03 [ID-47]: an escrow exception key whose seq is 0", bad: ops(sets("abpc.escrow.v1/till/b1/exception/0", "{}")), good: ops(sets("abpc.escrow.v1/till/b1/exception/7", "{}")) },
  { name: "03 [ID-47]: a saga target key whose number has a leading zero", bad: ops(sets("abpc.saga.v1/run/s1/target/07", "{}")), good: ops(sets("abpc.saga.v1/run/s1/target/7", "{}")) },
  { name: "03 [ID-47]: a saga step key whose step is no step name", bad: ops(sets("abpc.saga.v1/run/s1/step/Charge", "{}")), good: ops(sets("abpc.saga.v1/run/s1/step/copy.0", "{}")) },
  { name: "03 [ID-47]: an intent row with another shape", bad: ops(sets("abpc.intent/a/b", "{}")), good: ops(sets("abpc.intent/a", "{}")) },
  // -- the envelope (07 §7.3, 05 [STATE-20], [STATE-24], 03 [ID-23]) --
  { name: "07 Table 7-2 item 1: an OP record with no change", bad: ops() },
  { name: "05 [STATE-20]: a SET that sets no encoding", bad: ops(raw({ key: "settings/a", kind: ChangeKind.SET, value: J("1") })), good: ops(sets("settings/a", "1")) },
  { name: "05 [STATE-20]: a SET that sets a delta", bad: ops(raw({ key: "settings/a", kind: ChangeKind.SET, value: J("1"), encoding: "json/v1", delta: 1n })) },
  { name: "05 [STATE-20]: a DELETE that sets a value", bad: ops(raw({ key: "settings/a", kind: ChangeKind.DELETE, value: J("1") })), good: ops(del("settings/a")) },
  { name: "05 [STATE-20]: an INCR that sets a value", bad: ops(raw({ key: "counters/c", kind: ChangeKind.INCR, delta: 1n, value: J("1") })), good: ops(raw({ key: "counters/c", kind: ChangeKind.INCR, delta: 1n })) },
  { name: "05 [STATE-20]: a CRDT_UPDATE that sets no encoding", bad: ops(raw({ key: "docs/d1", kind: ChangeKind.CRDT_UPDATE, update: UPDATE })), good: ops(raw({ key: "docs/d1", kind: ChangeKind.CRDT_UPDATE, update: UPDATE, encoding: "yjs/v1" })) },
  { name: "05 [STATE-24]: a CAS with no expected", bad: ops(raw({ key: "notes/n1", kind: ChangeKind.CAS, value: J("1"), encoding: "json/v1" })), good: ops(casAbsent("notes/n1", "1")) },
  { name: "05 [STATE-24]: a CAS whose expected sets no member", bad: ops(raw({ key: "notes/n1", kind: ChangeKind.CAS, value: J("1"), encoding: "json/v1", expected: create(CasExpectSchema, {}) })) },
  { name: "05 [STATE-24]: a CAS whose absent is false", bad: ops(raw({ key: "notes/n1", kind: ChangeKind.CAS, value: J("1"), encoding: "json/v1", expected: create(CasExpectSchema, { expect: { case: "absent", value: false } }) })) },
  { name: "03 [ID-23]: a blob_refs entry that is no blob id", bad: { ...ops(sets("settings/a", "1")), blobRefs: ["short"] }, good: { ...ops(sets("settings/a", "1")), blobRefs: ["b".repeat(22)] } },
  { name: "03 [ID-23]: a causation_id with a space", bad: { ...ops(sets("settings/a", "1")), causationId: "has space" }, good: { ...ops(sets("settings/a", "1")), causationId: "saga:7f3a" } },
  // -- the json/v1 limits of 05 [STATE-7] --
  { name: "05 [STATE-7]: nesting of 33", bad: ops(sets("settings/a", nest(33))), good: ops(sets("settings/a", nest(32))) },
  { name: "05 [STATE-7]: an object of 1,025 members", bad: ops(sets("settings/a", members(1025))), good: ops(sets("settings/a", members(1024))) },
  { name: "05 [STATE-7]: U+0000 in a string", bad: ops(sets("settings/a", '"\\u0000"')), good: ops(sets("settings/a", '"a"')) },
  { name: "05 [STATE-7]: U+0000 in a member name", bad: ops(sets("settings/a", '{"\\u0000":1}')) },
  // R19 review S5: a value that breaks a limit is structurally invalid whatever other defect of its text the reader reports first
  { name: "05 [STATE-7]: U+0000 in a string, and a duplicate member before it", bad: ops(sets("settings/a", '{"a":"\\u0000","a":1}')) },
  { name: "05 [STATE-7]: U+0000 in a string, and a number that is no binary64 value", bad: ops(sets("settings/a", '["\\u0000",1e400]')) },
  {
    name: "05 [STATE-7]: U+0000 in a string, and an octet that is not UTF-8",
    bad: ops(raw({ key: "settings/a", kind: ChangeKind.SET, value: new Uint8Array([...J('["\\u0000","'), 0xff, ...J('"]')]), encoding: "json/v1" })),
  },
  { name: "05 [STATE-7]: nesting of 33 after a duplicate member", bad: ops(sets("settings/a", `{"a":1,"a":${nest(33)}}`)) },
  { name: "05 [STATE-7]: nesting of 33 after a bad number and an unpaired surrogate", bad: ops(sets("settings/a", `[1e400,"\\ud800",${nest(33)}]`)) },
  { name: "05 [STATE-7]: an object of 1,025 members, the first of them twice", bad: ops(sets("settings/a", `{"m":0,"m":0,${members(1024).slice(1, -1)}}`)) },
  { name: "05 [STATE-7]: an object of 1,025 members, with the name of one of them spelled by an escape", bad: ops(sets("settings/a", `{"\\u006d0000":0,${members(1025).slice(1, -1)}}`)) },
  { name: "05 [STATE-15]: the literal null as a value", bad: ops(sets("settings/a", "null")) },
  { name: "05 [STATE-15]: is_null with a value", bad: ops(raw({ key: "settings/a", kind: ChangeKind.SET, value: J("1"), encoding: "json/v1", isNull: true })), good: ops(raw({ key: "settings/a", kind: ChangeKind.SET, encoding: "json/v1", isNull: true })) },
  // -- the template and the value schema of the manifest version of the record (05 [STATE-6], [STATE-8], [STATE-10], [STATE-14], [STATE-19], [STATE-27]) --
  { name: "05 [STATE-10]: a value over the schema's max", bad: ops(sets("limits/a", "11")), good: ops(sets("limits/a", "5")) },
  { name: "05 [STATE-10]: a value of another type than the schema's", bad: ops(sets("limits/a", '"five"')) },
  { name: "05 [STATE-6]: an encoding that is not the template's", bad: ops(sets("limits/a", "5", "bytes/v1")) },
  { name: "05 [STATE-14]: a null for a schema that does not allow one", bad: ops(raw({ key: "nn/a", kind: ChangeKind.SET, encoding: "json/v1", isNull: true })), good: ops(raw({ key: "nu/a", kind: ChangeKind.SET, encoding: "json/v1", isNull: true })) },
  { name: "05 [STATE-14]: a null for a counter", bad: ops(raw({ key: "counters/c", kind: ChangeKind.SET, encoding: "json/v1", isNull: true })) },
  { name: "05 [STATE-8]: a bytes/v1 value over the schema's max_len", bad: ops(raw({ key: "blob/a", kind: ChangeKind.SET, value: new Uint8Array(5), encoding: "bytes/v1" })), good: ops(raw({ key: "blob/a", kind: ChangeKind.SET, value: new Uint8Array(4), encoding: "bytes/v1" })) },
  { name: "05 [STATE-19]: a SET of a `cas` key", bad: ops(sets("notes/n1", "1")), good: ops(casAbsent("notes/n1", "1")) },
  { name: "05 [STATE-19]: an INCR of an `lww` key", bad: ops(raw({ key: "settings/a", kind: ChangeKind.INCR, delta: 1n })) },
  { name: "05 [STATE-19]: a DELETE of a CRDT key", bad: ops(del("docs/d1")) },
  { name: "05 [STATE-27]: an INCR whose delta is 2^53", bad: ops(raw({ key: "counters/c", kind: ChangeKind.INCR, delta: 2n ** 53n })), good: ops(raw({ key: "counters/c", kind: ChangeKind.INCR, delta: 2n ** 53n - 1n })) },
];

/** A rig that holds one record (seq 1, applied), with the engine's clock steady, and the record under test delivered live or on a resume. */
async function deliver(mode: "live" | "resume", init: Init): Promise<Rig> {
  const r = await rig({ manifests: new StaticManifests(SPEC) });
  r.random.fixed = 0.5; // the reconnect waits 500 ms, and a hold's first re-add 30 s
  r.server.write(KS, [kc("settings/ok", "set", 1)]);
  await r.engine.settle();
  expect(r.engine.cursor(KS)?.seq).toBe(1n);
  if (mode === "resume") await goOffline(r);
  r.server.appendRaw(KS, init);
  if (mode === "resume") await goOnline(r);
  await r.engine.settle();
  if (mode === "resume") await tick(r, 1_000);
  return r;
}

const keysOf = (init: Init): string[] => (init.changes ?? []).map((c) => c.key).filter((k) => k !== "");

describe.each(["live", "resume"] as const)("16 [SEC-3]: a record that breaks a structural rule stops the keyspace, delivered %s", (mode) => {
  for (const c of cases) {
    it(`${c.name}: the engine reports protocol-error, holds the keyspace as PROTOCOL_ERROR, and applies none of the record`, async () => {
      const r = await deliver(mode, c.bad);
      expect(ofType(r, "protocol-error").map((e) => [e.what, e.keyspaces])).toEqual([["RecordsFrame", [KS]]]);
      expect(r.engine.status().holds.map((h) => [h.keyspaceId, h.code])).toEqual([[KS, "PROTOCOL_ERROR"]]);
      expect(r.engine.cursor(KS)?.seq).toBe(1n); // the confirmed state is as it was before the record
      for (const k of keysOf(c.bad)) expect(r.engine.confirmed(KS).has(k), k).toBe(false);
      expect(r.engine.confirmed(KS).has("settings/ok")).toBe(true);
    });
    if (c.good !== undefined) {
      const good = c.good;
      it(`${c.name}: the record that is well formed beside it is applied, with no hold`, async () => {
        const r = await deliver(mode, good);
        expect(r.engine.cursor(KS)?.seq).toBe(2n);
        expect(ofType(r, "protocol-error")).toEqual([]);
        expect(r.engine.status().holds).toEqual([]);
        for (const k of keysOf(good)) if (good.changes?.some((x) => x.key === k && x.kind !== ChangeKind.DELETE)) expect(r.engine.confirmed(KS).has(k), k).toBe(true);
      });
    }
  }
});

describe.each(["live", "resume"] as const)("16 [SEC-3], 03 [ID-19], 06 [LOG-44]: the server-written SET of a run key in a REDACTED record breaks none of the rules, delivered %s", (mode) => {
  const RUN = "abpc.saga.v1/run/saga-1";
  const redacted = (run: KeyChange): Init => ({
    kind: RecordKind.SYSTEM,
    system: { kind: SystemKind.REDACTED, mustUnderstand: true, body: jsonBytes({ redaction_id: "r1", keys: [], min_seq: 0 }) },
    changes: [run],
  });

  it("the manifest in force declares no saga and no saga module: the SET is applied, and no hold or protocol-error follows", async () => {
    const r = await deliver(mode, redacted(sets(RUN, '{"params":{"k":"blanked"},"status":"running"}')));
    expect(r.engine.cursor(KS)?.seq).toBe(2n);
    expect(r.engine.confirmed(KS).has(RUN)).toBe(true);
    expect(ofType(r, "protocol-error")).toEqual([]);
    expect(r.engine.status().holds).toEqual([]);
    expect(ofType(r, "skipped")).toEqual([]);
  });

  it("it is judged as a json/v1 value of a module template, as 05 [STATE-10] says: one that breaks the json/v1 limits stops the keyspace, as any value does", async () => {
    const r = await deliver(mode, redacted(sets(RUN, nest(33))));
    expect(ofType(r, "protocol-error").map((e) => e.what)).toEqual(["RecordsFrame"]);
    expect(r.engine.confirmed(KS).has(RUN)).toBe(false);
  });
});

describe("16 [SEC-3]: what the check leaves to 14 [CLI-5], and to the app's limits", () => {
  it("a json/v1 text that cannot be read at all (not JSON) is a value that the client cannot decode: it enters the confirmed state, `skipped` is emitted, and the engine goes on", async () => {
    const r = await deliver("live", ops(sets("settings/bad", "{not json")));
    expect(r.engine.cursor(KS)?.seq).toBe(2n);
    expect(r.engine.confirmed(KS).get("settings/bad")).toMatchObject({ decodable: false });
    expect(ofType(r, "skipped")).toMatchObject([{ keys: ["settings/bad"] }]);
    expect(r.engine.status().holds).toEqual([]);
  });

  it("an INTENT's requested changes are not applied, and their template and value are not judged: a requested change that breaks its template's schema leaves the keyspace going", async () => {
    const r = await rig({ manifests: new StaticManifests(SPEC) });
    r.server.appendRaw(KS, { kind: RecordKind.INTENT, changes: [sets("limits/a", "11"), sets("abpc.intent/other-1", '{"status":"PENDING"}')] });
    await r.engine.settle();
    expect(ofType(r, "protocol-error")).toEqual([]);
    expect(r.engine.status().holds).toEqual([]);
    expect(r.engine.cursor(KS)?.seq).toBe(1n); // applied: the cursor moved, and the requested change is in no state
    expect(r.engine.confirmed(KS).has("limits/a")).toBe(false);
  });
});

// R19 review, note 4. 16 [SEC-3]: "A client engine MUST check every record it applies against the same structural rules ... the envelope rules, the segment syntax, the reserved namespace and the `json/v1`
// limits ... always". An INTENT record is applied (its intent row is), and its requested changes are carried in it: a key that breaks the segment syntax, and a change that breaks the envelope of its kind,
// are rules of SEC-2 that the record breaks, whatever the engine does with the change. Reading taken (the text says nothing that exempts them): the key and the envelope of a requested change are judged;
// its template and its value are not (nothing of it is applied or read).
describe.each(["live", "resume"] as const)("16 [SEC-3]: the requested changes of an INTENT are judged by their key and their envelope, delivered %s", (mode) => {
  const intent = (...changes: KeyChange[]): Init => ({ kind: RecordKind.INTENT, changes });
  const bad: [string, Init][] = [
    ["a segment `..`", intent(sets("settings/..", "1"), sets("abpc.intent/other-1", '{"status":"PENDING"}'))],

    ["a DELETE that sets a value", intent(raw({ key: "settings/a", kind: ChangeKind.DELETE, value: J("1") }))],
    ["a SET that sets no encoding", intent(raw({ key: "settings/a", kind: ChangeKind.SET, value: J("1") }))],
    ["a CAS with no expected", intent(raw({ key: "notes/n1", kind: ChangeKind.CAS, value: J("1"), encoding: "json/v1" }))],
  ];
  for (const [name, init] of bad) {
    it(`${name}: protocol-error, the PROTOCOL_ERROR hold, and nothing of the record applied`, async () => {
      const r = await deliver(mode, init);
      expect(ofType(r, "protocol-error").map((e) => [e.what, e.keyspaces])).toEqual([["RecordsFrame", [KS]]]);
      expect(r.engine.status().holds.map((h) => [h.keyspaceId, h.code])).toEqual([[KS, "PROTOCOL_ERROR"]]);
      expect(r.engine.cursor(KS)?.seq).toBe(1n);
    });
  }
  // SPEC-QUESTION (16 [SEC-3] with 03 [ID-47]): the reserved namespace of a requested change is not judged, as the vector fold-moved-002 has it (an INTENT whose requested change names `abpc.saga.v1/saga/s1`)
  it("requested changes that are well formed, one with a reserved key that 03 §3.8 does not define and one with a value that breaks its template's schema: the record is applied, with no hold", async () => {
    const r = await deliver(mode, intent(sets("settings/a", "1"), sets("limits/a", "11"), sets("abpc.saga.v1/saga/s1", "{}"), casAbsent("notes/n1", "1"), del("settings/b")));
    expect(r.engine.cursor(KS)?.seq).toBe(2n);
    expect(ofType(r, "protocol-error")).toEqual([]);
    expect(r.engine.status().holds).toEqual([]);
    expect(r.engine.confirmed(KS).has("settings/a")).toBe(false); // requested, never applied
  });
});
