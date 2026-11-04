// Canonical bytes (conformance/vectors/canonical-bytes.json): the canonical protobuf encoding of 07 [MUT-45], the content
// hash, the refusals of 07 [MUT-45] and 02 [ARC-61], and the value digests, E, C and record_hash of 06 [LOG-71].
// Not run on a client engine, by design: `derived-id` and `module-action` (a C module builds those, on the server).
// `request-digest` is the server's audit digest (12 [ADM-94]); it is run here because it exercises the same encoder with
// the views of the other hashes.

import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { fromHex, toHex } from "../src/rules/bytes.js";
import {
  CanonError,
  checkTimestamps,
  contentHash,
  contentHashInput,
  decode,
  encode,
  hashRecord,
  hashRecordFromDigests,
  requestDigest,
  unmarshal,
  unmarshalJson,
} from "../src/rules/canon/index.js";
import { MutationSchema } from "../src/gen/abpc/v2/sync_pb.js";
import { KeyChangeSchema, RecordSchema, type Record as RecordMessage } from "../src/gen/abpc/v2/types_pb.js";
import { extendMessage, fromVectorJson, messageDesc, type FieldAddition } from "./helpers/proto.js";
import { loadVectors, utf8, type Vector } from "./helpers/vectors.js";

const all = loadVectors("canonical-bytes.json");
const ofKind = (k: string): Vector[] => all.filter((v) => v.kind === k);

/** What a vector's expect.refused names for a refusal: INVALID_ENVELOPE per mutation, InvalidArgument for the whole request. */
function verdict(err: unknown): string {
  if (err === undefined) return "";
  if (!(err instanceof CanonError)) throw err;
  return err.invalidEnvelope ? "INVALID_ENVELOPE" : "InvalidArgument";
}

describe("07 [MUT-45] canonical encoding (proto-canonical vectors)", () => {
  for (const v of ofKind("proto-canonical")) {
    it(`${v.id}: ${v.title}`, () => {
      const desc = messageDesc(v.type as string);
      const m = v.received !== undefined ? unmarshal(desc, fromHex(v.received as string)) : fromVectorJson(desc, v.message);
      expect(toHex(encode(desc, m))).toBe(v.expect.canonical);
    });
  }
});

const expectedHash = new Map(ofKind("content-hash").map((v) => [v.id, v.expect.content_hash as string | undefined]));

describe("07 [MUT-45] content hash and the refusals of 02 [ARC-61] (content-hash vectors)", () => {
  for (const v of ofKind("content-hash")) {
    it(`${v.id}: ${v.title.slice(0, 100)}`, () => {
      // the type that the receiver holds: this version's, or, at an upgraded server, one that knows more fields
      const additions = (v.schema_additions as Record<string, FieldAddition[]> | undefined)?.["abpc.v2.Mutation"];
      const desc = additions !== undefined ? extendMessage(MutationSchema, additions) : MutationSchema;

      let m: ReturnType<typeof unmarshal> | undefined;
      let err: unknown;
      try {
        if (v.received !== undefined) {
          // 02 [ARC-61]: a string that is not valid UTF-8 fails the request; 07 [MUT-45]: an encoding not in its shortest
          // form, and 07 Table 7-2 item 1: an authored_at out of range, are INVALID_ENVELOPE
          m = unmarshal(desc, fromHex(v.received as string));
          checkTimestamps(desc, m);
        } else if (v.received_json !== undefined) {
          m = unmarshalJson(desc, utf8(v.received_json as string));
        } else {
          m = fromVectorJson(desc, v.mutation);
        }
      } catch (e) {
        err = e;
      }
      expect(verdict(err), String(err)).toBe(v.expect.refused ?? "");
      if (err !== undefined) return;
      const msg = m as NonNullable<typeof m>;

      expect(toHex(contentHashInput(msg, desc))).toBe(v.expect.canonical);
      const key = fromHex(v.hmac_key as string);
      expect(toHex(contentHash(msg, key, desc))).toBe(v.expect.content_hash);
      if (v.expect.same_hash_as !== undefined) expect(expectedHash.get(v.expect.same_hash_as)).toBe(v.expect.content_hash);
      if (v.expect.different_hash_from !== undefined) expect(expectedHash.get(v.expect.different_hash_from)).not.toBe(v.expect.content_hash);
    });
  }
});

describe("06 [LOG-71] value digests, E, C, W and record_hash (record-hash vectors)", () => {
  for (const v of ofKind("record-hash")) {
    it(`${v.id}: ${v.title.slice(0, 100)}`, () => {
      const rec = fromVectorJson(RecordSchema, v.record) as RecordMessage;
      const prev = fromHex(v.prev_hash as string);
      const got =
        v.value_digests !== undefined
          ? hashRecordFromDigests(
              rec,
              (v.value_digests as string[]).map((d) => fromHex(d)),
              prev,
            )
          : hashRecord(
              rec,
              (v.salts as string[]).map((s) => fromHex(s)),
              ((v.previous_salts ?? []) as string[]).map((s) => fromHex(s)),
              prev,
            );
      expect(got.valueDigests.map(toHex)).toEqual(v.expect.value_digests);
      expect(toHex(got.e)).toBe(v.expect.E);
      expect(toHex(got.c)).toBe(v.expect.C);
      expect(toHex(got.w)).toBe(v.expect.W);
      expect(toHex(got.recordHash)).toBe(v.expect.record_hash);
    });
  }
});

// Round 18 (spec draft 16, omniview 6e5fe6f): the vectors the code does not pass yet, with the batch that will (the server's list is
// server/internal/vectors/pending.go). Remove an entry when its work lands.
const PENDING = new Map<string, string>([
  ["request-digest-009", "R19-B2: 12 [ADM-94] (J7), every split and archive prefix is cleared"],
  ["request-digest-010", "R19-B2: 12 [ADM-94] (J7), every split and archive prefix is cleared"],
  ["request-digest-011", "R19-B2: 12 [ADM-94] (J7), every split and archive prefix is cleared"],
]);

// Since round 18 (12 [ADM-94], m9) the digest also clears `RedactRequest.keys` and the `key` of every OVERRIDE change whose key
// matches a template that is `personal` in the manifest version in force; a vector lists those keys in `personal_keys` (the
// manifest is not carried), and `same_digest_as` names the vector whose digest it must equal.
describe("12 [ADM-94] request digest (request-digest vectors)", () => {
  const digests = new Map<string, string>();
  for (const v of ofKind("request-digest")) {
    (PENDING.has(v.id) ? it.skip : it)(`${v.id}: ${v.title.slice(0, 100)}`, () => {
      let canonical: Uint8Array = new Uint8Array(0);
      let digest: Uint8Array = new Uint8Array(0);
      if (v.event === undefined) {
        const desc = messageDesc(v.type as string);
        let refused = "";
        let m: ReturnType<typeof unmarshal> | undefined;
        try {
          if (v.received !== undefined) m = unmarshal(desc, fromHex(v.received as string));
          else if (v.received_json !== undefined) m = unmarshalJson(desc, utf8(v.received_json as string));
          else m = fromVectorJson(desc, v.request);
        } catch (e) {
          if (!(e instanceof CanonError)) throw e;
          refused = "InvalidArgument"; // a request that breaks 02 [ARC-61] fails as a whole, whatever the refusal
        }
        expect(refused).toBe(v.expect.refused ?? "");
        if (m !== undefined) {
          const personalKeys = (v.personal_keys ?? []) as string[];
          ({ canonical, digest } = requestDigest(desc, m, (key) => personalKeys.includes(key)));
        }
      }
      expect(toHex(canonical)).toBe(v.expect.canonical);
      expect(toHex(digest)).toBe(v.expect.request_digest);
      digests.set(v.id, toHex(digest));
      const other = v.expect.same_digest_as as string | undefined;
      if (other !== undefined && digests.has(other)) expect(toHex(digest), `the digest of ${other}`).toBe(digests.get(other));
    });
  }
});

describe("12 [ADM-94] the personal keys of a request digest, beyond the vectors", () => {
  const override = (kind: number, ...keys: string[]) =>
    fromVectorJson(MutationSchema, {
      mutation_id: "0579af62-c045-42bc-a979-5a9ce8f2e400",
      keyspace_id: "acme/clinic",
      op_type: "abpc.Override",
      lane: 1,
      reason: "fix",
      changes: keys.map((key) => ({ key, kind, value: { hex: "31" }, encoding: "json/v1", seen_version: "3" })),
    });
  const personal = (key: string): boolean => key.startsWith("patient/");

  it("the key of a personal OVERRIDE change is cleared, the key of another change stays, and no predicate clears none", () => {
    const withBoth = requestDigest(MutationSchema, override(6, "patient/p7/email", "stock/s1/qty"), personal);
    const other = requestDigest(MutationSchema, override(6, "patient/p9/phone", "stock/s1/qty"), personal);
    expect(toHex(withBoth.digest)).toBe(toHex(other.digest));
    expect(toHex(withBoth.canonical)).not.toContain(toHex(utf8("patient")));
    expect(toHex(withBoth.canonical)).toContain(toHex(utf8("stock/s1/qty")));
    const another = requestDigest(MutationSchema, override(6, "patient/p7/email", "stock/s2/qty"), personal);
    expect(toHex(another.digest)).not.toBe(toHex(withBoth.digest));
    const none = requestDigest(MutationSchema, override(6, "patient/p7/email", "stock/s1/qty"));
    expect(toHex(none.canonical)).toContain(toHex(utf8("patient/p7/email")));
  });

  it("only an OVERRIDE change is judged: the key of a change of another kind stays", () => {
    const set = requestDigest(MutationSchema, override(1, "patient/p7/email"), personal);
    expect(toHex(set.canonical)).toContain(toHex(utf8("patient/p7/email")));
  });
});

describe("07 [MUT-45] and 02 [ARC-61] beyond the vectors", () => {
  it("my decoder and protobuf-es's fromBinary give the same canonical bytes on well-formed messages", () => {
    for (const v of ofKind("record-hash")) {
      // record-hash-005 carries field 21 (blob_refs) with the wire type of a varint, which fromBinary reads as a string
      // and cannot read at all: the case of SPEC-QUESTIONS Q1, which `unmarshal` keeps as an unknown field
      if (JSON.stringify(v.record).includes("$unknown")) continue;
      const rec = fromVectorJson(RecordSchema, v.record);
      const wire = toBinary(RecordSchema, rec as RecordMessage);
      const mine = encode(RecordSchema, unmarshal(RecordSchema, wire));
      expect(toHex(mine), v.id).toBe(toHex(encode(RecordSchema, rec)));
      expect(toHex(mine), v.id).toBe(toHex(encode(RecordSchema, fromBinary(RecordSchema, wire))));
    }
  });

  it("unknown fields of a record and of a nested change survive a decode, in received order at their numbers", () => {
    // field 100 and 99 unknown to Record (a length-delimited and a varint), field 50 unknown to KeyChange
    const rec = fromVectorJson(RecordSchema, {
      seq: "3",
      log_epoch: 1,
      changes: [{ key: "k", kind: 2, $unknown: ["9203027879", "9003" + "05"] }],
      $unknown: ["a2060161", "9806" + "07", "a2060162"],
    });
    const wire = toBinary(RecordSchema, rec as RecordMessage);
    const back = unmarshal(RecordSchema, wire);
    expect(toHex(encode(RecordSchema, back))).toBe(toHex(encode(RecordSchema, rec)));
    const hex = toHex(encode(RecordSchema, back));
    // number order: 99 (varint 7), then 100 twice in received order
    expect(hex.endsWith("980607" + "a2060161" + "a2060162")).toBe(true);
  });

  it("a known field with a wire type that its kind does not have is refused by unmarshal, and kept as an unknown field by decode (07 [MUT-45], round 16)", () => {
    // KeyChange.key (1, a string) as a varint 5, then a valid kind: protobuf-es would read it as a string
    const wire = fromHex("0805" + "1001");
    expect(() => unmarshal(KeyChangeSchema, wire)).toThrow(CanonError);
    try {
      unmarshal(KeyChangeSchema, wire);
    } catch (e) {
      expect((e as CanonError).kind).toBe("wire-type");
      expect((e as CanonError).invalidEnvelope).toBe(true);
      expect((e as CanonError).path).toBe("key");
    }
    // decode runs no check (the caller has), and keeps the field as received, as Go's decoder does
    const m = decode(KeyChangeSchema, wire) as unknown as { key: string; kind: number; $unknown?: { no: number; wireType: number }[] };
    expect(m.key).toBe("");
    expect(m.kind).toBe(1);
    expect(m.$unknown?.map((u) => [u.no, u.wireType])).toEqual([[1, 0]]);
    expect(toHex(encode(KeyChangeSchema, m as never))).toBe("0805" + "1001");
  });

  it("a string is never repaired and a leading U+FEFF is kept", () => {
    const wire = fromHex("0a04efbbbf61"); // KeyChange.key = U+FEFF "a"
    const m = unmarshal(KeyChangeSchema, wire) as unknown as { key: string };
    expect(m.key).toBe("﻿a");
    expect(() => unmarshal(KeyChangeSchema, fromHex("0a01ff"))).toThrow(CanonError);
    expect(() => unmarshal(KeyChangeSchema, fromHex("0a02c080"))).toThrow(CanonError); // an overlong form
    expect(() => unmarshal(KeyChangeSchema, fromHex("0a03eda080"))).toThrow(CanonError); // a surrogate code point
  });

  it("a string with an unpaired surrogate has no canonical encoding", () => {
    const kc = create(KeyChangeSchema, { key: "a\ud800" });
    expect(() => encode(KeyChangeSchema, kc)).toThrow(CanonError);
  });

  it("never throws anything but a CanonError on arbitrary bytes (02 [ARC-46])", () => {
    let x = 99;
    const seed = [0x0a, 0x12, 0x32, 0x62, 0x80, 0x81, 0x00, 0xff, 0x03, 0x04, 0x01, 0x08, 0x5a, 0x7f];
    for (let n = 0; n < 4000; n++) {
      const b = new Uint8Array((n % 30) + 1);
      for (let i = 0; i < b.length; i++) {
        x = (x * 1103515245 + 12345) & 0x7fffffff;
        b[i] = seed[x % seed.length] as number;
      }
      for (const schema of [RecordSchema, MutationSchema]) {
        try {
          const m = unmarshal(schema, b);
          encode(schema, m);
        } catch (e) {
          if (!(e instanceof CanonError)) throw e;
        }
      }
    }
  });
});
