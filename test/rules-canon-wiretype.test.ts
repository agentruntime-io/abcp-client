// 07 [MUT-45] (round 16, Q1 and Q2): a known field that arrives with a wire type that its kind does not have, and a group tag on
// any field, are malformed in the same way as an encoding that is not in its shortest form. The tests are the twins of
// server/internal/canon/wiretype_test.go.

import type { DescMessage } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { MutateRequestSchema, MutationSchema } from "../src/gen/abpc/v2/sync_pb.js";
import { KeyChangeSchema, RecordSchema, RejectSchema } from "../src/gen/abpc/v2/types_pb.js";
import {
  CanonError,
  checkEncoding,
  checkShortest,
  checkUtf8,
  checkUtf8Request,
  checkWireTypes,
  unmarshal,
  type CanonKind,
} from "../src/rules/canon/index.js";

const varint = (num: number, v: number): number[] => [...tag(num, 0), v];
const bytesField = (num: number, payload: number[]): number[] => [...tag(num, 2), payload.length, ...payload];
const fixed32 = (num: number): number[] => [...tag(num, 5), 7, 0, 0, 0];
const fixed64 = (num: number): number[] => [...tag(num, 1), 7, 0, 0, 0, 0, 0, 0, 0];
function tag(num: number, wt: number): number[] {
  let t = num * 8 + wt;
  const out: number[] = [];
  while (t >= 128) {
    out.push((t % 128) | 128);
    t = Math.floor(t / 128);
  }
  out.push(t);
  return out;
}
const group = (num: number, inner: number[] = []): number[] => [...tag(num, 3), ...inner, ...tag(num, 4)];
const u8 = (...parts: number[][]): Uint8Array => Uint8Array.from(parts.flat());

function refusal(f: () => void): CanonError | undefined {
  try {
    f();
  } catch (e) {
    if (!(e instanceof CanonError)) throw e;
    return e;
  }
  return undefined;
}

describe("07 [MUT-45]: a known field with a wire type that its kind does not have", () => {
  // Mutation: 1 mutation_id (string), 3 op_type (string), 4 lane (enum), 5 changes (message, repeated), 8 client_seq (uint64),
  // 12 required_features (repeated string). KeyChange: 1 key, 3 value (bytes). Record: 12 overwrote_newer (repeated uint32).
  // Reject: 8 details (map of strings).
  const cases: [string, DescMessage, Uint8Array, string][] = [
    ["a string as a varint", MutationSchema, u8(varint(3, 5)), "op_type"],
    ["a string as a fixed32", MutationSchema, u8(fixed32(3)), "op_type"],
    ["a string as a fixed64", MutationSchema, u8(fixed64(1)), "mutation_id"],
    ["an enum as bytes", MutationSchema, u8(bytesField(4, [1])), "lane"],
    ["a repeated message as a varint", MutationSchema, u8(varint(5, 1)), "changes"],
    ["a repeated string as a varint", MutationSchema, u8(varint(12, 1)), "required_features"],
    ["a uint64 as bytes", MutationSchema, u8(bytesField(8, [1])), "client_seq"],
    ["a uint64 as a fixed64", MutationSchema, u8(fixed64(8)), "client_seq"],
    ["a bytes field as a varint", KeyChangeSchema, u8(varint(3, 1)), "value"],
    ["in a nested message", MutationSchema, u8(bytesField(5, varint(1, 9))), "changes.key"],
    ["a repeated numeric field as a fixed32", RecordSchema, u8(fixed32(12)), "overwrote_newer"],
    ["a map entry as a varint", RejectSchema, u8(varint(8, 1)), "details"],
    ["a map key as a varint", RejectSchema, u8(bytesField(8, varint(1, 1))), "details.key"],
    ["a string as it should be", MutationSchema, u8(bytesField(3, [111, 112])), ""],
    ["an enum as a varint", MutationSchema, u8(varint(4, 1)), ""],
    ["a uint64 as a varint", MutationSchema, u8(varint(8, 3)), ""],
    ["an unknown field of every wire type", MutationSchema, u8(varint(900, 1), fixed32(901), fixed64(902), bytesField(903, [120])), ""],
    ["a bytes field as bytes", KeyChangeSchema, u8(bytesField(3, [1, 2])), ""],
    ["a nested message as it should be", MutationSchema, u8(bytesField(5, bytesField(1, [107]))), ""],
    ["a repeated numeric field, unpacked", RecordSchema, u8(varint(12, 1), varint(12, 2)), ""],
    ["a repeated numeric field, packed", RecordSchema, u8(bytesField(12, [1, 2])), ""],
  ];
  for (const [name, schema, bytes, path] of cases) {
    it(name, () => {
      const checks: [string, () => void][] = [
        ["checkShortest", () => checkShortest(schema, bytes)],
        ["checkEncoding", () => checkEncoding(schema, bytes)],
        ["checkWireTypes", () => checkWireTypes(schema, bytes)],
      ];
      for (const [fn, f] of checks) {
        const e = refusal(f);
        if (path === "") {
          expect(e, fn).toBeUndefined();
          continue;
        }
        expect(e, fn).toBeDefined();
        expect(e?.kind, fn).toBe("wire-type" satisfies CanonKind);
        expect(e?.path, fn).toBe(path);
        expect(e?.invalidEnvelope, fn).toBe(true);
      }
      // the string and UTF-8 check alone does not look at wire types
      expect(refusal(() => checkUtf8(schema, bytes))).toBeUndefined();
    });
  }

  it("a wire type and a non-shortest form are one refusal: the first in the bytes is reported; a string that is not UTF-8 comes before both", () => {
    const nonShortest = [0x40, 0x81, 0x00]; // field 8 (client_seq) varint 1 as two octets
    const wrongWire = [0x18, 0x05]; // field 3 (op_type, a string) as a varint
    const badUtf8 = [0x0a, 0x01, 0xff]; // field 1 (mutation_id) holding 0xff
    expect(refusal(() => checkShortest(MutationSchema, u8(nonShortest, wrongWire)))?.kind).toBe("not-shortest");
    expect(refusal(() => checkShortest(MutationSchema, u8(wrongWire, nonShortest)))?.kind).toBe("wire-type");
    expect(refusal(() => checkWireTypes(MutationSchema, u8(nonShortest)))).toBeUndefined();
    const e = refusal(() => checkEncoding(MutationSchema, u8(wrongWire, badUtf8)));
    expect(e?.kind).toBe("invalid-utf8");
    expect(e?.invalidEnvelope).toBe(false);
    expect(refusal(() => unmarshal(MutationSchema, u8(wrongWire)))?.kind).toBe("wire-type");
  });
});

describe("07 [MUT-45]: a group tag is refused as a wire type that does not fit", () => {
  const cases: [string, Uint8Array, CanonKind][] = [
    ["an unknown field as a group", u8(group(900, varint(1, 7))), "wire-type"],
    ["a known field as a group", u8(group(3)), "wire-type"],
    ["a group inside a group", u8(group(900, group(901))), "wire-type"],
    ["an end tag with no start", u8(tag(900, 4)), "wire-type"],
    ["a group that is not closed", u8(tag(900, 3)), "malformed"],
    ["a group closed by another field's end tag", u8(tag(900, 3), tag(901, 4)), "malformed"],
    ["a wire type that no message has", u8(tag(900, 6)), "malformed"],
  ];
  for (const [name, bytes, kind] of cases) {
    it(name, () => {
      for (const [fn, f] of [
        ["checkShortest", () => checkShortest(MutationSchema, bytes)],
        ["checkEncoding", () => checkEncoding(MutationSchema, bytes)],
        ["checkWireTypes", () => checkWireTypes(MutationSchema, bytes)],
        ["checkUtf8", () => checkUtf8(MutationSchema, bytes)],
      ] as [string, () => void][]) {
        const e = refusal(f);
        expect(e?.kind, fn).toBe(kind);
        expect(e?.invalidEnvelope, fn).toBe(kind === "wire-type");
      }
    });
  }

  it("a group is passed over with its inside, so that a later refusal does not hide behind it", () => {
    expect(refusal(() => checkShortest(MutationSchema, u8(group(900), [0x40, 0x81, 0x00])))?.kind).toBe("wire-type");
    expect(refusal(() => checkEncoding(MutationSchema, u8([0x0a, 0x01, 0xff], group(900))))?.kind).toBe("invalid-utf8");
  });

  it("in a MutateRequest, checkUtf8 refuses the request, checkUtf8Request leaves the group to the mutation's own check", () => {
    const mutation = [...bytesField(1, [109, 49]), ...group(90, varint(1, 7))];
    const request = u8(bytesField(1, mutation));
    expect(refusal(() => checkUtf8(MutateRequestSchema, request))?.kind).toBe("wire-type");
    expect(refusal(() => checkUtf8Request(MutateRequestSchema, request))).toBeUndefined();
    const e = refusal(() => checkShortest(MutationSchema, Uint8Array.from(mutation)));
    expect(e?.kind).toBe("wire-type");
    expect(e?.invalidEnvelope).toBe(true);
    // a group that is not closed does not parse, and fails the request as a whole
    const open = refusal(() => checkUtf8Request(MutateRequestSchema, u8(bytesField(1, tag(90, 3)))));
    expect(open?.kind).toBe("malformed");
    expect(open?.invalidEnvelope).toBe(false);
  });

  it("deep groups and a long run of wrong wire types are bounded (02 [ARC-46])", () => {
    const deep = new Uint8Array(2 * 20000 * 2);
    const open = tag(900, 3);
    const close = tag(900, 4);
    let p = 0;
    for (let i = 0; i < 20000; i++) for (const x of open) deep[p++] = x;
    for (let i = 0; i < 20000; i++) for (const x of close) deep[p++] = x;
    expect(refusal(() => checkShortest(MutationSchema, deep.subarray(0, p)))?.kind).toBe("malformed");
    const many = new Uint8Array(2 * 100000);
    for (let i = 0; i < 100000; i++) {
      many[2 * i] = 0x18;
      many[2 * i + 1] = 1;
    }
    expect(refusal(() => checkShortest(MutationSchema, many))?.kind).toBe("wire-type");
  });
});
