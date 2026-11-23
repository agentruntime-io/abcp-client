// The rules that the Go canon package gained after the client's first port (C1): V1 SHOULD-1 (hashing a mutation of many unknown fields
// without a copy of the list for each, 07 [MUT-45]) and V1 SHOULD-3 (06 [LOG-71]: one value digest for each change and each `previous`
// entry). The tests are the twins of server/internal/canon/v1_test.go.

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { MutationSchema } from "../src/gen/abcp/v2/sync_pb.js";
import { KeyChangeSchema, KeyEntrySchema, RecordSchema, ChangeKind } from "../src/gen/abcp/v2/types_pb.js";
import { contentDigest, contentHashInput, hashRecordFromDigests } from "../src/rules/canon/index.js";
import { toHex } from "../src/rules/bytes.js";

describe("06 [LOG-71] (V1 SHOULD-3): one value digest for each change and one for each previous entry", () => {
  const record = create(RecordSchema, {
    seq: 4n,
    logEpoch: 1,
    changes: [create(KeyChangeSchema, { key: "a", kind: ChangeKind.SET }), create(KeyChangeSchema, { key: "b", kind: ChangeKind.DELETE })],
    previous: [create(KeyEntrySchema, { key: "c" })],
  });
  const digest = (i: number): Uint8Array => new Uint8Array(32).fill(i);

  it("a wrong count is refused, for the content digest and for the record hash that is built from it", () => {
    for (const n of [0, 1, 2, 4]) {
      const digests = Array.from({ length: n }, (_, i) => digest(i));
      expect(() => contentDigest(record, digests), `${n} digests`).toThrow(RangeError);
      expect(() => hashRecordFromDigests(record, digests, new Uint8Array(32)), `${n} digests`).toThrow(/want 3/);
    }
  });

  it("the right count gives the digest and the hash", () => {
    const digests = [digest(1), digest(2), digest(3)];
    const { c } = contentDigest(record, digests);
    expect(c).toHaveLength(32);
    const h = hashRecordFromDigests(record, digests, new Uint8Array(32));
    expect(toHex(h.c)).toBe(toHex(c));
    expect(h.valueDigests).toHaveLength(3);
    // a record with no change and no previous entry takes none
    expect(() => contentDigest(create(RecordSchema, { seq: 1n }), [])).not.toThrow();
    expect(() => contentDigest(create(RecordSchema, { seq: 1n }), [digest(1)])).toThrow(RangeError);
  });
});

describe("07 [MUT-45] (V1 SHOULD-1): the unknown fields of a mutation are written at the place of their numbers without a copy of the list", () => {
  /** An unknown field of a varint, which is all that these tests need: its wire bytes are the tag and one octet. */
  const field = (no: number, v: number): { no: number; wireType: number; data: Uint8Array } => ({ no, wireType: 0, data: Uint8Array.of(v & 0x7f) });
  const tag = (no: number): number[] => {
    const t = no * 8; // wire type 0
    return t < 128 ? [t] : t < 16384 ? [(t & 127) | 128, t >> 7] : [(t & 127) | 128, ((t >> 7) & 127) | 128, t >> 14];
  };
  const wire = (f: { no: number; data: Uint8Array }): number[] => [...tag(f.no), ...f.data];

  function mutationWith(fields: { no: number; wireType: number; data: Uint8Array }[]): ReturnType<typeof create<typeof MutationSchema>> {
    const m = create(MutationSchema, { mutationId: "m" });
    (m as { $unknown?: unknown }).$unknown = fields;
    return m;
  }

  /** The canonical bytes of the known fields that these tests set: mutation_id (field 1 of Mutation is the id), plus the unknown fields. */
  const known = contentHashInput(create(MutationSchema, { mutationId: "m" }));

  let seed = 12345;
  const rand = (n: number): number => {
    seed = (Math.imul(seed, 1103515245) + 12345) & 0x7fffffff;
    return seed % n;
  };

  it("fields in the order of their numbers, and fields in any order, give the bytes of a stable sort by number", () => {
    const n = 20000;
    // numbers above every field of Mutation, many of them equal, so that the order within a number is tested too
    const received = Array.from({ length: n }, (_, i) => field(1000 + rand(300), i));
    const ordered = [...received].sort((a, b) => a.no - b.no); // a stable sort: the order received within a number
    const want = [...known, ...ordered.flatMap(wire)];
    expect(toHex(contentHashInput(mutationWith(received)))).toBe(toHex(Uint8Array.from(want)));
    expect(toHex(contentHashInput(mutationWith(ordered)))).toBe(toHex(Uint8Array.from(want)));
    // and in reverse: the fields of one number come out in the order received (reversed), the numbers in increasing order
    const reversed = [...received].reverse();
    const wantReversed = [...known, ...[...reversed].sort((a, b) => a.no - b.no).flatMap(wire)];
    expect(toHex(contentHashInput(mutationWith(reversed)))).toBe(toHex(Uint8Array.from(wantReversed)));
  });

  it("a field below the numbers of the known fields is written before them, and one above after", () => {
    // Mutation has fields numbered from 1; an unknown field of number 99999 comes after every known one
    const m = mutationWith([field(99999, 7), field(99998, 1)]);
    const bytes = contentHashInput(m);
    expect(toHex(bytes)).toBe(toHex(Uint8Array.from([...known, ...wire(field(99998, 1)), ...wire(field(99999, 7))])));
  });

  it("hashing makes no copy of the list of fields, in order or out of it: no map, sort, slice or spread of an array of the size of the list", () => {
    const n = 5000;
    const inOrder = Array.from({ length: n }, (_, i) => field(1000 + i, i));
    const outOfOrder = Array.from({ length: n }, (_, i) => field(1000 + ((i * 7919) % n), i));
    for (const [name, fields] of [
      ["in order", inOrder],
      ["out of order", outOfOrder],
    ] as const) {
      const m = mutationWith(fields);
      const seen: string[] = [];
      const orig = {
        map: Array.prototype.map,
        sort: Array.prototype.sort,
        slice: Array.prototype.slice,
        filter: Array.prototype.filter,
        from: Array.from,
      };
      const watch = <K extends "map" | "sort" | "slice" | "filter">(k: K): void => {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (Array.prototype as any)[k] = function (this: unknown[], ...args: unknown[]) {
          if (this.length >= n) seen.push(`${k} of ${this.length}`);
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          return (orig[k] as any).apply(this, args);
        };
      };
      watch("map");
      watch("sort");
      watch("slice");
      watch("filter");
      try {
        contentHashInput(m);
      } finally {
        Array.prototype.map = orig.map;
        Array.prototype.sort = orig.sort;
        Array.prototype.slice = orig.slice;
        Array.prototype.filter = orig.filter;
      }
      expect(seen, `${name}: a copy of the list of unknown fields`).toEqual([]);
    }
  });
});
