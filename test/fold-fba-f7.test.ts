// FBA batch F7, the client fold: S-01 (A2-S2, 05 [STATE-38] for a present CRDT key built from records) and S-02 (A2-S3,
// 05 [STATE-35], [STATE-45]: a nested marker deleted in the same record as a staged entity).

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { ChangeKind, KeyChangeSchema, RecordKind, RecordSchema, SystemEventSchema, SystemKind, type KeyChange, type Record as RecordMessage } from "../src/gen/abpc/v2/types_pb.js";
import { fromHex, toHex, utf8Encode } from "../src/rules/bytes.js";
import { applyRecord, ReplicaState, type Rules, type Template } from "../src/rules/fold/index.js";

const rules: Rules = {
  template(_version, key): Template | undefined {
    const seg = key.split("/");
    switch (seg[0]) {
      case "m":
        if (seg.length === 2) return { rule: "cas", entity: true, parent: "" };
        if (seg.length === 3) return { rule: "lww", entity: false, parent: "m/{i}" };
        if (seg.length === 4 && seg[2] === "c") return { rule: "cas", entity: true, parent: "m/{i}" };
        if (seg.length === 5 && seg[2] === "c") return { rule: "lww", entity: false, parent: "m/{i}/c/{j}" };
        return undefined;
      case "k":
        return seg.length === 2 ? { rule: "lww", entity: false, parent: "" } : undefined;
      case "doc":
        return seg.length === 2 ? { rule: "crdt", entity: false, parent: "" } : undefined;
      default:
        return undefined;
    }
  },
};

const json = (s: string): Uint8Array => utf8Encode(s);
const set = (key: string, value: string): KeyChange => create(KeyChangeSchema, { key, kind: ChangeKind.SET, value: json(value), encoding: "json/v1" });
const del = (key: string): KeyChange => create(KeyChangeSchema, { key, kind: ChangeKind.DELETE });
const casAbsent = (key: string, value: string): KeyChange =>
  create(KeyChangeSchema, { key, kind: ChangeKind.CAS, value: json(value), encoding: "json/v1", expected: { expect: { case: "absent", value: true } } });
const update = (key: string, hex: string): KeyChange => create(KeyChangeSchema, { key, kind: ChangeKind.CRDT_UPDATE, update: fromHex(hex), encoding: "yjs/v1" });

function op(seq: number, ...changes: KeyChange[]): RecordMessage {
  return create(RecordSchema, {
    seq: BigInt(seq),
    logEpoch: 1,
    kind: RecordKind.OP,
    manifestVersion: 1,
    mutationId: `m-${seq}`,
    serverTime: { seconds: BigInt(1000 + seq), nanos: 0 },
    principal: { sub: "u1" },
    changes,
  });
}

/** A SYSTEM `KEY_EXPIRED` record (06 [LOG-46]). */
function expired(seq: number, ...changes: KeyChange[]): RecordMessage {
  return create(RecordSchema, {
    seq: BigInt(seq),
    logEpoch: 1,
    kind: RecordKind.SYSTEM,
    manifestVersion: 1,
    serverTime: { seconds: BigInt(1000 + seq), nanos: 0 },
    system: create(SystemEventSchema, { kind: SystemKind.KEY_EXPIRED }),
    changes,
  });
}

describe("S-01: 05 [STATE-38] a present CRDT key built from records, under 09 [READ-16] overlapping delivery", () => {
  it("STATE38_CRDTKeyFromRecords_RedeliveryBelowTheVersionIsSkipped", () => {
    const st = new ReplicaState();
    const recs = [
      op(1, set("k/a", "1")),
      op(2, set("k/b", "1")),
      op(3, update("doc/d", "0101050004010174016100")), // a document by client 5
      expired(4, del("doc/d")), // it expires
      op(5, update("doc/d", "0101060004010174016200")), // a new document by client 6
    ];
    for (const r of recs) applyRecord(st, rules, r);
    const hash = toHex(st.keyspaceStateHash());
    const canon = toHex(st.key("doc/d")?.doc?.canonical() ?? new Uint8Array());
    // a delivery that overlaps from 3: nothing that the key holds moves back
    expect(applyRecord(st, rules, recs[2] as RecordMessage).skipped).toBe(1);
    expect(applyRecord(st, rules, recs[3] as RecordMessage).skipped).toBe(1);
    expect(st.key("doc/d")?.version).toBe(5n);
    expect(st.tombstone("doc/d")).toBeUndefined();
    expect(toHex(st.key("doc/d")?.doc?.canonical() ?? new Uint8Array())).toBe(canon);
    expect(toHex(st.keyspaceStateHash())).toBe(hash);
  });
});

describe("S-02: 05 [STATE-35], [STATE-45] a nested marker deleted with a staged entity", () => {
  for (const [name, changes] of [
    ["outer first", [del("m/x"), del("m/x/c/1")]],
    ["inner first", [del("m/x/c/1"), del("m/x")]],
  ] as const) {
    it(`STATE35_STATE45_NestedMarkerTombstoneKindIsOrderFree (${name})`, () => {
      const st = new ReplicaState();
      applyRecord(st, rules, op(1, casAbsent("m/x", "{}"), casAbsent("m/x/c/1", "{}"), set("m/x/c/1/b", '"b"')));
      const staged = (s: ReplicaState, _rec: RecordMessage, key: string): boolean => key === "m/x" && s.key(key) !== undefined;
      applyRecord(st, rules, op(2, ...changes), { staged });
      expect(st.tombstone("m/x")).toMatchObject({ kind: "staged", version: 2n });
      expect(st.tombstone("m/x/c/1")).toMatchObject({ kind: "staged", version: 2n });
      expect(st.key("m/x/c/1/b")).toBeUndefined();
    });
  }
});
