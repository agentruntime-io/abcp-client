// What a client does with the fold that no vector of 17 [CONF-9] spells out for it: the version-gated test of 05 [STATE-38]
// (14 [CLI-2], 09 [READ-16]), the stops of 06 [LOG-11] (14 [CLI-4]), the gap check (14 [CLI-3]), the entries of a load
// (09 [READ-9], [READ-13]) and the typed refusals (02 [ARC-46], 16 [SEC-3]).

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import {
  ChangeKind,
  DecisionStatus,
  KeyChangeSchema,
  KeyEntrySchema,
  RecordKind,
  RecordSchema,
  SystemEventSchema,
  SystemKind,
  DecisionSchema,
  type KeyChange,
  type Record as RecordMessage,
} from "../src/gen/abpc/v2/types_pb.js";
import { fromHex, toHex, utf8Encode } from "../src/rules/bytes.js";
import {
  applyRecord,
  FoldError,
  installEntry,
  ReplicaState,
  StateRedacted,
  StateValue,
  type Rules,
  type Template,
} from "../src/rules/fold/index.js";

const rules: Rules = {
  template(_version, key): Template | undefined {
    const seg = key.split("/");
    switch (seg[0]) {
      case "m":
        if (seg.length === 2) return { rule: "cas", entity: true, parent: "" };
        if (seg.length === 3) return { rule: "lww", entity: false, parent: "m/{i}" };
        return undefined;
      case "k":
        return seg.length === 2 ? { rule: "lww", entity: false, parent: "" } : undefined;
      case "n":
        return seg.length === 2 ? { rule: "counter", entity: false, parent: "" } : undefined;
      case "doc":
        return seg.length === 2 ? { rule: "crdt", entity: false, parent: "" } : undefined;
      default:
        return undefined;
    }
  },
};

const json = (s: string): Uint8Array => utf8Encode(s);

function set(key: string, value: string): KeyChange {
  return create(KeyChangeSchema, { key, kind: ChangeKind.SET, value: json(value), encoding: "json/v1" });
}
const del = (key: string): KeyChange => create(KeyChangeSchema, { key, kind: ChangeKind.DELETE });
/** A `CAS` of a `cas` key (05 [STATE-19]); the record carries the change as authored, with its expectation (05 [STATE-24]). */
const casAbsent = (key: string, value: string): KeyChange =>
  create(KeyChangeSchema, { key, kind: ChangeKind.CAS, value: json(value), encoding: "json/v1", expected: { expect: { case: "absent", value: true } } });
const incr = (key: string, delta: bigint): KeyChange => create(KeyChangeSchema, { key, kind: ChangeKind.INCR, delta });
const update = (key: string, hex: string): KeyChange =>
  create(KeyChangeSchema, { key, kind: ChangeKind.CRDT_UPDATE, update: fromHex(hex), encoding: "yjs/v1" });

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

const text = (b: Uint8Array | undefined): string => (b === undefined ? "" : new TextDecoder().decode(b));

describe("05 [STATE-38] the version-gated test and 09 [READ-16] overlapping delivery", () => {
  it("a record at or below the cursor changes nothing that the state already holds: a counter is never counted twice", () => {
    const st = new ReplicaState();
    applyRecord(st, rules, op(1, incr("n/a", 5n), set("k/a", "1")));
    applyRecord(st, rules, op(2, incr("n/a", 3n), set("k/a", "2")));
    expect(text(st.key("n/a")?.value)).toBe("8");
    const again = applyRecord(st, rules, op(2, incr("n/a", 3n), set("k/a", "2")));
    expect(again).toEqual({ moved: false, skipped: 2 });
    const older = applyRecord(st, rules, op(1, incr("n/a", 5n), set("k/a", "1")));
    expect(older).toEqual({ moved: false, skipped: 2 });
    expect(text(st.key("n/a")?.value)).toBe("8");
    expect(text(st.key("k/a")?.value)).toBe("2");
    expect(st.seq).toBe(2n);
    expect(st.key("k/a")?.version).toBe(2n);
  });

  it("a tombstone gates too: a change below the deletion seq is skipped, one above it applies", () => {
    const st = new ReplicaState();
    applyRecord(st, rules, op(1, set("k/a", "1")));
    applyRecord(st, rules, op(2, del("k/a")));
    applyRecord(st, rules, op(3, set("k/b", "1")));
    expect(st.tombstone("k/a")).toMatchObject({ kind: "key", version: 2n });
    // an overlapping delivery of the SET at seq 1 (older than the tombstone) does not bring the key back
    expect(applyRecord(st, rules, op(1, set("k/a", "1")))).toEqual({ moved: false, skipped: 1 });
    expect(st.key("k/a")).toBeUndefined();
    // a SET above the tombstone makes the key present again and drops the tombstone (05 [STATE-37])
    applyRecord(st, rules, op(4, set("k/a", "9")));
    expect(text(st.key("k/a")?.value)).toBe("9");
    expect(st.tombstone("k/a")).toBeUndefined();
  });

  it("the deletion seq of a deleted ancestor marker gates the keys of its subtree", () => {
    const st = new ReplicaState();
    applyRecord(st, rules, op(1, casAbsent("m/x", "{}"), set("m/x/title", '"a"')));
    applyRecord(st, rules, op(2, del("m/x")));
    expect(st.tombstone("m/x")).toMatchObject({ kind: "entity", version: 2n });
    expect(st.key("m/x/title")).toBeUndefined();
    applyRecord(st, rules, op(3, set("k/z", "1")));
    // a record of seq 2 that changes a key of the subtree (which a server never writes after the deletion, but an overlapping
    // delivery of an older record would be) is skipped: the covering tombstone is at seq 2
    expect(applyRecord(st, rules, op(2, set("m/x/body", '"b"'))).skipped).toBe(1);
    expect(st.key("m/x/body")).toBeUndefined();
    // above it, a change of the subtree applies as any change does (the server's parent rule judged it, 05 [STATE-46])
    expect(applyRecord(st, rules, op(4, set("m/x/body", '"c"'))).skipped).toBe(0);
    expect(text(st.key("m/x/body")?.value)).toBe("\"c\"");
  });

  it("a present CRDT key is gated by the crdt_covered_seq of the base it holds, not by its version (05 [STATE-38], [STATE-75])", () => {
    const st = new ReplicaState();
    // the key as a load gives it: a base at seq 5 that covers one update by client 5
    installEntry(
      st,
      create(KeyEntrySchema, {
        key: "doc/d",
        value: fromHex("0101050004010174016100"),
        encoding: "yjs/v1",
        version: 5n,
        crdtCoveredSeq: 5n,
        changedBy: "u1",
      }),
      rules,
      1,
    );
    st.setCursor(5n, 1, 1);
    // a record at seq 5 is already in the base: skipped
    expect(applyRecord(st, rules, op(5, update("doc/d", "01010501840500016200"))).skipped).toBe(1);
    // the next record applies, and an overlapping delivery of it folds to no effect and never takes the version back
    applyRecord(st, rules, op(6, update("doc/d", "01010501840500016200")));
    const row = st.key("doc/d");
    expect(row?.version).toBe(6n);
    expect(toHex(row?.doc?.canonical() ?? new Uint8Array())).toBe("000000010000000000000005000000000000000200000000");
    const dup = applyRecord(st, rules, op(6, update("doc/d", "01010501840500016200")));
    expect(dup).toEqual({ moved: false, skipped: 0 });
    expect(st.key("doc/d")?.version).toBe(6n);
    expect(toHex(st.key("doc/d")?.doc?.canonical() ?? new Uint8Array())).toBe("000000010000000000000005000000000000000200000000");
  });
});

describe("14 [CLI-3], [CLI-4], 06 [LOG-11] gaps and stops", () => {
  it("refuses a record above the next one: a client never infers contiguity", () => {
    const st = new ReplicaState();
    applyRecord(st, rules, op(1, set("k/a", "1")));
    try {
      applyRecord(st, rules, op(3, set("k/b", "1")));
      throw new Error("not refused");
    } catch (e) {
      expect(e).toBeInstanceOf(FoldError);
      expect((e as FoldError).code).toBe("seq");
    }
    expect(st.seq).toBe(1n);
    expect(st.key("k/b")).toBeUndefined();
  });

  it("stops at a record of an unknown kind, an unsupported required feature and an unknown SYSTEM kind that must be understood or has changes", () => {
    const st = new ReplicaState();
    applyRecord(st, rules, op(1, set("k/a", "1")));
    const unknownKind = create(RecordSchema, { seq: 2n, logEpoch: 1, kind: 99 as RecordKind, manifestVersion: 1 });
    const feature = create(RecordSchema, { ...op(2, set("k/b", "1")), requiredFeatures: ["future.v9"] });
    const sysMust = create(RecordSchema, {
      seq: 2n,
      logEpoch: 1,
      kind: RecordKind.SYSTEM,
      manifestVersion: 1,
      system: create(SystemEventSchema, { kind: 99 as SystemKind, mustUnderstand: true }),
    });
    const sysChanges = create(RecordSchema, {
      seq: 2n,
      logEpoch: 1,
      kind: RecordKind.SYSTEM,
      manifestVersion: 1,
      system: create(SystemEventSchema, { kind: 99 as SystemKind }),
      changes: [set("k/b", "1")],
    });
    for (const rec of [unknownKind, feature, sysMust, sysChanges]) {
      let err: unknown;
      try {
        applyRecord(st, rules, rec);
      } catch (e) {
        err = e;
      }
      expect(err).toBeInstanceOf(FoldError);
      expect((err as FoldError).stop, (err as FoldError).message).toBe(true);
    }
    expect(st.seq).toBe(1n); // frozen at the previous seq, nothing partly applied
    expect(st.key("k/b")).toBeUndefined();
    // an unknown SYSTEM kind that need not be understood and has no change is skipped, and the cursor moves on (06 [LOG-41])
    const skippable = create(RecordSchema, {
      seq: 2n,
      logEpoch: 1,
      kind: RecordKind.SYSTEM,
      manifestVersion: 1,
      system: create(SystemEventSchema, { kind: 99 as SystemKind }),
    });
    expect(applyRecord(st, rules, skippable)).toEqual({ moved: true, skipped: 0 });
    expect(st.seq).toBe(2n);
  });

  it("applies a record whole or not at all (02 [ARC-28]): a change that cannot be applied leaves the state as it was", () => {
    const st = new ReplicaState();
    applyRecord(st, rules, op(1, set("k/a", "1")));
    const before = toHex(st.keyspaceStateHash());
    // the second change is a CRDT update that is not a Yjs update
    expect(() => applyRecord(st, rules, op(2, set("k/b", "2"), update("doc/d", "ff")))).toThrow(FoldError);
    expect(toHex(st.keyspaceStateHash())).toBe(before);
    expect(st.seq).toBe(1n);
    // a key that no template of the version matches
    expect(() => applyRecord(st, rules, op(2, set("zzz/q", "2")))).toThrow(FoldError);
    // one action never changes a key twice
    expect(() => applyRecord(st, rules, op(2, set("k/c", "1"), set("k/c", "2")))).toThrow(FoldError);
    expect(st.key("k/c")).toBeUndefined();
    // a record of seq 0 is no record
    expect(() => applyRecord(st, rules, op(0, set("k/c", "1")))).toThrow(FoldError);
  });

  it("applies the intent row of an INTENT and of a REJECTED DECISION only (05 [STATE-34])", () => {
    const st = new ReplicaState();
    const row = "abpc.intent/i-1";
    const intent = create(RecordSchema, { ...op(1, set("k/a", "1"), set(row, '{"status":"PENDING"}')), kind: RecordKind.INTENT, mutationId: "i-1" });
    applyRecord(st, rules, intent);
    expect(st.key("k/a")).toBeUndefined();
    expect(text(st.key(row)?.value)).toBe('{"status":"PENDING"}');
    const rejected = create(RecordSchema, {
      ...op(2, set("k/a", "1"), set(row, '{"status":"REJECTED"}')),
      kind: RecordKind.DECISION,
      mutationId: "i-1",
      decision: create(DecisionSchema, { intentId: "i-1", status: DecisionStatus.REJECTED }),
    });
    applyRecord(st, rules, rejected);
    expect(st.key("k/a")).toBeUndefined();
    expect(text(st.key(row)?.value)).toBe('{"status":"REJECTED"}');
    const committed = create(RecordSchema, {
      ...op(3, set("k/a", "1"), set(row, '{"status":"COMMITTED"}')),
      kind: RecordKind.DECISION,
      mutationId: "i-1",
      decision: create(DecisionSchema, { intentId: "i-1", status: DecisionStatus.COMMITTED }),
    });
    applyRecord(st, rules, committed);
    expect(text(st.key("k/a")?.value)).toBe("1");
  });

  it("judges the key and the envelope of the requested changes that an INTENT and a REJECTED DECISION do not apply, and nothing else of them (16 [SEC-3], R19 review note 4)", () => {
    const row = "abpc.intent/i-1";
    const intentOf = (...changes: KeyChange[]): RecordMessage => create(RecordSchema, { ...op(1, ...changes), kind: RecordKind.INTENT, mutationId: "i-1" });
    const rejectedOf = (...changes: KeyChange[]): RecordMessage =>
      create(RecordSchema, { ...op(1, ...changes), kind: RecordKind.DECISION, mutationId: "i-1", decision: create(DecisionSchema, { intentId: "i-1", status: DecisionStatus.REJECTED }) });
    for (const make of [intentOf, rejectedOf]) {
      // a key that breaks the segment syntax, and a change that breaks the envelope of its kind, in a requested change
      expect(() => applyRecord(new ReplicaState(), rules, make(set("k/..", "1"), set(row, "{}")))).toThrow(FoldError);

      expect(() => applyRecord(new ReplicaState(), rules, make(create(KeyChangeSchema, { key: "k/a", kind: ChangeKind.DELETE, value: utf8Encode("1") }), set(row, "{}")))).toThrow(FoldError);
      // a requested change with a key that matches no template, and a value that no schema would take, is not judged: it is not applied
      const st = new ReplicaState();
      applyRecord(st, rules, make(set("zzz/q", "2"), set("k/a", "not json"), set("abpc.saga.v1/saga/s1", "{}"), set(row, "{}"))); // nor its reserved namespace: vector fold-moved-002
      expect(st.key("zzz/q")).toBeUndefined();
    }
  });
});

describe("09 [READ-9], [READ-13] the entries of a load", () => {
  it("installs a value, a null, the redacted state and the three kinds of tombstone, the kind taken from the key and `staged`", () => {
    const st = new ReplicaState();
    const entries = [
      create(KeyEntrySchema, { key: "k/a", value: json("1"), encoding: "json/v1", version: 7n, changedBy: "u1" }),
      create(KeyEntrySchema, { key: "k/b", isNull: true, encoding: "json/v1", version: 8n }),
      create(KeyEntrySchema, { key: "k/c", encoding: "redacted/v1", redacted: true, version: 9n, changedBy: "u9" }),
      create(KeyEntrySchema, { key: "m/gone", deleted: true, version: 10n }),
      create(KeyEntrySchema, { key: "m/staged", deleted: true, staged: true, version: 11n }),
      create(KeyEntrySchema, { key: "k/dead", deleted: true, redacted: true, version: 12n }),
      create(KeyEntrySchema, { key: "n/c", value: json("41"), encoding: "", version: 13n }),
    ];
    for (const e of entries) installEntry(st, e, rules, 1);
    st.setCursor(13n, 1, 1);
    expect(st.key("k/a")?.state).toBe(StateValue);
    expect(st.key("k/b")?.state).toBe(2);
    expect(st.key("k/c")?.state).toBe(StateRedacted);
    expect(st.key("k/c")?.changedBy).toBe("");
    expect(st.tombstone("m/gone")).toMatchObject({ kind: "entity", version: 10n, mark: false });
    expect(st.tombstone("m/staged")).toMatchObject({ kind: "staged", version: 11n });
    expect(st.tombstone("k/dead")).toMatchObject({ kind: "key", version: 12n, mark: true });
    // a tombstone is never shown as a key
    expect(st.key("k/dead")).toBeUndefined();
    expect(st.keyNames()).toEqual(["k/a", "k/b", "k/c", "n/c"]);
    // a counter hashes as json/v1 whatever the entry's encoding says (05 [STATE-41])
    expect(st.keyState("n/c")?.counter).toBe(true);
    // the tail is gated by the loaded versions and tombstones: the entity tombstone at 10 covers the subtree
    expect(applyRecord(st, rules, op(10, set("m/gone/title", '"x"'))).skipped).toBe(1);
    expect(applyRecord(st, rules, op(14, incr("n/c", 1n)))).toEqual({ moved: true, skipped: 0 });
    expect(text(st.key("n/c")?.value)).toBe("42");
  });

  it("a load entry replaces what the state held for the key, a tombstone included", () => {
    const st = new ReplicaState();
    applyRecord(st, rules, op(1, set("k/a", "1")));
    applyRecord(st, rules, op(2, del("k/a")));
    expect(st.tombstone("k/a")).toBeDefined();
    installEntry(st, create(KeyEntrySchema, { key: "k/a", value: json("5"), encoding: "json/v1", version: 3n }), rules, 1);
    expect(st.tombstone("k/a")).toBeUndefined();
    expect(text(st.key("k/a")?.value)).toBe("5");
  });
});
