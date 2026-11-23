// The rules that the Go fold gained after the client's first port (C1), brought to the client's fold: the verifier fixes V1, V2 and V6
// of the Go packages and the rulings of docs/SPEC-QUESTIONS.md (Q11, Q12, Q13). Each test is the twin of one of the Go tests named in
// its comment (server/internal/fold/order_test.go and v1_test.go), written for the client's `applyRecord`.

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import {
  ChangeKind,
  DecisionSchema,
  DecisionStatus,
  KeyChangeSchema,
  RecordKind,
  RecordSchema,
  SystemEventSchema,
  SystemKind,
  type KeyChange,
  type Record as RecordMessage,
} from "../src/gen/abcp/v2/types_pb.js";
import { fromHex, toHex, utf8Encode } from "../src/rules/bytes.js";
import {
  type Anomaly,
  applyRecord,
  FoldError,
  type FoldCode,
  ReplicaState,
  StateRedacted,
  VersionTopology,
  type Rules,
} from "../src/rules/fold/index.js";
import { VectorRules } from "./helpers/rules.js";

// ---- the manifests of the unit tests, in the shorthand of the vectors (as server/internal/fold/helpers_test.go) ----

const MANIFEST_V1 = {
  version: 1,
  templates: {
    "issue/{i}": { rule: "cas", entity: true },
    "issue/{i}/title": { rule: "lww" },
    "issue/{i}/comment/{c}": { rule: "cas", entity: true },
    "issue/{i}/comment/{c}/body": { rule: "lww" },
    "v/{k}": { rule: "lww" },
    "c/{k}": { rule: "counter" },
    "d/{k}": { rule: "crdt:yjs/v1" },
    "old/{k}": { rule: "lww" },
    "oldd/{k}": { rule: "crdt:yjs/v1" },
    top: { rule: "lww" },
    "free/{k}": { rule: "cas" },
  },
};
const MANIFEST_V2 = {
  version: 2,
  templates: {
    "issue/{i}": { rule: "cas", entity: true },
    "issue/{i}/title": { rule: "lww" },
    "v/{k}": { rule: "lww" },
    "old/{k}": { rule: "lww" },
    "oldd/{k}": { rule: "crdt:yjs/v1" },
    "only2/{k}": { rule: "lww" },
    "new/{k}": { rule: "lww" },
    "newd/{k}": { rule: "crdt:yjs/v1" },
  },
};

function newRules(): VectorRules {
  const r = new VectorRules();
  r.addManifest(MANIFEST_V1);
  r.addManifest(MANIFEST_V2);
  return r;
}

// ---- builders (as the helpers of server/internal/fold) ----

const bytes = (s: string): Uint8Array => utf8Encode(s);

function set(key: string, value: string): KeyChange {
  return create(KeyChangeSchema, { key, kind: ChangeKind.SET, value: bytes(value), encoding: "json/v1" });
}
const setNull = (key: string): KeyChange => create(KeyChangeSchema, { key, kind: ChangeKind.SET, isNull: true, encoding: "json/v1" });
const del = (key: string): KeyChange => create(KeyChangeSchema, { key, kind: ChangeKind.DELETE });
const incr = (key: string, delta: bigint): KeyChange => create(KeyChangeSchema, { key, kind: ChangeKind.INCR, delta });
const casAbsent = (key: string, value: string): KeyChange =>
  create(KeyChangeSchema, { key, kind: ChangeKind.CAS, value: bytes(value), encoding: "json/v1", expected: { expect: { case: "absent", value: true } } });
const override = (key: string, value: string): KeyChange => create(KeyChangeSchema, { key, kind: ChangeKind.OVERRIDE, value: bytes(value), encoding: "json/v1" });
const crdtUpdate = (key: string, update: Uint8Array): KeyChange => create(KeyChangeSchema, { key, kind: ChangeKind.CRDT_UPDATE, update, encoding: "yjs/v1" });
/** A change as a redaction leaves it: the value fields cleared, the encoding `redacted/v1` (06 [LOG-76]). */
const blank = (c: KeyChange): KeyChange => create(KeyChangeSchema, { key: c.key, kind: c.kind, seenVersion: c.seenVersion, encoding: "redacted/v1" });

/** The update in which client `client` inserts "a" at the start of the root "t" (the vectors' smallest ContentString insert). */
const TEXT_UPDATE = fromHex("01010100" + "0401" + "0174" + "0161" + "00");

function record(kind: RecordKind, seq: number, changes: KeyChange[], sub = "ana"): RecordMessage {
  return create(RecordSchema, {
    seq: BigInt(seq),
    logEpoch: 1,
    kind,
    manifestVersion: 1,
    principal: { appId: "app", sub },
    serverTime: { seconds: BigInt(1000 + seq), nanos: 5 },
    changes,
  });
}
const op = (seq: number, ...changes: KeyChange[]): RecordMessage => record(RecordKind.OP, seq, changes);

/** JSON in the canonical form of RFC 8785 (members sorted), which the fold's reader of a SYSTEM body requires. */
function canonicalJson(v: unknown): string {
  if (Array.isArray(v)) return `[${v.map(canonicalJson).join(",")}]`;
  if (v !== null && typeof v === "object") {
    const o = v as Record<string, unknown>;
    return `{${Object.keys(o)
      .sort()
      .map((k) => `${JSON.stringify(k)}:${canonicalJson(o[k])}`)
      .join(",")}}`;
  }
  return JSON.stringify(v);
}

function system(seq: number, kind: SystemKind, body: unknown, ...changes: KeyChange[]): RecordMessage {
  const r = record(RecordKind.SYSTEM, seq, changes, "abcp.server");
  r.system = create(SystemEventSchema, { kind, mustUnderstand: true, encoding: "json/v1", body: bytes(canonicalJson(body ?? {})) });
  return r;
}

const CONVERSION = { conversion: "rename", from_template: "old/{k}", to_template: "new/{k}", manifest_version: 2, step: 1, done: true };

/** A keyspace state with the records applied one by one, the metadata hash reachable (prefix versions tracked). */
class Fixture {
  readonly st = new ReplicaState({ trackPrefixes: true });
  readonly rules = newRules();
  seq = 0;

  next(): number {
    return this.seq + 1;
  }

  apply(rec: RecordMessage, opts: Parameters<typeof applyRecord>[3] = {}): void {
    applyRecord(this.st, this.rules, rec, opts);
    this.seq = Number(rec.seq);
  }

  op(...changes: KeyChange[]): number {
    const rec = op(this.next(), ...changes);
    this.apply(rec);
    return Number(rec.seq);
  }

  system(kind: SystemKind, body: unknown, ...changes: KeyChange[]): number {
    const rec = system(this.next(), kind, body, ...changes);
    this.apply(rec);
    return Number(rec.seq);
  }

  /** Applies the record, which must be refused with the code, and checks that the state is as it was. */
  refused(rec: RecordMessage, code: FoldCode): FoldError {
    const before = toHex(this.st.keyspaceStateHash());
    const meta = toHex(this.st.metadataHash());
    const seq = this.st.seq;
    let err: unknown;
    try {
      applyRecord(this.st, this.rules, rec);
    } catch (e) {
      err = e;
    }
    expect(err, `seq ${rec.seq}: not refused`).toBeInstanceOf(FoldError);
    expect((err as FoldError).code, (err as FoldError).message).toBe(code);
    expect(toHex(this.st.keyspaceStateHash())).toBe(before);
    expect(toHex(this.st.metadataHash())).toBe(meta);
    expect(this.st.seq).toBe(seq);
    return err as FoldError;
  }

  top(version = 1): VersionTopology {
    return new VersionTopology(this.rules as Rules, version);
  }

  key(key: string): NonNullable<ReturnType<ReplicaState["key"]>> {
    const k = this.st.key(key);
    if (k === undefined) throw new Error(`key "${key}" is absent`);
    return k;
  }

  absent(key: string): void {
    expect(this.st.key(key), `key "${key}" is present`).toBeUndefined();
  }

  tomb(key: string): NonNullable<ReturnType<ReplicaState["tombstone"]>> {
    const t = this.st.tombstone(key);
    if (t === undefined) throw new Error(`key "${key}" holds no tombstone`);
    return t;
  }

  noTomb(key: string): void {
    expect(this.st.tombstone(key), `key "${key}" holds a tombstone`).toBeUndefined();
  }

  prefix(p: string, want: number): void {
    expect(this.st.prefixVersion(p), `prefix "${p}"`).toBe(BigInt(want));
  }
}

/** Calls fn with every ordering of the changes (the permutations of server/internal/fold/order_test.go). */
function permutations<T>(items: readonly T[], fn: (order: T[]) => void): number {
  let n = 0;
  const cur = [...items];
  const rec = (k: number): void => {
    if (k === cur.length) {
      n++;
      fn([...cur]);
      return;
    }
    for (let i = k; i < cur.length; i++) {
      [cur[k], cur[i]] = [cur[i] as T, cur[k] as T];
      rec(k + 1);
      [cur[k], cur[i]] = [cur[i] as T, cur[k] as T];
    }
  };
  rec(0);
  return n;
}

const factorial = (n: number): number => (n <= 1 ? 1 : n * factorial(n - 1));

// ---------------------------------------------------------------------------------------------------------------------------

describe("05 [STATE-43], [STATE-45] (Q12, V6 MUST-B): a change covered by a subtree deletion touches only the prefixes", () => {
  // The state that every order folds first: issue/x with its title, two nested comments (a marker, and one that holds a key
  // tombstone), and the comments' bodies.
  const setup = (): Fixture => {
    const f = new Fixture();
    f.op(casAbsent("issue/x", "{}"), set("issue/x/title", '"T"'), casAbsent("issue/x/comment/c1", "{}"), set("issue/x/comment/c1/body", '"B"'));
    f.op(casAbsent("issue/x/comment/c2", "{}"), set("issue/x/comment/c2/body", '"C"'));
    f.op(del("issue/x/comment/c2/body")); // 3: a key tombstone inside the subtree
    f.op(set("v/pad", "1"));
    return f;
  };
  const keys = [
    "issue/x",
    "issue/x/title",
    "issue/x/comment/c1",
    "issue/x/comment/c1/body",
    "issue/x/comment/c2",
    "issue/x/comment/c2/body",
    "issue/x/comment/c3",
  ];
  const prefixes = ["issue/", "issue/x/", "issue/x/comment/", "issue/x/comment/c1/", "issue/x/comment/c2/", "issue/x/comment/c3/"];

  /** Everything a fold leaves that two orders must agree on: the hashes, every tombstone, every present key and every prefix version. */
  const observe = (f: Fixture): { state: string; meta: string; detail: string } => {
    let detail = "";
    for (const k of keys) {
      const t = f.st.tombstone(k);
      if (t !== undefined) detail += `${k}:${t.kind},${t.version},${t.mark},${t.overrideSeq},${t.resetSeq};`;
      const row = f.st.key(k);
      if (row !== undefined) detail += `${k} present,${row.version};`;
    }
    for (const p of prefixes) detail += `${p}=${f.st.prefixVersion(p)};`;
    return { state: toHex(f.st.keyspaceStateHash()), meta: toHex(f.st.metadataHash()), detail };
  };

  const records: Record<string, KeyChange[]> = {
    // a nested marker that was present, changed by a CAS and by a SET
    "nested marker CAS and title SET": [del("issue/x"), casAbsent("issue/x/comment/c1", "{}"), set("issue/x/title", '"U"')],
    "nested marker SET, body SET": [del("issue/x"), casAbsent("issue/x/comment/c1", "{}"), set("issue/x/comment/c1/body", '"V"'), del("issue/x/title")],
    // a nested marker that the record deletes too, and changes
    "nested marker DELETE and body SET": [del("issue/x"), del("issue/x/comment/c1"), set("issue/x/comment/c1/body", '"V"'), set("issue/x/title", '"U"')],
    // a nested marker that holds a tombstone, and one that was never there
    "tombstoned and absent markers": [del("issue/x"), casAbsent("issue/x/comment/c2", "{}"), casAbsent("issue/x/comment/c3", "{}"), set("issue/x/comment/c2/body", '"V"')],
    "every kind of change": [
      del("issue/x"),
      override("issue/x/title", '"U"'),
      casAbsent("issue/x/comment/c1", "{}"),
      del("issue/x/comment/c2/body"),
      setNull("issue/x/comment/c1/body"),
    ],
  };

  // The same records as an OP and as a REDACTED record, whose DELETEs leave marked tombstones (05 [STATE-37]).
  for (const kind of ["OP", "REDACTED"] as const) {
    for (const [name, changes] of Object.entries(records)) {
      it(`${kind}: ${name}: every order of the changes gives the same state, tombstones, prefixes and metadata hash`, () => {
        let first: ReturnType<typeof observe> | undefined;
        let firstOrder: string[] = [];
        const label = (order: KeyChange[]): string[] => order.map((c) => `${ChangeKind[c.kind]} ${c.key}`);
        const n = permutations(changes, (order) => {
          const f = setup();
          if (kind === "OP") {
            f.op(...order); // not refused, in any order
          } else {
            f.system(SystemKind.REDACTED, { redaction_id: "r", keys: ["issue/x", "issue/x/comment/c1", "issue/x/title"], min_seq: 1, refused_keys: [] }, ...order);
          }
          const got = observe(f);
          if (first === undefined) {
            first = got;
            firstOrder = label(order);
            return;
          }
          expect(got, `the order\n  ${label(order).join("\n  ")}\ngives another state than\n  ${firstOrder.join("\n  ")}`).toEqual(first);
        });
        expect(n).toBe(factorial(changes.length));
        expect((first as { detail: string }).detail).not.toBe("");
      });
    }
  }

  it("a nested marker that was present before the record keeps its entity tombstone when the record changes it after the DELETE", () => {
    for (const c of [
      [del("issue/x"), casAbsent("issue/x/comment/c1", "{}")],
      [del("issue/x"), override("issue/x/comment/c1", "{}")],
    ]) {
      const f = setup();
      f.op(...c);
      expect(f.tomb("issue/x/comment/c1")).toMatchObject({ kind: "entity", version: 5n });
      f.absent("issue/x/comment/c1");
    }
  });
});

describe("05 [STATE-43], [STATE-35] (Q12, ruled reading a): a change to a key of a deleted subtree is overruled, and no stop", () => {
  // the state that every variant folds first: issue/x with its title, a nested comment with its body
  const setup = (): Fixture => {
    const f = new Fixture();
    f.op(casAbsent("issue/x", "{}"), set("issue/x/title", '"T"'), casAbsent("issue/x/comment/c1", "{}"), set("issue/x/comment/c1/body", '"B"'));
    f.op(set("v/pad", "1"));
    return f;
  };
  const reference = setup();
  reference.op(del("issue/x"), del("issue/x/title"));

  const cases: Record<string, KeyChange[]> = {
    "SET after": [del("issue/x"), set("issue/x/title", '"U"')],
    "SET before": [set("issue/x/title", '"U"'), del("issue/x")],
    "null SET": [del("issue/x"), setNull("issue/x/title")],
    OVERRIDE: [del("issue/x"), override("issue/x/title", '"U"')],
    "blanked SET": [del("issue/x"), blank(set("issue/x/title", '"U"'))],
    "CAS of a nest": [casAbsent("issue/x/comment/c1", "{}"), del("issue/x"), del("issue/x/title")],
    "nest under SET": [del("issue/x"), set("issue/x/title", '"U"'), set("issue/x/comment/c1/body", '"V"')],
  };
  for (const [name, changes] of Object.entries(cases)) {
    it(`${name}: not refused; the key is absent at the record's seq with no tombstone of its own`, () => {
      const f = setup();
      f.op(...changes); // not refused
      expect(f.tomb("issue/x")).toMatchObject({ kind: "entity", version: 3n });
      f.absent("issue/x/title");
      f.noTomb("issue/x/title"); // absent with version 3 and no tombstone of its own
      expect(f.st.effectiveVersion(f.top(), "issue/x/title")).toBe(3n);
      f.absent("issue/x/comment/c1/body");
      f.noTomb("issue/x/comment/c1/body");
      expect(f.tomb("issue/x/comment/c1")).toMatchObject({ kind: "entity", version: 3n }); // the nested marker present before the record
      expect(f.st.seqs("issue/x/title").resetSeq).toBe(0n);
      f.prefix("issue/x/", 3);
      f.prefix("issue/", 3);
      if (["SET after", "SET before", "null SET", "OVERRIDE", "blanked SET"].includes(name)) {
        // the same state as the record that deletes the title too
        expect(toHex(f.st.keyspaceStateHash())).toBe(toHex(reference.st.keyspaceStateHash()));
        expect(toHex(f.st.metadataHash())).toBe(toHex(reference.st.metadataHash()));
      }
    });
  }

  it("a tombstone of the key's own is covered", () => {
    const f = setup();
    f.op(del("issue/x/title")); // 3: a key tombstone
    f.op(del("issue/x"), set("issue/x/title", '"U"')); // 4
    f.noTomb("issue/x/title");
    expect(f.st.effectiveVersion(f.top(), "issue/x/title")).toBe(4n);
  });

  it("a later change is not hidden", () => {
    const f = setup();
    f.op(del("issue/x"), set("issue/x/title", '"U"')); // 3
    f.op(del("issue/x/title")); // 4: the key's own tombstone (STATE-37)
    expect(f.tomb("issue/x/title")).toMatchObject({ kind: "key", version: 4n });
  });

  it("is not a stop: the record is applied and the cursor moves", () => {
    const f = setup();
    expect(() => f.apply(op(f.next(), del("issue/x"), set("issue/x/title", '"U"')))).not.toThrow();
    expect(f.st.seq).toBe(3n);
  });

  it("the cost of a record of 2,000 covered changes is that of its changes, not of their pairs", () => {
    // 15 [COMPAT-35] and 02 [ARC-46]: the work of a record is bounded by its size. 1,000 marker DELETEs and 1,000 SETs under them.
    const f = new Fixture();
    const cs: KeyChange[] = [];
    for (let i = 0; i < 1000; i++) cs.push(del(`issue/i${i}`), set(`issue/i${i}/title`, '"x"'));
    const start = performance.now();
    f.op(...cs);
    expect(performance.now() - start).toBeLessThan(5000);
    expect(f.st.tombstoneKeys().length).toBe(1000);
    expect(f.st.size).toBe(0);
  });
});

describe("15 [COMPAT-35] (V1 SHOULD-4): a reader stops at exactly an unknown change kind, encoding or decision status", () => {
  const unknownKind = (key: string, kind: number): KeyChange => create(KeyChangeSchema, { key, kind: kind as ChangeKind });
  const withEncoding = (kind: ChangeKind, key: string, encoding: string): KeyChange =>
    create(KeyChangeSchema, { key, kind, value: bytes("1"), encoding });

  it("stops at a change kind, an encoding and a decision status that it does not know, wherever they stand in the record", () => {
    const f = new Fixture();
    f.op(set("v/a", "1"), override("c/n", '"not a counter"')); // a counter key that holds a value that is no counter: only an OVERRIDE can leave one (05 [STATE-26])
    const decision = (status: number, changes: KeyChange[]): RecordMessage => {
      const r = record(RecordKind.DECISION, 2, changes);
      r.mutationId = "m1";
      r.decision = create(DecisionSchema, { intentId: "m1", status: status as DecisionStatus });
      return r;
    };
    const intent = (changes: KeyChange[]): RecordMessage => {
      const r = record(RecordKind.INTENT, 2, changes);
      r.mutationId = "m1";
      return r;
    };
    const stops: Record<string, [RecordMessage, FoldCode]> = {
      "kind 99": [op(2, set("v/b", "1"), unknownKind("v/a", 99)), "unknown-change-kind"],
      "kind unspecified": [op(2, unknownKind("v/a", 0)), "unknown-change-kind"],
      "SET encoding": [op(2, withEncoding(ChangeKind.SET, "v/a", "json/v2")), "unknown-encoding"],
      "CAS encoding": [op(2, withEncoding(ChangeKind.CAS, "free/a", "cbor/v1")), "unknown-encoding"],
      "OVERRIDE encoding": [op(2, withEncoding(ChangeKind.OVERRIDE, "v/a", "x")), "unknown-encoding"],
      "update encoding": [op(2, create(KeyChangeSchema, { key: "d/a", kind: ChangeKind.CRDT_UPDATE, update: TEXT_UPDATE, encoding: "yjs/v2" })), "unknown-encoding"],
      // the change that the record's kind does not apply is read too: the reader meets every change
      "in an INTENT": [intent([set("v/b", "1"), withEncoding(ChangeKind.SET, "v/a", "json/v2")]), "unknown-encoding"],
      "in a REJECTED decision": [decision(DecisionStatus.REJECTED, [unknownKind("v/a", 99)]), "unknown-change-kind"],
      "in a SYSTEM record": [system(2, SystemKind.LIFECYCLE, {}, unknownKind("v/a", 7)), "unknown-change-kind"],
      "decision status 99": [decision(99, []), "unknown-decision-status"],
      "decision status unspecified": [decision(DecisionStatus.UNSPECIFIED, []), "unknown-decision-status"],
    };
    for (const [name, [rec, code]] of Object.entries(stops)) {
      const err = f.refused(rec, code);
      expect(err.stop, `${name}: ${err.message}`).toBe(true);
      expect(err.rule).toBe("COMPAT-35");
    }

    // the four encodings of Table 5.2.1 are known: no stop, each on a change that is well formed for it
    f.op(set("v/a", "2"));
    f.op(create(KeyChangeSchema, { key: "v/bytes", kind: ChangeKind.SET, value: bytes("raw"), encoding: "bytes/v1" }));
    f.op(crdtUpdate("d/enc", TEXT_UPDATE));
    f.op(blank(set("v/blanked", "1")));
    // a change with no encoding names none to know: it is no stop, and a `SET` without one is refused as an envelope that breaks 05 [STATE-20] (16 [SEC-3])
    expect(f.refused(op(f.next(), withEncoding(ChangeKind.SET, "v/noenc", "")), "invalid-envelope").stop).toBe(false);
    f.op(del("v/a"), incr("c/k", 1n));
  });

  it("a change of a known kind and encoding that the fold cannot apply is no stop (02 [ARC-46])", () => {
    const f = new Fixture();
    f.op(set("v/a", "1"), override("c/n", '"not a counter"')); // a counter key that holds a value that is no counter: only an OVERRIDE can leave one (05 [STATE-26])
    // an INCR on a value that is no counter
    expect(f.refused(op(f.next(), incr("c/n", 1n)), "malformed").stop).toBe(false);
    // a key that matches no template, two changes to one key, a CRDT update that is no Yjs update
    expect(f.refused(op(f.next(), set("nowhere/x", "1")), "no-template").stop).toBe(false);
    expect(f.refused(op(f.next(), set("v/z", "1"), set("v/z", "2")), "malformed").stop).toBe(false);
    expect(f.refused(op(f.next(), crdtUpdate("d/a", new Uint8Array([0xff]))), "malformed").stop).toBe(false);
  });
});

describe("06 [LOG-41] (Q13, V6 S-1): a known SYSTEM kind applies the changes it defines, and ignores and reports the rest", () => {
  it("a kind that defines no change ignores its changes and reports each; the record is applied, and the keyspace is not stopped", () => {
    const f = new Fixture();
    f.op(set("v/a", "1"));
    const rec = system(f.next(), SystemKind.MANIFEST_ACTIVATED, {}, set("v/b", "1"), del("v/a"), incr("c/k", 1n));
    rec.manifestVersion = 2;
    const got: Anomaly[] = [];
    f.apply(rec, { report: (a) => got.push(a) });
    expect(got).toHaveLength(3);
    got.forEach((a, i) => {
      expect(a).toMatchObject({ seq: 2n, index: i, rule: "LOG-41" });
      expect(a.detail).not.toBe("");
    });
    expect(got[1]?.key).toBe("v/a");
    f.key("v/a");
    f.absent("v/b");
    f.absent("c/k");
    expect(f.st.seq).toBe(2n);
    expect(f.st.manifestVersion).toBe(2);
  });

  it("a refused record reports none, a record with no ignored change reports none, an overlapping delivery reports none twice", () => {
    const f = new Fixture();
    f.op(set("v/a", "1"));
    const got: Anomaly[] = [];
    const report = (a: Anomaly): void => void got.push(a);
    const bad = system(f.next(), SystemKind.TOMBSTONES_DISCARDED, {}, set("v/b", "1")); // no floor_seq
    expect(() => applyRecord(f.st, f.rules, bad, { report })).toThrow(FoldError);
    expect(got).toHaveLength(0);
    f.apply(op(f.next(), set("v/c", "1")), { report });
    expect(got).toHaveLength(0);
    const lifecycle = system(f.next(), SystemKind.LIFECYCLE, {}, set("v/b", "1"));
    f.apply(lifecycle, { report });
    expect(got).toHaveLength(1);
    // the record at the cursor again: it was reported when it moved the state
    expect(applyRecord(f.st, f.rules, lifecycle, { report }).moved).toBe(false);
    expect(got).toHaveLength(1);
    // with no `report` the fold goes on
    f.apply(system(f.next(), SystemKind.LIFECYCLE, {}, set("v/b", "1")));
    f.absent("v/b");
  });

  it("TOMBSTONES_DISCARDED with a change ignores it and reports it with LOG-84 (it was a refusal)", () => {
    const f = new Fixture();
    f.op(set("v/a", "1"));
    f.op(del("v/a")); // 2: a key tombstone
    const got: Anomaly[] = [];
    f.apply(system(f.next(), SystemKind.TOMBSTONES_DISCARDED, { floor_seq: 2 }, set("v/z", "1")), { report: (a) => got.push(a) });
    expect(got).toHaveLength(1);
    expect(got[0]).toMatchObject({ rule: "LOG-84", key: "v/z" });
    f.absent("v/z");
    f.noTomb("v/a"); // the discard itself still applied
    expect(f.st.tombstoneFloor).toBe(2n);
  });

  // a known SYSTEM kind that defines some changes applies those and ignores and reports the rest. A SET in a KEY_EXPIRED record is not
  // stored; an INCR, a CAS, an OVERRIDE or a CRDT update in a conversion or a redaction is not applied.
  const setup = (): Fixture => {
    const f = new Fixture();
    f.op(set("v/a", "1"), set("v/b", "2"), set("old/a", "1"));
    return f;
  };
  const other = (): KeyChange[] => [incr("c/n", 1n), casAbsent("free/c", "1"), override("v/o", "1"), crdtUpdate("d/doc", TEXT_UPDATE)];
  const foreign = (): KeyChange[] => [set("v/z", "1"), ...other()]; // a SET is a change of another kind for KEY_EXPIRED only
  const cases: { name: string; kind: SystemKind; body: unknown; defined: () => KeyChange[]; foreign: () => KeyChange[]; checks: (f: Fixture) => void }[] = [
    {
      name: "KEY_EXPIRED",
      kind: SystemKind.KEY_EXPIRED,
      body: { keys: ["v/a"], cause: "intent_row" },
      defined: () => [del("v/a")],
      foreign,
      checks: (f) => f.absent("v/a"),
    },
    {
      name: "TEMPLATE_CONVERSION",
      kind: SystemKind.TEMPLATE_CONVERSION,
      body: CONVERSION,
      defined: () => [set("new/a", "1"), del("old/a")],
      foreign: other,
      checks: (f) => {
        f.key("new/a");
        f.absent("old/a");
      },
    },
    {
      name: "REDACTED",
      kind: SystemKind.REDACTED,
      body: { redaction_id: "r", keys: ["v/b"], min_seq: 1, refused_keys: [] },
      defined: () => [del("v/b"), set("v/r", "1")],
      foreign: other,
      checks: (f) => {
        f.absent("v/b");
        f.key("v/r");
      },
    },
  ];
  for (const c of cases) {
    it(`${c.name}: the changes it does not define are not applied, and are reported once each`, () => {
      // the record that carries only the defined changes, as the reference
      const ref = setup();
      const r = system(ref.next(), c.kind, c.body, ...c.defined());
      r.manifestVersion = 2;
      ref.apply(r);

      const f = setup();
      const defined = c.defined();
      const alien = c.foreign();
      // the foreign changes before, between and after the defined ones
      const changes = [...alien.slice(0, 1), defined[0] as KeyChange, ...alien.slice(1, 3), ...defined.slice(1), ...alien.slice(3)];
      const rec = system(f.next(), c.kind, c.body, ...changes);
      rec.manifestVersion = 2;
      const got: Anomaly[] = [];
      f.apply(rec, { report: (a) => got.push(a) });
      expect(got).toHaveLength(alien.length);
      for (const a of got) {
        expect(a).toMatchObject({ seq: rec.seq, rule: "LOG-41" });
        expect(a.detail).not.toBe("");
        expect((changes[a.index] as KeyChange).key).toBe(a.key);
        expect(defined.map((d) => d.key)).not.toContain(a.key); // a change that the kind defines is not reported
      }
      c.checks(f);
      for (const k of ["v/z", "c/n", "free/c", "v/o", "d/doc"]) f.absent(k);
      expect(toHex(f.st.keyspaceStateHash())).toBe(toHex(ref.st.keyspaceStateHash()));
      expect(toHex(f.st.metadataHash())).toBe(toHex(ref.st.metadataHash()));
      expect(f.st.seq).toBe(rec.seq);
    });
  }
});

describe("05 [STATE-82] with [STATE-67] (V1 MUST-1): a blanked conversion SET keeps the source key's changed_at", () => {
  it("takes the source's changed_at, an empty changed_by and the redacted state, and the same metadata entries as the unblanked log", () => {
    const blanked = new Fixture();
    blanked.apply(op(1, blank(set("old/a", "1")))); // old/a in the redacted state, changed_at 1001
    blanked.apply(op(2, set("old/b", "2"))); // old/b holds a value, by ana, changed_at 1002
    const r = system(3, SystemKind.TEMPLATE_CONVERSION, CONVERSION, blank(set("new/a", "1")), del("old/a"), blank(set("new/b", "2")), del("old/b"), set("new/c", "3"));
    r.manifestVersion = 2;
    blanked.apply(r);
    for (const [key, want] of [
      ["new/a", 1001n],
      ["new/b", 1002n],
      ["new/c", 1003n],
    ] as const) {
      const k = blanked.key(key);
      expect({ key, changedAt: k.changedAt.seconds, version: k.version }).toEqual({ key, changedAt: want, version: 3n });
    }
    for (const key of ["new/a", "new/b"]) {
      const k = blanked.key(key);
      expect({ state: k.state, by: k.changedBy, enc: k.encoding }).toEqual({ state: StateRedacted, by: "", enc: "redacted/v1" });
    }
    // new/c has no source key, so it takes the record's time and principal
    expect(blanked.key("new/c").changedBy).toBe("abcp.server");

    // The metadata hash through the blanked records is the one of the unblanked log's keys' changed_at, which is what a rewritten
    // checkpoint holds (06 [LOG-82]): fold the same log with and without the blanking and compare the entries.
    const plain = new Fixture();
    plain.apply(op(1, set("old/a", "1")));
    plain.apply(op(2, set("old/b", "2")));
    const r2 = system(3, SystemKind.TEMPLATE_CONVERSION, CONVERSION, set("new/a", "1"), del("old/a"), set("new/b", "2"), del("old/b"), set("new/c", "3"));
    r2.manifestVersion = 2;
    plain.apply(r2);
    const want = new Map(plain.st.metaEntries().map((e) => [e.key, e.changedAt]));
    for (const e of blanked.st.metaEntries()) {
      if (e.key === "new/a" || e.key === "new/b") expect(e.changedAt, e.key).toEqual(want.get(e.key));
    }
  });
});
