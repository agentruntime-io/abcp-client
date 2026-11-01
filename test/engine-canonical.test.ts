// The two fixes of C3 that are not about tabs, storage or revocation:
//  - 05 [STATE-7], 14 [CLI-22]: the engine authors every json/v1 value through the canonical writer of `rules/jsonv1` (RFC 8785), never
//    `JSON.stringify`, and refuses a value that is not valid json/v1;
//  - 02 [ARC-46]: the engine counts the changes that records carry and that their kind does not define, per keyspace, and reports them.

import { describe, expect, it } from "vitest";
import { LocalRefusal, cas, jsonBytes, set } from "../src/engine/index.js";
import { RecordKind, SystemKind } from "../src/gen/abpc/v2/types_pb.js";
import { parse, validate } from "../src/rules/jsonv1/index.js";
import { kc, KS, ofType, rig, text } from "./helpers/engine.js";

const s = (v: unknown): string => new TextDecoder().decode(jsonBytes(v));

describe("05 [STATE-7], 14 [CLI-22]: json/v1 values are authored in the RFC 8785 canonical form", () => {
  it("the members of an object are sorted by the UTF-16 code units of their names, where JSON.stringify keeps insertion order", () => {
    expect(s({ b: 1, a: 2 })).toBe('{"a":2,"b":1}');
    expect(JSON.stringify({ b: 1, a: 2 })).toBe('{"b":1,"a":2}');
    expect(s({ z: { y: 1, x: [{ d: 1, c: 2 }] }, a: null })).toBe('{"a":null,"z":{"x":[{"c":2,"d":1}],"y":1}}');
  });

  it("a name that is a number sorts as text: 10 before 9 (JavaScript lists integer-like keys first, in numeric order)", () => {
    expect(s({ "9": 1, "10": 2, b: 3 })).toBe('{"10":2,"9":1,"b":3}');
    expect(JSON.stringify({ "9": 1, "10": 2, b: 3 })).toBe('{"9":1,"10":2,"b":3}');
  });

  it("unicode: names sort by UTF-16 code units, so a name outside the BMP sorts before U+FFFD; strings are written as UTF-8 with the escapes of RFC 8785 only", () => {
    const o: Record<string, number> = {};
    o["\ufffd"] = 1;
    o["\u{1f600}"] = 2;
    expect(s(o)).toBe('{"\u{1f600}":2,"\ufffd":1}');
    expect(JSON.stringify(o)).toBe('{"\ufffd":1,"\u{1f600}":2}');
    expect([...jsonBytes("é")]).toEqual([0x22, 0xc3, 0xa9, 0x22]);
    expect(s("a\u001fb\n\u007f\u2028")).toBe('"a\\u001fb\\n\u007f\u2028"');
  });

  it("numbers: 1e21 is 1e+21, -0 is 0, and the shortest digits that round-trip; every one is valid json/v1 and reads back as the same number", () => {
    expect(s(1e21)).toBe("1e+21");
    expect(s(-0)).toBe("0");
    expect(Object.is(parse(jsonBytes(-0)), 0)).toBe(true);
    expect(s(0.1 + 0.2)).toBe("0.30000000000000004");
    expect(s(1e-7)).toBe("1e-7");
    expect(s(5e-324)).toBe("5e-324");
    expect(s(Number.MAX_SAFE_INTEGER)).toBe("9007199254740991");
    expect(s({ n: [1e21, -0, 1.5e300] })).toBe('{"n":[1e+21,0,1.5e+300]}');
    for (const v of [1e21, -0, 0.1 + 0.2, 1e-7, 5e-324, { b: 1, a: [1e21] }, "x", true]) expect(validate(jsonBytes(v))).toBeNull();
  });

  it("a value that is not valid json/v1 is refused with a typed local error before anything is stored: undefined, NaN, Infinity, a bigint, a lone surrogate, a function, a Date", () => {
    const bad: [string, unknown][] = [
      ["undefined", undefined],
      ["undefined member", { a: undefined }],
      ["NaN", Number.NaN],
      ["Infinity", Infinity],
      ["-Infinity", { a: [-Infinity] }],
      ["bigint", 10n],
      ["lone surrogate", "\ud800"],
      ["function", () => 1],
      ["symbol", Symbol("x")],
      ["Date", new Date(0)],
      ["class instance", new (class Foo {})()],
      ["null (is_null is the one representation, 05 [STATE-15])", null],
      ["nesting over 32", (() => { let v: unknown = 1; for (let i = 0; i < 33; i++) v = [v]; return v; })()],
      ["over 1,024 members", Object.fromEntries(Array.from({ length: 1025 }, (_, i) => [`k${i}`, i]))],
      ["U+0000 in a string", "a\u0000b"],
      ["a noncharacter, U+FFFF (I-JSON)", "a￿b"],
    ];
    for (const [name, v] of bad) {
      let err: unknown;
      try {
        jsonBytes(v);
      } catch (e) {
        err = e;
      }
      expect(err, name).toBeInstanceOf(LocalRefusal);
      expect((err as LocalRefusal).code, name).toBe("INVALID");
    }
    expect(() => set("settings/a", undefined)).toThrow(LocalRefusal);
    expect(() => cas("notes/n", Number.NaN)).toThrow(LocalRefusal);
  });

  it("an action's value reaches the server in canonical form, and the stored content holds it byte for byte", async () => {
    const r = await rig();
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/x", { b: 1, a: { "9": 1, "10": 2 }, n: 1e21 })] });
    await r.engine.settle();
    expect(text(r.server.value(KS, "settings/x"))).toBe('{"a":{"10":2,"9":1},"b":1,"n":1e+21}');
    expect(validate(r.server.value(KS, "settings/x") as Uint8Array)).toBeNull();
    expect(a.mutationId).toBeTruthy();
    const v = r.engine.confirmed(KS).json("settings/x") as Map<string, unknown>;
    expect(v.get("n")).toBe(1e21);
    expect(v.get("b")).toBe(1);
  });

  it("bytes that an app supplies as json/v1 are checked the same way: a text that is not canonical is refused at the authoring call, and the outbox is unchanged", async () => {
    const r = await rig();
    const enc = (x: string): Uint8Array => new TextEncoder().encode(x);
    const refused = async (value: Uint8Array): Promise<string> => {
      try {
        await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [{ key: "settings/y", kind: 1 /* SET */, value, encoding: "json/v1" }] });
      } catch (e) {
        expect(e).toBeInstanceOf(LocalRefusal);
        return (e as LocalRefusal).code;
      }
      return "accepted";
    };
    expect(await refused(enc('{"b":1,"a":2}'))).toBe("INVALID"); // not in canonical order
    expect(await refused(enc('{"a": 1}'))).toBe("INVALID"); // whitespace
    expect(await refused(enc("1E2"))).toBe("INVALID");
    expect(await refused(enc("null"))).toBe("INVALID"); // the literal null
    expect(await refused(enc(""))).toBe("INVALID");
    expect(r.engine.outbox()).toHaveLength(0);
    expect(r.storage.log.flat().filter((o) => o.op === "putEntry")).toHaveLength(0);
    expect(await refused(enc('{"a":2,"b":1}'))).toBe("accepted");
    // a CAS expectation by value is compared byte for byte with the stored value: it is canonical as well
    try {
      await r.engine.submit({ keyspaceId: KS, opType: "CasSet", changes: [{ key: "notes/n", kind: 4 /* CAS */, value: enc("1"), encoding: "json/v1", expected: { value: enc('{"b":1,"a":2}') } }] });
      throw new Error("accepted");
    } catch (e) {
      expect(e).toBeInstanceOf(LocalRefusal);
    }
  });
});

describe("02 [ARC-46]: the changes that a record carries and its kind does not define are counted per keyspace and reported", () => {
  it("a known SYSTEM kind that defines no change but carries one: the change is ignored, `skipped` is emitted with the rule, and the keyspace's count and the skipped count rise", async () => {
    const r = await rig({ keyspaces: [KS, "ks2"] });
    r.server.appendRaw(KS, { kind: RecordKind.SYSTEM, system: { kind: SystemKind.MANIFEST_ACTIVATED, mustUnderstand: false }, changes: [kc("settings/a", "set", 1), kc("settings/b", "set", 2)] });
    await r.engine.settle();
    const ev = ofType(r, "skipped");
    expect(ev).toHaveLength(1);
    expect(ev[0]).toMatchObject({ keyspaceId: KS, cause: "ANOMALY", keys: ["settings/a", "settings/b"], seq: 1n });
    expect(ev[0]?.anomalies?.[0]).toMatchObject({ index: 0, key: "settings/a", rule: "LOG-41" });
    expect(r.engine.status().anomalies).toEqual({ [KS]: 2 });
    expect(r.engine.status().skippedChanges).toBe(2);
    expect(r.engine.confirmed(KS).has("settings/a")).toBe(false); // not applied
    expect(r.engine.cursor(KS)?.seq).toBe(1n); // and the keyspace is not stopped (15 [COMPAT-35])
    // the count is per keyspace
    r.server.appendRaw("ks2", { kind: RecordKind.SYSTEM, system: { kind: SystemKind.EPOCH_ADVANCED, mustUnderstand: false }, changes: [kc("settings/c", "set", 1)] });
    await r.engine.settle();
    expect(r.engine.status().anomalies).toEqual({ [KS]: 2, ks2: 1 });
    expect(r.engine.status().skippedChanges).toBe(3);
  });

  it("a record that is delivered again reports nothing a second time", async () => {
    const r = await rig();
    r.server.faults.duplicateNext = 1;
    r.server.appendRaw(KS, { kind: RecordKind.SYSTEM, system: { kind: SystemKind.LIFECYCLE, mustUnderstand: false }, changes: [kc("settings/a", "set", 1)] });
    await r.engine.settle();
    expect(ofType(r, "skipped")).toHaveLength(1);
    expect(r.engine.status().anomalies).toEqual({ [KS]: 1 });
  });
});
