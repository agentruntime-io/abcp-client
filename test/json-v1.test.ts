// json/v1 vectors (conformance/vectors/json-v1.json): 05 [STATE-7], [STATE-15], [STATE-26] to [STATE-28], 04 [MAN-26],
// [MAN-27] (17 [CONF-9]).

import { describe, expect, it } from "vitest";
import { fromHex, toHex } from "../src/rules/bytes.js";
import {
  applyIncr,
  applyOverride,
  canonical,
  counterText,
  JsonV1Error,
  parse,
  parseDocument,
  parseSchema,
  rangeContains,
  schemaFromValue,
  validate,
  type Range,
} from "../src/rules/jsonv1/index.js";
import { keyStateHash, keyStatePreimage, StateValue, type KeyValueState } from "../src/rules/hashes/index.js";
import { loadVectors, utf8 } from "./helpers/vectors.js";

const all = loadVectors("json-v1.json");

describe("05 [STATE-7] json/v1 values (json-v1 vectors)", () => {
  for (const v of all.filter((x) => x.kind === "json-v1")) {
    it(`${v.id}: ${v.title}`, () => {
      const bytes = fromHex(v.value as string);
      // the vector's value_schema is a parsed JSON object; read it as a manifest would carry it (04 §4.4.2)
      const schema =
        v.value_schema !== undefined ? schemaFromValue(parseDocument(utf8(JSON.stringify(v.value_schema))), "json") : undefined;
      const err = validate(bytes, schema);
      if (v.expect === "VALID") {
        expect(err, err?.message).toBeNull();
      } else {
        expect(err, "a refusal").not.toBeNull();
        expect(err?.code).toBe("SCHEMA_INVALID");
        if (v.reason !== undefined) expect(err?.reason).toBe(v.reason);
      }
      // parse is validate with no schema and a tree: it agrees, and the tree of a valid value writes the same bytes (STATE-7)
      if (schema === undefined) {
        let parsed: ReturnType<typeof parse> | undefined;
        let perr: JsonV1Error | undefined;
        try {
          parsed = parse(bytes);
        } catch (e) {
          perr = e as JsonV1Error;
        }
        expect(perr?.reason).toBe(err?.reason);
        if (err === null) expect(toHex(canonical(parsed as NonNullable<typeof parsed>))).toBe(toHex(bytes));
      }
    });
  }
});

describe("05 [STATE-26] to [STATE-28] counters (json-counter vectors)", () => {
  for (const v of all.filter((x) => x.kind === "json-counter")) {
    it(`${v.id}: ${v.title}`, () => {
      const range: Range = { min: BigInt(v.range[0]), max: BigInt(v.range[1]) };
      let cur: bigint | null = null;
      let version = 0n;
      for (const step of v.steps as { op: string; delta?: string; value?: string; expect: string; reason?: string; seq?: number }[]) {
        let verdict = "APPLIED";
        let reason = "";
        let next: bigint | null = cur;
        try {
          if (step.op === "INCR") {
            next = applyIncr(cur, BigInt(step.delta as string), range);
          } else if (step.op === "OVERRIDE") {
            next = applyOverride(utf8(step.value as string), range);
          } else {
            next = null; // DELETE
          }
        } catch (e) {
          if (!(e instanceof JsonV1Error)) throw e;
          verdict = e.code;
          reason = e.reason;
        }
        expect(verdict, `step ${step.op}`).toBe(step.expect);
        if (step.reason !== undefined) expect(reason).toBe(step.reason);
        if (verdict === "APPLIED") {
          cur = next;
          version = BigInt(step.seq as number);
        }
      }
      expect(cur !== null).toBe(v.expect.present);
      if (cur === null) return;
      expect(cur.toString()).toBe(v.expect.value);
      expect(Number(version)).toBe(v.expect.version);
      // the stored counter hashes as json/v1, whatever the encoding says (05 [STATE-41])
      const ks = { key: v.key as string, state: StateValue as KeyValueState, counter: true, encoding: "", content: counterText(cur), version };
      expect(toHex(keyStatePreimage(ks))).toBe(v.expect.state_hash_preimage);
      expect(toHex(keyStateHash(ks))).toBe(v.expect.state_hash);
    });
  }
});

describe("05 [STATE-7] strictness that the vectors do not spell out", () => {
  it("never throws anything but a JsonV1Error, whatever the bytes (02 [ARC-46])", () => {
    const seed = [0x7b, 0x5b, 0x22, 0x5c, 0x75, 0x30, 0x2d, 0x31, 0x65, 0x2b, 0x7d, 0x5d, 0x2c, 0x3a, 0x6e, 0xf0, 0x80, 0x00, 0x20];
    let x = 12345;
    for (let n = 0; n < 3000; n++) {
      const len = (n % 24) + 1;
      const b = new Uint8Array(len);
      for (let i = 0; i < len; i++) {
        x = (x * 1103515245 + 12345) & 0x7fffffff;
        b[i] = seed[x % seed.length] as number;
      }
      validate(b);
      expect(() => {
        try {
          parse(b);
        } catch (e) {
          if (!(e instanceof JsonV1Error)) throw e;
        }
      }).not.toThrow();
    }
  });

  it("a very deep text is refused as json-limits and never overflows the stack", () => {
    const deep = utf8("[".repeat(100000) + "]".repeat(100000));
    expect(validate(deep)?.reason).toBe("json-limits");
    expect(() => parseDocument(deep)).toThrow(JsonV1Error);
  });

  it("RFC 8785 sorts members by UTF-16 code units, which differs from code point order above U+FFFF", () => {
    // U+1F600 is the pair D83D DE00 in UTF-16, which sorts before U+FF5E (a single unit)
    const text = utf8('{"～":1,"\u{1f600}":2}');
    expect(validate(text)?.reason).toBe("not-canonical");
    const v = parseDocument(text);
    expect(new TextDecoder().decode(canonical(v))).toBe('{"\u{1f600}":2,"～":1}');
    expect(validate(utf8('{"\u{1f600}":2,"～":1}'))).toBeNull();
  });

  it("a counter range always stays within ±(2^53-1) (05 [STATE-28])", () => {
    const wide: Range = { min: -(1n << 60n), max: 1n << 60n };
    expect(rangeContains(wide, (1n << 53n) - 1n)).toBe(true);
    expect(rangeContains(wide, 1n << 53n)).toBe(false);
  });

  it("a value schema is refused with INVALID_ENVELOPE and its attribute path (04 [MAN-25])", () => {
    try {
      parseSchema(utf8('{"type":"object","fields":{"a":{"type":"string","min":1}}}'), "json");
      throw new Error("not refused");
    } catch (e) {
      expect(e).toBeInstanceOf(JsonV1Error);
      expect((e as JsonV1Error).code).toBe("INVALID_ENVELOPE");
      expect((e as JsonV1Error).path).toBe("fields.a.min");
    }
  });
});
