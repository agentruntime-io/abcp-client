// The rules that the Go jsonv1 package gained after the client's first port (C1): V1 MUST-2 (a number token of any length),
// V1 SHOULD-6 (the value count of a manifest document), V1 SHOULD-8 (a message never repeats the input whole), V1 SHOULD-9 (the count of
// required names). The tests are the twins of server/internal/jsonv1/number_long_test.go and v1_test.go.

import { describe, expect, it } from "vitest";
import { utf8Encode } from "../src/rules/bytes.js";
import {
  canonicalDocument,
  checkJsonText,
  denote,
  JsonV1Error,
  MAX_DOCUMENT_VALUES,
  parse,
  parseDocument,
  parseSchema,
  validate,
  type Schema,
} from "../src/rules/jsonv1/index.js";
import { checkWellFormed } from "../src/rules/crdt/index.js";

// ---- the reference: the exact value of a token as a rational, rounded by BigInt arithmetic to the nearest binary64, ties to even ----

type Status = "ok" | "infinite" | "underflow";

const bitLength = (x: bigint): number => x.toString(2).length;

/** Whether num/den >= 2^e. */
const geShift = (num: bigint, den: bigint, e: number): boolean => (e >= 0 ? num >= den << BigInt(e) : num << BigInt(-e) >= den);

/** The binary64 value nearest to the positive rational num/den, ties to even; Infinity above the range, 0 below it. */
function ratToDouble(num: bigint, den: bigint): number {
  if (num === 0n) return 0;
  const e0 = bitLength(num) - bitLength(den);
  let e = geShift(num, den, e0) ? e0 : e0 - 1; // 2^e <= num/den < 2^(e+1)
  const roundDiv = (n: bigint, d: bigint): bigint => {
    const q = n / d;
    const r2 = (n % d) * 2n;
    return r2 > d || (r2 === d && (q & 1n) === 1n) ? q + 1n : q;
  };
  let bits: bigint;
  if (e < -1022) {
    // subnormal: a whole number of 2^-1074; a carry into 2^52 is the smallest normal number, whose bits are the same
    bits = roundDiv(num << 1074n, den);
  } else {
    const sh = 52 - e;
    let q = sh >= 0 ? roundDiv(num << BigInt(sh), den) : roundDiv(num, den << BigInt(-sh));
    if (q === 1n << 53n) {
      q = 1n << 52n;
      e++;
    }
    if (e > 1023) return Infinity;
    bits = (BigInt(e + 1023) << 52n) | (q - (1n << 52n));
  }
  const dv = new DataView(new ArrayBuffer(8));
  dv.setBigUint64(0, bits);
  return dv.getFloat64(0);
}

/** What 05 [STATE-7] says of the token, computed exactly. The exponent of a token of the tests is small enough to be cheap. */
function refNumber(tok: string): { value: number; status: Status } {
  const neg = tok.startsWith("-");
  const body = neg ? tok.slice(1) : tok;
  const [mant, expText = ""] = body.toLowerCase().split("e");
  const [ip = "", fp = ""] = (mant as string).split(".");
  const digits = BigInt(ip + fp);
  const e = (expText === "" ? 0n : BigInt(expText.replace(/^\+/, ""))) - BigInt(fp.length);
  const pow = 10n ** (e < 0n ? -e : e);
  const f = digits === 0n ? 0 : e >= 0n ? ratToDouble(digits * pow, 1n) : ratToDouble(digits, pow);
  if (f === Infinity) return { value: Infinity, status: "infinite" };
  if (f === 0 && digits !== 0n) return { value: 0, status: "underflow" };
  return { value: neg ? -f : f, status: "ok" };
}

const zeros = (n: number): string => "0".repeat(n);

/** The same answer from `denote`: the value bit for bit when it is valid (a zero keeps its sign), and the same status. */
function expectAgrees(tok: string): void {
  const want = refNumber(tok);
  const got = denote(tok);
  const label = `${tok.slice(0, 80)}${tok.length > 80 ? "..." : ""} (${tok.length} characters)`;
  expect(got.status, label).toBe(want.status);
  if (want.status === "ok") expect(Object.is(got.value, want.value), `${label}: ${got.value}, the exact value is ${want.value}`).toBe(true);
}

describe("05 [STATE-7] (V1 MUST-2): a number token of any length is read exactly", () => {
  const table: { name: string; tok: string; want?: number; status: Status; exact?: false }[] = [
    // V1 MUST-2: Go's strconv.ParseFloat gives 1e-101, 0.1 and 0.1 for these.
    { name: "10^1499 times 10^-900 is 10^599: not finite", tok: "1" + zeros(1499) + "e-900", status: "infinite" },
    { name: "10^800 times 10^-800 is 1", tok: "1" + zeros(800) + "e-800", want: 1, status: "ok" },
    { name: "0.0...01e1000000 is not finite", tok: "0." + zeros(10000) + "1e1000000", status: "infinite" },
    // The edges of the long form.
    { name: "801 digits, the point at the end", tok: "1" + zeros(800), status: "infinite" },
    { name: "801 digits and e-500: 10^300", tok: "1" + zeros(800) + "e-500", want: 1e300, status: "ok" },
    { name: "1e-400 as a long token is zero by rounding", tok: "0." + zeros(900) + "1e500", status: "underflow" },
    { name: "a long fraction of 1", tok: "1." + zeros(2000), want: 1, status: "ok" },
    { name: "a long fraction of 1, with an exponent", tok: "1." + zeros(2000) + "e2", want: 100, status: "ok" },
    { name: "leading zeros", tok: "0." + zeros(1500) + "5e1501", want: 5, status: "ok" },
    { name: "negative", tok: "-1" + zeros(800) + "e-800", want: -1, status: "ok" },
    { name: "every digit zero", tok: "0." + zeros(1000) + "e-99999", want: 0, status: "ok", exact: false },
    { name: "the largest binary64, with trailing zeros", tok: "1.7976931348623157" + zeros(900) + "e308", want: Number.MAX_VALUE, status: "ok" },
    { name: "just above the largest, rounds to it", tok: "1.7976931348623158" + zeros(900) + "e308", want: Number.MAX_VALUE, status: "ok" },
    { name: "above the halfway to 2^1024: not finite", tok: "1.797693134862315808" + zeros(900) + "e308", status: "infinite" },
    { name: "the smallest subnormal, long", tok: "4.9406564584124654" + zeros(800) + "e-324", want: 5e-324, status: "ok" },
    { name: "below half the smallest subnormal", tok: "2.4703282292062327" + zeros(800) + "e-324", status: "underflow" },
    { name: "an exponent that does not fit 64 bits", tok: "1" + zeros(800) + "e-" + "9".repeat(40), status: "underflow", exact: false },
    { name: "an exponent that does not fit 64 bits, the other way", tok: "1" + zeros(800) + "e" + "9".repeat(40), status: "infinite", exact: false },
  ];
  for (const t of table) {
    it(t.name, () => {
      const got = denote(t.tok);
      expect(got.status).toBe(t.status);
      if (t.status === "ok") expect(Object.is(got.value, t.want as number) || (got.value === 0 && t.want === 0)).toBe(true);
      // The same answer from the reference, so that a wrong row of the table is found (not for an exponent that BigInt cannot take).
      if (t.exact !== false) {
        const ref = refNumber(t.tok);
        expect(ref.status).toBe(t.status);
        if (t.status === "ok") expect(ref.value).toBe(t.want);
      }
    });
  }

  it("the reference itself is right on the numbers that every runtime reads (a sanity check of the oracle)", () => {
    for (const tok of ["0", "1", "0.1", "1e22", "1e23", "9007199254740993", "9007199254740995", "4.9e-324", "1.7976931348623157e308", "2.2250738585072014e-308", "123456789012345678901234567890"]) {
      expect(refNumber(tok).value, tok).toBe(Number(tok));
    }
  });

  // Differential test: tokens of 1 to 3,000 digits, with runs of zeros, the point and the exponent anywhere, around the edges of the
  // binary64 range, and the exact midpoints of two neighbours with a digit beyond them (the rounding ties), against BigInt arithmetic.
  // `denote` agrees on the value, bit for bit, and on whether 05 [STATE-7] accepts it.
  it("agrees with BigInt arithmetic on random tokens, near the edges of the range, and on the ties", () => {
    let seed = 0x5eed;
    const rand = (): number => {
      // mulberry32
      seed = (seed + 0x6d2b79f5) | 0;
      let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
    const int = (n: number): number => Math.floor(rand() * n);
    const digitRun = (): string => {
      const want = 1 + int(3000);
      let s = "";
      while (s.length < want) {
        switch (int(4)) {
          case 0:
            s += zeros(int(1200));
            break;
          case 1:
            s += "9".repeat(int(60));
            break;
          default:
            s += String(int(10));
        }
      }
      return s;
    };
    for (let i = 0; i < 1500; i++) {
      const ds = digitRun();
      // Split into an integer part with no leading zero (or 0) and a fraction.
      const cut = int(ds.length + 1);
      let intPart = ds.slice(0, cut).replace(/^0+/, "");
      let frac = ds.slice(cut);
      if (intPart === "") intPart = "0";
      let tok = intPart;
      if (frac !== "" && int(3) !== 0) tok += "." + frac;
      else frac = "";
      // The exponent that puts the value near the edges of the range, or anywhere in the bound of the test.
      const sig = (intPart + frac).replace(/^0+/, "").length;
      let e: number;
      switch (int(4)) {
        case 0:
          e = -sig - int(40) + 300 - 20; // the value is near 10^300
          break;
        case 1:
          e = 310 - intPart.length - int(20); // near the top of the range
          break;
        case 2:
          e = -320 - intPart.length + int(20); // near the bottom
          break;
        default:
          e = int(6001) - 3000;
      }
      if (int(5) !== 0) tok += "e" + (e < 0 ? "-" : "+") + String(Math.abs(e));
      if (int(4) === 0) tok = "-" + tok;
      expectAgrees(tok);
    }
    // The ties: the exact value halfway between two neighbouring binary64 values, which rounds to even, then the same with a 1 far below
    // it, which rounds up, written with enough zeros to be a long token.
    const dv = new DataView(new ArrayBuffer(8));
    for (let i = 0; i < 150; i++) {
      dv.setUint32(0, int(0x7fefffff)); // a positive finite value: the sign bit is clear and the exponent is below 2047
      dv.setUint32(4, int(0x100000000));
      const bits = dv.getBigUint64(0);
      // the midpoint of x and its successor, as an exact decimal: x and y are dyadic, so (x + y) / 2 has a finite decimal text
      const exact = midpointDecimal(bits);
      expectAgrees(exact);
      expectAgrees(exact + zeros(900));
      expectAgrees(exact + zeros(900) + "1");
    }
  });

  it("the sites that read a number token agree: a json/v1 value, the manifest's canonical document, the JSON text of a CRDT update", () => {
    const long = "1" + zeros(1499) + "e-900"; // 10^599
    for (const doc of [`{"a":${long}}`, `[${long}]`]) {
      expect(() => canonicalDocument(utf8Encode(doc)), doc.slice(0, 30)).toThrow(JsonV1Error);
      expect(validate(utf8Encode(doc)), "a number of 10^599 was accepted as a value").not.toBeNull();
      expect(checkJsonText(utf8Encode(doc)), "a number of 10^599 was accepted in the JSON text of an update").not.toBeNull();
    }
    const one = "1" + zeros(800) + "e-800";
    expect(new TextDecoder().decode(canonicalDocument(utf8Encode(`{"a":${one}}`)))).toBe('{"a":1}');
    expect(checkJsonText(utf8Encode(`{"a":${one}}`))).toBeNull();
    // and in a CRDT update (05 [STATE-71]): an embed whose JSON text holds the long number is SCHEMA_INVALID with reason json
    const embed = (text: string): Uint8Array => {
      const t = utf8Encode(text);
      const len = (n: number): number[] => (n < 128 ? [n] : [(n & 127) | 128, n >> 7]);
      // one group of one item: client 1, clock 0, info 0x05 (ContentEmbed, no origin), parent root "t", the JSON text, no deletes
      return new Uint8Array([1, 1, 1, 0, 0x05, 1, 1, 0x74, ...len(t.length), ...t, 0]);
    };
    expect(() => checkWellFormed(embed(`{"a":${long}}`))).toThrow(/json/);
    expect(() => checkWellFormed(embed(`{"a":${one}}`))).not.toThrow();
  });
});

/** The decimal text of (x + y) / 2 for the binary64 x with the bits and its successor y, exactly (both are dyadic rationals). */
function midpointDecimal(bits: bigint): string {
  const asRational = (b: bigint): { m: bigint; e: number } => {
    const frac = b & ((1n << 52n) - 1n);
    const exp = Number((b >> 52n) & 0x7ffn);
    return exp === 0 ? { m: frac, e: -1074 } : { m: frac | (1n << 52n), e: exp - 1075 };
  };
  const x = asRational(bits);
  const y = asRational(bits + 1n);
  // x = mx * 2^ex, y = my * 2^ey; the midpoint is (mx*2^(ex-lo) + my*2^(ey-lo)) * 2^(lo-1) over a common exponent lo = min(ex, ey)
  const lo = Math.min(x.e, y.e);
  const sum = (x.m << BigInt(x.e - lo)) + (y.m << BigInt(y.e - lo));
  const exp2 = lo - 1; // the midpoint is sum * 2^exp2
  if (exp2 >= 0) return (sum << BigInt(exp2)).toString();
  // sum * 2^exp2 = sum * 5^k / 10^k with k = -exp2: an integer over a power of ten
  const k = -exp2;
  const scaled = (sum * 5n ** BigInt(k)).toString().padStart(k + 1, "0");
  const intPart = scaled.slice(0, scaled.length - k);
  const frac = scaled.slice(scaled.length - k).replace(/0+$/, "");
  return frac === "" ? intPart : `${intPart}.${frac}`;
}

describe("04 [MAN-4] (V1 SHOULD-6): a manifest document of more values than 1 MiB can hold is refused before the tree is built", () => {
  const array = (n: number): Uint8Array => utf8Encode(`[${Array(n).fill("0").join(",")}]`);

  it("the cap is exact: the canonical form of V values has at least 2V-1 octets", () => {
    expect(MAX_DOCUMENT_VALUES).toBe(524288);
    // 524,287 items and the array itself: 524,288 values, a canonical form of 1,048,575 octets
    expect(() => parseDocument(array(MAX_DOCUMENT_VALUES - 1))).not.toThrow();
    expect(canonicalDocument(array(MAX_DOCUMENT_VALUES - 1)).length).toBe(2 * (MAX_DOCUMENT_VALUES - 1) + 1);
    // one more is 524,289 values, and no document of that many has a canonical form within 1 MiB
    let err: unknown;
    try {
      parseDocument(array(MAX_DOCUMENT_VALUES));
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(JsonV1Error);
    expect(err).toMatchObject({ code: "INVALID_ENVELOPE", reason: "json-limits", rule: "MAN-4" });
    expect((err as JsonV1Error).message).toMatch(/more than 524288 values/);
  });

  it("a 4 MiB document of values is refused after the cap, not after the whole text", () => {
    const big = array(2_000_000); // about 4 MiB of values
    const start = performance.now();
    expect(() => parseDocument(big)).toThrow(/more than 524288 values/);
    expect(performance.now() - start).toBeLessThan(2000);
  });

  it("objects and nested values count too, a member's value and an item each once", () => {
    // [{"a":0},{"a":0},...]: each object and each member's value, and the array: 2 x 262,144 + 1 values
    const objects = (n: number): Uint8Array => utf8Encode(`[${Array(n).fill('{"a":0}').join(",")}]`);
    expect(() => parseDocument(objects(262143))).not.toThrow(); // 524,287 values
    expect(() => parseDocument(objects(262144))).toThrow(/more than 524288 values/); // 524,289 values
    const nested = `[${Array(300000).fill("[]").join(",")}]`; // 300,001 arrays, each a value
    expect(() => parseDocument(utf8Encode(nested))).not.toThrow();
    expect(() => parseDocument(utf8Encode(`[${Array(524288).fill("[]").join(",")}]`))).toThrow(/more than 524288 values/);
  });

  it("a json/v1 value is not a manifest document: no value count applies", () => {
    expect(() => parse(array(600_000))).not.toThrow(); // within the 4 MiB of a value, though over the value count of a document
  });
});

describe("05 [STATE-7], 04 [MAN-26] (V1 SHOULD-8): an error message never repeats the input whole", () => {
  const long = "x".repeat(20000);
  it("a number token, a duplicate member name and a member that the schema does not have", () => {
    const messages: string[] = [];
    const collect = (f: () => unknown): void => {
      let r: unknown;
      try {
        r = f();
      } catch (e) {
        r = e;
      }
      if (r instanceof Error) messages.push(r.message);
    };
    collect(() => validate(utf8Encode(`1${zeros(20000)}`)));
    collect(() => validate(utf8Encode(`{"${long}":1,"${long}":2}`)));
    collect(() => parse(utf8Encode(`{"${long}":1,"${long}":2}`)));
    collect(() => parseDocument(utf8Encode(`{"${long}":1,"${long}":2}`)));
    const schema = parseSchema(utf8Encode('{"type":"object","fields":{"a":{"type":"int"}}}'), "json");
    collect(() => validate(utf8Encode(`{"${long}":1}`), schema));
    for (const m of messages) expect(m.length, m.slice(0, 100)).toBeLessThan(300);
    expect(messages.length).toBeGreaterThanOrEqual(4);
  });

  it("a name or a value of a value schema in a definition error and its path", () => {
    const attempts = [
      `{"type":"object","fields":{"${long}":{"type":"int"}}}`,
      `{"type":"object","${long}":1}`,
      `{"type":"${long}"}`,
      `{"enum":["a","a"],"type":"string","${long}":1}`,
      `{"type":"object","fields":{"a":{"type":"int"}},"required":["${long}"]}`,
      `{"type":"object","fields":{"a":{"type":"object","fields":{"${long}":{"type":"int"}}}}}`,
    ];
    for (const a of attempts) {
      let err: unknown;
      try {
        parseSchema(utf8Encode(a), "json");
      } catch (e) {
        err = e;
      }
      expect(err, a.slice(0, 60)).toBeInstanceOf(JsonV1Error);
      expect((err as JsonV1Error).message.length, (err as JsonV1Error).message.slice(0, 100)).toBeLessThan(400);
      expect((err as JsonV1Error).path.length).toBeLessThan(400);
    }
  });

  it("the echo is cut at a character, never between the halves of a pair, and says how long the input was", () => {
    const name = "😀".repeat(40); // 80 UTF-16 code units, 160 octets
    let err: unknown;
    try {
      validate(utf8Encode(`{"${name}":1,"${name}":2}`));
      parse(utf8Encode(`{"${name}":1,"${name}":2}`));
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(JsonV1Error);
    const m = (err as JsonV1Error).message;
    expect(m).toContain("(160 octets)");
    expect(m).not.toMatch(/[\ud800-\udbff](?![\udc00-\udfff])/); // no half of a pair
    expect(m).not.toContain(name);
  });
});

describe("04 [MAN-26] (V1 SHOULD-9): a required name counts once however often a Schema lists it", () => {
  it("an object that holds every distinct required name is valid, whatever the list says", () => {
    const parsed = parseSchema(utf8Encode('{"type":"object","fields":{"a":{"type":"int"},"b":{"type":"int"}},"required":["a","b"]}'), "json");
    // a Schema that a caller built with a name listed twice (the fields are readonly in the type, not in the language)
    const hand = { ...parsed, required: ["a", "a", "b"] } as Schema;
    expect(validate(utf8Encode('{"a":1,"b":2}'), hand)).toBeNull();
    expect(validate(utf8Encode('{"a":1}'), hand)).not.toBeNull(); // b is missing
    // and Q5, ruled: a name listed twice in a manifest is a set
    const dup = parseSchema(utf8Encode('{"type":"object","fields":{"a":{"type":"int"}},"required":["a","a"]}'), "json");
    expect(validate(utf8Encode('{"a":1}'), dup)).toBeNull();
  });
});
