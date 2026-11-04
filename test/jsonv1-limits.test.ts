// R19 review S5: `checkLimits` reads a json/v1 text for the limits of 05 [STATE-7] alone (nesting over 32, over 1,024 members in an object, U+0000 in a string or a member name), whatever else the text holds.

import { describe, expect, it } from "vitest";
import { checkLimits, validate } from "../src/rules/jsonv1/index.js";

const J = (s: string): Uint8Array => new TextEncoder().encode(s);
const BS = String.fromCharCode(92);
const nest = (n: number): string => "[".repeat(n) + "]".repeat(n);
const members = (n: number): string => Array.from({ length: n }, (_, i) => `"m${String(i).padStart(4, "0")}":0`).join(",");

describe("05 [STATE-7]: checkLimits", () => {
  it("finds a limit that the reader does not reach, because it stops at another defect first", () => {
    const hidden: [string, Uint8Array][] = [
      ["U+0000 after a duplicate member", J(`{"a":1,"a":2,"b":"${BS}u0000"}`)],
      ["U+0000 and a number that is no binary64 value", J(`["${BS}u0000",1e400]`)],
      ["nesting of 33 after a duplicate member", J(`{"a":1,"a":${nest(33)}}`)],
      ["nesting of 33 after text that is no JSON", J(`{"a":}${nest(33)}`)],
      ["1,025 members with a first name that is repeated", J(`{"m0000":0,${members(1025)}}`)],
      ["1,025 members, one of the names spelled by an escape", J(`{"${BS}u006d0000":0,${members(1025)}}`)],
      ["U+0000 in a member name, after an octet that is not UTF-8", new Uint8Array([...J('["'), 0xff, ...J(`","a"]`), ...J(`{"${BS}u0000":1}`)])],
    ];
    for (const [name, b] of hidden) {
      const err = checkLimits(b);
      expect(err?.reason, name).toBe("json-limits");
      expect(validate(b)?.reason, `${name}: the reader reports another defect first`).not.toBe("json-limits");
    }
  });

  it("finds the limits that the reader finds, with the reader's own count", () => {
    for (const b of [J(nest(33)), J(`{${members(1025)}}`), J(`"${BS}u0000"`), J(`{"${BS}u0000":1}`)]) {
      expect(checkLimits(b)?.reason).toBe("json-limits");
      expect(validate(b)?.reason).toBe("json-limits");
    }
  });

  it("finds none in a text that is within them, whatever else it holds", () => {
    const within: [string, Uint8Array][] = [
      ["nesting of 32", J(nest(32))],
      ["1,024 members", J(`{${members(1024)}}`)],
      ["1,024 members and a duplicate of one", J(`{"m0000":0,${members(1024)}}`)],
      ["an escaped backslash before u0000, which is no U+0000", J(`"${BS}${BS}u0000"`)],
      ["brackets and quotes inside a string", J(`["[[[[[{{{{{", "}}}}", "\\"[[["]`)],
      ["a duplicate member", J('{"a":1,"a":2}')],
      ["text that is no JSON", J("{{{ not json")],
      ["an unterminated string", J('["abc')],
      ["the empty text", J("")],
    ];
    for (const [name, b] of within) expect(checkLimits(b), name).toBeNull();
  });

  it("is one pass that holds at most 1,025 names for each object and never recurses: a text of 4 MiB of brackets is read at once", () => {
    const b = new Uint8Array(4 << 20).fill(0x5b); // `[` repeated
    expect(checkLimits(b)?.reason).toBe("json-limits"); // stops at the 33rd
    expect(checkLimits(new Uint8Array(4 << 20).fill(0x7d))).toBeNull(); // `}` repeated: closes nothing
  });
});
