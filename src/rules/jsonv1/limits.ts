// The limits of 05 [STATE-7] on their own: nesting over 32, an object of over 1,024 members, U+0000 in a string or a member name.
//
// 16 [SEC-3] judges a record against "the `json/v1` limits of 05 [STATE-7] always", and a record that breaks them is structurally invalid; 14 [CLI-5] keeps any other defect of a value's text (not JSON, a
// duplicate member, a number that is no binary64 value, bytes that are not UTF-8, not canonical form) as a value that the client cannot decode. `validate` reports the first defect of its order and stops
// at the syntax, duplicate-member, surrogate, noncharacter and number defects, so a value that has such a defect before a limit would have its limit left unjudged (R19 review S5). This scan reads
// the text for the limits alone and goes on past every other defect.
//
// It is one iterative pass over the octets (02 [ARC-46]: no recursion, every length bounded): the nesting depth is a counter that stops the scan at 33, the names of an object are a set that stops at
// 1,025 (so the sets together hold at most 32 x 1,025 names), a string is skipped by its quotes and its escapes. It does not check the grammar: a text that is no JSON is read as far as it goes, and
// the limits that it breaks before it stops are found. A member name is a string that follows `{` or `,` inside an object; it is decoded (escapes and UTF-8, a byte that is not UTF-8 as U+FFFD) so that
// two spellings of one name are one name, as the reader counts them (a duplicate member is not counted twice).

import { MAX_DEPTH, MAX_MEMBERS } from "./value.js";
import { type JsonV1Error, valueError } from "./errors.js";

interface Frame {
  /** The names of an object read so far; null for an array. */
  names: Set<string> | null;
  /** The next string of an object is a member name. */
  name: boolean;
}

const utf8 = new TextDecoder("utf-8", { fatal: false });

/** Reads four hex digits at b[j..], in either case, or returns -1. */
function hex4(b: Uint8Array, j: number): number {
  if (j < 0 || j + 4 > b.length) return -1;
  let v = 0;
  for (let k = 0; k < 4; k++) {
    const c = b[j + k] as number;
    let d: number;
    if (c >= 0x30 && c <= 0x39) d = c - 0x30;
    else if (c >= 0x61 && c <= 0x66) d = c - 0x61 + 10;
    else if (c >= 0x41 && c <= 0x46) d = c - 0x41 + 10;
    else return -1;
    v = (v << 4) | d;
  }
  return v;
}

const ESCAPES: Readonly<Record<number, string>> = { 0x22: '"', 0x5c: "\\", 0x2f: "/", 0x62: "\b", 0x66: "\f", 0x6e: "\n", 0x72: "\r", 0x74: "\t" };

/**
 * Checks b for the limits of 05 [STATE-7] alone, whatever else the text holds. Returns null when the text breaks none before it ends (or stops being readable), and a JsonV1Error of reason `json-limits`
 * for the first limit that it breaks otherwise.
 */
export function checkLimits(b: Uint8Array): JsonV1Error | null {
  const stack: Frame[] = [];
  const n = b.length;
  let i = 0;
  while (i < n) {
    const c = b[i] as number;
    switch (c) {
      case 0x7b: // {
      case 0x5b: // [
        stack.push({ names: c === 0x7b ? new Set<string>() : null, name: c === 0x7b });
        if (stack.length > MAX_DEPTH) return valueError("json-limits", "STATE-7", `nesting deeper than ${MAX_DEPTH} (byte ${i})`);
        i++;
        break;
      case 0x7d: // }
      case 0x5d: // ]
        stack.pop();
        i++;
        break;
      case 0x2c: {
        // , : the next string of an object is a member name
        const top = stack[stack.length - 1];
        if (top !== undefined && top.names !== null) top.name = true;
        i++;
        break;
      }
      case 0x3a: {
        // : the member's value follows
        const top = stack[stack.length - 1];
        if (top !== undefined && top.names !== null) top.name = false;
        i++;
        break;
      }
      case 0x22: {
        const top = stack[stack.length - 1];
        const isName = top !== undefined && top.names !== null && top.name;
        // the string: its decoded characters are kept for a member name only
        let name = "";
        let start = i + 1;
        let j = start;
        let closed = false;
        while (j < n) {
          const d = b[j] as number;
          if (d === 0x22) {
            closed = true;
            break;
          }
          if (d !== 0x5c) {
            j++;
            continue;
          }
          // an escape: \uXXXX or one character
          if (isName) name += utf8.decode(b.subarray(start, j));
          const e = b[j + 1];
          if (e === 0x75) {
            const cp = hex4(b, j + 2);
            if (cp === 0) return valueError("json-limits", "STATE-7", `a string or an object member name holds U+0000 (byte ${j})`);
            if (isName) name += cp < 0 ? "\\u" : String.fromCharCode(cp);
            j += cp < 0 ? 2 : 6;
          } else {
            if (isName && e !== undefined) name += ESCAPES[e] ?? String.fromCharCode(e);
            j += 2;
          }
          start = j;
        }
        if (!closed) return null; // an unterminated string: the text ends here
        if (isName) {
          name += utf8.decode(b.subarray(start, j));
          const names = (top as Frame).names as Set<string>;
          if (!names.has(name)) {
            if (names.size >= MAX_MEMBERS) return valueError("json-limits", "STATE-7", `more than ${MAX_MEMBERS} members (byte ${i})`);
            names.add(name);
          }
          (top as Frame).name = false;
        }
        i = j + 1;
        break;
      }
      default:
        i++;
    }
  }
  return null;
}
