// A reader of the lib0 encodings that Yjs update format v1 uses. Every read checks the bytes that are left, so a length field
// never drives an allocation or a slice beyond the input (02 [ARC-46]).

import { isValidUtf8 } from "../bytes.js";
import { fail } from "./errors.js";

/** Integers below this are exact in a JavaScript number. */
const EXACT = Number.MAX_SAFE_INTEGER;

export class Reader {
  p = 0;

  constructor(readonly b: Uint8Array) {}

  remaining(): number {
    return this.b.length - this.p;
  }

  u8(): number {
    if (this.p >= this.b.length) throw fail("truncated", `at offset ${this.p}`);
    return this.b[this.p++] as number;
  }

  /** Returns the next n octets, without copying them. n may be any number: one that is more than the octets left is a refusal. */
  take(n: number): Uint8Array {
    if (n > this.remaining()) throw fail("truncated", `need ${n} bytes at offset ${this.p}`);
    const out = this.b.subarray(this.p, this.p + n);
    this.p += n;
    return out;
  }

  /**
   * Reads a lib0 varUint. 05 [STATE-71] (Parse): a varUint of more than one byte never ends in a 0x00 byte, since that is not
   * its shortest form. A value above 2^53−1 reads as a number above it (Infinity once it does not fit 64 bits): every caller
   * bounds a value far below that (a client id by 2^53−1, a clock by 2^32−1, a length by the bytes left), so the inexact value
   * fails the same check that the exact one would.
   */
  varUint(): number {
    let v = 0;
    let shift = 0;
    for (let count = 1; ; count++) {
      const b = this.u8();
      v = addBits(v, b & 0x7f, shift);
      shift += 7;
      if (b < 0x80) {
        if (count > 1 && b === 0) throw fail("varint-not-shortest", `varUint at offset ${this.p - 1}`);
        return v;
      }
    }
  }

  /**
   * Reads a lib0 varInt: 6 bits in the first byte, sign 0x40, continuation 0x80, then 7 bits per byte. It returns the sign and
   * the magnitude (saturating like varUint). 05 [STATE-71] (Parse): the one-byte 0x40, which is −0, is valid; a multi-byte
   * varInt never ends in a 0x00 byte.
   */
  varInt(): { neg: boolean; mag: number } {
    const first = this.u8();
    let mag = first & 0x3f;
    const neg = (first & 0x40) !== 0;
    if ((first & 0x80) === 0) return { neg, mag };
    let shift = 6;
    for (;;) {
      const b = this.u8();
      mag = addBits(mag, b & 0x7f, shift);
      shift += 7;
      if (b < 0x80) {
        if (b === 0) throw fail("varint-not-shortest", `varInt at offset ${this.p - 1}`);
        return { neg, mag };
      }
    }
  }

  /**
   * Reads a varUint-prefixed string. 05 [STATE-71] (Strings): it is valid UTF-8, which refuses the surrogate code points,
   * overlong forms and values above U+10FFFF. A leading U+FEFF is an ordinary character here. The octets alias the input.
   */
  strBytes(): Uint8Array {
    const n = this.varUint();
    const raw = this.take(n);
    if (!isValidUtf8(raw)) throw fail("utf8", `invalid UTF-8 in the string that ends at offset ${this.p}`);
    return raw;
  }
}

/** Returns v + bits·2^shift, or a number above 2^53−1 (Infinity) when the bits do not fit. */
function addBits(v: number, bits: number, shift: number): number {
  if (v === Infinity || bits === 0) return v;
  if (shift >= 64) return Infinity;
  const add = bits * 2 ** shift;
  const r = v + add;
  return r > EXACT ? Math.max(r, EXACT + 1) : r;
}
