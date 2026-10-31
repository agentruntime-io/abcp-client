// Byte helpers shared by the rules modules: hex, concatenation, strict UTF-8, byte order of strings, and the big-endian
// writer that the hashes of 05 [STATE-41] and 06 [LOG-71] use. Nothing here throws on untrusted input except `fromHex`,
// which is for tests and trusted constants.

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { ignoreBOM: true });

/** Returns the lowercase hex text of b. */
export function toHex(b: Uint8Array): string {
  let s = "";
  for (let i = 0; i < b.length; i++) {
    s += (b[i] as number).toString(16).padStart(2, "0");
  }
  return s;
}

/** Returns the octets of a hex text (either case); throws a RangeError on a text that is not hex. For trusted input. */
export function fromHex(s: string): Uint8Array {
  if (s.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(s)) {
    throw new RangeError("not a hex text");
  }
  const out = new Uint8Array(s.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = parseInt(s.slice(2 * i, 2 * i + 2), 16);
  }
  return out;
}

/** Concatenates the parts into one new array. */
export function concat(...parts: Uint8Array[]): Uint8Array {
  let n = 0;
  for (const p of parts) n += p.length;
  const out = new Uint8Array(n);
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
}

/** True when a and b hold the same octets. */
export function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

/** Compares two octet strings in byte order. */
export function compareBytes(a: Uint8Array, b: Uint8Array): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const x = a[i] as number;
    const y = b[i] as number;
    if (x !== y) return x < y ? -1 : 1;
  }
  return a.length === b.length ? 0 : a.length < b.length ? -1 : 1;
}

/**
 * Compares two strings by the byte order of their UTF-8 encodings, which is the order of their code points: the
 * order that 05 [STATE-41] and [STATE-84] give keys. JavaScript's own `<` compares UTF-16 code units, which differs for a
 * character above U+FFFF against one from U+E000 to U+FFFF.
 */
export function compareUtf8(a: string, b: string): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    let x = a.charCodeAt(i);
    let y = b.charCodeAt(i);
    if (x !== y) {
      if (x >= 0xd800 && y >= 0xd800) {
        // move the surrogates (D800 to DFFF) above E000 to FFFF, as code point order has them
        x = x >= 0xe000 ? x - 0x800 : x + 0x2000;
        y = y >= 0xe000 ? y - 0x800 : y + 0x2000;
      }
      return x < y ? -1 : 1;
    }
  }
  return a.length === b.length ? 0 : a.length < b.length ? -1 : 1;
}

/** Compares two strings by the order of their UTF-16 code units, the order of RFC 8785 §3.2.3 for member names. */
export function compareUtf16(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

/** Encodes s as UTF-8. A string that holds an unpaired surrogate has no UTF-8 form: callers check `hasLoneSurrogate` first. */
export function utf8Encode(s: string): Uint8Array {
  return encoder.encode(s);
}

/** Decodes octets that are known to be valid UTF-8 (see `isValidUtf8`), keeping a leading U+FEFF as a character. */
export function utf8DecodeValid(b: Uint8Array): string {
  return decoder.decode(b);
}

/** True when s holds a UTF-16 surrogate that is not part of a pair: such a string has no UTF-8 form (02 [ARC-61]). */
export function hasLoneSurrogate(s: string): boolean {
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdbff) {
      const d = i + 1 < s.length ? s.charCodeAt(i + 1) : 0;
      if (d >= 0xdc00 && d <= 0xdfff) {
        i++;
        continue;
      }
      return true;
    }
    if (c >= 0xdc00 && c <= 0xdfff) return true;
  }
  return false;
}

/**
 * True when b is valid UTF-8 (RFC 3629): no overlong form, no surrogate code point, nothing above U+10FFFF, no stray or
 * missing continuation byte. A leading byte-order mark is an ordinary character. The check reads the octets themselves,
 * so a runtime's repairing decoder never sees invalid bytes (02 [ARC-61], note).
 */
export function isValidUtf8(b: Uint8Array, start = 0, end = b.length): boolean {
  let i = start;
  while (i < end) {
    const c = b[i] as number;
    if (c < 0x80) {
      i++;
    } else if (c >= 0xc2 && c <= 0xdf) {
      if (i + 1 >= end || ((b[i + 1] as number) & 0xc0) !== 0x80) return false;
      i += 2;
    } else if (c >= 0xe0 && c <= 0xef) {
      if (i + 2 >= end) return false;
      const c1 = b[i + 1] as number;
      const c2 = b[i + 2] as number;
      if ((c1 & 0xc0) !== 0x80 || (c2 & 0xc0) !== 0x80) return false;
      if (c === 0xe0 && c1 < 0xa0) return false; // overlong
      if (c === 0xed && c1 >= 0xa0) return false; // a surrogate code point
      i += 3;
    } else if (c >= 0xf0 && c <= 0xf4) {
      if (i + 3 >= end) return false;
      const c1 = b[i + 1] as number;
      const c2 = b[i + 2] as number;
      const c3 = b[i + 3] as number;
      if ((c1 & 0xc0) !== 0x80 || (c2 & 0xc0) !== 0x80 || (c3 & 0xc0) !== 0x80) return false;
      if (c === 0xf0 && c1 < 0x90) return false; // overlong
      if (c === 0xf4 && c1 >= 0x90) return false; // above U+10FFFF
      i += 4;
    } else {
      return false;
    }
  }
  return true;
}

/** A growing byte buffer with the big-endian writers of 05 [STATE-41]: u32, u64, i64, and L(b) = u32(len) || b. */
export class ByteWriter {
  private buf = new Uint8Array(64);
  private len = 0;

  private grow(n: number): void {
    if (this.len + n <= this.buf.length) return;
    let cap = this.buf.length * 2;
    while (cap < this.len + n) cap *= 2;
    const next = new Uint8Array(cap);
    next.set(this.buf.subarray(0, this.len));
    this.buf = next;
  }

  u8(v: number): this {
    this.grow(1);
    this.buf[this.len++] = v & 0xff;
    return this;
  }

  /** A 32-bit big-endian unsigned integer. */
  u32(v: number): this {
    if (!Number.isInteger(v) || v < 0 || v > 0xffffffff) throw new RangeError("u32 out of range");
    this.grow(4);
    new DataView(this.buf.buffer).setUint32(this.len, v, false);
    this.len += 4;
    return this;
  }

  /** A 64-bit big-endian unsigned integer. */
  u64(v: bigint | number): this {
    const b = typeof v === "bigint" ? v : BigInt(v);
    if (b < 0n || b > 0xffffffffffffffffn) throw new RangeError("u64 out of range");
    this.grow(8);
    new DataView(this.buf.buffer).setBigUint64(this.len, b, false);
    this.len += 8;
    return this;
  }

  /** A 64-bit big-endian two's-complement integer. */
  i64(v: bigint | number): this {
    const b = typeof v === "bigint" ? v : BigInt(v);
    if (b < -(1n << 63n) || b >= 1n << 63n) throw new RangeError("i64 out of range");
    this.grow(8);
    new DataView(this.buf.buffer).setBigInt64(this.len, b, false);
    this.len += 8;
    return this;
  }

  /** A protobuf varint of a non-negative integer that a number holds exactly (a tag, a length), in its shortest form. */
  varint(n: number): this {
    if (!Number.isSafeInteger(n) || n < 0) throw new RangeError("varint out of range");
    while (n >= 0x80) {
      this.u8((n % 0x80) | 0x80);
      n = Math.floor(n / 0x80);
    }
    return this.u8(n);
  }

  /** A protobuf varint of an unsigned 64-bit integer, in its shortest form. */
  varintBig(v: bigint): this {
    if (v < 0n || v > 0xffffffffffffffffn) throw new RangeError("varint out of range");
    while (v >= 0x80n) {
      this.u8(Number(v & 0x7fn) | 0x80);
      v >>= 7n;
    }
    return this.u8(Number(v));
  }

  bytes(b: Uint8Array): this {
    this.grow(b.length);
    this.buf.set(b, this.len);
    this.len += b.length;
    return this;
  }

  /** A protobuf length-delimited payload: the length of b as a varint in its shortest form, then b. */
  lbytesVarint(b: Uint8Array): this {
    return this.varint(b.length).bytes(b);
  }

  /** L(b): the length of b as a u32, then b. */
  lbytes(b: Uint8Array): this {
    return this.u32(b.length).bytes(b);
  }

  /** L of the UTF-8 octets of s. */
  lstring(s: string): this {
    return this.lbytes(utf8Encode(s));
  }

  /** The number of octets written so far. */
  get length(): number {
    return this.len;
  }

  finish(): Uint8Array {
    return this.buf.slice(0, this.len);
  }
}
