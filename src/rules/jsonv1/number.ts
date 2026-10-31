// Numbers of json/v1 (05 [STATE-7]): what a token denotes, and its RFC 8785 text.

/**
 * The RFC 8785 text of the number f (§3.2.2.3), which takes it from the ECMAScript Number::toString algorithm: the
 * shortest digits that round to f. JavaScript's own `String(f)` is that algorithm, with −0 written as 0. NaN and the
 * infinities have no text and give a RangeError (a caller never passes one: a number that `parse` returned is finite).
 */
export function numberText(f: number): string {
  if (!Number.isFinite(f)) throw new RangeError("a non-finite number has no JSON text");
  return f === 0 ? "0" : String(f);
}

export type NumberStatus = "ok" | "infinite" | "underflow";

export interface Denoted {
  value: number;
  status: NumberStatus;
  /** True when the token is an integer text of at most 15 digits, exact in binary64 and its own canonical text unless it is -0. */
  plain: boolean;
}

/**
 * Returns the binary64 value that the JSON number token tok denotes, rounded to nearest (ties to even), and says whether
 * 05 [STATE-7] accepts it: a number is valid when the rounded value is finite and is ±0 only when every digit of its
 * text before any exponent is 0 (`1e400` and `1e-400` are not valid). tok must match the JSON number grammar (RFC 8259 §6).
 */
export function denote(tok: string): Denoted {
  const neg = tok.charCodeAt(0) === 0x2d;
  const digitsLen = tok.length - (neg ? 1 : 0);
  if (digitsLen <= 15) {
    let plain = true;
    let n = 0;
    for (let i = neg ? 1 : 0; i < tok.length; i++) {
      const c = tok.charCodeAt(i);
      if (c < 0x30 || c > 0x39) {
        plain = false;
        break;
      }
      n = n * 10 + (c - 0x30);
    }
    if (plain) return { value: neg ? -n : n, status: "ok", plain: true };
  }
  const f = Number(tok);
  if (!Number.isFinite(f)) return { value: f, status: "infinite", plain: false };
  if (f === 0 && hasNonzeroMantissaDigit(tok)) return { value: f, status: "underflow", plain: false };
  return { value: f, status: "ok", plain: false };
}

/** True when a digit 1 to 9 occurs in tok before any exponent. */
export function hasNonzeroMantissaDigit(tok: string): boolean {
  for (let i = 0; i < tok.length; i++) {
    const c = tok.charCodeAt(i);
    if (c === 0x65 || c === 0x45) return false; // e or E
    if (c >= 0x31 && c <= 0x39) return true;
  }
  return false;
}
