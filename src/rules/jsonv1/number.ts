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
 *
 * It is right for a token of any length (V1 MUST-2, the twin of the Go `jsonv1.ParseNumber`): every check of a JSON number against
 * [STATE-7] goes through it, so a json/v1 value, the manifest's canonical document (04 [MAN-4]) and the JSON texts of a CRDT update
 * (05 [STATE-71]) agree with the other parties on a token of a million digits.
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
  const [value, status] = tok.length <= LONG_TOKEN ? toBinary64(tok) : parseLongDecimal(tok, neg);
  return { value, status, plain: false };
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

/**
 * The token length from which `Number(tok)` is not left to read the text as it stands. A decimal reader may keep only some hundreds
 * of digits and place the point by those it kept (Go's `strconv.ParseFloat` keeps 800 and takes the point to follow the 800th, so
 * `1`, 1,499 zeros, `e-900`, which is 10^599 and not finite, came out as 10^-101); a token of up to this many characters has fewer than
 * 800 digits, and every reader is right for it.
 */
const LONG_TOKEN = 700;

/** `Number(tok)` and what 05 [STATE-7] says of the value, for a token that the reader handles right. */
function toBinary64(tok: string): [number, NumberStatus] {
  const f = Number(tok);
  if (!Number.isFinite(f)) return [f, "infinite"];
  if (f === 0 && hasNonzeroMantissaDigit(tok)) return [f, "underflow"];
  return [f, "ok"];
}

/**
 * The decimal exponent beyond which a nonzero number cannot round to a finite nonzero binary64: the largest is below 10^309 and the
 * smallest subnormal is above 10^-324, so a value 0.D × 10^p with D's first digit nonzero is at least 10^(p-1) and below 10^p.
 */
const EXPONENT_BOUND = 400;

/**
 * The significant digits that decide the rounding: the midpoint between two adjacent binary64 values has at most 767 significant
 * decimal digits, so a decimal cut to 800 digits and followed by a 1, when it was cut, lies on the same side of every midpoint as the
 * whole decimal does, and rounds the same.
 */
const KEPT_DIGITS = 800;

/**
 * Reads a long token as 0.D × 10^p: D is its digits with the leading and trailing zeros taken off, and p follows from where the
 * decimal point stood and the exponent, which is kept saturated (an exponent of a million digits is far beyond the bound). When p is
 * out of the bound the value is not finite or not nonzero; otherwise the token is rewritten as `0.D` `e` `p` with at most 801 digits,
 * which any reader of a decimal takes right: no digit stands before the point, so the digits it keeps are the leading ones and the
 * point is where it belongs.
 */
function parseLongDecimal(tok: string, neg: boolean): [number, NumberStatus] {
  let i = neg ? 1 : 0;
  const intStart = i;
  while (i < tok.length && isDigit(tok.charCodeAt(i))) i++;
  const intEnd = i;
  let fracStart = i;
  let fracEnd = i;
  if (i < tok.length && tok.charCodeAt(i) === 0x2e) {
    fracStart = i + 1;
    fracEnd = fracStart;
    while (fracEnd < tok.length && isDigit(tok.charCodeAt(fracEnd))) fracEnd++;
    i = fracEnd;
  }
  let exp = 0;
  if (i < tok.length && (tok.charCodeAt(i) === 0x65 || tok.charCodeAt(i) === 0x45)) {
    i++;
    let expNeg = false;
    if (i < tok.length && (tok.charCodeAt(i) === 0x2b || tok.charCodeAt(i) === 0x2d)) {
      expNeg = tok.charCodeAt(i) === 0x2d;
      i++;
    }
    const saturate = 2 ** 40;
    for (; i < tok.length && isDigit(tok.charCodeAt(i)); i++) {
      if (exp < saturate) exp = exp * 10 + (tok.charCodeAt(i) - 0x30);
    }
    if (expNeg) exp = -exp;
  }

  const intLen = intEnd - intStart;
  const n = intLen + (fracEnd - fracStart);
  // the k-th digit of the integer and fraction digits
  const at = (k: number): number => tok.charCodeAt(k < intLen ? intStart + k : fracStart + (k - intLen));
  let lead = 0;
  while (lead < n && at(lead) === 0x30) lead++;
  if (lead === n) return [neg ? -0 : 0, "ok"]; // every digit is 0
  let end = n;
  while (at(end - 1) === 0x30) end--;
  const p = intLen - lead + exp;
  if (p > EXPONENT_BOUND) return [neg ? -Infinity : Infinity, "infinite"];
  if (p < -EXPONENT_BOUND) return [neg ? -0 : 0, "underflow"];

  const parts: string[] = [neg ? "-0." : "0."];
  const cut = Math.min(end, lead + KEPT_DIGITS);
  for (let k = lead; k < cut; k += 4096) {
    let chunk = "";
    for (let j = k; j < Math.min(cut, k + 4096); j++) chunk += String.fromCharCode(at(j));
    parts.push(chunk);
  }
  if (cut < end) parts.push("1"); // digits were dropped, and the last of D is nonzero: the value is above every midpoint that the kept digits end on
  parts.push("e", String(p));
  return toBinary64(parts.join(""));
}

const isDigit = (c: number): boolean => c >= 0x30 && c <= 0x39;
