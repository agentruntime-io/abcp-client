// Identifiers that a client mints: mutation ids (03 §3.5, 14 [CLI-23]), client ids (03 [ID-35], 14 [CLI-63]) and the Yjs
// client id of a view document (14 [CLI-13]).

import type { Random } from "./clock.js";

const HEX = "0123456789abcdef";

function hex(b: Uint8Array, from: number, to: number): string {
  let s = "";
  for (let i = from; i < to; i++) {
    const x = b[i] as number;
    s += (HEX[x >> 4] as string) + (HEX[x & 15] as string);
  }
  return s;
}

/** A version-4 UUID in the lowercase canonical form of 03 §3.5, whose free bits come from the random source (03 [ID-35]). */
export function newUuid(rnd: Random): string {
  const b = rnd.bytes(16);
  b[6] = ((b[6] as number) & 0x0f) | 0x40;
  b[8] = ((b[8] as number) & 0x3f) | 0x80;
  return `${hex(b, 0, 4)}-${hex(b, 4, 6)}-${hex(b, 6, 8)}-${hex(b, 8, 10)}-${hex(b, 10, 16)}`;
}

/** The syntax of a mutation id and a client id (03 §3.5): a UUID of version 4 or 8, variant 10, lowercase. */
export const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[48][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

export function isMutationId(s: string): boolean {
  return UUID_RE.test(s);
}

/** A Yjs client id of 53 random bits (14 [CLI-13]): an integer in [1, 2^53). */
export function newYjsClientId(rnd: Random): number {
  for (;;) {
    const b = rnd.bytes(7);
    // 53 bits: the low 5 bits of the first octet, then six whole octets
    let v = (b[0] as number) & 0x1f;
    for (let i = 1; i < 7; i++) v = v * 256 + (b[i] as number);
    if (v > 0) return v;
  }
}
