// The waits of 04 Table 4.11 that a client makes, all of them full jitter (04 [MAN-91]): a delay drawn uniformly from
// [0, min(cap, base × 2^n)], where n counts the consecutive failed attempts, starting at 0.

import type { Random } from "./clock.js";

export interface Backoff {
  readonly baseMs: number;
  readonly capMs: number;
}

/** The client backoff of 14 [CLI-43]: a resend after `Retry`, after an unknown outcome, or of a call other than `Mutate`. Base 1 s, cap 5 min. */
export const CLIENT_BACKOFF: Backoff = { baseMs: 1_000, capMs: 300_000 };
/** The transport reconnect of 09 [READ-57]: base 1 s, cap 30 s, and never less than `Reconnect.retry_after_ms`. */
export const RECONNECT_BACKOFF: Backoff = { baseMs: 1_000, capMs: 30_000 };
/** The hold backoff of 14 [CLI-45]: a bound from 1 min, doubling, cap 1 h. */
export const HOLD_BACKOFF: Backoff = { baseMs: 60_000, capMs: 3_600_000 };

/** The largest `after_ms` and `retry_after_ms` that a peer may send (04 Table 4.11, [MAN-91]); a larger value counts as this. */
export const MAX_RETRY_AFTER_MS = 3_600_000;

/** The upper bound of the n-th delay: min(cap, base × 2^n). */
export function bound(n: number, b: Backoff): number {
  return Math.min(b.capMs, b.baseMs * 2 ** Math.min(Math.max(n, 0), 40));
}

/** A delay drawn uniformly from [0, bound(n)]. */
export function fullJitter(n: number, b: Backoff, rnd: Random): number {
  return Math.floor(rnd.float() * (bound(n, b) + 1));
}

/** A hint (`Retry.after_ms`, `Reconnect.retry_after_ms`) as the client uses it: at most the table's maximum (04 [MAN-91]). */
export function clampAfter(afterMs: number | undefined): number {
  if (afterMs === undefined || !Number.isFinite(afterMs) || afterMs < 0) return 0;
  return Math.min(afterMs, MAX_RETRY_AFTER_MS);
}

/** The wait before the n-th consecutive resend after `Retry`: the larger of `after_ms` and the client backoff (14 [CLI-43]). */
export function resendDelay(n: number, afterMs: number | undefined, rnd: Random): number {
  return Math.max(clampAfter(afterMs), fullJitter(n, CLIENT_BACKOFF, rnd));
}
