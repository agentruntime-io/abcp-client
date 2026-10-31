// A clock that only moves when a test says so, and a random source that a test controls. The engine and the fake server both
// take them, so that backoffs, holds, heartbeats and delays are exact.

import type { Clock, Random } from "../engine/clock.js";

interface Timer {
  id: number;
  at: number;
  fn: () => void;
}

/** A manual clock: `now` and `mono` advance together, by `advance`, which fires the timers that fall due in their order. */
export class ManualClock implements Clock {
  private t: number;
  private nextId = 1;
  private timers: Timer[] = [];

  constructor(start = Date.UTC(2026, 0, 1, 12, 0, 0)) {
    this.t = start;
  }

  now(): number {
    return this.t;
  }

  mono(): number {
    return this.t;
  }

  setTimer(ms: number, fn: () => void): () => void {
    const timer: Timer = { id: this.nextId++, at: this.t + Math.max(0, ms), fn };
    this.timers.push(timer);
    return () => {
      this.timers = this.timers.filter((x) => x.id !== timer.id);
    };
  }

  /** The number of timers that have not fired. */
  get pending(): number {
    return this.timers.length;
  }

  /** The time at which the next timer is due, or undefined. */
  nextAt(): number | undefined {
    let min: number | undefined;
    for (const x of this.timers) if (min === undefined || x.at < min) min = x.at;
    return min;
  }

  /** Lets every promise that can run, run (a macrotask turn). */
  async yield(): Promise<void> {
    await new Promise<void>((r) => setImmediate(r));
  }

  /** Moves the clock forward by ms, firing the timers that fall due in order and letting the engine run after each. */
  async advance(ms: number): Promise<void> {
    const target = this.t + ms;
    for (;;) {
      let next: Timer | undefined;
      for (const x of this.timers) if (x.at <= target && (next === undefined || x.at < next.at || (x.at === next.at && x.id < next.id))) next = x;
      if (next === undefined) break;
      this.timers = this.timers.filter((x) => x.id !== (next as Timer).id);
      this.t = Math.max(this.t, next.at);
      next.fn();
      await this.yield();
    }
    this.t = target;
    await this.yield();
  }
}

/** A seeded random source (mulberry32). `fixed` makes `float` return one value, for the bounds of a backoff. */
export class SeededRandom implements Random {
  private s: number;
  /** When set, `float()` returns it. */
  fixed: number | undefined;

  constructor(seed = 1) {
    this.s = seed >>> 0;
  }

  float(): number {
    if (this.fixed !== undefined) return this.fixed;
    this.s = (this.s + 0x6d2b79f5) >>> 0;
    let t = this.s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  bytes(n: number): Uint8Array {
    const out = new Uint8Array(n);
    for (let i = 0; i < n; i++) {
      this.s = (this.s + 0x6d2b79f5) >>> 0;
      let t = this.s;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      out[i] = (t ^ (t >>> 14)) & 0xff;
    }
    return out;
  }
}
