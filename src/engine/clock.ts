// Time and randomness, injected so that a test (and the fake server) controls both. Folding and prediction never read either
// (14 [CLI-9]); the engine reads them to author, to back off, to bound holds and to time out a session.

export interface Clock {
  /** The device's wall clock, in ms since the epoch. */
  now(): number;
  /** A monotonic clock, in ms; it restarts at 0 or anywhere with the process (14 [CLI-55]). */
  mono(): number;
  /** Runs fn once after ms; returns the function that cancels it. */
  setTimer(ms: number, fn: () => void): () => void;
}

export interface Random {
  /** A number in [0, 1). */
  float(): number;
  /** n random octets from a cryptographically secure source, for ids (03 [ID-35]) and the Yjs client id (14 [CLI-13]). */
  bytes(n: number): Uint8Array;
}

export const systemClock: Clock = {
  now: () => Date.now(),
  mono: () => (typeof performance !== "undefined" ? performance.now() : Date.now()),
  setTimer(ms, fn) {
    // `setTimeout` runs a delay above 2^31-1 ms (24.8 days) after 1 ms, so a long timer (the offline horizon of a year, a hold bound) is set in steps (C5)
    const MAX = 2 ** 31 - 1;
    let t: ReturnType<typeof setTimeout>;
    const arm = (left: number): void => {
      t = setTimeout(left > MAX ? () => arm(left - MAX) : fn, Math.min(left, MAX));
    };
    arm(ms);
    return () => clearTimeout(t);
  },
};

export const systemRandom: Random = {
  float(): number {
    const b = new Uint32Array(1);
    globalThis.crypto.getRandomValues(b);
    return (b[0] as number) / 4294967296;
  },
  bytes(n: number): Uint8Array {
    const out = new Uint8Array(n);
    // getRandomValues is bounded at 65,536 octets per call
    for (let i = 0; i < n; i += 65536) globalThis.crypto.getRandomValues(out.subarray(i, Math.min(n, i + 65536)));
    return out;
  },
};
