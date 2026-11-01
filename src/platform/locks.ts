// An exclusive lock by name inside one process (14 [CLI-61]): the lock of a Node host, where one process is one tab, and the lock of two engines
// that one process runs (two principals, two apps) so that each partition still has one flusher. It grants as Web Locks does: one holder at a time,
// the waiting ones in the order that they asked, and a waiter is called back later than the release, never inside it. A holder that goes away
// without releasing is a bug of the process itself: there is no other process to take the lock back from.

import type { CrossTabLocks, LockLease } from "../engine/tabs.js";

interface Waiter {
  grant: (l: LockLease) => void;
  reject: (e: unknown) => void;
  cleanup: () => void;
}

class Lease implements LockLease {
  held = true;
  constructor(
    private readonly owner: InProcessLocks,
    readonly name: string,
  ) {}
  release(): Promise<void> {
    if (this.held) {
      this.held = false;
      this.owner.free(this);
    }
    return Promise.resolve();
  }
}

const abortError = (): Error => Object.assign(new Error("the lock request was aborted"), { name: "AbortError" });

export class InProcessLocks implements CrossTabLocks {
  readonly reliable = true;
  private readonly holders = new Map<string, Lease>();
  private readonly queues = new Map<string, Waiter[]>();

  tryAcquire(name: string): Promise<LockLease | undefined> {
    if (this.holders.has(name)) return Promise.resolve(undefined);
    const l = new Lease(this, name);
    this.holders.set(name, l);
    return Promise.resolve(l);
  }

  acquire(name: string, signal?: AbortSignal): Promise<LockLease> {
    if (signal?.aborted === true) return Promise.reject(abortError());
    if (!this.holders.has(name)) {
      const l = new Lease(this, name);
      this.holders.set(name, l);
      return Promise.resolve(l);
    }
    return new Promise<LockLease>((resolve, reject) => {
      const q = this.queues.get(name) ?? [];
      this.queues.set(name, q);
      const onAbort = (): void => {
        const i = q.indexOf(w);
        if (i >= 0) q.splice(i, 1);
        reject(abortError());
      };
      const w: Waiter = { grant: resolve, reject, cleanup: () => signal?.removeEventListener("abort", onAbort) };
      signal?.addEventListener("abort", onAbort, { once: true });
      q.push(w);
    });
  }

  /** @internal */
  free(l: Lease): void {
    if (this.holders.get(l.name) !== l) return;
    this.holders.delete(l.name);
    queueMicrotask(() => this.next(l.name));
  }

  private next(name: string): void {
    if (this.holders.has(name)) return;
    const w = this.queues.get(name)?.shift();
    if (w === undefined) return;
    w.cleanup();
    const l = new Lease(this, name);
    this.holders.set(name, l);
    w.grant(l);
  }

  /** The number of requests that wait for the lock (a test reads it). */
  waiting(name: string): number {
    return this.queues.get(name)?.length ?? 0;
  }
}
