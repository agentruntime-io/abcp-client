// Memos that must not grow without bound (a long-lived engine sees any number of records and failures).

/** A set that keeps the `max` most recently added members: adding one over the bound forgets the oldest. */
export class BoundedSet<T> {
  private readonly m = new Set<T>();

  constructor(readonly max: number) {}

  has(x: T): boolean {
    return this.m.has(x);
  }

  add(x: T): void {
    this.m.delete(x);
    this.m.add(x);
    if (this.m.size > this.max) {
      const oldest = this.m.values().next();
      if (oldest.done !== true) this.m.delete(oldest.value);
    }
  }

  delete(x: T): void {
    this.m.delete(x);
  }

  get size(): number {
    return this.m.size;
  }
}

/** Appends to an array that keeps its last `max` members. */
export function pushBounded<T>(a: T[], x: T, max: number): void {
  a.push(x);
  if (a.length > max) a.splice(0, a.length - max);
}
