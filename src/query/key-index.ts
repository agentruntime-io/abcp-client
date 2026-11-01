// The two indexes of the query layer.
//
//  - `KeyIndex`: the keys of one keyspace in byte order of their UTF-8 (02 [ARC-62]), so that the keys below a prefix are a range found by a search, and a
//    query costs its range and not the keyspace. It holds every key that is present in the confirmed state or in the predicted view, which is a superset of
//    what either query mode shows; each query reads the key through the view of its mode, so a key that the index holds too many is harmless and one that
//    it lacks is a bug. It is kept in step with the engine's change feed key by key, and built again after a change that says anything may have changed;
//  - `ScopeIndex`: the live queries by the scope they read (a key, or a key prefix that ends with `/`, `""` being the whole keyspace), so that a change to
//    a key finds the queries it touches by looking up the key's own prefixes, at most sixteen, and not by asking every query.

import { compareUtf8 } from "../rules/bytes.js";

export class KeyIndex {
  private keys: string[];

  /** `sorted` is in byte order of UTF-8 and holds no key twice. */
  constructor(sorted: string[]) {
    this.keys = sorted;
  }

  get size(): number {
    return this.keys.length;
  }

  at(i: number): string {
    return this.keys[i] as string;
  }

  /** The index of the first key that is not below `s` in byte order. */
  lowerBound(s: string): number {
    let lo = 0;
    let hi = this.keys.length;
    while (lo < hi) {
      const m = (lo + hi) >>> 1;
      if (compareUtf8(this.keys[m] as string, s) < 0) lo = m + 1;
      else hi = m;
    }
    return lo;
  }

  /** The index of the first key above `s`. */
  upperBound(s: string): number {
    const i = this.lowerBound(s);
    return i < this.keys.length && this.keys[i] === s ? i + 1 : i;
  }

  has(key: string): boolean {
    const i = this.lowerBound(key);
    return i < this.keys.length && this.keys[i] === key;
  }

  add(key: string): void {
    const i = this.lowerBound(key);
    if (i < this.keys.length && this.keys[i] === key) return;
    this.keys.splice(i, 0, key);
  }

  remove(key: string): void {
    const i = this.lowerBound(key);
    if (i < this.keys.length && this.keys[i] === key) this.keys.splice(i, 1);
  }

  /** The range `[lo, hi)` of the keys that start with `prefix` (every key, for `""`). Keys with a common prefix are contiguous in any lexicographic order. */
  range(prefix: string): [number, number] {
    const lo = this.lowerBound(prefix);
    let a = lo;
    let b = this.keys.length;
    // the keys from lo on that start with the prefix come first: the last of them is found by a search on that property
    while (a < b) {
      const m = (a + b) >>> 1;
      if ((this.keys[m] as string).startsWith(prefix)) a = m + 1;
      else b = m;
    }
    return [lo, a];
  }

  /** Whether some key starts with `prefix`. */
  hasPrefix(prefix: string): boolean {
    const i = this.lowerBound(prefix);
    return i < this.keys.length && (this.keys[i] as string).startsWith(prefix);
  }

  /** The keys of a range, a copy. */
  slice(lo: number, hi: number): string[] {
    return this.keys.slice(lo, hi);
  }
}

/** Merges two lists in byte order of UTF-8, each without a repeat, into one without. */
export function mergeSorted(a: readonly string[], b: readonly string[]): string[] {
  const out: string[] = [];
  let i = 0;
  let j = 0;
  while (i < a.length || j < b.length) {
    const x = a[i];
    const y = b[j];
    if (y === undefined) {
      out.push(x as string);
      i++;
    } else if (x === undefined) {
      out.push(y);
      j++;
    } else {
      const c = compareUtf8(x, y);
      if (c < 0) {
        out.push(x);
        i++;
      } else if (c > 0) {
        out.push(y);
        j++;
      } else {
        out.push(x);
        i++;
        j++;
      }
    }
  }
  return out;
}

/** The prefixes of a key at its segment boundaries, `a/`, `a/b/`, … (every proper one), and `""`. */
function* keyPrefixes(key: string): Generator<string> {
  yield "";
  for (let i = 0; i < key.length; i++) if (key.charCodeAt(i) === 0x2f) yield key.slice(0, i + 1);
}

export class ScopeIndex<T> {
  private readonly byScope = new Map<string, Set<T>>();
  private sorted: string[] | undefined;

  add(scope: string, item: T): void {
    let s = this.byScope.get(scope);
    if (s === undefined) {
      s = new Set();
      this.byScope.set(scope, s);
      this.sorted = undefined;
    }
    s.add(item);
  }

  remove(scope: string, item: T): void {
    const s = this.byScope.get(scope);
    if (s === undefined) return;
    s.delete(item);
    if (s.size === 0) {
      this.byScope.delete(scope);
      this.sorted = undefined;
    }
  }

  get size(): number {
    return this.byScope.size;
  }

  /** The items whose scope contains the key: the scope is the key itself, or a prefix of it that ends with `/` (or `""`). */
  forKey(key: string): Set<T> {
    const out = new Set<T>();
    for (const p of keyPrefixes(key)) for (const t of this.byScope.get(p) ?? []) out.add(t);
    for (const t of this.byScope.get(key) ?? []) out.add(t);
    return out;
  }

  /**
   * The items whose scope meets the subtree of `m` (the key `m` and every key below `m/`): a scope that contains `m`, or one inside the subtree (the scope
   * `m` itself, or one that starts with `m/`).
   */
  forSubtree(m: string): Set<T> {
    const out = this.forKey(m);
    if (this.sorted === undefined) this.sorted = [...this.byScope.keys()].sort(compareUtf8);
    const sorted = this.sorted;
    let lo = 0;
    let hi = sorted.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (compareUtf8(sorted[mid] as string, m) < 0) lo = mid + 1;
      else hi = mid;
    }
    for (let i = lo; i < sorted.length; i++) {
      const s = sorted[i] as string;
      if (!s.startsWith(m)) break;
      if (s === m || s.startsWith(`${m}/`)) for (const t of this.byScope.get(s) ?? []) out.add(t);
    }
    return out;
  }
}
