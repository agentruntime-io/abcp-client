// The plan of a query: what its selection reads (a range of the key index, with the structural test of the keys in it), the scopes that a change must touch to
// concern it, which row a touched key belongs to, and how much of it lies in the interest of the keyspace (14 [CLI-71]).

import { compareUtf8 } from "../rules/bytes.js";
import { isReservedKey, isSegment, matchKeyTemplate, parseKeyTemplate, type KeyTemplate } from "../rules/fold/index.js";
import { QueryError } from "./errors.js";
import { compileFilter, type Compiled } from "./filter.js";
import type { Coverage, Query } from "./types.js";

export const MOVED_PREFIX = "abcp.saga.v1/moved/";
export const RUN_PREFIX = "abcp.saga.v1/run/";

/** Whether the key is under one of the interest prefixes (09 [READ-22]): equal to it or below it; an empty interest is the whole keyspace. */
export function inInterest(interest: readonly string[], key: string): boolean {
  if (interest.length === 0) return true;
  for (const p of interest) if (key === p || key.startsWith(`${p}/`)) return true;
  return false;
}

/** A key path of 03 [ID-13]: one to sixteen segments, at most 512 octets. */
function isKeyPath(k: string): boolean {
  if (k.length === 0 || k.length > 512) return false;
  const parts = k.split("/");
  return parts.length <= 16 && parts.every(isSegment);
}

/** How much of the keys below the prefix `p` (a key path and `/`, or `""`) the interest covers. */
export function coverageOfPrefix(interest: readonly string[], p: string): Coverage {
  if (interest.length === 0) return "full";
  const base = p.endsWith("/") ? p.slice(0, -1) : p;
  let some = false;
  for (const i of interest) {
    if (base !== "" && (base === i || base.startsWith(`${i}/`))) return "full";
    if (i.startsWith(p)) some = true; // an interest prefix inside the range
  }
  return some ? "partial" : "none";
}

type Seg = { literal: string } | { variable: string };

export interface Match {
  bindings: Readonly<Record<string, string>> | undefined;
}

const NO_BINDINGS: Match = { bindings: undefined };

export type Order = "asc" | "desc";

export class Plan {
  readonly kind: "keys" | "prefix" | "template" | "entities";
  readonly keyspaceId: string;
  readonly mode: "predicted" | "confirmed";
  readonly order: Order;
  readonly limit: number | undefined;
  readonly after: string | undefined;
  readonly filter: Compiled | undefined;
  /** For `keys`, and for a template whose variables are all bound: the keys, in the query's order, without repeats. */
  readonly keys: readonly string[];
  /** The index range that holds every candidate of the selection: the keys that start with it (`""`: every key). Not used by `keys`. */
  readonly rangePrefix: string;
  /** `entities` with `subtree`, or a filter that reads below the entity: a key below a marker concerns the marker's row. */
  readonly deep: boolean;
  readonly subtree: boolean;
  /** The segments of the template that the selection matches, with the bound variables as literals. */
  private readonly segs: readonly Seg[];
  private readonly bind: Readonly<Record<string, string>> | undefined;
  private readonly tpl: KeyTemplate | undefined;
  private readonly prefix: string;
  private readonly keySet: ReadonlySet<string>;
  /** The scopes that a change must touch to concern the query (`ScopeIndex`). */
  readonly scopes: readonly string[];
  /** The template of an entity query, as written, for the check against the manifest. */
  readonly template: string | undefined;

  private constructor(q: Query) {
    this.keyspaceId = q.keyspaceId;
    this.mode = q.mode ?? "predicted";
    if (this.mode !== "predicted" && this.mode !== "confirmed") throw new QueryError("INVALID_QUERY", `the mode "${String(q.mode)}" does not exist`);
    this.order = q.order ?? "asc";
    if (this.order !== "asc" && this.order !== "desc") throw new QueryError("INVALID_QUERY", `the order "${String(q.order)}" does not exist`);
    if (q.limit !== undefined && (!Number.isInteger(q.limit) || q.limit < 1 || q.limit > 1_000_000)) throw new QueryError("INVALID_LIMIT", `a limit is an integer from 1 to 1,000,000, not ${String(q.limit)}`);
    this.limit = q.limit;
    this.after = q.after;
    this.filter = q.filter === undefined ? undefined : compileFilter(q.filter);

    const sel = q.select;
    let kind: Plan["kind"];
    let tplText: string | undefined;
    let bind: Readonly<Record<string, string>> | undefined;
    let subtree = false;
    if ("keys" in sel) kind = "keys";
    else if ("prefix" in sel) kind = "prefix";
    else if ("template" in sel) {
      kind = "template";
      tplText = sel.template;
      bind = sel.bind;
    } else if ("entities" in sel) {
      kind = "entities";
      tplText = sel.entities;
      bind = sel.bind;
      subtree = sel.subtree === true;
    } else throw new QueryError("INVALID_QUERY", "a selection is `keys`, `prefix`, `template` or `entities`");
    this.subtree = subtree;
    this.template = kind === "entities" ? tplText : undefined;

    let keys: string[] = [];
    let rangePrefix = "";
    let prefix = "";
    let segs: Seg[] = [];
    let tpl: KeyTemplate | undefined;
    const scopes: string[] = [];
    if (kind === "keys") {
      const sel2 = sel as { keys: readonly string[] };
      for (const k of sel2.keys) if (typeof k !== "string" || !isKeyPath(k)) throw new QueryError("INVALID_KEY", `"${String(k)}" is not a key (03 [ID-13])`);
      keys = [...new Set(sel2.keys)].sort(compareUtf8);
      if (this.order === "desc") keys.reverse();
      for (const k of keys) scopes.push(k);
    } else if (kind === "prefix") {
      prefix = (sel as { prefix: string }).prefix;
      if (typeof prefix !== "string" || !(prefix === "" || (prefix.endsWith("/") && isKeyPath(prefix.slice(0, -1))))) {
        throw new QueryError("INVALID_PREFIX", `"${String(prefix)}" is not a key prefix: a key path followed by "/", or "" (03 [ID-14])`);
      }
      rangePrefix = prefix;
      scopes.push(prefix);
    } else {
      tpl = parseKeyTemplate(tplText as string);
      if (tpl === undefined) throw new QueryError("INVALID_TEMPLATE", `"${String(tplText)}" is not a key template (03 [ID-16])`);
      const vars = new Set(tpl.segments.flatMap((s) => ("variable" in s ? [s.variable] : [])));
      for (const [name, value] of Object.entries(bind ?? {})) {
        if (!vars.has(name)) throw new QueryError("INVALID_TEMPLATE", `"${name}" is not a variable of "${tplText as string}"`);
        if (!isSegment(value)) throw new QueryError("INVALID_TEMPLATE", `"${value}" is not a key segment (03 [ID-1])`);
      }
      segs = tpl.segments.map((s) => ("variable" in s && bind !== undefined && Object.hasOwn(bind, s.variable) ? { literal: bind[s.variable] as string } : s));
      const firstVar = segs.findIndex((s) => "variable" in s);
      const head = (firstVar < 0 ? segs : segs.slice(0, firstVar)).map((s) => (s as { literal: string }).literal);
      if (firstVar < 0) {
        // every variable is bound: the template names one key
        const only = head.join("/");
        keys = [only];
        rangePrefix = only;
        scopes.push(only);
        if (kind === "entities" && (subtree || this.filter?.usesAt === true)) scopes.push(`${only}/`); // what lies below the entity
      } else {
        rangePrefix = head.length === 0 ? "" : `${head.join("/")}/`;
        scopes.push(rangePrefix);
      }
    }
    this.kind = kind;
    this.keys = keys;
    this.keySet = new Set(keys);
    this.rangePrefix = rangePrefix;
    this.prefix = prefix;
    this.segs = segs;
    this.bind = bind;
    this.tpl = tpl;
    this.deep = kind === "entities" && (subtree || this.filter?.usesAt === true);
    if (this.filter?.usesAt === true && kind !== "entities") throw new QueryError("INVALID_FILTER", "`at` reads a key below an entity: only an entity query has one");
    // a row of any query can be a moved entity's pointer, which says whether its move failed by the run key (14 [CLI-14]); an entity query also lists the
    // markers that are absent and whose moved key stands in their place
    scopes.push(RUN_PREFIX);
    if (kind === "entities" && !(rangePrefix !== "" && isReservedKey(rangePrefix))) scopes.push(`${MOVED_PREFIX}${rangePrefix}`);
    this.scopes = scopes;
  }

  static build(q: Query): Plan {
    return new Plan(q);
  }

  /** The bindings of the key when it is one that the selection reads; undefined when it is not. `keys` and `prefix` bind nothing. */
  match(key: string): Match | undefined {
    switch (this.kind) {
      case "keys":
        return this.keySet.has(key) ? NO_BINDINGS : undefined;
      case "prefix":
        return key.length > this.prefix.length && key.startsWith(this.prefix) ? NO_BINDINGS : undefined;
      default: {
        if (!key.startsWith(this.rangePrefix)) return undefined;
        const m = matchKeyTemplate(this.tpl as KeyTemplate, key);
        if (m === undefined) return undefined;
        if (this.bind !== undefined) for (const [name, value] of Object.entries(this.bind)) if (m.get(name) !== value) return undefined;
        const bindings: Record<string, string> = {};
        for (const [k, v] of m) bindings[k] = v;
        return { bindings };
      }
    }
  }

  /**
   * The row that a touched key concerns, or undefined when it concerns none: the key itself for a key, prefix or template query; for an entity query
   * the marker of the entity that the key is, or lies below when the query shows what lies below (`deep`), and the entity of a moved key.
   */
  rowKeyOf(key: string): string | undefined {
    if (this.kind !== "entities") return this.match(key) === undefined ? undefined : key;
    if (key.startsWith(MOVED_PREFIX)) return this.rowKeyOf(key.slice(MOVED_PREFIX.length));
    if (this.match(key) !== undefined) return key;
    if (!this.deep) return undefined;
    const marker = firstSegments(key, this.segs.length);
    return marker !== undefined && this.match(marker) !== undefined ? marker : undefined;
  }

  /** The key's position against the query's cursor and order: false for a key that the page cannot hold (before `after`). */
  isAfterCursor(rowKey: string): boolean {
    if (this.after === undefined) return true;
    const c = compareUtf8(rowKey, this.after);
    return this.order === "asc" ? c > 0 : c < 0;
  }

  /** The order of two row keys in the query's order. */
  compare(a: string, b: string): number {
    const c = compareUtf8(a, b);
    return this.order === "asc" ? c : -c;
  }

  /** How much of what the selection reads lies in the interest. */
  coverage(interest: readonly string[]): Coverage {
    if (this.kind === "keys") {
      let some = 0;
      for (const k of this.keys) if (inInterest(interest, k)) some++;
      return this.keys.length === 0 || some === this.keys.length ? "full" : some === 0 ? "none" : "partial";
    }
    return this.kind === "prefix" ? coverageOfPrefix(interest, this.prefix) : this.templateCoverage(interest);
  }

  /**
   * How much of a template's keys the interest covers: an interest prefix covers all of them when it names the first segments of the template, all
   * literal (or bound); some of them when it names segments that the template can match.
   */
  private templateCoverage(interest: readonly string[]): Coverage {
    if (interest.length === 0) return "full";
    if (this.keys.length === 1 && this.kind === "template") return inInterest(interest, this.keys[0] as string) ? "full" : "none";
    let some = false;
    for (const i of interest) {
      const parts = i.split("/");
      if (parts.length > this.segs.length) continue;
      let literalAll = true;
      let compatible = true;
      for (let n = 0; n < parts.length; n++) {
        const s = this.segs[n] as Seg;
        if ("variable" in s) literalAll = false;
        else if (s.literal !== parts[n]) {
          compatible = false;
          break;
        }
      }
      if (!compatible) continue;
      if (literalAll) return "full";
      some = true;
    }
    return some ? "partial" : "none";
  }

  /** The keys whose moved keys (`abcp.saga.v1/moved/…`) are read for this query are within the interest, so that moved and staged entities are recognised (14 [CLI-14]). */
  movesTracked(interest: readonly string[]): boolean {
    if (interest.length === 0) return true;
    if (this.kind === "keys") return this.keys.every((k) => isReservedKey(k) || inInterest(interest, `${MOVED_PREFIX}${k}`));
    if (this.rangePrefix !== "" && isReservedKey(this.rangePrefix)) return true;
    return coverageOfPrefix(interest, `${MOVED_PREFIX}${this.rangePrefix}`) === "full";
  }
}

/** The first n segments of a key, joined, when the key has more than n. */
export function firstSegments(key: string, n: number): string | undefined {
  for (let i = 0; i < key.length; i++) {
    if (key.charCodeAt(i) === 0x2f && --n === 0) return key.slice(0, i);
  }
  return undefined;
}
