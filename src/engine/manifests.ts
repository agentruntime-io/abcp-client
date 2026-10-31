// What the engine reads of a manifest (04): the template of a key (the fold's `Rules`), and, for the client's own rules, the
// attributes of an op type (04 §4.5: `dispatch`, `optimistic`, `barrier`, `offline_mode`), the counter range, the roots of a CRDT
// template and the fence of a key (11 §11.2.4).
//
// TODO(C3/C5): an adapter that reads the manifest of `GetManifest` (04 §4.7, json/v1) into this interface. C1 has the
// canonical reader of the document (`rules/jsonv1`) and the key templates (`rules/fold/template`), but no manifest model.
// `StaticManifests` below builds the same interface from a plain description, for the fake server and the tests.

import type { Roots, RootType } from "../rules/crdt/index.js";
import type { Range } from "../rules/jsonv1/index.js";
import {
  expandKeyTemplate,
  matchKeyTemplate,
  parseKeyTemplate,
  type KeyTemplate,
  type Rule,
  type Rules,
  type Template,
} from "../rules/fold/index.js";

export type LaneName = "SEQUENCED" | "GATED";

/** The attributes of an op type that a client reads (04 §4.5). */
export interface OpTypeInfo {
  readonly name: string;
  /** `dispatch`: the lane of its actions. */
  readonly lane: LaneName;
  /** `optimistic` (default true): the client shows the change at once (14 [CLI-16]). */
  readonly optimistic: boolean;
  /** `barrier` (default false): strict client-side order (14 [CLI-30] item 5). */
  readonly barrier: boolean;
  /** `offline_mode` (default `queue_recheck`, 14 [CLI-82]). */
  readonly offlineMode: "queue_recheck" | "fail_fast";
  /** A spend op type of an escrow class (11 [COORD-84]): a lost spend action is resent once (14 [CLI-64]). */
  readonly spend: boolean;
  /** The templates that the op type may write (`writes`), for the local check of 14 [CLI-19]; absent: not checked. */
  readonly writes?: readonly string[];
  /** `log_intent` (default true, 04 §4.5): with false a GATED action has no INTENT record, and its results carry `intent_seq = 0` (07 [MUT-8]). It is the server's; the fake reads it. */
  readonly logIntent: boolean;
}

/** What the engine reads of one manifest version. */
export interface EngineManifest extends Rules {
  opType(name: string): OpTypeInfo | undefined;
  /** The pattern of the template that the key matches (`a/{x}/b`), for the `writes` check. */
  templatePattern(key: string): string | undefined;
  /** The `crdt_roots` of the key's template. */
  crdtRoots(key: string): Roots | undefined;
  /** The range of a counter's template. */
  counterRange(key: string): Range | undefined;
  /** The owning key of a fenced key (11 §11.2.4): the key that a write to this key is fenced by, or undefined. */
  fencedBy(key: string): string | undefined;
}

/** The manifests of an app, by version. */
export interface Manifests extends Rules {
  /** Makes the version available (`GetManifest`, 04 §4.7). Resolves false when the server holds no such version. */
  ensure(version: number): Promise<boolean>;
  /** Fetches the version again, for the hold of `UNKNOWN_OP_TYPE` (14 Table 14.1). */
  reload(version: number): Promise<boolean>;
  /** The loaded version, or undefined until `ensure` has loaded it. */
  get(version: number): EngineManifest | undefined;
}

/** A template of a `StaticManifests` version. */
export interface StaticTemplate {
  rule: Rule;
  entity?: boolean;
  /** The owning key's template, with the same variables (11 §11.2.4): a write to this template is fenced by it. */
  fencedBy?: string;
  roots?: Record<string, RootType>;
  range?: Range;
  /** The value encoding that a client can display; default `json/v1`. */
  encoding?: string;
}

export interface StaticOpType {
  lane?: LaneName;
  optimistic?: boolean;
  barrier?: boolean;
  offlineMode?: "queue_recheck" | "fail_fast";
  spend?: boolean;
  writes?: string[];
  /** `log_intent`, default true: with false the fake server logs no INTENT record for a GATED action of the type. */
  logIntent?: boolean;
}

export interface StaticManifestSpec {
  version: number;
  templates: Record<string, StaticTemplate>;
  opTypes?: Record<string, StaticOpType>;
}

interface Compiled {
  tpl: KeyTemplate;
  template: Template;
  spec: StaticTemplate;
}

class StaticVersion implements EngineManifest {
  private readonly list: Compiled[] = [];
  private readonly ops = new Map<string, OpTypeInfo>();

  constructor(spec: StaticManifestSpec) {
    const entity = new Set(Object.entries(spec.templates).filter(([, t]) => t.entity === true).map(([p]) => p));
    for (const [pattern, t] of Object.entries(spec.templates)) {
      const tpl = parseKeyTemplate(pattern);
      if (tpl === undefined) throw new Error(`template "${pattern}" is not a key template`);
      // 04 [MAN-20]: the parent is the longest proper prefix of the template, by segments, that is an entity template
      let parent = "";
      const segs = pattern.split("/");
      for (let n = segs.length - 1; n >= 1 && parent === ""; n--) {
        const cand = segs.slice(0, n).join("/");
        if (entity.has(cand)) parent = cand;
      }
      this.list.push({ tpl, template: { rule: t.rule, entity: t.entity === true, parent }, spec: t });
    }
    for (const [name, o] of Object.entries(spec.opTypes ?? {})) {
      const info: OpTypeInfo = {
        name,
        lane: o.lane ?? "SEQUENCED",
        optimistic: o.optimistic ?? true,
        barrier: o.barrier ?? false,
        offlineMode: o.offlineMode ?? "queue_recheck",
        spend: o.spend ?? false,
        logIntent: o.logIntent ?? true,
        ...(o.writes !== undefined ? { writes: o.writes } : {}),
      };
      this.ops.set(name, info);
    }
  }

  private find(key: string): { c: Compiled; bindings: Map<string, string> } | undefined {
    for (const c of this.list) {
      const bindings = matchKeyTemplate(c.tpl, key);
      if (bindings !== undefined) return { c, bindings };
    }
    return undefined;
  }

  template(_version: number, key: string): Template | undefined {
    return this.find(key)?.c.template;
  }

  opType(name: string): OpTypeInfo | undefined {
    return this.ops.get(name);
  }

  templatePattern(key: string): string | undefined {
    return this.find(key)?.c.tpl.text;
  }

  crdtRoots(key: string): Roots | undefined {
    const r = this.find(key)?.c.spec.roots;
    return r === undefined ? undefined : new Map(Object.entries(r));
  }

  counterRange(key: string): Range | undefined {
    return this.find(key)?.c.spec.range;
  }

  fencedBy(key: string): string | undefined {
    const f = this.find(key);
    if (f === undefined || f.c.spec.fencedBy === undefined) return undefined;
    const owner = parseKeyTemplate(f.c.spec.fencedBy);
    return owner === undefined ? undefined : expandKeyTemplate(owner, f.bindings);
  }
}

/**
 * Manifests built from plain descriptions, in memory: the fake server and the tests use them, with the same object on both
 * sides. `ensure` resolves for every version that was added.
 */
export class StaticManifests implements Manifests {
  private readonly versions = new Map<number, StaticVersion>();
  /** How often each version was reloaded (a test reads it). */
  readonly reloads: number[] = [];
  /** Runs when `reload` is called; a test adds a version there (the manifest that the server activated meanwhile). */
  onReload: ((version: number) => void) | undefined;

  constructor(...specs: StaticManifestSpec[]) {
    for (const s of specs) this.add(s);
  }

  add(spec: StaticManifestSpec): void {
    this.versions.set(spec.version, new StaticVersion(spec));
  }

  ensure(version: number): Promise<boolean> {
    return Promise.resolve(this.versions.has(version));
  }

  reload(version: number): Promise<boolean> {
    this.reloads.push(version);
    this.onReload?.(version);
    return Promise.resolve(this.versions.has(version));
  }

  get(version: number): EngineManifest | undefined {
    return this.versions.get(version);
  }

  template(version: number, key: string): Template | undefined {
    return this.versions.get(version)?.template(version, key);
  }
}
