// `Rules` over manifests in the vectors' shorthand (conformance/vectors/README.md, "fold": `manifest.templates` with `rule`,
// `entity`, `parent`), by version. The real manifest reader gets its own adapter; this one reads the vectors' shorthand and
// nothing else. It is a port of server/internal/fold/foldtest.

import {
  matchKeyTemplate,
  parseKeyTemplate,
  type KeyTemplate,
  type Rule,
  type Rules,
  type Template,
} from "../../src/rules/fold/index.js";
import type { Vector } from "./vectors.js";

interface Shorthand {
  version: number;
  templates: Record<string, { rule: string; entity?: boolean; parent?: string }>;
}

const RULES: Record<string, Rule> = { lww: "lww", cas: "cas", counter: "counter", "crdt:yjs/v1": "crdt" };

export class VectorRules implements Rules {
  private readonly byVersion = new Map<number, { tpl: KeyTemplate; template: Template }[]>();

  /**
   * Adds a manifest of the vector shorthand under its `version`. The parent of each template is the one that 04 [MAN-20]
   * defines, the longest proper prefix of it, by segments, that is an entity template; a `parent` that the manifest declares
   * must name it.
   */
  addManifest(m: Shorthand): void {
    const entity = new Set(Object.entries(m.templates).filter(([, t]) => t.entity === true).map(([p]) => p));
    const list: { tpl: KeyTemplate; template: Template }[] = [];
    for (const [pattern, t] of Object.entries(m.templates)) {
      const tpl = parseKeyTemplate(pattern);
      if (tpl === undefined) throw new Error(`template "${pattern}" is not a key template`);
      const rule = RULES[t.rule];
      if (rule === undefined) throw new Error(`template "${pattern}": rule "${t.rule}"`);
      let parent = "";
      const segs = pattern.split("/");
      for (let n = segs.length - 1; n >= 1 && parent === ""; n--) {
        const cand = segs.slice(0, n).join("/");
        if (entity.has(cand)) parent = cand;
      }
      if (t.parent !== undefined && t.parent !== parent) throw new Error(`template "${pattern}": declared parent "${t.parent}", 04 [MAN-20] gives "${parent}"`);
      list.push({ tpl, template: { rule, entity: t.entity === true, parent } });
    }
    this.byVersion.set(m.version, list);
  }

  template(version: number, key: string): Template | undefined {
    for (const e of this.byVersion.get(version) ?? []) {
      if (matchKeyTemplate(e.tpl, key) !== undefined) return e.template;
    }
    return undefined;
  }
}

/** The Rules of a fold vector: its `manifest`, and the `manifest` of every step that gives one, an `activate` step's and a `redact` step's `activate`. */
export function vectorRules(v: Vector): VectorRules {
  const r = new VectorRules();
  r.addManifest(v.manifest as Shorthand);
  for (const s of v.steps as { op: string; manifest?: Shorthand; activate?: { manifest?: Shorthand } }[]) {
    if (s.manifest !== undefined) r.addManifest(s.manifest);
    if (s.activate?.manifest !== undefined) r.addManifest(s.activate.manifest);
  }
  return r;
}
