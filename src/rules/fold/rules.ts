// What the fold reads of a manifest: the template that a key matches under a manifest version (04 §4.4), and the topology of
// markers and parents that follows from the templates (05 §5.6).

import type { Schema } from "../jsonv1/index.js";

/** The merge rule of a key's template (05 §5.4). The state records it with each key, so that the per-key state hash needs no manifest. */
export type Rule = "lww" | "cas" | "counter" | "crdt";

/** What the fold reads of a key template of a manifest version (04 §4.4). */
export interface Template {
  /** The template's merge rule. A counter hashes as `json/v1` and a CRDT key by its canonical form (05 [STATE-41]). */
  rule: Rule;
  /** The template is a marker template, and its keys are marker keys (05 §5.6, 04 [MAN-20]). */
  entity: boolean;
  /**
   * The template's parent: the longest proper prefix of it, by segments, that is an entity template of the manifest, or "" when
   * there is none (04 [MAN-20]). The fold reads only its number of segments, since the ancestor marker of a key is the key's own
   * first segments (05 §5.6, "with the same id segments").
   */
  parent: string;
  /**
   * The template's encoding (05 [STATE-6]): `json/v1`, `bytes/v1` or `yjs/v1`. Absent where the reader of the manifest does not know it: a change's encoding is then not compared with it
   * (16 [SEC-3] judges a record against the manifest version that it carries, as far as the reader tells).
   */
  encoding?: string;
  /** The template's `value_schema` (04 §4.4.2), read for its use (04 [MAN-18]); absent when the template has none or the reader of the manifest does not read it. */
  schema?: Schema;
}

/**
 * What the fold needs from the manifests of a keyspace: the template that a key matches under a manifest version. A record is
 * applied under the version in `Record.manifest_version` (02 [ARC-26], 05 [STATE-39]). `template` answers undefined for a key
 * that matches no template of that version. It answers for every template that the version declares, deprecated and retired
 * ones included, which stay declared in every later version (04 [MAN-60]): the keys that they hold are still in the state.
 * The fold asks about business keys only: it answers for the reserved keys of 03 §3.8 itself (`RESERVED_TEMPLATE`).
 */
export interface Rules {
  template(version: number, key: string): Template | undefined;
}

/**
 * The template of every reserved key (03 §3.8): `lww`, `json/v1`, no marker, no parent. 03 [ID-19] gives the run key of a saga
 * that a `REDACTED` record sets this reading whatever the manifest in force, and 11 [COORD-106] gives every module key and
 * 06 §6.5.1 the intent row. A reserved key that no definition of 03 §3.8 names is read the same way for its state hash.
 */
export const RESERVED_TEMPLATE: Template = { rule: "lww", entity: false, parent: "" };

/** True when the key's first segment is `abpc` or starts with `abpc.` (03 §3.8). The key is not validated. */
export function isReservedKey(key: string): boolean {
  const slash = key.indexOf("/");
  const first = slash < 0 ? key : key.slice(0, slash);
  return first === "abpc" || first.startsWith("abpc.");
}

/** Tells the state how the keys of a keyspace relate (05 §5.6), which only the templates of a manifest version know. */
export interface Topology {
  /** Whether key is a marker key: it matches an entity template (05 [STATE-42]). */
  isMarker(key: string): boolean;
  /**
   * The ancestor marker keys of key, nearest first: the marker keys that its template's parent chain reaches, with the same id
   * segments (05 §5.6). A key with none, and a key that matches no template, gives [].
   */
  ancestors(key: string): string[];
}

/** Whether key is in the subtree of the marker key root (05 [STATE-42]): root itself, and every key that has root among its ancestor markers. */
export function inSubtree(top: Topology, root: string, key: string): boolean {
  if (key === root) return true;
  if (key.length <= root.length + 1 || !key.startsWith(root) || key[root.length] !== "/") return false; // ancestor markers are path prefixes of the key
  return top.ancestors(key).includes(root);
}

/** The Topology of one manifest version, with the templates it has looked up so far. */
export class VersionTopology implements Topology {
  private readonly memo = new Map<string, Template | undefined>();

  constructor(
    private readonly rules: Rules,
    private readonly version: number,
  ) {}

  /** The template of key under the version: the reserved template for a reserved key (03 [ID-19]), else the manifest's. */
  template(key: string): Template | undefined {
    if (this.memo.has(key)) return this.memo.get(key);
    const t = isReservedKey(key) ? RESERVED_TEMPLATE : this.rules.template(this.version, key);
    if (this.memo.size < 1 << 16) this.memo.set(key, t); // a bound: a record holds at most a few thousand changes
    return t;
  }

  isMarker(key: string): boolean {
    return this.template(key)?.entity === true;
  }

  /**
   * The markers that the parent chain of the key's template reaches, nearest first. The parent chain is at most as long as the
   * key has segments, which 03 [ID-13] bounds at 16.
   */
  ancestors(key: string): string[] {
    const out: string[] = [];
    let tpl = this.template(key);
    for (let depth = 0; tpl !== undefined && tpl.parent !== "" && depth < 16; depth++) {
      const n = tpl.parent.split("/").length;
      const anc = firstSegments(key, n);
      if (anc === undefined) break;
      const next = this.template(anc);
      if (next === undefined) break;
      if (next.entity) out.push(anc);
      tpl = next;
    }
    return out;
  }
}

/** The first n segments of key, joined, when the key has more than n. */
function firstSegments(key: string, n: number): string | undefined {
  for (let i = 0; i < key.length; i++) {
    if (key[i] === "/") {
      n--;
      if (n === 0) return key.slice(0, i);
    }
  }
  return undefined;
}
