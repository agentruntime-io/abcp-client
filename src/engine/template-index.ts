// An index of the key templates of a manifest by their segments (03 [ID-16] to [ID-18]), for the three things that a client asks of them: which template a key
// matches, which entity template is the parent of a template (04 [MAN-20]), and which templates a `writes` pattern selects (03 [ID-21]). A template has at most 16
// segments, each a literal or a variable, and the index is a trie over them, so that each question costs by the segments that it names and not by the number of
// templates: a scan of the templates for each key, and of the entity templates for each template, took seconds for a manifest of thousands (V19 N4).
//
// The edges of the trie are the literals and one edge for a variable, whatever the variable is called: a template's shape (03 [ID-17]: a literal equals the key's
// segment, a variable matches any one segment) is its path. Two templates of one shape are the overlap that a manifest does not have (03 [ID-18], 04 [MAN-17]
// row 6); where a manifest has them, the first one declared answers.

import { isSegment, matchKeyTemplate, type KeyTemplate } from "../rules/fold/index.js";

/** The most segments that a template has (03 [ID-16]); a key longer than this matches none, and no walk goes deeper. */
const MAX_SEGMENTS = 16;
const MAX_KEY_OCTETS = 512;

export interface IndexedTemplate<T> {
  readonly tpl: KeyTemplate;
  readonly value: T;
  /** The place of the template in the order in which they were added. */
  readonly order: number;
}

interface Node<T> {
  literal: Map<string, Node<T>> | undefined;
  variable: Node<T> | undefined;
  /** The templates whose segments end here, in the order added. */
  here: IndexedTemplate<T>[];
}

const empty = <T>(): Node<T> => ({ literal: undefined, variable: undefined, here: [] });

export type SelectForm = "plain" | "one" | "many";

export class TemplateIndex<T> {
  private readonly root: Node<T> = empty();
  private count = 0;
  /**
   * The nodes that the index has visited, in all: what a test reads to hold the cost of a lookup, a parent and a selection to the segments that they name, however
   * many templates there are.
   */
  steps = 0;

  get size(): number {
    return this.count;
  }

  add(tpl: KeyTemplate, value: T): void {
    let node = this.root;
    for (const seg of tpl.segments) {
      this.steps++;
      if ("literal" in seg) {
        node.literal ??= new Map();
        let next = node.literal.get(seg.literal);
        if (next === undefined) {
          next = empty();
          node.literal.set(seg.literal, next);
        }
        node = next;
      } else {
        node.variable ??= empty();
        node = node.variable;
      }
    }
    node.here.push({ tpl, value, order: this.count++ });
  }

  /**
   * The template that the key matches, with the bindings of its variables (03 [ID-17]); undefined when none does. A literal edge is tried before the variable
   * one, and the walk goes back to the variable edge when the literal one leads to no template (`a/b/c` and `a/{x}/d` do not overlap, and `a/b/d` is the second's).
   * No walk is deeper than a template, and none visits a node twice, so it costs at most the nodes of the index, and the segments of the key where the
   * templates do not overlap.
   */
  find(key: string): { tpl: KeyTemplate; value: T; bindings: Map<string, string> } | undefined {
    if (key.length === 0 || key.length > MAX_KEY_OCTETS) return undefined;
    const pieces = key.split("/");
    if (pieces.length > MAX_SEGMENTS) return undefined;
    for (const p of pieces) if (!isSegment(p)) return undefined;
    const hit = this.walk(this.root, pieces, 0);
    if (hit === undefined) return undefined;
    const bindings = matchKeyTemplate(hit.tpl, key);
    return bindings === undefined ? undefined : { tpl: hit.tpl, value: hit.value, bindings };
  }

  private walk(node: Node<T>, pieces: readonly string[], i: number): IndexedTemplate<T> | undefined {
    this.steps++;
    if (i === pieces.length) return node.here[0];
    const lit = node.literal?.get(pieces[i] as string);
    if (lit !== undefined) {
      const hit = this.walk(lit, pieces, i + 1);
      if (hit !== undefined) return hit;
    }
    return node.variable === undefined ? undefined : this.walk(node.variable, pieces, i + 1);
  }

  /** The node at the end of the shape of `tpl`'s first `n` segments, or undefined. */
  private shape(tpl: KeyTemplate, n: number): Node<T> | undefined {
    let node: Node<T> | undefined = this.root;
    for (let i = 0; i < n && node !== undefined; i++) {
      this.steps++;
      const seg = tpl.segments[i] as KeyTemplate["segments"][number];
      node = "literal" in seg ? node.literal?.get(seg.literal) : node.variable;
    }
    return node;
  }

  /**
   * The template that holds the longest proper prefix of `tpl`, by segments and by shape, among those that `accept` (04 [MAN-20]: the parent of a template is its
   * longest proper prefix that is an entity template); the first one added when several of that length do. Undefined when none does.
   */
  nearestAncestor(tpl: KeyTemplate, accept: (value: T) => boolean): IndexedTemplate<T> | undefined {
    let best: IndexedTemplate<T> | undefined;
    let node: Node<T> | undefined = this.root;
    for (let i = 0; i < tpl.segments.length - 1 && node !== undefined; i++) {
      this.steps++;
      const seg = tpl.segments[i] as KeyTemplate["segments"][number];
      node = "literal" in seg ? node.literal?.get(seg.literal) : node.variable;
      const hit = node?.here.find((e) => accept(e.value));
      if (hit !== undefined) best = hit;
    }
    return best;
  }

  /**
   * The templates that a `writes` pattern selects (03 [ID-21]), in the order added: with `base` the pattern's template, `plain` selects the templates of `base`'s
   * shape, `one` those of its shape and one segment more, `many` those below it (one or more segments more). A variable selects a variable, whatever its name.
   */
  select(base: KeyTemplate, form: SelectForm): IndexedTemplate<T>[] {
    const start = this.shape(base, base.segments.length);
    if (start === undefined) return [];
    const out: IndexedTemplate<T>[] = [];
    if (form === "plain") out.push(...start.here);
    else {
      // the nodes below `start`: the children for `one`, every descendant for `many`; the children are the first level of the walk
      const stack: { node: Node<T>; depth: number }[] = [{ node: start, depth: 0 }];
      while (stack.length > 0) {
        const { node, depth } = stack.pop() as { node: Node<T>; depth: number };
        this.steps++;
        if (depth > 0 && (form === "many" || depth === 1)) out.push(...node.here);
        if (form === "one" && depth >= 1) continue;
        for (const child of node.literal?.values() ?? []) stack.push({ node: child, depth: depth + 1 });
        if (node.variable !== undefined) stack.push({ node: node.variable, depth: depth + 1 });
      }
    }
    return out.sort((a, b) => a.order - b.order);
  }
}
