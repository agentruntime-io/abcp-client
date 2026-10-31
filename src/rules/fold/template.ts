// Key templates (03 §3.3): the shape of the keys of a template, `a/{x}/b`, with the segments of a key (03 [ID-1], [ID-2]).
// The fold needs them for one thing: a conversion keeps the changed_at and changed_by of the key it converted from, which is
// the source template's key bound by the same variable values as the target key (05 [STATE-67], 04 [MAN-66]). A manifest
// reader that builds `Rules` needs them to match a key to a template.

/** A key template: its segments, each a literal or a variable `{name}`. */
export interface KeyTemplate {
  readonly text: string;
  readonly segments: readonly ({ literal: string } | { variable: string })[];
}

const SEGMENT = /^[A-Za-z0-9._-]{1,64}$/;

/** 03 [ID-1], [ID-2]: ^(?!\.\.?$)(?!__proto__$)[A-Za-z0-9._-]{1,64}$. */
export function isSegment(s: string): boolean {
  return SEGMENT.test(s) && s !== "." && s !== ".." && s !== "__proto__";
}

const VAR_NAME = /^[a-z][a-z0-9_]{0,31}$/;

/**
 * Reads a key template (03 [ID-16]): 1 to 16 segments, each a literal segment or a variable `{name}` with a name of the form
 * [a-z][a-z0-9_]{0,31}, no variable twice. Returns undefined for text that is not one.
 */
export function parseKeyTemplate(text: string): KeyTemplate | undefined {
  if (text.length === 0 || text.length > 512) return undefined;
  const pieces = text.split("/");
  if (pieces.length > 16) return undefined;
  const segments: ({ literal: string } | { variable: string })[] = [];
  const names = new Set<string>();
  for (const piece of pieces) {
    if (piece.startsWith("{")) {
      if (!piece.endsWith("}")) return undefined;
      const name = piece.slice(1, -1);
      if (!VAR_NAME.test(name) || names.has(name)) return undefined;
      names.add(name);
      segments.push({ variable: name });
    } else if (piece.includes("{") || piece.includes("}") || !isSegment(piece)) {
      return undefined;
    } else {
      segments.push({ literal: piece });
    }
  }
  return { text, segments };
}

/**
 * Matches a key to the template (03 [ID-17]): the key has the template's number of segments, every literal segment equals the
 * segment of the key at the same position, and every variable faces a segment of the key, which it matches whole and is bound
 * to. Returns the bindings, or undefined when the key does not match.
 */
export function matchKeyTemplate(t: KeyTemplate, key: string): Map<string, string> | undefined {
  if (key.length === 0 || key.length > 512) return undefined;
  const pieces = key.split("/");
  if (pieces.length !== t.segments.length) return undefined;
  const bindings = new Map<string, string>();
  for (let i = 0; i < pieces.length; i++) {
    const seg = pieces[i] as string;
    if (!isSegment(seg)) return undefined;
    const ts = t.segments[i] as (typeof t.segments)[number];
    if ("variable" in ts) bindings.set(ts.variable, seg);
    else if (ts.literal !== seg) return undefined;
  }
  return bindings;
}

/** Writes the key that the template gives the bindings; undefined when a variable is not bound or a binding is no segment. */
export function expandKeyTemplate(t: KeyTemplate, bindings: ReadonlyMap<string, string>): string | undefined {
  const out: string[] = [];
  for (const s of t.segments) {
    if ("literal" in s) {
      out.push(s.literal);
      continue;
    }
    const v = bindings.get(s.variable);
    if (v === undefined || !isSegment(v)) return undefined;
    out.push(v);
  }
  const key = out.join("/");
  return key.length > 512 ? undefined : key;
}
