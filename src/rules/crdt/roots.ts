// The root conditions of 05 [STATE-71] (Roots), the last of the well-formedness conditions of a CRDT update.

import { fail } from "./errors.js";
import { ContentRef, ParentKind, type Update } from "./parse.js";

/** The declared type of a root of a CRDT template (04 §4.4.1, `crdt_roots`). */
export type RootType = "text" | "map" | "array" | "xml_fragment";

/** The template's `crdt_roots`: root name to declared type, under the manifest version in force for the record that carries the update. */
export type Roots = ReadonlyMap<string, RootType>;

/**
 * Checks the last condition of 05 [STATE-71], Roots. Each item that names a root as its parent, an item with neither origin nor
 * right origin whose encoded parent is a root name, meets these conditions: the root name is in `roots`; with a parentSub, the
 * root's type is map, and without one it is not; and a ContentString or ContentFormat item sits under a text root. An item
 * whose parent is another item, or that takes its parent from its origins, names no root, so the check does not reach it.
 * Throws a CrdtError.
 */
export function checkRoots(u: Update, roots: Roots): void {
  for (const s of u.all) {
    if (s.kind !== "item" || s.hasOrigin || s.hasRight || s.parentKind !== ParentKind.Root) continue;
    const typ = roots.get(s.root);
    if (typ === undefined) throw fail("root-name", `root "${s.root}" is not in crdt_roots`);
    if (s.hasSub && typ !== "map") throw fail("root-type", `item with a parentSub under the ${typ} root "${s.root}"`);
    if (!s.hasSub && typ === "map") throw fail("root-type", `item without a parentSub under the map root "${s.root}"`);
    if ((s.content === ContentRef.String || s.content === ContentRef.Format) && typ !== "text") {
      throw fail("root-type", `text content under the ${typ} root "${s.root}"`);
    }
  }
}
