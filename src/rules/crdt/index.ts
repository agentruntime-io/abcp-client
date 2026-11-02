// CRDT keys (05 §5.10): the strict reader of a Yjs update in format v1 (05 [STATE-71]), the fold of the integrated updates with
// its verdicts (05 [STATE-76]), the canonical form SV || DS of 05 [STATE-41], the validity of a base and the document length of a key
// (`Doc.documentLength`, 05 [STATE-74], round 17).
//
// Nothing here runs Yjs. A client that shows a CRDT key keeps a Yjs document of its own (14 [CLI-13]); this module is what
// the per-key state hash and the version-gated apply of the confirmed state read.

export { CrdtError, type NoopReason, type Reason, type RefusalReason } from "./errors.js";
export {
  type ClientClock,
  type ClientGroup,
  type ClientRanges,
  type DeleteGroup,
  type Id,
  type Range,
  type Struct,
  type StructKind,
  ContentRef,
  ParentKind,
  Update,
  normalizeRanges,
  parseUpdate,
} from "./parse.js";
export { type RootType, type Roots, checkRoots } from "./roots.js";
export { Doc, MAX_NESTING, canonicalForm, integrated, type Result, type Verdict } from "./doc.js";
export { type BaseError, type BaseFault, validateBase } from "./base.js";

import { type Update, parseUpdate } from "./parse.js";
import { checkRoots, type Roots } from "./roots.js";

/**
 * The conditions of 05 [STATE-71] that depend on the update and the template alone: `parseUpdate`, then `checkRoots`. Throws a CrdtError, to be
 * rejected with SCHEMA_INVALID. (`roots` may be left out where the root conditions were judged at commit.) The Nesting condition needs the key's
 * integrated updates: it is `Doc.checkNesting`, which `Doc.apply` makes.
 */
export function checkWellFormed(b: Uint8Array, roots?: Roots): Update {
  const u = parseUpdate(b);
  if (roots !== undefined) checkRoots(u, roots);
  return u;
}
