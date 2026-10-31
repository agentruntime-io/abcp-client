// The successor rules of a split or archived keyspace (14 [CLI-52], 12 [ADM-45]): which rule a key belongs to.

import type { SuccessorRule } from "../gen/abpc/v2/types_pb.js";
import { isReservedKey } from "../rules/fold/index.js";

/** The successor rule that a key belongs to (14 [CLI-52], 12 [ADM-45]); undefined for a key that matches none. */
export function matchSuccessor(rules: readonly SuccessorRule[], key: string): SuccessorRule | undefined {
  let k = key;
  if (isReservedKey(key)) {
    // a reserved key matches no rule, except an entity's moved key, which matches the rule of the entity's marker key (11 [COORD-73])
    const p = "abpc.saga.v1/moved/";
    if (!key.startsWith(p)) return undefined;
    k = key.slice(p.length);
  }
  let best: SuccessorRule | undefined;
  for (const r of rules) {
    if (r.prefix !== "" && k.startsWith(r.prefix) && (best === undefined || r.prefix.length > best.prefix.length)) best = r;
  }
  if (best !== undefined) return best;
  return rules.find((r) => r.prefix === "");
}
