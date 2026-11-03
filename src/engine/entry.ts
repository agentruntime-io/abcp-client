// The content of an outbox entry (14 [CLI-22]) and the dependencies between entries (14 §14.4).
//
// The content is every `Mutation` field but the stamping fields, in the canonical encoding (07 [MUT-45]), written once at
// authoring. Prediction reads the decoded stored content; every send puts the stamping fields on a copy of it and sends the rest
// byte for byte (07 [MUT-106]).

import { create } from "@bufbuild/protobuf";
import { MutationSchema, type Mutation } from "../gen/abpc/v2/sync_pb.js";
import { ChangeKind } from "../gen/abpc/v2/types_pb.js";
import { CanonError, encode, unmarshal } from "../rules/canon/index.js";
import type { OutboxEntry } from "./storage.js";

/** The canonical bytes of the action's content: the Mutation with its stamping fields cleared (07 [MUT-45], 14 [CLI-22]). */
export function encodeContent(m: Mutation): Uint8Array {
  const c = create(MutationSchema, { ...m, clientId: "", clientSeq: 0n, afterClientSeq: 0n });
  return encode(MutationSchema, c);
}

/**
 * Decodes stored content strictly (`rules/canon`). A CanonError means that the entry cannot be read: it goes to the dead-letter
 * store and stops no other entry (14 [CLI-29]).
 */
export function decodeContent(bytes: Uint8Array): Mutation {
  return unmarshal(MutationSchema, bytes) as Mutation;
}

/** The Mutation to send: the content with the entry's stamping fields (14 [CLI-27]). The rest is the stored content, unchanged. */
export function stamped(content: Mutation, e: Pick<OutboxEntry, "clientId" | "clientSeq" | "afterClientSeq">): Mutation {
  return create(MutationSchema, { ...content, clientId: e.clientId, clientSeq: BigInt(e.clientSeq), afterClientSeq: BigInt(e.afterClientSeq) });
}

export { CanonError };

/** The keys that an action's changes name. */
export function keysWritten(m: Mutation): string[] {
  return m.changes.map((c) => c.key);
}

/** The pieces of an action that the dependency rules of 14 [CLI-30] read. */
export interface DepView {
  mutationId: string;
  opType: string;
  keys: readonly string[];
  mutation: Mutation;
  /** The authoring order of the action in its outbox (`OutboxEntry.ordinal`): what a redaction reset of a CRDT key is judged by. */
  ordinal?: number;
}

export interface DepContext {
  /** Whether the op type declares `barrier: true` (14 [CLI-30] item 5). */
  barrier(opType: string): boolean;
  /** The owning key of a fenced key (11 §11.2.4), or undefined. */
  fencedBy(key: string): string | undefined;
  /** The first ordinal authored after the redaction reset of a CRDT key (14 [CLI-13], [CLI-76]); 0 or absent for a key that no redaction reset. */
  resetOrdinal?(key: string): number;
}

/** Whether the action updates the CRDT key (`CRDT_UPDATE`, or an `OVERRIDE` that carries an update). */
function updatesCrdt(m: Mutation, key: string): boolean {
  return m.changes.some((c) => c.key === key && (c.kind === ChangeKind.CRDT_UPDATE || (c.kind === ChangeKind.OVERRIDE && c.update.length > 0)));
}

/**
 * The exception of item 1 for a CRDT key that a redaction reset (14 [CLI-13], [CLI-76]): "no later action depends on [the pending updates authored before the reset] through that key".
 * An update of the key that the later action B authored after the reset, and an update of it that the earlier action A authored before, are not dependent on each other through the key: B
 * typed on the reset document, and A, which the server ends with `STALE{DOC_RESET}` (05 [STATE-85]), must not fail B with it.
 */
function afterReset(key: string, a: DepView, b: DepView, ctx: DepContext): boolean {
  const r = ctx.resetOrdinal?.(key) ?? 0;
  if (r <= 0 || a.ordinal === undefined || b.ordinal === undefined) return false;
  return a.ordinal < r && b.ordinal >= r && updatesCrdt(a.mutation, key) && updatesCrdt(b.mutation, key);
}

function related(a: string, b: string): boolean {
  return a === b || a.startsWith(`${b}/`) || b.startsWith(`${a}/`);
}

/**
 * Whether the later action B depends on the earlier action A of the same keyspace and outbox (14 [CLI-30]). `explicit` is the set
 * of mutation ids that B was submitted with a link to (item 4, client-only).
 *
 * The exception of item 1 for a CRDT key whose redaction reset the client applied after it authored A (B's update of that key does not depend on A's update of it,
 * 14 [CLI-13], [CLI-76]) is `afterReset`.
 */
export function dependsOn(b: DepView, a: DepView, explicit: ReadonlySet<string>, ctx: DepContext): boolean {
  // 1. the same key, or a key below a key that the other writes (parents and entity markers)
  for (const kb of b.keys) {
    for (const ka of a.keys) {
      if (!related(kb, ka)) continue;
      if (kb === ka && afterReset(kb, a, b, ctx)) continue;
      return true;
    }
  }
  // 2. a precondition on a key A writes, on a prefix of a key A writes, or after A's mutation id; 3. on A's intent row
  const intentRow = `abpc.intent/${a.mutationId}`;
  for (const p of b.mutation.preconditions) {
    if (p.target.case === "key") {
      if (a.keys.includes(p.target.value) || p.target.value === intentRow) return true;
    } else if (p.target.case === "prefix") {
      const prefix = p.target.value;
      if (a.keys.some((k) => k.startsWith(prefix))) return true;
    }
    if (p.check.case === "afterMutation" && p.check.value === a.mutationId) return true;
  }
  // 4. an explicit link
  if (explicit.has(a.mutationId)) return true;
  // 5. barrier
  if (ctx.barrier(a.opType) || ctx.barrier(b.opType)) return true;
  // 6. B writes a fenced key whose owning key A writes
  for (const kb of b.keys) {
    const owner = ctx.fencedBy(kb);
    if (owner !== undefined && a.keys.includes(owner)) return true;
  }
  return false;
}
