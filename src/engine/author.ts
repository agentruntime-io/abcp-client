// Authoring: turns what the app submits into the `Mutation` whose content goes to the outbox (14 [CLI-17], [CLI-18], [CLI-19]).

import { create } from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import { CasExpectSchema, ChangeKind, KeyChangeSchema, PreconditionSchema, type KeyChange, type Precondition } from "../gen/abpc/v2/types_pb.js";
import { Lane } from "../gen/abpc/v2/types_pb.js";
import { MutationSchema, type Mutation } from "../gen/abpc/v2/sync_pb.js";
import { isReservedKey, type Rule } from "../rules/fold/index.js";
import { LocalRefusal } from "./errors.js";
import type { EngineManifest, OpTypeInfo } from "./manifests.js";
import type { ActionInput, ChangeInput, PreconditionInput } from "./types.js";

/** What authoring reads of the keyspace's confirmed state (14 [CLI-17], [CLI-18]). */
export interface ConfirmedFacts {
  /** The engine holds confirmed state of the keyspace (a load installed it): its watermark and epoch are real, even for an empty keyspace. */
  loaded: boolean;
  /** The confirmed watermark: the cursor's seq. */
  seq: bigint;
  logEpoch: number;
  /** The key's version in the confirmed state, tombstones included, or 0 when it holds neither the key nor a tombstone (14 [CLI-17]). */
  versionOf(key: string): bigint;
  /** The manifest version in force for the keyspace. */
  manifestVersion: number;
}

export interface AuthorLimits {
  maxChangesPerAction: number;
  maxActionBytes: number;
  maxValueBytes: number;
}

/** The change kinds that a rule accepts (05 §5.4, 04 §4.5). */
function kindFits(rule: Rule | undefined, kind: ChangeKind): boolean {
  switch (kind) {
    case ChangeKind.DELETE:
      return true;
    case ChangeKind.SET:
      return rule === "lww" || rule === undefined;
    case ChangeKind.INCR:
      return rule === "counter";
    case ChangeKind.CAS:
      return rule === "cas";
    case ChangeKind.CRDT_UPDATE:
      return rule === "crdt";
    case ChangeKind.OVERRIDE:
      return true;
    default:
      return false;
  }
}

function keyChange(c: ChangeInput, facts: ConfirmedFacts): KeyChange {
  let expected;
  if (c.expected !== undefined) {
    if ("value" in c.expected) expected = create(CasExpectSchema, { expect: { case: "value", value: c.expected.value } });
    else if ("absent" in c.expected) expected = create(CasExpectSchema, { expect: { case: "absent", value: true } });
    else {
      // 14 [CLI-17]: a CasExpect.version that the engine sets is taken from the confirmed state
      const v = c.expected.version ?? facts.versionOf(c.key);
      expected = create(CasExpectSchema, { expect: { case: "version", value: v } });
    }
  }
  return create(KeyChangeSchema, {
    key: c.key,
    kind: c.kind,
    value: c.value ?? new Uint8Array(0),
    isNull: c.isNull ?? false,
    // a value change carries its encoding (json/v1 unless the app says otherwise); an INCR and a CRDT update carry none (05 [STATE-20])
    encoding: c.encoding ?? ((c.kind === ChangeKind.SET || c.kind === ChangeKind.CAS || c.kind === ChangeKind.OVERRIDE) && c.update === undefined ? "json/v1" : ""),
    delta: c.delta ?? 0n,
    update: c.update ?? new Uint8Array(0),
    ...(expected !== undefined ? { expected } : {}),
    // 14 [CLI-17]: the key's version in the confirmed state, tombstones included, or 0
    seenVersion: facts.versionOf(c.key),
  });
}

function precondition(p: PreconditionInput, facts: ConfirmedFacts): Precondition {
  const target = p.key !== undefined ? ({ case: "key", value: p.key } as const) : p.prefix !== undefined ? ({ case: "prefix", value: p.prefix } as const) : undefined;
  let check: Precondition["check"] = { case: undefined };
  if (p.unchangedSince !== undefined) check = { case: "unchangedSince", value: p.unchangedSince === "confirmed" ? facts.seq : p.unchangedSince };
  else if (p.exists === true) check = { case: "exists", value: true };
  else if (p.absent === true) check = { case: "absent", value: true };
  else if (p.afterMutation !== undefined) check = { case: "afterMutation", value: p.afterMutation };
  else if (p.expectedSeq !== undefined) check = { case: "expectedSeq", value: p.expectedSeq };
  // 14 [CLI-18]: the epoch of the confirmed state that the precondition was taken from; 0 when the client holds none
  return create(PreconditionSchema, { ...(target !== undefined ? { target } : {}), check, logEpoch: facts.loaded ? facts.logEpoch : 0 });
}

/**
 * Builds the Mutation of an action, without its stamping fields: the changes with `seen_version` from the confirmed state
 * ([CLI-17]), the preconditions with their epoch ([CLI-18]), `base_seq` and `base_log_epoch` from the confirmed watermark and
 * epoch, or both 0 when the client holds no confirmed state ([CLI-18]). An action that depends on a pending action takes the same
 * values from the confirmed state: the pending action's prediction never counts ([CLI-18]).
 */
export function buildMutation(
  input: ActionInput,
  mutationId: string,
  op: OpTypeInfo,
  facts: ConfirmedFacts,
  authoredAtMs: number,
): Mutation {
  return create(MutationSchema, {
    mutationId,
    keyspaceId: input.keyspaceId,
    opType: input.opType,
    lane: op.lane === "GATED" ? Lane.GATED : Lane.SEQUENCED,
    changes: input.changes.map((c) => keyChange(c, facts)),
    preconditions: (input.preconditions ?? []).map((p) => precondition(p, facts)),
    baseSeq: facts.loaded ? facts.seq : 0n,
    baseLogEpoch: facts.loaded ? facts.logEpoch : 0,
    authoredManifestVersion: facts.manifestVersion,
    requiredFeatures: [],
    causationId: input.causationId ?? "",
    correlationId: input.correlationId ?? "",
    authoredAt: timestampFromMs(authoredAtMs),
    reason: input.reason ?? "",
    blobRefs: input.blobRefs ?? [],
  });
}

/**
 * The local check of 14 [CLI-19]: the action against the manifest (templates, `writes`, change kinds) and the limits, so that the
 * client refuses an action that is certain to be dead-lettered. Returns the reason, or undefined when the action passes.
 * What the check cannot judge (value schemas, grants) is left to the server.
 */
export function checkAction(m: Mutation, manifest: EngineManifest, op: OpTypeInfo, limits: AuthorLimits, size: number): string | undefined {
  if (m.changes.length === 0) return "an action has at least one change (07 Table 7-2)";
  if (m.changes.length > limits.maxChangesPerAction) return `more than ${limits.maxChangesPerAction} changes`;
  if (size > limits.maxActionBytes) return `the action is larger than max_action_bytes (${limits.maxActionBytes})`;
  const seen = new Set<string>();
  for (const c of m.changes) {
    if (seen.has(c.key)) return `key "${c.key}" is changed twice (05 [STATE-5])`;
    seen.add(c.key);
    if (c.kind === ChangeKind.UNSPECIFIED) return `change of key "${c.key}" has no kind`;
    if (c.value.length > limits.maxValueBytes || c.update.length > limits.maxValueBytes) return `the value of key "${c.key}" is larger than max_value_bytes`;
    // SPEC-QUESTION: 07 Table 7-2 item 3 and [MUT-18], which the Go server flags (SPEC-QUESTIONS Q15.1, Q15.2): the form of an `OVERRIDE`
    // of a reserved key (is it judged as a key that matches no template, which needs `encoding`, or excepted and left to sub-step 4?),
    // and which code a mixed or reason-less override action gets. This engine authors no override-only action (12 §12.8 is an admin
    // tool), so it does not judge a reserved key at all and leaves every one of these forms to the server, whichever reading wins.
    if (isReservedKey(c.key)) continue; // a reserved key has no template of the manifest (03 §3.8)
    const tpl = manifest.template(m.authoredManifestVersion, c.key);
    if (tpl === undefined) return `key "${c.key}" matches no template`;
    if (!kindFits(tpl.rule, c.kind)) return `change kind ${ChangeKind[c.kind] ?? c.kind} does not fit the ${tpl.rule} template of key "${c.key}"`;
    if (op.writes !== undefined) {
      const pattern = manifest.templatePattern(c.key);
      if (pattern === undefined || !op.writes.includes(pattern)) return `op type ${op.name} does not write the template of key "${c.key}"`;
    }
  }
  if (m.reason !== "") return "only an override-only action has a reason (07 [MUT-18]), and this engine authors none";
  return undefined;
}

/** Refuses locally. */
export function refuse(detail: string): never {
  throw new LocalRefusal("INVALID", detail);
}
