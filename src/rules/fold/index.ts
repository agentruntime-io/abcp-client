// The fold of served records onto a client's confirmed state (02 [ARC-26], 05 §5.5 to §5.7, 06 §6.4 to §6.7, 14 [CLI-1] to [CLI-5]).

export { type Anomaly, FoldError, type FoldCode } from "./errors.js";
export {
  RESERVED_TEMPLATE,
  VersionTopology,
  inSubtree,
  isReservedKey,
  type Rule,
  type Rules,
  type Template,
  type Topology,
} from "./rules.js";
export {
  expandKeyTemplate,
  isSegment,
  matchKeyTemplate,
  parseKeyTemplate,
  type KeyTemplate,
} from "./template.js";
export {
  CRDT_ENCODING,
  ReplicaState,
  StateNull,
  StateRedacted,
  StateValue,
  covers,
  type KeyRow,
  type KeyValueState,
  type NestedMarker,
  type StateOptions,
  type Subtree,
  type Time,
  type Tombstone,
  type TombstoneKind,
} from "./state.js";
export { DEFAULT_FEATURES, applyRecord, applyRecords, type ApplyResult, type FoldOptions } from "./apply.js";
export { installEntry } from "./load.js";
