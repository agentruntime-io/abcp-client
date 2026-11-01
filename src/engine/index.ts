// The client engine (14 §14.1 to §14.6): `Engine`, its configuration and hooks, the events it emits, the storage interface of the
// durable outbox, the transport interface, and the helpers to build actions.

export { Engine, CrdtHandle, type SignOutOptions, type SignOutResult } from "./engine.js";
export type { EngineConfig } from "./core.js";
export * from "./types.js";
export { LocalRefusal, ProtocolViolation, RpcError, StorageError, type LocalRefusalCode, type RpcStatus } from "./errors.js";
export { type Clock, type Random, systemClock, systemRandom } from "./clock.js";
export type { Transport, CallOptions } from "./transport.js";
export { BytesTransport, decodeMessage, encodeMessage, type RawCaller } from "./wire.js";
export {
  StaticManifests,
  type EngineManifest,
  type Manifests,
  type OpTypeInfo,
  type StaticManifestSpec,
  type StaticOpType,
  type StaticTemplate,
} from "./manifests.js";
export {
  StoreFull,
  type Author,
  type DeadLetterEntry,
  type DraftEntry,
  type ExportEntry,
  type JournalEntry,
  type LastResult,
  type OutboxEntry,
  type OutboxStorage,
  type RejectionRecord,
  type StorageOp,
  type StoredOutcome,
  type ReplicaBase,
  type ReplicaHead,
  type ReplicaImage,
  type StorageMode,
  type StoredState,
  type StoreName,
} from "./storage.js";
export { MemoryStorage, type MemoryStorageOptions } from "./memory-storage.js";
export { KeyspaceView, type ViewValue } from "./overlay.js";
export { jsonBytes, set, setNull, del, incr, cas, crdtUpdate } from "./changes.js";
export { CLIENT_BACKOFF, HOLD_BACKOFF, RECONNECT_BACKOFF, fullJitter, bound, resendDelay } from "./backoff.js";
export { DEFAULT_CLASS, codeName, reactionOf, type Reaction } from "./codes.js";
export { makeReject } from "./endings.js";
export { newUuid } from "./ids.js";
export { DEFAULT_HORIZON_WARNING } from "./horizon.js";
export { TOKEN_REFRESH_LEAD_MS, type CopyPolicy, type PurgeHook } from "./revocation.js";
export { COMMAND_TIMEOUT_MS, type RuntimeSnapshot } from "./tab-layer.js";
export { checkInterest, inInterest } from "./replica-store.js";
export {
  lockName,
  partitionId,
  sameKey,
  type CrossTabLocks,
  type LockLease,
  type PartitionKey,
  type StorageProvider,
  type TabChannel,
  type TabHost,
} from "./tabs.js";
export { MemoryDevice, MemoryStorageProvider, MemoryTab, type MemoryTabOptions } from "./tabs-memory.js";
