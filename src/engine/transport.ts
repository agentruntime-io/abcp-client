// The transport the engine talks through: `Mutate`, `GetMutationResult(s)`, `CancelIntent`, `CreateKeyspace`, `LoadState`,
// `Subscribe` (an async iterator of frames), `UpdateSubscription`, `RefreshSession` and `GetManifest`. The fake server of
// `src/fake` implements it, and so does the adapter over raw bytes of wire.ts; a later milestone (C5) implements it over
// connect-es.
//
// A call that fails rejects with an `RpcError` (A.7). A message that breaks 02 [ARC-61] rejects with a `ProtocolViolation`: the
// transport, not the engine, decodes, and it decodes strictly (rules/canon), never with protobuf-es `fromBinary` alone.

import type {
  CancelIntentRequest,
  CancelIntentResponse,
  CreateKeyspaceRequest,
  CreateKeyspaceResponse,
  GetManifestRequest,
  GetManifestResponse,
  GetMutationResultRequest,
  GetMutationResultResponse,
  GetMutationResultsRequest,
  GetMutationResultsResponse,
  LoadStateRequest,
  LoadStateResponse,
  MutateRequest,
  MutateResponse,
  RefreshSessionRequest,
  RefreshSessionResponse,
  SubscribeRequest,
  SubscribeResponse,
  UpdateSubscriptionRequest,
  UpdateSubscriptionResponse,
} from "../gen/abpc/v2/sync_pb.js";

export interface CallOptions {
  /** Aborts the call (and ends a `Subscribe` stream). */
  signal?: AbortSignal;
}

export interface Transport {
  getManifest(req: GetManifestRequest, o?: CallOptions): Promise<GetManifestResponse>;
  mutate(req: MutateRequest, o?: CallOptions): Promise<MutateResponse>;
  getMutationResult(req: GetMutationResultRequest, o?: CallOptions): Promise<GetMutationResultResponse>;
  getMutationResults(req: GetMutationResultsRequest, o?: CallOptions): Promise<GetMutationResultsResponse>;
  cancelIntent(req: CancelIntentRequest, o?: CallOptions): Promise<CancelIntentResponse>;
  createKeyspace(req: CreateKeyspaceRequest, o?: CallOptions): Promise<CreateKeyspaceResponse>;
  loadState(req: LoadStateRequest, o?: CallOptions): Promise<LoadStateResponse>;
  /**
   * Opens a session. The iterator yields the frames in order. It ends when the stream ends cleanly, and throws an `RpcError` when
   * the stream ends with a status (after the `Closing` frame, for a session that the server ended), or a `ProtocolViolation`
   * for a frame that breaks 02 [ARC-61].
   */
  subscribe(req: SubscribeRequest, o?: CallOptions): AsyncIterable<SubscribeResponse>;
  updateSubscription(req: UpdateSubscriptionRequest, o?: CallOptions): Promise<UpdateSubscriptionResponse>;
  refreshSession(req: RefreshSessionRequest, o?: CallOptions): Promise<RefreshSessionResponse>;
}
