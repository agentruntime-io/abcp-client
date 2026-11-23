// The transport the engine talks through: `Mutate`, `GetMutationResult(s)`, `CancelIntent`, `CreateKeyspace`, `LoadState`,
// `Subscribe` (an async iterator of frames), `UpdateSubscription`, `RefreshSession`, `GetManifest`, the presence calls (09 §9.8:
// `JoinPresence`, `PublishPresence`, `LeavePresence`) and the blob calls (09 §9.9: `CreateUpload`, `CommitBlob`, `GetBlob`,
// `DeleteBlob`). The fake server of
// `src/fake` implements it, and so does the adapter over raw bytes of wire.ts; a later milestone (C5) implements it over
// connect-es.
//
// A call that fails rejects with an `RpcError` (A.7). A message that breaks 02 [ARC-61] rejects with a `ProtocolViolation`: the
// transport, not the engine, decodes, and it decodes strictly (rules/canon), never with protobuf-es `fromBinary` alone.

import type {
  CancelIntentRequest,
  CancelIntentResponse,
  CommitBlobRequest,
  CommitBlobResponse,
  CreateKeyspaceRequest,
  CreateKeyspaceResponse,
  CreateUploadRequest,
  CreateUploadResponse,
  DeleteBlobRequest,
  DeleteBlobResponse,
  GetBlobRequest,
  GetBlobResponse,
  GetManifestRequest,
  GetManifestResponse,
  GetMutationResultRequest,
  GetMutationResultResponse,
  GetMutationResultsRequest,
  GetMutationResultsResponse,
  JoinPresenceRequest,
  JoinPresenceResponse,
  LeavePresenceRequest,
  LeavePresenceResponse,
  LoadStateRequest,
  LoadStateResponse,
  MutateRequest,
  MutateResponse,
  PublishPresenceRequest,
  PublishPresenceResponse,
  RefreshSessionRequest,
  RefreshSessionResponse,
  SubscribeRequest,
  SubscribeResponse,
  UpdateSubscriptionRequest,
  UpdateSubscriptionResponse,
} from "../gen/abcp/v2/sync_pb.js";

export interface CallOptions {
  /** Aborts the call (and ends a `Subscribe` stream). */
  signal?: AbortSignal;
  /**
   * The call's deadline in ms (02 [ARC-60]: `connect-timeout-ms`; the call fails with `DeadlineExceeded` when it passes). The engine sets
   * none; a transport has a deadline of its own for each kind of call. Added with the real transport (C5).
   */
  timeoutMs?: number;
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
  // -- presence (09 §9.8): the calls name the session that `Subscribe` opened --
  joinPresence(req: JoinPresenceRequest, o?: CallOptions): Promise<JoinPresenceResponse>;
  publishPresence(req: PublishPresenceRequest, o?: CallOptions): Promise<PublishPresenceResponse>;
  leavePresence(req: LeavePresenceRequest, o?: CallOptions): Promise<LeavePresenceResponse>;
  // -- blobs (09 §9.9) --
  createUpload(req: CreateUploadRequest, o?: CallOptions): Promise<CreateUploadResponse>;
  commitBlob(req: CommitBlobRequest, o?: CallOptions): Promise<CommitBlobResponse>;
  getBlob(req: GetBlobRequest, o?: CallOptions): Promise<GetBlobResponse>;
  deleteBlob(req: DeleteBlobRequest, o?: CallOptions): Promise<DeleteBlobResponse>;
}
