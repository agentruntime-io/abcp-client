// A `Transport` over raw bytes: it encodes each request in the canonical encoding (07 [MUT-45]) and decodes each response and
// frame strictly, with `rules/canon`: invalid UTF-8 is a refusal and never repaired, a wrong wire type is an unknown field, and
// a Timestamp out of range is a refusal (02 [ARC-61]). protobuf-es `fromBinary` alone does none of this (docs/PLAN.md, the C1
// note), so every transport that moves bytes (connect-es, in C5) goes through here.
//
// A `CanonError` becomes a `ProtocolViolation`, which the engine handles as 16 [SEC-3] and 14 [CLI-50] say.

import type { DescMessage, Message } from "@bufbuild/protobuf";
import {
  CancelIntentRequestSchema,
  CancelIntentResponseSchema,
  CommitBlobRequestSchema,
  CommitBlobResponseSchema,
  CreateKeyspaceRequestSchema,
  CreateKeyspaceResponseSchema,
  CreateUploadRequestSchema,
  CreateUploadResponseSchema,
  DeleteBlobRequestSchema,
  DeleteBlobResponseSchema,
  GetBlobRequestSchema,
  GetBlobResponseSchema,
  GetManifestRequestSchema,
  GetManifestResponseSchema,
  GetMutationResultRequestSchema,
  GetMutationResultResponseSchema,
  GetMutationResultsRequestSchema,
  GetMutationResultsResponseSchema,
  JoinPresenceRequestSchema,
  JoinPresenceResponseSchema,
  LeavePresenceRequestSchema,
  LeavePresenceResponseSchema,
  LoadStateRequestSchema,
  LoadStateResponseSchema,
  ListKeyspacesRequestSchema,
  ListKeyspacesResponseSchema,
  MutateRequestSchema,
  MutateResponseSchema,
  PublishPresenceRequestSchema,
  PublishPresenceResponseSchema,
  RefreshSessionRequestSchema,
  RefreshSessionResponseSchema,
  SubscribeRequestSchema,
  SubscribeResponseSchema,
  UpdateSubscriptionRequestSchema,
  UpdateSubscriptionResponseSchema,
  type CancelIntentRequest,
  type CancelIntentResponse,
  type CommitBlobRequest,
  type CommitBlobResponse,
  type CreateKeyspaceRequest,
  type CreateKeyspaceResponse,
  type CreateUploadRequest,
  type CreateUploadResponse,
  type DeleteBlobRequest,
  type DeleteBlobResponse,
  type GetBlobRequest,
  type GetBlobResponse,
  type GetManifestRequest,
  type GetManifestResponse,
  type GetMutationResultRequest,
  type GetMutationResultResponse,
  type GetMutationResultsRequest,
  type GetMutationResultsResponse,
  type JoinPresenceRequest,
  type JoinPresenceResponse,
  type LeavePresenceRequest,
  type LeavePresenceResponse,
  type LoadStateRequest,
  type LoadStateResponse,
  type ListKeyspacesRequest,
  type ListKeyspacesResponse,
  type MutateRequest,
  type MutateResponse,
  type PublishPresenceRequest,
  type PublishPresenceResponse,
  type RefreshSessionRequest,
  type RefreshSessionResponse,
  type SubscribeRequest,
  type SubscribeResponse,
  type UpdateSubscriptionRequest,
  type UpdateSubscriptionResponse,
} from "../gen/abcp/v2/sync_pb.js";
import { CanonError, checkTimestamps, encode, unmarshal } from "../rules/canon/index.js";
import { checkCounts, listKeyspacesPageBound, pageBound, type DecodeLimits } from "./counts.js";
import { ProtocolViolation } from "./errors.js";
import type { CallOptions, Transport } from "./transport.js";

/** What a connect-es (or any other) binding gives the adapter: the bytes of a request and of a response, by the method's name. */
export interface RawCaller {
  unary(method: string, request: Uint8Array, o?: CallOptions): Promise<Uint8Array>;
  stream(method: string, request: Uint8Array, o?: CallOptions): AsyncIterable<Uint8Array>;
}

/**
 * Decodes the bytes of a message that a server sent, strictly. Throws a ProtocolViolation for bytes that break 02 [ARC-61], and for a message whose
 * repeated fields hold more elements than the text allows (16 [SEC-1]): the counts are judged on the octets, before one element is decoded
 * (`counts.ts`). `limits` are the counts that the call fixes (the page of a load, the mutations of a request).
 */
export function decodeMessage<T extends Message>(schema: DescMessage, bytes: Uint8Array, limits?: DecodeLimits): T {
  checkCounts(schema, bytes, limits);
  try {
    const m = unmarshal(schema, bytes);
    checkTimestamps(schema, m);
    return m as T;
  } catch (e) {
    if (e instanceof CanonError) throw new ProtocolViolation(schema.name, e.message);
    throw e;
  }
}

/** Encodes a request canonically (07 [MUT-45]). */
export function encodeMessage(schema: DescMessage, m: Message): Uint8Array {
  return encode(schema, m);
}

export class BytesTransport implements Transport {
  constructor(private readonly raw: RawCaller) {}

  private async unary<Req extends Message, Res extends Message>(
    method: string,
    reqSchema: DescMessage,
    resSchema: DescMessage,
    req: Req,
    o: CallOptions | undefined,
    limits?: DecodeLimits,
  ): Promise<Res> {
    const bytes = await this.raw.unary(method, encodeMessage(reqSchema, req), o);
    return decodeMessage<Res>(resSchema, bytes, limits);
  }

  getManifest(req: GetManifestRequest, o?: CallOptions): Promise<GetManifestResponse> {
    return this.unary("GetManifest", GetManifestRequestSchema, GetManifestResponseSchema, req, o);
  }
  mutate(req: MutateRequest, o?: CallOptions): Promise<MutateResponse> {
    // 07 [MUT-4], [MUT-7]: one result for each mutation, and one watermark at most for each
    const n = req.mutations.length;
    return this.unary("Mutate", MutateRequestSchema, MutateResponseSchema, req, o, { "abcp.v2.MutateResponse.results": n, "abcp.v2.MutateResponse.watermarks": n });
  }
  getMutationResult(req: GetMutationResultRequest, o?: CallOptions): Promise<GetMutationResultResponse> {
    return this.unary("GetMutationResult", GetMutationResultRequestSchema, GetMutationResultResponseSchema, req, o);
  }
  getMutationResults(req: GetMutationResultsRequest, o?: CallOptions): Promise<GetMutationResultsResponse> {
    return this.unary("GetMutationResults", GetMutationResultsRequestSchema, GetMutationResultsResponseSchema, req, o, { "abcp.v2.GetMutationResultsResponse.results": req.mutationIds.length });
  }
  cancelIntent(req: CancelIntentRequest, o?: CallOptions): Promise<CancelIntentResponse> {
    return this.unary("CancelIntent", CancelIntentRequestSchema, CancelIntentResponseSchema, req, o);
  }
  createKeyspace(req: CreateKeyspaceRequest, o?: CallOptions): Promise<CreateKeyspaceResponse> {
    return this.unary("CreateKeyspace", CreateKeyspaceRequestSchema, CreateKeyspaceResponseSchema, req, o);
  }
  loadState(req: LoadStateRequest, o?: CallOptions): Promise<LoadStateResponse> {
    // 09 [READ-10]: `page_size` is an upper bound on the entries of a page
    return this.unary("LoadState", LoadStateRequestSchema, LoadStateResponseSchema, req, o, { "abcp.v2.LoadStateResponse.entries": pageBound(req.pageSize) });
  }
  updateSubscription(req: UpdateSubscriptionRequest, o?: CallOptions): Promise<UpdateSubscriptionResponse> {
    // 09 [READ-62]: `rejected` is bounded by the request's entries
    return this.unary("UpdateSubscription", UpdateSubscriptionRequestSchema, UpdateSubscriptionResponseSchema, req, o, { "abcp.v2.UpdateSubscriptionResponse.rejected": req.add.length + req.setInterest.length });
  }
  refreshSession(req: RefreshSessionRequest, o?: CallOptions): Promise<RefreshSessionResponse> {
    return this.unary("RefreshSession", RefreshSessionRequestSchema, RefreshSessionResponseSchema, req, o);
  }
  // 09 §9.8, §9.9: the answers hold no repeated field but the headers of `CreateUploadResponse` (counts.ts bounds them); every string and every timestamp
  // is read by the strict decoder, which refuses invalid UTF-8 and a Timestamp out of range (02 [ARC-61])
  joinPresence(req: JoinPresenceRequest, o?: CallOptions): Promise<JoinPresenceResponse> {
    return this.unary("JoinPresence", JoinPresenceRequestSchema, JoinPresenceResponseSchema, req, o);
  }
  publishPresence(req: PublishPresenceRequest, o?: CallOptions): Promise<PublishPresenceResponse> {
    return this.unary("PublishPresence", PublishPresenceRequestSchema, PublishPresenceResponseSchema, req, o);
  }
  leavePresence(req: LeavePresenceRequest, o?: CallOptions): Promise<LeavePresenceResponse> {
    return this.unary("LeavePresence", LeavePresenceRequestSchema, LeavePresenceResponseSchema, req, o);
  }
  createUpload(req: CreateUploadRequest, o?: CallOptions): Promise<CreateUploadResponse> {
    return this.unary("CreateUpload", CreateUploadRequestSchema, CreateUploadResponseSchema, req, o);
  }
  commitBlob(req: CommitBlobRequest, o?: CallOptions): Promise<CommitBlobResponse> {
    return this.unary("CommitBlob", CommitBlobRequestSchema, CommitBlobResponseSchema, req, o);
  }
  getBlob(req: GetBlobRequest, o?: CallOptions): Promise<GetBlobResponse> {
    return this.unary("GetBlob", GetBlobRequestSchema, GetBlobResponseSchema, req, o);
  }
  deleteBlob(req: DeleteBlobRequest, o?: CallOptions): Promise<DeleteBlobResponse> {
    return this.unary("DeleteBlob", DeleteBlobRequestSchema, DeleteBlobResponseSchema, req, o);
  }
  listKeyspaces(req: ListKeyspacesRequest, o?: CallOptions): Promise<ListKeyspacesResponse> {
    // spec/09 §9.11 [XKS-6], experimental: `page_size` is an upper bound on the keyspaces of a page (0 is 100, a larger value counts as 1,000)
    return this.unary("ListKeyspaces", ListKeyspacesRequestSchema, ListKeyspacesResponseSchema, req, o, { "abcp.v2.ListKeyspacesResponse.keyspaces": listKeyspacesPageBound(req.pageSize) });
  }

  async *subscribe(req: SubscribeRequest, o?: CallOptions): AsyncIterable<SubscribeResponse> {
    for await (const bytes of this.raw.stream("Subscribe", encodeMessage(SubscribeRequestSchema, req), o)) {
      yield decodeMessage<SubscribeResponse>(SubscribeResponseSchema, bytes);
    }
  }
}
