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
  CreateKeyspaceRequestSchema,
  CreateKeyspaceResponseSchema,
  GetManifestRequestSchema,
  GetManifestResponseSchema,
  GetMutationResultRequestSchema,
  GetMutationResultResponseSchema,
  GetMutationResultsRequestSchema,
  GetMutationResultsResponseSchema,
  LoadStateRequestSchema,
  LoadStateResponseSchema,
  MutateRequestSchema,
  MutateResponseSchema,
  RefreshSessionRequestSchema,
  RefreshSessionResponseSchema,
  SubscribeRequestSchema,
  SubscribeResponseSchema,
  UpdateSubscriptionRequestSchema,
  UpdateSubscriptionResponseSchema,
  type CancelIntentRequest,
  type CancelIntentResponse,
  type CreateKeyspaceRequest,
  type CreateKeyspaceResponse,
  type GetManifestRequest,
  type GetManifestResponse,
  type GetMutationResultRequest,
  type GetMutationResultResponse,
  type GetMutationResultsRequest,
  type GetMutationResultsResponse,
  type LoadStateRequest,
  type LoadStateResponse,
  type MutateRequest,
  type MutateResponse,
  type RefreshSessionRequest,
  type RefreshSessionResponse,
  type SubscribeRequest,
  type SubscribeResponse,
  type UpdateSubscriptionRequest,
  type UpdateSubscriptionResponse,
} from "../gen/abpc/v2/sync_pb.js";
import { CanonError, checkTimestamps, encode, unmarshal } from "../rules/canon/index.js";
import { ProtocolViolation } from "./errors.js";
import type { CallOptions, Transport } from "./transport.js";

/** What a connect-es (or any other) binding gives the adapter: the bytes of a request and of a response, by the method's name. */
export interface RawCaller {
  unary(method: string, request: Uint8Array, o?: CallOptions): Promise<Uint8Array>;
  stream(method: string, request: Uint8Array, o?: CallOptions): AsyncIterable<Uint8Array>;
}

/** Decodes the bytes of a message that a server sent, strictly. Throws a ProtocolViolation for bytes that break 02 [ARC-61]. */
export function decodeMessage<T extends Message>(schema: DescMessage, bytes: Uint8Array): T {
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
  ): Promise<Res> {
    const bytes = await this.raw.unary(method, encodeMessage(reqSchema, req), o);
    return decodeMessage<Res>(resSchema, bytes);
  }

  getManifest(req: GetManifestRequest, o?: CallOptions): Promise<GetManifestResponse> {
    return this.unary("GetManifest", GetManifestRequestSchema, GetManifestResponseSchema, req, o);
  }
  mutate(req: MutateRequest, o?: CallOptions): Promise<MutateResponse> {
    return this.unary("Mutate", MutateRequestSchema, MutateResponseSchema, req, o);
  }
  getMutationResult(req: GetMutationResultRequest, o?: CallOptions): Promise<GetMutationResultResponse> {
    return this.unary("GetMutationResult", GetMutationResultRequestSchema, GetMutationResultResponseSchema, req, o);
  }
  getMutationResults(req: GetMutationResultsRequest, o?: CallOptions): Promise<GetMutationResultsResponse> {
    return this.unary("GetMutationResults", GetMutationResultsRequestSchema, GetMutationResultsResponseSchema, req, o);
  }
  cancelIntent(req: CancelIntentRequest, o?: CallOptions): Promise<CancelIntentResponse> {
    return this.unary("CancelIntent", CancelIntentRequestSchema, CancelIntentResponseSchema, req, o);
  }
  createKeyspace(req: CreateKeyspaceRequest, o?: CallOptions): Promise<CreateKeyspaceResponse> {
    return this.unary("CreateKeyspace", CreateKeyspaceRequestSchema, CreateKeyspaceResponseSchema, req, o);
  }
  loadState(req: LoadStateRequest, o?: CallOptions): Promise<LoadStateResponse> {
    return this.unary("LoadState", LoadStateRequestSchema, LoadStateResponseSchema, req, o);
  }
  updateSubscription(req: UpdateSubscriptionRequest, o?: CallOptions): Promise<UpdateSubscriptionResponse> {
    return this.unary("UpdateSubscription", UpdateSubscriptionRequestSchema, UpdateSubscriptionResponseSchema, req, o);
  }
  refreshSession(req: RefreshSessionRequest, o?: CallOptions): Promise<RefreshSessionResponse> {
    return this.unary("RefreshSession", RefreshSessionRequestSchema, RefreshSessionResponseSchema, req, o);
  }

  async *subscribe(req: SubscribeRequest, o?: CallOptions): AsyncIterable<SubscribeResponse> {
    for await (const bytes of this.raw.stream("Subscribe", encodeMessage(SubscribeRequestSchema, req), o)) {
      yield decodeMessage<SubscribeResponse>(SubscribeResponseSchema, bytes);
    }
  }
}
