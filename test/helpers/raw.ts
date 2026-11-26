// A RawCaller over the fake server: the engine talks to the fake through bytes, as a connect-es binding would, and a test may
// corrupt what the fake sends.

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
  JoinPresenceRequestSchema,
  JoinPresenceResponseSchema,
  LeavePresenceRequestSchema,
  LeavePresenceResponseSchema,
  PublishPresenceRequestSchema,
  PublishPresenceResponseSchema,
  GetManifestRequestSchema,
  GetManifestResponseSchema,
  GetMutationResultRequestSchema,
  GetMutationResultResponseSchema,
  GetMutationResultsRequestSchema,
  GetMutationResultsResponseSchema,
  LoadStateRequestSchema,
  LoadStateResponseSchema,
  ListKeyspacesRequestSchema,
  ListKeyspacesResponseSchema,
  MutateRequestSchema,
  MutateResponseSchema,
  RefreshSessionRequestSchema,
  RefreshSessionResponseSchema,
  SubscribeRequestSchema,
  SubscribeResponseSchema,
  UpdateSubscriptionRequestSchema,
  UpdateSubscriptionResponseSchema,
  type SubscribeResponse,
} from "../../src/gen/abcp/v2/sync_pb.js";
import type { RawCaller } from "../../src/engine/index.js";
import { encode, unmarshal } from "../../src/rules/canon/index.js";
import type { FakeTransport } from "../../src/fake/index.js";

type Unary = [DescMessage, DescMessage, (t: FakeTransport, req: never) => Promise<Message>];

const UNARY: Record<string, Unary> = {
  Mutate: [MutateRequestSchema, MutateResponseSchema, (t, r) => t.mutate(r)],
  LoadState: [LoadStateRequestSchema, LoadStateResponseSchema, (t, r) => t.loadState(r)],
  GetMutationResult: [GetMutationResultRequestSchema, GetMutationResultResponseSchema, (t, r) => t.getMutationResult(r)],
  GetMutationResults: [GetMutationResultsRequestSchema, GetMutationResultsResponseSchema, (t, r) => t.getMutationResults(r)],
  CancelIntent: [CancelIntentRequestSchema, CancelIntentResponseSchema, (t, r) => t.cancelIntent(r)],
  CreateKeyspace: [CreateKeyspaceRequestSchema, CreateKeyspaceResponseSchema, (t, r) => t.createKeyspace(r)],
  UpdateSubscription: [UpdateSubscriptionRequestSchema, UpdateSubscriptionResponseSchema, (t, r) => t.updateSubscription(r)],
  RefreshSession: [RefreshSessionRequestSchema, RefreshSessionResponseSchema, (t, r) => t.refreshSession(r)],
  GetManifest: [GetManifestRequestSchema, GetManifestResponseSchema, (t, r) => t.getManifest(r)],
  JoinPresence: [JoinPresenceRequestSchema, JoinPresenceResponseSchema, (t, r) => t.joinPresence(r)],
  PublishPresence: [PublishPresenceRequestSchema, PublishPresenceResponseSchema, (t, r) => t.publishPresence(r)],
  LeavePresence: [LeavePresenceRequestSchema, LeavePresenceResponseSchema, (t, r) => t.leavePresence(r)],
  CreateUpload: [CreateUploadRequestSchema, CreateUploadResponseSchema, (t, r) => t.createUpload(r)],
  CommitBlob: [CommitBlobRequestSchema, CommitBlobResponseSchema, (t, r) => t.commitBlob(r)],
  GetBlob: [GetBlobRequestSchema, GetBlobResponseSchema, (t, r) => t.getBlob(r)],
  DeleteBlob: [DeleteBlobRequestSchema, DeleteBlobResponseSchema, (t, r) => t.deleteBlob(r)],
  ListKeyspaces: [ListKeyspacesRequestSchema, ListKeyspacesResponseSchema, (t, r) => t.listKeyspaces(r)],
};

export class FakeRaw implements RawCaller {
  /** The bytes of every request that reached the fake, by method. */
  readonly requests: { method: string; bytes: Uint8Array }[] = [];
  /** Changes the bytes of a frame before the client reads them. */
  tamper: ((bytes: Uint8Array, frame: SubscribeResponse) => Uint8Array) | undefined;
  /** Changes the bytes of a unary response. */
  tamperUnary: ((method: string, bytes: Uint8Array) => Uint8Array) | undefined;

  constructor(private readonly t: FakeTransport) {}

  async unary(method: string, request: Uint8Array): Promise<Uint8Array> {
    this.requests.push({ method, bytes: request });
    const entry = UNARY[method];
    if (entry === undefined) throw new Error(`no method ${method}`);
    const [reqSchema, resSchema, call] = entry;
    const req = unmarshal(reqSchema, request) as never;
    const res = await call(this.t, req);
    const bytes = encode(resSchema, res);
    return this.tamperUnary?.(method, bytes) ?? bytes;
  }

  async *stream(method: string, request: Uint8Array, o?: { signal?: AbortSignal }): AsyncIterable<Uint8Array> {
    this.requests.push({ method, bytes: request });
    const req = unmarshal(SubscribeRequestSchema, request);
    for await (const f of this.t.subscribe(req as never, o ?? {})) {
      const bytes = encode(SubscribeResponseSchema, f);
      yield this.tamper?.(bytes, f) ?? bytes;
    }
  }
}

/** Replaces the first occurrence of an ASCII word in bytes with the same word with one byte set to 0xFF: a string that is not valid UTF-8. */
export function breakUtf8(bytes: Uint8Array, word: string): Uint8Array {
  const out = bytes.slice();
  const w = new TextEncoder().encode(word);
  outer: for (let i = 0; i + w.length <= out.length; i++) {
    for (let j = 0; j < w.length; j++) if (out[i + j] !== w[j]) continue outer;
    out[i + 1] = 0xff;
    return out;
  }
  throw new Error(`"${word}" is not in the bytes`);
}
