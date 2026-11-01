// The client on the real network (C5): the Connect transport (binary codec, strict decoding, error details per 02 [ARC-57]), the token with
// its in-band refresh, and the manifests of an app over `GetManifest`. `createNodeTransport` is for Node (connect-node); `createWebTransport`
// is for a browser (fetch). Import the Node one from `net/node.js`, which pulls in `node:http2`: this index leaves it out.

export { ConnectCaller, DEFAULT_UNARY_TIMEOUT_MS, MAX_MESSAGE_BYTES, type ConnectCallerOptions } from "./caller.js";
export { abpcDetail, statusOfCode, toFailure, type Ending } from "./errors.js";
export { ConnectManifests, MemoryManifestCache, ParsedManifest, readManifest, type ConnectManifestsOptions, type ManifestCache } from "./manifests.js";
export { TokenStore, jwtExpiresAt, type TokenGrant, type TokenSource, type TokenStoreOptions } from "./tokens.js";
export { createBytesTransport, type BytesTransportOptions } from "./transport.js";
export { createWebTransport, fetchHttpClient, type WebTransportOptions } from "./web.js";
