// A `Transport` over Connect: the `BytesTransport` of the engine (which encodes canonically and decodes strictly, rules/canon) on a
// `ConnectCaller` (the Connect protocol over an HTTP client). The Node and the browser transports differ only in the HTTP client.

import type { UniversalClientFn } from "@connectrpc/connect/protocol";
import { BytesTransport } from "../engine/wire.js";
import { ConnectCaller, type ConnectCallerOptions } from "./caller.js";

export type BytesTransportOptions = ConnectCallerOptions & { http: UniversalClientFn };

/** The transport of the engine over the HTTP client of the options. */
export function createBytesTransport(o: BytesTransportOptions): BytesTransport {
  return new BytesTransport(new ConnectCaller(o));
}
