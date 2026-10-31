// The node:crypto hash provider. It is a separate module so that a browser build, which never imports it, does not pull
// in node:crypto: `import { nodeHashProvider } from ".../rules/node-crypto.js"; setHashProvider(nodeHashProvider);`

import { createHash, createHmac } from "node:crypto";
import type { HashProvider } from "./crypto.js";

/** SHA-256 and HMAC-SHA-256 on node:crypto. */
export const nodeHashProvider: HashProvider = {
  sha256(data) {
    return new Uint8Array(createHash("sha256").update(data).digest());
  },
  hmacSha256(key, data) {
    return new Uint8Array(createHmac("sha256", key).update(data).digest());
  },
};
