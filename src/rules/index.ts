// The rules that the client engine shares with the server (C1): json/v1 values, canonical bytes and hashes, the CRDT reader and
// fold, and the fold of served records. Each module cites the rule ids it implements; the vectors of 17 [CONF-9] test them.
//
// Import the modules by name; their exports overlap in spirit, not in name:
//   import * as jsonv1 from ".../rules/jsonv1/index.js";
//   import * as canon from ".../rules/canon/index.js";
//   import * as hashes from ".../rules/hashes/index.js";
//   import * as crdt from ".../rules/crdt/index.js";
//   import * as fold from ".../rules/fold/index.js";
// The default hash provider is pure TypeScript; `setHashProvider(nodeHashProvider)` (from ./node-crypto.js) swaps in node:crypto.

export * as jsonv1 from "./jsonv1/index.js";
export * as canon from "./canon/index.js";
export * as hashes from "./hashes/index.js";
export * as crdt from "./crdt/index.js";
export * as fold from "./fold/index.js";
export { type HashProvider, getHashProvider, pureHashProvider, setHashProvider } from "./crypto.js";
export { compareBytes, compareUtf8, fromHex, toHex } from "./bytes.js";
