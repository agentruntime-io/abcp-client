// The platforms of the engine (C5): what a tab gets from Node (`node.ts`: in-process locks, files, `BroadcastChannel`) and from a browser
// (`web.ts`: Web Locks, IndexedDB, `BroadcastChannel`), behind the interfaces of `engine/tabs.ts`. This index leaves out the Node host, which pulls in
// `node:fs`: import it from `platform/node.js`.

export { BroadcastTabChannel, type BroadcastChannelConstructor, type BroadcastChannelLike } from "./channel.js";
export { InProcessLocks } from "./locks.js";
export { WebLocks, type LockManagerLike } from "./web-locks.js";
export { IndexedDbStorage, IndexedDbStorageProvider, type IDBFactoryLike, type IndexedDbOptions, type KeyRangeFactory, type StorageManagerLike } from "./idb-storage.js";
export { WebTab, createWebHost, globalWebEnv, type WebEnv, type WebHostOptions } from "./web.js";
