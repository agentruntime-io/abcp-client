# abcp-client

The ABCP v2 client engine and its local query layer, in TypeScript, for browsers (React and others) and for Node servers.

The engine keeps a local replica of the keyspaces an app follows, sends the app's changes as actions, folds the records the server
serves, and tells the app what it shows. The query layer answers live queries over that replica.

**Status: alpha, for testing.** The version is `2.0.0-alpha.6`. The package is **not on npm yet**; `package.json` is marked
`"private": true` until that is decided. Licensed under the Apache License 2.0 (see `LICENSE`).

## Build and test

Node 22 or later.

```sh
npm ci
npm run typecheck
npm run build      # compiles src/ to dist/
npm test           # the fast unit tests
```

`npm test` runs in a few seconds. The heavy tests need a checkout of the ABCP server repo and skip without one, naming the reason
"needs an abcp-server checkout (ABCP_SERVER_DIR); runs in the server's release checks":

- the conformance vectors (`conformance/vectors`),
- the CRDT test data (`server/internal/crdt/testdata`),
- the integration tests, which build the Go server from `server/` (they also need a Go toolchain on the `PATH`).

To run them, point `ABCP_SERVER_DIR` at that checkout:

```sh
ABCP_SERVER_DIR=../abcp-server npm test
```

`ABCP_SKIP_INTEGRATION=1` skips only the integration tests that run the Go server.

## Entry points

After `npm run build`, apps import from `dist/`:

| Import | What it is |
| --- | --- |
| `dist/engine/index.js` | the engine: `Engine`, storage and manifest interfaces, the error types |
| `dist/net/` (`index.js`, `node.js`) | the transport: `createWebTransport` for browsers, `createNodeTransport` for Node, token handling, manifest loading |
| `dist/query/index.js` | the local query layer |
| `dist/platform/` | storage for browsers (IndexedDB) and Node (files), tab coordination |
| `dist/fake/index.js` | an in-process fake server, for tests of an app |

## `listKeyspaces` (experimental)

The transport has `listKeyspaces(req, opts)` for the server's experimental `SyncService.ListKeyspaces` (spec 09 §9.11, not part of
ABCP v2.0; abcp-server `v2.0.0-alpha.6`, behind its `x-list-keyspaces.v1` switch).

- It lists the keyspaces of the app that the caller may read, in creation order.
- The caller is a SERVICE principal or a principal of the app's admin issuer; a USER or DEVICE token gets PermissionDenied.
- A server without the feature, or with its switch off, answers `Unimplemented`, and so does the fake server. Check that
  `ServerInfo.features` lists `x-list-keyspaces.v1` before calling it.
- The response is decoded strictly: a page with more keyspaces than the request's `page_size` allows is refused ([XKS-6]).
- The engine does not call it; service code does. To follow new keyspaces, list again with `createdAfter` set to the newest
  `createdAt` seen minus 60 s, and dedupe by `keyspaceId` (spec 09 [XKS-8]).

## The wire code

`src/gen` is generated from `proto/abcp` (the ABCP v2 wire protos) with `buf` and `protoc-gen-es`. To regenerate it, run
`npm run gen`; the output must not change unless the protos do. `test/gen/abcptest` is generated code for the test profile's control
API, used by the integration tests; it is kept as it is.
