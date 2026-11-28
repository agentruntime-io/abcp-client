# abcp-client

The ABCP v2 client engine and its local query layer, in TypeScript, for browsers (React and others) and for Node servers.

The engine keeps a local replica of the keyspaces an app follows, sends the app's changes as actions, folds the records the server
serves, and tells the app what it shows. The query layer answers live queries over that replica.

**Status: alpha, for testing.** The package is on npm under the dist-tag `alpha`:

```sh
npm install @agentruntime/abcp-client@alpha
```

This is client `2.0.0-alpha.7`, which pairs with abcp-server `v2.0.0-alpha.6`. The wire files are byte-identical to client
`2.0.0-alpha.6`; alpha.7 changes only packaging and docs. Licensed under the Apache License 2.0 (see `LICENSE`).

Two things to know before you start:

- The ABCP server is not publicly available yet, so outside the project the client runs against its in-process fake server
  (`@agentruntime/abcp-client/fake`).
- The ABCP specification is not public yet. Rule ids in the docs and in code comments (for example `[XKS-6]`, `09 [READ-35]`)
  refer to it.

## Entry points

Node 22 or later. Import from the package paths below (ES modules; TypeScript finds the types next to each file):

| Import | What it is |
| --- | --- |
| `@agentruntime/abcp-client` | the root: the namespaces `types`, `sync`, `host`, `admin` (the wire messages), `rules`, `engine`, `fake`, `query` |
| `@agentruntime/abcp-client/engine` | the engine: `Engine`, storage and manifest interfaces, the error types |
| `@agentruntime/abcp-client/net` | the transport: `createWebTransport`, token handling, manifest loading |
| `@agentruntime/abcp-client/net/node` | `createNodeTransport`, the transport for Node |
| `@agentruntime/abcp-client/net/web` | `createWebTransport` alone, for browsers |
| `@agentruntime/abcp-client/platform` | browser storage (IndexedDB), Web Locks and tab coordination |
| `@agentruntime/abcp-client/platform/node` | `createNodeHost`: file storage and tab hosting for Node |
| `@agentruntime/abcp-client/platform/web` | `createWebHost`: the browser host |
| `@agentruntime/abcp-client/query` | the local query layer |
| `@agentruntime/abcp-client/fake` | an in-process fake server, with a manual clock, for tests of an app |

Files under `dist/` can also be imported by path (`@agentruntime/abcp-client/dist/...`).

## Example

An engine against the in-process fake server: follow a keyspace, write a value, read it back.

```js
import { Engine, MemoryStorage, StaticManifests, set } from "@agentruntime/abcp-client/engine";
import { FakeServer, ManualClock } from "@agentruntime/abcp-client/fake";

const manifests = new StaticManifests({
  version: 1,
  templates: { "settings/{name}": { rule: "lww" } },
  opTypes: { Set: {} },
});
const clock = new ManualClock();
const server = new FakeServer({ manifests, clock });
server.createKeyspaceNow("demo");

const principal = server.principal("ana");
const engine = await Engine.create({
  appId: "demo-app",
  principal: { issuer: principal.issuer, sub: principal.sub, deviceId: principal.deviceId },
  transport: server.transport(principal),
  manifests,
  storage: new MemoryStorage(),
  clock,
});

await engine.addKeyspace("demo");
engine.start();
await engine.settle();

await engine.submit({ keyspaceId: "demo", opType: "Set", changes: [set("settings/theme", "dark")] });
await engine.settle();

const v = engine.view("demo").get("settings/theme");
console.log(JSON.parse(new TextDecoder().decode(v.value)), v.pending.length);
await engine.stop();
```

It prints:

```
dark 0
```

(`0` is the number of pending actions on the key: the server confirmed the write.)

## Build and test

For contributors, in a clone of this repository. Node 22 or later.

```sh
npm ci
npm run typecheck
npm run build      # compiles src/ to dist/
npm test           # the fast unit tests
```

`npm test` runs in a few seconds. The heavy tests need a checkout of the ABCP server repo, which is private, so outside contributors can run the unit tests only. The heavy tests skip without one, naming the reason
"needs an abcp-server checkout (ABCP_SERVER_DIR); runs in the server's release checks":

- the conformance vectors (`conformance/vectors`),
- the CRDT test data (`server/internal/crdt/testdata`),
- the integration tests, which build the Go server from `server/` (they also need a Go toolchain on the `PATH`).

To run them, point `ABCP_SERVER_DIR` at that checkout:

```sh
ABCP_SERVER_DIR=../abcp-server npm test
```

`ABCP_SKIP_INTEGRATION=1` skips only the integration tests that run the Go server.

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
