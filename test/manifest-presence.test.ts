// C6: what the engine reads of a kind's presence section (04 [MAN-15], 09 [READ-72], [READ-83]): whether a topic is a Yjs awareness topic (a key of a `crdt:*` template of the kind, also when a
// presence topic template of the kind equals it) or a plain one (any other topic that matches a presence topic template of the kind), as the server judges it (Q73.1, draft 16).

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { StaticManifests } from "../src/engine/index.js";
import { GetManifestResponseSchema } from "../src/gen/abcp/v2/sync_pb.js";
import { readManifest } from "../src/net/index.js";
import { sha256 } from "../src/rules/crypto.js";
import { jsonv1 } from "../src/rules/index.js";

const DOC = {
  app: "tracker",
  keyspace_kinds: {
    room: { keys: ["note/**", "doc/**", "both/**"], op_types: ["A"], presence: { topics: ["note/{id}", "both/{id}"], state: { type: "object", fields: { cursor: { type: "string" } } } } },
    team: { keys: ["note/**", "doc/**"], op_types: ["A"] }, // no presence: no topic
  },
  keys: {
    "note/{id}": { merge: "lww" },
    "doc/{id}": { merge: "crdt:yjs/v1", crdt_roots: { t: "text" } },
    "both/{id}": { merge: "crdt:yjs/v1", crdt_roots: { t: "text" } },
  },
  op_types: { A: { dispatch: "SEQUENCED", writes: ["note/{id}"] } },
};

const manifest = jsonv1.canonicalDocument(new TextEncoder().encode(JSON.stringify(DOC)));
const parsed = readManifest(create(GetManifestResponseSchema, { version: 1, manifest, encoding: "json/v1", sha256: sha256(manifest) }), 1, "tracker");
const fixed = new StaticManifests({
  version: 1,
  templates: { "note/{id}": { rule: "lww" }, "doc/{id}": { rule: "crdt", roots: { t: "text" } }, "both/{id}": { rule: "crdt", roots: { t: "text" } } },
  presence: { room: ["note/{id}", "both/{id}"] },
}).get(1);

describe.each([
  ["the manifest that GetManifest serves", parsed],
  ["the static manifest of the tests", fixed],
])("09 [READ-72], [READ-83]: %s", (_name, m) => {
  it("a topic that matches a presence topic template of the kind is a plain topic", () => {
    expect(m?.presenceTopicKind?.("room", "note/n1")).toBe("plain");
  });
  it("a key of a crdt template of the kind that no presence topic template matches is an awareness topic", () => {
    expect(m?.presenceTopicKind?.("room", "doc/d1")).toBe("awareness");
  });
  it("a topic that is a key of a crdt template is an awareness topic also when a presence topic template of the kind equals it (09 [READ-72], 04 [MAN-15])", () => {
    expect(m?.presenceTopicKind?.("room", "both/b1")).toBe("awareness");
  });
  it("a topic that is neither, a kind that declares no presence and a kind that is unknown give none", () => {
    expect(m?.presenceTopicKind?.("room", "other/x")).toBeUndefined();
    expect(m?.presenceTopicKind?.("team", "doc/d1")).toBeUndefined();
    expect(m?.presenceTopicKind?.("nope", "note/n1")).toBeUndefined();
  });
});
