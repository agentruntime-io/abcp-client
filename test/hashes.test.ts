// State hashes and the manifest hash (conformance/vectors/hashes.json): 05 [STATE-41], [STATE-84], 04 [MAN-4]. Also the
// pure TypeScript SHA-256 against node:crypto, since every hash of the rules modules runs on it by default.

import { createHash, createHmac, randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";
import { fromHex, toHex } from "../src/rules/bytes.js";
import { hmacSha256, pureHashProvider, sha256Pure } from "../src/rules/crypto.js";
import { nodeHashProvider } from "../src/rules/node-crypto.js";
import {
  EntryKind,
  keyspaceStateHash,
  keyStateHash,
  keyStatePreimage,
  metadataHash,
  metadataK,
  metadataP,
  StateNull,
  StateRedacted,
  StateValue,
  type EntryKindValue,
  type KeyState,
  type KeyValueState,
  type MetaEntry,
} from "../src/rules/hashes/index.js";
import { canonicalDocument, manifestHash } from "../src/rules/jsonv1/index.js";
import { loadVectors, utf8 } from "./helpers/vectors.js";

const all = loadVectors("hashes.json");

describe("pure SHA-256 and HMAC-SHA-256 (the default hash provider)", () => {
  it("agree with node:crypto on messages of every length around the block boundaries", () => {
    for (let n = 0; n <= 200; n++) {
      const data = randomBytes(n);
      expect(toHex(sha256Pure(data))).toBe(createHash("sha256").update(data).digest("hex"));
      expect(toHex(nodeHashProvider.sha256(data))).toBe(toHex(pureHashProvider.sha256(data)));
    }
  });

  it("HMAC agrees with node:crypto for short, block-sized and long keys", () => {
    for (const keyLen of [0, 1, 32, 64, 65, 200]) {
      const key = randomBytes(keyLen);
      const data = randomBytes(77);
      expect(toHex(hmacSha256(key, data))).toBe(createHmac("sha256", key).update(data).digest("hex"));
    }
  });
});

const STATES: Record<string, KeyValueState> = { value: StateValue, null: StateNull, redacted: StateRedacted };

describe("05 [STATE-41] per-key and keyspace state hashes (state-hash vectors)", () => {
  for (const v of all.filter((x) => x.kind === "state-hash")) {
    it(`${v.id}: ${v.title}`, () => {
      const keys: KeyState[] = (v.keys as { key: string; rule: string; state: string; encoding: string; content?: string; version: number }[]).map(
        (k) => ({
          key: k.key,
          state: STATES[k.state] as KeyValueState,
          counter: k.rule === "counter",
          encoding: k.encoding,
          content: k.content !== undefined ? fromHex(k.content) : undefined,
          version: k.version,
        }),
      );
      // the vector's order is the order of ascending UTF-8 bytes of the keys
      const sorted = [...keys].sort((a, b) => Buffer.compare(Buffer.from(a.key), Buffer.from(b.key)));
      expect(sorted.map((k) => k.key)).toEqual(v.expect.order);
      for (const [i, pk] of (v.expect.per_key as { key: string; preimage: string; hash: string }[]).entries()) {
        const k = sorted[i] as KeyState;
        expect(k.key).toBe(pk.key);
        expect(toHex(keyStatePreimage(k))).toBe(pk.preimage);
        expect(toHex(keyStateHash(k))).toBe(pk.hash);
      }
      // the keys may come in any order
      expect(toHex(keyspaceStateHash(keys))).toBe(v.expect.keyspace_state_hash);
      expect(toHex(keyspaceStateHash(sorted))).toBe(v.expect.keyspace_state_hash);
    });
  }
});

const KINDS: Record<string, EntryKindValue> = {
  present: EntryKind.Present,
  key_tombstone: EntryKind.KeyTombstone,
  entity_tombstone: EntryKind.EntityTombstone,
  staged_tombstone: EntryKind.StagedTombstone,
};

describe("05 [STATE-84] metadata hash (metadata-hash vectors)", () => {
  for (const v of all.filter((x) => x.kind === "metadata-hash")) {
    it(`${v.id}: ${v.title}`, () => {
      const entries: MetaEntry[] = (
        v.entries as {
          key: string;
          kind: string;
          mark?: boolean;
          version: number;
          override_seq?: number;
          reset_seq?: number;
          changed_at?: { seconds: number; nanos: number };
        }[]
      ).map((e) => ({
        key: e.key,
        kind: KINDS[e.kind] as EntryKindValue,
        mark: e.mark,
        version: e.version,
        overrideSeq: e.override_seq,
        resetSeq: e.reset_seq,
        changedAt: e.changed_at,
      }));
      const prefixes = v.prefixes as { prefix: string; version: number }[];
      expect(toHex(metadataK(entries))).toBe(v.expect.K);
      expect(toHex(metadataP(prefixes))).toBe(v.expect.P);
      expect(toHex(metadataHash(entries, prefixes))).toBe(v.expect.metadata_hash);
    });
  }
});

describe("04 [MAN-4] manifest hash (manifest-hash vectors)", () => {
  for (const v of all.filter((x) => x.kind === "manifest-hash")) {
    it(`${v.id}: ${v.title.slice(0, 80)}`, () => {
      const doc = utf8(v.document as string);
      expect(new TextDecoder().decode(canonicalDocument(doc))).toBe(v.expect.canonical);
      expect(toHex(manifestHash(doc))).toBe(v.expect.sha256);
    });
  }
});
