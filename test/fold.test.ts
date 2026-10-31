// The fold of served records (conformance/vectors/fold.json), under the input mapping of 17 [CONF-9] for a client engine
// (vector README, "The client engine: the input mapping of 17 [CONF-9]"): for every vector of kind `fold` whose
// `expect.records` start at seq 1 and run without a gap (172 at draft 13), the `served` form of each record, in the order of
// the list, is folded onto an empty replica, and the per-key state hashes of 05 [STATE-41] equal the entries of `expect.keys`.
//
// A client engine compares per-key hashes only (17 [CONF-9]). The replica of this test also keeps prefix versions, so it
// reaches the keyspace state hash and the metadata hash (05 [STATE-84]) too, and the test checks them: the metadata hash covers
// every tombstone, mark, override seq, reset seq and `changed_at` of the fold, which no per-key hash does. It also checks the
// values of the reserved `abpc.*` keys.

import { describe, expect, it } from "vitest";
import { toHex } from "../src/rules/bytes.js";
import { sha256 } from "../src/rules/crypto.js";
import { applyRecord, ReplicaState, StateValue } from "../src/rules/fold/index.js";
import { RecordSchema, type Record as RecordMessage } from "../src/gen/abpc/v2/types_pb.js";
import { fromVectorJson } from "./helpers/proto.js";
import { vectorRules } from "./helpers/rules.js";
import { loadVectors, type Vector } from "./helpers/vectors.js";

const all = loadVectors("fold.json");

interface ExpectedRecord {
  seq: number;
  served: unknown;
}

const records = (v: Vector): ExpectedRecord[] => (v.expect.records ?? []) as ExpectedRecord[];

/** True when the vector's records start at seq 1 and run without a gap: the vectors that 17 [CONF-9] feeds to a client engine. */
function contiguous(v: Vector): boolean {
  const rs = records(v);
  if (rs.length === 0 || (rs[0] as ExpectedRecord).seq !== 1) return false;
  return rs.every((r, i) => i === 0 || r.seq === (rs[i - 1] as ExpectedRecord).seq + 1);
}

const folds = all.filter((v) => v.kind === "fold");

describe("17 [CONF-9] input mapping on fold.json: per-key state hashes of the fold of the served records", () => {
  it("172 vectors are run on a client engine, as the README counts them at draft 13", () => {
    expect(folds.filter(contiguous).length).toBe(172);
  });

  for (const v of all) {
    const title = `${v.id}: ${v.title.slice(0, 110)}`;
    if (v.kind !== "fold") {
      // 17 [CONF-9]: the `server-json` cases have no `expect.records`; their answers are the server's JSON texts
      it.skip(`${title} (a server-json case: no expect.records to fold, 17 CONF-9)`, () => undefined);
      continue;
    }
    if (!contiguous(v)) {
      const rs = records(v);
      const why =
        rs.length === 0
          ? "no record in expect.records: its answers are verdicts of the model (17 CONF-9)"
          : `its records start at seq ${(rs[0] as ExpectedRecord).seq}, above 1: its first record needs a load of state that the vector does not carry (17 CONF-9)`;
      it.skip(`${title} (${why})`, () => undefined);
      continue;
    }
    it(title, () => {
      const rules = vectorRules(v);
      const st = new ReplicaState({ trackPrefixes: true });
      const rs = records(v);
      for (const r of rs) {
        const rec = fromVectorJson(RecordSchema, r.served) as RecordMessage;
        const res = applyRecord(st, rules, rec);
        expect(res.moved, `seq ${r.seq}`).toBe(true);
        expect(res.skipped, `seq ${r.seq}: STATE-38 skips nothing in a cold load of in-order records`).toBe(0);
      }
      expect(Number(st.seq)).toBe(rs.length);

      // the per-key state hashes: every present key, and no other (a key that the vector erased has no entry)
      const want = new Map((v.expect.keys as { key: string; hash: string }[]).map((k) => [k.key, k.hash]));
      const got = new Map(st.keyNames().map((key) => [key, toHex(st.keyStateHash(key) as Uint8Array)]));
      expect([...got.keys()].sort(), "present keys").toEqual([...want.keys()].sort());
      for (const [key, h] of want) expect(got.get(key), `state hash of "${key}"`).toBe(h);

      // the values of the reserved keys that hold a value: their text, or {len, sha256} above 4,096 octets
      const values = v.expect.values as Record<string, string | { len: number; sha256: string }>;
      let reserved = 0;
      for (const key of st.keyNames()) {
        const k = st.key(key);
        if (!key.startsWith("abpc.") || k === undefined || k.state !== StateValue) continue;
        reserved++;
        const w = values[key];
        expect(w, `reserved key "${key}" holds a value that the vector does not list`).toBeDefined();
        if (typeof w === "string") {
          expect(new TextDecoder().decode(k.value), key).toBe(w);
        } else {
          expect(k.value.length).toBe((w as { len: number }).len);
          expect(toHex(sha256(k.value))).toBe((w as { sha256: string }).sha256);
        }
      }
      expect(reserved, "reserved values").toBe(Object.keys(values).length);

      // beyond what a client compares: the server's keyspace state hash and metadata hash
      expect(toHex(st.keyspaceStateHash())).toBe(v.expect.keyspace_state_hash);
      expect(toHex(st.metadataHash())).toBe(v.expect.metadata_hash);
    });
  }
});
