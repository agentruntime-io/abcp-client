// The ordered index of the state's names (05 [STATE-17], [STATE-43], [STATE-45], [STATE-51]; the twin of server/internal/state's ordered
// indexes, V1 MUST-3): a subtree is a range, so a deletion costs the subtree and not the keyspace. The first test is a differential
// test against a model that scans every name, as the state did before; the second is the cost test of server/internal/fold/v1_test.go.

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { ChangeKind, KeyChangeSchema, RecordKind, RecordSchema } from "../src/gen/abpc/v2/types_pb.js";
import { applyRecord, inSubtree, ReplicaState, StateValue, VersionTopology, type KeyRow, type Tombstone } from "../src/rules/fold/index.js";
import { compareUtf8 } from "../src/rules/bytes.js";
import { VectorRules } from "./helpers/rules.js";

const rules = new VectorRules();
rules.addManifest({
  version: 1,
  templates: {
    "issue/{i}": { rule: "cas", entity: true },
    "issue/{i}/title": { rule: "lww" },
    "issue/{i}/comment/{c}": { rule: "cas", entity: true },
    "issue/{i}/comment/{c}/body": { rule: "lww" },
    "v/{k}": { rule: "lww" },
  },
});
const top = new VersionTopology(rules, 1);

const row = (version: bigint): KeyRow => ({
  state: StateValue,
  rule: "lww",
  encoding: "json/v1",
  value: new Uint8Array(0),
  doc: null,
  version,
  crdtCoveredSeq: 0n,
  changedAt: { seconds: 0n, nanos: 0 },
  changedBy: "",
  overrideSeq: 0n,
  resetSeq: 0n,
});

/** The state as three plain collections, with every question answered by a scan of all the names (the state before its index). */
class Model {
  keys = new Set<string>();
  tombs = new Map<string, Tombstone>();
  prefixes = new Map<string, bigint>();

  putKey(k: string): void {
    this.keys.add(k);
    this.tombs.delete(k);
  }
  putTomb(k: string, t: Tombstone): void {
    this.keys.delete(k);
    this.tombs.set(k, t);
  }
  dropKey(k: string): void {
    this.keys.delete(k);
    this.tombs.delete(k);
  }
  touch(key: string, seq: bigint): void {
    for (let i = 0; i < key.length; i++) if (key[i] === "/") this.prefixes.set(key.slice(0, i + 1), seq);
  }
  nested(root: string): string[] {
    const out: string[] = [];
    const visit = (k: string): void => {
      if (k.startsWith(`${root}/`) && top.isMarker(k) && inSubtree(top, root, k)) out.push(k);
    };
    for (const k of this.keys) visit(k);
    for (const k of this.tombs.keys()) if (!this.keys.has(k)) visit(k);
    return out.sort(compareUtf8);
  }
  deleteSubtree(root: string, seq: bigint, nested: string[]): void {
    const prefix = `${root}/`;
    for (const k of [...this.keys]) if (k.startsWith(prefix) && inSubtree(top, root, k)) this.keys.delete(k);
    for (const k of [...this.tombs.keys()]) if (k.startsWith(prefix) && inSubtree(top, root, k)) this.tombs.delete(k);
    this.keys.delete(root);
    const t: Tombstone = { kind: "entity", version: seq, mark: false, overrideSeq: 0n, resetSeq: 0n };
    this.tombs.set(root, t);
    for (const n of nested) if (n !== root) this.tombs.set(n, { ...t });
    this.prefixes.set(prefix, seq);
    for (const p of [...this.prefixes.keys()]) if (p.startsWith(prefix)) this.prefixes.set(p, seq);
  }
  discard(floor: bigint): void {
    for (const [k, t] of [...this.tombs]) if (t.version <= floor) this.tombs.delete(k);
    const names = [...this.keys, ...[...this.tombs.keys()].filter((k) => !this.keys.has(k))];
    const covered = (p: string): boolean => {
      for (let i = 0; i < p.length; i++) {
        if (p[i] !== "/") continue;
        const t = this.tombs.get(p.slice(0, i));
        if (t !== undefined && (t.kind === "entity" || t.kind === "staged")) return true;
      }
      return false;
    };
    for (const [p, v] of [...this.prefixes]) {
      if (v > floor || names.some((n) => n.startsWith(p)) || covered(p)) continue;
      this.prefixes.delete(p);
    }
  }
}

// a small alphabet, so that names collide, leave and come back: the cases of a lazy index
const NAMES: string[] = [];
for (const i of ["a", "b", "ab"]) {
  NAMES.push(`issue/${i}`, `issue/${i}/title`);
  for (const c of ["c1", "c2"]) NAMES.push(`issue/${i}/comment/${c}`, `issue/${i}/comment/${c}/body`);
}
for (const k of ["k1", "k2", "k3"]) NAMES.push(`v/${k}`);
NAMES.push("issue/a0", "issue/a0/title", "zzz/none");
const ROOTS = ["issue/a", "issue/b", "issue/ab", "issue/a/comment/c1", "issue/b/comment/c2", "issue/zz", "issue/a0"];

describe("05 [STATE-17], [STATE-43], [STATE-45], [STATE-51]: the ordered index agrees with a scan of every name", () => {
  it("random writes, subtree deletions and discards: the listings, the tombstones, the prefix versions and the nested markers of every root", () => {
    let seed = 20240607;
    const rand = (n: number): number => {
      seed = (Math.imul(seed, 1664525) + 1013904223) | 0;
      return (seed >>> 8) % n;
    };
    const st = new ReplicaState({ trackPrefixes: true });
    const m = new Model();
    let seq = 1n;
    const pick = <T>(xs: readonly T[]): T => xs[rand(xs.length)] as T;
    for (let step = 0; step < 6000; step++) {
      seq += BigInt(rand(2));
      const name = pick(NAMES);
      switch (rand(10)) {
        case 0:
        case 1:
        case 2:
          st.putKey(name, row(seq));
          m.putKey(name);
          break;
        case 3: {
          const t: Tombstone = { kind: "key", version: seq, mark: false, overrideSeq: 0n, resetSeq: 0n };
          st.putTombstone(name, t);
          m.putTomb(name, { ...t });
          break;
        }
        case 4:
          st.deleteKey(name, seq, false);
          m.putTomb(name, { kind: "key", version: seq, mark: false, overrideSeq: 0n, resetSeq: 0n });
          break;
        case 5:
          st.dropKey(name);
          m.dropKey(name);
          break;
        case 6:
          st.touchPrefixes(name, seq);
          m.touch(name, seq);
          break;
        case 7: {
          const root = pick(ROOTS);
          const nested = st.nestedMarkers(top, root);
          expect(nested, `nested markers of ${root} at step ${step}`).toEqual(m.nested(root));
          st.deleteSubtree(top, seq, { root, markRoot: false, staged: false, nested: nested.map((key) => ({ key, mark: false })) });
          m.deleteSubtree(root, seq, nested);
          break;
        }
        case 8: {
          if (rand(4) !== 0) break;
          const floor = BigInt(rand(Number(seq) + 1));
          st.discardTombstones(floor);
          m.discard(floor);
          break;
        }
        default: {
          const p = `${name.split("/").slice(0, 1 + rand(3)).join("/")}/`;
          const v = BigInt(rand(3)) === 0n ? 0n : seq;
          st.setPrefixVersion(p, v);
          if (v === 0n) m.prefixes.delete(p);
          else m.prefixes.set(p, v);
        }
      }
      if (step % 7 === 0 || step > 5900) {
        expect(st.keyNames(), `present keys at step ${step}`).toEqual([...m.keys].sort(compareUtf8));
        expect(st.tombstoneKeys(), `tombstones at step ${step}`).toEqual([...m.tombs.keys()].sort(compareUtf8));
        for (const [k, t] of m.tombs) expect(st.tombstone(k), k).toMatchObject({ kind: t.kind, version: t.version });
        expect(
          new Map(st.prefixVersions().map((p) => [p.prefix, p.version])),
          `prefix versions at step ${step}`,
        ).toEqual(m.prefixes);
        for (const root of ROOTS) expect(st.nestedMarkers(top, root), `nested markers of ${root} at step ${step}`).toEqual(m.nested(root));
      }
    }
  });
});

describe("05 [STATE-17] (V1 MUST-3): a DELETE of an absent marker key costs the subtree, not the keyspace", () => {
  it("1,000 marker DELETEs in one record on a keyspace of 200,000 keys", () => {
    // before the index each DELETE read every key and tombstone twice: 3.6 s for 1,000 of them on 300,000 keys in Go
    const st = new ReplicaState();
    const n = 200_000;
    for (let i = 0; i < n; i++) st.putKey(`v/k${String(i).padStart(7, "0")}`, row(1n));
    st.setCursor(1n, 1, 1);
    const changes = Array.from({ length: 1000 }, (_, i) => create(KeyChangeSchema, { key: `issue/x${String(i).padStart(4, "0")}`, kind: ChangeKind.DELETE }));
    const rec = create(RecordSchema, {
      seq: 2n,
      logEpoch: 1,
      kind: RecordKind.OP,
      manifestVersion: 1,
      serverTime: { seconds: 1000n, nanos: 0 },
      changes,
    });
    const start = performance.now();
    applyRecord(st, rules, rec);
    const elapsed = performance.now() - start;
    expect(st.tombstoneKeys()).toHaveLength(1000);
    expect(st.size).toBe(n);
    expect(elapsed, `${Math.round(elapsed)} ms: the cost grows with the keyspace`).toBeLessThan(1000);
  });
});
