// The local query layer (C4): a differential test of the live queries. Random sequences of what can happen to a keyspace's copy (records from other principals, pending
// actions of the app, their commit and cancel, going offline and online, a change of the interest, moved keys) are applied to an engine that several live queries watch, and
// after every step each live query's snapshot, which was kept by incremental updates since the start, must equal what a query layer that starts from nothing answers now
// (14 [CLI-6]: the view is the state and the prediction as they are now). Every update must also add up: the rows of the updates since the first, applied to the first, are the
// snapshot.

import { describe, expect, it } from "vitest";
import { createQueries, type Query, type QueryResult, type Row, type RowValue } from "../src/query/index.js";
import { del, set } from "../src/engine/index.js";
import { goOffline, goOnline } from "./helpers/engine.js";
import { drop, KS, put, qrig, watch, type QRig, type Watch } from "./helpers/query.js";

function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const ser = (v: RowValue): string => {
  switch (v.kind) {
    case "json":
      return `j:${JSON.stringify(v.value, (_k, x: unknown) => (typeof x === "bigint" ? `${x}n` : x))}`;
    case "counter":
      return `c:${v.value}`;
    case "bytes":
      return `b:${[...v.value].join(",")}`;
    case "crdt":
      return "crdt";
    case "moved":
      return `m:${v.status}:${v.sagaId}:${v.to?.key}:${v.failed}:${v.run?.status}`;
    case "undecodable":
      return `u:${v.encoding}`;
    default:
      return v.kind;
  }
};

const plainRow = (x: Row): unknown => ({ k: x.key, b: x.bindings, v: String(x.version), p: x.pending, pr: x.predicted, val: ser(x.value), sub: x.subtree?.map(plainRow) });
const plain = (res: QueryResult): unknown => ({ rows: res.rows.map(plainRow), next: res.next, notLoaded: res.notLoaded, coverage: res.coverage, moves: res.movesTracked, status: res.keyspace.status, interest: res.keyspace.interest });

const ISSUES = ["1", "2", "3"];
const COMMENTS = ["1", "2"];
const POOL: string[] = [];
for (const i of ISSUES) {
  POOL.push(`issue/${i}`, `issue/${i}/title`, `issue/${i}/meta`);
  for (const c of COMMENTS) POOL.push(`issue/${i}/comment/${c}`, `issue/${i}/comment/${c}/text`);
}
for (const x of "abcdefg") POOL.push(`item/${x}`);

const QUERIES: Omit<Query, "keyspaceId">[] = [
  { select: { prefix: "issue/" } },
  { select: { prefix: "issue/2/" }, order: "desc" },
  { select: { template: "issue/{i}/title" } },
  { select: { template: "issue/{i}/comment/{c}/text" } },
  { select: { template: "issue/{i}/title", bind: { i: "2" } } },
  { select: { entities: "issue/{i}" } },
  { select: { entities: "issue/{i}", subtree: true } },
  { select: { entities: "issue/{i}/comment/{c}", subtree: true }, order: "desc" },
  { select: { entities: "issue/{i}", subtree: true }, filter: { at: "title", op: "exists" } },
  { select: { keys: ["issue/1", "issue/2/title", "item/a", "item/zz", "issue/3/comment/1"] } },
  { select: { prefix: "item/" }, filter: { path: "n", op: "gte", value: 3 } },
  { select: { prefix: "item/" }, limit: 3 },
  { select: { prefix: "item/" }, limit: 2, order: "desc" },
  { select: { prefix: "item/" }, limit: 2, after: "item/b" },
  { select: { entities: "issue/{i}" }, limit: 2 },
  { select: { prefix: "" }, mode: "confirmed" },
  { select: { template: "issue/{i}/title" }, mode: "confirmed", filter: { path: "n", op: "lt", value: 4 } },
  { select: { entities: "issue/{i}", subtree: true }, mode: "confirmed" },
];

const INTERESTS: string[][] = [[], ["issue"], ["item"], ["issue", "item"], ["issue/1", "item"], ["issue", "abcp.saga.v1"]];

/** More seeds and steps for a long run: `QUERY_RANDOM_SEEDS=200 npx vitest run test/query-random.test.ts`. */
const SEEDS = Number(process.env["QUERY_RANDOM_SEEDS"] ?? 12);
const STEPS = Number(process.env["QUERY_RANDOM_STEPS"] ?? 120);

describe("live queries kept by updates equal queries that start from nothing, under random histories", () => {
  for (let seed = 1; seed <= SEEDS; seed++) {
    it(`seed ${seed}`, async () => {
      const rand = rng(seed * 7919);
      const pick = <T>(xs: readonly T[]): T => xs[Math.floor(rand() * xs.length)] as T;
      const r: QRig = await qrig({ seed });
      /** A live query, with the rows that its updates add up to. */
      interface Tracked {
        q: Omit<Query, "keyspaceId">;
        w: Watch;
        rows: Map<string, string>;
        applied: number;
      }
      const watches: Tracked[] = [];
      const absorb = (t: Tracked): void => {
        for (; t.applied < t.w.updates.length; t.applied++) {
          const u = t.w.updates[t.applied] as Watch["updates"][number];
          for (const x of u.removed) expect(t.rows.delete(x.key), `seed ${seed}: removed ${x.key} that the updates did not hold`).toBe(true);
          for (const x of u.added) {
            expect(t.rows.has(x.key), `seed ${seed}: added ${x.key} twice`).toBe(false);
            t.rows.set(x.key, JSON.stringify(plainRow(x)));
          }
          for (const x of u.changed) {
            expect(t.rows.get(x.before.key), `seed ${seed}: changed ${x.before.key}: its before is not what the updates held`).toBe(JSON.stringify(plainRow(x.before)));
            t.rows.set(x.after.key, JSON.stringify(plainRow(x.after)));
          }
        }
      };
      for (const q of QUERIES) watches.push({ q, w: watch(r, { keyspaceId: KS, ...q } as Query), rows: new Map(), applied: 0 });

      const log: string[] = [];
      const check = async (step: number): Promise<void> => {
        await r.engine.settle();
        const fresh = createQueries(r.engine);
        try {
          for (const t of watches) {
            absorb(t);
            const query = { keyspaceId: KS, ...t.q } as Query;
            const want = plain(fresh.run(query));
            const got = plain(t.w.live.snapshot());
            expect(got, `seed ${seed}, step ${step}, after [${log.slice(-6).join(" | ")}], query ${JSON.stringify(t.q)}`).toEqual(want);
            // the updates add up to the snapshot
            const snapRows = new Map(t.w.live.snapshot().rows.map((x) => [x.key, JSON.stringify(plainRow(x))]));
            expect([...t.rows.entries()].sort(), `seed ${seed}, step ${step}: the updates do not add up, query ${JSON.stringify(t.q)}`).toEqual([...snapRows.entries()].sort());
          }
        } finally {
          fresh.close();
        }
      };

      let offline = false;
      for (let step = 0; step < STEPS; step++) {
        const x = rand();
        if (x < 0.34) {
          const keys = new Set<string>();
          const n = 1 + Math.floor(rand() * 3);
          for (let i = 0; i < n; i++) keys.add(pick(POOL));
          put(r, Object.fromEntries([...keys].map((k) => [k, { n: Math.floor(rand() * 7), t: pick(["a", "b"]) }])));
          log.push(`put ${[...keys].join(",")}`);
        } else if (x < 0.46) {
          const k = pick(POOL);
          drop(r, k);
          log.push(`drop ${k}`);
        } else if (x < 0.54) {
          const m = pick(ISSUES.map((i) => `issue/${i}`));
          const status = pick(["moving", "moved", "importing", "purged", "active", undefined]);
          if (status === undefined) drop(r, `abcp.saga.v1/moved/${m}`);
          else put(r, { [`abcp.saga.v1/moved/${m}`]: { status, saga_id: "s1", saga: "MoveIssue", to: { keyspace_id: "k2", key: m } } });
          log.push(`moved ${m} ${String(status)}`);
        } else if (x < 0.58) {
          const status = pick(["running", "failed", undefined]);
          if (status === undefined) drop(r, "abcp.saga.v1/run/s1");
          else put(r, { "abcp.saga.v1/run/s1": { status } });
          log.push(`run ${String(status)}`);
        } else if (x < 0.74) {
          const k = pick(POOL);
          const mode = rand();
          const changes = mode < 0.7 ? [set(k, { n: Math.floor(rand() * 7), t: "local" })] : [del(k)];
          await r.engine.submit({ keyspaceId: KS, opType: "Set", changes }).catch(() => undefined);
          log.push(`submit ${mode < 0.7 ? "set" : "del"} ${k}`);
        } else if (x < 0.8) {
          const pending = r.engine.outbox().filter((e) => e.outcome === undefined);
          const e = pending.length > 0 ? pick(pending) : undefined;
          if (e !== undefined) await r.engine.cancel(e.mutationId).catch(() => undefined);
          log.push(`cancel ${e?.mutationId.slice(0, 4) ?? "-"}`);
        } else if (x < 0.9) {
          if (offline) await goOnline(r);
          else await goOffline(r);
          offline = !offline;
          log.push(offline ? "offline" : "online");
        } else if (x < 0.945) {
          const interest = pick(INTERESTS);
          await r.engine.setInterest(KS, interest).catch(() => undefined);
          log.push(`interest ${interest.join("+") || "all"}`);
        } else if (x < 0.965) {
          // a restore of the keyspace to an earlier seq: the epoch changes, and the copy is loaded again (14 [CLI-51])
          const head = r.server.head(KS);
          const base = BigInt(Math.floor(rand() * Number(head)));
          r.server.restore(KS, base);
          log.push(`restore ${base}`);
        } else {
          await r.clock.advance(rand() * 2000);
          log.push("tick");
        }
        await check(step);
      }
      if (offline) await goOnline(r);
      await check(999);
    });
  }
});
