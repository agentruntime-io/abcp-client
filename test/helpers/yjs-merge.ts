// A merge of Yjs v1 updates by first definition, as Yjs `mergeUpdates` does for updates that integrate. It serves the tests
// only: a compactor builds bases on the server, never a client. It is a port of server/internal/crdt/merge.go, and the tests
// compare it with the bytes that Yjs JS built (server/internal/crdt/testdata/library-bases.json).

import { ByteWriter } from "../../src/rules/bytes.js";
import { normalizeRanges, parseUpdate, type Range } from "../../src/rules/crdt/index.js";

export class NotMergeable extends Error {}

export function mergeUpdates(updates: readonly Uint8Array[]): Uint8Array {
  type Piece = { raw?: Uint8Array; gc: boolean; length: number };
  const runs = new Map<number, { next: number; pieces: Piece[] }>();
  const deletes = new Map<number, Range[]>();
  updates.forEach((b, ui) => {
    const u = parseUpdate(b);
    for (const g of u.groups) {
      let r = runs.get(g.client);
      if (r === undefined) {
        r = { next: 0, pieces: [] };
        runs.set(g.client, r);
      }
      for (const s of g.structs) {
        if (s.kind === "skip") throw new NotMergeable(`update ${ui} holds a Skip`);
        if (s.clock + s.len <= r.next) continue; // wholly below the merged structs: the first definition wins
        if (s.clock !== r.next) throw new NotMergeable(`update ${ui}: structs of client ${g.client} at clock ${s.clock}, next clock is ${r.next}`);
        const last = r.pieces[r.pieces.length - 1];
        if (s.kind === "gc") {
          if (last !== undefined && last.gc) last.length += s.len;
          else r.pieces.push({ gc: true, length: s.len });
        } else {
          r.pieces.push({ raw: u.data.subarray(s.off, s.end), gc: false, length: s.len });
        }
        r.next += s.len;
      }
    }
    for (const dg of u.deletes) {
      const cur = deletes.get(dg.client) ?? [];
      deletes.set(dg.client, [...cur, ...dg.ranges]);
    }
  });

  const w = new ByteWriter();
  const clients = [...runs.keys()].sort((a, b) => b - a);
  w.varint(clients.length);
  for (const c of clients) {
    const r = runs.get(c) as { next: number; pieces: Piece[] };
    w.varint(r.pieces.length).varint(c).varint(0); // the first struct of a client is at clock 0
    for (const p of r.pieces) {
      if (p.gc) w.u8(0).varint(p.length);
      else w.bytes(p.raw as Uint8Array);
    }
  }
  const dsClients = [...deletes.keys()].sort((a, b) => b - a);
  w.varint(dsClients.length);
  for (const c of dsClients) {
    const rs = normalizeRanges(deletes.get(c) as Range[]);
    w.varint(c).varint(rs.length);
    for (const r of rs) w.varint(r.clock).varint(r.len);
  }
  return w.finish();
}
