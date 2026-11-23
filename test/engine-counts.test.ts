// 16 [SEC-1], [SEC-3], 09 [READ-10]: the counts of a received message are judged on its octets, before one element is decoded (V19 MUST-1).
// A LoadStateResponse of 16 MiB of empty KeyEntry decoded to 8.4 million objects: 2.2 GB of heap and 9.5 s. Now it is a protocol violation at the
// 10,001st entry, and a message that is under every count but would cost more than two slots of heap for each octet is one too.

import { create, type DescMessage } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { BytesTransport, ProtocolViolation } from "../src/engine/index.js";
import { BOUNDS, DEFAULT_PAGE_SIZE, MAX_UNKNOWN_PER_MESSAGE, pageBound } from "../src/engine/counts.js";
import { decodeMessage } from "../src/engine/wire.js";
import {
  AppendedSchema,
  GetMutationResultsResponseSchema,
  LoadStateRequestSchema,
  LoadStateResponseSchema,
  MutateRequestSchema,
  MutateResponseSchema,
  MutationSchema,
  RecordsFrameSchema,
  RefreshSessionResponseSchema,
  SubscribeResponseSchema,
  UpdateSubscriptionResponseSchema,
  HeartbeatFrameSchema,
  PresenceFrameSchema,
  ControlFrameSchema,
  CommitBlobResponseSchema,
  CreateUploadResponseSchema,
  DeleteBlobResponseSchema,
  GetBlobResponseSchema,
  JoinPresenceResponseSchema,
  LeavePresenceResponseSchema,
  PublishPresenceResponseSchema,
} from "../src/gen/abcp/v2/sync_pb.js";
import { KeyEntrySchema, RecordSchema, RejectSchema } from "../src/gen/abcp/v2/types_pb.js";
import { encode } from "../src/rules/canon/index.js";
import type { RawCaller } from "../src/engine/index.js";

/** `n` copies of an element of `bytes` octets, as the octets of a message (`1a 00` is an empty element of field 3). */
function repeat(element: number[], n: number): Uint8Array {
  const out = new Uint8Array(element.length * n);
  for (let i = 0; i < n; i++) out.set(element, i * element.length);
  return out;
}

function varint(n: number): number[] {
  const out: number[] = [];
  while (n >= 0x80) {
    out.push((n & 0x7f) | 0x80);
    n = Math.floor(n / 128);
  }
  out.push(n);
  return out;
}

/** A length-delimited field of `payload`. */
function field(no: number, payload: Uint8Array): Uint8Array {
  const head = [...varint(no * 8 + 2), ...varint(payload.length)];
  const out = new Uint8Array(head.length + payload.length);
  out.set(head);
  out.set(payload, head.length);
  return out;
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

function violation(schema: DescMessage, bytes: Uint8Array, limits?: Record<string, number>): ProtocolViolation {
  try {
    decodeMessage(schema, bytes, limits);
  } catch (e) {
    expect(e).toBeInstanceOf(ProtocolViolation);
    return e as ProtocolViolation;
  }
  throw new Error("the message was decoded");
}

const MiB = 1024 * 1024;

/** A change of a key of 8 octets (`issues/1`) and a kind: 14 octets, the smallest that a message of a real keyspace holds, whose decoded object costs 47 slots against the 56 of its own octets. */
const CHANGE = [0x5a, 0x0c, 0x0a, 0x08, 0x69, 0x73, 0x73, 0x75, 0x65, 0x73, 0x2f, 0x31, 0x10, 0x03];

/** `decodeMessage` with the type of the fields that a test reads. */
const dec = <T,>(schema: DescMessage, bytes: Uint8Array): T => decodeMessage(schema, bytes) as unknown as T;

describe("16 [SEC-1]: the counts of a received message are bounded on its octets", () => {
  for (const mib of [1, 4, 16]) {
    it(`a LoadStateResponse of ${mib} MiB of empty KeyEntry is refused before it is decoded (the V19 probe)`, () => {
      const bytes = repeat([0x1a, 0x00], (mib * MiB) / 2);
      const before = process.memoryUsage().heapUsed;
      const t0 = Date.now();
      const v = violation(LoadStateResponseSchema, bytes);
      const ms = Date.now() - t0;
      const heapMB = (process.memoryUsage().heapUsed - before) / 1e6;
      expect(v.message).toContain("LoadStateResponse.entries");
      expect(ms).toBeLessThan(1000); // the probe took 9.5 s for 16 MiB
      expect(heapMB).toBeLessThan(100); // and 2,225 MB of heap
    });
  }

  it("the same message in a Subscribe frame, an unknown element of a frame, a repeated map entry and a packed field is refused too", () => {
    const records = repeat([0x22, 0x00], 4 * MiB);
    expect(violation(SubscribeResponseSchema, field(1, records)).message).toContain("RecordsFrame.records");
    expect(violation(HeartbeatFrameSchema, repeat([0x0a, 0x00], 8 * MiB)).message).toContain("HeartbeatFrame.cursors");
    expect(violation(RejectSchema, repeat([0x42, 0x00], 4 * MiB)).message).toContain("Reject.details");
    // ids of one octet: the octets of the frame hold at most one id of a UUID's size for 38 of them
    expect(violation(PresenceFrameSchema, repeat([0x2a, 0x01, 0x61], 4 * MiB)).message).toContain("PresenceFrame.left_client_ids");
    // 16 MiB of one-octet varints of a packed field
    expect(violation(AppendedSchema, field(2, repeat([0x01], 16 * MiB))).message).toContain("Appended.overwrote_newer");
    // unknown fields are kept by the decoder, an object for each
    expect(violation(KeyEntrySchema, repeat([0xf8, 0x01, 0x00], 1 * MiB)).message).toContain("unknown fields");
  });

  it("every count of the text is the bound: one more than it is refused, the bound itself is decoded", () => {
    const entries = (n: number): Uint8Array => repeat([0x1a, 0x07, 0x0a, 0x03, 0x61, 0x2f, 0x62, 0x48, 0x01], n); // a key tombstone
    expect(dec<{ entries: unknown[] }>(LoadStateResponseSchema, entries(10_000)).entries).toHaveLength(10_000);
    violation(LoadStateResponseSchema, entries(10_001));
    expect(dec<{ redactions: unknown[] }>(LoadStateResponseSchema, repeat([0x82, 0x01, 0x00], 1000)).redactions).toHaveLength(1000);
    violation(LoadStateResponseSchema, repeat([0x82, 0x01, 0x00], 1001));
    expect(dec<{ successors: unknown[] }>(LoadStateResponseSchema, repeat([0x4a, 0x00], 1000)).successors).toHaveLength(1000);
    violation(LoadStateResponseSchema, repeat([0x4a, 0x00], 1001));
    expect(dec<{ cursors: unknown[] }>(HeartbeatFrameSchema, repeat([0x0a, 0x02, 0x10, 0x01], 10_000)).cursors).toHaveLength(10_000);
    violation(HeartbeatFrameSchema, repeat([0x0a, 0x02, 0x10, 0x01], 10_001));
    // a record's changes: the action's 2,000, and the row that goes with them
    const changes = (n: number): Uint8Array => repeat(CHANGE, n);
    expect(dec<{ changes: unknown[] }>(RecordSchema, changes(2002)).changes).toHaveLength(2002);
    violation(RecordSchema, changes(2003));
    // Appended.overwrote_newer: packed, indexes of the action's changes
    expect(dec<{ overwroteNewer: number[] }>(AppendedSchema, field(2, repeat([0x01], 2000))).overwroteNewer).toHaveLength(2000);
    violation(AppendedSchema, field(2, repeat([0x01], 2001)));
    // the same, not packed
    violation(AppendedSchema, repeat([0x10, 0x01], 2001));
  });

  it("a singular message field that occurs again is one message: its counts add up", () => {
    // LoadStateResponse.server_info (7) written twice with 200 features each: 400 > 256
    const half = repeat([0x1a, 0x01, 0x61], 200);
    violation(LoadStateResponseSchema, concat(field(7, half), field(7, half)));
    // the cursor of a KeyspaceAdded (ControlFrame 4, cursor 1) written twice with 600 prefixes each is one cursor of 1,200
    const prefixes = repeat([0x22, 0x01, 0x61], 600);
    const added = field(4, field(1, prefixes));
    violation(ControlFrameSchema, concat(added, added));
    expect(dec<{ control: { case: string } }>(ControlFrameSchema, field(4, field(1, repeat([0x22, 0x01, 0x61], 1000)))).control.case).toBe("keyspaceAdded");
  });

  it("the call fixes the counts that depend on the request: the page of a load, the results of a Mutate", async () => {
    const page = (n: number): Uint8Array => repeat([0x1a, 0x07, 0x0a, 0x03, 0x61, 0x2f, 0x62, 0x48, 0x01], n);
    expect(pageBound(0)).toBe(DEFAULT_PAGE_SIZE);
    expect(pageBound(1)).toBe(1);
    expect(pageBound(1_000_000)).toBe(10_000);
    const answers = new Map<string, Uint8Array>();
    const raw: RawCaller = { unary: async (m) => answers.get(m) as Uint8Array, stream: () => (async function* () {})() };
    const t = new BytesTransport(raw);
    const load = (pageSize: number) => t.loadState(create(LoadStateRequestSchema, { pageSize }));
    answers.set("LoadState", page(1000));
    expect((await load(0)).entries).toHaveLength(1000);
    answers.set("LoadState", page(1001));
    await expect(load(0)).rejects.toBeInstanceOf(ProtocolViolation); // 09 [READ-10]: 0 is the default, 1,000
    await expect(load(1000)).rejects.toBeInstanceOf(ProtocolViolation);
    expect((await load(5000)).entries).toHaveLength(1001);
    answers.set("LoadState", page(2));
    await expect(load(1)).rejects.toBeInstanceOf(ProtocolViolation);
    // one result for each mutation of the request
    const mutation = create(MutationSchema, { mutationId: "m" });
    const results = (n: number): Uint8Array => repeat([0x0a, 0x03, 0x0a, 0x01, 0x6d], n);
    answers.set("Mutate", results(2));
    const req = create(MutateRequestSchema, { mutations: [mutation, mutation] });
    expect((await t.mutate(req)).results).toHaveLength(2);
    answers.set("Mutate", results(3));
    await expect(t.mutate(req)).rejects.toBeInstanceOf(ProtocolViolation);
    void encode;
    void MutateResponseSchema;
  });

  it("a message under every count that costs more than two slots of heap for each octet is refused: the nesting of counts is no way round", () => {
    // 3,000 records of 2,000 empty changes each: every count holds, 6 million KeyChange objects in 12 MB
    const record = repeat([0x5a, 0x00], 2000);
    const frame = concat(...Array.from({ length: 3000 }, () => field(4, record)));
    const v = violation(RecordsFrameSchema, frame);
    expect(v.message).toContain("heap");
  });

  it("the least valid element of each type fits its own heap budget, so a valid message is never refused for it", () => {
    // a page of 10,000 key tombstones (the smallest entry that a load carries) and 2,000 one-octet-key deletions in a record
    const tomb = repeat([0x1a, 0x07, 0x0a, 0x03, 0x61, 0x2f, 0x62, 0x48, 0x01], 10_000);
    expect(dec<{ entries: unknown[] }>(LoadStateResponseSchema, tomb).entries).toHaveLength(10_000);
    const del = repeat(CHANGE, 2000);
    expect(dec<{ changes: unknown[] }>(RecordSchema, del).changes).toHaveLength(2000);
    // heartbeat cursors and lane watermarks of 10,000 keyspaces
    const cur = repeat([0x0a, 0x09, 0x0a, 0x03, 0x6b, 0x73, 0x31, 0x10, 0x05, 0x18, 0x06], 10_000);
    expect(dec<{ cursors: unknown[] }>(HeartbeatFrameSchema, cur).cursors).toHaveLength(10_000);
  });

  it("an unknown field that a later revision adds is decoded; too many are not", () => {
    const unknown = (n: number): Uint8Array => repeat([0xf8, 0x01, 0x07], n);
    expect(dec<{ key: string }>(KeyEntrySchema, concat(field(1, new TextEncoder().encode("k")), unknown(MAX_UNKNOWN_PER_MESSAGE))).key).toBe("k");
    violation(KeyEntrySchema, unknown(MAX_UNKNOWN_PER_MESSAGE + 1));
  });

  it("every repeated or map field that a client decodes has a bound of its own in the table", () => {
    const roots: DescMessage[] = [
      MutateResponseSchema,
      GetMutationResultsResponseSchema,
      LoadStateResponseSchema,
      UpdateSubscriptionResponseSchema,
      RefreshSessionResponseSchema,
      SubscribeResponseSchema,
      RecordsFrameSchema,
      HeartbeatFrameSchema,
      ControlFrameSchema,
      PresenceFrameSchema,
      RejectSchema,
      // 09 §9.8, §9.9: the answers of the presence and blob calls
      JoinPresenceResponseSchema,
      PublishPresenceResponseSchema,
      LeavePresenceResponseSchema,
      CreateUploadResponseSchema,
      CommitBlobResponseSchema,
      GetBlobResponseSchema,
      DeleteBlobResponseSchema,
    ];
    const seen = new Set<string>();
    const missing: string[] = [];
    const visit = (m: DescMessage): void => {
      if (seen.has(m.typeName)) return;
      seen.add(m.typeName);
      for (const f of m.fields) {
        if ((f.fieldKind === "list" || f.fieldKind === "map") && BOUNDS[`${m.typeName}.${f.name}`] === undefined) missing.push(`${m.typeName}.${f.name}`);
        if (f.fieldKind === "message") visit(f.message);
        if (f.fieldKind === "list" && f.listKind === "message") visit(f.message);
        if (f.fieldKind === "map" && f.mapKind === "message") visit(f.message);
      }
    };
    for (const r of roots) visit(r);
    expect(missing).toEqual([]);
    // and the table names no field that does not exist
    const names = new Set<string>();
    const all = (m: DescMessage, done = new Set<string>()): void => {
      if (done.has(m.typeName)) return;
      done.add(m.typeName);
      for (const f of m.fields) {
        names.add(`${m.typeName}.${f.name}`);
        if (f.fieldKind === "message") all(f.message, done);
        if (f.fieldKind === "list" && f.listKind === "message") all(f.message, done);
        if (f.fieldKind === "map" && f.mapKind === "message") all(f.message, done);
      }
    };
    for (const r of roots) all(r);
    for (const key of Object.keys(BOUNDS)) expect(names.has(key), key).toBe(true);
  });
});

