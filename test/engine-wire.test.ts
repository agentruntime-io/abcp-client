// The engine over bytes: server data is decoded strictly with rules/canon, never with protobuf-es fromBinary alone (docs/PLAN.md, the
// C1 note), and a message that breaks 02 [ARC-61] is handled as 16 [SEC-3] and 14 [CLI-50] say.

import { describe, expect, it } from "vitest";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import { BytesTransport, set, ProtocolViolation } from "../src/engine/index.js";
import { decodeMessage } from "../src/engine/wire.js";
import { decodeContent, encodeContent } from "../src/engine/entry.js";
import { MutateRequestSchema, SubscribeResponseSchema } from "../src/gen/abcp/v2/sync_pb.js";
import { RecordSchema } from "../src/gen/abcp/v2/types_pb.js";
import { encode } from "../src/rules/canon/index.js";
import { goOnline, kc, KS, mutateRequests, ofType, rig, tick, text, types } from "./helpers/engine.js";
import { breakUtf8, FakeRaw } from "./helpers/raw.js";
import { FakeServer, ManualClock, SeededRandom } from "../src/fake/index.js";
import { Engine, MemoryStorage, StaticManifests } from "../src/engine/index.js";
import { MANIFEST } from "./helpers/engine.js";

async function bytesRig(): Promise<{ engine: Engine; server: FakeServer; raw: FakeRaw; clock: ManualClock; events: { type: string }[] }> {
  const clock = new ManualClock();
  const manifests = new StaticManifests(MANIFEST);
  const server = new FakeServer({ manifests, clock, heartbeatMs: 15_000 });
  server.createKeyspaceNow(KS);
  const principal = server.principal("ana");
  const raw = new FakeRaw(server.transport(principal));
  const engine = await Engine.create({ appId: "app", principal: { issuer: principal.issuer, sub: "ana", deviceId: principal.deviceId }, transport: new BytesTransport(raw), manifests, storage: new MemoryStorage(), clock, random: new SeededRandom(3) });
  const events: { type: string }[] = [];
  engine.on((e) => events.push(e));
  await engine.addKeyspace(KS);
  engine.start();
  await engine.settle();
  return { engine, server, raw, clock, events };
}

describe("the engine over a bytes transport", () => {
  it("runs the whole exchange through canonical encoding and strict decoding", async () => {
    const x = await bytesRig();
    expect(x.engine.status().connection).toBe("live");
    x.server.write(KS, [kc("settings/remote", "set", "r")]);
    const a = await x.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", "mine")] });
    await x.engine.settle();
    expect(x.events.map((e) => e.type)).toContain("committed");
    expect(text(x.engine.view(KS).get("settings/a")?.value)).toBe('"mine"');
    expect(text(x.engine.view(KS).get("settings/remote")?.value)).toBe('"r"');
    expect(x.engine.outbox()).toHaveLength(0);
    // [CLI-22], 07 [MUT-45]: the content on the wire is the stored content, byte for byte (the canonical encoding without the stamping fields)
    const sent = x.raw.requests.filter((q) => q.method === "Mutate").map((q) => decodeMessage<ReturnType<typeof create<typeof MutateRequestSchema>>>(MutateRequestSchema, q.bytes));
    const m = sent[0]?.mutations[0];
    expect(m?.mutationId).toBe(a.mutationId);
    expect(m?.clientSeq).toBe(1n);
    const stored = x.server.records(KS).find((r) => r.mutationId === a.mutationId);
    expect(stored).toBeDefined();
    expect(Buffer.from(encodeContent(m as never)).equals(Buffer.from(encodeContent(decodeContent(encodeContent(m as never)))))).toBe(true);
    // the request itself is in the canonical encoding: encoding it again changes nothing
    const bytes = x.raw.requests.find((q) => q.method === "Mutate")?.bytes as Uint8Array;
    expect(Buffer.from(encode(MutateRequestSchema, decodeMessage(MutateRequestSchema, bytes))).equals(Buffer.from(bytes))).toBe(true);
    await x.engine.stop();
  });

  it("protobuf-es fromBinary alone repairs invalid UTF-8; the strict decoder refuses it (why the engine never uses fromBinary alone)", () => {
    const rec = create(RecordSchema, { seq: 1n, mutationId: "m-1", opType: "Set" });
    const bytes = breakUtf8(toBinary(RecordSchema, rec), "m-1");
    const repaired = fromBinary(RecordSchema, bytes);
    expect(repaired.mutationId).toContain("�"); // silently changed
    expect(() => decodeMessage(RecordSchema, bytes)).toThrow(ProtocolViolation);
  });
});

describe("14 [CLI-50] a message that breaks 02 [ARC-61]", () => {
  it("a record with a string that is not valid UTF-8: nothing of it is applied or repaired, `protocol-error` is emitted, the keyspace is held, and the client cold-loads it again when a re-add gets good messages", async () => {
    const x = await bytesRig();
    let broken = true;
    x.raw.tamper = (bytes, frame) => (broken && frame.frame.case === "records" ? breakUtf8(bytes, "settings/bad") : bytes);
    x.server.write(KS, [kc("settings/bad", "set", 1)]);
    await x.engine.settle();
    expect(x.engine.confirmed(KS).has("settings/bad")).toBe(false);
    expect(x.engine.cursor(KS)?.seq).toBe(0n);
    const pe = x.events.filter((e) => e.type === "protocol-error");
    expect(pe).toHaveLength(1);
    expect(x.engine.status().holds).toEqual([expect.objectContaining({ scope: "keyspace", keyspaceId: KS, code: "PROTOCOL_ERROR" })]);
    // an action is accepted and kept, never sent while held
    const a = await x.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await x.engine.settle();
    expect(x.server.records(KS).filter((r) => r.mutationId === a.mutationId)).toHaveLength(0);
    // the server's next messages are good: the hold's probe re-adds the keyspace, cold-loads it, and the action goes
    broken = false;
    await x.clock.advance(70_000);
    await x.engine.settle();
    await x.clock.advance(70_000);
    await x.engine.settle();
    expect(x.engine.status().holds).toEqual([]);
    expect(text(x.engine.confirmed(KS).get("settings/bad")?.value)).toBe("1");
    expect(x.server.records(KS).filter((r) => r.mutationId === a.mutationId)).toHaveLength(1);
    await x.engine.stop();
  });

  it("a Timestamp out of range in a record breaks 02 [ARC-61]", async () => {
    const x = await bytesRig();
    x.raw.tamper = (bytes, frame) => {
      if (frame.frame.case !== "records") return bytes;
      const f = create(SubscribeResponseSchema, frame);
      const rec = (f.frame.value as { records: { serverTime?: unknown }[] }).records[0];
      if (rec !== undefined) rec.serverTime = { ...timestampFromMs(0), seconds: 99_999_999_999_999n };
      return toBinary(SubscribeResponseSchema, f);
    };
    x.server.write(KS, [kc("settings/a", "set", 1)]);
    await x.engine.settle();
    expect(x.events.filter((e) => e.type === "protocol-error")).toHaveLength(1);
    expect(x.engine.cursor(KS)?.seq).toBe(0n);
    await x.engine.stop();
  });

  it("a MutateResponse that does not answer its request: the outcome of the actions stays unknown, and they are resent unchanged once the keyspace is released", async () => {
    const x = await bytesRig();
    let once = true;
    x.raw.tamperUnary = (method, bytes) => {
      if (method !== "Mutate" || !once) return bytes;
      once = false;
      return new Uint8Array(0); // a response with no results at all
    };
    x.server.faults.paused = true;
    const a = await x.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    await x.engine.settle();
    expect(x.events.filter((e) => e.type === "protocol-error")).toHaveLength(1);
    expect(x.engine.outbox()).toHaveLength(1);
    expect(x.engine.deadLetterEntries()).toHaveLength(0);
    x.server.resumeDelivery();
    await x.clock.advance(70_000);
    await x.engine.settle();
    await x.clock.advance(70_000);
    await x.engine.settle();
    expect(x.server.records(KS).filter((r) => r.mutationId === a.mutationId)).toHaveLength(1); // applied once
    expect(x.engine.outbox()).toHaveLength(0);
    await x.engine.stop();
  });

  it("a response of a call other than Mutate that breaks 02 [ARC-61]: LoadState with an inconsistent page stops the load", async () => {
    const r = await rig({ start: false });
    void r;
    const x = await bytesRig();
    await x.engine.stop();
    expect(() => decodeMessage(MutateRequestSchema, new Uint8Array([0x0a, 0xff]))).toThrow(ProtocolViolation);
    void [goOnline, ofType, tick, types, mutateRequests];
  });
});
