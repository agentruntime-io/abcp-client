// The fake server's `redact` (src/fake/server.ts), which drives the client suites of 14 [CLI-76]: it answers as 06 [LOG-44], [LOG-76] and 12 [ADM-62] to [ADM-66] say on the points that a client's
// behaviour depends on, and lists the redactions in the load that answers a resume check (09 [READ-100]).

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { jsonBytes } from "../src/engine/index.js";
import { FakeServer, ManualClock } from "../src/fake/index.js";
import { StaticManifests } from "../src/engine/index.js";
import { LoadStateRequestSchema } from "../src/gen/abcp/v2/sync_pb.js";
import { ChangeKind, KeyChangeSchema, KeyEntrySchema, KeyspaceCursorSchema, RecordKind, SystemKind } from "../src/gen/abcp/v2/types_pb.js";
import { kc, KS, MANIFEST } from "./helpers/engine.js";

function world(): { server: FakeServer; load: (afterSeq?: bigint, epoch?: number) => Promise<ReturnType<FakeServer["transport"]> extends { loadState(...a: never[]): Promise<infer R> } ? R : never> } {
  const server = new FakeServer({ manifests: new StaticManifests(MANIFEST), clock: new ManualClock(), heartbeatMs: 0 });
  server.createKeyspaceNow(KS);
  const t = server.transport(server.principal("ana"));
  return {
    server,
    load: (afterSeq = 0n, epoch = 1) => t.loadState(create(LoadStateRequestSchema, { cursor: create(KeyspaceCursorSchema, { keyspaceId: KS, afterSeq, logEpoch: epoch }), pageSize: 0 })) as never,
  };
}

const body = (rec: { system?: { body: Uint8Array } }): { redaction_id: string; keys: string[]; erased_keys?: string[]; min_seq: number; refused_keys?: string[] } => JSON.parse(new TextDecoder().decode(rec.system?.body));

describe("FakeServer.redact (06 [LOG-44], [LOG-76])", () => {
  it("blanks every earlier change and `previous` entry of an erased key, appends one REDACTED record that deletes the present keys and lists the others, and leaves the rest as it was", () => {
    const { server } = world();
    server.write(KS, [kc("settings/a", "set", "A1"), kc("settings/b", "set", "B1")]); // 1
    server.write(KS, [kc("settings/a", "set", "A2")], { previous: [create(KeyEntrySchema, { key: "settings/a", value: jsonBytes("A1"), encoding: "json/v1", version: 1n, changedBy: "x" })] }); // 2
    server.write(KS, [kc("settings/gone", "set", "G")]); // 3
    server.write(KS, [kc("settings/gone", "delete")]); // 4
    const seq = server.redact(KS, ["settings/a", "settings/gone", "settings/never"]);
    const log = server.records(KS);
    const rec = log.at(-1);
    expect(rec?.seq).toBe(seq);
    expect(rec?.kind).toBe(RecordKind.SYSTEM);
    expect(rec?.system?.kind).toBe(SystemKind.REDACTED);
    expect(rec?.system?.mustUnderstand).toBe(true);
    const b = body(rec as never);
    expect(b.keys).toEqual(["settings/a", "settings/gone"]); // `gone` is tombstoned: the tombstone alone makes it redacted
    expect(b.erased_keys).toEqual(["settings/never"]);
    expect(b.min_seq).toBe(1);
    expect(b.refused_keys).toBeUndefined();
    expect(rec?.changes.map((c) => [c.key, c.kind])).toEqual([["settings/a", ChangeKind.DELETE]]); // the DELETEs of the redacted keys that were present
    // the earlier records: the erased keys' changes blanked, the others untouched
    const r1 = log[0];
    expect(r1?.changes[0]).toMatchObject({ key: "settings/a", encoding: "redacted/v1", isNull: false });
    expect(r1?.changes[0]?.value.length).toBe(0);
    expect(r1?.changes[1]?.encoding).toBe("json/v1");
    expect(new TextDecoder().decode(r1?.changes[1]?.value)).toBe('"B1"');
    expect(log[1]?.previous[0]).toMatchObject({ key: "settings/a", encoding: "redacted/v1", changedBy: "", version: 1n });
    expect(log[1]?.previous[0]?.value.length).toBe(0);
    expect(log[2]?.changes[0]).toMatchObject({ key: "settings/gone", encoding: "redacted/v1" });
    expect(log[3]?.changes[0]).toMatchObject({ kind: ChangeKind.DELETE, encoding: "" }); // a DELETE stays a DELETE
    // the server's own state: the erased key is absent, with a marked tombstone and a reset seq (05 [STATE-37], [STATE-85])
    expect(server.stateOf(KS).key("settings/a")).toBeUndefined();
    expect(server.stateOf(KS).tombstone("settings/a")?.mark).toBe(true);
    expect(server.stateOf(KS).key("settings/b")).toBeDefined();
  });

  it("a refused key (12 [ADM-63]) is not erased: its changes stay, it is in `refused_keys` and nowhere else", () => {
    const { server } = world();
    server.write(KS, [kc("settings/a", "set", "A1"), kc("settings/held", "set", "H1")]);
    server.redact(KS, ["settings/a", "settings/held"], { refused: ["settings/held"] });
    const rec = server.records(KS).at(-1);
    const b = body(rec as never);
    expect(b.keys).toEqual(["settings/a"]);
    expect(b.refused_keys).toEqual(["settings/held"]);
    expect(new TextDecoder().decode(server.records(KS)[0]?.changes[1]?.value)).toBe('"H1"');
    expect(server.stateOf(KS).key("settings/held")).toBeDefined();
  });

  it("a run key whose start document holds a change of an erased key gets a server-written SET with the change blanked (12 [ADM-65], 11 [COORD-58]); the module's members stay", () => {
    const { server } = world();
    const RUN = "abcp.saga.v1/run/s1";
    const step = (key: string): unknown => ({ name: "s", base_seq: 0, base_log_epoch: 1, changes: [{ key, kind: "CHANGE_KIND_SET", value: "U0VDUkVU", encoding: "json/v1", seenVersion: "2" }], compensate: [{ key, kind: "CHANGE_KIND_DELETE" }] });
    server.write(KS, [create(KeyChangeSchema, { key: RUN, kind: ChangeKind.SET, encoding: "json/v1", value: jsonBytes({ saga: "S", steps: [step("settings/a"), step("settings/b")], status: "running" }) })]);
    server.redact(KS, ["settings/a"]);
    const rec = server.records(KS).at(-1);
    expect(rec?.changes.map((c) => [c.key, c.kind])).toEqual([[RUN, ChangeKind.SET]]);
    const run = JSON.parse(new TextDecoder().decode(server.value(KS, RUN))) as { status: string; steps: { changes: Record<string, unknown>[]; compensate: Record<string, unknown>[] }[] };
    expect(run.status).toBe("running");
    expect(run.steps[0]?.changes[0]).toEqual({ key: "settings/a", kind: "CHANGE_KIND_SET", seenVersion: "2", redacted: true });
    expect(run.steps[0]?.compensate[0]).toEqual({ key: "settings/a", kind: "CHANGE_KIND_DELETE" }); // a DELETE holds no member that counts: never blanked
    expect(run.steps[1]?.changes[0]?.["value"]).toBe("U0VDUkVU"); // another key's change
    // the earlier record that set the run key is blanked as any value change is
    expect(server.records(KS)[0]?.changes[0]).toMatchObject({ key: RUN, encoding: "redacted/v1" });
  });

  it("a cold load that answers a resume check lists the redactions newer than the cursor, once, in order; a cold load at after_seq 0, and a redaction at or below the cursor, list none (09 [READ-100])", async () => {
    const { server, load } = world();
    server.write(KS, [kc("settings/a", "set", 1), kc("settings/b", "set", 1)]); // 1
    const first = server.redact(KS, ["settings/a"]); // 2
    server.write(KS, [kc("settings/c", "set", 1)]); // 3
    const second = server.redact(KS, ["settings/c", "settings/b"]); // 4
    server.prune(KS, 4n, { judgeHorizon: false });
    // the cursor 1 is below the floor: a cold load, with the redactions above 1
    const resp = await load(1n);
    expect(resp.resumed).toBe(false);
    expect(resp.redactions.map((r) => [r.key, r.redactionSeq, r.logEpoch])).toEqual([
      ["settings/a", first, 1],
      ["settings/b", second, 1],
      ["settings/c", second, 1],
    ]);
    // a cursor at the first redaction lists only the later one
    const later = await load(first);
    expect(later.redactions.map((r) => r.key)).toEqual(["settings/b", "settings/c"]);
    expect((await load(0n)).redactions).toEqual([]); // after_seq = 0: a cold load that is no resume check
    // a resume that succeeds carries none: the tail delivers the records
    server.prune(KS, 0n, { judgeHorizon: false });
    const resumed = await load(4n);
    expect(resumed.resumed).toBe(true);
    expect(resumed.redactions).toEqual([]);
  });

  it("a key in the redacted state is served as an entry with the mark, an empty value and the encoding redacted/v1 (05 [STATE-82], 09 [READ-13]), once a restore replays the blanked records", async () => {
    const { server, load } = world();
    server.write(KS, [kc("settings/a", "set", "A1")]); // 1
    server.write(KS, [kc("settings/other", "set", 1)]); // 2
    server.redact(KS, ["settings/a"]); // 3: the DELETE of settings/a
    // a restore to seq 2 keeps the blanked record 1 only: its change folds to the redacted state
    server.restore(KS, 2n);
    const resp = await load(0n, server.epoch(KS));
    const entry = resp.entries.find((e) => e.key === "settings/a");
    expect(entry).toMatchObject({ redacted: true, encoding: "redacted/v1", isNull: false, deleted: false, version: 1n });
    expect(entry?.value.length).toBe(0);
  });
});
