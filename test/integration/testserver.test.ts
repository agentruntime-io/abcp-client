// S1-P5: the client's harness for the test binary of the CONF-6 test profile (harness/testserver.ts), driven end to end against a deployment of three nodes on the
// in-memory stores (server/cmd/abpc-testserver). The cases are one for each family of hooks that 17 [CONF-6] asks the harness for, and the engine of the
// client is the system that uses them:
//  - the injectable clock: the one time source of 10 [AUTH-66] (SetClock, AdvanceClock) dates the records and judges the tokens, and an offset for one node's local clock
//    (SetNodeOffset) moves nothing that the source dates;
//  - token and key control: MintToken gives a token that an engine then uses, on every node, and a token that is signed wrongly is refused;
//  - a fault: SetFault when piece P4 has built the fault proxy, and the Unimplemented that the control API answers until then (the slots of later milestones, the restore
//    hook and the break-glass trigger, answer it still);
//  - the restart hook: StopNode (a crash and a graceful stop) and StartNode keep the stores of a node, so that its engine holds its work while the node is away and sends
//    it when the node is back, which the production binary of the other integration tests, whose store is lost with the process, cannot show.

import { create } from "@bufbuild/protobuf";
import { durationFromMs, durationMs, timestampFromDate, timestampMs, type Duration, type Timestamp } from "@bufbuild/protobuf/wkt";
import { Code, ConnectError, createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-node";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { RpcError, set } from "../../src/engine/index.js";
import { GetManifestRequestSchema, SubscribeRequestSchema } from "../../src/gen/abpc/v2/sync_pb.js";
import { KeyspaceCursorSchema, type Record as AbpcRecord } from "../../src/gen/abpc/v2/types_pb.js";
import { createNodeTransport } from "../../src/net/node.js";
import { StopMode, TestControlService } from "../gen/abpctest/v1/control_pb.js";
import { waitFor } from "./harness/env.js";
import { nodeOf, startTestServer, testServerAvailable, TESTSERVER_SKIP_REASON, type ServerClient, type TestServer, type TokenOptions } from "./harness/testserver.js";

if (!testServerAvailable) console.warn(`[integration] ${TESTSERVER_SKIP_REASON}`);

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
const dec = new TextDecoder();
const text = (b: Uint8Array | undefined): string | undefined => (b === undefined ? undefined : dec.decode(b));
const HOUR = 3_600_000;
/** A timestamp of a message, in ms since the epoch. */
const at = (t: Timestamp | undefined): number => {
  if (t === undefined) throw new Error("the message has no timestamp");
  return timestampMs(t);
};
const span = (d: Duration | undefined): number => (d === undefined ? 0 : durationMs(d));

/** The outcome of a call of the control API: its answer, or the Connect code that it failed with. */
async function settle<T>(p: Promise<T>): Promise<{ ok: true; value: T } | { ok: false; code: Code; message: string }> {
  try {
    return { ok: true, value: await p };
  } catch (e) {
    const c = ConnectError.from(e);
    return { ok: false, code: c.code, message: c.rawMessage };
  }
}

describe.skipIf(!testServerAvailable)("the engine against the test binary (three nodes, in memory)", () => {
  let ts: TestServer;
  const clients: ServerClient[] = [];
  beforeAll(async () => {
    ts = await startTestServer();
  }, 900_000);
  afterEach(async () => {
    await Promise.all(clients.splice(0).map((c) => c.close().catch(() => undefined)));
  });
  afterAll(async () => {
    await ts?.close();
  });

  /** An engine on a node; closed at the end of the test. */
  const client = async (o: Parameters<TestServer["client"]>[0]): Promise<ServerClient> => {
    const c = await ts.client(o);
    clients.push(c);
    return c;
  };
  /** `waitFor` that shows the server's log when it fails. */
  const until = (cond: () => boolean | Promise<boolean>, what: string, ms?: number): Promise<void> =>
    waitFor(cond, what, ms).catch((e: Error) => {
      throw new Error(`${e.message}\ntest binary log:\n${ts.log()}`);
    });
  const committed = (c: ServerClient, id: string): boolean => c.events.some((e) => e.type === "committed" && e.mutationId === id);
  const loaded = (c: ServerClient, ks: string): Promise<void> => until(() => c.engine.cursor(ks)?.loaded === true, `${ks} loaded`);
  /** A write of one note through an engine, waited for until the server has committed it. */
  const write = async (c: ServerClient, ks: string, key: string, value: string): Promise<string> => {
    const w = await c.engine.submit({ keyspaceId: ks, opType: "Set", changes: [set(key, value)] });
    await until(() => committed(c, w.mutationId), `${key} committed through ${c.node}`, 30_000);
    return w.mutationId;
  };
  /** The records of a keyspace, read from its start on a node by a session of its own, until `n` of them have arrived. */
  const records = async (c: ServerClient, ks: string, n: number): Promise<AbpcRecord[]> => {
    const ac = new AbortController();
    const out: AbpcRecord[] = [];
    try {
      const req = create(SubscribeRequestSchema, { clientId: crypto.randomUUID(), keyspaces: [create(KeyspaceCursorSchema, { keyspaceId: ks })] });
      for await (const f of c.net.transport.subscribe(req, { signal: ac.signal })) {
        if (f.frame.case === "records") out.push(...f.frame.value.records);
        if (out.length >= n) break;
      }
    } finally {
      ac.abort();
    }
    return out;
  };
  /** `GetManifest` through a node with a token: the cheapest call that the node authenticates. */
  const manifestWith = async (node: string, token: string): Promise<{ ok: true } | { ok: false; status: string }> => {
    const net = createNodeTransport({ baseUrl: ts.nodeUrl(node), token: () => token, httpVersion: "1.1" });
    try {
      await net.transport.getManifest(create(GetManifestRequestSchema, {}));
      return { ok: true };
    } catch (e) {
      return { ok: false, status: e instanceof RpcError ? e.status : String(e) };
    } finally {
      net.close();
    }
  };

  it("starts a deployment of three nodes, each at its own endpoint, marked as a test build, and the control API wants its secret", async () => {
    const d = await ts.deployment();
    expect(d.nodes.map((n) => n.name)).toEqual(["n1", "n2", "n3"]);
    expect(d.nodes.every((n) => n.running && n.restarts === 0)).toBe(true);
    expect(new Set(d.nodes.map((n) => n.url)).size).toBe(3);
    expect(d.nodes.map((n) => n.url)).toEqual(ts.nodes.map((n) => n.url));
    expect(d.build.endsWith("+testprofile")).toBe(true);
    expect(d.store).toBe("memory");
    expect(d.runId).toBe(ts.runId);
    expect(d.issuers.map((i) => [i.name, i.admin])).toEqual([
      ["user1", false],
      ["user2", false],
      ["admin", true],
    ]);
    expect(ts.caPem).toContain("BEGIN CERTIFICATE");
    expect(ts.operatorSecret).not.toBe("");
    // each node answers on its own endpoint, with a token that a test issuer signed
    const token = await ts.token({ sub: "ana" });
    for (const n of ts.nodes) expect(await manifestWith(n.name, token), n.name).toEqual({ ok: true });
    // the control listener is behind the run's secret
    const bare = createClient(TestControlService, createConnectTransport({ baseUrl: ts.ready.control_url, httpVersion: "1.1", useBinaryFormat: true }));
    const refused = await settle(bare.getDeployment({}));
    expect(refused.ok ? undefined : refused.code).toBe(Code.Unauthenticated);
  });

  it("token and key control: MintToken gives a token that an engine uses on every node, and a token that is signed wrongly is refused", async () => {
    const KS = "acme/minted";
    // the first user issuer's token on node 1, the second user issuer's on node 2: two issuers of one app, two nodes of one deployment
    const ana = await client({ node: "n1", sub: "ana", issuer: "user1" });
    const bob = await client({ node: "n2", sub: "bob", issuer: "user2" });
    expect((await ana.engine.createKeyspace(KS, "team")).created).toBe(true);
    await loaded(ana, KS);
    await write(ana, KS, "note/1", "from ana");
    await bob.engine.addKeyspace(KS);
    await until(() => text(bob.engine.view(KS).get("note/1")?.value) === '"from ana"', "bob, on another node and another issuer, saw the record");
    await write(bob, KS, "note/2", "from bob");
    await until(() => text(ana.engine.view(KS).get("note/2")?.value) === '"from bob"', "ana saw the record that bob wrote through node 2");
    for (const c of [ana, bob]) {
      expect(c.ofType("protocol-error")).toEqual([]);
      expect(c.engine.status().connection).toBe("live");
    }
    // the issuer signs what the harness gives, and the node refuses what it should: alg none, an HMAC, a key that no JWKS publishes, a forged kid, a crit header
    const rejects: [string, NonNullable<TokenOptions["mint"]>][] = [
      ["alg none", { headerJson: '{"alg": "none"}' }],
      ["HS256", { headerJson: '{"alg": "HS256"}', hmacSecret: new TextEncoder().encode("a secret of the harness") }],
      ["an unpublished key", { unpublishedKey: true }],
      ["a forged kid", { headerJson: '{"kid": "nobody"}' }],
      ["a crit header", { headerJson: '{"crit": ["exp"]}' }],
    ];
    for (const [name, mint] of rejects) {
      expect(await manifestWith("n3", await ts.token({ sub: "mallory", mint })), name).toEqual({ ok: false, status: "Unauthenticated" });
    }
    // another audience, and an issuer that is none
    expect(await manifestWith("n1", await ts.token({ sub: "ana", aud: "another-app" }))).toEqual({ ok: false, status: "Unauthenticated" });
    const none = await settle(ts.control.mintToken({ issuer: "nobody", claimsJson: "{}" }));
    expect(none.ok ? undefined : none.code).toBe(Code.NotFound);
  });

  it("the clock: the source dates the records and judges the tokens, never moves by itself or back, and an offset on one node moves no record", async () => {
    const KS = "acme/clock";
    const t0 = await ts.now();
    await sleep(300);
    expect((await ts.now()).getTime(), "the source moved by itself").toBe(t0.getTime());

    // a token is judged by the source: one that lives 120 s is accepted now, and refused on every node once the source is 10 minutes on (10 [AUTH-8])
    const short = await ts.token({ sub: "ana", lifetimeS: 120 });
    for (const n of ts.nodes) expect(await manifestWith(n.name, short), n.name).toEqual({ ok: true });
    const adv = await ts.control.advanceClock({ by: durationFromMs(10 * 60_000) });
    const t1 = t0.getTime() + 10 * 60_000;
    expect(at(adv.now)).toBe(t1);
    for (const n of ts.nodes) expect(await manifestWith(n.name, short), n.name).toEqual({ ok: false, status: "Unauthenticated" });
    expect(await manifestWith("n1", await ts.token({ sub: "ana" })), "a token minted at the new reading").toEqual({ ok: true });

    // the records that the nodes append are dated by the source (10 [AUTH-66]): the first write of the keyspace
    const ana = await client({ node: "n1", sub: "ana" });
    await ana.engine.createKeyspace(KS, "team");
    await loaded(ana, KS);
    await write(ana, KS, "note/a", "first");
    const reader = await client({ node: "n3", sub: "reader", start: false });
    expect((await records(reader, KS, 1)).map((r) => at(r.serverTime))).toEqual([t1]);

    // SetClock forward: the next record is dated by the new reading
    const later = new Date(t1 + 3 * HOUR);
    const set1 = await ts.control.setClock({ time: timestampFromDate(later) });
    expect(at(set1.now)).toBe(later.getTime());
    await write(ana, KS, "note/b", "second");
    // the source never goes back: a time below the reading is refused and the reading stays
    const back = await settle(ts.control.setClock({ time: timestampFromDate(new Date(t1)) }));
    expect(back.ok ? undefined : back.code).toBe(Code.InvalidArgument);
    expect((await ts.now()).getTime()).toBe(later.getTime());

    // an offset moves the local clock of one node, and no instant that the source dates: bob writes through node 2, whose clock is a day and a half ahead
    const off = await ts.control.setNodeOffset({ node: "n2", offset: durationFromMs(36 * HOUR) });
    expect(span(off.offset)).toBe(36 * HOUR);
    const d = await ts.deployment();
    expect(d.nodes.map((n) => span(n.clockOffset))).toEqual([0, 36 * HOUR, 0]);
    expect(at(d.clock?.now)).toBe(later.getTime());
    const bob = await client({ node: "n2", sub: "bob", issuer: "user2" });
    await bob.engine.addKeyspace(KS);
    await loaded(bob, KS);
    await write(bob, KS, "note/c", "third");
    expect((await records(reader, KS, 3)).map((r) => at(r.serverTime))).toEqual([t1, later.getTime(), later.getTime()]);
    const unknown = await settle(ts.control.setNodeOffset({ node: "n9", offset: durationFromMs(HOUR) }));
    expect(unknown.ok ? undefined : unknown.code).toBe(Code.NotFound);
    await ts.control.setNodeOffset({ node: "n2", offset: durationFromMs(0) });
  }, 60_000);

  it("a fault: the fault proxy delays a commit when piece P4 has built it, and SetFault answers Unimplemented until then", async () => {
    const KS = "acme/fault";
    const ana = await client({ node: "n1", sub: "ana" });
    await ana.engine.createKeyspace(KS, "team");
    await loaded(ana, KS);
    const DELAY_MS = 1000;
    const timed = async (key: string): Promise<number> => {
      const t = Date.now();
      await write(ana, KS, key, "x");
      return Date.now() - t;
    };
    const rule = { target: "commit", action: "delay", delay: durationFromMs(DELAY_MS) };
    const set1 = await settle(ts.control.setFault({ rule }));
    if (!set1.ok) {
      // not built: the hook answers Unimplemented, nothing changed, and a write is not delayed
      expect(set1.code).toBe(Code.Unimplemented);
      expect((await settle(ts.control.setFault({ clear: true }))).ok).toBe(false);
      expect(await timed("note/undelayed")).toBeLessThan(DELAY_MS);
    } else {
      // built: with the rule in force every commit call is delayed, and a write takes at least as long; cleared, it does not
      expect(set1.value.ruleId).not.toBe("");
      expect(await timed("note/delayed")).toBeGreaterThanOrEqual(DELAY_MS - 100);
      expect((await settle(ts.control.setFault({ clear: true }))).ok).toBe(true);
      expect(await timed("note/cleared")).toBeLessThan(DELAY_MS);
    }
    // the restore hook (R3-P1b) is built: a restore that names no backup point is refused as a request
    const restore = await settle(ts.control.restore({ app: "tracker", keyspace: KS }));
    expect(restore.ok ? undefined : restore.code, "Restore").toBe(Code.InvalidArgument);
    // the break-glass trigger (16 [SEC-44]) is a slot of a later milestone, and answers Unimplemented
    const breakGlass = await settle(ts.control.breakGlass({}));
    expect(breakGlass.ok ? undefined : breakGlass.code, "BreakGlass").toBe(Code.Unimplemented);
  });

  it("a crash: StopNode stops a node at once, its engine holds its work, and StartNode brings it back on the stores that it kept", async () => {
    const KS = "acme/crash";
    const ana = await client({ node: "n1", sub: "ana" });
    const bob = await client({ node: "n2", sub: "bob", issuer: "user2" });
    await ana.engine.createKeyspace(KS, "team");
    await loaded(ana, KS);
    await bob.engine.addKeyspace(KS);
    await loaded(bob, KS);
    await write(ana, KS, "note/1", "before");
    await until(() => bob.engine.view(KS).has("note/1"), "bob saw the first record");
    const url = ts.nodeUrl("n1");

    await ts.control.stopNode({ node: "n1", mode: StopMode.CRASH });
    const down = await ts.deployment();
    expect(down.nodes.map((n) => n.running)).toEqual([false, true, true]);
    // the engine on the node that crashed notices, and keeps what the app gives it (14 [CLI-6], [CLI-21]): the action is shown at once and waits in the outbox
    await until(() => ana.engine.status().connection !== "live", "ana noticed that node 1 is gone");
    const held = await ana.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("note/2", "while away")] });
    expect(ana.engine.status().pending).toBe(1);
    expect(text(ana.engine.view(KS).get("note/2")?.value)).toBe('"while away"');
    expect(committed(ana, held.mutationId)).toBe(false);
    // a node that is down is not there to call, and the other nodes serve on, on the stores that all three share
    expect(await manifestWith("n1", await ts.token({ sub: "ana" }))).toMatchObject({ ok: false });
    await write(bob, KS, "note/3", "through node 2");
    expect((await settle(ts.control.stopNode({ node: "n1", mode: StopMode.CRASH }))).ok, "a node that is stopped is stopped again").toBe(false);

    // the node starts again at its endpoint, on its stores: nothing of the keyspace was lost with it
    const started = await ts.control.startNode({ node: "n1" });
    expect(started.url).toBe(url);
    await until(() => committed(ana, held.mutationId), "ana's held action was sent and committed when node 1 came back", 30_000);
    await until(() => bob.engine.view(KS).has("note/2"), "bob saw the action that was held through the crash");
    await until(() => ana.engine.view(KS).has("note/3"), "ana saw the record that bob wrote while her node was down");
    for (const c of [ana, bob]) {
      expect(c.engine.confirmed(KS).keys("note/")).toEqual(["note/1", "note/2", "note/3"]);
      expect(c.engine.status().holds, "the keyspace is not lost, as it is when the production server restarts").toEqual([]);
      expect(c.ofType("protocol-error")).toEqual([]);
    }
    expect(ana.engine.status().pending).toBe(0);
    const up = await ts.deployment();
    expect(up.nodes.map((n) => [n.running, n.restarts])).toEqual([
      [true, 1],
      [true, 0],
      [true, 0],
    ]);
    expect((await settle(ts.control.startNode({ node: "n1" }))).ok, "a node that runs is not started").toBe(false);
    const unknown = await settle(ts.control.stopNode({ node: "n9", mode: StopMode.CRASH }));
    expect(unknown.ok ? undefined : unknown.code).toBe(Code.NotFound);
  }, 90_000);

  it("a graceful stop: the node drains its sessions with its own shutdown, and the engine on it is back when the node starts", async () => {
    const KS = "acme/graceful";
    const cy = await client({ node: "n3", sub: "cy" });
    await cy.engine.createKeyspace(KS, "team");
    await loaded(cy, KS);
    await write(cy, KS, "note/1", "before");
    await ts.control.stopNode({ node: "n3", mode: StopMode.GRACEFUL });
    expect(nodeOf(await ts.deployment(), "n3").running).toBe(false);
    await until(() => cy.engine.status().connection !== "live", "cy noticed that node 3 stopped");
    const held = await cy.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("note/2", "after the drain")] });
    await ts.control.startNode({ node: "n3" });
    await until(() => committed(cy, held.mutationId), "cy's action was committed when node 3 came back", 30_000);
    expect(cy.engine.confirmed(KS).keys("note/")).toEqual(["note/1", "note/2"]);
    expect(nodeOf(await ts.deployment(), "n3").restarts).toBe(1);
  }, 60_000);
});
