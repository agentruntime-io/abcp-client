// C5: the client engine on the real network, against the real Go server (server/cmd/abcp-server, started in development mode with a test
// issuer, 10 §10.1, and the manifest of harness/server.ts): the Connect transport with strict decoding, the manifests adapter over
// `GetManifest`, and the engine's sync of 09 and 14 through them. The network between a client and the server is a TCP proxy that a test cuts.

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import * as Y from "yjs";
import { incr, set } from "../../src/engine/index.js";
import { ALL, dump, Env, waitFor, type TestClient } from "./harness/env.js";
import { goAvailable, SKIP_REASON } from "./harness/server.js";

if (!goAvailable) console.warn(`[integration] ${SKIP_REASON}`);

const dec = new TextDecoder();
const text = (b: Uint8Array | undefined): string | undefined => (b === undefined ? undefined : dec.decode(b));

describe.skipIf(!goAvailable)("the engine against the Go server", () => {
  let env: Env;
  beforeAll(async () => {
    env = await Env.start();
  }, 180_000);
  afterAll(async () => {
    await env?.close();
  });

  const committed = (c: TestClient, id: string): boolean => c.events.some((e) => e.type === "committed" && e.mutationId === id);
  /** `waitFor` that shows the client's state when it fails. */
  const until = (c: TestClient, cond: () => boolean, what: string, ms?: number): Promise<void> =>
    waitFor(cond, what, ms).catch((e: Error) => {
      throw new Error(`${e.message}\n${dump(c, env)}`);
    });
  const loaded = (c: TestClient, ks: string): Promise<void> => until(c, () => c.engine.cursor(ks)?.loaded === true, `${ks} loaded`);

  it("one engine creates a keyspace and writes; a second subscribes and sees the records", async () => {
    const KS = "acme/s1";
    const a = await env.client({ sub: "ana" });
    const created = await a.engine.createKeyspace(KS, "team");
    expect(created.created).toBe(true);
    await loaded(a, KS);
    const w1 = await a.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("note/1", "hello")] });
    await until(a, () => committed(a, w1.mutationId), "A's write committed");
    expect(text(a.engine.confirmed(KS).get("note/1")?.value)).toBe('"hello"');

    const b = await env.client({ sub: "bob" });
    await b.engine.addKeyspace(KS);
    await until(b, () => text(b.engine.view(KS).get("note/1")?.value) === '"hello"', "B loaded the record of A");
    // and live: A writes again, B sees it on its subscription
    const w2 = await a.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("note/2", "world")] });
    await until(a, () => committed(a, w2.mutationId), "A's second write committed");
    await until(b, () => text(b.engine.view(KS).get("note/2")?.value) === '"world"', "B saw the second record live");
    expect(b.calls["subscribe"]).toBe(1);
    expect(b.engine.status().connection).toBe("live");
    // the manifest came once, by `GetManifest`, and the engine knows what the manifest says of the keys
    expect(a.calls["getManifest"]).toBe(1);
    expect(a.manifests.parsed(1)?.app).toBe("tracker");
    expect(a.manifests.get(1)?.opType("Approve")).toMatchObject({ lane: "GATED", optimistic: true });
    expect(a.manifests.get(1)?.opType("Set")).toMatchObject({ lane: "SEQUENCED", writes: ["note/{id}"] });
    expect(a.manifests.get(1)?.template(1, "count/x")).toMatchObject({ rule: "counter", entity: false, parent: "" });
    expect(a.ofType("protocol-error")).toEqual([]);
  });

  it("writes made offline are sent when the network comes back, and the other engine sees them", async () => {
    const KS = "acme/offline";
    const net = await env.network();
    const a = await env.client({ sub: "ana", network: net });
    await a.engine.createKeyspace(KS, "team");
    await loaded(a, KS);
    const b = await env.client({ sub: "bob" });
    await b.engine.addKeyspace(KS);
    await loaded(b, KS);
    await until(a, () => a.engine.status().connection === "live", "A live");

    net.down(); // the network of A goes away
    await until(a, () => a.engine.status().connection !== "live", "A noticed that it is offline");
    const ids: string[] = [];
    for (const [i, v] of ["one", "two"].entries()) ids.push((await a.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set(`note/${i}`, v)] })).mutationId);
    ids.push((await a.engine.submit({ keyspaceId: KS, opType: "Bump", changes: [incr("count/hits", 5)] })).mutationId);
    ids.push((await a.engine.submit({ keyspaceId: KS, opType: "Bump", changes: [incr("count/hits", 2)] })).mutationId);
    // offline: the actions are in the durable outbox and shown at once (14 [CLI-6], [CLI-21]); nothing reached the server
    expect(a.engine.status().pending).toBe(4);
    expect(text(a.engine.view(KS).get("note/0")?.value)).toBe('"one"');
    expect(a.engine.confirmed(KS).get("note/0")).toBeUndefined();
    expect(b.engine.view(KS).get("note/0")).toBeUndefined();

    net.up();
    await until(a, () => ids.every((id) => committed(a, id)), "every offline write committed after the reconnect", 30_000);
    expect(a.engine.status().pending).toBe(0);
    await until(b, () => text(b.engine.view(KS).get("note/1")?.value) === '"two"' && text(b.engine.view(KS).get("count/hits")?.value) === "7", "B saw the writes of A");
    // the order of A's actions is kept: the counter is the sum, each action applied once
    expect(text(b.engine.confirmed(KS).get("count/hits")?.value)).toBe("7");
    expect(a.ofType("outcome-unknown")).toEqual([]);
    expect(a.ofType("rejected")).toEqual([]);
  });

  it("a GATED action is decided by the server's built-in gate and committed", async () => {
    const KS = "acme/gated";
    const a = await env.client({ sub: "ana" });
    await a.engine.createKeyspace(KS, "team");
    await loaded(a, KS);
    const b = await env.client({ sub: "bob" });
    await b.engine.addKeyspace(KS);
    await loaded(b, KS);
    const w = await a.engine.submit({ keyspaceId: KS, opType: "Approve", changes: [set("task/7", "approved")] });
    await until(a, () => committed(a, w.mutationId), "the GATED action was decided and committed");
    // `committed` carries the seq of the DECISION record (14 [CLI-38]), not `corrected`: the request was applied as asked
    expect(a.events.filter((e) => e.type === "committed" && e.mutationId === w.mutationId)).toHaveLength(1);
    expect(a.ofType("corrected")).toEqual([]);
    await until(b, () => text(b.engine.view(KS).get("task/7")?.value) === '"approved"', "B saw the decision's effect");
    expect(a.engine.status().pending).toBe(0);
  });

  it("a CRDT key that both engines edit converges", async () => {
    const KS = "acme/crdt";
    const a = await env.client({ sub: "ana" });
    await a.engine.createKeyspace(KS, "team");
    await loaded(a, KS);
    const b = await env.client({ sub: "bob" });
    await b.engine.addKeyspace(KS);
    await loaded(b, KS);
    const ha = a.engine.crdt(KS, "doc/1");
    const hb = b.engine.crdt(KS, "doc/1");
    // both edit before either has heard of the other
    ha.doc.getText("t").insert(0, "alpha ");
    hb.doc.getText("t").insert(0, "beta ");
    await ha.flush("DocEdit");
    await hb.flush("DocEdit");
    const same = (): boolean => {
      const ta = a.engine.crdt(KS, "doc/1").confirmedDoc.getText("t").toString();
      const tb = b.engine.crdt(KS, "doc/1").confirmedDoc.getText("t").toString();
      return ta === tb && ta.includes("alpha") && ta.includes("beta");
    };
    await until(a, same, "both confirmed documents hold both edits and are equal", 20_000);
    // the view documents (what the users see) agree with the confirmed ones once nothing is pending
    await until(a, () => a.engine.status().pending === 0 && b.engine.status().pending === 0, "nothing pending");
    const final = a.engine.crdt(KS, "doc/1").confirmedDoc.getText("t").toString();
    expect(b.engine.crdt(KS, "doc/1").confirmedDoc.getText("t").toString()).toBe(final);
    expect(a.engine.crdt(KS, "doc/1").doc.getText("t").toString()).toBe(final);
    expect(b.engine.crdt(KS, "doc/1").doc.getText("t").toString()).toBe(final);
    // a third engine that loads the keyspace reads the same document from the server's own CRDT base
    const c = await env.client({ sub: "cy" });
    await c.engine.addKeyspace(KS);
    await loaded(c, KS);
    expect(c.engine.crdt(KS, "doc/1").confirmedDoc.getText("t").toString()).toBe(final);
    expect(Y).toBeDefined();
  });

  it("the token is refreshed in band on the live session, with RefreshSession, and the session is not reconnected", async () => {
    const KS = "acme/token";
    // a token that lives 30 s: the engine asks for a new one at once (never sooner than 5 s after the session is live, 14 [CLI-68])
    const keep = await env.client({ sub: "ana", lifetimeS: 30 });
    await keep.engine.createKeyspace(KS, "team");
    await loaded(keep, KS);
    // a second client whose new token no longer grants the keyspace: the session is governed by the new token (10 [AUTH-49])
    const narrowed = await env.client({
      sub: "bob",
      lifetimeS: 30,
      refresh: () => env.token({ sub: "bob", deviceId: "dev-bob", grants: [{ ks: "acme/other", actions: ["read"] }] }),
    });
    await narrowed.engine.addKeyspace(KS);
    await loaded(narrowed, KS);
    const firstExp = keep.tokens.expiresAt() as number;

    await until(keep, () => keep.refreshed.length >= 1, "RefreshSession was answered", 20_000);
    expect(keep.refreshes()).toBe(1);
    expect(keep.calls["refreshSession"]).toBe(1);
    expect(keep.calls["subscribe"]).toBe(1); // in band: no reconnect
    expect(keep.refreshed[0]?.dropped).toEqual([]);
    // the answer's `expires_at` is the new token's `exp`, which the token holder now reports
    expect(keep.refreshed[0]?.expiresAt).toBe(keep.tokens.expiresAt());
    expect(keep.tokens.expiresAt() as number).toBeGreaterThan(firstExp);
    expect(keep.engine.status().connection).toBe("live");
    expect(keep.engine.status().holds).toEqual([]);
    // it still works after the refresh
    const w = await keep.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("note/a", "after")] });
    await until(keep, () => committed(keep, w.mutationId), "a write after the refresh");

    await until(narrowed, () => narrowed.refreshed.length >= 1, "the narrowed client's RefreshSession was answered", 20_000);
    expect(narrowed.refreshed[0]?.dropped).toEqual([KS]);
    await until(narrowed, () => narrowed.engine.status().stopped[KS] === "GRANT_CHANGED", "the keyspace was dropped with GRANT_CHANGED");
    expect(narrowed.calls["subscribe"]).toBe(1);
  }, 60_000);

  it("keeps working over HTTP/2 and over fetch (the browser's shape)", async () => {
    for (const via of ["node-2", "web"] as const) {
      const KS = `acme/${via}`;
      const a = await env.client({ sub: `ana-${via}`, via });
      await a.engine.createKeyspace(KS, "team");
      await loaded(a, KS);
      const w = await a.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("note/1", via)] });
      await until(a, () => committed(a, w.mutationId), `${via}: a write committed`);
      const b = await env.client({ sub: `bob-${via}`, via });
      await b.engine.addKeyspace(KS);
      await until(b, () => text(b.engine.view(KS).get("note/1")?.value) === `"${via}"`, `${via}: B loaded the record`);
      const w2 = await a.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("note/2", "live")] });
      await until(a, () => committed(a, w2.mutationId), `${via}: second write committed`);
      await until(b, () => text(b.engine.view(KS).get("note/2")?.value) === '"live"', `${via}: B saw it live`);
      expect(b.ofType("protocol-error")).toEqual([]);
      expect(ALL).toBeDefined();
    }
  });
});
