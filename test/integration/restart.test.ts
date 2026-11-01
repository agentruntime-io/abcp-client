// C5: a restart of the Go server. Its store is in memory (server/internal/store/mem), so a restart loses every keyspace: the clients' view of
// the server changes under them, and the test asserts what the text says a client does then (14 Table 14.1 `KEYSPACE_NOT_FOUND` and the drop
// reason `NOT_FOUND`, [CLI-80], [CLI-45], [CLI-51]):
//  - it does not crash, and reports the hold (`held`, `stopped: NOT_FOUND`) instead of failing quietly;
//  - it keeps showing the copy that it has, and keeps the actions that it was given (nothing is dropped unseen, 02 [ARC-50]);
//  - when the keyspace exists again (somebody creates it again, under the same id and the same epoch 1) it adds it again with the hold backoff,
//    which loads the new history in place of the old one (a resume check from its cursor that the server cannot honour, 09 [READ-17]), and sends
//    the actions that waited.
// What a client cannot do is tell a keyspace that was created again from the one it followed by its epoch alone: the new history has epoch 1,
// like the old. The cursor's record hash and sequence number are what show it (09 [READ-17]).

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { set } from "../../src/engine/index.js";
import { dump, Env, waitFor, type TestClient } from "./harness/env.js";
import { goAvailable, SKIP_REASON } from "./harness/server.js";

if (!goAvailable) console.warn(`[integration] ${SKIP_REASON}`);

describe.skipIf(!goAvailable)("a restart of the server with the in-memory store", () => {
  let env: Env;
  beforeAll(async () => {
    env = await Env.start();
  }, 180_000);
  afterAll(async () => {
    await env?.close();
  });

  const until = (c: TestClient, cond: () => boolean, what: string, ms?: number): Promise<void> =>
    waitFor(cond, what, ms).catch((e: Error) => {
      throw new Error(`${e.message}\n${dump(c, env)}`);
    });

  // the new history is longer than the clients' cursor (6 records against 3), or exactly as long (3 against 3: only the record hash tells them apart)
  it.each([6, 3])("is survived: the clients hold the lost keyspace, keep their copy and their actions, and load the keyspace again when it exists again (a new history of %i records)", async (fresh) => {
    const KS = `acme/restart${fresh}`;
    const a = await env.client({ sub: "ana" });
    await a.engine.createKeyspace(KS, "team");
    await until(a, () => a.engine.cursor(KS)?.loaded === true, "A loaded");
    const b = await env.client({ sub: "bob" });
    await b.engine.addKeyspace(KS);
    await until(b, () => b.engine.cursor(KS)?.loaded === true, "B loaded");
    for (let i = 0; i < 3; i++) await a.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set(`note/${i}`, `old${i}`)] });
    await until(b, () => b.engine.view(KS).has("note/2"), "B saw the three notes");
    await until(a, () => a.engine.status().pending === 0, "A flushed");
    const oldCursor = a.engine.cursor(KS);
    expect(oldCursor).toMatchObject({ loaded: true, seq: 3n, logEpoch: 1 }); // the three notes

    await env.restartServer(); // the process is killed and another starts: nothing of the store is left

    // the clients notice, hold the keyspace and report it; the session itself lives on (it follows no keyspace now)
    for (const c of [a, b]) {
      await until(c, () => c.engine.status().stopped[KS] === "NOT_FOUND", "the keyspace was dropped with NOT_FOUND");
      expect(c.engine.status().holds).toMatchObject([{ scope: "keyspace", keyspaceId: KS, code: "NOT_FOUND" }]);
      expect(c.ofType("held").some((e) => e.state === "start" && e.scope === "keyspace" && e.code === "NOT_FOUND")).toBe(true);
      expect(c.ofType("protocol-error")).toEqual([]);
      expect(c.engine.status().connection).toBe("live");
      // the copy that the client has stays, as it was
      expect(c.engine.view(KS).keys("note/")).toEqual(["note/0", "note/1", "note/2"]);
    }
    // an action that the app gives meanwhile is kept, held, and not lost
    const late = await a.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("note/late", "late")] });
    expect(a.engine.status().pending).toBe(1);
    expect(a.engine.view(KS).has("note/late")).toBe(true);

    // the keyspace exists again, with a history of its own that is longer than the cursor of the clients
    const c = await env.client({ sub: "cy" });
    expect((await c.engine.createKeyspace(KS, "team")).logEpoch).toBe(1);
    await until(c, () => c.engine.cursor(KS)?.loaded === true, "C loaded");
    const ids: string[] = [];
    for (let i = 0; i < fresh; i++) ids.push((await c.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set(`note/n${i}`, `new${i}`)] })).mutationId);
    await until(c, () => c.engine.status().pending === 0, "C flushed");

    // A and B add the keyspace again by themselves (the hold backoff), and A's action goes once the add has succeeded
    await until(a, () => a.events.some((e) => e.type === "committed" && e.mutationId === late.mutationId), "A's held action was committed in the new keyspace", 30_000);
    await until(b, () => b.engine.status().stopped[KS] === undefined && b.engine.view(KS).has(`note/n${fresh - 1}`), "B follows the new keyspace", 30_000);
    await until(a, () => a.engine.view(KS).has(`note/n${fresh - 1}`) && a.engine.status().holds.length === 0, "A follows the new keyspace", 30_000);
    for (const x of [a, b]) {
      expect(x.engine.status().stopped).toEqual({});
      expect(x.engine.status().holds).toEqual([]);
      // the new history replaced the old one: none of the old notes is left
      expect(x.engine.confirmed(KS).keys("note/").filter((k) => /^note\/[0-2]$/.test(k))).toEqual([]);
      expect(x.ofType("reloaded").some((e) => e.keyspaceId === KS && e.cause === "RESUME_TOO_OLD")).toBe(true);
      expect(x.ofType("protocol-error")).toEqual([]);
      expect(x.engine.deadLetterEntries()).toEqual([]);
    }
    expect(b.engine.confirmed(KS).keys("note/")).toEqual(["note/late", ...ids.map((_, i) => `note/n${i}`)].sort());
    expect(a.engine.confirmed(KS).keys("note/")).toEqual(b.engine.confirmed(KS).keys("note/"));
    expect(a.engine.status().pending).toBe(0);
    // the epoch of the new keyspace is the old one's (1): the engine told the new history from the old one by its cursor, not by its epoch
    expect(a.engine.cursor(KS)?.logEpoch).toBe(1);
  }, 120_000);
});
