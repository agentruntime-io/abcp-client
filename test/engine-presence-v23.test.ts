// V23 (client), the presence findings: 16 [SEC-1], 10 [AUTH-8], [AUTH-13], [AUTH-26], 09 [READ-74], [READ-77]. Each test is the scenario of a verifier's probe (V23-report.md, P2 and P6) turned into a rule: what the
// view keeps of an entry counts the principal that it carries, and a topic that a tab left and joined again is not left by the answer of the first join.

import { create } from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "vitest";
import { jsonBytes } from "../src/engine/index.js";
import { PresenceEntrySchema, PresenceFrameSchema, SubscribeResponseSchema } from "../src/gen/abpc/v2/sync_pb.js";
import { PrincipalSchema } from "../src/gen/abpc/v2/types_pb.js";
import { frame, KS, prig, protocolErrors, ROOM } from "./helpers/presence.js";

interface P {
  sub?: string;
  deviceId?: string;
  actorChain?: string[];
  issuer?: string;
  appId?: string;
}

/** A presence frame whose entries carry the principals that a test chooses (`frame` of the harness sets only the `sub`). */
function pframe(entries: { clientId: string; principal: P; expiresMs: number }[], snapshot = false): ReturnType<typeof create<typeof SubscribeResponseSchema>> {
  return create(SubscribeResponseSchema, {
    frame: {
      case: "presence",
      value: create(PresenceFrameSchema, {
        keyspaceId: KS,
        topic: ROOM,
        snapshot,
        entries: entries.map((e) =>
          create(PresenceEntrySchema, {
            principal: create(PrincipalSchema, { appId: e.principal.appId ?? "app", sub: e.principal.sub ?? "zoe", issuer: e.principal.issuer ?? "https://issuer.test", deviceId: e.principal.deviceId ?? "", actorChain: e.principal.actorChain ?? [] }),
            clientId: e.clientId,
            state: jsonBytes({ n: 1 }),
            expiresAt: timestampFromMs(e.expiresMs),
          }),
        ),
        leftClientIds: [],
      }),
    },
  });
}

const enc = new TextEncoder();
const octets = (s: string): number => enc.encode(s).length;
const weigh = (e: { clientId: string; state: Uint8Array; principal?: { sub: string; issuer: string; appId: string; deviceId: string; actorChain: string[] } }): number =>
  e.state.length + e.clientId.length + (e.principal === undefined ? 0 : octets(e.principal.sub) + octets(e.principal.issuer) + octets(e.principal.appId) + octets(e.principal.deviceId) + e.principal.actorChain.reduce((n, s) => n + octets(s), 0));

describe("V23 MUST-2, 16 [SEC-1], 10 [AUTH-8], [AUTH-13]: the view counts the principal of an entry, and drops one whose principal is over the sizes of the text", () => {
  it("the probe P2: 100 upserts with a sub of 1 MiB are not kept (a sub is at most 255 bytes, [AUTH-8]), and the engine says so once", async () => {
    const a = await prig({ seed: 11 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    const big = "s".repeat(1 << 20);
    for (let i = 0; i < 100; i++) {
      a.server.sendFrame(frame({ topic: ROOM, entries: [{ clientId: `c-${String(i).padStart(3, "0")}`, state: { n: i }, expiresMs: a.clock.now() + 20_000, sub: big }] }));
      await a.engine.settle();
    }
    expect(a.engine.presenceEntries(KS, ROOM)).toEqual([]);
    const errs = protocolErrors(a);
    expect(errs).toHaveLength(1); // once, not a hundred times
    expect(errs[0]).toMatchObject({ type: "protocol-error", what: "PresenceFrame", keyspaces: [KS] });
    expect((errs[0] as { detail: string }).detail).toContain("AUTH-8");
  });

  it("each size of the principal is its own limit: a sub of 255 bytes is kept and one of 256 is not (bytes, not characters); an actor chain of 16 entries of 255 octets is kept, 17 entries or an entry of 256 is not; a device id over 255 is not", async () => {
    const a = await prig({ seed: 11 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    const at = (): number => a.clock.now() + 20_000;
    const send = async (entries: Parameters<typeof pframe>[0]): Promise<void> => {
      a.server.sendFrame(pframe(entries, true));
      await a.engine.settle();
    };
    await send([
      { clientId: "sub255", principal: { sub: "s".repeat(255) }, expiresMs: at() },
      { clientId: "sub256", principal: { sub: "s".repeat(256) }, expiresMs: at() },
      { clientId: "sub-é-255", principal: { sub: `${"é".repeat(127)}s` }, expiresMs: at() }, // 127 × 2 + 1 = 255 bytes in 128 characters
      { clientId: "sub-é-256", principal: { sub: "é".repeat(128) }, expiresMs: at() }, // 256 bytes in 128 characters
      { clientId: "chain16", principal: { actorChain: Array.from({ length: 16 }, () => "a".repeat(255)) }, expiresMs: at() },
      { clientId: "chain17", principal: { actorChain: Array.from({ length: 17 }, () => "a") }, expiresMs: at() },
      { clientId: "chain-entry-256", principal: { actorChain: ["a".repeat(256)] }, expiresMs: at() },
      { clientId: "device255", principal: { deviceId: "d".repeat(255) }, expiresMs: at() },
      { clientId: "device256", principal: { deviceId: "d".repeat(256) }, expiresMs: at() },
    ]);
    expect(a.engine.presenceEntries(KS, ROOM).map((e) => e.clientId).sort()).toEqual(["chain16", "device255", "sub-é-255", "sub255"].sort());
    expect(protocolErrors(a)).toHaveLength(1);
  });

  it("the byte bound counts the principal: a topic of entries that are each within the sizes of the text, with a member cap raised to 100,000, holds at most MAX_VIEW_BYTES of state, client ids and principals", async () => {
    const a = await prig({ seed: 11, serverOptions: { limits: { maxPresenceMembersPerTopic: 100_000 } } });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    const chain = Array.from({ length: 16 }, (_x, i) => `${String(i).padStart(3, "0")}${"a".repeat(252)}`); // 4 KiB of actor chain
    const entries = Array.from({ length: 3000 }, (_x, i) => ({ clientId: `c-${String(i).padStart(5, "0")}`, principal: { sub: "s".repeat(255), actorChain: chain, issuer: "https://issuer.test/" + "i".repeat(200) }, expiresMs: a.clock.now() + 20_000 }));
    for (let i = 0; i < entries.length; i += 250) {
      a.server.sendFrame(pframe(entries.slice(i, i + 250), i === 0));
      await a.engine.settle();
    }
    const held = a.engine.presenceEntries(KS, ROOM);
    const bytes = held.reduce((n, e) => n + weigh(e as never), 0);
    expect(held.length).toBeGreaterThan(1000); // the bound does not refuse what fits
    expect(held.length).toBeLessThan(entries.length); // 3,000 entries of about 4.8 KiB are over 8 MiB, and the view stops taking new members at the bound
    expect(bytes).toBeLessThanOrEqual(8 << 20);
  });
});

describe("V23 MUST-3, 09 [READ-74]: \"The engine MUST NOT leave a topic while any of its tabs is still in it\"", () => {
  const leavesOf = (a: { server: { calls: { method: string }[] } }): number => a.server.calls.filter((c) => c.method === "leavePresence").length;

  it("the probe P6: join (in flight for 2 s), leave, join again: the answer of the first join sends no LeavePresence, and the server holds the membership throughout", async () => {
    const a = await prig({ seed: 11 });
    a.server.inject("joinPresence", { kind: "delay", ms: 2000 }, { times: 1 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    await a.engine.leavePresence(KS, ROOM);
    await a.engine.joinPresence(KS, ROOM); // the tab comes back before the first join is answered (React StrictMode: mount, unmount, mount)
    await a.engine.publishPresence(KS, ROOM, { back: true });
    const trace: number[] = [];
    for (let t = 0; t < 40_000; t += 500) {
      await a.clock.advance(500);
      await a.engine.settle();
      if (t >= 3_000) trace.push(a.server.presence.members(KS, ROOM).length); // after the answer of the first join, which came at 2 s
    }
    expect(leavesOf(a)).toBe(0);
    expect(trace.every((n) => n === 1)).toBe(true); // never without a membership: 13.5 s without one in the probe
    expect(a.engine.inPresence(KS, ROOM)).toBe(true);
    expect(a.server.presence.entries(KS, ROOM)).toHaveLength(1);
  });

  it("the other order: join (in flight), leave, and no join again: the answer of the join finds the topic left and leaves it (N4), once", async () => {
    const a = await prig({ seed: 11 });
    a.server.inject("joinPresence", { kind: "delay", ms: 2000 }, { times: 1 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    await a.engine.leavePresence(KS, ROOM);
    for (let t = 0; t < 5_000; t += 500) {
      await a.clock.advance(500);
      await a.engine.settle();
    }
    expect(leavesOf(a)).toBe(1);
    expect(a.server.presence.members(KS, ROOM)).toHaveLength(0);
    expect(a.engine.inPresence(KS, ROOM)).toBe(false);
  });

  it("join, leave, join, leave while the first join is in flight: the topic is left in the end, and the membership does not outlive the last tab", async () => {
    const a = await prig({ seed: 11 });
    a.server.inject("joinPresence", { kind: "delay", ms: 2000 }, { times: 1 });
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.settle();
    await a.engine.leavePresence(KS, ROOM);
    await a.engine.joinPresence(KS, ROOM);
    await a.engine.leavePresence(KS, ROOM);
    for (let t = 0; t < 8_000; t += 500) {
      await a.clock.advance(500);
      await a.engine.settle();
    }
    expect(a.server.presence.members(KS, ROOM)).toHaveLength(0);
    expect(a.engine.inPresence(KS, ROOM)).toBe(false);
  });
});
