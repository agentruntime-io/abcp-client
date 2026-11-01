// V19 SHOULD-4, 14 [CLI-63]: "Before sending under a new `client_id`, it MUST re-stamp every SEQUENCED action that it sent and that has no definitive result ... in
// authoring order". An action whose request is in flight when the client re-identifies is one of them: it takes its slot with the others, in authoring order.

import { describe, expect, it } from "vitest";
import { set, StaticManifests, type Transport } from "../src/engine/index.js";
import { RpcError } from "../src/engine/errors.js";
import { FakeServer, ManualClock } from "../src/fake/index.js";
import { goOffline, goOnline, KS, MANIFEST, mutateRequests, rig } from "./helpers/engine.js";

describe("14 [CLI-63] (V19 SHOULD-4): an action in flight at a re-identification is re-stamped in authoring order", () => {
  it("A in flight and B (authored later) waiting behind it: A gets slot 1 and B slot 2 under the new client_id, not B first", async () => {
    const clock = new ManualClock();
    const manifests = new StaticManifests(MANIFEST);
    const server = new FakeServer({ manifests, clock, heartbeatMs: 15_000 });
    server.createKeyspaceNow(KS);
    const inner = server.transport(server.principal("ana"));
    // the `Mutate` calls are held until the test answers them
    const held: { req: unknown; answer: (how: "pass" | "InvalidArgument" | "Unavailable") => void }[] = [];
    const transport = new Proxy(inner, {
      get(target, p) {
        if (p === "mutate")
          return (req: unknown, o: unknown) =>
            new Promise((resolve, reject) => {
              held.push({ req, answer: (how) => (how !== "pass" ? reject(new RpcError(how, "scripted")) : (target as never as { mutate(r: unknown, o: unknown): Promise<unknown> }).mutate(req, o).then(resolve, reject)) });
            });
        const v = Reflect.get(target, p) as unknown;
        return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(target) : v;
      },
    }) as Transport;
    const r = await rig({ server, clock, manifests, cfg: { transport } });
    const settleSome = async (ms = 0): Promise<void> => {
      if (ms > 0) await r.clock.advance(ms);
      for (let i = 0; i < 6; i++) await r.engine.settle();
    };
    await goOffline(r);
    const a = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/a", 1)] });
    const b = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/b", 2)] });
    await goOnline(r);
    await settleSome();
    // the request that carried A and B fails as a whole (InvalidArgument): each is sent alone, in lane order (14 [CLI-50])
    expect(held).toHaveLength(1);
    expect((held[0]?.req as { mutations: { mutationId: string }[] }).mutations.map((m) => m.mutationId)).toEqual([a.mutationId, b.mutationId]);
    const first = held.shift();
    first?.answer("InvalidArgument");
    await settleSome(60_000);
    // A is alone in flight; B waits behind it
    expect(held.map((h) => (h.req as { mutations: { mutationId: string }[] }).mutations.map((m) => m.mutationId))).toEqual([[a.mutationId]]);
    const before = r.engine.clientId;
    await (r.engine as unknown as { run(f: () => unknown): Promise<void> }).run(() => (r.engine as unknown as { reidentify(why: string): void }).reidentify("CLIENT_UNKNOWN"));
    expect(r.engine.clientId).not.toBe(before);
    // the answer of A's request is lost; both are sent again under the new client_id
    held.shift()?.answer("Unavailable");
    await settleSome(60_000);
    for (let i = 0; i < 4 && held.length > 0; i++) {
      held.shift()?.answer("pass");
      await settleSome(60_000);
    }
    const under = mutateRequests(r)
      .flatMap((q) => q.mutations)
      .filter((m) => m.clientId === r.engine.clientId)
      .map((m) => [m.mutationId === a.mutationId ? "A" : "B", Number(m.clientSeq)]);
    // the first send of each under the new id: A takes slot 1 and B slot 2, in authoring order
    expect(under.find((x) => x[0] === "A")?.[1]).toBe(1);
    expect(under.find((x) => x[0] === "B")?.[1]).toBe(2);
    expect(r.engine.outbox()).toEqual([]);
    expect(server.records(KS).filter((rec) => rec.mutationId === a.mutationId)).toHaveLength(1);
  });
});
