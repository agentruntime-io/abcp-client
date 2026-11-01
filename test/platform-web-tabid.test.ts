// C16b, V19 N5: the id of a browser tab. A duplicated tab (or a window that a page opened) copies `sessionStorage`, so two live tabs would have one tab id,
// and where no reliable cross-tab lock exists the tab id names the tab's outbox (`PartitionKey.tab`, 14 [CLI-61]): two flushers would drain one outbox
// (10 [AUTH-25]: "Each outbox MUST have exactly one flusher"). A page that finds an id in its `sessionStorage` asks the other tabs of the origin, over
// the BroadcastChannel, whether a live tab holds it; one that does makes this tab take a fresh id. A reload (the old page is gone) keeps its id.

import { describe, expect, it } from "vitest";
import { Engine, StaticManifests, type EngineConfig, type TabHost } from "../src/engine/index.js";
import { MemoryDevice } from "../src/engine/tabs-memory.js";
import { FakeServer, ManualClock, SeededRandom } from "../src/fake/index.js";
import type { BroadcastChannelConstructor } from "../src/platform/channel.js";
import { createWebHost, type WebEnv, type WebTab } from "../src/platform/web.js";
import { FakeIDBFactory } from "./helpers/fake-idb.js";
import { KS, MANIFEST } from "./helpers/engine.js";

const TAB_KEY = "abpc.tabId";
const BC = globalThis.BroadcastChannel as unknown as BroadcastChannelConstructor;

/** The `sessionStorage` of one page: a Map that a duplicate of the tab copies. */
const session = (init?: Map<string, string>): { map: Map<string, string>; api: NonNullable<WebEnv["sessionStorage"]> } => {
  const map = new Map(init);
  return { map, api: { getItem: (k) => map.get(k) ?? null, setItem: (k, v) => void map.set(k, v) } };
};

const idb = new FakeIDBFactory();
const page = (s: ReturnType<typeof session>, o: { channel?: boolean } = {}): WebEnv => ({
  indexedDB: idb,
  sessionStorage: s.api,
  ...(o.channel === false ? {} : { BroadcastChannel: BC }),
});

/** A page that opens: the host, once its id is settled. */
async function open(env: WebEnv, extra: { tabId?: string } = {}): Promise<WebTab> {
  const host = createWebHost({ env, claimWaitMs: 40, ...extra });
  await host.ready();
  return host;
}

describe("V19 N5: a duplicated tab does not share the tab id of the tab it was copied from", () => {
  it("a page whose sessionStorage names an id that a live tab holds takes a fresh one, and keeps it in its own sessionStorage", async () => {
    const sa = session();
    const a = await open(page(sa));
    const x = a.tabId;
    expect(x).not.toBe("");
    expect(sa.map.get(TAB_KEY)).toBe(x);
    // "Duplicate tab": a new page whose sessionStorage is a copy of the first one's
    const sb = session(sa.map);
    expect(sb.map.get(TAB_KEY)).toBe(x);
    const b = await open(page(sb));
    expect(b.tabId).not.toBe(x);
    expect(sb.map.get(TAB_KEY)).toBe(b.tabId);
    expect(a.tabId).toBe(x); // the original is as it was
    expect(sa.map.get(TAB_KEY)).toBe(x);
    // a third copy, of either, is told apart from both
    const c = await open(page(session(sb.map)));
    expect(new Set([a.tabId, b.tabId, c.tabId]).size).toBe(3);
    for (const h of [a, b, c]) h.close();
  });

  it("a reload (the old page is gone) is the same tab: its id is kept, across any number of reloads", async () => {
    const sa = session();
    const first = await open(page(sa));
    const x = first.tabId;
    first.close(); // the page went away: nothing answers for its id
    const second = await open(page(session(sa.map)));
    expect(second.tabId).toBe(x);
    second.close();
    const third = await open(page(session(sa.map)));
    expect(third.tabId).toBe(x);
    third.close();
  });

  it("two duplicates of a tab that has gone, opened together, do not both keep the id: exactly one does", async () => {
    const sa = session();
    const first = await open(page(sa));
    const x = first.tabId;
    first.close();
    const [b, c] = await Promise.all([open(page(session(sa.map))), open(page(session(sa.map)))]);
    expect([b.tabId, c.tabId].filter((t) => t === x)).toHaveLength(1);
    expect(b.tabId).not.toBe(c.tabId);
    b.close();
    c.close();
  });

  it("an id that the app gives is the app's: nothing is asked, and a tab that has the same id is not told apart from it", async () => {
    const a = await open(page(session()), { tabId: "mine" });
    const b = await open(page(session()), { tabId: "mine" });
    expect([a.tabId, b.tabId]).toEqual(["mine", "mine"]);
    a.close();
    b.close();
  });

  it("a platform with no BroadcastChannel cannot ask: the id that sessionStorage names is kept, as before", async () => {
    const sa = session(new Map([[TAB_KEY, "kept"]]));
    const a = await open(page(sa, { channel: false }));
    expect(a.tabId).toBe("kept");
  });

  it("a page with no stored id mints one and answers for it at once: a copy that opens a moment later is told apart", async () => {
    const sa = session();
    const a = await open(page(sa));
    const b = await open(page(session(sa.map)));
    expect(b.tabId).not.toBe(a.tabId);
    a.close();
    b.close();
  });
});

describe("V19 N5: the engine reads the tab id only once the host has settled it", () => {
  it("a host that settles its id late (`ready`) is waited for: the outbox of a tab with no reliable lock is named by the settled id", async () => {
    const clock = new ManualClock();
    const manifests = new StaticManifests(MANIFEST);
    const server = new FakeServer({ manifests, clock });
    server.createKeyspaceNow(KS);
    const device = new MemoryDevice();
    const inner = device.newTab({ tabId: "inherited", reliableLocks: false });
    let id = "inherited";
    let settled = false;
    const host: TabHost = {
      get tabId(): string {
        return id;
      },
      locks: inner.locks,
      storage: inner.storage,
      channel: (name) => inner.channel(name),
      ready: async (): Promise<void> => {
        await new Promise((r) => setTimeout(r, 5));
        id = "fresh";
        settled = true;
      },
    };
    const principal = server.principal("ana", "ana-device");
    const config: EngineConfig = {
      appId: "app",
      principal: { issuer: principal.issuer, sub: principal.sub, deviceId: principal.deviceId },
      transport: server.transport(principal),
      manifests,
      clock,
      random: new SeededRandom(5),
      origin: "https://abpc.test",
      tab: host,
    };
    const engine = await Engine.create(config);
    expect(settled).toBe(true);
    expect(engine.tabId).toBe("fresh");
    expect(engine.partitionKey().tab).toBe("fresh");
    await engine.stop();
  });
});
