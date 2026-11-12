// FBA batch F7, B3-M4: 14 [CLI-66] "a client MUST purge its local copy of the keyspace ... The tabs of one engine share one store ([CLI-61]), so one purge
// covers them". A follower tab shows what the flusher persisted: once the flusher purged a keyspace, the follower shows nothing of it, drops its own
// downloaded blob bytes, and runs its own purge hooks (its query layer's pages).

import { describe, expect, it } from "vitest";
import { LocalRefusal } from "../src/engine/index.js";
import { kc, KS } from "./helpers/engine.js";
import { settleAll, tabsRig } from "./helpers/tabs.js";

describe("B3-M4: 14 [CLI-66], [CLI-61] the purge reaches the follower tabs", () => {
  it("CLI66_AFollowerTabShowsNothingOfAPurgedKeyspaceAndRunsItsPurgeHooks", async () => {
    const r = tabsRig();
    const a = await r.open({ cfg: { copyPolicy: "purge" } });
    const b = await r.open({ cfg: { copyPolicy: "purge" } });
    expect(a.engine.status().role).toBe("flusher");
    expect(b.engine.status().role).toBe("follower");
    const hooked: string[] = [];
    b.engine.registerPurgeHook((ks, cause) => void hooked.push(`${ks}:${cause}`));
    r.server.write(KS, [kc("settings/s", "set", 1)]);
    await settleAll(a.engine, b.engine);
    expect(b.engine.view(KS)).toBeDefined();
    r.server.denyRead("ana", KS); // GRANT_CHANGED with the purge policy: the keyspace stays followed, its copy goes
    await settleAll(a.engine, b.engine);
    expect(() => a.engine.view(KS)).toThrow(LocalRefusal);
    expect(() => b.engine.view(KS)).toThrow(LocalRefusal); // the follower shows nothing of it either
    expect(hooked).toEqual([`${KS}:GRANT_CHANGED`]);
  });
});
