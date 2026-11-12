// FBA batch F7, S-07 (A8-S3): 14 [CLI-61] "where the platform throttles a hidden tab so that it cannot, the flusher MUST hand its role over to a
// visible tab of the engine, when there is one". The handover is on by default where the tab host says that its platform throttles hidden tabs, the
// browser host included.

import { describe, expect, it } from "vitest";
import type { StorageProvider } from "../src/engine/tabs.js";
import { WebTab } from "../src/platform/web.js";
import { settleAll, tabsRig } from "./helpers/tabs.js";

describe("S-07: 14 [CLI-61] the hidden-flusher handover by default", () => {
  it("CLI61_HandoverOnByDefaultWhereTheHostThrottlesHiddenTabs", async () => {
    const r = tabsRig();
    const ta = r.device.newTab();
    const tb = r.device.newTab();
    ta.throttlesHidden = true;
    tb.throttlesHidden = true;
    const a = await r.open({ tab: ta }); // no handOverWhenHidden in the config
    const b = await r.open({ tab: tb });
    expect(a.engine.status().role).toBe("flusher");
    ta.setVisible(false);
    await settleAll(a.engine, b.engine);
    expect(a.engine.status().role).toBe("follower");
    expect(b.engine.status().role).toBe("flusher");
  });

  it("CLI61_ExplicitFalseKeepsTheRole", async () => {
    const r = tabsRig();
    const ta = r.device.newTab();
    ta.throttlesHidden = true;
    const a = await r.open({ tab: ta, cfg: { handOverWhenHidden: false } });
    const b = await r.open();
    ta.setVisible(false);
    await settleAll(a.engine, b.engine);
    expect(a.engine.status().role).toBe("flusher");
  });

  it("CLI61_TheBrowserHostThrottlesHiddenTabs", () => {
    const tab = new WebTab({}, {} as StorageProvider, "t-1");
    expect(tab.throttlesHidden).toBe(true);
    tab.close();
  });
});
