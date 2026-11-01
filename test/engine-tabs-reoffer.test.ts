// V19 SHOULD-1 / V14 MUST-3 residual, 14 [CLI-61]: "the flusher MUST hand its role over to a visible tab of the engine, when there is one". A flusher that
// went hidden while no tab was visible keeps the role; when a tab becomes visible later, or a visible tab joins, the role is offered again.

import { describe, expect, it } from "vitest";
import { settleAll, tabsRig } from "./helpers/tabs.js";

const hidden = { handOverWhenHidden: true };

describe("V19: a hidden flusher offers the role again when a tab becomes visible", () => {
  it("A hidden while B is hidden keeps the role; then B is shown: B is the flusher, and A a follower", async () => {
    const r = tabsRig();
    const a = await r.open({ cfg: hidden });
    const b = await r.open({ tab: { visible: false }, cfg: hidden });
    await settleAll(a.engine, b.engine);
    a.tab.setVisible(false);
    await r.clock.advance(5_000);
    await settleAll(a.engine, b.engine);
    expect([a.engine.status().role, b.engine.status().role]).toEqual(["flusher", "follower"]); // no visible tab: the role stays
    b.tab.setVisible(true);
    await r.clock.advance(5_000);
    await settleAll(a.engine, b.engine);
    await r.clock.advance(5_000);
    await settleAll(a.engine, b.engine);
    expect([a.engine.status().role, b.engine.status().role]).toEqual(["follower", "flusher"]);
    expect(a.events.some((e) => e.type === "role" && e.cause === "HANDED_OVER")).toBe(true);
  });

  it("a visible tab that joins later is given the role by the hidden flusher", async () => {
    const r = tabsRig();
    const a = await r.open({ cfg: hidden });
    a.tab.setVisible(false);
    await r.clock.advance(5_000);
    await settleAll(a.engine);
    expect(a.engine.status().role).toBe("flusher"); // alone: nobody to hand it to
    const c = await r.open({ cfg: hidden });
    await r.clock.advance(5_000);
    await settleAll(a.engine, c.engine);
    await r.clock.advance(5_000);
    await settleAll(a.engine, c.engine);
    expect([a.engine.status().role, c.engine.status().role]).toEqual(["follower", "flusher"]);
  });

  it("a visible flusher keeps the role when another tab is shown, and a hidden one does not offer it to a tab that is hidden", async () => {
    const r = tabsRig();
    const a = await r.open({ cfg: hidden });
    const b = await r.open({ tab: { visible: false }, cfg: hidden });
    await settleAll(a.engine, b.engine);
    b.tab.setVisible(true);
    await r.clock.advance(5_000);
    await settleAll(a.engine, b.engine);
    expect([a.engine.status().role, b.engine.status().role]).toEqual(["flusher", "follower"]);
    b.tab.setVisible(false);
    a.tab.setVisible(false);
    await r.clock.advance(5_000);
    await settleAll(a.engine, b.engine);
    expect([a.engine.status().role, b.engine.status().role]).toEqual(["flusher", "follower"]);
  });
});
