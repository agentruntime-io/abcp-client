import { describe, expect, it } from "vitest";
import { set } from "../src/engine/index.js";
import { KS, rig, text, types } from "./helpers/engine.js";

describe("smoke", () => {
  it("loads, submits, commits", async () => {
    const r = await rig();
    expect(r.engine.status().connection).toBe("live");
    expect(r.engine.cursor(KS)?.loaded).toBe(true);
    const { mutationId } = await r.engine.submit({ keyspaceId: KS, opType: "Set", changes: [set("settings/theme", "dark")] });
    await r.engine.settle();
    expect(text(r.server.value(KS, "settings/theme"))).toBe('"dark"');
    expect(types(r, mutationId)).toEqual(["committed"]);
    expect(r.engine.outbox()).toHaveLength(0);
    expect(r.engine.internalErrors).toEqual([]);
  });
});
