// `describe` for a suite whose data comes from an abcp-server checkout (see server-dir.ts). Kept apart from server-dir.ts, which the global
// setup imports and which therefore must not import vitest.

import { describe, it } from "vitest";
import { SKIP_NEEDS_SERVER } from "./server-dir.js";

/** With `available` false the body is not run, and one skipped test carries the reason. */
export function describeNeeds(available: boolean, name: string, body: () => void): void {
  if (available) {
    describe(name, body);
    return;
  }
  describe.skip(name, () => {
    it(SKIP_NEEDS_SERVER, () => {});
  });
}
