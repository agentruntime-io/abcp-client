// The global teardown of the run: the server binary that the integration tests built into `client/.tmp/` is deleted (the disk is small).
// Nothing is built here: the first integration test that needs the binary builds it (harness/server.ts), and a run without one builds none.

import { removeBinary } from "./harness/server.js";

export default function setup(): () => void {
  removeBinary(); // a binary of an earlier run that was killed is not trusted: this run builds its own
  return () => removeBinary();
}
