// The global teardown of the run: the binaries that the integration tests built into `client/.tmp/` are deleted (the disk is small): the production server
// (harness/server.ts) and the test binary of the CONF-6 test profile (harness/testserver.ts).
// Nothing is built here: the first integration test that needs a binary builds it, and a run without one builds none.

import { removeBinary } from "./harness/server.js";
import { removeTestServerBinary } from "./harness/testserver.js";

export default function setup(): () => void {
  // a binary of an earlier run that was killed is not trusted: this run builds its own
  removeBinary();
  removeTestServerBinary();
  return () => {
    removeBinary();
    removeTestServerBinary();
  };
}
