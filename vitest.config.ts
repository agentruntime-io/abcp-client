import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    // the fold vectors fold a few thousand records, some of them megabytes of bytes, through a pure TypeScript SHA-256
    testTimeout: 120_000,
    // the integration tests (test/integration) build the Go server once and delete the binary at the end; they skip with a message without Go
    globalSetup: ["test/integration/global-setup.ts"],
  },
});
