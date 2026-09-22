import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    globals: true,
    restoreMocks: true,
    exclude: ["tests/sqlite-integrity.test.ts", "tests/durable-orchestration.test.ts"],
  },
});
