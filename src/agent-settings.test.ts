import assert from "node:assert/strict";
import test from "node:test";
import { normalizeAgentSettingsConfig } from "./agent-settings.js";

test("preserves max and ultra Codex reasoning efforts", () => {
  for (const reasoningEffort of ["max", "ultra"] as const) {
    const config = normalizeAgentSettingsConfig({
      codex: { reasoningEffort },
    });

    assert.equal(config.codex.reasoningEffort, reasoningEffort);
  }
});
