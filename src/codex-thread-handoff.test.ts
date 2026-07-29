import assert from "node:assert/strict";
import test from "node:test";
import {
  buildBoundedHandoffTranscript,
  buildThreadHandoffInjectionItems,
  buildThreadHandoffSeedInput,
  summarizeThreadTurn,
} from "./codex-thread-handoff.js";

test("buildThreadHandoffInjectionItems adds the summary to model-visible history", () => {
  assert.deepEqual(buildThreadHandoffInjectionItems("# Thread handoff\n\nContinue the work."), [
    {
      type: "message",
      role: "assistant",
      content: [{
        type: "output_text",
        text: "# Thread handoff\n\nContinue the work.",
      }],
    },
  ]);
});

test("buildThreadHandoffSeedInput creates a real user turn for thread indexing", () => {
  assert.deepEqual(buildThreadHandoffSeedInput(), [{
    type: "text",
    text: "Continue from the handoff summary above and wait for my next request.",
  }]);
});

test("summarizeThreadTurn omits raw command output, diffs, images, and MCP results", () => {
  const summary = summarizeThreadTurn({
    id: "turn-1",
    status: "completed",
    items: [
      {
        type: "userMessage",
        content: [
          { type: "text", text: "Implement the migration" },
          { type: "image", url: `data:image/png;base64,${"a".repeat(10_000)}` },
        ],
      },
      {
        type: "commandExecution",
        command: "npm test",
        aggregatedOutput: `secret-output-${"x".repeat(10_000)}`,
        status: "completed",
        exitCode: 0,
      },
      {
        type: "fileChange",
        changes: [{ path: "src/app.ts", kind: "update", diff: `huge-diff-${"y".repeat(10_000)}` }],
      },
      {
        type: "mcpToolCall",
        server: "example",
        tool: "lookup",
        status: "completed",
        result: { content: `huge-result-${"z".repeat(10_000)}` },
      },
    ],
  });

  assert.match(summary, /Implement the migration/);
  assert.match(summary, /\[image omitted\]/);
  assert.match(summary, /COMMAND: npm test/);
  assert.match(summary, /update src\/app\.ts/);
  assert.match(summary, /MCP: example\/lookup status=completed/);
  assert.doesNotMatch(summary, /secret-output/);
  assert.doesNotMatch(summary, /huge-diff/);
  assert.doesNotMatch(summary, /huge-result/);
  assert.doesNotMatch(summary, /data:image/);
});

test("buildBoundedHandoffTranscript keeps early and recent context within a fixed budget", () => {
  const turns = Array.from({ length: 200 }, (_, index) => ({
    id: `turn-${index}`,
    status: "completed",
    items: [{
      type: "agentMessage",
      text: `${index === 0 ? "ORIGINAL OBJECTIVE " : ""}${index === 199 ? "LATEST STATE " : ""}${"x".repeat(1_500)}`,
    }],
  }));

  const result = buildBoundedHandoffTranscript(turns);

  assert.ok(result.transcript.length <= 160_000);
  assert.match(result.transcript, /ORIGINAL OBJECTIVE/);
  assert.match(result.transcript, /LATEST STATE/);
  assert.ok(result.includedTurns < turns.length);
  assert.ok(result.omittedTurns > 0);
});
