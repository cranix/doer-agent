import assert from "node:assert/strict";
import test from "node:test";
import { CodexThreadHandoffJobManager } from "./codex-thread-handoff-jobs.js";
import type { CodexThreadHandoffResult } from "./codex-thread-handoff.js";

function handoffResult(sourceThreadId: string): CodexThreadHandoffResult {
  return {
    sourceThreadId,
    thread: { id: "target-thread" },
    threadId: "target-thread",
    handoff: "summary",
    turnsCollected: 2,
    turnsOmitted: 0,
    warnings: [],
  };
}

async function nextTick(): Promise<void> {
  await new Promise<void>((resolve) => setImmediate(resolve));
}

test("starts handoff work asynchronously and reports progress", async () => {
  let finish: ((result: CodexThreadHandoffResult) => void) | undefined;
  const statuses: string[] = [];
  const manager = new CodexThreadHandoffJobManager(
    async (input, onProgress) => {
      onProgress("collecting");
      return await new Promise<CodexThreadHandoffResult>((resolve) => {
        finish = resolve;
      });
    },
    {
      onStatus: (job) => statuses.push(job.phase),
    },
  );

  const started = manager.start({ sourceThreadId: "source-thread" });
  assert.equal(started.job.phase, "queued");
  assert.equal(started.deduplicated, false);

  await nextTick();
  assert.equal(manager.get(started.job.jobId)?.phase, "collecting");

  finish?.(handoffResult("source-thread"));
  await nextTick();
  const completed = manager.get(started.job.jobId);
  assert.equal(completed?.phase, "completed");
  assert.equal(completed?.result?.threadId, "target-thread");
  assert.deepEqual(statuses, ["queued", "collecting", "completed"]);
});

test("deduplicates active jobs for the same source thread", () => {
  const manager = new CodexThreadHandoffJobManager(async () => {
    return await new Promise<CodexThreadHandoffResult>(() => undefined);
  });

  const first = manager.start({ sourceThreadId: "source-thread" });
  const second = manager.start({ sourceThreadId: "source-thread" });

  assert.equal(second.deduplicated, true);
  assert.equal(second.job.jobId, first.job.jobId);
});

test("keeps a failed job status for polling", async () => {
  const statuses: Array<{ phase: string; error?: string }> = [];
  const manager = new CodexThreadHandoffJobManager(
    async () => {
      throw new Error("summary failed");
    },
    {
      onStatus: (job) => statuses.push({ phase: job.phase, error: job.error }),
    },
  );

  const started = manager.start({ sourceThreadId: "source-thread" });
  await nextTick();
  await nextTick();

  const failed = manager.get(started.job.jobId);
  assert.equal(failed?.phase, "failed");
  assert.equal(failed?.error, "summary failed");
  assert.deepEqual(statuses, [
    { phase: "queued", error: undefined },
    { phase: "failed", error: "summary failed" },
  ]);
});
