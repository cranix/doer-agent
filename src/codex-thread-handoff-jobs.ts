import { randomUUID } from "node:crypto";
import {
  type CodexThreadHandoffGoal,
  type CodexThreadHandoffPhase,
  type CodexThreadHandoffResult,
} from "./codex-thread-handoff.js";

export interface CodexThreadHandoffJobInput {
  sourceThreadId: string;
  targetName?: string;
  activeTurnId?: string;
  latestUserText?: string;
  sourceGoal?: CodexThreadHandoffGoal | null;
}

export interface CodexThreadHandoffJobSnapshot {
  jobId: string;
  sourceThreadId: string;
  phase: CodexThreadHandoffPhase | "queued" | "completed" | "failed";
  createdAt: string;
  updatedAt: string;
  result?: CodexThreadHandoffResult;
  error?: string;
}

export interface CodexThreadHandoffJobStartResult {
  job: CodexThreadHandoffJobSnapshot;
  deduplicated: boolean;
}

type HandoffRunner = (
  input: CodexThreadHandoffJobInput,
  onProgress: (phase: CodexThreadHandoffPhase) => void,
) => Promise<CodexThreadHandoffResult>;

const TERMINAL_PHASES = new Set<CodexThreadHandoffJobSnapshot["phase"]>(["completed", "failed"]);

function copySnapshot(job: CodexThreadHandoffJobSnapshot): CodexThreadHandoffJobSnapshot {
  return {
    ...job,
    ...(job.result ? { result: { ...job.result } } : {}),
  };
}

export class CodexThreadHandoffJobManager {
  private readonly jobs = new Map<string, CodexThreadHandoffJobSnapshot>();
  private readonly activeJobBySourceThreadId = new Map<string, string>();

  constructor(
    private readonly run: HandoffRunner,
    private readonly terminalTtlMs = 30 * 60_000,
    private readonly maxJobs = 100,
  ) {}

  start(input: CodexThreadHandoffJobInput): CodexThreadHandoffJobStartResult {
    this.cleanup();
    const existingJobId = this.activeJobBySourceThreadId.get(input.sourceThreadId);
    const existing = existingJobId ? this.jobs.get(existingJobId) : undefined;
    if (existing && !TERMINAL_PHASES.has(existing.phase)) {
      return { job: copySnapshot(existing), deduplicated: true };
    }
    if (this.jobs.size >= this.maxJobs) {
      throw new Error("too many thread handoff jobs");
    }

    const now = new Date().toISOString();
    const job: CodexThreadHandoffJobSnapshot = {
      jobId: randomUUID(),
      sourceThreadId: input.sourceThreadId,
      phase: "queued",
      createdAt: now,
      updatedAt: now,
    };
    this.jobs.set(job.jobId, job);
    this.activeJobBySourceThreadId.set(input.sourceThreadId, job.jobId);

    setImmediate(() => {
      void this.execute(job.jobId, input);
    });
    return { job: copySnapshot(job), deduplicated: false };
  }

  get(jobId: string): CodexThreadHandoffJobSnapshot | null {
    this.cleanup();
    const job = this.jobs.get(jobId);
    return job ? copySnapshot(job) : null;
  }

  private async execute(jobId: string, input: CodexThreadHandoffJobInput): Promise<void> {
    const updatePhase = (phase: CodexThreadHandoffPhase) => {
      const current = this.jobs.get(jobId);
      if (!current || TERMINAL_PHASES.has(current.phase)) {
        return;
      }
      current.phase = phase;
      current.updatedAt = new Date().toISOString();
    };

    try {
      const result = await this.run(input, updatePhase);
      const current = this.jobs.get(jobId);
      if (current) {
        current.phase = "completed";
        current.result = result;
        current.updatedAt = new Date().toISOString();
      }
    } catch (error) {
      const current = this.jobs.get(jobId);
      if (current) {
        current.phase = "failed";
        current.error = error instanceof Error ? error.message : String(error);
        current.updatedAt = new Date().toISOString();
      }
    } finally {
      if (this.activeJobBySourceThreadId.get(input.sourceThreadId) === jobId) {
        this.activeJobBySourceThreadId.delete(input.sourceThreadId);
      }
    }
  }

  private cleanup(): void {
    const cutoff = Date.now() - this.terminalTtlMs;
    for (const [jobId, job] of this.jobs) {
      if (TERMINAL_PHASES.has(job.phase) && Date.parse(job.updatedAt) < cutoff) {
        this.jobs.delete(jobId);
      }
    }
    if (this.jobs.size < this.maxJobs) {
      return;
    }
    const terminalJobs = [...this.jobs.values()]
      .filter((job) => TERMINAL_PHASES.has(job.phase))
      .sort((left, right) => Date.parse(left.updatedAt) - Date.parse(right.updatedAt));
    while (this.jobs.size >= this.maxJobs && terminalJobs.length > 0) {
      const oldest = terminalJobs.shift();
      if (oldest) {
        this.jobs.delete(oldest.jobId);
      }
    }
  }
}
