import type { ChildProcess } from "node:child_process";
import type { NatsConnection } from "nats";
import type { RunTaskLike } from "./agent-run-rpc.js";

export interface PendingRunSessionDetection {
  sessionId: string;
  sessionFilePath: string;
}

export function createPendingRunSessionTracker<TRunTask extends RunTaskLike>(args: {
  task: TRunTask;
  detectPendingRunSession: () => Promise<PendingRunSessionDetection | null>;
  updateRunSessionMetadata: (metadata: PendingRunSessionDetection) => Promise<void>;
  pollIntervalMs?: number;
}): {
  poll: () => Promise<void>;
  start: () => void;
  stop: () => void;
} {
  let closed = false;
  let timer: ReturnType<typeof setInterval> | null = null;

  const stop = () => {
    closed = true;
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
  };

  const poll = async () => {
    if (closed || args.task.sessionId) {
      stop();
      return;
    }
    const detected = await args.detectPendingRunSession().catch(() => null);
    if (!detected) {
      return;
    }
    await args.updateRunSessionMetadata(detected).catch(() => undefined);
    if (args.task.sessionId) {
      stop();
    }
  };

  const start = () => {
    if (closed || args.task.sessionId || timer) {
      return;
    }
    timer = setInterval(() => {
      void poll();
    }, args.pollIntervalMs ?? 1000);
  };

  return { poll, start, stop };
}

export function attachManagedRunProcessLifecycle<TRunTask extends RunTaskLike>(args: {
  child: ChildProcess;
  task: TRunTask;
  nc: NatsConnection;
  stopPendingSessionPoll: () => void;
  getStoredRun: (runId: string) => Promise<TRunTask | null>;
  publishImmediateRunEvent: (args: {
    nc: NatsConnection;
    userId: string;
    task: TRunTask;
    type?: "run.changed" | "run.finished";
  }) => void;
  removeRunTask: (runId: string) => Promise<void>;
  releaseRunStartSlot: (args: { runId: string; sessionId?: string | null }) => Promise<void>;
  codexAuthCleanup: () => Promise<void>;
  writeRunStatus: (runId: string, message: string) => void;
  formatTimestamp: () => string;
}): void {
  args.child.once("error", (error) => {
    args.stopPendingSessionPoll();
    const message = error instanceof Error ? error.message : String(error);
    args.task.status = "failed";
    args.task.error = message;
    args.task.finishedAt = args.formatTimestamp();
    args.publishImmediateRunEvent({ nc: args.nc, userId: args.task.userId, task: args.task });
    args.publishImmediateRunEvent({ nc: args.nc, userId: args.task.userId, task: args.task, type: "run.finished" });
    void args.removeRunTask(args.task.id).catch(() => undefined);
    void args.releaseRunStartSlot({ runId: args.task.id, sessionId: args.task.sessionId }).catch(() => undefined);
    void args.codexAuthCleanup().catch(() => undefined);
    args.writeRunStatus(args.task.id, `failed error=${message}`);
  });

  args.child.once("close", async (code, signal) => {
    args.stopPendingSessionPoll();
    const latest = await args.getStoredRun(args.task.id).catch(() => null);
    if (latest?.cancelRequested) {
      args.task.cancelRequested = true;
    }
    args.task.resultExitCode = typeof code === "number" ? code : null;
    args.task.resultSignal = signal;
    args.task.finishedAt = args.formatTimestamp();
    args.task.status = args.task.cancelRequested ? "canceled" : (args.task.resultExitCode ?? 1) === 0 ? "completed" : "failed";
    args.task.error = args.task.status === "failed" ? `Command exited with code ${args.task.resultExitCode ?? "null"}` : null;
    args.publishImmediateRunEvent({ nc: args.nc, userId: args.task.userId, task: args.task });
    args.publishImmediateRunEvent({ nc: args.nc, userId: args.task.userId, task: args.task, type: "run.finished" });
    void args.removeRunTask(args.task.id).catch(() => undefined);
    void args.releaseRunStartSlot({ runId: args.task.id, sessionId: args.task.sessionId }).catch(() => undefined);
    void args.codexAuthCleanup().catch(() => undefined);
    args.writeRunStatus(
      args.task.id,
      `completed status=${args.task.status} exitCode=${args.task.resultExitCode ?? "null"} signal=${args.task.resultSignal ?? "null"}`,
    );
  });
}
