import { mkdir, open, readFile, readdir, rename, rm, unlink, writeFile } from "node:fs/promises";
import path from "node:path";
import { StringCodec, type NatsConnection } from "nats";
import type { RunTaskLike } from "./agent-run-rpc.js";

const runEventsCodec = StringCodec();

export interface ImmediateRunEventLike {
  type: "run.changed" | "run.finished";
  agentId: string;
  sessionId: string | null;
  filePath: string | null;
  runId: string;
  updatedAt: string;
  status: RunTaskLike["status"];
  cancelRequested: boolean;
  resultExitCode: number | null;
  resultSignal: string | null;
  error: string | null;
  finishedAt: string | null;
}

async function resolveRunsDir(workspaceRoot: string): Promise<string> {
  const dir = path.join(workspaceRoot, ".doer-agent", "runs");
  await mkdir(dir, { recursive: true });
  return dir;
}

function sanitizeRunLockSegment(value: string): string {
  return value.trim().replace(/[^a-zA-Z0-9._-]/g, "_").slice(0, 160) || "lock";
}

async function resolveRunLocksDir(workspaceRoot: string): Promise<string> {
  const dir = path.join(await resolveRunsDir(workspaceRoot), "locks");
  await mkdir(dir, { recursive: true });
  return dir;
}

async function resolveRunStartLockPath(args: {
  workspaceRoot: string;
  runId: string;
  sessionId?: string | null;
}): Promise<string> {
  const dir = await resolveRunLocksDir(args.workspaceRoot);
  if (typeof args.sessionId === "string" && args.sessionId.trim()) {
    return path.join(dir, `session__${sanitizeRunLockSegment(args.sessionId)}.lock`);
  }
  return path.join(dir, "pending_new_session.lock");
}

function normalizePersistedRunTask(value: unknown): RunTaskLike | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const record = value as Record<string, unknown>;
  const id = typeof record.runId === "string" && record.runId.trim()
    ? record.runId.trim()
    : typeof record.id === "string" && record.id.trim()
      ? record.id.trim()
      : "";
  const userId = typeof record.userId === "string" ? record.userId : "";
  const agentId = typeof record.agentId === "string" ? record.agentId : "";
  const status = record.status;
  if (!id || !userId || !agentId || !["queued", "running", "completed", "failed", "canceled"].includes(String(status))) {
    return null;
  }
  return {
    id,
    userId,
    agentId,
    processPid: typeof record.processPid === "number" ? record.processPid : null,
    sessionId: typeof record.sessionId === "string" && record.sessionId.trim() ? record.sessionId.trim() : null,
    sessionFilePath: typeof record.sessionFilePath === "string" && record.sessionFilePath.trim() ? record.sessionFilePath.trim() : null,
    status: status as RunTaskLike["status"],
    cancelRequested: Boolean(record.cancelRequested),
    resultExitCode: typeof record.resultExitCode === "number" ? record.resultExitCode : null,
    resultSignal: typeof record.resultSignal === "string" && record.resultSignal.trim() ? record.resultSignal.trim() : null,
    error: typeof record.error === "string" && record.error.trim() ? record.error : null,
    createdAt: typeof record.createdAt === "string" ? record.createdAt : "",
    updatedAt: typeof record.updatedAt === "string" ? record.updatedAt : "",
    startedAt: typeof record.startedAt === "string" && record.startedAt.trim() ? record.startedAt : null,
    finishedAt: typeof record.finishedAt === "string" && record.finishedAt.trim() ? record.finishedAt : null,
  };
}

function buildImmediateRunEvent(task: RunTaskLike, type: ImmediateRunEventLike["type"]): ImmediateRunEventLike {
  return {
    type,
    agentId: task.agentId,
    sessionId: task.sessionId,
    filePath: task.sessionFilePath,
    runId: task.id,
    updatedAt: task.updatedAt,
    status: task.status,
    cancelRequested: task.cancelRequested,
    resultExitCode: task.resultExitCode,
    resultSignal: task.resultSignal,
    error: task.error,
    finishedAt: task.finishedAt,
  };
}

export async function resetRunsDir(workspaceRoot: string): Promise<void> {
  const dir = await resolveRunsDir(workspaceRoot);
  await rm(dir, { recursive: true, force: true }).catch(() => undefined);
  await mkdir(dir, { recursive: true });
}

export async function persistRunTask(workspaceRoot: string, task: RunTaskLike): Promise<void> {
  const dir = await resolveRunsDir(workspaceRoot);
  const payload = {
    runId: task.id,
    agentId: task.agentId,
    userId: task.userId,
    processPid: task.processPid,
    sessionId: task.sessionId,
    sessionFilePath: task.sessionFilePath,
    status: task.status,
    cancelRequested: task.cancelRequested,
    resultExitCode: task.resultExitCode,
    resultSignal: task.resultSignal,
    createdAt: task.createdAt,
    updatedAt: task.updatedAt,
    startedAt: task.startedAt,
    finishedAt: task.finishedAt,
    error: task.error,
  };
  await writeFile(path.join(dir, `${task.id}.json`), `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

export async function removeRunTask(workspaceRoot: string, runId: string): Promise<void> {
  const dir = await resolveRunsDir(workspaceRoot);
  await unlink(path.join(dir, `${runId}.json`)).catch(() => undefined);
}

export async function claimRunStartSlot(args: {
  workspaceRoot: string;
  runId: string;
  sessionId?: string | null;
  formatTimestamp: () => string;
}): Promise<void> {
  const lockPath = await resolveRunStartLockPath(args);
  try {
    const handle = await open(lockPath, "wx");
    try {
      const payload = {
        runId: args.runId,
        sessionId: typeof args.sessionId === "string" && args.sessionId.trim() ? args.sessionId.trim() : null,
        pid: process.pid,
        createdAt: args.formatTimestamp(),
      };
      await handle.writeFile(`${JSON.stringify(payload, null, 2)}\n`, "utf8");
    } finally {
      await handle.close().catch(() => undefined);
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException | null)?.code === "EEXIST") {
      const lockContents = await readFile(lockPath, "utf8").catch(() => "");
      const existingRunId = (() => {
        try {
          const parsed = JSON.parse(lockContents) as { runId?: unknown };
          return typeof parsed.runId === "string" && parsed.runId.trim() ? parsed.runId.trim() : null;
        } catch {
          return null;
        }
      })();
      throw new Error(existingRunId ? `Another run is already active: ${existingRunId}` : "Another run is already active");
    }
    throw error;
  }
}

export async function updateRunStartSlotSession(args: {
  workspaceRoot: string;
  runId: string;
  previousSessionId?: string | null;
  sessionId: string;
  formatTimestamp: () => string;
}): Promise<void> {
  const nextSessionId = args.sessionId.trim();
  if (!nextSessionId) {
    return;
  }
  const previousSessionId = typeof args.previousSessionId === "string" && args.previousSessionId.trim() ? args.previousSessionId.trim() : null;
  if (previousSessionId === nextSessionId) {
    return;
  }
  const currentPath = await resolveRunStartLockPath({
    workspaceRoot: args.workspaceRoot,
    runId: args.runId,
    sessionId: previousSessionId,
  });
  const nextPath = await resolveRunStartLockPath({
    workspaceRoot: args.workspaceRoot,
    runId: args.runId,
    sessionId: nextSessionId,
  });
  if (currentPath === nextPath) {
    return;
  }
  try {
    await rename(currentPath, nextPath);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException | null)?.code;
    if (code === "ENOENT") {
      return;
    }
    if (code === "EEXIST") {
      throw new Error(`Another run is already active for session: ${nextSessionId}`);
    }
    throw error;
  }

  const payload = {
    runId: args.runId,
    sessionId: nextSessionId,
    pid: process.pid,
    createdAt: args.formatTimestamp(),
  };
  await writeFile(nextPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

export async function releaseRunStartSlot(args: {
  workspaceRoot: string;
  runId: string;
  sessionId?: string | null;
}): Promise<void> {
  const paths = new Set<string>();
  paths.add(await resolveRunStartLockPath({ workspaceRoot: args.workspaceRoot, runId: args.runId, sessionId: args.sessionId ?? null }));
  paths.add(await resolveRunStartLockPath({ workspaceRoot: args.workspaceRoot, runId: args.runId, sessionId: null }));
  for (const lockPath of paths) {
    await unlink(lockPath).catch(() => undefined);
  }
}

export function cloneRunTask<TRunTask extends RunTaskLike>(task: TRunTask, _sinceSeq?: number | null): TRunTask {
  return { ...task };
}

export function publishImmediateRunEvent(args: {
  nc: NatsConnection;
  userId: string;
  task: RunTaskLike;
  type?: ImmediateRunEventLike["type"];
  buildRunEventsSubject: (userId: string, agentId: string) => string;
}): void {
  args.nc.publish(
    args.buildRunEventsSubject(args.userId, args.task.agentId),
    runEventsCodec.encode(JSON.stringify(buildImmediateRunEvent(args.task, args.type ?? "run.changed"))),
  );
}

export async function listPersistedRunTasks(workspaceRoot: string): Promise<RunTaskLike[]> {
  const dir = await resolveRunsDir(workspaceRoot);
  const names = await readdir(dir).catch(() => [] as string[]);
  const tasks = await Promise.all(
    names
      .filter((name) => name.endsWith(".json"))
      .map(async (name) => {
        const raw = await readFile(path.join(dir, name), "utf8").catch(() => null);
        if (!raw) {
          return null;
        }
        try {
          return normalizePersistedRunTask(JSON.parse(raw));
        } catch {
          return null;
        }
      }),
  );
  return tasks.filter((task): task is RunTaskLike => task !== null);
}

export async function getStoredRun(workspaceRoot: string, runId: string): Promise<RunTaskLike | null> {
  const persisted = await readFile(path.join(await resolveRunsDir(workspaceRoot), `${runId}.json`), "utf8").catch(() => null);
  if (persisted) {
    try {
      const parsed = normalizePersistedRunTask(JSON.parse(persisted));
      if (parsed) {
        return parsed;
      }
    } catch {
      // Ignore malformed persisted state.
    }
  }
  return null;
}
