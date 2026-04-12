import { spawn, type ChildProcess } from "node:child_process";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import type { AgentJetStreamContext } from "./agent-jetstream.js";
import type { AgentSettingsConfig } from "./agent-settings.js";

export function sendSignalToTaskProcess(child: ChildProcess, signal: NodeJS.Signals): void {
  if (process.platform !== "win32" && typeof child.pid === "number") {
    try {
      process.kill(-child.pid, signal);
      return;
    } catch {
      // Fall back to direct child signaling.
    }
  }
  try {
    child.kill(signal);
  } catch {
    // noop
  }
}

export function sendSignalToPid(pid: number, signal: NodeJS.Signals): void {
  if (process.platform !== "win32") {
    try {
      process.kill(-pid, signal);
      return;
    } catch {
      // Fall back to direct pid signaling.
    }
  }
  process.kill(pid, signal);
}

async function checkCancelRequested(args: {
  serverBaseUrl: string;
  taskId: string;
  userId: string;
  agentToken: string;
  getJson: <T>(url: string) => Promise<T>;
}): Promise<boolean> {
  const query = new URLSearchParams({
    userId: args.userId,
    agentToken: args.agentToken,
  });
  const response = await args.getJson<{ task?: { cancelRequested?: boolean } }>(
    `${args.serverBaseUrl}/api/agent/tasks/${encodeURIComponent(args.taskId)}/events?${query.toString()}`,
  );
  return Boolean(response.task?.cancelRequested);
}

async function syncCodexAuthState(args: {
  source: "agent_local" | "server_bundle";
  authMode?: "api_key" | "oauth";
  apiKey?: string | null;
  authJson?: string | null;
  issuedAt?: string | null;
  expiresAt?: string | null;
}): Promise<{
  envPatch: Record<string, string>;
  cleanup: () => Promise<void>;
  meta: Record<string, unknown>;
}> {
  const envPatch: Record<string, string> = {};
  const synced = false;

  if (args.authMode === "api_key" && args.apiKey) {
    envPatch.OPENAI_API_KEY = args.apiKey;
  }

  return {
    envPatch,
    cleanup: async () => {},
    meta: {
      codexAuthSource: args.source,
      codexAuthMode: args.authMode ?? null,
      codexAuthHasApiKey: Boolean(args.apiKey),
      codexAuthHasAuthJson: Boolean(args.authJson),
      codexAuthIssuedAt: args.issuedAt ?? null,
      codexAuthExpiresAt: args.expiresAt ?? null,
      codexAuthSynced: synced,
    },
  };
}

async function prepareTaskCodexAuth(args: {
  serverBaseUrl: string;
  taskId: string;
  userId: string;
  agentToken: string;
}): Promise<{
  envPatch: Record<string, string>;
  cleanup: () => Promise<void>;
  meta: Record<string, unknown>;
} | null> {
  void args;
  return await syncCodexAuthState({
    source: "agent_local",
    authJson: null,
    issuedAt: null,
    expiresAt: null,
  });
}

export async function prepareCodexAuthBundle(bundle: {
  authMode?: "api_key" | "oauth";
  apiKey?: string | null;
  authJson?: string | null;
  issuedAt?: string | null;
  expiresAt?: string | null;
} | null): Promise<{
  envPatch: Record<string, string>;
  cleanup: () => Promise<void>;
  meta: Record<string, unknown>;
} | null> {
  if (!bundle) {
    return null;
  }
  return await syncCodexAuthState({
    source: "server_bundle",
    authMode: bundle.authMode,
    apiKey: bundle.apiKey,
    authJson: bundle.authJson ?? null,
    issuedAt: bundle.issuedAt ?? null,
    expiresAt: bundle.expiresAt ?? null,
  });
}

export async function runTask(args: {
  serverBaseUrl: string;
  taskId: string;
  command: string;
  cwd: string | null;
  userId: string;
  agentToken: string;
  jetstream: AgentJetStreamContext;
  agentProjectDir: string;
  resolveShellPath: () => string;
  resolveTaskWorkspace: (rawCwd: string | null) => string;
  resolveCodexHomePath: () => string;
  resolveWorkspaceRoot: () => string;
  prepareTaskRuntimeConfig: (args: {
    serverBaseUrl: string;
    taskId: string;
    userId: string;
    agentToken: string;
  }) => Promise<{
    envPatch: Record<string, string>;
    meta: Record<string, unknown>;
  } | null>;
  readAgentSettingsConfig: (args: { workspaceRoot: string }) => Promise<AgentSettingsConfig>;
  buildAgentSettingsEnvPatch: (config: AgentSettingsConfig) => Record<string, string>;
  prepareTaskGitEnv: (args: {
    cwd: string;
    baseEnvPatch: Record<string, string>;
  }) => Promise<{
    envPatch: Record<string, string>;
    meta: Record<string, unknown>;
  }>;
  reserveNextEventSeq: (taskId: string) => number;
  recordAgentEvent: (args: {
    jetstream: AgentJetStreamContext;
    serverBaseUrl: string;
    taskId: string;
    userId: string;
    type: "stdout" | "stderr" | "status" | "meta";
    seq: number;
    payload: Record<string, unknown>;
  }) => Promise<void>;
  persistEventOrFatal: (args: {
    jetstream: AgentJetStreamContext;
    serverBaseUrl: string;
    taskId: string;
    userId: string;
    type: "stdout" | "stderr" | "status" | "meta";
    seq: number;
    payload: Record<string, unknown>;
    context: string;
  }) => void;
  formatLocalTimestamp: (date?: Date) => string;
  sleep: (ms: number) => Promise<void>;
  writeTaskStream: (taskId: string, stream: "stdout" | "stderr", chunk: string) => void;
  writeAgentInfo: (message: string) => void;
  writeAgentError: (message: string) => void;
  getJson: <T>(url: string) => Promise<T>;
  setActiveTaskLogContext: (ctx: {
    jetstream: AgentJetStreamContext;
    serverBaseUrl: string;
    taskId: string;
    userId: string;
  }) => void;
  clearActiveTaskLogContext: (taskId: string) => void;
}): Promise<void> {
  args.setActiveTaskLogContext({
    jetstream: args.jetstream,
    serverBaseUrl: args.serverBaseUrl,
    taskId: args.taskId,
    userId: args.userId,
  });
  const shellPath = args.resolveShellPath();
  const taskWorkspace = args.resolveTaskWorkspace(args.cwd);
  const codexHome = args.resolveCodexHomePath();
  await mkdir(codexHome, { recursive: true });
  const runtimeConfig = await args.prepareTaskRuntimeConfig({
    serverBaseUrl: args.serverBaseUrl,
    taskId: args.taskId,
    userId: args.userId,
    agentToken: args.agentToken,
  });
  const codexAuth = await prepareTaskCodexAuth({
    serverBaseUrl: args.serverBaseUrl,
    taskId: args.taskId,
    userId: args.userId,
    agentToken: args.agentToken,
  });
  const localAgentSettings = await args.readAgentSettingsConfig({ workspaceRoot: args.resolveWorkspaceRoot() });
  const baseTaskEnvPatch = {
    CODEX_HOME: codexHome,
    ...args.buildAgentSettingsEnvPatch(localAgentSettings),
    ...(runtimeConfig?.envPatch ?? {}),
    ...(codexAuth?.envPatch ?? {}),
    WORKSPACE: taskWorkspace,
  };

  const taskGitEnv = await args.prepareTaskGitEnv({
    cwd: taskWorkspace,
    baseEnvPatch: baseTaskEnvPatch,
  });
  await args.recordAgentEvent({
    jetstream: args.jetstream,
    serverBaseUrl: args.serverBaseUrl,
    taskId: args.taskId,
    userId: args.userId,
    type: "meta",
    seq: args.reserveNextEventSeq(args.taskId),
    payload: {
      host: process.platform,
      pid: process.pid,
      startedAt: args.formatLocalTimestamp(),
      command: args.command,
      cwd: taskWorkspace,
      requestedCwd: args.cwd,
      shell: shellPath,
      ...(runtimeConfig?.meta ?? { runtimeConfigSynced: false }),
      ...(codexAuth?.meta ?? { codexAuthSynced: false }),
      ...(taskGitEnv.meta ?? {}),
    },
  });

  try {
    let terminationReason: "cancel" | null = null;
    let cancelStage1Timer: NodeJS.Timeout | null = null;
    let cancelStage2Timer: NodeJS.Timeout | null = null;
    let stopCancelPolling = false;
    let cancelSignalSent = false;

    const runtimeBinPath = path.join(args.agentProjectDir, "runtime/bin");
    const taskPath = [runtimeBinPath, process.env.PATH || ""].filter(Boolean).join(path.delimiter);

    const child = spawn(args.command, {
      cwd: taskWorkspace,
      shell: shellPath,
      detached: process.platform !== "win32",
      env: {
        ...process.env,
        ...baseTaskEnvPatch,
        ...taskGitEnv.envPatch,
        PATH: taskPath,
        DOER_AGENT_TOKEN: args.agentToken,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });

    child.stdout?.setEncoding("utf8");
    child.stderr?.setEncoding("utf8");

    const requestCancel = () => {
      if (cancelSignalSent || terminationReason === "cancel") {
        return;
      }
      cancelSignalSent = true;
      terminationReason = "cancel";
      sendSignalToTaskProcess(child, "SIGINT");
      cancelStage1Timer = setTimeout(() => {
        sendSignalToTaskProcess(child, "SIGTERM");
      }, 1200);
      cancelStage1Timer.unref?.();
      cancelStage2Timer = setTimeout(() => {
        sendSignalToTaskProcess(child, "SIGKILL");
      }, 3500);
      cancelStage2Timer.unref?.();
    };

    child.stdout?.on("data", (chunk: string) => {
      args.writeTaskStream(args.taskId, "stdout", chunk);
      const seq = args.reserveNextEventSeq(args.taskId);
      args.persistEventOrFatal({
        jetstream: args.jetstream,
        serverBaseUrl: args.serverBaseUrl,
        taskId: args.taskId,
        userId: args.userId,
        type: "stdout",
        seq,
        payload: { chunk, at: args.formatLocalTimestamp() },
        context: "stdout persist failed",
      });
    });

    child.stderr?.on("data", (chunk: string) => {
      args.writeTaskStream(args.taskId, "stderr", chunk);
      const seq = args.reserveNextEventSeq(args.taskId);
      args.persistEventOrFatal({
        jetstream: args.jetstream,
        serverBaseUrl: args.serverBaseUrl,
        taskId: args.taskId,
        userId: args.userId,
        type: "stderr",
        seq,
        payload: { chunk, at: args.formatLocalTimestamp() },
        context: "stderr persist failed",
      });
    });

    const cancelPoller = (async () => {
      while (!stopCancelPolling) {
        await args.sleep(5000);
        if (stopCancelPolling || terminationReason === "cancel") {
          continue;
        }
        const cancelRequested = await checkCancelRequested({
          serverBaseUrl: args.serverBaseUrl,
          taskId: args.taskId,
          userId: args.userId,
          agentToken: args.agentToken,
          getJson: args.getJson,
        }).catch(() => false);
        if (!cancelRequested) {
          continue;
        }
        requestCancel();
      }
    })();

    const result = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (code, signal) => {
        resolve({ code, signal });
      });
    }).finally(() => {
      stopCancelPolling = true;
      if (cancelStage1Timer) {
        clearTimeout(cancelStage1Timer);
      }
      if (cancelStage2Timer) {
        clearTimeout(cancelStage2Timer);
      }
    });
    await cancelPoller.catch(() => undefined);

    const canceled = await checkCancelRequested({
      serverBaseUrl: args.serverBaseUrl,
      taskId: args.taskId,
      userId: args.userId,
      agentToken: args.agentToken,
      getJson: args.getJson,
    }).catch(() => false);

    const status = canceled || terminationReason === "cancel"
      ? "canceled"
      : (result.code ?? 1) === 0
        ? "completed"
        : "failed";

    const statusPayload = {
      status,
      exitCode: typeof result.code === "number" ? result.code : null,
      signal: result.signal,
      finishedAt: args.formatLocalTimestamp(),
      error:
        status === "failed"
          ? `Command exited with code ${result.code ?? "null"}`
          : null,
    } satisfies Record<string, unknown>;
    await args.recordAgentEvent({
      jetstream: args.jetstream,
      serverBaseUrl: args.serverBaseUrl,
      taskId: args.taskId,
      userId: args.userId,
      type: "status",
      seq: args.reserveNextEventSeq(args.taskId),
      payload: statusPayload,
    });
    args.writeAgentInfo(
      `task=${args.taskId} status=${status} exitCode=${typeof result.code === "number" ? result.code : "null"} signal=${result.signal ?? "null"}`,
    );
  } finally {
    args.clearActiveTaskLogContext(args.taskId);
    await codexAuth?.cleanup().catch(() => undefined);
  }
}
