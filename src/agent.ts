import { mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { StringCodec, type Msg, type NatsConnection } from "nats";
import {
  buildAgentSettingsEnvPatch,
  readAgentModelInstructions,
  readAgentSettingsConfig,
  resolveAgentModelInstructionsFilePath,
} from "./agent-settings.js";
import { handleFsRpcMessage } from "./agent-fs-rpc.js";
import { handleGitRpcMessage } from "./agent-git-rpc.js";
import { subscribeToCodexAuthRpc } from "./agent-codex-auth-rpc.js";
import {
  buildManagedCodexArgs,
  createLocalCodexCliTools,
  normalizeCodexModel,
  normalizeShellRpcCodexAuthBundle,
  spawnManagedCodexCommand,
  type ShellRpcCodexAuthBundle,
} from "./agent-codex-cli.js";
import { connectBootstrapWithRetry, type AgentJetStreamContext } from "./agent-jetstream.js";
import { prepareCommandExecution } from "./agent-run-execution.js";
import { attachManagedRunProcessLifecycle, createPendingRunSessionTracker } from "./agent-run-lifecycle.js";
import {
  claimRunStartSlot,
  cloneRunTask,
  getStoredRun,
  listPersistedRunTasks,
  persistRunTask,
  publishImmediateRunEvent,
  releaseRunStartSlot,
  resetRunsDir,
  removeRunTask,
  updateRunStartSlotSession,
} from "./agent-run-state.js";
import { runConnectedAgentSession } from "./agent-session-loop.js";
import { subscribeToSkillRpc } from "./agent-skill-rpc.js";
import {
  prepareCodexAuthBundle,
  sendSignalToPid,
  sendSignalToTaskProcess,
} from "./agent-task-execution.js";
import {
  collectSessionJsonlFiles,
  detectPendingRunSession,
  findSessionFilePathBySessionId,
  stopAllSessionWatchers,
  subscribeToSessionRpc,
} from "./agent-session-rpc.js";
import {
  handleNonStartRunRpc,
  normalizeRunRpcRequest,
  publishRunRpcResponse,
  type AgentRunRpcRequest,
} from "./agent-run-rpc.js";
import {
  buildAgentCodexAuthRpcSubject,
  buildAgentFsRpcSubject,
  buildAgentGitRpcSubject,
  buildAgentRunEventsSubject,
  buildAgentRunRpcSubject,
  buildAgentSessionRpcSubject,
  buildAgentSettingsRpcSubject,
  buildAgentSkillRpcSubject,
  formatLocalTimestamp,
  normalizeEnvPatch,
  filterValidRunImagePaths,
  normalizeRunImagePaths,
  parseArgs,
  resolveAgentVersion,
  resolveArgOrEnv,
  resolveContainerReachableServerBaseUrl,
  sanitizeUserId,
  sleep,
  writeRunStatus,
  writeRunStream,
} from "./agent-runtime-utils.js";
import { createRuntimeEnvHelpers } from "./agent-runtime-env.js";
import {
  type AgentEventType,
  createEventPersistenceHelpers,
  getJson,
  heartbeatAgentSession,
  postJson,
} from "./agent-runtime-io.js";
import { handleSettingsRpcMessage } from "./agent-settings-rpc.js";

type CodexAuthBundleResponse = ShellRpcCodexAuthBundle;

interface AgentNatsBootstrapResponse {
  servers?: unknown;
  auth?: {
    token?: unknown;
  } | null;
  agentId?: unknown;
}

const DEFAULT_SERVER_BASE_URL = "https://doer.cranix.net";
const AGENT_MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const AGENT_PROJECT_DIR = path.join(AGENT_MODULE_DIR, "..");
const AGENT_PACKAGE_JSON_PATH = path.join(AGENT_PROJECT_DIR, "package.json");
const HEARTBEAT_INTERVAL_MS = 5_000;
const HEARTBEAT_FAILURE_THRESHOLD = 3;

interface AgentEventEnvelope {
  serverBaseUrl: string;
  userId: string;
  taskId: string;
  type: AgentEventType;
  seq: number;
  payload: Record<string, unknown>;
}

interface PublicRunTask {
  id: string;
  userId: string;
  agentId: string;
  processPid: number | null;
  sessionId: string | null;
  sessionFilePath: string | null;
  status: "queued" | "running" | "completed" | "failed" | "canceled";
  cancelRequested: boolean;
  resultExitCode: number | null;
  resultSignal: string | null;
  error: string | null;
  createdAt: string;
  updatedAt: string;
  startedAt: string | null;
  finishedAt: string | null;
}

interface ActiveTaskLogContext {
  jetstream: AgentJetStreamContext;
  serverBaseUrl: string;
  taskId: string;
  userId: string;
}

let activeTaskLogContext: ActiveTaskLogContext | null = null;
let workspaceRootOverride: string | null = null;

function resolveWorkspaceRoot(): string {
  return workspaceRootOverride ?? (process.env.WORKSPACE?.trim() || process.cwd());
}
const runRpcCodec = StringCodec();

function writeAgentInfo(message: string): void {
  process.stdout.write(`[doer-agent] ${message}\n`);
  eventPersistenceHelpers.emitAgentMetaLog("info", message);
}

function writeAgentError(message: string): void {
  process.stderr.write(`[doer-agent] ${message}\n`);
  eventPersistenceHelpers.emitAgentMetaLog("error", message);
}

function writeAgentInfraError(message: string): void {
  try {
    process.stderr.write(`[doer-agent] ${message}\n`);
  } catch {
    // Keep heartbeat/connectivity failures non-fatal.
  }
}

async function updateRunSessionMetadata(task: PublicRunTask, metadata: {
  sessionId?: string | null;
  sessionFilePath?: string | null;
  nc?: NatsConnection | null;
}): Promise<void> {
  let changed = false;
  const previousSessionId = task.sessionId;
  if (!task.sessionId && typeof metadata.sessionId === "string" && metadata.sessionId.trim()) {
    task.sessionId = metadata.sessionId.trim();
    changed = true;
  }
  if (!task.sessionFilePath && typeof metadata.sessionFilePath === "string" && metadata.sessionFilePath.trim()) {
    task.sessionFilePath = metadata.sessionFilePath.trim();
    changed = true;
  }
  if (!task.sessionFilePath && task.sessionId) {
    const resolvedSessionFilePath = await findSessionFilePathBySessionId(resolveWorkspaceRoot(), task.sessionId).catch(() => null);
    if (resolvedSessionFilePath) {
      task.sessionFilePath = resolvedSessionFilePath;
      changed = true;
    }
  }
  if (!changed) {
    return;
  }
  task.updatedAt = formatLocalTimestamp();
  await persistRunTask(resolveWorkspaceRoot(), task).catch(() => undefined);
  if (metadata.nc) {
    publishImmediateRunEvent({
      nc: metadata.nc,
      userId: task.userId,
      task,
      buildRunEventsSubject: buildAgentRunEventsSubject,
    });
  }
  if (!previousSessionId && task.sessionId) {
    await updateRunStartSlotSession({
      workspaceRoot: resolveWorkspaceRoot(),
      runId: task.id,
      previousSessionId,
      sessionId: task.sessionId,
      formatTimestamp: formatLocalTimestamp,
    }).catch(() => undefined);
  }
}

async function startManagedRun(args: {
  requestId: string;
  runId: string;
  serverBaseUrl: string;
  userId: string;
  agentId: string;
  nc: NatsConnection;
  sessionId?: string | null;
  codexArgs: string[];
  cwd: string | null;
  runtimeEnvPatch: Record<string, string>;
  codexAuthBundle: CodexAuthBundleResponse | null;
  agentToken: string;
}): Promise<PublicRunTask> {
  const prepared = await prepareCommandExecution({
    cwd: args.cwd,
    userId: args.userId,
    taskId: args.runId,
    codexAuthBundle: args.codexAuthBundle,
    runtimeEnvPatch: args.runtimeEnvPatch,
    agentProjectDir: AGENT_PROJECT_DIR,
    resolveShellPath: runtimeEnvHelpers.resolveShellPath,
    resolveTaskWorkspace: runtimeEnvHelpers.resolveTaskWorkspace,
    resolveCodexHomePath: runtimeEnvHelpers.resolveCodexHomePath,
    prepareCodexAuthBundle,
    readAgentSettingsConfig,
    resolveWorkspaceRoot,
    buildAgentSettingsEnvPatch,
    prepareTaskGitEnv: runtimeEnvHelpers.prepareTaskGitEnv,
  });
  const child = spawnManagedCodexCommand({
    codexArgs: args.codexArgs,
    taskWorkspace: prepared.taskWorkspace,
    env: prepared.env,
    agentToken: args.agentToken,
  });

  const now = formatLocalTimestamp();
  const task: PublicRunTask = {
    id: args.runId,
    userId: args.userId,
    agentId: args.agentId,
    processPid: typeof child.pid === "number" ? child.pid : null,
    sessionId: typeof args.sessionId === "string" && args.sessionId.trim() ? args.sessionId.trim() : null,
    sessionFilePath: null,
    status: "running",
    cancelRequested: false,
    resultExitCode: null,
    resultSignal: null,
    error: null,
    createdAt: now,
    updatedAt: now,
    startedAt: now,
    finishedAt: null,
  };

  let pendingSessionPollClosed = false;
  const knownPendingSessionFiles = new Set<string>();
  const pendingSessionTracker = createPendingRunSessionTracker({
    task,
    detectPendingRunSession: async () => detectPendingRunSession(resolveWorkspaceRoot(), knownPendingSessionFiles),
    updateRunSessionMetadata: async (metadata) => updateRunSessionMetadata(task, { ...metadata, nc: args.nc }),
  });
  const stopPendingSessionPoll = () => {
    pendingSessionPollClosed = true;
    pendingSessionTracker.stop();
  };
  const pollPendingSession = async () => {
    if (pendingSessionPollClosed) {
      stopPendingSessionPoll();
      return;
    }
    await pendingSessionTracker.poll();
  };

  if (!task.sessionId) {
    const existingFiles = await collectSessionJsonlFiles(resolveWorkspaceRoot()).catch(() => []);
    for (const file of existingFiles) {
      knownPendingSessionFiles.add(file.filePath);
    }
    pendingSessionTracker.start();
  }

  child.stdout!.on("data", (chunk: string) => writeRunStream(task.id, "stdout", chunk));
  child.stderr!.on("data", (chunk: string) => writeRunStream(task.id, "stderr", chunk));
  attachManagedRunProcessLifecycle({
    child,
    task,
    nc: args.nc,
    stopPendingSessionPoll,
    getStoredRun: (runId) => getStoredRun(resolveWorkspaceRoot(), runId),
    publishImmediateRunEvent: (eventArgs) => publishImmediateRunEvent({
      ...eventArgs,
      buildRunEventsSubject: buildAgentRunEventsSubject,
    }),
    removeRunTask: (runId) => removeRunTask(resolveWorkspaceRoot(), runId),
    releaseRunStartSlot: ({ runId, sessionId }) => releaseRunStartSlot({
      workspaceRoot: resolveWorkspaceRoot(),
      runId,
      sessionId,
    }),
    codexAuthCleanup: prepared.codexAuthCleanup,
    writeRunStatus,
    formatTimestamp: formatLocalTimestamp,
  });

  void persistRunTask(resolveWorkspaceRoot(), task).catch(() => undefined);
  publishImmediateRunEvent({ nc: args.nc, userId: task.userId, task, buildRunEventsSubject: buildAgentRunEventsSubject });
  writeRunStatus(task.id, `started requestId=${args.requestId} cwd=${prepared.taskWorkspace}`);
  if (!task.sessionId) {
    void pollPendingSession();
  }
  return cloneRunTask(task);
}

function subscribeToSettingsRpc(args: {
  jetstream: AgentJetStreamContext;
  userId: string;
  agentId: string;
}): void {
  const subject = buildAgentSettingsRpcSubject(args.userId, args.agentId);
  args.jetstream.nc.subscribe(subject, {
    callback: (error, msg) => {
      if (error) {
        const message = error instanceof Error ? error.message : String(error);
        writeAgentError(`settings rpc subscription error: ${message}`);
        return;
      }
      void handleSettingsRpcMessage({
        msg,
        nc: args.jetstream.nc,
        agentId: args.agentId,
        workspaceRoot: resolveWorkspaceRoot(),
        onError: writeAgentError,
      });
    },
  });
  writeAgentInfo(`settings rpc subscribed subject=${subject}`);
}

function subscribeToGitRpc(args: {
  jetstream: AgentJetStreamContext;
  userId: string;
  agentId: string;
}): void {
  const subject = buildAgentGitRpcSubject(args.userId, args.agentId);
  args.jetstream.nc.subscribe(subject, {
    callback: (error, msg) => {
      if (error) {
        const message = error instanceof Error ? error.message : String(error);
        writeAgentError(`git rpc subscription error: ${message}`);
        return;
      }
      void handleGitRpcMessage({
        msg,
        nc: args.jetstream.nc,
        agentId: args.agentId,
        onError: writeAgentError,
      });
    },
  });
  writeAgentInfo(`git rpc subscribed subject=${subject}`);
}

async function handleRunRpcMessage(args: {
  msg: Msg;
  jetstream: AgentJetStreamContext;
  serverBaseUrl: string;
  userId: string;
  agentId: string;
  agentToken: string;
}): Promise<void> {
  let requestId = "unknown";
  let responseSubject = "";
  try {
    const payload = JSON.parse(runRpcCodec.decode(args.msg.data)) as AgentRunRpcRequest;
    const request = normalizeRunRpcRequest<CodexAuthBundleResponse>({
      request: payload,
      agentId: args.agentId,
      normalizeModel: normalizeCodexModel,
      normalizeImagePaths: normalizeRunImagePaths,
      normalizeEnvPatch,
      normalizeCodexAuthBundle: normalizeShellRpcCodexAuthBundle,
    });
    requestId = request.requestId;
    responseSubject = request.responseSubject;

    if (request.action === "start") {
      const runId = request.runId ?? requestId;
      await claimRunStartSlot({
        workspaceRoot: resolveWorkspaceRoot(),
        runId,
        sessionId: request.sessionId,
        formatTimestamp: formatLocalTimestamp,
      });
      try {
        const workspaceRoot = resolveWorkspaceRoot();
        const localAgentSettings = await readAgentSettingsConfig({ workspaceRoot });
        const customInstructions = await readAgentModelInstructions(workspaceRoot);
        const validImagePaths = await filterValidRunImagePaths({
          workspaceRoot,
          imagePaths: request.imagePaths,
          onInvalidImage: (imagePath, reason) => {
            writeRunStatus(runId, `skipping invalid image path=${imagePath} reason=${reason}`);
          },
        });
        const task = await startManagedRun({
          requestId,
          runId,
          serverBaseUrl: args.serverBaseUrl,
          userId: args.userId,
          agentId: args.agentId,
          nc: args.jetstream.nc,
          sessionId: request.sessionId,
          codexArgs: buildManagedCodexArgs({
            prompt: request.prompt ?? "",
            imagePaths: validImagePaths,
            sessionId: request.sessionId,
            model: request.model,
            personality: localAgentSettings.general.personality,
            modelInstructionsFile: customInstructions ? resolveAgentModelInstructionsFilePath(workspaceRoot) : null,
          }),
          cwd: request.cwd,
          runtimeEnvPatch: request.runtimeEnvPatch,
          codexAuthBundle: request.codexAuthBundle,
          agentToken: args.agentToken,
        });
        publishRunRpcResponse({ nc: args.jetstream.nc, responseSubject, payload: { requestId, ok: true, task } });
      } catch (error) {
        await releaseRunStartSlot({
          workspaceRoot: resolveWorkspaceRoot(),
          runId,
          sessionId: request.sessionId,
        }).catch(() => undefined);
        throw error;
      }
      return;
    }
    await handleNonStartRunRpc({
      request,
      nc: args.jetstream.nc,
      userId: args.userId,
      agentId: args.agentId,
      listPersistedRunTasks: async () => listPersistedRunTasks(resolveWorkspaceRoot()),
      cloneRunTask,
      getStoredRun: async (runId) => getStoredRun(resolveWorkspaceRoot(), runId),
      persistRunTask: async (task) => persistRunTask(resolveWorkspaceRoot(), task),
      publishImmediateRunEvent: (task) => publishImmediateRunEvent({
        nc: args.jetstream.nc,
        userId: task.userId,
        task,
        buildRunEventsSubject: buildAgentRunEventsSubject,
      }),
      writeRunStatus,
      sendSignalToPid,
      formatTimestamp: formatLocalTimestamp,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (responseSubject) {
      publishRunRpcResponse({
        nc: args.jetstream.nc,
        responseSubject,
        payload: { requestId, ok: false, error: message },
      });
    }
    writeAgentError(`run rpc failed requestId=${requestId} error=${message}`);
  }
}

function subscribeToRunRpc(args: {
  jetstream: AgentJetStreamContext;
  serverBaseUrl: string;
  userId: string;
  agentId: string;
  agentToken: string;
}): void {
  const subject = buildAgentRunRpcSubject(args.userId, args.agentId);
  args.jetstream.nc.subscribe(subject, {
    callback: (error, msg) => {
      if (error) {
        const message = error instanceof Error ? error.message : String(error);
        writeAgentError(`run rpc subscription error: ${message}`);
        return;
      }
      void handleRunRpcMessage({
        msg,
        jetstream: args.jetstream,
        serverBaseUrl: args.serverBaseUrl,
        userId: args.userId,
        agentId: args.agentId,
        agentToken: args.agentToken,
      });
    },
  });
  writeAgentInfo(`run rpc subscribed subject=${subject}`);
}

function subscribeToFsRpc(args: {
  jetstream: AgentJetStreamContext;
  serverBaseUrl: string;
  userId: string;
  agentId: string;
  agentToken: string;
}): void {
  const subject = buildAgentFsRpcSubject(args.userId, args.agentId);
  args.jetstream.nc.subscribe(subject, {
    callback: (error, msg) => {
      if (error) {
        const message = error instanceof Error ? error.message : String(error);
        writeAgentError(`fs rpc subscription error: ${message}`);
        return;
      }
      void handleFsRpcMessage({
        msg,
        workspaceRoot: resolveWorkspaceRoot(),
        serverBaseUrl: args.serverBaseUrl,
        agentId: args.agentId,
        agentToken: args.agentToken,
        onError: writeAgentError,
      });
    },
  });
  writeAgentInfo(`fs rpc subscribed subject=${subject}`);
}

const runtimeEnvHelpers = createRuntimeEnvHelpers({
  resolveWorkspaceRoot,
  agentProjectDir: AGENT_PROJECT_DIR,
});

const localCodexCliTools = createLocalCodexCliTools({
  resolveWorkspaceRoot,
  resolveCodexHomePath: runtimeEnvHelpers.resolveCodexHomePath,
  resolveShellPath: runtimeEnvHelpers.resolveShellPath,
  sendSignalToTaskProcess,
});

const eventPersistenceHelpers = createEventPersistenceHelpers<AgentJetStreamContext>({
  getActiveTaskLogContext: () => activeTaskLogContext,
  publishEvent: async (args) => {
    await args.jetstream.js.publish(
      args.jetstream.subject,
      args.jetstream.codec.encode({
        serverBaseUrl: args.serverBaseUrl,
        userId: args.userId,
        taskId: args.taskId,
        type: args.type,
        seq: args.seq,
        payload: args.payload,
      } satisfies AgentEventEnvelope),
    );
  },
  formatTimestamp: formatLocalTimestamp,
  onError: writeAgentError,
  sleep,
});

const heartbeatSession = async (args: {
  nc: NatsConnection;
  serverBaseUrl: string;
  userId: string;
  agentToken: string;
}): Promise<void> => {
  await heartbeatAgentSession({
    ...args,
    postJson,
  });
};

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const workspaceDir = resolveArgOrEnv(args, ["workspace-dir", "workspaceDir"], ["WORKSPACE"]);
  const startupWorkspaceRoot = path.resolve(workspaceDir || process.cwd());
  workspaceRootOverride = startupWorkspaceRoot;
  process.chdir(startupWorkspaceRoot);
  process.env.WORKSPACE = startupWorkspaceRoot;
  process.env.CODEX_HOME = path.join(startupWorkspaceRoot, ".codex");
  await mkdir(process.env.CODEX_HOME, { recursive: true }).catch(() => undefined);
  await resetRunsDir(resolveWorkspaceRoot());

  const serverBaseUrlRaw = resolveArgOrEnv(args, ["server", "url"], ["DOER_AGENT_SERVER"], DEFAULT_SERVER_BASE_URL);
  const requestedServerBaseUrl = serverBaseUrlRaw.replace(/\/$/, "");
  const serverBaseUrl = resolveContainerReachableServerBaseUrl(requestedServerBaseUrl);
  const usesDefaultServer = requestedServerBaseUrl === DEFAULT_SERVER_BASE_URL;
  const userId = resolveArgOrEnv(args, ["user-id", "userId"], ["DOER_AGENT_USER_ID"]);
  const agentSecret = resolveArgOrEnv(args, ["agent-secret", "agentSecret"], ["DOER_AGENT_SECRET"]);
  if (!userId || !agentSecret) {
    throw new Error("user-id and agent-secret are required");
  }
  const agentToken = agentSecret;
  const agentVersion = await resolveAgentVersion(AGENT_PACKAGE_JSON_PATH);
  let bannerShown = false;

  while (true) {
    const { natsBootstrap, jetstream } = await connectBootstrapWithRetry<AgentNatsBootstrapResponse>({
      serverBaseUrl,
      userId,
      agentToken,
      postJson,
      sanitizeUserId,
      onInfraError: writeAgentInfraError,
      onError: writeAgentError,
      sleep,
    });
    const initialAgentId = typeof natsBootstrap.agentId === "string" ? natsBootstrap.agentId : "";
    if (!initialAgentId) {
      throw new Error("agent id missing from bootstrap");
    }

    if (!bannerShown) {
      process.stdout.write(`\n[doer-agent v${agentVersion}]\n`);
      if (!usesDefaultServer) {
        process.stdout.write(`- server: ${serverBaseUrl}\n`);
      }
      process.stdout.write(`- userId: ${userId}\n`);
      process.stdout.write(`- agentId: ${initialAgentId}\n`);
      process.stdout.write(`\n- transport: nats\n`);
      process.stdout.write(`- natsServers: ${jetstream.servers.join(",")}\n`);
      process.stdout.write(`- natsStream: ${jetstream.stream}\n`);
      process.stdout.write(`- natsSubject: ${jetstream.subject}\n`);
      process.stdout.write(`- natsDurable: ${jetstream.durable}\n\n`);
      process.stdout.write(`- workspace: ${process.cwd()}\n\n`);
      if (requestedServerBaseUrl !== serverBaseUrl) {
        writeAgentInfo(
          `detected container runtime, server endpoint rewritten: ${requestedServerBaseUrl} -> ${serverBaseUrl}`,
        );
      }
      bannerShown = true;
    } else {
      writeAgentInfraError(
        `nats session restored agentId=${initialAgentId} servers=${jetstream.servers.join(",")} at=${formatLocalTimestamp()}`,
      );
    }

    await runConnectedAgentSession({
      jetstream,
      serverBaseUrl,
      userId,
      agentToken,
      heartbeatIntervalMs: HEARTBEAT_INTERVAL_MS,
      heartbeatFailureThreshold: HEARTBEAT_FAILURE_THRESHOLD,
      formatTimestamp: formatLocalTimestamp,
      heartbeatAgentSession: heartbeatSession,
      subscribeAll: () => {
        subscribeToFsRpc({
          jetstream,
          serverBaseUrl,
          userId,
          agentId: initialAgentId,
          agentToken,
        });
        subscribeToSessionRpc({
          nc: jetstream.nc,
          subject: buildAgentSessionRpcSubject(userId, initialAgentId),
          agentId: initialAgentId,
          workspaceRoot: resolveWorkspaceRoot(),
          onInfo: writeAgentInfo,
          onError: writeAgentError,
          formatTimestamp: formatLocalTimestamp,
        });
        subscribeToCodexAuthRpc({
          nc: jetstream.nc,
          subject: buildAgentCodexAuthRpcSubject(userId, initialAgentId),
          agentId: initialAgentId,
          workspaceRoot: resolveWorkspaceRoot(),
          buildLocalCodexCliCommand: localCodexCliTools.buildLocalCodexCliCommand,
          resolveShellPath: runtimeEnvHelpers.resolveShellPath,
          resolveCodexHomePath: runtimeEnvHelpers.resolveCodexHomePath,
          runLocalCodexCli: localCodexCliTools.runLocalCodexCli,
          runLocalCodexCliWithInput: localCodexCliTools.runLocalCodexCliWithInput,
          sendSignalToTaskProcess,
          stripAnsi: localCodexCliTools.stripAnsi,
          onInfo: writeAgentInfo,
          onError: writeAgentError,
        });
        subscribeToSettingsRpc({
          jetstream,
          userId,
          agentId: initialAgentId,
        });
        subscribeToGitRpc({
          jetstream,
          userId,
          agentId: initialAgentId,
        });
        subscribeToSkillRpc({
          nc: jetstream.nc,
          subject: buildAgentSkillRpcSubject(userId, initialAgentId),
          agentId: initialAgentId,
          workspaceRoot: resolveWorkspaceRoot(),
          resolveCodexHomePath: runtimeEnvHelpers.resolveCodexHomePath,
          readAgentSettingsConfig,
          buildAgentSettingsEnvPatch,
          runLocalCodexCli: localCodexCliTools.runLocalCodexCli,
          stripAnsi: localCodexCliTools.stripAnsi,
          onInfo: writeAgentInfo,
          onError: writeAgentError,
        });
        subscribeToRunRpc({
          jetstream,
          serverBaseUrl,
          userId,
          agentId: initialAgentId,
          agentToken,
        });
      },
      stopAllSessionWatchers: () => stopAllSessionWatchers({ onError: writeAgentError }),
      onInfraError: writeAgentInfraError,
      sleep,
    });
  }
}
main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  writeAgentError(`fatal: ${message}`);
  process.exitCode = 1;
});
