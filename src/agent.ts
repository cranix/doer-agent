import { spawn, spawnSync } from "node:child_process";
import { existsSync, statSync, watch } from "node:fs";
import { chmod, mkdir, open, readFile, readdir, rename, rm, rmdir, stat, unlink, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { AckPolicy, connect, DeliverPolicy, JSONCodec, RetentionPolicy, StorageType, StringCodec, type JetStreamClient, type JetStreamManager, type Msg, type NatsConnection } from "nats";

interface PollResponse {
  task: {
    id: string;
    command: string;
    cwd: string | null;
    cancelRequested: boolean;
  } | null;
  error?: string;
}

interface CodexAuthBundleResponse {
  taskId?: string;
  authMode?: "api_key" | "oauth";
  issuedAt?: string;
  expiresAt?: string;
  authJson?: string;
  apiKey?: string | null;
}

interface RuntimeConfigBundleResponse {
  taskId?: string;
  issuedAt?: string;
  expiresAt?: string;
  envPatch?: Record<string, string> | null;
  meta?: Record<string, unknown> | null;
}

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

type AgentEventType = "stdout" | "stderr" | "status" | "meta";

interface AgentEventEnvelope {
  serverBaseUrl: string;
  userId: string;
  taskId: string;
  type: AgentEventType;
  seq: number;
  payload: Record<string, unknown>;
}

interface AgentJetStreamContext {
  nc: NatsConnection;
  js: JetStreamClient;
  jsm: JetStreamManager;
  codec: ReturnType<typeof JSONCodec<AgentEventEnvelope>>;
  subject: string;
  stream: string;
  durable: string;
  servers: string[];
}

type AgentFsRpcAction = "list" | "stat" | "fetch_file" | "read_text" | "read_file" | "write_file" | "download_file";

interface AgentFsRpcRequest {
  requestId?: unknown;
  action?: unknown;
  path?: unknown;
  contentBase64?: unknown;
  downloadPath?: unknown;
  offset?: unknown;
  length?: unknown;
  limit?: unknown;
  maxBytes?: unknown;
  encoding?: unknown;
  uploadUrl?: unknown;
  agentId?: unknown;
}

type AgentSessionRpcAction = "list" | "messages" | "delete" | "watch" | "stop_watch";

interface AgentSessionRpcRequest {
  requestId?: unknown;
  action?: unknown;
  agentId?: unknown;
  filePath?: unknown;
  sessionId?: unknown;
  sinceLine?: unknown;
  beforeRowId?: unknown;
  pageSize?: unknown;
  responseSubject?: unknown;
  watchId?: unknown;
}

interface AgentSessionRpcResponse {
  requestId: string;
  ok: boolean;
  action?: AgentSessionRpcAction;
  sessions?: unknown[];
  rawRows?: unknown[];
  nextCursor?: number;
  hasMoreBefore?: boolean;
  oldestRowId?: number | null;
  watchId?: string | null;
  event?: Record<string, unknown> | null;
  error?: string;
}

interface AgentSessionRpcNormalizedRequest {
  requestId: string;
  action: AgentSessionRpcAction;
  agentId: string;
  filePath: string | null;
  sessionId: string | null;
  sinceLine: number;
  beforeRowId: number | null;
  pageSize: number;
  responseSubject: string;
  watchId: string | null;
}

interface AgentShellRpcRequest {
  kind?: unknown;
  requestId?: unknown;
  command?: unknown;
  patch?: unknown;
  cwd?: unknown;
  timeoutMs?: unknown;
  responseSubject?: unknown;
  agentId?: unknown;
  runtimeEnvPatch?: unknown;
  codexAuth?: unknown;
}

interface AgentRunRpcRequest {
  requestId?: unknown;
  action?: unknown;
  runId?: unknown;
  prompt?: unknown;
  sessionId?: unknown;
  model?: unknown;
  cwd?: unknown;
  responseSubject?: unknown;
  agentId?: unknown;
  sinceSeq?: unknown;
  limit?: unknown;
  runtimeEnvPatch?: unknown;
  codexAuth?: unknown;
}

interface AgentCodexAuthRpcRequest {
  requestId?: unknown;
  responseSubject?: unknown;
  agentId?: unknown;
  action?: unknown;
}

interface AgentCodexAuthRpcResponse {
  requestId: string;
  ok: boolean;
  loggedIn?: boolean;
  output?: string;
  verificationUri?: string | null;
  userCode?: string | null;
  error?: string;
}

type AgentSettingsRpcAction = "get" | "update";

interface AgentSettingsConfig {
  general: {
    firstTurnPrompt: string | null;
  };
  codex: {
    model: string;
    authMode: "api_key" | "oauth";
    apiKey: string | null;
  };
  realtime: {
    model: string;
    voice: string;
    wakeName: string | null;
    requireWakeName: boolean;
    apiKey: string | null;
  };
  git: {
    enabled: boolean;
    name: string | null;
    email: string | null;
    authMode: "none" | "oauth_app";
    oauthToken: string | null;
    oauthLogin: string | null;
    oauthScope: string | null;
  };
  aws: {
    enabled: boolean;
    accessKeyId: string | null;
    defaultRegion: string | null;
    secretAccessKey: string | null;
    sessionToken: string | null;
  };
  jira: {
    baseUrl: string | null;
    email: string | null;
    enabled: boolean;
    apiToken: string | null;
  };
  notion: {
    baseUrl: string | null;
    version: string | null;
    enabled: boolean;
    apiToken: string | null;
  };
  slack: {
    baseUrl: string | null;
    enabled: boolean;
    botToken: string | null;
  };
  figma: {
    baseUrl: string | null;
    enabled: boolean;
    apiToken: string | null;
  };
}

interface AgentSettingsPublic {
  general: {
    firstTurnPrompt: string | null;
  };
  codex: {
    model: string;
    authMode: "api_key" | "oauth";
    hasApiKey: boolean;
    apiKeyMasked: string | null;
    apiKeyLength: number | null;
  };
  realtime: {
    model: string;
    voice: string;
    wakeName: string | null;
    requireWakeName: boolean;
    hasApiKey: boolean;
    apiKeyMasked: string | null;
    apiKeyLength: number | null;
  };
  git: {
    enabled: boolean;
    name: string | null;
    email: string | null;
    authMode: "none" | "oauth_app";
    hasOauthToken: boolean;
    oauthTokenMasked: string | null;
    oauthTokenLength: number | null;
    oauthLogin: string | null;
    oauthScope: string | null;
  };
  aws: {
    enabled: boolean;
    accessKeyId: string | null;
    defaultRegion: string | null;
    hasSecretAccessKey: boolean;
    secretAccessKeyMasked: string | null;
    secretAccessKeyLength: number | null;
    hasSessionToken: boolean;
    sessionTokenMasked: string | null;
    sessionTokenLength: number | null;
  };
  jira: {
    baseUrl: string | null;
    email: string | null;
    enabled: boolean;
    hasApiToken: boolean;
    apiTokenMasked: string | null;
    apiTokenLength: number | null;
  };
  notion: {
    baseUrl: string | null;
    version: string | null;
    enabled: boolean;
    hasApiToken: boolean;
    apiTokenMasked: string | null;
    apiTokenLength: number | null;
  };
  slack: {
    baseUrl: string | null;
    enabled: boolean;
    hasBotToken: boolean;
    botTokenMasked: string | null;
    botTokenLength: number | null;
  };
  figma: {
    baseUrl: string | null;
    enabled: boolean;
    hasApiToken: boolean;
    apiTokenMasked: string | null;
    apiTokenLength: number | null;
  };
}

interface AgentSettingsRpcRequest {
  requestId?: unknown;
  responseSubject?: unknown;
  agentId?: unknown;
  action?: unknown;
  patch?: unknown;
  defaults?: unknown;
}

interface AgentSettingsRpcResponse {
  requestId: string;
  ok: boolean;
  settings?: AgentSettingsPublic;
  error?: string;
}

interface AgentGitRpcRequest {
  requestId?: unknown;
  responseSubject?: unknown;
  agentId?: unknown;
  targetPath?: unknown;
  base?: unknown;
  target?: unknown;
  mergeBase?: unknown;
  staged?: unknown;
  format?: unknown;
  contextLines?: unknown;
  ignoreWhitespace?: unknown;
  diffAlgorithm?: unknown;
  findRenames?: unknown;
  pathspecs?: unknown;
}

interface AgentGitRpcResponse {
  requestId: string;
  ok: boolean;
  payload?: Record<string, unknown> | null;
  error?: string;
}

interface AgentRunRpcResponse {
  requestId: string;
  ok: boolean;
  task?: PublicRunTask | null;
  tasks?: PublicRunTask[];
  error?: string;
}

type AgentRunRpcAction = "start" | "get" | "cancel" | "list";

interface AgentRunRpcNormalizedRequest {
  requestId: string;
  action: AgentRunRpcAction;
  runId: string | null;
  prompt: string | null;
  sessionId: string | null;
  model: string;
  cwd: string | null;
  responseSubject: string;
  sinceSeq: number | null;
  limit: number;
  runtimeEnvPatch: Record<string, string>;
  codexAuthBundle: CodexAuthBundleResponse | null;
}

interface PublicRunTask {
  id: string;
  userId: string;
  agentId: string;
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

interface ActiveRunRecord {
  task: PublicRunTask;
  child: ReturnType<typeof spawn>;
  requestCancel: () => void;
}

interface AgentShellRpcResponse {
  requestId: string;
  ok: boolean;
  exitCode: number | null;
  signal: string | null;
  stdout: string;
  stderr: string;
  error?: string;
}

interface AgentShellRpcNormalizedRequest {
  kind: "shell" | "apply_patch";
  requestId: string;
  command: string | null;
  patch: string | null;
  cwd: string | null;
  timeoutMs: number;
  responseSubject: string;
  runtimeEnvPatch: Record<string, string>;
  codexAuthBundle: CodexAuthBundleResponse | null;
}

interface ActiveTaskLogContext {
  jetstream: AgentJetStreamContext;
  serverBaseUrl: string;
  taskId: string;
  userId: string;
}

let activeTaskLogContext: ActiveTaskLogContext | null = null;
const activeTaskCancelRequests = new Map<string, () => void>();
let workspaceRootOverride: string | null = null;
const fsRpcCodec = StringCodec();
const shellRpcCodec = StringCodec();
const runRpcCodec = StringCodec();
const sessionRpcCodec = StringCodec();
const codexAuthRpcCodec = StringCodec();
const settingsRpcCodec = StringCodec();
const gitRpcCodec = StringCodec();
const activeRuns = new Map<string, ActiveRunRecord>();
const retainedRuns = new Map<string, PublicRunTask>();
const activeSessionWatchers = new Map<string, () => void>();
const sessionLineIndexCache = new Map<string, SessionLineIndexCacheEntry>();
const ANSI_RE = /\u001b\[[0-9;]*m/g;

interface PendingCodexDeviceAuth {
  child: ReturnType<typeof spawn>;
  output: string;
}

let pendingCodexDeviceAuth: PendingCodexDeviceAuth | null = null;

function sanitizeUserId(userId: string): string {
  const normalized = userId.trim().replace(/[^a-zA-Z0-9_-]/g, "_");
  return normalized.length > 0 ? normalized : "anonymous";
}

function buildAgentRunRpcSubject(userId: string, agentId: string): string {
  return `doer.agent.run.rpc.${sanitizeUserId(userId)}.${agentId.trim()}`;
}

function buildAgentSessionRpcSubject(userId: string, agentId: string): string {
  return `doer.agent.session.rpc.${sanitizeUserId(userId)}.${agentId.trim()}`;
}

function buildAgentCodexAuthRpcSubject(userId: string, agentId: string): string {
  return `doer.agent.codex.auth.rpc.${sanitizeUserId(userId)}.${agentId.trim()}`;
}

function buildAgentSettingsRpcSubject(userId: string, agentId: string): string {
  return `doer.agent.settings.rpc.${sanitizeUserId(userId)}.${agentId.trim()}`;
}

function buildAgentGitRpcSubject(userId: string, agentId: string): string {
  return `doer.agent.git.rpc.${sanitizeUserId(userId)}.${agentId.trim()}`;
}

function normalizeNatsServers(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((item): item is string => typeof item === "string").map((v) => v.trim()).filter((v) => v.length > 0);
}

function parseBootstrapTaskConfig(value: unknown): { stream: string; subject: string; durable: string } | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const task = value as Record<string, unknown>;
  const stream = typeof task.stream === "string" ? task.stream.trim() : "";
  const subject = typeof task.subject === "string" ? task.subject.trim() : "";
  const durable = typeof task.durable === "string" ? task.durable.trim() : "";
  if (!stream || !subject || !durable) {
    return null;
  }
  return { stream, subject, durable };
}

function normalizeTaskIds(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const out: string[] = [];
  for (const item of value) {
    if (typeof item !== "string") {
      continue;
    }
    const id = item.trim();
    if (!id) {
      continue;
    }
    out.push(id);
  }
  return out;
}

function normalizeNatsToken(value: unknown): string | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const auth = value as Record<string, unknown>;
  const token = typeof auth.token === "string" ? auth.token.trim() : "";
  return token.length > 0 ? token : null;
}

async function ensureJetStreamInfra(args: {
  jsm: JetStreamManager;
  stream: string;
  subject: string;
  durable?: string;
}): Promise<void> {
  const streamInfo = await args.jsm.streams.info(args.stream).catch(() => null);
  if (!streamInfo) {
    await args.jsm.streams.add({
      name: args.stream,
      subjects: [args.subject],
      storage: StorageType.File,
      retention: RetentionPolicy.Limits,
    });
  }

  if (args.durable) {
    const consumerInfo = await args.jsm.consumers.info(args.stream, args.durable).catch(() => null);
    if (!consumerInfo) {
      await args.jsm.consumers.add(args.stream, {
        durable_name: args.durable,
        ack_policy: AckPolicy.Explicit,
        deliver_policy: DeliverPolicy.All,
        filter_subject: args.subject,
        ack_wait: 30_000_000_000,
      });
    }
  }
}

async function initJetStreamContext(args: {
  userId: string;
  servers: string[];
  token: string | null;
}): Promise<AgentJetStreamContext> {
  const sanitized = sanitizeUserId(args.userId);
  const stream = `DOER_AGENT_EVENTS_${sanitized}`;
  const subject = `doer.agent.events.${sanitized}`;
  const durable = `doer-agent-uploader-${sanitized}`;

  const nc = await connect(args.token ? { servers: args.servers, token: args.token } : { servers: args.servers });
  const jsm = await nc.jetstreamManager();
  await ensureJetStreamInfra({ jsm, stream, subject, durable });

  void nc.closed().then((error) => {
    if (error) {
      writeAgentInfraError(`nats connection closed with error: ${error.message}`);
      return;
    }
    writeAgentInfraError("nats connection closed cleanly");
  });

  void (async () => {
    try {
      for await (const status of nc.status()) {
        const statusType = typeof status.type === "string" ? status.type : "unknown";
        if (statusType === "pingTimer") {
          continue;
        }
        const statusData = formatNatsStatusData((status as { data?: unknown }).data);
        writeAgentInfraError("nats status type=" + statusType + " data=" + statusData);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      writeAgentInfraError(`nats status loop ended: ${message}`);
    }
  })();

  return {
    nc,
    js: nc.jetstream(),
    jsm,
    codec: JSONCodec<AgentEventEnvelope>(),
    subject,
    stream,
    durable,
    servers: args.servers,
  };
}

function resolveCodexHomePath(): string {
  const workspaceRoot = workspaceRootOverride ?? (process.env.WORKSPACE?.trim() || process.cwd());
  return path.join(workspaceRoot, ".codex");
}

function parseEnvBoolean(value: string | undefined): boolean {
  return value?.trim().toLowerCase() === "true";
}

function parseEnvInteger(value: string | undefined, fallback: number): number {
  const normalized = value?.trim();
  if (!normalized) {
    return fallback;
  }
  const parsed = Number.parseInt(normalized, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function resolveContainerReachableServerBaseUrl(serverBaseUrl: string): string {
  return serverBaseUrl;
}

async function resolveAgentVersion(): Promise<string> {
  const raw = await readFile(AGENT_PACKAGE_JSON_PATH, "utf8").catch(() => "");
  if (!raw) {
    return "unknown";
  }
  try {
    const parsed = JSON.parse(raw) as { version?: unknown };
    return typeof parsed.version === "string" && parsed.version.trim() ? parsed.version.trim() : "unknown";
  } catch {
    return "unknown";
  }
}

function pickFirstNonEmpty(values: Array<string | undefined | null>): string {
  for (const value of values) {
    if (typeof value !== "string") {
      continue;
    }
    const normalized = value.trim();
    if (normalized) {
      return normalized;
    }
  }
  return "";
}

async function ensureGitAskpassScript(): Promise<string> {
  const binDir = path.join(AGENT_PROJECT_DIR, "runtime/bin");
  const scriptPath = path.join(binDir, "git-askpass.sh");
  const scriptBody = `#!/bin/sh
case "$1" in
  *Username*) printf "%s\\n" "x-access-token" ;;
  *Password*) printf "%s\\n" "\${GITHUB_TOKEN:-\${GH_TOKEN:-}}" ;;
  *) printf "\\n" ;;
esac
`;
  await mkdir(binDir, { recursive: true });
  await writeFile(scriptPath, scriptBody, "utf8");
  await chmod(scriptPath, 0o700).catch(() => undefined);
  return scriptPath;
}

function applyGitIdentityIfPossible(args: { cwd: string | null; userName: string; userEmail: string }): boolean {
  if (!args.cwd) {
    return false;
  }
  const inRepo = spawnSync("git", ["rev-parse", "--is-inside-work-tree"], {
    cwd: args.cwd,
    stdio: "ignore",
  });
  if (inRepo.status !== 0) {
    return false;
  }
  const setName = spawnSync("git", ["config", "--local", "user.name", args.userName], {
    cwd: args.cwd,
    stdio: "ignore",
  });
  if (setName.status !== 0) {
    return false;
  }
  const setEmail = spawnSync("git", ["config", "--local", "user.email", args.userEmail], {
    cwd: args.cwd,
    stdio: "ignore",
  });
  return setEmail.status === 0;
}

async function prepareTaskGitEnv(args: {
  cwd: string | null;
  baseEnvPatch: Record<string, string>;
}): Promise<{ envPatch: Record<string, string>; meta: Record<string, unknown> }> {
  const envPatch: Record<string, string> = {
    GIT_TERMINAL_PROMPT: "0",
    GCM_INTERACTIVE: "Never",
  };

  const githubToken = pickFirstNonEmpty([
    args.baseEnvPatch.GITHUB_TOKEN,
    args.baseEnvPatch.GH_TOKEN,
    process.env.GITHUB_TOKEN,
    process.env.GH_TOKEN,
  ]);
  if (githubToken) {
    envPatch.GITHUB_TOKEN = githubToken;
    envPatch.GH_TOKEN = githubToken;
    envPatch.GIT_ASKPASS_REQUIRE = "force";
    envPatch.GIT_ASKPASS = await ensureGitAskpassScript();
  }

  const userName = pickFirstNonEmpty([
    args.baseEnvPatch.DOER_GIT_USER_NAME,
    args.baseEnvPatch.GIT_USER_NAME,
    args.baseEnvPatch.GIT_AUTHOR_NAME,
    args.baseEnvPatch.GIT_COMMITTER_NAME,
  ]);
  const userEmail = pickFirstNonEmpty([
    args.baseEnvPatch.DOER_GIT_USER_EMAIL,
    args.baseEnvPatch.GIT_USER_EMAIL,
    args.baseEnvPatch.GIT_AUTHOR_EMAIL,
    args.baseEnvPatch.GIT_COMMITTER_EMAIL,
  ]);

  const gitIdentityApplied =
    userName && userEmail
      ? applyGitIdentityIfPossible({
          cwd: args.cwd,
          userName,
          userEmail,
        })
      : false;

  return {
    envPatch,
    meta: {
      gitAskpassEnabled: Boolean(envPatch.GIT_ASKPASS),
      gitIdentityApplied,
      gitIdentityProvided: Boolean(userName && userEmail),
    },
  };
}

function normalizeEnvPatch(value: unknown): Record<string, string> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  const out: Record<string, string> = {};
  for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
    if (typeof raw !== "string") {
      continue;
    }
    const normalizedKey = key.trim();
    if (!normalizedKey) {
      continue;
    }
    out[normalizedKey] = raw;
  }
  return out;
}

async function prepareTaskRuntimeConfig(args: {
  serverBaseUrl: string;
  taskId: string;
  userId: string;
  agentToken: string;
}): Promise<{
  envPatch: Record<string, string>;
  meta: Record<string, unknown>;
} | null> {
  const bundle = await postJson<RuntimeConfigBundleResponse>(
    `${args.serverBaseUrl}/api/agent/tasks/${encodeURIComponent(args.taskId)}/runtime-config`,
    {
      userId: args.userId,
      agentToken: args.agentToken,
    },
  ).catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    writeAgentError(`task=${args.taskId} runtime config sync skipped: ${message}`);
    return null;
  });

  if (!bundle) {
    return null;
  }

  const envPatch = normalizeEnvPatch(bundle.envPatch);
  return {
    envPatch,
    meta: {
      runtimeConfigIssuedAt: bundle.issuedAt ?? null,
      runtimeConfigExpiresAt: bundle.expiresAt ?? null,
      runtimeConfigVarCount: Object.keys(envPatch).length,
      runtimeConfigSynced: true,
      ...(bundle.meta && typeof bundle.meta === "object" && !Array.isArray(bundle.meta) ? bundle.meta : {}),
    },
  };
}

function fatalExit(message: string, error?: unknown): never {
  const detail = error instanceof Error ? error.message : typeof error === "string" ? error : error ? String(error) : "";
  const full = detail ? `${message}: ${detail}` : message;
  writeAgentError(`fatal: ${full}`);
  process.exit(1);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function writeAgentInfo(message: string): void {
  process.stdout.write(`[doer-agent] ${message}\n`);
  emitAgentMetaLog("info", message);
}

function writeAgentError(message: string): void {
  process.stderr.write(`[doer-agent] ${message}\n`);
  emitAgentMetaLog("error", message);
}

function writeAgentInfraError(message: string): void {
  try {
    process.stderr.write(`[doer-agent] ${message}\n`);
  } catch {
    // Keep heartbeat/connectivity failures non-fatal.
  }
}

function formatNatsStatusData(value: unknown): string {
  if (value === null || value === undefined) {
    return "null";
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function writeTaskStream(taskId: string, stream: "stdout" | "stderr", chunk: string): void {
  const target = stream === "stdout" ? process.stdout : process.stderr;
  const lines = chunk.replace(/\r/g, "\n").split("\n");
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    if (line.length === 0 && i === lines.length - 1) {
      continue;
    }
    target.write(`[doer-agent][task=${taskId}][${stream}] ${line}\n`);
  }
}

function writeTaskUpload(taskId: string, message: string): void {
  process.stdout.write(`[doer-agent][task=${taskId}][upload] ${message}\n`);
}

function writeRpcStream(requestId: string, stream: "stdout" | "stderr", chunk: string): void {
  const target = stream === "stdout" ? process.stdout : process.stderr;
  const lines = chunk.replace(/\r/g, "\n").split("\n");
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    if (line.length === 0 && i === lines.length - 1) {
      continue;
    }
    target.write(`[doer-agent][rpc=${requestId}][${stream}] ${line}\n`);
  }
}

function writeRpcStatus(requestId: string, message: string): void {
  process.stdout.write(`[doer-agent][rpc=${requestId}][status] ${message}\n`);
}

function writeRunStatus(runId: string, message: string): void {
  process.stdout.write(`[doer-agent][run=${runId}][status] ${message}\n`);
}

function writeRunStream(runId: string, stream: "stdout" | "stderr", chunk: string): void {
  const target = stream === "stdout" ? process.stdout : process.stderr;
  const lines = chunk.split(/\r?\n/);
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    if (!line && index === lines.length - 1) {
      continue;
    }
    target.write(`[doer-agent][run=${runId}][${stream}] ${line}\n`);
  }
}

function normalizeRunRpcRequest(args: { request: AgentRunRpcRequest; agentId: string }): AgentRunRpcNormalizedRequest {
  const requestId = typeof args.request.requestId === "string" ? args.request.requestId.trim() : "";
  if (!requestId) {
    throw new Error("missing requestId");
  }
  const requestAgentId = typeof args.request.agentId === "string" ? args.request.agentId.trim() : "";
  if (!requestAgentId || requestAgentId !== args.agentId) {
    throw new Error("agent id mismatch");
  }
  const actionRaw = typeof args.request.action === "string" ? args.request.action.trim() : "";
  const action: AgentRunRpcAction = actionRaw === "cancel" || actionRaw === "get" || actionRaw === "list" ? actionRaw : "start";
  const responseSubject = typeof args.request.responseSubject === "string" ? args.request.responseSubject.trim() : "";
  if (!responseSubject) {
    throw new Error("missing responseSubject");
  }
  const runId = typeof args.request.runId === "string" && args.request.runId.trim() ? args.request.runId.trim() : null;
  const prompt = typeof args.request.prompt === "string" && args.request.prompt.trim() ? args.request.prompt.trim() : null;
  const sessionId = typeof args.request.sessionId === "string" && args.request.sessionId.trim() ? args.request.sessionId.trim() : null;
  const model = normalizeCodexModel(args.request.model);
  if (action === "start" && !prompt) {
    throw new Error("missing prompt");
  }
  if ((action === "get" || action === "cancel") && !runId) {
    throw new Error("missing runId");
  }
  const cwd = typeof args.request.cwd === "string" && args.request.cwd.trim() ? args.request.cwd.trim() : null;
  const sinceSeqRaw = Number(args.request.sinceSeq);
  const sinceSeq = Number.isInteger(sinceSeqRaw) && sinceSeqRaw >= 0 ? sinceSeqRaw : null;
  const limitRaw = Number(args.request.limit);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(Math.floor(limitRaw), 200)) : 50;
  return {
    requestId,
    action,
    runId,
    prompt,
    sessionId,
    model,
    cwd,
    responseSubject,
    sinceSeq,
    limit,
    runtimeEnvPatch: normalizeEnvPatch(args.request.runtimeEnvPatch),
    codexAuthBundle: normalizeShellRpcCodexAuthBundle(args.request.codexAuth),
  };
}

function publishRunRpcResponse(args: { nc: NatsConnection; responseSubject: string; payload: AgentRunRpcResponse }): void {
  args.nc.publish(args.responseSubject, runRpcCodec.encode(JSON.stringify(args.payload)));
}

async function resolveRunsDir(): Promise<string> {
  const workspaceRoot = workspaceRootOverride ?? (process.env.WORKSPACE?.trim() || process.cwd());
  const dir = path.join(workspaceRoot, ".doer-agent", "runs");
  await mkdir(dir, { recursive: true });
  return dir;
}

async function resetRunsDir(): Promise<void> {
  const dir = await resolveRunsDir();
  await rm(dir, { recursive: true, force: true }).catch(() => undefined);
  await mkdir(dir, { recursive: true });
}

async function persistRunTask(task: PublicRunTask): Promise<void> {
  const dir = await resolveRunsDir();
  const payload = {
    runId: task.id,
    agentId: task.agentId,
    userId: task.userId,
    sessionId: task.sessionId,
    sessionFilePath: task.sessionFilePath,
    status: task.status,
    cancelRequested: task.cancelRequested,
    createdAt: task.createdAt,
    updatedAt: task.updatedAt,
    startedAt: task.startedAt,
    finishedAt: task.finishedAt,
    error: task.error,
  };
  await writeFile(path.join(dir, `${task.id}.json`), `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

async function removeRunTask(runId: string): Promise<void> {
  const dir = await resolveRunsDir();
  await unlink(path.join(dir, `${runId}.json`)).catch(() => undefined);
}

function sanitizeRunLockSegment(value: string): string {
  return value.trim().replace(/[^a-zA-Z0-9._-]/g, "_").slice(0, 160) || "lock";
}

async function resolveRunLocksDir(): Promise<string> {
  const dir = path.join(await resolveRunsDir(), "locks");
  await mkdir(dir, { recursive: true });
  return dir;
}

async function resolveRunStartLockPath(args: { runId: string; sessionId?: string | null }): Promise<string> {
  const dir = await resolveRunLocksDir();
  if (typeof args.sessionId === "string" && args.sessionId.trim()) {
    return path.join(dir, `session__${sanitizeRunLockSegment(args.sessionId)}.lock`);
  }
  return path.join(dir, `run__${sanitizeRunLockSegment(args.runId)}.lock`);
}

async function claimRunStartSlot(args: { runId: string; sessionId?: string | null }): Promise<void> {
  const lockPath = await resolveRunStartLockPath(args);
  try {
    const handle = await open(lockPath, "wx");
    try {
      const payload = {
        runId: args.runId,
        sessionId: typeof args.sessionId === "string" && args.sessionId.trim() ? args.sessionId.trim() : null,
        pid: process.pid,
        createdAt: formatLocalTimestamp(),
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

async function updateRunStartSlotSession(args: {
  runId: string;
  previousSessionId?: string | null;
  sessionId: string;
}): Promise<void> {
  const nextSessionId = args.sessionId.trim();
  if (!nextSessionId) {
    return;
  }
  const previousSessionId = typeof args.previousSessionId === "string" && args.previousSessionId.trim() ? args.previousSessionId.trim() : null;
  if (previousSessionId === nextSessionId) {
    return;
  }
  const currentPath = await resolveRunStartLockPath({ runId: args.runId, sessionId: previousSessionId });
  const nextPath = await resolveRunStartLockPath({ runId: args.runId, sessionId: nextSessionId });
  if (currentPath === nextPath) {
    return;
  }
  try {
    await rename(currentPath, nextPath);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException | null)?.code;
    if (code === "ENOENT") {
      // Lock may already be released; nothing to migrate.
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
    createdAt: formatLocalTimestamp(),
  };
  await writeFile(nextPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

async function releaseRunStartSlot(args: { runId: string; sessionId?: string | null }): Promise<void> {
  const paths = new Set<string>();
  paths.add(await resolveRunStartLockPath({ runId: args.runId, sessionId: args.sessionId ?? null }));
  paths.add(await resolveRunStartLockPath({ runId: args.runId, sessionId: null }));
  for (const lockPath of paths) {
    await unlink(lockPath).catch(() => undefined);
  }
}

function resolveAgentSettingsDir(): string {
  const workspaceRoot = workspaceRootOverride ?? (process.env.WORKSPACE?.trim() || process.cwd());
  return path.join(workspaceRoot, ".doer-agent");
}

function resolveAgentSettingsFilePath(): string {
  return path.join(resolveAgentSettingsDir(), "config.json");
}

function createDefaultAgentSettingsConfig(): AgentSettingsConfig {
  return {
    general: {
      firstTurnPrompt: null,
    },
    codex: {
      model: "gpt-5.4",
      authMode: "api_key",
      apiKey: null,
    },
    realtime: {
      model: process.env.OPENAI_REALTIME_MODEL?.trim() || "gpt-realtime",
      voice: process.env.OPENAI_REALTIME_VOICE?.trim() || "alloy",
      wakeName: null,
      requireWakeName: true,
      apiKey: null,
    },
    git: {
      enabled: true,
      name: null,
      email: null,
      authMode: "none",
      oauthToken: null,
      oauthLogin: null,
      oauthScope: null,
    },
    aws: {
      enabled: true,
      accessKeyId: null,
      defaultRegion: null,
      secretAccessKey: null,
      sessionToken: null,
    },
    jira: {
      baseUrl: null,
      email: null,
      enabled: false,
      apiToken: null,
    },
    notion: {
      baseUrl: "https://api.notion.com",
      version: "2022-06-28",
      enabled: false,
      apiToken: null,
    },
    slack: {
      baseUrl: "https://slack.com/api",
      enabled: false,
      botToken: null,
    },
    figma: {
      baseUrl: "https://api.figma.com",
      enabled: false,
      apiToken: null,
    },
  };
}

function normalizeNullableString(value: unknown): string | null {
  if (value === null) {
    return null;
  }
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function normalizeAgentSettingsConfig(value: unknown, fallback?: AgentSettingsConfig | null): AgentSettingsConfig {
  const base = fallback ?? createDefaultAgentSettingsConfig();
  const raw = value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
  const general = raw.general && typeof raw.general === "object" ? (raw.general as Record<string, unknown>) : {};
  const codex = raw.codex && typeof raw.codex === "object" ? (raw.codex as Record<string, unknown>) : {};
  const realtime = raw.realtime && typeof raw.realtime === "object" ? (raw.realtime as Record<string, unknown>) : {};
  const git = raw.git && typeof raw.git === "object" ? (raw.git as Record<string, unknown>) : {};
  const aws = raw.aws && typeof raw.aws === "object" ? (raw.aws as Record<string, unknown>) : {};
  const jira = raw.jira && typeof raw.jira === "object" ? (raw.jira as Record<string, unknown>) : {};
  const notion = raw.notion && typeof raw.notion === "object" ? (raw.notion as Record<string, unknown>) : {};
  const slack = raw.slack && typeof raw.slack === "object" ? (raw.slack as Record<string, unknown>) : {};
  const figma = raw.figma && typeof raw.figma === "object" ? (raw.figma as Record<string, unknown>) : {};
  return {
    general: {
      firstTurnPrompt: normalizeNullableString(general.firstTurnPrompt) ?? base.general.firstTurnPrompt,
    },
    codex: {
      model: typeof codex.model === "string" && codex.model.trim() ? codex.model.trim() : base.codex.model,
      authMode: codex.authMode === "oauth" ? "oauth" : codex.authMode === "api_key" ? "api_key" : base.codex.authMode,
      apiKey: codex.apiKey === null ? null : normalizeNullableString(codex.apiKey) ?? base.codex.apiKey,
    },
    realtime: {
      model: typeof realtime.model === "string" && realtime.model.trim() ? realtime.model.trim() : base.realtime.model,
      voice: typeof realtime.voice === "string" && realtime.voice.trim() ? realtime.voice.trim() : base.realtime.voice,
      wakeName: realtime.wakeName === null ? null : normalizeNullableString(realtime.wakeName) ?? base.realtime.wakeName,
      requireWakeName: typeof realtime.requireWakeName === "boolean" ? realtime.requireWakeName : base.realtime.requireWakeName,
      apiKey: realtime.apiKey === null ? null : normalizeNullableString(realtime.apiKey) ?? base.realtime.apiKey,
    },
    git: {
      enabled: typeof git.enabled === "boolean" ? git.enabled : base.git.enabled,
      name: git.name === null ? null : normalizeNullableString(git.name) ?? base.git.name,
      email: git.email === null ? null : normalizeNullableString(git.email) ?? base.git.email,
      authMode: git.authMode === "oauth_app" ? "oauth_app" : git.authMode === "none" ? "none" : base.git.authMode,
      oauthToken: git.oauthToken === null ? null : normalizeNullableString(git.oauthToken) ?? base.git.oauthToken,
      oauthLogin: git.oauthLogin === null ? null : normalizeNullableString(git.oauthLogin) ?? base.git.oauthLogin,
      oauthScope: git.oauthScope === null ? null : normalizeNullableString(git.oauthScope) ?? base.git.oauthScope,
    },
    aws: {
      enabled: typeof aws.enabled === "boolean" ? aws.enabled : base.aws.enabled,
      accessKeyId: aws.accessKeyId === null ? null : normalizeNullableString(aws.accessKeyId) ?? base.aws.accessKeyId,
      defaultRegion: aws.defaultRegion === null ? null : normalizeNullableString(aws.defaultRegion) ?? base.aws.defaultRegion,
      secretAccessKey: aws.secretAccessKey === null ? null : normalizeNullableString(aws.secretAccessKey) ?? base.aws.secretAccessKey,
      sessionToken: aws.sessionToken === null ? null : normalizeNullableString(aws.sessionToken) ?? base.aws.sessionToken,
    },
    jira: {
      baseUrl: jira.baseUrl === null ? null : normalizeNullableString(jira.baseUrl) ?? base.jira.baseUrl,
      email: jira.email === null ? null : normalizeNullableString(jira.email) ?? base.jira.email,
      enabled: typeof jira.enabled === "boolean" ? jira.enabled : base.jira.enabled,
      apiToken: jira.apiToken === null ? null : normalizeNullableString(jira.apiToken) ?? base.jira.apiToken,
    },
    notion: {
      baseUrl: notion.baseUrl === null ? null : normalizeNullableString(notion.baseUrl) ?? base.notion.baseUrl,
      version: notion.version === null ? null : normalizeNullableString(notion.version) ?? base.notion.version,
      enabled: typeof notion.enabled === "boolean" ? notion.enabled : base.notion.enabled,
      apiToken: notion.apiToken === null ? null : normalizeNullableString(notion.apiToken) ?? base.notion.apiToken,
    },
    slack: {
      baseUrl: slack.baseUrl === null ? null : normalizeNullableString(slack.baseUrl) ?? base.slack.baseUrl,
      enabled: typeof slack.enabled === "boolean" ? slack.enabled : base.slack.enabled,
      botToken: slack.botToken === null ? null : normalizeNullableString(slack.botToken) ?? base.slack.botToken,
    },
    figma: {
      baseUrl: figma.baseUrl === null ? null : normalizeNullableString(figma.baseUrl) ?? base.figma.baseUrl,
      enabled: typeof figma.enabled === "boolean" ? figma.enabled : base.figma.enabled,
      apiToken: figma.apiToken === null ? null : normalizeNullableString(figma.apiToken) ?? base.figma.apiToken,
    },
  };
}

async function readAgentSettingsConfig(defaults?: AgentSettingsConfig | null): Promise<AgentSettingsConfig> {
  const fallback = normalizeAgentSettingsConfig(defaults ?? null);
  const filePath = resolveAgentSettingsFilePath();
  const raw = await readFile(filePath, "utf8").catch(() => "");
  if (!raw.trim()) {
    return fallback;
  }
  try {
    return normalizeAgentSettingsConfig(JSON.parse(raw), fallback);
  } catch {
    return fallback;
  }
}

async function writeAgentSettingsConfig(config: AgentSettingsConfig): Promise<void> {
  const dir = resolveAgentSettingsDir();
  await mkdir(dir, { recursive: true });
  await writeFile(resolveAgentSettingsFilePath(), `${JSON.stringify(config, null, 2)}\n`, "utf8");
}

function maskSecretPreview(secret: string): string {
  if (secret.length <= 6) {
    return `${secret.slice(0, 1)}***${secret.slice(-1)}`;
  }
  return `${secret.slice(0, 4)}...${secret.slice(-4)}`;
}

function toMaskedSecret(value: string | null): { has: boolean; masked: string | null; length: number | null } {
  if (!value) {
    return { has: false, masked: null, length: null };
  }
  return { has: true, masked: maskSecretPreview(value), length: value.length };
}

function toAgentSettingsPublic(config: AgentSettingsConfig): AgentSettingsPublic {
  const codexKey = toMaskedSecret(config.codex.apiKey);
  const realtimeKey = toMaskedSecret(config.realtime.apiKey);
  const gitOauth = toMaskedSecret(config.git.oauthToken);
  const awsSecret = toMaskedSecret(config.aws.secretAccessKey);
  const awsSession = toMaskedSecret(config.aws.sessionToken);
  const jiraToken = toMaskedSecret(config.jira.apiToken);
  const notionToken = toMaskedSecret(config.notion.apiToken);
  const slackToken = toMaskedSecret(config.slack.botToken);
  const figmaToken = toMaskedSecret(config.figma.apiToken);
  return {
    general: {
      firstTurnPrompt: config.general.firstTurnPrompt,
    },
    codex: {
      model: config.codex.model,
      authMode: config.codex.authMode,
      hasApiKey: codexKey.has,
      apiKeyMasked: codexKey.masked,
      apiKeyLength: codexKey.length,
    },
    realtime: {
      model: config.realtime.model,
      voice: config.realtime.voice,
      wakeName: config.realtime.wakeName,
      requireWakeName: config.realtime.requireWakeName,
      hasApiKey: realtimeKey.has,
      apiKeyMasked: realtimeKey.masked,
      apiKeyLength: realtimeKey.length,
    },
    git: {
      enabled: config.git.enabled,
      name: config.git.name,
      email: config.git.email,
      authMode: config.git.authMode,
      hasOauthToken: gitOauth.has,
      oauthTokenMasked: gitOauth.masked,
      oauthTokenLength: gitOauth.length,
      oauthLogin: config.git.oauthLogin,
      oauthScope: config.git.oauthScope,
    },
    aws: {
      enabled: config.aws.enabled,
      accessKeyId: config.aws.accessKeyId,
      defaultRegion: config.aws.defaultRegion,
      hasSecretAccessKey: awsSecret.has,
      secretAccessKeyMasked: awsSecret.masked,
      secretAccessKeyLength: awsSecret.length,
      hasSessionToken: awsSession.has,
      sessionTokenMasked: awsSession.masked,
      sessionTokenLength: awsSession.length,
    },
    jira: {
      baseUrl: config.jira.baseUrl,
      email: config.jira.email,
      enabled: config.jira.enabled,
      hasApiToken: jiraToken.has,
      apiTokenMasked: jiraToken.masked,
      apiTokenLength: jiraToken.length,
    },
    notion: {
      baseUrl: config.notion.baseUrl,
      version: config.notion.version,
      enabled: config.notion.enabled,
      hasApiToken: notionToken.has,
      apiTokenMasked: notionToken.masked,
      apiTokenLength: notionToken.length,
    },
    slack: {
      baseUrl: config.slack.baseUrl,
      enabled: config.slack.enabled,
      hasBotToken: slackToken.has,
      botTokenMasked: slackToken.masked,
      botTokenLength: slackToken.length,
    },
    figma: {
      baseUrl: config.figma.baseUrl,
      enabled: config.figma.enabled,
      hasApiToken: figmaToken.has,
      apiTokenMasked: figmaToken.masked,
      apiTokenLength: figmaToken.length,
    },
  };
}

function normalizeAgentSettingsPatch(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  const raw = value as Record<string, unknown>;
  const patch: Record<string, unknown> = { ...raw };

  const assignNested = (section: string, key: string, value: unknown) => {
    const current =
      patch[section] && typeof patch[section] === "object" && !Array.isArray(patch[section])
        ? ({ ...(patch[section] as Record<string, unknown>) })
        : {};
    current[key] = value;
    patch[section] = current;
  };

  const move = (flatKey: string, section: string, key: string) => {
    if (!(flatKey in raw)) {
      return;
    }
    assignNested(section, key, raw[flatKey]);
    delete patch[flatKey];
  };

  move("firstTurnPrompt", "general", "firstTurnPrompt");

  move("codexModel", "codex", "model");
  move("codexAuthMode", "codex", "authMode");
  move("codexApiKey", "codex", "apiKey");

  move("realtimeModel", "realtime", "model");
  move("realtimeVoice", "realtime", "voice");
  move("realtimeWakeName", "realtime", "wakeName");
  move("realtimeRequireWakeName", "realtime", "requireWakeName");
  move("realtimeApiKey", "realtime", "apiKey");

  move("gitEnabled", "git", "enabled");
  move("gitName", "git", "name");
  move("gitEmail", "git", "email");
  move("gitAuthMode", "git", "authMode");
  move("gitOauthToken", "git", "oauthToken");
  move("gitOauthLogin", "git", "oauthLogin");
  move("gitOauthScope", "git", "oauthScope");

  move("awsEnabled", "aws", "enabled");
  move("awsAccessKeyId", "aws", "accessKeyId");
  move("awsDefaultRegion", "aws", "defaultRegion");
  move("awsSecretAccessKey", "aws", "secretAccessKey");
  move("awsSessionToken", "aws", "sessionToken");

  move("jiraBaseUrl", "jira", "baseUrl");
  move("jiraEmail", "jira", "email");
  move("jiraEnabled", "jira", "enabled");
  move("jiraApiToken", "jira", "apiToken");

  move("notionBaseUrl", "notion", "baseUrl");
  move("notionVersion", "notion", "version");
  move("notionEnabled", "notion", "enabled");
  move("notionApiToken", "notion", "apiToken");

  move("slackBaseUrl", "slack", "baseUrl");
  move("slackEnabled", "slack", "enabled");
  move("slackBotToken", "slack", "botToken");

  move("figmaBaseUrl", "figma", "baseUrl");
  move("figmaEnabled", "figma", "enabled");
  move("figmaApiToken", "figma", "apiToken");

  return patch;
}

async function resolveAgentSettingsConfig(args: {
  defaults?: AgentSettingsConfig | null;
  patch?: Record<string, unknown> | null;
}): Promise<AgentSettingsConfig> {
  const existing = await readAgentSettingsConfig(args.defaults ?? null);
  const next = normalizeAgentSettingsConfig(args.patch ?? null, existing);
  return next;
}

function buildAgentSettingsEnvPatch(config: AgentSettingsConfig): Record<string, string> {
  const envPatch: Record<string, string> = {};
  if (config.codex.authMode === "api_key" && config.codex.apiKey) {
    envPatch.OPENAI_API_KEY = config.codex.apiKey;
  }
  if (config.git.enabled) {
    if (config.git.name) envPatch.GIT_AUTHOR_NAME = config.git.name;
    if (config.git.name) envPatch.GIT_COMMITTER_NAME = config.git.name;
    if (config.git.email) envPatch.GIT_AUTHOR_EMAIL = config.git.email;
    if (config.git.email) envPatch.GIT_COMMITTER_EMAIL = config.git.email;
    if (config.git.oauthToken) envPatch.GITHUB_TOKEN = config.git.oauthToken;
    if (config.git.oauthToken) envPatch.GH_TOKEN = config.git.oauthToken;
    if (config.git.oauthLogin) envPatch.DOER_GIT_OAUTH_LOGIN = config.git.oauthLogin;
    if (config.git.oauthScope) envPatch.DOER_GIT_OAUTH_SCOPE = config.git.oauthScope;
  }
  if (config.aws.enabled) {
    if (config.aws.accessKeyId) envPatch.AWS_ACCESS_KEY_ID = config.aws.accessKeyId;
    if (config.aws.defaultRegion) envPatch.AWS_DEFAULT_REGION = config.aws.defaultRegion;
    if (config.aws.defaultRegion) envPatch.AWS_REGION = config.aws.defaultRegion;
    if (config.aws.secretAccessKey) envPatch.AWS_SECRET_ACCESS_KEY = config.aws.secretAccessKey;
    if (config.aws.sessionToken) envPatch.AWS_SESSION_TOKEN = config.aws.sessionToken;
  }
  if (config.jira.enabled) {
    if (config.jira.baseUrl) envPatch.JIRA_BASE_URL = config.jira.baseUrl;
    if (config.jira.email) envPatch.JIRA_EMAIL = config.jira.email;
    if (config.jira.apiToken) envPatch.JIRA_API_TOKEN = config.jira.apiToken;
  }
  if (config.notion.enabled) {
    if (config.notion.baseUrl) envPatch.NOTION_BASE_URL = config.notion.baseUrl;
    if (config.notion.version) envPatch.NOTION_VERSION = config.notion.version;
    if (config.notion.apiToken) envPatch.NOTION_API_TOKEN = config.notion.apiToken;
  }
  if (config.slack.enabled) {
    if (config.slack.baseUrl) envPatch.SLACK_BASE_URL = config.slack.baseUrl;
    if (config.slack.botToken) envPatch.SLACK_BOT_TOKEN = config.slack.botToken;
  }
  if (config.figma.enabled) {
    if (config.figma.baseUrl) envPatch.FIGMA_BASE_URL = config.figma.baseUrl;
    if (config.figma.apiToken) envPatch.FIGMA_API_TOKEN = config.figma.apiToken;
  }
  return envPatch;
}

function cloneRunTask(task: PublicRunTask, _sinceSeq?: number | null): PublicRunTask {
  return {
    ...task,
  };
}

function extractCodexSessionMetadata(value: string): { sessionId: string | null; sessionFilePath: string | null } {
  try {
    const parsed = JSON.parse(value) as { type?: unknown; payload?: unknown };
    const lineType = typeof parsed.type === "string" ? parsed.type : "";
    if (!parsed.payload || typeof parsed.payload !== "object" || Array.isArray(parsed.payload)) {
      return { sessionId: null, sessionFilePath: null };
    }
    const payload = parsed.payload as Record<string, unknown>;
    const sessionIdCandidate =
      typeof payload.sessionId === "string" && payload.sessionId.trim()
        ? payload.sessionId.trim()
        : typeof payload.session_id === "string" && payload.session_id.trim()
          ? payload.session_id.trim()
          : typeof payload.id === "string" && payload.id.trim() && (lineType === "session_meta" || lineType === "session.started")
            ? payload.id.trim()
            : null;
    const filePathCandidate =
      typeof payload.rollout_path === "string" && payload.rollout_path.trim()
        ? payload.rollout_path.trim()
        : typeof payload.filePath === "string" && payload.filePath.trim()
          ? payload.filePath.trim()
          : null;
    return {
      sessionId: sessionIdCandidate,
      sessionFilePath: filePathCandidate,
    };
  } catch {
    return { sessionId: null, sessionFilePath: null };
  }
}

async function updateRunSessionMetadata(task: PublicRunTask, metadata: {
  sessionId?: string | null;
  sessionFilePath?: string | null;
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
  if (!changed) {
    return;
  }
  task.updatedAt = formatLocalTimestamp();
  await persistRunTask(task).catch(() => undefined);
  if (!previousSessionId && task.sessionId) {
    await updateRunStartSlotSession({
      runId: task.id,
      previousSessionId,
      sessionId: task.sessionId,
    }).catch(() => undefined);
  }
}

function persistRetainedRun(task: PublicRunTask): void {
  retainedRuns.set(task.id, cloneRunTask(task));
}

function getStoredRun(runId: string): PublicRunTask | null {
  const active = activeRuns.get(runId);
  if (active) {
    return active.task;
  }
  return retainedRuns.get(runId) ?? null;
}

async function startManagedRun(args: {
  requestId: string;
  runId: string;
  serverBaseUrl: string;
  userId: string;
  agentId: string;
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

  const cancellation = createManagedCancellation(child);
  const requestCancel = () => {
    if (task.status === "completed" || task.status === "failed" || task.status === "canceled") {
      return;
    }
    task.cancelRequested = true;
    task.updatedAt = formatLocalTimestamp();
    void persistRunTask(task).catch(() => undefined);
    writeRunStatus(task.id, "cancel requested");
    cancellation.requestCancel();
  };

  let stdoutBuffer = "";
  const recordChunk = (stream: "stdout" | "stderr", chunk: string) => {
    writeRunStream(task.id, stream, chunk);
    if (stream !== "stdout" || (task.sessionId && task.sessionFilePath)) {
      return;
    }
    stdoutBuffer += chunk;
    const lines = stdoutBuffer.split(/\r?\n/);
    stdoutBuffer = lines.pop() ?? "";
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) {
        continue;
      }
      const metadata = extractCodexSessionMetadata(trimmed);
      if (metadata.sessionId || metadata.sessionFilePath) {
        void updateRunSessionMetadata(task, metadata);
      }
    }
  };

  child.stdout!.on("data", (chunk: string) => recordChunk("stdout", chunk));
  child.stderr!.on("data", (chunk: string) => recordChunk("stderr", chunk));
  child.once("error", (error) => {
    const message = error instanceof Error ? error.message : String(error);
    task.status = "failed";
    task.error = message;
    task.finishedAt = formatLocalTimestamp();
    persistRetainedRun(task);
    activeRuns.delete(task.id);
    void removeRunTask(task.id).catch(() => undefined);
    void releaseRunStartSlot({ runId: task.id, sessionId: task.sessionId }).catch(() => undefined);
    void prepared.codexAuthCleanup().catch(() => undefined);
    writeRunStatus(task.id, `failed error=${message}`);
  });
  child.once("close", (code, signal) => {
    cancellation.clear();
    if (stdoutBuffer.trim() && (!task.sessionId || !task.sessionFilePath)) {
      const metadata = extractCodexSessionMetadata(stdoutBuffer.trim());
      if (metadata.sessionId || metadata.sessionFilePath) {
        void updateRunSessionMetadata(task, metadata);
      }
    }
    task.resultExitCode = typeof code === "number" ? code : null;
    task.resultSignal = signal;
    task.finishedAt = formatLocalTimestamp();
    task.status = task.cancelRequested ? "canceled" : (task.resultExitCode ?? 1) === 0 ? "completed" : "failed";
    task.error = task.status === "failed" ? `Command exited with code ${task.resultExitCode ?? "null"}` : null;
    persistRetainedRun(task);
    activeRuns.delete(task.id);
    void removeRunTask(task.id).catch(() => undefined);
    void releaseRunStartSlot({ runId: task.id, sessionId: task.sessionId }).catch(() => undefined);
    void prepared.codexAuthCleanup().catch(() => undefined);
    writeRunStatus(task.id, `completed status=${task.status} exitCode=${task.resultExitCode ?? "null"} signal=${task.resultSignal ?? "null"}`);
  });

  activeRuns.set(task.id, { task, child, requestCancel });
  persistRetainedRun(task);
  void persistRunTask(task).catch(() => undefined);
  writeRunStatus(task.id, `started requestId=${args.requestId} cwd=${prepared.taskWorkspace}`);
  return cloneRunTask(task);
}

function shellSingleQuote(value: string): string {
  return `'${value.replace(/'/g, `'"'"'`)}'`;
}

function stripAnsi(value: string): string {
  return value.replace(ANSI_RE, "");
}

function normalizeCodexModel(value: unknown): string {
  const normalized = typeof value === "string" ? value.trim() : "";
  return normalized || "gpt-5.4";
}

function buildManagedCodexArgs(args: {
  prompt: string;
  sessionId: string | null;
  model: string;
}): string[] {
  const promptArgs = ["--", args.prompt];
  const fixedArgs = ["--dangerously-bypass-approvals-and-sandbox"];
  return [
    ...fixedArgs,
    "--model",
    args.model,
    ...(args.sessionId
    ? ["exec", "resume", "--json", args.sessionId, ...promptArgs]
    : ["exec", "--json", ...promptArgs]),
  ];
}

function buildLocalCodexCliCommand(args: string[]): string {
  const quotedArgs = args.map(shellSingleQuote).join(" ");
  const direct = `exec codex ${quotedArgs}`;
  const fallback = `exec npm exec --yes --package doer-agent -- codex ${quotedArgs}`;
  const script = [
    "if command -v codex >/dev/null 2>&1; then",
    `  ${direct}`,
    "fi",
    fallback,
  ].join("\n");
  return `bash -lc ${shellSingleQuote(script)}`;
}

function hasDirectCodexBinary(): boolean {
  const result = spawnSync("bash", ["-lc", "command -v codex >/dev/null 2>&1"], {
    stdio: "ignore",
  });
  return result.status === 0;
}

function spawnManagedCodexCommand(args: {
  codexArgs: string[];
  taskWorkspace: string;
  env: NodeJS.ProcessEnv;
  agentToken: string;
}): ReturnType<typeof spawn> {
  const env = {
    ...args.env,
    DOER_AGENT_TOKEN: args.agentToken,
  };
  const child = hasDirectCodexBinary()
    ? spawn("codex", args.codexArgs, {
      cwd: args.taskWorkspace,
      detached: process.platform !== "win32",
      env,
      stdio: ["ignore", "pipe", "pipe"],
    })
    : spawn("npm", ["exec", "--yes", "--package", "doer-agent", "--", "codex", ...args.codexArgs], {
      cwd: args.taskWorkspace,
      detached: process.platform !== "win32",
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });
  child.stdout?.setEncoding("utf8");
  child.stderr?.setEncoding("utf8");
  return child;
}

async function runLocalCodexCli(args: string[], timeoutMs: number): Promise<{
  code: number | null;
  stdout: string;
  stderr: string;
  timedOut: boolean;
}> {
  const command = buildLocalCodexCliCommand(args);
  const workspaceRoot = workspaceRootOverride ?? (process.env.WORKSPACE?.trim() || process.cwd());
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    WORKSPACE: workspaceRoot,
    CODEX_HOME: resolveCodexHomePath(),
  };

  return await new Promise((resolve, reject) => {
    const child = spawn(command, {
      cwd: workspaceRoot,
      shell: resolveShellPath(),
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    let done = false;
    let timedOut = false;

    child.stdout!.setEncoding("utf8");
    child.stderr!.setEncoding("utf8");
    child.stdout!.on("data", (chunk: string) => {
      stdout += chunk;
    });
    child.stderr!.on("data", (chunk: string) => {
      stderr += chunk;
    });

    const timer = setTimeout(() => {
      timedOut = true;
      sendSignalToTaskProcess(child, "SIGTERM");
      setTimeout(() => sendSignalToTaskProcess(child, "SIGKILL"), 1000);
    }, Math.max(500, timeoutMs));

    child.once("error", (error) => {
      if (done) {
        return;
      }
      done = true;
      clearTimeout(timer);
      reject(error);
    });

    child.once("exit", (code) => {
      if (done) {
        return;
      }
      done = true;
      clearTimeout(timer);
      resolve({ code, stdout, stderr, timedOut });
    });
  });
}

function parseCodexDeviceAuthOutput(raw: string): { verificationUri: string | null; userCode: string | null } {
  const text = stripAnsi(raw);
  const urlMatch = text.match(/https?:\/\/[^\s]+/i);
  const codeMatch = text.match(/\b[A-Z0-9]{4,}(?:-[A-Z0-9]{4,})+\b/);
  return {
    verificationUri: urlMatch?.[0] ?? null,
    userCode: codeMatch?.[0] ?? null,
  };
}

function pendingCodexDeviceAuthMessage(state: PendingCodexDeviceAuth): string {
  const parsed = parseCodexDeviceAuthOutput(state.output);
  if (parsed.verificationUri && parsed.userCode) {
    return `Waiting for approval. Enter code ${parsed.userCode} at ${parsed.verificationUri}`;
  }
  return stripAnsi(state.output).trim() || "Waiting for approval";
}

async function getLocalCodexLoginStatus(): Promise<{ loggedIn: boolean; output: string }> {
  const result = await runLocalCodexCli(["login", "status"], 5000);
  const merged = stripAnsi([result.stdout, result.stderr].filter(Boolean).join("\n")).trim();
  return {
    loggedIn: (result.code ?? 1) === 0,
    output: merged || ((result.code ?? 1) === 0 ? "Logged in" : "Not logged in"),
  };
}

async function waitForCodexDeviceCode(state: PendingCodexDeviceAuth, timeoutMs: number): Promise<void> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const parsed = parseCodexDeviceAuthOutput(state.output);
    if (parsed.verificationUri && parsed.userCode) {
      return;
    }
    if (state.child.exitCode !== null) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

function normalizeSettingsRpcRequest(args: {
  request: AgentSettingsRpcRequest;
  agentId: string;
}): {
  requestId: string;
  responseSubject: string;
  action: AgentSettingsRpcAction;
  patch: Record<string, unknown>;
  defaults: AgentSettingsConfig | null;
} {
  const requestId = typeof args.request.requestId === "string" ? args.request.requestId.trim() : "";
  const responseSubject = typeof args.request.responseSubject === "string" ? args.request.responseSubject.trim() : "";
  const requestAgentId = typeof args.request.agentId === "string" ? args.request.agentId.trim() : "";
  const action = args.request.action === "update" ? "update" : "get";
  if (!requestId || !responseSubject || !requestAgentId || requestAgentId !== args.agentId) {
    throw new Error("invalid settings rpc request");
  }
  return {
    requestId,
    responseSubject,
    action,
    patch: normalizeAgentSettingsPatch(args.request.patch),
    defaults:
      args.request.defaults && typeof args.request.defaults === "object" && !Array.isArray(args.request.defaults)
        ? normalizeAgentSettingsConfig(args.request.defaults)
        : null,
  };
}

function publishSettingsRpcResponse(args: {
  nc: NatsConnection;
  responseSubject: string;
  payload: AgentSettingsRpcResponse;
}): void {
  args.nc.publish(args.responseSubject, settingsRpcCodec.encode(JSON.stringify(args.payload)));
}

async function handleSettingsRpcMessage(args: {
  msg: Msg;
  jetstream: AgentJetStreamContext;
  agentId: string;
}): Promise<void> {
  let requestId = "unknown";
  let responseSubject = "";
  try {
    const payload = JSON.parse(settingsRpcCodec.decode(args.msg.data)) as AgentSettingsRpcRequest;
    const request = normalizeSettingsRpcRequest({ request: payload, agentId: args.agentId });
    requestId = request.requestId;
    responseSubject = request.responseSubject;
    const existing = await readAgentSettingsConfig(request.defaults);
    const next = request.action === "update" ? normalizeAgentSettingsConfig(request.patch, existing) : existing;
    if (request.action === "update") {
      await writeAgentSettingsConfig(next);
    } else if (request.defaults) {
      const filePath = resolveAgentSettingsFilePath();
      const raw = await readFile(filePath, "utf8").catch(() => "");
      if (!raw.trim()) {
        await writeAgentSettingsConfig(next);
      }
    }
    publishSettingsRpcResponse({
      nc: args.jetstream.nc,
      responseSubject,
      payload: {
        requestId,
        ok: true,
        settings: toAgentSettingsPublic(next),
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (responseSubject) {
      publishSettingsRpcResponse({
        nc: args.jetstream.nc,
        responseSubject,
        payload: { requestId, ok: false, error: message },
      });
    }
    writeAgentError(`settings rpc failed requestId=${requestId} error=${message}`);
  }
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
        jetstream: args.jetstream,
        agentId: args.agentId,
      });
    },
  });
  writeAgentInfo(`settings rpc subscribed subject=${subject}`);
}

async function startLocalCodexDeviceAuth(): Promise<{
  loggedIn: false;
  output: string;
  verificationUri: string | null;
  userCode: string | null;
}> {
  if (pendingCodexDeviceAuth && pendingCodexDeviceAuth.child.exitCode === null) {
    const parsed = parseCodexDeviceAuthOutput(pendingCodexDeviceAuth.output);
    return {
      loggedIn: false,
      output: pendingCodexDeviceAuthMessage(pendingCodexDeviceAuth),
      verificationUri: parsed.verificationUri,
      userCode: parsed.userCode,
    };
  }

  const workspaceRoot = workspaceRootOverride ?? (process.env.WORKSPACE?.trim() || process.cwd());
  const child = spawn(buildLocalCodexCliCommand(["login", "--device-auth"]), {
    cwd: workspaceRoot,
    shell: resolveShellPath(),
    detached: process.platform !== "win32",
    env: {
      ...process.env,
      WORKSPACE: workspaceRoot,
      CODEX_HOME: resolveCodexHomePath(),
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  child.stdout!.setEncoding("utf8");
  child.stderr!.setEncoding("utf8");

  const state: PendingCodexDeviceAuth = { child, output: "" };
  pendingCodexDeviceAuth = state;

  const appendOutput = (chunk: string) => {
    state.output += chunk;
  };
  child.stdout!.on("data", appendOutput);
  child.stderr!.on("data", appendOutput);
  child.once("exit", () => {
    if (pendingCodexDeviceAuth === state) {
      pendingCodexDeviceAuth = null;
    }
  });

  await waitForCodexDeviceCode(state, 8000);

  const parsed = parseCodexDeviceAuthOutput(state.output);
  if ((!parsed.verificationUri || !parsed.userCode) && state.child.exitCode !== null) {
    throw new Error("Failed to read device code from Codex CLI");
  }

  return {
    loggedIn: false,
    output: pendingCodexDeviceAuthMessage(state),
    verificationUri: parsed.verificationUri,
    userCode: parsed.userCode,
  };
}

async function startLocalCodexLogin(): Promise<{
  loggedIn: boolean;
  output: string;
  verificationUri: string | null;
  userCode: string | null;
}> {
  const workspaceRoot = workspaceRootOverride ?? (process.env.WORKSPACE?.trim() || process.cwd());
  const child = spawn(buildLocalCodexCliCommand(["login"]), {
    cwd: workspaceRoot,
    shell: resolveShellPath(),
    detached: process.platform !== "win32",
    env: {
      ...process.env,
      WORKSPACE: workspaceRoot,
      CODEX_HOME: resolveCodexHomePath(),
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  let output = "";
  child.stdout!.setEncoding("utf8");
  child.stderr!.setEncoding("utf8");
  child.stdout!.on("data", (chunk: string) => {
    output += chunk;
  });
  child.stderr!.on("data", (chunk: string) => {
    output += chunk;
  });

  const result = await new Promise<{ code: number | null; output: string }>((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code) => resolve({ code, output }));
  });

  const normalized = stripAnsi(result.output).trim();
  const parsed = parseCodexDeviceAuthOutput(result.output);
  if ((result.code ?? 1) === 0) {
    const status = await getLocalCodexLoginStatus().catch(() => null);
    return {
      loggedIn: status?.loggedIn === true,
      output: status?.output || normalized || "Login started",
      verificationUri: parsed.verificationUri,
      userCode: parsed.userCode,
    };
  }

  throw new Error(normalized || `Codex login failed with code ${result.code ?? "null"}`);
}

async function logoutLocalCodexAuth(): Promise<{ loggedIn: false; output: string }> {
  if (pendingCodexDeviceAuth && pendingCodexDeviceAuth.child.exitCode === null) {
    sendSignalToTaskProcess(pendingCodexDeviceAuth.child, "SIGTERM");
    setTimeout(() => {
      if (pendingCodexDeviceAuth?.child.exitCode === null) {
        sendSignalToTaskProcess(pendingCodexDeviceAuth.child, "SIGKILL");
      }
    }, 1000);
    pendingCodexDeviceAuth = null;
  }
  const result = await runLocalCodexCli(["logout"], 5000);
  let merged = stripAnsi([result.stdout, result.stderr].filter(Boolean).join("\n")).trim();
  const statusAfterLogout = await getLocalCodexLoginStatus().catch(() => null);
  if (statusAfterLogout?.loggedIn) {
    const authFile = path.join(resolveCodexHomePath(), "auth.json");
    await unlink(authFile).catch(() => undefined);
    const statusAfterDelete = await getLocalCodexLoginStatus().catch(() => null);
    if (statusAfterDelete?.output) {
      merged = [merged, statusAfterDelete.output].filter(Boolean).join("\n");
    }
  }
  return {
    loggedIn: false,
    output: merged || "Logged out",
  };
}

function normalizeCodexAuthRpcRequest(args: {
  request: AgentCodexAuthRpcRequest;
  agentId: string;
}): {
  requestId: string;
  responseSubject: string;
  action: "start" | "status" | "logout";
} {
  const requestId = typeof args.request.requestId === "string" ? args.request.requestId.trim() : "";
  const responseSubject = typeof args.request.responseSubject === "string" ? args.request.responseSubject.trim() : "";
  const requestAgentId = typeof args.request.agentId === "string" ? args.request.agentId.trim() : "";
  const actionRaw = typeof args.request.action === "string" ? args.request.action.trim() : "";
  const action = actionRaw === "start" || actionRaw === "logout" ? actionRaw : "status";
  if (!requestId || !responseSubject || !requestAgentId || requestAgentId !== args.agentId) {
    throw new Error("invalid codex auth rpc request");
  }
  return { requestId, responseSubject, action };
}

function publishCodexAuthRpcResponse(args: {
  nc: NatsConnection;
  responseSubject: string;
  payload: AgentCodexAuthRpcResponse;
}): void {
  args.nc.publish(args.responseSubject, codexAuthRpcCodec.encode(JSON.stringify(args.payload)));
}

async function handleCodexAuthRpcMessage(args: {
  msg: Msg;
  jetstream: AgentJetStreamContext;
  agentId: string;
}): Promise<void> {
  let requestId = "unknown";
  let responseSubject = "";
  try {
    const payload = JSON.parse(codexAuthRpcCodec.decode(args.msg.data)) as AgentCodexAuthRpcRequest;
    const request = normalizeCodexAuthRpcRequest({ request: payload, agentId: args.agentId });
    requestId = request.requestId;
    responseSubject = request.responseSubject;

    let result:
      | { loggedIn: boolean; output: string; verificationUri?: string | null; userCode?: string | null }
      | null = null;

    if (request.action === "start") {
      const status = await getLocalCodexLoginStatus();
      if (status.loggedIn) {
        result = { loggedIn: true, output: status.output };
      } else {
        try {
          result = await startLocalCodexDeviceAuth();
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          const normalized = message.toLowerCase();
          if (
            normalized.includes("operation not permitted") ||
            normalized.includes("failed to read device code") ||
            normalized.includes("panic") ||
            normalized.includes("null object")
          ) {
            result = await startLocalCodexLogin();
          } else {
            throw error;
          }
        }
      }
    } else if (request.action === "logout") {
      result = await logoutLocalCodexAuth();
    } else {
      const status = await getLocalCodexLoginStatus();
      if (status.loggedIn) {
        result = { loggedIn: true, output: status.output };
      } else if (pendingCodexDeviceAuth && pendingCodexDeviceAuth.child.exitCode === null) {
        const parsed = parseCodexDeviceAuthOutput(pendingCodexDeviceAuth.output);
        result = {
          loggedIn: false,
          output: pendingCodexDeviceAuthMessage(pendingCodexDeviceAuth),
          verificationUri: parsed.verificationUri,
          userCode: parsed.userCode,
        };
      } else {
        result = { loggedIn: false, output: status.output || "Not logged in" };
      }
    }

    publishCodexAuthRpcResponse({
      nc: args.jetstream.nc,
      responseSubject,
      payload: {
        requestId,
        ok: true,
        loggedIn: result.loggedIn,
        output: result.output,
        verificationUri: result.verificationUri ?? null,
        userCode: result.userCode ?? null,
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (responseSubject) {
      publishCodexAuthRpcResponse({
        nc: args.jetstream.nc,
        responseSubject,
        payload: { requestId, ok: false, error: message },
      });
    }
    writeAgentError(`codex auth rpc failed requestId=${requestId} error=${message}`);
  }
}

function subscribeToCodexAuthRpc(args: {
  jetstream: AgentJetStreamContext;
  userId: string;
  agentId: string;
}): void {
  const subject = buildAgentCodexAuthRpcSubject(args.userId, args.agentId);
  args.jetstream.nc.subscribe(subject, {
    callback: (error, msg) => {
      if (error) {
        const message = error instanceof Error ? error.message : String(error);
        writeAgentError(`codex auth rpc subscription error: ${message}`);
        return;
      }
      void handleCodexAuthRpcMessage({
        msg,
        jetstream: args.jetstream,
        agentId: args.agentId,
      });
    },
  });
  writeAgentInfo(`codex auth rpc subscribed subject=${subject}`);
}

type AgentGitDiffFormat = "patch" | "name-only" | "name-status" | "stat" | "numstat" | "raw";
type AgentGitDiffIgnoreWhitespace = "none" | "at-eol" | "change" | "all";
type AgentGitDiffAlgorithm = "default" | "minimal" | "patience" | "histogram";

function runLocalCommand(command: string, args: string[], cwd: string): Promise<{ code: number; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout!.setEncoding("utf8");
    child.stderr!.setEncoding("utf8");
    child.stdout!.on("data", (chunk: string) => {
      stdout += chunk;
    });
    child.stderr!.on("data", (chunk: string) => {
      stderr += chunk;
    });
    child.once("error", reject);
    child.once("close", (code) => {
      resolve({ code: code ?? 1, stdout, stderr });
    });
  });
}

function sanitizeGitRef(value: unknown): string | null {
  if (typeof value !== "string" || !value.trim()) {
    return null;
  }
  const trimmed = value.trim();
  if (trimmed.startsWith("-") || /\s/.test(trimmed) || trimmed.includes("..") || trimmed.includes(":")) {
    throw new Error(`Invalid git ref: ${trimmed}`);
  }
  return trimmed;
}

function sanitizeGitPathspec(value: unknown): string {
  if (typeof value !== "string") {
    throw new Error("Invalid pathspec");
  }
  const trimmed = value.trim().replace(/\\/g, "/");
  if (!trimmed || trimmed.startsWith("-") || trimmed.includes("\0")) {
    throw new Error(`Invalid pathspec: ${trimmed}`);
  }
  return trimmed;
}

function normalizeGitRpcRequest(args: {
  request: AgentGitRpcRequest;
  agentId: string;
}): {
  requestId: string;
  responseSubject: string;
  targetPath: string;
  base: string | null;
  target: string | null;
  mergeBase: boolean;
  staged: boolean;
  format: AgentGitDiffFormat;
  contextLines: number | null;
  ignoreWhitespace: AgentGitDiffIgnoreWhitespace;
  diffAlgorithm: AgentGitDiffAlgorithm;
  findRenames: boolean;
  pathspecs: string[];
} {
  const requestId = typeof args.request.requestId === "string" ? args.request.requestId.trim() : "";
  const responseSubject = typeof args.request.responseSubject === "string" ? args.request.responseSubject.trim() : "";
  const requestAgentId = typeof args.request.agentId === "string" ? args.request.agentId.trim() : "";
  const targetPath = typeof args.request.targetPath === "string" ? args.request.targetPath.trim() : "";
  if (!requestId || !responseSubject || !requestAgentId || requestAgentId !== args.agentId || !targetPath) {
    throw new Error("invalid git rpc request");
  }
  const format =
    args.request.format === "name-only" ||
    args.request.format === "name-status" ||
    args.request.format === "stat" ||
    args.request.format === "numstat" ||
    args.request.format === "raw"
      ? args.request.format
      : "patch";
  const ignoreWhitespace =
    args.request.ignoreWhitespace === "at-eol" ||
    args.request.ignoreWhitespace === "change" ||
    args.request.ignoreWhitespace === "all"
      ? args.request.ignoreWhitespace
      : "none";
  const diffAlgorithm =
    args.request.diffAlgorithm === "minimal" ||
    args.request.diffAlgorithm === "patience" ||
    args.request.diffAlgorithm === "histogram"
      ? args.request.diffAlgorithm
      : "default";
  const contextRaw = Number(args.request.contextLines);
  const contextLines = Number.isFinite(contextRaw) ? Math.max(0, Math.min(200, Math.trunc(contextRaw))) : null;
  const pathspecs = Array.isArray(args.request.pathspecs) ? args.request.pathspecs.map((item) => sanitizeGitPathspec(item)) : [];
  return {
    requestId,
    responseSubject,
    targetPath,
    base: sanitizeGitRef(args.request.base),
    target: sanitizeGitRef(args.request.target),
    mergeBase: args.request.mergeBase === true,
    staged: args.request.staged === true,
    format,
    contextLines,
    ignoreWhitespace,
    diffAlgorithm,
    findRenames: args.request.findRenames === true,
    pathspecs,
  };
}

function buildAgentGitDiffArgs(repoRootAbs: string, request: ReturnType<typeof normalizeGitRpcRequest>): { args: string[]; display: string } {
  const args = ["-C", repoRootAbs, "diff", "--no-color"];
  const displayParts = ["git", "diff", "--no-color"];
  if (request.staged) {
    args.push("--cached");
    displayParts.push("--cached");
  }
  if (typeof request.contextLines === "number") {
    args.push(`-U${request.contextLines}`);
    displayParts.push(`-U${request.contextLines}`);
  }
  if (request.ignoreWhitespace === "at-eol") {
    args.push("--ignore-space-at-eol");
    displayParts.push("--ignore-space-at-eol");
  } else if (request.ignoreWhitespace === "change") {
    args.push("--ignore-space-change");
    displayParts.push("--ignore-space-change");
  } else if (request.ignoreWhitespace === "all") {
    args.push("--ignore-all-space");
    displayParts.push("--ignore-all-space");
  }
  if (request.diffAlgorithm !== "default") {
    args.push(`--diff-algorithm=${request.diffAlgorithm}`);
    displayParts.push(`--diff-algorithm=${request.diffAlgorithm}`);
  }
  if (request.findRenames) {
    args.push("--find-renames");
    displayParts.push("--find-renames");
  }
  if (request.format === "name-only") {
    args.push("--name-only");
    displayParts.push("--name-only");
  } else if (request.format === "name-status") {
    args.push("--name-status");
    displayParts.push("--name-status");
  } else if (request.format === "stat") {
    args.push("--stat");
    displayParts.push("--stat");
  } else if (request.format === "numstat") {
    args.push("--numstat");
    displayParts.push("--numstat");
  } else if (request.format === "raw") {
    args.push("--raw");
    displayParts.push("--raw");
  }
  if (request.mergeBase) {
    if (!request.base || !request.target) {
      throw new Error("mergeBase mode requires both base and target");
    }
    const merged = `${request.base}...${request.target}`;
    args.push(merged);
    displayParts.push(merged);
  } else {
    if (request.base) {
      args.push(request.base);
      displayParts.push(request.base);
    }
    if (request.target) {
      args.push(request.target);
      displayParts.push(request.target);
    }
  }
  if (request.pathspecs.length > 0) {
    args.push("--", ...request.pathspecs);
    displayParts.push("--", ...request.pathspecs);
  }
  return { args, display: displayParts.join(" ") };
}

function buildUntrackedText(format: AgentGitDiffFormat, untrackedPaths: string[]): string {
  if (untrackedPaths.length === 0) {
    return "";
  }
  if (format === "name-status" || format === "raw") {
    return `${untrackedPaths.map((item) => `??\t${item}`).join("\n")}\n`;
  }
  if (format === "name-only") {
    return `${untrackedPaths.join("\n")}\n`;
  }
  return `\n# Untracked files\n${untrackedPaths.join("\n")}\n`;
}

async function appendAgentLocalUntrackedDiff(
  repoRootAbs: string,
  request: ReturnType<typeof normalizeGitRpcRequest>,
  baseOutput: string,
): Promise<{ output: string; hasUntracked: boolean }> {
  const listArgs = ["-C", repoRootAbs, "ls-files", "--others", "--exclude-standard"];
  if (request.pathspecs.length > 0) {
    listArgs.push("--", ...request.pathspecs);
  }
  const listResult = await runLocalCommand("git", listArgs, repoRootAbs);
  if (listResult.code !== 0) {
    return { output: baseOutput, hasUntracked: false };
  }
  const untrackedPaths = listResult.stdout.split(/\r?\n/).map((item) => item.trim()).filter(Boolean);
  if (untrackedPaths.length === 0) {
    return { output: baseOutput, hasUntracked: false };
  }
  if (request.format !== "patch") {
    return { output: `${baseOutput}${buildUntrackedText(request.format, untrackedPaths)}`, hasUntracked: true };
  }
  let output = baseOutput;
  for (const relPath of untrackedPaths) {
    const diffResult = await runLocalCommand("git", ["-C", repoRootAbs, "diff", "--no-color", "--no-index", "--", "/dev/null", relPath], repoRootAbs);
    if (diffResult.code !== 0 && diffResult.code !== 1) {
      throw new Error(diffResult.stderr.trim() || `Failed to render agent untracked diff: ${relPath}`);
    }
    if (diffResult.stdout) {
      output += diffResult.stdout;
      if (!output.endsWith("\n")) {
        output += "\n";
      }
    }
  }
  return { output, hasUntracked: true };
}

function publishGitRpcResponse(args: {
  nc: NatsConnection;
  responseSubject: string;
  payload: AgentGitRpcResponse;
}): void {
  args.nc.publish(args.responseSubject, gitRpcCodec.encode(JSON.stringify(args.payload)));
}

async function handleGitRpcMessage(args: {
  msg: Msg;
  jetstream: AgentJetStreamContext;
  userId: string;
  agentId: string;
}): Promise<void> {
  let requestId = "unknown";
  let responseSubject = "";
  try {
    const payload = JSON.parse(gitRpcCodec.decode(args.msg.data)) as AgentGitRpcRequest;
    const request = normalizeGitRpcRequest({ request: payload, agentId: args.agentId });
    requestId = request.requestId;
    responseSubject = request.responseSubject;
    if (!request.targetPath.startsWith("/")) {
      throw new Error("agent source requires an absolute directory path");
    }

    const topLevelResult = await runLocalCommand("git", ["-C", request.targetPath, "rev-parse", "--show-toplevel"], request.targetPath);
    if (topLevelResult.code !== 0) {
      publishGitRpcResponse({
        nc: args.jetstream.nc,
        responseSubject,
        payload: {
          requestId,
          ok: true,
          payload: {
            isGitRepo: false,
            mode: "git_diff",
            source: "agent",
            agent: { id: args.agentId, name: null },
            currentPath: request.targetPath,
            repoRoot: null,
            repoRelativePath: null,
            branch: null,
            gitDiff: {
              command: "git diff --no-color",
              format: "patch",
              output: "",
              outputTruncated: false,
            },
            message: "현재 경로가 Git 저장소가 아닙니다.",
          },
        },
      });
      return;
    }

    const repoRootAbs = topLevelResult.stdout.trim();
    const prefixResult = await runLocalCommand("git", ["-C", request.targetPath, "rev-parse", "--show-prefix"], request.targetPath);
    const repoRelativePath = prefixResult.code === 0 ? (prefixResult.stdout.trim().replace(/\/$/, "") || ".") : ".";
    const branchResult = await runLocalCommand("git", ["-C", repoRootAbs, "symbolic-ref", "--quiet", "--short", "HEAD"], repoRootAbs);
    const detachedResult =
      branchResult.code === 0 ? null : await runLocalCommand("git", ["-C", repoRootAbs, "rev-parse", "--short", "HEAD"], repoRootAbs);
    const branch =
      branchResult.code === 0 ? branchResult.stdout.trim() || null : detachedResult && detachedResult.code === 0 ? detachedResult.stdout.trim() || null : null;

    const gitDiffArgs = buildAgentGitDiffArgs(repoRootAbs, request);
    const gitDiffResult = await runLocalCommand("git", gitDiffArgs.args, repoRootAbs);
    if (gitDiffResult.code !== 0) {
      throw new Error(gitDiffResult.stderr.trim() || "Failed to run agent git diff");
    }
    const withUntracked = await appendAgentLocalUntrackedDiff(repoRootAbs, request, gitDiffResult.stdout);
    publishGitRpcResponse({
      nc: args.jetstream.nc,
      responseSubject,
      payload: {
        requestId,
        ok: true,
        payload: {
          isGitRepo: true,
          mode: "git_diff",
          source: "agent",
          agent: { id: args.agentId, name: null },
          currentPath: request.targetPath,
          repoRoot: repoRootAbs,
          repoRelativePath,
          branch,
          gitDiff: {
            command: withUntracked.hasUntracked ? `${gitDiffArgs.display} (+ untracked)` : gitDiffArgs.display,
            format: request.format,
            output: withUntracked.output,
            outputTruncated: false,
          },
        },
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (responseSubject) {
      publishGitRpcResponse({
        nc: args.jetstream.nc,
        responseSubject,
        payload: { requestId, ok: false, error: message },
      });
    }
    writeAgentError(`git rpc failed requestId=${requestId} error=${message}`);
  }
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
        jetstream: args.jetstream,
        userId: args.userId,
        agentId: args.agentId,
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
    const request = normalizeRunRpcRequest({ request: payload, agentId: args.agentId });
    requestId = request.requestId;
    responseSubject = request.responseSubject;

    if (request.action === "start") {
      const runId = request.runId ?? requestId;
      await claimRunStartSlot({ runId, sessionId: request.sessionId });
      try {
        const task = await startManagedRun({
          requestId,
          runId,
          serverBaseUrl: args.serverBaseUrl,
          userId: args.userId,
          agentId: args.agentId,
          sessionId: request.sessionId,
          codexArgs: buildManagedCodexArgs({
            prompt: request.prompt ?? "",
            sessionId: request.sessionId,
            model: request.model,
          }),
          cwd: request.cwd,
          runtimeEnvPatch: request.runtimeEnvPatch,
          codexAuthBundle: request.codexAuthBundle,
          agentToken: args.agentToken,
        });
        publishRunRpcResponse({ nc: args.jetstream.nc, responseSubject, payload: { requestId, ok: true, task } });
      } catch (error) {
        await releaseRunStartSlot({ runId, sessionId: request.sessionId }).catch(() => undefined);
        throw error;
      }
      return;
    }

    if (request.action === "list") {
      const tasks = [...activeRuns.values()].map((entry) => cloneRunTask(entry.task));
      const retained = [...retainedRuns.values()].filter((task) => !activeRuns.has(task.id)).map((task) => cloneRunTask(task));
      const merged = [...tasks, ...retained]
        .sort((a, b) => Date.parse(b.updatedAt) - Date.parse(a.updatedAt))
        .slice(0, request.limit);
      publishRunRpcResponse({ nc: args.jetstream.nc, responseSubject, payload: { requestId, ok: true, tasks: merged } });
      return;
    }

    const stored = request.runId ? getStoredRun(request.runId) : null;
    if (!stored || stored.agentId !== args.agentId || stored.userId !== args.userId) {
      throw new Error("Run not found");
    }

    if (request.action === "cancel") {
      const active = activeRuns.get(stored.id);
      active?.requestCancel();
      const task = cloneRunTask(active?.task ?? stored);
      publishRunRpcResponse({ nc: args.jetstream.nc, responseSubject, payload: { requestId, ok: true, task } });
      return;
    }

    const task = cloneRunTask(stored, request.sinceSeq);
    publishRunRpcResponse({ nc: args.jetstream.nc, responseSubject, payload: { requestId, ok: true, task } });
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

function isLikelyNatsAuthError(error: unknown): boolean {
  const message = (error instanceof Error ? error.message : String(error)).toLowerCase();
  return (
    message.includes("auth")
    || message.includes("authorization")
    || message.includes("authentication")
    || message.includes("permission")
    || message.includes("jwt")
    || message.includes("token")
  );
}

function isLikelyNatsReconnectError(error: unknown): boolean {
  const message = (error instanceof Error ? error.message : String(error)).toLowerCase();
  return (
    message.includes("connection_closed")
    || message.includes("connection closed")
    || message.includes("closed connection")
    || message.includes("disconnected")
    || message.includes("timeout")
    || message.includes("no responders")
  );
}

function sendSignalToTaskProcess(child: ReturnType<typeof spawn>, signal: NodeJS.Signals): void {
  if (process.platform !== "win32" && typeof child.pid === "number") {
    try {
      // Detached child owns a process group; signal the whole group first.
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

function requestTaskCancellation(taskId: string, reason: string): boolean {
  const requestCancel = activeTaskCancelRequests.get(taskId);
  if (!requestCancel) {
    return false;
  }
  try {
    requestCancel();
    writeAgentInfo(`task cancel requested taskId=${taskId} via=${reason}`);
    return true;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    writeAgentError(`task cancel request failed taskId=${taskId} via=${reason}: ${message}`);
    return false;
  }
}

function resolveLogTimeZone(): string {
  const configured = process.env.DOER_AGENT_LOG_TIMEZONE?.trim() || process.env.TZ?.trim();
  return configured && configured.length > 0 ? configured : "Asia/Seoul";
}

function resolveTimeZoneOffsetString(date: Date, timeZone: string): string {
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone,
      timeZoneName: "shortOffset",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    }).formatToParts(date);
    const token = parts.find((part) => part.type === "timeZoneName")?.value || "GMT+0";
    const matched = token.match(/GMT([+-]\d{1,2})(?::?(\d{2}))?/i);
    if (!matched) {
      return "+00:00";
    }
    const hourRaw = matched[1] || "+0";
    const minuteRaw = matched[2] || "00";
    const sign = hourRaw.startsWith("-") ? "-" : "+";
    const absHour = String(Math.abs(Number.parseInt(hourRaw, 10))).padStart(2, "0");
    const absMinute = String(Math.abs(Number.parseInt(minuteRaw, 10))).padStart(2, "0");
    return `${sign}${absHour}:${absMinute}`;
  } catch {
    return "+00:00";
  }
}

function formatLocalTimestamp(date = new Date()): string {
  const timeZone = resolveLogTimeZone();
  try {
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    }).formatToParts(date);
    const pick = (type: Intl.DateTimeFormatPartTypes): string => {
      return parts.find((part) => part.type === type)?.value || "00";
    };
    const year = pick("year");
    const month = pick("month");
    const day = pick("day");
    const hours = pick("hour");
    const minutes = pick("minute");
    const seconds = pick("second");
    const ms = String(date.getMilliseconds()).padStart(3, "0");
    const offset = resolveTimeZoneOffsetString(date, timeZone);
    return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}.${ms}${offset}`;
  } catch {
    return date.toISOString();
  }
}

function parseArgs(argv: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    if (!key.startsWith("--")) {
      continue;
    }
    const value = argv[i + 1];
    if (typeof value === "string" && !value.startsWith("--")) {
      out[key.slice(2)] = value;
      i += 1;
      continue;
    }
    out[key.slice(2)] = "true";
  }
  return out;
}

function resolveArgOrEnv(args: Record<string, string>, argKeys: string[], envKeys: string[], fallback = ""): string {
  for (const key of argKeys) {
    const value = args[key]?.trim();
    if (value) {
      return value;
    }
  }
  for (const key of envKeys) {
    const value = process.env[key]?.trim();
    if (value) {
      return value;
    }
  }
  return fallback;
}

function resolveShellPath(): string {
  if (process.platform === "win32") {
    return process.env.ComSpec || "cmd.exe";
  }
  const candidates = [process.env.SHELL, "/bin/bash", "/usr/bin/bash", "/bin/sh", "/usr/bin/sh"].filter(
    (value): value is string => typeof value === "string" && value.trim().length > 0,
  );
  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }
  throw new Error("No shell executable found. Set SHELL env or install /bin/sh (or bash).");
}

function resolveTaskWorkspace(rawCwd: string | null): string {
  const workspaceRoot = workspaceRootOverride ?? (process.env.WORKSPACE?.trim() || process.cwd());
  const requestedCwd = rawCwd?.trim() || "";
  const resolvedCwd = requestedCwd
    ? path.isAbsolute(requestedCwd)
      ? path.resolve(requestedCwd)
      : path.resolve(workspaceRoot, requestedCwd)
    : workspaceRoot;

  if (!existsSync(resolvedCwd)) {
    throw new Error(
      `Invalid cwd: ${requestedCwd || "(empty)"} resolved to ${resolvedCwd} (path does not exist)`,
    );
  }

  let stats;
  try {
    stats = statSync(resolvedCwd);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Invalid cwd: ${requestedCwd || "(empty)"} resolved to ${resolvedCwd} (${message})`,
    );
  }

  if (!stats.isDirectory()) {
    throw new Error(
      `Invalid cwd: ${requestedCwd || "(empty)"} resolved to ${resolvedCwd} (not a directory)`,
    );
  }

  return resolvedCwd;
}

function buildAgentFsRpcSubject(userId: string, agentId: string): string {
  return `doer.agent.fs.rpc.${sanitizeUserId(userId)}.${agentId.trim()}`;
}

function buildAgentShellRpcSubject(userId: string, agentId: string): string {
  return `doer.agent.shell.rpc.${sanitizeUserId(userId)}.${agentId.trim()}`;
}

function normalizeFsRpcPath(rawPath: unknown): { abs: string; formatPath: (target: string) => string } {
  const root = workspaceRootOverride ?? (process.env.WORKSPACE?.trim() || process.cwd());
  const raw = typeof rawPath === "string" && rawPath.trim() ? rawPath.trim() : ".";
  const normalizedRaw = raw.replace(/\\/g, "/");
  const useAbsolute = path.isAbsolute(normalizedRaw);
  const rel = normalizedRaw.replace(/^\/+/, "") || ".";
  const abs = useAbsolute ? path.resolve(normalizedRaw) : path.resolve(root, rel);
  if (!useAbsolute && abs !== root && !abs.startsWith(root + path.sep)) {
    throw new Error("path escapes workspace root");
  }
  const formatPath = (target: string): string => {
    if (useAbsolute) {
      return target.split(path.sep).join("/") || "/";
    }
    return path.relative(root, target).split(path.sep).join("/") || ".";
  };
  return { abs, formatPath };
}

function parseFsRpcAction(value: unknown): AgentFsRpcAction {
  if (
    value === "list" ||
    value === "stat" ||
    value === "fetch_file" ||
    value === "read_text" ||
    value === "read_file" ||
    value === "write_file" ||
    value === "download_file"
  ) {
    return value;
  }
  throw new Error("unsupported action");
}

function normalizeFsRpcNumber(value: unknown, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return fallback;
  }
  return Math.floor(n);
}

function inferMimeType(filePath: string): string {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === ".txt" || ext === ".md" || ext === ".log") {
    return "text/plain";
  }
  if (ext === ".json") {
    return "application/json";
  }
  if (ext === ".js" || ext === ".mjs" || ext === ".cjs") {
    return "text/javascript";
  }
  if (ext === ".ts" || ext === ".tsx") {
    return "text/typescript";
  }
  if (ext === ".jsx") {
    return "text/jsx";
  }
  if (ext === ".css") {
    return "text/css";
  }
  if (ext === ".html" || ext === ".htm") {
    return "text/html";
  }
  if (ext === ".xml") {
    return "application/xml";
  }
  if (ext === ".svg") {
    return "image/svg+xml";
  }
  if (ext === ".png") {
    return "image/png";
  }
  if (ext === ".jpg" || ext === ".jpeg") {
    return "image/jpeg";
  }
  if (ext === ".gif") {
    return "image/gif";
  }
  if (ext === ".webp") {
    return "image/webp";
  }
  if (ext === ".pdf") {
    return "application/pdf";
  }
  return "application/octet-stream";
}

async function executeFsRpc(args: {
  request: AgentFsRpcRequest;
  serverBaseUrl: string;
  agentToken: string;
}): Promise<Record<string, unknown>> {
  const action = parseFsRpcAction(args.request.action);
  const { abs, formatPath } = normalizeFsRpcPath(args.request.path);

  if (action === "stat") {
    const entry = await stat(abs);
    return {
      ok: true,
      action,
      path: formatPath(abs),
      kind: entry.isDirectory() ? "dir" : "file",
      size: entry.size,
      mtimeMs: entry.mtimeMs,
    };
  }

  if (action === "list") {
    const entry = await stat(abs);
    if (!entry.isDirectory()) {
      throw new Error("path is not a directory");
    }
    const limit = Math.max(1, Math.min(normalizeFsRpcNumber(args.request.limit, 200), 1000));
    const rows = await readdir(abs, { withFileTypes: true });
    const items = await Promise.all(
      rows.map(async (row) => {
        const child = path.join(abs, row.name);
        const childStat = await stat(child);
        return {
          name: row.name,
          path: formatPath(child),
          kind: row.isDirectory() ? "dir" : "file",
          size: childStat.size,
          mtimeMs: childStat.mtimeMs,
        };
      }),
    );
    items.sort((a, b) => (a.kind === b.kind ? a.name.localeCompare(b.name) : a.kind === "dir" ? -1 : 1));
    return {
      ok: true,
      action,
      path: formatPath(abs),
      items: items.slice(0, limit),
      truncated: items.length > limit,
      total: items.length,
    };
  }

  if (action === "fetch_file") {
    const entry = await stat(abs);
    if (!entry.isFile()) {
      throw new Error("path is not a file");
    }
    const uploadUrl = typeof args.request.uploadUrl === "string" ? args.request.uploadUrl : "";
    const agentId = typeof args.request.agentId === "string" ? args.request.agentId : "";
    if (!uploadUrl || !agentId) {
      throw new Error("missing upload parameters");
    }
    const data = await readFile(abs);
    const fileName = path.basename(abs) || "file";
    const form = new FormData();
    form.append("file", new File([data], fileName));
    form.append("agentId", agentId);
    const response = await fetch(uploadUrl, {
      method: "POST",
      headers: { Authorization: `Bearer ${args.agentToken}` },
      body: form,
    });
    const text = await response.text();
    let upload: Record<string, unknown> = {};
    try {
      upload = JSON.parse(text || "{}") as Record<string, unknown>;
    } catch {
      upload = {};
    }
    if (!response.ok) {
      const message = typeof upload.error === "string" ? upload.error : `upload failed: ${response.status}`;
      throw new Error(message);
    }
    return {
      ok: true,
      action,
      path: formatPath(abs),
      size: entry.size,
      upload,
    };
  }

  if (action === "write_file") {
    const contentBase64 = typeof args.request.contentBase64 === "string" ? args.request.contentBase64 : "";
    if (!contentBase64) {
      throw new Error("contentBase64 is required");
    }
    const parentDir = path.dirname(abs);
    await mkdir(parentDir, { recursive: true });
    const bytes = Buffer.from(contentBase64, "base64");
    await writeFile(abs, bytes);
    const entry = await stat(abs);
    return {
      ok: true,
      action,
      path: formatPath(abs),
      absolutePath: abs.split(path.sep).join("/"),
      size: entry.size,
      mimeType: inferMimeType(abs),
      mtimeMs: entry.mtimeMs,
    };
  }

  if (action === "download_file") {
    const downloadPath = typeof args.request.downloadPath === "string" ? args.request.downloadPath.trim() : "";
    if (!downloadPath) {
      throw new Error("downloadPath is required");
    }
    const downloadUrl = new URL(downloadPath, `${args.serverBaseUrl}/`).toString();
    const response = await fetch(downloadUrl, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${args.agentToken}`,
      },
    });
    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new Error(text || `download failed: ${response.status}`);
    }
    const bytes = Buffer.from(await response.arrayBuffer());
    const parentDir = path.dirname(abs);
    await mkdir(parentDir, { recursive: true });
    await writeFile(abs, bytes);
    const entry = await stat(abs);
    return {
      ok: true,
      action,
      path: formatPath(abs),
      absolutePath: abs.split(path.sep).join("/"),
      size: entry.size,
      mimeType: inferMimeType(abs),
      mtimeMs: entry.mtimeMs,
    };
  }

  const entry = await stat(abs);
  if (!entry.isFile()) {
    throw new Error("path is not a file");
  }
  if (action === "read_file") {
    const maxBytes = Math.max(1, Math.min(normalizeFsRpcNumber(args.request.maxBytes, 2_000_000), 5_000_000));
    const data = await readFile(abs);
    const truncated = data.byteLength > maxBytes;
    const bytes = truncated ? data.subarray(0, maxBytes) : data;
    return {
      ok: true,
      action,
      path: formatPath(abs),
      mimeType: inferMimeType(abs),
      size: entry.size,
      truncated,
      contentBase64: bytes.toString("base64"),
    };
  }
  const offset = Math.max(0, normalizeFsRpcNumber(args.request.offset, 0));
  const length = Math.max(1, Math.min(normalizeFsRpcNumber(args.request.length, 65536), 262144));
  const encoding = typeof args.request.encoding === "string" && args.request.encoding ? args.request.encoding : "utf8";
  const fd = await open(abs, "r");
  try {
    const buffer = Buffer.alloc(length);
    const readResult = await fd.read(buffer, 0, length, offset);
    const slice = buffer.subarray(0, readResult.bytesRead);
    try {
      const text = slice.toString(encoding as BufferEncoding);
      return {
        ok: true,
        action,
        path: formatPath(abs),
        offset,
        length: readResult.bytesRead,
        totalSize: entry.size,
        eof: offset + readResult.bytesRead >= entry.size,
        encoding,
        text,
        bytesRead: readResult.bytesRead,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : "failed to decode text";
      return {
        ok: false,
        action,
        path: formatPath(abs),
        error: message,
      };
    }
  } finally {
    await fd.close();
  }
}

interface AgentSessionSummaryRecord {
  id: string;
  label: string;
  updatedAt: string;
  cwd: string | null;
  source: string;
  originator: string;
  filePath: string;
}

interface AgentSessionRawRowRecord {
  id: number;
  raw: string;
}

interface SessionLineIndexCacheEntry {
  size: number;
  lineStartOffsets: number[];
  endsWithNewline: boolean;
}

function normalizeSessionRpcRequest(args: {
  request: AgentSessionRpcRequest;
  agentId: string;
}): AgentSessionRpcNormalizedRequest {
  const requestId = typeof args.request.requestId === "string" ? args.request.requestId.trim() : "";
  if (!requestId) {
    throw new Error("missing requestId");
  }
  const requestAgentId = typeof args.request.agentId === "string" ? args.request.agentId.trim() : "";
  if (!requestAgentId || requestAgentId !== args.agentId) {
    throw new Error("agent id mismatch");
  }
  const actionRaw = typeof args.request.action === "string" ? args.request.action.trim() : "";
  const action: AgentSessionRpcAction =
    actionRaw === "messages" || actionRaw === "delete" || actionRaw === "watch" || actionRaw === "stop_watch"
      ? actionRaw
      : "list";
  const responseSubject = typeof args.request.responseSubject === "string" ? args.request.responseSubject.trim() : "";
  if (!responseSubject) {
    throw new Error("missing responseSubject");
  }
  const filePath = typeof args.request.filePath === "string" && args.request.filePath.trim() ? args.request.filePath.trim() : null;
  if ((action === "messages" || action === "delete" || action === "watch") && !filePath) {
    throw new Error("missing filePath");
  }
  const sinceLineRaw = Number(args.request.sinceLine);
  const sinceLine = Number.isInteger(sinceLineRaw) && sinceLineRaw > 0 ? sinceLineRaw : 0;
  const beforeRowIdRaw = Number(args.request.beforeRowId);
  const beforeRowId = Number.isInteger(beforeRowIdRaw) && beforeRowIdRaw > 0 ? beforeRowIdRaw : null;
  const pageSizeRaw = Number(args.request.pageSize);
  const pageSize = Number.isFinite(pageSizeRaw) ? Math.max(1, Math.min(Math.floor(pageSizeRaw), 100)) : 100;
  const watchId = typeof args.request.watchId === "string" && args.request.watchId.trim() ? args.request.watchId.trim() : null;
  if (action === "stop_watch" && !watchId) {
    throw new Error("missing watchId");
  }
  return {
    requestId,
    action,
    agentId: requestAgentId,
    filePath,
    sessionId: typeof args.request.sessionId === "string" && args.request.sessionId.trim() ? args.request.sessionId.trim() : null,
    sinceLine,
    beforeRowId,
    pageSize,
    responseSubject,
    watchId,
  };
}

function getSessionsRootPath(): string {
  const workspaceRoot = workspaceRootOverride ?? (process.env.WORKSPACE?.trim() || process.cwd());
  return path.join(workspaceRoot, ".codex", "sessions");
}

function resolveSessionFilePath(filePath: string): string {
  const root = path.resolve(getSessionsRootPath());
  const resolved = path.resolve(filePath);
  if (!(resolved === root || resolved.startsWith(root + path.sep))) {
    throw new Error("filePath is outside sessions root");
  }
  return resolved;
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function toTrimmedStringOrNull(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed || null;
}

function pickSessionString(...values: unknown[]): string | null {
  for (const value of values) {
    const picked = toTrimmedStringOrNull(value);
    if (picked) {
      return picked;
    }
  }
  return null;
}

async function collectSessionJsonlFiles(rootDir: string): Promise<Array<{ filePath: string; mtimeMs: number }>> {
  const out: Array<{ filePath: string; mtimeMs: number }> = [];
  const stack = [rootDir];
  while (stack.length > 0) {
    const current = stack.pop();
    if (!current) {
      continue;
    }
    let entries = [];
    try {
      entries = await readdir(current, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
        continue;
      }
      if (!entry.isFile() || !entry.name.toLowerCase().endsWith(".jsonl")) {
        continue;
      }
      try {
        const entryStat = await stat(fullPath);
        out.push({ filePath: fullPath, mtimeMs: entryStat.mtimeMs });
      } catch {
        // ignore removed files
      }
    }
  }
  return out;
}

async function readFirstLine(fileHandle: Awaited<ReturnType<typeof open>>, fileSize: number): Promise<string> {
  const chunkBytes = 16_384;
  const maxScanBytes = 262_144;
  let position = 0;
  let scanned = 0;
  let raw = "";

  while (position < fileSize && scanned < maxScanBytes) {
    const readSize = Math.min(chunkBytes, fileSize - position, maxScanBytes - scanned);
    const buffer = Buffer.alloc(readSize);
    const { bytesRead } = await fileHandle.read(buffer, 0, readSize, position);
    if (bytesRead <= 0) {
      break;
    }
    raw += buffer.toString("utf8", 0, bytesRead);
    scanned += bytesRead;
    position += bytesRead;
    const newlineIndex = raw.search(/\r?\n/);
    if (newlineIndex >= 0) {
      return raw.slice(0, newlineIndex).trim();
    }
  }

  return raw.trim();
}

function extractLastAgentMessage(candidateLines: string[]): { message: string; updatedAt: string | null } | null {
  let fallback: { message: string; updatedAt: string | null } | null = null;
  for (const line of candidateLines) {
    const trimmed = line.trim();
    if (!trimmed) {
      continue;
    }
    try {
      const parsed = JSON.parse(trimmed) as { type?: unknown; timestamp?: unknown; payload?: unknown };
      if (parsed.type !== "event_msg" || !isObjectRecord(parsed.payload)) {
        continue;
      }
      if (parsed.payload.type === "agent_message" && typeof parsed.payload.message === "string" && parsed.payload.message.trim()) {
        return {
          message: parsed.payload.message.trim(),
          updatedAt: toTrimmedStringOrNull(parsed.timestamp),
        };
      }
      if (
        !fallback &&
        parsed.payload.type === "task_complete" &&
        typeof parsed.payload.last_agent_message === "string" &&
        parsed.payload.last_agent_message.trim()
      ) {
        fallback = {
          message: parsed.payload.last_agent_message.trim(),
          updatedAt: toTrimmedStringOrNull(parsed.timestamp),
        };
      }
    } catch {
      // ignore malformed lines
    }
  }
  return fallback;
}

async function readLastAgentMessage(
  fileHandle: Awaited<ReturnType<typeof open>>,
  fileSize: number,
): Promise<{ message: string; updatedAt: string | null } | null> {
  const chunkBytes = 16_384;
  const maxScanBytes = 131_072;
  if (fileSize <= 0) {
    return null;
  }
  let position = fileSize;
  let scanned = 0;
  let carry = "";
  while (position > 0 && scanned < maxScanBytes) {
    const readSize = Math.min(chunkBytes, position, maxScanBytes - scanned);
    position -= readSize;
    scanned += readSize;
    const buffer = Buffer.alloc(readSize);
    const { bytesRead } = await fileHandle.read(buffer, 0, readSize, position);
    if (bytesRead <= 0) {
      break;
    }
    const merged = buffer.toString("utf8", 0, bytesRead) + carry;
    const lines = merged.split(/\r?\n/);
    carry = lines.shift() || "";
    const found = extractLastAgentMessage(lines.reverse());
    if (found) {
      return found;
    }
  }
  return extractLastAgentMessage([carry]);
}

function normalizeSessionMeta(rawMeta: unknown, filePath: string, mtimeMs: number): AgentSessionSummaryRecord {
  const baseName = path.basename(filePath, path.extname(filePath));
  const meta = isObjectRecord(rawMeta) ? rawMeta : {};
  const updatedAtCandidate = pickSessionString(meta.updatedAt, meta.updated_at, meta.timestamp);
  return {
    id: pickSessionString(meta.sessionId, meta.session_id, meta.id) || baseName,
    label: pickSessionString(meta.label, meta.title, meta.name, meta.sessionLabel, meta.session_label) || baseName,
    updatedAt: updatedAtCandidate || new Date(mtimeMs).toISOString(),
    cwd: pickSessionString(meta.cwd, meta.workingDirectory, meta.working_directory),
    source: pickSessionString(meta.source, meta.sessionSource, meta.session_source) || "codex",
    originator: pickSessionString(meta.originator, meta.author, meta.user, meta.username) || "unknown",
    filePath,
  };
}

async function readSessionSummary(filePath: string, mtimeMs: number): Promise<AgentSessionSummaryRecord> {
  let fileHandle: Awaited<ReturnType<typeof open>> | null = null;
  try {
    fileHandle = await open(filePath, "r");
    const entryStat = await fileHandle.stat();
    const firstLine = await readFirstLine(fileHandle, entryStat.size);
    const tailSummary = await readLastAgentMessage(fileHandle, entryStat.size);
    let normalized = normalizeSessionMeta({}, filePath, mtimeMs);

    if (firstLine) {
      try {
        const parsed = JSON.parse(firstLine) as Record<string, unknown>;
        const candidateMeta =
          parsed && parsed.type === "session_meta" && isObjectRecord(parsed.payload)
            ? parsed.payload
            : isObjectRecord(parsed.session_meta)
              ? parsed.session_meta
              : isObjectRecord(parsed.sessionMeta)
                ? parsed.sessionMeta
                : isObjectRecord(parsed.meta)
                  ? parsed.meta
                  : isObjectRecord(parsed.payload)
                    ? parsed.payload
                    : parsed;
        normalized = normalizeSessionMeta(candidateMeta, filePath, mtimeMs);
      } catch {
        normalized = normalizeSessionMeta({}, filePath, mtimeMs);
      }
    }

    return {
      ...normalized,
      label: tailSummary?.message || "(no agent message)",
      updatedAt: tailSummary?.updatedAt || normalized.updatedAt,
    };
  } catch {
    return normalizeSessionMeta({}, filePath, mtimeMs);
  } finally {
    await fileHandle?.close().catch(() => undefined);
  }
}

async function listAgentSessions(): Promise<AgentSessionSummaryRecord[]> {
  const sessionsRoot = getSessionsRootPath();
  let sessionsRootStat;
  try {
    sessionsRootStat = await stat(sessionsRoot);
  } catch {
    return [];
  }
  if (!sessionsRootStat.isDirectory()) {
    return [];
  }
  const files = await collectSessionJsonlFiles(sessionsRoot);
  files.sort((a, b) => b.mtimeMs - a.mtimeMs || a.filePath.localeCompare(b.filePath));
  const sessions = await Promise.all(files.slice(0, 10).map((file) => readSessionSummary(file.filePath, file.mtimeMs)));
  return sessions.sort((a, b) => Date.parse(b.updatedAt) - Date.parse(a.updatedAt) || b.filePath.localeCompare(a.filePath));
}

async function readSessionLineIndex(filePath: string): Promise<SessionLineIndexCacheEntry> {
  const resolvedFile = resolveSessionFilePath(filePath);
  const entryStat = await stat(resolvedFile);
  const nextSize = entryStat.size;
  const cached = sessionLineIndexCache.get(resolvedFile) ?? null;

  if (cached && cached.size === nextSize) {
    return cached;
  }

  const fileHandle = await open(resolvedFile, "r");
  try {
    let lineStartOffsets = cached?.lineStartOffsets.slice() ?? [];
    let scanStart = cached?.size ?? 0;
    let endsWithNewline = cached?.endsWithNewline ?? false;

    if (!cached || nextSize < cached.size) {
      lineStartOffsets = nextSize > 0 ? [0] : [];
      scanStart = 0;
      endsWithNewline = false;
    } else if (cached.endsWithNewline && nextSize > cached.size) {
      lineStartOffsets.push(cached.size);
    }

    let position = scanStart;
    const chunkBytes = 65_536;
    while (position < nextSize) {
      const readSize = Math.min(chunkBytes, nextSize - position);
      const buffer = Buffer.alloc(readSize);
      const { bytesRead } = await fileHandle.read(buffer, 0, readSize, position);
      if (bytesRead <= 0) {
        break;
      }
      for (let index = 0; index < bytesRead; index += 1) {
        if (buffer[index] !== 0x0a) {
          continue;
        }
        const nextLineStart = position + index + 1;
        if (nextLineStart < nextSize) {
          lineStartOffsets.push(nextLineStart);
        }
      }
      position += bytesRead;
    }

    if (nextSize > 0) {
      const tail = Buffer.alloc(1);
      const { bytesRead } = await fileHandle.read(tail, 0, 1, nextSize - 1);
      endsWithNewline = bytesRead > 0 && tail[0] === 0x0a;
    } else {
      endsWithNewline = false;
    }

    const nextEntry = {
      size: nextSize,
      lineStartOffsets,
      endsWithNewline,
    };
    sessionLineIndexCache.set(resolvedFile, nextEntry);
    return nextEntry;
  } finally {
    await fileHandle.close().catch(() => undefined);
  }
}

async function getAgentSessionRawRows(args: {
  filePath: string;
  sinceLine: number;
  beforeRowId: number | null;
  pageSize: number;
}): Promise<{
  rawRows: AgentSessionRawRowRecord[];
  nextCursor: number;
}> {
  const resolvedFile = resolveSessionFilePath(args.filePath);
  const index = await readSessionLineIndex(resolvedFile);
  const totalLines = index.lineStartOffsets.length;
  const sinceLine = Math.max(0, Math.floor(args.sinceLine));
  const beforeRowId = args.beforeRowId && args.beforeRowId > 0 ? Math.floor(args.beforeRowId) : null;
  const maxRawRows = 200;
  const maxRawBytes = 120_000;

  if (totalLines === 0) {
    return {
      rawRows: [],
      nextCursor: 0,
    };
  }

  let startLineIndex = 0;
  let endLineIndex = totalLines;

  const getLineSpanBytes = (lineIndex: number): number => {
    const start = index.lineStartOffsets[lineIndex] ?? index.size;
    const end = lineIndex + 1 < totalLines ? (index.lineStartOffsets[lineIndex + 1] ?? index.size) : index.size;
    return Math.max(0, end - start);
  };

  if (beforeRowId !== null) {
    endLineIndex = Math.max(0, Math.min(totalLines, beforeRowId - 1));
    startLineIndex = endLineIndex;
    let collectedRows = 0;
    let collectedBytes = 0;
    while (startLineIndex > 0 && collectedRows < maxRawRows) {
      const nextIndex = startLineIndex - 1;
      const nextBytes = getLineSpanBytes(nextIndex);
      if (collectedRows > 0 && collectedBytes + nextBytes > maxRawBytes) {
        break;
      }
      startLineIndex = nextIndex;
      collectedRows += 1;
      collectedBytes += nextBytes;
    }
  } else if (sinceLine > 0) {
    startLineIndex = Math.min(totalLines, sinceLine);
    endLineIndex = startLineIndex;
    let collectedRows = 0;
    let collectedBytes = 0;
    while (endLineIndex < totalLines && collectedRows < maxRawRows) {
      const nextBytes = getLineSpanBytes(endLineIndex);
      if (collectedRows > 0 && collectedBytes + nextBytes > maxRawBytes) {
        break;
      }
      endLineIndex += 1;
      collectedRows += 1;
      collectedBytes += nextBytes;
    }
  } else {
    startLineIndex = totalLines;
    let collectedRows = 0;
    let collectedBytes = 0;
    while (startLineIndex > 0 && collectedRows < maxRawRows) {
      const nextIndex = startLineIndex - 1;
      const nextBytes = getLineSpanBytes(nextIndex);
      if (collectedRows > 0 && collectedBytes + nextBytes > maxRawBytes) {
        break;
      }
      startLineIndex = nextIndex;
      collectedRows += 1;
      collectedBytes += nextBytes;
    }
  }

  if (startLineIndex >= endLineIndex) {
    return {
      rawRows: [],
      nextCursor: endLineIndex,
    };
  }

  const startOffset = index.lineStartOffsets[startLineIndex] ?? index.size;
  const endOffset = endLineIndex < totalLines ? (index.lineStartOffsets[endLineIndex] ?? index.size) : index.size;
  if (startOffset >= endOffset) {
    return {
      rawRows: [],
      nextCursor: endLineIndex,
    };
  }


  const fileHandle = await open(resolvedFile, "r");
  try {
    const readSize = endOffset - startOffset;
    const buffer = Buffer.alloc(readSize);
    const { bytesRead } = await fileHandle.read(buffer, 0, readSize, startOffset);
    const raw = buffer.toString("utf8", 0, bytesRead);
    const lines = raw.split(/\r?\n/);
    if (lines.length > 0 && lines[lines.length - 1] === "") {
      lines.pop();
    }
    const rawRows: AgentSessionRawRowRecord[] = [];
    let lineNumber = startLineIndex + 1;
    for (const line of lines) {
      if (line.trim()) {
        rawRows.push({
          id: lineNumber,
          raw: line,
        });
      }
      lineNumber += 1;
    }
    return {
      rawRows,
      nextCursor: endLineIndex,
    };
  } finally {
    await fileHandle.close().catch(() => undefined);
  }
}

function resolveSessionUploadsDir(sessionId: string): string {
  const workspaceRoot = workspaceRootOverride ?? (process.env.WORKSPACE?.trim() || process.cwd());
  const safeSessionId = sessionId.trim().replace(/[^a-zA-Z0-9._-]/g, "_").slice(0, 160) || "session";
  return path.join(workspaceRoot, ".doer-agent", "sessions", safeSessionId);
}

async function deleteAgentSession(filePath: string, sessionId: string | null): Promise<void> {
  const resolvedFile = resolveSessionFilePath(filePath);
  sessionLineIndexCache.delete(resolvedFile);
  await unlink(resolvedFile);
  if (sessionId) {
    await rm(resolveSessionUploadsDir(sessionId), { recursive: true, force: true }).catch(() => undefined);
  }
  const sessionsRoot = path.resolve(getSessionsRootPath());
  let currentDir = path.dirname(resolvedFile);
  while (currentDir.startsWith(sessionsRoot + path.sep)) {
    try {
      const entries = await readdir(currentDir);
      if (entries.length > 0) {
        break;
      }
      await rmdir(currentDir);
    } catch {
      break;
    }
    currentDir = path.dirname(currentDir);
  }
}

function publishSessionRpcResponse(args: {
  nc: NatsConnection;
  responseSubject: string;
  payload: AgentSessionRpcResponse;
}): void {
  args.nc.publish(args.responseSubject, sessionRpcCodec.encode(JSON.stringify(args.payload)));
}

async function startSessionWatch(args: {
  nc: NatsConnection;
  requestId: string;
  responseSubject: string;
  filePath: string;
}): Promise<string> {
  const resolvedFile = resolveSessionFilePath(args.filePath);
  const watchId = `watch_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
  let watcher: ReturnType<typeof watch> | null = null;
  let active = true;

  const emitEvent = (event: Record<string, unknown>) => {
    if (!active) {
      return;
    }
    publishSessionRpcResponse({
      nc: args.nc,
      responseSubject: args.responseSubject,
      payload: {
        requestId: args.requestId,
        ok: true,
        action: "watch",
        watchId,
        event,
      },
    });
  };

  const cleanup = () => {
    if (!active) {
      return;
    }
    active = false;
    watcher?.close();
    watcher = null;
    activeSessionWatchers.delete(watchId);
  };

  const notifyFromContent = () => {
    emitEvent({
      type: "messages.changed",
      at: formatLocalTimestamp(),
    });
  };

  watcher = watch(resolvedFile, () => {
    notifyFromContent();
  });
  activeSessionWatchers.set(watchId, cleanup);
  emitEvent({ type: "stream.started", watchId, at: formatLocalTimestamp() });
  return watchId;
}

async function handleSessionRpcMessage(args: {
  msg: Msg;
  jetstream: AgentJetStreamContext;
  agentId: string;
}): Promise<void> {
  let requestId = "unknown";
  let responseSubject = "";
  try {
    const payload = JSON.parse(sessionRpcCodec.decode(args.msg.data)) as AgentSessionRpcRequest;
    const request = normalizeSessionRpcRequest({ request: payload, agentId: args.agentId });
    requestId = request.requestId;
    responseSubject = request.responseSubject;

    if (request.action === "list") {
      const sessions = await listAgentSessions();
      publishSessionRpcResponse({
        nc: args.jetstream.nc,
        responseSubject,
        payload: { requestId, ok: true, action: "list", sessions },
      });
      return;
    }

    if (request.action === "messages") {
      const result = await getAgentSessionRawRows({
        filePath: request.filePath ?? "",
        sinceLine: request.sinceLine,
        beforeRowId: request.beforeRowId,
        pageSize: request.pageSize,
      });
      publishSessionRpcResponse({
        nc: args.jetstream.nc,
        responseSubject,
        payload: { requestId, ok: true, action: "messages", rawRows: result.rawRows, nextCursor: result.nextCursor },
      });
      return;
    }

    if (request.action === "delete") {
      await deleteAgentSession(request.filePath ?? "", request.sessionId);
      publishSessionRpcResponse({
        nc: args.jetstream.nc,
        responseSubject,
        payload: { requestId, ok: true, action: "delete" },
      });
      return;
    }

    if (request.action === "watch") {
      const watchId = await startSessionWatch({
        nc: args.jetstream.nc,
        requestId,
        responseSubject,
        filePath: request.filePath ?? "",
      });
      publishSessionRpcResponse({
        nc: args.jetstream.nc,
        responseSubject,
        payload: { requestId, ok: true, action: "watch", watchId },
      });
      return;
    }

    const stop = request.watchId ? activeSessionWatchers.get(request.watchId) : null;
    stop?.();
    publishSessionRpcResponse({
      nc: args.jetstream.nc,
      responseSubject,
      payload: { requestId, ok: true, action: "stop_watch", watchId: request.watchId },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (responseSubject) {
      publishSessionRpcResponse({
        nc: args.jetstream.nc,
        responseSubject,
        payload: {
          requestId,
          ok: false,
          error: message,
        },
      });
    }
    writeAgentError(`session rpc failed requestId=${requestId} error=${message}`);
  }
}

function subscribeToSessionRpc(args: {
  jetstream: AgentJetStreamContext;
  userId: string;
  agentId: string;
}): void {
  const subject = buildAgentSessionRpcSubject(args.userId, args.agentId);
  args.jetstream.nc.subscribe(subject, {
    callback: (error, msg) => {
      if (error) {
        const message = error instanceof Error ? error.message : String(error);
        writeAgentError(`session rpc subscription error: ${message}`);
        return;
      }
      void handleSessionRpcMessage({
        msg,
        jetstream: args.jetstream,
        agentId: args.agentId,
      });
    },
  });
  writeAgentInfo(`session rpc subscribed subject=${subject}`);
}

async function handleFsRpcMessage(args: {
  msg: Msg;
  serverBaseUrl: string;
  userId: string;
  agentId: string;
  agentToken: string;
}): Promise<void> {
  let payload: AgentFsRpcRequest = {};
  try {
    payload = JSON.parse(fsRpcCodec.decode(args.msg.data)) as AgentFsRpcRequest;
    if (typeof payload.agentId === "string" && payload.agentId.trim() && payload.agentId !== args.agentId) {
      throw new Error("agent id mismatch");
    }
    const result = await executeFsRpc({
      request: payload,
      serverBaseUrl: args.serverBaseUrl,
      agentToken: args.agentToken,
    });
    args.msg.respond(fsRpcCodec.encode(JSON.stringify(result)));
  } catch (error) {
    const message = error instanceof Error ? error.message : "unknown error";
    const action = typeof payload.action === "string" ? payload.action : "";
    const response = {
      ok: false,
      action,
      path: typeof payload.path === "string" ? payload.path : ".",
      error: message,
    };
    args.msg.respond(fsRpcCodec.encode(JSON.stringify(response)));
    writeAgentError(`fs rpc failed action=${action || "unknown"} error=${message}`);
  }
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
        serverBaseUrl: args.serverBaseUrl,
        userId: args.userId,
        agentId: args.agentId,
        agentToken: args.agentToken,
      });
    },
  });
  writeAgentInfo(`fs rpc subscribed subject=${subject}`);
}

function normalizeShellRpcRequest(args: {
  request: AgentShellRpcRequest;
  agentId: string;
}): AgentShellRpcNormalizedRequest {
  const requestId = typeof args.request.requestId === "string" ? args.request.requestId.trim() : "";
  if (!requestId) {
    throw new Error("missing requestId");
  }
  const requestAgentId = typeof args.request.agentId === "string" ? args.request.agentId.trim() : "";
  if (!requestAgentId) {
    throw new Error("missing agentId");
  }
  if (requestAgentId !== args.agentId) {
    throw new Error("agent id mismatch");
  }
  const kind = args.request.kind === "apply_patch" ? "apply_patch" : "shell";
  const command = typeof args.request.command === "string" ? args.request.command.trim() : "";
  const patch = typeof args.request.patch === "string" ? args.request.patch : "";
  if (kind === "shell" && !command) {
    throw new Error("missing command");
  }
  if (kind === "apply_patch" && !patch.trim()) {
    throw new Error("missing patch");
  }
  const responseSubject = typeof args.request.responseSubject === "string" ? args.request.responseSubject.trim() : "";
  if (!responseSubject) {
    throw new Error("missing responseSubject");
  }
  const cwd = typeof args.request.cwd === "string" && args.request.cwd.trim() ? args.request.cwd.trim() : null;
  const timeoutRaw = Number(args.request.timeoutMs);
  const timeoutMs = Number.isFinite(timeoutRaw) ? Math.max(1000, Math.min(Math.floor(timeoutRaw), 300000)) : 30000;
  return {
    kind,
    requestId,
    command: kind === "shell" ? command : null,
    patch: kind === "apply_patch" ? patch : null,
    cwd,
    timeoutMs,
    responseSubject,
    runtimeEnvPatch: normalizeEnvPatch(args.request.runtimeEnvPatch),
    codexAuthBundle: normalizeShellRpcCodexAuthBundle(args.request.codexAuth),
  };
}

function normalizeShellRpcCodexAuthBundle(value: unknown): CodexAuthBundleResponse | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const row = value as Record<string, unknown>;
  const authJson = typeof row.authJson === "string" ? row.authJson : null;
  if (!authJson) {
    return null;
  }
  return {
    taskId: typeof row.taskId === "string" ? row.taskId : undefined,
    authMode: row.authMode === "oauth" ? "oauth" : row.authMode === "api_key" ? "api_key" : undefined,
    issuedAt: typeof row.issuedAt === "string" ? row.issuedAt : undefined,
    expiresAt: typeof row.expiresAt === "string" ? row.expiresAt : undefined,
    authJson,
    apiKey: typeof row.apiKey === "string" || row.apiKey === null ? row.apiKey : undefined,
  };
}

function publishShellRpcResponse(args: {
  nc: NatsConnection;
  responseSubject: string;
  payload: AgentShellRpcResponse;
}): void {
  args.nc.publish(args.responseSubject, shellRpcCodec.encode(JSON.stringify(args.payload)));
}

async function handleShellRpcMessage(args: {
  msg: Msg;
  jetstream: AgentJetStreamContext;
  userId: string;
  agentId: string;
  agentToken: string;
}): Promise<void> {
  let requestId = "unknown";
  let responseSubject = "";
  let stdout = "";
  let stderr = "";
  try {
    const payload = JSON.parse(shellRpcCodec.decode(args.msg.data)) as AgentShellRpcRequest;
    const request = normalizeShellRpcRequest({ request: payload, agentId: args.agentId });
    requestId = request.requestId;
    responseSubject = request.responseSubject;
    const startedAtMs = Date.now();
    const prepared = await prepareCommandExecution({
      cwd: request.cwd,
      userId: args.userId,
      taskId: request.requestId,
      codexAuthBundle: request.codexAuthBundle,
    });
    const child = spawnPreparedCommand({
      kind: request.kind,
      command: request.command,
      patch: request.patch,
      shellPath: prepared.shellPath,
      taskWorkspace: prepared.taskWorkspace,
      env: prepared.env,
      agentToken: args.agentToken,
    });

    writeRpcStatus(
      requestId,
      `started kind=${request.kind} cwd=${prepared.taskWorkspace} shell=${request.kind === "shell" ? prepared.shellPath : "apply_patch"}`,
    );
    child.stdout!.on("data", (chunk: string) => {
      stdout += chunk;
      writeRpcStream(requestId, "stdout", chunk);
    });
    child.stderr!.on("data", (chunk: string) => {
      stderr += chunk;
      writeRpcStream(requestId, "stderr", chunk);
    });

    let timedOut = false;
    const timeout = setTimeout(() => {
      timedOut = true;
      sendSignalToTaskProcess(child, "SIGTERM");
      setTimeout(() => {
        sendSignalToTaskProcess(child, "SIGKILL");
      }, 1000).unref?.();
    }, request.timeoutMs);
    timeout.unref?.();

    const result = await new Promise<{ exitCode: number | null; signal: string | null }>((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (code, signal) => {
        resolve({ exitCode: typeof code === "number" ? code : null, signal });
      });
    }).finally(() => {
      clearTimeout(timeout);
    });
    await prepared.codexAuthCleanup().catch(() => undefined);

    publishShellRpcResponse({
      nc: args.jetstream.nc,
      responseSubject,
      payload: {
        requestId,
        ok: !timedOut,
        exitCode: result.exitCode,
        signal: result.signal,
        stdout,
        stderr,
        ...(timedOut ? { error: `Command timed out after ${request.timeoutMs}ms` } : {}),
      },
    });
    writeRpcStatus(
      requestId,
      `${timedOut ? "timed_out" : "completed"} exitCode=${result.exitCode ?? "null"} signal=${result.signal ?? "null"} durationMs=${Date.now() - startedAtMs}`,
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (responseSubject) {
      publishShellRpcResponse({
        nc: args.jetstream.nc,
        responseSubject,
        payload: {
          requestId,
          ok: false,
          exitCode: null,
          signal: null,
          stdout,
          stderr,
          error: message,
        },
      });
    }
    writeRpcStatus(requestId, `failed error=${message}`);
    writeAgentError(`shell rpc failed requestId=${requestId} error=${message}`);
  }
}

function subscribeToShellRpc(args: {
  jetstream: AgentJetStreamContext;
  userId: string;
  agentId: string;
  agentToken: string;
}): void {
  const subject = buildAgentShellRpcSubject(args.userId, args.agentId);
  args.jetstream.nc.subscribe(subject, {
    callback: (error, msg) => {
      if (error) {
        const message = error instanceof Error ? error.message : String(error);
        writeAgentError(`shell rpc subscription error: ${message}`);
        return;
      }
      void handleShellRpcMessage({
        msg,
        jetstream: args.jetstream,
        userId: args.userId,
        agentId: args.agentId,
        agentToken: args.agentToken,
      });
    },
  });
  writeAgentInfo(`shell rpc subscribed subject=${subject}`);
}

async function postJson<T>(url: string, body: unknown): Promise<T> {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let data: unknown = {};
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = {};
    }
  }
  if (!res.ok) {
    const errObj = (data && typeof data === "object" ? data : {}) as Record<string, unknown>;
    const message = typeof errObj.error === "string" ? errObj.error : `HTTP ${res.status}`;
    throw new Error(message);
  }
  return data as T;
}

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  const text = await res.text();
  let data: unknown = {};
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = {};
    }
  }
  if (!res.ok) {
    const errObj = (data && typeof data === "object" ? data : {}) as Record<string, unknown>;
    const message = typeof errObj.error === "string" ? errObj.error : `HTTP ${res.status}`;
    throw new Error(message);
  }
  return data as T;
}

const nextEventSeqByTask = new Map<string, number>();

function reserveNextEventSeq(taskId: string): number {
  const current = nextEventSeqByTask.get(taskId) ?? 1;
  nextEventSeqByTask.set(taskId, current + 1);
  return current;
}

function emitAgentMetaLog(level: "info" | "error", message: string): void {
  const ctx = activeTaskLogContext;
  if (!ctx) {
    return;
  }
  const seq = reserveNextEventSeq(ctx.taskId);
  void recordAgentEvent({
    jetstream: ctx.jetstream,
    serverBaseUrl: ctx.serverBaseUrl,
    taskId: ctx.taskId,
    userId: ctx.userId,
    type: "meta",
    seq,
    payload: {
      channel: "agent",
      level,
      message,
      at: formatLocalTimestamp(),
    },
  }).catch((error) => {
    const detail = error instanceof Error ? error.message : String(error);
    process.stderr.write(`[doer-agent] meta log persist failed task=${ctx.taskId}: ${detail}\n`);
  });
}

async function recordAgentEvent(args: {
  jetstream: AgentJetStreamContext;
  serverBaseUrl: string;
  taskId: string;
  userId: string;
  type: AgentEventType;
  seq: number;
  payload: Record<string, unknown>;
}): Promise<void> {
  await args.jetstream.js.publish(
    args.jetstream.subject,
    args.jetstream.codec.encode({
      serverBaseUrl: args.serverBaseUrl,
      userId: args.userId,
      taskId: args.taskId,
      type: args.type,
      seq: args.seq,
      payload: args.payload,
    }),
  );
}

function persistEventOrFatal(args: {
  jetstream: AgentJetStreamContext;
  serverBaseUrl: string;
  taskId: string;
  userId: string;
  type: AgentEventType;
  seq: number;
  payload: Record<string, unknown>;
  context: string;
}): void {
  void (async () => {
    let attempt = 0;
    let delayMs = 150;
    while (attempt < 3) {
      attempt += 1;
      try {
        await recordAgentEvent(args);
        return;
      } catch (error) {
        if (attempt >= 3) {
          const message = error instanceof Error ? error.message : String(error);
          writeAgentError(
            `task=${args.taskId} ${args.context}: ${message} (dropped after ${attempt} attempts)`,
          );
          return;
        }
        await sleep(delayMs);
        delayMs *= 2;
      }
    }
  })();
}

async function heartbeatAgent(args: {
  serverBaseUrl: string;
  userId: string;
  agentToken: string;
}): Promise<void> {
  await postJson<{ ok?: boolean }>(`${args.serverBaseUrl}/api/agent/heartbeat`, {
    userId: args.userId,
    agentToken: args.agentToken,
  });
}

async function claimTaskById(args: {
  serverBaseUrl: string;
  userId: string;
  agentToken: string;
  taskId: string;
}): Promise<PollResponse["task"]> {
  const response = await postJson<{ task?: PollResponse["task"] | null }>(
    `${args.serverBaseUrl}/api/agent/tasks/claim`,
    {
      userId: args.userId,
      agentToken: args.agentToken,
      taskId: args.taskId,
    },
  );
  return response.task ?? null;
}

async function runClaimedTask(args: {
  task: NonNullable<PollResponse["task"]>;
  serverBaseUrl: string;
  userId: string;
  agentToken: string;
  jetstream: AgentJetStreamContext;
}): Promise<void> {
  try {
    writeAgentInfo(`run task=${args.task.id} command=${args.task.command}`);
    await runTask({
      serverBaseUrl: args.serverBaseUrl,
      taskId: args.task.id,
      command: args.task.command,
      cwd: args.task.cwd,
      userId: args.userId,
      agentToken: args.agentToken,
      jetstream: args.jetstream,
    }).catch(async (error) => {
      const message = error instanceof Error ? error.message : String(error);
      writeAgentError(`task=${args.task.id} run failed: ${message}`);
      const failPayload = {
        status: "failed",
        error: message,
        finishedAt: formatLocalTimestamp(),
      } satisfies Record<string, unknown>;
      await recordAgentEvent({
        jetstream: args.jetstream,
        serverBaseUrl: args.serverBaseUrl,
        taskId: args.task.id,
        userId: args.userId,
        type: "status",
        seq: reserveNextEventSeq(args.task.id),
        payload: failPayload,
      });
    });
  } finally {
    if (activeTaskLogContext?.taskId === args.task.id) {
      activeTaskLogContext = null;
    }
  }
}

async function checkCancelRequested(args: {
  serverBaseUrl: string;
  taskId: string;
  userId: string;
  agentToken: string;
}): Promise<boolean> {
  const query = new URLSearchParams({
    userId: args.userId,
    agentToken: args.agentToken,
  });
  const response = await getJson<{ task?: { cancelRequested?: boolean } }>(
    `${args.serverBaseUrl}/api/agent/tasks/${encodeURIComponent(args.taskId)}/events?${query.toString()}`,
  );
  return Boolean(response.task?.cancelRequested);
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
  return {
    envPatch: {},
    cleanup: async () => {},
    meta: {
      codexAuthSource: "agent_local",
      codexAuthSynced: false,
    },
  };
}

async function prepareCodexAuthBundle(bundle: CodexAuthBundleResponse | null): Promise<{
  envPatch: Record<string, string>;
  cleanup: () => Promise<void>;
  meta: Record<string, unknown>;
} | null> {
  void bundle;
  return {
    envPatch: {},
    cleanup: async () => {},
    meta: {
      codexAuthSource: "agent_local",
      codexAuthSynced: false,
    },
  };
}

async function prepareCommandExecution(args: {
  cwd: string | null;
  userId: string;
  taskId: string;
  codexAuthBundle: CodexAuthBundleResponse | null;
}): Promise<{
  shellPath: string;
  taskWorkspace: string;
  taskPath: string;
  env: NodeJS.ProcessEnv;
  taskGitMeta: Record<string, unknown>;
  codexAuthMeta: Record<string, unknown>;
  codexAuthCleanup: () => Promise<void>;
}> {
  const shellPath = resolveShellPath();
  const taskWorkspace = resolveTaskWorkspace(args.cwd);
  const codexHome = resolveCodexHomePath();
  await mkdir(codexHome, { recursive: true });
  const codexAuth = await prepareCodexAuthBundle(args.codexAuthBundle);
  const localAgentSettings = await readAgentSettingsConfig(null);
  const baseTaskEnvPatch = {
    CODEX_HOME: codexHome,
    DOER_USER_ID: args.userId,
    DOER_AGENT_TASK_ID: args.taskId,
    ...buildAgentSettingsEnvPatch(localAgentSettings),
    ...(codexAuth?.envPatch ?? {}),
    WORKSPACE: taskWorkspace,
  };
  const taskGitEnv = await prepareTaskGitEnv({
    cwd: taskWorkspace,
    baseEnvPatch: baseTaskEnvPatch,
  });
  const runtimeBinPath = path.join(AGENT_PROJECT_DIR, "runtime/bin");
  const taskPath = [runtimeBinPath, process.env.PATH || ""].filter(Boolean).join(path.delimiter);
  return {
    shellPath,
    taskWorkspace,
    taskPath,
    env: {
      ...process.env,
      ...baseTaskEnvPatch,
      ...taskGitEnv.envPatch,
      PATH: taskPath,
    },
    taskGitMeta: taskGitEnv.meta ?? {},
    codexAuthMeta: codexAuth?.meta ?? { codexAuthSynced: false },
    codexAuthCleanup: codexAuth?.cleanup ?? (async () => {}),
  };
}

function spawnPreparedCommand(args: {
  kind: "shell" | "apply_patch";
  command: string | null;
  patch: string | null;
  shellPath: string;
  taskWorkspace: string;
  env: NodeJS.ProcessEnv;
  agentToken: string;
}): ReturnType<typeof spawn> {
  const env = {
    ...args.env,
    DOER_AGENT_TOKEN: args.agentToken,
  };
  const child = args.kind === "apply_patch"
    ? spawn("apply_patch", {
        cwd: args.taskWorkspace,
        detached: process.platform !== "win32",
        env,
        stdio: ["pipe", "pipe", "pipe"],
      })
    : spawn(args.command ?? "", {
        cwd: args.taskWorkspace,
        shell: args.shellPath,
        detached: process.platform !== "win32",
        env,
        stdio: ["ignore", "pipe", "pipe"],
      });
  if (args.kind === "apply_patch") {
    child.stdin?.write(args.patch ?? "");
    child.stdin?.end();
  }
  child.stdout!.setEncoding("utf8");
  child.stderr!.setEncoding("utf8");
  return child;
}

function createManagedCancellation(child: ReturnType<typeof spawn>): {
  requestCancel: () => void;
  clear: () => void;
} {
  let cancelStage1Timer: NodeJS.Timeout | null = null;
  let cancelStage2Timer: NodeJS.Timeout | null = null;
  let cancelSignalSent = false;
  return {
    requestCancel: () => {
      if (cancelSignalSent) {
        return;
      }
      cancelSignalSent = true;
      sendSignalToTaskProcess(child, "SIGINT");
      cancelStage1Timer = setTimeout(() => {
        sendSignalToTaskProcess(child, "SIGTERM");
      }, 1200);
      cancelStage1Timer.unref?.();
      cancelStage2Timer = setTimeout(() => {
        sendSignalToTaskProcess(child, "SIGKILL");
      }, 3500);
      cancelStage2Timer.unref?.();
    },
    clear: () => {
      if (cancelStage1Timer) {
        clearTimeout(cancelStage1Timer);
      }
      if (cancelStage2Timer) {
        clearTimeout(cancelStage2Timer);
      }
    },
  };
}

async function runTask(args: {
  serverBaseUrl: string;
  taskId: string;
  command: string;
  cwd: string | null;
  userId: string;
  agentToken: string;
  jetstream: AgentJetStreamContext;
}): Promise<void> {
  activeTaskLogContext = {
    jetstream: args.jetstream,
    serverBaseUrl: args.serverBaseUrl,
    taskId: args.taskId,
    userId: args.userId,
  };
  const shellPath = resolveShellPath();
  const taskWorkspace = resolveTaskWorkspace(args.cwd);
  const codexHome = resolveCodexHomePath();
  await mkdir(codexHome, { recursive: true });
  const runtimeConfig = await prepareTaskRuntimeConfig({
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
  const localAgentSettings = await readAgentSettingsConfig(null);
  const baseTaskEnvPatch = {
    CODEX_HOME: codexHome,
    ...buildAgentSettingsEnvPatch(localAgentSettings),
    ...(runtimeConfig?.envPatch ?? {}),
    ...(codexAuth?.envPatch ?? {}),
    WORKSPACE: taskWorkspace,
  };

  const taskGitEnv = await prepareTaskGitEnv({
    cwd: taskWorkspace,
    baseEnvPatch: baseTaskEnvPatch,
  });
  await recordAgentEvent({    jetstream: args.jetstream,
    serverBaseUrl: args.serverBaseUrl,
    taskId: args.taskId,
    userId: args.userId,
    type: "meta",
    seq: reserveNextEventSeq(args.taskId),
    payload: {
      host: process.platform,
      pid: process.pid,
      startedAt: formatLocalTimestamp(),
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

    const runtimeBinPath = path.join(AGENT_PROJECT_DIR, "runtime/bin");
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

    child.stdout!.setEncoding("utf8");
    child.stderr!.setEncoding("utf8");

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
    activeTaskCancelRequests.set(args.taskId, requestCancel);

    child.stdout.on("data", (chunk: string) => {
      writeTaskStream(args.taskId, "stdout", chunk);
      const seq = reserveNextEventSeq(args.taskId);
      persistEventOrFatal({
        jetstream: args.jetstream,
        serverBaseUrl: args.serverBaseUrl,
        taskId: args.taskId,
        userId: args.userId,
        type: "stdout",
        seq,
        payload: { chunk, at: formatLocalTimestamp() },
        context: "stdout persist failed",
      });
    });

    child.stderr.on("data", (chunk: string) => {
      writeTaskStream(args.taskId, "stderr", chunk);
      const seq = reserveNextEventSeq(args.taskId);
      persistEventOrFatal({
        jetstream: args.jetstream,
        serverBaseUrl: args.serverBaseUrl,
        taskId: args.taskId,
        userId: args.userId,
        type: "stderr",
        seq,
        payload: { chunk, at: formatLocalTimestamp() },
        context: "stderr persist failed",
      });
    });

    const cancelPoller = (async () => {
      while (!stopCancelPolling) {
        await sleep(5000);
        if (stopCancelPolling || terminationReason === "cancel") {
          continue;
        }
        const cancelRequested = await checkCancelRequested({
          serverBaseUrl: args.serverBaseUrl,
          taskId: args.taskId,
          userId: args.userId,
          agentToken: args.agentToken,
        }).catch(() => false);
        if (!cancelRequested) {
          continue;
        }
        requestCancel();
      }
    })();

    const result = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
      child.once("error", (error) => {
        reject(error);
      });
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
      finishedAt: formatLocalTimestamp(),
      error:
        status === "failed"
          ? `Command exited with code ${result.code ?? "null"}`
          : null,
    } satisfies Record<string, unknown>;
    await recordAgentEvent({    jetstream: args.jetstream,
      serverBaseUrl: args.serverBaseUrl,
      taskId: args.taskId,
      userId: args.userId,
      type: "status",
      seq: reserveNextEventSeq(args.taskId),
      payload: statusPayload,
    });
    writeAgentInfo(
      `task=${args.taskId} status=${status} exitCode=${typeof result.code === "number" ? result.code : "null"} signal=${result.signal ?? "null"}`,
    );
  } finally {
    activeTaskCancelRequests.delete(args.taskId);
    activeTaskLogContext = null;
    await codexAuth?.cleanup().catch(() => undefined);
  }
}

async function connectBootstrapWithRetry(args: {
  serverBaseUrl: string;
  userId: string;
  agentToken: string;
}): Promise<{
  natsBootstrap: AgentNatsBootstrapResponse;
  jetstream: AgentJetStreamContext;
}> {
  let attempt = 0;
  while (true) {
    attempt += 1;
    try {
      const natsBootstrap = await postJson<AgentNatsBootstrapResponse>(`${args.serverBaseUrl}/api/agent/nats`, {
        userId: args.userId,
        agentToken: args.agentToken,
      });
      const natsServers = normalizeNatsServers(natsBootstrap.servers);
      if (natsServers.length === 0) {
        throw new Error("No NATS servers configured by server");
      }
      const natsToken = normalizeNatsToken(natsBootstrap.auth);
      const jetstream = await initJetStreamContext({
        userId: args.userId,
        servers: natsServers,
        token: natsToken,
      });
      writeAgentInfraError(`bootstrap ok servers=${natsServers.length} eventStream=${jetstream.stream} eventSubject=${jetstream.subject}`);
      return { natsBootstrap, jetstream };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const retryMs = Math.min(30_000, 1000 * Math.max(1, attempt));
      writeAgentError(`bootstrap failed: ${message} (retry in ${Math.floor(retryMs / 1000)}s, attempt=${attempt})`);
      await sleep(retryMs);
    }
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const workspaceDir = resolveArgOrEnv(args, ["workspace-dir", "workspaceDir"], ["WORKSPACE"]);
  const startupWorkspaceRoot = path.resolve(workspaceDir || process.cwd());
  workspaceRootOverride = startupWorkspaceRoot;
  process.chdir(startupWorkspaceRoot);
  process.env.WORKSPACE = startupWorkspaceRoot;
  process.env.CODEX_HOME = path.join(startupWorkspaceRoot, ".codex");
  await mkdir(process.env.CODEX_HOME, { recursive: true }).catch(() => undefined);
  await resetRunsDir();

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
  const { natsBootstrap, jetstream } = await connectBootstrapWithRetry({
    serverBaseUrl,
    userId,
    agentToken,
  });
  const agentVersion = await resolveAgentVersion();
  const initialAgentId = typeof natsBootstrap.agentId === "string" ? natsBootstrap.agentId : "";
  if (!initialAgentId) {
    throw new Error("agent id missing from bootstrap");
  }

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

  let heartbeatHealthy: boolean | null = null;
  const heartbeatTimer = setInterval(() => {
    void heartbeatAgent({ serverBaseUrl, userId, agentToken })
      .then(() => {
        if (heartbeatHealthy === false) {
          writeAgentInfraError(`heartbeat reconnected at=${formatLocalTimestamp()}`);
        }
        heartbeatHealthy = true;
      })
      .catch((error) => {
        const message = error instanceof Error ? error.message : String(error);
        if (heartbeatHealthy !== false) {
          writeAgentInfraError(`heartbeat failed: ${message}`);
        }
        heartbeatHealthy = false;
      });
  }, 10_000);

  subscribeToFsRpc({
    jetstream,
    serverBaseUrl,
    userId,
    agentId: initialAgentId,
    agentToken,
  });
  subscribeToShellRpc({
    jetstream,
    userId,
    agentId: initialAgentId,
    agentToken,
  });
  subscribeToSessionRpc({
    jetstream,
    userId,
    agentId: initialAgentId,
  });
  subscribeToCodexAuthRpc({
    jetstream,
    userId,
    agentId: initialAgentId,
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
  subscribeToRunRpc({
    jetstream,
    serverBaseUrl,
    userId,
    agentId: initialAgentId,
    agentToken,
  });
  await new Promise<never>(() => {
    // Keep the long-lived agent process alive for RPC subscriptions and heartbeat.
  });
}
main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  writeAgentError(`fatal: ${message}`);
  process.exitCode = 1;
});
