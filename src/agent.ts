import { spawn, spawnSync } from "node:child_process";
import { existsSync, statSync } from "node:fs";
import { chmod, mkdir, open, readFile, readdir, stat, writeFile } from "node:fs/promises";
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

type AgentEventType = "stdout" | "stderr" | "status" | "meta";

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
  tasks?: {
    stream?: unknown;
    subject?: unknown;
    durable?: unknown;
  } | null;
  pendingTaskIds?: unknown;
}

const DEFAULT_SERVER_BASE_URL = "https://doer.cranix.net";
const AGENT_MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const AGENT_PROJECT_DIR = path.join(AGENT_MODULE_DIR, "..");
const AGENT_PACKAGE_JSON_PATH = path.join(AGENT_PROJECT_DIR, "package.json");

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
  taskCodec: ReturnType<typeof JSONCodec<AgentTaskDispatchEnvelope>>;
  subject: string;
  stream: string;
  durable: string;
  servers: string[];
  taskStream: string;
  taskSubject: string;
  taskDurable: string;
}

interface AgentTaskDispatchEnvelope {
  type?: "task" | "cancel";
  userId: string;
  agentId: string;
  taskId: string;
  createdAt: string;
}

type AgentFsRpcAction = "list" | "stat" | "fetch_file" | "read_text";

interface AgentFsRpcRequest {
  requestId?: unknown;
  action?: unknown;
  path?: unknown;
  offset?: unknown;
  length?: unknown;
  limit?: unknown;
  encoding?: unknown;
  uploadUrl?: unknown;
  chatId?: unknown;
  agentId?: unknown;
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

function sanitizeUserId(userId: string): string {
  const normalized = userId.trim().replace(/[^a-zA-Z0-9_-]/g, "_");
  return normalized.length > 0 ? normalized : "anonymous";
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
  taskStream: string;
  taskSubject: string;
  taskDurable: string;
}): Promise<AgentJetStreamContext> {
  const sanitized = sanitizeUserId(args.userId);
  const stream = `DOER_AGENT_EVENTS_${sanitized}`;
  const subject = `doer.agent.events.${sanitized}`;
  const durable = `doer-agent-uploader-${sanitized}`;

  const nc = await connect(args.token ? { servers: args.servers, token: args.token } : { servers: args.servers });
  const jsm = await nc.jetstreamManager();
  await ensureJetStreamInfra({ jsm, stream, subject, durable });
  await ensureJetStreamInfra({
    jsm,
    stream: args.taskStream,
    subject: args.taskSubject,
    durable: args.taskDurable,
  });

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
    taskCodec: JSONCodec<AgentTaskDispatchEnvelope>(),
    subject,
    stream,
    durable,
    servers: args.servers,
    taskStream: args.taskStream,
    taskSubject: args.taskSubject,
    taskDurable: args.taskDurable,
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
  if (value === "list" || value === "stat" || value === "fetch_file" || value === "read_text") {
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

async function executeFsRpc(args: {
  request: AgentFsRpcRequest;
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
    const chatId = typeof args.request.chatId === "string" ? args.request.chatId : "";
    const agentId = typeof args.request.agentId === "string" ? args.request.agentId : "";
    if (!uploadUrl || !chatId || !agentId) {
      throw new Error("missing upload parameters");
    }
    const data = await readFile(abs);
    const fileName = path.basename(abs) || "file";
    const form = new FormData();
    form.append("file", new File([data], fileName));
    form.append("chatId", chatId);
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

  const entry = await stat(abs);
  if (!entry.isFile()) {
    throw new Error("path is not a file");
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
    const result = await executeFsRpc({ request: payload, agentToken: args.agentToken });
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

    const shellPath = resolveShellPath();
    const taskWorkspace = resolveTaskWorkspace(request.cwd);
    const codexAuth = await prepareCodexAuthBundle(request.codexAuthBundle);
    const baseTaskEnvPatch = {
      ...request.runtimeEnvPatch,
      ...(codexAuth?.envPatch ?? {}),
      WORKSPACE: taskWorkspace,
    };
    const taskGitEnv = await prepareTaskGitEnv({
      cwd: taskWorkspace,
      baseEnvPatch: baseTaskEnvPatch,
    });
    const runtimeBinPath = path.join(AGENT_PROJECT_DIR, "runtime/bin");
    const taskPath = [runtimeBinPath, process.env.PATH || ""].filter(Boolean).join(path.delimiter);
    const child =
      request.kind === "apply_patch"
        ? spawn("apply_patch", {
            cwd: taskWorkspace,
            detached: process.platform !== "win32",
            env: {
              ...process.env,
              ...baseTaskEnvPatch,
              ...taskGitEnv.envPatch,
              PATH: taskPath,
              DOER_AGENT_TOKEN: args.agentToken,
            },
            stdio: ["pipe", "pipe", "pipe"],
          })
        : spawn(request.command ?? "", {
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
    if (request.kind === "apply_patch") {
      child.stdin?.write(request.patch ?? "");
      child.stdin?.end();
    }

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk: string) => {
      stderr += chunk;
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
  const bundle = await postJson<CodexAuthBundleResponse>(
    `${args.serverBaseUrl}/api/agent/tasks/${encodeURIComponent(args.taskId)}/codex-auth`,
    {
      userId: args.userId,
      agentToken: args.agentToken,
    },
  ).catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    writeAgentError(`task=${args.taskId} codex auth sync skipped: ${message}`);
    return null;
  });

  return await prepareCodexAuthBundle(bundle);
}

async function prepareCodexAuthBundle(bundle: CodexAuthBundleResponse | null): Promise<{
  envPatch: Record<string, string>;
  cleanup: () => Promise<void>;
  meta: Record<string, unknown>;
} | null> {

  if (!bundle || typeof bundle.authJson !== "string") {
    return null;
  }

  const codexHome = resolveCodexHomePath();
  await mkdir(codexHome, { recursive: true });
  const authFile = path.join(codexHome, "auth.json");
  await writeFile(authFile, bundle.authJson, "utf8");
  await chmod(authFile, 0o600).catch(() => undefined);

  const envPatch: Record<string, string> = {
    CODEX_HOME: codexHome,
  };
  if (typeof bundle.apiKey === "string" && bundle.apiKey.trim()) {
    envPatch.OPENAI_API_KEY = bundle.apiKey.trim();
  }

  const cleanup = async () => {};

  return {
    envPatch,
    cleanup,
    meta: {
      codexAuthMode: bundle.authMode ?? null,
      codexAuthIssuedAt: bundle.issuedAt ?? null,
      codexAuthExpiresAt: bundle.expiresAt ?? null,
      codexAuthSynced: true,
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
  const baseTaskEnvPatch = {
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

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");

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
  pendingTaskIds: string[];
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
      const taskConfig = parseBootstrapTaskConfig(natsBootstrap.tasks);
      if (!taskConfig) {
        throw new Error("Invalid task dispatch config from server");
      }
      const natsToken = normalizeNatsToken(natsBootstrap.auth);
      const pendingTaskIds = normalizeTaskIds(natsBootstrap.pendingTaskIds);
      const jetstream = await initJetStreamContext({
        userId: args.userId,
        servers: natsServers,
        token: natsToken,
        taskStream: taskConfig.stream,
        taskSubject: taskConfig.subject,
        taskDurable: taskConfig.durable,
      });
      writeAgentInfraError(
        `bootstrap ok servers=${natsServers.length} taskStream=${taskConfig.stream} taskSubject=${taskConfig.subject} taskDurable=${taskConfig.durable}`,
      );
      return { natsBootstrap, pendingTaskIds, jetstream };
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
  let { natsBootstrap, pendingTaskIds, jetstream } = await connectBootstrapWithRetry({
    serverBaseUrl,
    userId,
    agentToken,
  });
  const maxConcurrency = Math.max(1, parseEnvInteger(process.env.DOER_AGENT_MAX_CONCURRENCY, 5));
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
  process.stdout.write(`- taskStream: ${jetstream.taskStream}\n`);
  process.stdout.write(`- taskSubject: ${jetstream.taskSubject}\n`);
  process.stdout.write(`- taskDurable: ${jetstream.taskDurable}\n`);
  process.stdout.write(`- pendingTasks: ${pendingTaskIds.length}\n`);
  process.stdout.write(`- maxConcurrency: ${maxConcurrency}\n\n`);
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

  const inFlightTasks = new Set<Promise<void>>();

  async function waitForAvailableSlot(): Promise<void> {
    while (inFlightTasks.size >= maxConcurrency) {
      try {
        await Promise.race(inFlightTasks);
      } catch {
        // keep draining slots even when a task fails.
      }
    }
  }

  function trackInFlight(taskPromise: Promise<void>): void {
    inFlightTasks.add(taskPromise);
    void taskPromise.finally(() => {
      inFlightTasks.delete(taskPromise);
    });
  }

  function scheduleTask(taskPromiseFactory: () => Promise<void>): void {
    const taskPromise = taskPromiseFactory();
    trackInFlight(taskPromise);
  }

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

  for (const pendingTaskId of pendingTaskIds) {
    await waitForAvailableSlot();
    scheduleTask(async () => {
      try {
        const task = await claimTaskById({
          serverBaseUrl,
          userId,
          agentToken,
          taskId: pendingTaskId,
        });
        if (task) {
          await runClaimedTask({ task, serverBaseUrl, userId, agentToken, jetstream });
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        writeAgentError(`pending task bootstrap failed taskId=${pendingTaskId}: ${message}`);
      }
    });
  }

  let connected = false;
  while (true) {
    try {
      const consumer = await jetstream.js.consumers.get(jetstream.taskStream, jetstream.taskDurable);
      if (!connected) {
        writeAgentInfo(`connected to task stream (NATS ok) at=${formatLocalTimestamp()} userId=${userId}`);
        connected = true;
      }
      const messages = await consumer.fetch({ max_messages: 200, expires: 5_000 });
      for await (const msg of messages) {
        await waitForAvailableSlot();
        scheduleTask(async () => {
          let dispatch: AgentTaskDispatchEnvelope;
          try {
            dispatch = jetstream.taskCodec.decode(msg.data);
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            writeAgentError(`task dispatch decode failed: ${message}`);
            msg.term();
            return;
          }
          writeAgentInfo(
            `task dispatch received taskId=${dispatch.taskId} createdAt=${dispatch.createdAt} subject=${jetstream.taskSubject} durable=${jetstream.taskDurable}`,
          );

          const ackKeepAliveIntervalMs = 10_000;
          let ackKeepAliveTimer: NodeJS.Timeout | null = null;
          const stopAckKeepAlive = () => {
            if (ackKeepAliveTimer) {
              clearInterval(ackKeepAliveTimer);
              ackKeepAliveTimer = null;
            }
          };

          try {
            ackKeepAliveTimer = setInterval(() => {
              try {
                msg.working();
              } catch (error) {
                const message = error instanceof Error ? error.message : String(error);
                writeAgentError(`task dispatch keepalive failed taskId=${dispatch.taskId}: ${message}`);
              }
            }, ackKeepAliveIntervalMs);
            ackKeepAliveTimer.unref?.();

            if (dispatch.type === "cancel") {
              stopAckKeepAlive();
              const canceled = requestTaskCancellation(dispatch.taskId, "nats_dispatch");
              writeAgentInfo(
                `task cancel dispatch handled taskId=${dispatch.taskId} result=${canceled ? "signaled" : "not-running"}`,
              );
              msg.ack();
              return;
            }

            const task = await claimTaskById({
              serverBaseUrl,
              userId,
              agentToken,
              taskId: dispatch.taskId,
            });
            if (!task) {
              stopAckKeepAlive();
              writeAgentInfo(`task dispatch acked without run taskId=${dispatch.taskId} reason=already-claimed`);
              msg.ack();
              return;
            }

            await runClaimedTask({ task, serverBaseUrl, userId, agentToken, jetstream });
            stopAckKeepAlive();
            msg.ack();
            writeAgentInfo(`task dispatch acked taskId=${dispatch.taskId}`);
          } catch (error) {
            stopAckKeepAlive();
            const message = error instanceof Error ? error.message : String(error);
            writeAgentError(`task dispatch handle failed taskId=${dispatch.taskId}: ${message}`);
            writeAgentError(`task dispatch sending nak taskId=${dispatch.taskId}`);
            msg.nak();
          }
        });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (connected) {
        writeAgentError(`task stream disconnected at=${formatLocalTimestamp()} reason=${message}`);
      }
      connected = false;
      if (isLikelyNatsAuthError(error)) {
        writeAgentError(`nats auth error detected. refreshing bootstrap credentials...`);
      } else if (isLikelyNatsReconnectError(error)) {
        writeAgentError(`nats connection lost. refreshing bootstrap/session...`);
      } else {
        writeAgentError(`task stream error detected. forcing bootstrap/session refresh... reason=${message}`);
      }
      if (inFlightTasks.size > 0) {
        writeAgentInfo(`waiting for in-flight tasks before reconnect count=${inFlightTasks.size}`);
        await Promise.allSettled(Array.from(inFlightTasks));
      }
      try {
        await jetstream.nc.close();
      } catch {
        // noop
      }
      const refreshed = await connectBootstrapWithRetry({
        serverBaseUrl,
        userId,
        agentToken,
      });
      natsBootstrap = refreshed.natsBootstrap;
      pendingTaskIds = refreshed.pendingTaskIds;
      jetstream = refreshed.jetstream;
      const refreshedAgentId = typeof natsBootstrap.agentId === "string" ? natsBootstrap.agentId : "";
      if (!refreshedAgentId) {
        throw new Error("agent id missing from refreshed bootstrap");
      }
      subscribeToFsRpc({
        jetstream,
        serverBaseUrl,
        userId,
        agentId: refreshedAgentId,
        agentToken,
      });
      subscribeToShellRpc({
        jetstream,
        userId,
        agentId: refreshedAgentId,
        agentToken,
      });

      for (const pendingTaskId of pendingTaskIds) {
        await waitForAvailableSlot();
        scheduleTask(async () => {
          try {
            const task = await claimTaskById({
              serverBaseUrl,
              userId,
              agentToken,
              taskId: pendingTaskId,
            });
            if (task) {
              await runClaimedTask({ task, serverBaseUrl, userId, agentToken, jetstream });
            }
          } catch (pendingError) {
            const pendingMessage = pendingError instanceof Error ? pendingError.message : String(pendingError);
            writeAgentError(`pending task refresh failed taskId=${pendingTaskId}: ${pendingMessage}`);
          }
        });
      }
      writeAgentInfo(
        `nats credentials refreshed at=${formatLocalTimestamp()} agentId=${typeof natsBootstrap.agentId === "string" ? natsBootstrap.agentId : "unknown"}`,
      );
      continue;
    }
  }
}
main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  writeAgentError(`fatal: ${message}`);
  process.exitCode = 1;
});
