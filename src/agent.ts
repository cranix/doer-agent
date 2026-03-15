import { spawn, spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { chmod, mkdir, open, readFile, rename, writeFile } from "node:fs/promises";
import net from "node:net";
import { arch, homedir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

interface PollResponse {
  task: {
    id: string;
    command: string;
    cwd: string | null;
    cancelRequested: boolean;
  } | null;
  error?: string;
}

interface SseEnvelope {
  event: string;
  data: string;
}

type AgentEventType = "stdout" | "stderr" | "status" | "meta";

interface PendingAgentEvent {
  id: string;
  serverBaseUrl: string;
  userId: string;
  taskId: string;
  type: AgentEventType;
  seq: number;
  payload: Record<string, unknown>;
  createdAt: string;
  lastAttemptAt: string | null;
  attempts: number;
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

const PLAYWRIGHT_BROWSERS_PATH = "/ms-playwright";
const PLAYWRIGHT_SKIP_BROWSER_GC = "1";
const PLAYWRIGHT_MCP_DAEMON_IDLE_TTL_SECONDS_DEFAULT = 10800;
const PLAYWRIGHT_MCP_DAEMON_SIGNATURE_VERSION = "2026-03-15";
const AGENT_MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const AGENT_PROJECT_DIR = path.join(AGENT_MODULE_DIR, "..");

let pendingEventQueueLock = Promise.resolve();

async function withPendingEventQueueLock<T>(fn: () => Promise<T>): Promise<T> {
  const run = pendingEventQueueLock.then(fn, fn);
  pendingEventQueueLock = run.then(
    () => undefined,
    () => undefined,
  );
  return run;
}

function resolvePendingEventQueuePath(): string {
  return path.join(resolveAgentStateDir(), "pending-events.json");
}

function resolveCodexHomePath(): string {
  return path.join(homedir(), ".codex");
}

function parseEnvBoolean(value: string | undefined): boolean {
  return value?.trim().toLowerCase() === "true";
}

function parseEnvStringArray(value: string | undefined): string[] {
  if (!value?.trim()) {
    return [];
  }
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) && parsed.every((item) => typeof item === "string") ? parsed : [];
  } catch {
    return [];
  }
}

function parseEnvInteger(value: string | undefined, fallback: number): number {
  const normalized = value?.trim();
  if (!normalized) {
    return fallback;
  }
  const parsed = Number.parseInt(normalized, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function resolvePlaywrightMcpProxyPath(): string {
  const candidates = ["/app/.runtime/bin/doer-mcp-proxy"];
  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }
  return "";
}

function resolvePlaywrightMcpDaemonStatePaths() {
  const daemonDir = path.join(resolveAgentStateDir(), "playwright-mcp-daemon");
  return {
    daemonDir,
    proxyLauncherPath: path.join(daemonDir, "playwright-mcp-proxy-launcher.sh"),
    socketPath: path.join(daemonDir, "playwright-mcp.sock"),
    pidPath: path.join(daemonDir, "daemon.pid"),
    metaPath: path.join(daemonDir, "daemon-meta.json"),
  };
}

function parseEnvAssignmentArgs(values: string[]): Record<string, string> {
  const envPatch: Record<string, string> = {};
  for (const value of values) {
    const separatorIndex = value.indexOf("=");
    if (separatorIndex <= 0) {
      continue;
    }
    const key = value.slice(0, separatorIndex).trim();
    const envValue = value.slice(separatorIndex + 1);
    if (!key) {
      continue;
    }
    envPatch[key] = envValue;
  }
  return envPatch;
}

function escapeShellArg(value: string): string {
  return `'${value.replace(/'/g, `'\"'\"'`)}'`;
}

async function ensurePlaywrightMcpProxyLauncher(socketPath: string, proxyPath: string): Promise<string> {
  const paths = resolvePlaywrightMcpDaemonStatePaths();
  await mkdir(paths.daemonDir, { recursive: true });
  const scriptBody = `#!/bin/sh
export DOER_MCP_SOCKET=${escapeShellArg(socketPath)}
exec ${escapeShellArg(proxyPath)} "$@"
`;
  await writeFile(paths.proxyLauncherPath, scriptBody, "utf8");
  await chmod(paths.proxyLauncherPath, 0o700).catch(() => undefined);
  return paths.proxyLauncherPath;
}

async function readPidFile(pidPath: string): Promise<number | null> {
  const raw = await readFile(pidPath, "utf8").catch(() => "");
  const parsed = Number.parseInt(raw.trim(), 10);
  return Number.isInteger(parsed) && parsed > 1 ? parsed : null;
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

async function waitForPlaywrightMcpSocketReady(socketPath: string, timeoutMs: number): Promise<boolean> {
  const startedAt = Date.now();
  while (Date.now() - startedAt <= timeoutMs) {
    const isReady = await new Promise<boolean>((resolve) => {
      const socket = net.createConnection({ path: socketPath });
      let settled = false;
      const finish = (value: boolean) => {
        if (settled) {
          return;
        }
        settled = true;
        resolve(value);
      };
      socket.once("connect", () => {
        socket.end();
        finish(true);
      });
      socket.once("error", () => {
        finish(false);
      });
      setTimeout(() => {
        socket.destroy();
        finish(false);
      }, 250).unref?.();
    });
    if (isReady) {
      return true;
    }
    await sleep(120);
  }
  return false;
}

async function stopPlaywrightMcpDaemon(paths: { socketPath: string; pidPath: string; metaPath: string }): Promise<void> {
  const pid = await readPidFile(paths.pidPath);
  if (pid && isProcessAlive(pid)) {
    try {
      process.kill(pid, "SIGTERM");
    } catch {
      // ignore
    }
    const waitStartedAt = Date.now();
    while (Date.now() - waitStartedAt < 1800 && isProcessAlive(pid)) {
      await sleep(120);
    }
    if (isProcessAlive(pid)) {
      try {
        process.kill(pid, "SIGKILL");
      } catch {
        // ignore
      }
    }
  }
  await Promise.all([
    rename(paths.socketPath, `${paths.socketPath}.stale.${Date.now()}`).catch(() => undefined),
    rename(paths.pidPath, `${paths.pidPath}.stale.${Date.now()}`).catch(() => undefined),
    rename(paths.metaPath, `${paths.metaPath}.stale.${Date.now()}`).catch(() => undefined),
  ]);
}

async function ensureManagedPlaywrightMcpDaemon(args: {
  command: string;
  daemonArgs: string[];
  browserEnvArgs: string[];
}): Promise<string> {
  const paths = resolvePlaywrightMcpDaemonStatePaths();
  await mkdir(paths.daemonDir, { recursive: true });
  const daemonCommand = args.command;
  const targetEnvPatch = parseEnvAssignmentArgs(args.browserEnvArgs);
  const daemonCommandArgs = args.daemonArgs;
  const signature = JSON.stringify({
    version: PLAYWRIGHT_MCP_DAEMON_SIGNATURE_VERSION,
    daemonCommand,
    daemonCommandArgs,
    targetEnvPatch,
    idleTtlSeconds: parseEnvInteger(
      process.env.DOER_PLAYWRIGHT_MCP_DAEMON_IDLE_TTL_SECONDS,
      PLAYWRIGHT_MCP_DAEMON_IDLE_TTL_SECONDS_DEFAULT,
    ),
  });

  const existingMetaRaw = await readFile(paths.metaPath, "utf8").catch(() => "");
  let existingMeta: { signature?: string } | null = null;
  if (existingMetaRaw) {
    try {
      existingMeta = JSON.parse(existingMetaRaw) as { signature?: string };
    } catch {
      existingMeta = null;
    }
  }
  const existingPid = await readPidFile(paths.pidPath);
  if (
    existingMeta?.signature === signature
    && existingPid
    && isProcessAlive(existingPid)
    && await waitForPlaywrightMcpSocketReady(paths.socketPath, 350)
  ) {
    return paths.socketPath;
  }

  await stopPlaywrightMcpDaemon(paths);

  const daemonScriptPath = path.join(AGENT_MODULE_DIR, "playwright-mcp-daemon.ts");
  const idleTtlSeconds = String(
    parseEnvInteger(process.env.DOER_PLAYWRIGHT_MCP_DAEMON_IDLE_TTL_SECONDS, PLAYWRIGHT_MCP_DAEMON_IDLE_TTL_SECONDS_DEFAULT),
  );
  const child = spawn(process.execPath, ["--import", "tsx", daemonScriptPath], {
    cwd: AGENT_PROJECT_DIR,
    detached: true,
    stdio: "ignore",
    env: {
      ...process.env,
      DOER_PLAYWRIGHT_MCP_DAEMON_SOCKET: paths.socketPath,
      DOER_PLAYWRIGHT_MCP_DAEMON_IDLE_TTL_SECONDS: idleTtlSeconds,
      DOER_PLAYWRIGHT_MCP_TARGET_COMMAND: daemonCommand,
      DOER_PLAYWRIGHT_MCP_TARGET_ARGS_JSON: JSON.stringify(daemonCommandArgs),
      DOER_PLAYWRIGHT_MCP_TARGET_ENV_JSON: JSON.stringify(targetEnvPatch),
    },
  });
  child.unref();

  if (!child.pid) {
    throw new Error("failed to start playwright mcp daemon: missing pid");
  }

  await writeFile(paths.pidPath, `${child.pid}\n`, "utf8");
  await writeFile(
    paths.metaPath,
    `${JSON.stringify({ signature, pid: child.pid, socketPath: paths.socketPath, updatedAt: new Date().toISOString() }, null, 2)}\n`,
    "utf8",
  );

  const ready = await waitForPlaywrightMcpSocketReady(paths.socketPath, 6000);
  if (!ready) {
    throw new Error(`playwright mcp daemon socket not ready: ${paths.socketPath}`);
  }
  return paths.socketPath;
}

async function resolvePlaywrightMcpCommand(): Promise<{ command: string; args: string[] }> {
  const browserEnvArgs = [
    `PLAYWRIGHT_BROWSERS_PATH=${PLAYWRIGHT_BROWSERS_PATH}`,
    `PLAYWRIGHT_SKIP_BROWSER_GC=${PLAYWRIGHT_SKIP_BROWSER_GC}`,
  ];
  const daemonArgsFromEnv = parseEnvStringArray(process.env.DOER_PLAYWRIGHT_MCP_DAEMON_ARGS_JSON);
  const [daemonCommandFromArgs, ...daemonArgsRest] = daemonArgsFromEnv;

  const daemonCommand = daemonCommandFromArgs || "npx";
  let daemonArgs =
    daemonCommandFromArgs && daemonArgsFromEnv.length > 0
      ? daemonArgsRest
      : ["-y", "@playwright/mcp"];

  const hasBrowserOption = daemonArgs.some(
    (arg, index) => arg === "--browser" || arg.startsWith("--browser="),
  );
  if (arch() === "arm64" && !hasBrowserOption) {
    daemonArgs = [...daemonArgs, "--browser", "chromium"];
  }

  const proxyPath = resolvePlaywrightMcpProxyPath();
  if (!proxyPath) {
    throw new Error("playwright mcp daemon mode requires doer-mcp-proxy binary");
  }

  const socketPath = await ensureManagedPlaywrightMcpDaemon({
    command: daemonCommand,
    daemonArgs,
    browserEnvArgs,
  });
  const proxyLauncherPath = await ensurePlaywrightMcpProxyLauncher(socketPath, proxyPath);
  return {
    command: proxyLauncherPath,
    args: [],
  };
}
function escapeTomlString(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

function toTomlArray(values: string[]): string {
  return `[${values.map((item) => `"${escapeTomlString(item)}"`).join(", ")}]`;
}

async function ensureCodexPlaywrightMcpConfig(codexHome: string): Promise<{ configPath: string; changed: boolean }> {
  await mkdir(codexHome, { recursive: true });
  const configPath = path.join(codexHome, "config.toml");
  const sectionHeader = "[mcp_servers.playwright]";
  const existing = await readFile(configPath, "utf8").catch(() => "");

  const { command, args } = await resolvePlaywrightMcpCommand();
  const sectionLines = [sectionHeader, `command = "${escapeTomlString(command)}"`];
  if (args.length > 0) {
    sectionLines.push(`args = ${toTomlArray(args)}`);
  }
  const sectionText = `${sectionLines.join("\n")}\n`;
  let nextContent = sectionText;
  if (existing.trim().length > 0) {
    const lines = existing.split(/\r?\n/);
    const sectionStart = lines.findIndex((line) => line.trim() === sectionHeader);
    if (sectionStart < 0) {
      nextContent = `${existing.trimEnd()}\n\n${sectionText}`;
    } else {
      let sectionEnd = lines.length;
      for (let i = sectionStart + 1; i < lines.length; i += 1) {
        const trimmed = lines[i].trim();
        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
          sectionEnd = i;
          break;
        }
      }
      const merged = [...lines.slice(0, sectionStart), ...sectionText.trimEnd().split("\n"), ...lines.slice(sectionEnd)];
      nextContent = `${merged.join("\n").trimEnd()}\n`;
    }
  }
  const changed = nextContent !== existing;
  if (!changed) {
    return { configPath, changed: false };
  }
  await writeFile(configPath, nextContent, "utf8");
  await chmod(configPath, 0o600).catch(() => undefined);
  return { configPath, changed };
}

function resolveAgentStateDir(): string {
  return process.env.DOER_AGENT_STATE_DIR?.trim() || path.join(homedir(), ".doer-agent");
}

function resolveContainerReachableServerBaseUrl(serverBaseUrl: string): string {
  if (!existsSync("/.dockerenv")) {
    return serverBaseUrl;
  }
  try {
    const parsed = new URL(serverBaseUrl);
    if (parsed.hostname === "localhost" || parsed.hostname === "127.0.0.1" || parsed.hostname === "::1") {
      parsed.hostname = "host.docker.internal";
      return parsed.toString().replace(/\/$/, "");
    }
  } catch {
    return serverBaseUrl;
  }
  return serverBaseUrl;
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
  const binDir = path.join(resolveAgentStateDir(), "bin");
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
    const normalizedValue = raw.trim();
    if (!normalizedKey || !normalizedValue) {
      continue;
    }
    out[normalizedKey] = normalizedValue;
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

async function readPendingAgentEvents(queuePath: string): Promise<PendingAgentEvent[]> {
  try {
    const raw = await readFile(queuePath, "utf8");
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed.filter((row): row is PendingAgentEvent => {
      if (!row || typeof row !== "object") {
        return false;
      }
      const item = row as Record<string, unknown>;
      return (
        typeof item.id === "string" &&
        typeof item.serverBaseUrl === "string" &&
        typeof item.userId === "string" &&
        typeof item.taskId === "string" &&
        (item.type === "stdout" || item.type === "stderr" || item.type === "status" || item.type === "meta") &&
        typeof item.seq === "number" &&
        Number.isInteger(item.seq) &&
        item.seq > 0 &&
        item.payload !== null &&
        typeof item.payload === "object"
      );
    });
  } catch {
    return [];
  }
}

async function writePendingAgentEvents(queuePath: string, events: PendingAgentEvent[]): Promise<void> {
  const dir = path.dirname(queuePath);
  await mkdir(dir, { recursive: true });
  const tmpPath = `${queuePath}.tmp`;
  const handle = await open(tmpPath, "w");
  try {
    await handle.writeFile(JSON.stringify(events), "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  await rename(tmpPath, queuePath);
}

async function findMaxPendingEventSeq(args: {
  queuePath: string;
  serverBaseUrl: string;
  userId: string;
  taskId: string;
}): Promise<number> {
  const events = await readPendingAgentEvents(args.queuePath);
  let maxSeq = 0;
  for (const event of events) {
    if (event.serverBaseUrl !== args.serverBaseUrl || event.userId !== args.userId || event.taskId !== args.taskId) {
      continue;
    }
    if (typeof event.seq === "number" && Number.isInteger(event.seq) && event.seq > maxSeq) {
      maxSeq = event.seq;
    }
  }
  return maxSeq;
}

async function enqueuePendingAgentEvent(args: {
  queuePath: string;
  serverBaseUrl: string;
  userId: string;
  taskId: string;
  type: AgentEventType;
  seq: number;
  payload: Record<string, unknown>;
}): Promise<void> {
  await withPendingEventQueueLock(async () => {
    const events = await readPendingAgentEvents(args.queuePath);
    const next: PendingAgentEvent = {
      id: `${Date.now()}_${Math.random().toString(36).slice(2, 10)}`,
      serverBaseUrl: args.serverBaseUrl,
      userId: args.userId,
      taskId: args.taskId,
      type: args.type,
      seq: args.seq,
      payload: args.payload,
      createdAt: formatLocalTimestamp(),
      lastAttemptAt: null,
      attempts: 0,
    };
    events.push(next);
    await writePendingAgentEvents(args.queuePath, events);
  });
}

async function flushPendingAgentEvents(args: {
  queuePath: string;
  serverBaseUrl: string;
  userId: string;
  agentToken: string;
  maxCount?: number;
}): Promise<{ delivered: number; remaining: number }> {
  return withPendingEventQueueLock(async () => {
    const events = await readPendingAgentEvents(args.queuePath);
    if (events.length === 0) {
      return { delivered: 0, remaining: 0 };
    }
    const maxCount =
      typeof args.maxCount === "number" && Number.isFinite(args.maxCount) && args.maxCount > 0 ? Math.floor(args.maxCount) : 200;
    const scoped = events.filter((event) => event.serverBaseUrl === args.serverBaseUrl && event.userId === args.userId);
    const scopedIds = new Set(scoped.map((event) => event.id));
    const retained = events.filter((event) => !scopedIds.has(event.id));
    let delivered = 0;
    let processed = 0;

    const byTask = new Map<string, PendingAgentEvent[]>();
    for (const event of scoped) {
      const bucket = byTask.get(event.taskId);
      if (bucket) {
        bucket.push(event);
      } else {
        byTask.set(event.taskId, [event]);
      }
    }

    for (const taskEvents of byTask.values()) {
      taskEvents.sort((a, b) => a.seq - b.seq || a.createdAt.localeCompare(b.createdAt));
    }

    for (const taskEvents of byTask.values()) {
      for (let i = 0; i < taskEvents.length; i += 1) {
        const event = taskEvents[i];
        if (typeof event.seq === "number" && Number.isInteger(event.seq) && event.seq > 0) {
          bumpNextEventSeq(event.taskId, event.seq + 1);
        }
        if (processed >= maxCount) {
          retained.push(event);
          continue;
        }
        processed += 1;
        try {
          await emitEvent({
            serverBaseUrl: args.serverBaseUrl,
            taskId: event.taskId,
            userId: args.userId,
            agentToken: args.agentToken,
            type: event.type,
            seq: event.seq,
            payload: event.payload,
          });
          delivered += 1;
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          const match = /Out-of-order event seq:\s*expected\s+(\d+),\s*got\s+(\d+)/.exec(message);
          if (match) {
            const expected = Number(match[1]);
            const got = Number(match[2]);
            if (Number.isFinite(expected) && Number.isFinite(got) && expected > got) {
              // Server already acknowledged this or later seq; drop stale event and continue.
              writeTaskUpload(event.taskId, `drop-stale type=${event.type} seq=${event.seq} reason=${message}`);
              continue;
            }
          }
          writeTaskUpload(event.taskId, `retry type=${event.type} seq=${event.seq} nextAttempt=${event.attempts + 1} reason=${message}`);
          retained.push({
            ...event,
            attempts: event.attempts + 1,
            lastAttemptAt: formatLocalTimestamp(),
          });
          for (let j = i + 1; j < taskEvents.length; j += 1) {
            retained.push(taskEvents[j]);
          }
          break;
        }
      }
    }

    await writePendingAgentEvents(args.queuePath, retained);
    return { delivered, remaining: retained.length };
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function writeAgentInfo(message: string): void {
  process.stdout.write(`[doer-agent] ${message}\n`);
}

function writeAgentError(message: string): void {
  process.stderr.write(`[doer-agent] ${message}\n`);
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

function formatLocalTimestamp(date = new Date()): string {
  const pad = (value: number, size = 2) => String(value).padStart(size, "0");
  const year = date.getFullYear();
  const month = pad(date.getMonth() + 1);
  const day = pad(date.getDate());
  const hours = pad(date.getHours());
  const minutes = pad(date.getMinutes());
  const seconds = pad(date.getSeconds());
  const ms = pad(date.getMilliseconds(), 3);
  const offsetMinutes = -date.getTimezoneOffset();
  const sign = offsetMinutes >= 0 ? "+" : "-";
  const absOffset = Math.abs(offsetMinutes);
  const offsetHour = pad(Math.floor(absOffset / 60));
  const offsetMinute = pad(absOffset % 60);
  return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}.${ms}${sign}${offsetHour}:${offsetMinute}`;
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

function resolvePollWaitMs(): number {
  const raw = process.env.DOER_LOCAL_AGENT_POLL_WAIT_MS?.trim() ?? "";
  const parsed = raw ? Number(raw) : NaN;
  if (Number.isFinite(parsed) && parsed >= 1000) {
    return Math.floor(parsed);
  }
  return 5000;
}

function buildAgentStreamUrl(args: {
  serverBaseUrl: string;
  userId: string;
  agentToken: string;
  waitMs: number;
}): string {
  const url = new URL("/api/agent/stream", `${args.serverBaseUrl}/`);
  url.searchParams.set("userId", args.userId);
  url.searchParams.set("agentToken", args.agentToken);
  url.searchParams.set("waitMs", String(args.waitMs));
  return url.toString();
}

async function consumeTaskStream(args: {
  serverBaseUrl: string;
  userId: string;
  agentToken: string;
  waitMs: number;
  onOpen?: () => void;
  onTask: (task: NonNullable<PollResponse["task"]>) => Promise<void>;
}): Promise<void> {
  const streamUrl = buildAgentStreamUrl({
    serverBaseUrl: args.serverBaseUrl,
    userId: args.userId,
    agentToken: args.agentToken,
    waitMs: args.waitMs,
  });

  const res = await fetch(streamUrl, {
    method: "GET",
    headers: {
      Accept: "text/event-stream",
      "Cache-Control": "no-cache",
    },
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    let message = `HTTP ${res.status}`;
    if (text) {
      try {
        const parsed = JSON.parse(text) as { error?: unknown };
        if (typeof parsed.error === "string" && parsed.error.trim()) {
          message = parsed.error.trim();
        }
      } catch {
        // no-op
      }
    }
    throw new Error(message);
  }

  const reader = res.body?.getReader();
  if (!reader) {
    throw new Error("SSE response body is unavailable");
  }

  args.onOpen?.();

  const decoder = new TextDecoder();
  let buffer = "";
  let eventName = "message";
  const dataLines: string[] = [];

  const handleEvent = async (event: SseEnvelope): Promise<void> => {
    if (!event.data.trim()) {
      return;
    }
    if (event.event === "keepalive") {
      return;
    }
    if (event.event === "error") {
      let message = event.data;
      try {
        const parsed = JSON.parse(event.data) as { error?: unknown };
        if (typeof parsed.error === "string" && parsed.error.trim()) {
          message = parsed.error.trim();
        }
      } catch {
        // no-op
      }
      throw new Error(message || "SSE stream error");
    }

    if (event.event !== "task" && event.event !== "message") {
      return;
    }

    let parsed: { task?: PollResponse["task"] } = {};
    try {
      parsed = JSON.parse(event.data) as { task?: PollResponse["task"] };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`Invalid SSE payload: ${message}`);
    }

    const task = parsed.task;
    if (task) {
      await args.onTask(task);
    }
  };

  const dispatchEvent = async (): Promise<void> => {
    if (dataLines.length === 0) {
      eventName = "message";
      return;
    }
    const data = dataLines.join("\n");
    const event = eventName || "message";
    eventName = "message";
    dataLines.length = 0;
    await handleEvent({ event, data });
  };

  const processLine = async (line: string): Promise<void> => {
    if (line === "") {
      await dispatchEvent();
      return;
    }
    if (line.startsWith(":")) {
      return;
    }

    const separator = line.indexOf(":");
    const field = separator < 0 ? line : line.slice(0, separator);
    let value = separator < 0 ? "" : line.slice(separator + 1);
    if (value.startsWith(" ")) {
      value = value.slice(1);
    }

    if (field === "event") {
      eventName = value || "message";
      return;
    }
    if (field === "data") {
      dataLines.push(value);
    }
  };

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      buffer += decoder.decode();
      break;
    }
    buffer += decoder.decode(value, { stream: true });

    while (true) {
      const newlineIndex = buffer.indexOf("\n");
      if (newlineIndex < 0) {
        break;
      }
      let line = buffer.slice(0, newlineIndex);
      buffer = buffer.slice(newlineIndex + 1);
      if (line.endsWith("\r")) {
        line = line.slice(0, -1);
      }
      await processLine(line);
    }
  }

  if (buffer.length > 0) {
    const trailing = buffer.endsWith("\r") ? buffer.slice(0, -1) : buffer;
    if (trailing.length > 0) {
      await processLine(trailing);
    }
  }

  await dispatchEvent();
  throw new Error("SSE stream closed by server");
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

async function emitEvent(args: {
  serverBaseUrl: string;
  taskId: string;
  userId: string;
  agentToken: string;
  type: AgentEventType;
  seq: number;
  payload: Record<string, unknown>;
}): Promise<void> {
  await postJson(`${args.serverBaseUrl}/api/agent/tasks/${encodeURIComponent(args.taskId)}/events`, {
    userId: args.userId,
    agentToken: args.agentToken,
    type: args.type,
    seq: args.seq,
    payload: args.payload,
  });
}

const nextEventSeqByTask = new Map<string, number>();
let pendingUploaderWake: (() => void) | null = null;
let pendingUploaderStarted = false;

function reserveNextEventSeq(taskId: string): number {
  const current = nextEventSeqByTask.get(taskId) ?? 1;
  nextEventSeqByTask.set(taskId, current + 1);
  return current;
}

function bumpNextEventSeq(taskId: string, nextSeq: number): void {
  if (!Number.isInteger(nextSeq) || nextSeq <= 0) {
    return;
  }
  const current = nextEventSeqByTask.get(taskId) ?? 1;
  if (nextSeq > current) {
    nextEventSeqByTask.set(taskId, nextSeq);
  }
}

function signalPendingUploader(): void {
  pendingUploaderWake?.();
}

async function recordAgentEvent(args: {
  queuePath: string;
  serverBaseUrl: string;
  taskId: string;
  userId: string;
  type: AgentEventType;
  seq: number;
  payload: Record<string, unknown>;
}): Promise<void> {
  await enqueuePendingAgentEvent({
    queuePath: args.queuePath,
    serverBaseUrl: args.serverBaseUrl,
    userId: args.userId,
    taskId: args.taskId,
    type: args.type,
    seq: args.seq,
    payload: args.payload,
  });
  signalPendingUploader();
}

function startPendingEventUploader(args: {
  queuePath: string;
  serverBaseUrl: string;
  userId: string;
  agentToken: string;
}): void {
  if (pendingUploaderStarted) {
    return;
  }
  pendingUploaderStarted = true;
  void (async () => {
    while (true) {
      try {
        const result = await flushPendingAgentEvents({
          queuePath: args.queuePath,
          serverBaseUrl: args.serverBaseUrl,
          userId: args.userId,
          agentToken: args.agentToken,
        });
        if (result.delivered > 0) {
          writeAgentInfo(`flushed pending events delivered=${result.delivered} remaining=${result.remaining}`);
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        writeAgentError(`pending event flush failed: ${message}`);
      }

      await new Promise<void>((resolve) => {
        let settled = false;
        const finish = () => {
          if (settled) {
            return;
          }
          settled = true;
          if (pendingUploaderWake === finish) {
            pendingUploaderWake = null;
          }
          resolve();
        };
        pendingUploaderWake = finish;
        setTimeout(finish, 1000).unref?.();
      });
    }
  })();
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
  pendingEventQueuePath: string;
}): Promise<void> {
  const shellPath = resolveShellPath();
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
  };
  const taskGitEnv = await prepareTaskGitEnv({
    cwd: args.cwd || process.cwd(),
    baseEnvPatch: baseTaskEnvPatch,
  });
  const codexHome = pickFirstNonEmpty([baseTaskEnvPatch.CODEX_HOME, process.env.CODEX_HOME, resolveCodexHomePath()]);
  const codexMcpConfig = await ensureCodexPlaywrightMcpConfig(codexHome);
  const codexMcpEnvPatch: Record<string, string> = {
    CODEX_HOME: codexHome,
    PLAYWRIGHT_BROWSERS_PATH: process.env.PLAYWRIGHT_BROWSERS_PATH || PLAYWRIGHT_BROWSERS_PATH,
    PLAYWRIGHT_SKIP_BROWSER_GC: PLAYWRIGHT_SKIP_BROWSER_GC,
  };
  const maxQueuedSeq = await findMaxPendingEventSeq({
    queuePath: args.pendingEventQueuePath,
    serverBaseUrl: args.serverBaseUrl,
    userId: args.userId,
    taskId: args.taskId,
  }).catch(() => 0);
  if (maxQueuedSeq > 0) {
    bumpNextEventSeq(args.taskId, maxQueuedSeq + 1);
  }
  await recordAgentEvent({
    queuePath: args.pendingEventQueuePath,
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
      cwd: args.cwd,
      shell: shellPath,
      ...(runtimeConfig?.meta ?? { runtimeConfigSynced: false }),
      ...(codexAuth?.meta ?? { codexAuthSynced: false }),
      ...(taskGitEnv.meta ?? {}),
      codexPlaywrightMcpConfigPath: codexMcpConfig.configPath,
      codexPlaywrightMcpConfigUpdated: codexMcpConfig.changed,
    },
  });

  try {
    let terminationReason: "cancel" | null = null;
    let cancelStage1Timer: NodeJS.Timeout | null = null;
    let cancelStage2Timer: NodeJS.Timeout | null = null;
    let stopCancelPolling = false;

    const child = spawn(args.command, {
      cwd: args.cwd || process.cwd(),
      shell: shellPath,
      detached: process.platform !== "win32",
      env: {
        ...process.env,
        ...baseTaskEnvPatch,
        ...taskGitEnv.envPatch,
        ...codexMcpEnvPatch,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");

    child.stdout.on("data", (chunk: string) => {
      writeTaskStream(args.taskId, "stdout", chunk);
      const seq = reserveNextEventSeq(args.taskId);
      void recordAgentEvent({
        queuePath: args.pendingEventQueuePath,
        serverBaseUrl: args.serverBaseUrl,
        taskId: args.taskId,
        userId: args.userId,
        type: "stdout",
        seq,
        payload: { chunk, at: formatLocalTimestamp() },
      }).catch((error) => {
        const message = error instanceof Error ? error.message : String(error);
        writeAgentError(`task=${args.taskId} stdout failed to persist pending event: ${message}`);
      });
    });

    child.stderr.on("data", (chunk: string) => {
      writeTaskStream(args.taskId, "stderr", chunk);
      const seq = reserveNextEventSeq(args.taskId);
      void recordAgentEvent({
        queuePath: args.pendingEventQueuePath,
        serverBaseUrl: args.serverBaseUrl,
        taskId: args.taskId,
        userId: args.userId,
        type: "stderr",
        seq,
        payload: { chunk, at: formatLocalTimestamp() },
      }).catch((error) => {
        const message = error instanceof Error ? error.message : String(error);
        writeAgentError(`task=${args.taskId} stderr failed to persist pending event: ${message}`);
      });
    });

    const cancelPoller = (async () => {
      while (!stopCancelPolling) {
        await sleep(1500);
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
    await recordAgentEvent({
      queuePath: args.pendingEventQueuePath,
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
    await codexAuth?.cleanup().catch(() => undefined);
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const serverBaseUrlRaw = args.server || args.url || "http://localhost:2020";
  const requestedServerBaseUrl = serverBaseUrlRaw.replace(/\/$/, "");
  const serverBaseUrl = resolveContainerReachableServerBaseUrl(requestedServerBaseUrl);
  const userId = (args["user-id"] || args.userId || "").trim();
  const agentSecret = (args["agent-secret"] || args.agentSecret || "").trim();
  if (!userId || !agentSecret) {
    throw new Error("user-id and agent-secret are required");
  }
  const agentToken = agentSecret;
  const pendingEventQueuePath = resolvePendingEventQueuePath();
  const pollWaitMs = resolvePollWaitMs();

  process.stdout.write(`\n[doer-agent]\n`);
  process.stdout.write(`- server: ${serverBaseUrl}\n`);
  process.stdout.write(`- userId: ${userId}\n`);
  process.stdout.write(`\n- streamWaitMs: ${pollWaitMs}\n\n`);
  if (requestedServerBaseUrl !== serverBaseUrl) {
    writeAgentInfo(
      `detected container runtime, server endpoint rewritten: ${requestedServerBaseUrl} -> ${serverBaseUrl}`,
    );
  }
  process.stdout.write(
    `시작 커맨드 예시: npm run start -- --server ${serverBaseUrl} --user-id ${userId} --agent-secret <SECRET>\n`,
  );

  startPendingEventUploader({
    queuePath: pendingEventQueuePath,
    serverBaseUrl,
    userId,
    agentToken,
  });

  let isStreamConnected = false;
  let hasConnectedBefore = false;
  let consecutiveStreamErrors = 0;

  while (true) {
    try {
      await consumeTaskStream({
        serverBaseUrl,
        userId,
        agentToken,
        waitMs: pollWaitMs,
        onOpen: () => {
          consecutiveStreamErrors = 0;
          if (!isStreamConnected) {
            const statusText = hasConnectedBefore ? "reconnected" : "connected";
            writeAgentInfo(
              `${statusText} to server (SSE ok) at=${formatLocalTimestamp()} userId=${userId} server=${serverBaseUrl}`,
            );
            hasConnectedBefore = true;
          }
          isStreamConnected = true;
        },
        onTask: async (task) => {
          writeAgentInfo(`run task=${task.id} command=${task.command}`);
          await runTask({
            serverBaseUrl,
            taskId: task.id,
            command: task.command,
            cwd: task.cwd,
            userId,
            agentToken,
            pendingEventQueuePath,
          }).catch(async (error) => {
            const message = error instanceof Error ? error.message : String(error);
            writeAgentError(`task=${task.id} run failed: ${message}`);
            const failPayload = {
              status: "failed",
              error: message,
              finishedAt: formatLocalTimestamp(),
            } satisfies Record<string, unknown>;
            await recordAgentEvent({
              queuePath: pendingEventQueuePath,
              serverBaseUrl,
              taskId: task.id,
              userId,
              type: "status",
              seq: reserveNextEventSeq(task.id),
              payload: failPayload,
            });
          });
        },
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      consecutiveStreamErrors += 1;
      if (isStreamConnected) {
        writeAgentError(`disconnected from server at=${formatLocalTimestamp()} reason=${message}`);
      }
      isStreamConnected = false;
      writeAgentError(`stream loop error: ${message} (retry in 2s, attempt=${consecutiveStreamErrors})`);
      await sleep(2000);
    }
  }
}
main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  writeAgentError(`fatal: ${message}`);
  process.exitCode = 1;
});
