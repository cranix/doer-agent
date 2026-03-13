import { spawn, spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { chmod, mkdir, open, readFile, rename, writeFile } from "node:fs/promises";
import { arch, homedir } from "node:os";
import path from "node:path";

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

function resolvePlaywrightMcpCommand(): { command: string; args: string[] } {
  const browserArgs = arch() === "arm64" ? ["--browser", "chromium"] : [];
  const browserEnvArgs = [
    `PLAYWRIGHT_BROWSERS_PATH=${PLAYWRIGHT_BROWSERS_PATH}`,
    `PLAYWRIGHT_SKIP_BROWSER_GC=${PLAYWRIGHT_SKIP_BROWSER_GC}`,
  ];
  const localBin = path.join("/app", "node_modules", ".bin", "playwright-mcp");
  if (existsSync(localBin)) {
    return { command: "env", args: [...browserEnvArgs, localBin, ...browserArgs] };
  }
  const workspaceBin = path.join(process.cwd(), "node_modules", ".bin", "playwright-mcp");
  if (existsSync(workspaceBin)) {
    return { command: "env", args: [...browserEnvArgs, workspaceBin, ...browserArgs] };
  }
  return { command: "env", args: [...browserEnvArgs, "npx", "-y", "@playwright/mcp", ...browserArgs] };
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

  const { command, args } = resolvePlaywrightMcpCommand();
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
  process.stdout.write(`\n- pollWaitMs: ${pollWaitMs}\n\n`);
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

  let isPollConnected = false;
  let hasConnectedBefore = false;
  let consecutivePollErrors = 0;

  while (true) {
    try {
      const polled = await postJson<PollResponse>(`${serverBaseUrl}/api/agent/poll`, {
        userId,
        agentToken,
        waitMs: pollWaitMs,
      });
      consecutivePollErrors = 0;
      if (!isPollConnected) {
        const statusText = hasConnectedBefore ? "reconnected" : "connected";
        writeAgentInfo(
          `${statusText} to server (poll ok) at=${formatLocalTimestamp()} userId=${userId} server=${serverBaseUrl}`,
        );
        hasConnectedBefore = true;
      }
      isPollConnected = true;
      const task = polled.task;
      if (!task) {
        continue;
      }

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
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      consecutivePollErrors += 1;
      if (isPollConnected) {
        writeAgentError(`disconnected from server at=${formatLocalTimestamp()} reason=${message}`);
      }
      isPollConnected = false;
      writeAgentError(`poll loop error: ${message} (retry in 2s, attempt=${consecutivePollErrors})`);
      await sleep(2000);
    }
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  writeAgentError(`fatal: ${message}`);
  process.exitCode = 1;
});
