import { spawn } from "node:child_process";
import crypto from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { StringCodec, type Msg, type NatsConnection } from "nats";
import { buildAgentSettingsEnvPatch, type AgentSettingsConfig } from "./agent-settings.js";
import { normalizeEnvPatch, sleep } from "./agent-runtime-utils.js";
import { sendSignalToPid } from "./agent-task-execution.js";

const daemonRpcCodec = StringCodec();
const DAEMON_ID_PATTERN = /^[A-Za-z0-9_-]{6,32}$/;

type AgentDaemonRpcAction = "list" | "inspect" | "start" | "stop" | "restart" | "delete" | "logs";
type AgentDaemonStatus = "running" | "stopped" | "failed";
type AgentDaemonLogEventType = "start" | "stdout" | "stderr" | "exit" | "signal" | "error";

interface AgentDaemonStateRecord {
  id: string;
  label: string | null;
  command: string;
  cwd: string;
  pid: number | null;
  runnerPid: number | null;
  startedAt: string;
  stoppedAt: string | null;
  lastExitCode: number | null;
}

export interface AgentDaemonSnapshot extends AgentDaemonStateRecord {
  status: AgentDaemonStatus;
  displayName: string;
}

export interface AgentDaemonLogEvent {
  ts: string;
  type: AgentDaemonLogEventType;
  text: string | null;
  pid: number | null;
  code: number | null;
  signal: string | null;
}

interface AgentDaemonRpcRequest {
  requestId?: unknown;
  action?: unknown;
  daemonId?: unknown;
  label?: unknown;
  command?: unknown;
  cwd?: unknown;
  envPatch?: unknown;
  limit?: unknown;
}

function getDaemonsRoot(workspaceRoot: string): string {
  return path.join(workspaceRoot, ".doer-agent", "daemons");
}

function getDaemonDir(workspaceRoot: string, daemonId: string): string {
  return path.join(getDaemonsRoot(workspaceRoot), daemonId);
}

function getDaemonStatePath(workspaceRoot: string, daemonId: string): string {
  return path.join(getDaemonDir(workspaceRoot, daemonId), "state.json");
}

function getDaemonEventsPath(workspaceRoot: string, daemonId: string): string {
  return path.join(getDaemonDir(workspaceRoot, daemonId), "events.jsonl");
}

function isPidAlive(pid: number | null): boolean {
  if (!Number.isInteger(pid) || (pid ?? 0) <= 0) {
    return false;
  }
  try {
    process.kill(pid as number, 0);
    return true;
  } catch {
    return false;
  }
}

function createDaemonId(): string {
  return crypto.randomBytes(9).toString("base64url").slice(0, 12);
}

function normalizeAction(value: unknown): AgentDaemonRpcAction {
  if (value === "list" || value === "inspect" || value === "start" || value === "stop" || value === "restart" || value === "delete" || value === "logs") {
    return value;
  }
  throw new Error("unsupported action");
}

function normalizeDaemonId(value: unknown): string {
  const daemonId = typeof value === "string" ? value.trim() : "";
  if (!DAEMON_ID_PATTERN.test(daemonId)) {
    throw new Error("invalid daemonId");
  }
  return daemonId;
}

function normalizeCommand(value: unknown): string {
  const command = typeof value === "string" ? value.trim() : "";
  if (!command) {
    throw new Error("command is required");
  }
  return command;
}

function normalizeLabel(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const label = value.trim();
  return label ? label.slice(0, 120) : null;
}

function normalizeLimit(value: unknown, fallback: number): number {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }
  return Math.max(1, Math.min(Math.floor(numeric), 1000));
}

function normalizeStateRecord(value: unknown): AgentDaemonStateRecord | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const row = value as Record<string, unknown>;
  const id = typeof row.id === "string" ? row.id.trim() : "";
  const command = typeof row.command === "string" ? row.command.trim() : "";
  const cwd = typeof row.cwd === "string" ? row.cwd.trim() : "";
  const startedAt = typeof row.startedAt === "string" ? row.startedAt.trim() : "";
  const label = typeof row.label === "string" && row.label.trim() ? row.label.trim() : null;
  const stoppedAt = typeof row.stoppedAt === "string" && row.stoppedAt.trim() ? row.stoppedAt.trim() : null;
  const pid = typeof row.pid === "number" && Number.isInteger(row.pid) && row.pid > 0 ? row.pid : null;
  const runnerPid =
    typeof row.runnerPid === "number" && Number.isInteger(row.runnerPid) && row.runnerPid > 0 ? row.runnerPid : null;
  const lastExitCode =
    typeof row.lastExitCode === "number" && Number.isInteger(row.lastExitCode) ? row.lastExitCode : null;
  if (!DAEMON_ID_PATTERN.test(id) || !command || !cwd || !startedAt) {
    return null;
  }
  return {
    id,
    label,
    command,
    cwd,
    pid,
    runnerPid,
    startedAt,
    stoppedAt,
    lastExitCode,
  };
}

function normalizeLogEvent(value: unknown): AgentDaemonLogEvent | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const row = value as Record<string, unknown>;
  const ts = typeof row.ts === "string" ? row.ts.trim() : "";
  const type =
    row.type === "start" || row.type === "stdout" || row.type === "stderr" || row.type === "exit" || row.type === "signal" || row.type === "error"
      ? row.type
      : null;
  if (!ts || !type) {
    return null;
  }
  return {
    ts,
    type,
    text: typeof row.text === "string" ? row.text : null,
    pid: typeof row.pid === "number" && Number.isInteger(row.pid) && row.pid > 0 ? row.pid : null,
    code: typeof row.code === "number" && Number.isInteger(row.code) ? row.code : null,
    signal: typeof row.signal === "string" && row.signal.trim() ? row.signal.trim() : null,
  };
}

async function readDaemonState(workspaceRoot: string, daemonId: string): Promise<AgentDaemonStateRecord | null> {
  const raw = await readFile(getDaemonStatePath(workspaceRoot, daemonId), "utf8").catch(() => null);
  if (!raw) {
    return null;
  }
  const parsed = JSON.parse(raw) as unknown;
  return normalizeStateRecord(parsed);
}

async function writeDaemonState(workspaceRoot: string, state: AgentDaemonStateRecord): Promise<void> {
  await mkdir(getDaemonDir(workspaceRoot, state.id), { recursive: true });
  await writeFile(getDaemonStatePath(workspaceRoot, state.id), `${JSON.stringify(state, null, 2)}\n`, "utf8");
}

function toDaemonSnapshot(state: AgentDaemonStateRecord): AgentDaemonSnapshot {
  const runnerAlive = isPidAlive(state.runnerPid);
  const processAlive = isPidAlive(state.pid);
  const status: AgentDaemonStatus =
    runnerAlive || processAlive ? "running" : state.lastExitCode !== null && state.lastExitCode !== 0 ? "failed" : "stopped";
  return {
    ...state,
    status,
    displayName: state.label ?? state.command,
  };
}

async function readJsonlTail(filePath: string, limit: number): Promise<AgentDaemonLogEvent[]> {
  const raw = await readFile(filePath, "utf8").catch(() => "");
  if (!raw) {
    return [];
  }
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .slice(-limit)
    .map((line) => {
      try {
        return normalizeLogEvent(JSON.parse(line) as unknown);
      } catch {
        return null;
      }
    })
    .filter((event): event is AgentDaemonLogEvent => Boolean(event));
}

function resolveDaemonRunnerEntry(agentProjectDir: string): { command: string; args: string[] } {
  const distEntry = path.join(agentProjectDir, "dist", "daemon-log-runner.js");
  const srcEntry = path.join(agentProjectDir, "src", "daemon-log-runner.ts");
  const tsxLoaderPath = path.join(agentProjectDir, "node_modules", "tsx", "dist", "loader.mjs");
  if (existsSync(distEntry)) {
    return {
      command: process.execPath,
      args: [distEntry],
    };
  }
  return {
    command: process.execPath,
    args: ["--import", tsxLoaderPath, srcEntry],
  };
}

export async function listAgentDaemonsLocal(workspaceRoot: string): Promise<AgentDaemonSnapshot[]> {
  const root = getDaemonsRoot(workspaceRoot);
  const entries = await readdir(root, { withFileTypes: true }).catch(() => []);
  const states = await Promise.all(
    entries
      .filter((entry) => entry.isDirectory() && DAEMON_ID_PATTERN.test(entry.name))
      .map(async (entry) => readDaemonState(workspaceRoot, entry.name)),
  );
  return states
    .filter((state): state is AgentDaemonStateRecord => Boolean(state))
    .map((state) => toDaemonSnapshot(state))
    .sort((a, b) => b.startedAt.localeCompare(a.startedAt));
}

export async function getAgentDaemonLocal(workspaceRoot: string, daemonId: string): Promise<AgentDaemonSnapshot> {
  const state = await readDaemonState(workspaceRoot, daemonId);
  if (!state) {
    throw new Error("daemon not found");
  }
  return toDaemonSnapshot(state);
}

export async function stopAgentDaemonLocal(workspaceRoot: string, daemonId: string): Promise<AgentDaemonSnapshot> {
  const state = await readDaemonState(workspaceRoot, daemonId);
  if (!state) {
    throw new Error("daemon not found");
  }
  const targetPid = isPidAlive(state.runnerPid) ? state.runnerPid : state.pid;
  if (!targetPid) {
    const stopped = {
      ...state,
      pid: null,
      runnerPid: null,
      stoppedAt: state.stoppedAt ?? new Date().toISOString(),
    } satisfies AgentDaemonStateRecord;
    await writeDaemonState(workspaceRoot, stopped);
    return toDaemonSnapshot(stopped);
  }

  try {
    sendSignalToPid(targetPid, "SIGTERM");
  } catch {
    // Ignore and fall through to polling.
  }
  for (let index = 0; index < 30; index += 1) {
    await sleep(100);
    const latest = await readDaemonState(workspaceRoot, daemonId);
    if (!latest || (!isPidAlive(latest.runnerPid) && !isPidAlive(latest.pid))) {
      return latest ? toDaemonSnapshot(latest) : toDaemonSnapshot({
        ...state,
        pid: null,
        runnerPid: null,
        stoppedAt: new Date().toISOString(),
      });
    }
  }
  try {
    sendSignalToPid(targetPid, "SIGKILL");
  } catch {
    // noop
  }
  for (let index = 0; index < 15; index += 1) {
    await sleep(100);
    const latest = await readDaemonState(workspaceRoot, daemonId);
    if (!latest || (!isPidAlive(latest.runnerPid) && !isPidAlive(latest.pid))) {
      return latest ? toDaemonSnapshot(latest) : toDaemonSnapshot({
        ...state,
        pid: null,
        runnerPid: null,
        stoppedAt: new Date().toISOString(),
      });
    }
  }
  throw new Error("failed to stop daemon");
}

async function spawnDaemonLocal(args: {
  workspaceRoot: string;
  agentProjectDir: string;
  daemonId?: string;
  command: string;
  cwd: string;
  label: string | null;
  envPatch: Record<string, string>;
  resolveShellPath: () => string;
  readAgentSettingsConfig: (args: { workspaceRoot: string }) => Promise<AgentSettingsConfig>;
}): Promise<AgentDaemonSnapshot> {
  const daemonId = args.daemonId ?? createDaemonId();
  const daemonDir = getDaemonDir(args.workspaceRoot, daemonId);
  const statePath = getDaemonStatePath(args.workspaceRoot, daemonId);
  const eventsPath = getDaemonEventsPath(args.workspaceRoot, daemonId);
  await mkdir(daemonDir, { recursive: true });
  await mkdir(path.join(args.workspaceRoot, ".codex"), { recursive: true });

  const settings = await args.readAgentSettingsConfig({ workspaceRoot: args.workspaceRoot });
  const runtimeBinPath = path.join(args.agentProjectDir, "runtime/bin");
  const baseState: AgentDaemonStateRecord = {
    id: daemonId,
    label: args.label,
    command: args.command,
    cwd: args.cwd,
    pid: null,
    runnerPid: null,
    startedAt: new Date().toISOString(),
    stoppedAt: null,
    lastExitCode: null,
  };
  await writeDaemonState(args.workspaceRoot, baseState);

  const env: NodeJS.ProcessEnv = {
    ...process.env,
    ...buildAgentSettingsEnvPatch(settings),
    ...args.envPatch,
    WORKSPACE: args.cwd,
    CODEX_HOME: path.join(args.workspaceRoot, ".codex"),
    PATH: [runtimeBinPath, process.env.PATH || ""].filter(Boolean).join(path.delimiter),
    DOER_DAEMON_ID: daemonId,
    DOER_DAEMON_COMMAND: args.command,
    DOER_DAEMON_CWD: args.cwd,
    DOER_DAEMON_STATE_PATH: statePath,
    DOER_DAEMON_EVENTS_PATH: eventsPath,
    DOER_DAEMON_SHELL_PATH: args.resolveShellPath(),
  };
  const runner = resolveDaemonRunnerEntry(args.agentProjectDir);
  const child = spawn(runner.command, runner.args, {
    cwd: args.cwd,
    env,
    detached: process.platform !== "win32",
    stdio: "ignore",
  });
  if (typeof child.pid !== "number" || child.pid <= 0) {
    throw new Error("failed to start daemon process");
  }
  child.unref();

  await writeDaemonState(args.workspaceRoot, {
    ...baseState,
    runnerPid: child.pid,
  });

  for (let index = 0; index < 20; index += 1) {
    await sleep(50);
    const latest = await readDaemonState(args.workspaceRoot, daemonId);
    if (latest && (latest.pid || latest.lastExitCode !== null)) {
      return toDaemonSnapshot(latest);
    }
  }
  const latest = await readDaemonState(args.workspaceRoot, daemonId);
  return toDaemonSnapshot(latest ?? { ...baseState, runnerPid: child.pid });
}

export async function startAgentDaemonLocal(args: {
  workspaceRoot: string;
  agentProjectDir: string;
  request: AgentDaemonRpcRequest;
  resolveShellPath: () => string;
  resolveTaskWorkspace: (cwd: string | null) => string;
  readAgentSettingsConfig: (args: { workspaceRoot: string }) => Promise<AgentSettingsConfig>;
}): Promise<AgentDaemonSnapshot> {
  return await spawnDaemonLocal({
    workspaceRoot: args.workspaceRoot,
    agentProjectDir: args.agentProjectDir,
    command: normalizeCommand(args.request.command),
    cwd: args.resolveTaskWorkspace(typeof args.request.cwd === "string" ? args.request.cwd : null),
    label: normalizeLabel(args.request.label),
    envPatch: normalizeEnvPatch(args.request.envPatch),
    resolveShellPath: args.resolveShellPath,
    readAgentSettingsConfig: args.readAgentSettingsConfig,
  });
}

export async function restartAgentDaemonLocal(args: {
  workspaceRoot: string;
  agentProjectDir: string;
  daemonId: string;
  resolveShellPath: () => string;
  readAgentSettingsConfig: (args: { workspaceRoot: string }) => Promise<AgentSettingsConfig>;
}): Promise<AgentDaemonSnapshot> {
  const state = await readDaemonState(args.workspaceRoot, args.daemonId);
  if (!state) {
    throw new Error("daemon not found");
  }
  if (isPidAlive(state.runnerPid) || isPidAlive(state.pid)) {
    await stopAgentDaemonLocal(args.workspaceRoot, args.daemonId);
  }
  return await spawnDaemonLocal({
    workspaceRoot: args.workspaceRoot,
    agentProjectDir: args.agentProjectDir,
    daemonId: state.id,
    command: state.command,
    cwd: state.cwd,
    label: state.label,
    envPatch: {},
    resolveShellPath: args.resolveShellPath,
    readAgentSettingsConfig: args.readAgentSettingsConfig,
  });
}

export async function deleteAgentDaemonLocal(workspaceRoot: string, daemonId: string): Promise<void> {
  const state = await readDaemonState(workspaceRoot, daemonId);
  if (!state) {
    return;
  }
  if (isPidAlive(state.runnerPid) || isPidAlive(state.pid)) {
    await stopAgentDaemonLocal(workspaceRoot, daemonId);
  }
  await rm(getDaemonDir(workspaceRoot, daemonId), { recursive: true, force: true });
}

export async function readAgentDaemonLogsLocal(args: {
  workspaceRoot: string;
  daemonId: string;
  limit?: number;
}): Promise<{
  daemon: AgentDaemonSnapshot;
  events: AgentDaemonLogEvent[];
}> {
  const state = await readDaemonState(args.workspaceRoot, args.daemonId);
  if (!state) {
    throw new Error("daemon not found");
  }
  const limit = normalizeLimit(args.limit, 100);
  return {
    daemon: toDaemonSnapshot(state),
    events: await readJsonlTail(getDaemonEventsPath(args.workspaceRoot, args.daemonId), limit),
  };
}

async function executeDaemonRpc(args: {
  workspaceRoot: string;
  agentProjectDir: string;
  request: AgentDaemonRpcRequest;
  resolveShellPath: () => string;
  resolveTaskWorkspace: (cwd: string | null) => string;
  readAgentSettingsConfig: (args: { workspaceRoot: string }) => Promise<AgentSettingsConfig>;
}): Promise<Record<string, unknown>> {
  const action = normalizeAction(args.request.action);
  if (action === "list") {
    return {
      ok: true,
      action,
      daemons: await listAgentDaemonsLocal(args.workspaceRoot),
    };
  }

  if (action === "inspect") {
    const daemonId = normalizeDaemonId(args.request.daemonId);
    return {
      ok: true,
      action,
      daemon: await getAgentDaemonLocal(args.workspaceRoot, daemonId),
    };
  }

  if (action === "start") {
    return {
      ok: true,
      action,
      daemon: await startAgentDaemonLocal({
        workspaceRoot: args.workspaceRoot,
        agentProjectDir: args.agentProjectDir,
        request: args.request,
        resolveShellPath: args.resolveShellPath,
        resolveTaskWorkspace: args.resolveTaskWorkspace,
        readAgentSettingsConfig: args.readAgentSettingsConfig,
      }),
    };
  }

  const daemonId = normalizeDaemonId(args.request.daemonId);
  if (action === "stop") {
    return {
      ok: true,
      action,
      daemon: await stopAgentDaemonLocal(args.workspaceRoot, daemonId),
    };
  }
  if (action === "restart") {
    return {
      ok: true,
      action,
      daemon: await restartAgentDaemonLocal({
        workspaceRoot: args.workspaceRoot,
        agentProjectDir: args.agentProjectDir,
        daemonId,
        resolveShellPath: args.resolveShellPath,
        readAgentSettingsConfig: args.readAgentSettingsConfig,
      }),
    };
  }
  if (action === "delete") {
    await deleteAgentDaemonLocal(args.workspaceRoot, daemonId);
    return {
      ok: true,
      action,
      daemonId,
    };
  }

  const limit = normalizeLimit(args.request.limit, 100);
  return {
    ok: true,
    action,
    ...(await readAgentDaemonLogsLocal({ workspaceRoot: args.workspaceRoot, daemonId, limit })),
    limit,
  };
}

export async function handleDaemonRpcMessage(args: {
  msg: Msg;
  nc: NatsConnection;
  workspaceRoot: string;
  agentProjectDir: string;
  resolveShellPath: () => string;
  resolveTaskWorkspace: (cwd: string | null) => string;
  readAgentSettingsConfig: (args: { workspaceRoot: string }) => Promise<AgentSettingsConfig>;
  onError?: (message: string) => void;
}): Promise<void> {
  let requestId = "unknown";
  try {
    const request = JSON.parse(daemonRpcCodec.decode(args.msg.data)) as AgentDaemonRpcRequest;
    requestId = typeof request.requestId === "string" ? request.requestId : "unknown";
    const payload = await executeDaemonRpc({
      workspaceRoot: args.workspaceRoot,
      agentProjectDir: args.agentProjectDir,
      request,
      resolveShellPath: args.resolveShellPath,
      resolveTaskWorkspace: args.resolveTaskWorkspace,
      readAgentSettingsConfig: args.readAgentSettingsConfig,
    });
    args.msg.respond(daemonRpcCodec.encode(JSON.stringify({ requestId, ...payload })));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    args.onError?.(`daemon rpc failed requestId=${requestId} error=${message}`);
    args.msg.respond(daemonRpcCodec.encode(JSON.stringify({ requestId, ok: false, error: message })));
  }
}

export function subscribeToDaemonRpc(args: {
  nc: NatsConnection;
  subject: string;
  workspaceRoot: string;
  agentProjectDir: string;
  resolveShellPath: () => string;
  resolveTaskWorkspace: (cwd: string | null) => string;
  readAgentSettingsConfig: (args: { workspaceRoot: string }) => Promise<AgentSettingsConfig>;
  onInfo: (message: string) => void;
  onError: (message: string) => void;
}): void {
  args.nc.subscribe(args.subject, {
    callback: (error, msg) => {
      if (error) {
        const message = error instanceof Error ? error.message : String(error);
        args.onError(`daemon rpc subscription error: ${message}`);
        return;
      }
      void handleDaemonRpcMessage({
        msg,
        nc: args.nc,
        workspaceRoot: args.workspaceRoot,
        agentProjectDir: args.agentProjectDir,
        resolveShellPath: args.resolveShellPath,
        resolveTaskWorkspace: args.resolveTaskWorkspace,
        readAgentSettingsConfig: args.readAgentSettingsConfig,
        onError: args.onError,
      });
    },
  });
  args.onInfo(`daemon rpc subscribed subject=${args.subject}`);
}
