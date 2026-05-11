import { execFile } from "node:child_process";
import { spawn } from "node:child_process";
import { promisify } from "node:util";
import { StringCodec, type Msg, type NatsConnection } from "nats";
import { resolveAgentVersion } from "./agent-runtime-utils.js";

const execFileAsync = promisify(execFile);
const maintenanceRpcCodec = StringCodec();

type AgentMaintenanceAction = "info" | "update/check" | "update/restart";

interface AgentMaintenanceRpcRequest {
  requestId?: unknown;
  action?: unknown;
}

interface AgentMaintenanceInfo {
  currentVersion: string;
  latestVersion: string | null;
  updateAvailable: boolean | null;
  packageName: string;
  nodeVersion: string;
  command: string | null;
  updateCommand: string | null;
  cwd: string;
  startedAt: string;
  canRestartToLatest: boolean;
}

function normalizeAction(value: unknown): AgentMaintenanceAction {
  if (value === "info" || value === "update/check" || value === "update/restart") {
    return value;
  }
  throw new Error("unsupported action");
}

function parseSemver(value: string): number[] | null {
  const match = value.trim().match(/^v?(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$/);
  if (!match) {
    return null;
  }
  return [Number(match[1]), Number(match[2]), Number(match[3])];
}

function compareSemver(a: string, b: string): number | null {
  const left = parseSemver(a);
  const right = parseSemver(b);
  if (!left || !right) {
    return null;
  }
  for (let index = 0; index < 3; index += 1) {
    if (left[index] !== right[index]) {
      return left[index] - right[index];
    }
  }
  return 0;
}

async function resolveLatestPackageVersion(): Promise<string | null> {
  try {
    const { stdout } = await execFileAsync("npm", ["view", "doer-agent", "version"], {
      timeout: 15_000,
      maxBuffer: 1024 * 64,
    });
    const version = stdout.trim().split(/\s+/)[0]?.trim() || "";
    return version || null;
  } catch {
    return null;
  }
}

function resolveUpdateCommand(command: string | undefined): string | null {
  const rawCommand = command?.trim() || "";
  if (!rawCommand) {
    return null;
  }
  const replaced = rawCommand.replace(
    /(^|\s)npx(\s+-y)?\s+(?:doer-agent(?:@[^\s]+)?|\.)\b/,
    (_match, prefix: string, yesFlag: string | undefined) => `${prefix}npx${yesFlag ?? " -y"} doer-agent@latest`,
  );
  return replaced !== rawCommand ? replaced : null;
}

function shellQuote(value: string): string {
  if (/^[A-Za-z0-9_./:=@+-]+$/.test(value)) {
    return value;
  }
  return `'${value.replace(/'/g, "'\\''")}'`;
}

function buildFallbackUpdateCommand(): string {
  const workspace = process.env.WORKSPACE?.trim() || process.cwd();
  const args = process.argv.slice(2).map(shellQuote).join(" ");
  return [`WORKSPACE=${shellQuote(workspace)}`, "npx -y doer-agent@latest", args].filter(Boolean).join(" ");
}

async function resolveMaintenanceInfo(agentPackageJsonPath: string): Promise<AgentMaintenanceInfo> {
  const currentVersion = await resolveAgentVersion(agentPackageJsonPath);
  const latestVersion = await resolveLatestPackageVersion();
  const updateComparison = latestVersion ? compareSemver(currentVersion, latestVersion) : null;
  const launchCommand = process.env.DOER_AGENT_LAUNCH_COMMAND?.trim() || process.env.DOER_DAEMON_COMMAND?.trim() || "";
  const updateCommand = resolveUpdateCommand(launchCommand) ?? buildFallbackUpdateCommand();
  return {
    currentVersion,
    latestVersion,
    updateAvailable: updateComparison === null ? null : updateComparison < 0,
    packageName: "doer-agent",
    nodeVersion: process.version,
    command: launchCommand || updateCommand,
    updateCommand,
    cwd: process.env.DOER_AGENT_LAUNCH_CWD?.trim() || process.env.DOER_DAEMON_CWD?.trim() || process.cwd(),
    startedAt: new Date(Number(process.env.DOER_AGENT_STARTED_AT_MS || Date.now())).toISOString(),
    canRestartToLatest: Boolean(updateCommand),
  };
}

function scheduleRestartToLatest(command: string, cwd: string): void {
  setTimeout(() => {
    const env: NodeJS.ProcessEnv = { ...process.env };
    delete env.DOER_DAEMON_ID;
    delete env.DOER_DAEMON_COMMAND;
    delete env.DOER_DAEMON_CWD;
    delete env.DOER_DAEMON_STATE_PATH;
    delete env.DOER_DAEMON_EVENTS_PATH;
    delete env.DOER_DAEMON_SHELL_PATH;
    delete env.DOER_AGENT_LAUNCH_COMMAND;
    delete env.DOER_AGENT_LAUNCH_CWD;
    delete env.DOER_AGENT_LAUNCH_SHELL;

    const child = spawn(command, {
      cwd,
      env,
      shell: process.env.DOER_AGENT_LAUNCH_SHELL || process.env.DOER_DAEMON_SHELL_PATH || process.env.SHELL || true,
      detached: true,
      stdio: "ignore",
    });
    child.unref();
    setTimeout(() => {
      process.exit(0);
    }, 250);
  }, 250);
}

function publicMaintenanceInfo(info: AgentMaintenanceInfo): AgentMaintenanceInfo {
  return {
    ...info,
    command: null,
    updateCommand: null,
  };
}

export async function handleMaintenanceRpcMessage(args: {
  msg: Msg;
  nc: NatsConnection;
  agentPackageJsonPath: string;
  onError?: (message: string) => void;
}): Promise<void> {
  let requestId = "unknown";
  try {
    const request = JSON.parse(maintenanceRpcCodec.decode(args.msg.data)) as AgentMaintenanceRpcRequest;
    requestId = typeof request.requestId === "string" ? request.requestId : "unknown";
    const action = normalizeAction(request.action);
    const info = await resolveMaintenanceInfo(args.agentPackageJsonPath);
    if (action === "update/restart") {
      if (!info.updateCommand) {
        throw new Error("agent was not started with a supported npx doer-agent command");
      }
      scheduleRestartToLatest(info.updateCommand, info.cwd);
    }
    args.msg.respond(maintenanceRpcCodec.encode(JSON.stringify({
      requestId,
      ok: true,
      action,
      info: publicMaintenanceInfo(info),
      restartScheduled: action === "update/restart",
    })));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    args.onError?.(`maintenance rpc failed requestId=${requestId} error=${message}`);
    args.msg.respond(maintenanceRpcCodec.encode(JSON.stringify({ requestId, ok: false, error: message })));
  }
}

export function subscribeToMaintenanceRpc(args: {
  nc: NatsConnection;
  subject: string;
  agentPackageJsonPath: string;
  onInfo: (message: string) => void;
  onError: (message: string) => void;
}): void {
  args.nc.subscribe(args.subject, {
    callback: (error, msg) => {
      if (error) {
        const message = error instanceof Error ? error.message : String(error);
        args.onError(`maintenance rpc subscription error: ${message}`);
        return;
      }
      void handleMaintenanceRpcMessage({
        msg,
        nc: args.nc,
        agentPackageJsonPath: args.agentPackageJsonPath,
        onError: args.onError,
      });
    },
  });
  args.onInfo(`maintenance rpc subscribed subject=${args.subject}`);
}
