import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import type { AgentMcpServerConfig, CodexPersonality } from "./agent-settings.js";

const require = createRequire(import.meta.url);
const ANSI_RE = /\u001b\[[0-9;]*m/g;

export interface ShellRpcCodexAuthBundle {
  taskId?: string;
  authMode?: "api_key" | "chatgpt";
  issuedAt?: string;
  expiresAt?: string;
  authJson?: string;
  apiKey?: string | null;
}

function shellSingleQuote(value: string): string {
  return `'${value.replace(/'/g, `'"'"'`)}'`;
}

function toTomlStringLiteral(value: string): string {
  return `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

function toTomlStringArray(values: string[]): string {
  return `[${values.map((value) => toTomlStringLiteral(value)).join(", ")}]`;
}

function toTomlStringMap(values: Record<string, string>): string {
  return `{ ${Object.entries(values)
    .map(([key, value]) => `${toTomlStringLiteral(key)} = ${toTomlStringLiteral(value)}`)
    .join(", ")} }`;
}

function buildMcpServerConfigArgs(args: {
  serverName: string;
  command: string;
  commandArgs: string[];
  env?: Record<string, string>;
  enabled?: boolean;
}): string[] {
  const serverName = args.serverName.trim();
  const configArgs = [
    "--config",
    `mcp_servers.${serverName}.command=${toTomlStringLiteral(args.command)}`,
    "--config",
    `mcp_servers.${serverName}.args=${toTomlStringArray(args.commandArgs)}`,
    "--config",
    `mcp_servers.${serverName}.enabled=${args.enabled === false ? "false" : "true"}`,
  ];
  for (const [key, value] of Object.entries(args.env ?? {})) {
    configArgs.push("--config", `mcp_servers.${serverName}.env.${key}=${toTomlStringLiteral(value)}`);
  }
  return configArgs;
}

function buildRemoteMcpServerConfigArgs(args: {
  serverName: string;
  url: string;
  auth?: "oauth" | "chatgpt";
  bearerTokenEnvVar?: string;
  scopes?: string[];
  oauthResource?: string;
  httpHeaders?: Record<string, string>;
  envHttpHeaders?: Record<string, string>;
  enabled?: boolean;
}): string[] {
  const prefix = `mcp_servers.${args.serverName.trim()}`;
  const configArgs = [
    "--config",
    `${prefix}.url=${toTomlStringLiteral(args.url)}`,
    "--config",
    `${prefix}.enabled=${args.enabled === false ? "false" : "true"}`,
  ];
  if (args.auth) {
    configArgs.push("--config", `${prefix}.auth=${toTomlStringLiteral(args.auth)}`);
  }
  if (args.bearerTokenEnvVar?.trim()) {
    configArgs.push("--config", `${prefix}.bearer_token_env_var=${toTomlStringLiteral(args.bearerTokenEnvVar.trim())}`);
  }
  if (Object.keys(args.httpHeaders ?? {}).length > 0) {
    configArgs.push("--config", `${prefix}.http_headers=${toTomlStringMap(args.httpHeaders ?? {})}`);
  }
  if (Object.keys(args.envHttpHeaders ?? {}).length > 0) {
    configArgs.push("--config", `${prefix}.env_http_headers=${toTomlStringMap(args.envHttpHeaders ?? {})}`);
  }
  if ((args.scopes ?? []).length > 0) {
    configArgs.push("--config", `${prefix}.scopes=${toTomlStringArray(args.scopes ?? [])}`);
  }
  if (args.oauthResource?.trim()) {
    configArgs.push("--config", `${prefix}.oauth_resource=${toTomlStringLiteral(args.oauthResource.trim())}`);
  }
  return configArgs;
}

function hasDirectCodexBinary(): boolean {
  const result = spawnSync("bash", ["-lc", "command -v codex >/dev/null 2>&1"], {
    stdio: "ignore",
  });
  return result.status === 0;
}

function resolveBundledCodexCliBinPath(): string | null {
  try {
    const packageJsonPath = require.resolve("@openai/codex/package.json");
    const packageJson = require(packageJsonPath) as { bin?: { codex?: string } };
    const codexBin = packageJson.bin?.codex;
    if (!codexBin) {
      return null;
    }
    return path.resolve(path.dirname(packageJsonPath), codexBin);
  } catch {
    return null;
  }
}

export function stripAnsi(value: string): string {
  return value.replace(ANSI_RE, "");
}

export function normalizeCodexModel(value: unknown): string {
  const normalized = typeof value === "string" ? value.trim() : "";
  return normalized || "gpt-5.5";
}

export function buildManagedCodexArgs(args: {
  prompt: string;
  imagePaths: string[];
  sessionId: string | null;
  model: string;
  personality?: CodexPersonality | null;
  modelInstructionsFile?: string | null;
  configOverrides?: string[];
}): string[] {
  const promptArgs = ["--", args.prompt];
  const fixedArgs = ["--dangerously-bypass-approvals-and-sandbox"];
  const configArgs = [
    ...(args.personality ? ["--config", `personality=${toTomlStringLiteral(args.personality)}`] : []),
    ...(args.modelInstructionsFile
      ? ["--config", `model_instructions_file=${toTomlStringLiteral(args.modelInstructionsFile)}`]
      : []),
    ...(args.configOverrides ?? []),
  ];
  const imageArgs = args.imagePaths.flatMap((imagePath) => ["--image", imagePath]);
  return [
    ...fixedArgs,
    ...configArgs,
    "--model",
    args.model,
    ...(args.sessionId
      ? ["exec", "resume", ...imageArgs, args.sessionId, ...promptArgs]
      : ["exec", ...imageArgs, ...promptArgs]),
  ];
}

export function buildDaemonMcpConfigArgs(args: {
  agentProjectDir: string;
  workspaceRoot: string;
  serverName?: string;
}): string[] {
  return buildWorkspaceMcpConfigArgs({
    agentProjectDir: args.agentProjectDir,
    workspaceRoot: args.workspaceRoot,
    serverName: args.serverName?.trim() || "doer_daemon",
    distEntryRelativePath: path.join("dist", "daemon-mcp-server.js"),
    srcEntryRelativePath: path.join("src", "daemon-mcp-server.ts"),
    workspaceRootEnvName: "DOER_DAEMON_WORKSPACE_ROOT",
  });
}

export function buildThreadsMcpConfigArgs(args: {
  agentId: string;
  agentProjectDir: string;
  agentToken: string;
  serverBaseUrl: string;
  userId: string;
  workspaceRoot: string;
  serverName?: string;
}): string[] {
  return buildWorkspaceMcpConfigArgs({
    agentProjectDir: args.agentProjectDir,
    workspaceRoot: args.workspaceRoot,
    serverName: args.serverName?.trim() || "doer_threads",
    distEntryRelativePath: path.join("dist", "threads-mcp-server.js"),
    srcEntryRelativePath: path.join("src", "threads-mcp-server.ts"),
    workspaceRootEnvName: "DOER_THREADS_WORKSPACE_ROOT",
    env: {
      DOER_THREADS_AGENT_ID: args.agentId,
      DOER_AGENT_TOKEN: args.agentToken,
      DOER_THREADS_SERVER_BASE_URL: args.serverBaseUrl,
      DOER_THREADS_USER_ID: args.userId,
    },
  });
}

export function buildMobileMcpConfigArgs(args: {
  agentId: string;
  agentProjectDir: string;
  agentToken: string;
  serverBaseUrl: string;
  userId: string;
  workspaceRoot: string;
  serverName?: string;
}): string[] {
  return buildWorkspaceMcpConfigArgs({
    agentProjectDir: args.agentProjectDir,
    workspaceRoot: args.workspaceRoot,
    serverName: args.serverName?.trim() || "doer_mobile",
    distEntryRelativePath: path.join("dist", "mobile-mcp-server.js"),
    srcEntryRelativePath: path.join("src", "mobile-mcp-server.ts"),
    workspaceRootEnvName: "DOER_MOBILE_WORKSPACE_ROOT",
    env: {
      DOER_MOBILE_SERVER_BASE_URL: args.serverBaseUrl,
      DOER_MOBILE_USER_ID: args.userId,
      DOER_MOBILE_AGENT_ID: args.agentId,
      DOER_AGENT_TOKEN: args.agentToken,
    },
  });
}

export function buildCustomMcpConfigArgs(servers: AgentMcpServerConfig[]): string[] {
  const configArgs: string[] = [];
  const reservedNames = new Set(["doer_daemon", "doer_mobile", "doer_threads"]);
  const seenNames = new Set<string>();
  for (const server of servers) {
    const serverName = server.name.trim();
    const hasEndpoint = server.transport === "streamable_http" ? server.url.trim() : server.command.trim();
    if (!server.enabled || !serverName || !hasEndpoint || reservedNames.has(serverName) || seenNames.has(serverName)) {
      continue;
    }
    seenNames.add(serverName);
    if (server.transport === "streamable_http") {
      configArgs.push(
        ...buildRemoteMcpServerConfigArgs({
          serverName,
          url: server.url,
          auth: server.auth,
          bearerTokenEnvVar: server.bearerTokenEnvVar,
          scopes: server.scopes,
          oauthResource: server.oauthResource,
          httpHeaders: Object.fromEntries(server.httpHeaders.map((header) => [header.key, header.value])),
          envHttpHeaders: Object.fromEntries(server.envHttpHeaders.map((header) => [header.key, header.value])),
          enabled: true,
        }),
      );
      continue;
    }
    configArgs.push(
      ...buildMcpServerConfigArgs({
        serverName,
        command: server.command,
        commandArgs: server.args,
        env: Object.fromEntries(server.env.map((variable) => [variable.key, variable.value])),
        enabled: true,
      }),
    );
  }
  return configArgs;
}

function buildWorkspaceMcpConfigArgs(args: {
  agentProjectDir: string;
  workspaceRoot: string;
  serverName: string;
  distEntryRelativePath: string;
  srcEntryRelativePath: string;
  workspaceRootEnvName: string;
  env?: Record<string, string>;
}): string[] {
  const serverName = args.serverName.trim();
  const distEntry = path.join(args.agentProjectDir, args.distEntryRelativePath);
  const srcEntry = path.join(args.agentProjectDir, args.srcEntryRelativePath);
  const tsxLoaderPath = path.join(args.agentProjectDir, "node_modules", "tsx", "dist", "loader.mjs");
  const command = process.execPath;
  const commandArgs = existsSync(distEntry)
    ? [distEntry, "--workspace-root", args.workspaceRoot]
    : ["--import", tsxLoaderPath, srcEntry, "--workspace-root", args.workspaceRoot];
  return buildMcpServerConfigArgs({
    serverName,
    command,
    commandArgs,
    env: {
      [args.workspaceRootEnvName]: args.workspaceRoot,
      ...(args.env ?? {}),
    },
    enabled: true,
  });
}

export function buildLocalCodexCliCommand(args: string[]): string {
  const quotedArgs = args.map(shellSingleQuote).join(" ");
  const bundledCodex = resolveBundledCodexCliBinPath();
  if (bundledCodex) {
    return `exec ${shellSingleQuote(process.execPath)} ${shellSingleQuote(bundledCodex)} ${quotedArgs}`;
  }
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

export function spawnManagedCodexCommand(args: {
  codexArgs: string[];
  taskWorkspace: string;
  env: NodeJS.ProcessEnv;
  agentToken: string;
}): ReturnType<typeof spawn> {
  const env = {
    ...args.env,
    DOER_AGENT_TOKEN: args.agentToken,
  };
  const bundledCodex = resolveBundledCodexCliBinPath();
  const child = bundledCodex
    ? spawn(process.execPath, [bundledCodex, ...args.codexArgs], {
      cwd: args.taskWorkspace,
      detached: process.platform !== "win32",
      env,
      stdio: ["ignore", "pipe", "pipe"],
    })
    : hasDirectCodexBinary()
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

export function createLocalCodexCliTools(args: {
  resolveWorkspaceRoot: () => string;
  resolveCodexHomePath: () => string;
  resolveShellPath: () => string;
  sendSignalToTaskProcess: (child: ChildProcess, signal: NodeJS.Signals) => void;
}): {
  buildLocalCodexCliCommand: typeof buildLocalCodexCliCommand;
  runLocalCodexCli: (cmdArgs: string[], timeoutMs: number, envPatch?: Record<string, string>) => Promise<{
    code: number | null;
    stdout: string;
    stderr: string;
    timedOut: boolean;
  }>;
  runLocalCodexCliWithInput: (
    cmdArgs: string[],
    input: string,
    timeoutMs: number,
    envPatch?: Record<string, string>,
  ) => Promise<{
    code: number | null;
    stdout: string;
    stderr: string;
    timedOut: boolean;
  }>;
  stripAnsi: typeof stripAnsi;
} {
  async function runLocalCodexCli(
    cmdArgs: string[],
    timeoutMs: number,
    envPatch?: Record<string, string>,
  ): Promise<{
    code: number | null;
    stdout: string;
    stderr: string;
    timedOut: boolean;
  }> {
    const command = buildLocalCodexCliCommand(cmdArgs);
    const workspaceRoot = args.resolveWorkspaceRoot();
    const env: NodeJS.ProcessEnv = {
      ...process.env,
      ...(envPatch ?? {}),
      WORKSPACE: workspaceRoot,
      CODEX_HOME: args.resolveCodexHomePath(),
    };

    return await new Promise((resolve, reject) => {
      const child = spawn(command, {
        cwd: workspaceRoot,
        shell: args.resolveShellPath(),
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
        args.sendSignalToTaskProcess(child, "SIGTERM");
        setTimeout(() => args.sendSignalToTaskProcess(child, "SIGKILL"), 1000);
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

  async function runLocalCodexCliWithInput(
    cmdArgs: string[],
    input: string,
    timeoutMs: number,
    envPatch?: Record<string, string>,
  ): Promise<{
    code: number | null;
    stdout: string;
    stderr: string;
    timedOut: boolean;
  }> {
    const command = buildLocalCodexCliCommand(cmdArgs);
    const workspaceRoot = args.resolveWorkspaceRoot();
    const env: NodeJS.ProcessEnv = {
      ...process.env,
      ...(envPatch ?? {}),
      WORKSPACE: workspaceRoot,
      CODEX_HOME: args.resolveCodexHomePath(),
    };

    return await new Promise((resolve, reject) => {
      const child = spawn(command, {
        cwd: workspaceRoot,
        shell: args.resolveShellPath(),
        env,
        stdio: ["pipe", "pipe", "pipe"],
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

      child.stdin?.write(input);
      if (!input.endsWith("\n")) {
        child.stdin?.write("\n");
      }
      child.stdin?.end();

      const timer = setTimeout(() => {
        timedOut = true;
        args.sendSignalToTaskProcess(child, "SIGTERM");
        setTimeout(() => args.sendSignalToTaskProcess(child, "SIGKILL"), 1000);
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

  return {
    buildLocalCodexCliCommand,
    runLocalCodexCli,
    runLocalCodexCliWithInput,
    stripAnsi,
  };
}

export function normalizeShellRpcCodexAuthBundle(value: unknown): ShellRpcCodexAuthBundle | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const row = value as Record<string, unknown>;
  const authJson = typeof row.authJson === "string" ? row.authJson : null;
  const authMode = row.authMode === "chatgpt" ? "chatgpt" : row.authMode === "api_key" ? "api_key" : undefined;
  const apiKey = typeof row.apiKey === "string" || row.apiKey === null ? row.apiKey : undefined;
  if (!authJson && authMode !== "api_key" && apiKey === undefined) {
    return null;
  }
  return {
    taskId: typeof row.taskId === "string" ? row.taskId : undefined,
    authMode,
    issuedAt: typeof row.issuedAt === "string" ? row.issuedAt : undefined,
    expiresAt: typeof row.expiresAt === "string" ? row.expiresAt : undefined,
    authJson: authJson ?? undefined,
    apiKey,
  };
}
