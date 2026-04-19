import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import path from "node:path";
import type { CodexPersonality } from "./agent-settings.js";

const ANSI_RE = /\u001b\[[0-9;]*m/g;

export interface ShellRpcCodexAuthBundle {
  taskId?: string;
  authMode?: "api_key" | "oauth";
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

function hasDirectCodexBinary(): boolean {
  const result = spawnSync("bash", ["-lc", "command -v codex >/dev/null 2>&1"], {
    stdio: "ignore",
  });
  return result.status === 0;
}

export function stripAnsi(value: string): string {
  return value.replace(ANSI_RE, "");
}

export function normalizeCodexModel(value: unknown): string {
  const normalized = typeof value === "string" ? value.trim() : "";
  return normalized || "gpt-5.4";
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
  const serverName = args.serverName?.trim() || "doer_daemon";
  const distEntry = path.join(args.agentProjectDir, "dist", "daemon-mcp-server.js");
  const srcEntry = path.join(args.agentProjectDir, "src", "daemon-mcp-server.ts");
  const tsxLoaderPath = path.join(args.agentProjectDir, "node_modules", "tsx", "dist", "loader.mjs");
  const command = process.execPath;
  const commandArgs = existsSync(distEntry)
    ? [distEntry, "--workspace-root", args.workspaceRoot]
    : ["--import", tsxLoaderPath, srcEntry, "--workspace-root", args.workspaceRoot];
  return [
    "--config",
    `mcp_servers.${serverName}.command=${toTomlStringLiteral(command)}`,
    "--config",
    `mcp_servers.${serverName}.args=${toTomlStringArray(commandArgs)}`,
    "--config",
    `mcp_servers.${serverName}.env.DOER_DAEMON_WORKSPACE_ROOT=${toTomlStringLiteral(args.workspaceRoot)}`,
    "--config",
    `mcp_servers.${serverName}.enabled=true`,
  ];
}

export function buildLocalCodexCliCommand(args: string[]): string {
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
  const authMode = row.authMode === "oauth" ? "oauth" : row.authMode === "api_key" ? "api_key" : undefined;
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
