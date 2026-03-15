import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdir, rm } from "node:fs/promises";
import net, { type Socket } from "node:net";
import path from "node:path";

function parseInteger(value: string | undefined, fallback: number): number {
  const normalized = value?.trim();
  if (!normalized) {
    return fallback;
  }
  const parsed = Number.parseInt(normalized, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

const socketPath = process.env.DOER_PLAYWRIGHT_MCP_DAEMON_SOCKET?.trim() || "";
const targetCommand = process.env.DOER_PLAYWRIGHT_MCP_TARGET_COMMAND?.trim() || "";
const idleTtlSeconds = parseInteger(process.env.DOER_PLAYWRIGHT_MCP_DAEMON_IDLE_TTL_SECONDS, 10800);
let targetArgs: string[] = [];
try {
  const parsed = JSON.parse(process.env.DOER_PLAYWRIGHT_MCP_TARGET_ARGS_JSON || "[]");
  if (Array.isArray(parsed) && parsed.every((item) => typeof item === "string")) {
    targetArgs = parsed;
  }
} catch {
  targetArgs = [];
}

let targetEnvPatch: Record<string, string> = {};
try {
  const parsed = JSON.parse(process.env.DOER_PLAYWRIGHT_MCP_TARGET_ENV_JSON || "{}");
  if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
    targetEnvPatch = Object.fromEntries(
      Object.entries(parsed).flatMap(([key, value]) => (typeof value === "string" ? [[key, value] as [string, string]] : [])),
    );
  }
} catch {
  targetEnvPatch = {};
}

if (!socketPath) {
  process.stderr.write("playwright-mcp-daemon error: DOER_PLAYWRIGHT_MCP_DAEMON_SOCKET is required\n");
  process.exit(1);
}
if (!targetCommand) {
  process.stderr.write("playwright-mcp-daemon error: DOER_PLAYWRIGHT_MCP_TARGET_COMMAND is required\n");
  process.exit(1);
}

let child: ChildProcessWithoutNullStreams | null = null;
let activeClient: Socket | null = null;
let shuttingDown = false;
let lastActivityAt = Date.now();
const idleTtlMs = idleTtlSeconds * 1000;

function markActivity(): void {
  lastActivityAt = Date.now();
}

function spawnTargetIfNeeded(): void {
  if (child) {
    return;
  }
  child = spawn(targetCommand, targetArgs, {
    stdio: ["pipe", "pipe", "pipe"],
    env: { ...process.env, ...targetEnvPatch },
  });
  child.stdout.on("data", (chunk: Buffer) => {
    markActivity();
    if (activeClient && !activeClient.destroyed) {
      activeClient.write(chunk);
    }
  });
  child.stderr.on("data", (chunk: Buffer) => {
    process.stderr.write(chunk);
  });
  child.on("exit", () => {
    child = null;
    if (activeClient && !activeClient.destroyed) {
      activeClient.end();
      activeClient = null;
    }
    markActivity();
  });
}

async function shutdown(server: net.Server): Promise<void> {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  if (activeClient && !activeClient.destroyed) {
    activeClient.destroy();
    activeClient = null;
  }
  if (child) {
    child.kill("SIGTERM");
    await new Promise<void>((resolve) => {
      const timeout = setTimeout(() => {
        if (child) {
          child.kill("SIGKILL");
        }
        resolve();
      }, 2000);
      timeout.unref?.();
      child?.once("exit", () => {
        clearTimeout(timeout);
        resolve();
      });
    });
    child = null;
  }
  await new Promise<void>((resolve) => server.close(() => resolve()));
  if (existsSync(socketPath)) {
    await rm(socketPath, { force: true });
  }
  process.exit(0);
}

async function main(): Promise<void> {
  await mkdir(path.dirname(socketPath), { recursive: true });
  if (existsSync(socketPath)) {
    await rm(socketPath, { force: true });
  }

  const server = net.createServer((socket) => {
    if (activeClient && !activeClient.destroyed) {
      socket.end();
      return;
    }
    activeClient = socket;
    markActivity();
    spawnTargetIfNeeded();

    socket.on("data", (chunk: Buffer) => {
      markActivity();
      spawnTargetIfNeeded();
      if (child && !child.killed) {
        child.stdin.write(chunk);
      }
    });

    socket.on("close", () => {
      if (activeClient === socket) {
        activeClient = null;
      }
      markActivity();
    });

    socket.on("error", () => {
      if (activeClient === socket) {
        activeClient = null;
      }
      markActivity();
    });
  });

  server.on("error", (error) => {
    process.stderr.write(`playwright-mcp-daemon error: ${error instanceof Error ? error.message : String(error)}\n`);
    process.exit(1);
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, () => {
      server.off("error", reject);
      resolve();
    });
  });

  const interval = setInterval(() => {
    if (shuttingDown) {
      return;
    }
    if (activeClient && !activeClient.destroyed) {
      return;
    }
    if (Date.now() - lastActivityAt < idleTtlMs) {
      return;
    }
    void shutdown(server);
  }, 30000);
  interval.unref?.();

  process.on("SIGTERM", () => {
    void shutdown(server);
  });
  process.on("SIGINT", () => {
    void shutdown(server);
  });
}

void main();
