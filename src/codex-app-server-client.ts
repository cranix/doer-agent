import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createRequire } from "node:module";
import path from "node:path";
import { createInterface, type Interface } from "node:readline";

const require = createRequire(import.meta.url);

function resolveCodexCliBinPath(): string {
  const packageJsonPath = require.resolve("@openai/codex/package.json");
  const packageJson = require(packageJsonPath) as { bin?: { codex?: string } };
  const codexBin = packageJson.bin?.codex;
  if (!codexBin) {
    throw new Error("@openai/codex package does not expose a codex binary");
  }
  return path.resolve(path.dirname(packageJsonPath), codexBin);
}

type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
};

export interface CodexAppServerClientOptions {
  cwd: string;
  args: string[];
  env: NodeJS.ProcessEnv;
  requestTimeoutMs?: number;
  onLog?: (message: string) => void;
  onNotification?: (method: string, params: unknown) => void;
}

export class CodexAppServerClient {
  private child: ChildProcessWithoutNullStreams | null = null;
  private stdoutLines: Interface | null = null;
  private nextRequestId = 1;
  private startPromise: Promise<void> | null = null;
  private stopPromise: Promise<void> | null = null;
  private readonly pending = new Map<number, PendingRequest>();

  constructor(private readonly options: CodexAppServerClientOptions) {}

  async request(method: string, params?: unknown, timeoutMs?: number): Promise<unknown> {
    await this.start();
    return await this.requestStarted(method, params, timeoutMs);
  }

  async notify(method: string, params?: unknown): Promise<void> {
    await this.start();
    const child = this.child;
    if (!child || child.killed) {
      throw new Error("Codex app-server is not running");
    }
    const payload = params === undefined ? { method } : { method, params };
    child.stdin.write(`${JSON.stringify(payload)}\n`, "utf8");
  }

  async stop(): Promise<void> {
    const child = this.child;
    if (!child) {
      return;
    }
    if (this.stopPromise) {
      return await this.stopPromise;
    }
    this.stopPromise = this.stopChild(child);
    try {
      await this.stopPromise;
    } finally {
      this.stopPromise = null;
    }
  }

  private async start(): Promise<void> {
    if (this.child && !this.child.killed) {
      return;
    }
    if (this.startPromise) {
      return await this.startPromise;
    }
    this.startPromise = this.startInner();
    try {
      await this.startPromise;
    } finally {
      this.startPromise = null;
    }
  }

  private async startInner(): Promise<void> {
    this.child = spawn(process.execPath, [resolveCodexCliBinPath(), ...this.options.args], {
      cwd: this.options.cwd,
      detached: process.platform !== "win32",
      env: this.options.env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    const childPid = this.child.pid;
    const removeExitHooks = this.registerProcessExitHooks(childPid);
    this.child.stdout.setEncoding("utf8");
    this.child.stderr.setEncoding("utf8");

    this.stdoutLines = createInterface({ input: this.child.stdout });
    this.stdoutLines.on("line", (line) => this.handleLine(line));
    this.child.stderr.on("data", (chunk: string) => {
      const message = chunk.trim();
      if (message) {
        this.options.onLog?.(`[codex-app-server] ${message}`);
      }
    });
    this.child.once("exit", (code, signal) => {
      this.options.onLog?.(`[codex-app-server] exited code=${code ?? "null"} signal=${signal ?? "null"}`);
      removeExitHooks();
      this.signalProcessGroup(childPid, "SIGTERM");
      this.rejectPending(new Error("Codex app-server exited"));
      this.stdoutLines?.close();
      this.stdoutLines = null;
      this.child = null;
    });

    await this.requestStarted("initialize", {
      clientInfo: {
        name: "doer-agent",
        title: "Doer Agent",
        version: "0.5.9",
      },
      capabilities: {
        experimentalApi: true,
      },
    });
    await this.notify("initialized");
  }

  private async requestStarted(method: string, params?: unknown, timeoutMsOverride?: number): Promise<unknown> {
    const child = this.child;
    if (!child || child.killed) {
      throw new Error("Codex app-server is not running");
    }

    const id = this.nextRequestId++;
    const payload = params === undefined ? { id, method } : { id, method, params };
    const timeoutMs = timeoutMsOverride ?? this.options.requestTimeoutMs ?? 30_000;
    return await new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Timed out waiting for Codex app-server response: ${method}`));
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      child.stdin.write(`${JSON.stringify(payload)}\n`, "utf8", (error) => {
        if (!error) {
          return;
        }
        clearTimeout(timer);
        this.pending.delete(id);
        reject(error);
      });
    });
  }

  private handleLine(line: string): void {
    let message: unknown;
    try {
      message = JSON.parse(line);
    } catch {
      this.options.onLog?.("[codex-app-server] ignored malformed JSON line");
      return;
    }
    if (!message || typeof message !== "object" || Array.isArray(message)) {
      return;
    }
    const record = message as Record<string, unknown>;
    if (record.id !== undefined) {
      const id = Number(record.id);
      const pending = Number.isInteger(id) ? this.pending.get(id) : null;
      if (!pending) {
        return;
      }
      this.pending.delete(id);
      clearTimeout(pending.timer);
      if (record.error && typeof record.error === "object" && !Array.isArray(record.error)) {
        const error = record.error as Record<string, unknown>;
        pending.reject(new Error(typeof error.message === "string" ? error.message : "Codex app-server request failed"));
      } else {
        pending.resolve(record.result);
      }
      return;
    }
    if (typeof record.method === "string") {
      this.options.onNotification?.(record.method, record.params);
    }
  }

  private rejectPending(error: Error): void {
    for (const [id, pending] of this.pending) {
      this.pending.delete(id);
      clearTimeout(pending.timer);
      pending.reject(error);
    }
  }

  private async stopChild(child: ChildProcessWithoutNullStreams): Promise<void> {
    const pid = child.pid;
    if (!pid) {
      child.kill("SIGTERM");
      return;
    }

    this.signalProcessGroup(pid, "SIGTERM") || child.kill("SIGTERM");
    const exited = await this.waitForExit(child, 5_000);
    if (!exited) {
      this.options.onLog?.("[codex-app-server] forcing process group shutdown after timeout");
      this.signalProcessGroup(pid, "SIGKILL") || child.kill("SIGKILL");
      await this.waitForExit(child, 1_000);
    } else {
      this.signalProcessGroup(pid, "SIGTERM");
    }
  }

  private registerProcessExitHooks(pid: number | undefined): () => void {
    if (!pid || process.platform === "win32") {
      return () => {};
    }
    let removed = false;
    const cleanup = () => {
      this.signalProcessGroup(pid, "SIGTERM");
    };
    const signalHandlers = new Map<NodeJS.Signals, () => void>();
    const remove = () => {
      if (removed) {
        return;
      }
      removed = true;
      process.off("exit", cleanup);
      for (const [signal, handler] of signalHandlers) {
        process.off(signal, handler);
      }
    };
    process.once("exit", cleanup);
    for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"] as NodeJS.Signals[]) {
      const handler = () => {
        cleanup();
        remove();
        try {
          process.kill(process.pid, signal);
        } catch {
          process.exitCode = 1;
        }
      };
      signalHandlers.set(signal, handler);
      process.once(signal, handler);
    }
    return remove;
  }

  private signalProcessGroup(pid: number | undefined, signal: NodeJS.Signals): boolean {
    if (!pid || process.platform === "win32") {
      return false;
    }
    try {
      process.kill(-pid, signal);
      return true;
    } catch (error) {
      const code = typeof error === "object" && error !== null && "code" in error
        ? String((error as { code?: unknown }).code)
        : "";
      if (code && code !== "ESRCH") {
        this.options.onLog?.(`[codex-app-server] failed to signal process group pid=${pid} signal=${signal} code=${code}`);
      }
      return false;
    }
  }

  private async waitForExit(child: ChildProcessWithoutNullStreams, timeoutMs: number): Promise<boolean> {
    if (child.exitCode !== null || child.signalCode !== null) {
      return true;
    }
    return await new Promise<boolean>((resolve) => {
      const timer = setTimeout(() => {
        cleanup();
        resolve(false);
      }, timeoutMs);
      const onExit = () => {
        cleanup();
        resolve(true);
      };
      const cleanup = () => {
        clearTimeout(timer);
        child.off("exit", onExit);
      };
      child.once("exit", onExit);
    });
  }
}
