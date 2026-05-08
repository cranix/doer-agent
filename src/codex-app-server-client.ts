import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createInterface, type Interface } from "node:readline";

type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
};

export interface CodexAppServerClientOptions {
  cwd: string;
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
  private readonly pending = new Map<number, PendingRequest>();

  constructor(private readonly options: CodexAppServerClientOptions) {}

  async request(method: string, params?: unknown): Promise<unknown> {
    await this.start();
    return await this.requestStarted(method, params);
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
    if (!child || child.killed) {
      return;
    }
    child.kill("SIGTERM");
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
    this.child = spawn("codex", ["app-server", "--listen", "stdio://"], {
      cwd: this.options.cwd,
      env: this.options.env,
      stdio: ["pipe", "pipe", "pipe"],
    });
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

  private async requestStarted(method: string, params?: unknown): Promise<unknown> {
    const child = this.child;
    if (!child || child.killed) {
      throw new Error("Codex app-server is not running");
    }

    const id = this.nextRequestId++;
    const payload = params === undefined ? { id, method } : { id, method, params };
    const timeoutMs = this.options.requestTimeoutMs ?? 30_000;
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
}
