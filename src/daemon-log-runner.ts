import { spawn, type ChildProcess } from "node:child_process";
import { appendFile, readFile, writeFile } from "node:fs/promises";

type RunnerEventType = "start" | "stdout" | "stderr" | "exit" | "signal" | "error";

interface RunnerStateRecord {
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

function readRequiredEnv(name: string): string {
  const value = process.env[name]?.trim() || "";
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

async function readState(statePath: string): Promise<RunnerStateRecord> {
  const raw = await readFile(statePath, "utf8");
  return JSON.parse(raw) as RunnerStateRecord;
}

async function writeState(statePath: string, state: RunnerStateRecord): Promise<void> {
  await writeFile(statePath, `${JSON.stringify(state, null, 2)}\n`, "utf8");
}

async function appendEvent(
  eventsPath: string,
  event: {
    type: RunnerEventType;
    text?: string | null;
    pid?: number | null;
    code?: number | null;
    signal?: string | null;
  },
): Promise<void> {
  const row = {
    ts: new Date().toISOString(),
    type: event.type,
    text: event.text ?? null,
    pid: typeof event.pid === "number" && event.pid > 0 ? event.pid : null,
    code: typeof event.code === "number" ? event.code : null,
    signal: event.signal?.trim() || null,
  };
  await appendFile(eventsPath, `${JSON.stringify(row)}\n`, "utf8");
}

function attachLineLogger(
  stream: NodeJS.ReadableStream | null | undefined,
  type: "stdout" | "stderr",
  eventsPath: string,
  pid: number,
): void {
  if (!stream) {
    return;
  }
  stream.setEncoding("utf8");
  let pending = "";
  stream.on("data", (chunk: string) => {
    pending += chunk;
    const lines = pending.split(/\r\n|\n|\r/);
    pending = lines.pop() ?? "";
    for (const line of lines) {
      void appendEvent(eventsPath, {
        type,
        text: line,
        pid,
      });
    }
  });
  stream.on("end", () => {
    if (!pending) {
      return;
    }
    void appendEvent(eventsPath, {
      type,
      text: pending,
      pid,
    });
    pending = "";
  });
}

async function main(): Promise<void> {
  const statePath = readRequiredEnv("DOER_DAEMON_STATE_PATH");
  const eventsPath = readRequiredEnv("DOER_DAEMON_EVENTS_PATH");
  const command = readRequiredEnv("DOER_DAEMON_COMMAND");
  const cwd = readRequiredEnv("DOER_DAEMON_CWD");
  const shellPath = readRequiredEnv("DOER_DAEMON_SHELL_PATH");

  const childEnv: NodeJS.ProcessEnv = { ...process.env };
  delete childEnv.DOER_DAEMON_STATE_PATH;
  delete childEnv.DOER_DAEMON_EVENTS_PATH;
  delete childEnv.DOER_DAEMON_COMMAND;
  delete childEnv.DOER_DAEMON_CWD;
  delete childEnv.DOER_DAEMON_SHELL_PATH;

  let state = await readState(statePath);
  state = {
    ...state,
    runnerPid: process.pid,
  };
  await writeState(statePath, state);

  const child = spawn(command, {
    cwd,
    env: childEnv,
    shell: shellPath,
    detached: false,
    stdio: ["ignore", "pipe", "pipe"],
  });
  if (typeof child.pid !== "number" || child.pid <= 0) {
    throw new Error("failed to spawn daemon child");
  }

  state = {
    ...state,
    pid: child.pid,
    stoppedAt: null,
    lastExitCode: null,
  };
  await writeState(statePath, state);
  await appendEvent(eventsPath, {
    type: "start",
    pid: child.pid,
  });

  attachLineLogger(child.stdout, "stdout", eventsPath, child.pid);
  attachLineLogger(child.stderr, "stderr", eventsPath, child.pid);

  const forwardSignal = (signal: NodeJS.Signals) => {
    if (child.exitCode !== null || child.killed) {
      return;
    }
    try {
      child.kill(signal);
    } catch {
      // ignore forwarding failures
    }
  };

  const signals: NodeJS.Signals[] = ["SIGINT", "SIGTERM", "SIGHUP"];
  for (const signal of signals) {
    process.on(signal, () => {
      void appendEvent(eventsPath, {
        type: "signal",
        pid: child.pid,
        signal,
      });
      forwardSignal(signal);
    });
  }

  await new Promise<void>((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", async (code, signal) => {
      try {
        const latest = await readState(statePath);
        await writeState(statePath, {
          ...latest,
          pid: null,
          runnerPid: null,
          stoppedAt: new Date().toISOString(),
          lastExitCode: typeof code === "number" ? code : null,
        });
        await appendEvent(eventsPath, {
          type: code === 0 ? "exit" : "error",
          pid: child.pid,
          code,
          signal,
          text: signal ? `process exited due to ${signal}` : code === 0 ? "process exited cleanly" : `process exited with code ${code}`,
        });
        resolve();
      } catch (error) {
        reject(error);
      }
    });
  });
}

main().catch(async (error) => {
  const eventsPath = process.env.DOER_DAEMON_EVENTS_PATH?.trim();
  const statePath = process.env.DOER_DAEMON_STATE_PATH?.trim();
  const message = error instanceof Error ? error.stack || error.message : String(error);
  if (eventsPath) {
    await appendEvent(eventsPath, {
      type: "error",
      pid: process.pid,
      text: message,
    }).catch(() => undefined);
  }
  if (statePath) {
    try {
      const state = await readState(statePath);
      await writeState(statePath, {
        ...state,
        pid: null,
        runnerPid: null,
        stoppedAt: new Date().toISOString(),
        lastExitCode: state.lastExitCode ?? 1,
      });
    } catch {
      // ignore
    }
  }
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
