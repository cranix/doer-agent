import type { CodexAppServerManager } from "./codex-app-server-manager.js";

const DEFAULT_TIMEOUT_MS = 180_000;

function stringValue(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function recordValue(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function threadIdFromParams(params: unknown): string {
  const record = recordValue(params);
  return stringValue(record?.threadId) || stringValue(recordValue(record?.thread)?.id);
}

function turnIdFromParams(params: unknown): string {
  const record = recordValue(params);
  return stringValue(record?.turnId) || stringValue(recordValue(record?.turn)?.id);
}

function terminalErrorFromParams(params: unknown): string {
  const record = recordValue(params);
  const error = recordValue(record?.error);
  return stringValue(record?.message) || stringValue(error?.message) || stringValue(record?.reason);
}

function isTerminalTurnMethod(method: string): boolean {
  return method === "turn/completed" ||
    method === "turn/failed" ||
    method === "turn/error" ||
    method === "turn/cancelled" ||
    method === "turn/canceled" ||
    method === "turn/interrupted" ||
    method === "turn/aborted";
}

function buildCodexUserInput(prompt: string): unknown[] {
  return [{
    type: "text",
    text: prompt,
    text_elements: [],
  }];
}

export async function runCodexTextTask(args: {
  manager: CodexAppServerManager;
  prompt: string;
  timeoutMs?: number;
  onLog?: (message: string) => void;
}): Promise<string> {
  const startedAt = Date.now();
  let threadId = "";
  let turnId = "";
  let output = "";
  let outputStarted = false;
  let settled = false;
  let cleanupNotification = () => {};
  let settle = (_callback: () => void) => {};
  const completed = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      args.onLog?.(
        `codex text task status phase=timed_out threadId=${threadId || "pending"} turnId=${turnId || "pending"} elapsedMs=${Date.now() - startedAt} outputChars=${output.length}`,
      );
      settle(() => reject(new Error("Timed out while waiting for Codex text task")));
    }, args.timeoutMs ?? DEFAULT_TIMEOUT_MS);
    settle = (callback: () => void) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      cleanupNotification();
      callback();
    };
    cleanupNotification = args.manager.onNotification((method, params) => {
      const eventThreadId = threadIdFromParams(params);
      const eventTurnId = turnIdFromParams(params);
      if (
        (!threadId || eventThreadId !== threadId) &&
        (!turnId || eventTurnId !== turnId)
      ) {
        return;
      }
      if (turnId && eventTurnId && eventTurnId !== turnId) {
        return;
      }
      if (method === "item/agentMessage/delta") {
        const record = recordValue(params);
        output += stringValue(record?.delta) || stringValue(record?.text);
        if (!outputStarted && output) {
          outputStarted = true;
          args.onLog?.(
            `codex text task status phase=responding threadId=${threadId} turnId=${turnId || eventTurnId || "pending"} elapsedMs=${Date.now() - startedAt}`,
          );
        }
        return;
      }
      if (!isTerminalTurnMethod(method)) {
        return;
      }
      args.onLog?.(
        `codex text task status phase=terminal_notification method=${method} threadId=${threadId} turnId=${turnId || eventTurnId || "pending"} elapsedMs=${Date.now() - startedAt} outputChars=${output.length}`,
      );
      if (method === "turn/completed") {
        settle(resolve);
        return;
      }
      const message = terminalErrorFromParams(params) || `Codex text task ended with ${method}`;
      settle(() => reject(new Error(message)));
    });
  });

  try {
    const threadResult = recordValue(await args.manager.request("thread/start", {
      cwd: null,
      ephemeral: true,
      sessionStartSource: "clear",
    }, 30_000));
    threadId = stringValue(recordValue(threadResult?.thread)?.id);
    if (!threadId) {
      throw new Error("Codex app-server did not return a text task thread id");
    }
    args.onLog?.(
      `codex text task status phase=thread_started threadId=${threadId} elapsedMs=${Date.now() - startedAt}`,
    );

    const turnResult = recordValue(await args.manager.request("turn/start", {
      threadId,
      input: buildCodexUserInput(args.prompt),
    }, 30_000));
    turnId = stringValue(recordValue(turnResult?.turn)?.id);
    if (!turnId) {
      throw new Error("Codex app-server did not return a text task turn id");
    }
    args.onLog?.(
      `codex text task status phase=turn_started threadId=${threadId} turnId=${turnId} elapsedMs=${Date.now() - startedAt}`,
    );

    await completed.finally(() => cleanupNotification());
    args.onLog?.(
      `codex text task status phase=completed threadId=${threadId} turnId=${turnId} elapsedMs=${Date.now() - startedAt} outputChars=${output.length}`,
    );
    return output;
  } catch (error) {
    settle(() => {});
    args.onLog?.(
      `codex text task status phase=failed threadId=${threadId || "pending"} turnId=${turnId || "pending"} elapsedMs=${Date.now() - startedAt} error=${JSON.stringify((error instanceof Error ? error.message : String(error)).replace(/\s+/g, " ").slice(0, 500))}`,
    );
    throw error;
  } finally {
    if (threadId) {
      await args.manager.request("thread/unsubscribe", { threadId }, 30_000)
        .then(() => {
          args.onLog?.(
            `codex text task status phase=unsubscribed threadId=${threadId} turnId=${turnId || "pending"} elapsedMs=${Date.now() - startedAt}`,
          );
        })
        .catch((error) => {
          args.onLog?.(
            `codex text task status phase=unsubscribe_failed threadId=${threadId} turnId=${turnId || "pending"} elapsedMs=${Date.now() - startedAt} error=${JSON.stringify((error instanceof Error ? error.message : String(error)).replace(/\s+/g, " ").slice(0, 500))}`,
          );
        });
    }
  }
}
