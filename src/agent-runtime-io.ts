import type { NatsConnection } from "nats";

export type AgentEventType = "stdout" | "stderr" | "status" | "meta";

export interface JsonRequestOptions {
  timeoutMs?: number;
}

export function publishNatsBestEffort(args: {
  nc: NatsConnection;
  subject: string;
  data: Uint8Array;
  context: string;
  onError: (message: string) => void;
}): boolean {
  if (args.nc.isClosed()) {
    args.onError(`${args.context}: NATS connection is closed; event dropped`);
    return false;
  }
  try {
    args.nc.publish(args.subject, args.data);
    return true;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    args.onError(`${args.context}: ${message}; event dropped`);
    return false;
  }
}

export async function postJson<T>(url: string, body: unknown, options: JsonRequestOptions = {}): Promise<T> {
  let res: Response;
  try {
    res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      ...(options.timeoutMs ? { signal: AbortSignal.timeout(options.timeoutMs) } : {}),
    });
  } catch (error) {
    if (options.timeoutMs && error instanceof Error && error.name === "TimeoutError") {
      throw new Error(`request timed out after ${options.timeoutMs}ms`);
    }
    throw error;
  }
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

export async function getJson<T>(url: string): Promise<T> {
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

export function createEventPersistenceHelpers<TJetstream>(args: {
  getActiveTaskLogContext: () => {
    jetstream: TJetstream;
    serverBaseUrl: string;
    taskId: string;
    userId: string;
  } | null;
  publishEvent: (args: {
    jetstream: TJetstream;
    serverBaseUrl: string;
    taskId: string;
    userId: string;
    type: AgentEventType;
    seq: number;
    payload: Record<string, unknown>;
  }) => Promise<void>;
  formatTimestamp: () => string;
  onError: (message: string) => void;
  sleep: (ms: number) => Promise<void>;
}): {
  reserveNextEventSeq: (taskId: string) => number;
  emitAgentMetaLog: (level: "info" | "error", message: string) => void;
  recordAgentEvent: (args: {
    jetstream: TJetstream;
    serverBaseUrl: string;
    taskId: string;
    userId: string;
    type: AgentEventType;
    seq: number;
    payload: Record<string, unknown>;
  }) => Promise<void>;
  persistEventOrFatal: (args: {
    jetstream: TJetstream;
    serverBaseUrl: string;
    taskId: string;
    userId: string;
    type: AgentEventType;
    seq: number;
    payload: Record<string, unknown>;
    context: string;
  }) => void;
} {
  const nextEventSeqByTask = new Map<string, number>();

  async function recordAgentEvent(recordArgs: {
    jetstream: TJetstream;
    serverBaseUrl: string;
    taskId: string;
    userId: string;
    type: AgentEventType;
    seq: number;
    payload: Record<string, unknown>;
  }): Promise<void> {
    await args.publishEvent(recordArgs);
  }

  function reserveNextEventSeq(taskId: string): number {
    const current = nextEventSeqByTask.get(taskId) ?? 1;
    nextEventSeqByTask.set(taskId, current + 1);
    return current;
  }

  function emitAgentMetaLog(level: "info" | "error", message: string): void {
    const ctx = args.getActiveTaskLogContext();
    if (!ctx) {
      return;
    }
    const seq = reserveNextEventSeq(ctx.taskId);
    void recordAgentEvent({
      jetstream: ctx.jetstream,
      serverBaseUrl: ctx.serverBaseUrl,
      taskId: ctx.taskId,
      userId: ctx.userId,
      type: "meta",
      seq,
      payload: {
        channel: "agent",
        level,
        message,
        at: args.formatTimestamp(),
      },
    }).catch((error) => {
      const detail = error instanceof Error ? error.message : String(error);
      process.stderr.write(`[doer-agent] meta log persist failed task=${ctx.taskId}: ${detail}\n`);
    });
  }

  function persistEventOrFatal(persistArgs: {
    jetstream: TJetstream;
    serverBaseUrl: string;
    taskId: string;
    userId: string;
    type: AgentEventType;
    seq: number;
    payload: Record<string, unknown>;
    context: string;
  }): void {
    void (async () => {
      let attempt = 0;
      let delayMs = 150;
      while (attempt < 3) {
        attempt += 1;
        try {
          await recordAgentEvent(persistArgs);
          return;
        } catch (error) {
          if (attempt >= 3) {
            const message = error instanceof Error ? error.message : String(error);
            args.onError(
              `task=${persistArgs.taskId} ${persistArgs.context}: ${message} (dropped after ${attempt} attempts)`,
            );
            return;
          }
          await args.sleep(delayMs);
          delayMs *= 2;
        }
      }
    })();
  }

  return {
    reserveNextEventSeq,
    emitAgentMetaLog,
    recordAgentEvent,
    persistEventOrFatal,
  };
}

export async function heartbeatAgentSession(args: {
  nc: NatsConnection;
  serverBaseUrl: string;
  userId: string;
  agentToken: string;
  timeoutMs: number;
  postJson: <T>(url: string, body: unknown, options?: JsonRequestOptions) => Promise<T>;
}): Promise<void> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      args.nc.flush(),
      new Promise<never>((_, reject) => {
        timeout = setTimeout(
          () => reject(new Error(`nats flush timed out after ${args.timeoutMs}ms`)),
          args.timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
  await args.postJson<{ ok?: boolean }>(`${args.serverBaseUrl}/api/agent/heartbeat`, {
    userId: args.userId,
    agentToken: args.agentToken,
  }, { timeoutMs: args.timeoutMs });
}
