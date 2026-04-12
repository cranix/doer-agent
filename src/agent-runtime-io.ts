import type { NatsConnection } from "nats";

export type AgentEventType = "stdout" | "stderr" | "status" | "meta";

export async function postJson<T>(url: string, body: unknown): Promise<T> {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
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
  postJson: <T>(url: string, body: unknown) => Promise<T>;
}): Promise<void> {
  await args.nc.flush();
  await args.postJson<{ ok?: boolean }>(`${args.serverBaseUrl}/api/agent/heartbeat`, {
    userId: args.userId,
    agentToken: args.agentToken,
  });
}
