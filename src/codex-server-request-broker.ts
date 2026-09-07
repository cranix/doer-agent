export interface PendingCodexServerRequest {
  requestId: string;
  method: string;
  params: unknown;
  threadId: string | null;
  turnId: string | null;
  createdAt: string;
  expiresAt: string;
}

interface PendingEntry extends PendingCodexServerRequest {
  resolve: (response: unknown) => void;
  timer: ReturnType<typeof setTimeout>;
}

function recordValue(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function stringValue(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function requestTimeoutMs(method: string, params: unknown): number {
  const record = recordValue(params);
  const autoResolutionMs = typeof record?.autoResolutionMs === "number" && Number.isFinite(record.autoResolutionMs)
    ? Math.trunc(record.autoResolutionMs)
    : null;
  if (autoResolutionMs !== null && autoResolutionMs > 0) {
    return Math.min(10 * 60_000, Math.max(1_000, autoResolutionMs));
  }
  return method === "item/tool/requestUserInput" ? 10 * 60_000 : 5 * 60_000;
}

export class CodexServerRequestBroker {
  private readonly pending = new Map<string, PendingEntry>();

  constructor(
    private readonly fallbackResponse: (method: string) => unknown,
    private readonly onChanged?: (event: "pending" | "resolved", request: PendingCodexServerRequest) => void,
  ) {}

  waitForResponse(args: {
    requestId: string | number;
    method: string;
    params: unknown;
  }): Promise<unknown> {
    const requestId = String(args.requestId);
    const timeoutMs = requestTimeoutMs(args.method, args.params);
    const now = Date.now();
    return new Promise<unknown>((resolve) => {
      const request: PendingCodexServerRequest = {
        requestId,
        method: args.method,
        params: args.params,
        threadId: stringValue(recordValue(args.params)?.threadId),
        turnId: stringValue(recordValue(args.params)?.turnId),
        createdAt: new Date(now).toISOString(),
        expiresAt: new Date(now + timeoutMs).toISOString(),
      };
      const timer = setTimeout(() => {
        this.finish(requestId, this.fallbackResponse(args.method));
      }, timeoutMs);
      this.pending.set(requestId, { ...request, resolve, timer });
      this.onChanged?.("pending", request);
    });
  }

  list(threadId?: string | null): PendingCodexServerRequest[] {
    const normalizedThreadId = threadId?.trim() || null;
    return [...this.pending.values()]
      .filter((entry) => !normalizedThreadId || entry.threadId === normalizedThreadId)
      .map(({ resolve: _resolve, timer: _timer, ...request }) => request)
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt));
  }

  respond(requestId: string, response: unknown): boolean {
    return this.finish(requestId.trim(), response);
  }

  resolveWithFallback(requestId: string): boolean {
    const entry = this.pending.get(requestId.trim());
    return entry ? this.finish(entry.requestId, this.fallbackResponse(entry.method)) : false;
  }

  close(): void {
    for (const entry of [...this.pending.values()]) {
      this.finish(entry.requestId, this.fallbackResponse(entry.method));
    }
  }

  private finish(requestId: string, response: unknown): boolean {
    const entry = this.pending.get(requestId);
    if (!entry) {
      return false;
    }
    this.pending.delete(requestId);
    clearTimeout(entry.timer);
    const { resolve: _resolve, timer: _timer, ...request } = entry;
    entry.resolve(response);
    this.onChanged?.("resolved", request);
    return true;
  }
}
