import { StringCodec, type Msg, type NatsConnection } from "nats";
import type { CodexAppServerManager } from "./codex-app-server-manager.js";
import {
  createCodexThreadHandoff,
  type CodexThreadHandoffGoal,
} from "./codex-thread-handoff.js";

const codexAppRpcCodec = StringCodec();

interface AgentCodexAppRpcRequest {
  requestId?: unknown;
  action?: unknown;
  agentId?: unknown;
  method?: unknown;
  params?: unknown;
  path?: unknown;
  search?: unknown;
  name?: unknown;
  sourceThreadId?: unknown;
  targetName?: unknown;
  activeTurnId?: unknown;
  latestUserText?: unknown;
  sourceGoal?: unknown;
  timeoutMs?: unknown;
}

interface AgentCodexAppRpcResponse {
  requestId: string;
  ok: boolean;
  result?: unknown;
  error?: string;
}

interface CodexAppRpcOmitRule {
  method: string;
  itemType?: string;
  keys: string[];
}

const codexAppRpcOmitRules: CodexAppRpcOmitRule[] = [
  {
    method: "thread/turns/list",
    itemType: "imageGeneration",
    keys: ["result"],
  },
];

function recordValue(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}

function omitKeysFromRecord(record: Record<string, unknown>, keys: string[]): Record<string, unknown> {
  const next = { ...record };
  for (const key of keys) {
    delete next[key];
  }
  return next;
}

function omitRulesForThreadTurnItem(method: string, item: Record<string, unknown>): CodexAppRpcOmitRule[] {
  return codexAppRpcOmitRules.filter((rule) => {
    if (rule.method !== method) {
      return false;
    }
    return !rule.itemType || item.type === rule.itemType;
  });
}

function applyCodexAppRpcOmitRules(method: string, value: unknown): unknown {
  if (!codexAppRpcOmitRules.some((rule) => rule.method === method)) {
    return value;
  }
  const response = recordValue(value);
  if (!response || !Array.isArray(response.data)) {
    return value;
  }

  return {
    ...response,
    data: response.data.map((turnValue) => {
      const turn = recordValue(turnValue);
      if (!turn || !Array.isArray(turn.items)) {
        return turnValue;
      }
      return {
        ...turn,
        items: turn.items.map((itemValue) => {
          const item = recordValue(itemValue);
          if (!item) {
            return itemValue;
          }
          const rules = omitRulesForThreadTurnItem(method, item);
          if (rules.length === 0) {
            return itemValue;
          }
          return rules.reduce(
            (current, rule) => omitKeysFromRecord(current, rule.keys),
            item,
          );
        }),
      };
    }),
  };
}

function normalizeCodexAppRpcRequest(args: {
  request: AgentCodexAppRpcRequest;
  agentId: string;
}): {
  requestId: string;
  action: "request";
  method: string;
  params: unknown;
  timeoutMs?: number;
} | {
  requestId: string;
  action: "mcp-oauth-callback";
  path: string;
  search: string;
} | {
  requestId: string;
  action: "mcp-oauth-logout";
  name: string;
} | {
  requestId: string;
  action: "thread-handoff";
  sourceThreadId: string;
  targetName: string;
  activeTurnId: string;
  latestUserText: string;
  sourceGoal: CodexThreadHandoffGoal | null;
} {
  const requestId = typeof args.request.requestId === "string" ? args.request.requestId.trim() : "";
  const requestAgentId = typeof args.request.agentId === "string" ? args.request.agentId.trim() : "";
  const actionRaw = typeof args.request.action === "string" ? args.request.action.trim() : "";
  const method = typeof args.request.method === "string" ? args.request.method.trim() : "";
  const timeoutMs = typeof args.request.timeoutMs === "number" && Number.isFinite(args.request.timeoutMs)
    ? Math.min(180_000, Math.max(1_000, Math.trunc(args.request.timeoutMs)))
    : undefined;
  if (!requestId || !requestAgentId || requestAgentId !== args.agentId) {
    throw new Error("invalid codex app rpc request");
  }
  if (actionRaw === "mcp-oauth-callback") {
    const path = typeof args.request.path === "string" ? args.request.path.trim() : "";
    const search = typeof args.request.search === "string" ? args.request.search.trim() : "";
    if (!path.startsWith("/") || search.length > 16_384) {
      throw new Error("invalid MCP OAuth callback request");
    }
    return { requestId, action: "mcp-oauth-callback", path, search };
  }
  if (actionRaw === "mcp-oauth-logout") {
    const name = typeof args.request.name === "string" ? args.request.name.trim() : "";
    if (!/^[A-Za-z0-9_-]+$/.test(name)) {
      throw new Error("invalid MCP OAuth logout request");
    }
    return { requestId, action: "mcp-oauth-logout", name };
  }
  if (actionRaw === "thread-handoff") {
    const sourceThreadId = typeof args.request.sourceThreadId === "string" ? args.request.sourceThreadId.trim() : "";
    const targetName = typeof args.request.targetName === "string" ? args.request.targetName.trim() : "";
    const activeTurnId = typeof args.request.activeTurnId === "string" ? args.request.activeTurnId.trim() : "";
    const latestUserText = typeof args.request.latestUserText === "string" ? args.request.latestUserText.trim().slice(0, 4_000) : "";
    const sourceGoalRecord = recordValue(args.request.sourceGoal);
    const sourceGoal = sourceGoalRecord && typeof sourceGoalRecord.objective === "string" && typeof sourceGoalRecord.status === "string"
      ? {
          objective: sourceGoalRecord.objective,
          status: sourceGoalRecord.status,
          tokenBudget: typeof sourceGoalRecord.tokenBudget === "number" ? sourceGoalRecord.tokenBudget : null,
        } as CodexThreadHandoffGoal
      : null;
    if (!sourceThreadId || targetName.length > 200) {
      throw new Error("invalid thread handoff request");
    }
    return {
      requestId,
      action: "thread-handoff",
      sourceThreadId,
      targetName,
      activeTurnId,
      latestUserText,
      sourceGoal,
    };
  }
  if (actionRaw !== "request" || !method) {
    throw new Error("invalid codex app rpc request");
  }
  return {
    requestId,
    action: "request",
    method,
    params: args.request.params,
    timeoutMs,
  };
}

async function handleCodexAppRpcMessage(args: {
  msg: Msg;
  nc: NatsConnection;
  agentId: string;
  manager: CodexAppServerManager;
  onInfo: (message: string) => void;
  onError: (message: string) => void;
}): Promise<void> {
  let requestId = "unknown";
  try {
    const payload = JSON.parse(codexAppRpcCodec.decode(args.msg.data)) as AgentCodexAppRpcRequest;
    const request = normalizeCodexAppRpcRequest({ request: payload, agentId: args.agentId });
    requestId = request.requestId;

    const result = request.action === "request"
      ? applyCodexAppRpcOmitRules(
        request.method,
        await args.manager.request(request.method, request.params, request.timeoutMs),
      )
      : request.action === "mcp-oauth-callback"
        ? await args.manager.relayMcpOauthCallback(request.path, request.search)
        : request.action === "mcp-oauth-logout"
          ? await args.manager.logoutMcpServer(request.name)
          : await createCodexThreadHandoff({
              manager: args.manager,
              sourceThreadId: request.sourceThreadId,
              targetName: request.targetName,
              activeTurnId: request.activeTurnId,
              latestUserText: request.latestUserText,
              sourceGoal: request.sourceGoal,
              onLog: args.onInfo,
            });
    args.msg.respond(codexAppRpcCodec.encode(JSON.stringify({
      requestId,
      ok: true,
      result,
    } satisfies AgentCodexAppRpcResponse)));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    args.onError(`codex app rpc failed requestId=${requestId} error=${message}`);
    args.msg.respond(codexAppRpcCodec.encode(JSON.stringify({
      requestId,
      ok: false,
      error: message,
    } satisfies AgentCodexAppRpcResponse)));
  }
}

export function subscribeToCodexAppRpc(args: {
  nc: NatsConnection;
  subject: string;
  eventsSubject: string;
  agentId: string;
  manager: CodexAppServerManager;
  onInfo: (message: string) => void;
  onError: (message: string) => void;
}): void {
  args.nc.closed().finally(() => {
    void args.manager.stop().catch(() => undefined);
  });
  args.nc.subscribe(args.subject, {
    callback: (error, msg) => {
      if (error) {
        const message = error instanceof Error ? error.message : String(error);
        args.onError(`codex app rpc subscription error: ${message}`);
        return;
      }
      void handleCodexAppRpcMessage({
        msg,
        nc: args.nc,
        agentId: args.agentId,
        manager: args.manager,
        onInfo: args.onInfo,
        onError: args.onError,
      });
    },
  });
  args.onInfo(`codex app rpc subscribed subject=${args.subject} eventsSubject=${args.eventsSubject}`);
}
