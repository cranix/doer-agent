import { StringCodec, type Msg, type NatsConnection } from "nats";
import type { CodexAppServerManager } from "./codex-app-server-manager.js";

const codexAppRpcCodec = StringCodec();

interface AgentCodexAppRpcRequest {
  requestId?: unknown;
  action?: unknown;
  agentId?: unknown;
  method?: unknown;
  params?: unknown;
}

interface AgentCodexAppRpcResponse {
  requestId: string;
  ok: boolean;
  result?: unknown;
  error?: string;
}

function normalizeCodexAppRpcRequest(args: {
  request: AgentCodexAppRpcRequest;
  agentId: string;
}): {
  requestId: string;
  action: "request";
  method: string;
  params: unknown;
} {
  const requestId = typeof args.request.requestId === "string" ? args.request.requestId.trim() : "";
  const requestAgentId = typeof args.request.agentId === "string" ? args.request.agentId.trim() : "";
  const actionRaw = typeof args.request.action === "string" ? args.request.action.trim() : "";
  const method = typeof args.request.method === "string" ? args.request.method.trim() : "";
  if (!requestId || !requestAgentId || requestAgentId !== args.agentId || actionRaw !== "request" || !method) {
    throw new Error("invalid codex app rpc request");
  }
  return {
    requestId,
    action: "request",
    method,
    params: args.request.params,
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

    const result = await args.manager.request(request.method, request.params);
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
