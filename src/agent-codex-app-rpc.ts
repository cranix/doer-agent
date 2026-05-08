import { StringCodec, type Msg, type NatsConnection } from "nats";
import { buildAgentSettingsEnvPatch, type AgentSettingsConfig } from "./agent-settings.js";
import { CodexAppServerClient } from "./codex-app-server-client.js";

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

interface AgentCodexAppNotificationEvent {
  type: "codex.app.notification";
  agentId: string;
  method: string;
  params: unknown;
  emittedAt: string;
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

async function buildCodexAppServerEnv(args: {
  workspaceRoot: string;
  resolveCodexHomePath: () => string;
  readAgentSettingsConfig: (args: { workspaceRoot: string }) => Promise<AgentSettingsConfig>;
}): Promise<NodeJS.ProcessEnv> {
  const settings = await args.readAgentSettingsConfig({ workspaceRoot: args.workspaceRoot });
  return {
    ...process.env,
    ...buildAgentSettingsEnvPatch(settings),
    CODEX_HOME: args.resolveCodexHomePath(),
  };
}

async function handleCodexAppRpcMessage(args: {
  msg: Msg;
  nc: NatsConnection;
  agentId: string;
  workspaceRoot: string;
  resolveCodexHomePath: () => string;
  readAgentSettingsConfig: (args: { workspaceRoot: string }) => Promise<AgentSettingsConfig>;
  clientRef: { current: CodexAppServerClient | null };
  eventsSubject: string;
  onInfo: (message: string) => void;
  onError: (message: string) => void;
}): Promise<void> {
  let requestId = "unknown";
  try {
    const payload = JSON.parse(codexAppRpcCodec.decode(args.msg.data)) as AgentCodexAppRpcRequest;
    const request = normalizeCodexAppRpcRequest({ request: payload, agentId: args.agentId });
    requestId = request.requestId;

    if (!args.clientRef.current) {
      args.clientRef.current = new CodexAppServerClient({
        cwd: args.workspaceRoot,
        env: await buildCodexAppServerEnv({
          workspaceRoot: args.workspaceRoot,
          resolveCodexHomePath: args.resolveCodexHomePath,
          readAgentSettingsConfig: args.readAgentSettingsConfig,
        }),
        onLog: args.onInfo,
        onNotification: (method, params) => {
          args.onInfo(`codex app-server notification method=${method}`);
          const event: AgentCodexAppNotificationEvent = {
            type: "codex.app.notification",
            agentId: args.agentId,
            method,
            params,
            emittedAt: new Date().toISOString(),
          };
          args.nc.publish(args.eventsSubject, codexAppRpcCodec.encode(JSON.stringify(event)));
        },
      });
    }

    const result = await args.clientRef.current.request(request.method, request.params);
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
  workspaceRoot: string;
  resolveCodexHomePath: () => string;
  readAgentSettingsConfig: (args: { workspaceRoot: string }) => Promise<AgentSettingsConfig>;
  onInfo: (message: string) => void;
  onError: (message: string) => void;
}): void {
  const clientRef: { current: CodexAppServerClient | null } = { current: null };
  args.nc.closed().finally(() => {
    void clientRef.current?.stop().catch(() => undefined);
    clientRef.current = null;
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
        workspaceRoot: args.workspaceRoot,
        resolveCodexHomePath: args.resolveCodexHomePath,
        readAgentSettingsConfig: args.readAgentSettingsConfig,
        clientRef,
        eventsSubject: args.eventsSubject,
        onInfo: args.onInfo,
        onError: args.onError,
      });
    },
  });
  args.onInfo(`codex app rpc subscribed subject=${args.subject} eventsSubject=${args.eventsSubject}`);
}
