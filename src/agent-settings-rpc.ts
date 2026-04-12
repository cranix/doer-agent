import { readFile } from "node:fs/promises";
import { StringCodec, type Msg, type NatsConnection } from "nats";
import {
  normalizeAgentSettingsConfig,
  normalizeAgentSettingsPatch,
  readAgentSettingsConfig,
  resolveAgentSettingsFilePath,
  toAgentSettingsPublic,
  writeAgentModelInstructions,
  writeAgentSettingsConfig,
  type AgentSettingsConfig,
  type AgentSettingsPublic,
} from "./agent-settings.js";

const settingsRpcCodec = StringCodec();

export type AgentSettingsRpcAction = "get" | "update";

export interface AgentSettingsRpcRequest {
  requestId?: unknown;
  responseSubject?: unknown;
  agentId?: unknown;
  action?: unknown;
  patch?: unknown;
  defaults?: unknown;
}

export interface AgentSettingsRpcResponse {
  requestId: string;
  ok: boolean;
  settings?: AgentSettingsPublic;
  error?: string;
}

interface NormalizedSettingsRpcRequest {
  requestId: string;
  responseSubject: string;
  action: AgentSettingsRpcAction;
  patch: Record<string, unknown>;
  defaults: AgentSettingsConfig | null;
}

function normalizeSettingsRpcRequest(args: {
  request: AgentSettingsRpcRequest;
  agentId: string;
}): NormalizedSettingsRpcRequest {
  const requestId = typeof args.request.requestId === "string" ? args.request.requestId.trim() : "";
  const responseSubject = typeof args.request.responseSubject === "string" ? args.request.responseSubject.trim() : "";
  const requestAgentId = typeof args.request.agentId === "string" ? args.request.agentId.trim() : "";
  const action = args.request.action === "update" ? "update" : "get";
  if (!requestId || !responseSubject || !requestAgentId || requestAgentId !== args.agentId) {
    throw new Error("invalid settings rpc request");
  }
  return {
    requestId,
    responseSubject,
    action,
    patch: normalizeAgentSettingsPatch(args.request.patch),
    defaults:
      args.request.defaults && typeof args.request.defaults === "object" && !Array.isArray(args.request.defaults)
        ? normalizeAgentSettingsConfig(args.request.defaults)
        : null,
  };
}

function publishSettingsRpcResponse(args: {
  nc: NatsConnection;
  responseSubject: string;
  payload: AgentSettingsRpcResponse;
}): void {
  args.nc.publish(args.responseSubject, settingsRpcCodec.encode(JSON.stringify(args.payload)));
}

export async function handleSettingsRpcMessage(args: {
  msg: Msg;
  nc: NatsConnection;
  agentId: string;
  workspaceRoot: string;
  onError: (message: string) => void;
}): Promise<void> {
  let requestId = "unknown";
  let responseSubject = "";
  try {
    const payload = JSON.parse(settingsRpcCodec.decode(args.msg.data)) as AgentSettingsRpcRequest;
    const request = normalizeSettingsRpcRequest({ request: payload, agentId: args.agentId });
    requestId = request.requestId;
    responseSubject = request.responseSubject;
    const existing = await readAgentSettingsConfig({ workspaceRoot: args.workspaceRoot, defaults: request.defaults });
    const next = request.action === "update" ? normalizeAgentSettingsConfig(request.patch, existing) : existing;
    if (request.action === "update") {
      await writeAgentSettingsConfig({ workspaceRoot: args.workspaceRoot, config: next });
      const customInstructions =
        typeof request.patch.customInstructions === "string"
          ? request.patch.customInstructions
          : request.patch.customInstructions === null
            ? null
            : undefined;
      if (customInstructions !== undefined) {
        await writeAgentModelInstructions({ workspaceRoot: args.workspaceRoot, value: customInstructions });
      }
    } else if (request.defaults) {
      const filePath = resolveAgentSettingsFilePath(args.workspaceRoot);
      const raw = await readFile(filePath, "utf8").catch(() => "");
      if (!raw.trim()) {
        await writeAgentSettingsConfig({ workspaceRoot: args.workspaceRoot, config: next });
      }
    }
    publishSettingsRpcResponse({
      nc: args.nc,
      responseSubject,
      payload: {
        requestId,
        ok: true,
        settings: await toAgentSettingsPublic({ workspaceRoot: args.workspaceRoot, config: next }),
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (responseSubject) {
      publishSettingsRpcResponse({
        nc: args.nc,
        responseSubject,
        payload: { requestId, ok: false, error: message },
      });
    }
    args.onError(`settings rpc failed requestId=${requestId} error=${message}`);
  }
}
