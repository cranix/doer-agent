import { StringCodec, type NatsConnection } from "nats";

const runRpcCodec = StringCodec();

export interface AgentRunRpcRequest {
  requestId?: unknown;
  action?: unknown;
  runId?: unknown;
  prompt?: unknown;
  imagePaths?: unknown;
  sessionId?: unknown;
  model?: unknown;
  cwd?: unknown;
  responseSubject?: unknown;
  agentId?: unknown;
  sinceSeq?: unknown;
  limit?: unknown;
  runtimeEnvPatch?: unknown;
  codexAuth?: unknown;
}

export interface AgentRunRpcResponse<TRunTask = RunTaskLike> {
  requestId: string;
  ok: boolean;
  task?: TRunTask | null;
  tasks?: TRunTask[];
  error?: string;
}

export type AgentRunRpcAction = "start" | "get" | "cancel" | "list";

export interface AgentRunRpcNormalizedRequest<TCodexAuth = unknown> {
  requestId: string;
  action: AgentRunRpcAction;
  runId: string | null;
  prompt: string | null;
  imagePaths: string[];
  sessionId: string | null;
  model: string;
  cwd: string | null;
  responseSubject: string;
  sinceSeq: number | null;
  limit: number;
  runtimeEnvPatch: Record<string, string>;
  codexAuthBundle: TCodexAuth | null;
}

export interface RunTaskLike {
  id: string;
  userId: string;
  agentId: string;
  processPid: number | null;
  sessionId: string | null;
  sessionFilePath: string | null;
  status: "queued" | "running" | "completed" | "failed" | "canceled";
  cancelRequested: boolean;
  resultExitCode: number | null;
  resultSignal: string | null;
  error: string | null;
  createdAt: string;
  updatedAt: string;
  startedAt: string | null;
  finishedAt: string | null;
}

export function normalizeRunRpcRequest<TCodexAuth>(args: {
  request: AgentRunRpcRequest;
  agentId: string;
  normalizeModel: (value: unknown) => string;
  normalizeImagePaths: (value: unknown) => string[];
  normalizeEnvPatch: (value: unknown) => Record<string, string>;
  normalizeCodexAuthBundle: (value: unknown) => TCodexAuth | null;
}): AgentRunRpcNormalizedRequest<TCodexAuth> {
  const requestId = typeof args.request.requestId === "string" ? args.request.requestId.trim() : "";
  if (!requestId) {
    throw new Error("missing requestId");
  }
  const requestAgentId = typeof args.request.agentId === "string" ? args.request.agentId.trim() : "";
  if (!requestAgentId || requestAgentId !== args.agentId) {
    throw new Error("agent id mismatch");
  }
  const actionRaw = typeof args.request.action === "string" ? args.request.action.trim() : "";
  const action: AgentRunRpcAction = actionRaw === "cancel" || actionRaw === "get" || actionRaw === "list" ? actionRaw : "start";
  const responseSubject = typeof args.request.responseSubject === "string" ? args.request.responseSubject.trim() : "";
  if (!responseSubject) {
    throw new Error("missing responseSubject");
  }
  const runId = typeof args.request.runId === "string" && args.request.runId.trim() ? args.request.runId.trim() : null;
  const prompt = typeof args.request.prompt === "string" && args.request.prompt.trim() ? args.request.prompt.trim() : null;
  const imagePaths = args.normalizeImagePaths(args.request.imagePaths);
  const sessionId = typeof args.request.sessionId === "string" && args.request.sessionId.trim() ? args.request.sessionId.trim() : null;
  const model = args.normalizeModel(args.request.model);
  if (action === "start" && !prompt) {
    throw new Error("missing prompt");
  }
  if ((action === "get" || action === "cancel") && !runId) {
    throw new Error("missing runId");
  }
  const cwd = typeof args.request.cwd === "string" && args.request.cwd.trim() ? args.request.cwd.trim() : null;
  const sinceSeqRaw = Number(args.request.sinceSeq);
  const sinceSeq = Number.isInteger(sinceSeqRaw) && sinceSeqRaw >= 0 ? sinceSeqRaw : null;
  const limitRaw = Number(args.request.limit);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(Math.floor(limitRaw), 200)) : 50;
  return {
    requestId,
    action,
    runId,
    prompt,
    imagePaths,
    sessionId,
    model,
    cwd,
    responseSubject,
    sinceSeq,
    limit,
    runtimeEnvPatch: args.normalizeEnvPatch(args.request.runtimeEnvPatch),
    codexAuthBundle: args.normalizeCodexAuthBundle(args.request.codexAuth),
  };
}

export function publishRunRpcResponse<TRunTask = RunTaskLike>(args: {
  nc: NatsConnection;
  responseSubject: string;
  payload: AgentRunRpcResponse<TRunTask>;
}): void {
  args.nc.publish(args.responseSubject, runRpcCodec.encode(JSON.stringify(args.payload)));
}

export async function handleNonStartRunRpc<TRunTask extends RunTaskLike>(args: {
  request: AgentRunRpcNormalizedRequest<unknown>;
  nc: NatsConnection;
  userId: string;
  agentId: string;
  listPersistedRunTasks: () => Promise<TRunTask[]>;
  cloneRunTask: (task: TRunTask, sinceSeq?: number | null) => TRunTask;
  getStoredRun: (runId: string) => Promise<TRunTask | null>;
  persistRunTask: (task: TRunTask) => Promise<void>;
  publishImmediateRunEvent: (task: TRunTask) => void;
  writeRunStatus: (runId: string, message: string) => void;
  sendSignalToPid: (pid: number, signal: NodeJS.Signals) => void;
  formatTimestamp: () => string;
}): Promise<void> {
  const { request } = args;

  if (request.action === "list") {
    const merged = (await args.listPersistedRunTasks())
      .map((task) => args.cloneRunTask(task))
      .sort((a, b) => Date.parse(b.updatedAt) - Date.parse(a.updatedAt))
      .slice(0, request.limit);
    publishRunRpcResponse({
      nc: args.nc,
      responseSubject: request.responseSubject,
      payload: { requestId: request.requestId, ok: true, tasks: merged },
    });
    return;
  }

  const stored = request.runId ? await args.getStoredRun(request.runId) : null;
  if (!stored || stored.agentId !== args.agentId || stored.userId !== args.userId) {
    throw new Error("Run not found");
  }

  if (request.action === "cancel") {
    if (stored.processPid === null) {
      throw new Error("Run pid not found");
    }
    stored.cancelRequested = true;
    stored.updatedAt = args.formatTimestamp();
    await args.persistRunTask(stored);
    args.publishImmediateRunEvent(stored);
    args.writeRunStatus(stored.id, `cancel requested pid=${stored.processPid}`);
    args.sendSignalToPid(stored.processPid, "SIGINT");
    publishRunRpcResponse({
      nc: args.nc,
      responseSubject: request.responseSubject,
      payload: { requestId: request.requestId, ok: true, task: args.cloneRunTask(stored) },
    });
    return;
  }

  publishRunRpcResponse({
    nc: args.nc,
    responseSubject: request.responseSubject,
    payload: { requestId: request.requestId, ok: true, task: args.cloneRunTask(stored, request.sinceSeq) },
  });
}
