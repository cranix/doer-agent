import { appendFile, readFile, writeFile, mkdir, unlink } from "node:fs/promises";
import path from "node:path";
import { StringCodec, type Msg, type NatsConnection } from "nats";

const proxyRpcCodec = StringCodec();
const PROXY_ID_PATTERN = /^[a-z0-9][a-z0-9-]{1,30}[a-z0-9]$/;
const MAX_PROXY_BODY_BYTES = 5 * 1024 * 1024;
const MAX_PROXY_LOGS = 100;

type HttpProxyRpcAction = "list" | "create" | "update" | "delete" | "logs" | "handle";

export interface AgentHttpProxyRecord {
  id: string;
  name: string | null;
  host: string;
  port: number;
  enabled: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface AgentHttpProxyLogRecord {
  id: string;
  at: string;
  method: string;
  path: string;
  status: number | null;
  durationMs: number;
  requestBytes: number;
  responseBytes: number;
  error: string | null;
}

interface AgentHttpProxyRpcRequest {
  requestId?: unknown;
  action?: unknown;
  proxyId?: unknown;
  name?: unknown;
  host?: unknown;
  port?: unknown;
  enabled?: unknown;
  limit?: unknown;
  method?: unknown;
  path?: unknown;
  headers?: unknown;
  bodyBase64?: unknown;
}

interface AgentHttpProxyFetchResponse {
  status: number;
  statusText: string;
  headers: Record<string, string>;
  bodyBase64: string;
}

function getProxyRegistryPath(workspaceRoot: string): string {
  return path.join(workspaceRoot, ".doer-agent", "http-proxies.json");
}

function getProxyLogsPath(workspaceRoot: string, proxyId: string): string {
  return path.join(workspaceRoot, ".doer-agent", "http-proxy-logs", `${proxyId}.jsonl`);
}

function slugify(value: string): string {
  const slug = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/-{2,}/g, "-")
    .slice(0, 32);
  return PROXY_ID_PATTERN.test(slug) ? slug : `p${Date.now().toString(36)}`;
}

function normalizeProxyId(value: unknown): string {
  const id = typeof value === "string" ? value.trim().toLowerCase() : "";
  if (!PROXY_ID_PATTERN.test(id)) {
    throw new Error("invalid proxyId");
  }
  return id;
}

function normalizeName(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const name = value.trim();
  return name ? name.slice(0, 120) : null;
}

function normalizeHost(value: unknown): string {
  const host = typeof value === "string" && value.trim() ? value.trim() : "127.0.0.1";
  if (host !== "127.0.0.1" && host !== "localhost") {
    throw new Error("proxy host must be localhost or 127.0.0.1");
  }
  return host;
}

function normalizePort(value: unknown): number {
  const port = Number(value);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("port must be between 1 and 65535");
  }
  return port;
}

function normalizeMethod(value: unknown): string {
  const method = typeof value === "string" ? value.trim().toUpperCase() : "GET";
  if (!/^[A-Z]+$/.test(method)) {
    throw new Error("invalid method");
  }
  return method;
}

function normalizePath(value: unknown): string {
  const raw = typeof value === "string" && value ? value : "/";
  return raw.startsWith("/") ? raw : `/${raw}`;
}

function normalizeHeaders(value: unknown): Record<string, string> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  const out: Record<string, string> = {};
  for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
    if (typeof raw !== "string") {
      continue;
    }
    const normalizedKey = key.trim().toLowerCase();
    if (!normalizedKey || normalizedKey === "host" || normalizedKey === "connection") {
      continue;
    }
    out[normalizedKey] = raw;
  }
  return out;
}

function normalizeProxyRecord(value: unknown): AgentHttpProxyRecord | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const row = value as Record<string, unknown>;
  const id = typeof row.id === "string" ? row.id.trim().toLowerCase() : "";
  const host = normalizeHost(row.host);
  const port = Number(row.port);
  const createdAt = typeof row.createdAt === "string" ? row.createdAt : new Date().toISOString();
  const updatedAt = typeof row.updatedAt === "string" ? row.updatedAt : createdAt;
  if (!PROXY_ID_PATTERN.test(id) || !Number.isInteger(port) || port < 1 || port > 65535) {
    return null;
  }
  return {
    id,
    name: normalizeName(row.name),
    host,
    port,
    enabled: row.enabled !== false,
    createdAt,
    updatedAt,
  };
}

function normalizeProxyLogRecord(value: unknown): AgentHttpProxyLogRecord | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const row = value as Record<string, unknown>;
  const id = typeof row.id === "string" && row.id.trim() ? row.id.trim() : "";
  const at = typeof row.at === "string" && row.at.trim() ? row.at.trim() : "";
  const method = typeof row.method === "string" && row.method.trim() ? row.method.trim().toUpperCase() : "";
  const requestPath = typeof row.path === "string" && row.path.trim() ? row.path.trim() : "/";
  const status = typeof row.status === "number" && Number.isInteger(row.status) ? row.status : null;
  const durationMs = typeof row.durationMs === "number" && Number.isFinite(row.durationMs) ? Math.max(0, Math.round(row.durationMs)) : 0;
  const requestBytes = typeof row.requestBytes === "number" && Number.isFinite(row.requestBytes) ? Math.max(0, Math.round(row.requestBytes)) : 0;
  const responseBytes = typeof row.responseBytes === "number" && Number.isFinite(row.responseBytes) ? Math.max(0, Math.round(row.responseBytes)) : 0;
  if (!id || !at || !method) {
    return null;
  }
  return {
    id,
    at,
    method,
    path: requestPath,
    status,
    durationMs,
    requestBytes,
    responseBytes,
    error: typeof row.error === "string" && row.error.trim() ? row.error.trim().slice(0, 500) : null,
  };
}

async function readProxyRegistry(workspaceRoot: string): Promise<AgentHttpProxyRecord[]> {
  const raw = await readFile(getProxyRegistryPath(workspaceRoot), "utf8").catch(() => "");
  if (!raw) {
    return [];
  }
  const parsed = JSON.parse(raw) as unknown;
  const rows = Array.isArray(parsed) ? parsed : Array.isArray((parsed as { proxies?: unknown[] })?.proxies) ? (parsed as { proxies: unknown[] }).proxies : [];
  return rows
    .map((row) => {
      try {
        return normalizeProxyRecord(row);
      } catch {
        return null;
      }
    })
    .filter((row): row is AgentHttpProxyRecord => Boolean(row));
}

async function writeProxyRegistry(workspaceRoot: string, proxies: AgentHttpProxyRecord[]): Promise<void> {
  const registryPath = getProxyRegistryPath(workspaceRoot);
  await mkdir(path.dirname(registryPath), { recursive: true });
  await writeFile(registryPath, `${JSON.stringify({ proxies }, null, 2)}\n`, "utf8");
}

function normalizeLimit(value: unknown, fallback: number): number {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }
  return Math.max(1, Math.min(Math.floor(numeric), 1000));
}

async function readProxyLogs(workspaceRoot: string, proxyId: string, limit: number): Promise<AgentHttpProxyLogRecord[]> {
  const raw = await readFile(getProxyLogsPath(workspaceRoot, proxyId), "utf8").catch(() => "");
  if (!raw) {
    return [];
  }
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .slice(-limit)
    .map((line) => {
      try {
        return normalizeProxyLogRecord(JSON.parse(line) as unknown);
      } catch {
        return null;
      }
    })
    .filter((log): log is AgentHttpProxyLogRecord => Boolean(log));
}

async function createProxy(workspaceRoot: string, request: AgentHttpProxyRpcRequest): Promise<AgentHttpProxyRecord> {
  const proxies = await readProxyRegistry(workspaceRoot);
  const name = normalizeName(request.name);
  const port = normalizePort(request.port);
  const host = normalizeHost(request.host);
  const requestedId = typeof request.proxyId === "string" && request.proxyId.trim()
    ? normalizeProxyId(request.proxyId)
    : slugify(name || `p-${port}`);
  let id = requestedId;
  for (let index = 2; proxies.some((proxy) => proxy.id === id); index += 1) {
    id = `${requestedId.slice(0, 26)}-${index}`;
  }
  const now = new Date().toISOString();
  const proxy: AgentHttpProxyRecord = {
    id,
    name,
    host,
    port,
    enabled: request.enabled !== false,
    createdAt: now,
    updatedAt: now,
  };
  await writeProxyRegistry(workspaceRoot, [...proxies, proxy].sort((a, b) => b.createdAt.localeCompare(a.createdAt)));
  return proxy;
}

async function updateProxy(workspaceRoot: string, request: AgentHttpProxyRpcRequest): Promise<AgentHttpProxyRecord> {
  const proxyId = normalizeProxyId(request.proxyId);
  const proxies = await readProxyRegistry(workspaceRoot);
  const proxy = proxies.find((item) => item.id === proxyId);
  if (!proxy) {
    throw new Error("proxy not found");
  }
  const updated: AgentHttpProxyRecord = {
    ...proxy,
    name: request.name === undefined ? proxy.name : normalizeName(request.name),
    host: request.host === undefined ? proxy.host : normalizeHost(request.host),
    port: request.port === undefined ? proxy.port : normalizePort(request.port),
    enabled: request.enabled === undefined ? proxy.enabled : request.enabled !== false,
    updatedAt: new Date().toISOString(),
  };
  await writeProxyRegistry(workspaceRoot, proxies.map((item) => (item.id === proxyId ? updated : item)));
  return updated;
}

async function deleteProxy(workspaceRoot: string, proxyId: string): Promise<void> {
  const proxies = await readProxyRegistry(workspaceRoot);
  await writeProxyRegistry(workspaceRoot, proxies.filter((proxy) => proxy.id !== proxyId));
  await unlink(getProxyLogsPath(workspaceRoot, proxyId)).catch(() => undefined);
}

async function appendProxyLog(workspaceRoot: string, proxyId: string, log: Omit<AgentHttpProxyLogRecord, "id" | "at">): Promise<void> {
  const entry: AgentHttpProxyLogRecord = {
    id: `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
    at: new Date().toISOString(),
    ...log,
  };
  const logsPath = getProxyLogsPath(workspaceRoot, proxyId);
  await mkdir(path.dirname(logsPath), { recursive: true });
  await appendFile(logsPath, `${JSON.stringify(entry)}\n`, "utf8");
}

async function readProxyLogsForRequest(workspaceRoot: string, request: AgentHttpProxyRpcRequest): Promise<{
  proxy: AgentHttpProxyRecord;
  events: AgentHttpProxyLogRecord[];
}> {
  const proxyId = normalizeProxyId(request.proxyId);
  const proxy = (await readProxyRegistry(workspaceRoot)).find((item) => item.id === proxyId);
  if (!proxy) {
    throw new Error("proxy not found");
  }
  return {
    proxy,
    events: await readProxyLogs(workspaceRoot, proxyId, normalizeLimit(request.limit, MAX_PROXY_LOGS)),
  };
}

async function handleProxyFetch(workspaceRoot: string, request: AgentHttpProxyRpcRequest): Promise<AgentHttpProxyFetchResponse> {
  const proxyId = normalizeProxyId(request.proxyId);
  const proxy = (await readProxyRegistry(workspaceRoot)).find((item) => item.id === proxyId);
  const startedAt = Date.now();
  const method = normalizeMethod(request.method);
  const requestPath = normalizePath(request.path);
  const bodyBase64 = typeof request.bodyBase64 === "string" ? request.bodyBase64 : "";
  const body = bodyBase64 ? Buffer.from(bodyBase64, "base64") : undefined;
  const requestBytes = body?.byteLength ?? 0;
  let status: number | null = null;
  let responseBytes = 0;
  let errorMessage: string | null = null;

  const finishLog = async () => {
    await appendProxyLog(workspaceRoot, proxyId, {
      method,
      path: requestPath,
      status,
      durationMs: Date.now() - startedAt,
      requestBytes,
      responseBytes,
      error: errorMessage,
    }).catch(() => undefined);
  };

  if (!proxy) {
    throw new Error("proxy not found");
  }
  if (!proxy.enabled) {
    errorMessage = "proxy disabled";
    await finishLog();
    throw new Error("proxy disabled");
  }
  const headers = normalizeHeaders(request.headers);
  if ((body?.byteLength ?? 0) > MAX_PROXY_BODY_BYTES) {
    errorMessage = "proxy request body too large";
    await finishLog();
    throw new Error("proxy request body too large");
  }
  try {
    const url = new URL(requestPath, `http://${proxy.host}:${proxy.port}`);
    const response = await fetch(url, {
      method,
      headers,
      body: method === "GET" || method === "HEAD" ? undefined : body,
      redirect: "manual",
    });
    status = response.status;
    const responseBuffer = Buffer.from(await response.arrayBuffer());
    responseBytes = responseBuffer.byteLength;
    if (responseBuffer.byteLength > MAX_PROXY_BODY_BYTES) {
      errorMessage = "proxy response body too large";
      await finishLog();
      throw new Error("proxy response body too large");
    }
    const responseHeaders: Record<string, string> = {};
    response.headers.forEach((value, key) => {
      responseHeaders[key] = value;
    });
    await finishLog();
    return {
      status: response.status,
      statusText: response.statusText,
      headers: responseHeaders,
      bodyBase64: responseBuffer.toString("base64"),
    };
  } catch (error) {
    if (!errorMessage) {
      errorMessage = error instanceof Error ? error.message : String(error);
      await finishLog();
    }
    throw error;
  }
}

async function executeProxyRpc(args: {
  workspaceRoot: string;
  request: AgentHttpProxyRpcRequest;
}): Promise<Record<string, unknown>> {
  const action = args.request.action === "create" || args.request.action === "update" || args.request.action === "delete" || args.request.action === "logs" || args.request.action === "handle"
    ? args.request.action
    : "list";
  if (action === "list") {
    return { ok: true, action, proxies: await readProxyRegistry(args.workspaceRoot) };
  }
  if (action === "create") {
    return { ok: true, action, proxy: await createProxy(args.workspaceRoot, args.request) };
  }
  if (action === "update") {
    return { ok: true, action, proxy: await updateProxy(args.workspaceRoot, args.request) };
  }
  if (action === "delete") {
    await deleteProxy(args.workspaceRoot, normalizeProxyId(args.request.proxyId));
    return { ok: true, action };
  }
  if (action === "logs") {
    return { ok: true, action, ...await readProxyLogsForRequest(args.workspaceRoot, args.request) };
  }
  return { ok: true, action, response: await handleProxyFetch(args.workspaceRoot, args.request) };
}

export async function handleHttpProxyRpcMessage(args: {
  msg: Msg;
  workspaceRoot: string;
  onError?: (message: string) => void;
}): Promise<void> {
  let requestId = "unknown";
  try {
    const request = JSON.parse(proxyRpcCodec.decode(args.msg.data)) as AgentHttpProxyRpcRequest;
    requestId = typeof request.requestId === "string" ? request.requestId : "unknown";
    const payload = await executeProxyRpc({ workspaceRoot: args.workspaceRoot, request });
    args.msg.respond(proxyRpcCodec.encode(JSON.stringify({ requestId, ...payload })));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    args.onError?.(`http proxy rpc failed requestId=${requestId} error=${message}`);
    args.msg.respond(proxyRpcCodec.encode(JSON.stringify({ requestId, ok: false, error: message })));
  }
}

export function subscribeToHttpProxyRpc(args: {
  nc: NatsConnection;
  subject: string;
  workspaceRoot: string;
  onInfo: (message: string) => void;
  onError: (message: string) => void;
}): void {
  args.nc.subscribe(args.subject, {
    callback: (error, msg) => {
      if (error) {
        const message = error instanceof Error ? error.message : String(error);
        args.onError(`http proxy rpc subscription error: ${message}`);
        return;
      }
      void handleHttpProxyRpcMessage({
        msg,
        workspaceRoot: args.workspaceRoot,
        onError: args.onError,
      });
    },
  });
  args.onInfo(`http proxy rpc subscribed subject=${args.subject}`);
}
