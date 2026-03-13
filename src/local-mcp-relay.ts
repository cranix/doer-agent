import fs from "node:fs/promises";
import { homedir } from "node:os";
import path from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

interface PollResponse {
  request: {
    id: string;
    serverKey: string;
    method: string;
    params: Record<string, unknown>;
    timeoutMs: number | null;
    cancelRequested: boolean;
  } | null;
  error?: string;
}

interface RelayConfig {
  servers: RelayServerConfig[];
}

interface RelayServerConfig {
  serverKey: string;
  name?: string;
  command: string;
  args?: string[];
  cwd?: string;
  env?: Record<string, string>;
}

interface ClientEntry {
  client: Client;
  transport: StdioClientTransport;
  tools: string[];
}

interface PendingRelayEvent {
  id: string;
  serverBaseUrl: string;
  userId: string;
  requestId: string;
  type: "status" | "log" | "result";
  clientEventId: string;
  payload: Record<string, unknown>;
  createdAt: string;
  lastAttemptAt: string | null;
  attempts: number;
}

let pendingRelayEventQueueLock = Promise.resolve();
let pendingRelayUploaderWake: (() => void) | null = null;
let pendingRelayUploaderStarted = false;

async function withPendingRelayEventQueueLock<T>(fn: () => Promise<T>): Promise<T> {
  const run = pendingRelayEventQueueLock.then(fn, fn);
  pendingRelayEventQueueLock = run.then(
    () => undefined,
    () => undefined,
  );
  return run;
}

function resolvePendingRelayEventQueuePath(): string {
  const baseDir =
    process.env.DOER_LOCAL_MCP_RELAY_STATE_DIR?.trim() || path.join(homedir(), ".doer-local-mcp-relay");
  return path.join(baseDir, "pending-events.json");
}

async function readPendingRelayEvents(queuePath: string): Promise<PendingRelayEvent[]> {
  try {
    const raw = await fs.readFile(queuePath, "utf8");
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed.filter((row): row is PendingRelayEvent => {
      if (!row || typeof row !== "object") {
        return false;
      }
      const item = row as Record<string, unknown>;
      return (
        typeof item.id === "string" &&
        typeof item.serverBaseUrl === "string" &&
        typeof item.userId === "string" &&
        typeof item.requestId === "string" &&
        (item.type === "status" || item.type === "log" || item.type === "result") &&
        typeof item.clientEventId === "string" &&
        item.payload !== null &&
        typeof item.payload === "object"
      );
    });
  } catch {
    return [];
  }
}

async function writePendingRelayEvents(queuePath: string, events: PendingRelayEvent[]): Promise<void> {
  await fs.mkdir(path.dirname(queuePath), { recursive: true });
  const tmpPath = `${queuePath}.tmp`;
  const handle = await fs.open(tmpPath, "w");
  try {
    await handle.writeFile(JSON.stringify(events), "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  await fs.rename(tmpPath, queuePath);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function writeInfo(message: string): void {
  process.stdout.write(`[doer-local-mcp-relay] ${message}\n`);
}

function writeError(message: string): void {
  process.stderr.write(`[doer-local-mcp-relay] ${message}\n`);
}

function signalPendingRelayUploader(): void {
  pendingRelayUploaderWake?.();
}

async function recordRelayEvent(args: {
  queuePath: string;
  serverBaseUrl: string;
  requestId: string;
  userId: string;
  type: "status" | "log" | "result";
  payload: Record<string, unknown>;
}): Promise<void> {
  await withPendingRelayEventQueueLock(async () => {
    const events = await readPendingRelayEvents(args.queuePath);
    events.push({
      id: `${Date.now()}_${Math.random().toString(36).slice(2, 10)}`,
      serverBaseUrl: args.serverBaseUrl,
      userId: args.userId,
      requestId: args.requestId,
      type: args.type,
      clientEventId: `evt_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`,
      payload: args.payload,
      createdAt: formatLocalTimestamp(),
      lastAttemptAt: null,
      attempts: 0,
    });
    await writePendingRelayEvents(args.queuePath, events);
  });
  signalPendingRelayUploader();
}

async function flushPendingRelayEvents(args: {
  queuePath: string;
  serverBaseUrl: string;
  userId: string;
  relayToken: string;
}): Promise<{ delivered: number; remaining: number }> {
  return withPendingRelayEventQueueLock(async () => {
    const events = await readPendingRelayEvents(args.queuePath);
    if (events.length === 0) {
      return { delivered: 0, remaining: 0 };
    }
    const retained: PendingRelayEvent[] = [];
    let delivered = 0;
    for (let i = 0; i < events.length; i += 1) {
      const event = events[i];
      if (event.serverBaseUrl !== args.serverBaseUrl || event.userId !== args.userId) {
        retained.push(event);
        continue;
      }
      try {
        await emitEvent({
          serverBaseUrl: args.serverBaseUrl,
          requestId: event.requestId,
          userId: args.userId,
          relayToken: args.relayToken,
          type: event.type,
          clientEventId: event.clientEventId,
          payload: event.payload,
        });
        delivered += 1;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        writeError(
          `request=${event.requestId} upload retry type=${event.type} attempt=${event.attempts + 1} reason=${message}`,
        );
        retained.push({
          ...event,
          attempts: event.attempts + 1,
          lastAttemptAt: formatLocalTimestamp(),
        });
        for (let j = i + 1; j < events.length; j += 1) {
          retained.push(events[j]);
        }
        break;
      }
    }
    await writePendingRelayEvents(args.queuePath, retained);
    return { delivered, remaining: retained.length };
  });
}

function startPendingRelayUploader(args: {
  queuePath: string;
  serverBaseUrl: string;
  userId: string;
  relayToken: string;
}): void {
  if (pendingRelayUploaderStarted) {
    return;
  }
  pendingRelayUploaderStarted = true;
  void (async () => {
    while (true) {
      try {
        const result = await flushPendingRelayEvents({
          queuePath: args.queuePath,
          serverBaseUrl: args.serverBaseUrl,
          userId: args.userId,
          relayToken: args.relayToken,
        });
        if (result.delivered > 0) {
          writeInfo(`flushed pending relay events delivered=${result.delivered} remaining=${result.remaining}`);
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        writeError(`pending relay flush failed: ${message}`);
      }

      await new Promise<void>((resolve) => {
        let settled = false;
        const finish = () => {
          if (settled) {
            return;
          }
          settled = true;
          if (pendingRelayUploaderWake === finish) {
            pendingRelayUploaderWake = null;
          }
          resolve();
        };
        pendingRelayUploaderWake = finish;
        setTimeout(finish, 1000).unref?.();
      });
    }
  })();
}

function formatLocalTimestamp(date = new Date()): string {
  const pad = (value: number, size = 2) => String(value).padStart(size, "0");
  const year = date.getFullYear();
  const month = pad(date.getMonth() + 1);
  const day = pad(date.getDate());
  const hours = pad(date.getHours());
  const minutes = pad(date.getMinutes());
  const seconds = pad(date.getSeconds());
  const ms = pad(date.getMilliseconds(), 3);
  const offsetMinutes = -date.getTimezoneOffset();
  const sign = offsetMinutes >= 0 ? "+" : "-";
  const absOffset = Math.abs(offsetMinutes);
  const offsetHour = pad(Math.floor(absOffset / 60));
  const offsetMinute = pad(absOffset % 60);
  return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}.${ms}${sign}${offsetHour}:${offsetMinute}`;
}

function parseArgs(argv: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    if (!key.startsWith("--")) {
      continue;
    }
    const value = argv[i + 1];
    if (typeof value === "string" && !value.startsWith("--")) {
      out[key.slice(2)] = value;
      i += 1;
      continue;
    }
    out[key.slice(2)] = "true";
  }
  return out;
}

async function postJson<T>(url: string, body: unknown): Promise<T> {
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

async function getJson<T>(url: string): Promise<T> {
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

async function emitEvent(args: {
  serverBaseUrl: string;
  requestId: string;
  userId: string;
  relayToken: string;
  type: "status" | "log" | "result";
  clientEventId: string;
  payload: Record<string, unknown>;
}): Promise<void> {
  await postJson(`${args.serverBaseUrl}/api/mcp-relay/requests/${encodeURIComponent(args.requestId)}/events`, {
    userId: args.userId,
    relayToken: args.relayToken,
    type: args.type,
    clientEventId: args.clientEventId,
    payload: args.payload,
  });
}

async function checkCancelRequested(args: {
  serverBaseUrl: string;
  requestId: string;
  userId: string;
  relayToken: string;
}): Promise<boolean> {
  const query = new URLSearchParams({ userId: args.userId, relayToken: args.relayToken });
  const response = await getJson<{ request?: { cancelRequested?: boolean } }>(
    `${args.serverBaseUrl}/api/mcp-relay/requests/${encodeURIComponent(args.requestId)}/events?${query.toString()}`,
  );
  return Boolean(response.request?.cancelRequested);
}

function normalizeToolNames(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const out: string[] = [];
  for (const row of value) {
    if (typeof row !== "string") {
      continue;
    }
    const trimmed = row.trim();
    if (!trimmed) {
      continue;
    }
    out.push(trimmed);
    if (out.length >= 200) {
      break;
    }
  }
  return out;
}

async function loadConfig(configPath: string): Promise<RelayConfig> {
  const raw = await fs.readFile(configPath, "utf8");
  const parsed = JSON.parse(raw) as RelayConfig;
  const servers = Array.isArray(parsed.servers) ? parsed.servers : [];
  const normalized: RelayServerConfig[] = [];
  for (const row of servers) {
    if (!row || typeof row !== "object") {
      continue;
    }
    const serverKey = typeof row.serverKey === "string" ? row.serverKey.trim().toLowerCase() : "";
    const command = typeof row.command === "string" ? row.command.trim() : "";
    if (!serverKey || !command) {
      continue;
    }
    normalized.push({
      serverKey,
      name: typeof row.name === "string" && row.name.trim() ? row.name.trim() : serverKey,
      command,
      args: Array.isArray(row.args) ? row.args.filter((value) => typeof value === "string") : [],
      cwd: typeof row.cwd === "string" ? row.cwd : undefined,
      env:
        row.env && typeof row.env === "object"
          ? Object.fromEntries(
              Object.entries(row.env).filter(
                (entry): entry is [string, string] => typeof entry[0] === "string" && typeof entry[1] === "string",
              ),
            )
          : undefined,
    });
  }

  const unique = new Map<string, RelayServerConfig>();
  for (const row of normalized) {
    unique.set(row.serverKey, row);
  }
  return { servers: Array.from(unique.values()) };
}

async function ensureClient(
  server: RelayServerConfig,
  clients: Map<string, ClientEntry>,
): Promise<ClientEntry> {
  const existing = clients.get(server.serverKey);
  if (existing) {
    return existing;
  }

  const transport = new StdioClientTransport({
    command: server.command,
    args: Array.isArray(server.args) ? server.args : [],
    cwd: server.cwd,
    env: Object.fromEntries(
      Object.entries({
        ...process.env,
        ...(server.env || {}),
      }).filter((entry): entry is [string, string] => typeof entry[1] === "string"),
    ),
  });
  const client = new Client(
    {
      name: "doer-local-mcp-relay",
      version: "0.1.0",
    },
    {
      capabilities: {},
    },
  );

  await client.connect(transport);
  const listed = await client.listTools();
  const tools = normalizeToolNames(listed.tools?.map((row) => row.name));
  const entry: ClientEntry = {
    client,
    transport,
    tools,
  };
  clients.set(server.serverKey, entry);
  return entry;
}

async function refreshAdvertisedTools(args: {
  servers: RelayServerConfig[];
  clients: Map<string, ClientEntry>;
}): Promise<void> {
  for (const server of args.servers) {
    try {
      const entry = await ensureClient(server, args.clients);
      if (entry.tools.length > 0) {
        continue;
      }
      const listed = await entry.client.listTools();
      entry.tools = normalizeToolNames(listed.tools?.map((row) => row.name));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      writeError(`failed to refresh tools server=${server.serverKey} reason=${message}`);
    }
  }
}

async function runRequest(args: {
  serverBaseUrl: string;
  userId: string;
  relayToken: string;
  pendingEventQueuePath: string;
  request: NonNullable<PollResponse["request"]>;
  serverMap: Map<string, RelayServerConfig>;
  clients: Map<string, ClientEntry>;
}): Promise<void> {
  const request = args.request;

  await recordRelayEvent({
    queuePath: args.pendingEventQueuePath,
    serverBaseUrl: args.serverBaseUrl,
    requestId: request.id,
    userId: args.userId,
    type: "log",
    payload: {
      message: "request started",
      at: formatLocalTimestamp(),
      serverKey: request.serverKey,
      method: request.method,
    },
  });

  const cancelRequested = await checkCancelRequested({
    serverBaseUrl: args.serverBaseUrl,
    requestId: request.id,
    userId: args.userId,
    relayToken: args.relayToken,
  }).catch(() => false);
  if (cancelRequested) {
    await recordRelayEvent({
      queuePath: args.pendingEventQueuePath,
      serverBaseUrl: args.serverBaseUrl,
      requestId: request.id,
      userId: args.userId,
      type: "status",
      payload: {
        status: "canceled",
        error: "canceled before execution",
      },
    });
    return;
  }

  const target = args.serverMap.get(request.serverKey);
  if (!target) {
    await recordRelayEvent({
      queuePath: args.pendingEventQueuePath,
      serverBaseUrl: args.serverBaseUrl,
      requestId: request.id,
      userId: args.userId,
      type: "status",
      payload: {
        status: "failed",
        error: `Unknown serverKey: ${request.serverKey}`,
      },
    });
    return;
  }

  try {
    const clientEntry = await ensureClient(target, args.clients);
    let resultPayload: Record<string, unknown>;

    if (request.method === "tools/list") {
      const listed = await clientEntry.client.listTools();
      const toolNames = normalizeToolNames(listed.tools?.map((row) => row.name));
      clientEntry.tools = toolNames;
      resultPayload = {
        tools: listed.tools ?? [],
      };
    } else if (request.method === "tools/call") {
      const name = typeof request.params?.name === "string" ? request.params.name.trim() : "";
      const toolArgs =
        request.params && typeof request.params.arguments === "object" && request.params.arguments
          ? (request.params.arguments as Record<string, unknown>)
          : {};
      if (!name) {
        throw new Error("tools/call requires params.name");
      }
      const called = await clientEntry.client.callTool({
        name,
        arguments: toolArgs,
      });
      resultPayload = {
        content: Array.isArray(called.content) ? called.content : [],
        structuredContent:
          called.structuredContent && typeof called.structuredContent === "object" ? called.structuredContent : null,
        isError: Boolean(called.isError),
      };
    } else {
      throw new Error(`Unsupported method: ${request.method}`);
    }

    await recordRelayEvent({
      queuePath: args.pendingEventQueuePath,
      serverBaseUrl: args.serverBaseUrl,
      requestId: request.id,
      userId: args.userId,
      type: "result",
      payload: resultPayload,
    });
    await recordRelayEvent({
      queuePath: args.pendingEventQueuePath,
      serverBaseUrl: args.serverBaseUrl,
      requestId: request.id,
      userId: args.userId,
      type: "status",
      payload: {
        status: "completed",
      },
    });
    writeInfo(`request=${request.id} status=completed server=${request.serverKey} method=${request.method}`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await recordRelayEvent({
      queuePath: args.pendingEventQueuePath,
      serverBaseUrl: args.serverBaseUrl,
      requestId: request.id,
      userId: args.userId,
      type: "status",
      payload: {
        status: "failed",
        error: message,
      },
    }).catch(() => undefined);
    writeError(`request=${request.id} status=failed reason=${message}`);
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const serverBaseUrlRaw = args.server || args.url || "http://localhost:2020";
  const serverBaseUrl = serverBaseUrlRaw.replace(/\/$/, "");
  const userId = (args["user-id"] || args.userId || "").trim();
  const relaySecret = (args["relay-secret"] || args.relaySecret || "").trim();
  const defaultConfigName = path.join("agent", "local-mcp-relay.config.json");
  const configPath = path.resolve(process.cwd(), args.config || defaultConfigName);

  if (!userId || !relaySecret) {
    throw new Error("user-id and relay-secret are required");
  }

  const relayToken = relaySecret;
  const pendingEventQueuePath = resolvePendingRelayEventQueuePath();
  let config = await loadConfig(configPath);
  let serverMap = new Map<string, RelayServerConfig>(config.servers.map((row) => [row.serverKey, row]));
  const clients = new Map<string, ClientEntry>();

  writeInfo(`server=${serverBaseUrl}`);
  writeInfo(`userId=${userId}`);
  writeInfo(`config=${configPath}`);
  writeInfo(`loaded servers=${config.servers.length}`);
  writeInfo(
    `start example: npm run relay:mcp -- --server ${serverBaseUrl} --user-id ${userId} --relay-secret <SECRET> --config ${path.basename(configPath)}`,
  );

  startPendingRelayUploader({
    queuePath: pendingEventQueuePath,
    serverBaseUrl,
    userId,
    relayToken,
  });

  let isPollConnected = false;
  let hasConnectedBefore = false;
  let consecutivePollErrors = 0;

  while (true) {
    try {
      if (config.servers.length === 0) {
        writeError("no servers in config. waiting 5s.");
        await sleep(5000);
        config = await loadConfig(configPath).catch(() => config);
        serverMap = new Map<string, RelayServerConfig>(config.servers.map((row) => [row.serverKey, row]));
        continue;
      }

      await refreshAdvertisedTools({
        servers: config.servers,
        clients,
      });

      const advertised = config.servers.map((row) => ({
        serverKey: row.serverKey,
        name: row.name || row.serverKey,
        toolNames: clients.get(row.serverKey)?.tools || [],
      }));

      const polled = await postJson<PollResponse>(`${serverBaseUrl}/api/mcp-relay/poll`, {
        userId,
        relayToken,
        waitMs: 25000,
        servers: advertised,
      });

      consecutivePollErrors = 0;
      if (!isPollConnected) {
        const statusText = hasConnectedBefore ? "reconnected" : "connected";
        writeInfo(`${statusText} to server (poll ok) at=${formatLocalTimestamp()} userId=${userId} server=${serverBaseUrl}`);
        hasConnectedBefore = true;
      }
      isPollConnected = true;

      const request = polled.request;
      if (!request) {
        continue;
      }

      writeInfo(`run request=${request.id} server=${request.serverKey} method=${request.method}`);
      await runRequest({
        serverBaseUrl,
        userId,
        relayToken,
        pendingEventQueuePath,
        request,
        serverMap,
        clients,
      });

      config = await loadConfig(configPath).catch(() => config);
      serverMap = new Map<string, RelayServerConfig>(config.servers.map((row) => [row.serverKey, row]));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      consecutivePollErrors += 1;
      if (isPollConnected) {
        writeError(`disconnected from server at=${formatLocalTimestamp()} reason=${message}`);
      }
      isPollConnected = false;
      writeError(`poll loop error: ${message} (retry in 2s, attempt=${consecutivePollErrors})`);
      await sleep(2000);
    }
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  writeError(`fatal: ${message}`);
  process.exitCode = 1;
});
