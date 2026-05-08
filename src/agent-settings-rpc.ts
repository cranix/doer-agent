import { readFile } from "node:fs/promises";
import { createConnection as createMysqlConnection, type RowDataPacket } from "mysql2/promise";
import { StringCodec, type Msg, type NatsConnection } from "nats";
import { Client as PostgresClient } from "pg";
import {
  normalizeAgentDatabaseConnection,
  normalizeAgentSettingsConfig,
  normalizeAgentSettingsPatch,
  readAgentSettingsConfig,
  resolveAgentDatabaseConnectionUrl,
  resolveAgentSettingsFilePath,
  toAgentSettingsPublic,
  writeAgentModelInstructions,
  writeAgentSettingsConfig,
  type AgentDatabaseConnectionConfig,
  type AgentSettingsConfig,
  type AgentSettingsPublic,
} from "./agent-settings.js";

const settingsRpcCodec = StringCodec();

export type AgentSettingsRpcAction = "get" | "update" | "test_database_connection";

export interface AgentSettingsRpcRequest {
  requestId?: unknown;
  responseSubject?: unknown;
  agentId?: unknown;
  action?: unknown;
  patch?: unknown;
  defaults?: unknown;
  connection?: unknown;
}

export interface AgentSettingsRpcResponse {
  requestId: string;
  ok: boolean;
  settings?: AgentSettingsPublic;
  testResult?: {
    ok: true;
    provider: "postgres" | "mysql";
    message: string;
    latencyMs: number;
    database: string | null;
    serverVersion: string | null;
  };
  error?: string;
}

interface NormalizedSettingsRpcRequest {
  requestId: string;
  responseSubject: string;
  action: AgentSettingsRpcAction;
  patch: Record<string, unknown>;
  defaults: AgentSettingsConfig | null;
  connection: AgentDatabaseConnectionConfig | null;
}

function normalizeSettingsRpcRequest(args: {
  request: AgentSettingsRpcRequest;
  agentId: string;
}): NormalizedSettingsRpcRequest {
  const requestId = typeof args.request.requestId === "string" ? args.request.requestId.trim() : "";
  const responseSubject = typeof args.request.responseSubject === "string" ? args.request.responseSubject.trim() : "";
  const requestAgentId = typeof args.request.agentId === "string" ? args.request.agentId.trim() : "";
  const action =
    args.request.action === "update"
      ? "update"
      : args.request.action === "test_database_connection"
        ? "test_database_connection"
        : "get";
  if (!requestId || !responseSubject || !requestAgentId || requestAgentId !== args.agentId) {
    throw new Error("invalid settings rpc request");
  }
  const connection = args.request.connection === undefined ? null : normalizeAgentDatabaseConnection(args.request.connection);
  if (action === "test_database_connection" && !connection) {
    throw new Error("invalid database connection test payload");
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
    connection,
  };
}

function publishSettingsRpcResponse(args: {
  nc: NatsConnection;
  responseSubject: string;
  payload: AgentSettingsRpcResponse;
}): void {
  args.nc.publish(args.responseSubject, settingsRpcCodec.encode(JSON.stringify(args.payload)));
}

async function testDatabaseConnection(connection: AgentDatabaseConnectionConfig): Promise<NonNullable<AgentSettingsRpcResponse["testResult"]>> {
  const connectionUrl = resolveAgentDatabaseConnectionUrl(connection);
  if (!connectionUrl) {
    if (connection.connection.mode === "env") {
      throw new Error(`Database URL env is missing: ${connection.connection.urlEnv}`);
    }
    throw new Error(`Database URL is missing for connection: ${connection.id}`);
  }

  const startedAt = Date.now();

  if (connection.provider === "mysql") {
    const client = await createMysqlConnection({
      uri: connectionUrl,
      connectTimeout: 5_000,
    });
    try {
      const [rows] = await client.query<RowDataPacket[]>(
        "SELECT DATABASE() AS database_name, VERSION() AS version",
      );
      const firstRow = Array.isArray(rows)
        ? (rows[0] as { database_name?: string | null; version?: string | null } | undefined)
        : undefined;
      const latencyMs = Date.now() - startedAt;
      const database = firstRow?.database_name ?? null;
      const serverVersion = firstRow?.version ?? null;
      return {
        ok: true,
        provider: connection.provider,
        message: `Connected to MySQL${database ? ` (${database})` : ""} in ${latencyMs}ms`,
        latencyMs,
        database,
        serverVersion,
      };
    } finally {
      await client.end().catch(() => undefined);
    }
  }

  const client = new PostgresClient({
    connectionString: connectionUrl,
    connectionTimeoutMillis: 5_000,
  });
  await client.connect();
  try {
    const result = await client.query<{ database_name: string | null; version: string | null }>(
      "SELECT current_database() AS database_name, version() AS version",
    );
    const firstRow = result.rows[0] ?? { database_name: null, version: null };
    const latencyMs = Date.now() - startedAt;
    return {
      ok: true,
      provider: connection.provider,
      message: `Connected to PostgreSQL${firstRow.database_name ? ` (${firstRow.database_name})` : ""} in ${latencyMs}ms`,
      latencyMs,
      database: firstRow.database_name,
      serverVersion: firstRow.version,
    };
  } finally {
    await client.end().catch(() => undefined);
  }
}

export async function handleSettingsRpcMessage(args: {
  msg: Msg;
  nc: NatsConnection;
  agentId: string;
  workspaceRoot: string;
  onSettingsUpdated?: () => Promise<void> | void;
  onError: (message: string) => void;
}): Promise<void> {
  let requestId = "unknown";
  let responseSubject = "";
  let didUpdateSettings = false;
  try {
    const payload = JSON.parse(settingsRpcCodec.decode(args.msg.data)) as AgentSettingsRpcRequest;
    const request = normalizeSettingsRpcRequest({ request: payload, agentId: args.agentId });
    requestId = request.requestId;
    responseSubject = request.responseSubject;
    if (request.action === "test_database_connection") {
      publishSettingsRpcResponse({
        nc: args.nc,
        responseSubject,
        payload: {
          requestId,
          ok: true,
          testResult: await testDatabaseConnection(request.connection!),
        },
      });
      return;
    }
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
      didUpdateSettings = true;
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
    if (didUpdateSettings) {
      await Promise.resolve(args.onSettingsUpdated?.()).catch((error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        args.onError(`settings update hook failed requestId=${requestId} error=${message}`);
      });
    }
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
