import path from "node:path";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { createPool, type FieldPacket, type Pool as MySqlPool, type ResultSetHeader, type RowDataPacket } from "mysql2/promise";
import { Pool as PostgresPool } from "pg";
import * as z from "zod/v4";
import {
  getAgentDatabaseConnectionById,
  readAgentSettingsConfig,
  resolveAgentDatabaseConnectionUrl,
  type AgentDatabaseConnectionConfig,
  type AgentSettingsConfig,
} from "./agent-settings.js";

const DEFAULT_ROW_LIMIT = 200;
const MAX_ROW_LIMIT = 1000;
const DEFAULT_TIMEOUT_MS = 10_000;
const MAX_TIMEOUT_MS = 30_000;

const postgresPoolByKey = new Map<string, PostgresPool>();
const mysqlPoolByKey = new Map<string, MySqlPool>();

type QueryResult = {
  connectionId: string;
  rowCount: number;
  rows: Record<string, unknown>[];
  fields: string[];
  truncated: boolean;
  readOnly: boolean;
};

function parseWorkspaceRoot(argv: string[]): string {
  const flagIndex = argv.findIndex((token) => token === "--workspace-root");
  const flagValue = flagIndex >= 0 ? argv[flagIndex + 1] : "";
  const envValue = process.env.DOER_DB_WORKSPACE_ROOT?.trim() || process.env.WORKSPACE?.trim() || process.cwd();
  return path.resolve((flagValue || envValue || process.cwd()).trim());
}

function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function clampNumber(value: number | undefined, fallback: number, max: number): number {
  if (!Number.isFinite(value)) {
    return fallback;
  }
  return Math.max(1, Math.min(max, Math.trunc(value as number)));
}

function normalizeSql(sql: string): string {
  const trimmed = sql.trim().replace(/;+$/g, "").trim();
  if (!trimmed) {
    throw new Error("sql is required");
  }
  if (trimmed.includes(";")) {
    throw new Error("Multiple SQL statements are not supported");
  }
  return trimmed;
}

function requireConnection(args: {
  settings: AgentSettingsConfig;
  connectionId?: string;
}): AgentDatabaseConnectionConfig {
  const enabledConnections = args.settings.databases.connections.filter((connection) => connection.enabled);
  const requestedId = args.connectionId?.trim() || args.settings.databases.defaultConnectionId || enabledConnections[0]?.id || "";
  const connection = getAgentDatabaseConnectionById(args.settings, requestedId);
  if (!connection || !connection.enabled) {
    throw new Error(
      requestedId
        ? `Database connection not found or disabled: ${requestedId}`
        : "No enabled database connections are configured",
    );
  }
  return connection;
}

function requireConnectionUrl(connection: AgentDatabaseConnectionConfig): string {
  const url = resolveAgentDatabaseConnectionUrl(connection);
  if (!url) {
    if (connection.connection.mode === "env") {
      throw new Error(`Database URL env is missing: ${connection.connection.urlEnv}`);
    }
    throw new Error(`Database URL is missing for connection: ${connection.id}`);
  }
  return url;
}

function getPostgresPool(connection: AgentDatabaseConnectionConfig): PostgresPool {
  const connectionUrl = requireConnectionUrl(connection);
  const key = `${connection.provider}:${connection.id}:${connectionUrl}`;
  const existing = postgresPoolByKey.get(key);
  if (existing) {
    return existing;
  }
  const pool = new PostgresPool({
    connectionString: connectionUrl,
    max: 4,
  });
  postgresPoolByKey.set(key, pool);
  return pool;
}

function getMysqlPool(connection: AgentDatabaseConnectionConfig): MySqlPool {
  const connectionUrl = requireConnectionUrl(connection);
  const key = `${connection.provider}:${connection.id}:${connectionUrl}`;
  const existing = mysqlPoolByKey.get(key);
  if (existing) {
    return existing;
  }
  const pool = createPool({
    uri: connectionUrl,
    connectionLimit: 4,
    namedPlaceholders: false,
    multipleStatements: false,
  });
  mysqlPoolByKey.set(key, pool);
  return pool;
}

function serializeRow(row: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(row));
}

async function runPostgresSql(args: {
  connection: AgentDatabaseConnectionConfig;
  sql: string;
  rowLimit?: number;
  timeoutMs?: number;
}): Promise<QueryResult> {
  const pool = getPostgresPool(args.connection);
  const client = await pool.connect();
  const rowLimit = clampNumber(args.rowLimit, DEFAULT_ROW_LIMIT, MAX_ROW_LIMIT);
  const timeoutMs = clampNumber(args.timeoutMs, DEFAULT_TIMEOUT_MS, MAX_TIMEOUT_MS);
  let inTransaction = false;

  try {
    await client.query("BEGIN");
    inTransaction = true;
    await client.query(`SET LOCAL statement_timeout = ${timeoutMs}`);
    if (args.connection.readOnly) {
      await client.query("SET TRANSACTION READ ONLY");
    }
    const result = await client.query(normalizeSql(args.sql));
    await client.query("COMMIT");
    inTransaction = false;
    return {
      connectionId: args.connection.id,
      rowCount: result.rowCount ?? result.rows.length,
      rows: result.rows.slice(0, rowLimit).map((row) => serializeRow(row)),
      fields: result.fields.map((field) => field.name),
      truncated: result.rows.length > rowLimit,
      readOnly: args.connection.readOnly,
    };
  } catch (error) {
    if (inTransaction) {
      await client.query("ROLLBACK").catch(() => undefined);
    }
    throw error;
  } finally {
    client.release();
  }
}

async function runMysqlSql(args: {
  connection: AgentDatabaseConnectionConfig;
  sql: string;
  rowLimit?: number;
  timeoutMs?: number;
}): Promise<QueryResult> {
  const pool = getMysqlPool(args.connection);
  const client = await pool.getConnection();
  const rowLimit = clampNumber(args.rowLimit, DEFAULT_ROW_LIMIT, MAX_ROW_LIMIT);
  const timeoutMs = clampNumber(args.timeoutMs, DEFAULT_TIMEOUT_MS, MAX_TIMEOUT_MS);
  let inTransaction = false;

  try {
    await client.query(args.connection.readOnly ? "START TRANSACTION READ ONLY" : "START TRANSACTION");
    inTransaction = true;
    const [rows, fields] = await client.query({
      sql: normalizeSql(args.sql),
      timeout: timeoutMs,
    });
    await client.query("COMMIT");
    inTransaction = false;

    if (Array.isArray(rows)) {
      const rowPackets = rows as RowDataPacket[];
      const fieldPackets = Array.isArray(fields) ? (fields as FieldPacket[]) : [];
      return {
        connectionId: args.connection.id,
        rowCount: rowPackets.length,
        rows: rowPackets.slice(0, rowLimit).map((row) => serializeRow(row as Record<string, unknown>)),
        fields: fieldPackets.map((field) => field.name),
        truncated: rowPackets.length > rowLimit,
        readOnly: args.connection.readOnly,
      };
    }

    const result = rows as ResultSetHeader;
    return {
      connectionId: args.connection.id,
      rowCount: typeof result.affectedRows === "number" ? result.affectedRows : 0,
      rows: [],
      fields: [],
      truncated: false,
      readOnly: args.connection.readOnly,
    };
  } catch (error) {
    if (inTransaction) {
      await client.query("ROLLBACK").catch(() => undefined);
    }
    throw error;
  } finally {
    client.release();
  }
}

async function runSql(args: {
  connection: AgentDatabaseConnectionConfig;
  sql: string;
  rowLimit?: number;
  timeoutMs?: number;
}): Promise<QueryResult> {
  if (args.connection.provider === "mysql") {
    return runMysqlSql(args);
  }
  return runPostgresSql(args);
}

async function listTables(connection: AgentDatabaseConnectionConfig, schema?: string): Promise<Array<{ schema: string; name: string }>> {
  const normalizedSchema = schema?.trim();
  if (connection.provider === "mysql") {
    const pool = getMysqlPool(connection);
    const [rows] = normalizedSchema
      ? await pool.query<RowDataPacket[]>(
          `
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = ? AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
          `,
          [normalizedSchema],
        )
      : await pool.query<RowDataPacket[]>(
          `
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
          `,
        );
    return rows.map((row) => ({
      schema: String(row.table_schema),
      name: String(row.table_name),
    }));
  }

  const pool = getPostgresPool(connection);
  const result = normalizedSchema
    ? await pool.query(
        `
          SELECT table_schema, table_name
          FROM information_schema.tables
          WHERE table_schema = $1 AND table_type = 'BASE TABLE'
          ORDER BY table_schema, table_name
        `,
        [normalizedSchema],
      )
    : await pool.query(
        `
          SELECT table_schema, table_name
          FROM information_schema.tables
          WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            AND table_type = 'BASE TABLE'
          ORDER BY table_schema, table_name
        `,
      );
  return result.rows.map((row) => ({
    schema: String(row.table_schema),
    name: String(row.table_name),
  }));
}

async function describeTable(args: {
  connection: AgentDatabaseConnectionConfig;
  schema: string;
  table: string;
}): Promise<Array<{ name: string; dataType: string; nullable: boolean; defaultValue: string | null }>> {
  const schema = args.schema.trim();
  const table = args.table.trim();
  if (args.connection.provider === "mysql") {
    const pool = getMysqlPool(args.connection);
    const [rows] = await pool.query<RowDataPacket[]>(
      `
        SELECT column_name, column_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
      `,
      [schema, table],
    );
    return rows.map((row) => ({
      name: String(row.column_name),
      dataType: String(row.column_type),
      nullable: String(row.is_nullable) === "YES",
      defaultValue: row.column_default === null ? null : String(row.column_default),
    }));
  }

  const pool = getPostgresPool(args.connection);
  const result = await pool.query(
    `
      SELECT column_name, data_type, is_nullable, column_default
      FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position
    `,
    [schema, table],
  );
  return result.rows.map((row) => ({
    name: String(row.column_name),
    dataType: String(row.data_type),
    nullable: String(row.is_nullable) === "YES",
    defaultValue: row.column_default === null ? null : String(row.column_default),
  }));
}

async function main(): Promise<void> {
  const workspaceRoot = parseWorkspaceRoot(process.argv.slice(2));

  const server = new McpServer({
    name: "doer-database",
    version: "0.1.0",
  }, {
    capabilities: {
      tools: {},
    },
    instructions: "Inspect configured PostgreSQL or MySQL databases and run SQL queries through named connections.",
  });

  server.registerTool("db_list_connections", {
    description: "List configured database connections available to the local agent.",
    inputSchema: {},
  }, async () => {
    const settings = await readAgentSettingsConfig({ workspaceRoot });
    const connections = settings.databases.connections
      .filter((connection) => connection.enabled)
      .map((connection) => ({
        id: connection.id,
        description: connection.description,
        provider: connection.provider,
        readOnly: connection.readOnly,
        default: settings.databases.defaultConnectionId === connection.id,
        configuredVia: connection.connection.mode,
        urlResolved: Boolean(resolveAgentDatabaseConnectionUrl(connection)),
      }));
    return {
      content: [
        {
          type: "text",
          text: formatJson({ connections }),
        },
      ],
      structuredContent: { connections },
    };
  });

  server.registerTool("db_list_tables", {
    description: "List tables for a configured database connection.",
    inputSchema: {
      connectionId: z.string().optional().describe("Database connection id. Defaults to the configured default connection."),
      schema: z.string().optional().describe("Optional schema or database name to filter by."),
    },
  }, async ({ connectionId, schema }) => {
    const settings = await readAgentSettingsConfig({ workspaceRoot });
    const connection = requireConnection({ settings, connectionId });
    const tables = await listTables(connection, schema);
    return {
      content: [
        {
          type: "text",
          text: formatJson({ connectionId: connection.id, provider: connection.provider, tables }),
        },
      ],
      structuredContent: { connectionId: connection.id, provider: connection.provider, tables },
    };
  });

  server.registerTool("db_describe_table", {
    description: "Describe the columns for a specific table.",
    inputSchema: {
      connectionId: z.string().optional().describe("Database connection id. Defaults to the configured default connection."),
      schema: z.string().min(1).describe("Schema name for PostgreSQL, or database name for MySQL."),
      table: z.string().min(1).describe("Table name."),
    },
  }, async ({ connectionId, schema, table }) => {
    const settings = await readAgentSettingsConfig({ workspaceRoot });
    const connection = requireConnection({ settings, connectionId });
    const columns = await describeTable({
      connection,
      schema,
      table,
    });
    return {
      content: [
        {
          type: "text",
          text: formatJson({ connectionId: connection.id, provider: connection.provider, schema: schema.trim(), table: table.trim(), columns }),
        },
      ],
      structuredContent: {
        connectionId: connection.id,
        provider: connection.provider,
        schema: schema.trim(),
        table: table.trim(),
        columns,
      },
    };
  });

  server.registerTool("db_query", {
    description: "Run a SQL query through a configured database connection.",
    inputSchema: {
      connectionId: z.string().optional().describe("Database connection id. Defaults to the configured default connection."),
      sql: z.string().min(1).describe("A single SQL statement to execute."),
      rowLimit: z.number().int().min(1).max(MAX_ROW_LIMIT).optional().describe("Maximum number of rows to return."),
      timeoutMs: z.number().int().min(1).max(MAX_TIMEOUT_MS).optional().describe("Statement timeout in milliseconds."),
    },
  }, async ({ connectionId, sql, rowLimit, timeoutMs }) => {
    const settings = await readAgentSettingsConfig({ workspaceRoot });
    const connection = requireConnection({ settings, connectionId });
    const result = await runSql({
      connection,
      sql,
      rowLimit,
      timeoutMs,
    });
    return {
      content: [
        {
          type: "text",
          text: formatJson({ ...result, provider: connection.provider }),
        },
      ],
      structuredContent: { ...result, provider: connection.provider },
    };
  });

  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((error) => {
  const message = error instanceof Error ? error.stack || error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
