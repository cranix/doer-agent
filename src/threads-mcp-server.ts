import path from "node:path";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import * as z from "zod/v4";

function parseWorkspaceRoot(argv: string[]): string {
  const flagIndex = argv.findIndex((token) => token === "--workspace-root");
  const flagValue = flagIndex >= 0 ? argv[flagIndex + 1] : "";
  const envValue = process.env.DOER_THREADS_WORKSPACE_ROOT?.trim() || process.env.WORKSPACE?.trim() || process.cwd();
  return path.resolve((flagValue || envValue || process.cwd()).trim());
}

function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function optionalEnv(name: string): string {
  return process.env[name]?.trim() || "";
}

function getThreadApiConfig(): {
  agentId: string;
  agentToken: string;
  serverBaseUrl: string;
  userId: string;
} {
  const agentId = optionalEnv("DOER_THREADS_AGENT_ID");
  const agentToken = optionalEnv("DOER_AGENT_TOKEN");
  const serverBaseUrl = optionalEnv("DOER_THREADS_SERVER_BASE_URL").replace(/\/$/, "");
  const userId = optionalEnv("DOER_THREADS_USER_ID");
  const missing = [
    agentId ? null : "DOER_THREADS_AGENT_ID",
    agentToken ? null : "DOER_AGENT_TOKEN",
    serverBaseUrl ? null : "DOER_THREADS_SERVER_BASE_URL",
    userId ? null : "DOER_THREADS_USER_ID",
  ].filter((item): item is string => Boolean(item));
  if (missing.length > 0) {
    throw new Error(`thread tools are unavailable; missing ${missing.join(", ")}`);
  }
  return { agentId, agentToken, serverBaseUrl, userId };
}

function codexThreadPath(config: { agentId: string; userId: string }, method: string): string {
  return `/api/users/${encodeURIComponent(config.userId)}/agents/${encodeURIComponent(config.agentId)}/codex/${method}`;
}

async function postDoerJson<T>(pathValue: string, body: Record<string, unknown>, timeoutMs?: number): Promise<T> {
  const config = getThreadApiConfig();
  const response = await fetch(`${config.serverBaseUrl}${pathValue}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.agentToken}`,
      Accept: "application/json",
      "Content-Type": "application/json",
      ...(timeoutMs ? { "x-doer-rpc-timeout-ms": String(timeoutMs) } : {}),
    },
    body: JSON.stringify(body),
  });
  const data = await response.json().catch(() => ({})) as { error?: unknown };
  if (!response.ok) {
    throw new Error(typeof data.error === "string" ? data.error : `Doer server returned ${response.status}`);
  }
  return data as T;
}

function jsonToolResult(result: unknown): {
  content: Array<{ type: "text"; text: string }>;
  structuredContent: Record<string, unknown>;
} {
  return {
    content: [{ type: "text", text: formatJson(result) }],
    structuredContent: result && typeof result === "object" && !Array.isArray(result)
      ? result as Record<string, unknown>
      : { result },
  };
}

function threadListParams(args: {
  archived?: boolean;
  cursor?: string;
  limit?: number;
  searchTerm?: string;
}): Record<string, unknown> {
  const limit = Number.isFinite(args.limit) && args.limit
    ? Math.min(100, Math.max(1, Math.trunc(args.limit)))
    : 50;
  return {
    cursor: args.cursor?.trim() || null,
    limit,
    sortKey: "updated_at",
    sortDirection: "desc",
    sourceKinds: [
      "cli",
      "vscode",
      "exec",
      "appServer",
      "subAgent",
      "subAgentReview",
      "subAgentCompact",
      "subAgentThreadSpawn",
      "subAgentOther",
      "unknown",
    ],
    archived: args.archived ?? false,
    searchTerm: args.searchTerm?.trim() || null,
  };
}

async function archiveThread(threadId: string): Promise<unknown> {
  const config = getThreadApiConfig();
  return postDoerJson<unknown>(
    codexThreadPath(config, "thread/archive"),
    { threadId: threadId.trim() },
    180_000,
  );
}

async function main(): Promise<void> {
  parseWorkspaceRoot(process.argv.slice(2));

  const server = new McpServer({
    name: "doer-threads",
    version: "0.1.0",
  }, {
    capabilities: {
      tools: {},
    },
    instructions: "Start, list, read, close, and archive Codex threads for the current Doer agent.",
  });

  server.registerTool("threads_list", {
    description: "List Codex threads for this Doer agent using the same API as the Doer thread list.",
    inputSchema: {
      archived: z.boolean().optional().describe("Whether to list archived threads. Defaults to false."),
      cursor: z.string().optional().describe("Optional pagination cursor returned by the thread list API."),
      limit: z.number().int().min(1).max(100).optional().describe("Maximum number of threads to return. Defaults to 50."),
      searchTerm: z.string().optional().describe("Optional text search term."),
    },
  }, async ({ archived, cursor, limit, searchTerm }) => {
    const config = getThreadApiConfig();
    const result = await postDoerJson<unknown>(
      codexThreadPath(config, "thread/list"),
      threadListParams({ archived, cursor, limit, searchTerm }),
      180_000,
    );
    return jsonToolResult(result);
  });

  server.registerTool("threads_start", {
    description: "Create a new Codex thread for this Doer agent and start its first turn.",
    inputSchema: {
      prompt: z.string().min(1).describe("User prompt for the first turn in the new thread."),
    },
  }, async ({ prompt }) => {
    const config = getThreadApiConfig();
    const result = await postDoerJson<unknown>(
      codexThreadPath(config, "thread/send"),
      { prompt },
      180_000,
    );
    return jsonToolResult(result);
  });

  server.registerTool("threads_read", {
    description: "Read a Codex thread and its recent turn contents for this Doer agent.",
    inputSchema: {
      threadId: z.string().min(1).describe("Codex thread id to read."),
      cursor: z.string().optional().describe("Optional pagination cursor for turns."),
      limit: z.number().int().min(1).max(100).optional().describe("Maximum number of turns to return. Defaults to 50."),
    },
  }, async ({ threadId, cursor, limit }) => {
    const config = getThreadApiConfig();
    const normalizedThreadId = threadId.trim();
    const turnsLimit = Number.isFinite(limit) && limit ? Math.min(100, Math.max(1, Math.trunc(limit))) : 50;
    const [thread, turns] = await Promise.all([
      postDoerJson<unknown>(
        codexThreadPath(config, "thread/read"),
        { threadId: normalizedThreadId },
        180_000,
      ),
      postDoerJson<unknown>(
        codexThreadPath(config, "thread/turns/list"),
        {
          threadId: normalizedThreadId,
          cursor: cursor?.trim() || null,
          limit: turnsLimit,
          sortDirection: "desc",
        },
        180_000,
      ),
    ]);
    return jsonToolResult({ thread, turns });
  });

  server.registerTool("threads_close", {
    description: "Close a Codex thread for this Doer agent by archiving it with the same API as Doer thread delete.",
    inputSchema: {
      threadId: z.string().min(1).describe("Codex thread id to close."),
    },
  }, async ({ threadId }) => {
    return jsonToolResult(await archiveThread(threadId) ?? { ok: true });
  });

  server.registerTool("threads_archive", {
    description: "Archive a Codex thread for this Doer agent using the same API as Doer thread delete.",
    inputSchema: {
      threadId: z.string().min(1).describe("Codex thread id to archive."),
    },
  }, async ({ threadId }) => {
    return jsonToolResult(await archiveThread(threadId) ?? { ok: true });
  });

  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((error) => {
  const message = error instanceof Error ? error.stack || error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
