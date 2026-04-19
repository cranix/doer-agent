import path from "node:path";
import { fileURLToPath } from "node:url";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import * as z from "zod/v4";
import {
  deleteAgentDaemonLocal,
  listAgentDaemonsLocal,
  readAgentDaemonLogsLocal,
  restartAgentDaemonLocal,
  startAgentDaemonLocal,
  stopAgentDaemonLocal,
} from "./agent-daemon-rpc.js";
import { readAgentSettingsConfig } from "./agent-settings.js";
import { createRuntimeEnvHelpers } from "./agent-runtime-env.js";

const MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const AGENT_PROJECT_DIR = path.join(MODULE_DIR, "..");

function parseWorkspaceRoot(argv: string[]): string {
  const flagIndex = argv.findIndex((token) => token === "--workspace-root");
  const flagValue = flagIndex >= 0 ? argv[flagIndex + 1] : "";
  const envValue = process.env.DOER_DAEMON_WORKSPACE_ROOT?.trim() || process.env.WORKSPACE?.trim() || process.cwd();
  return path.resolve((flagValue || envValue || process.cwd()).trim());
}

function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

async function main(): Promise<void> {
  const workspaceRoot = parseWorkspaceRoot(process.argv.slice(2));
  const runtimeEnvHelpers = createRuntimeEnvHelpers({
    resolveWorkspaceRoot: () => workspaceRoot,
    agentProjectDir: AGENT_PROJECT_DIR,
  });

  const server = new McpServer({
    name: "doer-daemon",
    version: "0.1.0",
  }, {
    capabilities: {
      tools: {},
    },
    instructions: "Manage long-lived workspace daemons. Use these tools to list, start, stop, and inspect daemon logs.",
  });

  server.registerTool("daemon_list", {
    description: "List daemons managed for the current workspace.",
    inputSchema: {},
  }, async () => {
    const daemons = await listAgentDaemonsLocal(workspaceRoot);
    return {
      content: [
        {
          type: "text",
          text: formatJson({ daemons }),
        },
      ],
      structuredContent: { daemons },
    };
  });

  server.registerTool("daemon_start", {
    description: "Start a new long-lived daemon process for the current workspace.",
    inputSchema: {
      command: z.string().min(1).describe("Shell command to run, such as `npm run dev`."),
      cwd: z.string().optional().describe("Optional working directory relative to the workspace root."),
      label: z.string().optional().describe("Optional UI label for the daemon."),
    },
  }, async ({ command, cwd, label }) => {
    const daemon = await startAgentDaemonLocal({
      workspaceRoot,
      agentProjectDir: AGENT_PROJECT_DIR,
      request: {
        command,
        cwd: cwd ?? ".",
        label,
      },
      resolveShellPath: runtimeEnvHelpers.resolveShellPath,
      resolveTaskWorkspace: runtimeEnvHelpers.resolveTaskWorkspace,
      readAgentSettingsConfig,
    });
    return {
      content: [
        {
          type: "text",
          text: formatJson({ daemon }),
        },
      ],
      structuredContent: { daemon },
    };
  });

  server.registerTool("daemon_stop", {
    description: "Stop a running daemon by id.",
    inputSchema: {
      id: z.string().min(1).describe("Daemon id returned by daemon_list or daemon_start."),
    },
  }, async ({ id }) => {
    const daemon = await stopAgentDaemonLocal(workspaceRoot, id);
    return {
      content: [
        {
          type: "text",
          text: formatJson({ daemon }),
        },
      ],
      structuredContent: { daemon },
    };
  });

  server.registerTool("daemon_restart", {
    description: "Restart a daemon by id.",
    inputSchema: {
      id: z.string().min(1).describe("Daemon id returned by daemon_list or daemon_start."),
    },
  }, async ({ id }) => {
    const daemon = await restartAgentDaemonLocal({
      workspaceRoot,
      agentProjectDir: AGENT_PROJECT_DIR,
      daemonId: id,
      resolveShellPath: runtimeEnvHelpers.resolveShellPath,
      readAgentSettingsConfig,
    });
    return {
      content: [
        {
          type: "text",
          text: formatJson({ daemon }),
        },
      ],
      structuredContent: { daemon },
    };
  });

  server.registerTool("daemon_delete", {
    description: "Delete a daemon by id, stopping it first if needed.",
    inputSchema: {
      id: z.string().min(1).describe("Daemon id returned by daemon_list or daemon_start."),
    },
  }, async ({ id }) => {
    await deleteAgentDaemonLocal(workspaceRoot, id);
    return {
      content: [
        {
          type: "text",
          text: formatJson({ deleted: true, daemonId: id }),
        },
      ],
      structuredContent: { deleted: true, daemonId: id },
    };
  });

  server.registerTool("daemon_logs", {
    description: "Read recent tail log events for a daemon.",
    inputSchema: {
      id: z.string().min(1).describe("Daemon id returned by daemon_list or daemon_start."),
      limit: z.number().int().min(1).max(1000).optional().describe("Maximum number of recent log lines to read."),
    },
  }, async ({ id, limit }) => {
    const logs = await readAgentDaemonLogsLocal({
      workspaceRoot,
      daemonId: id,
      limit,
    });
    return {
      content: [
        {
          type: "text",
          text: formatJson(logs),
        },
      ],
      structuredContent: logs,
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
