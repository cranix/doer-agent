import { spawn } from "node:child_process";
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
import {
  agentNotesCapabilitiesLocal,
  createAgentNoteLocal,
  deleteAgentNoteLocal,
  getAgentNoteLocal,
  listAgentNotesLocal,
  renameAgentNoteLocal,
  saveAgentNoteLocal,
} from "./agent-notes-local.js";

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

function runSearch(command: string, args: string[], cwd: string): Promise<{ code: number; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { cwd, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk: string) => {
      stderr += chunk;
    });
    child.once("error", reject);
    child.once("close", (code) => resolve({ code: code ?? 1, stdout, stderr }));
  });
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

  server.registerTool("notes_list", {
    description: "List Doer note files in .doer-agent/notes.",
    inputSchema: {},
  }, async () => {
    const notes = await listAgentNotesLocal(workspaceRoot);
    return {
      content: [{ type: "text", text: formatJson({ notes, capabilities: agentNotesCapabilitiesLocal() }) }],
      structuredContent: { notes, capabilities: agentNotesCapabilitiesLocal() },
    };
  });

  server.registerTool("notes_read", {
    description: "Read a Doer note file from .doer-agent/notes.",
    inputSchema: {
      noteId: z.string().optional().describe("Note filename. Defaults to the first note when omitted."),
    },
  }, async ({ noteId }) => {
    const notes = await listAgentNotesLocal(workspaceRoot);
    const targetNoteId = noteId?.trim() || notes[0]?.id || "";
    const note = targetNoteId ? await getAgentNoteLocal(workspaceRoot, targetNoteId) : null;
    return {
      content: [{ type: "text", text: formatJson({ note, capabilities: agentNotesCapabilitiesLocal() }) }],
      structuredContent: { note, capabilities: agentNotesCapabilitiesLocal() },
    };
  });

  server.registerTool("notes_create", {
    description: "Create an empty Doer note and record a patch entry.",
    inputSchema: {
      name: z.string().min(1).describe("Note filename. .md is appended if omitted."),
    },
  }, async ({ name }) => {
    const result = await createAgentNoteLocal(workspaceRoot, name);
    return {
      content: [{ type: "text", text: formatJson({ ...result, capabilities: agentNotesCapabilitiesLocal() }) }],
      structuredContent: { ...result, capabilities: agentNotesCapabilitiesLocal() },
    };
  });

  server.registerTool("notes_save", {
    description: "Save a Doer note and record a git-diff patch entry for the content change.",
    inputSchema: {
      noteId: z.string().min(1).describe("Note filename."),
      content: z.string().describe("Full note content to save."),
    },
  }, async ({ noteId, content }) => {
    const result = await saveAgentNoteLocal({ workspaceRoot, noteId, content });
    return {
      content: [{ type: "text", text: formatJson({ ...result, capabilities: agentNotesCapabilitiesLocal() }) }],
      structuredContent: { ...result, capabilities: agentNotesCapabilitiesLocal() },
    };
  });

  server.registerTool("notes_rename", {
    description: "Rename a Doer note and record a git-diff rename patch entry.",
    inputSchema: {
      noteId: z.string().describe("Current note filename."),
      name: z.string().min(1).describe("New filename. .md is appended if omitted."),
    },
  }, async ({ noteId, name }) => {
    const result = await renameAgentNoteLocal({ workspaceRoot, noteId, name });
    return {
      content: [{ type: "text", text: formatJson({ ...result, capabilities: agentNotesCapabilitiesLocal() }) }],
      structuredContent: { ...result, capabilities: agentNotesCapabilitiesLocal() },
    };
  });

  server.registerTool("notes_delete", {
    description: "Delete a Doer note and record a git-diff deletion patch entry.",
    inputSchema: {
      noteId: z.string().describe("Note filename to delete."),
    },
  }, async ({ noteId }) => {
    const result = await deleteAgentNoteLocal({ workspaceRoot, noteId });
    return {
      content: [{ type: "text", text: formatJson({ ...result, capabilities: agentNotesCapabilitiesLocal() }) }],
      structuredContent: { ...result, capabilities: agentNotesCapabilitiesLocal() },
    };
  });

  server.registerTool("notes_search", {
    description: "Search Doer notes with ripgrep under .doer-agent/notes. Patches are excluded by default.",
    inputSchema: {
      query: z.string().min(1).describe("Text or regex pattern to search for."),
      includePatches: z.boolean().optional().describe("Search patch history too."),
      fixedStrings: z.boolean().optional().describe("Treat query as a literal string."),
      limit: z.number().int().min(1).max(200).optional().describe("Maximum matching lines to return."),
    },
  }, async ({ query, includePatches, fixedStrings, limit }) => {
    const args = [
      "--line-number",
      "--column",
      "--no-heading",
      "--color=never",
      fixedStrings === false ? null : "--fixed-strings",
      "--glob",
      "*.md",
      includePatches ? "--glob" : null,
      includePatches ? "*.patch" : null,
      query,
      ".doer-agent/notes",
    ].filter((item): item is string => Boolean(item));
    const result = await runSearch("rg", args, workspaceRoot);
    if (result.code !== 0 && result.code !== 1) {
      throw new Error(result.stderr || "notes search failed");
    }
    const lines = result.stdout.split("\n").filter(Boolean).slice(0, limit ?? 50);
    return {
      content: [{ type: "text", text: formatJson({ matches: lines, truncated: result.stdout.split("\n").filter(Boolean).length > lines.length }) }],
      structuredContent: { matches: lines, truncated: result.stdout.split("\n").filter(Boolean).length > lines.length },
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
