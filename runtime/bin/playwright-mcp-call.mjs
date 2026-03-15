#!/usr/bin/env node

import { Buffer } from "node:buffer";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import path from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

const DOER_MCP_PROXY_DEFAULT_PATH = "/app/.runtime/bin/doer-mcp-proxy";

function parseArgs(argv) {
  const parsed = { tool: "", argsBase64: "" };
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (token === "--tool") {
      parsed.tool = (argv[i + 1] || "").trim();
      i += 1;
      continue;
    }
    if (token === "--args-base64") {
      parsed.argsBase64 = (argv[i + 1] || "").trim();
      i += 1;
    }
  }
  return parsed;
}

function resolveProxyPath() {
  const explicitProxyPath = (process.env.DOER_MCP_PROXY_PATH || "").trim();
  if (explicitProxyPath && existsSync(explicitProxyPath)) {
    return explicitProxyPath;
  }
  if (existsSync(DOER_MCP_PROXY_DEFAULT_PATH)) {
    return DOER_MCP_PROXY_DEFAULT_PATH;
  }
  throw new Error(
    `doer-mcp-proxy binary not found; set DOER_MCP_PROXY_PATH to an existing file or install ${DOER_MCP_PROXY_DEFAULT_PATH}`,
  );
}

function resolveSocketPath() {
  const explicit = (process.env.DOER_MCP_SOCKET || "").trim();
  if (explicit) {
    return explicit;
  }
  const stateDir = (process.env.DOER_AGENT_STATE_DIR || "").trim() || path.join(homedir(), ".doer-agent");
  return path.join(stateDir, "playwright-mcp-daemon", "playwright-mcp.sock");
}

function decodeToolArgs(argsBase64) {
  if (!argsBase64) {
    return {};
  }
  const raw = Buffer.from(argsBase64, "base64").toString("utf8");
  const parsed = JSON.parse(raw);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("decoded tool arguments must be a JSON object");
  }
  return parsed;
}

async function main() {
  const { tool, argsBase64 } = parseArgs(process.argv.slice(2));
  if (!tool) {
    throw new Error("--tool is required");
  }

  const proxyPath = resolveProxyPath();
  const socketPath = resolveSocketPath();
  const toolArgs = decodeToolArgs(argsBase64);

  const transport = new StdioClientTransport({
    command: proxyPath,
    args: [],
    env: {
      ...process.env,
      DOER_MCP_SOCKET: socketPath,
    },
    stderr: "pipe",
  });

  const client = new Client(
    {
      name: "doer-agent-playwright-mcp-runner",
      version: "0.1.0",
    },
    {
      capabilities: {},
    },
  );

  try {
    await client.connect(transport);
    const result = await client.callTool({
      name: tool,
      arguments: toolArgs,
    });
    process.stdout.write(
      `${JSON.stringify({
        ok: true,
        tool,
        content: result.content ?? null,
        isError: result.isError === true,
        structuredContent: result.structuredContent ?? null,
        result,
      })}\n`,
    );
  } finally {
    await client.close().catch(() => undefined);
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`${JSON.stringify({ ok: false, error: message })}\n`);
  process.exit(1);
});
