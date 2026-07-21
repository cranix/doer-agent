import assert from "node:assert/strict";
import test from "node:test";
import { buildCustomMcpConfigArgs } from "./agent-codex-cli.js";

test("buildCustomMcpConfigArgs builds streamable HTTP MCP overrides", () => {
  const args = buildCustomMcpConfigArgs([{
    name: "remote_docs",
    transport: "streamable_http",
    command: "",
    args: [],
    env: [],
    url: "https://example.com/mcp",
    bearerTokenEnvVar: "MCP_TOKEN",
    httpHeaders: [{ key: "X-Region", value: "seoul" }],
    envHttpHeaders: [{ key: "X-API-Key", value: "MCP_API_KEY" }],
    enabled: true,
  }]);

  assert.deepEqual(args, [
    "--config", 'mcp_servers.remote_docs.url="https://example.com/mcp"',
    "--config", "mcp_servers.remote_docs.enabled=true",
    "--config", 'mcp_servers.remote_docs.bearer_token_env_var="MCP_TOKEN"',
    "--config", 'mcp_servers.remote_docs.http_headers={ "X-Region" = "seoul" }',
    "--config", 'mcp_servers.remote_docs.env_http_headers={ "X-API-Key" = "MCP_API_KEY" }',
  ]);
});

test("buildCustomMcpConfigArgs keeps stdio MCP overrides", () => {
  const args = buildCustomMcpConfigArgs([{
    name: "local_tools",
    transport: "stdio",
    command: "npx",
    args: ["-y", "local-mcp"],
    env: [{ key: "LOCAL_TOKEN", value: "secret" }],
    url: "",
    bearerTokenEnvVar: "",
    httpHeaders: [],
    envHttpHeaders: [],
    enabled: true,
  }]);

  assert.deepEqual(args, [
    "--config", 'mcp_servers.local_tools.command="npx"',
    "--config", 'mcp_servers.local_tools.args=["-y", "local-mcp"]',
    "--config", "mcp_servers.local_tools.enabled=true",
    "--config", 'mcp_servers.local_tools.env.LOCAL_TOKEN="secret"',
  ]);
});
