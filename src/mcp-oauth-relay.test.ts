import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";
import {
  buildMcpOauthCallbackBaseUrl,
  relayMcpOauthCallbackToLocalListener,
} from "./mcp-oauth-relay.js";

test("buildMcpOauthCallbackBaseUrl scopes callbacks to the user and agent", () => {
  assert.equal(
    buildMcpOauthCallbackBaseUrl({
      serverBaseUrl: "https://doer.example.com/",
      userId: "user id",
      agentId: "agent/id",
    }),
    "https://doer.example.com/api/mcp-oauth/callback/user%20id/agent%2Fid",
  );
});

test("relayMcpOauthCallbackToLocalListener preserves the callback path and query", async () => {
  const server = http.createServer((request, response) => {
    response.statusCode = 200;
    response.setHeader("content-type", "text/html; charset=utf-8");
    response.end(`<p>${request.url}</p>`);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  try {
    const result = await relayMcpOauthCallbackToLocalListener({
      callbackPort: address.port,
      callbackPathPrefix: "/api/mcp-oauth/callback/user/agent",
      path: "/api/mcp-oauth/callback/user/agent/callback-id",
      search: "?code=abc&state=xyz",
    });
    assert.equal(result.status, 200);
    assert.equal(result.headers["content-type"], "text/html; charset=utf-8");
    assert.equal(result.body, "<p>/api/mcp-oauth/callback/user/agent/callback-id?code=abc&state=xyz</p>");
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
});

test("relayMcpOauthCallbackToLocalListener rejects callbacks outside the configured path", async () => {
  await assert.rejects(
    relayMcpOauthCallbackToLocalListener({
      callbackPort: 4321,
      callbackPathPrefix: "/api/mcp-oauth/callback/user/agent",
      path: "/api/mcp-oauth/callback/other/agent/callback-id",
      search: "",
    }),
    /Invalid MCP OAuth callback path/,
  );
});
