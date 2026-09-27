import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import * as z from "zod/v4";

function config(name: string) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}
const base = config("DOER_BROWSER_SERVER_BASE_URL").replace(/\/$/, "");
const userId = config("DOER_BROWSER_USER_ID");
const agentId = config("DOER_BROWSER_AGENT_ID");
const path = `/api/users/${encodeURIComponent(userId)}/agents/${encodeURIComponent(agentId)}/browser-login/tools`;
async function call(operation: string, params: Record<string, unknown> = {}) {
  const response = await fetch(base + path, { method: "POST", signal: AbortSignal.timeout(30_000),
    headers: { Authorization: `Bearer ${config("DOER_AGENT_TOKEN")}`, "Content-Type": "application/json" },
    body: JSON.stringify({ operation, ...params }) });
  if (!response.ok) throw new Error("Browser login service is unavailable. Do not request credentials in chat.");
  return await response.json() as Record<string, unknown>;
}
const instructions = "When a browser task needs login, use browser_login_request for its tab. Ask the user to open the Doer Browser login panel. Stop all browser actions, screenshots, shell/CDP inspection of that tab while waiting; keep using browser_login_wait until completed/cancelled/expired. Never ask for passwords or OTPs in chat or pass them to tools. After completed, verify the intended account is signed in before continuing. Completion means the user finished, not proof of authentication.";
const server = new McpServer({ name: "doer-browser", version: "0.1.0" }, { instructions });
function result(value: unknown) { return { content: [{ type: "text" as const, text: JSON.stringify(value) }] }; }
server.registerTool("browser_login_tabs", { description: "List local Chromium tab IDs and origins for login handoff. No page contents or credentials.", inputSchema: {} }, async () => result(await call("tabs")));
server.registerTool("browser_login_request", {
  description: instructions,
  inputSchema: { tabId: z.string().min(1).describe("Existing Chromium tab ID from browser_login_tabs.") },
}, async ({ tabId }) => result({ ...await call("request", { tabId }), loginUrl: `${base}/agents/${encodeURIComponent(agentId)}/browser`, next: "Wait with browser_login_wait. User enters credentials only in Doer Browser login." }));
server.registerTool("browser_login_wait", {
  description: "Wait up to 20 seconds for a login handoff. If still waiting, call again without inspecting or controlling the browser. On completion verify login before resuming; stop on cancellation/expiry.",
  inputSchema: { id: z.string().min(1) },
}, async ({ id }, extra) => {
  const deadline = Date.now() + 20_000;
  let status: Record<string, unknown>;
  do {
    if (extra.signal.aborted) return result({ status: "interrupted", next: "Do not control a tab with a pending login." });
    status = await call("status", { id });
    if (status.status !== "waiting") break;
    await new Promise((resolve) => setTimeout(resolve, 1_000));
  } while (Date.now() < deadline);
  return result({ ...status!, next: status!.status === "waiting" ? "Call browser_login_wait again." : status!.status === "completed" ? "User finished. Verify the intended account is signed in before resuming." : "Login did not complete. Do not continue authenticated actions." });
});
await server.connect(new StdioServerTransport());
