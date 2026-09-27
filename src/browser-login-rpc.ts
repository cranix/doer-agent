import { BrowserLoginBroker, BrowserLoginError, browserLoginError, type EncryptedLoginCommand } from "./browser-login-broker.js";

// Survives NATS reconnects; pending requests fail closed on process restart.
const broker = new BrowserLoginBroker();
export async function handleBrowserLoginRpc(params: unknown): Promise<unknown> {
  try {
    const p = params as Record<string, unknown>;
    if (!p || typeof p !== "object") throw new BrowserLoginError("Invalid browser request.");
    const id = typeof p.id === "string" ? p.id : "";
    switch (p.operation) {
      case "tabs": return { tabs: await broker.tabs() };
      case "list": return { requests: broker.list() };
      case "request": return await broker.request(typeof p.tabId === "string" ? p.tabId : "");
      case "status": return broker.status(id);
      case "cancel": return broker.cancel(id);
      case "view": return await broker.view(id);
      case "command": return await broker.command(id, typeof p.revision === "string" ? p.revision : "", p.envelope as EncryptedLoginCommand);
      default: throw new BrowserLoginError("Unsupported browser request.");
    }
  } catch (error) { throw new BrowserLoginError(browserLoginError(error)); }
}
