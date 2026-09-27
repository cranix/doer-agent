import { BrowserLoginBroker, BrowserLoginError, browserLoginError, type EncryptedLoginCommand } from "./browser-login-broker.js";
import { BrowserCredentialVault, browserVaultDirectory } from "./browser-credential-vault.js";

// Survives NATS reconnects; pending requests fail closed on process restart.
let broker = new BrowserLoginBroker();
let currentScope = "";
export async function configureBrowserLoginBroker(server: string, userId: string, agentId: string) {
  const endpoint = process.env.DOER_BROWSER_CDP_URL || "http://127.0.0.1:9222";
  const scope = JSON.stringify([server, userId, agentId, endpoint]);
  if (currentScope === scope) return;
  await broker.close();
  broker = new BrowserLoginBroker(endpoint, 15 * 60_000, new BrowserCredentialVault(browserVaultDirectory(scope)));
  currentScope = scope;
}
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
      case "vault-view": return await broker.vaultView();
      case "vault-command": return await broker.vaultCommand(id, typeof p.revision === "string" ? p.revision : "", p.envelope as EncryptedLoginCommand);
      case "login-saved": return await broker.loginSaved(typeof p.tabId === "string" ? p.tabId : "", typeof p.accountId === "string" ? p.accountId : undefined);
      case "command": return await broker.command(id, typeof p.revision === "string" ? p.revision : "", p.envelope as EncryptedLoginCommand);
      default: throw new BrowserLoginError("Unsupported browser request.");
    }
  } catch (error) { throw new BrowserLoginError(browserLoginError(error)); }
}
