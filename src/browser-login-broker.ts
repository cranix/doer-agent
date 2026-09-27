import { generateKeyPairSync, privateDecrypt, createDecipheriv, constants, randomUUID, type KeyObject } from "node:crypto";
import { LoginTab } from "./browser-login-cdp.js";
import { BrowserCredentialVault, type SavedBrowserAccount } from "./browser-credential-vault.js";

export class BrowserLoginError extends Error {}
function fail(message: string): never { throw new BrowserLoginError(message); }
export function browserLoginError(error: unknown): string {
  // Never propagate Playwright call logs: they can contain input values and URLs.
  return error instanceof BrowserLoginError ? error.message : "Browser operation failed. Refresh the login screen and try again.";
}
export type LoginStatus = "waiting" | "completed" | "cancelled" | "expired";
interface Field { objectId: string; origin: string; type: string; label: string; purpose: "username" | "password" | "other" }
interface LoginSession {
  id: string; tabId: string; origin: string; status: LoginStatus; expiresAt: number;
  page: LoginTab; privateKey: KeyObject | null; publicKey: JsonWebKey;
  revision: string; fields: Map<string, Field>; document: string | null; contextId: number; width: number; height: number;
  touched: Set<string>; busy: boolean; timer: ReturnType<typeof setTimeout>;
}
export interface EncryptedLoginCommand { key: string; iv: string; data: string }
export function loginAad(id: string, revision: string): Buffer {
  return Buffer.from(`doer-browser-login:v1:${id}:${revision}`);
}
export function decryptLoginCommand(key: KeyObject, id: string, revision: string, envelope: EncryptedLoginCommand): Record<string, unknown> {
  if (![envelope?.key, envelope?.iv, envelope?.data].every((v) => typeof v === "string" && v.length < 32_000)) fail("Invalid encrypted command.");
  const aes = privateDecrypt({ key, oaepHash: "sha256", padding: constants.RSA_PKCS1_OAEP_PADDING }, Buffer.from(envelope.key, "base64"));
  let plain: Buffer | undefined;
  try {
    const iv = Buffer.from(envelope.iv, "base64");
    const data = Buffer.from(envelope.data, "base64");
    if (aes.length !== 32 || iv.length !== 12 || data.length < 17) fail("Invalid encrypted command.");
    const decipher = createDecipheriv("aes-256-gcm", aes, iv);
    decipher.setAAD(loginAad(id, revision));
    decipher.setAuthTag(data.subarray(-16));
    plain = Buffer.concat([decipher.update(data.subarray(0, -16)), decipher.final()]);
    const command = JSON.parse(plain.toString());
    if (!command || typeof command !== "object" || Array.isArray(command)) fail("Invalid command.");
    return command;
  } finally { aes.fill(0); plain?.fill(0); }
}
function originOf(url: string): string {
  try {
    const parsed = new URL(url);
    if (parsed.protocol === "https:" || (parsed.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(parsed.hostname))) return parsed.origin;
  } catch { /* Not a supported login page. */ }
  return fail("Login requires HTTPS (or localhost for development).");
}

/** In-memory handoff only. This is NOT a sandbox against a same-user shell or raw CDP. */
export class BrowserLoginBroker {
  private readonly targets = new Map<string, { origin: string; websocket: string }>();
  private readonly sessions = new Map<string, LoginSession>();
  private readonly vaultScreens = new Map<string, { privateKey: KeyObject; revision: string; expiresAt: number }>();
  private readonly automaticTabs = new Set<string>();
  private readonly attempts = new Map<string, number>();
  private creating = false;
  constructor(private readonly endpoint = process.env.DOER_BROWSER_CDP_URL || "http://127.0.0.1:9222", private readonly ttlMs = 15 * 60_000, private readonly vault?: BrowserCredentialVault) {}

  private credentialVault() { if (!this.vault) return fail("Saved logins are unavailable. Update the agent."); return this.vault; }
  async vaultView() {
    const accounts = await this.credentialVault().list();
    for (const [id, screen] of this.vaultScreens) if (screen.expiresAt < Date.now() || this.vaultScreens.size >= 8) this.vaultScreens.delete(id);
    const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
    const id = randomUUID(), revision = randomUUID();
    this.vaultScreens.set(id, { privateKey, revision, expiresAt: Date.now() + this.ttlMs });
    return { id, revision, publicKey: publicKey.export({ format: "jwk" }), accounts };
  }
  async vaultCommand(id: string, revision: string, envelope: EncryptedLoginCommand) {
    const screen = this.vaultScreens.get(id);
    if (!screen || screen.expiresAt < Date.now() || screen.revision !== revision) fail("Saved logins screen is stale. Refresh and try again.");
    this.vaultScreens.delete(id);
    const command = decryptLoginCommand(screen.privateKey, id, revision, envelope);
    try {
      if (command.action === "save") await this.credentialVault().save(command);
      else if (command.action === "delete" && typeof command.accountId === "string") await this.credentialVault().remove(command.accountId);
      else fail("Unsupported saved login operation.");
      return { ok: true };
    } finally { command.password = ""; }
  }
  async loginSaved(tabId: string, accountId?: string) {
    if (this.automaticTabs.has(tabId) || this.list().some((session) => session.tabId === tabId)) return { status: "needs_user", reason: "A login is already in progress. Wait for it to finish." };
    this.automaticTabs.add(tabId);
    let page: LoginTab | undefined;
    let credential: Awaited<ReturnType<BrowserCredentialVault["get"]>> | undefined;
    try {
      await this.tabs();
      const target = this.targets.get(tabId);
      if (!target) fail("Browser tab is unavailable.");
      const eligible = (await this.credentialVault().list()).filter((entry) => entry.origin === target.origin && entry.autoLogin);
      const account = accountId ? eligible.find((entry) => entry.id === accountId) : eligible.length === 1 ? eligible[0] : undefined;
      if (!account) return { status: "needs_user", reason: eligible.length > 1 ? "Multiple accounts. Ask the user which saved account to use." : "No permitted saved account for this exact site.", accounts: eligible.map(({ id, label, origin }) => ({ id, label, origin })) };
      const attemptKey = `${tabId}:${account.id}:${account.updatedAt}`;
      for (const [key, time] of this.attempts) if (Date.now() - time > 5 * 60_000) this.attempts.delete(key);
      if (this.attempts.has(attemptKey) || this.attempts.size >= 128) return { status: "needs_user", reason: "Already attempted this account. Use a human handoff instead of retrying." };
      page = await LoginTab.connect(target.websocket);
      const snapshot = await page.snapshot(false);
      const usernames = snapshot.fields.filter((field) => field.purpose === "username");
      const passwords = snapshot.fields.filter((field) => field.purpose === "password");
      if (snapshot.origin !== target.origin || snapshot.fields.length !== 2 || usernames.length !== 1 || passwords.length !== 1 || !await page.canAutoSubmit(passwords[0].objectId, usernames[0].objectId, target.origin)) return { status: "needs_user", reason: "Login form needs human input (additional verification, multi-step or unsupported form)." };
      const username = usernames[0], password = passwords[0];
      credential = await this.credentialVault().get(account.id, target.origin, true);
      this.attempts.set(attemptKey, Date.now());
      let submitted = false;
      try {
        if (!await page.fill(username.objectId, target.origin, credential.username) || !await page.fill(password.objectId, target.origin, credential.password)) fail("Login destination changed. Use a human handoff.");
        submitted = await page.autoSubmit(password.objectId, username.objectId, target.origin);
        return { status: submitted ? "submitted" : "needs_user", origin: target.origin, accountId: account.id,
          next: "Verify the intended account is signed in. Submission is not proof of login. If authentication, CAPTCHA or a different account is needed, request a human handoff; never retry the password." };
      } finally {
        if (!submitted) for (const field of [username, password]) await page.onObject(field.objectId, "function(){if(this.isConnected)this.value=''}").catch(() => undefined);
      }
    } finally { if (credential) credential.password = ""; page?.close(); this.automaticTabs.delete(tabId); }
  }

  async tabs() {
    const url = new URL(this.endpoint);
    if (url.protocol !== "http:" || !["localhost", "127.0.0.1", "[::1]"].includes(url.hostname) || url.username || url.password) fail("CDP must use a local HTTP endpoint.");
    const response = await fetch(new URL("/json/list", url), { signal: AbortSignal.timeout(5_000), redirect: "error" });
    if (!response.ok) fail("Chrome is unavailable.");
    const targets = await response.json() as Array<{ id: string; type: string; url: string; webSocketDebuggerUrl: string }>;
    const tabs: Array<{ id: string; origin: string }> = [];
    this.targets.clear();
    for (const target of targets) {
      if (target.type !== "page" || !/^[a-zA-Z0-9_-]+$/.test(target.id)) continue;
      let origin: string;
      try { origin = originOf(target.url); } catch { continue; }
      // Build the WS destination from the configured loopback address, not page-controlled data.
      const websocket = new URL(`/devtools/page/${target.id}`, url); websocket.protocol = "ws:";
      this.targets.set(target.id, { origin, websocket: websocket.toString() });
      tabs.push({ id: target.id, origin });
    }
    return tabs;
  }
  private summary(session: LoginSession) {
    if (session.status === "waiting" && !session.page.connected) this.finish(session, "cancelled");
    return { id: session.id, tabId: session.tabId, origin: session.origin, status: session.status, expiresAt: new Date(session.expiresAt).toISOString() };
  }
  list() { return [...this.sessions.values()].map((session) => this.summary(session)).filter((session) => session.status === "waiting"); }
  status(id: string) { return this.summary(this.get(id, false)); }
  async request(tabId: string) {
    if (this.automaticTabs.has(tabId)) fail("A saved login is in progress. Try again shortly.");
    if (this.creating) fail("Another login request is being created. Try again.");
    this.creating = true;
    try {
      await this.tabs();
      const target = this.targets.get(tabId);
      if (!target) fail("Browser tab is unavailable.");
      const existing = [...this.sessions.values()].find((s) => s.tabId === tabId && s.status === "waiting");
      if (existing && this.summary(existing).status === "waiting") return this.summary(existing);
      if (this.list().length >= 8) fail("Too many pending login requests.");
      const origin = target.origin;
      const page = await LoginTab.connect(target.websocket);
      const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
      const id = randomUUID();
      const session: LoginSession = { id, tabId, page, origin, status: "waiting", expiresAt: Date.now() + this.ttlMs,
        privateKey, publicKey: publicKey.export({ format: "jwk" }), revision: "", fields: new Map(), document: null, contextId: 0, width: 0, height: 0,
        touched: new Set(), busy: false, timer: setTimeout(() => this.finish(session, "expired"), this.ttlMs) };
      session.timer.unref();
      this.sessions.set(id, session);
      // Retain terminal status briefly for waiting MCP clients, with a strict memory bound.
      for (const [oldId, old] of this.sessions) if (old.status !== "waiting" && (Date.now() - old.expiresAt > 60 * 60_000 || this.sessions.size > 64)) this.sessions.delete(oldId);
      return this.summary(session);
    } finally { this.creating = false; }
  }
  private get(id: string, active = true): LoginSession {
    const session = this.sessions.get(id);
    if (!session) return fail("Login request not found. It may have ended when the agent restarted.");
    if (session.status === "waiting" && Date.now() >= session.expiresAt) this.finish(session, "expired");
    if (session.status === "waiting" && !session.page.connected) this.finish(session, "cancelled");
    if (active && session.status !== "waiting") fail("Login request is no longer active.");
    return session;
  }
  private releaseFields(session: LoginSession) {
    for (const field of session.fields.values()) if (!session.touched.has(field.objectId)) void session.page.call("Runtime.releaseObject", { objectId: field.objectId }).catch(() => undefined);
    session.fields.clear();
    if (session.document) void session.page.call("Runtime.releaseObject", { objectId: session.document }).catch(() => undefined);
    session.document = null;
  }
  private finish(session: LoginSession, status: LoginStatus) {
    clearTimeout(session.timer);
    session.status = status;
    session.privateKey = null;
    session.revision = "";
    this.releaseFields(session);
    const cleanup = [...session.touched].map((objectId) => session.page.onObject(objectId, "function(){if(this.isConnected){const setter=Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value')?.set;setter?.call(this,'')}}").catch(() => undefined));
    session.touched.clear();
    void Promise.allSettled(cleanup).finally(() => session.page.close());
  }
  cancel(id: string) {
    const session = this.get(id, false);
    if (session.busy) fail("A browser operation is in progress.");
    if (session.status === "waiting") this.finish(session, "cancelled");
    return this.summary(session);
  }
  async view(id: string) {
    const session = this.get(id);
    if (session.busy) fail("A browser operation is in progress.");
    session.busy = true;
    try {
      this.releaseFields(session);
      session.revision = "";
      const snapshot = await session.page.snapshot();
      const origin = originOf(snapshot.origin);
      session.document = snapshot.documentId;
      session.contextId = snapshot.contextId;
      session.width = snapshot.width; session.height = snapshot.height;
      const fields = snapshot.fields.map((field) => {
        const id = randomUUID(); session.fields.set(id, field);
        return { id, label: field.label, type: field.type, origin: field.origin, purpose: field.purpose };
      });
      this.get(id);
      if (snapshot.screenshot.length > 800_000) fail("Browser preview is too large. Reduce the browser window size.");
      let savedAccounts: SavedBrowserAccount[] = [], canSave = false;
      if (this.vault) {
        try { savedAccounts = (await this.vault.list()).filter((account) => account.origin === origin); canSave = true; }
        catch { /* A damaged/unavailable vault must not prevent one-time human input. */ }
      }
      this.get(id);
      session.revision = randomUUID();
      return { ...this.summary(session), currentOrigin: origin, revision: session.revision, publicKey: session.publicKey, savedAccounts, canSave,
        fields, screenshot: snapshot.screenshot, width: snapshot.width, height: snapshot.height };
    } finally { session.busy = false; }
  }
  async command(id: string, revision: string, envelope: EncryptedLoginCommand) {
    const session = this.get(id);
    if (session.busy) fail("A browser operation is in progress.");
    if (!revision || revision !== session.revision || !session.privateKey) fail("Login screen is stale. Refresh before sending input.");
    session.busy = true;
    session.revision = ""; // Single-use revision prevents replay, including failed operations.
    try {
      const command = decryptLoginCommand(session.privateKey, id, revision, envelope);
      if (command.action === "cancel") { this.finish(session, "cancelled"); return this.summary(session); }
      if (command.action === "complete") { this.finish(session, "completed"); return this.summary(session); }
      if (!session.document || !await session.page.onObject<boolean>(session.document, "function(){return this.isConnected}").catch(() => false)) fail("Page changed. Refresh before sending input.");
      const origin = originOf(await session.page.evaluate<string>("location.href", session.contextId));
      if (command.origin !== origin) fail("Site changed. Review the new site before sending input.");
      if (command.action === "fill") {
        const values = command.values;
        if (!values || typeof values !== "object" || Array.isArray(values) || Object.keys(values).length > 20) fail("Invalid login input.");
        const entries = Object.entries(values as Record<string, unknown>);
        for (const [fieldId, value] of entries) {
          if (!session.fields.has(fieldId) || typeof value !== "string" || value.length > 4096) fail("Invalid login input.");
        }
        let save: Record<string, unknown> | undefined;
        if (command.saveAccount === true) {
          const usernames = [...session.fields.entries()].filter(([, field]) => field.purpose === "username");
          const passwords = [...session.fields.entries()].filter(([, field]) => field.purpose === "password");
          if (usernames.length !== 1 || passwords.length !== 1) fail("Select a username and password in saved login management.");
          const username = (values as Record<string, unknown>)[usernames[0][0]], password = (values as Record<string, unknown>)[passwords[0][0]];
          if (typeof username !== "string" || !username || typeof password !== "string" || !password) fail("Enter both username and password to save this account.");
          const previous = (await this.credentialVault().list()).find((account) => account.origin === origin && account.username === username);
          save = { id: previous?.id, origin, username, password, label: previous?.label || "", autoLogin: command.autoLogin === true };
        }
        for (const [fieldId, value] of entries) {
          this.get(id);
          const field = session.fields.get(fieldId)!;
          // Remote object IDs bind to the exact document. Check the origin and write atomically;
          // never retry a secret write against a replacement page.
          const written = await session.page.fill(field.objectId, field.origin, value as string);
          if (written) session.touched.add(field.objectId);
          if (!written) fail("Input destination changed or uses another site. Refresh and review the page.");
          (values as Record<string, unknown>)[fieldId] = "";
        }
        if (save) { try { await this.credentialVault().save(save); } finally { save.password = ""; } }
      } else if (command.action === "fillSaved") {
        const usernames = [...session.fields.values()].filter((field) => field.purpose === "username");
        const passwords = [...session.fields.values()].filter((field) => field.purpose === "password");
        if (typeof command.accountId !== "string" || usernames.length !== 1 || passwords.length !== 1) fail("Saved input requires username and password fields.");
        const account = await this.credentialVault().get(command.accountId, origin);
        try {
          for (const [field, value] of [[usernames[0], account.username], [passwords[0], account.password]] as const) {
            if (!await session.page.fill(field.objectId, field.origin, value)) fail("Input destination changed. Refresh and review the page.");
            session.touched.add(field.objectId);
          }
        } finally { account.password = ""; }
      } else if (command.action === "click") {
        const { x, y } = command;
        const size = { width: session.width, height: session.height };
        if (typeof x !== "number" || typeof y !== "number" || !Number.isFinite(x) || !Number.isFinite(y) || x < 0 || y < 0 || x > size.width || y > size.height) fail("Invalid click position.");
        await session.page.click(x, y);
      } else if (command.action === "key") {
        if (!["Tab", "Shift+Tab", "Enter", "Escape", "ArrowDown", "ArrowUp"].includes(String(command.key))) fail("Unsupported key.");
        await session.page.key(String(command.key));
      } else if (command.action === "scroll") {
        if (command.delta !== 500 && command.delta !== -500) fail("Invalid scroll.");
        await session.page.call("Input.dispatchMouseEvent", { type: "mouseWheel", x: session.width / 2, y: session.height / 2, deltaX: 0, deltaY: command.delta });
      } else fail("Unsupported login action.");
      return this.summary(session);
    } finally { session.busy = false; }
  }
  async close() {
    this.vaultScreens.clear();
    for (const session of this.sessions.values()) if (session.status === "waiting") this.finish(session, "cancelled");
  }
}
