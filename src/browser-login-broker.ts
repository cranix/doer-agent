import { generateKeyPairSync, privateDecrypt, createDecipheriv, constants, randomUUID, type KeyObject } from "node:crypto";
import { LoginTab } from "./browser-login-cdp.js";

export class BrowserLoginError extends Error {}
function fail(message: string): never { throw new BrowserLoginError(message); }
export function browserLoginError(error: unknown): string {
  // Never propagate Playwright call logs: they can contain input values and URLs.
  return error instanceof BrowserLoginError ? error.message : "Browser operation failed. Refresh the login screen and try again.";
}
export type LoginStatus = "waiting" | "completed" | "cancelled" | "expired";
interface Field { objectId: string; origin: string; type: string; label: string }
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
  private creating = false;
  constructor(private readonly endpoint = process.env.DOER_BROWSER_CDP_URL || "http://127.0.0.1:9222", private readonly ttlMs = 15 * 60_000) {}

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
        return { id, label: field.label, type: field.type, origin: field.origin };
      });
      this.get(id);
      if (snapshot.screenshot.length > 800_000) fail("Browser preview is too large. Reduce the browser window size.");
      session.revision = randomUUID();
      return { ...this.summary(session), currentOrigin: origin, revision: session.revision, publicKey: session.publicKey,
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
    for (const session of this.sessions.values()) if (session.status === "waiting") this.finish(session, "cancelled");
  }
}
