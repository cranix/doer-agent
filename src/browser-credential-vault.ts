import { createCipheriv, createDecipheriv, createHash, randomBytes, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { mkdir, open, readFile, rename, rm, stat } from "node:fs/promises";
import path from "node:path";
import os from "node:os";

export interface SavedBrowserAccount {
  id: string; origin: string; label: string; username: string; autoLogin: boolean; updatedAt: string;
}
interface Credential extends SavedBrowserAccount { password: string }
const aad = Buffer.from("doer-browser-vault:v1");
export function credentialOrigin(value: unknown): string {
  if (typeof value !== "string") throw new Error("Invalid site");
  const url = new URL(value);
  if (url.username || url.password || !(url.protocol === "https:" || (url.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)))) throw new Error("Invalid site");
  return url.origin;
}
export function browserVaultDirectory(scope: string) {
  return path.join(process.env.DOER_BROWSER_VAULT_DIR || path.join(os.homedir(), ".doer-agent", "browser-vault"), createHash("sha256").update(scope).digest("hex"));
}
function publicAccount({ password: _password, ...account }: Credential): SavedBrowserAccount { return account; }

/** Local encrypted storage. The same OS user can read its key; this is not an OS sandbox. */
export class BrowserCredentialVault {
  private queue: Promise<unknown> = Promise.resolve();
  constructor(private readonly directory: string) {}
  private async exclusive<T>(work: () => Promise<T>): Promise<T> {
    const next = this.queue.then(work, work);
    this.queue = next.catch(() => undefined);
    return next;
  }
  private async key() {
    await mkdir(this.directory, { recursive: true, mode: 0o700 });
    const info = await stat(this.directory);
    if ((info.mode & 0o077) || (process.getuid && info.uid !== process.getuid())) throw new Error("Unsafe vault permissions");
    const filename = path.join(this.directory, "key");
    try {
      const handle = await open(filename, constants.O_RDONLY | constants.O_NOFOLLOW);
      try {
        const info = await handle.stat();
        if (!info.isFile() || (info.mode & 0o077) || (process.getuid && info.uid !== process.getuid())) throw new Error("Unsafe key permissions");
        const key = await handle.readFile();
        if (key.length !== 32) throw new Error("Invalid vault key");
        return key;
      } finally { await handle.close(); }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      // Never replace a missing key for an existing vault.
      try { await stat(path.join(this.directory, "vault.json")); } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        const key = randomBytes(32);
        const handle = await open(filename, "wx", 0o600);
        try { await handle.writeFile(key); await handle.sync(); } finally { await handle.close(); }
        return key;
      }
      throw new Error("Vault key missing");
    }
  }
  private async read(key: Buffer): Promise<Credential[]> {
    let data: string;
    try { data = await readFile(path.join(this.directory, "vault.json"), "utf8"); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
    const envelope = JSON.parse(data);
    if (envelope.version !== 1) throw new Error("Unsupported vault version");
    const decipher = createDecipheriv("aes-256-gcm", key, Buffer.from(envelope.iv, "base64"));
    decipher.setAAD(aad); decipher.setAuthTag(Buffer.from(envelope.tag, "base64"));
    const plain = Buffer.concat([decipher.update(Buffer.from(envelope.data, "base64")), decipher.final()]);
    try {
      const entries = JSON.parse(plain.toString());
      if (!Array.isArray(entries) || entries.length > 100) throw new Error("Invalid vault");
      return entries;
    } finally { plain.fill(0); }
  }
  private async access<T>(work: (entries: Credential[]) => T, write = false): Promise<T> {
    return this.exclusive(async () => {
      const key = await this.key();
      let entries: Credential[] = [];
      try {
        entries = await this.read(key);
        const result = work(entries);
        if (write) {
          const iv = randomBytes(12), cipher = createCipheriv("aes-256-gcm", key, iv);
          cipher.setAAD(aad);
          const plain = Buffer.from(JSON.stringify(entries));
          let encrypted: Buffer;
          try { encrypted = Buffer.concat([cipher.update(plain), cipher.final()]); } finally { plain.fill(0); }
          const temp = path.join(this.directory, `${randomUUID()}.tmp`);
          try {
            const handle = await open(temp, "wx", 0o600);
            try { await handle.writeFile(JSON.stringify({ version: 1, iv: iv.toString("base64"), tag: cipher.getAuthTag().toString("base64"), data: encrypted.toString("base64") })); await handle.sync(); }
            finally { await handle.close(); }
            await rename(temp, path.join(this.directory, "vault.json"));
          } finally { await rm(temp, { force: true }); }
        }
        return result;
      } finally { key.fill(0); for (const entry of entries) entry.password = ""; }
    });
  }
  list() { return this.access((entries) => entries.map(publicAccount)); }
  async get(id: string, origin: string, automatic = false) {
    return this.access((entries) => {
      const entry = entries.find((entry) => entry.id === id && entry.origin === origin && (!automatic || entry.autoLogin));
      if (!entry) throw new Error("Account unavailable for this site");
      return { ...entry };
    });
  }
  async save(input: Record<string, unknown>) {
    const origin = credentialOrigin(input.origin);
    if (typeof input.username !== "string" || !input.username.trim() || input.username.length > 4096 || typeof input.password !== "string" || input.password.length > 4096 || typeof input.autoLogin !== "boolean") throw new Error("Invalid account");
    return this.access((entries) => {
      const previous = typeof input.id === "string" ? entries.find((entry) => entry.id === input.id) : undefined;
      if (input.id && !previous) throw new Error("Account not found");
      if (previous && previous.origin !== origin) throw new Error("Create a new account to change its site");
      const password = (input.password as string) || previous?.password;
      if (!password || (!previous && entries.length >= 100)) throw new Error("Invalid account");
      const entry: Credential = { id: previous?.id || randomUUID(), origin, username: input.username as string, password,
        label: typeof input.label === "string" ? input.label.slice(0, 100) : "", autoLogin: input.autoLogin as boolean, updatedAt: new Date().toISOString() };
      if (previous) entries.splice(entries.indexOf(previous), 1, entry); else entries.push(entry);
      return publicAccount(entry);
    }, true);
  }
  async remove(id: string) { return this.access((entries) => { const index = entries.findIndex((entry) => entry.id === id); if (index < 0) throw new Error("Account not found"); entries.splice(index, 1); }, true); }
}
