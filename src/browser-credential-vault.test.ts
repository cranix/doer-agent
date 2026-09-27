import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { BrowserCredentialVault } from "./browser-credential-vault.js";

test("saved credentials persist encrypted, update without disclosure, and obey exact site and auto-login permission", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "doer-vault-test-"));
  try {
    const vault = new BrowserCredentialVault(directory);
    const account = await vault.save({ origin: "https://nid.example.com/login", username: "test-user", password: "synthetic-secret", label: "Personal", autoLogin: false });
    assert.ok(!JSON.stringify(account).includes("synthetic-secret"));
    const disk = await readFile(path.join(directory, "vault.json"), "utf8");
    for (const value of ["test-user", "synthetic-secret", "nid.example.com"]) assert.ok(!disk.includes(value));
    assert.equal((await stat(path.join(directory, "vault.json"))).mode & 0o777, 0o600);
    assert.equal((await stat(path.join(directory, "key"))).mode & 0o777, 0o600);
    const restarted = new BrowserCredentialVault(directory);
    assert.equal((await restarted.get(account.id, account.origin)).password, "synthetic-secret");
    await assert.rejects(restarted.get(account.id, "https://example.com"));
    await assert.rejects(restarted.get(account.id, account.origin, true));
    await restarted.save({ ...account, username: "new-user", password: "", autoLogin: true });
    assert.equal((await restarted.get(account.id, account.origin, true)).password, "synthetic-secret");
    await restarted.save({ ...account, password: "replacement-secret", autoLogin: true });
    assert.equal((await restarted.get(account.id, account.origin, true)).password, "replacement-secret");
    await assert.rejects(restarted.save({ ...account, origin: "https://other.example.com", password: "", autoLogin: true }));
    await restarted.remove(account.id);
    assert.deepEqual(await vault.list(), []);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test("corrupt vault and missing key fail closed without replacing stored credentials", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "doer-vault-test-"));
  try {
    const vault = new BrowserCredentialVault(directory);
    const input = { origin: "https://example.com", username: "user", password: "secret", autoLogin: false };
    await vault.save(input);
    const filename = path.join(directory, "vault.json");
    const original = await readFile(filename, "utf8");
    const damaged = JSON.parse(original); damaged.tag = Buffer.alloc(16).toString("base64");
    await writeFile(filename, JSON.stringify(damaged));
    await assert.rejects(vault.save(input));
    assert.equal(await readFile(filename, "utf8"), JSON.stringify(damaged));
    await writeFile(filename, original);
    await rm(path.join(directory, "key"));
    await assert.rejects(vault.list(), /missing/);
    await assert.rejects(stat(path.join(directory, "key")), { code: "ENOENT" });
    assert.equal(await readFile(filename, "utf8"), original);
  } finally { await rm(directory, { recursive: true, force: true }); }
});
