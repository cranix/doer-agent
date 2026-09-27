import assert from "node:assert/strict";
import { test } from "node:test";
import { generateKeyPairSync, publicEncrypt, createPublicKey, randomBytes, createCipheriv, constants } from "node:crypto";
import { createServer } from "node:http";
import net from "node:net";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { BrowserCredentialVault } from "./browser-credential-vault.js";
import { chromium } from "playwright-core";
import { BrowserLoginBroker, browserLoginError, decryptLoginCommand } from "./browser-login-broker.js";

async function encryptBrowserLoginCommand(publicKey: JsonWebKey, id: string, revision: string, command: Record<string, unknown>) {
  const aes = randomBytes(32), iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", aes, iv);
  cipher.setAAD(Buffer.from(`doer-browser-login:v1:${id}:${revision}`));
  const data = Buffer.concat([cipher.update(JSON.stringify(command)), cipher.final(), cipher.getAuthTag()]);
  const key = publicEncrypt({ key: createPublicKey({ key: publicKey as import("node:crypto").JsonWebKey, format: "jwk" }), oaepHash: "sha256", padding: constants.RSA_PKCS1_OAEP_PADDING }, aes);
  return { key: key.toString("base64"), iv: iv.toString("base64"), data: data.toString("base64") };
}

test("encrypted login commands bind to the request and single-use screen revision", async () => {
  const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  const command = { action: "fill", values: { password: "test-only-secret" } };
  const envelope = await encryptBrowserLoginCommand(publicKey.export({ format: "jwk" }), "session", "revision", command);
  assert.ok(!JSON.stringify(envelope).includes("test-only-secret"));
  assert.deepEqual(decryptLoginCommand(privateKey, "session", "revision", envelope), command);
  assert.throws(() => decryptLoginCommand(privateKey, "different-session", "revision", envelope));
  assert.throws(() => decryptLoginCommand(privateKey, "session", "stale-revision", envelope));
  assert.throws(() => decryptLoginCommand(privateKey, "session", "revision", { ...envelope, data: "AAAA" }));
  assert.ok(!browserLoginError(new Error("fill(test-only-secret) failed")).includes("test-only-secret"));
});

test("saved login management and Chromium automation respect opt-in, form boundaries and handoff exclusivity", { skip: !process.env.DOER_BROWSER_TEST_EXECUTABLE }, async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "doer-saved-login-"));
  let submitted = "";
  const server = createServer((req, res) => {
    res.setHeader("Content-Type", "text/html");
    if (req.url === "/done") {
      req.on("data", (chunk) => { submitted += chunk.toString(); });
      req.on("end", () => res.end("<h1>Signed in</h1>")); return;
    }
    const cross = req.url === "/cross", otp = req.url === "/otp", registration = req.url === "/register";
    res.end(`<form method="post" action="${cross ? "https://example.com" : "/done"}"><input name="email"><input name="password" type="password" ${registration ? 'autocomplete="new-password"' : ""}>${otp ? '<input autocomplete="one-time-code" name="otp">' : ""}<button>Sign in</button></form>`);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const base = `http://127.0.0.1:${(server.address() as net.AddressInfo).port}`;
  const portServer = net.createServer();
  await new Promise<void>((resolve) => portServer.listen(0, "127.0.0.1", resolve));
  const port = (portServer.address() as net.AddressInfo).port;
  await new Promise<void>((resolve) => portServer.close(() => resolve()));
  const browser = await chromium.launch({ executablePath: process.env.DOER_BROWSER_TEST_EXECUTABLE, headless: true, args: [`--remote-debugging-port=${port}`] });
  const vault = new BrowserCredentialVault(directory);
  const broker = new BrowserLoginBroker(`http://127.0.0.1:${port}`, 60_000, vault);
  try {
    let management = await broker.vaultView();
    const input = { action: "save", origin: base, username: "test-user", password: "test-password", label: "Fixture", autoLogin: false };
    const envelope = await encryptBrowserLoginCommand(management.publicKey, management.id, management.revision, input);
    await broker.vaultCommand(management.id, management.revision, envelope);
    await assert.rejects(broker.vaultCommand(management.id, management.revision, envelope), /stale/);
    management = await broker.vaultView();
    assert.ok(!JSON.stringify(management).includes("test-password"));
    const account = management.accounts[0];
    const page = await browser.newPage(); await page.goto(base);
    const tab = (await broker.tabs()).find((tab) => tab.origin === base)!;
    assert.equal((await broker.loginSaved(tab.id)).status, "needs_user");
    assert.equal(await page.locator('[name=password]').inputValue(), "");
    await vault.save({ ...account, password: "", autoLogin: true });
    const pending = await broker.request(tab.id);
    assert.equal((await broker.loginSaved(tab.id)).status, "needs_user");
    let view = await broker.view(pending.id);
    await broker.command(view.id, view.revision, await encryptBrowserLoginCommand(view.publicKey, view.id, view.revision, { action: "fillSaved", accountId: account.id, origin: base }));
    assert.equal(await page.locator('[name=password]').inputValue(), "test-password");
    assert.ok(!JSON.stringify(await broker.view(pending.id)).includes("test-password"));
    broker.cancel(pending.id);
    for (const route of ["/cross", "/otp", "/register"]) {
      await page.goto(base + route);
      assert.equal((await broker.loginSaved(tab.id)).status, "needs_user");
      assert.equal(await page.locator('[name=password]').inputValue(), "");
    }
    await page.goto(base);
    const automatic = await broker.loginSaved(tab.id);
    assert.equal(automatic.status, "submitted");
    assert.ok(!JSON.stringify(automatic).includes("test-password"));
    assert.ok(!JSON.stringify(automatic).includes("test-user"));
    await page.waitForURL(/\/done/);
    assert.match(submitted, /email=test-user/); assert.match(submitted, /password=test-password/);
    await page.goto(base);
    assert.equal((await broker.loginSaved(tab.id)).status, "needs_user");
    // Explicit save during handoff updates credentials; normal fill never persists an OTP.
    const next = await broker.request(tab.id); view = await broker.view(next.id);
    const username = view.fields.find((field) => field.purpose === "username")!, password = view.fields.find((field) => field.purpose === "password")!;
    await broker.command(view.id, view.revision, await encryptBrowserLoginCommand(view.publicKey, view.id, view.revision, { action: "fill", origin: base, values: { [username.id]: "test-user", [password.id]: "new-password" }, saveAccount: true, autoLogin: false }));
    assert.equal((await vault.get(account.id, base)).password, "new-password");
    await assert.rejects(vault.get(account.id, base, true));
    await writeFile(path.join(directory, "vault.json"), "damaged vault");
    view = await broker.view(next.id);
    assert.equal(view.canSave, false);
    assert.deepEqual(view.savedAccounts, []);
    assert.equal(view.fields.length, 2); // One-time handoff still works if saved storage is unavailable.
  } finally {
    await broker.close(); await browser.close(); server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});

test("real Chromium handoff: fill, multi-step OTP, replay, navigation, cancel and expiry", { skip: !process.env.DOER_BROWSER_TEST_EXECUTABLE }, async () => {
  const server = createServer((req, res) => {
    res.setHeader("Content-Type", "text/html");
    if (new URL(req.url!, "http://localhost").pathname === "/done") return res.end("<h1>Signed in</h1>");
    if (new URL(req.url!, "http://localhost").pathname === "/otp") return res.end('<form method="post" action="/done"><label>Code<input name="code" autocomplete="one-time-code"></label><button>Verify</button></form>');
    if (new URL(req.url!, "http://localhost").pathname === "/cross") return res.end('<form method="post" action="https://example.com"><input name="password" type="password"></form>');
    res.end('<form method="post" action="/otp"><label>Email<input name="email"></label><label>Password<input name="password" type="password"></label><button>Sign in</button></form>');
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address() as net.AddressInfo;
  const base = `http://127.0.0.1:${address.port}`;
  const portServer = net.createServer();
  await new Promise<void>((resolve) => portServer.listen(0, "127.0.0.1", resolve));
  const port = (portServer.address() as net.AddressInfo).port;
  await new Promise<void>((resolve) => portServer.close(() => resolve()));
  const browser = await chromium.launch({ executablePath: process.env.DOER_BROWSER_TEST_EXECUTABLE, headless: true, args: [`--remote-debugging-port=${port}`] });
  const endpoint = `http://127.0.0.1:${port}`;
  const broker = new BrowserLoginBroker(endpoint);
  const expiryBroker = new BrowserLoginBroker(endpoint, 50);
  try {
    const page = await browser.newPage();
    await page.goto(base);
    const tab = (await broker.tabs()).find((tab) => tab.origin === base)!;
    const pending = await broker.request(tab.id);
    assert.equal((await broker.request(tab.id)).id, pending.id);
    assert.equal(broker.status(pending.id).status, "waiting");
    let view = await broker.view(pending.id);
    assert.equal(view.fields.length, 2);
    assert.ok(view.screenshot.length > 100);
    assert.ok(!JSON.stringify(broker.list()).includes("publicKey"));
    const password = view.fields.find((field) => field.type === "password")!;
    const envelope = await encryptBrowserLoginCommand(view.publicKey, view.id, view.revision, { action: "fill", origin: base, values: { [password.id]: "test-only-secret" } });
    await broker.command(view.id, view.revision, envelope);
    assert.equal(await page.locator('[name=password]').inputValue(), "test-only-secret");
    await assert.rejects(broker.command(view.id, view.revision, envelope), /stale/);
    view = await broker.view(pending.id);
    // A credential is not returned in a refreshed preview or field inventory.
    assert.ok(!JSON.stringify(view).includes("test-only-secret"));
    const button = await page.locator("button").boundingBox();
    const click = await encryptBrowserLoginCommand(view.publicKey, view.id, view.revision, { action: "click", origin: base, x: button!.x + 5, y: button!.y + 5 });
    await broker.command(view.id, view.revision, click);
    await page.waitForURL(/\/otp/);
    view = await broker.view(pending.id);
    const otp = view.fields[0];
    await broker.command(view.id, view.revision, await encryptBrowserLoginCommand(view.publicKey, view.id, view.revision, { action: "fill", origin: base, values: { [otp.id]: "123456" } }));
    assert.equal(await page.locator("input").inputValue(), "123456");
    await page.locator("button").click();
    await page.waitForURL(/\/done/);
    view = await broker.view(pending.id);
    await broker.command(view.id, view.revision, await encryptBrowserLoginCommand(view.publicKey, view.id, view.revision, { action: "complete" }));
    assert.equal(broker.status(pending.id).status, "completed");
    await assert.rejects(broker.view(pending.id), /no longer active/);
    await page.goto(base);
    const second = await broker.request(tab.id);
    view = await broker.view(second.id);
    const stale = await encryptBrowserLoginCommand(view.publicKey, view.id, view.revision, { action: "fill", origin: base, values: { [view.fields[0].id]: "never-inject" } });
    await page.goto(base + "/cross");
    await assert.rejects(broker.command(view.id, view.revision, stale), /Page changed/);
    assert.equal(await page.locator("input").inputValue(), "");
    view = await broker.view(second.id);
    await assert.rejects(broker.command(view.id, view.revision, await encryptBrowserLoginCommand(view.publicKey, view.id, view.revision, { action: "fill", origin: base, values: { [view.fields[0].id]: "never-inject" } })), /another site/);
    assert.equal(await page.locator("input").inputValue(), "");
    view = await broker.view(second.id);
    await broker.command(view.id, view.revision, await encryptBrowserLoginCommand(view.publicKey, view.id, view.revision, { action: "cancel" }));
    assert.equal(broker.status(second.id).status, "cancelled");
    const expiry = await expiryBroker.request(tab.id);
    await new Promise((resolve) => setTimeout(resolve, 70));
    assert.equal(expiryBroker.status(expiry.id).status, "expired");
    await assert.rejects(expiryBroker.view(expiry.id), /no longer active/);
  } finally {
    await broker.close(); await expiryBroker.close(); await browser.close();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});

test("general browser control: ordinary input, navigation, secure boundaries and exclusive human handoff", { skip: !process.env.DOER_BROWSER_TEST_EXECUTABLE }, async () => {
  const server = createServer((req, res) => {
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    if (req.url === "/login") return res.end('<title>Secure fixture</title><form><label>User<input name="username" autocomplete="username"></label><label>Secret<input id="secret" name="password" type="text"></label><button>Sign in</button></form>');
    res.end(`<title>Control fixture</title><h1>${req.url === "/next" ? "Next page" : "First page"}</h1><form><label>Search<input name="search"></label><label>Note<textarea name="note"></textarea></label><button type="button" onclick="document.querySelector('h1').textContent='Clicked'">Choose</button></form><div style="height:1500px">Scroll content</div>`);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const base = `http://127.0.0.1:${(server.address() as net.AddressInfo).port}`;
  const portServer = net.createServer();
  await new Promise<void>((resolve) => portServer.listen(0, "127.0.0.1", resolve));
  const port = (portServer.address() as net.AddressInfo).port;
  await new Promise<void>((resolve) => portServer.close(() => resolve()));
  const browser = await chromium.launch({ executablePath: process.env.DOER_BROWSER_TEST_EXECUTABLE, headless: true, args: [`--remote-debugging-port=${port}`] });
  const broker = new BrowserLoginBroker(`http://127.0.0.1:${port}`);
  try {
    const page = await browser.newPage(); await page.goto(base);
    const tab = (await broker.tabs()).find((tab) => tab.origin === base)!;
    let view = await broker.controlView(tab.id);
    assert.equal(view.title, "Control fixture");
    assert.ok(view.fields.every((field) => !field.sensitive));
    const search = view.fields.find((field) => field.label === "Search")!;
    await broker.controlAction(tab.id, view.revision, { action: "fill", fieldId: search.id, text: "ordinary search" });
    assert.equal(await page.locator('[name=search]').inputValue(), "ordinary search");
    await assert.rejects(broker.controlAction(tab.id, view.revision, { action: "reload" }), /stale/);
    view = await broker.controlView(tab.id);
    await broker.controlAction(tab.id, view.revision, { action: "fill", fieldId: view.fields.find((field) => field.type === "textarea")!.id, text: "line one\nline two" });
    assert.equal(await page.locator("textarea").inputValue(), "line one\nline two");
    // Human control invalidates agent snapshots and blocks actions/saved login on that tab.
    view = await broker.controlView(tab.id);
    const human = await broker.request(tab.id, "control", "Choose the desired option");
    assert.equal(human.kind, "control"); assert.equal(human.reason, "Choose the desired option");
    await assert.rejects(broker.controlView(tab.id), /user control/);
    await assert.rejects(broker.controlAction(tab.id, view.revision, { action: "reload" }), /user control/);
    assert.equal((await broker.loginSaved(tab.id)).status, "needs_user");
    let humanView = await broker.view(human.id);
    const ordinary = humanView.fields.find((field) => field.label === "Search")!;
    assert.equal(ordinary.sensitive, false);
    await broker.command(human.id, humanView.revision, await encryptBrowserLoginCommand(humanView.publicKey, human.id, humanView.revision, { action: "fill", origin: base, values: { [ordinary.id]: "human choice" } }));
    humanView = await broker.view(human.id);
    await broker.command(human.id, humanView.revision, await encryptBrowserLoginCommand(humanView.publicKey, human.id, humanView.revision, { action: "complete" }));
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(await page.locator('[name=search]').inputValue(), "human choice"); // Normal work is not erased on return.
    assert.equal(broker.status(human.id).status, "completed");
    view = await broker.controlView(tab.id);
    await broker.controlAction(tab.id, view.revision, { action: "navigate", url: base + "/next" });
    await page.waitForURL(base + "/next");
    view = await broker.controlView(tab.id);
    await broker.controlAction(tab.id, view.revision, { action: "back" }); await page.waitForURL(base + "/");
    view = await broker.controlView(tab.id);
    await broker.controlAction(tab.id, view.revision, { action: "forward" }); await page.waitForURL(base + "/next");
    view = await broker.controlView(tab.id);
    await broker.controlAction(tab.id, view.revision, { action: "scroll", delta: 500 });
    await page.waitForFunction(() => scrollY > 0);
    // Navigation after a snapshot cannot retarget a normal fill to a replacement document.
    view = await broker.controlView(tab.id);
    await page.goto(base + "/login");
    await assert.rejects(broker.controlAction(tab.id, view.revision, { action: "fill", fieldId: view.fields[0].id, text: "must not write" }), /Page changed/);
    view = await broker.controlView(tab.id);
    assert.ok(view.fields.every((field) => field.sensitive));
    const hidden = view.fields.find((field) => field.label === "Secret")!;
    await assert.rejects(broker.controlAction(tab.id, view.revision, { action: "fill", fieldId: hidden.id, text: "must not write" }), /Sensitive/);
    assert.equal(await page.locator("#secret").inputValue(), "");
    await page.locator("#secret").evaluate((element) => { (element as HTMLInputElement).value = "visible-secret-one"; });
    const redactedOne = await broker.controlView(tab.id);
    await page.locator("#secret").evaluate((element) => { (element as HTMLInputElement).value = "a-very-different-visible-secret-two"; });
    const redactedTwo = await broker.controlView(tab.id);
    assert.equal(redactedOne.screenshot, redactedTwo.screenshot); // Even a visible-password field is actually masked in the image.
    assert.ok(!JSON.stringify(redactedTwo).includes("visible-secret"));
    // Changing an ordinary field to a sensitive one after capture is checked atomically on fill.
    await page.goto(base); view = await broker.controlView(tab.id);
    await page.locator('[name=search]').evaluate((element) => { element.setAttribute("autocomplete", "one-time-code"); });
    await assert.rejects(broker.controlAction(tab.id, view.revision, { action: "fill", fieldId: view.fields[0].id, text: "123456" }), /secure input/);
    assert.equal(await page.locator('[name=search]').inputValue(), "");
    for (const url of ["javascript:alert(1)", "file:///etc/passwd", "https://user:secret@example.com", "data:text/html,test"]) await assert.rejects(broker.openTab(url));
    const opened = await broker.openTab(base + "/next");
    assert.ok((await broker.tabs()).some((tab) => tab.id === opened.tabId));
    const second = await broker.request(tab.id, "control");
    humanView = await broker.view(second.id);
    await broker.command(second.id, humanView.revision, await encryptBrowserLoginCommand(humanView.publicKey, second.id, humanView.revision, { action: "navigate", origin: base, url: base + "/next" }));
    await page.waitForURL(base + "/next");
    broker.cancel(second.id);
  } finally {
    await broker.close(); await browser.close(); server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
