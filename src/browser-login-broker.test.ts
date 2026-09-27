import assert from "node:assert/strict";
import { test } from "node:test";
import { generateKeyPairSync, publicEncrypt, createPublicKey, randomBytes, createCipheriv, constants } from "node:crypto";
import { createServer } from "node:http";
import net from "node:net";
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
