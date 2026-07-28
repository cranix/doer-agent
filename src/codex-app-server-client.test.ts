import assert from "node:assert/strict";
import { mkdtemp, readFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { CodexAppServerClient } from "./codex-app-server-client.js";

const fixtureSource = String.raw`
  const fs = require("node:fs");
  const readline = require("node:readline");
  const tracePath = process.env.DOER_CODEX_FIXTURE_TRACE;
  const trace = (value) => fs.appendFileSync(tracePath, value + "\n");
  const send = (value) => process.stdout.write(JSON.stringify(value) + "\n");
  let waitingRequestId = null;
  readline.createInterface({ input: process.stdin }).on("line", (line) => {
    const message = JSON.parse(line);
    if (message.method === "initialize") {
      send({ id: message.id, result: {} });
      return;
    }
    if (message.method === "test/server-request") {
      waitingRequestId = message.id;
      send({
        id: "server-request-1",
        method: "item/tool/call",
        params: { tool: "fixture", arguments: {} },
      });
      return;
    }
    if (message.id === "server-request-1") {
      trace("server-request-response");
      send({ id: waitingRequestId, result: message.result });
      return;
    }
    if (message.method === "test/start-turn") {
      send({
        method: "turn/started",
        params: {
          threadId: "thread-1",
          turn: { id: "turn-1", status: "inProgress" },
        },
      });
      send({ id: message.id, result: {} });
      return;
    }
    if (message.method === "turn/interrupt") {
      trace("turn-interrupt");
      send({ id: message.id, result: {} });
      send({
        method: "turn/completed",
        params: {
          threadId: "thread-1",
          turn: { id: "turn-1", status: "interrupted" },
        },
      });
    }
  });
  process.on("SIGTERM", () => {
    trace("sigterm");
    process.exit(0);
  });
`;

test("handles server requests and drains active turns before shutdown", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "doer-app-server-client-"));
  const tracePath = path.join(tempDir, "trace.log");
  const client = new CodexAppServerClient({
    cwd: tempDir,
    args: [],
    executable: process.execPath,
    executableArgs: ["--eval", fixtureSource],
    env: {
      ...process.env,
      DOER_CODEX_FIXTURE_TRACE: tracePath,
    },
    gracefulShutdownTimeoutMs: 2_000,
    onServerRequest: async (method, params) => {
      assert.equal(method, "item/tool/call");
      assert.deepEqual(params, { tool: "fixture", arguments: {} });
      return { contentItems: [], success: true };
    },
  });

  const serverRequestResult = await client.request("test/server-request");
  assert.deepEqual(serverRequestResult, { contentItems: [], success: true });

  await client.request("test/start-turn");
  await client.stop();

  const trace = (await readFile(tracePath, "utf8")).trim().split("\n");
  assert.deepEqual(trace, [
    "server-request-response",
    "turn-interrupt",
    "sigterm",
  ]);
});
