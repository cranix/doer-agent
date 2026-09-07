import assert from "node:assert/strict";
import test from "node:test";
import { CodexServerRequestBroker } from "./codex-server-request-broker.js";

test("lists and resolves interactive Codex server requests", async () => {
  const events: string[] = [];
  const broker = new CodexServerRequestBroker(
    () => ({ answers: {} }),
    (event, request) => events.push(`${event}:${request.requestId}`),
  );
  const responsePromise = broker.waitForResponse({
    requestId: "request-1",
    method: "item/tool/requestUserInput",
    params: { threadId: "thread-1", turnId: "turn-1", questions: [] },
  });

  assert.deepEqual(broker.list("thread-1").map((request) => request.requestId), ["request-1"]);
  assert.equal(broker.respond("request-1", { answers: { choice: { answers: ["Yes"] } } }), true);
  assert.deepEqual(await responsePromise, { answers: { choice: { answers: ["Yes"] } } });
  assert.deepEqual(broker.list("thread-1"), []);
  assert.deepEqual(events, ["pending:request-1", "resolved:request-1"]);
});

test("settles pending requests with a safe fallback on close", async () => {
  const broker = new CodexServerRequestBroker((method) => ({ fallback: method }));
  const responsePromise = broker.waitForResponse({
    requestId: 7,
    method: "mcpServer/elicitation/request",
    params: { threadId: "thread-2" },
  });

  broker.close();
  assert.deepEqual(await responsePromise, { fallback: "mcpServer/elicitation/request" });
  assert.deepEqual(broker.list(), []);
});
