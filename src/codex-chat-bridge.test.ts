import assert from "node:assert/strict";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { test } from "node:test";
import { startCodexChatBridge, type CodexChatBridge } from "./codex-chat-bridge.js";

type UpstreamHandler = (args: {
  body: Record<string, unknown>;
  req: IncomingMessage;
  res: ServerResponse;
}) => void | Promise<void>;

type BridgeFixture = {
  bridge: CodexChatBridge;
  capturedBodies: Record<string, unknown>[];
  logs: string[];
  close: () => Promise<void>;
};

async function startBridgeFixture(handler: UpstreamHandler, options?: { providerId?: string }): Promise<BridgeFixture> {
  const capturedBodies: Record<string, unknown>[] = [];
  const logs: string[] = [];
  const upstream = createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) {
      chunks.push(Buffer.from(chunk));
    }
    const raw = Buffer.concat(chunks).toString("utf8").trim();
    const body = raw ? JSON.parse(raw) as Record<string, unknown> : {};
    capturedBodies.push(body);
    await handler({ body, req, res });
  });
  await listen(upstream);
  const address = upstream.address();
  assert(address && typeof address !== "string");
  const bridge = await startCodexChatBridge({
    providerApiKey: "upstream-test-key",
    providerId: options?.providerId ?? "openai-compatible",
    providerName: "test upstream",
    targetBaseUrl: `http://127.0.0.1:${address.port}`,
    onLog: (message) => logs.push(message),
  });
  return {
    bridge,
    capturedBodies,
    logs,
    close: async () => {
      await bridge.stop();
      await closeServer(upstream);
    },
  };
}

function listen(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
}

function closeServer(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

async function responsesRequest(args: {
  bridge: CodexChatBridge;
  body: Record<string, unknown>;
}): Promise<Response> {
  return await fetch(`${args.bridge.baseUrl}/responses`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${args.bridge.apiKey}`,
      "content-type": "application/json",
    },
    body: JSON.stringify(args.body),
  });
}

function writeChatJson(res: ServerResponse, body: Record<string, unknown>, status = 200): void {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}

function writeChatSse(res: ServerResponse, values: unknown[], options?: { keepOpen?: boolean }): void {
  res.writeHead(200, {
    "content-type": "text/event-stream; charset=utf-8",
    "cache-control": "no-cache",
  });
  for (const value of values) {
    res.write(`data: ${typeof value === "string" ? value : JSON.stringify(value)}\n\n`);
  }
  if (!options?.keepOpen) {
    res.end();
  }
}

function parseSse(raw: string): Array<{ event: string; data: Record<string, unknown> }> {
  return raw
    .split(/\n\n+/)
    .map((block) => block.trim())
    .filter(Boolean)
    .map((block) => {
      const event = block.match(/^event: (.+)$/m)?.[1];
      const data = block.match(/^data: (.+)$/m)?.[1];
      assert(event, `missing event in block: ${block}`);
      assert(data, `missing data in block: ${block}`);
      return { event, data: JSON.parse(data) as Record<string, unknown> };
    });
}

test("maps Responses create request fields to Chat Completions", async () => {
  const fixture = await startBridgeFixture(({ res }) => {
    writeChatJson(res, {
      choices: [{ message: { role: "assistant", content: "mapped" } }],
      usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
    });
  });
  try {
    const response = await responsesRequest({
      bridge: fixture.bridge,
      body: {
        model: "glm-5.2",
        instructions: "system instruction",
        input: [
          {
            role: "developer",
            content: [{ type: "input_text", text: "developer note" }],
          },
          {
            role: "user",
            content: [
              { type: "input_text", text: "look at this" },
              { type: "input_image", image_url: "data:image/png;base64,AA==", detail: "high" },
              { type: "input_file", filename: "notes.txt", file_id: "file_123" },
            ],
          },
          {
            type: "function_call",
            call_id: "call_123",
            name: "lookup",
            arguments: "{\"q\":\"doer\"}",
          },
          {
            type: "function_call_output",
            call_id: "call_123",
            output: { ok: true },
          },
        ],
        tools: [{
          type: "function",
          name: "lookup",
          description: "Lookup a value",
          parameters: { type: "object", properties: { q: { type: "string" } } },
        }],
        tool_choice: { type: "function", name: "lookup" },
        text: { format: { type: "json_schema", name: "result", strict: true, schema: { type: "object" } } },
        max_output_tokens: 64,
        temperature: 0.2,
        top_p: 0.8,
        presence_penalty: 0.1,
        frequency_penalty: 0.2,
        seed: 42,
        stop: ["END"],
        parallel_tool_calls: false,
        user: "user_123",
      },
    });
    assert.equal(response.status, 200);
    const chat = fixture.capturedBodies[0]!;
    assert.equal(chat.model, "glm-5.2");
    assert.equal(chat.stream, false);
    assert.equal(chat.max_tokens, 64);
    assert.equal(chat.temperature, 0.2);
    assert.equal(chat.top_p, 0.8);
    assert.equal(chat.presence_penalty, 0.1);
    assert.equal(chat.frequency_penalty, 0.2);
    assert.equal(chat.seed, 42);
    assert.deepEqual(chat.stop, ["END"]);
    assert.equal(chat.parallel_tool_calls, false);
    assert.equal(chat.user, "user_123");
    assert.deepEqual(chat.tool_choice, { type: "function", function: { name: "lookup" } });
    assert.deepEqual(chat.response_format, {
      type: "json_schema",
      json_schema: { name: "result", schema: { type: "object" }, strict: true },
    });
    assert.deepEqual(chat.tools, [{
      type: "function",
      function: {
        name: "lookup",
        description: "Lookup a value",
        parameters: { type: "object", properties: { q: { type: "string" } } },
      },
    }]);
    const messages = chat.messages as Array<Record<string, unknown>>;
    assert.deepEqual(messages[0], { role: "system", content: "system instruction" });
    assert.deepEqual(messages[1], { role: "system", content: "developer note" });
    assert.equal(messages[2]?.role, "user");
    assert.deepEqual(messages[2]?.content, [
      { type: "text", text: "look at this" },
      { type: "image_url", image_url: { url: "data:image/png;base64,AA==", detail: "high" } },
      { type: "text", text: "[attached file: notes.txt file_id=file_123]" },
    ]);
    assert.deepEqual(messages[3], {
      role: "assistant",
      content: null,
      tool_calls: [{
        id: "call_123",
        type: "function",
        function: { name: "lookup", arguments: "{\"q\":\"doer\"}" },
      }],
    });
    assert.deepEqual(messages[4], { role: "tool", tool_call_id: "call_123", content: "{\"ok\":true}" });
  } finally {
    await fixture.close();
  }
});

test("returns a Responses object for non-streaming assistant text and usage", async () => {
  const fixture = await startBridgeFixture(({ res }) => {
    writeChatJson(res, {
      choices: [{ message: { role: "assistant", content: "안녕 하세요" } }],
      usage: {
        prompt_tokens: 11,
        completion_tokens: 5,
        total_tokens: 16,
        prompt_tokens_details: { cached_tokens: 3 },
        completion_tokens_details: { reasoning_tokens: 2 },
      },
    });
  });
  try {
    const response = await responsesRequest({
      bridge: fixture.bridge,
      body: {
        model: "glm-5.2",
        stream: false,
        input: "hello",
        metadata: { smoke: "non-stream" },
        previous_response_id: "resp_prev",
        truncation: "auto",
      },
    });
    assert.equal(response.status, 200);
    const body = await response.json() as Record<string, unknown>;
    assert.equal(body.object, "response");
    assert.equal(body.status, "completed");
    assert.equal(body.model, "glm-5.2");
    assert.deepEqual(body.metadata, { smoke: "non-stream" });
    assert.equal(body.previous_response_id, "resp_prev");
    assert.equal(body.truncation, "auto");
    assert.deepEqual(body.usage, {
      input_tokens: 11,
      input_tokens_details: { cached_tokens: 3 },
      output_tokens: 5,
      output_tokens_details: { reasoning_tokens: 2 },
      total_tokens: 16,
    });
    const output = body.output as Array<Record<string, unknown>>;
    assert.equal(output[0]?.type, "message");
    assert.deepEqual(output[0]?.content, [{ type: "output_text", text: "안녕 하세요", annotations: [] }]);
  } finally {
    await fixture.close();
  }
});

test("drops unsupported Responses hosted tools before forwarding to Chat Completions", async () => {
  const fixture = await startBridgeFixture(({ res }) => {
    writeChatJson(res, {
      choices: [{ message: { role: "assistant", content: "ok" } }],
    });
  });
  try {
    const response = await responsesRequest({
      bridge: fixture.bridge,
      body: {
        model: "glm-5.2",
        input: "hello",
        tools: [
          { type: "function", name: "lookup", parameters: { type: "object", properties: {} } },
          {
            type: "namespace",
            name: "doer_daemon",
            description: "Manage daemons",
            tools: [{
              name: "daemon_list",
              description: "List daemons",
              input_schema: { type: "object", properties: { includeStopped: { type: "boolean" } } },
            }],
          },
          { type: "web_search_preview" },
          { type: "file_search", vector_store_ids: ["vs_123"] },
          { type: "local_shell" },
          { type: "computer_use_preview" },
        ],
        tool_choice: { type: "web_search_preview" },
      },
    });
    assert.equal(response.status, 200);
    const chat = fixture.capturedBodies[0]!;
    assert.deepEqual(chat.tools, [{
      type: "function",
      function: {
        name: "lookup",
        description: "",
        parameters: { type: "object", properties: {} },
      },
    }, {
      type: "function",
      function: {
        name: "mcp__doer_daemon__daemon_list",
        description: "Namespace: mcp__doer_daemon__. Original tool: daemon_list. List daemons",
        parameters: { type: "object", properties: { includeStopped: { type: "boolean" } } },
      },
    }]);
    assert.equal(chat.tool_choice, undefined);
    assert.deepEqual(fixture.logs.filter((line) => line.includes("dropped unsupported Responses tool")), [
      "Codex chat bridge dropped unsupported Responses tool type=web_search_preview",
      "Codex chat bridge dropped unsupported Responses tool type=file_search",
      "Codex chat bridge dropped unsupported Responses tool type=local_shell",
      "Codex chat bridge dropped unsupported Responses tool type=computer_use_preview",
    ]);
  } finally {
    await fixture.close();
  }
});

test("returns function_call output for non-streaming Chat tool calls", async () => {
  const fixture = await startBridgeFixture(({ res }) => {
    writeChatJson(res, {
      choices: [{
        message: {
          role: "assistant",
          content: null,
          tool_calls: [{
            id: "call_abc",
            type: "function",
            function: { name: "mcp__doer_daemon__daemon_list", arguments: "{}" },
          }],
        },
      }],
    });
  });
  try {
    const response = await responsesRequest({ bridge: fixture.bridge, body: { stream: false, input: "call tool" } });
    assert.equal(response.status, 200);
    const body = await response.json() as Record<string, unknown>;
    assert.deepEqual(body.output, [{
      type: "function_call",
      id: "call_abc",
      call_id: "call_abc",
      namespace: "mcp__doer_daemon",
      name: "daemon_list",
      arguments: "{}",
      status: "completed",
    }]);
  } finally {
    await fixture.close();
  }
});

test("maps Responses create request fields to Anthropic Messages", async () => {
  const fixture = await startBridgeFixture(({ req, res }) => {
    assert.equal(req.url, "/messages");
    assert.equal(req.headers["x-api-key"], "upstream-test-key");
    assert.equal(req.headers["anthropic-version"], "2023-06-01");
    writeChatJson(res, {
      content: [
        { type: "text", text: "claude mapped" },
        { type: "tool_use", id: "toolu_123", name: "mcp__doer_daemon__daemon_list", input: { includeStopped: true } },
      ],
      usage: { input_tokens: 7, output_tokens: 3 },
    });
  }, { providerId: "anthropic" });
  try {
    const response = await responsesRequest({
      bridge: fixture.bridge,
      body: {
        model: "claude-sonnet-4-6",
        instructions: "system instruction",
        input: [
          { role: "developer", content: [{ type: "input_text", text: "developer note" }] },
          {
            role: "user",
            content: [
              { type: "input_text", text: "look at this" },
              { type: "input_image", image_url: "data:image/png;base64,AA==", detail: "high" },
            ],
          },
          { type: "function_call", call_id: "call_123", name: "lookup", arguments: "{\"q\":\"doer\"}" },
          { type: "function_call_output", call_id: "call_123", output: "tool result" },
        ],
        tools: [{
          type: "namespace",
          name: "doer_daemon",
          tools: [{ name: "daemon_list", description: "List daemons", input_schema: { type: "object" } }],
        }],
        tool_choice: "required",
        max_output_tokens: 64,
      },
    });
    assert.equal(response.status, 200);
    const anthropic = fixture.capturedBodies[0]!;
    assert.equal(anthropic.model, "claude-sonnet-4-6");
    assert.equal(anthropic.stream, false);
    assert.equal(anthropic.max_tokens, 64);
    assert.equal(anthropic.system, "system instruction\n\ndeveloper note");
    assert.deepEqual(anthropic.tool_choice, { type: "any" });
    assert.deepEqual(anthropic.tools, [{
      name: "mcp__doer_daemon__daemon_list",
      description: "Namespace: mcp__doer_daemon__. Original tool: daemon_list. List daemons",
      input_schema: { type: "object" },
    }]);
    assert.deepEqual(anthropic.messages, [
      {
        role: "user",
        content: [
          { type: "text", text: "look at this" },
          { type: "image", source: { type: "base64", media_type: "image/png", data: "AA==" } },
        ],
      },
      {
        role: "assistant",
        content: [{ type: "tool_use", id: "call_123", name: "lookup", input: { q: "doer" } }],
      },
      {
        role: "user",
        content: [{ type: "tool_result", tool_use_id: "call_123", content: "tool result" }],
      },
    ]);
    const body = await response.json() as Record<string, unknown>;
    assert.deepEqual(body.usage, { input_tokens: 7, output_tokens: 3, total_tokens: 10 });
    const output = body.output as Array<Record<string, unknown>>;
    assert.equal(output[0]?.type, "message");
    assert.deepEqual(output[1], {
      type: "function_call",
      id: "toolu_123",
      call_id: "toolu_123",
      namespace: "mcp__doer_daemon",
      name: "daemon_list",
      arguments: "{\"includeStopped\":true}",
      status: "completed",
    });
  } finally {
    await fixture.close();
  }
});

test("streams Anthropic Messages text and tool_use as Responses events", async () => {
  const fixture = await startBridgeFixture(({ res }) => {
    writeChatSse(res, [
      { type: "message_start", message: { usage: { input_tokens: 5, output_tokens: 0 } } },
      { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
      { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "hello" } },
      { type: "content_block_stop", index: 0 },
      { type: "content_block_start", index: 1, content_block: { type: "tool_use", id: "toolu_abc", name: "lookup", input: {} } },
      { type: "content_block_delta", index: 1, delta: { type: "input_json_delta", partial_json: "{\"q\"" } },
      { type: "content_block_delta", index: 1, delta: { type: "input_json_delta", partial_json: ":\"doer\"}" } },
      { type: "content_block_stop", index: 1 },
      { type: "message_delta", usage: { output_tokens: 4 } },
      { type: "message_stop" },
    ]);
  }, { providerId: "anthropic" });
  try {
    const response = await responsesRequest({
      bridge: fixture.bridge,
      body: { model: "claude-sonnet-4-6", stream: true, input: "hello" },
    });
    const events = parseSse(await response.text());
    assert.equal(response.status, 200);
    assert.equal(fixture.capturedBodies[0]?.stream, true);
    assert.equal(events.find((event) => event.event === "response.output_text.delta")?.data.delta, "hello");
    assert.deepEqual(
      events.filter((event) => event.event === "response.function_call_arguments.delta").map((event) => event.data.delta),
      ["{\"q\"", ":\"doer\"}"],
    );
    const completed = events.at(-1)?.data.response as Record<string, unknown>;
    assert.deepEqual(completed.usage, { input_tokens: 5, output_tokens: 4, total_tokens: 9 });
    assert.deepEqual(completed.output, [
      {
        id: (completed.output as Array<Record<string, unknown>>)[0]?.id,
        type: "message",
        status: "completed",
        role: "assistant",
        content: [{ type: "output_text", text: "hello", annotations: [] }],
      },
      {
        id: "fc_toolu_abc",
        type: "function_call",
        status: "completed",
        call_id: "toolu_abc",
        name: "lookup",
        arguments: "{\"q\":\"doer\"}",
      },
    ]);
  } finally {
    await fixture.close();
  }
});

test("normalizes upstream errors for non-streaming requests", async () => {
  const fixture = await startBridgeFixture(({ res }) => {
    writeChatJson(res, { error: { message: "bad upstream", code: "bad_request" } }, 400);
  });
  try {
    const response = await responsesRequest({ bridge: fixture.bridge, body: { stream: false, input: "fail" } });
    assert.equal(response.status, 400);
    const body = await response.json() as Record<string, Record<string, unknown>>;
    assert.equal(body.error?.message, "bad upstream");
    assert.equal(body.error?.type, "server_error");
    assert.equal(body.error?.status, 400);
    assert.deepEqual(body.error?.upstream, { message: "bad upstream", code: "bad_request" });
  } finally {
    await fixture.close();
  }
});

test("streams Responses text lifecycle events, preserves whitespace, and stops at DONE", async () => {
  const fixture = await startBridgeFixture(({ res }) => {
    writeChatSse(res, [
      { choices: [{ delta: { content: "안녕" } }] },
      { choices: [{ delta: { content: " 하세요" } }] },
      { choices: [], usage: { prompt_tokens: 4, completion_tokens: 2, total_tokens: 6 } },
      "[DONE]",
    ], { keepOpen: true });
  });
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 3_000);
    const response = await fetch(`${fixture.bridge.baseUrl}/responses`, {
      method: "POST",
      signal: controller.signal,
      headers: {
        authorization: `Bearer ${fixture.bridge.apiKey}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ model: "glm-5.2", stream: true, input: "hello" }),
    });
    const raw = await response.text();
    clearTimeout(timeout);
    assert.equal(response.status, 200);
    const chat = fixture.capturedBodies[0]!;
    assert.equal(chat.stream, true);
    assert.deepEqual(chat.stream_options, { include_usage: true });
    const events = parseSse(raw);
    assert.deepEqual(events.map((event) => event.event), [
      "response.created",
      "response.in_progress",
      "response.output_item.added",
      "response.content_part.added",
      "response.output_text.delta",
      "response.output_text.delta",
      "response.output_text.done",
      "response.content_part.done",
      "response.output_item.done",
      "response.completed",
    ]);
    assert.equal(events[4]?.data.delta, "안녕");
    assert.equal(events[5]?.data.delta, " 하세요");
    assert.equal(events[6]?.data.text, "안녕 하세요");
    const completed = events.at(-1)?.data.response as Record<string, unknown>;
    assert.equal(completed.status, "completed");
    assert.deepEqual(completed.usage, { input_tokens: 4, output_tokens: 2, total_tokens: 6 });
  } finally {
    await fixture.close();
  }
});

test("streams reasoning text as fallback assistant text", async () => {
  const fixture = await startBridgeFixture(({ res }) => {
    writeChatSse(res, [
      { choices: [{ delta: { reasoning_content: "생각 결과" } }] },
      "[DONE]",
    ]);
  });
  try {
    const response = await responsesRequest({ bridge: fixture.bridge, body: { stream: true, input: "reason" } });
    const events = parseSse(await response.text());
    assert.equal(response.status, 200);
    assert.equal(events.find((event) => event.event === "response.output_text.delta")?.data.delta, "생각 결과");
    const done = events.find((event) => event.event === "response.output_text.done");
    assert.equal(done?.data.text, "생각 결과");
  } finally {
    await fixture.close();
  }
});

test("streams function_call argument events from Chat tool call deltas", async () => {
  const fixture = await startBridgeFixture(({ res }) => {
    writeChatSse(res, [
      { choices: [{ delta: { tool_calls: [{ index: 0, id: "call_xyz", function: { name: "lookup", arguments: "{\"q\"" } }] } }] },
      { choices: [{ delta: { tool_calls: [{ index: 0, id: "call_xyz", function: { arguments: ":\"doer\"}" } }] } }] },
      "[DONE]",
    ]);
  });
  try {
    const response = await responsesRequest({ bridge: fixture.bridge, body: { stream: true, input: "tool" } });
    const events = parseSse(await response.text());
    assert.equal(response.status, 200);
    const added = events.find((event) => event.event === "response.output_item.added");
    assert.deepEqual(added?.data.item, {
      id: "fc_call_xyz",
      type: "function_call",
      status: "in_progress",
      call_id: "call_xyz",
      name: "lookup",
      arguments: "",
    });
    assert.deepEqual(
      events.filter((event) => event.event === "response.function_call_arguments.delta").map((event) => event.data.delta),
      ["{\"q\"", ":\"doer\"}"],
    );
    const done = events.find((event) => event.event === "response.function_call_arguments.done");
    assert.equal(done?.data.arguments, "{\"q\":\"doer\"}");
    const completed = events.at(-1)?.data.response as Record<string, unknown>;
    assert.deepEqual(completed.output, [{
      id: "fc_call_xyz",
      type: "function_call",
      status: "completed",
      call_id: "call_xyz",
      name: "lookup",
      arguments: "{\"q\":\"doer\"}",
    }]);
  } finally {
    await fixture.close();
  }
});

test("emits response.failed for upstream streaming errors and malformed chunks", async () => {
  const upstreamFailure = await startBridgeFixture(({ res }) => {
    writeChatJson(res, { error: { message: "stream bad" } }, 429);
  });
  try {
    const response = await responsesRequest({ bridge: upstreamFailure.bridge, body: { stream: true, input: "fail" } });
    const events = parseSse(await response.text());
    assert.equal(response.status, 200);
    assert.equal(events.at(-1)?.event, "response.failed");
    const failed = events.at(-1)?.data.response as Record<string, unknown>;
    assert.equal(failed.status, "failed");
    assert.match((failed.error as Record<string, unknown>).message as string, /429/);
  } finally {
    await upstreamFailure.close();
  }

  const malformed = await startBridgeFixture(({ res }) => {
    writeChatSse(res, ["{not json"]);
  });
  try {
    const response = await responsesRequest({ bridge: malformed.bridge, body: { stream: true, input: "bad json" } });
    const events = parseSse(await response.text());
    assert.equal(response.status, 200);
    assert.equal(events.at(-1)?.event, "response.failed");
    const failed = events.at(-1)?.data.response as Record<string, unknown>;
    assert.equal(failed.status, "failed");
    assert.match((failed.error as Record<string, unknown>).message as string, /parse/);
  } finally {
    await malformed.close();
  }
});

test("rejects missing or invalid bridge authorization", async () => {
  const fixture = await startBridgeFixture(({ res }) => {
    writeChatJson(res, { choices: [{ message: { content: "unused" } }] });
  });
  try {
    const response = await fetch(`${fixture.bridge.baseUrl}/responses`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ input: "hello" }),
    });
    assert.equal(response.status, 401);
    assert.deepEqual(await response.json(), { error: { message: "Invalid Authorization header" } });
  } finally {
    await fixture.close();
  }
});
