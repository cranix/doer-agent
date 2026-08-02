import assert from "node:assert/strict";
import test from "node:test";
import type { NatsConnection } from "nats";
import { buildNatsConnectionOptions } from "./agent-jetstream.js";
import {
  heartbeatAgentSession,
  publishNatsBestEffort,
  type JsonRequestOptions,
} from "./agent-runtime-io.js";

test("NATS connection detects stale networks quickly and keeps reconnecting", () => {
  const options = buildNatsConnectionOptions({
    servers: ["tls://nats.example.com:4222"],
    token: "secret",
  });

  assert.equal(options.pingInterval, 2_000);
  assert.equal(options.maxPingOut, 2);
  assert.equal(options.maxReconnectAttempts, -1);
  assert.equal(options.reconnectTimeWait, 1_000);
  assert.equal(options.token, "secret");
});

test("heartbeat stops waiting when a NATS flush stalls", async () => {
  const startedAt = Date.now();
  await assert.rejects(
    heartbeatAgentSession({
      nc: { flush: () => new Promise<void>(() => undefined) } as NatsConnection,
      serverBaseUrl: "https://doer.example.com",
      userId: "user-1",
      agentToken: "token-1",
      timeoutMs: 20,
      postJson: async <T>() => ({ ok: true }) as T,
    }),
    /nats flush timed out after 20ms/,
  );
  assert.ok(Date.now() - startedAt < 500);
});

test("heartbeat forwards its timeout to the HTTP probe", async () => {
  let observedTimeout: number | undefined;
  await heartbeatAgentSession({
    nc: { flush: async () => undefined } as NatsConnection,
    serverBaseUrl: "https://doer.example.com",
    userId: "user-1",
    agentToken: "token-1",
    timeoutMs: 3_000,
    postJson: async <T>(_url: string, _body: unknown, options?: JsonRequestOptions) => {
      observedTimeout = options?.timeoutMs;
      return { ok: true } as T;
    },
  });

  assert.equal(observedTimeout, 3_000);
});

test("Codex notifications are dropped instead of crashing after NATS closes", () => {
  const errors: string[] = [];
  let publishCalled = false;
  const published = publishNatsBestEffort({
    nc: {
      isClosed: () => true,
      publish: () => {
        publishCalled = true;
      },
    } as unknown as NatsConnection,
    subject: "agent.codex.events",
    data: new Uint8Array([1]),
    context: "failed to forward codex notification",
    onError: (message) => errors.push(message),
  });

  assert.equal(published, false);
  assert.equal(publishCalled, false);
  assert.deepEqual(errors, [
    "failed to forward codex notification: NATS connection is closed; event dropped",
  ]);
});

test("Codex notification publish errors stay non-fatal during a close race", () => {
  const errors: string[] = [];
  const published = publishNatsBestEffort({
    nc: {
      isClosed: () => false,
      publish: () => {
        throw new Error("CONNECTION_CLOSED");
      },
    } as unknown as NatsConnection,
    subject: "agent.codex.events",
    data: new Uint8Array([1]),
    context: "failed to forward codex notification",
    onError: (message) => errors.push(message),
  });

  assert.equal(published, false);
  assert.deepEqual(errors, [
    "failed to forward codex notification: CONNECTION_CLOSED; event dropped",
  ]);
});
