import assert from "node:assert/strict";
import test from "node:test";
import type { NatsConnection } from "nats";
import { subscribeToCodexAppRpc } from "./agent-codex-app-rpc.js";
import type { CodexAppServerManager } from "./codex-app-server-manager.js";

test("keeps the Codex app-server alive when a NATS session closes", async () => {
  let resolveClosed: (() => void) | undefined;
  const closed = new Promise<void>((resolve) => {
    resolveClosed = resolve;
  });
  let stopCalls = 0;
  const manager = {
    stop: async () => {
      stopCalls += 1;
    },
  } as unknown as CodexAppServerManager;
  const nc = {
    closed: () => closed,
    subscribe: () => ({}),
  } as unknown as NatsConnection;

  subscribeToCodexAppRpc({
    nc,
    subject: "agent.codex.rpc",
    eventsSubject: "agent.codex.events",
    agentId: "agent-1",
    manager,
    onInfo: () => undefined,
    onError: () => undefined,
  });

  resolveClosed?.();
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(stopCalls, 0);
});
