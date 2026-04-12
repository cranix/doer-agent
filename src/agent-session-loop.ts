import type { NatsConnection } from "nats";

interface ConnectedAgentSession {
  nc: NatsConnection;
  servers: string[];
  stream: string;
  subject: string;
  durable: string;
}

export async function runConnectedAgentSession(args: {
  jetstream: ConnectedAgentSession;
  serverBaseUrl: string;
  userId: string;
  agentToken: string;
  heartbeatIntervalMs: number;
  heartbeatFailureThreshold: number;
  formatTimestamp: () => string;
  heartbeatAgentSession: (args: {
    nc: NatsConnection;
    serverBaseUrl: string;
    userId: string;
    agentToken: string;
  }) => Promise<void>;
  subscribeAll: () => void;
  stopAllSessionWatchers: () => void;
  onInfraError: (message: string) => void;
  sleep: (ms: number) => Promise<void>;
}): Promise<void> {
  let heartbeatFailures = 0;
  let heartbeatInFlight = false;
  let sessionInvalidated = false;

  const invalidateAgentSession = (reason: string) => {
    if (sessionInvalidated) {
      return;
    }
    sessionInvalidated = true;
    args.onInfraError(`closing nats session: ${reason}`);
    void args.jetstream.nc.close().catch((error) => {
      const message = error instanceof Error ? error.message : String(error);
      args.onInfraError(`failed to close nats session: ${message}`);
    });
  };

  const heartbeatTimer = setInterval(() => {
    if (heartbeatInFlight || sessionInvalidated) {
      return;
    }
    heartbeatInFlight = true;
    void args.heartbeatAgentSession({
      nc: args.jetstream.nc,
      serverBaseUrl: args.serverBaseUrl,
      userId: args.userId,
      agentToken: args.agentToken,
    })
      .then(() => {
        heartbeatInFlight = false;
        if (heartbeatFailures > 0) {
          args.onInfraError(`heartbeat reconnected at=${args.formatTimestamp()}`);
        }
        heartbeatFailures = 0;
      })
      .catch((error) => {
        heartbeatInFlight = false;
        const message = error instanceof Error ? error.message : String(error);
        heartbeatFailures += 1;
        if (heartbeatFailures > 1) {
          args.onInfraError(
            `heartbeat failed: ${message} (count=${heartbeatFailures}/${args.heartbeatFailureThreshold})`,
          );
        }
        if (heartbeatFailures >= args.heartbeatFailureThreshold) {
          invalidateAgentSession(`heartbeat failure threshold reached at=${args.formatTimestamp()}`);
        }
      });
  }, args.heartbeatIntervalMs);

  args.subscribeAll();

  const closeError = await args.jetstream.nc.closed();
  clearInterval(heartbeatTimer);
  args.stopAllSessionWatchers();
  const detail = closeError instanceof Error ? closeError.message : "clean close";
  args.onInfraError(`nats session ended: ${detail}; reconnecting`);
  await args.sleep(1000);
}
