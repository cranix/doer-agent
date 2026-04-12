import {
  AckPolicy,
  connect,
  DeliverPolicy,
  JSONCodec,
  RetentionPolicy,
  StorageType,
  type JetStreamClient,
  type JetStreamManager,
  type NatsConnection,
} from "nats";

export interface AgentJetStreamContext {
  nc: NatsConnection;
  js: JetStreamClient;
  jsm: JetStreamManager;
  codec: ReturnType<typeof JSONCodec<Record<string, unknown>>>;
  subject: string;
  stream: string;
  durable: string;
  servers: string[];
}

export function normalizeNatsServers(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((item): item is string => typeof item === "string").map((v) => v.trim()).filter((v) => v.length > 0);
}

export function normalizeNatsToken(value: unknown): string | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const auth = value as Record<string, unknown>;
  const token = typeof auth.token === "string" ? auth.token.trim() : "";
  return token.length > 0 ? token : null;
}

function formatNatsStatusData(value: unknown): string {
  if (value === null || value === undefined) {
    return "null";
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

async function ensureJetStreamInfra(args: {
  jsm: JetStreamManager;
  stream: string;
  subject: string;
  durable?: string;
}): Promise<void> {
  const streamInfo = await args.jsm.streams.info(args.stream).catch(() => null);
  if (!streamInfo) {
    await args.jsm.streams.add({
      name: args.stream,
      subjects: [args.subject],
      storage: StorageType.File,
      retention: RetentionPolicy.Limits,
    });
  }

  if (args.durable) {
    const consumerInfo = await args.jsm.consumers.info(args.stream, args.durable).catch(() => null);
    if (!consumerInfo) {
      await args.jsm.consumers.add(args.stream, {
        durable_name: args.durable,
        ack_policy: AckPolicy.Explicit,
        deliver_policy: DeliverPolicy.All,
        filter_subject: args.subject,
        ack_wait: 30_000_000_000,
      });
    }
  }
}

async function initJetStreamContext(args: {
  userId: string;
  servers: string[];
  token: string | null;
  sanitizeUserId: (userId: string) => string;
  onInfraError: (message: string) => void;
}): Promise<AgentJetStreamContext> {
  const sanitized = args.sanitizeUserId(args.userId);
  const stream = `DOER_AGENT_EVENTS_${sanitized}`;
  const subject = `doer.agent.events.${sanitized}`;
  const durable = `doer-agent-uploader-${sanitized}`;

  const nc = await connect(args.token ? { servers: args.servers, token: args.token } : { servers: args.servers });
  const jsm = await nc.jetstreamManager();
  await ensureJetStreamInfra({ jsm, stream, subject, durable });

  void (async () => {
    try {
      for await (const status of nc.status()) {
        const statusType = typeof status.type === "string" ? status.type : "unknown";
        if (statusType === "pingTimer") {
          continue;
        }
        const statusData = formatNatsStatusData((status as { data?: unknown }).data);
        args.onInfraError("nats status type=" + statusType + " data=" + statusData);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      args.onInfraError(`nats status loop ended: ${message}`);
    }
  })();

  return {
    nc,
    js: nc.jetstream(),
    jsm,
    codec: JSONCodec<Record<string, unknown>>(),
    subject,
    stream,
    durable,
    servers: args.servers,
  };
}

export async function connectBootstrapWithRetry<TBootstrap>(args: {
  serverBaseUrl: string;
  userId: string;
  agentToken: string;
  postJson: <T>(url: string, body: unknown) => Promise<T>;
  sanitizeUserId: (userId: string) => string;
  onInfraError: (message: string) => void;
  onError: (message: string) => void;
  sleep: (ms: number) => Promise<void>;
}): Promise<{
  natsBootstrap: TBootstrap;
  jetstream: AgentJetStreamContext;
}> {
  let attempt = 0;
  while (true) {
    attempt += 1;
    try {
      const natsBootstrap = await args.postJson<TBootstrap>(`${args.serverBaseUrl}/api/agent/nats`, {
        userId: args.userId,
        agentToken: args.agentToken,
      });
      const bootstrapRecord = natsBootstrap as Record<string, unknown>;
      const natsServers = normalizeNatsServers(bootstrapRecord.servers);
      if (natsServers.length === 0) {
        throw new Error("No NATS servers configured by server");
      }
      const natsToken = normalizeNatsToken(bootstrapRecord.auth);
      const jetstream = await initJetStreamContext({
        userId: args.userId,
        servers: natsServers,
        token: natsToken,
        sanitizeUserId: args.sanitizeUserId,
        onInfraError: args.onInfraError,
      });
      args.onInfraError(`bootstrap ok servers=${natsServers.length} eventStream=${jetstream.stream} eventSubject=${jetstream.subject}`);
      return { natsBootstrap, jetstream };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const retryMs = Math.min(30_000, 1000 * Math.max(1, attempt));
      args.onError(`bootstrap failed: ${message} (retry in ${Math.floor(retryMs / 1000)}s, attempt=${attempt})`);
      await args.sleep(retryMs);
    }
  }
}
