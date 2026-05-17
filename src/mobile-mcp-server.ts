import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import * as z from "zod/v4";

interface MobileAgentRecord {
  deviceId: string;
  deviceName: string;
  platform: string;
  status?: string;
  lastSeenAt?: string | null;
  createdAt?: string;
  updatedAt?: string;
}

interface MobileLogEvent {
  seq: number;
  kind: string;
  observedAt: string;
  payload: unknown;
}

interface MobileLogPage extends Record<string, unknown> {
  events: MobileLogEvent[];
  hasMore?: boolean;
  latestSeq?: number | null;
  nextBeforeSeq?: number | null;
}

interface MobileInfo {
  deviceId: string;
  deviceName: string;
  platform: string;
  appVersion: string | null;
  logCount: number;
  lastObservedAt: string | null;
}

interface MobileActiveNotificationsSnapshot extends Record<string, unknown> {
  mobileAgent?: MobileAgentRecord;
  notifications: unknown[];
  count?: number;
}

function env(name: string): string {
  const value = process.env[name]?.trim() || "";
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function optionalEnv(name: string): string {
  return process.env[name]?.trim() || "";
}

function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function normalizeLimit(value: number | undefined, fallback: number, max: number): number {
  if (!Number.isFinite(value) || !value) {
    return fallback;
  }
  return Math.min(max, Math.max(1, Math.trunc(value)));
}

function getConfig(): {
  agentId: string;
  agentToken: string;
  serverBaseUrl: string;
  userId: string;
} {
  return {
    agentId: env("DOER_MOBILE_AGENT_ID"),
    agentToken: env("DOER_AGENT_TOKEN"),
    serverBaseUrl: env("DOER_MOBILE_SERVER_BASE_URL").replace(/\/$/, ""),
    userId: env("DOER_MOBILE_USER_ID"),
  };
}

async function requestJson<T>(path: string): Promise<T> {
  const config = getConfig();
  const response = await fetch(`${config.serverBaseUrl}${path}`, {
    headers: {
      Authorization: `Bearer ${config.agentToken}`,
      Accept: "application/json",
    },
  });
  const data = await response.json().catch(() => ({})) as { error?: unknown };
  if (!response.ok) {
    throw new Error(typeof data.error === "string" ? data.error : `Doer server returned ${response.status}`);
  }
  return data as T;
}

async function postJson<T>(path: string, body: Record<string, unknown>): Promise<T> {
  const config = getConfig();
  const response = await fetch(`${config.serverBaseUrl}${path}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.agentToken}`,
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  const data = await response.json().catch(() => ({})) as { error?: unknown };
  if (!response.ok) {
    throw new Error(typeof data.error === "string" ? data.error : `Doer server returned ${response.status}`);
  }
  return data as T;
}

async function listMobileAgents(): Promise<MobileAgentRecord[]> {
  const config = getConfig();
  const data = await requestJson<{ mobileAgents?: MobileAgentRecord[] }>(
    `/api/users/${encodeURIComponent(config.userId)}/agents/${encodeURIComponent(config.agentId)}/mobile-agents`,
  );
  return Array.isArray(data.mobileAgents) ? data.mobileAgents : [];
}

async function resolveDeviceId(deviceId?: string): Promise<string> {
  const normalized = deviceId?.trim() || "";
  if (normalized) {
    return normalized;
  }
  const mobileAgents = await listMobileAgents();
  const first = mobileAgents[0]?.deviceId?.trim() || "";
  if (!first) {
    throw new Error("No mobile agents registered");
  }
  return first;
}

async function getMobileInfo(deviceId?: string): Promise<{ mobileAgent?: MobileAgentRecord; info: MobileInfo }> {
  const config = getConfig();
  const resolvedDeviceId = await resolveDeviceId(deviceId);
  return await requestJson<{ mobileAgent?: MobileAgentRecord; info: MobileInfo }>(
    `/api/users/${encodeURIComponent(config.userId)}/agents/${encodeURIComponent(config.agentId)}/mobile-agents/${encodeURIComponent(resolvedDeviceId)}/info`,
  );
}

async function getMobileActiveNotifications(deviceId?: string): Promise<MobileActiveNotificationsSnapshot> {
  const config = getConfig();
  const resolvedDeviceId = await resolveDeviceId(deviceId);
  const data = await requestJson<MobileActiveNotificationsSnapshot>(
    `/api/users/${encodeURIComponent(config.userId)}/agents/${encodeURIComponent(config.agentId)}/mobile-agents/${encodeURIComponent(resolvedDeviceId)}/active-notifications`,
  );
  return {
    ...data,
    notifications: Array.isArray(data.notifications) ? data.notifications : [],
  };
}

async function showMobileNotification(args: {
  deviceId?: string;
  notificationId?: number;
  text: string;
  title: string;
}): Promise<Record<string, unknown>> {
  const config = getConfig();
  const resolvedDeviceId = await resolveDeviceId(args.deviceId);
  return await postJson<Record<string, unknown>>(
    `/api/users/${encodeURIComponent(config.userId)}/agents/${encodeURIComponent(config.agentId)}/mobile-agents/${encodeURIComponent(resolvedDeviceId)}/show-notification`,
    {
      title: args.title,
      text: args.text,
      notificationId: args.notificationId,
    },
  );
}


async function getMobileLogs(args: {
  afterSeq?: number;
  beforeSeq?: number;
  deviceId?: string;
  limit?: number;
}): Promise<MobileLogPage> {
  const config = getConfig();
  const resolvedDeviceId = await resolveDeviceId(args.deviceId);
  const qs = new URLSearchParams({
    limit: String(normalizeLimit(args.limit, 100, 1000)),
  });
  if (Number.isInteger(args.beforeSeq)) {
    qs.set("beforeSeq", String(args.beforeSeq));
  }
  if (Number.isInteger(args.afterSeq)) {
    qs.set("afterSeq", String(args.afterSeq));
  }
  const data = await requestJson<MobileLogPage>(
    `/api/users/${encodeURIComponent(config.userId)}/agents/${encodeURIComponent(config.agentId)}/mobile-agents/${encodeURIComponent(resolvedDeviceId)}/events?${qs.toString()}`,
  );
  return {
    events: Array.isArray(data.events) ? data.events : [],
    hasMore: data.hasMore === true,
    latestSeq: typeof data.latestSeq === "number" ? data.latestSeq : null,
    nextBeforeSeq: typeof data.nextBeforeSeq === "number" ? data.nextBeforeSeq : null,
  };
}

async function searchMobileLogs(args: {
  afterSeq?: number;
  beforeSeq?: number;
  deviceId?: string;
  kind?: string;
  limit?: number;
  query?: string;
  since?: string;
  until?: string;
}): Promise<MobileLogPage> {
  const config = getConfig();
  const resolvedDeviceId = await resolveDeviceId(args.deviceId);
  const qs = new URLSearchParams({
    limit: String(normalizeLimit(args.limit, 100, 1000)),
  });
  if (args.kind?.trim()) {
    qs.set("kind", args.kind.trim());
  }
  if (args.query?.trim()) {
    qs.set("query", args.query.trim());
  }
  if (args.since?.trim()) {
    qs.set("since", args.since.trim());
  }
  if (args.until?.trim()) {
    qs.set("until", args.until.trim());
  }
  if (Number.isInteger(args.beforeSeq)) {
    qs.set("beforeSeq", String(args.beforeSeq));
  }
  if (Number.isInteger(args.afterSeq)) {
    qs.set("afterSeq", String(args.afterSeq));
  }
  const data = await requestJson<MobileLogPage>(
    `/api/users/${encodeURIComponent(config.userId)}/agents/${encodeURIComponent(config.agentId)}/mobile-agents/${encodeURIComponent(resolvedDeviceId)}/events?${qs.toString()}`,
  );
  return {
    events: Array.isArray(data.events) ? data.events : [],
    hasMore: data.hasMore === true,
    latestSeq: typeof data.latestSeq === "number" ? data.latestSeq : null,
    nextBeforeSeq: typeof data.nextBeforeSeq === "number" ? data.nextBeforeSeq : null,
  };
}

function extractLatestLocation(events: MobileLogEvent[]): MobileLogEvent | null {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index];
    if (event?.kind === "location.current" || event?.kind === "location.changed") {
      return event;
    }
  }
  return null;
}

function extractLocationEvents(events: MobileLogEvent[]): MobileLogEvent[] {
  return events.filter((event) => event.kind.startsWith("location."));
}

function extractNotifications(events: MobileLogEvent[]): MobileLogEvent[] {
  return events.filter((event) => event.kind.startsWith("notification."));
}

async function main(): Promise<void> {
  const server = new McpServer({
    name: "doer-mobile",
    version: "0.1.0",
  }, {
    capabilities: {
      tools: {},
    },
    instructions: "Inspect and control mobile devices paired with the current Doer agent. Use list/get/read/search tools for device context, and show tools for user-visible mobile actions.",
  });

  server.registerTool("mobile_list_devices", {
    description: "List mobile agents paired with the current Doer agent.",
    inputSchema: {},
  }, async () => {
    const mobileAgents = await listMobileAgents();
    return {
      content: [{ type: "text", text: formatJson({ mobileAgents }) }],
      structuredContent: { mobileAgents },
    };
  });

  server.registerTool("mobile_get_device_info", {
    description: "Read info for a mobile agent. Defaults to the first registered device.",
    inputSchema: {
      deviceId: z.string().optional().describe("Mobile device id. Defaults to the first registered mobile agent."),
    },
  }, async ({ deviceId }) => {
    const result = await getMobileInfo(deviceId);
    return {
      content: [{ type: "text", text: formatJson(result) }],
      structuredContent: result,
    };
  });

  server.registerTool("mobile_read_event_logs", {
    description: "Read a cursor-based page of mobile log events from the device-local SQLite log.",
    inputSchema: {
      afterSeq: z.number().int().min(0).optional().describe("Return events newer than this seq, in ascending order."),
      beforeSeq: z.number().int().min(1).optional().describe("Return events older than this seq, in ascending order."),
      deviceId: z.string().optional().describe("Mobile device id. Defaults to the first registered mobile agent."),
      limit: z.number().int().min(1).max(1000).optional().describe("Maximum number of recent events."),
    },
  }, async ({ afterSeq, beforeSeq, deviceId, limit }) => {
    const page = await getMobileLogs({ afterSeq, beforeSeq, deviceId, limit });
    return {
      content: [{ type: "text", text: formatJson(page) }],
      structuredContent: page,
    };
  });

  server.registerTool("mobile_search_event_logs", {
    description: "Search the device-local mobile SQLite event log by kind, text query, cursor, and observed time range.",
    inputSchema: {
      afterSeq: z.number().int().min(0).optional().describe("Return matching events newer than this seq, in ascending order."),
      beforeSeq: z.number().int().min(1).optional().describe("Return matching events older than this seq, in ascending order."),
      deviceId: z.string().optional().describe("Mobile device id. Defaults to the first registered mobile agent."),
      kind: z.string().optional().describe("Event kind or kind prefix, such as location.current or notification."),
      query: z.string().optional().describe("Case-insensitive text to search in the event kind or payload."),
      since: z.string().optional().describe("Inclusive ISO timestamp lower bound."),
      until: z.string().optional().describe("Inclusive ISO timestamp upper bound."),
      limit: z.number().int().min(1).max(1000).optional().describe("Maximum number of matching events."),
    },
  }, async ({ afterSeq, beforeSeq, deviceId, kind, query, since, until, limit }) => {
    const page = await searchMobileLogs({ afterSeq, beforeSeq, deviceId, kind, query, since, until, limit });
    return {
      content: [{ type: "text", text: formatJson(page) }],
      structuredContent: page,
    };
  });

  server.registerTool("mobile_get_latest_location", {
    description: "Return the latest location event from recent mobile logs.",
    inputSchema: {
      deviceId: z.string().optional().describe("Mobile device id. Defaults to the first registered mobile agent."),
      limit: z.number().int().min(1).max(1000).optional().describe("How many recent events to scan."),
    },
  }, async ({ deviceId, limit }) => {
    const page = await searchMobileLogs({ deviceId, kind: "location.", limit: limit ?? 200 });
    const location = extractLatestLocation(page.events);
    return {
      content: [{ type: "text", text: formatJson({ location }) }],
      structuredContent: { location },
    };
  });

  server.registerTool("mobile_search_location_logs", {
    description: "Return time-range location events recorded by the mobile agent.",
    inputSchema: {
      afterSeq: z.number().int().min(0).optional().describe("Return location events newer than this seq, in ascending order."),
      beforeSeq: z.number().int().min(1).optional().describe("Return location events older than this seq, in ascending order."),
      deviceId: z.string().optional().describe("Mobile device id. Defaults to the first registered mobile agent."),
      since: z.string().optional().describe("Inclusive ISO timestamp lower bound."),
      until: z.string().optional().describe("Inclusive ISO timestamp upper bound."),
      limit: z.number().int().min(1).max(1000).optional().describe("Maximum number of matching location events."),
    },
  }, async ({ afterSeq, beforeSeq, deviceId, since, until, limit }) => {
    const page = await searchMobileLogs({ afterSeq, beforeSeq, deviceId, kind: "location.", since, until, limit: limit ?? 200 });
    const events = extractLocationEvents(page.events);
    const result = { ...page, events };
    return {
      content: [{ type: "text", text: formatJson(result) }],
      structuredContent: result,
    };
  });

  server.registerTool("mobile_get_active_notifications", {
    description: "Return the current active notification snapshot from a mobile device without storing it in the log.",
    inputSchema: {
      deviceId: z.string().optional().describe("Mobile device id. Defaults to the first registered mobile agent."),
    },
  }, async ({ deviceId }) => {
    const snapshot = await getMobileActiveNotifications(deviceId);
    return {
      content: [{ type: "text", text: formatJson(snapshot) }],
      structuredContent: snapshot,
    };
  });

  server.registerTool("mobile_show_alert_notification", {
    description: "Show a high-priority notification on a mobile device through the Doer mobile agent.",
    inputSchema: {
      deviceId: z.string().optional().describe("Mobile device id. Defaults to the first registered mobile agent."),
      title: z.string().min(1).describe("Notification title."),
      text: z.string().min(1).describe("Notification body text."),
      notificationId: z.number().int().optional().describe("Optional Android notification id. Reusing an id updates the existing notification."),
    },
  }, async ({ deviceId, title, text, notificationId }) => {
    const result = await showMobileNotification({ deviceId, title, text, notificationId });
    return {
      content: [{ type: "text", text: formatJson(result) }],
      structuredContent: result,
    };
  });

  server.registerTool("mobile_search_notification_logs", {
    description: "Return recent notification events stored in mobile logs.",
    inputSchema: {
      deviceId: z.string().optional().describe("Mobile device id. Defaults to the first registered mobile agent."),
      limit: z.number().int().min(1).max(1000).optional().describe("How many recent events to scan."),
    },
  }, async ({ deviceId, limit }) => {
    const page = await searchMobileLogs({ deviceId, kind: "notification.", limit: limit ?? 200 });
    const events = extractNotifications(page.events);
    return {
      content: [{ type: "text", text: formatJson({ events }) }],
      structuredContent: { events },
    };
  });

  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((error) => {
  const message = error instanceof Error ? error.stack || error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
