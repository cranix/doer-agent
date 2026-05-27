import { StringCodec, type Msg, type NatsConnection } from "nats";
import type { CodexAppServerManager } from "./codex-app-server-manager.js";

const codec = StringCodec();
const SESSION_TIMEOUT_MS = 180_000;

interface NotesAiRpcRequest {
  requestId?: unknown;
  action?: unknown;
  agentId?: unknown;
  sessionId?: unknown;
  eventsSubject?: unknown;
  document?: unknown;
  selection?: unknown;
  instruction?: unknown;
}

interface ActiveNotesAiSession {
  abortController: AbortController;
}

const activeSessions = new Map<string, ActiveNotesAiSession>();

function stringValue(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function recordValue(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}

function publishEvent(args: {
  nc: NatsConnection;
  subject: string;
  payload: Record<string, unknown>;
}): void {
  args.nc.publish(args.subject, codec.encode(JSON.stringify(args.payload)));
}

function respond(msg: Msg, payload: Record<string, unknown>): void {
  msg.respond(codec.encode(JSON.stringify(payload)));
}

function buildCodexUserInput(prompt: string): unknown[] {
  return [{
    type: "text",
    text: prompt,
    text_elements: [],
  }];
}

function buildNotesAiPrompt(request: NotesAiRpcRequest): string {
  const document = stringValue(request.document);
  const selection = stringValue(request.selection);
  const instruction = stringValue(request.instruction);
  const parts = [
    "You are editing a Markdown note inside Doer.",
    "Return only Markdown content. Do not include explanations, preambles, or code fences unless the requested content itself needs them.",
    "If a selection is provided, return only the replacement for that selection. If no selection is provided, return content to insert at the cursor.",
    "",
    `<instruction>\n${instruction}\n</instruction>`,
    `<document>\n${document}\n</document>`,
  ];
  if (selection) {
    parts.push(`<selection>\n${selection}\n</selection>`);
  }
  return parts.join("\n\n");
}

function isTerminalTurnMethod(method: string): boolean {
  return method === "turn/completed" ||
    method === "turn/failed" ||
    method === "turn/error" ||
    method === "turn/cancelled" ||
    method === "turn/canceled" ||
    method === "turn/interrupted" ||
    method === "turn/aborted";
}

function threadIdFromParams(params: unknown): string {
  const record = recordValue(params);
  const thread = recordValue(record?.thread);
  return stringValue(record?.threadId) || stringValue(thread?.id);
}

function turnIdFromParams(params: unknown): string {
  const record = recordValue(params);
  const turn = recordValue(record?.turn);
  return stringValue(record?.turnId) || stringValue(turn?.id);
}

function agentMessageDeltaFromParams(params: unknown): string {
  const record = recordValue(params);
  if (!record) {
    return "";
  }
  return stringValue(record.delta) || stringValue(record.text);
}

function terminalErrorFromParams(params: unknown): string {
  const record = recordValue(params);
  const error = recordValue(record?.error);
  return stringValue(record?.message) ||
    stringValue(error?.message) ||
    stringValue(record?.reason);
}

async function archiveThread(args: {
  manager: CodexAppServerManager;
  threadId: string;
  onError: (message: string) => void;
}): Promise<void> {
  try {
    await args.manager.request("thread/archive", { threadId: args.threadId }, 30_000);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    args.onError(`notes ai thread archive failed threadId=${args.threadId} error=${message}`);
  }
}

async function runNotesAiSession(args: {
  nc: NatsConnection;
  manager: CodexAppServerManager;
  request: NotesAiRpcRequest;
  sessionId: string;
  eventsSubject: string;
  abortController: AbortController;
  onError: (message: string) => void;
}): Promise<void> {
  const instruction = stringValue(args.request.instruction);
  if (!instruction) {
    throw new Error("instruction is required");
  }
  if (args.abortController.signal.aborted) {
    return;
  }

  let threadId = "";
  let turnId = "";
  let settled = false;
  let cleanupNotification = () => {};
  let settleCompleted = (_callback: () => void) => {};
  const completed = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      if (!settled) {
        reject(new Error("Timed out while waiting for Codex notes AI result"));
      }
    }, SESSION_TIMEOUT_MS);
    settleCompleted = (callback: () => void) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      cleanupNotification();
      callback();
    };
    args.abortController.signal.addEventListener("abort", () => {
      settleCompleted(() => resolve());
    }, { once: true });
    cleanupNotification = args.manager.onNotification((method, params) => {
      const eventThreadId = threadIdFromParams(params);
      const eventTurnId = turnIdFromParams(params);
      const isSessionEvent =
        (threadId && eventThreadId === threadId) ||
        (turnId && eventTurnId === turnId);
      if (!isSessionEvent) {
        return;
      }
      if (turnId && eventTurnId && eventTurnId !== turnId) {
        return;
      }
      if (method === "item/agentMessage/delta") {
        const text = agentMessageDeltaFromParams(params);
        if (!text) {
          return;
        }
        publishEvent({
          nc: args.nc,
          subject: args.eventsSubject,
          payload: { type: "delta", sessionId: args.sessionId, text },
        });
        return;
      }
      if (!isTerminalTurnMethod(method)) {
        return;
      }
      if (method === "turn/completed") {
        settleCompleted(() => {
          publishEvent({
            nc: args.nc,
            subject: args.eventsSubject,
            payload: { type: "done", sessionId: args.sessionId },
          });
          resolve();
        });
        return;
      }
      const message = terminalErrorFromParams(params) || `Codex notes AI turn ended with ${method}`;
      settleCompleted(() => reject(new Error(message)));
    });
  });

  try {
    const threadResult = recordValue(await args.manager.request("thread/start", {
      cwd: null,
      sessionStartSource: "clear",
    }, 30_000));
    const thread = recordValue(threadResult?.thread);
    threadId = stringValue(thread?.id);
    if (!threadId) {
      throw new Error("Codex app-server did not return a thread id");
    }
    if (args.abortController.signal.aborted) {
      return;
    }

    const turnResult = recordValue(await args.manager.request("turn/start", {
      threadId,
      input: buildCodexUserInput(buildNotesAiPrompt(args.request)),
    }, 30_000));
    const turn = recordValue(turnResult?.turn);
    turnId = stringValue(turn?.id);
    if (!turnId) {
      throw new Error("Codex app-server did not return a turn id");
    }
    if (args.abortController.signal.aborted) {
      return;
    }

    await completed.finally(() => cleanupNotification());
  } catch (error) {
    settleCompleted(() => {});
    throw error;
  } finally {
    if (threadId) {
      await archiveThread({
        manager: args.manager,
        threadId,
        onError: args.onError,
      });
    }
  }
}

async function handleStart(args: {
  msg: Msg;
  nc: NatsConnection;
  manager: CodexAppServerManager;
  request: NotesAiRpcRequest;
  agentId: string;
  onError: (message: string) => void;
}): Promise<void> {
  const sessionId = stringValue(args.request.sessionId);
  const eventsSubject = stringValue(args.request.eventsSubject);
  if (!sessionId || !eventsSubject) {
    throw new Error("sessionId and eventsSubject are required");
  }
  if (stringValue(args.request.agentId) && stringValue(args.request.agentId) !== args.agentId) {
    throw new Error("agent id mismatch");
  }
  activeSessions.get(sessionId)?.abortController.abort();
  const abortController = new AbortController();
  activeSessions.set(sessionId, { abortController });
  respond(args.msg, { requestId: args.request.requestId, ok: true, action: "start", sessionId });

  void runNotesAiSession({
    nc: args.nc,
    manager: args.manager,
    request: args.request,
    sessionId,
    eventsSubject,
    abortController,
    onError: args.onError,
  }).catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    if (!abortController.signal.aborted) {
      publishEvent({
        nc: args.nc,
        subject: eventsSubject,
        payload: { type: "error", sessionId, error: message },
      });
      args.onError(`notes ai session failed sessionId=${sessionId} error=${message}`);
    }
  }).finally(() => {
    if (activeSessions.get(sessionId)?.abortController === abortController) {
      activeSessions.delete(sessionId);
    }
  });
}

export async function handleNotesAiRpcMessage(args: {
  msg: Msg;
  nc: NatsConnection;
  manager: CodexAppServerManager;
  agentId: string;
  onError: (message: string) => void;
}): Promise<void> {
  let payload: NotesAiRpcRequest = {};
  try {
    payload = JSON.parse(codec.decode(args.msg.data)) as NotesAiRpcRequest;
    const action = stringValue(payload.action);
    if (action === "cancel") {
      const sessionId = stringValue(payload.sessionId);
      activeSessions.get(sessionId)?.abortController.abort();
      activeSessions.delete(sessionId);
      respond(args.msg, { requestId: payload.requestId, ok: true, action, sessionId });
      return;
    }
    if (action !== "start") {
      throw new Error("unsupported notes ai action");
    }
    await handleStart({
      msg: args.msg,
      nc: args.nc,
      manager: args.manager,
      request: payload,
      agentId: args.agentId,
      onError: args.onError,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "unknown error";
    respond(args.msg, {
      requestId: payload.requestId,
      ok: false,
      action: stringValue(payload.action),
      error: message,
    });
    args.onError(`notes ai rpc failed action=${stringValue(payload.action) || "unknown"} error=${message}`);
  }
}

export function subscribeToNotesAiRpc(args: {
  nc: NatsConnection;
  subject: string;
  manager: CodexAppServerManager;
  agentId: string;
  onInfo: (message: string) => void;
  onError: (message: string) => void;
}): void {
  args.nc.subscribe(args.subject, {
    callback: (error, msg) => {
      if (error) {
        const message = error instanceof Error ? error.message : String(error);
        args.onError(`notes ai rpc subscription error: ${message}`);
        return;
      }
      void handleNotesAiRpcMessage({
        msg,
        nc: args.nc,
        manager: args.manager,
        agentId: args.agentId,
        onError: args.onError,
      });
    },
  });
  args.onInfo(`notes ai rpc subscribed subject=${args.subject}`);
}
