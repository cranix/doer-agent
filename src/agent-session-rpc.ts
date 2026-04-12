import { watch, type FSWatcher } from "node:fs";
import { open, readdir, realpath, rm, rmdir, stat, unlink } from "node:fs/promises";
import path from "node:path";
import { StringCodec, type Msg, type NatsConnection } from "nats";

const sessionRpcCodec = StringCodec();
const activeSessionWatchers = new Map<string, () => void>();
const sessionLineIndexCache = new Map<string, SessionLineIndexCacheEntry>();
const SESSION_RPC_BLOB_KEYS = new Set([
  "image_url",
  "image_base64",
  "content_base64",
  "file_data",
  "bytes",
  "data",
]);

type AgentSessionRpcAction = "list" | "messages" | "delete" | "watch" | "stop_watch";

interface AgentSessionRpcRequest {
  requestId?: unknown;
  action?: unknown;
  agentId?: unknown;
  filePath?: unknown;
  sessionId?: unknown;
  sinceLine?: unknown;
  beforeRowId?: unknown;
  pageSize?: unknown;
  responseSubject?: unknown;
  watchId?: unknown;
}

interface AgentSessionRpcResponse {
  requestId: string;
  ok: boolean;
  action?: AgentSessionRpcAction;
  sessions?: unknown[];
  rawRows?: unknown[];
  nextCursor?: number;
  hasMoreBefore?: boolean;
  oldestRowId?: number | null;
  watchId?: string | null;
  event?: Record<string, unknown> | null;
  error?: string;
}

interface AgentSessionRpcNormalizedRequest {
  requestId: string;
  action: AgentSessionRpcAction;
  agentId: string;
  filePath: string | null;
  sessionId: string | null;
  sinceLine: number;
  beforeRowId: number | null;
  pageSize: number;
  responseSubject: string;
  watchId: string | null;
}

interface AgentSessionSummaryRecord {
  id: string;
  label: string;
  updatedAt: string;
  cwd: string | null;
  source: string;
  originator: string;
  filePath: string;
}

interface AgentSessionRawRowRecord {
  id: number;
  raw: string;
}

interface SessionLineIndexCacheEntry {
  size: number;
  lineStartOffsets: number[];
  endsWithNewline: boolean;
}

function getSessionsRootPath(workspaceRoot: string): string {
  return path.join(workspaceRoot, ".codex", "sessions");
}

function resolveSessionFilePath(workspaceRoot: string, filePath: string): string {
  const root = path.resolve(getSessionsRootPath(workspaceRoot));
  const resolved = path.resolve(filePath);
  if (!(resolved === root || resolved.startsWith(root + path.sep))) {
    throw new Error("filePath is outside sessions root");
  }
  return resolved;
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function normalizeSessionRpcRequest(args: {
  request: AgentSessionRpcRequest;
  agentId: string;
}): AgentSessionRpcNormalizedRequest {
  const requestId = typeof args.request.requestId === "string" ? args.request.requestId.trim() : "";
  if (!requestId) {
    throw new Error("missing requestId");
  }
  const requestAgentId = typeof args.request.agentId === "string" ? args.request.agentId.trim() : "";
  if (!requestAgentId || requestAgentId !== args.agentId) {
    throw new Error("agent id mismatch");
  }
  const actionRaw = typeof args.request.action === "string" ? args.request.action.trim() : "";
  const action: AgentSessionRpcAction =
    actionRaw === "messages" || actionRaw === "delete" || actionRaw === "watch" || actionRaw === "stop_watch"
      ? actionRaw
      : "list";
  const responseSubject = typeof args.request.responseSubject === "string" ? args.request.responseSubject.trim() : "";
  if (!responseSubject) {
    throw new Error("missing responseSubject");
  }
  const filePath = typeof args.request.filePath === "string" && args.request.filePath.trim() ? args.request.filePath.trim() : null;
  if ((action === "messages" || action === "delete" || action === "watch") && !filePath) {
    throw new Error("missing filePath");
  }
  const sinceLineRaw = Number(args.request.sinceLine);
  const sinceLine = Number.isInteger(sinceLineRaw) && sinceLineRaw > 0 ? sinceLineRaw : 0;
  const beforeRowIdRaw = Number(args.request.beforeRowId);
  const beforeRowId = Number.isInteger(beforeRowIdRaw) && beforeRowIdRaw > 0 ? beforeRowIdRaw : null;
  const pageSizeRaw = Number(args.request.pageSize);
  const pageSize = Number.isFinite(pageSizeRaw) ? Math.max(1, Math.min(Math.floor(pageSizeRaw), 100)) : 100;
  const watchId = typeof args.request.watchId === "string" && args.request.watchId.trim() ? args.request.watchId.trim() : null;
  if (action === "stop_watch" && !watchId) {
    throw new Error("missing watchId");
  }
  return {
    requestId,
    action,
    agentId: requestAgentId,
    filePath,
    sessionId: typeof args.request.sessionId === "string" && args.request.sessionId.trim() ? args.request.sessionId.trim() : null,
    sinceLine,
    beforeRowId,
    pageSize,
    responseSubject,
    watchId,
  };
}

function isInlineBlobString(value: string): boolean {
  const trimmed = value.trim();
  if (!trimmed) {
    return false;
  }
  return trimmed.startsWith("data:") || trimmed.includes(";base64,");
}

function buildInlineBlobMarker(value: string): string {
  const trimmed = value.trim();
  if (trimmed.startsWith("data:")) {
    const mimeEnd = trimmed.indexOf(";");
    const mimeType = mimeEnd > 5 ? trimmed.slice(5, mimeEnd) : "";
    if (mimeType) {
      return `[inline blob omitted: ${mimeType}]`;
    }
  }
  return "[inline blob omitted]";
}

function sanitizeSessionRpcPayload(value: unknown): unknown {
  if (typeof value === "string") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((entry) => sanitizeSessionRpcPayload(entry));
  }
  if (!isObjectRecord(value)) {
    return value;
  }

  const sanitized: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(value)) {
    if (SESSION_RPC_BLOB_KEYS.has(key) && typeof entry === "string" && isInlineBlobString(entry)) {
      sanitized[key] = buildInlineBlobMarker(entry);
      continue;
    }
    sanitized[key] = sanitizeSessionRpcPayload(entry);
  }
  return sanitized;
}

function sanitizeSessionRpcRawLine(line: string): string | null {
  const trimmed = line.trim();
  if (!trimmed.startsWith("{")) {
    return line;
  }

  try {
    const parsed = JSON.parse(line) as { type?: unknown; payload?: unknown };
    if (!isObjectRecord(parsed)) {
      return line;
    }
    if (parsed.type === "compacted" || parsed.type === "turn_context" || parsed.type === "session_meta") {
      return null;
    }
    if (!isObjectRecord(parsed.payload) || parsed.type !== "response_item") {
      return line;
    }
    const payloadType = typeof parsed.payload.type === "string" ? parsed.payload.type : "";
    if (payloadType === "message" || payloadType === "reasoning") {
      return null;
    }
    return JSON.stringify({
      ...parsed,
      payload: sanitizeSessionRpcPayload(parsed.payload),
    });
  } catch {
    return line;
  }
}

function toTrimmedStringOrNull(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed || null;
}

function pickSessionString(...values: unknown[]): string | null {
  for (const value of values) {
    const picked = toTrimmedStringOrNull(value);
    if (picked) {
      return picked;
    }
  }
  return null;
}

export async function collectSessionJsonlFiles(workspaceRoot: string): Promise<Array<{ filePath: string; mtimeMs: number }>> {
  const out: Array<{ filePath: string; mtimeMs: number }> = [];
  const stack = [getSessionsRootPath(workspaceRoot)];
  while (stack.length > 0) {
    const current = stack.pop();
    if (!current) {
      continue;
    }
    let entries = [];
    try {
      entries = await readdir(current, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
        continue;
      }
      if (!entry.isFile() || !entry.name.toLowerCase().endsWith(".jsonl")) {
        continue;
      }
      try {
        const entryStat = await stat(fullPath);
        out.push({ filePath: fullPath, mtimeMs: entryStat.mtimeMs });
      } catch {
        // ignore removed files
      }
    }
  }
  return out;
}

async function readFirstLine(fileHandle: Awaited<ReturnType<typeof open>>, fileSize: number): Promise<string> {
  const chunkBytes = 16_384;
  const maxScanBytes = 262_144;
  let position = 0;
  let scanned = 0;
  let raw = "";

  while (position < fileSize && scanned < maxScanBytes) {
    const readSize = Math.min(chunkBytes, fileSize - position, maxScanBytes - scanned);
    const buffer = Buffer.alloc(readSize);
    const { bytesRead } = await fileHandle.read(buffer, 0, readSize, position);
    if (bytesRead <= 0) {
      break;
    }
    raw += buffer.toString("utf8", 0, bytesRead);
    scanned += bytesRead;
    position += bytesRead;
    const newlineIndex = raw.search(/\r?\n/);
    if (newlineIndex >= 0) {
      return raw.slice(0, newlineIndex).trim();
    }
  }

  return raw.trim();
}

function extractLastAgentMessage(candidateLines: string[]): { message: string; updatedAt: string | null } | null {
  let fallback: { message: string; updatedAt: string | null } | null = null;
  for (const line of candidateLines) {
    const trimmed = line.trim();
    if (!trimmed) {
      continue;
    }
    try {
      const parsed = JSON.parse(trimmed) as { type?: unknown; timestamp?: unknown; payload?: unknown };
      if (parsed.type !== "event_msg" || !isObjectRecord(parsed.payload)) {
        continue;
      }
      if (parsed.payload.type === "agent_message" && typeof parsed.payload.message === "string" && parsed.payload.message.trim()) {
        return {
          message: parsed.payload.message.trim(),
          updatedAt: toTrimmedStringOrNull(parsed.timestamp),
        };
      }
      if (
        !fallback &&
        parsed.payload.type === "task_complete" &&
        typeof parsed.payload.last_agent_message === "string" &&
        parsed.payload.last_agent_message.trim()
      ) {
        fallback = {
          message: parsed.payload.last_agent_message.trim(),
          updatedAt: toTrimmedStringOrNull(parsed.timestamp),
        };
      }
    } catch {
      // ignore malformed lines
    }
  }
  return fallback;
}

async function readLastAgentMessage(
  fileHandle: Awaited<ReturnType<typeof open>>,
  fileSize: number,
): Promise<{ message: string; updatedAt: string | null } | null> {
  const chunkBytes = 16_384;
  const maxScanBytes = 131_072;
  if (fileSize <= 0) {
    return null;
  }
  let position = fileSize;
  let scanned = 0;
  let carry = "";
  while (position > 0 && scanned < maxScanBytes) {
    const readSize = Math.min(chunkBytes, position, maxScanBytes - scanned);
    position -= readSize;
    scanned += readSize;
    const buffer = Buffer.alloc(readSize);
    const { bytesRead } = await fileHandle.read(buffer, 0, readSize, position);
    if (bytesRead <= 0) {
      break;
    }
    const merged = buffer.toString("utf8", 0, bytesRead) + carry;
    const lines = merged.split(/\r?\n/);
    carry = lines.shift() || "";
    const found = extractLastAgentMessage(lines.reverse());
    if (found) {
      return found;
    }
  }
  return extractLastAgentMessage([carry]);
}

function normalizeSessionMeta(rawMeta: unknown, filePath: string, mtimeMs: number): AgentSessionSummaryRecord {
  const baseName = path.basename(filePath, path.extname(filePath));
  const meta = isObjectRecord(rawMeta) ? rawMeta : {};
  const updatedAtCandidate = pickSessionString(meta.updatedAt, meta.updated_at, meta.timestamp);
  return {
    id: pickSessionString(meta.sessionId, meta.session_id, meta.id) || baseName,
    label: pickSessionString(meta.label, meta.title, meta.name, meta.sessionLabel, meta.session_label) || baseName,
    updatedAt: updatedAtCandidate || new Date(mtimeMs).toISOString(),
    cwd: pickSessionString(meta.cwd, meta.workingDirectory, meta.working_directory),
    source: pickSessionString(meta.source, meta.sessionSource, meta.session_source) || "codex",
    originator: pickSessionString(meta.originator, meta.author, meta.user, meta.username) || "unknown",
    filePath,
  };
}

async function readSessionSummary(filePath: string, mtimeMs: number): Promise<AgentSessionSummaryRecord> {
  let fileHandle: Awaited<ReturnType<typeof open>> | null = null;
  try {
    fileHandle = await open(filePath, "r");
    const entryStat = await fileHandle.stat();
    const firstLine = await readFirstLine(fileHandle, entryStat.size);
    const tailSummary = await readLastAgentMessage(fileHandle, entryStat.size);
    let normalized = normalizeSessionMeta({}, filePath, mtimeMs);

    if (firstLine) {
      try {
        const parsed = JSON.parse(firstLine) as Record<string, unknown>;
        const candidateMeta =
          parsed && parsed.type === "session_meta" && isObjectRecord(parsed.payload)
            ? parsed.payload
            : isObjectRecord(parsed.session_meta)
              ? parsed.session_meta
              : isObjectRecord(parsed.sessionMeta)
                ? parsed.sessionMeta
                : isObjectRecord(parsed.meta)
                  ? parsed.meta
                  : isObjectRecord(parsed.payload)
                    ? parsed.payload
                    : parsed;
        normalized = normalizeSessionMeta(candidateMeta, filePath, mtimeMs);
      } catch {
        normalized = normalizeSessionMeta({}, filePath, mtimeMs);
      }
    }

    return {
      ...normalized,
      label: tailSummary?.message || "(no agent message)",
      updatedAt: tailSummary?.updatedAt || normalized.updatedAt,
    };
  } catch {
    return normalizeSessionMeta({}, filePath, mtimeMs);
  } finally {
    await fileHandle?.close().catch(() => undefined);
  }
}

async function listAgentSessions(workspaceRoot: string): Promise<AgentSessionSummaryRecord[]> {
  const sessionsRoot = getSessionsRootPath(workspaceRoot);
  let sessionsRootStat;
  try {
    sessionsRootStat = await stat(sessionsRoot);
  } catch {
    return [];
  }
  if (!sessionsRootStat.isDirectory()) {
    return [];
  }
  const files = await collectSessionJsonlFiles(workspaceRoot);
  files.sort((a, b) => b.mtimeMs - a.mtimeMs || a.filePath.localeCompare(b.filePath));
  const sessions = await Promise.all(files.slice(0, 10).map((file) => readSessionSummary(file.filePath, file.mtimeMs)));
  return sessions.sort((a, b) => Date.parse(b.updatedAt) - Date.parse(a.updatedAt) || b.filePath.localeCompare(a.filePath));
}

async function readSessionLineIndex(workspaceRoot: string, filePath: string): Promise<SessionLineIndexCacheEntry> {
  const resolvedFile = resolveSessionFilePath(workspaceRoot, filePath);
  const entryStat = await stat(resolvedFile);
  const nextSize = entryStat.size;
  const cached = sessionLineIndexCache.get(resolvedFile) ?? null;

  if (cached && cached.size === nextSize) {
    return cached;
  }

  const fileHandle = await open(resolvedFile, "r");
  try {
    let lineStartOffsets = cached?.lineStartOffsets.slice() ?? [];
    let scanStart = cached?.size ?? 0;
    let endsWithNewline = cached?.endsWithNewline ?? false;

    if (!cached || nextSize < cached.size) {
      lineStartOffsets = nextSize > 0 ? [0] : [];
      scanStart = 0;
      endsWithNewline = false;
    } else if (cached.endsWithNewline && nextSize > cached.size) {
      lineStartOffsets.push(cached.size);
    }

    let position = scanStart;
    const chunkBytes = 65_536;
    while (position < nextSize) {
      const readSize = Math.min(chunkBytes, nextSize - position);
      const buffer = Buffer.alloc(readSize);
      const { bytesRead } = await fileHandle.read(buffer, 0, readSize, position);
      if (bytesRead <= 0) {
        break;
      }
      for (let index = 0; index < bytesRead; index += 1) {
        if (buffer[index] !== 0x0a) {
          continue;
        }
        const nextLineStart = position + index + 1;
        if (nextLineStart < nextSize) {
          lineStartOffsets.push(nextLineStart);
        }
      }
      position += bytesRead;
    }

    if (nextSize > 0) {
      const tail = Buffer.alloc(1);
      const { bytesRead } = await fileHandle.read(tail, 0, 1, nextSize - 1);
      endsWithNewline = bytesRead > 0 && tail[0] === 0x0a;
    } else {
      endsWithNewline = false;
    }

    const nextEntry = {
      size: nextSize,
      lineStartOffsets,
      endsWithNewline,
    };
    sessionLineIndexCache.set(resolvedFile, nextEntry);
    return nextEntry;
  } finally {
    await fileHandle.close().catch(() => undefined);
  }
}

async function getAgentSessionRawRows(args: {
  workspaceRoot: string;
  filePath: string;
  sinceLine: number;
  beforeRowId: number | null;
  pageSize: number;
}): Promise<{
  rawRows: AgentSessionRawRowRecord[];
  nextCursor: number;
}> {
  const resolvedFile = resolveSessionFilePath(args.workspaceRoot, args.filePath);
  const index = await readSessionLineIndex(args.workspaceRoot, resolvedFile);
  const totalLines = index.lineStartOffsets.length;
  const sinceLine = Math.max(0, Math.floor(args.sinceLine));
  const beforeRowId = args.beforeRowId && args.beforeRowId > 0 ? Math.floor(args.beforeRowId) : null;
  const maxRawRows = 200;
  const maxSelectionBytes = 120_000;
  const maxLineSelectionBytes = 4_096;
  const maxReadBytes = 2_000_000;

  if (totalLines === 0) {
    return {
      rawRows: [],
      nextCursor: 0,
    };
  }

  let startLineIndex = 0;
  let endLineIndex = totalLines;

  const getLineSpanBytes = (lineIndex: number): number => {
    const start = index.lineStartOffsets[lineIndex] ?? index.size;
    const end = lineIndex + 1 < totalLines ? (index.lineStartOffsets[lineIndex + 1] ?? index.size) : index.size;
    return Math.max(0, end - start);
  };

  if (beforeRowId !== null) {
    endLineIndex = Math.max(0, Math.min(totalLines, beforeRowId - 1));
    startLineIndex = endLineIndex;
    let collectedRows = 0;
    let collectedSelectionBytes = 0;
    let collectedReadBytes = 0;
    while (startLineIndex > 0 && collectedRows < maxRawRows) {
      const nextIndex = startLineIndex - 1;
      const nextReadBytes = getLineSpanBytes(nextIndex);
      const nextSelectionBytes = Math.min(nextReadBytes, maxLineSelectionBytes);
      if (collectedRows > 0 && collectedSelectionBytes + nextSelectionBytes > maxSelectionBytes) {
        break;
      }
      if (collectedRows > 0 && collectedReadBytes + nextReadBytes > maxReadBytes) {
        break;
      }
      startLineIndex = nextIndex;
      collectedRows += 1;
      collectedSelectionBytes += nextSelectionBytes;
      collectedReadBytes += nextReadBytes;
    }
  } else if (sinceLine > 0) {
    startLineIndex = Math.min(totalLines, sinceLine);
    endLineIndex = startLineIndex;
    let collectedRows = 0;
    let collectedSelectionBytes = 0;
    let collectedReadBytes = 0;
    while (endLineIndex < totalLines && collectedRows < maxRawRows) {
      const nextReadBytes = getLineSpanBytes(endLineIndex);
      const nextSelectionBytes = Math.min(nextReadBytes, maxLineSelectionBytes);
      if (collectedRows > 0 && collectedSelectionBytes + nextSelectionBytes > maxSelectionBytes) {
        break;
      }
      if (collectedRows > 0 && collectedReadBytes + nextReadBytes > maxReadBytes) {
        break;
      }
      endLineIndex += 1;
      collectedRows += 1;
      collectedSelectionBytes += nextSelectionBytes;
      collectedReadBytes += nextReadBytes;
    }
  } else {
    startLineIndex = totalLines;
    let collectedRows = 0;
    let collectedSelectionBytes = 0;
    let collectedReadBytes = 0;
    while (startLineIndex > 0 && collectedRows < maxRawRows) {
      const nextIndex = startLineIndex - 1;
      const nextReadBytes = getLineSpanBytes(nextIndex);
      const nextSelectionBytes = Math.min(nextReadBytes, maxLineSelectionBytes);
      if (collectedRows > 0 && collectedSelectionBytes + nextSelectionBytes > maxSelectionBytes) {
        break;
      }
      if (collectedRows > 0 && collectedReadBytes + nextReadBytes > maxReadBytes) {
        break;
      }
      startLineIndex = nextIndex;
      collectedRows += 1;
      collectedSelectionBytes += nextSelectionBytes;
      collectedReadBytes += nextReadBytes;
    }
  }

  if (startLineIndex >= endLineIndex) {
    return {
      rawRows: [],
      nextCursor: endLineIndex,
    };
  }

  const startOffset = index.lineStartOffsets[startLineIndex] ?? index.size;
  const endOffset = endLineIndex < totalLines ? (index.lineStartOffsets[endLineIndex] ?? index.size) : index.size;
  if (startOffset >= endOffset) {
    return {
      rawRows: [],
      nextCursor: endLineIndex,
    };
  }

  const fileHandle = await open(resolvedFile, "r");
  try {
    const readSize = endOffset - startOffset;
    const buffer = Buffer.alloc(readSize);
    const { bytesRead } = await fileHandle.read(buffer, 0, readSize, startOffset);
    const raw = buffer.toString("utf8", 0, bytesRead);
    const lines = raw.split(/\r?\n/);
    if (lines.length > 0 && lines[lines.length - 1] === "") {
      lines.pop();
    }
    const rawRows: AgentSessionRawRowRecord[] = [];
    let lineNumber = startLineIndex + 1;
    for (const line of lines) {
      if (line.trim()) {
        const sanitized = sanitizeSessionRpcRawLine(line);
        if (!sanitized) {
          lineNumber += 1;
          continue;
        }
        rawRows.push({
          id: lineNumber,
          raw: sanitized,
        });
      }
      lineNumber += 1;
    }
    return {
      rawRows,
      nextCursor: endLineIndex,
    };
  } finally {
    await fileHandle.close().catch(() => undefined);
  }
}

function resolveSessionUploadsDir(workspaceRoot: string, sessionId: string): string {
  const safeSessionId = sessionId.trim().replace(/[^a-zA-Z0-9._-]/g, "_").slice(0, 160) || "session";
  return path.join(workspaceRoot, ".doer-agent", "sessions", safeSessionId);
}

async function deleteAgentSession(workspaceRoot: string, filePath: string, sessionId: string | null): Promise<void> {
  const resolvedFile = resolveSessionFilePath(workspaceRoot, filePath);
  sessionLineIndexCache.delete(resolvedFile);
  await unlink(resolvedFile);
  if (sessionId) {
    await rm(resolveSessionUploadsDir(workspaceRoot, sessionId), { recursive: true, force: true }).catch(() => undefined);
  }
  const sessionsRoot = path.resolve(getSessionsRootPath(workspaceRoot));
  let currentDir = path.dirname(resolvedFile);
  while (currentDir.startsWith(sessionsRoot + path.sep)) {
    try {
      const entries = await readdir(currentDir);
      if (entries.length > 0) {
        break;
      }
      await rmdir(currentDir);
    } catch {
      break;
    }
    currentDir = path.dirname(currentDir);
  }
}

function publishSessionRpcResponse(args: {
  nc: NatsConnection;
  responseSubject: string;
  payload: AgentSessionRpcResponse;
  onError: (message: string) => void;
}): void {
  try {
    args.nc.publish(args.responseSubject, sessionRpcCodec.encode(JSON.stringify(args.payload)));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    args.onError(`session rpc publish failed responseSubject=${args.responseSubject}: ${message}`);
  }
}

export function stopAllSessionWatchers(args: { onError: (message: string) => void }): void {
  const stops = [...activeSessionWatchers.values()];
  for (const stop of stops) {
    try {
      stop();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      args.onError(`session watcher cleanup failed: ${message}`);
    }
  }
}

async function startSessionWatch(args: {
  nc: NatsConnection;
  requestId: string;
  responseSubject: string;
  filePath: string;
  workspaceRoot: string;
  onError: (message: string) => void;
  formatTimestamp: () => string;
}): Promise<string> {
  const resolvedFile = resolveSessionFilePath(args.workspaceRoot, args.filePath);
  const canonicalFile = await realpath(resolvedFile).catch(() => resolvedFile);
  const watchId = `watch_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
  let watcher: FSWatcher | null = null;
  let active = true;
  let eventCount = 0;

  const emitEvent = (event: Record<string, unknown>) => {
    if (!active) {
      return;
    }
    publishSessionRpcResponse({
      nc: args.nc,
      responseSubject: args.responseSubject,
      payload: {
        requestId: args.requestId,
        ok: true,
        action: "watch",
        watchId,
        event,
      },
      onError: args.onError,
    });
  };

  const cleanup = () => {
    if (!active) {
      return;
    }
    active = false;
    activeSessionWatchers.delete(watchId);
    watcher?.close();
    watcher = null;
  };

  const notifyFromContent = () => {
    eventCount += 1;
    emitEvent({
      type: "messages.changed",
      at: args.formatTimestamp(),
    });
  };

  watcher = watch(canonicalFile, { persistent: false }, (eventType) => {
    if (!active) {
      return;
    }
    if (eventType === "change" || eventType === "rename") {
      notifyFromContent();
    }
  });
  watcher.on("error", () => undefined);
  activeSessionWatchers.set(watchId, cleanup);
  emitEvent({ type: "stream.started", watchId, at: args.formatTimestamp() });
  return watchId;
}

async function handleSessionRpcMessage(args: {
  msg: Msg;
  nc: NatsConnection;
  agentId: string;
  workspaceRoot: string;
  onError: (message: string) => void;
  formatTimestamp: () => string;
}): Promise<void> {
  let requestId = "unknown";
  let responseSubject = "";
  try {
    const payload = JSON.parse(sessionRpcCodec.decode(args.msg.data)) as AgentSessionRpcRequest;
    const request = normalizeSessionRpcRequest({ request: payload, agentId: args.agentId });
    requestId = request.requestId;
    responseSubject = request.responseSubject;

    if (request.action === "list") {
      const sessions = await listAgentSessions(args.workspaceRoot);
      publishSessionRpcResponse({
        nc: args.nc,
        responseSubject,
        payload: { requestId, ok: true, action: "list", sessions },
        onError: args.onError,
      });
      return;
    }

    if (request.action === "messages") {
      const result = await getAgentSessionRawRows({
        workspaceRoot: args.workspaceRoot,
        filePath: request.filePath ?? "",
        sinceLine: request.sinceLine,
        beforeRowId: request.beforeRowId,
        pageSize: request.pageSize,
      });
      publishSessionRpcResponse({
        nc: args.nc,
        responseSubject,
        payload: { requestId, ok: true, action: "messages", rawRows: result.rawRows, nextCursor: result.nextCursor },
        onError: args.onError,
      });
      return;
    }

    if (request.action === "delete") {
      await deleteAgentSession(args.workspaceRoot, request.filePath ?? "", request.sessionId);
      publishSessionRpcResponse({
        nc: args.nc,
        responseSubject,
        payload: { requestId, ok: true, action: "delete" },
        onError: args.onError,
      });
      return;
    }

    if (request.action === "watch") {
      const watchId = await startSessionWatch({
        nc: args.nc,
        requestId,
        responseSubject,
        filePath: request.filePath ?? "",
        workspaceRoot: args.workspaceRoot,
        onError: args.onError,
        formatTimestamp: args.formatTimestamp,
      });
      publishSessionRpcResponse({
        nc: args.nc,
        responseSubject,
        payload: { requestId, ok: true, action: "watch", watchId },
        onError: args.onError,
      });
      return;
    }

    const stop = request.watchId ? activeSessionWatchers.get(request.watchId) : null;
    stop?.();
    publishSessionRpcResponse({
      nc: args.nc,
      responseSubject,
      payload: { requestId, ok: true, action: "stop_watch", watchId: request.watchId },
      onError: args.onError,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (responseSubject) {
      publishSessionRpcResponse({
        nc: args.nc,
        responseSubject,
        payload: {
          requestId,
          ok: false,
          error: message,
        },
        onError: args.onError,
      });
    }
    args.onError(`session rpc failed requestId=${requestId} error=${message}`);
  }
}

export function subscribeToSessionRpc(args: {
  nc: NatsConnection;
  subject: string;
  agentId: string;
  workspaceRoot: string;
  onInfo: (message: string) => void;
  onError: (message: string) => void;
  formatTimestamp: () => string;
}): void {
  args.nc.subscribe(args.subject, {
    callback: (error, msg) => {
      if (error) {
        const message = error instanceof Error ? error.message : String(error);
        args.onError(`session rpc subscription error: ${message}`);
        return;
      }
      void handleSessionRpcMessage({
        msg,
        nc: args.nc,
        agentId: args.agentId,
        workspaceRoot: args.workspaceRoot,
        onError: args.onError,
        formatTimestamp: args.formatTimestamp,
      });
    },
  });
  args.onInfo(`session rpc subscribed subject=${args.subject}`);
}

export async function findSessionFilePathBySessionId(workspaceRoot: string, sessionId: string): Promise<string | null> {
  const targetSessionId = sessionId.trim();
  if (!targetSessionId) {
    return null;
  }

  const sessionsRoot = getSessionsRootPath(workspaceRoot);
  let sessionsRootStat;
  try {
    sessionsRootStat = await stat(sessionsRoot);
  } catch {
    return null;
  }
  if (!sessionsRootStat.isDirectory()) {
    return null;
  }

  const files = await collectSessionJsonlFiles(workspaceRoot);
  files.sort((a, b) => b.mtimeMs - a.mtimeMs || a.filePath.localeCompare(b.filePath));
  for (const file of files) {
    let fileHandle: Awaited<ReturnType<typeof open>> | null = null;
    try {
      fileHandle = await open(file.filePath, "r");
      const entryStat = await fileHandle.stat();
      const firstLine = await readFirstLine(fileHandle, entryStat.size);
      if (!firstLine) {
        continue;
      }
      const parsed = JSON.parse(firstLine) as Record<string, unknown>;
      const candidateMeta =
        parsed && parsed.type === "session_meta" && isObjectRecord(parsed.payload)
          ? parsed.payload
          : isObjectRecord(parsed.session_meta)
            ? parsed.session_meta
            : isObjectRecord(parsed.sessionMeta)
              ? parsed.sessionMeta
              : isObjectRecord(parsed.meta)
                ? parsed.meta
                : isObjectRecord(parsed.payload)
                  ? parsed.payload
                  : parsed;
      const candidateId = pickSessionString(
        candidateMeta.sessionId,
        candidateMeta.session_id,
        candidateMeta.id,
      );
      if (candidateId === targetSessionId) {
        return file.filePath;
      }
    } catch {
      // ignore malformed session files
    } finally {
      await fileHandle?.close().catch(() => undefined);
    }
  }
  return null;
}

export async function readSessionIdFromSessionFile(filePath: string): Promise<string | null> {
  let fileHandle: Awaited<ReturnType<typeof open>> | null = null;
  try {
    fileHandle = await open(filePath, "r");
    const entryStat = await fileHandle.stat();
    const firstLine = await readFirstLine(fileHandle, entryStat.size);
    if (!firstLine) {
      return null;
    }
    const parsed = JSON.parse(firstLine) as Record<string, unknown>;
    const candidateMeta =
      parsed && parsed.type === "session_meta" && isObjectRecord(parsed.payload)
        ? parsed.payload
        : isObjectRecord(parsed.session_meta)
          ? parsed.session_meta
          : isObjectRecord(parsed.sessionMeta)
            ? parsed.sessionMeta
            : isObjectRecord(parsed.meta)
              ? parsed.meta
              : isObjectRecord(parsed.payload)
                ? parsed.payload
                : parsed;
    return pickSessionString(candidateMeta.sessionId, candidateMeta.session_id, candidateMeta.id);
  } catch {
    return null;
  } finally {
    await fileHandle?.close().catch(() => undefined);
  }
}

export async function detectPendingRunSession(workspaceRoot: string, knownFilePaths: Set<string>): Promise<{
  sessionId: string;
  sessionFilePath: string;
} | null> {
  const sessionsRoot = getSessionsRootPath(workspaceRoot);
  let sessionsRootStat;
  try {
    sessionsRootStat = await stat(sessionsRoot);
  } catch {
    return null;
  }
  if (!sessionsRootStat.isDirectory()) {
    return null;
  }

  const files = await collectSessionJsonlFiles(workspaceRoot);
  files.sort((a, b) => a.mtimeMs - b.mtimeMs || a.filePath.localeCompare(b.filePath));
  for (const file of files) {
    if (knownFilePaths.has(file.filePath)) {
      continue;
    }
    const sessionId = await readSessionIdFromSessionFile(file.filePath);
    if (!sessionId) {
      continue;
    }
    knownFilePaths.add(file.filePath);
    return {
      sessionId,
      sessionFilePath: file.filePath,
    };
  }
  return null;
}
