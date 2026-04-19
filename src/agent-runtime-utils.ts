import { readFile } from "node:fs/promises";
import path from "node:path";

export function sanitizeUserId(userId: string): string {
  const normalized = userId.trim().replace(/[^a-zA-Z0-9_-]/g, "_");
  return normalized.length > 0 ? normalized : "anonymous";
}

export function buildAgentRunRpcSubject(userId: string, agentId: string): string {
  return `doer.agent.run.rpc.${sanitizeUserId(userId)}.${agentId.trim()}`;
}

export function buildAgentRunEventsSubject(userId: string, agentId: string): string {
  return `doer.agent.run.events.${sanitizeUserId(userId)}.${agentId.trim()}`;
}

export function buildAgentSessionRpcSubject(userId: string, agentId: string): string {
  return `doer.agent.session.rpc.${sanitizeUserId(userId)}.${agentId.trim()}`;
}

export function buildAgentCodexAuthRpcSubject(userId: string, agentId: string): string {
  return `doer.agent.codex.auth.rpc.${sanitizeUserId(userId)}.${agentId.trim()}`;
}

export function buildAgentSettingsRpcSubject(userId: string, agentId: string): string {
  return `doer.agent.settings.rpc.${sanitizeUserId(userId)}.${agentId.trim()}`;
}

export function buildAgentGitRpcSubject(userId: string, agentId: string): string {
  return `doer.agent.git.rpc.${sanitizeUserId(userId)}.${agentId.trim()}`;
}

export function buildAgentSkillRpcSubject(userId: string, agentId: string): string {
  return `doer.agent.skill.rpc.${sanitizeUserId(userId)}.${agentId.trim()}`;
}

export function buildAgentFsRpcSubject(userId: string, agentId: string): string {
  return `doer.agent.fs.rpc.${sanitizeUserId(userId)}.${agentId.trim()}`;
}

export function parseBootstrapTaskConfig(value: unknown): { stream: string; subject: string; durable: string } | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const task = value as Record<string, unknown>;
  const stream = typeof task.stream === "string" ? task.stream.trim() : "";
  const subject = typeof task.subject === "string" ? task.subject.trim() : "";
  const durable = typeof task.durable === "string" ? task.durable.trim() : "";
  if (!stream || !subject || !durable) {
    return null;
  }
  return { stream, subject, durable };
}

export function normalizeTaskIds(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const out: string[] = [];
  for (const item of value) {
    if (typeof item !== "string") {
      continue;
    }
    const id = item.trim();
    if (!id) {
      continue;
    }
    out.push(id);
  }
  return out;
}

export function normalizeEnvPatch(value: unknown): Record<string, string> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  const out: Record<string, string> = {};
  for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
    if (typeof raw !== "string") {
      continue;
    }
    const normalizedKey = key.trim();
    if (!normalizedKey) {
      continue;
    }
    out[normalizedKey] = raw;
  }
  return out;
}

export function normalizeRunImagePaths(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const seen = new Set<string>();
  const out: string[] = [];
  for (const item of value) {
    if (typeof item !== "string") {
      continue;
    }
    const normalized = item.trim();
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    out.push(normalized);
  }
  return out;
}

const PNG_SIGNATURE = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
const crc32Table = (() => {
  const table = new Uint32Array(256);
  for (let index = 0; index < 256; index += 1) {
    let crc = index;
    for (let bit = 0; bit < 8; bit += 1) {
      crc = (crc & 1) !== 0 ? 0xedb88320 ^ (crc >>> 1) : crc >>> 1;
    }
    table[index] = crc >>> 0;
  }
  return table;
})();

function crc32(parts: Buffer[]): number {
  let crc = 0xffffffff;
  for (const part of parts) {
    for (let index = 0; index < part.length; index += 1) {
      crc = crc32Table[(crc ^ part[index]) & 0xff] ^ (crc >>> 8);
    }
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function validatePngBytes(bytes: Buffer): string | null {
  if (bytes.length < PNG_SIGNATURE.length || !bytes.subarray(0, PNG_SIGNATURE.length).equals(PNG_SIGNATURE)) {
    return "missing PNG signature";
  }

  let offset = PNG_SIGNATURE.length;
  let sawIend = false;
  while (offset < bytes.length) {
    if (offset + 12 > bytes.length) {
      return "truncated PNG chunk";
    }
    const chunkLength = bytes.readUInt32BE(offset);
    const typeStart = offset + 4;
    const dataStart = offset + 8;
    const dataEnd = dataStart + chunkLength;
    const crcOffset = dataEnd;
    const nextOffset = crcOffset + 4;
    if (dataEnd > bytes.length || nextOffset > bytes.length) {
      return "truncated PNG chunk payload";
    }
    const type = bytes.subarray(typeStart, dataStart);
    const expectedCrc = bytes.readUInt32BE(crcOffset);
    const actualCrc = crc32([type, bytes.subarray(dataStart, dataEnd)]);
    if (expectedCrc !== actualCrc) {
      const chunkName = type.toString("ascii");
      return `PNG CRC mismatch in ${chunkName} chunk`;
    }
    if (type.equals(Buffer.from("IEND"))) {
      sawIend = true;
      if (nextOffset !== bytes.length) {
        return "unexpected trailing bytes after PNG IEND";
      }
      break;
    }
    offset = nextOffset;
  }

  return sawIend ? null : "missing PNG IEND chunk";
}

export function validateImageBytes(filePath: string, bytes: Buffer): string | null {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === ".png") {
    return validatePngBytes(bytes);
  }
  return null;
}

export async function filterValidRunImagePaths(args: {
  workspaceRoot: string;
  imagePaths: string[];
  onInvalidImage?: (imagePath: string, reason: string) => void;
}): Promise<string[]> {
  const valid: string[] = [];
  for (const imagePath of args.imagePaths) {
    const absPath = path.isAbsolute(imagePath) ? imagePath : path.resolve(args.workspaceRoot, imagePath);
    let bytes: Buffer;
    try {
      bytes = await readFile(absPath);
    } catch (error) {
      const reason = error instanceof Error ? error.message : "failed to read image";
      args.onInvalidImage?.(imagePath, reason);
      continue;
    }
    const validationError = validateImageBytes(absPath, bytes);
    if (validationError) {
      args.onInvalidImage?.(imagePath, validationError);
      continue;
    }
    valid.push(imagePath);
  }
  return valid;
}

export function fatalExit(message: string, error: unknown, writeAgentError: (message: string) => void): never {
  const detail = error instanceof Error ? error.message : typeof error === "string" ? error : error ? String(error) : "";
  const full = detail ? `${message}: ${detail}` : message;
  writeAgentError(`fatal: ${full}`);
  process.exit(1);
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function writeTaskUpload(taskId: string, message: string): void {
  process.stdout.write(`[doer-agent][task=${taskId}][upload] ${message}\n`);
}

export function writeRpcStream(requestId: string, stream: "stdout" | "stderr", chunk: string): void {
  const target = stream === "stdout" ? process.stdout : process.stderr;
  const lines = chunk.replace(/\r/g, "\n").split("\n");
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    if (line.length === 0 && i === lines.length - 1) {
      continue;
    }
    target.write(`[doer-agent][rpc=${requestId}][${stream}] ${line}\n`);
  }
}

export function writeRunStatus(runId: string, message: string): void {
  process.stdout.write(`[doer-agent][run=${runId}][status] ${message}\n`);
}

export function writeRunStream(runId: string, stream: "stdout" | "stderr", chunk: string): void {
  const target = stream === "stdout" ? process.stdout : process.stderr;
  const lines = chunk.split(/\r?\n/);
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    if (!line && index === lines.length - 1) {
      continue;
    }
    target.write(`[doer-agent][run=${runId}][${stream}] ${line}\n`);
  }
}

function resolveLogTimeZone(): string {
  const configured = process.env.DOER_AGENT_LOG_TIMEZONE?.trim() || process.env.TZ?.trim();
  return configured && configured.length > 0 ? configured : "Asia/Seoul";
}

function resolveTimeZoneOffsetString(date: Date, timeZone: string): string {
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone,
      timeZoneName: "shortOffset",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    }).formatToParts(date);
    const token = parts.find((part) => part.type === "timeZoneName")?.value || "GMT+0";
    const matched = token.match(/GMT([+-]\d{1,2})(?::?(\d{2}))?/i);
    if (!matched) {
      return "+00:00";
    }
    const hourRaw = matched[1] || "+0";
    const minuteRaw = matched[2] || "00";
    const sign = hourRaw.startsWith("-") ? "-" : "+";
    const absHour = String(Math.abs(Number.parseInt(hourRaw, 10))).padStart(2, "0");
    const absMinute = String(Math.abs(Number.parseInt(minuteRaw, 10))).padStart(2, "0");
    return `${sign}${absHour}:${absMinute}`;
  } catch {
    return "+00:00";
  }
}

export function formatLocalTimestamp(date = new Date()): string {
  const timeZone = resolveLogTimeZone();
  try {
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    }).formatToParts(date);
    const pick = (type: Intl.DateTimeFormatPartTypes): string => {
      return parts.find((part) => part.type === type)?.value || "00";
    };
    const year = pick("year");
    const month = pick("month");
    const day = pick("day");
    const hours = pick("hour");
    const minutes = pick("minute");
    const seconds = pick("second");
    const ms = String(date.getMilliseconds()).padStart(3, "0");
    const offset = resolveTimeZoneOffsetString(date, timeZone);
    return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}.${ms}${offset}`;
  } catch {
    return date.toISOString();
  }
}

export function parseArgs(argv: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    if (!key.startsWith("--")) {
      continue;
    }
    const value = argv[i + 1];
    if (typeof value === "string" && !value.startsWith("--")) {
      out[key.slice(2)] = value;
      i += 1;
      continue;
    }
    out[key.slice(2)] = "true";
  }
  return out;
}

export function resolveArgOrEnv(
  args: Record<string, string>,
  argKeys: string[],
  envKeys: string[],
  fallback = "",
): string {
  for (const key of argKeys) {
    const value = args[key]?.trim();
    if (value) {
      return value;
    }
  }
  for (const key of envKeys) {
    const value = process.env[key]?.trim();
    if (value) {
      return value;
    }
  }
  return fallback;
}

export function resolveContainerReachableServerBaseUrl(serverBaseUrl: string): string {
  return serverBaseUrl;
}

export async function resolveAgentVersion(packageJsonPath: string): Promise<string> {
  const raw = await readFile(packageJsonPath, "utf8").catch(() => "");
  if (!raw) {
    return "unknown";
  }
  try {
    const parsed = JSON.parse(raw) as { version?: unknown };
    return typeof parsed.version === "string" && parsed.version.trim() ? parsed.version.trim() : "unknown";
  } catch {
    return "unknown";
  }
}
