import crypto from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { StringCodec, type Msg, type NatsConnection } from "nats";
import type { CodexAppServerManager } from "./codex-app-server-manager.js";
import { runCodexTextTask } from "./codex-text-task.js";

const codec = StringCodec();
const MAX_THREADS_PER_CLASSIFICATION = 50;
const MAX_THREADS_PER_REBUILD = 2_000;
const MAX_TAGS = 12;
const MIN_TAGS = 2;
const OTHER_TAG_ID = "other";

export interface ThreadAiTagDefinition {
  id: string;
  label: string;
  description: string;
  color: string;
}

interface ThreadAiTagConfig {
  version: 1;
  tags: ThreadAiTagDefinition[];
  updatedAt: string;
}

interface ResolvedThreadAiTagConfig extends ThreadAiTagConfig {
  source: "default" | "file";
  fingerprint: string;
}

interface ThreadTagInput {
  threadId: string;
  label: string;
  preview: string | null;
  cwd: string | null;
}

interface ThreadTagClassification {
  threadId: string;
  activityTag: string;
  confidence: number;
  source: "ai";
}

interface StoredThreadTags {
  version: 2;
  threadId: string;
  contentFingerprint: string;
  configFingerprint: string;
  activityTag: string;
  confidence: number;
  source: "ai";
  updatedAt: string;
}

interface ThreadTagsRpcRequest {
  requestId?: unknown;
  action?: unknown;
  agentId?: unknown;
  threads?: unknown;
  tags?: unknown;
  force?: unknown;
}

const DEFAULT_TAGS: ThreadAiTagDefinition[] = [
  { id: "deployment", label: "Deployment", description: "Deploy, release, publish, production rollout, or app-store delivery.", color: "#8b5cf6" },
  { id: "bug", label: "Bug · diagnosis", description: "Debugging, errors, failures, regressions, diagnosis, or fixes.", color: "#ef4444" },
  { id: "documentation", label: "Documentation", description: "Documentation, reports, notes, guides, or written content.", color: "#0ea5e9" },
  { id: "research", label: "Research · review", description: "Investigation, audit, comparison, review, analysis, or planning.", color: "#14b8a6" },
  { id: "feature", label: "Feature", description: "Implementation, feature work, UI/UX changes, or integrations.", color: "#22c55e" },
  { id: "operations", label: "Operations", description: "Servers, daemons, infrastructure, databases, or maintenance.", color: "#f59e0b" },
  { id: OTHER_TAG_ID, label: "Other", description: "Threads that do not fit any other configured tag.", color: "#64748b" },
];

let classificationQueue = Promise.resolve();

function stringValue(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function nullableString(value: unknown): string | null {
  return stringValue(value) || null;
}

function recordValue(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function sanitizeThreadId(value: string): string {
  const sanitized = value.trim().replace(/[^a-zA-Z0-9._-]/g, "_").slice(0, 160);
  if (!sanitized || sanitized === "." || sanitized === "..") {
    throw new Error("Invalid thread id");
  }
  return sanitized;
}

function sanitizeTagId(value: string): string {
  return value
    .trim()
    .toLocaleLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 48);
}

function normalizedColor(value: unknown, fallback: string): string {
  const color = stringValue(value).toLocaleLowerCase();
  return /^#[0-9a-f]{6}$/.test(color) ? color : fallback;
}

function tagsFilePath(workspaceRoot: string, threadId: string): string {
  return path.join(workspaceRoot, ".doer-agent", "threads", sanitizeThreadId(threadId), "tags.json");
}

function configFilePath(workspaceRoot: string): string {
  return path.join(workspaceRoot, ".doer-agent", "thread-tags", "config.json");
}

function normalizeInputs(value: unknown, limit: number): ThreadTagInput[] {
  const rows = Array.isArray(value) ? value.slice(0, limit) : [];
  const seen = new Set<string>();
  const inputs: ThreadTagInput[] = [];
  for (const rowValue of rows) {
    const row = recordValue(rowValue);
    const threadId = stringValue(row?.threadId).slice(0, 160);
    if (!threadId || seen.has(threadId)) {
      continue;
    }
    sanitizeThreadId(threadId);
    seen.add(threadId);
    inputs.push({
      threadId,
      label: stringValue(row?.label).slice(0, 500),
      preview: nullableString(row?.preview)?.slice(0, 2_000) ?? null,
      cwd: nullableString(row?.cwd)?.slice(0, 1_000) ?? null,
    });
  }
  return inputs;
}

function normalizeTagDefinitions(value: unknown): ThreadAiTagDefinition[] {
  const rows = Array.isArray(value) ? value.slice(0, MAX_TAGS) : [];
  const seen = new Set<string>();
  const tags: ThreadAiTagDefinition[] = [];
  for (let index = 0; index < rows.length; index += 1) {
    const row = recordValue(rows[index]);
    const id = sanitizeTagId(stringValue(row?.id) || stringValue(row?.label));
    const label = stringValue(row?.label).slice(0, 60);
    const description = stringValue(row?.description).slice(0, 500);
    if (!id || !label || !description || seen.has(id)) {
      continue;
    }
    seen.add(id);
    tags.push({
      id,
      label,
      description,
      color: normalizedColor(row?.color, DEFAULT_TAGS[index % DEFAULT_TAGS.length]?.color ?? "#64748b"),
    });
  }
  if (!seen.has(OTHER_TAG_ID)) {
    if (tags.length >= MAX_TAGS) {
      tags.pop();
    }
    tags.push({ ...DEFAULT_TAGS.find((tag) => tag.id === OTHER_TAG_ID)! });
  }
  if (tags.length < MIN_TAGS) {
    throw new Error(`At least ${MIN_TAGS} AI tags are required`);
  }
  return tags.slice(0, MAX_TAGS);
}

function configFingerprint(tags: ThreadAiTagDefinition[]): string {
  return crypto.createHash("sha256").update(JSON.stringify(tags.map((tag) => ({
    id: tag.id,
    label: tag.label,
    description: tag.description,
    color: tag.color,
  })))).digest("hex");
}

function publicConfig(config: ResolvedThreadAiTagConfig) {
  return {
    version: config.version,
    tags: config.tags,
    updatedAt: config.updatedAt,
    source: config.source,
    fingerprint: config.fingerprint,
  };
}

async function readConfig(workspaceRoot: string): Promise<ResolvedThreadAiTagConfig> {
  try {
    const parsed = recordValue(JSON.parse(await readFile(configFilePath(workspaceRoot), "utf8")));
    const tags = normalizeTagDefinitions(parsed?.tags);
    return {
      version: 1,
      tags,
      updatedAt: stringValue(parsed?.updatedAt) || new Date(0).toISOString(),
      source: "file",
      fingerprint: configFingerprint(tags),
    };
  } catch {
    const tags = DEFAULT_TAGS.map((tag) => ({ ...tag }));
    return {
      version: 1,
      tags,
      updatedAt: new Date(0).toISOString(),
      source: "default",
      fingerprint: configFingerprint(tags),
    };
  }
}

async function writeConfig(workspaceRoot: string, value: unknown): Promise<ResolvedThreadAiTagConfig> {
  const tags = normalizeTagDefinitions(value);
  const filePath = configFilePath(workspaceRoot);
  await mkdir(path.dirname(filePath), { recursive: true });
  const tempPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  const payload: ThreadAiTagConfig = {
    version: 1,
    tags,
    updatedAt: new Date().toISOString(),
  };
  await writeFile(tempPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  await rename(tempPath, filePath);
  return {
    ...payload,
    source: "file",
    fingerprint: configFingerprint(tags),
  };
}

function contentFingerprint(input: ThreadTagInput): string {
  return crypto.createHash("sha256").update(JSON.stringify({
    label: input.label,
    preview: input.preview ?? "",
    cwd: input.cwd ?? "",
  })).digest("hex");
}

function boundedConfidence(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value)
    ? Math.min(1, Math.max(0, value))
    : 0.5;
}

function jsonObjectFromText(text: string): Record<string, unknown> {
  const trimmed = text.trim().replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/, "").trim();
  const start = trimmed.indexOf("{");
  const end = trimmed.lastIndexOf("}");
  if (start < 0 || end < start) {
    throw new Error("Thread tag AI task did not return JSON");
  }
  const payload = recordValue(JSON.parse(trimmed.slice(start, end + 1)));
  if (!payload) {
    throw new Error("Thread tag AI task returned an invalid object");
  }
  return payload;
}

export function parseThreadTagClassificationResponse(
  text: string,
  requestedThreadIds: ReadonlySet<string>,
  allowedTagIds: ReadonlySet<string> = new Set(DEFAULT_TAGS.map((tag) => tag.id)),
): ThreadTagClassification[] {
  const payload = jsonObjectFromText(text);
  const assignments = Array.isArray(payload.assignments) ? payload.assignments : [];
  const seen = new Set<string>();
  const results: ThreadTagClassification[] = [];
  for (const assignmentValue of assignments) {
    const assignment = recordValue(assignmentValue);
    const threadId = stringValue(assignment?.threadId);
    const activityTag = sanitizeTagId(stringValue(assignment?.activityTag));
    if (
      !threadId ||
      seen.has(threadId) ||
      !requestedThreadIds.has(threadId) ||
      !allowedTagIds.has(activityTag)
    ) {
      continue;
    }
    seen.add(threadId);
    results.push({
      threadId,
      activityTag,
      confidence: boundedConfidence(assignment?.confidence),
      source: "ai",
    });
  }
  return results;
}

export function parseSuggestedThreadTags(text: string): ThreadAiTagDefinition[] {
  return normalizeTagDefinitions(jsonObjectFromText(text).tags);
}

async function readStoredTags(
  workspaceRoot: string,
  input: ThreadTagInput,
  config: ResolvedThreadAiTagConfig,
): Promise<ThreadTagClassification | null> {
  try {
    const row = recordValue(JSON.parse(await readFile(tagsFilePath(workspaceRoot, input.threadId), "utf8")));
    const activityTag = sanitizeTagId(stringValue(row?.activityTag));
    const allowedTagIds = new Set(config.tags.map((tag) => tag.id));
    const isLegacyDefaultCache = row?.version === 1 && config.source === "default";
    const isCurrentCache = row?.version === 2 && stringValue(row.configFingerprint) === config.fingerprint;
    if (
      (!isLegacyDefaultCache && !isCurrentCache) ||
      stringValue(row?.threadId) !== input.threadId ||
      stringValue(row?.contentFingerprint) !== contentFingerprint(input) ||
      !allowedTagIds.has(activityTag)
    ) {
      return null;
    }
    return {
      threadId: input.threadId,
      activityTag,
      confidence: boundedConfidence(row?.confidence),
      source: "ai",
    };
  } catch {
    return null;
  }
}

async function writeStoredTags(
  workspaceRoot: string,
  input: ThreadTagInput,
  classification: ThreadTagClassification,
  config: ResolvedThreadAiTagConfig,
): Promise<void> {
  const filePath = tagsFilePath(workspaceRoot, input.threadId);
  await mkdir(path.dirname(filePath), { recursive: true });
  const tempPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  const payload: StoredThreadTags = {
    version: 2,
    threadId: input.threadId,
    contentFingerprint: contentFingerprint(input),
    configFingerprint: config.fingerprint,
    activityTag: classification.activityTag,
    confidence: classification.confidence,
    source: "ai",
    updatedAt: new Date().toISOString(),
  };
  await writeFile(tempPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  await rename(tempPath, filePath);
}

function configuredTagsPrompt(tags: ThreadAiTagDefinition[]): string[] {
  return tags.map((tag) => `- ${tag.id} (${tag.label}): ${tag.description}`);
}

function classifierPrompt(inputs: ThreadTagInput[], tags: ThreadAiTagDefinition[]): string {
  return [
    "You classify Codex threads inside Doer.",
    "Return only one valid JSON object. Do not use Markdown fences or add explanations.",
    "Classify every input thread into exactly one activityTag from the configured list.",
    "Configured activityTag values:",
    ...configuredTagsPrompt(tags),
    "Use label and preview as the main evidence. Use cwd only as supporting project context.",
    `Output shape: {"assignments":[{"threadId":"...","activityTag":"${tags[0]?.id ?? OTHER_TAG_ID}","confidence":0.9}]}`,
    "",
    JSON.stringify({ threads: inputs }),
  ].join("\n");
}

function suggestionPrompt(inputs: ThreadTagInput[], candidateTags?: ThreadAiTagDefinition[]): string {
  return [
    "Design a compact taxonomy for automatically classifying Codex threads inside Doer.",
    "Return only one valid JSON object. Do not use Markdown fences or add explanations.",
    "Create 5 to 9 mutually useful tags based on the supplied threads.",
    "Every tag needs: a stable lowercase kebab-case id, a concise label, a one-sentence classification description, and a distinct #RRGGBB color.",
    "Write labels and descriptions in the predominant language of the supplied threads. Use Korean when the threads are predominantly Korean.",
    `Always include "${OTHER_TAG_ID}" as the final fallback tag.`,
    "Avoid tags for project paths or running status; those are managed separately.",
    "Prefer work-intent categories that will remain useful for future threads.",
    'Output shape: {"tags":[{"id":"feature","label":"Feature","description":"Implementation and product changes.","color":"#22c55e"}]}',
    candidateTags?.length
      ? `Consolidate these batch candidates into one taxonomy:\n${JSON.stringify(candidateTags)}`
      : "",
    inputs.length ? `Threads to inspect:\n${JSON.stringify({
      threads: inputs.map((input) => ({
        threadId: input.threadId,
        label: input.label.slice(0, 300),
        preview: input.preview?.slice(0, 500) ?? null,
      })),
    })}` : "",
  ].filter(Boolean).join("\n");
}

async function classifyThreads(args: {
  workspaceRoot: string;
  manager: CodexAppServerManager;
  inputs: ThreadTagInput[];
  force: boolean;
}): Promise<{
  classifications: ThreadTagClassification[];
  classificationError: string | null;
  config: ReturnType<typeof publicConfig>;
}> {
  const config = await readConfig(args.workspaceRoot);
  const classifications = new Map<string, ThreadTagClassification>();
  const staleInputs: ThreadTagInput[] = [];
  for (const input of args.inputs) {
    const stored = args.force ? null : await readStoredTags(args.workspaceRoot, input, config);
    if (stored) {
      classifications.set(input.threadId, stored);
    } else {
      staleInputs.push(input);
    }
  }

  let classificationError: string | null = null;
  if (staleInputs.length > 0) {
    try {
      const resultText = await runCodexTextTask({
        manager: args.manager,
        prompt: classifierPrompt(staleInputs, config.tags),
      });
      const resolved = parseThreadTagClassificationResponse(
        resultText,
        new Set(staleInputs.map((input) => input.threadId)),
        new Set(config.tags.map((tag) => tag.id)),
      );
      const inputsById = new Map(staleInputs.map((input) => [input.threadId, input]));
      await Promise.all(resolved.map(async (classification) => {
        const input = inputsById.get(classification.threadId);
        if (!input) {
          return;
        }
        await writeStoredTags(args.workspaceRoot, input, classification, config);
        classifications.set(classification.threadId, classification);
      }));
      if (resolved.length !== staleInputs.length) {
        classificationError = `AI classified ${resolved.length} of ${staleInputs.length} threads`;
      }
    } catch (error) {
      classificationError = error instanceof Error ? error.message : String(error);
    }
  }

  return {
    classifications: [...classifications.values()],
    classificationError,
    config: publicConfig(config),
  };
}

async function suggestConfig(args: {
  manager: CodexAppServerManager;
  inputs: ThreadTagInput[];
}): Promise<ThreadAiTagDefinition[]> {
  if (args.inputs.length === 0) {
    throw new Error("No threads available for AI tag reconstruction");
  }
  const candidates: ThreadAiTagDefinition[] = [];
  for (let offset = 0; offset < args.inputs.length; offset += 100) {
    const batch = args.inputs.slice(offset, offset + 100);
    const text = await runCodexTextTask({
      manager: args.manager,
      prompt: suggestionPrompt(batch),
    });
    candidates.push(...parseSuggestedThreadTags(text));
  }
  if (args.inputs.length <= 100) {
    return candidates.slice(0, MAX_TAGS);
  }
  const consolidated = await runCodexTextTask({
    manager: args.manager,
    prompt: suggestionPrompt([], candidates),
  });
  return parseSuggestedThreadTags(consolidated);
}

async function handleThreadTagsRpcMessage(args: {
  msg: Msg;
  agentId: string;
  workspaceRoot: string;
  manager: CodexAppServerManager;
  onError: (message: string) => void;
}): Promise<void> {
  let request: ThreadTagsRpcRequest = {};
  try {
    request = JSON.parse(codec.decode(args.msg.data)) as ThreadTagsRpcRequest;
    if (stringValue(request.agentId) !== args.agentId) {
      throw new Error("Agent id mismatch");
    }
    const action = stringValue(request.action);
    let result: Record<string, unknown>;
    if (action === "get-config") {
      result = { config: publicConfig(await readConfig(args.workspaceRoot)) };
    } else if (action === "save-config") {
      result = { config: publicConfig(await writeConfig(args.workspaceRoot, request.tags)) };
    } else if (action === "suggest-config") {
      result = {
        suggestedTags: await suggestConfig({
          manager: args.manager,
          inputs: normalizeInputs(request.threads, MAX_THREADS_PER_REBUILD),
        }),
      };
    } else if (action === "classify") {
      result = await classifyThreads({
        workspaceRoot: args.workspaceRoot,
        manager: args.manager,
        inputs: normalizeInputs(request.threads, MAX_THREADS_PER_CLASSIFICATION),
        force: request.force === true,
      });
    } else {
      throw new Error("Unsupported thread tags action");
    }
    args.msg.respond(codec.encode(JSON.stringify({
      requestId: request.requestId,
      ok: true,
      ...result,
    })));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    args.msg.respond(codec.encode(JSON.stringify({
      requestId: request.requestId,
      ok: false,
      error: message,
    })));
    args.onError(`thread tags rpc failed error=${message}`);
  }
}

export function subscribeToThreadTagsRpc(args: {
  nc: NatsConnection;
  subject: string;
  agentId: string;
  workspaceRoot: string;
  manager: CodexAppServerManager;
  onInfo: (message: string) => void;
  onError: (message: string) => void;
}): void {
  args.nc.subscribe(args.subject, {
    callback: (error, msg) => {
      if (error) {
        args.onError(`thread tags rpc subscription error=${error.message}`);
        return;
      }
      classificationQueue = classificationQueue.then(
        () => handleThreadTagsRpcMessage({ ...args, msg }),
        () => handleThreadTagsRpcMessage({ ...args, msg }),
      );
    },
  });
  args.onInfo(`thread tags rpc subscribed subject=${args.subject}`);
}
