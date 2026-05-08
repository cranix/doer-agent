import path from "node:path";
import { mkdir, readFile, unlink, writeFile } from "node:fs/promises";

export type CodexPersonality = "friendly" | "pragmatic";

export interface AgentEnvironmentVariableConfig {
  key: string;
  value: string;
}

export interface AgentSettingsConfig {
  general: {
    personality: CodexPersonality;
  };
  codex: {
    model: string;
    authMode: "api_key" | "chatgpt";
    computerUseEnabled: boolean;
    browserUseEnabled: boolean;
  };
  realtime: {
    model: string;
    voice: string;
    wakeName: string | null;
    requireWakeName: boolean;
    apiKey: string | null;
  };
  git: {
    enabled: boolean;
    name: string | null;
    email: string | null;
    authMode: "none" | "oauth_app";
    oauthToken: string | null;
    oauthLogin: string | null;
    oauthScope: string | null;
  };
  env: {
    variables: AgentEnvironmentVariableConfig[];
  };
}

export interface AgentSettingsPublic {
  general: {
    personality: CodexPersonality;
    customInstructions: string | null;
  };
  codex: {
    model: string;
    authMode: "api_key" | "chatgpt";
    computerUseEnabled: boolean;
    browserUseEnabled: boolean;
    hasApiKey: boolean;
    apiKeyMasked: string | null;
    apiKeyLength: number | null;
  };
  realtime: {
    model: string;
    voice: string;
    wakeName: string | null;
    requireWakeName: boolean;
    hasApiKey: boolean;
    apiKeyMasked: string | null;
    apiKeyLength: number | null;
  };
  git: {
    enabled: boolean;
    name: string | null;
    email: string | null;
    authMode: "none" | "oauth_app";
    hasOauthToken: boolean;
    oauthTokenMasked: string | null;
    oauthTokenLength: number | null;
    oauthLogin: string | null;
    oauthScope: string | null;
  };
  env: {
    variables: AgentEnvironmentVariableConfig[];
  };
}

function resolveAgentSettingsDir(workspaceRoot: string): string {
  return path.join(workspaceRoot, ".doer-agent");
}

export function resolveAgentSettingsFilePath(workspaceRoot: string): string {
  return path.join(resolveAgentSettingsDir(workspaceRoot), "config.json");
}

export function resolveAgentModelInstructionsFilePath(workspaceRoot: string): string {
  return path.join(resolveAgentSettingsDir(workspaceRoot), "model-instructions.md");
}

export function createDefaultAgentSettingsConfig(): AgentSettingsConfig {
  return {
    general: {
      personality: "pragmatic",
    },
    codex: {
      model: "gpt-5.5",
      authMode: "api_key",
      computerUseEnabled: false,
      browserUseEnabled: false,
    },
    realtime: {
      model: process.env.OPENAI_REALTIME_MODEL?.trim() || "gpt-realtime",
      voice: process.env.OPENAI_REALTIME_VOICE?.trim() || "alloy",
      wakeName: null,
      requireWakeName: true,
      apiKey: null,
    },
    git: {
      enabled: true,
      name: null,
      email: null,
      authMode: "none",
      oauthToken: null,
      oauthLogin: null,
      oauthScope: null,
    },
    env: {
      variables: [],
    },
  };
}

function normalizeNullableString(value: unknown): string | null {
  if (value === null) {
    return null;
  }
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function normalizeCodexPersonality(value: unknown, fallback: CodexPersonality): CodexPersonality {
  return value === "friendly" || value === "pragmatic" ? value : fallback;
}

function normalizeEnvVarName(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed || !/^[A-Z_][A-Z0-9_]*$/.test(trimmed)) {
    return null;
  }
  return trimmed;
}

function normalizeAgentEnvironmentVariable(value: unknown): AgentEnvironmentVariableConfig | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const raw = value as Record<string, unknown>;
  const key = normalizeEnvVarName(raw.key);
  if (!key || typeof raw.value !== "string") {
    return null;
  }
  return {
    key,
    value: raw.value.replace(/\r/g, ""),
  };
}

function normalizeAgentEnvironmentSettings(
  value: unknown,
  fallback: AgentSettingsConfig["env"],
): AgentSettingsConfig["env"] {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return fallback;
  }
  const raw = value as Record<string, unknown>;
  const variablesRaw = Array.isArray(raw.variables) ? raw.variables : [];
  const variables: AgentEnvironmentVariableConfig[] = [];
  const seenKeys = new Set<string>();
  for (const item of variablesRaw) {
    const normalized = normalizeAgentEnvironmentVariable(item);
    if (!normalized || seenKeys.has(normalized.key)) {
      continue;
    }
    seenKeys.add(normalized.key);
    variables.push(normalized);
  }
  return { variables };
}

export function normalizeAgentSettingsConfig(
  value: unknown,
  fallback?: AgentSettingsConfig | null,
): AgentSettingsConfig {
  const base = fallback ?? createDefaultAgentSettingsConfig();
  const raw = value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
  const general = raw.general && typeof raw.general === "object" ? (raw.general as Record<string, unknown>) : {};
  const codex = raw.codex && typeof raw.codex === "object" ? (raw.codex as Record<string, unknown>) : {};
  const realtime = raw.realtime && typeof raw.realtime === "object" ? (raw.realtime as Record<string, unknown>) : {};
  const git = raw.git && typeof raw.git === "object" ? (raw.git as Record<string, unknown>) : {};
  const env = raw.env && typeof raw.env === "object" ? raw.env : null;
  return {
    general: {
      personality: normalizeCodexPersonality(general.personality, base.general.personality),
    },
    codex: {
      model: typeof codex.model === "string" && codex.model.trim() ? codex.model.trim() : base.codex.model,
      authMode: codex.authMode === "chatgpt" ? "chatgpt" : codex.authMode === "api_key" ? "api_key" : base.codex.authMode,
      computerUseEnabled:
        typeof codex.computerUseEnabled === "boolean" ? codex.computerUseEnabled : base.codex.computerUseEnabled,
      browserUseEnabled:
        typeof codex.browserUseEnabled === "boolean" ? codex.browserUseEnabled : base.codex.browserUseEnabled,
    },
    realtime: {
      model: typeof realtime.model === "string" && realtime.model.trim() ? realtime.model.trim() : base.realtime.model,
      voice: typeof realtime.voice === "string" && realtime.voice.trim() ? realtime.voice.trim() : base.realtime.voice,
      wakeName: realtime.wakeName === null ? null : normalizeNullableString(realtime.wakeName) ?? base.realtime.wakeName,
      requireWakeName: typeof realtime.requireWakeName === "boolean" ? realtime.requireWakeName : base.realtime.requireWakeName,
      apiKey: realtime.apiKey === null ? null : normalizeNullableString(realtime.apiKey) ?? base.realtime.apiKey,
    },
    git: {
      enabled: typeof git.enabled === "boolean" ? git.enabled : base.git.enabled,
      name: git.name === null ? null : normalizeNullableString(git.name) ?? base.git.name,
      email: git.email === null ? null : normalizeNullableString(git.email) ?? base.git.email,
      authMode: git.authMode === "oauth_app" ? "oauth_app" : git.authMode === "none" ? "none" : base.git.authMode,
      oauthToken: git.oauthToken === null ? null : normalizeNullableString(git.oauthToken) ?? base.git.oauthToken,
      oauthLogin: git.oauthLogin === null ? null : normalizeNullableString(git.oauthLogin) ?? base.git.oauthLogin,
      oauthScope: git.oauthScope === null ? null : normalizeNullableString(git.oauthScope) ?? base.git.oauthScope,
    },
    env: normalizeAgentEnvironmentSettings(env, base.env),
  };
}

export async function readAgentSettingsConfig(args: {
  workspaceRoot: string;
  defaults?: AgentSettingsConfig | null;
}): Promise<AgentSettingsConfig> {
  const fallback = normalizeAgentSettingsConfig(args.defaults ?? null);
  const filePath = resolveAgentSettingsFilePath(args.workspaceRoot);
  const raw = await readFile(filePath, "utf8").catch(() => "");
  if (!raw.trim()) {
    return fallback;
  }
  try {
    return normalizeAgentSettingsConfig(JSON.parse(raw), fallback);
  } catch {
    return fallback;
  }
}

export async function writeAgentSettingsConfig(args: {
  workspaceRoot: string;
  config: AgentSettingsConfig;
}): Promise<void> {
  const dir = resolveAgentSettingsDir(args.workspaceRoot);
  await mkdir(dir, { recursive: true });
  await writeFile(resolveAgentSettingsFilePath(args.workspaceRoot), `${JSON.stringify(args.config, null, 2)}\n`, "utf8");
}

export async function readAgentModelInstructions(workspaceRoot: string): Promise<string | null> {
  const raw = await readFile(resolveAgentModelInstructionsFilePath(workspaceRoot), "utf8").catch(() => "");
  return raw.trim() ? raw : null;
}

export async function writeAgentModelInstructions(args: {
  workspaceRoot: string;
  value: string | null;
}): Promise<void> {
  const filePath = resolveAgentModelInstructionsFilePath(args.workspaceRoot);
  const nextValue = typeof args.value === "string" ? args.value.trim() : "";
  if (!nextValue) {
    await unlink(filePath).catch(() => undefined);
    return;
  }
  await mkdir(resolveAgentSettingsDir(args.workspaceRoot), { recursive: true });
  await writeFile(filePath, args.value ?? "", "utf8");
}

function maskSecretPreview(secret: string): string {
  if (secret.length <= 6) {
    return `${secret.slice(0, 1)}***${secret.slice(-1)}`;
  }
  return `${secret.slice(0, 4)}...${secret.slice(-4)}`;
}

function toMaskedSecret(value: string | null): { has: boolean; masked: string | null; length: number | null } {
  if (!value) {
    return { has: false, masked: null, length: null };
  }
  return { has: true, masked: maskSecretPreview(value), length: value.length };
}

export async function toAgentSettingsPublic(args: {
  workspaceRoot: string;
  config: AgentSettingsConfig;
}): Promise<AgentSettingsPublic> {
  const realtimeKey = toMaskedSecret(args.config.realtime.apiKey);
  const gitOauth = toMaskedSecret(args.config.git.oauthToken);
  const customInstructions = await readAgentModelInstructions(args.workspaceRoot);
  return {
    general: {
      personality: args.config.general.personality,
      customInstructions,
    },
    codex: {
      model: args.config.codex.model,
      authMode: args.config.codex.authMode,
      computerUseEnabled: args.config.codex.computerUseEnabled,
      browserUseEnabled: args.config.codex.browserUseEnabled,
      hasApiKey: false,
      apiKeyMasked: null,
      apiKeyLength: null,
    },
    realtime: {
      model: args.config.realtime.model,
      voice: args.config.realtime.voice,
      wakeName: args.config.realtime.wakeName,
      requireWakeName: args.config.realtime.requireWakeName,
      hasApiKey: realtimeKey.has,
      apiKeyMasked: realtimeKey.masked,
      apiKeyLength: realtimeKey.length,
    },
    git: {
      enabled: args.config.git.enabled,
      name: args.config.git.name,
      email: args.config.git.email,
      authMode: args.config.git.authMode,
      hasOauthToken: gitOauth.has,
      oauthTokenMasked: gitOauth.masked,
      oauthTokenLength: gitOauth.length,
      oauthLogin: args.config.git.oauthLogin,
      oauthScope: args.config.git.oauthScope,
    },
    env: {
      variables: args.config.env.variables.map((variable) => ({
        key: variable.key,
        value: variable.value,
      })),
    },
  };
}

export function normalizeAgentSettingsPatch(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  const raw = value as Record<string, unknown>;
  const patch: Record<string, unknown> = { ...raw };

  const assignNested = (section: string, key: string, nextValue: unknown) => {
    const current =
      patch[section] && typeof patch[section] === "object" && !Array.isArray(patch[section])
        ? { ...(patch[section] as Record<string, unknown>) }
        : {};
    current[key] = nextValue;
    patch[section] = current;
  };

  const move = (flatKey: string, section: string, key: string) => {
    if (!(flatKey in raw)) {
      return;
    }
    assignNested(section, key, raw[flatKey]);
    delete patch[flatKey];
  };

  move("personality", "general", "personality");

  move("codexModel", "codex", "model");
  move("codexAuthMode", "codex", "authMode");
  move("computerUseEnabled", "codex", "computerUseEnabled");
  move("browserUseEnabled", "codex", "browserUseEnabled");

  move("realtimeModel", "realtime", "model");
  move("realtimeVoice", "realtime", "voice");
  move("realtimeWakeName", "realtime", "wakeName");
  move("realtimeRequireWakeName", "realtime", "requireWakeName");
  move("realtimeApiKey", "realtime", "apiKey");

  move("gitEnabled", "git", "enabled");
  move("gitName", "git", "name");
  move("gitEmail", "git", "email");
  move("gitAuthMode", "git", "authMode");
  move("gitOauthToken", "git", "oauthToken");
  move("gitOauthLogin", "git", "oauthLogin");
  move("gitOauthScope", "git", "oauthScope");
  move("environmentVariables", "env", "variables");

  return patch;
}

export function buildAgentSettingsEnvPatch(config: AgentSettingsConfig): Record<string, string> {
  const envPatch: Record<string, string> = {};
  if (config.git.enabled) {
    if (config.git.name) envPatch.GIT_AUTHOR_NAME = config.git.name;
    if (config.git.name) envPatch.GIT_COMMITTER_NAME = config.git.name;
    if (config.git.email) envPatch.GIT_AUTHOR_EMAIL = config.git.email;
    if (config.git.email) envPatch.GIT_COMMITTER_EMAIL = config.git.email;
    if (config.git.oauthToken) envPatch.GITHUB_TOKEN = config.git.oauthToken;
    if (config.git.oauthToken) envPatch.GH_TOKEN = config.git.oauthToken;
    if (config.git.oauthLogin) envPatch.DOER_GIT_OAUTH_LOGIN = config.git.oauthLogin;
    if (config.git.oauthScope) envPatch.DOER_GIT_OAUTH_SCOPE = config.git.oauthScope;
  }
  for (const variable of config.env.variables) {
    envPatch[variable.key] = variable.value;
  }
  return envPatch;
}
