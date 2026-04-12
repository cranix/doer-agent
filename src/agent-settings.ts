import path from "node:path";
import { mkdir, readFile, unlink, writeFile } from "node:fs/promises";

export type CodexPersonality = "friendly" | "pragmatic";

export interface AgentSettingsConfig {
  general: {
    personality: CodexPersonality;
  };
  codex: {
    model: string;
    authMode: "api_key" | "oauth";
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
  aws: {
    enabled: boolean;
    accessKeyId: string | null;
    defaultRegion: string | null;
    secretAccessKey: string | null;
    sessionToken: string | null;
  };
  jira: {
    baseUrl: string | null;
    email: string | null;
    enabled: boolean;
    apiToken: string | null;
  };
  notion: {
    baseUrl: string | null;
    version: string | null;
    enabled: boolean;
    apiToken: string | null;
  };
  slack: {
    baseUrl: string | null;
    enabled: boolean;
    botToken: string | null;
  };
  figma: {
    baseUrl: string | null;
    enabled: boolean;
    apiToken: string | null;
  };
}

export interface AgentSettingsPublic {
  general: {
    personality: CodexPersonality;
    customInstructions: string | null;
  };
  codex: {
    model: string;
    authMode: "api_key" | "oauth";
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
  aws: {
    enabled: boolean;
    accessKeyId: string | null;
    defaultRegion: string | null;
    hasSecretAccessKey: boolean;
    secretAccessKeyMasked: string | null;
    secretAccessKeyLength: number | null;
    hasSessionToken: boolean;
    sessionTokenMasked: string | null;
    sessionTokenLength: number | null;
  };
  jira: {
    baseUrl: string | null;
    email: string | null;
    enabled: boolean;
    hasApiToken: boolean;
    apiTokenMasked: string | null;
    apiTokenLength: number | null;
  };
  notion: {
    baseUrl: string | null;
    version: string | null;
    enabled: boolean;
    hasApiToken: boolean;
    apiTokenMasked: string | null;
    apiTokenLength: number | null;
  };
  slack: {
    baseUrl: string | null;
    enabled: boolean;
    hasBotToken: boolean;
    botTokenMasked: string | null;
    botTokenLength: number | null;
  };
  figma: {
    baseUrl: string | null;
    enabled: boolean;
    hasApiToken: boolean;
    apiTokenMasked: string | null;
    apiTokenLength: number | null;
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
      model: "gpt-5.4",
      authMode: "api_key",
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
    aws: {
      enabled: true,
      accessKeyId: null,
      defaultRegion: null,
      secretAccessKey: null,
      sessionToken: null,
    },
    jira: {
      baseUrl: null,
      email: null,
      enabled: false,
      apiToken: null,
    },
    notion: {
      baseUrl: "https://api.notion.com",
      version: "2022-06-28",
      enabled: false,
      apiToken: null,
    },
    slack: {
      baseUrl: "https://slack.com/api",
      enabled: false,
      botToken: null,
    },
    figma: {
      baseUrl: "https://api.figma.com",
      enabled: false,
      apiToken: null,
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
  const aws = raw.aws && typeof raw.aws === "object" ? (raw.aws as Record<string, unknown>) : {};
  const jira = raw.jira && typeof raw.jira === "object" ? (raw.jira as Record<string, unknown>) : {};
  const notion = raw.notion && typeof raw.notion === "object" ? (raw.notion as Record<string, unknown>) : {};
  const slack = raw.slack && typeof raw.slack === "object" ? (raw.slack as Record<string, unknown>) : {};
  const figma = raw.figma && typeof raw.figma === "object" ? (raw.figma as Record<string, unknown>) : {};
  return {
    general: {
      personality: normalizeCodexPersonality(general.personality, base.general.personality),
    },
    codex: {
      model: typeof codex.model === "string" && codex.model.trim() ? codex.model.trim() : base.codex.model,
      authMode: codex.authMode === "oauth" ? "oauth" : codex.authMode === "api_key" ? "api_key" : base.codex.authMode,
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
    aws: {
      enabled: typeof aws.enabled === "boolean" ? aws.enabled : base.aws.enabled,
      accessKeyId: aws.accessKeyId === null ? null : normalizeNullableString(aws.accessKeyId) ?? base.aws.accessKeyId,
      defaultRegion: aws.defaultRegion === null ? null : normalizeNullableString(aws.defaultRegion) ?? base.aws.defaultRegion,
      secretAccessKey:
        aws.secretAccessKey === null
          ? null
          : normalizeNullableString(aws.secretAccessKey) ?? base.aws.secretAccessKey,
      sessionToken: aws.sessionToken === null ? null : normalizeNullableString(aws.sessionToken) ?? base.aws.sessionToken,
    },
    jira: {
      baseUrl: jira.baseUrl === null ? null : normalizeNullableString(jira.baseUrl) ?? base.jira.baseUrl,
      email: jira.email === null ? null : normalizeNullableString(jira.email) ?? base.jira.email,
      enabled: typeof jira.enabled === "boolean" ? jira.enabled : base.jira.enabled,
      apiToken: jira.apiToken === null ? null : normalizeNullableString(jira.apiToken) ?? base.jira.apiToken,
    },
    notion: {
      baseUrl: notion.baseUrl === null ? null : normalizeNullableString(notion.baseUrl) ?? base.notion.baseUrl,
      version: notion.version === null ? null : normalizeNullableString(notion.version) ?? base.notion.version,
      enabled: typeof notion.enabled === "boolean" ? notion.enabled : base.notion.enabled,
      apiToken: notion.apiToken === null ? null : normalizeNullableString(notion.apiToken) ?? base.notion.apiToken,
    },
    slack: {
      baseUrl: slack.baseUrl === null ? null : normalizeNullableString(slack.baseUrl) ?? base.slack.baseUrl,
      enabled: typeof slack.enabled === "boolean" ? slack.enabled : base.slack.enabled,
      botToken: slack.botToken === null ? null : normalizeNullableString(slack.botToken) ?? base.slack.botToken,
    },
    figma: {
      baseUrl: figma.baseUrl === null ? null : normalizeNullableString(figma.baseUrl) ?? base.figma.baseUrl,
      enabled: typeof figma.enabled === "boolean" ? figma.enabled : base.figma.enabled,
      apiToken: figma.apiToken === null ? null : normalizeNullableString(figma.apiToken) ?? base.figma.apiToken,
    },
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
  const awsSecret = toMaskedSecret(args.config.aws.secretAccessKey);
  const awsSession = toMaskedSecret(args.config.aws.sessionToken);
  const jiraToken = toMaskedSecret(args.config.jira.apiToken);
  const notionToken = toMaskedSecret(args.config.notion.apiToken);
  const slackToken = toMaskedSecret(args.config.slack.botToken);
  const figmaToken = toMaskedSecret(args.config.figma.apiToken);
  const customInstructions = await readAgentModelInstructions(args.workspaceRoot);
  return {
    general: {
      personality: args.config.general.personality,
      customInstructions,
    },
    codex: {
      model: args.config.codex.model,
      authMode: args.config.codex.authMode,
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
    aws: {
      enabled: args.config.aws.enabled,
      accessKeyId: args.config.aws.accessKeyId,
      defaultRegion: args.config.aws.defaultRegion,
      hasSecretAccessKey: awsSecret.has,
      secretAccessKeyMasked: awsSecret.masked,
      secretAccessKeyLength: awsSecret.length,
      hasSessionToken: awsSession.has,
      sessionTokenMasked: awsSession.masked,
      sessionTokenLength: awsSession.length,
    },
    jira: {
      baseUrl: args.config.jira.baseUrl,
      email: args.config.jira.email,
      enabled: args.config.jira.enabled,
      hasApiToken: jiraToken.has,
      apiTokenMasked: jiraToken.masked,
      apiTokenLength: jiraToken.length,
    },
    notion: {
      baseUrl: args.config.notion.baseUrl,
      version: args.config.notion.version,
      enabled: args.config.notion.enabled,
      hasApiToken: notionToken.has,
      apiTokenMasked: notionToken.masked,
      apiTokenLength: notionToken.length,
    },
    slack: {
      baseUrl: args.config.slack.baseUrl,
      enabled: args.config.slack.enabled,
      hasBotToken: slackToken.has,
      botTokenMasked: slackToken.masked,
      botTokenLength: slackToken.length,
    },
    figma: {
      baseUrl: args.config.figma.baseUrl,
      enabled: args.config.figma.enabled,
      hasApiToken: figmaToken.has,
      apiTokenMasked: figmaToken.masked,
      apiTokenLength: figmaToken.length,
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

  move("awsEnabled", "aws", "enabled");
  move("awsAccessKeyId", "aws", "accessKeyId");
  move("awsDefaultRegion", "aws", "defaultRegion");
  move("awsSecretAccessKey", "aws", "secretAccessKey");
  move("awsSessionToken", "aws", "sessionToken");

  move("jiraBaseUrl", "jira", "baseUrl");
  move("jiraEmail", "jira", "email");
  move("jiraEnabled", "jira", "enabled");
  move("jiraApiToken", "jira", "apiToken");

  move("notionBaseUrl", "notion", "baseUrl");
  move("notionVersion", "notion", "version");
  move("notionEnabled", "notion", "enabled");
  move("notionApiToken", "notion", "apiToken");

  move("slackBaseUrl", "slack", "baseUrl");
  move("slackEnabled", "slack", "enabled");
  move("slackBotToken", "slack", "botToken");

  move("figmaBaseUrl", "figma", "baseUrl");
  move("figmaEnabled", "figma", "enabled");
  move("figmaApiToken", "figma", "apiToken");

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
  if (config.aws.enabled) {
    if (config.aws.accessKeyId) envPatch.AWS_ACCESS_KEY_ID = config.aws.accessKeyId;
    if (config.aws.defaultRegion) envPatch.AWS_DEFAULT_REGION = config.aws.defaultRegion;
    if (config.aws.defaultRegion) envPatch.AWS_REGION = config.aws.defaultRegion;
    if (config.aws.secretAccessKey) envPatch.AWS_SECRET_ACCESS_KEY = config.aws.secretAccessKey;
    if (config.aws.sessionToken) envPatch.AWS_SESSION_TOKEN = config.aws.sessionToken;
  }
  if (config.jira.enabled) {
    if (config.jira.baseUrl) envPatch.JIRA_BASE_URL = config.jira.baseUrl;
    if (config.jira.email) envPatch.JIRA_EMAIL = config.jira.email;
    if (config.jira.apiToken) envPatch.JIRA_API_TOKEN = config.jira.apiToken;
  }
  if (config.notion.enabled) {
    if (config.notion.baseUrl) envPatch.NOTION_BASE_URL = config.notion.baseUrl;
    if (config.notion.version) envPatch.NOTION_VERSION = config.notion.version;
    if (config.notion.apiToken) envPatch.NOTION_API_TOKEN = config.notion.apiToken;
  }
  if (config.slack.enabled) {
    if (config.slack.baseUrl) envPatch.SLACK_BASE_URL = config.slack.baseUrl;
    if (config.slack.botToken) envPatch.SLACK_BOT_TOKEN = config.slack.botToken;
  }
  if (config.figma.enabled) {
    if (config.figma.baseUrl) envPatch.FIGMA_BASE_URL = config.figma.baseUrl;
    if (config.figma.apiToken) envPatch.FIGMA_API_TOKEN = config.figma.apiToken;
  }
  return envPatch;
}
