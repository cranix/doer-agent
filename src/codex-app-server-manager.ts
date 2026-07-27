import {
  buildAgentSettingsEnvPatch,
  readAgentModelInstructions,
  resolveAgentModelInstructionsFilePath,
  type AgentSettingsConfig,
} from "./agent-settings.js";
import {
  buildCustomMcpConfigArgs,
  buildDaemonMcpConfigArgs,
  buildMobileMcpConfigArgs,
  buildThreadsMcpConfigArgs,
  spawnManagedCodexCommand,
} from "./agent-codex-cli.js";
import { CodexAppServerClient } from "./codex-app-server-client.js";
import { startCodexChatBridge } from "./codex-chat-bridge.js";
import {
  allocateMcpOauthCallbackPort,
  buildMcpOauthCallbackBaseUrl,
  relayMcpOauthCallbackToLocalListener,
} from "./mcp-oauth-relay.js";

function toTomlStringLiteral(value: string): string {
  return `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

function buildConfigArg(key: string, tomlValue: string): string[] {
  return ["--config", `${key}=${tomlValue}`];
}

function buildFeatureArg(enabled: boolean, name: string): string[] {
  return [enabled ? "--enable" : "--disable", name];
}

function resolveCodexModel(settings: AgentSettingsConfig): string {
  const providerKey = settings.codex.modelProvider || "openai";
  if (providerKey === "zai") {
    return settings.codex.providerModels.zai || "glm-5.2";
  }
  if (providerKey === "anthropic") {
    return settings.codex.providerModels.anthropic || "claude-sonnet-4-6";
  }
  return settings.codex.providerModels[providerKey] || settings.codex.providerModels.openai || "gpt-5.5";
}

function resolveCodexModelContextWindow(settings: AgentSettingsConfig): number | null {
  if (settings.codex.modelProvider === "zai") {
    return 258400;
  }
  if (settings.codex.modelProvider === "anthropic") {
    const model = resolveCodexModel(settings);
    return model.startsWith("claude-haiku-") ? 200000 : 1000000;
  }
  return null;
}

function buildModelProviderConfigArgs(settings: AgentSettingsConfig): string[] {
  const provider = settings.codex.customProvider;
  const providerId = settings.codex.modelProvider;
  if (!provider || !providerId || provider.id !== providerId) {
    return [];
  }
  return [
    ...buildConfigArg("model_provider", toTomlStringLiteral(provider.id)),
    ...buildConfigArg(`model_providers.${provider.id}.name`, toTomlStringLiteral(provider.name)),
    ...buildConfigArg(`model_providers.${provider.id}.base_url`, toTomlStringLiteral(provider.baseUrl)),
    ...buildConfigArg(`model_providers.${provider.id}.env_key`, toTomlStringLiteral(provider.envKey)),
    ...buildConfigArg(`model_providers.${provider.id}.wire_api`, toTomlStringLiteral("responses")),
  ];
}

async function buildCodexAppServerArgs(args: {
  agentId: string;
  agentToken: string;
  workspaceRoot: string;
  agentProjectDir: string;
  serverBaseUrl: string;
  mcpOauthCallbackPort: number;
  mcpOauthCallbackUrl: string;
  settings: AgentSettingsConfig;
  userId: string;
}): Promise<string[]> {
  const codexModel = resolveCodexModel(args.settings);
  const modelContextWindow = resolveCodexModelContextWindow(args.settings);
  const configArgs = [
    ...buildConfigArg("model", toTomlStringLiteral(codexModel)),
    ...(modelContextWindow ? buildConfigArg("model_context_window", String(modelContextWindow)) : []),
    ...buildModelProviderConfigArgs(args.settings),
    ...buildConfigArg("model_reasoning_effort", toTomlStringLiteral(args.settings.codex.reasoningEffort)),
    ...(args.settings.codex.serviceTier
      ? buildConfigArg("service_tier", toTomlStringLiteral(args.settings.codex.serviceTier))
      : []),
    ...buildConfigArg("personality", toTomlStringLiteral(args.settings.general.personality)),
    ...buildConfigArg("approval_policy", toTomlStringLiteral("never")),
    ...buildConfigArg("sandbox_mode", toTomlStringLiteral("danger-full-access")),
    ...buildConfigArg("mcp_oauth_callback_port", String(args.mcpOauthCallbackPort)),
    ...buildConfigArg("mcp_oauth_callback_url", toTomlStringLiteral(args.mcpOauthCallbackUrl)),
  ];
  const customInstructions = await readAgentModelInstructions(args.workspaceRoot);
  if (customInstructions) {
    configArgs.push(
      ...buildConfigArg(
        "model_instructions_file",
        toTomlStringLiteral(resolveAgentModelInstructionsFilePath(args.workspaceRoot)),
      ),
    );
  }
  return [
    "app-server",
    ...configArgs,
    ...buildDaemonMcpConfigArgs({
      agentProjectDir: args.agentProjectDir,
      workspaceRoot: args.workspaceRoot,
    }),
    ...buildThreadsMcpConfigArgs({
      agentId: args.agentId,
      agentProjectDir: args.agentProjectDir,
      agentToken: args.agentToken,
      serverBaseUrl: args.serverBaseUrl,
      userId: args.userId,
      workspaceRoot: args.workspaceRoot,
    }),
    ...buildMobileMcpConfigArgs({
      agentId: args.agentId,
      agentProjectDir: args.agentProjectDir,
      agentToken: args.agentToken,
      serverBaseUrl: args.serverBaseUrl,
      userId: args.userId,
      workspaceRoot: args.workspaceRoot,
    }),
    ...buildCustomMcpConfigArgs(args.settings.mcp.servers),
    ...buildFeatureArg(true, "goals"),
    ...buildFeatureArg(args.settings.codex.computerUseEnabled, "computer_use"),
    ...buildFeatureArg(args.settings.codex.browserUseEnabled, "browser_use"),
    "--listen",
    "stdio://",
  ];
}

type ProviderProxy = {
  stop: () => Promise<void>;
};

async function resolveAppServerSettings(args: {
  settings: AgentSettingsConfig;
  onLog?: (message: string) => void;
}): Promise<{ settings: AgentSettingsConfig; proxy: ProviderProxy | null }> {
  const provider = args.settings.codex.customProvider;
  const providerId = args.settings.codex.modelProvider;
  const gatewayProviderIds = new Set(["zai", "anthropic"]);
  if (!providerId || provider?.id !== providerId || !gatewayProviderIds.has(providerId)) {
    return { settings: args.settings, proxy: null };
  }
  const proxy = await startCodexChatBridge({
    providerApiKey: provider.apiKey ?? "",
    providerId: provider.id,
    providerName: provider.name,
    targetBaseUrl: provider.baseUrl,
    onLog: args.onLog,
  });
  return {
    proxy,
    settings: {
      ...args.settings,
      codex: {
        ...args.settings.codex,
        customProvider: {
          ...provider,
          baseUrl: proxy.baseUrl,
          envKey: proxy.envKey,
          apiKey: proxy.apiKey,
        },
      },
    },
  };
}

async function buildCodexAppServerEnv(args: {
  agentId: string;
  agentToken: string;
  workspaceRoot: string;
  serverBaseUrl: string;
  resolveCodexHomePath: () => string;
  settings: AgentSettingsConfig;
  userId: string;
}): Promise<NodeJS.ProcessEnv> {
  return {
    ...process.env,
    ...buildAgentSettingsEnvPatch(args.settings),
    CODEX_HOME: args.resolveCodexHomePath(),
    DOER_AGENT_TOKEN: args.agentToken,
    DOER_MOBILE_AGENT_ID: args.agentId,
    DOER_MOBILE_SERVER_BASE_URL: args.serverBaseUrl,
    DOER_MOBILE_USER_ID: args.userId,
  };
}

export interface CodexAppServerManager {
  onNotification(listener: (method: string, params: unknown) => void): () => void;
  request(method: string, params: unknown, timeoutMs?: number): Promise<unknown>;
  relayMcpOauthCallback(path: string, search: string): Promise<{
    status: number;
    headers: Record<string, string>;
    body: string;
  }>;
  logoutMcpServer(name: string): Promise<void>;
  restart(reason: string): Promise<void>;
  stop(): Promise<void>;
}

export function createCodexAppServerManager(args: {
  agentId: string;
  agentToken: string;
  workspaceRoot: string;
  agentProjectDir: string;
  serverBaseUrl: string;
  resolveCodexHomePath: () => string;
  readAgentSettingsConfig: (args: { workspaceRoot: string }) => Promise<AgentSettingsConfig>;
  userId: string;
  onLog?: (message: string) => void;
  onNotification?: (method: string, params: unknown) => void;
}): CodexAppServerManager {
  let client: CodexAppServerClient | null = null;
  let providerProxy: ProviderProxy | null = null;
  let createPromise: Promise<CodexAppServerClient> | null = null;
  let mcpOauthCallbackPortPromise: Promise<number> | null = null;
  let generation = 0;
  const notificationListeners = new Set<(method: string, params: unknown) => void>();
  const mcpOauthCallbackUrl = buildMcpOauthCallbackBaseUrl({
    serverBaseUrl: args.serverBaseUrl,
    userId: args.userId,
    agentId: args.agentId,
  });
  const mcpOauthCallbackPathPrefix = new URL(mcpOauthCallbackUrl).pathname;
  const resolveMcpOauthCallbackPort = async () => {
    if (!mcpOauthCallbackPortPromise) {
      mcpOauthCallbackPortPromise = allocateMcpOauthCallbackPort();
    }
    return await mcpOauthCallbackPortPromise;
  };

  const createClient = async (): Promise<CodexAppServerClient> => {
    const settings = await args.readAgentSettingsConfig({ workspaceRoot: args.workspaceRoot });
    const resolved = await resolveAppServerSettings({ settings, onLog: args.onLog });
    providerProxy = resolved.proxy;
    const mcpOauthCallbackPort = await resolveMcpOauthCallbackPort();
    const appServerArgs = await buildCodexAppServerArgs({
      agentId: args.agentId,
      agentToken: args.agentToken,
      workspaceRoot: args.workspaceRoot,
      agentProjectDir: args.agentProjectDir,
      serverBaseUrl: args.serverBaseUrl,
      mcpOauthCallbackPort,
      mcpOauthCallbackUrl,
      settings: resolved.settings,
      userId: args.userId,
    });
    const env = await buildCodexAppServerEnv({
      agentId: args.agentId,
      agentToken: args.agentToken,
      workspaceRoot: args.workspaceRoot,
      serverBaseUrl: args.serverBaseUrl,
      resolveCodexHomePath: args.resolveCodexHomePath,
      settings: resolved.settings,
      userId: args.userId,
    });
    args.onLog?.(
      `starting codex app-server model=${resolveCodexModel(resolved.settings)} reasoningEffort=${resolved.settings.codex.reasoningEffort} personality=${resolved.settings.general.personality} computerUse=${resolved.settings.codex.computerUseEnabled} browserUse=${resolved.settings.codex.browserUseEnabled} mcpServers=${resolved.settings.mcp.servers.filter((server) => server.enabled).length} mcpOauthCallbackPort=${mcpOauthCallbackPort}`,
    );
    return new CodexAppServerClient({
      cwd: args.workspaceRoot,
      args: appServerArgs,
      env,
      onLog: args.onLog,
      onNotification: (method, params) => {
        for (const listener of notificationListeners) {
          listener(method, params);
        }
        args.onNotification?.(method, params);
      },
    });
  };

  const getClient = async (): Promise<CodexAppServerClient> => {
    if (client) {
      return client;
    }
    if (!createPromise) {
      createPromise = createClient();
    }
    const activeCreatePromise = createPromise;
    const requestedGeneration = generation;
    try {
      const createdClient = await activeCreatePromise;
      if (requestedGeneration !== generation) {
        await createdClient.stop();
        return await getClient();
      }
      client = createdClient;
      return client;
    } finally {
      if (createPromise === activeCreatePromise) {
        createPromise = null;
      }
    }
  };

  const restartClient = async (reason: string): Promise<void> => {
    generation += 1;
    const activeClient = client;
    const activeProxy = providerProxy;
    client = null;
    providerProxy = null;
    createPromise = null;
    if (!activeClient) {
      args.onLog?.(`codex app-server restart requested before start reason=${reason}`);
      return;
    }
    args.onLog?.(`restarting codex app-server reason=${reason}`);
    await activeClient.stop();
    await activeProxy?.stop().catch((error) => {
      args.onLog?.(`failed to stop provider proxy: ${error instanceof Error ? error.message : String(error)}`);
    });
  };

  const runMcpLogout = async (name: string): Promise<void> => {
    const settings = await args.readAgentSettingsConfig({ workspaceRoot: args.workspaceRoot });
    const server = settings.mcp.servers.find((candidate) => candidate.name === name && candidate.transport === "streamable_http");
    if (!server) {
      throw new Error("Remote MCP server not found");
    }
    const env = await buildCodexAppServerEnv({
      agentId: args.agentId,
      agentToken: args.agentToken,
      workspaceRoot: args.workspaceRoot,
      serverBaseUrl: args.serverBaseUrl,
      resolveCodexHomePath: args.resolveCodexHomePath,
      settings,
      userId: args.userId,
    });
    const child = spawnManagedCodexCommand({
      codexArgs: ["mcp", "logout", name, ...buildCustomMcpConfigArgs([server])],
      taskWorkspace: args.workspaceRoot,
      env,
      agentToken: args.agentToken,
    });
    let stdout = "";
    let stderr = "";
    child.stdout?.on("data", (chunk: string) => {
      stdout = `${stdout}${chunk}`.slice(-16_384);
    });
    child.stderr?.on("data", (chunk: string) => {
      stderr = `${stderr}${chunk}`.slice(-16_384);
    });
    const exitCode = await new Promise<number | null>((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", resolve);
    });
    if (exitCode !== 0) {
      throw new Error(stderr.trim() || stdout.trim() || `MCP OAuth logout failed with exit code ${exitCode ?? "unknown"}`);
    }
    await restartClient(`mcp oauth logout ${name}`);
  };

  return {
    onNotification(listener) {
      notificationListeners.add(listener);
      return () => {
        notificationListeners.delete(listener);
      };
    },
    async request(method, params, timeoutMs) {
      const activeClient = await getClient();
      return await activeClient.request(method, params, timeoutMs);
    },
    async relayMcpOauthCallback(path, search) {
      await getClient();
      return await relayMcpOauthCallbackToLocalListener({
        callbackPort: await resolveMcpOauthCallbackPort(),
        callbackPathPrefix: mcpOauthCallbackPathPrefix,
        path,
        search,
      });
    },
    async logoutMcpServer(name) {
      const normalizedName = name.trim();
      if (!/^[A-Za-z0-9_-]+$/.test(normalizedName)) {
        throw new Error("Invalid MCP server name");
      }
      await runMcpLogout(normalizedName);
    },
    async restart(reason) {
      await restartClient(reason);
    },
    async stop() {
      const activeClient = client;
      const activeProxy = providerProxy;
      client = null;
      providerProxy = null;
      createPromise = null;
      await activeClient?.stop();
      await activeProxy?.stop().catch((error) => {
        args.onLog?.(`failed to stop provider proxy: ${error instanceof Error ? error.message : String(error)}`);
      });
    },
  };
}
