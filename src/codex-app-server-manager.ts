import {
  buildAgentSettingsEnvPatch,
  readAgentModelInstructions,
  resolveAgentModelInstructionsFilePath,
  type AgentSettingsConfig,
} from "./agent-settings.js";
import { buildCustomMcpConfigArgs, buildDaemonMcpConfigArgs, buildMobileMcpConfigArgs, buildThreadsMcpConfigArgs } from "./agent-codex-cli.js";
import { CodexAppServerClient } from "./codex-app-server-client.js";

function toTomlStringLiteral(value: string): string {
  return `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

function buildConfigArg(key: string, tomlValue: string): string[] {
  return ["--config", `${key}=${tomlValue}`];
}

function buildFeatureArg(enabled: boolean, name: string): string[] {
  return [enabled ? "--enable" : "--disable", name];
}

async function buildCodexAppServerArgs(args: {
  agentId: string;
  agentToken: string;
  workspaceRoot: string;
  agentProjectDir: string;
  serverBaseUrl: string;
  settings: AgentSettingsConfig;
  userId: string;
}): Promise<string[]> {
  const configArgs = [
    ...buildConfigArg("model", toTomlStringLiteral(args.settings.codex.model)),
    ...buildConfigArg("model_reasoning_effort", toTomlStringLiteral(args.settings.codex.reasoningEffort)),
    ...(args.settings.codex.serviceTier
      ? buildConfigArg("service_tier", toTomlStringLiteral(args.settings.codex.serviceTier))
      : []),
    ...buildConfigArg("personality", toTomlStringLiteral(args.settings.general.personality)),
    ...buildConfigArg("approval_policy", toTomlStringLiteral("never")),
    ...buildConfigArg("sandbox_mode", toTomlStringLiteral("danger-full-access")),
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
  let createPromise: Promise<CodexAppServerClient> | null = null;
  let generation = 0;
  const notificationListeners = new Set<(method: string, params: unknown) => void>();

  const createClient = async (): Promise<CodexAppServerClient> => {
    const settings = await args.readAgentSettingsConfig({ workspaceRoot: args.workspaceRoot });
    const appServerArgs = await buildCodexAppServerArgs({
      agentId: args.agentId,
      agentToken: args.agentToken,
      workspaceRoot: args.workspaceRoot,
      agentProjectDir: args.agentProjectDir,
      serverBaseUrl: args.serverBaseUrl,
      settings,
      userId: args.userId,
    });
    const env = await buildCodexAppServerEnv({
      agentId: args.agentId,
      agentToken: args.agentToken,
      workspaceRoot: args.workspaceRoot,
      serverBaseUrl: args.serverBaseUrl,
      resolveCodexHomePath: args.resolveCodexHomePath,
      settings,
      userId: args.userId,
    });
    args.onLog?.(
      `starting codex app-server model=${settings.codex.model} reasoningEffort=${settings.codex.reasoningEffort} personality=${settings.general.personality} computerUse=${settings.codex.computerUseEnabled} browserUse=${settings.codex.browserUseEnabled} mcpServers=${settings.mcp.servers.filter((server) => server.enabled).length}`,
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
    async restart(reason) {
      generation += 1;
      const activeClient = client;
      client = null;
      createPromise = null;
      if (!activeClient) {
        args.onLog?.(`codex app-server restart requested before start reason=${reason}`);
        return;
      }
      args.onLog?.(`restarting codex app-server reason=${reason}`);
      await activeClient.stop();
    },
    async stop() {
      const activeClient = client;
      client = null;
      createPromise = null;
      await activeClient?.stop();
    },
  };
}
