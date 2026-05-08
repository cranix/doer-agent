import {
  buildAgentSettingsEnvPatch,
  readAgentModelInstructions,
  resolveAgentModelInstructionsFilePath,
  type AgentSettingsConfig,
} from "./agent-settings.js";
import { buildDaemonMcpConfigArgs } from "./agent-codex-cli.js";
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
  workspaceRoot: string;
  agentProjectDir: string;
  settings: AgentSettingsConfig;
}): Promise<string[]> {
  const configArgs = [
    ...buildConfigArg("model", toTomlStringLiteral(args.settings.codex.model)),
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
    ...buildFeatureArg(true, "goals"),
    ...buildFeatureArg(args.settings.codex.computerUseEnabled, "computer_use"),
    ...buildFeatureArg(args.settings.codex.browserUseEnabled, "browser_use"),
    "--listen",
    "stdio://",
  ];
}

async function buildCodexAppServerEnv(args: {
  workspaceRoot: string;
  resolveCodexHomePath: () => string;
  settings: AgentSettingsConfig;
}): Promise<NodeJS.ProcessEnv> {
  return {
    ...process.env,
    ...buildAgentSettingsEnvPatch(args.settings),
    CODEX_HOME: args.resolveCodexHomePath(),
  };
}

export interface CodexAppServerManager {
  request(method: string, params: unknown): Promise<unknown>;
  restart(reason: string): Promise<void>;
  stop(): Promise<void>;
}

export function createCodexAppServerManager(args: {
  workspaceRoot: string;
  agentProjectDir: string;
  resolveCodexHomePath: () => string;
  readAgentSettingsConfig: (args: { workspaceRoot: string }) => Promise<AgentSettingsConfig>;
  onLog?: (message: string) => void;
  onNotification?: (method: string, params: unknown) => void;
}): CodexAppServerManager {
  let client: CodexAppServerClient | null = null;
  let createPromise: Promise<CodexAppServerClient> | null = null;
  let generation = 0;

  const createClient = async (): Promise<CodexAppServerClient> => {
    const settings = await args.readAgentSettingsConfig({ workspaceRoot: args.workspaceRoot });
    const appServerArgs = await buildCodexAppServerArgs({
      workspaceRoot: args.workspaceRoot,
      agentProjectDir: args.agentProjectDir,
      settings,
    });
    const env = await buildCodexAppServerEnv({
      workspaceRoot: args.workspaceRoot,
      resolveCodexHomePath: args.resolveCodexHomePath,
      settings,
    });
    args.onLog?.(
      `starting codex app-server model=${settings.codex.model} personality=${settings.general.personality} computerUse=${settings.codex.computerUseEnabled} browserUse=${settings.codex.browserUseEnabled}`,
    );
    return new CodexAppServerClient({
      cwd: args.workspaceRoot,
      args: appServerArgs,
      env,
      onLog: args.onLog,
      onNotification: args.onNotification,
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
    async request(method, params) {
      const activeClient = await getClient();
      return await activeClient.request(method, params);
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
