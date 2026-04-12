import path from "node:path";
import { mkdir } from "node:fs/promises";

export interface PreparedCommandExecution {
  shellPath: string;
  taskWorkspace: string;
  taskPath: string;
  env: NodeJS.ProcessEnv;
  taskGitMeta: Record<string, unknown>;
  codexAuthMeta: Record<string, unknown>;
  codexAuthCleanup: () => Promise<void>;
}

export async function prepareCommandExecution<TCodexAuthBundle, TSettings>(args: {
  cwd: string | null;
  userId: string;
  taskId: string;
  codexAuthBundle: TCodexAuthBundle | null;
  runtimeEnvPatch: Record<string, string>;
  agentProjectDir: string;
  resolveShellPath: () => string;
  resolveTaskWorkspace: (cwd: string | null) => string;
  resolveCodexHomePath: () => string;
  prepareCodexAuthBundle: (bundle: TCodexAuthBundle | null) => Promise<{
    envPatch?: Record<string, string>;
    meta?: Record<string, unknown>;
    cleanup?: () => Promise<void>;
  } | null>;
  readAgentSettingsConfig: (args: { workspaceRoot: string }) => Promise<TSettings>;
  resolveWorkspaceRoot: () => string;
  buildAgentSettingsEnvPatch: (settings: TSettings) => Record<string, string>;
  prepareTaskGitEnv: (args: {
    cwd: string;
    baseEnvPatch: Record<string, string>;
  }) => Promise<{ envPatch: Record<string, string>; meta?: Record<string, unknown> | null }>;
}): Promise<PreparedCommandExecution> {
  const shellPath = args.resolveShellPath();
  const taskWorkspace = args.resolveTaskWorkspace(args.cwd);
  const codexHome = args.resolveCodexHomePath();
  await mkdir(codexHome, { recursive: true });
  const codexAuth = await args.prepareCodexAuthBundle(args.codexAuthBundle);
  const localAgentSettings = await args.readAgentSettingsConfig({ workspaceRoot: args.resolveWorkspaceRoot() });
  const baseTaskEnvPatch = {
    CODEX_HOME: codexHome,
    DOER_USER_ID: args.userId,
    DOER_AGENT_TASK_ID: args.taskId,
    ...args.buildAgentSettingsEnvPatch(localAgentSettings),
    ...args.runtimeEnvPatch,
    ...(codexAuth?.envPatch ?? {}),
    WORKSPACE: taskWorkspace,
  };
  const taskGitEnv = await args.prepareTaskGitEnv({
    cwd: taskWorkspace,
    baseEnvPatch: baseTaskEnvPatch,
  });
  const runtimeBinPath = path.join(args.agentProjectDir, "runtime/bin");
  const taskPath = [runtimeBinPath, process.env.PATH || ""].filter(Boolean).join(path.delimiter);
  return {
    shellPath,
    taskWorkspace,
    taskPath,
    env: {
      ...process.env,
      ...baseTaskEnvPatch,
      ...taskGitEnv.envPatch,
      PATH: taskPath,
    },
    taskGitMeta: taskGitEnv.meta ?? {},
    codexAuthMeta: codexAuth?.meta ?? { codexAuthSynced: false },
    codexAuthCleanup: codexAuth?.cleanup ?? (async () => {}),
  };
}
