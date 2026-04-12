import { spawnSync } from "node:child_process";
import { existsSync, statSync } from "node:fs";
import { chmod, mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

function pickFirstNonEmpty(values: Array<string | undefined | null>): string {
  for (const value of values) {
    if (typeof value !== "string") {
      continue;
    }
    const normalized = value.trim();
    if (normalized) {
      return normalized;
    }
  }
  return "";
}

async function ensureGitAskpassScript(agentProjectDir: string): Promise<string> {
  const binDir = path.join(agentProjectDir, "runtime/bin");
  const scriptPath = path.join(binDir, "git-askpass.sh");
  const scriptBody = `#!/bin/sh
case "$1" in
  *Username*) printf "%s\\n" "x-access-token" ;;
  *Password*) printf "%s\\n" "\${GITHUB_TOKEN:-\${GH_TOKEN:-}}" ;;
  *) printf "\\n" ;;
esac
`;
  await mkdir(binDir, { recursive: true });
  await writeFile(scriptPath, scriptBody, "utf8");
  await chmod(scriptPath, 0o700).catch(() => undefined);
  return scriptPath;
}

function applyGitIdentityIfPossible(args: { cwd: string | null; userName: string; userEmail: string }): boolean {
  if (!args.cwd) {
    return false;
  }
  const inRepo = spawnSync("git", ["rev-parse", "--is-inside-work-tree"], {
    cwd: args.cwd,
    stdio: "ignore",
  });
  if (inRepo.status !== 0) {
    return false;
  }
  const setName = spawnSync("git", ["config", "--local", "user.name", args.userName], {
    cwd: args.cwd,
    stdio: "ignore",
  });
  if (setName.status !== 0) {
    return false;
  }
  const setEmail = spawnSync("git", ["config", "--local", "user.email", args.userEmail], {
    cwd: args.cwd,
    stdio: "ignore",
  });
  return setEmail.status === 0;
}

export function createRuntimeEnvHelpers(args: {
  resolveWorkspaceRoot: () => string;
  agentProjectDir: string;
}): {
  resolveCodexHomePath: () => string;
  resolveShellPath: () => string;
  resolveTaskWorkspace: (rawCwd: string | null) => string;
  prepareTaskGitEnv: (args: {
    cwd: string | null;
    baseEnvPatch: Record<string, string>;
  }) => Promise<{ envPatch: Record<string, string>; meta: Record<string, unknown> }>;
} {
  function resolveCodexHomePath(): string {
    return path.join(args.resolveWorkspaceRoot(), ".codex");
  }

  function resolveShellPath(): string {
    if (process.platform === "win32") {
      return process.env.ComSpec || "cmd.exe";
    }
    const candidates = [process.env.SHELL, "/bin/bash", "/usr/bin/bash", "/bin/sh", "/usr/bin/sh"].filter(
      (value): value is string => typeof value === "string" && value.trim().length > 0,
    );
    for (const candidate of candidates) {
      if (existsSync(candidate)) {
        return candidate;
      }
    }
    throw new Error("No shell executable found. Set SHELL env or install /bin/sh (or bash).");
  }

  function resolveTaskWorkspace(rawCwd: string | null): string {
    const workspaceRoot = args.resolveWorkspaceRoot();
    const requestedCwd = rawCwd?.trim() || "";
    const resolvedCwd = requestedCwd
      ? path.isAbsolute(requestedCwd)
        ? path.resolve(requestedCwd)
        : path.resolve(workspaceRoot, requestedCwd)
      : workspaceRoot;

    if (!existsSync(resolvedCwd)) {
      throw new Error(
        `Invalid cwd: ${requestedCwd || "(empty)"} resolved to ${resolvedCwd} (path does not exist)`,
      );
    }

    let stats;
    try {
      stats = statSync(resolvedCwd);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(
        `Invalid cwd: ${requestedCwd || "(empty)"} resolved to ${resolvedCwd} (${message})`,
      );
    }

    if (!stats.isDirectory()) {
      throw new Error(
        `Invalid cwd: ${requestedCwd || "(empty)"} resolved to ${resolvedCwd} (not a directory)`,
      );
    }

    return resolvedCwd;
  }

  async function prepareTaskGitEnv(prepareArgs: {
    cwd: string | null;
    baseEnvPatch: Record<string, string>;
  }): Promise<{ envPatch: Record<string, string>; meta: Record<string, unknown> }> {
    const envPatch: Record<string, string> = {
      GIT_TERMINAL_PROMPT: "0",
      GCM_INTERACTIVE: "Never",
    };

    const githubToken = pickFirstNonEmpty([
      prepareArgs.baseEnvPatch.GITHUB_TOKEN,
      prepareArgs.baseEnvPatch.GH_TOKEN,
      process.env.GITHUB_TOKEN,
      process.env.GH_TOKEN,
    ]);
    if (githubToken) {
      envPatch.GITHUB_TOKEN = githubToken;
      envPatch.GH_TOKEN = githubToken;
      envPatch.GIT_ASKPASS_REQUIRE = "force";
      envPatch.GIT_ASKPASS = await ensureGitAskpassScript(args.agentProjectDir);
    }

    const userName = pickFirstNonEmpty([
      prepareArgs.baseEnvPatch.DOER_GIT_USER_NAME,
      prepareArgs.baseEnvPatch.GIT_USER_NAME,
      prepareArgs.baseEnvPatch.GIT_AUTHOR_NAME,
      prepareArgs.baseEnvPatch.GIT_COMMITTER_NAME,
    ]);
    const userEmail = pickFirstNonEmpty([
      prepareArgs.baseEnvPatch.DOER_GIT_USER_EMAIL,
      prepareArgs.baseEnvPatch.GIT_USER_EMAIL,
      prepareArgs.baseEnvPatch.GIT_AUTHOR_EMAIL,
      prepareArgs.baseEnvPatch.GIT_COMMITTER_EMAIL,
    ]);

    const gitIdentityApplied =
      userName && userEmail
        ? applyGitIdentityIfPossible({
            cwd: prepareArgs.cwd,
            userName,
            userEmail,
          })
        : false;

    return {
      envPatch,
      meta: {
        gitAskpassEnabled: Boolean(envPatch.GIT_ASKPASS),
        gitIdentityApplied,
        gitIdentityProvided: Boolean(userName && userEmail),
      },
    };
  }

  return {
    resolveCodexHomePath,
    resolveShellPath,
    resolveTaskWorkspace,
    prepareTaskGitEnv,
  };
}
