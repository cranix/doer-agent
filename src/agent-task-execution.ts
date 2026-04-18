import type { ChildProcess } from "node:child_process";

export function sendSignalToTaskProcess(child: ChildProcess, signal: NodeJS.Signals): void {
  if (process.platform !== "win32" && typeof child.pid === "number") {
    try {
      process.kill(-child.pid, signal);
      return;
    } catch {
      // Fall back to direct child signaling.
    }
  }
  try {
    child.kill(signal);
  } catch {
    // noop
  }
}

export function sendSignalToPid(pid: number, signal: NodeJS.Signals): void {
  if (process.platform !== "win32") {
    try {
      process.kill(-pid, signal);
      return;
    } catch {
      // Fall back to direct pid signaling.
    }
  }
  process.kill(pid, signal);
}

async function syncCodexAuthState(args: {
  source: "server_bundle";
  authMode?: "api_key" | "oauth";
  apiKey?: string | null;
  authJson?: string | null;
  issuedAt?: string | null;
  expiresAt?: string | null;
}): Promise<{
  envPatch: Record<string, string>;
  cleanup: () => Promise<void>;
  meta: Record<string, unknown>;
}> {
  const envPatch: Record<string, string> = {};

  if (args.authMode === "api_key" && args.apiKey) {
    envPatch.OPENAI_API_KEY = args.apiKey;
  }

  return {
    envPatch,
    cleanup: async () => {},
    meta: {
      codexAuthSource: args.source,
      codexAuthMode: args.authMode ?? null,
      codexAuthHasApiKey: Boolean(args.apiKey),
      codexAuthHasAuthJson: Boolean(args.authJson),
      codexAuthIssuedAt: args.issuedAt ?? null,
      codexAuthExpiresAt: args.expiresAt ?? null,
      codexAuthSynced: false,
    },
  };
}

export async function prepareCodexAuthBundle(bundle: {
  authMode?: "api_key" | "oauth";
  apiKey?: string | null;
  authJson?: string | null;
  issuedAt?: string | null;
  expiresAt?: string | null;
} | null): Promise<{
  envPatch: Record<string, string>;
  cleanup: () => Promise<void>;
  meta: Record<string, unknown>;
} | null> {
  if (!bundle) {
    return null;
  }
  return await syncCodexAuthState({
    source: "server_bundle",
    authMode: bundle.authMode,
    apiKey: bundle.apiKey,
    authJson: bundle.authJson ?? null,
    issuedAt: bundle.issuedAt ?? null,
    expiresAt: bundle.expiresAt ?? null,
  });
}
