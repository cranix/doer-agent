#!/usr/bin/env node
import { existsSync, realpathSync } from "node:fs";
import { delimiter, join } from "node:path";
import { spawn } from "node:child_process";

function isWindows(): boolean {
  return process.platform === "win32";
}

function candidatesFor(baseDir: string): string[] {
  if (isWindows()) {
    return [join(baseDir, "codex.cmd"), join(baseDir, "codex.exe"), join(baseDir, "codex")];
  }
  return [join(baseDir, "codex")];
}

function resolveSelf(): string | null {
  const selfPath = process.argv[1];
  if (!selfPath) return null;
  try {
    return realpathSync(selfPath);
  } catch {
    return selfPath;
  }
}

function findCodexBinary(): string | null {
  const pathValue = process.env.PATH ?? "";
  const dirs = pathValue.split(delimiter).filter(Boolean);
  const self = resolveSelf();

  for (const dir of dirs) {
    for (const candidate of candidatesFor(dir)) {
      if (!existsSync(candidate)) continue;
      try {
        const resolved = realpathSync(candidate);
        if (self && resolved === self) continue;
      } catch {
        // no-op
      }
      return candidate;
    }
  }

  return null;
}

const binary = findCodexBinary();
if (!binary) {
  console.error("Unable to find a real 'codex' binary in PATH.");
  console.error("Install Codex CLI first, or set PATH so another codex binary is discoverable.");
  process.exit(1);
}

const child = spawn(binary, process.argv.slice(2), {
  stdio: "inherit",
  env: process.env,
});

child.on("error", (error) => {
  console.error(`Failed to execute codex: ${error.message}`);
  process.exit(1);
});

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 1);
});
