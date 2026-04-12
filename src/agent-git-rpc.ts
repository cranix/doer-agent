import { spawn } from "node:child_process";
import { StringCodec, type Msg, type NatsConnection } from "nats";

const gitRpcCodec = StringCodec();

type AgentGitDiffFormat = "patch" | "name-only" | "name-status" | "stat" | "numstat" | "raw";
type AgentGitDiffIgnoreWhitespace = "none" | "at-eol" | "change" | "all";
type AgentGitDiffAlgorithm = "default" | "minimal" | "patience" | "histogram";

export interface AgentGitRpcRequest {
  requestId?: unknown;
  responseSubject?: unknown;
  agentId?: unknown;
  targetPath?: unknown;
  base?: unknown;
  target?: unknown;
  mergeBase?: unknown;
  staged?: unknown;
  format?: unknown;
  contextLines?: unknown;
  ignoreWhitespace?: unknown;
  diffAlgorithm?: unknown;
  findRenames?: unknown;
  pathspecs?: unknown;
}

interface AgentGitRpcResponse {
  requestId: string;
  ok: boolean;
  payload?: Record<string, unknown> | null;
  error?: string;
}

interface NormalizedGitRpcRequest {
  requestId: string;
  responseSubject: string;
  targetPath: string;
  base: string | null;
  target: string | null;
  mergeBase: boolean;
  staged: boolean;
  format: AgentGitDiffFormat;
  contextLines: number | null;
  ignoreWhitespace: AgentGitDiffIgnoreWhitespace;
  diffAlgorithm: AgentGitDiffAlgorithm;
  findRenames: boolean;
  pathspecs: string[];
}

function runLocalCommand(command: string, args: string[], cwd: string): Promise<{ code: number; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout!.setEncoding("utf8");
    child.stderr!.setEncoding("utf8");
    child.stdout!.on("data", (chunk: string) => {
      stdout += chunk;
    });
    child.stderr!.on("data", (chunk: string) => {
      stderr += chunk;
    });
    child.once("error", reject);
    child.once("close", (code) => {
      resolve({ code: code ?? 1, stdout, stderr });
    });
  });
}

function sanitizeGitRef(value: unknown): string | null {
  if (typeof value !== "string" || !value.trim()) {
    return null;
  }
  const trimmed = value.trim();
  if (trimmed.startsWith("-") || /\s/.test(trimmed) || trimmed.includes("..") || trimmed.includes(":")) {
    throw new Error(`Invalid git ref: ${trimmed}`);
  }
  return trimmed;
}

function sanitizeGitPathspec(value: unknown): string {
  if (typeof value !== "string") {
    throw new Error("Invalid pathspec");
  }
  const trimmed = value.trim().replace(/\\/g, "/");
  if (!trimmed || trimmed.startsWith("-") || trimmed.includes("\0")) {
    throw new Error(`Invalid pathspec: ${trimmed}`);
  }
  return trimmed;
}

function normalizeGitRpcRequest(args: {
  request: AgentGitRpcRequest;
  agentId: string;
}): NormalizedGitRpcRequest {
  const requestId = typeof args.request.requestId === "string" ? args.request.requestId.trim() : "";
  const responseSubject = typeof args.request.responseSubject === "string" ? args.request.responseSubject.trim() : "";
  const requestAgentId = typeof args.request.agentId === "string" ? args.request.agentId.trim() : "";
  const targetPath = typeof args.request.targetPath === "string" ? args.request.targetPath.trim() : "";
  if (!requestId || !responseSubject || !requestAgentId || requestAgentId !== args.agentId || !targetPath) {
    throw new Error("invalid git rpc request");
  }
  const format =
    args.request.format === "name-only" ||
    args.request.format === "name-status" ||
    args.request.format === "stat" ||
    args.request.format === "numstat" ||
    args.request.format === "raw"
      ? args.request.format
      : "patch";
  const ignoreWhitespace =
    args.request.ignoreWhitespace === "at-eol" ||
    args.request.ignoreWhitespace === "change" ||
    args.request.ignoreWhitespace === "all"
      ? args.request.ignoreWhitespace
      : "none";
  const diffAlgorithm =
    args.request.diffAlgorithm === "minimal" ||
    args.request.diffAlgorithm === "patience" ||
    args.request.diffAlgorithm === "histogram"
      ? args.request.diffAlgorithm
      : "default";
  const contextRaw = Number(args.request.contextLines);
  const contextLines = Number.isFinite(contextRaw) ? Math.max(0, Math.min(200, Math.trunc(contextRaw))) : null;
  const pathspecs = Array.isArray(args.request.pathspecs) ? args.request.pathspecs.map((item) => sanitizeGitPathspec(item)) : [];
  return {
    requestId,
    responseSubject,
    targetPath,
    base: sanitizeGitRef(args.request.base),
    target: sanitizeGitRef(args.request.target),
    mergeBase: args.request.mergeBase === true,
    staged: args.request.staged === true,
    format,
    contextLines,
    ignoreWhitespace,
    diffAlgorithm,
    findRenames: args.request.findRenames === true,
    pathspecs,
  };
}

function buildAgentGitDiffArgs(repoRootAbs: string, request: NormalizedGitRpcRequest): { args: string[]; display: string } {
  const args = ["-C", repoRootAbs, "diff", "--no-color"];
  const displayParts = ["git", "diff", "--no-color"];
  if (request.staged) {
    args.push("--cached");
    displayParts.push("--cached");
  }
  if (typeof request.contextLines === "number") {
    args.push(`-U${request.contextLines}`);
    displayParts.push(`-U${request.contextLines}`);
  }
  if (request.ignoreWhitespace === "at-eol") {
    args.push("--ignore-space-at-eol");
    displayParts.push("--ignore-space-at-eol");
  } else if (request.ignoreWhitespace === "change") {
    args.push("--ignore-space-change");
    displayParts.push("--ignore-space-change");
  } else if (request.ignoreWhitespace === "all") {
    args.push("--ignore-all-space");
    displayParts.push("--ignore-all-space");
  }
  if (request.diffAlgorithm !== "default") {
    args.push(`--diff-algorithm=${request.diffAlgorithm}`);
    displayParts.push(`--diff-algorithm=${request.diffAlgorithm}`);
  }
  if (request.findRenames) {
    args.push("--find-renames");
    displayParts.push("--find-renames");
  }
  if (request.format === "name-only") {
    args.push("--name-only");
    displayParts.push("--name-only");
  } else if (request.format === "name-status") {
    args.push("--name-status");
    displayParts.push("--name-status");
  } else if (request.format === "stat") {
    args.push("--stat");
    displayParts.push("--stat");
  } else if (request.format === "numstat") {
    args.push("--numstat");
    displayParts.push("--numstat");
  } else if (request.format === "raw") {
    args.push("--raw");
    displayParts.push("--raw");
  }
  if (request.mergeBase) {
    if (!request.base || !request.target) {
      throw new Error("mergeBase mode requires both base and target");
    }
    const merged = `${request.base}...${request.target}`;
    args.push(merged);
    displayParts.push(merged);
  } else {
    if (request.base) {
      args.push(request.base);
      displayParts.push(request.base);
    }
    if (request.target) {
      args.push(request.target);
      displayParts.push(request.target);
    }
  }
  if (request.pathspecs.length > 0) {
    args.push("--", ...request.pathspecs);
    displayParts.push("--", ...request.pathspecs);
  }
  return { args, display: displayParts.join(" ") };
}

function buildUntrackedText(format: AgentGitDiffFormat, untrackedPaths: string[]): string {
  if (untrackedPaths.length === 0) {
    return "";
  }
  if (format === "name-status" || format === "raw") {
    return `${untrackedPaths.map((item) => `??\t${item}`).join("\n")}\n`;
  }
  if (format === "name-only") {
    return `${untrackedPaths.join("\n")}\n`;
  }
  return `\n# Untracked files\n${untrackedPaths.join("\n")}\n`;
}

async function appendAgentLocalUntrackedDiff(
  repoRootAbs: string,
  request: NormalizedGitRpcRequest,
  baseOutput: string,
): Promise<{ output: string; hasUntracked: boolean }> {
  const listArgs = ["-C", repoRootAbs, "ls-files", "--others", "--exclude-standard"];
  if (request.pathspecs.length > 0) {
    listArgs.push("--", ...request.pathspecs);
  }
  const listResult = await runLocalCommand("git", listArgs, repoRootAbs);
  if (listResult.code !== 0) {
    return { output: baseOutput, hasUntracked: false };
  }
  const untrackedPaths = listResult.stdout.split(/\r?\n/).map((item) => item.trim()).filter(Boolean);
  if (untrackedPaths.length === 0) {
    return { output: baseOutput, hasUntracked: false };
  }
  if (request.format !== "patch") {
    return { output: `${baseOutput}${buildUntrackedText(request.format, untrackedPaths)}`, hasUntracked: true };
  }
  let output = baseOutput;
  for (const relPath of untrackedPaths) {
    const diffResult = await runLocalCommand("git", ["-C", repoRootAbs, "diff", "--no-color", "--no-index", "--", "/dev/null", relPath], repoRootAbs);
    if (diffResult.code !== 0 && diffResult.code !== 1) {
      throw new Error(diffResult.stderr.trim() || `Failed to render agent untracked diff: ${relPath}`);
    }
    if (diffResult.stdout) {
      output += diffResult.stdout;
      if (!output.endsWith("\n")) {
        output += "\n";
      }
    }
  }
  return { output, hasUntracked: true };
}

function publishGitRpcResponse(args: {
  nc: NatsConnection;
  responseSubject: string;
  payload: AgentGitRpcResponse;
}): void {
  args.nc.publish(args.responseSubject, gitRpcCodec.encode(JSON.stringify(args.payload)));
}

export async function handleGitRpcMessage(args: {
  msg: Msg;
  nc: NatsConnection;
  agentId: string;
  onError: (message: string) => void;
}): Promise<void> {
  let requestId = "unknown";
  let responseSubject = "";
  try {
    const payload = JSON.parse(gitRpcCodec.decode(args.msg.data)) as AgentGitRpcRequest;
    const request = normalizeGitRpcRequest({ request: payload, agentId: args.agentId });
    requestId = request.requestId;
    responseSubject = request.responseSubject;
    if (!request.targetPath.startsWith("/")) {
      throw new Error("agent source requires an absolute directory path");
    }

    const topLevelResult = await runLocalCommand("git", ["-C", request.targetPath, "rev-parse", "--show-toplevel"], request.targetPath);
    if (topLevelResult.code !== 0) {
      publishGitRpcResponse({
        nc: args.nc,
        responseSubject,
        payload: {
          requestId,
          ok: true,
          payload: {
            isGitRepo: false,
            mode: "git_diff",
            source: "agent",
            agent: { id: args.agentId, name: null },
            currentPath: request.targetPath,
            repoRoot: null,
            repoRelativePath: null,
            branch: null,
            gitDiff: {
              command: "git diff --no-color",
              format: "patch",
              output: "",
              outputTruncated: false,
            },
            message: "현재 경로가 Git 저장소가 아닙니다.",
          },
        },
      });
      return;
    }

    const repoRootAbs = topLevelResult.stdout.trim();
    const prefixResult = await runLocalCommand("git", ["-C", request.targetPath, "rev-parse", "--show-prefix"], request.targetPath);
    const repoRelativePath = prefixResult.code === 0 ? (prefixResult.stdout.trim().replace(/\/$/, "") || ".") : ".";
    const branchResult = await runLocalCommand("git", ["-C", repoRootAbs, "symbolic-ref", "--quiet", "--short", "HEAD"], repoRootAbs);
    const detachedResult =
      branchResult.code === 0 ? null : await runLocalCommand("git", ["-C", repoRootAbs, "rev-parse", "--short", "HEAD"], repoRootAbs);
    const branch =
      branchResult.code === 0 ? branchResult.stdout.trim() || null : detachedResult && detachedResult.code === 0 ? detachedResult.stdout.trim() || null : null;

    const gitDiffArgs = buildAgentGitDiffArgs(repoRootAbs, request);
    const gitDiffResult = await runLocalCommand("git", gitDiffArgs.args, repoRootAbs);
    if (gitDiffResult.code !== 0) {
      throw new Error(gitDiffResult.stderr.trim() || "Failed to run agent git diff");
    }
    const withUntracked = await appendAgentLocalUntrackedDiff(repoRootAbs, request, gitDiffResult.stdout);
    publishGitRpcResponse({
      nc: args.nc,
      responseSubject,
      payload: {
        requestId,
        ok: true,
        payload: {
          isGitRepo: true,
          mode: "git_diff",
          source: "agent",
          agent: { id: args.agentId, name: null },
          currentPath: request.targetPath,
          repoRoot: repoRootAbs,
          repoRelativePath,
          branch,
          gitDiff: {
            command: withUntracked.hasUntracked ? `${gitDiffArgs.display} (+ untracked)` : gitDiffArgs.display,
            format: request.format,
            output: withUntracked.output,
            outputTruncated: false,
          },
        },
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (responseSubject) {
      publishGitRpcResponse({
        nc: args.nc,
        responseSubject,
        payload: { requestId, ok: false, error: message },
      });
    }
    args.onError(`git rpc failed requestId=${requestId} error=${message}`);
  }
}
