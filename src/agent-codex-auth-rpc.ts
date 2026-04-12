import { spawn, type ChildProcess } from "node:child_process";
import { unlink } from "node:fs/promises";
import path from "node:path";
import { StringCodec, type Msg, type NatsConnection } from "nats";

const codexAuthRpcCodec = StringCodec();

interface AgentCodexAuthRpcRequest {
  requestId?: unknown;
  responseSubject?: unknown;
  agentId?: unknown;
  action?: unknown;
  apiKey?: unknown;
}

interface AgentCodexAuthRpcResponse {
  requestId: string;
  ok: boolean;
  loggedIn?: boolean;
  output?: string;
  verificationUri?: string | null;
  userCode?: string | null;
  error?: string;
}

interface PendingCodexDeviceAuth {
  child: ChildProcess;
  output: string;
}

let pendingCodexDeviceAuth: PendingCodexDeviceAuth | null = null;

function parseCodexDeviceAuthOutput(raw: string, stripAnsi: (value: string) => string): { verificationUri: string | null; userCode: string | null } {
  const text = stripAnsi(raw);
  const urlMatch = text.match(/https?:\/\/[^\s]+/i);
  const codeMatch = text.match(/\b[A-Z0-9]{4,}(?:-[A-Z0-9]{4,})+\b/);
  return {
    verificationUri: urlMatch?.[0] ?? null,
    userCode: codeMatch?.[0] ?? null,
  };
}

function pendingCodexDeviceAuthMessage(state: PendingCodexDeviceAuth, stripAnsi: (value: string) => string): string {
  const parsed = parseCodexDeviceAuthOutput(state.output, stripAnsi);
  if (parsed.verificationUri && parsed.userCode) {
    return `Waiting for approval. Enter code ${parsed.userCode} at ${parsed.verificationUri}`;
  }
  return stripAnsi(state.output).trim() || "Waiting for approval";
}

async function getLocalCodexLoginStatus(args: {
  runLocalCodexCli: (cmdArgs: string[], timeoutMs: number, envPatch?: Record<string, string>) => Promise<{
    code: number | null;
    stdout: string;
    stderr: string;
    timedOut: boolean;
  }>;
  stripAnsi: (value: string) => string;
}): Promise<{ loggedIn: boolean; output: string }> {
  const result = await args.runLocalCodexCli(["login", "status"], 5000);
  const merged = args.stripAnsi([result.stdout, result.stderr].filter(Boolean).join("\n")).trim();
  return {
    loggedIn: (result.code ?? 1) === 0,
    output: merged || ((result.code ?? 1) === 0 ? "Logged in" : "Not logged in"),
  };
}

async function waitForCodexDeviceCode(
  state: PendingCodexDeviceAuth,
  timeoutMs: number,
  stripAnsi: (value: string) => string,
): Promise<void> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const parsed = parseCodexDeviceAuthOutput(state.output, stripAnsi);
    if (parsed.verificationUri && parsed.userCode) {
      return;
    }
    if (state.child.exitCode !== null) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

async function startLocalCodexDeviceAuth(args: {
  workspaceRoot: string;
  buildLocalCodexCliCommand: (cmdArgs: string[]) => string;
  resolveShellPath: () => string;
  resolveCodexHomePath: () => string;
  stripAnsi: (value: string) => string;
}): Promise<{
  loggedIn: false;
  output: string;
  verificationUri: string | null;
  userCode: string | null;
}> {
  if (pendingCodexDeviceAuth && pendingCodexDeviceAuth.child.exitCode === null) {
    const parsed = parseCodexDeviceAuthOutput(pendingCodexDeviceAuth.output, args.stripAnsi);
    return {
      loggedIn: false,
      output: pendingCodexDeviceAuthMessage(pendingCodexDeviceAuth, args.stripAnsi),
      verificationUri: parsed.verificationUri,
      userCode: parsed.userCode,
    };
  }

  const child = spawn(args.buildLocalCodexCliCommand(["login", "--device-auth"]), {
    cwd: args.workspaceRoot,
    shell: args.resolveShellPath(),
    detached: process.platform !== "win32",
    env: {
      ...process.env,
      WORKSPACE: args.workspaceRoot,
      CODEX_HOME: args.resolveCodexHomePath(),
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  child.stdout?.setEncoding("utf8");
  child.stderr?.setEncoding("utf8");

  const state: PendingCodexDeviceAuth = { child, output: "" };
  pendingCodexDeviceAuth = state;

  const appendOutput = (chunk: string) => {
    state.output += chunk;
  };
  child.stdout?.on("data", appendOutput);
  child.stderr?.on("data", appendOutput);
  child.once("exit", () => {
    if (pendingCodexDeviceAuth === state) {
      pendingCodexDeviceAuth = null;
    }
  });

  await waitForCodexDeviceCode(state, 8000, args.stripAnsi);

  const parsed = parseCodexDeviceAuthOutput(state.output, args.stripAnsi);
  if ((!parsed.verificationUri || !parsed.userCode) && state.child.exitCode !== null) {
    throw new Error("Failed to read device code from Codex CLI");
  }

  return {
    loggedIn: false,
    output: pendingCodexDeviceAuthMessage(state, args.stripAnsi),
    verificationUri: parsed.verificationUri,
    userCode: parsed.userCode,
  };
}

async function startLocalCodexLogin(args: {
  workspaceRoot: string;
  buildLocalCodexCliCommand: (cmdArgs: string[]) => string;
  resolveShellPath: () => string;
  resolveCodexHomePath: () => string;
  stripAnsi: (value: string) => string;
  getLocalCodexLoginStatus: () => Promise<{ loggedIn: boolean; output: string }>;
}): Promise<{
  loggedIn: boolean;
  output: string;
  verificationUri: string | null;
  userCode: string | null;
}> {
  const child = spawn(args.buildLocalCodexCliCommand(["login"]), {
    cwd: args.workspaceRoot,
    shell: args.resolveShellPath(),
    detached: process.platform !== "win32",
    env: {
      ...process.env,
      WORKSPACE: args.workspaceRoot,
      CODEX_HOME: args.resolveCodexHomePath(),
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  let output = "";
  child.stdout?.setEncoding("utf8");
  child.stderr?.setEncoding("utf8");
  child.stdout?.on("data", (chunk: string) => {
    output += chunk;
  });
  child.stderr?.on("data", (chunk: string) => {
    output += chunk;
  });

  const result = await new Promise<{ code: number | null; output: string }>((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code) => resolve({ code, output }));
  });

  const normalized = args.stripAnsi(result.output).trim();
  const parsed = parseCodexDeviceAuthOutput(result.output, args.stripAnsi);
  if ((result.code ?? 1) === 0) {
    const status = await args.getLocalCodexLoginStatus().catch(() => null);
    return {
      loggedIn: status?.loggedIn === true,
      output: status?.output || normalized || "Login started",
      verificationUri: parsed.verificationUri,
      userCode: parsed.userCode,
    };
  }

  throw new Error(normalized || `Codex login failed with code ${result.code ?? "null"}`);
}

async function loginLocalCodexWithApiKey(args: {
  apiKey: string;
  runLocalCodexCliWithInput: (cmdArgs: string[], input: string, timeoutMs: number, envPatch?: Record<string, string>) => Promise<{
    code: number | null;
    stdout: string;
    stderr: string;
    timedOut: boolean;
  }>;
  stripAnsi: (value: string) => string;
  getLocalCodexLoginStatus: () => Promise<{ loggedIn: boolean; output: string }>;
}): Promise<{
  loggedIn: boolean;
  output: string;
  verificationUri: null;
  userCode: null;
}> {
  const result = await args.runLocalCodexCliWithInput(["login", "--with-api-key"], args.apiKey, 15000);
  const normalized = args.stripAnsi([result.stdout, result.stderr].filter(Boolean).join("\n")).trim();
  if ((result.code ?? 1) !== 0) {
    throw new Error(normalized || `Codex API key login failed with code ${result.code ?? "null"}`);
  }
  const status = await args.getLocalCodexLoginStatus().catch(() => null);
  return {
    loggedIn: status?.loggedIn === true,
    output: status?.output || normalized || "Logged in",
    verificationUri: null,
    userCode: null,
  };
}

async function logoutLocalCodexAuth(args: {
  sendSignalToTaskProcess: (child: ChildProcess, signal: NodeJS.Signals) => void;
  runLocalCodexCli: (cmdArgs: string[], timeoutMs: number, envPatch?: Record<string, string>) => Promise<{
    code: number | null;
    stdout: string;
    stderr: string;
    timedOut: boolean;
  }>;
  stripAnsi: (value: string) => string;
  resolveCodexHomePath: () => string;
  getLocalCodexLoginStatus: () => Promise<{ loggedIn: boolean; output: string }>;
}): Promise<{ loggedIn: false; output: string }> {
  if (pendingCodexDeviceAuth && pendingCodexDeviceAuth.child.exitCode === null) {
    args.sendSignalToTaskProcess(pendingCodexDeviceAuth.child, "SIGTERM");
    setTimeout(() => {
      if (pendingCodexDeviceAuth?.child.exitCode === null) {
        args.sendSignalToTaskProcess(pendingCodexDeviceAuth.child, "SIGKILL");
      }
    }, 1000);
    pendingCodexDeviceAuth = null;
  }
  const result = await args.runLocalCodexCli(["logout"], 5000);
  let merged = args.stripAnsi([result.stdout, result.stderr].filter(Boolean).join("\n")).trim();
  const statusAfterLogout = await args.getLocalCodexLoginStatus().catch(() => null);
  if (statusAfterLogout?.loggedIn) {
    const authFile = path.join(args.resolveCodexHomePath(), "auth.json");
    await unlink(authFile).catch(() => undefined);
    const statusAfterDelete = await args.getLocalCodexLoginStatus().catch(() => null);
    if (statusAfterDelete?.output) {
      merged = [merged, statusAfterDelete.output].filter(Boolean).join("\n");
    }
  }
  return {
    loggedIn: false,
    output: merged || "Logged out",
  };
}

function normalizeCodexAuthRpcRequest(args: {
  request: AgentCodexAuthRpcRequest;
  agentId: string;
}): {
  requestId: string;
  responseSubject: string;
  action: "start" | "status" | "logout" | "login_api_key";
  apiKey: string | null;
} {
  const requestId = typeof args.request.requestId === "string" ? args.request.requestId.trim() : "";
  const responseSubject = typeof args.request.responseSubject === "string" ? args.request.responseSubject.trim() : "";
  const requestAgentId = typeof args.request.agentId === "string" ? args.request.agentId.trim() : "";
  const actionRaw = typeof args.request.action === "string" ? args.request.action.trim() : "";
  const action: "start" | "status" | "logout" | "login_api_key" =
    actionRaw === "start" || actionRaw === "logout" || actionRaw === "login_api_key" ? actionRaw : "status";
  const apiKey = typeof args.request.apiKey === "string" && args.request.apiKey.trim() ? args.request.apiKey.trim() : null;
  if (!requestId || !responseSubject || !requestAgentId || requestAgentId !== args.agentId) {
    throw new Error("invalid codex auth rpc request");
  }
  if (action === "login_api_key" && !apiKey) {
    throw new Error("api key is required");
  }
  return { requestId, responseSubject, action, apiKey };
}

function publishCodexAuthRpcResponse(args: {
  nc: NatsConnection;
  responseSubject: string;
  payload: AgentCodexAuthRpcResponse;
}): void {
  args.nc.publish(args.responseSubject, codexAuthRpcCodec.encode(JSON.stringify(args.payload)));
}

async function handleCodexAuthRpcMessage(args: {
  msg: Msg;
  nc: NatsConnection;
  agentId: string;
  workspaceRoot: string;
  buildLocalCodexCliCommand: (cmdArgs: string[]) => string;
  resolveShellPath: () => string;
  resolveCodexHomePath: () => string;
  runLocalCodexCli: (cmdArgs: string[], timeoutMs: number, envPatch?: Record<string, string>) => Promise<{
    code: number | null;
    stdout: string;
    stderr: string;
    timedOut: boolean;
  }>;
  runLocalCodexCliWithInput: (cmdArgs: string[], input: string, timeoutMs: number, envPatch?: Record<string, string>) => Promise<{
    code: number | null;
    stdout: string;
    stderr: string;
    timedOut: boolean;
  }>;
  sendSignalToTaskProcess: (child: ChildProcess, signal: NodeJS.Signals) => void;
  stripAnsi: (value: string) => string;
  onError: (message: string) => void;
}): Promise<void> {
  let requestId = "unknown";
  let responseSubject = "";
  try {
    const payload = JSON.parse(codexAuthRpcCodec.decode(args.msg.data)) as AgentCodexAuthRpcRequest;
    const request = normalizeCodexAuthRpcRequest({ request: payload, agentId: args.agentId });
    requestId = request.requestId;
    responseSubject = request.responseSubject;

    const getStatus = () => getLocalCodexLoginStatus({
      runLocalCodexCli: args.runLocalCodexCli,
      stripAnsi: args.stripAnsi,
    });

    let result:
      | { loggedIn: boolean; output: string; verificationUri?: string | null; userCode?: string | null }
      | null = null;

    if (request.action === "login_api_key") {
      result = await loginLocalCodexWithApiKey({
        apiKey: request.apiKey ?? "",
        runLocalCodexCliWithInput: args.runLocalCodexCliWithInput,
        stripAnsi: args.stripAnsi,
        getLocalCodexLoginStatus: getStatus,
      });
    } else if (request.action === "start") {
      const status = await getStatus();
      if (status.loggedIn) {
        result = { loggedIn: true, output: status.output };
      } else {
        try {
          result = await startLocalCodexDeviceAuth({
            workspaceRoot: args.workspaceRoot,
            buildLocalCodexCliCommand: args.buildLocalCodexCliCommand,
            resolveShellPath: args.resolveShellPath,
            resolveCodexHomePath: args.resolveCodexHomePath,
            stripAnsi: args.stripAnsi,
          });
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          const normalized = message.toLowerCase();
          if (
            normalized.includes("operation not permitted") ||
            normalized.includes("failed to read device code") ||
            normalized.includes("panic") ||
            normalized.includes("null object")
          ) {
            result = await startLocalCodexLogin({
              workspaceRoot: args.workspaceRoot,
              buildLocalCodexCliCommand: args.buildLocalCodexCliCommand,
              resolveShellPath: args.resolveShellPath,
              resolveCodexHomePath: args.resolveCodexHomePath,
              stripAnsi: args.stripAnsi,
              getLocalCodexLoginStatus: getStatus,
            });
          } else {
            throw error;
          }
        }
      }
    } else if (request.action === "logout") {
      result = await logoutLocalCodexAuth({
        sendSignalToTaskProcess: args.sendSignalToTaskProcess,
        runLocalCodexCli: args.runLocalCodexCli,
        stripAnsi: args.stripAnsi,
        resolveCodexHomePath: args.resolveCodexHomePath,
        getLocalCodexLoginStatus: getStatus,
      });
    } else {
      const status = await getStatus();
      if (status.loggedIn) {
        result = { loggedIn: true, output: status.output };
      } else if (pendingCodexDeviceAuth && pendingCodexDeviceAuth.child.exitCode === null) {
        const parsed = parseCodexDeviceAuthOutput(pendingCodexDeviceAuth.output, args.stripAnsi);
        result = {
          loggedIn: false,
          output: pendingCodexDeviceAuthMessage(pendingCodexDeviceAuth, args.stripAnsi),
          verificationUri: parsed.verificationUri,
          userCode: parsed.userCode,
        };
      } else {
        result = { loggedIn: false, output: status.output || "Not logged in" };
      }
    }

    publishCodexAuthRpcResponse({
      nc: args.nc,
      responseSubject,
      payload: {
        requestId,
        ok: true,
        loggedIn: result.loggedIn,
        output: result.output,
        verificationUri: result.verificationUri ?? null,
        userCode: result.userCode ?? null,
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (responseSubject) {
      publishCodexAuthRpcResponse({
        nc: args.nc,
        responseSubject,
        payload: { requestId, ok: false, error: message },
      });
    }
    args.onError(`codex auth rpc failed requestId=${requestId} error=${message}`);
  }
}

export function subscribeToCodexAuthRpc(args: {
  nc: NatsConnection;
  subject: string;
  agentId: string;
  workspaceRoot: string;
  buildLocalCodexCliCommand: (cmdArgs: string[]) => string;
  resolveShellPath: () => string;
  resolveCodexHomePath: () => string;
  runLocalCodexCli: (cmdArgs: string[], timeoutMs: number, envPatch?: Record<string, string>) => Promise<{
    code: number | null;
    stdout: string;
    stderr: string;
    timedOut: boolean;
  }>;
  runLocalCodexCliWithInput: (cmdArgs: string[], input: string, timeoutMs: number, envPatch?: Record<string, string>) => Promise<{
    code: number | null;
    stdout: string;
    stderr: string;
    timedOut: boolean;
  }>;
  sendSignalToTaskProcess: (child: ChildProcess, signal: NodeJS.Signals) => void;
  stripAnsi: (value: string) => string;
  onInfo: (message: string) => void;
  onError: (message: string) => void;
}): void {
  args.nc.subscribe(args.subject, {
    callback: (error, msg) => {
      if (error) {
        const message = error instanceof Error ? error.message : String(error);
        args.onError(`codex auth rpc subscription error: ${message}`);
        return;
      }
      void handleCodexAuthRpcMessage({
        msg,
        nc: args.nc,
        agentId: args.agentId,
        workspaceRoot: args.workspaceRoot,
        buildLocalCodexCliCommand: args.buildLocalCodexCliCommand,
        resolveShellPath: args.resolveShellPath,
        resolveCodexHomePath: args.resolveCodexHomePath,
        runLocalCodexCli: args.runLocalCodexCli,
        runLocalCodexCliWithInput: args.runLocalCodexCliWithInput,
        sendSignalToTaskProcess: args.sendSignalToTaskProcess,
        stripAnsi: args.stripAnsi,
        onError: args.onError,
      });
    },
  });
  args.onInfo(`codex auth rpc subscribed subject=${args.subject}`);
}
