import net from "node:net";

const MAX_CALLBACK_QUERY_LENGTH = 16_384;
const MAX_CALLBACK_BODY_LENGTH = 512_000;

export function buildMcpOauthCallbackBaseUrl(args: {
  serverBaseUrl: string;
  userId: string;
  agentId: string;
}): string {
  const url = new URL(
    `/api/mcp-oauth/callback/${encodeURIComponent(args.userId)}/${encodeURIComponent(args.agentId)}`,
    `${args.serverBaseUrl.replace(/\/+$/, "")}/`,
  );
  return url.toString().replace(/\/$/, "");
}

export async function allocateMcpOauthCallbackPort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.once("error", reject);
    server.listen(0, "0.0.0.0", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close();
        reject(new Error("Failed to allocate MCP OAuth callback port"));
        return;
      }
      const port = address.port;
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(port);
      });
    });
  });
}

export async function relayMcpOauthCallbackToLocalListener(args: {
  callbackPort: number;
  callbackPathPrefix: string;
  path: string;
  search: string;
}): Promise<{
  status: number;
  headers: Record<string, string>;
  body: string;
}> {
  if (!Number.isInteger(args.callbackPort) || args.callbackPort < 1 || args.callbackPort > 65_535) {
    throw new Error("MCP OAuth callback listener is unavailable");
  }
  if (!args.path.startsWith(`${args.callbackPathPrefix}/`)) {
    throw new Error("Invalid MCP OAuth callback path");
  }
  if (args.search.length > MAX_CALLBACK_QUERY_LENGTH) {
    throw new Error("MCP OAuth callback query is too large");
  }
  const target = new URL(`http://127.0.0.1:${args.callbackPort}`);
  target.pathname = args.path;
  target.search = args.search;
  const response = await fetch(target, {
    method: "GET",
    redirect: "manual",
    signal: AbortSignal.timeout(30_000),
  });
  const body = await response.text();
  if (body.length > MAX_CALLBACK_BODY_LENGTH) {
    throw new Error("MCP OAuth callback response is too large");
  }
  const headers: Record<string, string> = {};
  for (const name of ["content-type", "location", "cache-control"]) {
    const value = response.headers.get(name);
    if (value) headers[name] = value;
  }
  return {
    status: response.status,
    headers,
    body,
  };
}
