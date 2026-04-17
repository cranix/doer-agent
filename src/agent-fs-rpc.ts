import path from "node:path";
import { gunzipSync, gzipSync } from "node:zlib";
import { mkdir, open, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { StringCodec, type Msg } from "nats";

const fsRpcCodec = StringCodec();

export type AgentFsRpcAction =
  | "list"
  | "stat"
  | "upload_file"
  | "read_text"
  | "read_file"
  | "write_file"
  | "download_file"
  | "delete_path"
  | "archive_dir"
  | "extract_archive";

export interface AgentFsRpcRequest {
  requestId?: unknown;
  action?: unknown;
  path?: unknown;
  contentBase64?: unknown;
  downloadPath?: unknown;
  offset?: unknown;
  length?: unknown;
  limit?: unknown;
  maxBytes?: unknown;
  encoding?: unknown;
  uploadUrl?: unknown;
  uploadMode?: unknown;
  uploadMethod?: unknown;
  uploadContentType?: unknown;
  uploadFieldName?: unknown;
  formFields?: unknown;
  agentId?: unknown;
  archivePath?: unknown;
  destinationPath?: unknown;
}

interface ArchivedFileEntry {
  relPath: string;
  contentBase64: string;
  sizeBytes: number;
}

function normalizeFsRpcPath(workspaceRoot: string, rawPath: unknown): { abs: string; formatPath: (target: string) => string } {
  const raw = typeof rawPath === "string" && rawPath.trim() ? rawPath.trim() : ".";
  const normalizedRaw = raw.replace(/\\/g, "/");
  const useAbsolute = path.isAbsolute(normalizedRaw);
  const rel = normalizedRaw.replace(/^\/+/, "") || ".";
  const abs = useAbsolute ? path.resolve(normalizedRaw) : path.resolve(workspaceRoot, rel);
  if (!useAbsolute && abs !== workspaceRoot && !abs.startsWith(workspaceRoot + path.sep)) {
    throw new Error("path escapes workspace root");
  }
  const formatPath = (target: string): string => {
    if (useAbsolute) {
      return target.split(path.sep).join("/") || "/";
    }
    return path.relative(workspaceRoot, target).split(path.sep).join("/") || ".";
  };
  return { abs, formatPath };
}

function parseFsRpcAction(value: unknown): AgentFsRpcAction {
  if (
    value === "list" ||
    value === "stat" ||
    value === "upload_file" ||
    value === "read_text" ||
    value === "read_file" ||
    value === "write_file" ||
    value === "download_file" ||
    value === "delete_path" ||
    value === "archive_dir" ||
    value === "extract_archive"
  ) {
    return value;
  }
  throw new Error("unsupported action");
}

function normalizeFsRpcNumber(value: unknown, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return fallback;
  }
  return Math.floor(n);
}

function inferMimeType(filePath: string): string {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === ".txt" || ext === ".md" || ext === ".log") {
    return "text/plain";
  }
  if (ext === ".json") {
    return "application/json";
  }
  if (ext === ".js" || ext === ".mjs" || ext === ".cjs") {
    return "text/javascript";
  }
  if (ext === ".ts" || ext === ".tsx") {
    return "text/typescript";
  }
  if (ext === ".jsx") {
    return "text/jsx";
  }
  if (ext === ".css") {
    return "text/css";
  }
  if (ext === ".html" || ext === ".htm") {
    return "text/html";
  }
  if (ext === ".xml") {
    return "application/xml";
  }
  if (ext === ".svg") {
    return "image/svg+xml";
  }
  if (ext === ".png") {
    return "image/png";
  }
  if (ext === ".jpg" || ext === ".jpeg") {
    return "image/jpeg";
  }
  if (ext === ".gif") {
    return "image/gif";
  }
  if (ext === ".webp") {
    return "image/webp";
  }
  if (ext === ".pdf") {
    return "application/pdf";
  }
  return "application/octet-stream";
}

function normalizeArchiveRelativePath(value: string): string {
  const normalized = value.replace(/\\/g, "/").replace(/^\/+|\/+$/g, "");
  if (!normalized || normalized.includes("..")) {
    throw new Error("invalid archive entry path");
  }
  return normalized;
}

async function collectDirectoryFiles(absDir: string, rootDir = absDir): Promise<ArchivedFileEntry[]> {
  const rows = await readdir(absDir, { withFileTypes: true });
  const files: ArchivedFileEntry[] = [];
  for (const row of rows.sort((a, b) => a.name.localeCompare(b.name))) {
    const child = path.join(absDir, row.name);
    if (row.isDirectory()) {
      files.push(...await collectDirectoryFiles(child, rootDir));
      continue;
    }
    if (!row.isFile()) {
      continue;
    }
    const bytes = await readFile(child);
    files.push({
      relPath: normalizeArchiveRelativePath(path.relative(rootDir, child)),
      contentBase64: Buffer.from(bytes).toString("base64"),
      sizeBytes: bytes.byteLength,
    });
  }
  return files;
}

async function executeFsRpc(args: {
  workspaceRoot: string;
  request: AgentFsRpcRequest;
  serverBaseUrl: string;
  agentToken: string;
}): Promise<Record<string, unknown>> {
  const action = parseFsRpcAction(args.request.action);
  const { abs, formatPath } = normalizeFsRpcPath(args.workspaceRoot, args.request.path);

  if (action === "stat") {
    const entry = await stat(abs);
    return {
      ok: true,
      action,
      path: formatPath(abs),
      kind: entry.isDirectory() ? "dir" : "file",
      size: entry.size,
      mtimeMs: entry.mtimeMs,
    };
  }

  if (action === "list") {
    const entry = await stat(abs);
    if (!entry.isDirectory()) {
      throw new Error("path is not a directory");
    }
    const limit = Math.max(1, Math.min(normalizeFsRpcNumber(args.request.limit, 200), 1000));
    const rows = await readdir(abs, { withFileTypes: true });
    const items = await Promise.all(
      rows.map(async (row) => {
        const child = path.join(abs, row.name);
        const childStat = await stat(child);
        return {
          name: row.name,
          path: formatPath(child),
          kind: row.isDirectory() ? "dir" : "file",
          size: childStat.size,
          mtimeMs: childStat.mtimeMs,
        };
      }),
    );
    items.sort((a, b) => (a.kind === b.kind ? a.name.localeCompare(b.name) : a.kind === "dir" ? -1 : 1));
    return {
      ok: true,
      action,
      path: formatPath(abs),
      items: items.slice(0, limit),
      truncated: items.length > limit,
      total: items.length,
    };
  }

  if (action === "archive_dir") {
    const entry = await stat(abs);
    if (!entry.isDirectory()) {
      throw new Error("path is not a directory");
    }
    const rawArchivePath = typeof args.request.archivePath === "string" ? args.request.archivePath : "";
    if (!rawArchivePath) {
      throw new Error("archivePath is required");
    }
    const archiveTarget = normalizeFsRpcPath(args.workspaceRoot, rawArchivePath);
    const files = await collectDirectoryFiles(abs);
    if (!files.some((file) => file.relPath === "SKILL.md")) {
      throw new Error("Selected skill directory must contain SKILL.md");
    }
    const payload = gzipSync(Buffer.from(JSON.stringify({ files }), "utf8"));
    await mkdir(path.dirname(archiveTarget.abs), { recursive: true });
    await writeFile(archiveTarget.abs, payload);
    const archiveStat = await stat(archiveTarget.abs);
    return {
      ok: true,
      action,
      path: formatPath(abs),
      archivePath: archiveTarget.formatPath(archiveTarget.abs),
      size: archiveStat.size,
    };
  }

  if (action === "upload_file") {
    const entry = await stat(abs);
    if (!entry.isFile()) {
      throw new Error("path is not a file");
    }
    const uploadUrl = typeof args.request.uploadUrl === "string" ? args.request.uploadUrl : "";
    const uploadMode = args.request.uploadMode === "multipart" ? "multipart" : "raw";
    const uploadMethod = args.request.uploadMethod === "POST" ? "POST" : "PUT";
    const uploadContentType =
      typeof args.request.uploadContentType === "string" && args.request.uploadContentType.trim()
        ? args.request.uploadContentType.trim()
        : inferMimeType(abs);
    const uploadFieldName =
      typeof args.request.uploadFieldName === "string" && args.request.uploadFieldName.trim()
        ? args.request.uploadFieldName.trim()
        : "file";
    if (!uploadUrl) {
      throw new Error("uploadUrl is required");
    }
    const resolvedUploadUrl = new URL(uploadUrl, `${args.serverBaseUrl}/`).toString();
    const data = await readFile(abs);
    const fileName = path.basename(abs) || "file";
    const serverOrigin = new URL(args.serverBaseUrl).origin;
    const targetOrigin = new URL(resolvedUploadUrl).origin;
    const headers: Record<string, string> = {};
    if (targetOrigin === serverOrigin) {
      headers.Authorization = `Bearer ${args.agentToken}`;
    }

    let response: Response;
    if (uploadMode === "multipart") {
      const form = new FormData();
      const formFields =
        args.request.formFields && typeof args.request.formFields === "object" && !Array.isArray(args.request.formFields)
          ? args.request.formFields as Record<string, unknown>
          : {};
      for (const [key, value] of Object.entries(formFields)) {
        if (typeof value === "string") {
          form.append(key, value);
        }
      }
      form.append(uploadFieldName, new File([data], fileName, { type: uploadContentType }));
      response = await fetch(resolvedUploadUrl, {
        method: uploadMethod,
        headers,
        body: form,
      });
    } else {
      headers["Content-Type"] = uploadContentType;
      response = await fetch(resolvedUploadUrl, {
        method: uploadMethod,
        headers,
        body: data,
      });
    }
    const text = await response.text();
    let upload: Record<string, unknown> = {};
    try {
      upload = JSON.parse(text || "{}") as Record<string, unknown>;
    } catch {
      upload = {};
    }
    if (!response.ok) {
      const message = typeof upload.error === "string" ? upload.error : `upload failed: ${response.status}`;
      throw new Error(message);
    }
    return {
      ok: true,
      action,
      path: formatPath(abs),
      size: entry.size,
      upload,
    };
  }

  if (action === "write_file") {
    const contentBase64 = typeof args.request.contentBase64 === "string" ? args.request.contentBase64 : "";
    if (!contentBase64) {
      throw new Error("contentBase64 is required");
    }
    const parentDir = path.dirname(abs);
    await mkdir(parentDir, { recursive: true });
    const bytes = Buffer.from(contentBase64, "base64");
    await writeFile(abs, bytes);
    const entry = await stat(abs);
    return {
      ok: true,
      action,
      path: formatPath(abs),
      absolutePath: abs.split(path.sep).join("/"),
      size: entry.size,
      mimeType: inferMimeType(abs),
      mtimeMs: entry.mtimeMs,
    };
  }

  if (action === "delete_path") {
    await rm(abs, { recursive: true, force: true });
    return {
      ok: true,
      action,
      path: formatPath(abs),
      absolutePath: abs.split(path.sep).join("/"),
    };
  }

  if (action === "download_file") {
    const downloadPath = typeof args.request.downloadPath === "string" ? args.request.downloadPath.trim() : "";
    if (!downloadPath) {
      throw new Error("downloadPath is required");
    }
    const downloadUrl = new URL(downloadPath, `${args.serverBaseUrl}/`).toString();
    const serverOrigin = new URL(args.serverBaseUrl).origin;
    const headers: Record<string, string> = {};
    if (new URL(downloadUrl).origin === serverOrigin) {
      headers.Authorization = `Bearer ${args.agentToken}`;
    }
    const response = await fetch(downloadUrl, {
      method: "GET",
      headers,
    });
    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new Error(text || `download failed: ${response.status}`);
    }
    const bytes = Buffer.from(await response.arrayBuffer());
    const parentDir = path.dirname(abs);
    await mkdir(parentDir, { recursive: true });
    await writeFile(abs, bytes);
    const entry = await stat(abs);
    return {
      ok: true,
      action,
      path: formatPath(abs),
      absolutePath: abs.split(path.sep).join("/"),
      size: entry.size,
      mimeType: inferMimeType(abs),
      mtimeMs: entry.mtimeMs,
    };
  }

  if (action === "extract_archive") {
    const archiveEntry = await stat(abs);
    if (!archiveEntry.isFile()) {
      throw new Error("path is not a file");
    }
    const rawDestinationPath = typeof args.request.destinationPath === "string" ? args.request.destinationPath : "";
    if (!rawDestinationPath) {
      throw new Error("destinationPath is required");
    }
    const destinationTarget = normalizeFsRpcPath(args.workspaceRoot, rawDestinationPath);
    const archiveBytes = await readFile(abs);
    const decoded = JSON.parse(gunzipSync(archiveBytes).toString("utf8")) as {
      files?: Array<{ relPath?: unknown; contentBase64?: unknown }>;
    };
    const files = Array.isArray(decoded.files) ? decoded.files : [];
    await mkdir(destinationTarget.abs, { recursive: true });
    for (const file of files) {
      const relPath = typeof file.relPath === "string" ? normalizeArchiveRelativePath(file.relPath) : "";
      const contentBase64 = typeof file.contentBase64 === "string" ? file.contentBase64 : "";
      if (!relPath || !contentBase64) {
        throw new Error("archive contains an invalid file entry");
      }
      const targetPath = path.join(destinationTarget.abs, relPath);
      await mkdir(path.dirname(targetPath), { recursive: true });
      await writeFile(targetPath, Buffer.from(contentBase64, "base64"));
    }
    return {
      ok: true,
      action,
      path: formatPath(abs),
      absolutePath: destinationTarget.formatPath(destinationTarget.abs),
    };
  }

  const entry = await stat(abs);
  if (!entry.isFile()) {
    throw new Error("path is not a file");
  }
  if (action === "read_file") {
    const maxBytes = Math.max(1, Math.min(normalizeFsRpcNumber(args.request.maxBytes, 2_000_000), 5_000_000));
    const data = await readFile(abs);
    const truncated = data.byteLength > maxBytes;
    const bytes = truncated ? data.subarray(0, maxBytes) : data;
    return {
      ok: true,
      action,
      path: formatPath(abs),
      mimeType: inferMimeType(abs),
      size: entry.size,
      truncated,
      contentBase64: bytes.toString("base64"),
    };
  }
  const offset = Math.max(0, normalizeFsRpcNumber(args.request.offset, 0));
  const length = Math.max(1, Math.min(normalizeFsRpcNumber(args.request.length, 65536), 262144));
  const encoding = typeof args.request.encoding === "string" && args.request.encoding ? args.request.encoding : "utf8";
  const fd = await open(abs, "r");
  try {
    const buffer = Buffer.alloc(length);
    const readResult = await fd.read(buffer, 0, length, offset);
    const slice = buffer.subarray(0, readResult.bytesRead);
    try {
      const text = slice.toString(encoding as BufferEncoding);
      return {
        ok: true,
        action,
        path: formatPath(abs),
        offset,
        length: readResult.bytesRead,
        totalSize: entry.size,
        eof: offset + readResult.bytesRead >= entry.size,
        encoding,
        text,
        bytesRead: readResult.bytesRead,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : "failed to decode text";
      return {
        ok: false,
        action,
        path: formatPath(abs),
        error: message,
      };
    }
  } finally {
    await fd.close();
  }
}

export async function handleFsRpcMessage(args: {
  msg: Msg;
  workspaceRoot: string;
  serverBaseUrl: string;
  agentId: string;
  agentToken: string;
  onError: (message: string) => void;
}): Promise<void> {
  let payload: AgentFsRpcRequest = {};
  try {
    payload = JSON.parse(fsRpcCodec.decode(args.msg.data)) as AgentFsRpcRequest;
    if (typeof payload.agentId === "string" && payload.agentId.trim() && payload.agentId !== args.agentId) {
      throw new Error("agent id mismatch");
    }
    const result = await executeFsRpc({
      workspaceRoot: args.workspaceRoot,
      request: payload,
      serverBaseUrl: args.serverBaseUrl,
      agentToken: args.agentToken,
    });
    args.msg.respond(fsRpcCodec.encode(JSON.stringify(result)));
  } catch (error) {
    const message = error instanceof Error ? error.message : "unknown error";
    const action = typeof payload.action === "string" ? payload.action : "";
    const response = {
      ok: false,
      action,
      path: typeof payload.path === "string" ? payload.path : ".",
      error: message,
    };
    args.msg.respond(fsRpcCodec.encode(JSON.stringify(response)));
    args.onError(`fs rpc failed action=${action || "unknown"} error=${message}`);
  }
}
