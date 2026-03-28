import fs from "node:fs/promises";
import path from "node:path";

const BEGIN_MARKER = "*** Begin Patch";
const END_MARKER = "*** End Patch";
const ADD_FILE_PREFIX = "*** Add File: ";
const DELETE_FILE_PREFIX = "*** Delete File: ";
const UPDATE_FILE_PREFIX = "*** Update File: ";
const MOVE_TO_PREFIX = "*** Move to: ";
const EOF_MARKER = "*** End of File";

type AddOp = {
  type: "add";
  filePath: string;
  lines: string[];
};

type DeleteOp = {
  type: "delete";
  filePath: string;
};

type UpdateChunk = {
  lines: string[];
};

type UpdateOp = {
  type: "update";
  filePath: string;
  moveTo: string | null;
  chunks: UpdateChunk[];
};

type PatchOp = AddOp | DeleteOp | UpdateOp;

function fail(message: string): never {
  throw new Error(message);
}

async function readStdin(): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of process.stdin) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)));
  }
  return Buffer.concat(chunks).toString("utf8");
}

async function readPatchFromInput(): Promise<string> {
  const argv = process.argv.slice(2);
  if (argv.length === 1) {
    return argv[0] ?? "";
  }
  if (argv.length > 1) {
    fail("apply_patch accepts exactly one argument or stdin.");
  }
  if (process.stdin.isTTY) {
    fail("Usage: apply_patch 'PATCH' or echo 'PATCH' | apply_patch");
  }
  return await readStdin();
}

function normalizePatchText(value: string): string {
  return value.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
}

function splitPatchLines(value: string): string[] {
  const normalized = normalizePatchText(value);
  const lines = normalized.split("\n");
  while (lines.length > 0 && lines[lines.length - 1] === "") {
    lines.pop();
  }
  return lines;
}

function validateRelativePath(filePath: string): string {
  const trimmed = filePath.trim();
  if (!trimmed) {
    fail("Patch path is empty.");
  }
  if (path.isAbsolute(trimmed)) {
    fail(`Absolute paths are not allowed: ${trimmed}`);
  }
  const normalized = path.posix.normalize(trimmed.replace(/\\/g, "/"));
  if (normalized.startsWith("../") || normalized === "..") {
    fail(`Path escapes workspace: ${trimmed}`);
  }
  return normalized;
}

function parseUpdateChunks(lines: string[]): UpdateChunk[] {
  const chunks: UpdateChunk[] = [];
  let current: string[] = [];
  for (const line of lines) {
    if (line === EOF_MARKER) {
      continue;
    }
    if (line.startsWith("@@")) {
      if (current.length > 0) {
        chunks.push({ lines: current });
        current = [];
      }
      continue;
    }
    if (!/^[ +\-]/.test(line)) {
      fail(`Unsupported patch line: ${line}`);
    }
    current.push(line);
  }
  if (current.length > 0) {
    chunks.push({ lines: current });
  }
  return chunks;
}

function parsePatch(text: string): PatchOp[] {
  const lines = splitPatchLines(text);
  if (lines[0] !== BEGIN_MARKER) {
    fail("Patch must start with '*** Begin Patch'.");
  }
  if (lines[lines.length - 1] !== END_MARKER) {
    fail("Patch must end with '*** End Patch'.");
  }

  const ops: PatchOp[] = [];
  let index = 1;
  while (index < lines.length - 1) {
    const line = lines[index] ?? "";
    if (line.startsWith(ADD_FILE_PREFIX)) {
      const filePath = validateRelativePath(line.slice(ADD_FILE_PREFIX.length));
      index += 1;
      const addLines: string[] = [];
      while (index < lines.length - 1) {
        const next = lines[index] ?? "";
        if (next.startsWith("*** ")) {
          break;
        }
        if (!next.startsWith("+")) {
          fail(`Add file lines must start with '+': ${next}`);
        }
        addLines.push(next.slice(1));
        index += 1;
      }
      ops.push({ type: "add", filePath, lines: addLines });
      continue;
    }

    if (line.startsWith(DELETE_FILE_PREFIX)) {
      const filePath = validateRelativePath(line.slice(DELETE_FILE_PREFIX.length));
      index += 1;
      ops.push({ type: "delete", filePath });
      continue;
    }

    if (line.startsWith(UPDATE_FILE_PREFIX)) {
      const filePath = validateRelativePath(line.slice(UPDATE_FILE_PREFIX.length));
      index += 1;
      let moveTo: string | null = null;
      if ((lines[index] ?? "").startsWith(MOVE_TO_PREFIX)) {
        moveTo = validateRelativePath((lines[index] ?? "").slice(MOVE_TO_PREFIX.length));
        index += 1;
      }
      const updateLines: string[] = [];
      while (index < lines.length - 1) {
        const next = lines[index] ?? "";
        if (
          next.startsWith(ADD_FILE_PREFIX) ||
          next.startsWith(DELETE_FILE_PREFIX) ||
          next.startsWith(UPDATE_FILE_PREFIX)
        ) {
          break;
        }
        updateLines.push(next);
        index += 1;
      }
      ops.push({ type: "update", filePath, moveTo, chunks: parseUpdateChunks(updateLines) });
      continue;
    }

    fail(`Unsupported patch header: ${line}`);
  }
  return ops;
}

function splitFileContent(content: string): { lines: string[]; hadTrailingNewline: boolean } {
  const hadTrailingNewline = content.endsWith("\n");
  const normalized = normalizePatchText(content);
  let lines = normalized.split("\n");
  if (hadTrailingNewline) {
    lines = lines.slice(0, -1);
  }
  return { lines, hadTrailingNewline };
}

function joinFileContent(lines: string[], trailingNewline: boolean): string {
  const body = lines.join("\n");
  return trailingNewline && lines.length > 0 ? `${body}\n` : body;
}

function findSequence(source: string[], needle: string[], start: number): number {
  if (needle.length === 0) {
    return start;
  }
  for (let i = start; i <= source.length - needle.length; i += 1) {
    let matched = true;
    for (let j = 0; j < needle.length; j += 1) {
      if (source[i + j] !== needle[j]) {
        matched = false;
        break;
      }
    }
    if (matched) {
      return i;
    }
  }
  return -1;
}

function applyChunksToLines(originalLines: string[], chunks: UpdateChunk[]): string[] {
  const output: string[] = [];
  let cursor = 0;

  for (const chunk of chunks) {
    const oldLines: string[] = [];
    const newLines: string[] = [];
    for (const line of chunk.lines) {
      const marker = line[0];
      const value = line.slice(1);
      if (marker === " ") {
        oldLines.push(value);
        newLines.push(value);
      } else if (marker === "-") {
        oldLines.push(value);
      } else if (marker === "+") {
        newLines.push(value);
      }
    }

    const matchIndex = findSequence(originalLines, oldLines, cursor);
    if (matchIndex < 0) {
      fail(`Failed to match patch context near: ${oldLines[0] ?? "<insert>"}`);
    }

    output.push(...originalLines.slice(cursor, matchIndex));
    output.push(...newLines);
    cursor = matchIndex + oldLines.length;
  }

  output.push(...originalLines.slice(cursor));
  return output;
}

async function ensureParentDir(filePath: string): Promise<void> {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
}

async function applyAddOp(cwd: string, op: AddOp): Promise<void> {
  const absPath = path.join(cwd, op.filePath);
  try {
    await fs.stat(absPath);
    fail(`File already exists: ${op.filePath}`);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
      throw error;
    }
  }
  await ensureParentDir(absPath);
  await fs.writeFile(absPath, joinFileContent(op.lines, op.lines.length > 0), "utf8");
}

async function applyDeleteOp(cwd: string, op: DeleteOp): Promise<void> {
  const absPath = path.join(cwd, op.filePath);
  await fs.unlink(absPath);
}

async function applyUpdateOp(cwd: string, op: UpdateOp): Promise<void> {
  const sourcePath = path.join(cwd, op.filePath);
  const sourceText = await fs.readFile(sourcePath, "utf8");
  const { lines: originalLines, hadTrailingNewline } = splitFileContent(sourceText);
  const nextLines = applyChunksToLines(originalLines, op.chunks);
  const nextPath = path.join(cwd, op.moveTo ?? op.filePath);
  await ensureParentDir(nextPath);
  await fs.writeFile(nextPath, joinFileContent(nextLines, hadTrailingNewline), "utf8");
  if (op.moveTo && op.moveTo !== op.filePath) {
    await fs.unlink(sourcePath);
  }
}

async function main(): Promise<void> {
  const patchText = await readPatchFromInput();
  const ops = parsePatch(patchText);
  const cwd = process.cwd();
  for (const op of ops) {
    if (op.type === "add") {
      await applyAddOp(cwd, op);
    } else if (op.type === "delete") {
      await applyDeleteOp(cwd, op);
    } else {
      await applyUpdateOp(cwd, op);
    }
  }
  process.stdout.write(`Applied patch to ${ops.length} file(s).\n`);
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
