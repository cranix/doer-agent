import crypto from "node:crypto";
import path from "node:path";
import { mkdir, readFile, readdir, rename, rm, stat, writeFile } from "node:fs/promises";
import { formatPatch, structuredPatch, type StructuredPatch } from "diff";

export type AgentNoteSummary = {
  id: string;
  path: string;
  name: string;
  size: number;
  mtimeMs: number;
};

export type AgentNoteDocument = {
  id: string;
  path: string;
  name: string;
  content: string;
  totalSize: number;
};

export type AgentNoteChangeResult = {
  note: AgentNoteDocument | null;
  notes: AgentNoteSummary[];
  patchPath: string | null;
  patchId: string | null;
};

const AGENT_NOTES_ROOT = ".doer-agent/notes";
const PATCHES_ROOT = `${AGENT_NOTES_ROOT}/patches`;

function nowIso(): string {
  return new Date().toISOString();
}

function createTimeBasedPatchId(createdAt: string): string {
  const stamp = createdAt.replace(/[-:.]/g, "").replace("T", "t").replace("Z", "z");
  const random = Math.random().toString(36).slice(2, 8);
  return `${stamp}-${random}`;
}

function patchRelPath(id: string, createdAt: string): string {
  const date = new Date(createdAt);
  const year = String(date.getUTCFullYear());
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  return path.posix.join(PATCHES_ROOT, year, month, `${id}.patch`);
}

export function sanitizeNoteId(value: string | null | undefined): string {
  const trimmed = typeof value === "string" ? value.trim() : "";
  const baseName = path.posix.basename(trimmed);
  const withoutSlashes = baseName.replace(/[\\/]/g, "");
  if (!withoutSlashes || withoutSlashes === "." || withoutSlashes === "..") {
    throw new Error("note filename is required");
  }
  return withoutSlashes.endsWith(".md") ? withoutSlashes : `${withoutSlashes}.md`;
}

function workspacePath(workspaceRoot: string, relPath: string): string {
  const root = path.resolve(workspaceRoot);
  const abs = path.resolve(root, relPath.replace(/^\/+/, ""));
  if (abs !== root && !abs.startsWith(root + path.sep)) {
    throw new Error("path escapes workspace root");
  }
  return abs;
}

function noteRelPath(noteId: string): string {
  return path.posix.join(AGENT_NOTES_ROOT, sanitizeNoteId(noteId));
}

function sha256(text: string): string {
  return crypto.createHash("sha256").update(text).digest("hex");
}

async function readTextIfExists(abs: string): Promise<string> {
  try {
    return await readFile(abs, "utf8");
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
      return "";
    }
    throw error;
  }
}

function metadata(args: {
  patchId: string;
  createdAt: string;
  operation: "create" | "content" | "rename" | "delete";
  noteId: string;
  oldText?: string;
  nextText?: string;
  nextNoteId?: string;
}): string[] {
  const rows = [
    `# doer-note-patch-id: ${args.patchId}`,
    `# created-at: ${args.createdAt}`,
    `# operation: ${args.operation}`,
    `# note-id: ${args.noteId}`,
    `# note-path: ${noteRelPath(args.noteId)}`,
  ];
  if (args.nextNoteId) {
    rows.push(`# next-note-id: ${args.nextNoteId}`);
    rows.push(`# next-note-path: ${noteRelPath(args.nextNoteId)}`);
  }
  if (typeof args.oldText === "string" && typeof args.nextText === "string") {
    rows.push(`# base-sha256: ${sha256(args.oldText)}`);
    rows.push(`# next-sha256: ${sha256(args.nextText)}`);
  }
  return rows;
}

function changedPatch(args: {
  noteId: string;
  oldText: string;
  nextText: string;
  patchId: string;
  createdAt: string;
  operation: "create" | "content" | "delete";
}): string {
  const patch = structuredPatch(
    `a/${args.noteId}`,
    `b/${args.noteId}`,
    args.oldText,
    args.nextText,
    undefined,
    undefined,
    { context: 3 },
  );
  patch.isGit = true;
  return `${metadata(args).join("\n")}\n${formatPatch(patch)}`;
}

function renamePatch(args: {
  oldNoteId: string;
  nextNoteId: string;
  patchId: string;
  createdAt: string;
}): string {
  const patch: StructuredPatch = {
    oldFileName: `a/${args.oldNoteId}`,
    newFileName: `b/${args.nextNoteId}`,
    oldHeader: undefined,
    newHeader: undefined,
    hunks: [],
    isGit: true,
    isRename: true,
  };
  return `${metadata({
    patchId: args.patchId,
    createdAt: args.createdAt,
    operation: "rename",
    noteId: args.oldNoteId,
    nextNoteId: args.nextNoteId,
  }).join("\n")}\n${formatPatch(patch)}`;
}

async function writePatch(workspaceRoot: string, patchId: string, createdAt: string, text: string): Promise<string> {
  const relPath = patchRelPath(patchId, createdAt);
  const abs = workspacePath(workspaceRoot, relPath);
  await mkdir(path.dirname(abs), { recursive: true });
  await writeFile(abs, text, "utf8");
  return relPath;
}

export async function listAgentNotesLocal(workspaceRoot: string): Promise<AgentNoteSummary[]> {
  const rootAbs = workspacePath(workspaceRoot, AGENT_NOTES_ROOT);
  const rows = await readdir(rootAbs, { withFileTypes: true }).catch(() => []);
  const notes = await Promise.all(rows
    .filter((row) => row.isFile() && row.name.endsWith(".md"))
    .map(async (row) => {
      const abs = path.join(rootAbs, row.name);
      const entry = await stat(abs);
      const id = sanitizeNoteId(row.name);
      return {
        id,
        path: noteRelPath(id),
        name: row.name,
        size: entry.size,
        mtimeMs: entry.mtimeMs,
      };
    }));
  notes.sort((a, b) => {
    return b.mtimeMs - a.mtimeMs || a.name.localeCompare(b.name);
  });
  return notes;
}

export async function getAgentNoteLocal(workspaceRoot: string, noteId?: string): Promise<AgentNoteDocument> {
  const id = sanitizeNoteId(noteId);
  const content = await readTextIfExists(workspacePath(workspaceRoot, noteRelPath(id)));
  return {
    id,
    path: noteRelPath(id),
    name: id,
    content,
    totalSize: Buffer.byteLength(content, "utf8"),
  };
}

export async function saveAgentNoteLocal(args: {
  workspaceRoot: string;
  noteId?: string;
  content: string;
}): Promise<AgentNoteChangeResult> {
  const noteId = sanitizeNoteId(args.noteId);
  const noteAbs = workspacePath(args.workspaceRoot, noteRelPath(noteId));
  const oldText = await readTextIfExists(noteAbs);
  const nextText = args.content.replace(/\r\n/g, "\n");
  if (oldText === nextText) {
    return {
      note: await getAgentNoteLocal(args.workspaceRoot, noteId),
      notes: await listAgentNotesLocal(args.workspaceRoot),
      patchPath: null,
      patchId: null,
    };
  }
  const createdAt = nowIso();
  const patchId = createTimeBasedPatchId(createdAt);
  const patchText = changedPatch({ noteId, oldText, nextText, patchId, createdAt, operation: "content" });
  const patchPath = await writePatch(args.workspaceRoot, patchId, createdAt, patchText);
  await mkdir(path.dirname(noteAbs), { recursive: true });
  await writeFile(noteAbs, nextText, "utf8");
  return {
    note: await getAgentNoteLocal(args.workspaceRoot, noteId),
    notes: await listAgentNotesLocal(args.workspaceRoot),
    patchPath,
    patchId,
  };
}

export async function createAgentNoteLocal(workspaceRoot: string, name: string): Promise<AgentNoteChangeResult> {
  const createdAt = nowIso();
  const noteId = sanitizeNoteId(name);
  const noteAbs = workspacePath(workspaceRoot, noteRelPath(noteId));
  try {
    await stat(noteAbs);
    throw new Error("A note with that filename already exists");
  } catch (error) {
    if (!(error && typeof error === "object" && "code" in error && error.code === "ENOENT")) {
      throw error;
    }
  }
  const patchId = createTimeBasedPatchId(createdAt);
  const patchText = changedPatch({ noteId, oldText: "", nextText: "", patchId, createdAt, operation: "create" });
  const patchPath = await writePatch(workspaceRoot, patchId, createdAt, patchText);
  await mkdir(path.dirname(noteAbs), { recursive: true });
  await writeFile(noteAbs, "", "utf8");
  return {
    note: await getAgentNoteLocal(workspaceRoot, noteId),
    notes: await listAgentNotesLocal(workspaceRoot),
    patchPath,
    patchId,
  };
}

export async function renameAgentNoteLocal(args: {
  workspaceRoot: string;
  noteId?: string;
  name: string;
}): Promise<AgentNoteChangeResult> {
  const currentId = sanitizeNoteId(args.noteId);
  const nextId = sanitizeNoteId(args.name);
  if (currentId === nextId) {
    return {
      note: await getAgentNoteLocal(args.workspaceRoot, currentId),
      notes: await listAgentNotesLocal(args.workspaceRoot),
      patchPath: null,
      patchId: null,
    };
  }
  const nextAbs = workspacePath(args.workspaceRoot, noteRelPath(nextId));
  try {
    await stat(nextAbs);
    throw new Error("A note with that filename already exists");
  } catch (error) {
    if (!(error && typeof error === "object" && "code" in error && error.code === "ENOENT")) {
      throw error;
    }
  }
  const currentAbs = workspacePath(args.workspaceRoot, noteRelPath(currentId));
  const createdAt = nowIso();
  const patchId = createTimeBasedPatchId(createdAt);
  const patchPath = await writePatch(
    args.workspaceRoot,
    patchId,
    createdAt,
    renamePatch({ oldNoteId: currentId, nextNoteId: nextId, patchId, createdAt }),
  );
  await mkdir(path.dirname(nextAbs), { recursive: true });
  await rename(currentAbs, nextAbs);
  return {
    note: await getAgentNoteLocal(args.workspaceRoot, nextId),
    notes: await listAgentNotesLocal(args.workspaceRoot),
    patchPath,
    patchId,
  };
}

export async function deleteAgentNoteLocal(args: {
  workspaceRoot: string;
  noteId?: string;
}): Promise<AgentNoteChangeResult> {
  const noteId = sanitizeNoteId(args.noteId);
  const noteAbs = workspacePath(args.workspaceRoot, noteRelPath(noteId));
  const oldText = await readTextIfExists(noteAbs);
  const createdAt = nowIso();
  const patchId = createTimeBasedPatchId(createdAt);
  const patchPath = await writePatch(
    args.workspaceRoot,
    patchId,
    createdAt,
    changedPatch({ noteId, oldText, nextText: "", patchId, createdAt, operation: "delete" }),
  );
  await rm(noteAbs, { force: true });
  const notes = await listAgentNotesLocal(args.workspaceRoot);
  return {
    note: notes[0] ? await getAgentNoteLocal(args.workspaceRoot, notes[0].id) : null,
    notes,
    patchPath,
    patchId,
  };
}

export function agentNotesCapabilitiesLocal(): {
  storage: "agent-workspace";
  notesRoot: string;
  patchFormat: "git-diff";
  patchesRoot: string;
} {
  return {
    storage: "agent-workspace",
    notesRoot: AGENT_NOTES_ROOT,
    patchFormat: "git-diff",
    patchesRoot: PATCHES_ROOT,
  };
}
