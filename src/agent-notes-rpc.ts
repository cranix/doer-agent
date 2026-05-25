import { StringCodec, type Msg } from "nats";
import {
  agentNotesCapabilitiesLocal,
  createAgentNoteLocal,
  deleteAgentNoteLocal,
  getAgentNoteLocal,
  listAgentNotesLocal,
  renameAgentNoteLocal,
  saveAgentNoteLocal,
} from "./agent-notes-local.js";

const notesRpcCodec = StringCodec();

type AgentNotesRpcAction = "capabilities" | "list" | "get" | "create" | "save" | "rename" | "delete";

interface AgentNotesRpcRequest {
  requestId?: unknown;
  action?: unknown;
  agentId?: unknown;
  noteId?: unknown;
  content?: unknown;
  name?: unknown;
}

function parseAction(value: unknown): AgentNotesRpcAction {
  if (
    value === "capabilities" ||
    value === "list" ||
    value === "get" ||
    value === "create" ||
    value === "save" ||
    value === "rename" ||
    value === "delete"
  ) {
    return value;
  }
  throw new Error("unsupported notes action");
}

function stringValue(value: unknown): string {
  return typeof value === "string" ? value : "";
}

async function executeNotesRpc(workspaceRoot: string, request: AgentNotesRpcRequest): Promise<Record<string, unknown>> {
  const action = parseAction(request.action);
  if (action === "capabilities") {
    return { ok: true, action, capabilities: agentNotesCapabilitiesLocal() };
  }
  if (action === "list") {
    return { ok: true, action, notes: await listAgentNotesLocal(workspaceRoot) };
  }
  if (action === "get") {
    const notes = await listAgentNotesLocal(workspaceRoot);
    const noteId = stringValue(request.noteId) || notes[0]?.id || "";
    return {
      ok: true,
      action,
      note: noteId ? await getAgentNoteLocal(workspaceRoot, noteId) : null,
      notes,
      capabilities: agentNotesCapabilitiesLocal(),
    };
  }
  if (action === "create") {
    const name = stringValue(request.name).trim();
    if (!name) {
      throw new Error("name is required");
    }
    return {
      ok: true,
      action,
      ...(await createAgentNoteLocal(workspaceRoot, name)),
      capabilities: agentNotesCapabilitiesLocal(),
    };
  }
  if (action === "save") {
    return {
      ok: true,
      action,
      ...(await saveAgentNoteLocal({
        workspaceRoot,
        noteId: stringValue(request.noteId),
        content: stringValue(request.content),
      })),
      capabilities: agentNotesCapabilitiesLocal(),
    };
  }
  if (action === "rename") {
    const name = stringValue(request.name).trim();
    if (!name) {
      throw new Error("name is required");
    }
    return {
      ok: true,
      action,
      ...(await renameAgentNoteLocal({
        workspaceRoot,
        noteId: stringValue(request.noteId),
        name,
      })),
      capabilities: agentNotesCapabilitiesLocal(),
    };
  }
  return {
    ok: true,
    action,
    ...(await deleteAgentNoteLocal({
      workspaceRoot,
      noteId: stringValue(request.noteId),
    })),
    capabilities: agentNotesCapabilitiesLocal(),
  };
}

export async function handleNotesRpcMessage(args: {
  msg: Msg;
  workspaceRoot: string;
  agentId: string;
  onError: (message: string) => void;
}): Promise<void> {
  let payload: AgentNotesRpcRequest = {};
  try {
    payload = JSON.parse(notesRpcCodec.decode(args.msg.data)) as AgentNotesRpcRequest;
    if (typeof payload.agentId === "string" && payload.agentId.trim() && payload.agentId !== args.agentId) {
      throw new Error("agent id mismatch");
    }
    args.msg.respond(notesRpcCodec.encode(JSON.stringify(await executeNotesRpc(args.workspaceRoot, payload))));
  } catch (error) {
    const message = error instanceof Error ? error.message : "unknown error";
    args.msg.respond(notesRpcCodec.encode(JSON.stringify({
      ok: false,
      action: typeof payload.action === "string" ? payload.action : "",
      error: message,
    })));
    args.onError(`notes rpc failed action=${typeof payload.action === "string" ? payload.action : "unknown"} error=${message}`);
  }
}
