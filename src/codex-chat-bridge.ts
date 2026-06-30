import { randomUUID } from "node:crypto";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";

const DEFAULT_CHAT_BASE_URL = "https://api.z.ai/api/coding/paas/v4";
const DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1";
const CHAT_BRIDGE_ENV_KEY = "DOER_CODEX_CHAT_BRIDGE_API_KEY";
const ANTHROPIC_VERSION = "2023-06-01";

export interface CodexChatBridge {
  baseUrl: string;
  envKey: string;
  apiKey: string;
  stop: () => Promise<void>;
}

type ResponseUsage = {
  input_tokens: number;
  input_tokens_details?: {
    cached_tokens?: number;
  };
  output_tokens: number;
  output_tokens_details?: {
    reasoning_tokens?: number;
  };
  total_tokens: number;
};

type ResponseMetadata = {
  instructions: string | null;
  maxOutputTokens: number | null;
  metadata: unknown;
  parallelToolCalls: boolean | null;
  previousResponseId: string | null;
  temperature: number | null;
  text: unknown;
  toolChoice: unknown;
  tools: unknown[];
  topP: number | null;
  truncation: string | null;
  user: string | null;
};

type ChatMessageContent = string | Array<Record<string, unknown>> | null;
type ProviderAdapterKind = "chat_completions" | "anthropic_messages";

type ProviderAdapter = {
  kind: ProviderAdapterKind;
  defaultBaseUrl: string;
  upstreamAuthHeaders: (apiKey: string) => Record<string, string>;
};

const PROVIDER_ADAPTERS: Record<string, ProviderAdapter> = {
  anthropic: {
    kind: "anthropic_messages",
    defaultBaseUrl: DEFAULT_ANTHROPIC_BASE_URL,
    upstreamAuthHeaders: (apiKey) => ({
      "x-api-key": apiKey,
      "anthropic-version": ANTHROPIC_VERSION,
    }),
  },
  default: {
    kind: "chat_completions",
    defaultBaseUrl: DEFAULT_CHAT_BASE_URL,
    upstreamAuthHeaders: (apiKey) => ({
      authorization: `Bearer ${apiKey}`,
    }),
  },
};

function normalizeBaseUrl(value: string): string {
  return value.replace(/\/+$/, "");
}

function resolveProviderAdapter(providerId?: string | null): ProviderAdapter {
  return PROVIDER_ADAPTERS[providerId?.trim().toLowerCase() || ""] ?? PROVIDER_ADAPTERS.default;
}

function resolveTargetBaseUrl(args: { providerId?: string | null; targetBaseUrl?: string | null }): string {
  const providerId = args.providerId?.trim().toLowerCase();
  const baseUrl = args.targetBaseUrl?.trim();
  if (providerId === "zai" && (!baseUrl || !baseUrl.includes("/coding/paas/"))) {
    return DEFAULT_CHAT_BASE_URL;
  }
  const adapter = resolveProviderAdapter(providerId);
  return normalizeBaseUrl(baseUrl || adapter.defaultBaseUrl);
}

function stringValue(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function textValue(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function finiteNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function recordValue(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}

function readRequestJson(req: IncomingMessage): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
    req.on("end", () => {
      const raw = Buffer.concat(chunks).toString("utf8").trim();
      if (!raw) {
        resolve({});
        return;
      }
      try {
        const parsed = JSON.parse(raw);
        resolve(parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Record<string, unknown> : {});
      } catch (error) {
        reject(error);
      }
    });
    req.on("error", reject);
  });
}

function contentToText(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (!Array.isArray(value)) {
    return "";
  }
  return value
    .map((part) => {
      if (typeof part === "string") {
        return part;
      }
      if (!part || typeof part !== "object" || Array.isArray(part)) {
        return "";
      }
      const record = part as Record<string, unknown>;
      return textValue(record.text) ?? textValue(record.output_text) ?? "";
    })
    .filter(Boolean)
    .join("\n");
}

function parseJsonObject(value: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Record<string, unknown> : {};
  } catch {
    return {};
  }
}

function responsePartToChatPart(part: unknown): Record<string, unknown> | null {
  if (typeof part === "string") {
    return part ? { type: "text", text: part } : null;
  }
  const record = recordValue(part);
  if (!record) {
    return null;
  }
  const type = stringValue(record.type);
  if (type === "input_text" || type === "output_text" || type === "text") {
    const text = textValue(record.text) ?? textValue(record.output_text);
    return text ? { type: "text", text } : null;
  }
  if (type === "input_image" || type === "image_url") {
    const rawImageUrl = recordValue(record.image_url);
    const imageUrl = textValue(record.image_url) ?? textValue(rawImageUrl?.url) ?? textValue(record.url);
    if (!imageUrl) {
      return null;
    }
    const detail = stringValue(record.detail) ?? stringValue(rawImageUrl?.detail);
    return {
      type: "image_url",
      image_url: {
        url: imageUrl,
        ...(detail ? { detail } : {}),
      },
    };
  }
  if (type === "input_file" || type === "file") {
    const filename = stringValue(record.filename) ?? stringValue(record.name) ?? "attached file";
    const fileId = stringValue(record.file_id);
    const fileData = textValue(record.file_data);
    const suffix = fileId ? ` file_id=${fileId}` : "";
    return {
      type: "text",
      text: fileData ? `[attached file: ${filename}]\n${fileData}` : `[attached file: ${filename}${suffix}]`,
    };
  }
  return null;
}

function contentToChatContent(value: unknown): ChatMessageContent {
  if (typeof value === "string") {
    return value;
  }
  if (!Array.isArray(value)) {
    return "";
  }
  const parts = value.map(responsePartToChatPart).filter((part): part is Record<string, unknown> => Boolean(part));
  if (parts.length === 0) {
    return "";
  }
  if (parts.every((part) => part.type === "text")) {
    return parts.map((part) => textValue(part.text) ?? "").filter(Boolean).join("\n");
  }
  return parts;
}

function inputItemToMessages(item: unknown): Array<Record<string, unknown>> {
  if (!item || typeof item !== "object" || Array.isArray(item)) {
    return [];
  }
  const record = item as Record<string, unknown>;
  const type = stringValue(record.type);
  const role = stringValue(record.role);

  if (role === "user" || role === "assistant" || role === "system" || role === "developer") {
    return [{
      role: role === "developer" ? "system" : role,
      content: contentToChatContent(record.content),
    }];
  }

  if (type === "message") {
    const messageRole = stringValue(record.role) ?? "user";
    return [{
      role: messageRole === "developer" ? "system" : messageRole,
      content: contentToChatContent(record.content),
    }];
  }

  if (type === "function_call") {
    const name = stringValue(record.name);
    const callId = stringValue(record.call_id) ?? stringValue(record.id) ?? `call_${randomUUID().replace(/-/g, "")}`;
    if (!name) {
      return [];
    }
    const namespace = stringValue(record.namespace);
    return [{
      role: "assistant",
      content: null,
      tool_calls: [{
        id: callId,
        type: "function",
        function: {
          name: chatNameFromResponsesFunctionCall(namespace, name),
          arguments: typeof record.arguments === "string" ? record.arguments : "",
        },
      }],
    }];
  }

  if (type === "function_call_output") {
    const callId = stringValue(record.call_id);
    if (!callId) {
      return [];
    }
    return [{
      role: "tool",
      tool_call_id: callId,
      content: typeof record.output === "string" ? record.output : JSON.stringify(record.output ?? ""),
    }];
  }

  return [];
}

function inputToMessages(input: unknown): Array<Record<string, unknown>> {
  if (typeof input === "string") {
    return [{ role: "user", content: input }];
  }
  if (!Array.isArray(input)) {
    return [];
  }
  return input.flatMap(inputItemToMessages);
}

function schemaValue(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : { type: "object", properties: {} };
}

function chatFunctionTool(args: {
  name: string;
  description: string;
  parameters: unknown;
}): Record<string, unknown> | null {
  if (!/^[a-zA-Z0-9_-]+$/.test(args.name)) {
    return null;
  }
  return {
    type: "function",
    function: {
      name: args.name,
      description: args.description,
      parameters: schemaValue(args.parameters),
    },
  };
}

function namespaceToolChatName(namespace: string | null, name: string): string {
  if (!namespace) {
    return name;
  }
  const prefix = namespace.startsWith("mcp__") ? namespace : `mcp__${namespace}`;
  return `${prefix.endsWith("__") ? prefix : `${prefix}__`}${name}`;
}

function chatNameFromResponsesFunctionCall(namespace: string | null, name: string): string {
  if (!namespace) {
    return name;
  }
  return `${namespace.endsWith("__") ? namespace : `${namespace}__`}${name}`;
}

function responsesFunctionCallNameFromChatName(name: string): { name: string; namespace?: string } {
  if (!name.startsWith("mcp__")) {
    return { name };
  }
  const separatorIndex = name.lastIndexOf("__");
  if (separatorIndex <= "mcp__".length || separatorIndex >= name.length - 2) {
    return { name };
  }
  return {
    namespace: name.slice(0, separatorIndex),
    name: name.slice(separatorIndex + 2),
  };
}

function responsesToolToChatTools(tool: unknown, onLog?: (message: string) => void): Record<string, unknown>[] {
  if (!tool || typeof tool !== "object" || Array.isArray(tool)) {
    return [];
  }
  const record = tool as Record<string, unknown>;
  const type = stringValue(record.type);
  if (type === "namespace") {
    const namespace = stringValue(record.name);
    const namespaceDescription = stringValue(record.description);
    const namespaceTools = Array.isArray(record.tools)
      ? record.tools
      : Array.isArray(record.functions)
        ? record.functions
        : [];
    const tools = namespaceTools
      .map((namespaceTool) => {
        const toolRecord = recordValue(namespaceTool);
        const name = stringValue(toolRecord?.name);
        if (!name) {
          return null;
        }
        const description = [
          namespace ? `Namespace: ${namespaceToolChatName(namespace, "").replace(/__$/, "__")}. Original tool: ${name}.` : "",
          stringValue(toolRecord?.description) ?? namespaceDescription ?? "",
        ].filter(Boolean).join(" ");
        return chatFunctionTool({
          name: namespaceToolChatName(namespace, name),
          description,
          parameters: toolRecord?.parameters ?? toolRecord?.input_schema ?? toolRecord?.schema,
        });
      })
      .filter((item): item is Record<string, unknown> => Boolean(item));
    if (tools.length === 0) {
      onLog?.(`Codex chat bridge dropped unsupported Responses tool type=namespace`);
    }
    return tools;
  }
  if (type !== "function") {
    if (type) {
      onLog?.(`Codex chat bridge dropped unsupported Responses tool type=${type}`);
    }
    return [];
  }
  const name = stringValue(record.name);
  if (!name) {
    return [];
  }
  const chatTool = chatFunctionTool({
    name,
    description: stringValue(record.description) ?? "",
    parameters: record.parameters,
  });
  return chatTool ? [chatTool] : [];
}

function chatToolChoiceFromResponses(value: unknown): unknown {
  const text = stringValue(value);
  if (text === "auto" || text === "none" || text === "required") {
    return text;
  }
  const record = recordValue(value);
  if (!record) {
    return undefined;
  }
  const type = stringValue(record.type);
  const name = stringValue(record.name);
  if (type === "function" && name) {
    return { type: "function", function: { name } };
  }
  return undefined;
}

function chatResponseFormatFromResponsesText(value: unknown): unknown {
  const text = recordValue(value);
  const format = recordValue(text?.format);
  const type = stringValue(format?.type);
  if (type === "json_object") {
    return { type: "json_object" };
  }
  if (type !== "json_schema") {
    return undefined;
  }
  const schema = recordValue(format?.schema) ?? recordValue(format?.json_schema);
  if (!schema) {
    return undefined;
  }
  const name = stringValue(format?.name) ?? "response";
  return {
    type: "json_schema",
    json_schema: {
      name,
      schema,
      ...(typeof format?.strict === "boolean" ? { strict: format.strict } : {}),
    },
  };
}

function responseMetadataFromBody(body: Record<string, unknown>): ResponseMetadata {
  const tools = Array.isArray(body.tools) ? body.tools : [];
  return {
    instructions: stringValue(body.instructions),
    maxOutputTokens: finiteNumber(body.max_output_tokens),
    metadata: recordValue(body.metadata) ?? {},
    parallelToolCalls: typeof body.parallel_tool_calls === "boolean" ? body.parallel_tool_calls : null,
    previousResponseId: stringValue(body.previous_response_id),
    temperature: finiteNumber(body.temperature),
    text: recordValue(body.text) ?? { format: { type: "text" } },
    toolChoice: body.tool_choice ?? "auto",
    tools,
    topP: finiteNumber(body.top_p),
    truncation: stringValue(body.truncation),
    user: stringValue(body.user),
  };
}

function buildChatRequest(
  body: Record<string, unknown>,
  stream: boolean,
  onLog?: (message: string) => void,
): Record<string, unknown> {
  const messages = inputToMessages(body.input);
  const instructions = stringValue(body.instructions);
  if (instructions) {
    messages.unshift({ role: "system", content: instructions });
  }
  const tools = Array.isArray(body.tools) ? body.tools.flatMap((tool) => responsesToolToChatTools(tool, onLog)) : [];
  const toolChoice = chatToolChoiceFromResponses(body.tool_choice);
  const responseFormat = chatResponseFormatFromResponsesText(body.text);
  return {
    model: stringValue(body.model) ?? "glm-5.2",
    messages,
    stream,
    ...(stream ? { stream_options: { include_usage: true } } : {}),
    ...(typeof body.temperature === "number" ? { temperature: body.temperature } : {}),
    ...(typeof body.top_p === "number" ? { top_p: body.top_p } : {}),
    ...(typeof body.max_output_tokens === "number" ? { max_tokens: body.max_output_tokens } : {}),
    ...(typeof body.presence_penalty === "number" ? { presence_penalty: body.presence_penalty } : {}),
    ...(typeof body.frequency_penalty === "number" ? { frequency_penalty: body.frequency_penalty } : {}),
    ...(typeof body.seed === "number" ? { seed: body.seed } : {}),
    ...(typeof body.stop === "string" || Array.isArray(body.stop) ? { stop: body.stop } : {}),
    ...(typeof body.parallel_tool_calls === "boolean" ? { parallel_tool_calls: body.parallel_tool_calls } : {}),
    ...(typeof body.user === "string" ? { user: body.user } : {}),
    ...(responseFormat ? { response_format: responseFormat } : {}),
    ...(toolChoice ? { tool_choice: toolChoice } : {}),
    ...(tools.length > 0 ? { tools } : {}),
  };
}

function anthropicContentPartFromChatPart(part: Record<string, unknown>): Record<string, unknown> | null {
  const type = stringValue(part.type);
  if (type === "text") {
    const text = textValue(part.text);
    return text ? { type: "text", text } : null;
  }
  if (type === "image_url") {
    const rawImageUrl = recordValue(part.image_url);
    const imageUrl = textValue(part.image_url) ?? textValue(rawImageUrl?.url);
    const dataUrlMatch = imageUrl?.match(/^data:([^;,]+);base64,(.+)$/);
    if (!dataUrlMatch) {
      return null;
    }
    return {
      type: "image",
      source: {
        type: "base64",
        media_type: dataUrlMatch[1],
        data: dataUrlMatch[2],
      },
    };
  }
  return null;
}

function anthropicContentFromChatContent(content: unknown): string | Array<Record<string, unknown>> {
  if (typeof content === "string") {
    return content;
  }
  if (!Array.isArray(content)) {
    return "";
  }
  const parts = content
    .map((part) => recordValue(part))
    .filter((part): part is Record<string, unknown> => Boolean(part))
    .map(anthropicContentPartFromChatPart)
    .filter((part): part is Record<string, unknown> => Boolean(part));
  return parts.length > 0 ? parts : "";
}

function anthropicMessagesFromChatMessages(messages: Array<Record<string, unknown>>): {
  system: string | null;
  messages: Array<Record<string, unknown>>;
} {
  const systemParts: string[] = [];
  const anthropicMessages: Array<Record<string, unknown>> = [];
  for (const message of messages) {
    const role = stringValue(message.role);
    if (role === "system" || role === "developer") {
      const text = contentToText(message.content);
      if (text) {
        systemParts.push(text);
      }
      continue;
    }
    if (role === "tool") {
      const toolUseId = stringValue(message.tool_call_id);
      if (!toolUseId) {
        continue;
      }
      anthropicMessages.push({
        role: "user",
        content: [{
          type: "tool_result",
          tool_use_id: toolUseId,
          content: contentToText(message.content),
        }],
      });
      continue;
    }
    if (role === "assistant") {
      const toolCalls = Array.isArray(message.tool_calls) ? message.tool_calls : [];
      if (toolCalls.length > 0) {
        const content: Record<string, unknown>[] = toolCalls
          .map((toolCall) => {
            const toolCallRecord = recordValue(toolCall);
            const fn = recordValue(toolCallRecord?.function);
            const name = stringValue(fn?.name);
            if (!toolCallRecord || !fn || !name) {
              return null;
            }
            return {
              type: "tool_use",
              id: stringValue(toolCallRecord.id) ?? `call_${randomUUID().replace(/-/g, "")}`,
              name,
              input: parseJsonObject(typeof fn.arguments === "string" ? fn.arguments : ""),
            };
          })
          .filter((item): item is NonNullable<typeof item> => item !== null);
        if (content.length > 0) {
          anthropicMessages.push({ role: "assistant", content });
        }
        continue;
      }
      anthropicMessages.push({ role: "assistant", content: anthropicContentFromChatContent(message.content) });
      continue;
    }
    anthropicMessages.push({ role: "user", content: anthropicContentFromChatContent(message.content) });
  }
  return {
    system: systemParts.length > 0 ? systemParts.join("\n\n") : null,
    messages: anthropicMessages,
  };
}

function anthropicToolsFromChatTools(tools: unknown): Record<string, unknown>[] {
  if (!Array.isArray(tools)) {
    return [];
  }
  const converted = tools
    .map((tool) => {
      const record = recordValue(tool);
      const fn = recordValue(record?.function);
      const name = stringValue(fn?.name);
      if (!fn || !name) {
        return null;
      }
      return {
        name,
        description: stringValue(fn?.description) ?? "",
        input_schema: schemaValue(fn?.parameters),
      };
    });
  return converted.filter((tool): tool is NonNullable<typeof tool> => tool !== null);
}

function anthropicToolChoiceFromChatToolChoice(value: unknown): unknown {
  const text = stringValue(value);
  if (text === "auto") {
    return { type: "auto" };
  }
  if (text === "required") {
    return { type: "any" };
  }
  if (text === "none") {
    return undefined;
  }
  const record = recordValue(value);
  const fn = recordValue(record?.function);
  const name = stringValue(fn?.name);
  return name ? { type: "tool", name } : undefined;
}

function buildAnthropicRequest(
  body: Record<string, unknown>,
  stream: boolean,
  onLog?: (message: string) => void,
): Record<string, unknown> {
  const chatRequest = buildChatRequest(body, stream, onLog);
  const chatMessages = Array.isArray(chatRequest.messages) ? chatRequest.messages as Array<Record<string, unknown>> : [];
  const converted = anthropicMessagesFromChatMessages(chatMessages);
  const tools = anthropicToolsFromChatTools(chatRequest.tools);
  const toolChoice = anthropicToolChoiceFromChatToolChoice(chatRequest.tool_choice);
  return {
    model: stringValue(chatRequest.model) ?? "claude-sonnet-4-6",
    messages: converted.messages,
    stream,
    max_tokens: finiteNumber(chatRequest.max_tokens) ?? 4096,
    ...(converted.system ? { system: converted.system } : {}),
    ...(typeof chatRequest.temperature === "number" ? { temperature: chatRequest.temperature } : {}),
    ...(typeof chatRequest.top_p === "number" ? { top_p: chatRequest.top_p } : {}),
    ...(typeof chatRequest.stop === "string" || Array.isArray(chatRequest.stop) ? { stop_sequences: chatRequest.stop } : {}),
    ...(tools.length > 0 ? { tools } : {}),
    ...(toolChoice ? { tool_choice: toolChoice } : {}),
  };
}

function upstreamErrorMessage(value: unknown): string {
  const body = recordValue(value);
  const error = recordValue(body?.error);
  return stringValue(error?.message)
    ?? stringValue(body?.message)
    ?? (typeof value === "string" && value ? value : "Chat Completions upstream failed");
}

function streamDeltaText(delta: Record<string, unknown>): string {
  const content = delta.content;
  if (typeof content === "string") {
    return content;
  }
  if (Array.isArray(content)) {
    return contentToText(content);
  }
  return "";
}

function writeSse(res: ServerResponse, event: string, data: unknown): void {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

function chatUsageToResponseUsage(value: unknown): ResponseUsage | null {
  const usage = recordValue(value);
  if (!usage) {
    return null;
  }
  const promptTokens = finiteNumber(usage.prompt_tokens) ?? 0;
  const completionTokens = finiteNumber(usage.completion_tokens) ?? 0;
  const totalTokens = finiteNumber(usage.total_tokens) ?? promptTokens + completionTokens;
  const promptDetails = recordValue(usage.prompt_tokens_details);
  const completionDetails = recordValue(usage.completion_tokens_details);
  const cachedTokens = finiteNumber(promptDetails?.cached_tokens);
  const reasoningTokens = finiteNumber(completionDetails?.reasoning_tokens);
  return {
    input_tokens: promptTokens,
    ...(cachedTokens !== null ? { input_tokens_details: { cached_tokens: cachedTokens } } : {}),
    output_tokens: completionTokens,
    ...(reasoningTokens !== null ? { output_tokens_details: { reasoning_tokens: reasoningTokens } } : {}),
    total_tokens: totalTokens,
  };
}

function responseBase(
  responseId: string,
  model: string,
  output: unknown[] = [],
  metadata: ResponseMetadata = responseMetadataFromBody({}),
  usage: ResponseUsage | null = null,
  status = "completed",
  error: unknown = null,
): Record<string, unknown> {
  return {
    id: responseId,
    object: "response",
    created_at: Math.floor(Date.now() / 1000),
    status,
    error,
    incomplete_details: null,
    instructions: metadata.instructions,
    max_output_tokens: metadata.maxOutputTokens,
    model,
    output,
    parallel_tool_calls: metadata.parallelToolCalls ?? true,
    previous_response_id: metadata.previousResponseId,
    reasoning: null,
    store: false,
    temperature: metadata.temperature,
    text: metadata.text,
    tool_choice: metadata.toolChoice,
    tools: metadata.tools,
    top_p: metadata.topP,
    truncation: metadata.truncation ?? "disabled",
    usage,
    user: metadata.user,
    metadata: metadata.metadata,
  };
}

function chatMessageToResponseOutput(message: Record<string, unknown>): unknown[] {
  const toolCalls = Array.isArray(message.tool_calls) ? message.tool_calls : [];
  if (toolCalls.length > 0) {
    return toolCalls
      .map((toolCall) => {
        if (!toolCall || typeof toolCall !== "object" || Array.isArray(toolCall)) {
          return null;
        }
        const record = toolCall as Record<string, unknown>;
        const fn = record.function && typeof record.function === "object" && !Array.isArray(record.function)
          ? record.function as Record<string, unknown>
          : {};
        const name = stringValue(fn.name);
        if (!name) {
          return null;
        }
        const responseName = responsesFunctionCallNameFromChatName(name);
        return {
          type: "function_call",
          id: stringValue(record.id) ?? `fc_${randomUUID().replace(/-/g, "")}`,
          call_id: stringValue(record.id) ?? `call_${randomUUID().replace(/-/g, "")}`,
          ...responseName,
          arguments: typeof fn.arguments === "string" ? fn.arguments : "",
          status: "completed",
        };
      })
      .filter(Boolean);
  }
  return [{
    id: `msg_${randomUUID().replace(/-/g, "")}`,
    type: "message",
    status: "completed",
    role: "assistant",
    content: [{ type: "output_text", text: contentToText(message.content), annotations: [] }],
  }];
}

function anthropicUsageToResponseUsage(value: unknown): ResponseUsage | null {
  const usage = recordValue(value);
  if (!usage) {
    return null;
  }
  const inputTokens = finiteNumber(usage.input_tokens) ?? 0;
  const outputTokens = finiteNumber(usage.output_tokens) ?? 0;
  return {
    input_tokens: inputTokens,
    output_tokens: outputTokens,
    total_tokens: inputTokens + outputTokens,
  };
}

function anthropicContentToResponseOutput(content: unknown): unknown[] {
  const blocks = Array.isArray(content) ? content : [];
  const output: unknown[] = [];
  const text = blocks
    .map((block) => {
      const record = recordValue(block);
      return stringValue(record?.type) === "text" ? textValue(record?.text) ?? "" : "";
    })
    .filter(Boolean)
    .join("");
  if (text) {
    output.push({
      id: `msg_${randomUUID().replace(/-/g, "")}`,
      type: "message",
      status: "completed",
      role: "assistant",
      content: [{ type: "output_text", text, annotations: [] }],
    });
  }
  for (const block of blocks) {
    const record = recordValue(block);
    if (stringValue(record?.type) !== "tool_use") {
      continue;
    }
    const name = stringValue(record?.name);
    if (!name) {
      continue;
    }
    const responseName = responsesFunctionCallNameFromChatName(name);
    output.push({
      type: "function_call",
      id: stringValue(record?.id) ?? `fc_${randomUUID().replace(/-/g, "")}`,
      call_id: stringValue(record?.id) ?? `call_${randomUUID().replace(/-/g, "")}`,
      ...responseName,
      arguments: JSON.stringify(record?.input ?? {}),
      status: "completed",
    });
  }
  return output;
}

async function forwardAnthropicNonStreaming(args: {
  body: Record<string, unknown>;
  res: ServerResponse;
  signal: AbortSignal;
  targetBaseUrl: string;
  upstreamHeaders: Record<string, string>;
  onLog?: (message: string) => void;
}): Promise<void> {
  const anthropicRequest = buildAnthropicRequest(args.body, false, args.onLog);
  const responseMetadata = responseMetadataFromBody(args.body);
  const upstream = await fetch(`${normalizeBaseUrl(args.targetBaseUrl)}/messages`, {
    method: "POST",
    headers: {
      ...args.upstreamHeaders,
      "content-type": "application/json",
    },
    signal: args.signal,
    body: JSON.stringify(anthropicRequest),
  });
  const upstreamBody = await upstream.json().catch(async () => ({ error: { message: await upstream.text() } })) as Record<string, unknown>;
  if (!upstream.ok) {
    args.res.writeHead(upstream.status, { "content-type": "application/json" });
    args.res.end(JSON.stringify({
      error: {
        message: upstreamErrorMessage(upstreamBody),
        type: "server_error",
        status: upstream.status,
        upstream: recordValue(upstreamBody.error) ?? upstreamBody,
      },
    }));
    return;
  }
  const responseId = `resp_${randomUUID().replace(/-/g, "")}`;
  const usage = anthropicUsageToResponseUsage(upstreamBody.usage);
  args.res.writeHead(200, { "content-type": "application/json" });
  args.res.end(JSON.stringify(responseBase(
    responseId,
    stringValue(anthropicRequest.model) ?? "claude-sonnet-4-6",
    anthropicContentToResponseOutput(upstreamBody.content),
    responseMetadata,
    usage,
  )));
}

async function forwardNonStreaming(args: {
  body: Record<string, unknown>;
  res: ServerResponse;
  signal: AbortSignal;
  targetBaseUrl: string;
  upstreamAuthorization: string;
  onLog?: (message: string) => void;
}): Promise<void> {
  const chatRequest = buildChatRequest(args.body, false, args.onLog);
  const responseMetadata = responseMetadataFromBody(args.body);
  const upstream = await fetch(`${normalizeBaseUrl(args.targetBaseUrl)}/chat/completions`, {
    method: "POST",
    headers: {
      authorization: args.upstreamAuthorization,
      "content-type": "application/json",
    },
    signal: args.signal,
    body: JSON.stringify(chatRequest),
  });
  const upstreamBody = await upstream.json().catch(async () => ({ error: { message: await upstream.text() } })) as Record<string, unknown>;
  if (!upstream.ok) {
    args.res.writeHead(upstream.status, { "content-type": "application/json" });
    args.res.end(JSON.stringify({
      error: {
        message: upstreamErrorMessage(upstreamBody),
        type: "server_error",
        status: upstream.status,
        upstream: recordValue(upstreamBody.error) ?? upstreamBody,
      },
    }));
    return;
  }
  const choices = Array.isArray(upstreamBody.choices) ? upstreamBody.choices : [];
  const firstChoice = choices[0] && typeof choices[0] === "object" ? choices[0] as Record<string, unknown> : {};
  const message = firstChoice.message && typeof firstChoice.message === "object" ? firstChoice.message as Record<string, unknown> : {};
  if (!message.content && typeof message.reasoning_content === "string") {
    message.content = message.reasoning_content;
  }
  const responseId = `resp_${randomUUID().replace(/-/g, "")}`;
  const usage = chatUsageToResponseUsage(upstreamBody.usage);
  args.res.writeHead(200, { "content-type": "application/json" });
  args.res.end(JSON.stringify(responseBase(responseId, stringValue(chatRequest.model) ?? "glm-5.2", chatMessageToResponseOutput(message), responseMetadata, usage)));
}

type StreamingToolCall = {
  id: string;
  itemId: string;
  name: string;
  arguments: string;
  outputIndex: number;
  started: boolean;
};

function parseStreamingToolCalls(delta: Record<string, unknown>): StreamingToolCall[] {
  const toolCalls = Array.isArray(delta.tool_calls) ? delta.tool_calls : [];
  return toolCalls
    .map((toolCall) => {
      if (!toolCall || typeof toolCall !== "object" || Array.isArray(toolCall)) {
        return null;
      }
      const record = toolCall as Record<string, unknown>;
      const fn = record.function && typeof record.function === "object" && !Array.isArray(record.function)
        ? record.function as Record<string, unknown>
        : {};
      const id = stringValue(record.id) ?? `call_${randomUUID().replace(/-/g, "")}`;
      return {
        id,
        itemId: `fc_${id.replace(/[^a-zA-Z0-9_-]/g, "")}`,
        name: stringValue(fn.name) ?? "",
        arguments: typeof fn.arguments === "string" ? fn.arguments : "",
        outputIndex: typeof record.index === "number" ? record.index : 0,
        started: false,
      };
    })
    .filter((item): item is StreamingToolCall => Boolean(item));
}

function writeFailedSse(args: {
  errorMessage: string;
  model: string;
  res: ServerResponse;
  responseId: string;
  responseMetadata: ResponseMetadata;
}): void {
  writeSse(args.res, "response.failed", {
    type: "response.failed",
    response: responseBase(args.responseId, args.model, [], args.responseMetadata, null, "failed", {
      message: args.errorMessage,
      type: "server_error",
    }),
  });
}

async function forwardStreaming(args: {
  body: Record<string, unknown>;
  res: ServerResponse;
  signal: AbortSignal;
  targetBaseUrl: string;
  upstreamAuthorization: string;
  onLog?: (message: string) => void;
}): Promise<void> {
  const chatRequest = buildChatRequest(args.body, true, args.onLog);
  const model = stringValue(chatRequest.model) ?? "glm-5.2";
  const responseMetadata = responseMetadataFromBody(args.body);
  const responseId = `resp_${randomUUID().replace(/-/g, "")}`;
  const messageId = `msg_${randomUUID().replace(/-/g, "")}`;
  const outputItems: unknown[] = [];
  let text = "";
  let reasoningText = "";
  let textStarted = false;
  let streamUsage: ResponseUsage | null = null;
  const tools = new Map<string, StreamingToolCall>();

  args.res.writeHead(200, {
    "content-type": "text/event-stream; charset=utf-8",
    "cache-control": "no-cache",
    connection: "keep-alive",
  });
  writeSse(args.res, "response.created", { type: "response.created", response: responseBase(responseId, model, [], responseMetadata) });
  writeSse(args.res, "response.in_progress", { type: "response.in_progress", response: responseBase(responseId, model, [], responseMetadata, null, "in_progress") });

  const upstream = await fetch(`${normalizeBaseUrl(args.targetBaseUrl)}/chat/completions`, {
    method: "POST",
    headers: {
      authorization: args.upstreamAuthorization,
      "content-type": "application/json",
    },
    signal: args.signal,
    body: JSON.stringify(chatRequest),
  });
  if (!upstream.ok || !upstream.body) {
    const errorText = await upstream.text();
    writeFailedSse({
      errorMessage: `Chat Completions upstream failed: ${upstream.status} ${errorText}`,
      model,
      res: args.res,
      responseId,
      responseMetadata,
    });
    args.res.end();
    return;
  }

  const reader = upstream.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let upstreamDone = false;
  let streamFailed = false;
  const processDataLine = (data: string): boolean => {
    if (!data) {
      return false;
    }
    if (data === "[DONE]") {
      upstreamDone = true;
      return true;
    }
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(data) as Record<string, unknown>;
    } catch (error) {
      writeFailedSse({
        errorMessage: `Failed to parse Chat Completions stream chunk: ${error instanceof Error ? error.message : String(error)}`,
        model,
        res: args.res,
        responseId,
        responseMetadata,
      });
      args.res.end();
      upstreamDone = true;
      streamFailed = true;
      return true;
    }
    const usage = chatUsageToResponseUsage(parsed.usage);
    if (usage) {
      streamUsage = usage;
    }
    const choices = Array.isArray(parsed.choices) ? parsed.choices : [];
    const firstChoice = choices[0] && typeof choices[0] === "object" ? choices[0] as Record<string, unknown> : {};
    const delta = firstChoice.delta && typeof firstChoice.delta === "object" ? firstChoice.delta as Record<string, unknown> : {};
    const message = firstChoice.message && typeof firstChoice.message === "object" ? firstChoice.message as Record<string, unknown> : {};
    const effectiveDelta = Object.keys(delta).length > 0 ? delta : message;
    const reasoningChunk = textValue(effectiveDelta.reasoning_content) ?? "";
    if (reasoningChunk) {
      reasoningText += reasoningChunk;
    }
    const chunk = streamDeltaText(effectiveDelta);
    if (chunk) {
      if (!textStarted) {
        textStarted = true;
        writeSse(args.res, "response.output_item.added", {
          type: "response.output_item.added",
          output_index: 0,
          item: { id: messageId, type: "message", status: "in_progress", role: "assistant", content: [] },
        });
        writeSse(args.res, "response.content_part.added", {
          type: "response.content_part.added",
          item_id: messageId,
          output_index: 0,
          content_index: 0,
          part: { type: "output_text", text: "", annotations: [] },
        });
      }
      text += chunk;
      writeSse(args.res, "response.output_text.delta", {
        type: "response.output_text.delta",
        item_id: messageId,
        output_index: 0,
        content_index: 0,
        delta: chunk,
      });
    }
    for (const partial of parseStreamingToolCalls(effectiveDelta)) {
      const existing = tools.get(partial.id) ?? { ...partial, arguments: "" };
      existing.name = partial.name || existing.name;
      existing.arguments += partial.arguments;
      if (!existing.started && existing.name) {
        existing.started = true;
        const outputIndex = tools.size + (textStarted ? 1 : 0);
        existing.outputIndex = outputIndex;
        const responseName = responsesFunctionCallNameFromChatName(existing.name);
        writeSse(args.res, "response.output_item.added", {
          type: "response.output_item.added",
          output_index: outputIndex,
          item: {
            id: existing.itemId,
            type: "function_call",
            status: "in_progress",
            call_id: existing.id,
            ...responseName,
            arguments: "",
          },
        });
      }
      if (partial.arguments) {
        writeSse(args.res, "response.function_call_arguments.delta", {
          type: "response.function_call_arguments.delta",
          item_id: existing.itemId,
          output_index: existing.outputIndex,
          delta: partial.arguments,
        });
      }
      tools.set(partial.id, existing);
    }
    return false;
  };
  try {
    while (!upstreamDone) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split(/\r?\n/);
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed.startsWith("data:")) {
          continue;
        }
        const data = trimmed.slice(5).trim();
        if (processDataLine(data)) {
          await reader.cancel().catch(() => undefined);
          break;
        }
      }
    }
    const tail = buffer.trim();
    if (!upstreamDone && tail.startsWith("data:")) {
      processDataLine(tail.slice(5).trim());
    }
  } catch (error) {
    if (args.signal.aborted) {
      return;
    }
    writeFailedSse({
      errorMessage: `Chat Completions stream failed: ${error instanceof Error ? error.message : String(error)}`,
      model,
      res: args.res,
      responseId,
      responseMetadata,
    });
    args.res.end();
    return;
  }
  if (streamFailed) {
    return;
  }

  if (!textStarted && reasoningText) {
    textStarted = true;
    text = reasoningText;
    writeSse(args.res, "response.output_item.added", {
      type: "response.output_item.added",
      output_index: 0,
      item: { id: messageId, type: "message", status: "in_progress", role: "assistant", content: [] },
    });
    writeSse(args.res, "response.content_part.added", {
      type: "response.content_part.added",
      item_id: messageId,
      output_index: 0,
      content_index: 0,
      part: { type: "output_text", text: "", annotations: [] },
    });
    writeSse(args.res, "response.output_text.delta", {
      type: "response.output_text.delta",
      item_id: messageId,
      output_index: 0,
      content_index: 0,
      delta: text,
    });
  }

  if (textStarted) {
    writeSse(args.res, "response.output_text.done", {
      type: "response.output_text.done",
      item_id: messageId,
      output_index: 0,
      content_index: 0,
      text,
    });
    writeSse(args.res, "response.content_part.done", {
      type: "response.content_part.done",
      item_id: messageId,
      output_index: 0,
      content_index: 0,
      part: { type: "output_text", text, annotations: [] },
    });
    const item = {
      id: messageId,
      type: "message",
      status: "completed",
      role: "assistant",
      content: [{ type: "output_text", text, annotations: [] }],
    };
    outputItems.push(item);
    writeSse(args.res, "response.output_item.done", { type: "response.output_item.done", output_index: 0, item });
  }

  for (const tool of tools.values()) {
    const responseName = responsesFunctionCallNameFromChatName(tool.name);
    const item = {
      id: tool.itemId,
      type: "function_call",
      status: "completed",
      call_id: tool.id,
      ...responseName,
      arguments: tool.arguments,
    };
    outputItems.push(item);
    writeSse(args.res, "response.function_call_arguments.done", {
      type: "response.function_call_arguments.done",
      item_id: tool.itemId,
      output_index: tool.outputIndex,
      arguments: tool.arguments,
    });
    writeSse(args.res, "response.output_item.done", {
      type: "response.output_item.done",
      output_index: tool.outputIndex,
      item,
    });
  }

  writeSse(args.res, "response.completed", { type: "response.completed", response: responseBase(responseId, model, outputItems, responseMetadata, streamUsage) });
  args.res.end();
}

type AnthropicStreamingBlock = {
  id: string;
  outputIndex: number;
  type: "text" | "tool_use";
  text: string;
  toolId: string;
  toolName: string;
  toolInputJson: string;
  started: boolean;
};

async function forwardAnthropicStreaming(args: {
  body: Record<string, unknown>;
  res: ServerResponse;
  signal: AbortSignal;
  targetBaseUrl: string;
  upstreamHeaders: Record<string, string>;
  onLog?: (message: string) => void;
}): Promise<void> {
  const anthropicRequest = buildAnthropicRequest(args.body, true, args.onLog);
  const model = stringValue(anthropicRequest.model) ?? "claude-sonnet-4-6";
  const responseMetadata = responseMetadataFromBody(args.body);
  const responseId = `resp_${randomUUID().replace(/-/g, "")}`;
  const outputItems: unknown[] = [];
  const blocks = new Map<number, AnthropicStreamingBlock>();
  let streamUsage: ResponseUsage | null = null;

  args.res.writeHead(200, {
    "content-type": "text/event-stream; charset=utf-8",
    "cache-control": "no-cache",
    connection: "keep-alive",
  });
  writeSse(args.res, "response.created", { type: "response.created", response: responseBase(responseId, model, [], responseMetadata) });
  writeSse(args.res, "response.in_progress", { type: "response.in_progress", response: responseBase(responseId, model, [], responseMetadata, null, "in_progress") });

  const upstream = await fetch(`${normalizeBaseUrl(args.targetBaseUrl)}/messages`, {
    method: "POST",
    headers: {
      ...args.upstreamHeaders,
      "content-type": "application/json",
    },
    signal: args.signal,
    body: JSON.stringify(anthropicRequest),
  });
  if (!upstream.ok || !upstream.body) {
    const errorText = await upstream.text();
    writeFailedSse({
      errorMessage: `Anthropic Messages upstream failed: ${upstream.status} ${errorText}`,
      model,
      res: args.res,
      responseId,
      responseMetadata,
    });
    args.res.end();
    return;
  }

  const reader = upstream.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let upstreamDone = false;
  let streamFailed = false;
  let currentEvent = "";

  const processAnthropicEvent = (event: string, data: string): boolean => {
    if (!data) {
      return false;
    }
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(data) as Record<string, unknown>;
    } catch (error) {
      writeFailedSse({
        errorMessage: `Failed to parse Anthropic stream chunk: ${error instanceof Error ? error.message : String(error)}`,
        model,
        res: args.res,
        responseId,
        responseMetadata,
      });
      args.res.end();
      streamFailed = true;
      upstreamDone = true;
      return true;
    }
    const type = stringValue(parsed.type) ?? event;
    if (type === "error") {
      writeFailedSse({
        errorMessage: upstreamErrorMessage(parsed),
        model,
        res: args.res,
        responseId,
        responseMetadata,
      });
      args.res.end();
      streamFailed = true;
      upstreamDone = true;
      return true;
    }
    if (type === "message_start") {
      const usage = anthropicUsageToResponseUsage(recordValue(parsed.message)?.usage);
      if (usage) {
        streamUsage = usage;
      }
      return false;
    }
    if (type === "message_delta") {
      const usage = anthropicUsageToResponseUsage(parsed.usage);
      if (usage) {
        streamUsage = {
          input_tokens: streamUsage?.input_tokens ?? usage.input_tokens,
          output_tokens: usage.output_tokens,
          total_tokens: (streamUsage?.input_tokens ?? usage.input_tokens) + usage.output_tokens,
        };
      }
      return false;
    }
    if (type === "content_block_start") {
      const index = finiteNumber(parsed.index) ?? blocks.size;
      const block = recordValue(parsed.content_block);
      const blockType = stringValue(block?.type);
      if (blockType === "text") {
        const itemId = `msg_${randomUUID().replace(/-/g, "")}`;
        blocks.set(index, {
          id: itemId,
          outputIndex: index,
          type: "text",
          text: "",
          toolId: "",
          toolName: "",
          toolInputJson: "",
          started: true,
        });
        writeSse(args.res, "response.output_item.added", {
          type: "response.output_item.added",
          output_index: index,
          item: { id: itemId, type: "message", status: "in_progress", role: "assistant", content: [] },
        });
        writeSse(args.res, "response.content_part.added", {
          type: "response.content_part.added",
          item_id: itemId,
          output_index: index,
          content_index: 0,
          part: { type: "output_text", text: "", annotations: [] },
        });
      } else if (blockType === "tool_use") {
        const toolId = stringValue(block?.id) ?? `call_${randomUUID().replace(/-/g, "")}`;
        const toolName = stringValue(block?.name) ?? "";
        const responseName = responsesFunctionCallNameFromChatName(toolName);
        const itemId = `fc_${toolId.replace(/[^a-zA-Z0-9_-]/g, "")}`;
        blocks.set(index, {
          id: itemId,
          outputIndex: index,
          type: "tool_use",
          text: "",
          toolId,
          toolName,
          toolInputJson: "",
          started: true,
        });
        writeSse(args.res, "response.output_item.added", {
          type: "response.output_item.added",
          output_index: index,
          item: {
            id: itemId,
            type: "function_call",
            status: "in_progress",
            call_id: toolId,
            ...responseName,
            arguments: "",
          },
        });
      }
      return false;
    }
    if (type === "content_block_delta") {
      const index = finiteNumber(parsed.index) ?? 0;
      const block = blocks.get(index);
      const delta = recordValue(parsed.delta);
      if (!block || !delta) {
        return false;
      }
      const deltaType = stringValue(delta.type);
      if (block.type === "text" && deltaType === "text_delta") {
        const chunk = textValue(delta.text) ?? "";
        if (!chunk) {
          return false;
        }
        block.text += chunk;
        writeSse(args.res, "response.output_text.delta", {
          type: "response.output_text.delta",
          item_id: block.id,
          output_index: block.outputIndex,
          content_index: 0,
          delta: chunk,
        });
      } else if (block.type === "tool_use" && deltaType === "input_json_delta") {
        const chunk = textValue(delta.partial_json) ?? "";
        if (!chunk) {
          return false;
        }
        block.toolInputJson += chunk;
        writeSse(args.res, "response.function_call_arguments.delta", {
          type: "response.function_call_arguments.delta",
          item_id: block.id,
          output_index: block.outputIndex,
          delta: chunk,
        });
      }
      blocks.set(index, block);
      return false;
    }
    if (type === "content_block_stop") {
      const index = finiteNumber(parsed.index) ?? 0;
      const block = blocks.get(index);
      if (!block) {
        return false;
      }
      if (block.type === "text") {
        writeSse(args.res, "response.output_text.done", {
          type: "response.output_text.done",
          item_id: block.id,
          output_index: block.outputIndex,
          content_index: 0,
          text: block.text,
        });
        writeSse(args.res, "response.content_part.done", {
          type: "response.content_part.done",
          item_id: block.id,
          output_index: block.outputIndex,
          content_index: 0,
          part: { type: "output_text", text: block.text, annotations: [] },
        });
        const item = {
          id: block.id,
          type: "message",
          status: "completed",
          role: "assistant",
          content: [{ type: "output_text", text: block.text, annotations: [] }],
        };
        outputItems.push(item);
        writeSse(args.res, "response.output_item.done", { type: "response.output_item.done", output_index: block.outputIndex, item });
      } else {
        const responseName = responsesFunctionCallNameFromChatName(block.toolName);
        const item = {
          id: block.id,
          type: "function_call",
          status: "completed",
          call_id: block.toolId,
          ...responseName,
          arguments: block.toolInputJson || "{}",
        };
        outputItems.push(item);
        writeSse(args.res, "response.function_call_arguments.done", {
          type: "response.function_call_arguments.done",
          item_id: block.id,
          output_index: block.outputIndex,
          arguments: block.toolInputJson || "{}",
        });
        writeSse(args.res, "response.output_item.done", { type: "response.output_item.done", output_index: block.outputIndex, item });
      }
      return false;
    }
    if (type === "message_stop") {
      upstreamDone = true;
      return true;
    }
    return false;
  };

  try {
    while (!upstreamDone) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split(/\r?\n/);
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) {
          continue;
        }
        if (trimmed.startsWith("event:")) {
          currentEvent = trimmed.slice(6).trim();
          continue;
        }
        if (!trimmed.startsWith("data:")) {
          continue;
        }
        const data = trimmed.slice(5).trim();
        if (processAnthropicEvent(currentEvent, data)) {
          await reader.cancel().catch(() => undefined);
          break;
        }
      }
    }
  } catch (error) {
    if (args.signal.aborted) {
      return;
    }
    writeFailedSse({
      errorMessage: `Anthropic Messages stream failed: ${error instanceof Error ? error.message : String(error)}`,
      model,
      res: args.res,
      responseId,
      responseMetadata,
    });
    args.res.end();
    return;
  }
  if (streamFailed) {
    return;
  }
  writeSse(args.res, "response.completed", { type: "response.completed", response: responseBase(responseId, model, outputItems, responseMetadata, streamUsage) });
  args.res.end();
}

function requestAbortSignal(req: IncomingMessage, res: ServerResponse): AbortSignal {
  const abortController = new AbortController();
  const abort = () => {
    if (!res.writableEnded && !abortController.signal.aborted) {
      abortController.abort();
    }
  };
  req.once("aborted", abort);
  res.once("close", abort);
  return abortController.signal;
}

async function startResponsesBridge(args: {
  adapter: ProviderAdapter;
  localApiKey: string;
  targetBaseUrl: string;
  upstreamApiKey: string;
  onLog?: (message: string) => void;
}): Promise<{ baseUrl: string; stop: () => Promise<void> }> {
  const targetBaseUrl = normalizeBaseUrl(args.targetBaseUrl);
  const expectedAuthorization = `Bearer ${args.localApiKey}`;
  const upstreamHeaders = args.adapter.upstreamAuthHeaders(args.upstreamApiKey);
  const server = createServer(async (req, res) => {
    try {
      if (req.method !== "POST" || !req.url?.replace(/\?.*$/, "").endsWith("/responses")) {
        res.writeHead(404, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { message: "Not Found" } }));
        return;
      }
      const authorization = stringValue(req.headers.authorization) ?? "";
      if (authorization !== expectedAuthorization) {
        res.writeHead(401, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { message: "Invalid Authorization header" } }));
        return;
      }
      const body = await readRequestJson(req);
      const signal = requestAbortSignal(req, res);
      if (args.adapter.kind === "anthropic_messages") {
        if (body.stream === true) {
          await forwardAnthropicStreaming({ body, res, signal, targetBaseUrl, upstreamHeaders, onLog: args.onLog });
        } else {
          await forwardAnthropicNonStreaming({ body, res, signal, targetBaseUrl, upstreamHeaders, onLog: args.onLog });
        }
      } else if (body.stream === true) {
        await forwardStreaming({
          body,
          res,
          signal,
          targetBaseUrl,
          upstreamAuthorization: upstreamHeaders.authorization ?? "",
          onLog: args.onLog,
        });
      } else {
        await forwardNonStreaming({
          body,
          res,
          signal,
          targetBaseUrl,
          upstreamAuthorization: upstreamHeaders.authorization ?? "",
          onLog: args.onLog,
        });
      }
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") {
        return;
      }
      args.onLog?.(`Codex chat bridge failed: ${error instanceof Error ? error.message : String(error)}`);
      if (!res.headersSent) {
        res.writeHead(500, { "content-type": "application/json" });
      }
      res.end(JSON.stringify({ error: { message: error instanceof Error ? error.message : String(error) } }));
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("Failed to start Codex chat bridge");
  }
  return {
    baseUrl: `http://127.0.0.1:${address.port}`,
    stop: () => stopServer(server),
  };
}

export async function startCodexChatBridge(args: {
  providerApiKey: string;
  providerId?: string | null;
  providerName?: string | null;
  targetBaseUrl?: string | null;
  onLog?: (message: string) => void;
}): Promise<CodexChatBridge> {
  const providerName = args.providerName?.trim() || "Chat Completions provider";
  const providerApiKey = args.providerApiKey.trim();
  if (!providerApiKey) {
    throw new Error(`${providerName} API key is required to start the Codex chat bridge`);
  }

  const targetBaseUrl = resolveTargetBaseUrl({ providerId: args.providerId, targetBaseUrl: args.targetBaseUrl });
  const adapter = resolveProviderAdapter(args.providerId);
  const apiKey = `sk-doer-chat-bridge-${randomUUID().replace(/-/g, "")}`;
  const bridge = await startResponsesBridge({
    adapter,
    localApiKey: apiKey,
    targetBaseUrl,
    upstreamApiKey: providerApiKey,
    onLog: args.onLog,
  });
  args.onLog?.(`Codex provider gateway listening baseUrl=${bridge.baseUrl} target=${targetBaseUrl} provider=${providerName} adapter=${adapter.kind}`);

  return {
    baseUrl: bridge.baseUrl,
    envKey: CHAT_BRIDGE_ENV_KEY,
    apiKey,
    stop: bridge.stop,
  };
}

function stopServer(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}
