import path from "node:path";
import { mkdir, stat, writeFile } from "node:fs/promises";
import { StringCodec, type Msg, type NatsConnection } from "nats";
import type { AgentSettingsConfig } from "./agent-settings.js";

const skillRpcCodec = StringCodec();

interface AgentSkillRpcRequest {
  requestId?: unknown;
  action?: unknown;
  prompt?: unknown;
  agentId?: unknown;
}

function buildSkillGeneratorPrompt(userPrompt: string): string {
  return [
    "Create a Codex skill from the user's description.",
    "",
    "Return JSON only with this shape:",
    '{ "skillName": "kebab-or-dot-or-underscore-name", "skillMd": "full SKILL.md content" }',
    "",
    "Requirements:",
    "- skillName must be lowercase ASCII and use only letters, numbers, dot, underscore, or dash.",
    "- skillName must not start with a dot and must not be .system.",
    "- skillMd must be a complete SKILL.md file.",
    "- skillMd must start with YAML frontmatter containing name and description.",
    "- Keep the skill concise and action-oriented.",
    "- Include when to use the skill, the core workflow, and any important constraints.",
    "- Do not add README, changelog, or any extra files.",
    "",
    "User request:",
    userPrompt.trim(),
  ].join("\n");
}

function extractJsonObject(value: string): string {
  const trimmed = value.trim();
  if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
    return trimmed;
  }
  const fenced = trimmed.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fenced?.[1]) {
    return fenced[1].trim();
  }
  const start = trimmed.indexOf("{");
  const end = trimmed.lastIndexOf("}");
  if (start >= 0 && end > start) {
    return trimmed.slice(start, end + 1);
  }
  throw new Error("Codex did not return JSON");
}

function slugifySkillName(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/-{2,}/g, "-");
}

function normalizeGeneratedSkill(value: unknown): { skillName: string; skillMd: string } {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Invalid generated skill payload");
  }
  const row = value as Record<string, unknown>;
  const skillName = slugifySkillName(typeof row.skillName === "string" ? row.skillName : "");
  const skillMd = typeof row.skillMd === "string" ? row.skillMd.trim() : "";
  if (!skillName || skillName.startsWith(".") || skillName === ".system") {
    throw new Error("Codex returned an invalid skill name");
  }
  if (!skillMd) {
    throw new Error("Codex returned an empty SKILL.md");
  }
  if (!/^---\s*\n[\s\S]*?\n---\s*\n/m.test(skillMd)) {
    throw new Error("Generated SKILL.md is missing YAML frontmatter");
  }
  if (!/\nname:\s*[^\n]+/i.test(skillMd) || !/\ndescription:\s*[^\n]+/i.test(skillMd)) {
    throw new Error("Generated SKILL.md frontmatter is incomplete");
  }
  return { skillName, skillMd };
}

function buildSkillGeneratorCodexArgs(prompt: string, model: string): string[] {
  return ["--dangerously-bypass-approvals-and-sandbox", "--model", model, "exec", "--", prompt];
}

async function generateSkillViaCodex(args: {
  userPrompt: string;
  workspaceRoot: string;
  resolveCodexHomePath: () => string;
  readAgentSettingsConfig: (args: { workspaceRoot: string }) => Promise<AgentSettingsConfig>;
  buildAgentSettingsEnvPatch: (config: AgentSettingsConfig) => Record<string, string>;
  runLocalCodexCli: (cmdArgs: string[], timeoutMs: number, envPatch?: Record<string, string>) => Promise<{
    code: number | null;
    stdout: string;
    stderr: string;
    timedOut: boolean;
  }>;
  stripAnsi: (value: string) => string;
}): Promise<{
  skillName: string;
  skillPath: string;
  skillFilePath: string;
}> {
  const localAgentSettings = await args.readAgentSettingsConfig({ workspaceRoot: args.workspaceRoot });
  const envPatch = args.buildAgentSettingsEnvPatch(localAgentSettings);
  const prompt = buildSkillGeneratorPrompt(args.userPrompt);
  const result = await args.runLocalCodexCli(
    buildSkillGeneratorCodexArgs(prompt, localAgentSettings.codex.model || "gpt-5.5"),
    120_000,
    envPatch,
  );

  if (result.timedOut) {
    throw new Error("Codex timed out while generating the skill");
  }
  if ((result.code ?? 1) !== 0) {
    const details = args.stripAnsi(result.stderr || result.stdout).trim();
    throw new Error(details || `Codex exited with code ${result.code ?? "null"}`);
  }

  const payload = JSON.parse(extractJsonObject(args.stripAnsi(result.stdout))) as unknown;
  const generated = normalizeGeneratedSkill(payload);
  const skillPath = path.join(args.resolveCodexHomePath(), "skills", generated.skillName);
  const skillFilePath = path.join(skillPath, "SKILL.md");

  try {
    await stat(skillPath);
    throw new Error("A skill with that name already exists");
  } catch (error) {
    if (!(error instanceof Error) || !/ENOENT/i.test(error.message)) {
      throw error;
    }
  }

  await mkdir(skillPath, { recursive: true });
  await writeFile(skillFilePath, `${generated.skillMd}\n`, "utf8");

  return {
    skillName: generated.skillName,
    skillPath: `.codex/skills/${generated.skillName}`,
    skillFilePath: `.codex/skills/${generated.skillName}/SKILL.md`,
  };
}

async function handleSkillRpcMessage(args: {
  msg: Msg;
  agentId: string;
  workspaceRoot: string;
  resolveCodexHomePath: () => string;
  readAgentSettingsConfig: (args: { workspaceRoot: string }) => Promise<AgentSettingsConfig>;
  buildAgentSettingsEnvPatch: (config: AgentSettingsConfig) => Record<string, string>;
  runLocalCodexCli: (cmdArgs: string[], timeoutMs: number, envPatch?: Record<string, string>) => Promise<{
    code: number | null;
    stdout: string;
    stderr: string;
    timedOut: boolean;
  }>;
  stripAnsi: (value: string) => string;
  onError: (message: string) => void;
}): Promise<void> {
  let payload: AgentSkillRpcRequest = {};
  try {
    payload = JSON.parse(skillRpcCodec.decode(args.msg.data)) as AgentSkillRpcRequest;
    if (typeof payload.agentId === "string" && payload.agentId.trim() && payload.agentId !== args.agentId) {
      throw new Error("agent id mismatch");
    }
    const prompt = typeof payload.prompt === "string" ? payload.prompt.trim() : "";
    if (!prompt) {
      throw new Error("prompt is required");
    }
    const result = await generateSkillViaCodex({
      userPrompt: prompt,
      workspaceRoot: args.workspaceRoot,
      resolveCodexHomePath: args.resolveCodexHomePath,
      readAgentSettingsConfig: args.readAgentSettingsConfig,
      buildAgentSettingsEnvPatch: args.buildAgentSettingsEnvPatch,
      runLocalCodexCli: args.runLocalCodexCli,
      stripAnsi: args.stripAnsi,
    });
    args.msg.respond(
      skillRpcCodec.encode(
        JSON.stringify({
          ok: true,
          skillName: result.skillName,
          skillPath: result.skillPath,
          skillFilePath: result.skillFilePath,
        }),
      ),
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "unknown error";
    args.msg.respond(
      skillRpcCodec.encode(
        JSON.stringify({
          ok: false,
          error: message,
        }),
      ),
    );
    args.onError(`skill rpc failed error=${message}`);
  }
}

export function subscribeToSkillRpc(args: {
  nc: NatsConnection;
  subject: string;
  agentId: string;
  workspaceRoot: string;
  resolveCodexHomePath: () => string;
  readAgentSettingsConfig: (args: { workspaceRoot: string }) => Promise<AgentSettingsConfig>;
  buildAgentSettingsEnvPatch: (config: AgentSettingsConfig) => Record<string, string>;
  runLocalCodexCli: (cmdArgs: string[], timeoutMs: number, envPatch?: Record<string, string>) => Promise<{
    code: number | null;
    stdout: string;
    stderr: string;
    timedOut: boolean;
  }>;
  stripAnsi: (value: string) => string;
  onInfo: (message: string) => void;
  onError: (message: string) => void;
}): void {
  args.nc.subscribe(args.subject, {
    callback: (error, msg) => {
      if (error) {
        const message = error instanceof Error ? error.message : String(error);
        args.onError(`skill rpc subscription error: ${message}`);
        return;
      }
      void handleSkillRpcMessage({
        msg,
        agentId: args.agentId,
        workspaceRoot: args.workspaceRoot,
        resolveCodexHomePath: args.resolveCodexHomePath,
        readAgentSettingsConfig: args.readAgentSettingsConfig,
        buildAgentSettingsEnvPatch: args.buildAgentSettingsEnvPatch,
        runLocalCodexCli: args.runLocalCodexCli,
        stripAnsi: args.stripAnsi,
        onError: args.onError,
      });
    },
  });
  args.onInfo(`skill rpc subscribed subject=${args.subject}`);
}
