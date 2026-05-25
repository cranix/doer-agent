import path from "node:path";
import { mkdir, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";

const MANAGED_MARKER = "<!-- managed-by: doer-agent bundled-skill -->";
const LEGACY_MANAGED_SKILLS_DIRS = [".doer", "doer-managed"];

async function pathExists(target: string): Promise<boolean> {
  try {
    await stat(target);
    return true;
  } catch {
    return false;
  }
}

async function shouldWriteManagedSkill(targetFile: string): Promise<boolean> {
  if (!(await pathExists(targetFile))) {
    return true;
  }
  const current = await readFile(targetFile, "utf8").catch(() => "");
  return current.includes(MANAGED_MARKER);
}

function withManagedMarker(source: string): string {
  const normalizedSource = source.trimEnd();
  if (normalizedSource.includes(MANAGED_MARKER)) {
    return `${normalizedSource}\n`;
  }
  const frontmatterMatch = normalizedSource.match(/^(---\n[\s\S]*?\n---)(\n?)([\s\S]*)$/);
  if (frontmatterMatch) {
    const [, frontmatter, , body] = frontmatterMatch;
    return `${frontmatter}\n${MANAGED_MARKER}\n${body.replace(/^\n/, "")}\n`;
  }
  return `${MANAGED_MARKER}\n${normalizedSource}\n`;
}

async function removeLegacyManagedSkill(args: {
  codexHome: string;
  skillName: string;
  onInfo?: (message: string) => void;
}): Promise<void> {
  for (const legacyRoot of LEGACY_MANAGED_SKILLS_DIRS) {
    const legacySkillDir = path.join(args.codexHome, "skills", legacyRoot, args.skillName);
    const legacySkillFile = path.join(legacySkillDir, "SKILL.md");
    if (!(await pathExists(legacySkillFile))) {
      continue;
    }
    const current = await readFile(legacySkillFile, "utf8").catch(() => "");
    if (!current.includes(MANAGED_MARKER)) {
      continue;
    }
    await rm(legacySkillDir, { recursive: true, force: true });
    args.onInfo?.(`legacy bundled skill removed name=${args.skillName} path=.codex/skills/${legacyRoot}/${args.skillName}`);
  }
}

export async function ensureBundledDoerSkills(args: {
  bundledSkillsRoot: string;
  codexHome: string;
  onInfo?: (message: string) => void;
  onError?: (message: string) => void;
}): Promise<void> {
  if (!(await pathExists(args.bundledSkillsRoot))) {
    return;
  }
  const entries = await readdir(args.bundledSkillsRoot, { withFileTypes: true });
  for (const entry of entries) {
    if (!entry.isDirectory() || entry.name.startsWith(".")) {
      continue;
    }
    const sourceSkillFile = path.join(args.bundledSkillsRoot, entry.name, "SKILL.md");
    const targetSkillDir = path.join(args.codexHome, "skills", entry.name);
    const targetSkillFile = path.join(targetSkillDir, "SKILL.md");
    try {
      const source = await readFile(sourceSkillFile, "utf8");
      if (!(await shouldWriteManagedSkill(targetSkillFile))) {
        args.onInfo?.(`bundled skill skipped name=${entry.name} reason=user-managed-target`);
        continue;
      }
      await mkdir(targetSkillDir, { recursive: true });
      await writeFile(targetSkillFile, withManagedMarker(source), "utf8");
      await removeLegacyManagedSkill({ codexHome: args.codexHome, skillName: entry.name, onInfo: args.onInfo });
      args.onInfo?.(`bundled skill synced name=${entry.name} path=.codex/skills/${entry.name}/SKILL.md`);
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown error";
      args.onError?.(`bundled skill sync failed name=${entry.name} error=${message}`);
    }
  }
}
