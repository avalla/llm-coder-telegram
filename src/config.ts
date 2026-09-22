import { realpathSync, statSync } from "node:fs";
import { resolve } from "node:path";
import type { Project } from "./domain.js";

export interface BotConfig {
  telegramToken: string;
  allowedChatIds: readonly string[];
  users: Readonly<Record<string, "owner" | "operator" | "viewer">>;
  controlTopic: { chatId: string; threadId: string; title: string };
  projects: readonly Project[];
  databasePath: string;
}

export function assertConfiguredWorkspacePath(
  candidate: string,
  allowedRoots: readonly string[] = [],
): string {
  const canonical = realpathSync(candidate);
  if (!statSync(canonical).isDirectory())
    throw new Error(`Workspace is not a directory: ${candidate}`);
  if (allowedRoots.length > 0) {
    const canonicalRoots = allowedRoots.map((root) => realpathSync(root));
    const allowed = canonicalRoots.some(
      (root) => canonical === root || canonical.startsWith(`${root}/`),
    );
    if (!allowed) throw new Error(`Workspace is outside configured roots: ${candidate}`);
  }
  return canonical;
}

export function parseProjects(
  value: string,
  allowedRoots: readonly string[] = [],
): readonly Project[] {
  const raw = JSON.parse(value) as Record<
    string,
    { path: string; name?: string; allowedExecutors?: string[] }
  >;
  return Object.entries(raw).map(([id, config]) => ({
    id,
    name: config.name ?? id,
    workspacePath: assertConfiguredWorkspacePath(resolve(config.path), allowedRoots),
    allowedExecutorIds: config.allowedExecutors ?? ["fake"],
  }));
}

export function parseUsers(
  value: string,
): Readonly<Record<string, "owner" | "operator" | "viewer">> {
  const users = JSON.parse(value) as Record<string, "owner" | "operator" | "viewer">;
  for (const role of Object.values(users)) {
    if (role !== "owner" && role !== "operator" && role !== "viewer")
      throw new Error(`Invalid user role: ${role}`);
  }
  return users;
}
