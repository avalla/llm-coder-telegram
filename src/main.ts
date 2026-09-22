import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import {
  AgentOrchestrator,
  AuthorizationService,
  InMemoryExecutorRegistry,
} from "./application.js";
import { FakeAgentExecutor } from "./adapters.js";
import { parseProjects, parseUsers } from "./config.js";
import { SqlitePersistence } from "./sqlite.js";
import { TelegramApiGateway, runTelegramPolling } from "./telegram.js";

const token = process.env.TELEGRAM_BOT_TOKEN;
const chatId = process.env.TELEGRAM_CHAT_ID;
const controlThreadId = process.env.TELEGRAM_CONTROL_THREAD_ID;
const projectsJson = process.env.BOT_PROJECTS_JSON;
const usersJson = process.env.BOT_USERS_JSON;

if (!token || !chatId || !controlThreadId || !projectsJson || !usersJson) {
  throw new Error(
    "Set TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_CONTROL_THREAD_ID, BOT_PROJECTS_JSON and BOT_USERS_JSON",
  );
}

const databasePath = process.env.BOT_DATABASE_PATH ?? "./data/bot.sqlite";
mkdirSync(dirname(databasePath), { recursive: true });
const persistence = SqlitePersistence.open(databasePath);
for (const project of parseProjects(projectsJson)) persistence.saveProject(project);
await persistence.topics.save({
  chatId,
  threadId: controlThreadId,
  kind: "control",
  title: "Control",
  status: "open",
  updatedAt: new Date(),
});

const gateway = new TelegramApiGateway(token);
const registry = new InMemoryExecutorRegistry();
registry.register(new FakeAgentExecutor());
const orchestrator = new AgentOrchestrator(
  persistence,
  registry,
  gateway,
  new AuthorizationService({ allowedChatIds: [chatId], users: parseUsers(usersJson) }),
);

const abort = new AbortController();
process.once("SIGINT", () => abort.abort());
process.once("SIGTERM", () => abort.abort());
await runTelegramPolling(gateway, (message) => orchestrator.handleMessage(message), abort.signal);
