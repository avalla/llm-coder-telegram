import { unlinkSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import {
  AgentOrchestrator,
  AuthorizationService,
  InMemoryExecutorRegistry,
} from "../src/application.js";
import { FakeAgentExecutor, InMemoryChatGateway } from "../src/adapters.js";
import { SqlitePersistence } from "../src/sqlite.js";

test("fresh SQLite migration enables integrity constraints", () => {
  const db = new Database(":memory:");
  const persistence = new SqlitePersistence(db);

  expect(db.query<{ foreign_keys: number }, any>("PRAGMA foreign_keys").get()?.foreign_keys).toBe(
    1,
  );
  expect(
    db
      .query<{ version: number }, any>("SELECT version FROM schema_migrations ORDER BY version")
      .all(),
  ).toEqual([{ version: 1 }, { version: 2 }]);

  expect(() =>
    db
      .query(
        "INSERT INTO executions (id, bot_session_id, requested_by_user_id, prompt, status, correlation_id) VALUES (?, ?, ?, ?, ?, ?)",
      )
      .run("execution", "missing-session", "operator", "prompt", "completed", "correlation"),
  ).toThrow();

  persistence.saveProject({
    id: "project",
    name: "Project",
    workspacePath: "/workspace/project",
    allowedExecutorIds: ["fake"],
  });
  expect(() =>
    db
      .query(
        "INSERT INTO bot_sessions (id, telegram_chat_id, telegram_thread_id, executor_id, project_id, workspace_path, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
      )
      .run("session", "chat", "thread", "fake", "project", "/workspace/project", "invalid", 0, 0),
  ).toThrow();

  db.close();
});

test("reopens SQLite state and resumes the persisted native session", async () => {
  const databasePath = join(tmpdir(), `telegram-bot-${crypto.randomUUID()}.sqlite`);
  const executor = new FakeAgentExecutor();
  try {
    const firstPersistence = SqlitePersistence.open(databasePath);
    firstPersistence.saveProject({
      id: "project",
      name: "Project",
      workspacePath: "/workspace/project",
      allowedExecutorIds: ["fake"],
    });
    const firstGateway = new InMemoryChatGateway();
    const firstRegistry = new InMemoryExecutorRegistry();
    firstRegistry.register(executor);
    const firstOrchestrator = new AgentOrchestrator(
      firstPersistence,
      firstRegistry,
      firstGateway,
      new AuthorizationService({ allowedChatIds: ["chat"], users: { operator: "operator" } }),
    );
    const session = await firstOrchestrator.createSession({
      chatId: "chat",
      userId: "operator",
      projectId: "project",
      executorId: "fake",
    });
    await firstOrchestrator.handleMessage({
      chatId: "chat",
      threadId: session.telegramThreadId,
      userId: "operator",
      text: "first",
    });
    firstPersistence.db.close();

    const secondPersistence = SqlitePersistence.open(databasePath);
    const secondGateway = new InMemoryChatGateway();
    const secondRegistry = new InMemoryExecutorRegistry();
    secondRegistry.register(executor);
    const secondOrchestrator = new AgentOrchestrator(
      secondPersistence,
      secondRegistry,
      secondGateway,
      new AuthorizationService({ allowedChatIds: ["chat"], users: { operator: "operator" } }),
    );
    await secondOrchestrator.handleMessage({
      chatId: "chat",
      threadId: session.telegramThreadId,
      userId: "operator",
      text: "second",
    });

    expect(executor.starts).toHaveLength(1);
    expect(executor.resumes).toHaveLength(1);
    expect(executor.resumes[0]?.nativeSessionId).toBe("fake-native-fake-runtime-1");
    expect(
      (await secondPersistence.executions.listBySession(session.id)).map((item) => item.status),
    ).toEqual(["completed", "completed"]);
    secondPersistence.db.close();
  } finally {
    unlinkSync(databasePath);
  }
});
