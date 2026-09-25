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
  ).toEqual([{ version: 1 }, { version: 2 }, { version: 3 }, { version: 4 }, { version: 5 }]);

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

test("upgrades a v4 database to v5 without losing execution state", async () => {
  const db = new Database(":memory:");
  const initial = new SqlitePersistence(db);
  initial.saveProject({
    id: "project",
    name: "Project",
    workspacePath: "/workspace/project",
    allowedExecutorIds: ["fake"],
  });
  db.query(
    `INSERT INTO bot_sessions
      (id, telegram_chat_id, telegram_thread_id, executor_id, project_id, workspace_path, status, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  ).run("session", "chat", "thread", "fake", "project", "/workspace/project", "failed", 1, 2);
  db.query(
    `INSERT INTO executions
      (id, bot_session_id, requested_by_user_id, prompt, status, correlation_id, owner_fence)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
  ).run("execution", "session", "operator", "ambiguous", "unknown", "correlation", 3);
  db.run("DROP TABLE execution_reconciliations");
  db.query("DELETE FROM schema_migrations WHERE version=5").run();

  const upgraded = new SqlitePersistence(db);
  expect(
    db
      .query<{ version: number }, any>("SELECT MAX(version) AS version FROM schema_migrations")
      .get()?.version,
  ).toBe(5);
  expect(await upgraded.executions.getById("execution")).toMatchObject({
    status: "unknown",
    ownerFence: 3,
  });
  expect(db.query("PRAGMA foreign_key_check").all()).toEqual([]);
  db.close();
});

function createLegacySchema(db: Database): void {
  db.run(`
    CREATE TABLE projects (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      workspace_path TEXT NOT NULL,
      allowed_executor_ids TEXT NOT NULL
    );
    CREATE TABLE bot_sessions (
      id TEXT PRIMARY KEY,
      telegram_chat_id TEXT NOT NULL,
      telegram_thread_id TEXT NOT NULL,
      executor_id TEXT NOT NULL,
      project_id TEXT NOT NULL,
      workspace_path TEXT NOT NULL,
      agent_session_id TEXT,
      status TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      UNIQUE (telegram_chat_id, telegram_thread_id)
    );
    CREATE TABLE executions (
      id TEXT PRIMARY KEY,
      bot_session_id TEXT NOT NULL,
      requested_by_user_id TEXT NOT NULL,
      prompt TEXT NOT NULL,
      status TEXT NOT NULL,
      correlation_id TEXT NOT NULL,
      agent_session_id TEXT,
      started_at INTEGER,
      finished_at INTEGER,
      error_message TEXT
    );
    CREATE TABLE telegram_topics (
      chat_id TEXT NOT NULL,
      thread_id TEXT NOT NULL,
      session_id TEXT,
      kind TEXT NOT NULL,
      title TEXT NOT NULL,
      status TEXT NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY (chat_id, thread_id)
    );
    CREATE TABLE audit_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      action TEXT NOT NULL,
      user_id TEXT,
      chat_id TEXT,
      session_id TEXT,
      execution_id TEXT,
      correlation_id TEXT NOT NULL,
      metadata TEXT,
      created_at INTEGER NOT NULL
    );
  `);
}

function logicalSchema(db: Database): Record<string, unknown> {
  const tables = [
    "projects",
    "executor_sessions",
    "bot_sessions",
    "executions",
    "execution_jobs",
    "execution_reconciliations",
    "telegram_topics",
    "audit_log",
  ];
  return Object.fromEntries(
    tables.map((table) => [
      table,
      {
        columns: db
          .query<{ name: string; type: string; notnull: number; pk: number }, any>(
            `PRAGMA table_info(${table})`,
          )
          .all()
          .map(({ name, type, notnull, pk }) => ({ name, type, notnull, pk })),
        foreignKeys: db
          .query<{ table: string; from: string; to: string }, any>(
            `PRAGMA foreign_key_list(${table})`,
          )
          .all()
          .map(({ table: referencedTable, from, to }) => ({
            table: referencedTable,
            from,
            to,
          })),
        indexes: db
          .query<{ name: string; unique: number }, any>(`PRAGMA index_list(${table})`)
          .all()
          .map(({ name, unique }) => ({ name, unique }))
          .sort((a, b) => a.name.localeCompare(b.name)),
      },
    ]),
  );
}

test("migrates the exact pre-M2 SQLite schema without losing runtime state", async () => {
  const databasePath = join(tmpdir(), `telegram-bot-legacy-${crypto.randomUUID()}.sqlite`);
  try {
    const legacy = new Database(databasePath);
    createLegacySchema(legacy);
    legacy
      .query("INSERT INTO projects VALUES (?, ?, ?, ?)")
      .run("project", "Legacy project", "/workspace/project", '["fake"]');
    legacy
      .query("INSERT INTO bot_sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
      .run(
        "session",
        "chat",
        "thread",
        "fake",
        "project",
        "/workspace/project",
        "runtime-123",
        "idle",
        100,
        200,
      );
    legacy
      .query("INSERT INTO executions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
      .run(
        "execution",
        "session",
        "operator",
        "legacy prompt",
        "completed",
        "correlation",
        "runtime-123",
        100,
        200,
        null,
      );
    legacy
      .query("INSERT INTO telegram_topics VALUES (?, ?, ?, ?, ?, ?, ?)")
      .run("chat", "thread", "session", "workspace", "Legacy", "open", 200);
    legacy
      .query("INSERT INTO audit_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
      .run(
        1,
        "execution.created",
        "operator",
        "chat",
        "session",
        "execution",
        "correlation",
        null,
        100,
      );
    legacy.close();

    const upgraded = SqlitePersistence.open(databasePath);
    expect(
      upgraded.db.query<{ foreign_keys: number }, any>("PRAGMA foreign_keys").get()?.foreign_keys,
    ).toBe(1);
    expect(
      upgraded.db
        .query<{ version: number }, any>("SELECT version FROM schema_migrations ORDER BY version")
        .all(),
    ).toEqual([{ version: 1 }, { version: 2 }, { version: 3 }, { version: 4 }, { version: 5 }]);
    expect(await upgraded.projects.getById("project")).toMatchObject({
      id: "project",
      workspacePath: "/workspace/project",
    });
    const session = await upgraded.sessions.getById("session");
    expect(session?.executorSessionId).toBe("legacy-runtime-session");
    const executorSession = await upgraded.executorSessions.getById(session!.executorSessionId!);
    expect(executorSession).toMatchObject({
      resumable: false,
      legacyRuntimeSessionId: "runtime-123",
    });
    expect(executorSession?.nativeSessionId).toBeUndefined();
    const execution = (await upgraded.executions.listBySession("session"))[0];
    expect(execution?.executorSessionId).toBe("legacy-execution-execution");
    expect(await upgraded.topics.get("chat", "thread")).toMatchObject({ sessionId: "session" });
    expect(await upgraded.db.query("SELECT * FROM audit_log").all()).toHaveLength(1);

    const freshDb = new Database(":memory:");
    new SqlitePersistence(freshDb);
    expect(logicalSchema(upgraded.db)).toEqual(logicalSchema(freshDb));
    expect(upgraded.db.query("PRAGMA foreign_key_check").all()).toEqual([]);
    expect(() =>
      upgraded.db
        .query(
          "INSERT INTO bot_sessions (id, telegram_chat_id, telegram_thread_id, executor_id, project_id, workspace_path, status, created_at, updated_at) VALUES ('bad', 'chat', 'bad', 'fake', 'missing', '/workspace', 'idle', 1, 1)",
        )
        .run(),
    ).toThrow();
    expect(() =>
      upgraded.db
        .query(
          "INSERT INTO executions (id, bot_session_id, requested_by_user_id, prompt, status, correlation_id) VALUES ('bad', 'session', 'operator', '', 'completed', 'bad-correlation')",
        )
        .run(),
    ).toThrow();
    expect(() =>
      upgraded.db
        .query(
          "INSERT INTO bot_sessions (id, telegram_chat_id, telegram_thread_id, executor_id, project_id, workspace_path, status, created_at, updated_at) VALUES ('bad-status', 'chat', 'bad-status', 'fake', 'project', '/workspace', 'invalid', 1, 1)",
        )
        .run(),
    ).toThrow();
    expect(() =>
      upgraded.db
        .query(
          "INSERT INTO executor_sessions (id, executor_id, project_id, workspace_path, resumable, created_at, last_used_at) VALUES ('bad-resume', 'fake', 'project', '/workspace', 1, 1, 1)",
        )
        .run(),
    ).toThrow();
    expect(() =>
      upgraded.db
        .query(
          "INSERT INTO telegram_topics (chat_id, thread_id, session_id, kind, title, status, updated_at) VALUES ('chat', 'bad-topic', 'missing', 'workspace', 'bad', 'open', 1)",
        )
        .run(),
    ).toThrow();
    expect(() =>
      upgraded.db
        .query(
          "INSERT INTO audit_log (action, session_id, correlation_id, created_at) VALUES ('bad', 'missing', 'bad-audit', 1)",
        )
        .run(),
    ).toThrow();
    freshDb.close();
    upgraded.db.close();
  } finally {
    unlinkSync(databasePath);
  }
});

test("failed schema rebuild rolls back schema and migration version together", () => {
  const db = new Database(":memory:");
  createLegacySchema(db);
  db.run("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY)");
  db.run("INSERT INTO schema_migrations VALUES (2)");
  db.query("INSERT INTO projects VALUES (?, ?, ?, ?)").run("project", "Broken", "", "[]");

  expect(() => new SqlitePersistence(db)).toThrow();
  expect(
    db.query<{ version: number }, any>("SELECT version FROM schema_migrations").get()?.version,
  ).toBe(2);
  expect(
    db
      .query<{ name: string }, any>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'executor_sessions'",
      )
      .get(),
  ).toBeNull();
  expect(
    db.query<{ workspace_path: string }, any>("SELECT workspace_path FROM projects").get()
      ?.workspace_path,
  ).toBe("");
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
