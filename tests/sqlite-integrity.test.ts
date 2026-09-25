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

test("upgrades a real M2 v3 database through M3 and M4 to canonical v5", async () => {
  const databasePath = join(tmpdir(), `telegram-bot-m2-v3-${crypto.randomUUID()}.sqlite`);
  try {
    const legacy = new Database(databasePath);
    legacy.run("PRAGMA foreign_keys = ON");
    createM2V3Schema(legacy);
    legacy
      .query("INSERT INTO projects VALUES (?, ?, ?, ?)")
      .run("project", "M2 project", "/workspace/project", '["fake"]');
    legacy
      .query("INSERT INTO executor_sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
      .run(
        "executor-session",
        "fake",
        "native-m2",
        "project",
        "/workspace/project",
        null,
        null,
        1,
        10,
        20,
      );
    legacy
      .query(`INSERT INTO bot_sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(
        "session",
        "chat",
        "thread",
        "fake",
        "project",
        "/workspace/project",
        "executor-session",
        "failed",
        10,
        20,
      );
    legacy
      .query(
        `INSERT INTO executions
        (id, bot_session_id, requested_by_user_id, prompt, status, correlation_id,
         executor_session_id, started_at, finished_at, error_message)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        "unknown-execution",
        "session",
        "operator",
        "historical unknown",
        "unknown",
        "unknown-correlation",
        "executor-session",
        11,
        12,
        "M2 uncertainty",
      );
    legacy
      .query(
        `INSERT INTO executions
        (id, bot_session_id, requested_by_user_id, prompt, status, correlation_id)
       VALUES (?, ?, ?, ?, ?, ?)`,
      )
      .run(
        "pending-execution",
        "session",
        "operator",
        "queued in M2",
        "pending",
        "pending-correlation",
      );
    legacy.close();

    const upgraded = SqlitePersistence.open(databasePath);
    expect(
      upgraded.db
        .query<{ version: number }, any>("SELECT version FROM schema_migrations ORDER BY version")
        .all(),
    ).toEqual([{ version: 1 }, { version: 2 }, { version: 3 }, { version: 4 }, { version: 5 }]);
    expect(
      upgraded.db
        .query<{ name: string }, any>(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='execution_jobs'",
        )
        .get(),
    ).toBeTruthy();
    expect(
      upgraded.db
        .query<{ name: string }, any>(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='execution_reconciliations'",
        )
        .get(),
    ).toBeTruthy();
    expect(
      upgraded.db
        .query<{ name: string }, any>("PRAGMA table_info(executions)")
        .all()
        .map((c) => c.name),
    ).toEqual(
      expect.arrayContaining([
        "owner_id",
        "owner_fence",
        "lease_expires_at",
        "cancel_requested_at",
        "cancel_requested_by_user_id",
      ]),
    );
    expect(await upgraded.projects.getById("project")).toMatchObject({
      name: "M2 project",
      workspacePath: "/workspace/project",
    });
    expect(await upgraded.executorSessions.getById("executor-session")).toMatchObject({
      nativeSessionId: "native-m2",
      resumable: true,
    });
    expect(await upgraded.executions.getById("unknown-execution")).toMatchObject({
      status: "unknown",
      prompt: "historical unknown",
      ownerFence: 0,
      errorMessage: "M2 uncertainty",
    });
    expect(
      await upgraded.executions.claim("pending-execution", "worker", new Date(100), 1_000),
    ).toBeUndefined();

    const reconciliation = await upgraded.reconciliations.reconcile(
      "unknown-execution",
      "session",
      {
        executionId: "unknown-execution",
        outcome: "confirmed_completed",
        reconciledByUserId: "operator",
        note: "confirmed from external record",
        reconciledAt: new Date(200),
      },
    );
    expect(reconciliation).toMatchObject({ status: "reconciled" });
    expect(await upgraded.executions.getById("unknown-execution")).toMatchObject({
      status: "unknown",
    });
    expect(await upgraded.reconciliations.listBySession("session")).toMatchObject([
      { executionId: "unknown-execution", outcome: "confirmed_completed" },
    ]);
    expect(
      upgraded.db
        .query<{ action: string; execution_id: string; metadata: string }, any>(
          "SELECT action, execution_id, metadata FROM audit_log WHERE action='execution.reconciled'",
        )
        .all(),
    ).toEqual([
      {
        action: "execution.reconciled",
        execution_id: "unknown-execution",
        metadata: '{"outcome":"confirmed_completed"}',
      },
    ]);
    expect((await upgraded.sessions.getById("session"))?.status).toBe("idle");
    expect(
      await upgraded.executions.claim("pending-execution", "worker", new Date(300), 1_000),
    ).toMatchObject({ fence: 1, ownerId: "worker" });

    const freshDb = new Database(":memory:");
    new SqlitePersistence(freshDb);
    expect(logicalSchema(upgraded.db)).toEqual(logicalSchema(freshDb));
    expect(upgraded.db.query("PRAGMA foreign_key_check").all()).toEqual([]);
    freshDb.close();
    upgraded.db.close();

    const reopened = SqlitePersistence.open(databasePath);
    const canonicalDb = new Database(":memory:");
    new SqlitePersistence(canonicalDb);
    expect(logicalSchema(reopened.db)).toEqual(logicalSchema(canonicalDb));
    expect(reopened.db.query("PRAGMA foreign_key_check").all()).toEqual([]);
    reopened.db.close();
    canonicalDb.close();
  } finally {
    unlinkSync(databasePath);
  }
});

test("rolls back SQLite reconciliation when its audit insert fails", async () => {
  const db = new Database(":memory:");
  const persistence = new SqlitePersistence(db);
  persistence.saveProject({
    id: "project",
    name: "Project",
    workspacePath: "/workspace/project",
    allowedExecutorIds: ["fake"],
  });
  db.query(
    `INSERT INTO bot_sessions
      (id, telegram_chat_id, telegram_thread_id, executor_id, project_id, workspace_path,
       status, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  ).run("session", "chat", "thread", "fake", "project", "/workspace/project", "failed", 1, 2);
  db.query(
    `INSERT INTO executions
      (id, bot_session_id, requested_by_user_id, prompt, status, correlation_id)
     VALUES (?, ?, ?, ?, ?, ?)`,
  ).run("unknown", "session", "operator", "prompt", "unknown", "correlation");
  db.run(`
    CREATE TRIGGER fail_reconciliation_audit
    BEFORE INSERT ON audit_log
    WHEN NEW.action='execution.reconciled'
    BEGIN
      SELECT RAISE(ABORT, 'audit insert failed');
    END
  `);

  await expect(
    persistence.reconciliations.reconcile("unknown", "session", {
      executionId: "unknown",
      outcome: "abandoned",
      reconciledByUserId: "operator",
      note: "verified",
      reconciledAt: new Date(3),
    }),
  ).rejects.toThrow("audit insert failed");
  expect(await persistence.reconciliations.listBySession("session")).toEqual([]);
  expect((await persistence.executions.getById("unknown"))?.status).toBe("unknown");
  expect((await persistence.sessions.getById("session"))?.status).toBe("failed");
  expect(db.query("SELECT * FROM audit_log WHERE action='execution.reconciled'").all()).toEqual([]);
  db.close();
});

test("upgrades a physical M3 v4 database to canonical v5", async () => {
  const databasePath = join(tmpdir(), `telegram-bot-m3-v4-${crypto.randomUUID()}.sqlite`);
  try {
    const prior = new Database(databasePath);
    prior.run("PRAGMA foreign_keys = ON");
    createM2V3Schema(prior);
    prior
      .query("INSERT INTO projects VALUES (?, ?, ?, ?)")
      .run("project", "M3 project", "/workspace/project", '["fake"]');
    prior
      .query(
        `INSERT INTO bot_sessions
        (id, telegram_chat_id, telegram_thread_id, executor_id, project_id, workspace_path,
         status, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run("session", "chat", "thread", "fake", "project", "/workspace/project", "failed", 1, 2);
    prior
      .query(
        `INSERT INTO executions
        (id, bot_session_id, requested_by_user_id, prompt, status, correlation_id)
       VALUES (?, ?, ?, ?, ?, ?)`,
      )
      .run("execution", "session", "operator", "ambiguous", "unknown", "correlation");
    applyM3ToM2Fixture(prior);
    prior.query("UPDATE executions SET owner_fence=3 WHERE id='execution'").run();
    prior.close();

    const upgraded = SqlitePersistence.open(databasePath);
    expect(
      upgraded.db
        .query<{ version: number }, any>("SELECT version FROM schema_migrations ORDER BY version")
        .all(),
    ).toEqual([{ version: 1 }, { version: 2 }, { version: 3 }, { version: 4 }, { version: 5 }]);
    expect(await upgraded.executions.getById("execution")).toMatchObject({
      status: "unknown",
      ownerFence: 3,
    });
    const freshDb = new Database(":memory:");
    new SqlitePersistence(freshDb);
    expect(logicalSchema(upgraded.db)).toEqual(logicalSchema(freshDb));
    expect(upgraded.db.query("PRAGMA foreign_key_check").all()).toEqual([]);
    freshDb.close();
    upgraded.db.close();
  } finally {
    unlinkSync(databasePath);
  }
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

function createM2V3Schema(db: Database): void {
  db.run(`
    CREATE TABLE projects (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      workspace_path TEXT NOT NULL CHECK (length(workspace_path) > 0),
      allowed_executor_ids TEXT NOT NULL
    );
    CREATE TABLE executor_sessions (
      id TEXT PRIMARY KEY,
      executor_id TEXT NOT NULL,
      native_session_id TEXT CHECK (native_session_id IS NULL OR length(native_session_id) > 0),
      project_id TEXT NOT NULL REFERENCES projects(id),
      workspace_path TEXT NOT NULL CHECK (length(workspace_path) > 0),
      host_id TEXT,
      legacy_runtime_session_id TEXT,
      resumable INTEGER NOT NULL CHECK (resumable IN (0, 1) AND (resumable = 0 OR native_session_id IS NOT NULL)),
      created_at INTEGER NOT NULL,
      last_used_at INTEGER NOT NULL,
      UNIQUE (executor_id, native_session_id)
    );
    CREATE TABLE bot_sessions (
      id TEXT PRIMARY KEY,
      telegram_chat_id TEXT NOT NULL,
      telegram_thread_id TEXT NOT NULL,
      executor_id TEXT NOT NULL,
      project_id TEXT NOT NULL REFERENCES projects(id),
      workspace_path TEXT NOT NULL CHECK (length(workspace_path) > 0),
      executor_session_id TEXT REFERENCES executor_sessions(id),
      status TEXT NOT NULL CHECK (status IN ('idle', 'starting', 'running', 'awaiting_approval', 'failed', 'stopped', 'closed')),
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      UNIQUE (telegram_chat_id, telegram_thread_id)
    );
    CREATE TABLE executions (
      id TEXT PRIMARY KEY,
      bot_session_id TEXT NOT NULL REFERENCES bot_sessions(id),
      requested_by_user_id TEXT NOT NULL,
      prompt TEXT NOT NULL CHECK (length(prompt) > 0),
      status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'awaiting_approval', 'completed', 'failed', 'stopped', 'interrupted', 'unknown')),
      correlation_id TEXT NOT NULL UNIQUE,
      executor_session_id TEXT REFERENCES executor_sessions(id),
      started_at INTEGER,
      finished_at INTEGER,
      error_message TEXT
    );
    CREATE TABLE telegram_topics (
      chat_id TEXT NOT NULL,
      thread_id TEXT NOT NULL,
      session_id TEXT REFERENCES bot_sessions(id),
      kind TEXT NOT NULL CHECK (kind IN ('control', 'workspace')),
      title TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('open', 'closed', 'deleted')),
      updated_at INTEGER NOT NULL,
      PRIMARY KEY (chat_id, thread_id)
    );
    CREATE TABLE audit_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      action TEXT NOT NULL,
      user_id TEXT,
      chat_id TEXT,
      session_id TEXT REFERENCES bot_sessions(id),
      execution_id TEXT REFERENCES executions(id),
      correlation_id TEXT NOT NULL,
      metadata TEXT,
      created_at INTEGER NOT NULL
    );
    CREATE INDEX idx_bot_sessions_project ON bot_sessions(project_id);
    CREATE INDEX idx_executions_session ON executions(bot_session_id);
    CREATE INDEX idx_executions_status ON executions(status);
    CREATE INDEX idx_executor_sessions_native ON executor_sessions(executor_id, native_session_id);
    CREATE INDEX idx_topics_session ON telegram_topics(session_id);
  `);
  db.run("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY)");
  db.run("INSERT INTO schema_migrations (version) VALUES (1), (2), (3)");
}

function applyM3ToM2Fixture(db: Database): void {
  db.run(`
    ALTER TABLE executions ADD COLUMN owner_id TEXT;
    ALTER TABLE executions ADD COLUMN owner_fence INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE executions ADD COLUMN lease_expires_at INTEGER;
    ALTER TABLE executions ADD COLUMN cancel_requested_at INTEGER;
    ALTER TABLE executions ADD COLUMN cancel_requested_by_user_id TEXT;
    CREATE TABLE execution_jobs (
      execution_id TEXT PRIMARY KEY REFERENCES executions(id) ON DELETE CASCADE,
      status TEXT NOT NULL CHECK (status IN ('queued', 'processing', 'cancelled', 'completed')),
      worker_id TEXT,
      lease_expires_at INTEGER,
      attempts INTEGER NOT NULL DEFAULT 0,
      available_at INTEGER NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE INDEX idx_executions_session_active ON executions(bot_session_id, status, lease_expires_at);
    CREATE INDEX idx_execution_jobs_ready ON execution_jobs(status, available_at, lease_expires_at);
  `);
  db.run("INSERT INTO schema_migrations (version) VALUES (4)");
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
