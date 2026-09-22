import { Database } from "bun:sqlite";
import type {
  AuditLog,
  BotSession,
  BotSessionRepository,
  Execution,
  ExecutionRepository,
  Persistence,
  Project,
  ProjectRepository,
  TelegramTopic,
  TelegramTopicRepository,
} from "./domain.js";

interface ProjectRow {
  id: string;
  name: string;
  workspace_path: string;
  allowed_executor_ids: string;
}
interface SessionRow {
  id: string;
  telegram_chat_id: string;
  telegram_thread_id: string;
  executor_id: string;
  project_id: string;
  workspace_path: string;
  agent_session_id: string | null;
  status: BotSession["status"];
  created_at: number;
  updated_at: number;
}
interface ExecutionRow {
  id: string;
  bot_session_id: string;
  requested_by_user_id: string;
  prompt: string;
  status: Execution["status"];
  correlation_id: string;
  agent_session_id: string | null;
  started_at: number | null;
  finished_at: number | null;
  error_message: string | null;
}
interface TopicRow {
  chat_id: string;
  thread_id: string;
  session_id: string | null;
  kind: TelegramTopic["kind"];
  title: string;
  status: TelegramTopic["status"];
  updated_at: number;
}

export class SqlitePersistence implements Persistence {
  readonly projects: ProjectRepository;
  readonly sessions: BotSessionRepository;
  readonly executions: ExecutionRepository;
  readonly topics: TelegramTopicRepository;
  readonly audit: AuditLog;

  constructor(readonly db: Database) {
    migrate(db);
    this.projects = new SqliteProjects(db);
    this.sessions = new SqliteSessions(db);
    this.executions = new SqliteExecutions(db);
    this.topics = new SqliteTopics(db);
    this.audit = new SqliteAuditLog(db);
  }

  static open(path: string): SqlitePersistence {
    return new SqlitePersistence(new Database(path));
  }

  saveProject(project: Project): void {
    this.db
      .query(
        "INSERT INTO projects (id, name, workspace_path, allowed_executor_ids) VALUES (?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET name=excluded.name, workspace_path=excluded.workspace_path, allowed_executor_ids=excluded.allowed_executor_ids",
      )
      .run(
        project.id,
        project.name,
        project.workspacePath,
        JSON.stringify(project.allowedExecutorIds),
      );
  }
}

function migrate(db: Database): void {
  db.run(`
    CREATE TABLE IF NOT EXISTS projects (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      workspace_path TEXT NOT NULL,
      allowed_executor_ids TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS bot_sessions (
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
    CREATE TABLE IF NOT EXISTS executions (
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
    CREATE TABLE IF NOT EXISTS telegram_topics (
      chat_id TEXT NOT NULL,
      thread_id TEXT NOT NULL,
      session_id TEXT,
      kind TEXT NOT NULL,
      title TEXT NOT NULL,
      status TEXT NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY (chat_id, thread_id)
    );
    CREATE TABLE IF NOT EXISTS audit_log (
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

class SqliteProjects implements ProjectRepository {
  constructor(private readonly db: Database) {}
  async getById(id: string): Promise<Project | undefined> {
    const row = this.db.query<ProjectRow, any>("SELECT * FROM projects WHERE id = ?").get(id);
    return row
      ? {
          id: row.id,
          name: row.name,
          workspacePath: row.workspace_path,
          allowedExecutorIds: JSON.parse(row.allowed_executor_ids) as string[],
        }
      : undefined;
  }
  async list(): Promise<readonly Project[]> {
    return this.db
      .query<ProjectRow, any>("SELECT * FROM projects ORDER BY id")
      .all()
      .map((row) => ({
        id: row.id,
        name: row.name,
        workspacePath: row.workspace_path,
        allowedExecutorIds: JSON.parse(row.allowed_executor_ids) as string[],
      }));
  }
}

class SqliteSessions implements BotSessionRepository {
  constructor(private readonly db: Database) {}
  async getById(id: string): Promise<BotSession | undefined> {
    return this.map(
      this.db.query<SessionRow, any>("SELECT * FROM bot_sessions WHERE id = ?").get(id),
    );
  }
  async getByTopic(chatId: string, threadId: string): Promise<BotSession | undefined> {
    return this.map(
      this.db
        .query<SessionRow, any>(
          "SELECT * FROM bot_sessions WHERE telegram_chat_id = ? AND telegram_thread_id = ?",
        )
        .get(chatId, threadId),
    );
  }
  async save(session: BotSession): Promise<void> {
    this.db
      .query(
        `INSERT INTO bot_sessions (id, telegram_chat_id, telegram_thread_id, executor_id, project_id, workspace_path, agent_session_id, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET telegram_chat_id=excluded.telegram_chat_id, telegram_thread_id=excluded.telegram_thread_id, executor_id=excluded.executor_id, project_id=excluded.project_id, workspace_path=excluded.workspace_path, agent_session_id=excluded.agent_session_id, status=excluded.status, updated_at=excluded.updated_at`,
      )
      .run(
        session.id,
        session.telegramChatId,
        session.telegramThreadId,
        session.executorId,
        session.projectId,
        session.workspacePath,
        session.agentSessionId ?? null,
        session.status,
        session.createdAt.getTime(),
        session.updatedAt.getTime(),
      );
  }
  async list(): Promise<readonly BotSession[]> {
    return this.db
      .query<SessionRow, any>("SELECT * FROM bot_sessions ORDER BY created_at")
      .all()
      .flatMap((row) => {
        const value = this.map(row);
        return value ? [value] : [];
      });
  }
  private map(row: SessionRow | null | undefined): BotSession | undefined {
    if (!row) return undefined;
    return {
      id: row.id,
      telegramChatId: row.telegram_chat_id,
      telegramThreadId: row.telegram_thread_id,
      executorId: row.executor_id,
      projectId: row.project_id,
      workspacePath: row.workspace_path,
      ...(row.agent_session_id ? { agentSessionId: row.agent_session_id } : {}),
      status: row.status,
      createdAt: new Date(row.created_at),
      updatedAt: new Date(row.updated_at),
    };
  }
}

class SqliteExecutions implements ExecutionRepository {
  constructor(private readonly db: Database) {}
  async getById(id: string): Promise<Execution | undefined> {
    return this.map(
      this.db.query<ExecutionRow, any>("SELECT * FROM executions WHERE id = ?").get(id),
    );
  }
  async save(execution: Execution): Promise<void> {
    this.db
      .query(
        `INSERT INTO executions (id, bot_session_id, requested_by_user_id, prompt, status, correlation_id, agent_session_id, started_at, finished_at, error_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET status=excluded.status, agent_session_id=excluded.agent_session_id, started_at=excluded.started_at, finished_at=excluded.finished_at, error_message=excluded.error_message`,
      )
      .run(
        execution.id,
        execution.botSessionId,
        execution.requestedByUserId,
        execution.prompt,
        execution.status,
        execution.correlationId,
        execution.agentSessionId ?? null,
        execution.startedAt?.getTime() ?? null,
        execution.finishedAt?.getTime() ?? null,
        execution.errorMessage ?? null,
      );
  }
  async listBySession(sessionId: string): Promise<readonly Execution[]> {
    return this.db
      .query<ExecutionRow, any>(
        "SELECT * FROM executions WHERE bot_session_id = ? ORDER BY COALESCE(started_at, 0), id",
      )
      .all(sessionId)
      .flatMap((row) => {
        const value = this.map(row);
        return value ? [value] : [];
      });
  }
  private map(row: ExecutionRow | null | undefined): Execution | undefined {
    if (!row) return undefined;
    return {
      id: row.id,
      botSessionId: row.bot_session_id,
      requestedByUserId: row.requested_by_user_id,
      prompt: row.prompt,
      status: row.status,
      correlationId: row.correlation_id,
      ...(row.agent_session_id ? { agentSessionId: row.agent_session_id } : {}),
      ...(row.started_at === null ? {} : { startedAt: new Date(row.started_at) }),
      ...(row.finished_at === null ? {} : { finishedAt: new Date(row.finished_at) }),
      ...(row.error_message ? { errorMessage: row.error_message } : {}),
    };
  }
}

class SqliteTopics implements TelegramTopicRepository {
  constructor(private readonly db: Database) {}
  async get(chatId: string, threadId: string): Promise<TelegramTopic | undefined> {
    return this.map(
      this.db
        .query<TopicRow, any>("SELECT * FROM telegram_topics WHERE chat_id = ? AND thread_id = ?")
        .get(chatId, threadId),
    );
  }
  async save(topic: TelegramTopic): Promise<void> {
    this.db
      .query(
        `INSERT INTO telegram_topics (chat_id, thread_id, session_id, kind, title, status, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(chat_id, thread_id) DO UPDATE SET session_id=excluded.session_id, kind=excluded.kind, title=excluded.title, status=excluded.status, updated_at=excluded.updated_at`,
      )
      .run(
        topic.chatId,
        topic.threadId,
        topic.sessionId ?? null,
        topic.kind,
        topic.title,
        topic.status,
        topic.updatedAt.getTime(),
      );
  }
  private map(row: TopicRow | null | undefined): TelegramTopic | undefined {
    return row
      ? {
          chatId: row.chat_id,
          threadId: row.thread_id,
          ...(row.session_id ? { sessionId: row.session_id } : {}),
          kind: row.kind,
          title: row.title,
          status: row.status,
          updatedAt: new Date(row.updated_at),
        }
      : undefined;
  }
}

class SqliteAuditLog implements AuditLog {
  constructor(private readonly db: Database) {}
  async append(entry: Parameters<AuditLog["append"]>[0]): Promise<void> {
    this.db
      .query(
        "INSERT INTO audit_log (action, user_id, chat_id, session_id, execution_id, correlation_id, metadata, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      )
      .run(
        entry.action,
        entry.userId ?? null,
        entry.chatId ?? null,
        entry.sessionId ?? null,
        entry.executionId ?? null,
        entry.correlationId,
        entry.metadata ? JSON.stringify(entry.metadata) : null,
        Date.now(),
      );
  }
}
