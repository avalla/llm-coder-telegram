import { Database } from "bun:sqlite";
import type {
  AuditLog,
  BotSession,
  BotSessionRepository,
  Execution,
  ExecutionJob,
  ExecutionJobDisposition,
  ExecutionJobHandler,
  ExecutionJobQueue,
  ExecutionJobWorker,
  ExecutionLease,
  ExecutionRepository,
  ExecutionReconciliation,
  ExecutionReconciliationRepository,
  ExecutionReconciliationResult,
  ExecutorSession,
  ExecutorSessionRepository,
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
interface ExecutorSessionRow {
  id: string;
  executor_id: string;
  native_session_id: string | null;
  project_id: string;
  workspace_path: string;
  host_id: string | null;
  legacy_runtime_session_id: string | null;
  resumable: number;
  created_at: number;
  last_used_at: number;
}
interface SessionRow {
  id: string;
  telegram_chat_id: string;
  telegram_thread_id: string;
  executor_id: string;
  project_id: string;
  workspace_path: string;
  executor_session_id: string | null;
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
  executor_session_id: string | null;
  started_at: number | null;
  finished_at: number | null;
  error_message: string | null;
  owner_id: string | null;
  owner_fence: number;
  lease_expires_at: number | null;
  cancel_requested_at: number | null;
  cancel_requested_by_user_id: string | null;
}
interface ExecutionJobRow {
  execution_id: string;
  status: "queued" | "processing" | "cancelled" | "completed";
  worker_id: string | null;
  lease_expires_at: number | null;
  attempts: number;
  available_at: number;
  created_at: number;
  updated_at: number;
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
interface ExecutionReconciliationRow {
  execution_id: string;
  outcome: ExecutionReconciliation["outcome"];
  reconciled_by_user_id: string;
  note: string;
  reconciled_at: number;
}

export class SqlitePersistence implements Persistence {
  readonly projects: ProjectRepository;
  readonly sessions: BotSessionRepository;
  readonly executions: ExecutionRepository;
  readonly executorSessions: ExecutorSessionRepository;
  readonly topics: TelegramTopicRepository;
  readonly reconciliations: ExecutionReconciliationRepository;
  readonly audit: AuditLog;

  constructor(readonly db: Database) {
    configureDatabase(db);
    migrate(db);
    this.projects = new SqliteProjects(db);
    this.sessions = new SqliteSessions(db);
    this.executions = new SqliteExecutions(db);
    this.executorSessions = new SqliteExecutorSessions(db);
    this.topics = new SqliteTopics(db);
    this.reconciliations = new SqliteReconciliations(db);
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

// Job leases govern delivery ownership; execution leases/fences govern authority to mutate
// orchestration state. Reclaiming either lease never authorizes provider replay.
const DEFAULT_JOB_LEASE_MS = 30_000;
const DEFAULT_JOB_POLL_MS = 25;

export interface SqliteExecutionJobQueueOptions {
  concurrency?: number;
  leaseDurationMs?: number;
  pollIntervalMs?: number;
}

export class SqliteExecutionJobQueue implements ExecutionJobQueue, ExecutionJobWorker {
  private readonly workerId = `sqlite-worker-${crypto.randomUUID()}`;
  private readonly concurrency: number;
  private readonly leaseDurationMs: number;
  private readonly pollIntervalMs: number;
  private started = false;
  private closed = false;
  private inFlight = 0;
  private loopPromise?: Promise<void>;

  constructor(
    private readonly db: Database,
    options: SqliteExecutionJobQueueOptions = {},
  ) {
    this.concurrency = Math.max(1, options.concurrency ?? 4);
    this.leaseDurationMs = Math.max(1_000, options.leaseDurationMs ?? DEFAULT_JOB_LEASE_MS);
    this.pollIntervalMs = Math.max(1, options.pollIntervalMs ?? DEFAULT_JOB_POLL_MS);
  }

  async enqueue(job: ExecutionJob): Promise<void> {
    const now = Date.now();
    this.db.run("BEGIN IMMEDIATE");
    try {
      const execution = this.db
        .query<{ status: Execution["status"] }, any>("SELECT status FROM executions WHERE id=?")
        .get(job.executionId);
      if (!execution) throw new Error(`Cannot enqueue unknown execution: ${job.executionId}`);
      if (execution.status === "pending") {
        this.db
          .query(
            `INSERT INTO execution_jobs
              (execution_id, status, attempts, available_at, created_at, updated_at)
             VALUES (?, 'queued', 0, ?, ?, ?)
             ON CONFLICT(execution_id) DO UPDATE SET
               status=CASE WHEN execution_jobs.status IN ('cancelled', 'completed')
                 THEN 'queued' ELSE execution_jobs.status END,
               worker_id=CASE WHEN execution_jobs.status IN ('cancelled', 'completed')
                 THEN NULL ELSE execution_jobs.worker_id END,
               lease_expires_at=CASE WHEN execution_jobs.status IN ('cancelled', 'completed')
                 THEN NULL ELSE execution_jobs.lease_expires_at END,
               available_at=CASE WHEN execution_jobs.status IN ('cancelled', 'completed')
                 THEN excluded.available_at ELSE execution_jobs.available_at END,
               updated_at=excluded.updated_at`,
          )
          .run(job.executionId, now, now, now);
      } else {
        this.db
          .query(
            `INSERT INTO execution_jobs
              (execution_id, status, attempts, available_at, created_at, updated_at)
             VALUES (?, 'completed', 0, ?, ?, ?)
             ON CONFLICT(execution_id) DO NOTHING`,
          )
          .run(job.executionId, now, now, now);
      }
      this.db.run("COMMIT");
    } catch (error) {
      this.db.run("ROLLBACK");
      throw error;
    }
  }

  async cancel(executionId: string): Promise<void> {
    this.db
      .query(
        "UPDATE execution_jobs SET status='cancelled', worker_id=NULL, lease_expires_at=NULL, updated_at=? WHERE execution_id=? AND status='queued'",
      )
      .run(Date.now(), executionId);
  }

  async start(handler: ExecutionJobHandler): Promise<void> {
    if (this.started) throw new Error("Execution job worker already started");
    this.started = true;
    this.loopPromise = this.run(handler);
  }

  async close(): Promise<void> {
    this.closed = true;
    await this.loopPromise;
  }

  private async run(handler: ExecutionJobHandler): Promise<void> {
    while (!this.closed || this.inFlight > 0) {
      if (!this.closed && this.inFlight < this.concurrency) {
        const job = this.claimJob();
        if (job) {
          this.inFlight += 1;
          void this.process(job, handler).finally(() => {
            this.inFlight -= 1;
          });
          continue;
        }
      }
      await wait(this.pollIntervalMs);
    }
  }

  private claimJob(): ExecutionJob | undefined {
    const now = Date.now();
    const leaseExpiresAt = now + this.leaseDurationMs;
    this.db.run("BEGIN IMMEDIATE");
    try {
      const row = this.db
        .query<ExecutionJobRow, any>(
          `SELECT execution_jobs.* FROM execution_jobs
           JOIN executions ON executions.id = execution_jobs.execution_id
           WHERE (
             executions.status='pending' OR
             (executions.status IN ('running', 'awaiting_approval') AND
              executions.lease_expires_at IS NOT NULL AND executions.lease_expires_at <= ?)
           ) AND
           ((execution_jobs.status='queued' AND execution_jobs.available_at <= ?) OR
            (execution_jobs.status='processing' AND execution_jobs.lease_expires_at IS NOT NULL AND execution_jobs.lease_expires_at <= ?))
           ORDER BY execution_jobs.available_at, execution_jobs.created_at, execution_jobs.execution_id LIMIT 1`,
        )
        .get(now, now, now);
      if (!row) {
        this.db.run("COMMIT");
        return undefined;
      }
      const result = this.db
        .query(
          `UPDATE execution_jobs
           SET status='processing', worker_id=?, lease_expires_at=?,
               attempts=attempts+1, updated_at=?
           WHERE execution_id=? AND
             ((status='queued' AND available_at <= ?) OR
              (status='processing' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?))`,
        )
        .run(this.workerId, leaseExpiresAt, now, row.execution_id, now, now);
      if (result.changes !== 1) {
        this.db.run("COMMIT");
        return undefined;
      }
      this.db.run("COMMIT");
      return { executionId: row.execution_id };
    } catch (error) {
      this.db.run("ROLLBACK");
      throw error;
    }
  }

  private async process(job: ExecutionJob, handler: ExecutionJobHandler): Promise<void> {
    const renewTimer = setInterval(
      () => {
        this.db
          .query(
            "UPDATE execution_jobs SET lease_expires_at=?, updated_at=? WHERE execution_id=? AND status='processing' AND worker_id=?",
          )
          .run(Date.now() + this.leaseDurationMs, Date.now(), job.executionId, this.workerId);
      },
      Math.max(250, this.leaseDurationMs / 3),
    );
    let result: ExecutionJobDisposition = { disposition: "retry", delayMs: this.pollIntervalMs };
    try {
      result = await handler(job);
    } catch {
      // Redelivery is orchestration-only: claim() rejects live owners and
      // reclaimed provider states are fail-closed without starting a provider.
      result = { disposition: "retry", delayMs: 100 };
    } finally {
      clearInterval(renewTimer);
    }
    if (result.disposition === "ack") {
      this.db
        .query(
          "UPDATE execution_jobs SET status='completed', worker_id=NULL, lease_expires_at=NULL, updated_at=? WHERE execution_id=? AND status='processing' AND worker_id=?",
        )
        .run(Date.now(), job.executionId, this.workerId);
      return;
    }
    const availableAt = Date.now() + (result.delayMs ?? 100);
    this.db
      .query(
        "UPDATE execution_jobs SET status='queued', worker_id=NULL, lease_expires_at=NULL, available_at=?, updated_at=? WHERE execution_id=? AND status='processing' AND worker_id=?",
      )
      .run(availableAt, Date.now(), job.executionId, this.workerId);
  }
}

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function configureDatabase(db: Database): void {
  db.run("PRAGMA foreign_keys = ON");
  db.run("PRAGMA busy_timeout = 5000");
  db.run("PRAGMA journal_mode = WAL");
  db.run("PRAGMA synchronous = NORMAL");
}

function migrate(db: Database): void {
  db.run("BEGIN IMMEDIATE");
  try {
    db.run("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY)");
    const version =
      db
        .query<{ version: number }, any>(
          "SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations",
        )
        .get()?.version ?? 0;

    if (!tableExists(db, "projects")) {
      createCanonicalSchema(db);
      for (const completedVersion of [1, 2, 3, 4, 5]) {
        recordSchemaVersion(db, completedVersion);
      }
    } else if (version < 3) {
      rebuildPreM2OrIncompleteSchema(db);
      for (const completedVersion of [1, 2, 3, 4, 5]) {
        recordSchemaVersion(db, completedVersion);
      }
    } else {
      if (version < 4) {
        upgradeToM3Schema(db);
        recordSchemaVersion(db, 4);
      }
      if (version < 5) {
        upgradeToM4Schema(db);
        recordSchemaVersion(db, 5);
      }
    }
    db.run("COMMIT");
  } catch (error) {
    db.run("ROLLBACK");
    throw error;
  }
}

function recordSchemaVersion(db: Database, version: number): void {
  db.query("INSERT OR IGNORE INTO schema_migrations (version) VALUES (?)").run(version);
}

function upgradeToM3Schema(db: Database): void {
  for (const statement of [
    "ALTER TABLE executions ADD COLUMN owner_id TEXT",
    "ALTER TABLE executions ADD COLUMN owner_fence INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE executions ADD COLUMN lease_expires_at INTEGER",
    "ALTER TABLE executions ADD COLUMN cancel_requested_at INTEGER",
    "ALTER TABLE executions ADD COLUMN cancel_requested_by_user_id TEXT",
  ]) {
    db.run(statement);
  }
  db.run(`
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
    CREATE INDEX idx_executions_session_active
      ON executions(bot_session_id, status, lease_expires_at);
    CREATE INDEX idx_execution_jobs_ready
      ON execution_jobs(status, available_at, lease_expires_at);
  `);
}

function upgradeToM4Schema(db: Database): void {
  db.run(`
    CREATE TABLE execution_reconciliations (
      execution_id TEXT PRIMARY KEY REFERENCES executions(id) ON DELETE CASCADE,
      outcome TEXT NOT NULL CHECK (outcome IN ('confirmed_completed', 'abandoned')),
      reconciled_by_user_id TEXT NOT NULL CHECK (length(trim(reconciled_by_user_id)) > 0),
      note TEXT NOT NULL CHECK (length(trim(note)) > 0),
      reconciled_at INTEGER NOT NULL
    );
  `);
}

function createCanonicalSchema(db: Database): void {
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
      error_message TEXT,
      owner_id TEXT,
      owner_fence INTEGER NOT NULL DEFAULT 0,
      lease_expires_at INTEGER,
      cancel_requested_at INTEGER,
      cancel_requested_by_user_id TEXT
    );
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
    CREATE TABLE execution_reconciliations (
      execution_id TEXT PRIMARY KEY REFERENCES executions(id) ON DELETE CASCADE,
      outcome TEXT NOT NULL CHECK (outcome IN ('confirmed_completed', 'abandoned')),
      reconciled_by_user_id TEXT NOT NULL CHECK (length(trim(reconciled_by_user_id)) > 0),
      note TEXT NOT NULL CHECK (length(trim(note)) > 0),
      reconciled_at INTEGER NOT NULL
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
    CREATE INDEX idx_executions_session_active
      ON executions(bot_session_id, status, lease_expires_at);
    CREATE INDEX idx_execution_jobs_ready
      ON execution_jobs(status, available_at, lease_expires_at);
    CREATE INDEX idx_executor_sessions_native ON executor_sessions(executor_id, native_session_id);
    CREATE INDEX idx_topics_session ON telegram_topics(session_id);
  `);
}

function rebuildPreM2OrIncompleteSchema(db: Database): void {
  for (const table of [
    "audit_log",
    "telegram_topics",
    "executions",
    "bot_sessions",
    "executor_sessions",
    "projects",
  ]) {
    if (tableExists(db, table)) db.run(`ALTER TABLE ${table} RENAME TO legacy_${table}`);
  }
  const hasLegacyAgentId = columnExists(db, "legacy_bot_sessions", "agent_session_id");
  const hasLegacyExecutionAgentId = columnExists(db, "legacy_executions", "agent_session_id");
  createCanonicalSchema(db);

  db.run(
    "INSERT INTO projects SELECT id, name, workspace_path, allowed_executor_ids FROM legacy_projects",
  );

  if (!hasLegacyAgentId && tableExists(db, "legacy_executor_sessions")) {
    const hasLegacyRuntimeId = columnExists(
      db,
      "legacy_executor_sessions",
      "legacy_runtime_session_id",
    );
    db.run(`
      INSERT INTO executor_sessions
        (id, executor_id, native_session_id, project_id, workspace_path, host_id, legacy_runtime_session_id, resumable, created_at, last_used_at)
      SELECT id, executor_id, native_session_id, project_id, workspace_path, host_id,
        ${hasLegacyRuntimeId ? "legacy_runtime_session_id" : "NULL"},
        CASE WHEN resumable = 1 AND native_session_id IS NOT NULL THEN 1 ELSE 0 END,
        created_at, last_used_at
      FROM legacy_executor_sessions
    `);
  }

  if (hasLegacyAgentId) {
    db.run(`
      INSERT INTO executor_sessions
        (id, executor_id, native_session_id, project_id, workspace_path, legacy_runtime_session_id, resumable, created_at, last_used_at)
      SELECT 'legacy-runtime-' || id, executor_id, NULL, project_id, workspace_path, agent_session_id, 0, created_at, updated_at
      FROM legacy_bot_sessions
      WHERE agent_session_id IS NOT NULL
    `);
  }

  const sessionExecutorId = hasLegacyAgentId
    ? "CASE WHEN agent_session_id IS NULL THEN NULL ELSE 'legacy-runtime-' || id END"
    : "executor_session_id";
  db.run(`
    INSERT INTO bot_sessions
      (id, telegram_chat_id, telegram_thread_id, executor_id, project_id, workspace_path, executor_session_id, status, created_at, updated_at)
    SELECT id, telegram_chat_id, telegram_thread_id, executor_id, project_id, workspace_path,
      ${sessionExecutorId}, status, created_at, updated_at
    FROM legacy_bot_sessions
  `);

  if (hasLegacyExecutionAgentId) {
    db.run(`
      INSERT INTO executor_sessions
        (id, executor_id, native_session_id, project_id, workspace_path, legacy_runtime_session_id, resumable, created_at, last_used_at)
      SELECT 'legacy-execution-' || e.id, s.executor_id, NULL, s.project_id, s.workspace_path, e.agent_session_id, 0,
        COALESCE(e.started_at, s.created_at), COALESCE(e.finished_at, s.updated_at)
      FROM legacy_executions e
      JOIN legacy_bot_sessions s ON s.id = e.bot_session_id
      WHERE e.agent_session_id IS NOT NULL
    `);
  }
  const executionExecutorId = hasLegacyExecutionAgentId
    ? "CASE WHEN agent_session_id IS NULL THEN NULL ELSE 'legacy-execution-' || id END"
    : "executor_session_id";
  db.run(`
    INSERT INTO executions
      (id, bot_session_id, requested_by_user_id, prompt, status, correlation_id, executor_session_id, started_at, finished_at, error_message)
    SELECT id, bot_session_id, requested_by_user_id, prompt, status, correlation_id,
      ${executionExecutorId}, started_at, finished_at, error_message
    FROM legacy_executions
  `);
  db.run(`
    INSERT INTO telegram_topics (chat_id, thread_id, session_id, kind, title, status, updated_at)
    SELECT chat_id, thread_id, session_id, kind, title, status, updated_at FROM legacy_telegram_topics
  `);
  db.run(`
    INSERT INTO audit_log (id, action, user_id, chat_id, session_id, execution_id, correlation_id, metadata, created_at)
    SELECT id, action, user_id, chat_id, session_id, execution_id, correlation_id, metadata, created_at FROM legacy_audit_log
  `);

  for (const table of [
    "audit_log",
    "telegram_topics",
    "executions",
    "bot_sessions",
    "executor_sessions",
    "projects",
  ]) {
    if (tableExists(db, `legacy_${table}`)) db.run(`DROP TABLE legacy_${table}`);
  }
}

function tableExists(db: Database, table: string): boolean {
  return Boolean(
    db
      .query<{ name: string }, any>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
      )
      .get(table),
  );
}

function columnExists(db: Database, table: string, column: string): boolean {
  return (
    tableExists(db, table) &&
    db
      .query<{ name: string }, any>(`PRAGMA table_info(${table})`)
      .all()
      .some((item) => item.name === column)
  );
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

class SqliteExecutorSessions implements ExecutorSessionRepository {
  constructor(private readonly db: Database) {}

  async getById(id: string): Promise<ExecutorSession | undefined> {
    const row = this.db
      .query<ExecutorSessionRow, any>("SELECT * FROM executor_sessions WHERE id = ?")
      .get(id);
    return row ? mapExecutorSession(row) : undefined;
  }

  async save(session: ExecutorSession): Promise<void> {
    this.db
      .query(
        `INSERT INTO executor_sessions (id, executor_id, native_session_id, project_id, workspace_path, host_id, legacy_runtime_session_id, resumable, created_at, last_used_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET executor_id=excluded.executor_id, native_session_id=excluded.native_session_id, project_id=excluded.project_id, workspace_path=excluded.workspace_path, host_id=excluded.host_id, legacy_runtime_session_id=excluded.legacy_runtime_session_id, resumable=excluded.resumable, last_used_at=excluded.last_used_at`,
      )
      .run(
        session.id,
        session.executorId,
        session.nativeSessionId ?? null,
        session.projectId,
        session.workspacePath,
        session.hostId ?? null,
        session.legacyRuntimeSessionId ?? null,
        session.resumable ? 1 : 0,
        session.createdAt.getTime(),
        session.lastUsedAt.getTime(),
      );
  }

  async updateOwned(
    session: ExecutorSession,
    executionId: string,
    ownerId: string,
    fence: number,
    now: Date,
  ): Promise<boolean> {
    const result = this.db
      .query(
        `INSERT INTO executor_sessions
           (id, executor_id, native_session_id, project_id, workspace_path, host_id,
            legacy_runtime_session_id, resumable, created_at, last_used_at)
         SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
         WHERE EXISTS (
           SELECT 1 FROM executions
           WHERE id=? AND (executor_session_id IS NULL OR executor_session_id=?)
             AND owner_id=? AND owner_fence=? AND lease_expires_at > ?
         )
         ON CONFLICT(id) DO UPDATE SET
           executor_id=excluded.executor_id,
           native_session_id=excluded.native_session_id,
           project_id=excluded.project_id,
           workspace_path=excluded.workspace_path,
           host_id=excluded.host_id,
           legacy_runtime_session_id=excluded.legacy_runtime_session_id,
           resumable=excluded.resumable,
           last_used_at=excluded.last_used_at`,
      )
      .run(
        session.id,
        session.executorId,
        session.nativeSessionId ?? null,
        session.projectId,
        session.workspacePath,
        session.hostId ?? null,
        session.legacyRuntimeSessionId ?? null,
        session.resumable ? 1 : 0,
        session.createdAt.getTime(),
        session.lastUsedAt.getTime(),
        executionId,
        session.id,
        ownerId,
        fence,
        now.getTime(),
      );
    return result.changes === 1;
  }
}

function mapExecutorSession(row: ExecutorSessionRow): ExecutorSession {
  return {
    id: row.id,
    executorId: row.executor_id,
    projectId: row.project_id,
    workspacePath: row.workspace_path,
    ...(row.legacy_runtime_session_id
      ? { legacyRuntimeSessionId: row.legacy_runtime_session_id }
      : {}),
    ...(row.native_session_id ? { nativeSessionId: row.native_session_id } : {}),
    ...(row.host_id ? { hostId: row.host_id } : {}),
    resumable: row.resumable === 1,
    createdAt: new Date(row.created_at),
    lastUsedAt: new Date(row.last_used_at),
  };
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
        `INSERT INTO bot_sessions (id, telegram_chat_id, telegram_thread_id, executor_id, project_id, workspace_path, executor_session_id, status, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET telegram_chat_id=excluded.telegram_chat_id, telegram_thread_id=excluded.telegram_thread_id, executor_id=excluded.executor_id, project_id=excluded.project_id, workspace_path=excluded.workspace_path, executor_session_id=excluded.executor_session_id, status=excluded.status, updated_at=excluded.updated_at`,
      )
      .run(
        session.id,
        session.telegramChatId,
        session.telegramThreadId,
        session.executorId,
        session.projectId,
        session.workspacePath,
        session.executorSessionId ?? null,
        session.status,
        session.createdAt.getTime(),
        session.updatedAt.getTime(),
      );
  }

  async updateOwned(
    session: BotSession,
    executionId: string,
    ownerId: string,
    fence: number,
    now: Date,
  ): Promise<boolean> {
    const result = this.db
      .query(
        `UPDATE bot_sessions
         SET telegram_chat_id=?, telegram_thread_id=?, executor_id=?, project_id=?,
             workspace_path=?, executor_session_id=?, status=?, updated_at=?
         WHERE id=? AND EXISTS (
           SELECT 1 FROM executions
           WHERE id=? AND bot_session_id=bot_sessions.id AND owner_id=? AND owner_fence=?
             AND lease_expires_at > ?
         )`,
      )
      .run(
        session.telegramChatId,
        session.telegramThreadId,
        session.executorId,
        session.projectId,
        session.workspacePath,
        session.executorSessionId ?? null,
        session.status,
        session.updatedAt.getTime(),
        session.id,
        executionId,
        ownerId,
        fence,
        now.getTime(),
      );
    return result.changes === 1;
  }

  async list(): Promise<readonly BotSession[]> {
    return this.db
      .query<SessionRow, any>("SELECT * FROM bot_sessions ORDER BY created_at")
      .all()
      .map((row) => this.map(row)!);
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
      ...(row.executor_session_id ? { executorSessionId: row.executor_session_id } : {}),
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
    this.db.run("BEGIN IMMEDIATE");
    try {
      this.db
        .query(
          `INSERT INTO executions
            (id, bot_session_id, requested_by_user_id, prompt, status, correlation_id,
             executor_session_id, started_at, finished_at, error_message, owner_id,
             owner_fence, lease_expires_at, cancel_requested_at, cancel_requested_by_user_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET status=excluded.status,
             executor_session_id=excluded.executor_session_id, started_at=excluded.started_at,
             finished_at=excluded.finished_at, error_message=excluded.error_message,
             owner_id=excluded.owner_id, owner_fence=excluded.owner_fence,
             lease_expires_at=excluded.lease_expires_at,
             cancel_requested_at=excluded.cancel_requested_at,
             cancel_requested_by_user_id=excluded.cancel_requested_by_user_id`,
        )
        .run(
          execution.id,
          execution.botSessionId,
          execution.requestedByUserId,
          execution.prompt,
          execution.status,
          execution.correlationId,
          execution.executorSessionId ?? null,
          execution.startedAt?.getTime() ?? null,
          execution.finishedAt?.getTime() ?? null,
          execution.errorMessage ?? null,
          execution.ownerId ?? null,
          execution.ownerFence,
          execution.leaseExpiresAt?.getTime() ?? null,
          execution.cancelRequestedAt?.getTime() ?? null,
          execution.cancelRequestedByUserId ?? null,
        );
      // A pending execution and its delivery intent commit as one durable unit.
      if (execution.status === "unknown") {
        this.db
          .query(
            "UPDATE bot_sessions SET status=CASE WHEN status='closed' THEN 'closed' ELSE 'failed' END, updated_at=? WHERE id=?",
          )
          .run(Date.now(), execution.botSessionId);
      }
      if (execution.status === "pending") {
        const now = Date.now();
        this.db
          .query(
            `INSERT INTO execution_jobs
              (execution_id, status, attempts, available_at, created_at, updated_at)
             VALUES (?, 'queued', 0, ?, ?, ?)
             ON CONFLICT(execution_id) DO UPDATE SET
               status=CASE WHEN execution_jobs.status IN ('cancelled', 'completed')
                 THEN 'queued' ELSE execution_jobs.status END,
               worker_id=CASE WHEN execution_jobs.status IN ('cancelled', 'completed')
                 THEN NULL ELSE execution_jobs.worker_id END,
               lease_expires_at=CASE WHEN execution_jobs.status IN ('cancelled', 'completed')
                 THEN NULL ELSE execution_jobs.lease_expires_at END,
               available_at=CASE WHEN execution_jobs.status IN ('cancelled', 'completed')
                 THEN excluded.available_at ELSE execution_jobs.available_at END,
               updated_at=excluded.updated_at`,
          )
          .run(execution.id, now, now, now);
      }
      this.db.run("COMMIT");
    } catch (error) {
      this.db.run("ROLLBACK");
      throw error;
    }
  }

  async listBySession(sessionId: string): Promise<readonly Execution[]> {
    return this.db
      .query<ExecutionRow, any>(
        "SELECT * FROM executions WHERE bot_session_id = ? ORDER BY COALESCE(started_at, 0), id",
      )
      .all(sessionId)
      .map((row) => this.map(row)!);
  }

  async claim(
    executionId: string,
    ownerId: string,
    now: Date,
    leaseDurationMs: number,
  ): Promise<ExecutionLease | undefined> {
    const nowMs = now.getTime();
    const leaseExpiresAt = new Date(nowMs + leaseDurationMs);
    this.db.run("BEGIN IMMEDIATE");
    try {
      const row = this.db
        .query<ExecutionRow, any>("SELECT * FROM executions WHERE id = ?")
        .get(executionId);
      const recovered = row?.status === "running" || row?.status === "awaiting_approval";
      if (
        !row ||
        (!recovered && row.status !== "pending") ||
        (recovered && (row.lease_expires_at === null || row.lease_expires_at > nowMs))
      ) {
        this.db.run("COMMIT");
        return undefined;
      }
      const unresolvedUnknown = this.db
        .query<{ id: string }, any>(
          `SELECT id FROM executions
           WHERE bot_session_id = ? AND status='unknown'
             AND NOT EXISTS (
               SELECT 1 FROM execution_reconciliations
               WHERE execution_reconciliations.execution_id = executions.id
             )
           LIMIT 1`,
        )
        .get(row.bot_session_id);
      if (unresolvedUnknown) {
        this.db.run("COMMIT");
        return undefined;
      }
      const active = this.db
        .query<{ id: string }, any>(
          `SELECT id FROM executions
           WHERE bot_session_id = ? AND id <> ?
             AND status IN ('running', 'awaiting_approval')
           LIMIT 1`,
        )
        .get(row.bot_session_id, executionId);
      if (active) {
        this.db.run("COMMIT");
        return undefined;
      }
      const fence = row.owner_fence + 1;
      const result = this.db
        .query(
          `UPDATE executions
           SET status='running', owner_id=?, owner_fence=?, lease_expires_at=?,
               started_at=COALESCE(started_at, ?)
           WHERE id=? AND (status='pending' OR
              (status IN ('running', 'awaiting_approval') AND lease_expires_at <= ?))`,
        )
        .run(ownerId, fence, leaseExpiresAt.getTime(), nowMs, executionId, nowMs);
      if (result.changes !== 1) {
        this.db.run("COMMIT");
        return undefined;
      }
      this.db.run("COMMIT");
      return {
        executionId,
        botSessionId: row.bot_session_id,
        ownerId,
        fence,
        leaseExpiresAt,
        recovered,
      };
    } catch (error) {
      this.db.run("ROLLBACK");
      throw error;
    }
  }

  async renew(
    executionId: string,
    ownerId: string,
    fence: number,
    now: Date,
    leaseDurationMs: number,
  ): Promise<boolean> {
    const nowMs = now.getTime();
    const result = this.db
      .query(
        `UPDATE executions SET lease_expires_at=?
         WHERE id=? AND owner_id=? AND owner_fence=?
           AND status IN ('running', 'awaiting_approval')
           AND lease_expires_at IS NOT NULL AND lease_expires_at > ?`,
      )
      .run(nowMs + leaseDurationMs, executionId, ownerId, fence, nowMs);
    return result.changes === 1;
  }

  async updateOwned(
    execution: Execution,
    ownerId: string,
    fence: number,
    now: Date,
  ): Promise<boolean> {
    const result = this.db
      .query(
        `UPDATE executions
         SET status=?, executor_session_id=?, started_at=?, finished_at=?, error_message=?,
             owner_id=?, owner_fence=?, lease_expires_at=?,
             cancel_requested_at=?, cancel_requested_by_user_id=?
         WHERE id=? AND owner_id=? AND owner_fence=? AND lease_expires_at > ?`,
      )
      .run(
        execution.status,
        execution.executorSessionId ?? null,
        execution.startedAt?.getTime() ?? null,
        execution.finishedAt?.getTime() ?? null,
        execution.errorMessage ?? null,
        execution.status === "completed" ||
          execution.status === "failed" ||
          execution.status === "stopped" ||
          execution.status === "interrupted" ||
          execution.status === "unknown"
          ? null
          : ownerId,
        fence,
        execution.status === "completed" ||
          execution.status === "failed" ||
          execution.status === "stopped" ||
          execution.status === "interrupted" ||
          execution.status === "unknown"
          ? null
          : (execution.leaseExpiresAt?.getTime() ?? null),
        execution.cancelRequestedAt?.getTime() ?? null,
        execution.cancelRequestedByUserId ?? null,
        execution.id,
        ownerId,
        fence,
        now.getTime(),
      );
    return result.changes === 1;
  }

  async requestCancellation(
    executionId: string,
    requestedByUserId: string,
    now: Date,
  ): Promise<Execution | undefined> {
    const nowMs = now.getTime();
    this.db.run("BEGIN IMMEDIATE");
    try {
      const row = this.db
        .query<{ status: Execution["status"]; owner_id: string | null }, any>(
          "SELECT status, owner_id FROM executions WHERE id=?",
        )
        .get(executionId);
      if (!row || !["pending", "running", "awaiting_approval"].includes(row.status)) {
        this.db.run("COMMIT");
        return this.getById(executionId);
      }
      const stopped = row.status === "pending" && row.owner_id === null;
      this.db
        .query(
          `UPDATE executions
           SET cancel_requested_at=?, cancel_requested_by_user_id=?,
               status=CASE WHEN status='pending' AND owner_id IS NULL THEN 'stopped' ELSE status END,
               finished_at=CASE WHEN status='pending' AND owner_id IS NULL THEN ? ELSE finished_at END,
               error_message=CASE WHEN status='pending' AND owner_id IS NULL THEN 'Execution stopped before start.' ELSE error_message END
           WHERE id=? AND status IN ('pending', 'running', 'awaiting_approval')`,
        )
        .run(nowMs, requestedByUserId, nowMs, executionId);
      if (stopped) {
        this.db
          .query(
            "UPDATE execution_jobs SET status='cancelled', worker_id=NULL, lease_expires_at=NULL, updated_at=? WHERE execution_id=? AND status IN ('queued', 'processing')",
          )
          .run(nowMs, executionId);
      }
      this.db.run("COMMIT");
      return this.getById(executionId);
    } catch (error) {
      this.db.run("ROLLBACK");
      throw error;
    }
  }

  async recoverAfterRestart(now: Date): Promise<readonly Execution[]> {
    const nowMs = now.getTime();
    this.db.run("BEGIN IMMEDIATE");
    try {
      const interruptedIds = this.db
        .query<{ id: string }, any>(
          "SELECT id FROM executions WHERE status IN ('running', 'awaiting_approval')",
        )
        .all()
        .map((row) => row.id);
      this.db
        .query(
          `UPDATE bot_sessions
           SET status=CASE WHEN status='closed' THEN 'closed' ELSE 'failed' END, updated_at=?
           WHERE id IN (
             SELECT bot_session_id FROM executions
             WHERE status IN ('running', 'awaiting_approval')
           )`,
        )
        .run(nowMs);
      this.db
        .query(
          `UPDATE executions
           SET status='unknown', finished_at=?, error_message=?,
               owner_id=NULL, lease_expires_at=NULL
           WHERE status IN ('running', 'awaiting_approval')`,
        )
        .run(nowMs, "Execution owner was lost during restart; provider state was not guessed.");
      const interrupted = interruptedIds
        .map((id) =>
          this.map(
            this.db.query<ExecutionRow, any>("SELECT * FROM executions WHERE id = ?").get(id),
          ),
        )
        .filter((execution): execution is Execution => execution !== undefined);
      this.db
        .query("UPDATE executions SET owner_id=NULL, lease_expires_at=NULL WHERE status='pending'")
        .run();
      this.db
        .query(
          "UPDATE execution_jobs SET status='completed', worker_id=NULL, lease_expires_at=NULL, updated_at=? WHERE execution_id IN (SELECT id FROM executions WHERE status='unknown')",
        )
        .run(nowMs);
      this.db
        .query(
          "UPDATE execution_jobs SET status='queued', worker_id=NULL, lease_expires_at=NULL, available_at=?, updated_at=? WHERE execution_id IN (SELECT id FROM executions WHERE status='pending')",
        )
        .run(nowMs, nowMs);
      this.db
        .query(
          `INSERT INTO execution_jobs (execution_id, status, attempts, available_at, created_at, updated_at)
           SELECT id, 'queued', 0, ?, ?, ? FROM executions
           WHERE status='pending' AND id NOT IN (SELECT execution_id FROM execution_jobs)`,
        )
        .run(nowMs, nowMs, nowMs);
      const pending = this.db
        .query<ExecutionRow, any>("SELECT * FROM executions WHERE status='pending'")
        .all()
        .map((row) => this.map(row)!);
      this.db.run("COMMIT");
      return [...interrupted, ...pending];
    } catch (error) {
      this.db.run("ROLLBACK");
      throw error;
    }
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
      ...(row.executor_session_id ? { executorSessionId: row.executor_session_id } : {}),
      ...(row.started_at === null ? {} : { startedAt: new Date(row.started_at) }),
      ...(row.finished_at === null ? {} : { finishedAt: new Date(row.finished_at) }),
      ...(row.error_message ? { errorMessage: row.error_message } : {}),
      ...(row.owner_id ? { ownerId: row.owner_id } : {}),
      ownerFence: row.owner_fence,
      ...(row.lease_expires_at === null ? {} : { leaseExpiresAt: new Date(row.lease_expires_at) }),
      ...(row.cancel_requested_at === null
        ? {}
        : { cancelRequestedAt: new Date(row.cancel_requested_at) }),
      ...(row.cancel_requested_by_user_id
        ? { cancelRequestedByUserId: row.cancel_requested_by_user_id }
        : {}),
    };
  }
}

class SqliteReconciliations implements ExecutionReconciliationRepository {
  constructor(private readonly db: Database) {}

  async listBySession(sessionId: string): Promise<readonly ExecutionReconciliation[]> {
    return this.db
      .query<ExecutionReconciliationRow, any>(
        `SELECT r.* FROM execution_reconciliations r
         JOIN executions e ON e.id = r.execution_id
         WHERE e.bot_session_id = ? ORDER BY r.reconciled_at, r.execution_id`,
      )
      .all(sessionId)
      .map((row) => this.map(row));
  }

  async reconcile(
    executionId: string,
    botSessionId: string,
    reconciliation: ExecutionReconciliation,
  ): Promise<ExecutionReconciliationResult> {
    const note = reconciliation.note.trim();
    const reconciledByUserId = reconciliation.reconciledByUserId.trim();
    if (!note) throw new Error("Reconciliation note is required.");
    if (!reconciledByUserId) throw new Error("Reconciled user is required.");
    if (reconciliation.executionId !== executionId) {
      throw new Error("Reconciliation execution ID does not match the target.");
    }
    this.db.run("BEGIN IMMEDIATE");
    try {
      const execution = this.db
        .query<
          {
            bot_session_id: string;
            status: string;
            correlation_id: string;
            telegram_chat_id: string;
          },
          any
        >(
          `SELECT e.bot_session_id, e.status, e.correlation_id, s.telegram_chat_id
           FROM executions e JOIN bot_sessions s ON s.id=e.bot_session_id WHERE e.id=?`,
        )
        .get(executionId);
      if (!execution) {
        this.db.run("COMMIT");
        return { status: "not_found" };
      }
      if (execution.bot_session_id !== botSessionId) {
        this.db.run("COMMIT");
        return { status: "wrong_session" };
      }
      const existing = this.db
        .query<ExecutionReconciliationRow, any>(
          "SELECT * FROM execution_reconciliations WHERE execution_id=?",
        )
        .get(executionId);
      if (existing) {
        this.db.run("COMMIT");
        return { status: "already_reconciled", reconciliation: this.map(existing) };
      }
      if (execution.status !== "unknown") {
        this.db.run("COMMIT");
        return { status: "not_unknown" };
      }
      this.db
        .query(
          `INSERT INTO execution_reconciliations
             (execution_id, outcome, reconciled_by_user_id, note, reconciled_at)
           VALUES (?, ?, ?, ?, ?)`,
        )
        .run(
          executionId,
          reconciliation.outcome,
          reconciledByUserId,
          note,
          reconciliation.reconciledAt.getTime(),
        );
      this.db
        .query(
          `UPDATE bot_sessions
           SET status=CASE
             WHEN status='closed' THEN 'closed'
             WHEN EXISTS (
               SELECT 1 FROM executions e
               WHERE e.bot_session_id=bot_sessions.id AND e.status='unknown'
                 AND NOT EXISTS (
                   SELECT 1 FROM execution_reconciliations r
                   WHERE r.execution_id=e.id
                 )
             ) THEN 'failed'
             ELSE 'idle'
           END, updated_at=? WHERE id=?`,
        )
        .run(reconciliation.reconciledAt.getTime(), botSessionId);
      this.db
        .query(
          `INSERT INTO audit_log
             (action, user_id, chat_id, session_id, execution_id, correlation_id, metadata, created_at)
           VALUES ('execution.reconciled', ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          reconciledByUserId,
          execution.telegram_chat_id,
          botSessionId,
          executionId,
          execution.correlation_id,
          JSON.stringify({ outcome: reconciliation.outcome }),
          reconciliation.reconciledAt.getTime(),
        );
      this.db.run("COMMIT");
      return {
        status: "reconciled",
        reconciliation: { ...reconciliation, reconciledByUserId, note },
      };
    } catch (error) {
      this.db.run("ROLLBACK");
      throw error;
    }
  }

  private map(row: ExecutionReconciliationRow): ExecutionReconciliation {
    return {
      executionId: row.execution_id,
      outcome: row.outcome,
      reconciledByUserId: row.reconciled_by_user_id,
      note: row.note,
      reconciledAt: new Date(row.reconciled_at),
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
        `INSERT INTO telegram_topics (chat_id, thread_id, session_id, kind, title, status, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(chat_id, thread_id) DO UPDATE SET session_id=excluded.session_id, kind=excluded.kind, title=excluded.title, status=excluded.status, updated_at=excluded.updated_at`,
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
