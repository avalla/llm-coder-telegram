export type Role = "owner" | "operator" | "viewer";

export type BotSessionStatus =
  "idle" | "starting" | "running" | "awaiting_approval" | "failed" | "stopped" | "closed";

export type ExecutionStatus =
  | "pending"
  | "running"
  | "awaiting_approval"
  | "completed"
  | "failed"
  | "stopped"
  | "interrupted"
  | "unknown";

export type TopicStatus = "open" | "closed" | "deleted";

export interface Project {
  id: string;
  name: string;
  workspacePath: string;
  allowedExecutorIds: readonly string[];
}

export interface BotSession {
  id: string;
  telegramChatId: string;
  telegramThreadId: string;
  executorId: string;
  projectId: string;
  workspacePath: string;
  executorSessionId?: string;
  status: BotSessionStatus;
  createdAt: Date;
  updatedAt: Date;
}

export interface Execution {
  id: string;
  botSessionId: string;
  requestedByUserId: string;
  prompt: string;
  status: ExecutionStatus;
  correlationId: string;
  executorSessionId?: string;
  startedAt?: Date;
  finishedAt?: Date;
  errorMessage?: string | undefined;
  ownerId?: string | undefined;
  ownerFence: number;
  leaseExpiresAt?: Date | undefined;
  cancelRequestedAt?: Date | undefined;
  cancelRequestedByUserId?: string | undefined;
}

export type ExecutionReconciliationOutcome = "confirmed_completed" | "abandoned";

export interface ExecutionReconciliation {
  executionId: string;
  outcome: ExecutionReconciliationOutcome;
  reconciledByUserId: string;
  note: string;
  reconciledAt: Date;
}

export type ExecutionReconciliationResult =
  | { status: "reconciled"; reconciliation: ExecutionReconciliation }
  | { status: "already_reconciled"; reconciliation: ExecutionReconciliation }
  | { status: "not_found" | "wrong_session" | "not_unknown" };

export interface ExecutionJob {
  executionId: string;
}

export type ExecutionJobDisposition =
  { disposition: "ack" } | { disposition: "retry"; delayMs?: number };

export type ExecutionJobHandler = (job: ExecutionJob) => Promise<ExecutionJobDisposition>;

export interface ExecutionJobQueue {
  enqueue(job: ExecutionJob): Promise<void>;
  cancel(executionId: string): Promise<void>;
}

export interface ExecutionJobWorker {
  start(handler: ExecutionJobHandler): Promise<void>;
  close(): Promise<void>;
}

export interface ExecutionLease {
  executionId: string;
  botSessionId: string;
  ownerId: string;
  fence: number;
  leaseExpiresAt: Date;
  recovered?: boolean;
}

export interface ExecutorSession {
  id: string;
  executorId: string;
  projectId: string;
  workspacePath: string;
  legacyRuntimeSessionId?: string;
  nativeSessionId?: string;
  hostId?: string;
  resumable: boolean;
  createdAt: Date;
  lastUsedAt: Date;
}

export interface TelegramTopic {
  chatId: string;
  threadId: string;
  sessionId?: string;
  kind: "control" | "workspace";
  title: string;
  status: TopicStatus;
  updatedAt: Date;
}

export interface ApprovalRequest {
  id: string;
  executionId: string;
  action: string;
  details: string;
  status: "pending" | "approved" | "rejected" | "expired";
  requestedAt: Date;
  resolvedAt?: Date;
  resolvedByUserId?: string;
}

export interface AgentSession {
  runtimeSessionId: string;
  executorId: string;
  projectId: string;
  workspacePath: string;
  nativeSessionId?: string;
  resumable?: boolean;
  createdAt: Date;
}

export interface StartSessionOptions {
  projectId: string;
  workspacePath: string;
  model?: string;
  signal?: AbortSignal;
}

export interface ResumeSessionOptions extends StartSessionOptions {
  nativeSessionId: string;
}

export interface AgentAttachment {
  id: string;
  filename: string;
  mimeType: string;
  localPath: string;
}

export interface AgentInput {
  prompt: string;
  model?: string;
  attachments?: readonly AgentAttachment[];
  signal?: AbortSignal;
}

export interface ExecutorCapabilities {
  resume: boolean;
  interrupt: boolean;
  close: boolean;
  sessionIdentity: boolean;
  streaming: boolean;
  approvals: boolean;
  models: boolean;
  fileAttachments: boolean;
  structuredOutput: boolean;
  worktrees: boolean;
}

export interface AgentEventBase {
  timestamp: Date;
  sequence: number;
}

export type AgentEvent =
  | (AgentEventBase & { type: "text_delta"; text: string })
  | (AgentEventBase & { type: "thinking"; text: string })
  | (AgentEventBase & {
      type: "tool_call";
      toolName: string;
      inputSummary: string;
    })
  | (AgentEventBase & { type: "tool_result"; toolName: string; summary: string })
  | (AgentEventBase & { type: "command"; command: string; cwd?: string })
  | (AgentEventBase & {
      type: "file_changed";
      path: string;
      change: "created" | "modified" | "deleted";
    })
  | (AgentEventBase & {
      type: "approval_request";
      approvalId: string;
      action: string;
      details: string;
    })
  | (AgentEventBase & {
      type: "session_identity";
      nativeSessionId: string;
      resumable: boolean;
    })
  | (AgentEventBase & {
      type: "usage";
      inputTokens?: number;
      outputTokens?: number;
      costUsd?: number;
    })
  | (AgentEventBase & { type: "error"; message: string; retryable: boolean })
  | (AgentEventBase & {
      type: "completed";
      summary?: string;
      exitCode?: number;
    });

export interface AgentExecutor {
  readonly id: string;
  readonly name: string;
  capabilities(): ExecutorCapabilities;
  start(options: StartSessionOptions): Promise<AgentSession>;
  send(session: AgentSession, input: AgentInput): AsyncIterable<AgentEvent>;
  interrupt(session: AgentSession): Promise<void>;
  close(session: AgentSession): Promise<void>;
  resume?(options: ResumeSessionOptions): Promise<AgentSession>;
  describeSession?(session: ExecutorSession): { resumeCommand?: string };
}

export interface IncomingMessage {
  chatId: string;
  threadId?: string;
  userId: string;
  text: string;
  messageId?: string;
}

export interface MessageRef {
  chatId: string;
  messageId: string;
  threadId?: string;
}

export interface TopicRef {
  chatId: string;
  threadId: string;
  title: string;
}

export interface InlineButton {
  label: string;
  callbackData: string;
}

export interface ChatMessageOptions {
  buttons?: readonly (readonly InlineButton[])[];
}

export interface ChatGateway {
  sendMessage(
    target: { chatId: string; threadId?: string },
    text: string,
    options?: ChatMessageOptions,
  ): Promise<MessageRef>;
  editMessage(message: MessageRef, text: string, options?: ChatMessageOptions): Promise<void>;
  sendDocument(
    target: { chatId: string; threadId?: string },
    filename: string,
    content: string,
  ): Promise<void>;
  createTopic(chatId: string, title: string): Promise<TopicRef>;
  closeTopic(topic: TopicRef): Promise<void>;
}

export interface ProjectRepository {
  getById(id: string): Promise<Project | undefined>;
  list(): Promise<readonly Project[]>;
}

export interface BotSessionRepository {
  getById(id: string): Promise<BotSession | undefined>;
  getByTopic(chatId: string, threadId: string): Promise<BotSession | undefined>;
  save(session: BotSession): Promise<void>;
  updateOwned(
    session: BotSession,
    executionId: string,
    ownerId: string,
    fence: number,
    now: Date,
  ): Promise<boolean>;
  list(): Promise<readonly BotSession[]>;
}

export interface ExecutionRepository {
  getById(id: string): Promise<Execution | undefined>;
  save(execution: Execution): Promise<void>;
  listBySession(sessionId: string): Promise<readonly Execution[]>;
  claim(
    executionId: string,
    ownerId: string,
    now: Date,
    leaseDurationMs: number,
  ): Promise<ExecutionLease | undefined>;
  renew(
    executionId: string,
    ownerId: string,
    fence: number,
    now: Date,
    leaseDurationMs: number,
  ): Promise<boolean>;
  updateOwned(execution: Execution, ownerId: string, fence: number, now: Date): Promise<boolean>;
  updateOwnedUnknown(
    execution: Execution & { status: "unknown" },
    ownerId: string,
    fence: number,
    now: Date,
  ): Promise<boolean>;
  requestCancellation(
    executionId: string,
    requestedByUserId: string,
    now: Date,
  ): Promise<Execution | undefined>;
  recoverAfterRestart(now: Date): Promise<readonly Execution[]>;
}

export interface ExecutionReconciliationRepository {
  listBySession(sessionId: string): Promise<readonly ExecutionReconciliation[]>;
  reconcile(
    executionId: string,
    botSessionId: string,
    reconciliation: ExecutionReconciliation,
  ): Promise<ExecutionReconciliationResult>;
}

export interface ExecutorSessionRepository {
  getById(id: string): Promise<ExecutorSession | undefined>;
  save(session: ExecutorSession): Promise<void>;
  updateOwned(
    session: ExecutorSession,
    executionId: string,
    ownerId: string,
    fence: number,
    now: Date,
  ): Promise<boolean>;
}

export interface TelegramTopicRepository {
  get(chatId: string, threadId: string): Promise<TelegramTopic | undefined>;
  save(topic: TelegramTopic): Promise<void>;
}

export interface AuditLog {
  append(entry: {
    action: string;
    userId?: string;
    chatId?: string;
    sessionId?: string;
    executionId?: string;
    correlationId: string;
    metadata?: Readonly<Record<string, string>>;
  }): Promise<void>;
}

export interface Persistence {
  projects: ProjectRepository;
  sessions: BotSessionRepository;
  executions: ExecutionRepository;
  executorSessions: ExecutorSessionRepository;
  topics: TelegramTopicRepository;
  reconciliations: ExecutionReconciliationRepository;
  audit: AuditLog;
}
