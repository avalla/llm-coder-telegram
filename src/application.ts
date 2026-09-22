import {
  type AgentEvent,
  type AgentExecutor,
  type AgentSession,
  type AuditLog,
  type BotSession,
  type BotSessionRepository,
  type ChatGateway,
  type Execution,
  type ExecutionRepository,
  type ExecutorCapabilities,
  type IncomingMessage,
  type MessageRef,
  type Persistence,
  type Project,
  type ProjectRepository,
  type Role,
  type TelegramTopic,
  type TelegramTopicRepository,
} from "./domain.js";

export interface ExecutorRegistry {
  register(executor: AgentExecutor): void;
  get(id: string): AgentExecutor | undefined;
  require(id: string): AgentExecutor;
  list(): readonly AgentExecutor[];
}

export class InMemoryExecutorRegistry implements ExecutorRegistry {
  private readonly executors = new Map<string, AgentExecutor>();

  register(executor: AgentExecutor): void {
    if (this.executors.has(executor.id)) {
      throw new Error(`Executor already registered: ${executor.id}`);
    }
    this.executors.set(executor.id, executor);
  }

  get(id: string): AgentExecutor | undefined {
    return this.executors.get(id);
  }

  require(id: string): AgentExecutor {
    const executor = this.get(id);
    if (!executor) {
      throw new Error(`Unknown executor: ${id}`);
    }
    return executor;
  }

  list(): readonly AgentExecutor[] {
    return [...this.executors.values()];
  }
}

export interface AuthorizationConfig {
  allowedChatIds: readonly string[];
  users: Readonly<Record<string, Role>>;
}

export class AuthorizationService {
  constructor(private readonly config: AuthorizationConfig) {}

  authorize(message: Pick<IncomingMessage, "chatId" | "userId">, requiredRole: Role): void {
    if (!this.config.allowedChatIds.includes(message.chatId)) {
      throw new AuthorizationError("chat_not_allowed");
    }

    const actualRole = this.config.users[message.userId];
    if (!actualRole || !hasRole(actualRole, requiredRole)) {
      throw new AuthorizationError("user_not_allowed");
    }
  }

  roleFor(userId: string): Role | undefined {
    return this.config.users[userId];
  }
}

export class AuthorizationError extends Error {
  constructor(public readonly reason: "chat_not_allowed" | "user_not_allowed") {
    super(`Authorization denied: ${reason}`);
    this.name = "AuthorizationError";
  }
}

function hasRole(actual: Role, required: Role): boolean {
  const rank: Record<Role, number> = { viewer: 1, operator: 2, owner: 3 };
  return rank[actual] >= rank[required];
}

export interface Clock {
  now(): Date;
}

export const systemClock: Clock = { now: () => new Date() };

export class SessionQueue {
  private readonly tails = new Map<string, Promise<void>>();

  async run<T>(key: string, operation: () => Promise<T>): Promise<T> {
    const previous = this.tails.get(key) ?? Promise.resolve();
    let release!: () => void;
    const current = new Promise<void>((resolve) => {
      release = resolve;
    });
    const tail = previous.then(() => current);
    this.tails.set(key, tail);

    await previous;
    try {
      return await operation();
    } finally {
      release();
      if (this.tails.get(key) === tail) {
        this.tails.delete(key);
      }
    }
  }
}

export interface RendererConfig {
  editIntervalMs: number;
  maxTailCharacters: number;
}

export class ThrottledEventRenderer {
  private statusMessage?: MessageRef;
  private lastEditAt = 0;
  private readonly tail: string[] = [];
  private currentStatus = "Starting";
  private executorName = "Agent";
  private projectName = "";
  private workspacePath = "";

  constructor(
    private readonly gateway: ChatGateway,
    private readonly target: { chatId: string; threadId: string },
    private readonly config: RendererConfig = {
      editIntervalMs: 1_000,
      maxTailCharacters: 3_000,
    },
    private readonly clock: Clock = systemClock,
  ) {}

  async start(executorName: string, project: Project): Promise<void> {
    this.executorName = executorName;
    this.projectName = project.name;
    this.workspacePath = project.workspacePath;
    this.statusMessage = await this.gateway.sendMessage(
      this.target,
      statusText(executorName, project.name, project.workspacePath, this.currentStatus, ""),
    );
    this.lastEditAt = this.clock.now().getTime();
  }

  async handle(event: AgentEvent): Promise<void> {
    switch (event.type) {
      case "text_delta":
        this.tail.push(event.text);
        break;
      case "command":
        this.tail.push(`$ ${event.command}`);
        break;
      case "tool_call":
        this.tail.push(`Tool: ${event.toolName} — ${event.inputSummary}`);
        break;
      case "tool_result":
        this.tail.push(`${event.toolName}: ${event.summary}`);
        break;
      case "thinking":
        this.tail.push(`Thinking: ${event.text}`);
        break;
      case "file_changed":
        this.tail.push(`${event.change}: ${event.path}`);
        break;
      case "approval_request":
        this.currentStatus = "Awaiting approval";
        this.tail.push(`Approval required: ${event.action} — ${event.details}`);
        break;
      case "error":
        this.currentStatus = "Failed";
        this.tail.push(`Error: ${event.message}`);
        break;
      case "completed":
        this.currentStatus = "Completed";
        if (event.summary) this.tail.push(event.summary);
        break;
      case "usage":
        break;
    }

    const now = this.clock.now().getTime();
    if (
      event.type === "completed" ||
      event.type === "error" ||
      event.type === "approval_request" ||
      now - this.lastEditAt >= this.config.editIntervalMs
    ) {
      await this.flush();
    }
  }

  async complete(status: "Completed" | "Failed" | "Stopped", summary: string): Promise<void> {
    this.currentStatus = status;
    if (summary) this.tail.push(summary);
    await this.flush();
  }

  private async flush(): Promise<void> {
    if (!this.statusMessage) return;
    await this.gateway.editMessage(
      this.statusMessage,
      statusText(
        this.executorName,
        this.projectName,
        this.workspacePath,
        this.currentStatus,
        tailText(this.tail, this.config.maxTailCharacters),
      ),
    );
    this.lastEditAt = this.clock.now().getTime();
  }
}

function tailText(lines: readonly string[], maxCharacters: number): string {
  let output = "";
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    const next = `${lines[index]}\n${output}`;
    if (next.length > maxCharacters) break;
    output = next;
  }
  return output.trim();
}

function statusText(
  executorName: string,
  projectName: string,
  workspacePath: string,
  status: string,
  current: string,
): string {
  const projectLine = projectName ? `\n📂 ${projectName}` : "";
  const pathLine = workspacePath ? `\n📁 ${workspacePath}` : "";
  return `🤖 ${executorName}${projectLine}${pathLine}\n\nStatus: ${status}\n\nCurrent:\n> ${current.replaceAll("\n", "\n> ")}`;
}

export interface CreateSessionRequest {
  chatId: string;
  userId: string;
  projectId: string;
  executorId: string;
  title?: string;
}

export class AgentOrchestrator {
  private readonly active = new Map<
    string,
    { execution: Execution; session: AgentSession; abort: AbortController }
  >();

  constructor(
    private readonly persistence: Persistence,
    private readonly executors: ExecutorRegistry,
    private readonly gateway: ChatGateway,
    private readonly authorization: AuthorizationService,
    private readonly queue = new SessionQueue(),
    private readonly clock: Clock = systemClock,
  ) {}

  async createSession(request: CreateSessionRequest): Promise<BotSession> {
    this.authorization.authorize(request, "operator");
    const project = await this.requireProject(request.projectId);
    const executor = this.executors.require(request.executorId);
    if (!project.allowedExecutorIds.includes(executor.id)) {
      throw new Error(`Executor ${executor.id} is not allowed for project ${project.id}`);
    }

    const topic = await this.gateway.createTopic(
      request.chatId,
      request.title ?? `[${executor.name}] ${project.name}`,
    );
    const now = this.clock.now();
    const session: BotSession = {
      id: crypto.randomUUID(),
      telegramChatId: topic.chatId,
      telegramThreadId: topic.threadId,
      executorId: executor.id,
      projectId: project.id,
      workspacePath: project.workspacePath,
      status: "idle",
      createdAt: now,
      updatedAt: now,
    };
    await this.persistence.sessions.save(session);
    await this.persistence.topics.save({
      chatId: topic.chatId,
      threadId: topic.threadId,
      sessionId: session.id,
      kind: "workspace",
      title: topic.title,
      status: "open",
      updatedAt: now,
    });
    await this.audit("session.created", request.userId, session.id, undefined, topic.chatId);
    return session;
  }

  async handleMessage(message: IncomingMessage): Promise<void> {
    const session = message.threadId
      ? await this.persistence.sessions.getByTopic(message.chatId, message.threadId)
      : undefined;
    if (!session) {
      await this.handleControlMessage(message);
      return;
    }

    if (message.text.startsWith("/")) {
      await this.handleSessionCommand(session, message);
      return;
    }

    this.authorization.authorize(message, "operator");
    await this.runPrompt(session, message);
  }

  private async handleControlMessage(message: IncomingMessage): Promise<void> {
    if (!message.threadId) return;
    const topic = await this.persistence.topics.get(message.chatId, message.threadId);
    const request = parseNewCommand(message.text);
    if (topic?.kind !== "control" || !request) return;
    this.authorization.authorize(message, "operator");
    const session = await this.createSession({
      ...request,
      chatId: message.chatId,
      userId: message.userId,
    });
    await this.gateway.sendMessage(
      { chatId: message.chatId, threadId: message.threadId },
      `Created ${session.executorId} session for ${session.projectId} in topic ${session.telegramThreadId}.`,
    );
  }

  async stop(sessionId: string, userId: string, chatId: string): Promise<void> {
    this.authorization.authorize({ userId, chatId }, "operator");
    const active = this.active.get(sessionId);
    if (!active) return;
    active.abort.abort();
    await this.executors
      .require(
        active.execution.agentSessionId ? active.session.executorId : active.session.executorId,
      )
      .interrupt(active.session);
    await this.audit("execution.stop_requested", userId, sessionId, active.execution.id, chatId);
  }

  private async handleSessionCommand(session: BotSession, message: IncomingMessage): Promise<void> {
    this.authorization.authorize(message, message.text === "/status" ? "viewer" : "operator");
    switch (message.text.trim()) {
      case "/status":
        await this.gateway.sendMessage(
          { chatId: session.telegramChatId, threadId: session.telegramThreadId },
          await this.statusText(session),
        );
        return;
      case "/stop":
        await this.stop(session.id, message.userId, message.chatId);
        await this.gateway.sendMessage(
          { chatId: session.telegramChatId, threadId: session.telegramThreadId },
          "🛑 Stop requested.",
        );
        return;
      case "/close":
        await this.closeSession(session, message.userId);
        return;
      default:
        await this.gateway.sendMessage(
          { chatId: session.telegramChatId, threadId: session.telegramThreadId },
          "Unknown command. Try /status, /stop or /close.",
        );
    }
  }

  private async runPrompt(session: BotSession, message: IncomingMessage): Promise<void> {
    await this.queue.run(session.id, async () => {
      const executor = this.executors.require(session.executorId);
      const project = await this.requireProject(session.projectId);
      const correlationId = crypto.randomUUID();
      const execution: Execution = {
        id: crypto.randomUUID(),
        botSessionId: session.id,
        requestedByUserId: message.userId,
        prompt: message.text,
        status: "pending",
        correlationId,
      };
      await this.persistence.executions.save(execution);
      await this.audit(
        "execution.created",
        message.userId,
        session.id,
        execution.id,
        message.chatId,
        correlationId,
      );

      const updatedSession = {
        ...session,
        status: "starting" as const,
        updatedAt: this.clock.now(),
      };
      await this.persistence.sessions.save(updatedSession);
      const agentSession =
        session.agentSessionId && executor.resume
          ? await executor.resume({
              externalSessionId: session.agentSessionId,
              projectId: project.id,
              workspacePath: project.workspacePath,
            })
          : await executor.start({ projectId: project.id, workspacePath: project.workspacePath });

      const abort = new AbortController();
      const active = {
        execution: {
          ...execution,
          status: "running" as const,
          startedAt: this.clock.now(),
          agentSessionId: agentSession.id,
        },
        session: agentSession,
        abort,
      };
      this.active.set(session.id, active);
      await this.persistence.sessions.save({
        ...updatedSession,
        status: "running",
        agentSessionId: agentSession.externalSessionId ?? agentSession.id,
        updatedAt: this.clock.now(),
      });
      await this.persistence.executions.save(active.execution);

      const renderer = new ThrottledEventRenderer(this.gateway, {
        chatId: session.telegramChatId,
        threadId: session.telegramThreadId,
      });
      await renderer.start(executor.name, project);
      try {
        for await (const event of executor.send(agentSession, {
          prompt: message.text,
          signal: abort.signal,
        })) {
          if (event.type === "approval_request") {
            await this.persistence.executions.save({
              ...active.execution,
              status: "awaiting_approval",
            });
            await this.persistence.sessions.save({
              ...updatedSession,
              status: "awaiting_approval",
              agentSessionId: agentSession.externalSessionId ?? agentSession.id,
              updatedAt: this.clock.now(),
            });
          }
          await renderer.handle(event);
        }
        await this.persistence.executions.save({
          ...active.execution,
          status: "completed",
          finishedAt: this.clock.now(),
        });
        await this.persistence.sessions.save({
          ...updatedSession,
          status: "idle",
          agentSessionId: agentSession.externalSessionId ?? agentSession.id,
          updatedAt: this.clock.now(),
        });
        await renderer.complete("Completed", "Execution finished.");
      } catch (error) {
        const stopped = abort.signal.aborted;
        await this.persistence.executions.save({
          ...active.execution,
          status: stopped ? "stopped" : "failed",
          finishedAt: this.clock.now(),
          errorMessage: errorMessage(error),
        });
        await this.persistence.sessions.save({
          ...updatedSession,
          status: stopped ? "stopped" : "failed",
          agentSessionId: agentSession.externalSessionId ?? agentSession.id,
          updatedAt: this.clock.now(),
        });
        await renderer.complete(
          stopped ? "Stopped" : "Failed",
          stopped ? "Execution stopped." : `Execution failed: ${errorMessage(error)}`,
        );
      } finally {
        this.active.delete(session.id);
      }
    });
  }

  private async closeSession(session: BotSession, userId: string): Promise<void> {
    this.authorization.authorize({ userId, chatId: session.telegramChatId }, "operator");
    await this.persistence.sessions.save({
      ...session,
      status: "closed",
      updatedAt: this.clock.now(),
    });
    await this.persistence.topics.save({
      chatId: session.telegramChatId,
      threadId: session.telegramThreadId,
      sessionId: session.id,
      kind: "workspace",
      title: "",
      status: "closed",
      updatedAt: this.clock.now(),
    });
    await this.gateway.closeTopic({
      chatId: session.telegramChatId,
      threadId: session.telegramThreadId,
      title: "",
    });
  }

  private async statusText(session: BotSession): Promise<string> {
    const executions = await this.persistence.executions.listBySession(session.id);
    const last = executions.at(-1);
    return `Status: ${session.status}\nExecutor: ${session.executorId}\nProject: ${session.projectId}\nLast execution: ${last?.status ?? "none"}`;
  }

  private async requireProject(id: string): Promise<Project> {
    const project = await this.persistence.projects.getById(id);
    if (!project) throw new Error(`Unknown project: ${id}`);
    return project;
  }

  private async audit(
    action: string,
    userId: string,
    sessionId: string,
    executionId?: string,
    chatId?: string,
    correlationId = crypto.randomUUID(),
  ): Promise<void> {
    await this.persistence.audit.append({
      action,
      userId,
      sessionId,
      correlationId,
      ...(executionId ? { executionId } : {}),
      ...(chatId ? { chatId } : {}),
    });
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Unknown error";
}

export function parseNewCommand(
  text: string,
): { projectId: string; executorId: string; title?: string } | undefined {
  const match = text.trim().match(/^\/new\s+(\S+)\s+(\S+)(?:\s+(.+))?$/);
  if (!match) return undefined;
  const projectId = match[1];
  const executorId = match[2];
  if (!projectId || !executorId) return undefined;
  const title = match[3]?.trim().replace(/^"|"$/g, "");
  return title ? { projectId, executorId, title } : { projectId, executorId };
}

export function executorSummary(executor: AgentExecutor): {
  id: string;
  name: string;
  capabilities: ExecutorCapabilities;
} {
  return { id: executor.id, name: executor.name, capabilities: executor.capabilities() };
}
