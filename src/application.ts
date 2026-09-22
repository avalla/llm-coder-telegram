import {
  type AgentEvent,
  type AgentExecutor,
  type AgentSession,
  type AuditLog,
  type ExecutorSession,
  type BotSession,
  type BotSessionRepository,
  type ChatGateway,
  type Execution,
  type ExecutionJob,
  type ExecutionJobDisposition,
  type ExecutionJobQueue,
  type ExecutionJobWorker,
  type ExecutionRepository,
  type ExecutionLease,
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

export class ProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProtocolError";
  }
}

export class NativeSessionIdentityConflictError extends ProtocolError {
  constructor(
    public readonly expectedNativeSessionId: string,
    public readonly observedNativeSessionId: string,
  ) {
    super("Provider returned a conflicting native session identity.");
    this.name = "NativeSessionIdentityConflictError";
  }
}

type ActiveExecution = {
  execution: Execution;
  executor: AgentExecutor;
  abort: AbortController;
  lease: ExecutionLease;
  agentSession?: AgentSession;
  interrupted: boolean;
  ownershipLost: boolean;
  done: Promise<void>;
  resolveDone: () => void;
};

class StaleExecutionOwnershipError extends Error {
  constructor() {
    super("Execution ownership was lost.");
    this.name = "StaleExecutionOwnershipError";
  }
}

const EXECUTION_LEASE_MS = 30_000;
const CANCELLATION_POLL_MS = 100;

type StreamResult =
  | { status: "completed"; message: string }
  | { status: "failed"; message: string }
  | { status: "stopped"; message: string }
  | { status: "unknown"; message: string };

export class AgentOrchestrator {
  private readonly active = new Map<string, ActiveExecution>();
  private readonly runtimeSessions = new Map<string, AgentSession>();
  private readonly workerId = `orchestrator-${crypto.randomUUID()}`;
  private shuttingDown = false;
  private started = false;
  private shutdownPromise?: Promise<void>;

  constructor(
    private readonly persistence: Persistence,
    private readonly executors: ExecutorRegistry,
    private readonly gateway: ChatGateway,
    private readonly authorization: AuthorizationService,
    private readonly queue = new SessionQueue(),
    private readonly clock: Clock = systemClock,
    private readonly jobQueue?: ExecutionJobQueue,
    private readonly jobWorker?: ExecutionJobWorker,
  ) {}

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    const recovered = await this.persistence.executions.recoverAfterRestart(this.clock.now());
    for (const execution of recovered) {
      if (execution.status === "unknown") {
        const session = await this.persistence.sessions.getById(execution.botSessionId);
        if (session && session.status !== "closed") {
          await this.persistence.sessions.save({
            ...session,
            status: "failed",
            updatedAt: this.clock.now(),
          });
        }
        await this.audit(
          "execution.recovered_unknown",
          execution.requestedByUserId,
          execution.botSessionId,
          execution.id,
          undefined,
          execution.correlationId,
        );
      } else if (execution.status === "pending") {
        if (!this.jobQueue) {
          throw new Error("Durable execution recovery requires an execution job queue.");
        }
        await this.jobQueue.enqueue({ executionId: execution.id });
      }
    }
    if (this.jobWorker) await this.jobWorker.start((job) => this.processJob(job));
  }

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
      const authoritative = (await this.persistence.sessions.getById(session.id)) ?? session;
      await this.handleSessionCommand(authoritative, message);
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
    const executions = await this.persistence.executions.listBySession(sessionId);
    const target = [...executions]
      .reverse()
      .find(
        (execution) =>
          execution.status === "pending" ||
          execution.status === "running" ||
          execution.status === "awaiting_approval",
      );
    if (!target) return;
    const requested = await this.persistence.executions.requestCancellation(
      target.id,
      userId,
      this.clock.now(),
    );
    await this.jobQueue?.cancel(target.id);
    const active = this.active.get(sessionId);
    if (active && active.execution.id === target.id) {
      active.abort.abort();
      await this.interruptActive(active);
    }
    await this.audit(
      "execution.stop_requested",
      userId,
      sessionId,
      target.id,
      chatId,
      requested?.correlationId,
    );
  }

  async shutdown(): Promise<void> {
    if (this.shutdownPromise) return this.shutdownPromise;
    this.shuttingDown = true;
    this.shutdownPromise = (async () => {
      const active = [...this.active.values()];
      await Promise.all(
        active.map(async (execution) => {
          execution.abort.abort();
          await this.interruptActive(execution);
        }),
      );
      await Promise.all(active.map((execution) => execution.done));
      await this.jobWorker?.close();
    })();
    return this.shutdownPromise;
  }

  private async handleSessionCommand(session: BotSession, message: IncomingMessage): Promise<void> {
    const command = message.text.trim();
    this.authorization.authorize(
      message,
      command === "/status" || command === "/session" || command === "/session info"
        ? "viewer"
        : "operator",
    );
    switch (command) {
      case "/status":
      case "/session":
      case "/session info":
        await this.gateway.sendMessage(
          { chatId: session.telegramChatId, threadId: session.telegramThreadId },
          await this.statusText((await this.persistence.sessions.getById(session.id)) ?? session),
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
        try {
          await this.closeSession(session, message.userId);
        } catch (error) {
          await this.gateway.sendMessage(
            { chatId: session.telegramChatId, threadId: session.telegramThreadId },
            `Cannot close session: ${errorMessage(error)}`,
          );
        }
        return;
      default:
        await this.gateway.sendMessage(
          { chatId: session.telegramChatId, threadId: session.telegramThreadId },
          "Unknown command. Try /status, /session, /stop or /close.",
        );
    }
  }

  private async runPrompt(session: BotSession, message: IncomingMessage): Promise<void> {
    if (this.shuttingDown) throw new Error("Orchestrator is shutting down");
    const current = await this.persistence.sessions.getById(session.id);
    if (!current) throw new Error(`Unknown session: ${session.id}`);
    if (current.status === "closed") throw new Error("Session is closed");

    const execution: Execution = {
      id: crypto.randomUUID(),
      botSessionId: current.id,
      requestedByUserId: message.userId,
      prompt: message.text,
      status: "pending",
      correlationId: crypto.randomUUID(),
      ownerFence: 0,
    };
    await this.persistence.executions.save(execution);
    await this.audit(
      "execution.created",
      message.userId,
      current.id,
      execution.id,
      message.chatId,
      execution.correlationId,
    );

    const job: ExecutionJob = { executionId: execution.id };
    if (this.jobQueue) {
      // save() atomically creates the queued delivery intent; this idempotent enqueue is
      // a delivery nudge that also supports queue implementations without that transaction.
      await this.jobQueue.enqueue(job);
    } else {
      await this.queue.run(current.id, () => this.processJob(job));
    }
  }

  private async processJob(job: ExecutionJob): Promise<ExecutionJobDisposition> {
    const lease = await this.persistence.executions.claim(
      job.executionId,
      this.workerId,
      this.clock.now(),
      EXECUTION_LEASE_MS,
    );
    if (!lease) {
      const execution = await this.persistence.executions.getById(job.executionId);
      return execution?.status === "pending"
        ? { disposition: "retry", delayMs: CANCELLATION_POLL_MS }
        : { disposition: "ack" };
    }
    const execution = await this.persistence.executions.getById(job.executionId);
    if (!execution) return { disposition: "ack" };
    if (lease.recovered) {
      await this.finishWithoutProvider(execution, lease, {
        status: "unknown",
        message: "Execution ownership expired; provider state was not guessed.",
      });
      return { disposition: "ack" };
    }
    await this.executeClaimed(execution, lease);
    return { disposition: "ack" };
  }

  private async executeClaimed(execution: Execution, lease: ExecutionLease): Promise<void> {
    const current = await this.persistence.sessions.getById(execution.botSessionId);
    if (!current || current.status === "closed") {
      await this.finishWithoutProvider(execution, lease, {
        status: "failed",
        message: current ? "Session is closed." : "Session no longer exists.",
      });
      return;
    }
    const executor = this.executors.require(current.executorId);
    const project = await this.requireProject(current.projectId);
    const abort = new AbortController();
    let resolveDone!: () => void;
    const done = new Promise<void>((resolve) => {
      resolveDone = resolve;
    });
    const active: ActiveExecution = {
      execution,
      executor,
      abort,
      lease,
      interrupted: false,
      ownershipLost: false,
      done,
      resolveDone,
    };
    this.active.set(current.id, active);
    const renewTimer = setInterval(
      () => {
        void this.renewLease(active).catch(() => undefined);
      },
      Math.max(250, EXECUTION_LEASE_MS / 3),
    );
    const cancellationTimer = setInterval(() => {
      void this.observeCancellation(active).catch(() => undefined);
    }, CANCELLATION_POLL_MS);
    let renderer: ThrottledEventRenderer | undefined;
    let executorSession: ExecutorSession | undefined;
    try {
      executorSession = current.executorSessionId
        ? await this.persistence.executorSessions.getById(current.executorSessionId)
        : undefined;
      const createdExecutorSession = !executorSession;
      if (!executorSession) {
        const now = this.clock.now();
        executorSession = {
          id: current.executorSessionId ?? crypto.randomUUID(),
          executorId: executor.id,
          projectId: project.id,
          workspacePath: project.workspacePath,
          resumable: false,
          createdAt: now,
          lastUsedAt: now,
        };
      }
      if (createdExecutorSession) {
        await this.persistOwnedExecutorSession(active, executorSession);
      }
      await this.persistExecution(active, {
        ...active.execution,
        executorSessionId: executorSession.id,
      });
      await this.persistOwnedSession(active, {
        ...current,
        executorSessionId: executorSession.id,
        status: "starting",
        updatedAt: this.clock.now(),
      });
      await this.observeCancellation(active);
      if (active.abort.signal.aborted) {
        await this.finish(
          active,
          { status: "stopped", message: "Execution stopped before provider start." },
          undefined,
          executorSession.id,
        );
        return;
      }

      const hasNativeSession = Boolean(executorSession.nativeSessionId);
      let agentSession: AgentSession;
      if (hasNativeSession) {
        if (!executorSession.resumable || !executor.capabilities().resume || !executor.resume) {
          throw new ProtocolError("Executor cannot safely resume the persisted native session.");
        }
        const nativeSessionId = executorSession.nativeSessionId!;
        const resumed = await executor.resume({
          nativeSessionId,
          projectId: project.id,
          workspacePath: project.workspacePath,
          signal: abort.signal,
        });
        if (resumed.nativeSessionId && resumed.nativeSessionId !== nativeSessionId) {
          throw new ProtocolError("Executor returned a different native session during resume.");
        }
        agentSession = { ...resumed, nativeSessionId, resumable: true };
      } else {
        agentSession = await executor.start({
          projectId: project.id,
          workspacePath: project.workspacePath,
          signal: abort.signal,
        });
      }
      active.agentSession = agentSession;
      this.runtimeSessions.set(current.id, agentSession);
      await this.interruptIfAborted(active);
      executorSession = { ...executorSession, lastUsedAt: this.clock.now() };
      await this.persistOwnedExecutorSession(active, executorSession);
      if (agentSession.nativeSessionId) {
        executorSession = await this.persistNativeIdentity(
          active,
          executorSession,
          agentSession.nativeSessionId,
          agentSession.resumable ?? true,
        );
      }

      await this.persistExecution(active, {
        ...active.execution,
        status: "running",
        startedAt: active.execution.startedAt ?? this.clock.now(),
        executorSessionId: executorSession.id,
      });
      await this.persistOwnedSession(active, {
        ...((await this.persistence.sessions.getById(current.id)) ?? current),
        executorSessionId: executorSession.id,
        status: "running",
        updatedAt: this.clock.now(),
      });

      renderer = new ThrottledEventRenderer(this.gateway, {
        chatId: current.telegramChatId,
        threadId: current.telegramThreadId,
      });
      await this.assertOwned(active);
      await renderer.start(executor.name, project);
      const result = await this.consumeStream(
        active,
        executorSession,
        agentSession,
        renderer,
        execution.prompt,
      );
      await this.finish(active, result, renderer, executorSession.id);
    } catch (error) {
      if (error instanceof StaleExecutionOwnershipError) {
        active.ownershipLost = true;
      } else {
        if (error instanceof NativeSessionIdentityConflictError) {
          await this.audit(
            "execution.session_identity_conflict",
            execution.requestedByUserId,
            current.id,
            execution.id,
            current.telegramChatId,
            execution.correlationId,
          );
        }
        await this.finish(
          active,
          abort.signal.aborted
            ? { status: "stopped", message: "Execution stopped." }
            : { status: "failed", message: `Execution failed: ${errorMessage(error)}` },
          renderer,
          executorSession?.id,
        ).catch((finishError) => {
          if (!(finishError instanceof StaleExecutionOwnershipError)) throw finishError;
          active.ownershipLost = true;
        });
      }
    } finally {
      clearInterval(renewTimer);
      clearInterval(cancellationTimer);
      if (this.active.get(current.id) === active) this.active.delete(current.id);
      active.resolveDone();
    }
  }

  private async finishWithoutProvider(
    execution: Execution,
    lease: ExecutionLease,
    result: StreamResult,
  ): Promise<void> {
    await this.persistence.executions.updateOwned(
      {
        ...execution,
        status: result.status,
        finishedAt: this.clock.now(),
        ownerFence: lease.fence,
        errorMessage: result.status === "completed" ? undefined : result.message,
      },
      lease.ownerId,
      lease.fence,
      this.clock.now(),
    );
  }

  private async finish(
    active: ActiveExecution,
    result: StreamResult,
    renderer: ThrottledEventRenderer | undefined,
    executorSessionId: string | undefined,
  ): Promise<void> {
    if (active.ownershipLost) return;
    const finishedAt = this.clock.now();
    const finalStatus =
      active.abort.signal.aborted && result.status === "completed"
        ? { status: "stopped" as const, message: "Execution stopped." }
        : result;
    const finalExecution: Execution = {
      ...active.execution,
      status: finalStatus.status,
      finishedAt,
      ...(finalStatus.status === "completed" ? {} : { errorMessage: finalStatus.message }),
      ...(executorSessionId ? { executorSessionId } : {}),
      ownerFence: active.lease.fence,
    };
    const current = await this.persistence.sessions.getById(active.execution.botSessionId);
    const sessionStatus =
      current?.status === "closed"
        ? "closed"
        : finalStatus.status === "completed"
          ? "idle"
          : finalStatus.status === "stopped"
            ? "stopped"
            : "failed";
    if (current) {
      await this.persistOwnedSession(active, {
        ...current,
        ...(executorSessionId ? { executorSessionId } : {}),
        status: sessionStatus,
        updatedAt: finishedAt,
      });
    }
    const updated = await this.persistence.executions.updateOwned(
      finalExecution,
      active.lease.ownerId,
      active.lease.fence,
      finishedAt,
    );
    if (!updated) {
      active.ownershipLost = true;
      return;
    }
    active.execution = finalExecution;
    if (renderer) {
      await renderer.complete(
        finalStatus.status === "completed"
          ? "Completed"
          : finalStatus.status === "stopped"
            ? "Stopped"
            : "Failed",
        finalStatus.message,
      );
    }
  }

  private async consumeStream(
    active: ActiveExecution,
    executorSession: ExecutorSession,
    agentSession: AgentSession,
    renderer: ThrottledEventRenderer,
    prompt: string,
  ): Promise<StreamResult> {
    let terminal: StreamResult | undefined;
    for await (const event of active.executor.send(agentSession, {
      prompt,
      signal: active.abort.signal,
    })) {
      await this.assertOwned(active);
      if (active.abort.signal.aborted) return { status: "stopped", message: "Execution stopped." };
      if (terminal) throw new ProtocolError("Executor emitted events after a terminal event");
      if (event.type === "session_identity") {
        executorSession = await this.persistNativeIdentity(
          active,
          executorSession,
          event.nativeSessionId,
          event.resumable,
        );
        active.agentSession = { ...agentSession, nativeSessionId: event.nativeSessionId };
        agentSession = active.agentSession;
      }
      if (event.type === "approval_request") {
        await this.persistExecution(active, {
          ...active.execution,
          status: "awaiting_approval",
        });
        const current = await this.persistence.sessions.getById(active.execution.botSessionId);
        if (current && current.status !== "closed") {
          await this.persistOwnedSession(active, {
            ...current,
            executorSessionId: executorSession.id,
            status: "awaiting_approval",
            updatedAt: this.clock.now(),
          });
        }
      }
      await this.assertOwned(active);
      await renderer.handle(event);
      if (event.type === "error") {
        terminal = { status: "failed", message: event.message };
      } else if (event.type === "completed") {
        terminal =
          event.exitCode !== undefined && event.exitCode !== 0
            ? { status: "failed", message: `Provider exited with code ${event.exitCode}.` }
            : { status: "completed", message: event.summary ?? "Execution finished." };
      }
    }
    if (active.abort.signal.aborted) return { status: "stopped", message: "Execution stopped." };
    return (
      terminal ?? {
        status: "unknown",
        message: "Executor stream ended without a terminal event.",
      }
    );
  }

  private async persistNativeIdentity(
    active: ActiveExecution,
    executorSession: ExecutorSession,
    nativeSessionId: string,
    resumable: boolean,
  ): Promise<ExecutorSession> {
    if (!nativeSessionId.trim())
      throw new ProtocolError("Executor emitted an empty native session ID");
    if (executorSession.nativeSessionId && executorSession.nativeSessionId !== nativeSessionId) {
      throw new NativeSessionIdentityConflictError(
        executorSession.nativeSessionId,
        nativeSessionId,
      );
    }
    const updated = {
      ...executorSession,
      nativeSessionId,
      resumable: executorSession.resumable || resumable,
      lastUsedAt: this.clock.now(),
    };
    await this.persistOwnedExecutorSession(active, updated);
    const session = await this.persistence.sessions.getById(active.execution.botSessionId);
    if (session && session.status !== "closed") {
      await this.persistOwnedSession(active, {
        ...session,
        executorSessionId: executorSession.id,
        updatedAt: this.clock.now(),
      });
    }
    return updated;
  }

  private async persistOwnedSession(active: ActiveExecution, session: BotSession): Promise<void> {
    const updated = await this.persistence.sessions.updateOwned(
      session,
      active.execution.id,
      active.lease.ownerId,
      active.lease.fence,
      this.clock.now(),
    );
    if (!updated) {
      active.ownershipLost = true;
      throw new StaleExecutionOwnershipError();
    }
  }

  private async persistOwnedExecutorSession(
    active: ActiveExecution,
    session: ExecutorSession,
  ): Promise<void> {
    const updated = await this.persistence.executorSessions.updateOwned(
      session,
      active.execution.id,
      active.lease.ownerId,
      active.lease.fence,
      this.clock.now(),
    );
    if (!updated) {
      active.ownershipLost = true;
      throw new StaleExecutionOwnershipError();
    }
  }

  private async renewLease(active: ActiveExecution): Promise<void> {
    if (active.abort.signal.aborted || active.ownershipLost) return;
    const renewed = await this.persistence.executions.renew(
      active.execution.id,
      active.lease.ownerId,
      active.lease.fence,
      this.clock.now(),
      EXECUTION_LEASE_MS,
    );
    if (!renewed) {
      active.ownershipLost = true;
      active.abort.abort();
      await this.interruptActive(active);
    } else {
      active.execution = {
        ...active.execution,
        leaseExpiresAt: new Date(this.clock.now().getTime() + EXECUTION_LEASE_MS),
      };
    }
  }

  private async observeCancellation(active: ActiveExecution): Promise<void> {
    if (active.abort.signal.aborted || active.ownershipLost) return;
    const execution = await this.persistence.executions.getById(active.execution.id);
    if (execution?.cancelRequestedAt) {
      active.abort.abort();
      await this.interruptActive(active);
    }
  }

  private async assertOwned(active: ActiveExecution): Promise<void> {
    const execution = await this.persistence.executions.getById(active.execution.id);
    if (
      !execution ||
      execution.ownerId !== active.lease.ownerId ||
      execution.ownerFence !== active.lease.fence ||
      !execution.leaseExpiresAt ||
      execution.leaseExpiresAt <= this.clock.now()
    ) {
      active.ownershipLost = true;
      throw new StaleExecutionOwnershipError();
    }
  }

  private async persistExecution(active: ActiveExecution, execution: Execution): Promise<void> {
    await this.assertOwned(active);
    const updated = await this.persistence.executions.updateOwned(
      { ...execution, ownerId: active.lease.ownerId, ownerFence: active.lease.fence },
      active.lease.ownerId,
      active.lease.fence,
      this.clock.now(),
    );
    if (!updated) {
      active.ownershipLost = true;
      throw new StaleExecutionOwnershipError();
    }
    active.execution = {
      ...execution,
      ownerId: active.lease.ownerId,
      ownerFence: active.lease.fence,
    };
  }

  private async interruptActive(active: ActiveExecution): Promise<void> {
    if (!active.agentSession || active.interrupted || !active.executor.capabilities().interrupt)
      return;
    active.interrupted = true;
    await active.executor.interrupt(active.agentSession);
  }

  private async interruptIfAborted(active: ActiveExecution): Promise<void> {
    if (!active.abort.signal.aborted) return;
    await this.interruptActive(active);
    throw new Error("Execution stopped before provider stream started");
  }

  private async closeSession(session: BotSession, userId: string): Promise<void> {
    this.authorization.authorize({ userId, chatId: session.telegramChatId }, "operator");
    if (this.active.has(session.id)) {
      throw new Error("An execution is active; use /stop before closing the session.");
    }
    await this.queue.run(session.id, async () => {
      const current = await this.persistence.sessions.getById(session.id);
      if (!current || current.status === "closed") return;
      const executions = await this.persistence.executions.listBySession(session.id);
      if (
        executions.some(
          (execution) =>
            execution.status === "pending" ||
            execution.status === "running" ||
            execution.status === "awaiting_approval",
        )
      ) {
        throw new Error("An execution is active; use /stop before closing the session.");
      }
      const runtimeSession = this.runtimeSessions.get(session.id);
      if (runtimeSession) {
        const executor = this.executors.require(current.executorId);
        if (executor.capabilities().close) await executor.close(runtimeSession);
        this.runtimeSessions.delete(session.id);
      }
      const now = this.clock.now();
      await this.persistence.sessions.save({ ...current, status: "closed", updatedAt: now });
      await this.persistence.topics.save({
        chatId: current.telegramChatId,
        threadId: current.telegramThreadId,
        sessionId: current.id,
        kind: "workspace",
        title: "",
        status: "closed",
        updatedAt: now,
      });
      await this.gateway.closeTopic({
        chatId: current.telegramChatId,
        threadId: current.telegramThreadId,
        title: "",
      });
    });
  }

  private async statusText(session: BotSession): Promise<string> {
    const executions = await this.persistence.executions.listBySession(session.id);
    const last = executions.at(-1);
    const executorSession = session.executorSessionId
      ? await this.persistence.executorSessions.getById(session.executorSessionId)
      : undefined;
    const executor = this.executors.require(session.executorId);
    const activeExecution = [...executions]
      .reverse()
      .find(
        (execution) =>
          execution.status === "pending" ||
          execution.status === "running" ||
          execution.status === "awaiting_approval",
      );
    const active = this.active.get(session.id);
    const runtimeSession = this.runtimeSessions.get(session.id);
    return [
      `Telegram topic: ${session.telegramChatId}/${session.telegramThreadId}`,
      `Runtime session: ${runtimeSession?.runtimeSessionId ?? "not active"}`,
      `Executor: ${executor.name} (${executor.id})`,
      `Native session: ${executorSession?.nativeSessionId ?? "not established"}`,
      `Status: ${session.status}`,
      `Active execution: ${activeExecution?.status ?? active?.execution.status ?? "none"}`,
      `Resume supported: ${executor.capabilities().resume ? "yes" : "no"}`,
      `Last execution: ${last?.status ?? "none"}`,
    ].join("\n");
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
    correlationId: string = crypto.randomUUID(),
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
