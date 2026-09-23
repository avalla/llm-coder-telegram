import {
  type AgentEvent,
  type AgentExecutor,
  type AgentInput,
  type AgentSession,
  type ExecutorCapabilities,
  type BotSession,
  type BotSessionRepository,
  type ExecutorSession,
  type ExecutorSessionRepository,
  type ResumeSessionOptions,
  type StartSessionOptions,
  type ChatGateway,
  type Execution,
  type ExecutionRepository,
  type MessageRef,
  type Persistence,
  type Project,
  type ProjectRepository,
  type TelegramTopic,
  type TelegramTopicRepository,
  type TopicRef,
  type ChatMessageOptions,
} from "./domain.js";

export interface FakeAgentExecutorOptions {
  script?: readonly AgentEvent[];
  startGate?: Promise<void>;
  resumeGate?: Promise<void>;
  startError?: string;
  resumeError?: string;
}

export class FakeAgentExecutor implements AgentExecutor {
  readonly id = "fake";
  readonly name = "FakeAgent";
  readonly starts: StartSessionOptions[] = [];
  readonly resumes: ResumeSessionOptions[] = [];
  readonly sends: Array<{ session: AgentSession; input: AgentInput }> = [];
  readonly interrupts: AgentSession[] = [];
  readonly closes: AgentSession[] = [];
  private counter = 0;
  private readonly sessions = new Map<string, AgentSession>();
  private readonly knownNativeSessionIds = new Set<string>();
  private readonly streamGates: Promise<void>[] = [];
  private readonly streamGatesByPrompt = new Map<string, Promise<void>[]>();
  private script: readonly AgentEvent[] | undefined;
  private readonly startGate: Promise<void> | undefined;
  private readonly resumeGate: Promise<void> | undefined;
  private readonly startError: string | undefined;
  private readonly resumeError: string | undefined;

  constructor(options: FakeAgentExecutorOptions = {}) {
    this.script = options.script;
    this.startGate = options.startGate;
    this.resumeGate = options.resumeGate;
    this.startError = options.startError;
    this.resumeError = options.resumeError;
  }

  queueStreamGate(gate: Promise<void>): void {
    this.streamGates.push(gate);
  }

  queueStreamGateForPrompt(prompt: string, gate: Promise<void>): void {
    const gates = this.streamGatesByPrompt.get(prompt) ?? [];
    gates.push(gate);
    this.streamGatesByPrompt.set(prompt, gates);
  }

  setScript(script: readonly AgentEvent[]): void {
    this.script = script;
  }

  capabilities(): ExecutorCapabilities {
    return {
      resume: true,
      interrupt: true,
      close: true,
      sessionIdentity: true,
      streaming: true,
      approvals: true,
      models: true,
      fileAttachments: true,
      structuredOutput: true,
      worktrees: false,
    };
  }

  async start(options: StartSessionOptions): Promise<AgentSession> {
    this.starts.push(options);
    if (this.startError) throw new Error(this.startError);
    if (this.startGate) await abortableWait(this.startGate, options.signal);
    return this.createSession(options.projectId, options.workspacePath);
  }

  async *send(session: AgentSession, input: AgentInput): AsyncIterable<AgentEvent> {
    if (!this.sessions.has(session.runtimeSessionId)) throw new Error("Unknown fake session");
    this.sends.push({ session, input });
    const promptGates = this.streamGatesByPrompt.get(input.prompt);
    const gate = promptGates?.shift() ?? this.streamGates.shift();
    if (promptGates?.length === 0) this.streamGatesByPrompt.delete(input.prompt);
    if (gate) await abortableWait(gate, input.signal);

    const events = this.script ?? [
      {
        type: "session_identity",
        nativeSessionId: session.nativeSessionId ?? `fake-native-${session.runtimeSessionId}`,
        resumable: true,
        timestamp: new Date(0),
        sequence: 1,
      },
      {
        type: "text_delta",
        text: "Inspecting workspace…",
        timestamp: new Date(0),
        sequence: 2,
      },
      {
        type: "completed",
        summary: "No changes required.",
        exitCode: 0,
        timestamp: new Date(0),
        sequence: 3,
      },
    ];
    for (const event of events) {
      if (input.signal?.aborted) throw new Error("aborted");
      if (event.type === "session_identity") this.knownNativeSessionIds.add(event.nativeSessionId);
      yield event;
    }
  }

  async interrupt(session: AgentSession): Promise<void> {
    this.interrupts.push(session);
    if (!this.sessions.has(session.runtimeSessionId)) throw new Error("Unknown fake session");
  }

  async close(session: AgentSession): Promise<void> {
    this.closes.push(session);
    this.sessions.delete(session.runtimeSessionId);
  }

  async resume(options: ResumeSessionOptions): Promise<AgentSession> {
    this.resumes.push(options);
    if (this.resumeError) throw new Error(this.resumeError);
    if (this.resumeGate) await abortableWait(this.resumeGate, options.signal);
    if (!this.knownNativeSessionIds.has(options.nativeSessionId)) {
      throw new Error(`Unknown fake native session: ${options.nativeSessionId}`);
    }
    return this.createSession(options.projectId, options.workspacePath, options.nativeSessionId);
  }

  private createSession(
    projectId: string,
    workspacePath: string,
    nativeSessionId?: string,
  ): AgentSession {
    const session: AgentSession = {
      runtimeSessionId: `fake-runtime-${++this.counter}`,
      executorId: this.id,
      projectId,
      workspacePath,
      ...(nativeSessionId ? { nativeSessionId } : {}),
      createdAt: new Date(),
    };
    this.sessions.set(session.runtimeSessionId, session);
    return session;
  }
}

async function abortableWait(gate: Promise<void>, signal?: AbortSignal): Promise<void> {
  if (!signal) return gate;
  if (signal.aborted) throw new Error("aborted");
  await Promise.race([
    gate,
    new Promise<never>((_, reject) => {
      signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
    }),
  ]);
}

export class InMemoryChatGateway implements ChatGateway {
  readonly sent: Array<{
    target: { chatId: string; threadId?: string };
    text: string;
    options?: ChatMessageOptions;
  }> = [];
  readonly edits: Array<{ message: MessageRef; text: string }> = [];
  readonly topics: TopicRef[] = [];
  private messageId = 0;
  private threadId = 100;

  async sendMessage(
    target: { chatId: string; threadId?: string },
    text: string,
    options?: ChatMessageOptions,
  ): Promise<MessageRef> {
    this.sent.push(options ? { target, text, options } : { target, text });
    return target.threadId
      ? { chatId: target.chatId, threadId: target.threadId, messageId: String(++this.messageId) }
      : { chatId: target.chatId, messageId: String(++this.messageId) };
  }

  async editMessage(message: MessageRef, text: string): Promise<void> {
    this.edits.push({ message, text });
  }

  async sendDocument(): Promise<void> {}

  async createTopic(chatId: string, title: string): Promise<TopicRef> {
    const topic = { chatId, threadId: String(++this.threadId), title };
    this.topics.push(topic);
    return topic;
  }

  async closeTopic(topic: TopicRef): Promise<void> {
    const existing = this.topics.find(
      (candidate) => candidate.chatId === topic.chatId && candidate.threadId === topic.threadId,
    );
    if (existing) existing.title = `${existing.title} [closed]`;
  }
}

export class InMemoryPersistence implements Persistence {
  readonly projects = new InMemoryProjects();
  readonly executions = new InMemoryExecutions();
  readonly sessions = new InMemorySessions(this.executions);
  readonly executorSessions = new InMemoryExecutorSessions(this.executions);
  readonly topics = new InMemoryTopics();
  readonly audit = new InMemoryAuditLog();
}

class InMemoryProjects implements ProjectRepository {
  private readonly values = new Map<string, Project>();
  add(project: Project): void {
    this.values.set(project.id, project);
  }
  async getById(id: string): Promise<Project | undefined> {
    return this.values.get(id);
  }
  async list(): Promise<readonly Project[]> {
    return [...this.values.values()];
  }
}

class InMemorySessions implements BotSessionRepository {
  private readonly values = new Map<string, BotSession>();

  constructor(private readonly executions: InMemoryExecutions) {}

  async getById(id: string): Promise<BotSession | undefined> {
    return this.values.get(id);
  }
  async getByTopic(chatId: string, threadId: string): Promise<BotSession | undefined> {
    return [...this.values.values()].find(
      (session) => session.telegramChatId === chatId && session.telegramThreadId === threadId,
    );
  }
  async save(session: BotSession): Promise<void> {
    this.values.set(session.id, session);
  }

  async updateOwned(
    session: BotSession,
    executionId: string,
    ownerId: string,
    fence: number,
    now: Date,
  ): Promise<boolean> {
    const execution = await this.executions.getById(executionId);
    if (
      !execution ||
      execution.botSessionId !== session.id ||
      execution.ownerId !== ownerId ||
      execution.ownerFence !== fence ||
      !execution.leaseExpiresAt ||
      execution.leaseExpiresAt <= now
    ) {
      return false;
    }
    this.values.set(session.id, session);
    return true;
  }

  async list(): Promise<readonly BotSession[]> {
    return [...this.values.values()];
  }
}

class InMemoryExecutions implements ExecutionRepository {
  private readonly values = new Map<string, Execution>();

  async getById(id: string): Promise<Execution | undefined> {
    return this.values.get(id);
  }

  async save(execution: Execution): Promise<void> {
    this.values.set(execution.id, execution);
  }

  async listBySession(sessionId: string): Promise<readonly Execution[]> {
    return [...this.values.values()].filter((execution) => execution.botSessionId === sessionId);
  }

  async claim(
    executionId: string,
    ownerId: string,
    now: Date,
    leaseDurationMs: number,
  ): Promise<import("./domain.js").ExecutionLease | undefined> {
    const execution = this.values.get(executionId);
    if (!execution || execution.status !== "pending") return undefined;
    const active = [...this.values.values()].some(
      (candidate) =>
        candidate.botSessionId === execution.botSessionId &&
        candidate.id !== execution.id &&
        (candidate.status === "running" || candidate.status === "awaiting_approval") &&
        candidate.leaseExpiresAt !== undefined &&
        candidate.leaseExpiresAt.getTime() > now.getTime(),
    );
    if (active) return undefined;
    const fence = execution.ownerFence + 1;
    const leaseExpiresAt = new Date(now.getTime() + leaseDurationMs);
    this.values.set(execution.id, {
      ...execution,
      status: "running",
      ownerId,
      ownerFence: fence,
      leaseExpiresAt,
      startedAt: execution.startedAt ?? now,
    });
    return {
      executionId,
      botSessionId: execution.botSessionId,
      ownerId,
      fence,
      leaseExpiresAt,
      recovered: false,
    };
  }

  async renew(
    executionId: string,
    ownerId: string,
    fence: number,
    now: Date,
    leaseDurationMs: number,
  ): Promise<boolean> {
    const execution = this.values.get(executionId);
    if (
      !execution ||
      execution.ownerId !== ownerId ||
      execution.ownerFence !== fence ||
      (execution.status !== "running" && execution.status !== "awaiting_approval") ||
      !execution.leaseExpiresAt ||
      execution.leaseExpiresAt <= now
    ) {
      return false;
    }
    this.values.set(execution.id, {
      ...execution,
      leaseExpiresAt: new Date(now.getTime() + leaseDurationMs),
    });
    return true;
  }

  async updateOwned(
    execution: Execution,
    ownerId: string,
    fence: number,
    now: Date,
  ): Promise<boolean> {
    const current = this.values.get(execution.id);
    if (
      !current ||
      current.ownerId !== ownerId ||
      current.ownerFence !== fence ||
      !current.leaseExpiresAt ||
      current.leaseExpiresAt <= now
    )
      return false;
    this.values.set(execution.id, {
      ...execution,
      ownerFence: fence,
      ...(execution.status === "completed" ||
      execution.status === "failed" ||
      execution.status === "stopped" ||
      execution.status === "interrupted" ||
      execution.status === "unknown"
        ? { ownerId: undefined, leaseExpiresAt: undefined }
        : { ownerId, leaseExpiresAt: execution.leaseExpiresAt }),
    });
    return true;
  }

  async requestCancellation(
    executionId: string,
    requestedByUserId: string,
    now: Date,
  ): Promise<Execution | undefined> {
    const execution = this.values.get(executionId);
    if (
      !execution ||
      (execution.status !== "pending" &&
        execution.status !== "running" &&
        execution.status !== "awaiting_approval")
    ) {
      return execution;
    }
    const stopped = execution.status === "pending" && !execution.ownerId;
    const updated = {
      ...execution,
      ...(stopped
        ? {
            status: "stopped" as const,
            finishedAt: now,
            errorMessage: "Execution stopped before start.",
          }
        : {}),
      cancelRequestedAt: now,
      cancelRequestedByUserId: requestedByUserId,
    };
    this.values.set(executionId, updated);
    return updated;
  }

  async recoverAfterRestart(now: Date): Promise<readonly Execution[]> {
    const recovered = [];
    for (const execution of this.values.values()) {
      if (execution.status === "running" || execution.status === "awaiting_approval") {
        const updated: Execution = {
          ...execution,
          status: "unknown",
          finishedAt: now,
          errorMessage: "Execution owner was lost during restart; provider state was not guessed.",
          ownerId: undefined,
          leaseExpiresAt: undefined,
        };
        this.values.set(execution.id, updated);
        recovered.push(updated);
      }
    }
    const pending = [...this.values.values()].filter((execution) => execution.status === "pending");
    for (const execution of pending) {
      if (execution.ownerId || execution.leaseExpiresAt) {
        this.values.set(execution.id, {
          ...execution,
          ownerId: undefined,
          leaseExpiresAt: undefined,
        });
      }
    }
    return [...recovered, ...pending];
  }
}

export class InMemoryExecutorSessions implements ExecutorSessionRepository {
  private readonly values = new Map<string, ExecutorSession>();

  constructor(private readonly executions: InMemoryExecutions) {}

  async getById(id: string): Promise<ExecutorSession | undefined> {
    return this.values.get(id);
  }

  async save(session: ExecutorSession): Promise<void> {
    this.values.set(session.id, session);
  }

  async updateOwned(
    session: ExecutorSession,
    executionId: string,
    ownerId: string,
    fence: number,
    now: Date,
  ): Promise<boolean> {
    const execution = await this.executions.getById(executionId);
    if (
      !execution ||
      (execution.executorSessionId && execution.executorSessionId !== session.id) ||
      execution.ownerId !== ownerId ||
      execution.ownerFence !== fence ||
      !execution.leaseExpiresAt ||
      execution.leaseExpiresAt <= now
    ) {
      return false;
    }
    this.values.set(session.id, session);
    return true;
  }
}

class InMemoryTopics implements TelegramTopicRepository {
  private readonly values = new Map<string, TelegramTopic>();
  async get(chatId: string, threadId: string): Promise<TelegramTopic | undefined> {
    return this.values.get(`${chatId}:${threadId}`);
  }
  async save(topic: TelegramTopic): Promise<void> {
    this.values.set(`${topic.chatId}:${topic.threadId}`, topic);
  }
}

export class InMemoryAuditLog {
  readonly entries: Array<Record<string, unknown>> = [];
  async append(entry: Record<string, unknown>): Promise<void> {
    this.entries.push(entry);
  }
}
