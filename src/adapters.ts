import {
  type AgentEvent,
  type AgentExecutor,
  type AgentInput,
  type AgentSession,
  type BotSession,
  type BotSessionRepository,
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

export class FakeAgentExecutor implements AgentExecutor {
  readonly id = "fake";
  readonly name = "FakeAgent";
  private counter = 0;
  private readonly sessions = new Map<string, AgentSession>();

  constructor(private readonly script: readonly AgentEvent[] = defaultScript) {}

  capabilities() {
    return {
      resume: true,
      streaming: true,
      approvals: true,
      models: true,
      fileAttachments: true,
      structuredOutput: true,
      worktrees: false,
    } as const;
  }

  async start(options: { projectId: string; workspacePath: string }): Promise<AgentSession> {
    const session: AgentSession = {
      id: `fake-session-${++this.counter}`,
      executorId: this.id,
      projectId: options.projectId,
      workspacePath: options.workspacePath,
      externalSessionId: `fake-external-${this.counter}`,
      createdAt: new Date(),
    };
    this.sessions.set(session.id, session);
    return session;
  }

  async *send(session: AgentSession, input: AgentInput): AsyncIterable<AgentEvent> {
    if (!this.sessions.has(session.id)) throw new Error("Unknown fake session");
    for (const event of this.script) {
      if (input.signal?.aborted) throw new Error("aborted");
      yield event;
    }
  }

  async interrupt(session: AgentSession): Promise<void> {
    if (!this.sessions.has(session.id)) throw new Error("Unknown fake session");
  }

  async close(session: AgentSession): Promise<void> {
    this.sessions.delete(session.id);
  }

  async resume(options: {
    externalSessionId: string;
    projectId: string;
    workspacePath: string;
  }): Promise<AgentSession> {
    return this.start(options);
  }
}

const defaultScript: readonly AgentEvent[] = [
  { type: "text_delta", text: "Inspecting workspace…", timestamp: new Date(0), sequence: 1 },
  { type: "command", command: "bun test", timestamp: new Date(0), sequence: 2 },
  { type: "text_delta", text: "Tests passed.", timestamp: new Date(0), sequence: 3 },
  {
    type: "completed",
    summary: "No changes required.",
    exitCode: 0,
    timestamp: new Date(0),
    sequence: 4,
  },
];

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
  readonly sessions = new InMemorySessions();
  readonly executions = new InMemoryExecutions();
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
