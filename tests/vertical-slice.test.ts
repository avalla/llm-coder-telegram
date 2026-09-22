import { describe, expect, it } from "vitest";
import {
  AgentOrchestrator,
  AuthorizationError,
  AuthorizationService,
  InMemoryExecutorRegistry,
  SessionQueue,
  parseNewCommand,
} from "../src/application.js";
import { FakeAgentExecutor, InMemoryChatGateway, InMemoryPersistence } from "../src/adapters.js";
import { assertConfiguredWorkspacePath } from "../src/config.js";

function fixture() {
  const persistence = new InMemoryPersistence();
  persistence.projects.add({
    id: "ai-office",
    name: "ai-office",
    workspacePath: "/workspace/ai-office",
    allowedExecutorIds: ["fake"],
  });
  const gateway = new InMemoryChatGateway();
  const registry = new InMemoryExecutorRegistry();
  registry.register(new FakeAgentExecutor());
  const authorization = new AuthorizationService({
    allowedChatIds: ["chat-1"],
    users: { "owner-1": "owner", "operator-1": "operator", "viewer-1": "viewer" },
  });
  const orchestrator = new AgentOrchestrator(persistence, registry, gateway, authorization);
  return { persistence, gateway, orchestrator };
}

describe("Telegram topic vertical slice", () => {
  it("routes a plain prompt through topic, BotSession, Execution and FakeAgentExecutor", async () => {
    const { persistence, gateway, orchestrator } = fixture();
    const session = await orchestrator.createSession({
      chatId: "chat-1",
      userId: "operator-1",
      projectId: "ai-office",
      executorId: "fake",
      title: "[Fake] ai-office · test",
    });

    await orchestrator.handleMessage({
      chatId: "chat-1",
      threadId: session.telegramThreadId,
      userId: "operator-1",
      text: "Run the tests",
    });

    const executions = await persistence.executions.listBySession(session.id);
    expect(executions).toHaveLength(1);
    expect(executions[0]?.status).toBe("completed");
    expect((await persistence.sessions.getById(session.id))?.status).toBe("idle");
    expect(gateway.sent[0]?.target).toEqual({
      chatId: "chat-1",
      threadId: session.telegramThreadId,
    });
    expect(gateway.edits.at(-1)?.text).toContain("Completed");
  });

  it("keeps two topics isolated", async () => {
    const { persistence, orchestrator } = fixture();
    const first = await orchestrator.createSession({
      chatId: "chat-1",
      userId: "operator-1",
      projectId: "ai-office",
      executorId: "fake",
    });
    const second = await orchestrator.createSession({
      chatId: "chat-1",
      userId: "operator-1",
      projectId: "ai-office",
      executorId: "fake",
    });

    await Promise.all([
      orchestrator.handleMessage({
        chatId: "chat-1",
        threadId: first.telegramThreadId,
        userId: "operator-1",
        text: "first",
      }),
      orchestrator.handleMessage({
        chatId: "chat-1",
        threadId: second.telegramThreadId,
        userId: "operator-1",
        text: "second",
      }),
    ]);

    expect((await persistence.executions.listBySession(first.id))[0]?.prompt).toBe("first");
    expect((await persistence.executions.listBySession(second.id))[0]?.prompt).toBe("second");
  });

  it("enforces viewer read-only policy server-side", async () => {
    const { orchestrator } = fixture();
    const session = await orchestrator.createSession({
      chatId: "chat-1",
      userId: "operator-1",
      projectId: "ai-office",
      executorId: "fake",
    });

    await expect(
      orchestrator.handleMessage({
        chatId: "chat-1",
        threadId: session.telegramThreadId,
        userId: "viewer-1",
        text: "change files",
      }),
    ).rejects.toEqual(expect.any(AuthorizationError));
  });

  it("creates a session from the Control topic", async () => {
    const { persistence, orchestrator } = fixture();
    await persistence.topics.save({
      chatId: "chat-1",
      threadId: "control-1",
      kind: "control",
      title: "Control",
      status: "open",
      updatedAt: new Date(),
    });

    await orchestrator.handleMessage({
      chatId: "chat-1",
      threadId: "control-1",
      userId: "operator-1",
      text: '/new ai-office fake "PR61 hardening"',
    });

    const sessions = await persistence.sessions.list();
    expect(sessions).toHaveLength(1);
    expect((await persistence.topics.get("chat-1", sessions[0]!.telegramThreadId))?.kind).toBe(
      "workspace",
    );
  });
});

describe("core policies", () => {
  it("serializes executions per session while allowing different keys to proceed", async () => {
    const queue = new SessionQueue();
    const order: string[] = [];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const first = queue.run("session-a", async () => {
      order.push("a1");
      await gate;
      order.push("a2");
    });
    const second = queue.run("session-a", async () => {
      order.push("b");
    });
    const other = queue.run("session-b", async () => {
      order.push("c");
    });
    await other;
    expect(order).toEqual(["a1", "c"]);
    release();
    await Promise.all([first, second]);
    expect(order).toEqual(["a1", "c", "a2", "b"]);
  });

  it("parses the control command and rejects unsafe workspace roots", () => {
    expect(parseNewCommand('/new ai-office codex "PR61 hardening"')).toEqual({
      projectId: "ai-office",
      executorId: "codex",
      title: "PR61 hardening",
    });
    expect(() => assertConfiguredWorkspacePath("/tmp", [process.cwd()])).toThrow(
      "outside configured roots",
    );
    expect(assertConfiguredWorkspacePath(process.cwd(), [process.cwd()])).toBe(process.cwd());
  });
});
