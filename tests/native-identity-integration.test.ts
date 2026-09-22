import { describe, expect, it } from "vitest";
import {
  AgentOrchestrator,
  AuthorizationService,
  InMemoryExecutorRegistry,
} from "../src/application.js";
import { InMemoryChatGateway, InMemoryPersistence } from "../src/adapters.js";
import { ClaudeCodeExecutor, CodexCliExecutor } from "../src/provider-adapters.js";
import type { ProcessExit, ProcessHandle, ProcessRunner, ProcessSpec } from "../src/process.js";

const nativeA = "11111111-1111-4111-8111-111111111111";
const nativeB = "22222222-2222-4222-8222-222222222222";

class ScriptedRunner implements ProcessRunner {
  readonly specs: ProcessSpec[] = [];
  private readonly scripts: string[][];

  constructor(scripts: readonly string[][]) {
    this.scripts = scripts.map((script) => [...script]);
  }

  async spawn(spec: ProcessSpec): Promise<ProcessHandle> {
    this.specs.push(spec);
    const lines = this.scripts.shift();
    if (!lines) throw new Error("Missing scripted process");
    return new ScriptedHandle(lines);
  }
}

class ScriptedHandle implements ProcessHandle {
  readonly stdout: AsyncIterable<Uint8Array>;
  readonly stderr = emptyStream();

  constructor(private readonly lines: readonly string[]) {
    this.stdout = this.output();
  }

  async *output(): AsyncIterable<Uint8Array> {
    const encoder = new TextEncoder();
    for (const line of this.lines) yield encoder.encode(`${line}\n`);
  }

  async writeStdin(): Promise<void> {}
  async closeStdin(): Promise<void> {}
  async signal(): Promise<void> {}
  async wait(): Promise<ProcessExit> {
    return { code: 0 };
  }
}

async function* emptyStream(): AsyncIterable<Uint8Array> {}

function fixture(executor: CodexCliExecutor | ClaudeCodeExecutor) {
  const persistence = new InMemoryPersistence();
  persistence.projects.add({
    id: "project",
    name: "Project",
    workspacePath: "/workspace/project",
    allowedExecutorIds: [executor.id],
  });
  const gateway = new InMemoryChatGateway();
  const registry = new InMemoryExecutorRegistry();
  registry.register(executor);
  const orchestrator = new AgentOrchestrator(
    persistence,
    registry,
    gateway,
    new AuthorizationService({ allowedChatIds: ["chat"], users: { operator: "operator" } }),
  );
  return { persistence, orchestrator };
}

describe("real provider identity authority", () => {
  it.each([
    [
      "codex",
      (runner: ProcessRunner) => new CodexCliExecutor({ runner }),
      (id: string) => [
        JSON.stringify({ type: "thread.started", thread_id: id }),
        JSON.stringify({ type: "turn.completed" }),
      ],
    ],
    [
      "claude",
      (runner: ProcessRunner) => new ClaudeCodeExecutor({ runner }),
      (id: string) => [
        JSON.stringify({ type: "system", subtype: "init", session_id: id }),
        JSON.stringify({ type: "result", subtype: "success", session_id: id }),
      ],
    ],
  ])("retains A and audits one conflict for %s", async (_name, create, lines) => {
    const runner = new ScriptedRunner([lines(nativeA), lines(nativeB)]);
    const executor = create(runner);
    const { persistence, orchestrator } = fixture(executor);
    const session = await orchestrator.createSession({
      chatId: "chat",
      userId: "operator",
      projectId: "project",
      executorId: executor.id,
    });
    const message = (text: string) => ({
      chatId: "chat",
      threadId: session.telegramThreadId,
      userId: "operator",
      text,
    });

    await orchestrator.handleMessage(message("first"));
    await orchestrator.handleMessage(message("second"));

    const stored = await persistence.sessions.getById(session.id);
    const executorSession = await persistence.executorSessions.getById(stored!.executorSessionId!);
    expect(executorSession?.nativeSessionId).toBe(nativeA);
    expect((await persistence.executions.listBySession(session.id)).at(-1)?.status).toBe("failed");
    expect(
      persistence.audit.entries.filter(
        (entry) => entry.action === "execution.session_identity_conflict",
      ),
    ).toHaveLength(1);
    expect(runner.specs[1]?.args).toContain(nativeA);
    expect(runner.specs[1]?.args).not.toContain(nativeB);
  });
});
