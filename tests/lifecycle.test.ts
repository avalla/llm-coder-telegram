import { describe, expect, it } from "vitest";
import {
  AgentOrchestrator,
  AuthorizationService,
  InMemoryExecutorRegistry,
} from "../src/application.js";
import { FakeAgentExecutor, InMemoryChatGateway, InMemoryPersistence } from "../src/adapters.js";
import type { AgentEvent, IncomingMessage } from "../src/domain.js";
import { runTelegramPolling } from "../src/telegram.js";
import type { TelegramApiGateway } from "../src/telegram.js";

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((value) => {
    resolve = value;
  });
  return { promise, resolve };
}

async function waitUntil(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) return;
    await Promise.resolve();
  }
  throw new Error("Condition was not reached");
}

function eventScript(...events: AgentEvent[]): readonly AgentEvent[] {
  return events;
}

function identity(nativeSessionId = "native-x"): AgentEvent {
  return {
    type: "session_identity",
    nativeSessionId,
    resumable: true,
    timestamp: new Date(0),
    sequence: 1,
  };
}

function completed(): AgentEvent {
  return {
    type: "completed",
    summary: "done",
    exitCode: 0,
    timestamp: new Date(0),
    sequence: 2,
  };
}

class NonResumableFakeAgentExecutor extends FakeAgentExecutor {
  override capabilities() {
    return { ...super.capabilities(), resume: false };
  }
}

function fixture(executor = new FakeAgentExecutor()) {
  const persistence = new InMemoryPersistence();
  persistence.projects.add({
    id: "project",
    name: "Project",
    workspacePath: "/workspace/project",
    allowedExecutorIds: ["fake"],
  });
  const gateway = new InMemoryChatGateway();
  const registry = new InMemoryExecutorRegistry();
  registry.register(executor);
  const orchestrator = new AgentOrchestrator(
    persistence,
    registry,
    gateway,
    new AuthorizationService({
      allowedChatIds: ["chat"],
      users: { operator: "operator", viewer: "viewer" },
    }),
  );
  return { persistence, gateway, executor, orchestrator };
}

async function createSession(orchestrator: AgentOrchestrator) {
  return orchestrator.createSession({
    chatId: "chat",
    userId: "operator",
    projectId: "project",
    executorId: "fake",
  });
}

function message(sessionId: string, text: string): IncomingMessage {
  return {
    chatId: "chat",
    threadId: sessionId,
    userId: "operator",
    text,
  };
}

describe("executor/session lifecycle hardening", () => {
  it("reloads session state inside the queue and resumes the native session", async () => {
    const firstStream = deferred();
    const { persistence, executor, orchestrator } = fixture();
    executor.queueStreamGate(firstStream.promise);
    const session = await createSession(orchestrator);

    const first = orchestrator.handleMessage(message(session.telegramThreadId, "first"));
    await waitUntil(() => executor.sends.length === 1);
    const second = orchestrator.handleMessage(message(session.telegramThreadId, "second"));
    firstStream.resolve();
    await Promise.all([first, second]);

    expect(executor.starts).toHaveLength(1);
    expect(executor.resumes).toHaveLength(1);
    expect(executor.resumes[0]?.nativeSessionId).toBe("fake-native-fake-runtime-1");
    expect(
      (await persistence.executions.listBySession(session.id)).map((item) => item.status),
    ).toEqual(["completed", "completed"]);
  });

  it("runs different BotSessions concurrently", async () => {
    const firstStream = deferred();
    const { executor, orchestrator } = fixture();
    executor.queueStreamGate(firstStream.promise);
    const first = await createSession(orchestrator);
    const second = await createSession(orchestrator);

    const firstRun = orchestrator.handleMessage(message(first.telegramThreadId, "first"));
    await waitUntil(() => executor.sends.length === 1);
    const secondRun = orchestrator.handleMessage(message(second.telegramThreadId, "second"));
    await waitUntil(() => executor.sends.length === 2);

    firstStream.resolve();
    await Promise.all([firstRun, secondRun]);
    expect(executor.starts).toHaveLength(2);
  });

  it("keeps polling responsive while a handler is still running", async () => {
    const handlerGate = deferred();
    const abort = new AbortController();
    let polls = 0;
    let secondPollObserved = false;
    const gateway = {
      poll: async () => {
        polls += 1;
        if (polls === 1) {
          return {
            messages: [{ chatId: "chat", threadId: "1", userId: "operator", text: "first" }],
          };
        }
        secondPollObserved = true;
        abort.abort();
        return { messages: [], nextOffset: polls };
      },
    } as unknown as TelegramApiGateway;

    const polling = runTelegramPolling(
      gateway,
      async (incoming) => {
        if (incoming.text === "first") await handlerGate.promise;
      },
      abort.signal,
    );
    await waitUntil(() => secondPollObserved);
    expect(secondPollObserved).toBe(true);
    handlerGate.resolve();
    await polling;
  });

  it("stops a running execution and makes repeated stop requests harmless", async () => {
    const streamGate = deferred();
    const { persistence, executor, orchestrator } = fixture();
    executor.queueStreamGate(streamGate.promise);
    const session = await createSession(orchestrator);
    const run = orchestrator.handleMessage(message(session.telegramThreadId, "running"));
    await waitUntil(() => executor.sends.length === 1);

    await orchestrator.handleMessage(message(session.telegramThreadId, "/stop"));
    await orchestrator.handleMessage(message(session.telegramThreadId, "/stop"));
    await run;

    expect(executor.interrupts).toHaveLength(1);
    expect((await persistence.executions.listBySession(session.id))[0]?.status).toBe("stopped");
  });

  it("cancels during start and resume", async () => {
    const startGate = deferred();
    const startingExecutor = new FakeAgentExecutor({ startGate: startGate.promise });
    const starting = fixture(startingExecutor);
    const startingSession = await createSession(starting.orchestrator);
    const startingRun = starting.orchestrator.handleMessage(
      message(startingSession.telegramThreadId, "start"),
    );
    await waitUntil(() => startingExecutor.starts.length === 1);
    await starting.orchestrator.handleMessage(message(startingSession.telegramThreadId, "/stop"));
    await startingRun;
    expect(
      (await starting.persistence.executions.listBySession(startingSession.id))[0]?.status,
    ).toBe("stopped");

    const resumeGate = deferred();
    const resumeExecutor = new FakeAgentExecutor({ resumeGate: resumeGate.promise });
    const resumed = fixture(resumeExecutor);
    const resumedSession = await createSession(resumed.orchestrator);
    await resumed.orchestrator.handleMessage(message(resumedSession.telegramThreadId, "first"));
    const resumeRun = resumed.orchestrator.handleMessage(
      message(resumedSession.telegramThreadId, "resume"),
    );
    await waitUntil(() => resumeExecutor.resumes.length === 1);
    await resumed.orchestrator.handleMessage(message(resumedSession.telegramThreadId, "/stop"));
    await resumeRun;
    expect(
      (await resumed.persistence.executions.listBySession(resumedSession.id)).at(-1)?.status,
    ).toBe("stopped");
  });

  it("maps aborted EOF to stopped, terminal errors to failed, and bare EOF to unknown", async () => {
    const abortedGate = deferred();
    const abortedExecutor = new FakeAgentExecutor();
    abortedExecutor.queueStreamGate(abortedGate.promise);
    const aborted = fixture(abortedExecutor);
    const abortedSession = await createSession(aborted.orchestrator);
    const abortedRun = aborted.orchestrator.handleMessage(
      message(abortedSession.telegramThreadId, "abort"),
    );
    await waitUntil(() => abortedExecutor.sends.length === 1);
    await aborted.orchestrator.handleMessage(message(abortedSession.telegramThreadId, "/stop"));
    await abortedRun;
    expect((await aborted.persistence.executions.listBySession(abortedSession.id))[0]?.status).toBe(
      "stopped",
    );

    const errorExecutor = new FakeAgentExecutor({
      script: eventScript(identity("native-error"), {
        type: "error",
        message: "provider failed",
        retryable: false,
        timestamp: new Date(0),
        sequence: 2,
      }),
    });
    const failed = fixture(errorExecutor);
    const failedSession = await createSession(failed.orchestrator);
    await failed.orchestrator.handleMessage(message(failedSession.telegramThreadId, "error"));
    expect((await failed.persistence.executions.listBySession(failedSession.id))[0]?.status).toBe(
      "failed",
    );

    const conflictExecutor = new FakeAgentExecutor({
      script: eventScript(identity("native-conflict"), completed(), {
        type: "error",
        message: "conflicting terminal",
        retryable: false,
        timestamp: new Date(0),
        sequence: 3,
      }),
    });
    const conflict = fixture(conflictExecutor);
    const conflictSession = await createSession(conflict.orchestrator);
    await conflict.orchestrator.handleMessage(
      message(conflictSession.telegramThreadId, "conflict"),
    );
    expect(
      (await conflict.persistence.executions.listBySession(conflictSession.id))[0]?.status,
    ).toBe("failed");

    const eofExecutor = new FakeAgentExecutor({ script: eventScript(identity("native-eof")) });
    const unknown = fixture(eofExecutor);
    const unknownSession = await createSession(unknown.orchestrator);
    await unknown.orchestrator.handleMessage(message(unknownSession.telegramThreadId, "eof"));
    expect((await unknown.persistence.executions.listBySession(unknownSession.id))[0]?.status).toBe(
      "unknown",
    );
  });

  it("rejects close while active, closes the executor once, and never returns to idle", async () => {
    const streamGate = deferred();
    const { persistence, executor, gateway, orchestrator } = fixture();
    executor.queueStreamGate(streamGate.promise);
    const session = await createSession(orchestrator);
    const run = orchestrator.handleMessage(message(session.telegramThreadId, "work"));
    await waitUntil(() => executor.sends.length === 1);

    await orchestrator.handleMessage(message(session.telegramThreadId, "/close"));
    expect((await persistence.sessions.getById(session.id))?.status).toBe("running");
    expect(gateway.sent.at(-1)?.text).toContain("active");
    expect(executor.closes).toHaveLength(0);

    await orchestrator.handleMessage(message(session.telegramThreadId, "/stop"));
    await run;
    await orchestrator.handleMessage(message(session.telegramThreadId, "/close"));
    await orchestrator.handleMessage(message(session.telegramThreadId, "/close"));
    await expect(
      orchestrator.handleMessage(message(session.telegramThreadId, "late prompt")),
    ).rejects.toThrow("closed");

    expect((await persistence.sessions.getById(session.id))?.status).toBe("closed");
    expect(executor.closes).toHaveLength(1);
  });

  it("persists native identity learned during the stream", async () => {
    const { persistence, orchestrator } = fixture(
      new FakeAgentExecutor({ script: eventScript(identity("learned-native"), completed()) }),
    );
    const session = await createSession(orchestrator);
    await orchestrator.handleMessage(message(session.telegramThreadId, "learn"));

    const storedSession = await persistence.sessions.getById(session.id);
    const executorSession = storedSession?.executorSessionId
      ? await persistence.executorSessions.getById(storedSession.executorSessionId)
      : undefined;
    expect(executorSession?.nativeSessionId).toBe("learned-native");
    expect(executorSession?.resumable).toBe(true);
  });

  it("uses the requested native identity when fake executor resumes", async () => {
    const executor = new FakeAgentExecutor({ script: eventScript(identity("requested-native")) });
    const session = await executor.start({
      projectId: "project",
      workspacePath: "/workspace/project",
    });
    for await (const _event of executor.send(session, { prompt: "learn" })) {
      break;
    }
    const resumed = await executor.resume({
      nativeSessionId: "requested-native",
      projectId: "project",
      workspacePath: "/workspace/project",
    });
    expect(resumed.nativeSessionId).toBe("requested-native");
    expect(executor.resumes[0]?.nativeSessionId).toBe("requested-native");
  });
});

describe("native identity and command contracts", () => {
  it("keeps repeated identical identities idempotent", async () => {
    const executor = new FakeAgentExecutor({
      script: eventScript(identity("native-repeat"), identity("native-repeat"), completed()),
    });
    const { persistence, orchestrator } = fixture(executor);
    const session = await createSession(orchestrator);
    await orchestrator.handleMessage(message(session.telegramThreadId, "repeat"));

    const stored = await persistence.sessions.getById(session.id);
    const native = stored?.executorSessionId
      ? await persistence.executorSessions.getById(stored.executorSessionId)
      : undefined;
    expect(native?.nativeSessionId).toBe("native-repeat");
    expect((await persistence.executions.listBySession(session.id))[0]?.status).toBe("completed");
  });

  it("fails closed when resume capability is unavailable", async () => {
    const executor = new NonResumableFakeAgentExecutor({
      script: eventScript(identity("native-no-resume"), completed()),
    });
    const { persistence, orchestrator } = fixture(executor);
    const session = await createSession(orchestrator);
    await orchestrator.handleMessage(message(session.telegramThreadId, "first"));
    await orchestrator.handleMessage(message(session.telegramThreadId, "second"));

    expect(executor.starts).toHaveLength(1);
    expect(executor.resumes).toHaveLength(0);
    expect((await persistence.executions.listBySession(session.id)).at(-1)?.status).toBe("failed");
  });

  it("rejects a conflicting identity and retains the persisted identity", async () => {
    const executor = new FakeAgentExecutor({
      script: eventScript(identity("native-a"), completed()),
    });
    const { persistence, orchestrator } = fixture(executor);
    const session = await createSession(orchestrator);
    await orchestrator.handleMessage(message(session.telegramThreadId, "first"));

    executor.setScript(eventScript(identity("native-b"), completed()));
    await orchestrator.handleMessage(message(session.telegramThreadId, "second"));

    const stored = await persistence.sessions.getById(session.id);
    const native = stored?.executorSessionId
      ? await persistence.executorSessions.getById(stored.executorSessionId)
      : undefined;
    expect(native?.nativeSessionId).toBe("native-a");
    expect((await persistence.executions.listBySession(session.id)).at(-1)?.status).toBe("failed");
    expect(
      (await persistence.audit.entries).some(
        (entry) => entry.action === "execution.session_identity_conflict",
      ),
    ).toBe(true);
  });

  it("exposes provider-neutral session state through /session", async () => {
    const { gateway, orchestrator } = fixture();
    const session = await createSession(orchestrator);
    await orchestrator.handleMessage(message(session.telegramThreadId, "learn"));
    await orchestrator.handleMessage({
      ...message(session.telegramThreadId, "/session"),
      userId: "viewer",
    });

    const text = gateway.sent.at(-1)?.text ?? "";
    expect(text).toContain(`Telegram topic: chat/${session.telegramThreadId}`);
    expect(text).toContain("Runtime session:");
    expect(text).toContain("Executor: FakeAgent (fake)");
    expect(text).toContain("Native session:");
    expect(text).toContain("Resume supported: yes");
  });
});
