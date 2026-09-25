import { expect, test } from "vitest";
import {
  AgentOrchestrator,
  AuthorizationError,
  AuthorizationService,
  InMemoryExecutorRegistry,
  parseReconcileCommand,
} from "../src/application.js";
import { FakeAgentExecutor, InMemoryChatGateway, InMemoryPersistence } from "../src/adapters.js";
import type { Execution } from "../src/domain.js";

function unknownExecution(botSessionId: string, prompt = "ambiguous"): Execution {
  return {
    id: crypto.randomUUID(),
    botSessionId,
    requestedByUserId: "operator",
    prompt,
    status: "unknown",
    correlationId: crypto.randomUUID(),
    finishedAt: new Date(1_000),
    errorMessage: "Executor stream ended without a terminal event.",
    ownerFence: 1,
  };
}

function pendingExecution(botSessionId: string, prompt = "pending"): Execution {
  return {
    id: crypto.randomUUID(),
    botSessionId,
    requestedByUserId: "operator",
    prompt,
    status: "pending",
    correlationId: crypto.randomUUID(),
    ownerFence: 0,
  };
}

function message(threadId: string, userId: string, text: string) {
  return { chatId: "chat", threadId, userId, text };
}

function inMemoryFixture(executor = new FakeAgentExecutor()) {
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

test("parses only the explicit reconciliation commands and requires a reason", () => {
  expect(parseReconcileCommand("/reconcile")).toEqual({ kind: "status" });
  expect(parseReconcileCommand("/reconcile status")).toEqual({ kind: "status" });
  expect(parseReconcileCommand("/reconcile e1 complete verified externally")).toEqual({
    kind: "mutation",
    executionId: "e1",
    outcome: "confirmed_completed",
    note: "verified externally",
  });
  expect(parseReconcileCommand("/reconcile e1 abandon   ")).toBeUndefined();
  expect(parseReconcileCommand("/reconcile e1 failed wrong assertion")).toBeUndefined();
});

test("viewer can inspect unknown safely, while operator reconciliation is immutable", async () => {
  const { persistence, gateway, executor, orchestrator } = inMemoryFixture();
  const session = await createSession(orchestrator);
  const sensitiveProviderError = "provider-token=secret raw-argv=--api-key";
  const execution = {
    ...unknownExecution(session.id, "sensitive prompt"),
    errorMessage: sensitiveProviderError,
  };
  await persistence.executions.save(execution);
  await persistence.sessions.save({ ...session, status: "failed" });

  await orchestrator.handleMessage(
    message(session.telegramThreadId, "viewer", "/reconcile status"),
  );
  const status = gateway.sent.at(-1)?.text ?? "";
  expect(status).toContain(execution.id);
  expect(status).toContain(execution.correlationId);
  expect(status).not.toContain(sensitiveProviderError);
  expect(status).not.toContain(execution.prompt);
  await orchestrator.handleMessage(message(session.telegramThreadId, "viewer", "/status"));
  const sessionStatus = gateway.sent.at(-1)?.text ?? "";
  expect(sessionStatus).toContain("unresolved execution ambiguity");
  expect(sessionStatus).not.toContain(sensitiveProviderError);
  expect(sessionStatus).not.toContain(execution.prompt);

  await expect(
    orchestrator.handleMessage(
      message(session.telegramThreadId, "viewer", `/reconcile ${execution.id} abandon no result`),
    ),
  ).rejects.toBeInstanceOf(AuthorizationError);

  await orchestrator.handleMessage(
    message(
      session.telegramThreadId,
      "operator",
      `/reconcile ${execution.id} abandon no reliable result`,
    ),
  );
  expect((await persistence.executions.getById(execution.id))?.status).toBe("unknown");
  expect((await persistence.sessions.getById(session.id))?.status).toBe("idle");
  expect(await persistence.reconciliations.listBySession(session.id)).toMatchObject([
    { executionId: execution.id, outcome: "abandoned", note: "no reliable result" },
  ]);
  expect(persistence.audit.entries).toEqual(
    expect.arrayContaining([
      expect.objectContaining({
        action: "execution.reconciled",
        userId: "operator",
        executionId: execution.id,
        metadata: { outcome: "abandoned" },
      }),
    ]),
  );
  expect(executor.starts).toHaveLength(0);
  expect(executor.sends).toHaveLength(0);

  await orchestrator.handleMessage(
    message(
      session.telegramThreadId,
      "operator",
      `/reconcile ${execution.id} complete conflicting result`,
    ),
  );
  expect((await persistence.reconciliations.listBySession(session.id))[0]?.outcome).toBe(
    "abandoned",
  );
  expect(gateway.sent.at(-1)?.text).toContain("already reconciled as abandoned");
});

test("unknown executions quarantine prompts and all pending claims until final reconciliation", async () => {
  const { persistence, executor, orchestrator } = inMemoryFixture();
  const session = await createSession(orchestrator);
  const first = unknownExecution(session.id, "a1");
  const second = unknownExecution(session.id, "a1-legacy");
  const pending = pendingExecution(session.id, "a2");
  await persistence.executions.save(first);
  await persistence.executions.save(second);
  await persistence.executions.save(pending);
  await persistence.sessions.save({ ...session, status: "failed" });

  await orchestrator.handleMessage(message(session.telegramThreadId, "operator", "new prompt"));
  expect(
    (await persistence.executions.listBySession(session.id)).filter(
      (item) => item.prompt === "new prompt",
    ),
  ).toHaveLength(0);
  expect(
    await persistence.executions.claim(pending.id, "worker", new Date(2_000), 1_000),
  ).toBeUndefined();
  expect((await persistence.sessions.getById(session.id))?.status).toBe("failed");

  await persistence.reconciliations.reconcile(first.id, session.id, {
    executionId: first.id,
    outcome: "confirmed_completed",
    reconciledByUserId: "operator",
    note: "externally verified",
    reconciledAt: new Date(2_001),
  });
  expect(
    await persistence.executions.claim(pending.id, "worker", new Date(2_002), 1_000),
  ).toBeUndefined();

  await persistence.reconciliations.reconcile(second.id, session.id, {
    executionId: second.id,
    outcome: "abandoned",
    reconciledByUserId: "operator",
    note: "no reliable result",
    reconciledAt: new Date(2_003),
  });
  expect((await persistence.sessions.getById(session.id))?.status).toBe("idle");
  expect(
    await persistence.executions.claim(pending.id, "worker", new Date(2_004), 1_000),
  ).toMatchObject({
    executionId: pending.id,
  });
  expect(executor.starts).toHaveLength(0);
});

test("rejects missing, cross-session, non-unknown, and blank-note reconciliations", async () => {
  const { persistence, orchestrator } = inMemoryFixture();
  const firstSession = await createSession(orchestrator);
  const secondSession = await createSession(orchestrator);
  const unknown = unknownExecution(firstSession.id);
  const completed = { ...pendingExecution(firstSession.id), status: "completed" as const };
  await persistence.executions.save(unknown);
  await persistence.executions.save(completed);

  expect(
    await persistence.reconciliations.reconcile("missing", firstSession.id, {
      executionId: "missing",
      outcome: "abandoned",
      reconciledByUserId: "operator",
      note: "not found",
      reconciledAt: new Date(),
    }),
  ).toEqual({ status: "not_found" });
  expect(
    await persistence.reconciliations.reconcile(unknown.id, secondSession.id, {
      executionId: unknown.id,
      outcome: "abandoned",
      reconciledByUserId: "operator",
      note: "wrong topic",
      reconciledAt: new Date(),
    }),
  ).toEqual({ status: "wrong_session" });
  expect(
    await persistence.reconciliations.reconcile(completed.id, firstSession.id, {
      executionId: completed.id,
      outcome: "abandoned",
      reconciledByUserId: "operator",
      note: "not ambiguous",
      reconciledAt: new Date(),
    }),
  ).toEqual({ status: "not_unknown" });
  await expect(
    persistence.reconciliations.reconcile(unknown.id, firstSession.id, {
      executionId: unknown.id,
      outcome: "abandoned",
      reconciledByUserId: "operator",
      note: "   ",
      reconciledAt: new Date(),
    }),
  ).rejects.toThrow("Reconciliation note is required");
});

test("every stream ambiguity quarantines the session and emits an explicit audit event", async () => {
  const executor = new FakeAgentExecutor({
    script: [
      {
        type: "session_identity",
        nativeSessionId: "native-unknown",
        resumable: true,
        timestamp: new Date(0),
        sequence: 1,
      },
    ],
  });
  const { persistence, orchestrator } = inMemoryFixture(executor);
  const session = await createSession(orchestrator);
  await orchestrator.handleMessage(
    message(session.telegramThreadId, "operator", "ambiguous prompt"),
  );
  const execution = (await persistence.executions.listBySession(session.id))[0]!;
  expect(execution.status).toBe("unknown");
  expect((await persistence.sessions.getById(session.id))?.status).toBe("failed");
  expect(persistence.audit.entries).toEqual(
    expect.arrayContaining([
      expect.objectContaining({
        action: "execution.reconciliation_required",
        executionId: execution.id,
        sessionId: session.id,
      }),
    ]),
  );
  expect(executor.starts).toHaveLength(1);
  expect(executor.sends).toHaveLength(1);
});
