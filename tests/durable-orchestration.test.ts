import { unlinkSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { expect, test } from "bun:test";
import {
  AgentOrchestrator,
  AuthorizationService,
  InMemoryExecutorRegistry,
} from "../src/application.js";
import { FakeAgentExecutor, InMemoryChatGateway } from "../src/adapters.js";
import type { Execution, ExecutorSession } from "../src/domain.js";
import { SqliteExecutionJobQueue, SqlitePersistence } from "../src/sqlite.js";

async function waitUntil(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("Condition was not reached");
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((value) => {
    resolve = value;
  });
  return { promise, resolve };
}

function insertExecutionWithJob(
  persistence: SqlitePersistence,
  execution: Execution,
  availableAt = Date.now(),
): void {
  const now = Date.now();
  persistence.db.run("BEGIN IMMEDIATE");
  try {
    persistence.db
      .query(
        `INSERT INTO executions
          (id, bot_session_id, requested_by_user_id, prompt, status, correlation_id,
           executor_session_id, started_at, finished_at, error_message, owner_id,
           owner_fence, lease_expires_at, cancel_requested_at, cancel_requested_by_user_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
    persistence.db
      .query(
        `INSERT INTO execution_jobs
          (execution_id, status, attempts, available_at, created_at, updated_at)
         VALUES (?, 'queued', 0, ?, ?, ?)`,
      )
      .run(execution.id, availableAt, now, now);
    persistence.db.run("COMMIT");
  } catch (error) {
    persistence.db.run("ROLLBACK");
    throw error;
  }
}

function message(threadId: string, text: string) {
  return { chatId: "chat", threadId, userId: "operator", text };
}

async function fixture(path: string, executor: FakeAgentExecutor, concurrency = 2) {
  const persistence = SqlitePersistence.open(path);
  persistence.saveProject({
    id: "project",
    name: "Project",
    workspacePath: "/workspace/project",
    allowedExecutorIds: ["fake"],
  });
  const gateway = new InMemoryChatGateway();
  const registry = new InMemoryExecutorRegistry();
  registry.register(executor);
  const queue = new SqliteExecutionJobQueue(persistence.db, {
    concurrency,
    pollIntervalMs: 2,
  });
  const orchestrator = new AgentOrchestrator(
    persistence,
    registry,
    gateway,
    new AuthorizationService({ allowedChatIds: ["chat"], users: { operator: "operator" } }),
    undefined,
    undefined,
    queue,
    queue,
  );
  return { persistence, gateway, orchestrator, queue };
}

async function createSession(orchestrator: AgentOrchestrator) {
  return orchestrator.createSession({
    chatId: "chat",
    userId: "operator",
    projectId: "project",
    executorId: "fake",
  });
}

test("durable queued execution survives restart and is delivered by executionId", async () => {
  const path = join(tmpdir(), `telegram-bot-m3-${crypto.randomUUID()}.sqlite`);
  const executor = new FakeAgentExecutor();
  try {
    const first = await fixture(path, executor);
    const session = await createSession(first.orchestrator);
    await first.orchestrator.handleMessage(message(session.telegramThreadId, "queued"));
    const pending = (await first.persistence.executions.listBySession(session.id))[0]!;
    expect(pending.status).toBe("pending");
    expect(
      first.persistence.db
        .query("SELECT status FROM execution_jobs WHERE execution_id=?")
        .get(pending.id),
    ).toEqual({ status: "queued" });
    await first.orchestrator.shutdown();
    first.persistence.db.close();

    const second = await fixture(path, executor);
    await second.orchestrator.start();
    await waitUntil(
      async () =>
        (await second.persistence.executions.listBySession(session.id))[0]?.status === "completed",
    );
    expect(executor.starts).toHaveLength(1);
    await second.orchestrator.shutdown();
    second.persistence.db.close();
  } finally {
    unlinkSync(path);
  }
});

test("SQLite claim serializes one BotSession while different sessions run concurrently", async () => {
  const path = join(tmpdir(), `telegram-bot-m3-${crypto.randomUUID()}.sqlite`);
  const executor = new FakeAgentExecutor();
  const a1 = deferred();
  const b1 = deferred();
  const a2 = deferred();
  executor.queueStreamGateForPrompt("a1", a1.promise);
  executor.queueStreamGateForPrompt("b1", b1.promise);
  executor.queueStreamGateForPrompt("a2", a2.promise);
  let current: Awaited<ReturnType<typeof fixture>> | undefined;
  try {
    current = await fixture(path, executor, 2);
    await current.orchestrator.start();
    const first = await createSession(current.orchestrator);
    const second = await createSession(current.orchestrator);

    await current.orchestrator.handleMessage(message(first.telegramThreadId, "a1"));
    await waitUntil(
      async () =>
        (await current!.persistence.executions.listBySession(first.id)).find(
          (execution) => execution.prompt === "a1",
        )?.status === "running",
    );

    await current.orchestrator.handleMessage(message(second.telegramThreadId, "b1"));
    await waitUntil(
      async () =>
        (await current!.persistence.executions.listBySession(second.id)).find(
          (execution) => execution.prompt === "b1",
        )?.status === "running",
    );

    await current.orchestrator.handleMessage(message(first.telegramThreadId, "a2"));
    await waitUntil(async () => {
      const executions = await current!.persistence.executions.listBySession(first.id);
      return executions.find((execution) => execution.prompt === "a2")?.status === "pending";
    });
    expect(executor.sends.map((item) => item.input.prompt)).toEqual(
      expect.arrayContaining(["a1", "b1"]),
    );
    expect(executor.sends.map((item) => item.input.prompt)).not.toContain("a2");

    a1.resolve();
    await waitUntil(
      async () =>
        (await current!.persistence.executions.listBySession(first.id)).find(
          (execution) => execution.prompt === "a2",
        )?.status === "running",
    );
    expect(executor.sends.map((item) => item.input.prompt)).toContain("a2");

    a2.resolve();
    b1.resolve();
    await waitUntil(async () =>
      [
        ...(await current!.persistence.executions.listBySession(first.id)),
        ...(await current!.persistence.executions.listBySession(second.id)),
      ].every((execution) => execution.status === "completed"),
    );
  } finally {
    a1.resolve();
    a2.resolve();
    b1.resolve();
    if (current) {
      await current.orchestrator.shutdown();
      current.persistence.db.close();
    }
    unlinkSync(path);
  }
});

test("stale owner fences cannot mutate after deterministic recovery", async () => {
  const path = join(tmpdir(), `telegram-bot-m3-${crypto.randomUUID()}.sqlite`);
  try {
    const current = await fixture(path, new FakeAgentExecutor());
    const session = await createSession(current.orchestrator);
    const oldExecution: Execution = {
      id: crypto.randomUUID(),
      botSessionId: session.id,
      requestedByUserId: "operator",
      prompt: "old",
      status: "pending",
      correlationId: crypto.randomUUID(),
      ownerFence: 0,
    };
    await current.persistence.executions.save(oldExecution);
    const oldLease = await current.persistence.executions.claim(
      oldExecution.id,
      "old-worker",
      new Date(1_000),
      1_000,
    );
    expect(oldLease?.fence).toBe(1);
    const recovered = await current.persistence.executions.recoverAfterRestart(new Date(3_000));
    expect(recovered.find((execution) => execution.id === oldExecution.id)?.status).toBe("unknown");
    expect(
      await current.persistence.reconciliations.reconcile(oldExecution.id, session.id, {
        executionId: oldExecution.id,
        outcome: "abandoned",
        reconciledByUserId: "operator",
        note: "no reliable result",
        reconciledAt: new Date(3_001),
      }),
    ).toMatchObject({ status: "reconciled" });

    const newExecution: Execution = {
      ...oldExecution,
      id: crypto.randomUUID(),
      prompt: "new",
      status: "pending",
      correlationId: crypto.randomUUID(),
      ownerFence: 0,
    };
    await current.persistence.executions.save(newExecution);
    const newLease = await current.persistence.executions.claim(
      newExecution.id,
      "new-worker",
      new Date(4_000),
      1_000,
    );
    expect(newLease?.fence).toBe(1);
    expect(
      await current.persistence.executions.updateOwned(
        { ...oldExecution, status: "completed", ownerFence: oldLease!.fence },
        oldLease!.ownerId,
        oldLease!.fence,
        new Date(4_000),
      ),
    ).toBe(false);
    expect((await current.persistence.executions.getById(newExecution.id))?.ownerId).toBe(
      "new-worker",
    );
    await current.orchestrator.shutdown();
    current.persistence.db.close();
  } finally {
    unlinkSync(path);
  }
});

test("queued cancellation is authoritative and prevents provider start", async () => {
  const path = join(tmpdir(), `telegram-bot-m3-${crypto.randomUUID()}.sqlite`);
  const executor = new FakeAgentExecutor();
  try {
    const first = await fixture(path, executor);
    const session = await createSession(first.orchestrator);
    await first.orchestrator.handleMessage(message(session.telegramThreadId, "cancel me"));
    await first.orchestrator.handleMessage(message(session.telegramThreadId, "/stop"));
    expect((await first.persistence.executions.listBySession(session.id))[0]?.status).toBe(
      "stopped",
    );
    await first.orchestrator.shutdown();
    first.persistence.db.close();

    const second = await fixture(path, executor);
    await second.orchestrator.start();
    expect(
      second.persistence.db
        .query<{ status: string }, any>("SELECT status FROM execution_jobs")
        .get()?.status,
    ).toBe("cancelled");
    expect(executor.starts).toHaveLength(0);
    await second.orchestrator.shutdown();
    second.persistence.db.close();
  } finally {
    unlinkSync(path);
  }
});

test("duplicate and terminal job delivery never replays a provider", async () => {
  const path = join(tmpdir(), `telegram-bot-m3-${crypto.randomUUID()}.sqlite`);
  const executor = new FakeAgentExecutor();
  try {
    const current = await fixture(path, executor);
    await current.orchestrator.start();
    const session = await createSession(current.orchestrator);
    await current.orchestrator.handleMessage(message(session.telegramThreadId, "once"));
    await waitUntil(
      async () =>
        (await current.persistence.executions.listBySession(session.id))[0]?.status === "completed",
    );
    const completed = (await current.persistence.executions.listBySession(session.id))[0]!;
    await current.queue.enqueue({ executionId: completed.id });
    for (const status of ["failed", "stopped", "unknown"] as const) {
      const terminal: Execution = {
        ...completed,
        id: crypto.randomUUID(),
        correlationId: crypto.randomUUID(),
        status,
        finishedAt: new Date(),
      };
      await current.persistence.executions.save(terminal);
      await current.queue.enqueue({ executionId: terminal.id });
    }
    expect(executor.starts).toHaveLength(1);
    expect(
      current.persistence.db
        .query<{ status: string }, any>("SELECT status FROM execution_jobs WHERE execution_id=?")
        .get(completed.id)?.status,
    ).toBe("completed");
    await current.orchestrator.shutdown();
    current.persistence.db.close();
  } finally {
    unlinkSync(path);
  }
});

test("independent SQLite handles produce one authoritative claim", async () => {
  const path = join(tmpdir(), `telegram-bot-m3-${crypto.randomUUID()}.sqlite`);
  try {
    const first = await fixture(path, new FakeAgentExecutor());
    const session = await createSession(first.orchestrator);
    const execution: Execution = {
      id: crypto.randomUUID(),
      botSessionId: session.id,
      requestedByUserId: "operator",
      prompt: "compete",
      status: "pending",
      correlationId: crypto.randomUUID(),
      ownerFence: 0,
    };
    await first.persistence.executions.save(execution);
    const second = SqlitePersistence.open(path);
    try {
      const [left, right] = await Promise.all([
        first.persistence.executions.claim(execution.id, "worker-a", new Date(1_000), 1_000),
        second.executions.claim(execution.id, "worker-b", new Date(1_000), 1_000),
      ]);
      expect([left, right].filter(Boolean)).toHaveLength(1);
      expect((left ?? right)?.fence).toBe(1);
    } finally {
      second.db.close();
    }
    first.persistence.db.close();
  } finally {
    unlinkSync(path);
  }
});

test("expired ownership advances fencing and rejects stale mutation", async () => {
  const path = join(tmpdir(), `telegram-bot-m3-${crypto.randomUUID()}.sqlite`);
  try {
    const current = await fixture(path, new FakeAgentExecutor());
    const session = await createSession(current.orchestrator);
    const execution: Execution = {
      id: crypto.randomUUID(),
      botSessionId: session.id,
      requestedByUserId: "operator",
      prompt: "fenced",
      status: "pending",
      correlationId: crypto.randomUUID(),
      ownerFence: 0,
    };
    await current.persistence.executions.save(execution);
    const oldLease = await current.persistence.executions.claim(
      execution.id,
      "worker-a",
      new Date(1_000),
      100,
    );
    const newLease = await current.persistence.executions.claim(
      execution.id,
      "worker-b",
      new Date(1_101),
      100,
    );
    expect(oldLease?.fence).toBe(1);
    expect(newLease?.fence).toBe(2);
    expect(newLease?.recovered).toBe(true);
    expect(
      await current.persistence.executions.updateOwned(
        { ...execution, status: "completed", ownerFence: oldLease!.fence },
        oldLease!.ownerId,
        oldLease!.fence,
        new Date(1_101),
      ),
    ).toBe(false);
    expect(
      await current.persistence.executions.renew(
        execution.id,
        newLease!.ownerId,
        newLease!.fence,
        new Date(1_101),
        1_000,
      ),
    ).toBe(true);
    expect((await current.persistence.executions.getById(execution.id))?.ownerId).toBe("worker-b");
    await current.orchestrator.shutdown();
    current.persistence.db.close();
  } finally {
    unlinkSync(path);
  }
});

test("expired running execution is recovered through the durable queue without provider replay", async () => {
  const path = join(tmpdir(), `telegram-bot-m3-${crypto.randomUUID()}.sqlite`);
  const executor = new FakeAgentExecutor();
  let current: Awaited<ReturnType<typeof fixture>> | undefined;
  try {
    current = await fixture(path, executor, 1);
    await current.orchestrator.start();
    const session = await createSession(current.orchestrator);
    const execution: Execution = {
      id: crypto.randomUUID(),
      botSessionId: session.id,
      requestedByUserId: "operator",
      prompt: "expired",
      status: "running",
      correlationId: crypto.randomUUID(),
      startedAt: new Date(Date.now() - 1_000),
      ownerId: "expired-worker",
      ownerFence: 7,
      leaseExpiresAt: new Date(Date.now() - 1),
    };
    insertExecutionWithJob(current.persistence, execution);

    await waitUntil(
      async () =>
        (await current!.persistence.executions.getById(execution.id))?.status === "unknown",
    );
    expect(executor.starts).toHaveLength(0);
    expect(executor.resumes).toHaveLength(0);
    expect(
      current.persistence.db
        .query<{ status: string }, any>("SELECT status FROM execution_jobs WHERE execution_id=?")
        .get(execution.id)?.status,
    ).toBe("completed");
  } finally {
    if (current) {
      await current.orchestrator.shutdown();
      current.persistence.db.close();
    }
    unlinkSync(path);
  }
});

test("expired same-session execution blocks pending delivery until reconciliation", async () => {
  const path = join(tmpdir(), `telegram-bot-m3-${crypto.randomUUID()}.sqlite`);
  const executor = new FakeAgentExecutor();
  let current: Awaited<ReturnType<typeof fixture>> | undefined;
  try {
    current = await fixture(path, executor, 1);
    await current.orchestrator.start();
    const session = await createSession(current.orchestrator);
    const now = Date.now();
    const expired: Execution = {
      id: crypto.randomUUID(),
      botSessionId: session.id,
      requestedByUserId: "operator",
      prompt: "a1-expired",
      status: "running",
      correlationId: crypto.randomUUID(),
      startedAt: new Date(now - 1_000),
      ownerId: "expired-worker",
      ownerFence: 3,
      leaseExpiresAt: new Date(now - 1),
    };
    const pending: Execution = {
      id: crypto.randomUUID(),
      botSessionId: session.id,
      requestedByUserId: "operator",
      prompt: "a2-pending",
      status: "pending",
      correlationId: crypto.randomUUID(),
      ownerFence: 0,
    };
    insertExecutionWithJob(current.persistence, expired, now + 1_000);
    insertExecutionWithJob(current.persistence, pending, now);

    await waitUntil(
      () =>
        (current!.persistence.db
          .query<{ attempts: number }, any>(
            "SELECT attempts FROM execution_jobs WHERE execution_id=?",
          )
          .get(pending.id)?.attempts ?? 0) > 0,
    );
    expect((await current.persistence.executions.getById(expired.id))?.status).toBe("running");
    expect((await current.persistence.executions.getById(pending.id))?.status).toBe("pending");
    expect(executor.sends).toHaveLength(0);

    current.persistence.db
      .query("UPDATE execution_jobs SET available_at=? WHERE execution_id=?")
      .run(Date.now(), expired.id);
    await waitUntil(
      async () => (await current!.persistence.executions.getById(expired.id))?.status === "unknown",
    );
    expect((await current.persistence.executions.getById(pending.id))?.status).toBe("pending");
    expect(executor.sends).toHaveLength(0);
    expect(
      await current.persistence.reconciliations.reconcile(expired.id, session.id, {
        executionId: expired.id,
        outcome: "confirmed_completed",
        reconciledByUserId: "operator",
        note: "verified externally",
        reconciledAt: new Date(),
      }),
    ).toMatchObject({ status: "reconciled" });
    await waitUntil(
      async () =>
        (await current!.persistence.executions.getById(pending.id))?.status === "completed",
    );
    expect(executor.sends.map((item) => item.input.prompt)).toEqual(["a2-pending"]);
  } finally {
    if (current) {
      await current.orchestrator.shutdown();
      current.persistence.db.close();
    }
    unlinkSync(path);
  }
});

test("stale BotSession execution-state writes are rejected by the current fence", async () => {
  const path = join(tmpdir(), `telegram-bot-m3-${crypto.randomUUID()}.sqlite`);
  try {
    const first = await fixture(path, new FakeAgentExecutor());
    const session = await createSession(first.orchestrator);
    const execution: Execution = {
      id: crypto.randomUUID(),
      botSessionId: session.id,
      requestedByUserId: "operator",
      prompt: "session-fence",
      status: "pending",
      correlationId: crypto.randomUUID(),
      ownerFence: 0,
    };
    await first.persistence.executions.save(execution);
    const oldLease = await first.persistence.executions.claim(
      execution.id,
      "worker-a",
      new Date(1_000),
      100,
    );
    const second = SqlitePersistence.open(path);
    try {
      const newLease = await second.executions.claim(
        execution.id,
        "worker-b",
        new Date(1_101),
        100,
      );
      expect(newLease?.fence).toBe(oldLease!.fence + 1);
      const current = (await second.sessions.getById(session.id))!;
      expect(
        await second.sessions.updateOwned(
          { ...current, status: "running" },
          execution.id,
          newLease!.ownerId,
          newLease!.fence,
          new Date(1_101),
        ),
      ).toBe(true);
      expect(
        await first.persistence.sessions.updateOwned(
          { ...current, status: "failed" },
          execution.id,
          oldLease!.ownerId,
          oldLease!.fence,
          new Date(1_101),
        ),
      ).toBe(false);
      expect((await second.sessions.getById(session.id))?.status).toBe("running");
    } finally {
      second.db.close();
    }
    first.persistence.db.close();
  } finally {
    unlinkSync(path);
  }
});

test("stale native identity writes are rejected by the current fence", async () => {
  const path = join(tmpdir(), `telegram-bot-m3-${crypto.randomUUID()}.sqlite`);
  try {
    const first = await fixture(path, new FakeAgentExecutor());
    const session = await createSession(first.orchestrator);
    const executorSession: ExecutorSession = {
      id: crypto.randomUUID(),
      executorId: "fake",
      projectId: "project",
      workspacePath: "/workspace/project",
      nativeSessionId: "native-a",
      resumable: true,
      createdAt: new Date(1_000),
      lastUsedAt: new Date(1_000),
    };
    await first.persistence.executorSessions.save(executorSession);
    const execution: Execution = {
      id: crypto.randomUUID(),
      botSessionId: session.id,
      requestedByUserId: "operator",
      prompt: "identity-fence",
      status: "pending",
      correlationId: crypto.randomUUID(),
      executorSessionId: executorSession.id,
      ownerFence: 0,
    };
    await first.persistence.executions.save(execution);
    const oldLease = await first.persistence.executions.claim(
      execution.id,
      "worker-a",
      new Date(1_000),
      100,
    );
    const second = SqlitePersistence.open(path);
    try {
      const newLease = await second.executions.claim(
        execution.id,
        "worker-b",
        new Date(1_101),
        100,
      );
      expect(newLease?.fence).toBe(oldLease!.fence + 1);
      expect(
        await second.executorSessions.updateOwned(
          { ...executorSession, nativeSessionId: "native-b" },
          execution.id,
          newLease!.ownerId,
          newLease!.fence,
          new Date(1_101),
        ),
      ).toBe(true);
      expect(
        await first.persistence.executorSessions.updateOwned(
          { ...executorSession, nativeSessionId: "native-stale" },
          execution.id,
          oldLease!.ownerId,
          oldLease!.fence,
          new Date(1_101),
        ),
      ).toBe(false);
      expect((await second.executorSessions.getById(executorSession.id))?.nativeSessionId).toBe(
        "native-b",
      );
    } finally {
      second.db.close();
    }
    first.persistence.db.close();
  } finally {
    unlinkSync(path);
  }
});

test("running cancellation interrupts the provider and persists stopped", async () => {
  const path = join(tmpdir(), `telegram-bot-m3-${crypto.randomUUID()}.sqlite`);
  const executor = new FakeAgentExecutor();
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  executor.queueStreamGate(gate);
  try {
    const current = await fixture(path, executor);
    await current.orchestrator.start();
    const session = await createSession(current.orchestrator);
    await current.orchestrator.handleMessage(message(session.telegramThreadId, "interrupt me"));
    await waitUntil(() => executor.sends.length === 1);
    await current.orchestrator.handleMessage(message(session.telegramThreadId, "/stop"));
    await waitUntil(
      async () =>
        (await current.persistence.executions.listBySession(session.id))[0]?.status === "stopped",
    );
    expect(executor.interrupts).toHaveLength(1);
    release();
    await current.orchestrator.shutdown();
    current.persistence.db.close();
  } finally {
    unlinkSync(path);
  }
});

test("starting cancellation aborts before a provider session is established", async () => {
  const path = join(tmpdir(), `telegram-bot-m3-${crypto.randomUUID()}.sqlite`);
  let release!: () => void;
  const startGate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const executor = new FakeAgentExecutor({ startGate });
  try {
    const current = await fixture(path, executor);
    await current.orchestrator.start();
    const session = await createSession(current.orchestrator);
    await current.orchestrator.handleMessage(message(session.telegramThreadId, "starting"));
    await waitUntil(() => executor.starts.length === 1);
    await current.orchestrator.handleMessage(message(session.telegramThreadId, "/stop"));
    await waitUntil(
      async () =>
        (await current.persistence.executions.listBySession(session.id))[0]?.status === "stopped",
    );
    expect(executor.sends).toHaveLength(0);
    release();
    await current.orchestrator.shutdown();
    current.persistence.db.close();
  } finally {
    unlinkSync(path);
  }
});

test("startup reconciles a claimed execution fail-closed without provider restart", async () => {
  const path = join(tmpdir(), `telegram-bot-m3-${crypto.randomUUID()}.sqlite`);
  const firstExecutor = new FakeAgentExecutor();
  try {
    const first = await fixture(path, firstExecutor);
    const session = await createSession(first.orchestrator);
    const execution: Execution = {
      id: crypto.randomUUID(),
      botSessionId: session.id,
      requestedByUserId: "operator",
      prompt: "crash boundary",
      status: "pending",
      correlationId: crypto.randomUUID(),
      ownerFence: 0,
    };
    await first.persistence.executions.save(execution);
    await first.persistence.executions.claim(
      execution.id,
      "crashed-worker",
      new Date(1_000),
      1_000,
    );
    first.persistence.db.close();

    const second = await fixture(path, new FakeAgentExecutor());
    await second.orchestrator.start();
    expect((await second.persistence.executions.getById(execution.id))?.status).toBe("unknown");
    expect(
      second.persistence.db.query("SELECT * FROM execution_jobs WHERE status='queued'").all(),
    ).toEqual([]);
    await second.orchestrator.shutdown();
    second.persistence.db.close();
  } finally {
    unlinkSync(path);
  }
});

function reconciliationUnknownExecution(botSessionId: string): Execution {
  return {
    id: crypto.randomUUID(),
    botSessionId,
    requestedByUserId: "operator",
    prompt: "ambiguous",
    status: "unknown",
    correlationId: crypto.randomUUID(),
    finishedAt: new Date(1_000),
    errorMessage: "Executor stream ended without a terminal event.",
    ownerFence: 1,
  };
}

function reconciliationPendingExecution(botSessionId: string): Execution {
  return {
    id: crypto.randomUUID(),
    botSessionId,
    requestedByUserId: "operator",
    prompt: "a2",
    status: "pending",
    correlationId: crypto.randomUUID(),
    ownerFence: 0,
  };
}

test("SQLite claim quarantine survives independent handles and reconciliation races", async () => {
  const path = join(tmpdir(), `telegram-bot-m4-${crypto.randomUUID()}.sqlite`);
  try {
    const first = await fixture(path, new FakeAgentExecutor());
    const session = await createSession(first.orchestrator);
    const unknown = reconciliationUnknownExecution(session.id);
    const pending = reconciliationPendingExecution(session.id);
    insertExecutionWithJob(first.persistence, unknown);
    insertExecutionWithJob(first.persistence, pending);
    const second = SqlitePersistence.open(path);

    expect(
      await second.executions.claim(pending.id, "worker", new Date(2_000), 1_000),
    ).toBeUndefined();
    const [left, right] = await Promise.all([
      first.persistence.reconciliations.reconcile(unknown.id, session.id, {
        executionId: unknown.id,
        outcome: "confirmed_completed",
        reconciledByUserId: "operator-a",
        note: "verified externally",
        reconciledAt: new Date(2_001),
      }),
      second.reconciliations.reconcile(unknown.id, session.id, {
        executionId: unknown.id,
        outcome: "abandoned",
        reconciledByUserId: "operator-b",
        note: "no reliable result",
        reconciledAt: new Date(2_002),
      }),
    ]);
    expect([left, right].filter((result) => result.status === "reconciled")).toHaveLength(1);
    expect([left, right].filter((result) => result.status === "already_reconciled")).toHaveLength(
      1,
    );
    expect(
      await second.executions.claim(pending.id, "worker", new Date(2_003), 1_000),
    ).toMatchObject({
      executionId: pending.id,
    });
    second.db.close();
    first.persistence.db.close();
  } finally {
    unlinkSync(path);
  }
});

test("restart preserves unresolved quarantine and resolved reconciliation", async () => {
  const path = join(tmpdir(), `telegram-bot-m4-restart-${crypto.randomUUID()}.sqlite`);
  const executor = new FakeAgentExecutor();
  let first: Awaited<ReturnType<typeof fixture>> | undefined;
  try {
    first = await fixture(path, executor);
    const session = await createSession(first.orchestrator);
    const unknown = reconciliationUnknownExecution(session.id);
    const pending = reconciliationPendingExecution(session.id);
    insertExecutionWithJob(first.persistence, unknown);
    insertExecutionWithJob(first.persistence, pending);
    await first.persistence.sessions.save({ ...session, status: "failed" });
    first.persistence.db.close();
    first = undefined;

    const second = await fixture(path, executor);
    await second.orchestrator.start();
    expect((await second.persistence.executions.getById(unknown.id))?.status).toBe("unknown");
    expect((await second.persistence.executions.getById(pending.id))?.status).toBe("pending");
    expect(executor.starts).toHaveLength(0);
    await second.persistence.reconciliations.reconcile(unknown.id, session.id, {
      executionId: unknown.id,
      outcome: "abandoned",
      reconciledByUserId: "operator",
      note: "no reliable result",
      reconciledAt: new Date(),
    });
    await waitUntil(
      async () => (await second.persistence.executions.getById(pending.id))?.status === "completed",
    );
    expect(executor.sends.map((item) => item.input.prompt)).toEqual(["a2"]);
    await second.orchestrator.shutdown();
    second.persistence.db.close();

    const third = SqlitePersistence.open(path);
    expect((await third.executions.getById(unknown.id))?.status).toBe("unknown");
    expect((await third.sessions.getById(session.id))?.status).toBe("idle");
    expect(await third.reconciliations.listBySession(session.id)).toMatchObject([
      { executionId: unknown.id, outcome: "abandoned" },
    ]);
    third.db.close();
  } finally {
    if (first) first.persistence.db.close();
    unlinkSync(path);
  }
});
