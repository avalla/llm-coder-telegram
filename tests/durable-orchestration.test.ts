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
import type { Execution } from "../src/domain.js";
import { SqliteExecutionJobQueue, SqlitePersistence } from "../src/sqlite.js";

async function waitUntil(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("Condition was not reached");
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
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  executor.queueStreamGate(gate);
  try {
    const current = await fixture(path, executor, 2);
    await current.orchestrator.start();
    const first = await createSession(current.orchestrator);
    const second = await createSession(current.orchestrator);

    await Promise.all([
      current.orchestrator.handleMessage(message(first.telegramThreadId, "a1")),
      current.orchestrator.handleMessage(message(first.telegramThreadId, "a2")),
      current.orchestrator.handleMessage(message(second.telegramThreadId, "b1")),
    ]);
    await waitUntil(() => executor.sends.length === 2);
    expect(executor.sends.map((item) => item.input.prompt)).toContain("b1");
    const firstSessionPrompt = executor.sends.find((item) => item.input.prompt.startsWith("a"))
      ?.input.prompt;
    expect(firstSessionPrompt).toBeDefined();
    expect(["a1", "a2"]).toContain(firstSessionPrompt!);

    release();
    await waitUntil(async () =>
      (await current.persistence.executions.listBySession(first.id)).every(
        (execution) => execution.status === "completed",
      ),
    );
    await waitUntil(
      async () =>
        (await current.persistence.executions.listBySession(second.id))[0]?.status === "completed",
    );
    await current.orchestrator.shutdown();
    current.persistence.db.close();
  } finally {
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
