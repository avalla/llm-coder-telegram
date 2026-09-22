import { describe, expect, it } from "vitest";
import { EventEmitter } from "node:events";
import { PassThrough, Readable } from "node:stream";
import type { AgentEvent, AgentSession } from "../src/domain.js";
import { ClaudeCodeExecutor, CodexCliExecutor } from "../src/provider-adapters.js";
import {
  NodeProcessRunner,
  ProcessStartError,
  type ProcessExit,
  type ProcessHandle,
  type ProcessRunner,
  type ProcessSpec,
} from "../src/process.js";

const codexId = "11111111-1111-4111-8111-111111111111";
const claudeId = "22222222-2222-4222-8222-222222222222";

type Script = { lines: readonly string[]; exit?: ProcessExit };

class ScriptedRunner implements ProcessRunner {
  readonly specs: ProcessSpec[] = [];
  readonly handles: ScriptedHandle[] = [];
  private readonly scripts: Script[] = [];

  enqueue(script: Script): void {
    this.scripts.push(script);
  }

  async spawn(spec: ProcessSpec): Promise<ProcessHandle> {
    this.specs.push(spec);
    const script = this.scripts.shift();
    if (!script) throw new Error("No scripted process");
    const handle = new ScriptedHandle(script);
    this.handles.push(handle);
    return handle;
  }
}

class ScriptedHandle implements ProcessHandle {
  readonly writes: string[] = [];
  readonly signals: string[] = [];
  readonly stdout: AsyncIterable<Uint8Array>;
  readonly stderr: AsyncIterable<Uint8Array>;

  constructor(private readonly script: Script) {
    this.stdout = chunks(script.lines);
    this.stderr = chunks([]);
  }

  async writeStdin(chunk: string | Uint8Array): Promise<void> {
    this.writes.push(typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk));
  }

  async closeStdin(): Promise<void> {}

  async signal(signal: "SIGINT" | "SIGTERM" | "SIGKILL"): Promise<void> {
    this.signals.push(signal);
  }

  async wait(): Promise<ProcessExit> {
    return this.script.exit ?? { code: 0 };
  }
}

async function* chunks(lines: readonly string[]): AsyncIterable<Uint8Array> {
  const encoder = new TextEncoder();
  for (const line of lines) yield encoder.encode(`${line}\n`);
}

class BlockingRunner implements ProcessRunner {
  readonly handle = new BlockingHandle(codexId);
  readonly specs: ProcessSpec[] = [];

  async spawn(spec: ProcessSpec): Promise<ProcessHandle> {
    this.specs.push(spec);
    return this.handle;
  }
}

class BlockingHandle implements ProcessHandle {
  readonly signals: string[] = [];
  readonly stderr = chunks([]);
  private release!: () => void;
  private readonly released = new Promise<void>((resolve) => {
    this.release = resolve;
  });
  readonly stdout = this.output();

  constructor(private readonly nativeSessionId: string) {}

  async *output(): AsyncIterable<Uint8Array> {
    yield new TextEncoder().encode(
      `${JSON.stringify({ type: "thread.started", thread_id: this.nativeSessionId })}\n`,
    );
    await this.released;
  }

  async writeStdin(): Promise<void> {}
  async closeStdin(): Promise<void> {}
  async signal(signal: "SIGINT" | "SIGTERM" | "SIGKILL"): Promise<void> {
    this.signals.push(signal);
    this.release();
  }
  async wait(): Promise<ProcessExit> {
    return { code: null, signal: "SIGINT" };
  }
}

class EscalatingHandle implements ProcessHandle {
  readonly signals: string[] = [];
  readonly stderr = chunks([]);
  private releaseOutput!: () => void;
  private releaseExit!: (exit: ProcessExit) => void;
  private readonly outputReleased = new Promise<void>((resolve) => {
    this.releaseOutput = resolve;
  });
  private readonly exit = new Promise<ProcessExit>((resolve) => {
    this.releaseExit = resolve;
  });
  readonly stdout = this.output();

  async *output(): AsyncIterable<Uint8Array> {
    yield new TextEncoder().encode(
      `${JSON.stringify({ type: "thread.started", thread_id: codexId })}\n`,
    );
    await this.outputReleased;
  }

  async writeStdin(): Promise<void> {}
  async closeStdin(): Promise<void> {}
  async signal(signal: "SIGINT" | "SIGTERM" | "SIGKILL"): Promise<void> {
    this.signals.push(signal);
    if (signal === "SIGTERM") this.releaseOutput();
    if (signal === "SIGKILL") this.releaseExit({ code: null, signal: "SIGKILL" });
  }
  async wait(): Promise<ProcessExit> {
    return this.exit;
  }
}

class EscalatingRunner implements ProcessRunner {
  readonly handle = new EscalatingHandle();
  async spawn(): Promise<ProcessHandle> {
    return this.handle;
  }
}

class PendingWriteHandle implements ProcessHandle {
  readonly signals: string[] = [];
  readonly stdout = chunks([]);
  readonly stderr = chunks([]);
  private releaseExit!: (exit: ProcessExit) => void;
  private readonly exit = new Promise<ProcessExit>((resolve) => {
    this.releaseExit = resolve;
  });
  private markWriteStarted!: () => void;
  readonly writeStarted = new Promise<void>((resolve) => {
    this.markWriteStarted = resolve;
  });

  async writeStdin(_chunk: string | Uint8Array, signal?: AbortSignal): Promise<void> {
    this.markWriteStarted();
    await new Promise<void>((_, reject) => {
      if (signal?.aborted) reject(new Error("aborted"));
      signal?.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
    });
  }
  async closeStdin(): Promise<void> {}
  async signal(signal: "SIGINT" | "SIGTERM" | "SIGKILL"): Promise<void> {
    this.signals.push(signal);
    if (signal === "SIGTERM") this.releaseExit({ code: null, signal: "SIGTERM" });
    if (signal === "SIGKILL") this.releaseExit({ code: null, signal: "SIGKILL" });
  }
  async wait(): Promise<ProcessExit> {
    return this.exit;
  }
}

class PendingWriteRunner implements ProcessRunner {
  readonly handle = new PendingWriteHandle();
  constructor(private readonly beforeWriteAbort?: AbortController) {}
  async spawn(): Promise<ProcessHandle> {
    const abort = this.beforeWriteAbort;
    if (abort) queueMicrotask(() => abort.abort());
    return this.handle;
  }
}

async function collect(executor: CodexCliExecutor | ClaudeCodeExecutor, session: AgentSession) {
  const events: AgentEvent[] = [];
  for await (const event of executor.send(session, { prompt: "prompt from Telegram" })) {
    events.push(event);
  }
  return events;
}

describe("real provider CLI adapters", () => {
  it("invokes Codex fresh and resumes the exact native session", async () => {
    const runner = new ScriptedRunner();
    runner.enqueue({
      lines: [
        JSON.stringify({ type: "thread.started", thread_id: codexId }),
        JSON.stringify({ type: "item.completed", item: { type: "agent_message", text: "first" } }),
        JSON.stringify({ type: "turn.completed" }),
      ],
    });
    runner.enqueue({
      lines: [
        JSON.stringify({ type: "thread.started", thread_id: codexId }),
        JSON.stringify({ type: "turn.completed" }),
      ],
    });
    const executor = new CodexCliExecutor({ runner });
    const first = await executor.start({ projectId: "project", workspacePath: "/workspace" });
    const firstEvents = await collect(executor, first);
    const resumed = await executor.resume({
      projectId: "project",
      workspacePath: "/workspace",
      nativeSessionId: codexId,
    });
    const secondEvents = await collect(executor, resumed);

    expect(firstEvents.map((event) => event.type)).toEqual([
      "session_identity",
      "text_delta",
      "completed",
    ]);
    expect(secondEvents.at(-1)?.type).toBe("completed");
    expect(runner.specs[0]?.args).toEqual([
      "exec",
      "--json",
      "--color",
      "never",
      "--skip-git-repo-check",
      "--cd",
      "/workspace",
      "-",
    ]);
    expect(runner.specs[1]?.args).toEqual([
      "exec",
      "resume",
      codexId,
      "--json",
      "--color",
      "never",
      "--skip-git-repo-check",
      "--cd",
      "/workspace",
      "-",
    ]);
    expect(runner.handles[0]?.writes).toEqual(["prompt from Telegram"]);
  });

  it("invokes Claude fresh and resumes the exact native session", async () => {
    const runner = new ScriptedRunner();
    runner.enqueue({
      lines: [
        JSON.stringify({ type: "system", subtype: "init", session_id: claudeId }),
        JSON.stringify({
          type: "assistant",
          message: { content: [{ type: "text", text: "hello" }] },
        }),
        JSON.stringify({
          type: "result",
          subtype: "success",
          result: "done",
          session_id: claudeId,
        }),
      ],
    });
    runner.enqueue({
      lines: [
        JSON.stringify({ type: "system", subtype: "init", session_id: claudeId }),
        JSON.stringify({ type: "result", subtype: "success", session_id: claudeId }),
      ],
    });
    const executor = new ClaudeCodeExecutor({ runner });
    const first = await executor.start({ projectId: "project", workspacePath: "/workspace" });
    const firstEvents = await collect(executor, first);
    const resumed = await executor.resume({
      projectId: "project",
      workspacePath: "/workspace",
      nativeSessionId: claudeId,
    });
    await collect(executor, resumed);

    expect(firstEvents.map((event) => event.type)).toEqual([
      "session_identity",
      "text_delta",
      "completed",
    ]);
    expect(runner.specs[0]?.args).toEqual([
      "--print",
      "--output-format",
      "stream-json",
      "--input-format",
      "stream-json",
      "--verbose",
      "--permission-prompts",
      "none",
    ]);
    expect(runner.specs[1]?.args).toEqual([
      "--print",
      "--output-format",
      "stream-json",
      "--input-format",
      "stream-json",
      "--verbose",
      "--permission-prompts",
      "none",
      "--resume",
      claudeId,
    ]);
    expect(JSON.parse(runner.handles[0]?.writes[0] ?? "{}")).toMatchObject({
      type: "user",
      message: { content: [{ text: "prompt from Telegram" }] },
    });
  });

  it("normalizes conflicting native identity without mutating the requested session", async () => {
    const runner = new ScriptedRunner();
    runner.enqueue({
      lines: [JSON.stringify({ type: "system", subtype: "init", session_id: claudeId })],
    });
    const executor = new ClaudeCodeExecutor({ runner });
    const session = await executor.resume({
      projectId: "project",
      workspacePath: "/workspace",
      nativeSessionId: codexId,
    });
    const events = await collect(executor, session);

    expect(events[0]).toMatchObject({ type: "session_identity", nativeSessionId: claudeId });
    expect(runner.specs[0]?.args).toContain(codexId);
    expect(session.nativeSessionId).toBe(codexId);
  });

  it("passes only the provider environment allowlist to Codex and Claude", async () => {
    const cases = [
      {
        create: (runner: ScriptedRunner) =>
          new CodexCliExecutor({
            runner,
            environmentSource: {
              PATH: "/safe/bin",
              HOME: "/safe/home",
              OPENAI_API_KEY: "provider-key",
              TELEGRAM_BOT_TOKEN: "super-secret",
              BOT_AUTHORIZED_CHAT_IDS: "private",
            } as NodeJS.ProcessEnv,
          }),
        authKey: "OPENAI_API_KEY",
        line: { type: "thread.started", thread_id: codexId },
      },
      {
        create: (runner: ScriptedRunner) =>
          new ClaudeCodeExecutor({
            runner,
            environmentSource: {
              PATH: "/safe/bin",
              HOME: "/safe/home",
              ANTHROPIC_API_KEY: "provider-key",
              TELEGRAM_BOT_TOKEN: "super-secret",
              DATABASE_URL: "private",
            } as NodeJS.ProcessEnv,
          }),
        authKey: "ANTHROPIC_API_KEY",
        line: { type: "system", subtype: "init", session_id: claudeId },
      },
    ] as const;
    for (const testCase of cases) {
      const runner = new ScriptedRunner();
      runner.enqueue({
        lines: [JSON.stringify(testCase.line), JSON.stringify({ type: "turn.completed" })],
      });
      const executor = testCase.create(runner);
      const session = await executor.start({ projectId: "project", workspacePath: "/workspace" });
      await collect(executor, session);
      const env = runner.specs[0]?.env ?? {};
      expect(env[testCase.authKey]).toBe("provider-key");
      expect(env.PATH).toBe("/safe/bin");
      expect(env.HOME).toBe("/safe/home");
      expect(env.TELEGRAM_BOT_TOKEN).toBeUndefined();
      expect(env.BOT_AUTHORIZED_CHAT_IDS).toBeUndefined();
      expect(env.DATABASE_URL).toBeUndefined();
    }
  });

  it("fails malformed output and unavailable providers with normalized errors", async () => {
    const malformedRunner = new ScriptedRunner();
    malformedRunner.enqueue({ lines: ["not-json"] });
    const malformed = new CodexCliExecutor({ runner: malformedRunner });
    const malformedSession = await malformed.start({
      projectId: "project",
      workspacePath: "/workspace",
    });
    await expect(collect(malformed, malformedSession)).resolves.toEqual([
      expect.objectContaining({
        type: "error",
        message: "codex emitted malformed structured output.",
      }),
    ]);

    const missingRunner: ProcessRunner = {
      async spawn(): Promise<ProcessHandle> {
        throw new ProcessStartError("not_found");
      },
    };
    const missing = new ClaudeCodeExecutor({ runner: missingRunner });
    const missingSession = await missing.start({
      projectId: "project",
      workspacePath: "/workspace",
    });
    await expect(collect(missing, missingSession)).resolves.toEqual([
      expect.objectContaining({
        type: "error",
        message: "Anthropic Claude Code is unavailable. Install it and ensure it is on PATH.",
      }),
    ]);
  });

  it("does not spawn when cancellation is already established", async () => {
    const runner = new ScriptedRunner();
    const executor = new CodexCliExecutor({ runner });
    const session = await executor.start({ projectId: "project", workspacePath: "/workspace" });
    const abort = new AbortController();
    abort.abort();
    const events: AgentEvent[] = [];
    for await (const event of executor.send(session, { prompt: "ignored", signal: abort.signal })) {
      events.push(event);
    }
    expect(events).toEqual([]);
    expect(runner.specs).toHaveLength(0);
  });

  it("interrupts a running provider stream and never emits completion after abort", async () => {
    const runner = new BlockingRunner();
    const executor = new CodexCliExecutor({ runner });
    const session = await executor.start({ projectId: "project", workspacePath: "/workspace" });
    const abort = new AbortController();
    const iterator = executor
      .send(session, { prompt: "running", signal: abort.signal })
      [Symbol.asyncIterator]();

    const identityEvent = await iterator.next();
    expect(identityEvent.value?.type).toBe("session_identity");
    abort.abort();
    await executor.interrupt(session);
    const terminal = await iterator.next();

    expect(terminal.done).toBe(true);
    expect(runner.handle.signals).toEqual(["SIGTERM", "SIGINT"]);
  });

  it("terminates and reaps a provider when the stream consumer throws", async () => {
    const runner = new EscalatingRunner();
    const executor = new CodexCliExecutor({ runner });
    const session = await executor.start({ projectId: "project", workspacePath: "/workspace" });

    await expect(
      (async () => {
        for await (const _event of executor.send(session, { prompt: "renderer fails" })) {
          throw new Error("renderer failed");
        }
      })(),
    ).rejects.toThrow("renderer failed");
    expect(runner.handle.signals).toEqual(["SIGTERM", "SIGKILL"]);
    await expect(runner.handle.wait()).resolves.toEqual({ code: null, signal: "SIGKILL" });
  });

  it("handles abort after spawn and during stdin write", async () => {
    const beforeWriteAbort = new AbortController();
    const beforeWriteRunner = new PendingWriteRunner(beforeWriteAbort);
    const beforeWriteExecutor = new CodexCliExecutor({ runner: beforeWriteRunner });
    const beforeWriteSession = await beforeWriteExecutor.start({
      projectId: "project",
      workspacePath: "/workspace",
    });
    const beforeWriteEvents: AgentEvent[] = [];
    for await (const event of beforeWriteExecutor.send(beforeWriteSession, {
      prompt: "cancel before stdin",
      signal: beforeWriteAbort.signal,
    }))
      beforeWriteEvents.push(event);
    expect(beforeWriteEvents).toEqual([]);

    const duringWriteAbort = new AbortController();
    const duringWriteRunner = new PendingWriteRunner();
    const duringWriteExecutor = new CodexCliExecutor({ runner: duringWriteRunner });
    const duringWriteSession = await duringWriteExecutor.start({
      projectId: "project",
      workspacePath: "/workspace",
    });
    const pending = duringWriteExecutor
      .send(duringWriteSession, {
        prompt: "cancel during stdin",
        signal: duringWriteAbort.signal,
      })
      [Symbol.asyncIterator]();
    const next = pending.next();
    await duringWriteRunner.handle.writeStarted;
    duringWriteAbort.abort();
    await expect(next).resolves.toMatchObject({ done: true });
    expect(duringWriteRunner.handle.signals).toContain("SIGTERM");
  });

  it("does not emit events after abort immediately follows a terminal event", async () => {
    const runner = new ScriptedRunner();
    runner.enqueue({
      lines: [
        JSON.stringify({ type: "thread.started", thread_id: codexId }),
        JSON.stringify({ type: "turn.completed" }),
      ],
    });
    const executor = new CodexCliExecutor({ runner });
    const session = await executor.start({ projectId: "project", workspacePath: "/workspace" });
    const abort = new AbortController();
    const iterator = executor
      .send(session, { prompt: "terminal", signal: abort.signal })
      [Symbol.asyncIterator]();
    expect((await iterator.next()).value?.type).toBe("session_identity");
    expect((await iterator.next()).value?.type).toBe("completed");
    abort.abort();
    await expect(iterator.next()).resolves.toMatchObject({ done: true });
  });
});

describe("process execution boundary", () => {
  it("owns a POSIX process group and signals only that group", async () => {
    class FakeChild extends EventEmitter {
      readonly pid = 4321;
      readonly stdout = Readable.from([]);
      readonly stderr = Readable.from([]);
      readonly stdin = new PassThrough();
      kill(): boolean {
        return true;
      }
    }
    const child = new FakeChild();
    const spawned: { detached?: boolean; shell?: boolean; env?: NodeJS.ProcessEnv } = {};
    const killed: Array<{ pid: number; signal: NodeJS.Signals }> = [];
    const runner = new NodeProcessRunner({
      platform: "linux",
      spawnImpl: ((
        _executable: string,
        _args: readonly string[],
        options: { detached?: boolean; shell?: boolean; env?: NodeJS.ProcessEnv },
      ) => {
        Object.assign(spawned, options);
        queueMicrotask(() => child.emit("spawn"));
        return child;
      }) as never,
      killImpl: (pid, signal) => killed.push({ pid, signal }),
    });
    const handle = await runner.spawn({ executable: "provider", args: [], cwd: "/workspace" });
    expect(spawned).toMatchObject({ detached: true, shell: false, env: {} });
    await handle.signal("SIGTERM");
    expect(killed).toEqual([{ pid: -4321, signal: "SIGTERM" }]);
    child.emit("close", null, "SIGTERM");
    await expect(handle.wait()).resolves.toEqual({ code: null, signal: "SIGTERM" });
  });

  it("passes hostile text as an argument without shell interpolation", async () => {
    const runner = new NodeProcessRunner();
    const hostile = "$(touch /tmp/should-not-exist)";
    const handle = await runner.spawn({
      executable: process.execPath,
      args: ["-e", "process.stdout.write(process.argv[1] ?? '')", hostile],
      cwd: process.cwd(),
    });
    await handle.closeStdin();
    let output = "";
    for await (const chunk of handle.stdout) output += new TextDecoder().decode(chunk);
    const exit = await handle.wait();
    expect(output).toBe(hostile);
    expect(exit).toEqual({ code: 0 });
  });
});
