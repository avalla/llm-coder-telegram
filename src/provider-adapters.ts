import type {
  AgentEvent,
  AgentExecutor,
  AgentInput,
  AgentSession,
  ExecutorCapabilities,
  ResumeSessionOptions,
  StartSessionOptions,
} from "./domain.js";
import {
  ProcessStartError,
  type ProcessHandle,
  type ProcessRunner,
  NodeProcessRunner,
} from "./process.js";

export type ProviderErrorKind = "unavailable" | "protocol" | "resume_rejected" | "process_failed";

export class ProviderError extends Error {
  constructor(
    public readonly provider: string,
    public readonly kind: ProviderErrorKind,
    message: string,
  ) {
    super(message);
    this.name = "ProviderError";
  }
}

export interface ProviderProbe {
  available: boolean;
  version?: string;
  capabilities: ExecutorCapabilities;
}

type PendingCompletion = Extract<AgentEvent, { type: "completed" }>;

abstract class ProcessExecutor implements AgentExecutor {
  abstract readonly id: string;
  abstract readonly name: string;
  private counter = 0;
  private readonly processes = new Map<string, ProcessHandle>();

  constructor(
    private readonly runner: ProcessRunner,
    private readonly executable: string,
  ) {}

  abstract capabilities(): ExecutorCapabilities;
  protected abstract buildArgs(session: AgentSession, input: AgentInput): readonly string[];
  protected abstract promptPayload(prompt: string): string;
  protected abstract parseLine(
    value: unknown,
    session: AgentSession,
    sequence: number,
  ): AgentEvent[];
  protected abstract validateNativeSessionId(nativeSessionId: string): void;
  protected abstract versionArgs(): readonly string[];

  async start(options: StartSessionOptions): Promise<AgentSession> {
    if (options.signal?.aborted)
      throw new ProviderError(
        this.id,
        "process_failed",
        "Execution cancelled before provider start.",
      );
    return this.createRuntimeSession(options);
  }

  async resume(options: ResumeSessionOptions): Promise<AgentSession> {
    if (options.signal?.aborted)
      throw new ProviderError(
        this.id,
        "process_failed",
        "Execution cancelled before provider resume.",
      );
    try {
      this.validateNativeSessionId(options.nativeSessionId);
    } catch {
      throw new ProviderError(
        this.id,
        "resume_rejected",
        "Persisted provider session ID has an invalid format.",
      );
    }
    return this.createRuntimeSession(options, options.nativeSessionId);
  }

  async *send(session: AgentSession, input: AgentInput): AsyncIterable<AgentEvent> {
    if (input.signal?.aborted) return;
    let handle: ProcessHandle | undefined;
    let sequence = 0;
    let pendingCompletion: PendingCompletion | undefined;
    try {
      handle = await this.runner.spawn({
        executable: this.executable,
        args: this.buildArgs(session, input),
        cwd: session.workspacePath,
        ...(input.signal ? { signal: input.signal } : {}),
      });
      this.processes.set(session.runtimeSessionId, handle);
      await handle.writeStdin(this.promptPayload(input.prompt));
      await handle.closeStdin();

      const stderr = collectStderr(handle.stderr);
      for await (const line of readLines(handle.stdout)) {
        if (input.signal?.aborted) return;
        const parsed = parseJson(line, this.id);
        for (const event of this.parseLine(parsed, session, ++sequence)) {
          if (event.type === "completed") {
            pendingCompletion = event;
          } else if (event.type === "error") {
            yield event;
            await terminateQuietly(handle);
            return;
          } else {
            yield event;
          }
        }
      }

      const exit = await handle.wait();
      await stderr;
      if (input.signal?.aborted) return;
      if (!session.nativeSessionId) {
        throw new ProviderError(
          this.id,
          "protocol",
          `${this.name} did not emit a native session identity.`,
        );
      }
      if (pendingCompletion) {
        yield {
          ...pendingCompletion,
          ...(exit.code === null ? {} : { exitCode: exit.code }),
        };
      } else if (exit.code !== 0) {
        yield {
          type: "error",
          message: `${this.name} provider process failed${exit.code === null ? "" : ` with exit code ${exit.code}`}.`,
          retryable: true,
          timestamp: new Date(),
          sequence: ++sequence,
        };
      }
    } catch (error) {
      if (input.signal?.aborted) return;
      if (handle) await terminateQuietly(handle);
      yield {
        type: "error",
        message: providerErrorMessage(this.name, error),
        retryable: error instanceof ProviderError && error.kind === "unavailable",
        timestamp: new Date(),
        sequence: ++sequence,
      };
    } finally {
      if (handle) await handle.wait().catch(() => undefined);
      this.processes.delete(session.runtimeSessionId);
    }
  }

  async interrupt(session: AgentSession): Promise<void> {
    const handle = this.processes.get(session.runtimeSessionId);
    if (handle) await handle.signal("SIGINT");
  }

  async close(session: AgentSession): Promise<void> {
    const handle = this.processes.get(session.runtimeSessionId);
    if (handle) await handle.signal("SIGTERM");
  }

  async probe(): Promise<ProviderProbe> {
    try {
      const handle = await this.runner.spawn({
        executable: this.executable,
        args: this.versionArgs(),
        cwd: process.cwd(),
      });
      const output = collectText(handle.stdout);
      await handle.closeStdin();
      const exit = await handle.wait();
      await output;
      if (exit.code !== 0)
        throw new ProviderError(this.id, "unavailable", "Provider version probe failed.");
      const version = (await output).trim().split(/\r?\n/, 1)[0]?.trim();
      return {
        available: true,
        ...(version ? { version } : {}),
        capabilities: this.capabilities(),
      };
    } catch {
      return { available: false, capabilities: this.capabilities() };
    }
  }

  private createRuntimeSession(
    options: StartSessionOptions,
    nativeSessionId?: string,
  ): AgentSession {
    return {
      runtimeSessionId: `${this.id}-runtime-${++this.counter}`,
      executorId: this.id,
      projectId: options.projectId,
      workspacePath: options.workspacePath,
      ...(nativeSessionId ? { nativeSessionId, resumable: true } : {}),
      createdAt: new Date(),
    };
  }
}

export interface CodexCliExecutorOptions {
  runner?: ProcessRunner;
  executable?: string;
}

export class CodexCliExecutor extends ProcessExecutor {
  readonly id = "codex";
  readonly name = "OpenAI Codex";
  constructor(options: CodexCliExecutorOptions = {}) {
    super(options.runner ?? new NodeProcessRunner(), options.executable ?? "codex");
  }

  capabilities(): ExecutorCapabilities {
    return {
      resume: true,
      interrupt: true,
      close: true,
      sessionIdentity: true,
      streaming: true,
      approvals: false,
      models: true,
      fileAttachments: false,
      structuredOutput: true,
      worktrees: false,
    };
  }

  protected buildArgs(session: AgentSession, input: AgentInput): readonly string[] {
    const common = [
      "--json",
      "--color",
      "never",
      "--skip-git-repo-check",
      "--cd",
      session.workspacePath,
      ...(input.model ? ["--model", input.model] : []),
    ];
    return session.nativeSessionId
      ? ["exec", "resume", session.nativeSessionId, ...common, "-"]
      : ["exec", ...common, "-"];
  }

  protected promptPayload(prompt: string): string {
    return prompt;
  }

  protected parseLine(value: unknown, session: AgentSession, sequence: number): AgentEvent[] {
    if (!isRecord(value) || typeof value.type !== "string") {
      throw new ProviderError(
        this.id,
        "protocol",
        "Codex emitted an unsupported structured event.",
      );
    }
    const timestamp = new Date();
    if (value.type === "thread.started") {
      const nativeSessionId = requiredString(value.thread_id, "Codex thread ID");
      this.validateNativeSessionId(nativeSessionId);
      ensureExpectedIdentity(session, nativeSessionId, this.id);
      session.nativeSessionId = nativeSessionId;
      session.resumable = true;
      return [{ type: "session_identity", nativeSessionId, resumable: true, timestamp, sequence }];
    }
    if (value.type === "turn.completed") {
      return [{ type: "completed", summary: "Codex turn completed.", timestamp, sequence }];
    }
    if (value.type === "turn.failed" || value.type === "error") {
      return [
        {
          type: "error",
          message: "Codex reported a provider error.",
          retryable: true,
          timestamp,
          sequence,
        },
      ];
    }
    if (value.type === "item.completed" || value.type === "item.updated") {
      const item = isRecord(value.item) ? value.item : undefined;
      if (!item || typeof item.type !== "string") return [];
      if (item.type === "agent_message" && typeof item.text === "string") {
        return [{ type: "text_delta", text: item.text, timestamp, sequence }];
      }
      if (item.type === "command_execution") {
        return [
          {
            type: "tool_call",
            toolName: "command_execution",
            inputSummary: "Structured command execution",
            timestamp,
            sequence,
          },
        ];
      }
    }
    return [];
  }

  protected validateNativeSessionId(nativeSessionId: string): void {
    if (!UUID_RE.test(nativeSessionId)) throw new Error("Invalid Codex thread ID");
  }

  protected versionArgs(): readonly string[] {
    return ["--version"];
  }
}

export interface ClaudeCodeExecutorOptions {
  runner?: ProcessRunner;
  executable?: string;
}

export class ClaudeCodeExecutor extends ProcessExecutor {
  readonly id = "claude";
  readonly name = "Anthropic Claude Code";

  constructor(options: ClaudeCodeExecutorOptions = {}) {
    super(options.runner ?? new NodeProcessRunner(), options.executable ?? "claude");
  }

  capabilities(): ExecutorCapabilities {
    return {
      resume: true,
      interrupt: true,
      close: true,
      sessionIdentity: true,
      streaming: true,
      approvals: false,
      models: true,
      fileAttachments: false,
      structuredOutput: true,
      worktrees: false,
    };
  }

  protected buildArgs(session: AgentSession, input: AgentInput): readonly string[] {
    return [
      "--print",
      "--output-format",
      "stream-json",
      "--input-format",
      "stream-json",
      "--verbose",
      "--permission-prompts",
      "none",
      ...(input.model ? ["--model", input.model] : []),
      ...(session.nativeSessionId ? ["--resume", session.nativeSessionId] : []),
    ];
  }

  protected promptPayload(prompt: string): string {
    return JSON.stringify({
      type: "user",
      message: { role: "user", content: [{ type: "text", text: prompt }] },
    });
  }

  protected parseLine(value: unknown, session: AgentSession, sequence: number): AgentEvent[] {
    if (!isRecord(value) || typeof value.type !== "string") {
      throw new ProviderError(
        this.id,
        "protocol",
        "Claude Code emitted an unsupported structured event.",
      );
    }
    const timestamp = new Date();
    if (value.type === "system" && value.subtype === "init") {
      const nativeSessionId = requiredString(value.session_id, "Claude session ID");
      this.validateNativeSessionId(nativeSessionId);
      ensureExpectedIdentity(session, nativeSessionId, this.id);
      session.nativeSessionId = nativeSessionId;
      session.resumable = true;
      return [{ type: "session_identity", nativeSessionId, resumable: true, timestamp, sequence }];
    }
    if (value.type === "assistant") {
      const message = isRecord(value.message) ? value.message : undefined;
      const content = message && Array.isArray(message.content) ? message.content : [];
      return content.flatMap((block): AgentEvent[] => {
        if (!isRecord(block) || typeof block.type !== "string") return [];
        if (block.type === "text" && typeof block.text === "string") {
          return [{ type: "text_delta", text: block.text, timestamp, sequence }];
        }
        if (block.type === "tool_use" && typeof block.name === "string") {
          return [
            {
              type: "tool_call",
              toolName: block.name,
              inputSummary: "Structured tool invocation",
              timestamp,
              sequence,
            },
          ];
        }
        return [];
      });
    }
    if (value.type === "result") {
      const subtype = typeof value.subtype === "string" ? value.subtype : "";
      if (subtype === "success") {
        return [
          {
            type: "completed",
            summary:
              typeof value.result === "string" ? value.result : "Claude Code turn completed.",
            timestamp,
            sequence,
          },
        ];
      }
      return [
        {
          type: "error",
          message: "Claude Code reported a provider error.",
          retryable: true,
          timestamp,
          sequence,
        },
      ];
    }
    return [];
  }

  protected validateNativeSessionId(nativeSessionId: string): void {
    if (!UUID_RE.test(nativeSessionId)) throw new Error("Invalid Claude session ID");
  }

  protected versionArgs(): readonly string[] {
    return ["--version"];
  }
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function ensureExpectedIdentity(session: AgentSession, observed: string, provider: string): void {
  if (session.nativeSessionId && session.nativeSessionId !== observed) {
    throw new ProviderError(
      provider,
      "protocol",
      "Provider returned a different native session ID during resume.",
    );
  }
}

function requiredString(value: unknown, label: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new ProviderError("provider", "protocol", `${label} is missing from structured output.`);
  }
  return value;
}

function parseJson(line: string, provider: string): unknown {
  try {
    return JSON.parse(line) as unknown;
  } catch {
    throw new ProviderError(
      provider,
      "protocol",
      `${provider} emitted malformed structured output.`,
    );
  }
}

async function* readLines(stream: AsyncIterable<Uint8Array>): AsyncIterable<string> {
  const decoder = new TextDecoder();
  let pending = "";
  for await (const chunk of stream) {
    pending += decoder.decode(chunk, { stream: true });
    const lines = pending.split(/\r?\n/);
    pending = lines.pop() ?? "";
    for (const line of lines) if (line.trim()) yield line;
  }
  pending += decoder.decode();
  if (pending.trim()) yield pending;
}

async function collectStderr(stream: AsyncIterable<Uint8Array>): Promise<string> {
  const decoder = new TextDecoder();
  let output = "";
  for await (const chunk of stream) {
    if (output.length < 512)
      output += decoder.decode(chunk, { stream: true }).slice(0, 512 - output.length);
  }
  return output;
}

async function collectText(stream: AsyncIterable<Uint8Array>): Promise<string> {
  const decoder = new TextDecoder();
  let output = "";
  for await (const chunk of stream) output += decoder.decode(chunk, { stream: true });
  return output + decoder.decode();
}

async function terminateQuietly(handle: ProcessHandle): Promise<void> {
  await handle.signal("SIGTERM").catch(() => undefined);
  await handle.wait().catch(() => undefined);
}

function providerErrorMessage(provider: string, error: unknown): string {
  if (error instanceof ProcessStartError && error.reason === "not_found") {
    return `${provider} is unavailable. Install it and ensure it is on PATH.`;
  }
  if (error instanceof ProviderError) return error.message;
  if (error instanceof ProcessStartError) return `${provider} could not start.`;
  return `${provider} execution failed.`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
