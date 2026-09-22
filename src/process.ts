import { spawn as nodeSpawn, type ChildProcessWithoutNullStreams } from "node:child_process";

export interface ProcessSpec {
  executable: string;
  args: readonly string[];
  cwd: string;
  env?: Readonly<Record<string, string | undefined>>;
  signal?: AbortSignal;
}

export interface ProcessExit {
  code: number | null;
  signal?: string;
}

export interface ProcessHandle {
  readonly pid?: number;
  readonly stdout: AsyncIterable<Uint8Array>;
  readonly stderr: AsyncIterable<Uint8Array>;
  writeStdin(chunk: string | Uint8Array): Promise<void>;
  closeStdin(): Promise<void>;
  signal(signal: "SIGINT" | "SIGTERM" | "SIGKILL"): Promise<void>;
  wait(): Promise<ProcessExit>;
}

export interface ProcessRunner {
  spawn(spec: ProcessSpec): Promise<ProcessHandle>;
}

export class ProcessStartError extends Error {
  constructor(public readonly reason: "not_found" | "spawn_failed") {
    super(
      reason === "not_found"
        ? "Provider executable is unavailable."
        : "Provider process could not start.",
    );
    this.name = "ProcessStartError";
  }
}

export interface NodeProcessRunnerOptions {
  gracePeriodMs?: number;
  spawnImpl?: typeof nodeSpawn;
}

export class NodeProcessRunner implements ProcessRunner {
  private readonly gracePeriodMs: number;
  private readonly spawnImpl: typeof nodeSpawn;

  constructor(options: NodeProcessRunnerOptions = {}) {
    this.gracePeriodMs = options.gracePeriodMs ?? 250;
    this.spawnImpl = options.spawnImpl ?? nodeSpawn;
  }

  async spawn(spec: ProcessSpec): Promise<ProcessHandle> {
    if (spec.signal?.aborted) throw new Error("Process start aborted.");
    let child: ChildProcessWithoutNullStreams;
    try {
      child = this.spawnImpl(spec.executable, [...spec.args], {
        cwd: spec.cwd,
        env: spec.env ? { ...spec.env } : process.env,
        stdio: ["pipe", "pipe", "pipe"],
        shell: false,
      });
    } catch {
      throw new ProcessStartError("spawn_failed");
    }

    const handle = this.createHandle(child, spec.signal);
    try {
      await new Promise<void>((resolve, reject) => {
        child.once("spawn", resolve);
        child.once("error", (cause: NodeJS.ErrnoException) => {
          reject(new ProcessStartError(cause.code === "ENOENT" ? "not_found" : "spawn_failed"));
        });
      });
      return handle;
    } catch (error) {
      await handle.wait().catch(() => undefined);
      throw error;
    }
  }

  private createHandle(
    child: ChildProcessWithoutNullStreams,
    signal: AbortSignal | undefined,
  ): ProcessHandle {
    let terminated = false;
    let forceTimer: ReturnType<typeof setTimeout> | undefined;
    const terminate = (terminationSignal: "SIGINT" | "SIGTERM" | "SIGKILL"): void => {
      if (terminated && terminationSignal !== "SIGKILL") return;
      if (terminationSignal !== "SIGKILL") terminated = true;
      try {
        child.kill(terminationSignal);
      } catch {
        return;
      }
      if (terminationSignal !== "SIGKILL") {
        forceTimer = setTimeout(() => terminate("SIGKILL"), this.gracePeriodMs);
      }
    };
    const abortHandler = (): void => terminate("SIGTERM");
    signal?.addEventListener("abort", abortHandler, { once: true });

    const exitPromise = new Promise<ProcessExit>((resolve, reject) => {
      child.once("error", (cause: NodeJS.ErrnoException) => {
        reject(new ProcessStartError(cause.code === "ENOENT" ? "not_found" : "spawn_failed"));
      });
      child.once("close", (code: number | null, childSignal: NodeJS.Signals | null) =>
        resolve({ code, ...(childSignal ? { signal: childSignal } : {}) }),
      );
    });
    const wait = async (): Promise<ProcessExit> => {
      const exit = await exitPromise;
      if (forceTimer) clearTimeout(forceTimer);
      signal?.removeEventListener("abort", abortHandler);
      return exit;
    };

    return {
      ...(child.pid === undefined ? {} : { pid: child.pid }),
      stdout: child.stdout,
      stderr: child.stderr,
      writeStdin: async (chunk) => {
        if (!child.stdin.write(chunk)) await onceDrain(child.stdin);
      },
      closeStdin: async () => {
        child.stdin.end();
      },
      signal: async (terminationSignal) => terminate(terminationSignal),
      wait,
    };
  }
}

function onceDrain(stream: NodeJS.WritableStream): Promise<void> {
  return new Promise((resolve) => stream.once("drain", resolve));
}

export class FakeProcessRunner implements ProcessRunner {
  readonly specs: ProcessSpec[] = [];
  private readonly handles: ProcessHandle[] = [];

  constructor(private readonly handleFactory: () => ProcessHandle) {}

  async spawn(spec: ProcessSpec): Promise<ProcessHandle> {
    this.specs.push(spec);
    const handle = this.handleFactory();
    this.handles.push(handle);
    return handle;
  }

  createdHandles(): readonly ProcessHandle[] {
    return this.handles;
  }
}
