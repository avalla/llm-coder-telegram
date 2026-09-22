export interface ProcessSpec {
  executable: string;
  args: readonly string[];
  cwd: string;
  env?: Readonly<Record<string, string>>;
  timeoutMs?: number;
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

/**
 * Executor adapters depend on this port rather than Bun.spawn directly.
 * The Bun implementation is intentionally scheduled with the first real CLI
 * adapter, because PTY and approval semantics differ between providers.
 */
export interface ProcessRunner {
  spawn(spec: ProcessSpec): Promise<ProcessHandle>;
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
