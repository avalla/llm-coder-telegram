# Architettura

## Boundary

Il core conosce solo `AgentExecutor`, `AgentEvent`, repository e `ChatGateway`. Telegram è un inbound/outbound adapter; ogni CLI è un adapter di executor; SQLite è un adapter di persistence. La composition root in `src/main.ts` collega le implementazioni.

La gerarchia Telegram è:

```text
Supergroup forum
├── Control                 TelegramTopic(kind=control)
├── [Codex] ai-office · PR61   TelegramTopic → BotSession → AgentSession
├── [Claude] ai-office · storage
└── [Codex] autoepoque · PR98
```

`chatId + threadId` è una chiave persistente di routing. Un topic operativo indica un logical workspace/session, non un provider: il provider è `BotSession.executorId`.

## Modello di dominio

- `Project`: allowlist di workspace e executor consentiti.
- `BotSession`: identità logica del topic; lega progetto, executor e directory controllata.
- `Execution`: singolo prompt/run, con stato, correlation ID, utente e tempi.
- `AgentSession`: conversazione persistente del provider quando supportata; il suo ID non viene usato come routing Telegram.
- `ExecutorSession`: forma persistibile per metadata provider; nella slice è rappresentata dall’`AgentSession` runtime.
- `TelegramTopic`: mapping e stato del topic, inclusi `control`, `closed` e `deleted`.
- `ApprovalRequest`: contratto per mediare approval native dell’executor in una futura UI inline.

La relazione importante è `BotSession 1 → N Execution`, mentre più execution possono riusare un `AgentSession` nativo.

## Contratti

`AgentExecutor` espone `start`, `send`, `interrupt`, `close` e un `resume` opzionale. `send` restituisce `AsyncIterable<AgentEvent>`, così il core non deve sapere se l’executor usa stdout JSONL, stdin streaming o un SDK.

`AgentEvent` è discriminated union: testo, thinking, tool call/result, command, file change, approval, usage, error e completed. Il renderer Telegram reagisce solo a questi eventi.

`ChatGateway` supporta messaggi, edit, documenti, creazione e chiusura topic. La Bot API usa `message_thread_id`; non è presente nel dominio applicativo oltre agli adapter e alla chiave persistente.

## Lifecycle

```text
Telegram update
  → Telegram adapter normalizza chatId/threadId/userId/text
  → TelegramTopic/BotSession lookup
  → AuthorizationService (chat + user ID + ruolo)
  → SessionQueue serializza per BotSession
  → Execution pending → running
  → ExecutorRegistry seleziona adapter
  → AgentExecutor.start/resume
  → AgentExecutor.send → AgentEvent normalizzati
  → ThrottledEventRenderer aggiorna un solo messaggio
  → Execution completed/failed/stopped + BotSession idle/failed/stopped
  → audit + persistence
```

Una seconda richiesta nello stesso topic aspetta la prima; topic diversi non condividono la coda.

## Capability reali verificate

Versioni presenti sulla macchina durante la ricognizione: `codex-cli 0.155.1`, `Claude Code 2.1.278`, `Bun 1.4.2`.

Codex espone `codex exec --json` con eventi JSONL, `exec resume --last` o un session ID, `--cd`, `--model`, `--sandbox`, `--image`, `--worktree`, `--output-last-message` e segnali di processo. Il requisito di repository Git e la semantica sandbox/approval devono restare responsabilità dell’adapter. La documentazione ufficiale descrive anche `--output-schema` e la persistenza degli ID thread.

Claude Code espone `claude -p --output-format stream-json`, input `stream-json`, `--resume`, `--continue`, `--model`, `--permission-mode`, `--permission-prompts`, `--include-partial-messages` e `--session-id`. Il flusso interattivo permission prompt non va emulato in base a testo umano: l’adapter dovrà usare il canale strutturato previsto dalla CLI/SDK.

Differenze sostanziali:

| Aspetto         | Codex                         | Claude Code                               |
| --------------- | ----------------------------- | ----------------------------------------- |
| non-interactive | `codex exec`                  | `claude -p`                               |
| eventi          | JSONL su stdout con `--json`  | `stream-json` su stdout                   |
| resume          | `exec resume <id>` / `--last` | `--resume <id>` / `--continue`            |
| sandbox         | `--sandbox` e approval CLI    | permission mode/prompt host               |
| immagini        | `--image`                     | file/input secondo CLI/SDK                |
| worktree        | flag nativa `--worktree`      | worktree support da valutare nell’adapter |

Queste differenze sono isolate in M2/M3; non entrano in `AgentEvent` oltre alle capability dichiarate.

## Recovery e cancellation

Durante il bootstrap, execution `running` senza processo riconciliabile deve diventare `unknown`/`interrupted` secondo una policy esplicita, non `completed`. Se l’executor conserva un native session ID, `/restart` può invocare `resume`; altrimenti si crea una nuova `AgentSession` e si informa l’utente.

`/stop` cancella l’`AbortController`, invia `interrupt` al provider e persiste `stopped`. Il processo adapter deve poi inviare il signal OS corretto e attendere l’exit; il core non interpreta l’assenza di output come successo.

## Deferred

Approval inline, attachment storage, real Codex/Claude adapters, worktree lifecycle, webhook Telegram, PostgreSQL/Supabase e recovery dei PID sono deliberatamente fuori da questa PR.
