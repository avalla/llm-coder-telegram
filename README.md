# Telegram Coding Agent Bot

Bot Telegram pluggabile per pilotare coding agent CLI tramite Forum Topics. M3 aggiunge orchestrazione durable e recovery restart-safe; M4 aggiunge reconciliation operativa per gli esiti provider incerti. Il core resta provider-neutral.

## Stato M4

- dominio tipizzato: `Project`, `BotSession`, `Execution`, runtime `AgentSession`, persisted `ExecutorSession`, `TelegramTopic`, `ApprovalRequest`;
- native provider identity discovered from the event stream and persisted separately from Telegram/runtime IDs;
- registry di executor senza `switch` sul provider;
- execution orchestration durable tramite porte `ExecutionJobQueue`/`ExecutionJobWorker` e outbox SQLite; claim atomica per `BotSession`, concorrenza tra sessioni diverse, polling Telegram supervisionato;
- authorization server-side per chat/user/ruolo;
- gateway Telegram nativo via `fetch`, con `message_thread_id`, creazione e chiusura topic; topic rename/delete reconciliation è differita;
- SQLite persistence tramite `bun:sqlite`;
- rendering di stato con edit throttled e coda output limitata;
- adapter reali Codex CLI e Claude Code con fresh execution, resume esatto, identity validation e cancellation;
- process boundary con `spawn(executable, argv)`, stdin strutturato e nessuna shell interpolation;
- `/session` mostra stato conciso senza token, environment o command line sensibili;
- `FakeAgentExecutor` e process fakes mantengono i test deterministici senza provider installati;
- un execution `unknown` mette in quarantena il BotSession: nessun prompt successivo raggiunge il provider fino a reconciliation esplicita;
- la reconciliation è un’asserzione operativa immutabile, non una prova del provider, e non invoca mai `start`, `resume`, `send`, `interrupt` o `close`.

Le capability e il lifecycle provider sono documentati in [docs/architecture.md](docs/architecture.md).

## Avvio locale

Prerequisiti: Bun 1.4+, un Supergroup Telegram configurato come forum e un bot amministratore con `can_manage_topics`.

```bash
bun install
bun run check
```

Preparare il topic `Control` e impostare il suo `message_thread_id`. Poi avviare il bot con un adapter reale:

```bash
TELEGRAM_BOT_TOKEN='...' \
TELEGRAM_CHAT_ID='-1001234567890' \
TELEGRAM_CONTROL_THREAD_ID='42' \
BOT_USERS_JSON='{"123456789":"owner"}' \
BOT_PROJECTS_JSON='{"ai-office":{"path":"/home/assistant/projects/ai-office","allowedExecutors":["codex","claude"]}}' \
bun run dev
```

Nel topic `Control`:

```text
/new ai-office codex "PR61 hardening"
```

Il bot crea il topic operativo e un messaggio plain text nel nuovo topic diventa un `Execution` inviato all’adapter configurato. Nel topic operativo `/session` o `/session info` mostra topic, runtime ID, executor, native ID, stato e supporto resume. Se un execution è `unknown`, il topic resta bloccato e `/reconcile status` mostra l’ambiguità senza stampare il prompt.

## Layout

```text
src/domain.ts       contratti di dominio e porte
src/application.ts  authorization, registry, queue, orchestrator, renderer
src/adapters.ts     fake executor, gateway e store in-memory per test
src/provider-adapters.ts  adapter reali Codex/Claude + stream parser
src/process.ts      ProcessRunner, spawn senza shell e fake per test
src/telegram.ts     adapter Telegram Bot API via fetch
src/sqlite.ts       adapter persistence SQLite
src/config.ts       configurazione e canonicalizzazione workspace
src/main.ts         composition root dell’MVP
tests/              test core e vertical slice
docs/               architettura, threat model e ADR
```

## Comandi

Nel topic operativo:

```text
/status
/reconcile
/reconcile status
/reconcile <execution-id> complete <reason>
/reconcile <execution-id> abandon <reason>
```

`/reconcile` e `/reconcile status` richiedono ruolo `viewer`; `complete` e `abandon` richiedono ruolo `operator`. Le due mutazioni registrano una sola reconciliation immutabile: `complete` significa `confirmed_completed`, mentre `abandon` sblocca senza dichiarare che il provider abbia completato. Ogni retry futuro è un nuovo execution.

```bash
bun run check       # typecheck + ESLint + test + Prettier
bun run test:watch
bun run dev
```

Provider richiesti: Codex CLI `codex` e/o Claude Code `claude` disponibili nel `PATH`, autenticati secondo la documentazione del provider. Versioni verificate per M2: Codex CLI 0.155.1 e Claude Code 2.1.280. Un executable mancante viene riportato come provider unavailable; il bot non esegue una sessione sostitutiva. Troubleshooting: eseguire codex --version o claude --version nello stesso environment del bot. Se /session mostra native session not established, controllare availability/auth e lo stream strutturato; un resume rifiutato fallisce l’execution e conserva il native ID precedente.

Non inserire token Telegram, `auth.json` di Codex, chiavi provider, prompt o environment nei log, nella configurazione versionata o nei commit.
