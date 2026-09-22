# Telegram Coding Agent Bot

Bot Telegram pluggabile per pilotare coding agent CLI tramite Forum Topics. La prima iterazione contiene M0 e una vertical slice M1 con `FakeAgentExecutor`: il core non importa Telegram, Claude Code o Codex.

## Stato della prima PR

- dominio tipizzato: `Project`, `BotSession`, `Execution`, `AgentSession`, `TelegramTopic`, `ApprovalRequest`;
- registry di executor senza `switch` sul provider;
- coda seriale per `BotSession`, concorrenza tra sessioni diverse;
- authorization server-side per chat/user/ruolo;
- gateway Telegram nativo via `fetch`, con `message_thread_id`, creazione e chiusura topic;
- SQLite persistence tramite `bun:sqlite`;
- rendering di stato con edit throttled e coda output limitata;
- `FakeAgentExecutor` e test Vitest senza Telegram o CLI installati.

Gli adapter reali Codex e Claude Code sono deliberatamente rimandati a M2/M3. Le capability verificate e le differenze note sono in [docs/architecture.md](docs/architecture.md).

## Avvio locale

Prerequisiti: Bun 1.4+, un Supergroup Telegram configurato come forum e un bot amministratore con `can_manage_topics`.

```bash
bun install
bun run check
```

Preparare il topic `Control` e impostare il suo `message_thread_id`. Poi avviare la slice fake:

```bash
TELEGRAM_BOT_TOKEN='...' \
TELEGRAM_CHAT_ID='-1001234567890' \
TELEGRAM_CONTROL_THREAD_ID='42' \
BOT_USERS_JSON='{"123456789":"owner"}' \
BOT_PROJECTS_JSON='{"ai-office":{"path":"/home/assistant/projects/ai-office","allowedExecutors":["fake"]}}' \
bun run dev
```

Nel topic `Control`:

```text
/new ai-office fake "PR61 hardening"
```

Il bot crea il topic operativo e un messaggio plain text nel nuovo topic diventa un `Execution` inviato al fake executor.

## Layout

```text
src/domain.ts       contratti di dominio e porte
src/application.ts  authorization, registry, queue, orchestrator, renderer
src/adapters.ts     fake executor, gateway e store in-memory per test
src/telegram.ts     adapter Telegram Bot API via fetch
src/process.ts      ProcessRunner port e fake per gli adapter CLI
src/sqlite.ts       adapter persistence SQLite
src/config.ts       configurazione e canonicalizzazione workspace
src/main.ts         composition root dell’MVP
tests/              test core e vertical slice
docs/               architettura, threat model e ADR
```

## Comandi

```bash
bun run check       # typecheck + ESLint + test + Prettier
bun run test:watch
bun run dev
```

Non inserire token Telegram, `auth.json` di Codex o chiavi provider nei log, nella configurazione versionata o nei commit.
