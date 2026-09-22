# ADR-003 — Normalized events and executor registry

- Stato: accettato
- Data: 2026-09-22

## Contesto

Codex e Claude hanno output e semantiche di resume/approval diversi. Parsing di testo human-readable nel Telegram layer sarebbe fragile.

## Decisione

Ogni executor dichiara capability e converte il proprio stream in `AgentEvent`, una discriminated union comune. Gli executor si registrano con `ExecutorRegistry`; l’orchestrator risolve per ID e non contiene switch per provider.

## Conseguenze

Il renderer rimane stabile quando si aggiunge Aider o un agent locale. Gli adapter devono mantenere compatibilità con versioni CLI e testare fixture JSON/JSONL reali. Le capability governano la UX disponibile.
