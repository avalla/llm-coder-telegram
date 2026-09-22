# ADR-001 — Boundary ports-and-adapters

- Stato: accettato
- Data: 2026-09-22

## Contesto

Il bot deve supportare più CLI e più canali senza fare del core un adapter Claude, Codex o Telegram.

## Decisione

Il dominio espone porte tipizzate (`AgentExecutor`, `ChatGateway`, repository, `AuditLog`). Gli adapter implementano le porte e la composition root li collega. `AgentExecutor` riceve `workspacePath` già validato dal progetto; non riceve un cwd arbitrario dal testo Telegram.

## Conseguenze

Il core è testabile con fake e non richiede installazioni CLI. Un adapter deve tradurre le peculiarità del provider prima di emettere `AgentEvent`. Il numero di package resta volutamente ridotto finché i boundary non richiedono estrazione.
