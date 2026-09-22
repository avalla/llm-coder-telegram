# ADR-004 — SQLite first, repository ports

- Stato: accettato
- Data: 2026-09-22

## Contesto

Il deployment è un singolo VPS e il bot deve recuperare mapping e execution dopo restart. In futuro può servire PostgreSQL/Supabase.

## Decisione

SQLite via `bun:sqlite` è l’implementazione iniziale. Il dominio dipende da repository interfaces; lo schema contiene progetti, bot session, execution, topic e audit log. Le date e gli ID nativi sono persistiti senza assumere un database specifico.

## Conseguenze

Setup e backup restano semplici. Locking distribuito e alta disponibilità non sono obiettivi M0; un futuro adapter PostgreSQL può implementare le stesse porte.
