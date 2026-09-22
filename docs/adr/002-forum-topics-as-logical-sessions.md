# ADR-002 — Forum Topics as logical sessions

- Stato: accettato
- Data: 2026-09-22

## Contesto

Una chat unica mescolerebbe repository, provider e conversazioni. Telegram Forum Topics fornisce un thread stabile e indirizzabile.

## Decisione

`chatId + messageThreadId` identifica un `TelegramTopic`. `Control` è un topic speciale; ogni topic operativo mappa a una `BotSession`. Il titolo è UX, non identità: rinominarlo non rompe il routing.

## Conseguenze

La persistenza sopravvive ai restart e permette più provider sullo stesso supergroup. Creazione/chiusura topic richiede `can_manage_topics`; deletion/close diventano stati persistiti e non cancellano automaticamente lo storico agent.
