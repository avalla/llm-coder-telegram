# Threat model

Questo sistema è una remote shell mediata da agenti. Il confine di fiducia è tra Telegram/user autorizzato, bot, processo del bot, CLI provider e workspace.

## Asset

- repository e file del workspace;
- credenziali provider, Telegram token e `auth.json`;
- prompt, output, diff e log;
- mapping topic/sessione e audit trail;
- disponibilità e integrità del processo bot.

## Attori e minacce principali

| Attore/percorso                 | Rischio                          | Mitigazione                                                                             |
| ------------------------------- | -------------------------------- | --------------------------------------------------------------------------------------- |
| utente Telegram non autorizzato | invio prompt o stop              | allowlist numerica di `userId` e `chatId`; ruoli server-side                            |
| username spoof/change           | bypass identità                  | username mai usato per authorization                                                    |
| prompt injection nel repository | agent esegue azioni indesiderate | sandbox/permission native dell’executor; approval mediata; directory allowlist          |
| path traversal / cwd manipolato | accesso fuori progetto           | `realpath`, canonicalizzazione, root allowlist, nessun cwd dal prompt                   |
| secret in output/log            | esfiltrazione                    | redaction, output tail, documenti controllati, niente prompt/token nei log              |
| abuso Telegram                  | flood/costi                      | rate limit per user/sessione, budget provider, audit e correlation ID                   |
| topic rename/delete/close       | routing incoerente               | mapping persistente; eventi topic aggiornano lo stato; mai ricavare sessione dal titolo |
| restart durante run             | stato falso                      | riconciliazione `unknown/interrupted`, resume solo con ID nativo verificato             |
| concorrenza sul repository      | file corrotti                    | una execution per `BotSession`; worktree per stesso repository in M5                    |
| shell arbitraria del bot        | RCE ampliata                     | nessuna command feature Telegram generica in M1; solo CLI configurate                   |

## Regole operative

1. Token e credenziali arrivano da environment/secret store e non da Git.
2. Un progetto configurato è l’unico modo di ottenere un workspace.
3. Il testo Telegram è input agent, non comando shell.
4. Ogni mutazione passa dalla policy applicativa e produce audit.
5. Approval provider-native deve rimanere provider-native; Telegram media una decisione, non inventa una seconda policy.

## Residui accettati in M0/M1

La slice fake non esegue codice reale. Il polling MVP non implementa ancora callback query per approval e il `ProcessRunner` è per ora una porta con fake: l’implementazione Bun/PTY verrà validata insieme agli adapter reali. Sono prerequisiti per M2/M4, non autorizzazioni implicite.
