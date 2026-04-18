# Hyperliquid Scanner & Trader

This project scans watched Hyperliquid perpetual pairs, learns long/short fingerprints from recent moves, sends Telegram alerts, and can trade in either `REAL` or `DEMO` execution mode with separate `SIMPLE` and `COMPOUNDING` capital modes.

## Key upgrades

- Hyperliquid links and inline buttons on signal/trade messages
- Two independent runtime mode families:
  - Capital mode: `SIMPLE`, `COMPOUNDING`
  - Execution mode: `REAL`, `DEMO`
- Strict mode lock while trades are still active/pending/protected/reconciling
- Durable per-profile runtime settings in SQLite
- Telegram-first runtime control surface for pairs, modes, balances, reconciliation, automation, and strategy maintenance
- Dynamic pair management validated against Hyperliquid metadata
- Separate PnL views by execution mode and capital mode
- Safe multi-user automation onboarding with masked admin review and encrypted agent secrets at rest
- Restart-safe reconciliation and TP/SL re-arming
- Weekly strategy pruning with archive output

## Storage

- SQLite DB: `storage/bot-state.sqlite`
- Strategy exports and prune archives: `storage/exports/*.txt`
- Legacy JSON files are read once for migration where possible

## Required environment

Set Telegram plus Hyperliquid values in `.env`.

Core bot values:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `TELEGRAM_ADMIN_IDS=12345,67890`
- `BOT_STATE_ENCRYPTION_KEY=...`

Legacy/default live wallet values for the default system profile:

- `HYPERLIQUID_SECRET_KEY`
- `HYPERLIQUID_ACCOUNT_ADDRESS`
- `HYPERLIQUID_VAULT_ADDRESS`

Useful runtime defaults:

- `DEFAULT_TRADE_LEVERAGE=10`
- `DEFAULT_TRADE_BALANCE=100`
- `DEFAULT_CAPITAL_MODE=SIMPLE`
- `DEFAULT_EXECUTION_MODE=DEMO`
- `DEFAULT_SIMPLE_SLOTS=1`
- `DEFAULT_DEMO_BALANCE=100`
- `FORCE_MODE_CHANGE_ENABLED=false`

## Main Telegram commands

General:

- `/help`
- `/status`
- `/scan`
- `/positions`
- `/closed`
- `/pnl`
- `/balance`

Capital mode:

- `/capitalmode`
- `/setcapitalmode simple`
- `/setcapitalmode compounding`
- `/principal`
- `/setprincipal 100`
- `/slots`
- `/setslots 2`
- `/capitalstatus`
- `/sweepstatus`

Execution mode:

- `/executionmode`
- `/setexecutionmode real`
- `/setexecutionmode demo`
- `/demobalance`
- `/setdemobalance 100`
- `/realstatus`
- `/demostatus`

Trading controls:

- `/autotrade on`
- `/autotrade off`
- `/tradebalance`
- `/settradebalance 100`
- `/leverage`
- `/setleverage 10`
- `/dryrun`
- `/dryrunlong`
- `/dryrunshort`
- `/dryrunpair BTCUSDT long`

Pairs:

- `/pairs`
- `/addpair BTCUSDT`
- `/removepair BTCUSDT`
- `/reloadpairs`
- `/pairstatus BTCUSDT`

Reconciliation:

- `/reconcile`
- `/reconcile me`
- `/reconcile user <user>`
- `/reconcileall`

Automation onboarding:

- `/connecttrading`
- `/setwallet 0x...`
- `/setagentaddress 0x...`
- `/setagentprivatekey <secret>`
- `/submitautomationrequest`
- `/automationstatus`
- `/disableautomation`
- `/enableautomation`

Admin:

- `/pendingapprovals`
- `/approveautomation <request_id>`
- `/rejectautomation <request_id> <reason>`
- `/removeautomation <user>`
- `/disablewallet <user>`
- `/enablewallet <user>`
- `/listautomationusers`
- `/viewuserconfig <user>`
- `/forcesetcapitalmode <user> simple|compounding`
- `/forcesetexecutionmode <user> real|demo`

Strategy maintenance:

- `/strategystatus`
- `/strategytop`
- `/strategyprune`
- `/exportstrategies`
- `/strategyexports`
- `/importstrategies latest`
- `/importstrategies strategies-1234567890.txt`
- `/strategyretention`
- `/setstrategyretentiondays 7`
- `/setstrategycap 500`

Legacy strategy utilities:

- `/rebuildstrategies`

## Notes

- Hyperliquid metadata is the source of truth for tradable pair validation.
- `pair.js` is now only a bootstrap default list, not a runtime allowlist gate.
- `REAL` execution is blocked until the profile is approved and enabled.
- Agent secrets are stored encrypted at rest and are never echoed back in Telegram.
- In restricted sandboxes, live Hyperliquid and Telegram verification still needs testing in the target runtime environment.
