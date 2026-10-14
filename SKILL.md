---
name: pmwallets
description: Follow profitable Polymarket traders with PMWallets and act on their trades — find traders on the smart-money leaderboard, subscribe to their live fills, read each fill with its market, and (with pmwallets-copytrade) place your own Polymarket orders. Use when the user mentions PMWallets, Polymarket copy trading, smart money, whale tracking, or reacting to a trader's fills.
---

# PMWallets for AI agents

PMWallets scores every Polymarket trader from the Polygon chain and streams the fills of the traders an account
follows. Two command-line tools, both printing JSON:

- `pmwallets` (npm `pmwallets` / PyPI `pmwallets`) — the data: leaderboard, traders, subscriptions, live fills.
  Needs `PMW_API_KEY` (create one on https://pmwallets.com/keys).
- `pmwallets-copytrade` (npm / PyPI `pmwallets-copytrade`) — the trading: market and order book, balance,
  positions, orders on Polymarket. Reads a `config.yaml` (`pmwallets-copytrade init` writes one; Polymarket keys go
  in environment variables it names). Its `mode` decides everything: `dry-run` simulates each order against the live
  book and sends nothing; `live` trades real money.

Both also run as MCP servers (`pmwallets mcp`, `pmwallets-copytrade mcp --config <path>`) with the same tools.

## Find and follow traders

```bash
pmwallets leaderboard --period 30d --minWinLo 0.55 --category politics --limit 20
pmwallets entity <handle>                 # one trader's stats; a handle is 12 characters, e.g. 7KQ2MF9X4B1C
pmwallets subscribe <handle>              # BILLS the account per hour (or uses the plan); 402 = balance too low
pmwallets subscriptions
pmwallets unsubscribe <subscription-id>
```

`minWinLo` is the lower bound of the 95% win-rate interval: a high value means the record is unlikely to be luck.

## Receive fills

Every fill carries `eventId` (unique; use it as an idempotency key), `side` (BUY/SELL), `price` (0–1 per share),
`tokenId` (the outcome), `usdcAmount` / `sharesAmount` (decimals; the raw `usdc` / `shares` are 1e-6 integer
units), and `market` (`question`, `outcome`, `endDate`) when Polymarket could be asked.

- **Pull** (any agent, no long-running process): `pmwallets new-fills` returns the fills since the previous call,
  each once. Call it on a schedule. The position lives in `~/.pmwallets/mcp-cursor.json` (or `$PMW_MCP_STATE`),
  shared with the MCP tool `new_fills`: one consumer per file — give each agent its own `PMW_MCP_STATE`.
- **Push**: `pmwallets watch` keeps a live connection and prints one JSON line per fill. With `--exec "<command>"` it
  runs the command for each batch of fills, the fills on its stdin as JSON lines (also in `$PMW_BATCH_FILE`); fills
  that arrive while the command runs make the next batch; `--batch-sec N` waits N seconds to collect a batch. A batch
  is handed over at most once: a failed or interrupted one is kept in `pmw-watch/failed/`, never re-run.
  One live connection per PMWallets account — a second `watch` (or the copy-trading bot) on the same account is
  refused unless `--takeover`.

## Trade

```bash
pmwallets-copytrade market <tokenId>                 # question, outcomes, prices, order book: read before deciding
pmwallets-copytrade balance
pmwallets-copytrade positions
pmwallets-copytrade trades --limit 20
pmwallets-copytrade order buy <tokenId> --usdc 5 --max-price 0.45 --key <eventId>
pmwallets-copytrade order sell <tokenId> --all --min-price 0.40 --key <eventId>:exit
```

- BUY is fill-or-kill: all of it at `--max-price` or better, or nothing. `--usdc` is a ceiling — the size is
  `usdc / max-price`, so the order can never cost more; set `--max-price` close to the ask to spend close to `--usdc`.
  Without `--max-price` the limit is the best ask now.
- SELL is fill-and-kill: as much as crosses at `--min-price` or better, the rest cancelled; capped at what the
  account holds.
- **Always pass `--key`.** The same key never sends a second order, so a retry after a timeout is safe.
- `status`: `filled`, `none` (nothing matched), `skipped` (price outside your limit, market closed — nothing sent),
  `failed` (rejected), `dry_run`, or `unknown` / `verify: true` — the order MAY have filled: check `trades` before
  sending another. Exit code 0 = filled or dry_run, 2 = not traded, 3 = unknown.
- Polymarket refuses orders from some countries (the UK, the US and others); `pmwallets-copytrade check` tells.

## Recipes

React to every fill with an agent CLI (any agent that takes a prompt and reads stdin works the same way):

```bash
export PMW_API_KEY=pmw_…
pmwallets watch --batch-sec 30 --exec 'claude -p "Fills from traders I follow are on stdin. For each BUY above $1000 in a market ending after 2026-12-31, read the market with pmwallets-copytrade market, then buy $5 with --key <eventId> if the price is below 0.6. One line per fill: what you did." --allowedTools "Bash(pmwallets-copytrade:*)"'
# Codex:  --exec 'codex exec --sandbox danger-full-access "…same instructions…"'   (its default sandbox has no network)
# Gemini: --exec 'gemini -p "…" --yolo'
```

MCP (Claude Desktop, Cursor, Windsurf, Cline, WorkBuddy, …):

```json
{
  "mcpServers": {
    "pmwallets": { "command": "npx", "args": ["-y", "pmwallets", "mcp"], "env": { "PMW_API_KEY": "pmw_…" } },
    "copytrade": { "command": "npx", "args": ["-y", "pmwallets-copytrade", "mcp", "--config", "/abs/path/config.yaml"] }
  }
}
```

No shell or MCP (Dify, Coze, n8n, GPT Actions): import https://pmwallets.com/openapi.json and call the REST API with
the `x-api-key` header, or receive fills as signed webhooks (set one up on https://pmwallets.com/feed).

Docs: https://pmwallets.com/docs · Python: `pip install pmwallets pmwallets-copytrade` gives the same commands.
