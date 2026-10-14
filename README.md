# pmwallets — Python SDK for PMWallets

<!-- mcp-name: io.github.polymarketwallets/pmwallets -->

[![PyPI](https://img.shields.io/pypi/v/pmwallets.svg)](https://pypi.org/project/pmwallets/) [![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE) [![Discord](https://img.shields.io/badge/Discord-join-5865F2?logo=discord&logoColor=white)](https://discord.gg/Xy5uTTb3gN)

Official Python client for [PMWallets](https://pmwallets.com): the **Polymarket smart-money leaderboard** computed
from the Polygon chain, and the **real-time fills of the Polymarket wallets you follow** — delivered in order,
exactly once, with every gap replayed.

[中文说明](README.zh.md) · Node.js SDK: [pmwallets-node](https://github.com/polymarketwallets/pmwallets-node) ·
Ready-to-run copy-trading bot built on it: [polymarket-copy-trading-bot-python](https://github.com/polymarketwallets/polymarket-copy-trading-bot-python)

## Install

```bash
pip install pmwallets     # Python ≥ 3.10
```

Create an API key on [pmwallets.com/keys](https://pmwallets.com/keys).

## Usage

```python
import asyncio
from pmwallets import AsyncClient, Client, FillStream, FileStateStore

# synchronous REST
with Client(api_key="pmw_...") as pmw:
    board = pmw.leaderboard(minWinLo=0.55, minEligible=30, style="taker", status="active", limit=50)
    cheap = pmw.leaderboard(bands=[0, 1], minWinLo=0.55)   # address unlocks free or in the $1 band (0 = free ... 5)
    pmw.subscribe(board["rows"][0]["entityId"], channels=["ws"])   # billed per entity per hour

# every fill of every entity you follow
async def main():
    async with AsyncClient(api_key="pmw_...") as pmw:
        stream = FillStream(
            client=pmw,
            store=FileStateStore("stream.json"),     # a restart resumes exactly where it stopped
            on_fill=lambda fill, meta: print(meta.source, fill["side"], fill["price"], fill["tokenId"]),
            on_event=lambda e: e["type"] == "fatal" and print(e["error"]),   # e.g. another bot already holds the stream
            # takeover=True,   # replace whichever API client holds this account's stream (it stops with "replaced")
        )
        await stream.run()

asyncio.run(main())
```

## What is in it

| | |
|---|---|
| `Client` / `AsyncClient` | leaderboard, entities, address unlocks (balance or plan), your unlocked addresses (`my_addresses`), plan and balance, subscriptions, webhook status and deliveries, fills replay (`fills`, `fills_since`), trade-history exports |
| `FillStream` | asyncio WebSocket with reconnect and keep-alive; detects missed frames by `session`/`seq` and replays them from the last fill delivered; de-duplicates by `eventId`; anchors behind the chain head on first start; persisted cursor |
| `verify_webhook(raw_body, signature, secret)` | checks `x-pmw-signature` (HMAC-SHA256 of the raw body) |

One stream per account:

- An API-key connection takes priority over the pmwallets.com feed page (the page never takes the stream from it).
- Between two API connections **the first one keeps the stream**. A second one — another key of the same account, or the same bot started twice — is refused: `blocked`, then `fatal`, and the stream stops. Pass `takeover=True` to replace the holder instead; the holder gets `replaced`, then `fatal`, and stops (it does not fight back).
- A `FillStream` identifies itself with a random `client_id`, so its own reconnects after a network drop replace its own dead connection without needing `takeover`.
- When the server restarts it closes the socket with `1012`; the stream reconnects at once (event `restarting`) and replays anything it missed.

`HTTPS_PROXY` is honoured.

PMWallets' servers are in the United Kingdom. A consumer that also trades on Polymarket should run from Ireland (AWS eu-west-1): Polymarket does not accept API orders from the UK, the US and several EU countries.

## AI agents: command line and MCP

The package installs a `pmwallets` command that prints JSON — for scripts, and for AI agents (Claude Code, Codex,
Gemini CLI, Cursor, WorkBuddy, …) that act on the fills of the traders you follow.

```bash
export PMW_API_KEY=pmw_…
pmwallets leaderboard --minWinLo 0.55 --category politics --limit 20
pmwallets subscribe <handle>          # billed per hour
pmwallets new-fills                   # fills since the previous call, each once (poll it)
pmwallets watch                       # live: one JSON line per fill, with its market and decimal amounts
pmwallets watch --batch-sec 30 --exec 'claude -p "Fills from traders I follow are on stdin. …"'
pmwallets mcp                         # the same as MCP tools
pmwallets skill                       # the guide to give your agent (SKILL.md)
```

- `watch --exec` runs the command once per batch, the fills on its stdin as JSON lines (and in `$PMW_BATCH_FILE`);
  fills that arrive while it runs make the next batch. A batch is handed over **at most once**: one whose command
  fails, times out or is interrupted is kept in `pmw-watch/failed/` and never re-run — the agent may already have
  placed an order. Restarts resume from where the stream stopped.
- To trade, give the agent [pmwallets-copytrade](https://github.com/polymarketwallets/polymarket-copy-trading-bot-python):
  `order buy <tokenId> --usdc 5 --key <eventId>` (the same key never orders twice), `market`, `balance`, `positions`.
- MCP config (Claude Desktop, Cursor, Windsurf, WorkBuddy, …):

  ```json
  {
    "mcpServers": {
      "pmwallets": {
        "command": "uvx",
        "args": ["pmwallets", "mcp"],
        "env": { "PMW_API_KEY": "pmw_..." }
      }
    }
  }
  ```
- Platforms without a shell (Dify, Coze, n8n, GPT Actions): import [openapi.json](https://pmwallets.com/openapi.json).
- `HTTPS_PROXY` / `NO_PROXY` are honoured by the command line.
- The same pieces are importable: `Watcher`, `MarketLookup`, `serve_mcp` / `McpTool`, `pmwallets_tools`.

## Trade-history exports

A purchase is access to files: one zstd-compressed CSV per wallet per UTC day, downloadable at once and again later.

```python
order = client.create_export(["0x…"], "2026-09-01", "2026-09-30")
for f in client.export_files(order["id"])["files"]:
    data = client.download_export_file(f["wallet"], f["day"])  # .csv.zst
    # pandas: pd.read_csv(io.BytesIO(data), compression="zstd")
```

`export_file_url(wallet, day)` returns the short-lived link instead; the API key is never sent to the storage host.

## Resources

- [Polymarket smart-money leaderboard](https://pmwallets.com) — profitable Polymarket traders scored from the Polygon chain, with win-rate confidence intervals
- [Polymarket copy trading guide](https://pmwallets.com/copy-trading) — which wallets are worth following and how to get their fills in time
- [How to learn from Polymarket smart money](https://pmwallets.com/learn) — reading a trader's record: confidence intervals, maker vs taker, market specialism
- [PMWallets API documentation](https://pmwallets.com/docs) — WebSocket and webhook fill push, fills replay, trade-history exports
- [Measured fill-push latency](https://pmwallets.com/latency) — block-to-push p50 / p95, published live
- [Ways to follow Polymarket wallets, compared](https://pmwallets.com/compare) — official leaderboard, free trackers, SQL dashboards
- [FAQ](https://pmwallets.com/faq) · [中文站](https://pmwallets.com/zh)

**Guides**

- [PMWallets SDKs](https://pmwallets.com/sdk) — Node.js and Python clients, side by side
- [Polymarket data in Python](https://pmwallets.com/guides/polymarket-api-python) — markets, a wallet's trades, and live fills
- [How to get a Polymarket wallet's trades in real time](https://pmwallets.com/guides/polymarket-real-time-trades-api) — polling the Data API, reading OrderFilled events on Polygon, or a pushed fill stream with replay
- [Polymarket WebSocket API](https://pmwallets.com/guides/polymarket-websocket-api) — what the market and user channels give you, and what they don't
- [Polymarket trade alerts by webhook](https://pmwallets.com/guides/polymarket-trade-alerts-webhook) — verify the HMAC-SHA256 signature, answer fast, deduplicate on eventId
- [How to export a Polymarket wallet's full trade history](https://pmwallets.com/guides/polymarket-trade-history-export) — past the Data API's 10,000-trade cap: every fill from the chain, one CSV per wallet per day
- [Polymarket token IDs, condition IDs and slugs](https://pmwallets.com/guides/polymarket-token-ids) — how they connect
- [How to calculate a Polymarket wallet's PnL](https://pmwallets.com/guides/polymarket-pnl-explained) — and why sites disagree
- [Polymarket leaderboards by category](https://pmwallets.com/leaderboard) — sports, crypto, politics, esports and more, ranked from on-chain PnL
- [Busiest Polymarket markets](https://pmwallets.com/markets) — the 1,000 most-traded markets of the last 30 days and how their traders did
- [All guides](https://pmwallets.com/guides) · [llms.txt](https://pmwallets.com/llms.txt)

## License

MIT
