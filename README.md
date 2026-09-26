# pmwallets — Python SDK for PMWallets

[![PyPI](https://img.shields.io/pypi/v/pmwallets.svg)](https://pypi.org/project/pmwallets/) [![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

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
    pmw.subscribe(board["rows"][0]["entityId"], channels=["ws"])   # billed per entity per hour

# every fill of every entity you follow
async def main():
    async with AsyncClient(api_key="pmw_...") as pmw:
        stream = FillStream(
            client=pmw,
            store=FileStateStore("stream.json"),     # a restart resumes exactly where it stopped
            on_fill=lambda fill, meta: print(meta.source, fill["side"], fill["price"], fill["tokenId"]),
        )
        await stream.run()

asyncio.run(main())
```

## What is in it

| | |
|---|---|
| `Client` / `AsyncClient` | leaderboard, entities, address unlocks, subscriptions, fills replay (`fills`, `fills_since`), trade-history exports |
| `FillStream` | asyncio WebSocket with reconnect and keep-alive; detects missed frames by `session`/`seq` and replays them from the last fill delivered; de-duplicates by `eventId`; anchors behind the chain head on first start; persisted cursor |
| `verify_webhook(raw_body, signature, secret)` | checks `x-pmw-signature` (HMAC-SHA256 of the raw body) |

One stream per account. An API-key connection takes priority over the pmwallets.com feed page (the page never takes the stream from it); between two API connections the newest wins, so run one consumer per account. `HTTPS_PROXY` is honoured.

PMWallets' servers are in the United Kingdom. A consumer that also trades on Polymarket should run from Ireland (AWS eu-west-1): Polymarket does not accept API orders from the UK, the US and several EU countries.

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

## License

MIT
