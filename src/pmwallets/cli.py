"""`pmwallets` — PMWallets from the command line, for people and AI agents. Every command prints JSON.

Proxies: HTTPS_PROXY / HTTP_PROXY / NO_PROXY need no code here. httpx (the REST client and the market lookups)
reads them itself (trust_env, on by default). The WebSocket of `watch` goes through `websockets` >= 15, whose
connect(proxy=True) default asks urllib.request.getproxies() for the proxy (wss → https_proxy, also socks) and skips
it when urllib.request.proxy_bypass() matches the host against NO_PROXY (hosts, domain suffixes with or without the
leading dot, `*`) — the same rules as the Node package's noProxy(). An http:// proxy is used with CONNECT; a socks
one needs the python-socks package. Checked against websockets 15–17 (tests/test_agent.py::test_websocket_proxy_*).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import os
import signal
import sys
import time
from importlib import resources
from typing import Any, Optional, Union

from .client import DEFAULT_BASE_URL, AsyncClient
from .market import MarketLookup
from .mcp import serve_mcp
from .tools import MCP_INSTRUCTIONS, pmwallets_tools
from .version import VERSION
from .watch import Watcher, iso

HELP = f"""pmwallets {VERSION} — PMWallets from the command line, for people and AI agents

Set PMW_API_KEY (create one on https://pmwallets.com/keys). Every command prints JSON.

  pmwallets watch [--exec "<command>"] [--dir ./pmw-watch] [--batch-sec 0] [--max-batch 50]
                  [--exec-timeout-sec 600] [--takeover] [--no-market]
      follow the fills of the traders you subscribe to, live. Without --exec: one JSON line per fill on stdout.
      With --exec: the command runs once per batch, the fills on its stdin as JSON lines (and in $PMW_BATCH_FILE);
      fills arriving while it runs make the next batch. A batch is handed over at most once.
  pmwallets leaderboard [--period 30d] [--sort realizedPnl] [--limit 20] [--minWinLo 0.55] [--category politics] …
  pmwallets entity <handle|address> [--period 30d]
  pmwallets subscriptions
  pmwallets subscribe <handle|address> [--accept-inactive]     starts hourly billing
  pmwallets unsubscribe <subscription-id>
  pmwallets new-fills [--limit 100]        fills since the previous call; the position is kept in ~/.pmwallets/mcp-cursor.json
                                           (PMW_MCP_STATE), shared with the MCP tool new_fills — one consumer per file
  pmwallets fills --since-block <n> [--since-log-index <n>] [--limit 100]
  pmwallets market <tokenId>               the Polymarket question and outcome of a token
  pmwallets account                        plan and balance
  pmwallets signals [--source mine] [--category sports] [--q fed]   smart-money signals (Pro and Max)
  pmwallets radar [--minUsd 25000] [--side BUY] [--smart]           large fills, whole market (Pro and Max)
  pmwallets mcp                            MCP server on stdio (tools: leaderboard, entity, subscribe, new_fills, …)
  pmwallets skill                          print the guide for AI agents (SKILL.md)

Placing orders: pmwallets-copytrade (pip install pmwallets-copytrade), commands order / positions / balance.
Docs: https://pmwallets.com/docs"""

Flags = dict[str, Union[str, bool]]


def parse_args(argv: list[str], booleans: list[str]) -> tuple[list[str], Flags]:
    """`--name value`, `--name=value` and bare `--flag`; everything else is positional."""
    pos: list[str] = []
    flags: Flags = {}
    i = 0
    while i < len(argv):
        a = argv[i]
        if not a.startswith("--"):
            pos.append(a)
        elif "=" in a:
            name, _, value = a[2:].partition("=")
            flags[name] = value
        else:
            name = a[2:]
            if name in booleans or i + 1 >= len(argv) or argv[i + 1].startswith("--"):
                flags[name] = True
            else:
                i += 1
                flags[name] = argv[i]
        i += 1
    return pos, flags


def positive_number(v: Union[str, bool, None], name: str, fallback: float) -> Union[int, float]:
    if v is None:
        return fallback
    try:
        n = float(v) if not isinstance(v, bool) else math.nan
    except ValueError:
        n = math.nan
    if not math.isfinite(n) or n < 0:
        raise ValueError(f"--{name} must be a number ≥ 0")
    return int(n) if n.is_integer() else n


def _client() -> Optional[AsyncClient]:
    key = os.environ.get("PMW_API_KEY")
    return AsyncClient(key, base_url=os.environ.get("PMW_BASE_URL") or DEFAULT_BASE_URL) if key else None


def _print(v: Any) -> None:
    print(json.dumps(v, indent=2, ensure_ascii=False))


def _skill() -> str:
    return resources.files("pmwallets").joinpath("SKILL.md").read_text("utf-8")


async def _main(argv: list[str]) -> int:
    cmd = argv[0] if argv else None
    pos, flags = parse_args(argv[1:], ["takeover", "no-market", "accept-inactive"])
    if cmd in ("--version", "version"):
        print(VERSION)
        return 0
    if cmd == "skill":
        sys.stdout.write(_skill())
        return 0
    client = _client()
    markets: Optional[MarketLookup] = None
    try:
        if cmd == "mcp":
            # stdout belongs to the protocol: nothing else may be printed there
            markets = MarketLookup()
            await serve_mcp(name="pmwallets", version=VERSION, tools=pmwallets_tools(client, markets=markets), instructions=MCP_INSTRUCTIONS)
            return 0
        if cmd == "watch":
            if flags.get("exec") is True:
                raise ValueError("--exec needs a command, e.g. --exec \"claude -p '…'\"")
            if client is None:
                raise ValueError("set PMW_API_KEY (create a key on https://pmwallets.com/keys)")
            markets = None if flags.get("no-market") is True else MarketLookup()
            d, ex = flags.get("dir"), flags.get("exec")
            watcher = Watcher(
                client,
                d if isinstance(d, str) else "./pmw-watch",
                exec=ex if isinstance(ex, str) else None,
                batch_sec=positive_number(flags.get("batch-sec"), "batch-sec", 0),
                max_batch=max(1, math.floor(positive_number(flags.get("max-batch"), "max-batch", 50))),
                exec_timeout_sec=positive_number(flags.get("exec-timeout-sec"), "exec-timeout-sec", 600),
                takeover=flags.get("takeover") is True,
                markets=markets,
                log=_log,
            )
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                # not on Windows: there Ctrl-C cancels this task, and the finally below stops the watcher
                with contextlib.suppress(NotImplementedError, RuntimeError):
                    loop.add_signal_handler(sig, lambda: watcher._spawn(watcher.stop(0)))
            await watcher.start()
            try:
                return await watcher.wait()
            finally:
                await watcher.stop(0)

        markets = MarketLookup()
        tools = {t.name: t for t in pmwallets_tools(client, markets=markets)}

        async def call(name: str, args: dict[str, Any]) -> int:
            _print(await tools[name].handler(args))
            return 0

        if cmd == "leaderboard":
            return await call("leaderboard", {k: v for k, v in flags.items() if v is not True})
        if cmd in ("signals", "radar"):
            return await call(cmd, {k: (float(v) if k == "minUsd" else v) for k, v in flags.items()})
        if cmd == "entity":
            p = flags.get("period")
            return await call("entity", {"id": pos[0] if pos else None, **({"period": p} if isinstance(p, str) else {})})
        if cmd == "subscriptions":
            return await call("subscriptions", {})
        if cmd == "subscribe":
            return await call("subscribe", {"entityId": pos[0] if pos else None, "acceptInactive": flags.get("accept-inactive") is True})
        if cmd == "unsubscribe":
            return await call("cancel_subscription", {"id": pos[0] if pos else None})
        if cmd == "new-fills":
            return await call("new_fills", {"limit": positive_number(flags.get("limit"), "limit", 100)})
        if cmd == "fills":
            if flags.get("since-block") is None:
                raise ValueError("--since-block is required (or use new-fills)")
            return await call("fills", {
                "sinceBlock": positive_number(flags.get("since-block"), "since-block", 0),
                "sinceLogIndex": positive_number(flags.get("since-log-index"), "since-log-index", 0),
                "limit": positive_number(flags.get("limit"), "limit", 100),
            })
        if cmd == "market":
            return await call("market", {"tokenId": pos[0] if pos else None})
        if cmd == "account":
            return await call("account", {})
        print(HELP)
        return 1 if cmd and cmd not in ("help", "--help") else 0
    finally:
        if markets is not None:
            await markets.aclose()
        if client is not None:
            await client.aclose()


def _log(e: dict[str, Any]) -> None:
    # stderr: stdout carries the fills
    sys.stderr.write(json.dumps({"at": iso(time.time()), **e}, separators=(",", ":"), ensure_ascii=False, default=str) + "\n")
    sys.stderr.flush()


def main(argv: Optional[list[str]] = None) -> None:
    # JSON goes out as UTF-8 whatever the console's code page is (market questions are not all ASCII)
    for s in (sys.stdin, sys.stdout, sys.stderr):
        with contextlib.suppress(AttributeError, ValueError, OSError):
            s.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    try:
        code = asyncio.run(_main(sys.argv[1:] if argv is None else argv))
    except KeyboardInterrupt:
        code = 130
    except Exception as e:
        status = getattr(e, "status", None)
        err = {"error": f"HTTP {status}", "body": getattr(e, "body", None)} if status else {"error": str(e) or type(e).__name__}
        sys.stderr.write(json.dumps(err, separators=(",", ":"), ensure_ascii=False) + "\n")
        code = 1
    sys.exit(code)


if __name__ == "__main__":
    main()
