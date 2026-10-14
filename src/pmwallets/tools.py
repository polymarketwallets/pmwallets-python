"""The PMWallets tools an agent uses — served by `pmwallets mcp`, and run directly by the other CLI commands."""

from __future__ import annotations

import asyncio
import json
import math
import os
import weakref
from pathlib import Path
from typing import Any, Optional, Union

from .client import AsyncClient
from .market import MarketLookup, micro_to_decimal
from .mcp import McpTool
from .types import Fill, FillCursor


async def describe_fill(fill: Fill, markets: Optional[MarketLookup]) -> dict[str, Any]:
    """A fill with its decimal amounts and its market: what an agent can read without knowing the 1e-6 units."""
    market = await markets.by_token(fill["tokenId"]) if markets else None
    return {
        **fill,
        "usdcAmount": micro_to_decimal(fill.get("usdc")),
        "sharesAmount": micro_to_decimal(fill.get("shares")),
        "feeAmount": micro_to_decimal(fill.get("fee")),
        "market": market or frame_market(fill),
    }


def frame_market(fill: Any) -> Optional[dict[str, Any]]:
    """The market as the fill frame itself names it (title + condition), when Polymarket could not be asked."""
    title, cid = fill.get("marketTitle"), fill.get("conditionId")
    if title and cid:
        return {"conditionId": cid, "question": title, "outcome": None, "slug": None, "endDate": None, "closed": None}
    return None


class CursorFile:
    """Where `new_fills` left off, in a small JSON file (default ~/.pmwallets/mcp-cursor.json, or PMW_MCP_STATE).
    Advanced as soon as the fills are returned: each fill is handed out once — to ONE consumer per file. Calls in
    one process are serialized; two processes on one file (the CLI and an MCP server, two agents) are not, and may
    both receive a page or move the cursor back: give each its own PMW_MCP_STATE. Same file and format as the Node
    package."""

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)

    def load(self) -> Optional[FillCursor]:
        try:
            c = json.loads(self.path.read_text("utf8"))
        except (OSError, ValueError):
            return None
        if not isinstance(c, dict):
            return None
        b, li = _whole(c.get("sinceBlock")), _whole(c.get("sinceLogIndex"))
        return {"sinceBlock": b, "sinceLogIndex": li} if b is not None and li is not None else None

    def save(self, c: FillCursor) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(f"{self.path.name}.{os.getpid()}.tmp")
        tmp.write_text(json.dumps(c, separators=(",", ":")))
        os.replace(tmp, self.path)


def default_cursor_path() -> Path:
    return Path(os.environ.get("PMW_MCP_STATE") or Path.home() / ".pmwallets" / "mcp-cursor.json")


# blocks before the head a first `new_fills` starts from: about one hour of Polygon
FIRST_LOOKBACK_BLOCKS = 1_800


def _after(a: FillCursor, b: FillCursor) -> bool:
    return a["sinceBlock"] > b["sinceBlock"] or (a["sinceBlock"] == b["sinceBlock"] and a["sinceLogIndex"] > b["sinceLogIndex"])


# one new_fills at a time per cursor file in this process: two concurrent calls would read the same position.
# Per event loop, as an asyncio.Lock belongs to the loop it first waited on.
_cursor_locks: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[str, asyncio.Lock]]" = weakref.WeakKeyDictionary()


async def new_fills(client: AsyncClient, cursor_file: CursorFile, markets: Optional[MarketLookup], limit: Any = 100) -> dict[str, Any]:
    """Every fill after the saved cursor up to `limit`, and where the next call should start."""
    locks = _cursor_locks.setdefault(asyncio.get_running_loop(), {})
    # one key per file, however its path is written (normalised and absolute, as Node's path.resolve)
    lock = locks.setdefault(os.path.abspath(cursor_file.path), asyncio.Lock())  # asyncio.Lock wakes waiters in order
    async with lock:
        return await _new_fills_now(client, cursor_file, markets, limit)


async def _new_fills_now(client: AsyncClient, cursor_file: CursorFile, markets: Optional[MarketLookup], limit: Any) -> dict[str, Any]:
    cursor = cursor_file.load()
    anchored = False
    if cursor is None:
        r = await client.latency()
        head = _whole((r.get("head") or {}).get("block") if isinstance(r, dict) else None)
        if head is None or head <= 0:
            raise RuntimeError("could not read the chain head")
        cursor = {"sinceBlock": max(1, head - FIRST_LOOKBACK_BLOCKS), "sinceLogIndex": 0}
        anchored = True
    page = await client.fills(cursor["sinceBlock"], cursor["sinceLogIndex"], _limit(limit))
    rows = page["rows"]
    nxt: FillCursor = {"sinceBlock": rows[-1]["block"], "sinceLogIndex": rows[-1]["logIndex"]} if rows else cursor
    # the answer is complete (markets looked up) before the cursor moves: an interrupted call loses nothing
    out: dict[str, Any] = {
        "fills": list(await asyncio.gather(*(describe_fill(f, markets) for f in rows))),
        "more": page.get("next") is not None,
        "subscriptions": page.get("subscriptions"),
    }
    if anchored:
        out["note"] = "first call: started about one hour back; later calls continue from here"
    # never move back over what is on disk (this narrows, but does not close, the window for two processes on one file)
    on_disk = cursor_file.load()
    if on_disk is None or _after(nxt, on_disk):
        cursor_file.save(nxt)
    return out


def _str(description: str) -> dict[str, Any]:
    return {"type": "string", "description": description}


def _num(description: str) -> dict[str, Any]:
    return {"type": "number", "description": description}


def _obj(properties: dict[str, Any], required: Optional[list[str]] = None) -> dict[str, Any]:
    return {"type": "object", "properties": properties, "required": required or [], "additionalProperties": False}


BOARD_FILTERS: dict[str, Any] = {
    "period": _str('statistics window: "30d" (default) or "7d"'),
    "sort": {"type": "string", "enum": ["realizedPnl", "roiOnBuys", "eligible", "winsLosses", "nFills", "conditions", "grossBuyUsdc", "unrealizedPnl", "winLo", "fillsPerDay", "lastFill"], "description": "column to sort by (default realizedPnl)"},
    "dir": {"type": "string", "enum": ["asc", "desc"]},
    "limit": _num("rows, at most 200 (rows count against a daily allowance of 20,000)"),
    "offset": _num("rows to skip (signed-in accounts; anonymous callers see a fixed sample)"),
    "minPnl": _num("floor on realized PnL in USD"),
    "minRoi": _num("floor on ROI on buys (0.1 = 10%)"),
    "minWinLo": _num("floor on the lower bound of the 95% win-rate interval (0–1): a win rate that is not luck"),
    "minEligible": _num("floor on markets that count toward the win rate"),
    "minWinsLosses": _num("floor on markets won or lost"),
    "minFills": _num("floor on number of fills"),
    "minVolume": _num("floor on traded volume in USD"),
    "style": {"type": "string", "enum": ["maker", "taker", "two-sided"]},
    "category": _str('market category, e.g. "politics", "sports", "crypto", "crypto-updown", "esports"'),
    "minCategoryShare": _num("with category: at least this share (0–1] of the trader's volume in it"),
    "status": {"type": "string", "enum": ["active", "quiet", "dormant"]},
    "maxWallets": _num("at most this many wallets in the entity"),
    "bands": _str("unlock price bands to include, comma-separated: 0 = free … 5"),
}

MCP_INSTRUCTIONS = """PMWallets: the Polymarket smart-money leaderboard (traders scored from the Polygon chain) and the live fills of the traders this account follows.
- leaderboard / entity: find traders. entityId is a 12-character handle until the address is unlocked.
- subscribe starts hourly billing on the account (unless the plan covers it); cancel_subscription stops it.
- new_fills returns the fills of followed traders since the previous call (each fill once). usdc/shares/fee are 1e-6 integer units; usdcAmount/sharesAmount are the same as decimals. market tells the question and outcome.
- Placing orders on Polymarket is not part of this server: use the pmwallets-copytrade tools (order_buy / order_sell) for that."""

_DEFAULT: Any = object()


def pmwallets_tools(client: Optional[AsyncClient], *, markets: Optional[MarketLookup] = _DEFAULT,
                    cursor: Optional[CursorFile] = None) -> list[McpTool]:
    """The PMWallets tools for an MCP host. `client` is None when no API key is set: every tool then says how to set
    one. `markets=None` turns market lookups off (default: a MarketLookup)."""
    mk: Optional[MarketLookup] = MarketLookup() if markets is _DEFAULT else markets
    cur = cursor or CursorFile(default_cursor_path())

    def need() -> AsyncClient:
        if client is None:
            raise RuntimeError("PMW_API_KEY is not set: create a key on https://pmwallets.com/keys and put it in the MCP server's env")
        return client

    def ident(v: Any, name: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError(f"{name} is required")
        return v.strip()

    async def leaderboard(a: dict) -> Any:
        return await need().leaderboard(**a)

    async def entity(a: dict) -> Any:
        return await need().entity(ident(a.get("id"), "id"), str(a["period"]) if a.get("period") else None)

    async def subscriptions(a: dict) -> Any:
        return await need().subscriptions()

    async def subscribe(a: dict) -> Any:
        return await need().subscribe(ident(a.get("entityId"), "entityId"), ["ws"], True if a.get("acceptInactive") is True else None)

    async def cancel(a: dict) -> Any:
        return await need().cancel_subscription(ident(a.get("id"), "id"))

    async def new(a: dict) -> Any:
        return await new_fills(need(), cur, mk, 100 if a.get("limit") is None else a["limit"])

    async def fills(a: dict) -> Any:
        c = need()
        page = await c.fills(_whole_arg(a.get("sinceBlock"), "sinceBlock", None), _whole_arg(a.get("sinceLogIndex"), "sinceLogIndex", 0),
                             min(max(1, _whole_arg(a.get("limit"), "limit", 100)), 500))
        return {**page, "rows": list(await asyncio.gather(*(describe_fill(f, mk) for f in page["rows"])))}

    async def market(a: dict) -> Any:
        return (await (mk or MarketLookup()).by_token(ident(a.get("tokenId"), "tokenId"))) or {"error": "not found"}

    async def account(a: dict) -> Any:
        c = need()
        plan, billing = await asyncio.gather(c.plan(), c.billing())
        return {"plan": plan, "billing": billing}

    return [
        McpTool("leaderboard",
                "Polymarket traders ranked by realized PnL and other stats over a window, with filters. Each row is an entity (one trader, possibly several wallets).",
                _obj(BOARD_FILTERS), leaderboard),
        McpTool("entity", "One trader's statistics by handle or address (full address only once unlocked).",
                _obj({"id": _str("12-character handle or 0x address"), "period": _str('"30d" or "7d"')}, ["id"]), entity),
        McpTool("subscriptions", "The traders this account follows (active and paused subscriptions).", _obj({}), subscriptions),
        McpTool("subscribe",
                "Follow a trader: their fills start arriving from now on. BILLS THE ACCOUNT per hour (unless the plan covers it). 402 = balance too low; 409 = no fill for 7 days (retry with acceptInactive).",
                _obj({"entityId": _str("handle or 0x address"), "acceptInactive": {"type": "boolean"}}, ["entityId"]), subscribe),
        McpTool("cancel_subscription", "Stop following (and stop paying for) a subscription, by its id from `subscriptions`.",
                _obj({"id": _str("subscription id")}, ["id"]), cancel),
        McpTool("new_fills",
                "Fills of the followed traders since the previous call of this tool (first call: the last hour). Each fill is returned once to this server; the CLI `pmwallets new-fills` shares the position file, so do not poll with both at once (or give each its own PMW_MCP_STATE). Call again while `more` is true.",
                _obj({"limit": _num("at most this many fills (default 100, max 500)")}), new),
        McpTool("fills",
                "Fills of the followed traders strictly after a ledger position (block, logIndex), oldest first. Does not move the new_fills cursor.",
                _obj({"sinceBlock": _num("block"), "sinceLogIndex": _num("log index within the block (default 0)"), "limit": _num("max 500")}, ["sinceBlock"]), fills),
        McpTool("market", "The Polymarket market and outcome an outcome token id belongs to (question, outcome name, slug, end date).",
                _obj({"tokenId": _str("outcome token id (decimal)")}, ["tokenId"]), market),
        McpTool("account", "The account's plan (allowances used) and balance.", _obj({}), account),
    ]


def _whole(v: Any) -> Optional[int]:
    """An integer from JSON (4000 or 4000.0), else None."""
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v.is_integer():
        return int(v)
    return None


def _whole_arg(v: Any, name: str, default: Optional[int]) -> int:
    """A whole number ≥ 0 from an agent's arguments (a number or a numeric string); missing → `default`."""
    if v is None and default is not None:
        return default
    try:
        f = float(v) if not isinstance(v, bool) else math.nan
    except (TypeError, ValueError):
        f = math.nan
    if not math.isfinite(f) or not f.is_integer() or f < 0:
        raise ValueError(f"{name} must be a whole number ≥ 0")
    return int(f)


def _limit(v: Any) -> int:
    """Clamped to 1..500, as the API allows; anything that is not a finite number is the default 100."""
    try:
        f = float(v) if not isinstance(v, bool) else math.nan
    except (TypeError, ValueError):
        f = math.nan
    return int(min(max(1.0, math.floor(f)), 500.0)) if math.isfinite(f) else 100
