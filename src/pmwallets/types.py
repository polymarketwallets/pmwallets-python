from __future__ import annotations

from typing import Any, Literal, Optional, TypedDict


class Fill(TypedDict):
    """One fill of an entity you subscribe to, exactly as the WebSocket frame and GET /v1/account/fills carry it."""

    eventId: str  # chain:block:blockHash:txHash:logIndex — deduplicate on it
    chain: int
    entityId: str  # the subscribed entity (0x address)
    wallet: str  # the address inside the entity that traded
    ts: str  # block time, UTC, "YYYY-MM-DD HH:MM:SS"
    block: int
    blockHash: str
    txHash: str
    logIndex: int
    exchange: str
    side: Literal["BUY", "SELL"]
    role: Literal["maker", "taker"]
    tokenId: str  # Polymarket outcome token id (uint256, decimal string)
    price: str  # decimal string, e.g. "0.570000"
    shares: str  # integer string, 1e-6 share units
    usdc: str  # integer string, 1e-6 USDC units
    fee: str  # integer string, 1e-6 USDC units


class FillCursor(TypedDict):
    sinceBlock: int
    sinceLogIndex: int


class FillsPage(TypedDict):
    rows: list[Fill]
    next: Optional[FillCursor]
    subscriptions: int


Subscription = dict[str, Any]
Leaderboard = dict[str, Any]
