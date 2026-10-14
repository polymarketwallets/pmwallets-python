"""What a fill was about, from Polymarket's public CLOB API: a fill carries an outcome token id, and an agent (or a
person) reading it needs the question and which side of it was bought. Best effort: any failure is `None`, never an
exception — a fill is still worth delivering without its market."""

from __future__ import annotations

import re
import time
from typing import Any, Callable, Optional, TypedDict

import httpx


class MarketInfo(TypedDict):
    conditionId: str
    question: str
    outcome: Optional[str]  # the outcome this token pays on, e.g. "Yes" / "No" / a candidate's name; None when the CLOB does not list it
    slug: Optional[str]
    endDate: Optional[str]
    closed: Optional[bool]


# a Polymarket outcome id: a canonical decimal uint256 — anything else is never put into a URL
_TOKEN_ID = re.compile(r"0|[1-9][0-9]{0,77}")
_CONDITION_ID = re.compile(r"0x[0-9a-fA-F]{64}")
_MICRO = re.compile(r"-?[0-9]+")


class MarketLookup:
    """Token id → market, cached: a found market for `ttl` seconds (default 10 min), a failed lookup for 30 s.

    `http` is the httpx client to use (tests pass one with a MockTransport); by default one is created that reads
    HTTPS_PROXY / NO_PROXY from the environment (httpx's trust_env)."""

    def __init__(self, clob_url: str = "https://clob.polymarket.com", http: Optional[httpx.AsyncClient] = None,
                 timeout: float = 5.0, ttl: float = 600.0, max_entries: int = 5_000,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.clob_url = clob_url.rstrip("/")
        self._http = http or httpx.AsyncClient(timeout=timeout)
        self.ttl = ttl
        self.max_entries = max_entries
        self._clock = clock
        # insertion-ordered: the first key is the oldest, evicted first (re-setting a key keeps its place, as a JS Map does)
        self._cache: dict[str, tuple[float, Optional[MarketInfo]]] = {}

    async def aclose(self) -> None:
        await self._http.aclose()

    async def by_token(self, token_id: str) -> Optional[MarketInfo]:
        if not isinstance(token_id, str) or not _TOKEN_ID.fullmatch(token_id):
            return None
        hit = self._cache.get(token_id)
        if hit and self._clock() - hit[0] < (self.ttl if hit[1] else 30.0):
            return hit[1]
        info: Optional[MarketInfo] = None
        try:
            by_token = await self._get_json(f"/markets-by-token/{token_id}")
            cid = by_token.get("condition_id") if isinstance(by_token, dict) else None
            if isinstance(cid, str) and _CONDITION_ID.fullmatch(cid):
                m = await self._get_json(f"/markets/{cid}")
                if isinstance(m, dict) and isinstance(m.get("question"), str):
                    tokens = m.get("tokens")
                    tok = next((t for t in tokens if isinstance(t, dict) and _str(t.get("token_id")) == token_id), None) if isinstance(tokens, list) else None
                    outcome = tok.get("outcome") if tok else None
                    info = {
                        "conditionId": cid,
                        "question": m["question"],
                        "outcome": outcome if isinstance(outcome, str) else None,
                        "slug": _nonempty(m.get("market_slug")),
                        "endDate": _nonempty(m.get("end_date_iso")),
                        "closed": m["closed"] if isinstance(m.get("closed"), bool) else None,
                    }
        except Exception:  # unknown: retried after 30 s
            pass
        if len(self._cache) >= self.max_entries:
            del self._cache[next(iter(self._cache))]
        self._cache[token_id] = (self._clock(), info)
        return info

    async def _get_json(self, path: str) -> Any:
        res = await self._http.get(self.clob_url + path)
        if not res.is_success:
            raise RuntimeError(f"CLOB {res.status_code}")
        return res.json()


def _str(v: Any) -> str:
    return "" if v is None else str(v)


def _nonempty(v: Any) -> Optional[str]:
    return v if isinstance(v, str) and v else None


def micro_to_decimal(micro: Any) -> Optional[str]:
    """"1824000000" (1e-6 units) → "1824.000000"; anything that is not a whole number comes back as None."""
    if not isinstance(micro, str) or not _MICRO.fullmatch(micro):
        return None
    neg = micro.startswith("-")
    digits = (micro[1:] if neg else micro).rjust(7, "0")
    return f"{'-' if neg else ''}{digits[:-6].lstrip('0') or '0'}.{digits[-6:]}"
