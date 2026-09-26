from __future__ import annotations

import json
from typing import Any, AsyncIterator, Iterator, Optional
from urllib.parse import quote

import httpx

from .types import Fill, FillCursor, FillsPage, Leaderboard, Subscription

DEFAULT_BASE_URL = "https://api.pmwallets.com"


class PmwError(Exception):
    """A non-2xx answer from the API. `body` is the parsed JSON body when there was one."""

    def __init__(self, status: int, body: Any, message: Optional[str] = None):
        self.status = status
        self.body = body
        super().__init__(message or f"PMWallets API {status}: {body if isinstance(body, str) else json.dumps(body)}")


def _q(v: str) -> str:
    return quote(v, safe="")


class _Base:
    def __init__(self, api_key: str, base_url: str = DEFAULT_BASE_URL, timeout: float = 15.0):
        if not api_key:
            raise ValueError("api_key is required")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    @property
    def ws_url(self) -> str:
        """The WebSocket URL derived from the base URL (https → wss)."""
        return ("ws" + self.base_url[4:] if self.base_url.startswith("http") else self.base_url) + "/v1/ws"

    @property
    def _headers(self) -> dict[str, str]:
        return {"x-api-key": self.api_key, "accept": "application/json"}

    @staticmethod
    def _params(query: Optional[dict[str, Any]]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for k, v in (query or {}).items():
            if v is None:
                continue
            out[k] = ("true" if v else "false") if isinstance(v, bool) else v
        return out

    @staticmethod
    def _parse(res: httpx.Response) -> Any:
        text = res.text
        body: Any = text
        if text:
            try:
                body = json.loads(text)
            except ValueError:
                pass
        if res.status_code < 200 or res.status_code >= 300:
            raise PmwError(res.status_code, body)
        return body if text else None

    @classmethod
    def _location(cls, res: httpx.Response) -> str:
        """The redirect target of a purchased-file request; anything else is raised as PmwError."""
        loc = res.headers.get("location")
        if 300 <= res.status_code < 400 and loc:
            return loc
        cls._parse(res)
        raise PmwError(res.status_code, res.text, "expected a redirect to the file")

    @staticmethod
    def _fills_query(since_block: int, since_log_index: int, limit: int) -> dict[str, Any]:
        return {"sinceBlock": since_block, "sinceLogIndex": since_log_index, "limit": limit}


class Client(_Base):
    """Synchronous REST client for https://api.pmwallets.com. Every method authenticates with the API key."""

    def __init__(self, api_key: str, base_url: str = DEFAULT_BASE_URL, timeout: float = 15.0, http: Optional[httpx.Client] = None):
        super().__init__(api_key, base_url, timeout)
        self._http = http or httpx.Client(timeout=timeout)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def request(self, method: str, path: str, query: Optional[dict[str, Any]] = None, body: Any = None) -> Any:
        res = self._http.request(method, self.base_url + path, params=self._params(query), headers=self._headers,
                                 json=body if body is not None else None)
        return self._parse(res)

    # ── the board
    def leaderboard(self, **query: Any) -> Leaderboard:
        return self.request("GET", "/v1/leaderboard", query)

    def entity(self, entity_id: str, period: Optional[str] = None) -> dict[str, Any]:
        """One entity by handle or address. The full address comes back only for entities you own."""
        return self.request("GET", f"/v1/entities/{_q(entity_id)}", {"period": period})

    def latency(self) -> dict[str, Any]:
        return self.request("GET", "/v1/latency")

    # ── addresses
    def buy_reveal(self, entity_id: str, max_price_cents: int) -> dict[str, Any]:
        """Buy the address behind a handle. `max_price_cents` is a ceiling: the charge never exceeds it."""
        return self.request("POST", "/v1/account/reveals", body={"entityId": entity_id, "maxPriceCents": max_price_cents})

    def reveals(self) -> list[dict[str, Any]]:
        return self.request("GET", "/v1/account/reveals")

    # ── subscriptions
    def subscriptions(self) -> list[Subscription]:
        """Active and paused subscriptions, newest first."""
        return self.request("GET", "/v1/account/subscriptions")

    def subscribe(self, entity_id: str, channels: Optional[list[str]] = None, accept_inactive: Optional[bool] = None) -> Subscription:
        """Starts billing per hour. 402 = balance too low, 409 = dormant entity (resend with accept_inactive)."""
        body: dict[str, Any] = {"channels": channels or ["ws"], "entityId": entity_id}
        if accept_inactive is not None:
            body["acceptInactive"] = accept_inactive
        return self.request("POST", "/v1/account/subscriptions", body=body)

    def cancel_subscription(self, sub_id: str) -> Any:
        return self.request("DELETE", f"/v1/account/subscriptions/{_q(sub_id)}")

    def resume_subscription(self, sub_id: str) -> Subscription:
        """Charges another hour and resumes from the current head (the paused gap is not backfilled)."""
        return self.request("POST", f"/v1/account/subscriptions/{_q(sub_id)}/resume")

    # ── fills
    def fills(self, since_block: int = 0, since_log_index: int = 0, limit: int = 500) -> FillsPage:
        """One page of fills strictly after the cursor, oldest first (all active subscriptions)."""
        return self.request("GET", "/v1/account/fills", self._fills_query(since_block, since_log_index, limit))

    def fills_since(self, cursor: FillCursor, limit: int = 500) -> Iterator[Fill]:
        """Every fill after the cursor, walking pages until the end."""
        at: Optional[FillCursor] = cursor
        while at:
            page = self.fills(at["sinceBlock"], at["sinceLogIndex"], limit)
            yield from page["rows"]
            at = page["next"]

    # ── trade-history exports
    def export_quote(self, entity_ids: list[str], from_: str, to: str) -> dict[str, Any]:
        return self.request("POST", "/v1/account/exports/quote", body={"entityIds": entity_ids, "from": from_, "to": to})

    def create_export(self, entity_ids: list[str], from_: str, to: str) -> dict[str, Any]:
        """Charges the balance; the price is recomputed server-side."""
        return self.request("POST", "/v1/account/exports", body={"entityIds": entity_ids, "from": from_, "to": to})

    def exports(self) -> list[dict[str, Any]]:
        return self.request("GET", "/v1/account/exports")

    def get_export(self, export_id: str) -> dict[str, Any]:
        return self.request("GET", f"/v1/account/exports/{_q(export_id)}")

    def export_files(self, export_id: str) -> dict[str, Any]:
        """The daily files an export grants: one per wallet per UTC day."""
        return self.request("GET", f"/v1/account/exports/{_q(export_id)}/files")

    def export_file_url(self, wallet: str, day: str) -> str:
        """A short-lived (5 minute) link to one purchased daily file. The redirect is read, not followed,
        so the API key is never sent to the storage host."""
        res = self._http.request("GET", self.base_url + f"/v1/account/data/{_q(wallet)}/{_q(day)}", headers=self._headers, follow_redirects=False)
        return self._location(res)

    def download_export_file(self, wallet: str, day: str) -> bytes:
        """One purchased daily file: zstd-compressed CSV bytes (.csv.zst)."""
        res = self._http.get(self.export_file_url(wallet, day), timeout=max(self.timeout, 60.0))
        if res.status_code != 200:
            raise PmwError(res.status_code, res.text)
        return res.content


class AsyncClient(_Base):
    """asyncio twin of Client (the FillStream uses it for replays)."""

    def __init__(self, api_key: str, base_url: str = DEFAULT_BASE_URL, timeout: float = 15.0, http: Optional[httpx.AsyncClient] = None):
        super().__init__(api_key, base_url, timeout)
        self._http = http or httpx.AsyncClient(timeout=timeout)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    async def request(self, method: str, path: str, query: Optional[dict[str, Any]] = None, body: Any = None) -> Any:
        res = await self._http.request(method, self.base_url + path, params=self._params(query), headers=self._headers,
                                       json=body if body is not None else None)
        return self._parse(res)

    async def leaderboard(self, **query: Any) -> Leaderboard:
        return await self.request("GET", "/v1/leaderboard", query)

    async def entity(self, entity_id: str, period: Optional[str] = None) -> dict[str, Any]:
        return await self.request("GET", f"/v1/entities/{_q(entity_id)}", {"period": period})

    async def latency(self) -> dict[str, Any]:
        return await self.request("GET", "/v1/latency")

    async def buy_reveal(self, entity_id: str, max_price_cents: int) -> dict[str, Any]:
        return await self.request("POST", "/v1/account/reveals", body={"entityId": entity_id, "maxPriceCents": max_price_cents})

    async def reveals(self) -> list[dict[str, Any]]:
        return await self.request("GET", "/v1/account/reveals")

    async def subscriptions(self) -> list[Subscription]:
        return await self.request("GET", "/v1/account/subscriptions")

    async def subscribe(self, entity_id: str, channels: Optional[list[str]] = None, accept_inactive: Optional[bool] = None) -> Subscription:
        body: dict[str, Any] = {"channels": channels or ["ws"], "entityId": entity_id}
        if accept_inactive is not None:
            body["acceptInactive"] = accept_inactive
        return await self.request("POST", "/v1/account/subscriptions", body=body)

    async def cancel_subscription(self, sub_id: str) -> Any:
        return await self.request("DELETE", f"/v1/account/subscriptions/{_q(sub_id)}")

    async def resume_subscription(self, sub_id: str) -> Subscription:
        return await self.request("POST", f"/v1/account/subscriptions/{_q(sub_id)}/resume")

    async def fills(self, since_block: int = 0, since_log_index: int = 0, limit: int = 500) -> FillsPage:
        return await self.request("GET", "/v1/account/fills", self._fills_query(since_block, since_log_index, limit))

    async def fills_since(self, cursor: FillCursor, limit: int = 500) -> AsyncIterator[Fill]:
        at: Optional[FillCursor] = cursor
        while at:
            page = await self.fills(at["sinceBlock"], at["sinceLogIndex"], limit)
            for row in page["rows"]:
                yield row
            at = page["next"]

    async def export_quote(self, entity_ids: list[str], from_: str, to: str) -> dict[str, Any]:
        return await self.request("POST", "/v1/account/exports/quote", body={"entityIds": entity_ids, "from": from_, "to": to})

    async def create_export(self, entity_ids: list[str], from_: str, to: str) -> dict[str, Any]:
        return await self.request("POST", "/v1/account/exports", body={"entityIds": entity_ids, "from": from_, "to": to})

    async def exports(self) -> list[dict[str, Any]]:
        return await self.request("GET", "/v1/account/exports")

    async def get_export(self, export_id: str) -> dict[str, Any]:
        return await self.request("GET", f"/v1/account/exports/{_q(export_id)}")

    async def export_files(self, export_id: str) -> dict[str, Any]:
        return await self.request("GET", f"/v1/account/exports/{_q(export_id)}/files")

    async def export_file_url(self, wallet: str, day: str) -> str:
        res = await self._http.request("GET", self.base_url + f"/v1/account/data/{_q(wallet)}/{_q(day)}", headers=self._headers, follow_redirects=False)
        return self._location(res)

    async def download_export_file(self, wallet: str, day: str) -> bytes:
        res = await self._http.get(await self.export_file_url(wallet, day), timeout=max(self.timeout, 60.0))
        if res.status_code != 200:
            raise PmwError(res.status_code, res.text)
        return res.content
