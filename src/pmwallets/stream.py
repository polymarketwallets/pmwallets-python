from __future__ import annotations

import asyncio
import contextlib
import json
import math
import os
from collections import OrderedDict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, AsyncContextManager, Awaitable, Callable, Optional, Protocol, Union

from .client import AsyncClient
from .types import Fill


@dataclass
class StreamState:
    """The WebSocket numbering last accepted and the ledger position of the last delivered fill.
    Persist it (FileStateStore) and a restart resumes exactly where it stopped."""

    session: Optional[str] = None
    seq: int = 0
    block: int = 0
    logIndex: int = 0


class StateStore(Protocol):
    async def load(self) -> Optional[StreamState]: ...
    async def save(self, state: StreamState) -> None: ...


class MemoryStateStore:
    def __init__(self) -> None:
        self._state: Optional[StreamState] = None

    async def load(self) -> Optional[StreamState]:
        return StreamState(**asdict(self._state)) if self._state else None

    async def save(self, state: StreamState) -> None:
        self._state = StreamState(**asdict(state))


class FileStateStore:
    """JSON file, replaced atomically (temp file + os.replace) so a crash never leaves half a file.
    Same format as the Node SDK's FileStateStore — the two are interchangeable."""

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)

    async def load(self) -> Optional[StreamState]:
        try:
            s = json.loads(self.path.read_text("utf8"))
        except FileNotFoundError:
            return None
        return StreamState(session=s.get("session"), seq=s.get("seq", 0), block=s.get("block", 0), logIndex=s.get("logIndex", 0))

    async def save(self, state: StreamState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".tmp")
        tmp.write_text(json.dumps(asdict(state), separators=(",", ":")))
        os.replace(tmp, self.path)


@dataclass(frozen=True)
class FillMeta:
    source: str  # "ws" | "replay"


class UpgradeRefused(Exception):
    """The server answered the WebSocket upgrade with an HTTP status instead of 101."""

    def __init__(self, status_code: int):
        super().__init__(f"WebSocket upgrade refused with {status_code}")
        self.status_code = status_code


class SocketLike(Protocol):
    close_code: Optional[int]
    close_reason: Optional[str]

    def __aiter__(self) -> Any: ...
    async def close(self, code: int = 1000, reason: str = "") -> None: ...


Connector = Callable[[str, dict[str, str]], AsyncContextManager[SocketLike]]
OnFill = Callable[[Fill, FillMeta], Union[None, Awaitable[None]]]
OnEvent = Callable[[dict[str, Any]], None]


def websockets_connector(ping_interval: float = 20.0, **ws_options: Any) -> Connector:
    """The real transport: `websockets` asyncio client. websockets>=15 picks up HTTPS_PROXY by itself."""
    from websockets.asyncio.client import connect
    from websockets.exceptions import InvalidStatus

    @contextlib.asynccontextmanager
    async def _connect(url: str, headers: dict[str, str]):
        try:
            cm = connect(url, additional_headers=headers, ping_interval=ping_interval, ping_timeout=ping_interval, **ws_options)
            ws = await cm.__aenter__()
        except InvalidStatus as e:
            raise UpgradeRefused(e.response.status_code) from e
        try:
            yield ws
        finally:
            await cm.__aexit__(None, None, None)

    return _connect


class FillStream:
    """The fills of every entity your account subscribes to, delivered exactly once and in order.

    The WebSocket is best effort: a frame dropped for a slow consumer, or everything sent while you were
    reconnecting, is not resent. Every frame carries `session` + a consecutive `seq`, so a skip is
    detectable — and on any skip or new session this stream pulls the gap from GET /v1/account/fills
    (keyset-paged from the last delivered fill) before it moves on. Duplicates are dropped by eventId.

    `on_fill(fill, meta)` is called once per fill, in ledger order, never concurrently. A fill counts as
    delivered only after it returns: if it raises, the connection is dropped and the fill is offered again
    after the reconnect's replay — make it idempotent on eventId, or never raise.
    """

    def __init__(
        self,
        client: AsyncClient,
        on_fill: OnFill,
        on_event: Optional[OnEvent] = None,
        store: Optional[StateStore] = None,
        ping_interval: float = 20.0,
        min_backoff: float = 1.0,
        max_backoff: float = 30.0,
        seen_capacity: int = 10_000,
        connector: Optional[Connector] = None,
        ws_options: Optional[dict[str, Any]] = None,
        replay_without_cursor: bool = False,
        anchor_lag_blocks: int = 200,
    ) -> None:
        self.client = client
        self.on_fill = on_fill
        self.on_event = on_event
        self.store: StateStore = store or MemoryStateStore()
        self.min_backoff = min_backoff
        self.max_backoff = max_backoff
        self.seen_capacity = seen_capacity
        # Where to start when there is no saved position. Default False: at the first connection the cursor
        # is anchored at the current chain head, so a disconnect before the first fill is still replayed —
        # but the history from before you started is not. True: start from zero and receive every fill
        # since each subscription began.
        self.replay_without_cursor = replay_without_cursor
        # How far behind the chain head to anchor (~5 min). The head can run ahead of the fills already indexed for
        # your account; anchoring exactly at it could exclude a fill mined earlier but not yet pushed. The extra
        # blocks are replayed at most once more and dropped by eventId.
        self.anchor_lag_blocks = anchor_lag_blocks
        self.connector = connector or websockets_connector(ping_interval, **(ws_options or {}))
        self.state = StreamState()
        self._seen: "OrderedDict[str, bool]" = OrderedDict()
        self._running = False
        self._task: Optional[asyncio.Task[None]] = None
        self._socket: Optional[SocketLike] = None
        self._wake = asyncio.Event()

    @property
    def position(self) -> StreamState:
        return StreamState(**asdict(self.state))

    async def start(self) -> None:
        """Loads the saved state and starts connecting. Returns immediately; call stop() to end."""
        if self._running:
            return
        self.state = (await self.store.load()) or self.state
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """Closes the socket and waits for the fill being handled (if any) to finish."""
        self._running = False
        self._wake.set()
        if self._socket is not None:
            with contextlib.suppress(Exception):
                await self._socket.close(1000)
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def run(self) -> None:
        """start() and block until the stream stops."""
        await self.start()
        await self.wait()

    async def wait(self) -> None:
        """Block until the stream stops (stop() or a fatal error)."""
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    def _emit(self, event: dict[str, Any]) -> None:
        if self.on_event is None:
            return
        with contextlib.suppress(Exception):  # a listener must not break the stream
            self.on_event(event)

    async def _loop(self) -> None:
        backoff = self.min_backoff
        while self._running:
            healthy = await self._connect_once()
            if not self._running:
                break
            backoff = self.min_backoff if healthy else min(backoff * 2, self.max_backoff)
            self._wake.clear()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._wake.wait(), backoff)

    async def _connect_once(self) -> bool:
        """One connection's life; True when it got as far as a hello and did not fail."""
        url = self.client.ws_url
        self._emit({"type": "connecting", "url": url})
        greeted = False
        failed = False
        code, reason = 1006, ""
        try:
            async with self.connector(url, {"x-api-key": self.client.api_key}) as ws:
                self._socket = ws
                self._emit({"type": "connected"})
                try:
                    async for raw in ws:
                        try:
                            frame = json.loads(raw)
                        except (ValueError, TypeError):
                            continue
                        if not isinstance(frame, dict):
                            continue
                        try:
                            await self._handle_frame(frame)
                        except Exception as e:  # leave the position where it is; the next hello replays from it
                            failed = True
                            self._emit({"type": "error", "error": e})
                            break
                        if frame.get("type") == "hello":
                            greeted = True
                except Exception as e:  # ConnectionClosedError and friends
                    self._emit({"type": "error", "error": e})
                code = getattr(ws, "close_code", None) or 1006
                reason = getattr(ws, "close_reason", None) or ""
        except UpgradeRefused as e:
            if e.status_code in (401, 403):
                self._running = False
                self._emit({"type": "fatal", "error": RuntimeError(f"WebSocket upgrade refused with {e.status_code}: check the API key")})
            else:
                self._emit({"type": "error", "error": e})
            return False
        except Exception as e:
            self._emit({"type": "error", "error": e})
            return False
        finally:
            self._socket = None
        if code == 1000 and "replaced" in reason.lower():
            self._emit({"type": "replaced"})
        self._emit({"type": "disconnected", "code": code, "reason": reason})
        return greeted and not failed

    async def _handle_frame(self, m: dict[str, Any]) -> None:
        if m.get("type") == "hello" and isinstance(m.get("session"), str):
            # A new session means the socket (or this process) was down: replay BEFORE adopting it —
            # adopting first is what silently swallows an outage.
            if self.state.block == 0 and not self.replay_without_cursor:
                await self._anchor()
            elif self.state.session is not None and m["session"] != self.state.session:
                await self._replay("new_session")
            self.state.session = m["session"]
            self.state.seq = int(m.get("seq") or 0)
            await self.store.save(self.state)
            self._emit({"type": "hello", "session": m["session"], "seq": self.state.seq})
            return
        if m.get("type") != "fill" or not m.get("data") or not isinstance(m.get("seq"), int):
            return
        if m.get("session") != self.state.session or m["seq"] != self.state.seq + 1:
            await self._replay("seq_skip")
        await self._deliver(m["data"], "ws")
        # only once the fill is handled: a position that ran ahead of a failed delivery would hide the gap
        self.state.session = m.get("session")
        self.state.seq = m["seq"]
        await self.store.save(self.state)

    async def _anchor(self) -> None:
        """No position yet: take the chain head as the starting point. Without it a disconnect before the first
        fill could not be replayed, and replaying from zero would hand over every fill since each subscription began."""
        r = await self.client.latency()
        raw = (r.get("head") or {}).get("block") if isinstance(r, dict) else None
        try:
            head = float(raw) if raw is not None and not isinstance(raw, bool) else math.nan
        except (TypeError, ValueError):
            head = math.nan
        if not (math.isfinite(head) and head == int(head) and head > 0):
            raise RuntimeError("could not read the chain head to anchor the stream")
        head = int(head)
        # strictly-after semantics: everything from (head − lag) on; never 0, which means "no position"
        self.state.block = max(1, head - self.anchor_lag_blocks - 1)
        self.state.logIndex = 0xFFFFFFFF
        self._emit({"type": "anchored", "block": self.state.block + 1})

    async def _replay(self, reason: str) -> None:
        self._emit({"type": "gap", "reason": reason, "fromBlock": self.state.block, "fromLogIndex": self.state.logIndex})
        delivered = 0
        async for fill in self.client.fills_since({"sinceBlock": self.state.block, "sinceLogIndex": self.state.logIndex}):
            if await self._deliver(fill, "replay"):
                delivered += 1
        await self.store.save(self.state)
        self._emit({"type": "replayed", "delivered": delivered})

    async def _deliver(self, fill: Fill, source: str) -> bool:
        if fill["eventId"] in self._seen:
            return False
        r = self.on_fill(fill, FillMeta(source))
        if asyncio.iscoroutine(r) or isinstance(r, asyncio.Future):
            await r
        self._seen[fill["eventId"]] = True
        if len(self._seen) > self.seen_capacity:
            self._seen.popitem(last=False)
        # never move backwards: a live frame can be older than what the replay just walked past
        if fill["block"] > self.state.block or (fill["block"] == self.state.block and fill["logIndex"] > self.state.logIndex):
            self.state.block = fill["block"]
            self.state.logIndex = fill["logIndex"]
        return True
