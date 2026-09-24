import asyncio
import contextlib
import json

from pmwallets.stream import FileStateStore, FillStream, MemoryStateStore, UpgradeRefused


def fill(block, log_index):
    return {
        "eventId": f"137:{block}:0xh:0xt{block}:{log_index}", "chain": 137, "entityId": "0xe", "wallet": "0xw",
        "ts": "2026-09-24 00:00:00", "block": block, "blockHash": "0xh", "txHash": f"0xt{block}", "logIndex": log_index,
        "exchange": "pm_ctf_v2", "side": "BUY", "role": "taker", "tokenId": "1", "price": "0.5", "shares": "1000000",
        "usdc": "500000", "fee": "0",
    }


_END = object()


class FakeSocket:
    """a scripted socket: the test pushes frames and closes it"""

    def __init__(self):
        self.q: asyncio.Queue = asyncio.Queue()
        self.close_code = None
        self.close_reason = None
        self.closed = False

    def send(self, frame):
        self.q.put_nowait(json.dumps(frame))

    def remote_close(self, code=1006, reason=""):
        self.close_code, self.close_reason = code, reason
        self.q.put_nowait(_END)

    async def close(self, code=1000, reason=""):
        self.closed = True
        if self.close_code is None:
            self.close_code, self.close_reason = code, reason
        self.q.put_nowait(_END)

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.q.get()
        if item is _END:
            raise StopAsyncIteration
        return item


class FakeClient:
    api_key = "pmw_x_y"
    ws_url = "wss://example/v1/ws"

    def __init__(self, ledger):
        self.ledger = ledger
        self.replays = []

    async def fills_since(self, c):
        self.replays.append(dict(c))
        for f in self.ledger:
            if f["block"] > c["sinceBlock"] or (f["block"] == c["sinceBlock"] and f["logIndex"] > c["sinceLogIndex"]):
                yield f


def harness(ledger, fail_on=None, refuse=None):
    sockets, delivered, events = [], [], []
    client = FakeClient(ledger)
    state = {"fail": fail_on}

    @contextlib.asynccontextmanager
    async def connector(url, headers):
        if refuse:
            raise UpgradeRefused(refuse)
        s = FakeSocket()
        sockets.append(s)
        try:
            yield s
        finally:
            s.closed = True

    def on_fill(f, meta):
        if state["fail"] == f["eventId"]:
            state["fail"] = None
            raise RuntimeError("handler failed")
        delivered.append((f["eventId"], meta.source))

    stream = FillStream(client, on_fill, on_event=events.append, store=MemoryStateStore(), min_backoff=0.001,
                        max_backoff=0.002, connector=connector)
    return stream, sockets, client, delivered, events


async def tick():
    await asyncio.sleep(0.02)


async def test_delivers_consecutive_frames_in_order():
    stream, sockets, client, delivered, _ = harness([])
    await stream.start(); await tick()
    s = sockets[0]
    s.send({"type": "hello", "session": "A", "seq": 0})
    s.send({"type": "fill", "session": "A", "seq": 1, "data": fill(10, 1)})
    s.send({"type": "fill", "session": "A", "seq": 2, "data": fill(10, 2)})
    await tick()
    assert [d[0] for d in delivered] == [fill(10, 1)["eventId"], fill(10, 2)["eventId"]]
    assert client.replays == []
    p = stream.position
    assert (p.session, p.seq, p.block, p.logIndex) == ("A", 2, 10, 2)
    await stream.stop()


async def test_seq_skip_replays_and_drops_duplicate():
    ledger = [fill(10, 1), fill(11, 1), fill(12, 1)]
    stream, sockets, client, delivered, _ = harness(ledger)
    await stream.start(); await tick()
    s = sockets[0]
    s.send({"type": "hello", "session": "A", "seq": 0})
    s.send({"type": "fill", "session": "A", "seq": 1, "data": ledger[0]})
    s.send({"type": "fill", "session": "A", "seq": 3, "data": ledger[2]})
    await tick()
    assert client.replays == [{"sinceBlock": 10, "sinceLogIndex": 1}]
    assert delivered == [(ledger[0]["eventId"], "ws"), (ledger[1]["eventId"], "replay"), (ledger[2]["eventId"], "replay")]
    assert stream.position.seq == 3 and stream.position.block == 12
    await stream.stop()


async def test_new_session_replays_what_was_missed():
    ledger = [fill(10, 1), fill(11, 1)]
    stream, sockets, client, delivered, _ = harness(ledger)
    await stream.start(); await tick()
    sockets[0].send({"type": "hello", "session": "A", "seq": 0})
    sockets[0].send({"type": "fill", "session": "A", "seq": 1, "data": ledger[0]})
    await tick()
    sockets[0].remote_close(1006)
    await tick()
    sockets[1].send({"type": "hello", "session": "B", "seq": 0})
    await tick()
    assert client.replays == [{"sinceBlock": 10, "sinceLogIndex": 1}]
    assert [d[0] for d in delivered] == [ledger[0]["eventId"], ledger[1]["eventId"]]
    assert stream.position.session == "B"
    await stream.stop()


async def test_throwing_handler_is_redelivered_after_reconnect():
    ledger = [fill(10, 1), fill(11, 1)]
    stream, sockets, client, delivered, _ = harness(ledger, fail_on=ledger[1]["eventId"])
    await stream.start(); await tick()
    s = sockets[0]
    s.send({"type": "hello", "session": "A", "seq": 0})
    s.send({"type": "fill", "session": "A", "seq": 1, "data": ledger[0]})
    s.send({"type": "fill", "session": "A", "seq": 2, "data": ledger[1]})
    await tick()
    assert s.closed
    assert stream.position.seq == 1 and stream.position.block == 10
    sockets[1].send({"type": "hello", "session": "B", "seq": 0})
    await tick()
    assert [d[0] for d in delivered] == [ledger[0]["eventId"], ledger[1]["eventId"]]
    await stream.stop()


async def test_401_is_fatal():
    stream, sockets, _, _, events = harness([], refuse=401)
    await stream.start()
    await asyncio.wait_for(stream.wait(), 1)
    assert [e["type"] for e in events].count("fatal") == 1
    assert [e["type"] for e in events].count("connecting") == 1


async def test_reports_takeover():
    stream, sockets, _, _, events = harness([])
    await stream.start(); await tick()
    sockets[0].remote_close(1000, "replaced by a newer connection")
    await tick()
    assert any(e["type"] == "replaced" for e in events)
    await stream.stop()


async def test_no_replay_of_all_history_without_a_cursor():
    stream, sockets, client, _, events = harness([fill(5, 1)])
    await stream.start(); await tick()
    sockets[0].send({"type": "hello", "session": "A", "seq": 0})
    await tick()
    sockets[0].remote_close(1006); await tick()
    sockets[1].send({"type": "hello", "session": "B", "seq": 0})
    await tick()
    assert client.replays == []
    assert any(e["type"] == "gap" and e.get("skipped") == "no_cursor" for e in events)
    await stream.stop()


async def test_file_state_store_roundtrip(tmp_path):
    from pmwallets.stream import StreamState
    store = FileStateStore(tmp_path / "sub" / "state.json")
    assert await store.load() is None
    await store.save(StreamState("s", 3, 9, 2))
    assert await store.load() == StreamState("s", 3, 9, 2)
    assert json.loads((tmp_path / "sub" / "state.json").read_text()) == {"session": "s", "seq": 3, "block": 9, "logIndex": 2}
