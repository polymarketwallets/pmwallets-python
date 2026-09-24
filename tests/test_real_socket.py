import asyncio
import json

from websockets.asyncio.server import serve
from websockets.http11 import Response
from websockets.datastructures import Headers

from pmwallets import AsyncClient, FillStream


async def _server():
    def process_request(conn, request):
        if request.headers.get("x-api-key") != "good":
            return Response(401, "Unauthorized", Headers([("Content-Length", "0")]), b"")
        return None

    async def handler(ws):
        await ws.send(json.dumps({"type": "hello", "userId": "u", "session": "S", "seq": 0}))
        await ws.send(json.dumps({"type": "fill", "session": "S", "seq": 1, "data": {"eventId": "e1", "block": 1, "logIndex": 0}}))
        await ws.wait_closed()

    server = await serve(handler, "127.0.0.1", 0, process_request=process_request)
    port = server.sockets[0].getsockname()[1]
    return server, f"http://127.0.0.1:{port}"


async def test_receives_frames_with_a_good_key():
    server, base = await _server()
    got = []
    stream = FillStream(AsyncClient("good", base), lambda f, m: got.append(f["eventId"]), ws_options={"proxy": None})
    await stream.start()
    for _ in range(50):
        if got:
            break
        await asyncio.sleep(0.02)
    await stream.stop()
    server.close()
    assert got == ["e1"]


async def test_fatal_on_401():
    server, base = await _server()
    events = []
    stream = FillStream(AsyncClient("bad", base), lambda f, m: None, on_event=events.append, min_backoff=0.005, ws_options={"proxy": None})
    await stream.start()
    await asyncio.wait_for(stream.wait(), 2)
    server.close()
    types = [e["type"] for e in events]
    assert types.count("fatal") == 1 and types.count("connecting") == 1
