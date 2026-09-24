import hashlib
import hmac
import json

import httpx
import pytest

from pmwallets import AsyncClient, Client, PmwError, verify_webhook


def transport(pages, status=200, calls=None):
    def handler(req: httpx.Request):
        if calls is not None:
            calls.append(req)
        body = pages.pop(0) if pages else None
        return httpx.Response(status, text="" if body is None else json.dumps(body))
    return httpx.MockTransport(handler)


def test_sends_key_builds_query_and_ws_url():
    calls = []
    c = Client("pmw_a_b", "https://api.example.com/", http=httpx.Client(transport=transport([{"rows": [], "next": None, "subscriptions": 0}], calls=calls)))
    c.fills(5, 2)
    assert str(calls[0].url) == "https://api.example.com/v1/account/fills?sinceBlock=5&sinceLogIndex=2&limit=500"
    assert calls[0].headers["x-api-key"] == "pmw_a_b"
    assert c.ws_url == "wss://api.example.com/v1/ws"


async def test_walks_every_page():
    calls = []
    pages = [
        {"rows": [{"eventId": "a"}, {"eventId": "b"}], "next": {"sinceBlock": 2, "sinceLogIndex": 0}, "subscriptions": 1},
        {"rows": [{"eventId": "c"}], "next": None, "subscriptions": 1},
    ]
    c = AsyncClient("k", http=httpx.AsyncClient(transport=transport(pages, calls=calls)))
    ids = [r["eventId"] async for r in c.fills_since({"sinceBlock": 0, "sinceLogIndex": 0}, 2)]
    assert ids == ["a", "b", "c"]
    assert "sinceBlock=2&sinceLogIndex=0&limit=2" in str(calls[1].url)


def test_non_2xx_is_pmw_error():
    calls = []
    c = Client("k", http=httpx.Client(transport=transport([{"statusCode": 402, "message": "insufficient balance"}], 402, calls)))
    with pytest.raises(PmwError) as e:
        c.subscribe("0xabc")
    assert e.value.status == 402 and e.value.body["message"] == "insufficient balance"
    assert json.loads(calls[0].content) == {"channels": ["ws"], "entityId": "0xabc"}


def test_verify_webhook():
    body = '{"type":"fill","data":{}}'
    sig = hmac.new(b"sec", body.encode(), hashlib.sha256).hexdigest()
    assert verify_webhook(body, sig, "sec")
    assert verify_webhook(body.encode(), sig, "sec")
    assert not verify_webhook(body + " ", sig, "sec")
    assert not verify_webhook(body, sig, "other")
    assert not verify_webhook(body, "zz", "sec")
    assert not verify_webhook(body, None, "sec")
