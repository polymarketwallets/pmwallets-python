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


def test_bands_go_out_as_one_comma_separated_value():
    calls = []
    c = Client("k", "https://api.example.com", http=httpx.Client(transport=transport([{"rows": []}, {"rows": []}], calls=calls)))
    c.leaderboard(bands=[0, 2], limit=5)
    c.leaderboard(bands="1,3")
    assert str(calls[0].url) == "https://api.example.com/v1/leaderboard?bands=0%2C2&limit=5"
    assert str(calls[1].url) == "https://api.example.com/v1/leaderboard?bands=1%2C3"


def test_my_addresses_asks_the_board_for_mine():
    calls = []
    c = Client("k", "https://api.example.com", http=httpx.Client(transport=transport([{"periodId": "30d", "tracked": 2, "rows": []}], calls=calls)))
    assert c.my_addresses(sort="realizedPnl", offset=15)["tracked"] == 2
    assert str(calls[0].url) == "https://api.example.com/v1/leaderboard?sort=realizedPnl&offset=15&mine=1"


async def test_async_my_addresses():
    calls = []
    c = AsyncClient("k", "https://api.example.com", http=httpx.AsyncClient(transport=transport([{"tracked": 0, "rows": []}], calls=calls)))
    await c.my_addresses()
    assert str(calls[0].url) == "https://api.example.com/v1/leaderboard?mine=1"


CASES = [
    (lambda c: c.periods(), "GET", "/v1/periods", None),
    (lambda c: c.plan(), "GET", "/v1/account/plan", None),
    (lambda c: c.plans(), "GET", "/v1/plans", None),
    (lambda c: c.billing(), "GET", "/v1/account/billing", None),
    (lambda c: c.ledger(limit=10, cursor="x|1"), "GET", "/v1/account/ledger?limit=10&cursor=x%7C1", None),
    (lambda c: c.unlock_with_plan("H1"), "POST", "/v1/account/reveals", {"entityId": "H1", "maxPriceCents": 0, "viaPlan": True}),
    (lambda c: c.create_plan_export(["H1"]), "POST", "/v1/account/exports", {"entityIds": ["H1"], "viaPlan": True}),
    (lambda c: c.export_window(), "GET", "/v1/account/exports/window", None),
    (lambda c: c.export_rate_card(), "GET", "/v1/exports/rate-card", None),
    (lambda c: c.webhooks(), "GET", "/v1/account/webhooks", None),
    (lambda c: c.reactivate_webhook("w 1"), "POST", "/v1/account/webhooks/w%201/reactivate", None),
    (lambda c: c.deliveries(limit=5), "GET", "/v1/account/deliveries?limit=5", None),
    (lambda c: c.delivery_stats(hours=24), "GET", "/v1/account/delivery-stats?hours=24", None),
]


@pytest.mark.parametrize("call,method,path,body", CASES)
def test_account_calls_map_to_endpoints(call, method, path, body):
    calls = []
    call(Client("k", "https://api.example.com", http=httpx.Client(transport=transport([{}], calls=calls))))
    assert f"{calls[0].method} {calls[0].url}" == f"{method} https://api.example.com{path}"
    if body is not None:
        assert json.loads(calls[0].content) == body


@pytest.mark.parametrize("call,method,path,body", CASES)
async def test_async_account_calls_map_to_endpoints(call, method, path, body):
    calls = []
    await call(AsyncClient("k", "https://api.example.com", http=httpx.AsyncClient(transport=transport([{}], calls=calls))))
    assert f"{calls[0].method} {calls[0].url}" == f"{method} https://api.example.com{path}"


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


W = "0x" + "ab" * 20


def _file_transport(calls):
    def handler(req: httpx.Request):
        calls.append(req)
        if req.url.host == "api.example.com":
            return httpx.Response(302, headers={"location": "https://r2.example/signed"})
        return httpx.Response(200, content=b"\x01\x02\x03")
    return httpx.MockTransport(handler)


def test_download_reads_the_redirect_and_never_sends_the_key_to_storage():
    calls = []
    c = Client("pmw_a_b", "https://api.example.com", http=httpx.Client(transport=_file_transport(calls)))
    assert c.download_export_file(W, "2026-09-25") == b"\x01\x02\x03"
    assert str(calls[0].url) == f"https://api.example.com/v1/account/data/{W}/2026-09-25"
    assert str(calls[1].url) == "https://r2.example/signed"
    assert "x-api-key" not in calls[1].headers


async def test_async_download_reads_the_redirect():
    calls = []
    c = AsyncClient("pmw_a_b", "https://api.example.com", http=httpx.AsyncClient(transport=_file_transport(calls)))
    assert await c.download_export_file(W, "2026-09-25") == b"\x01\x02\x03"
    assert "x-api-key" not in calls[1].headers


def test_no_purchased_file_is_pmw_error():
    c = Client("k", http=httpx.Client(transport=transport([{"statusCode": 404, "message": "no purchased file for this wallet and day"}], 404)))
    with pytest.raises(PmwError) as e:
        c.export_file_url(W, "2026-09-25")
    assert e.value.status == 404
