import io
import json
import re
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

import pmwallets
from pmwallets import CursorFile, MarketLookup, McpTool, PmwError, micro_to_decimal, new_fills, pmwallets_tools, serve_mcp
from pmwallets.cli import parse_args
from pmwallets.tools import MCP_INSTRUCTIONS

ROOT = Path(__file__).resolve().parent.parent


def test_micro_to_decimal_converts_exactly():
    assert micro_to_decimal("1824000000") == "1824.000000"
    assert micro_to_decimal("1") == "0.000001"
    assert micro_to_decimal("0") == "0.000000"
    assert micro_to_decimal("-2500000") == "-2.500000"
    assert micro_to_decimal("123456789012345678901234567890") == "123456789012345678901234.567890"
    assert micro_to_decimal("1.5") is None
    assert micro_to_decimal("") is None
    assert micro_to_decimal("12\n") is None
    assert micro_to_decimal(5) is None


CID = "0x" + "ab" * 32


def fake_clob(fail=False):
    urls = []

    def handler(req: httpx.Request):
        urls.append(str(req.url))
        if fail:
            return httpx.Response(500, text="no")
        if "/markets-by-token/" in str(req.url):
            return httpx.Response(200, json={"condition_id": CID})
        return httpx.Response(200, json={"question": "Xi out?", "market_slug": "xi", "end_date_iso": "2027-01-01T00:00:00Z", "closed": False,
                                         "tokens": [{"token_id": "5", "outcome": "Yes"}, {"token_id": "6", "outcome": "No"}]})
    return urls, httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_market_lookup_finds_question_and_outcome_and_caches():
    urls, http = fake_clob()
    m = MarketLookup(http=http)
    assert await m.by_token("6") == {"conditionId": CID, "question": "Xi out?", "outcome": "No", "slug": "xi", "endDate": "2027-01-01T00:00:00Z", "closed": False}
    await m.by_token("6")
    assert len(urls) == 2
    assert urls[0] == "https://clob.polymarket.com/markets-by-token/6"
    assert urls[1] == f"https://clob.polymarket.com/markets/{CID}"


async def test_market_lookup_never_puts_a_noncanonical_id_in_a_url_and_answers_none_on_failure():
    urls, http = fake_clob(fail=True)
    now = [0.0]
    m = MarketLookup(http=http, clock=lambda: now[0])
    assert await m.by_token("../x") is None
    assert await m.by_token("0x12") is None
    assert await m.by_token("007") is None
    assert await m.by_token("5\n") is None
    assert urls == []
    assert await m.by_token("5") is None
    assert len(urls) == 1
    await m.by_token("5")  # a miss is cached for 30 s
    assert len(urls) == 1
    now[0] = 31
    await m.by_token("5")
    assert len(urls) == 2


def test_parse_args():
    assert parse_args(["X", "--period", "7d", "--limit=5", "--takeover", "--exec", 'claude -p "hi"', "Y"], ["takeover"]) == (
        ["X", "Y"], {"period": "7d", "limit": "5", "takeover": True, "exec": 'claude -p "hi"'})
    assert parse_args(["--dir"], [])[1] == {"dir": True}
    assert parse_args(["--a", "--b", "v"], [])[1] == {"a": True, "b": "v"}


async def rpc(tools, messages):
    inp = io.StringIO("".join((m if isinstance(m, str) else json.dumps(m)) + "\n" for m in messages))
    out = io.StringIO()
    await serve_mcp(name="t", version="1", tools=tools, input=inp, output=out, instructions="hello")
    return [json.loads(x) for x in out.getvalue().strip().split("\n") if x]


async def _echo(a):
    return {"got": a}


async def _boom(a):
    raise PmwError(402, {"error": "insufficient_balance"})


async def _plain(a):
    raise ValueError("bad input")


ECHO = McpTool("echo", "d", {"type": "object"}, _echo)
BOOM = McpTool("boom", "d", {"type": "object"}, _boom)
PLAIN = McpTool("plain", "d", {"type": "object"}, _plain)


async def test_mcp_speaks_the_protocol_and_answers_no_notification():
    out = await rpc([ECHO, BOOM, PLAIN], [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "c", "version": "1"}}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
        {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "echo", "arguments": {"a": 1}}},
        {"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "boom", "arguments": {}}},
        {"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"name": "nope"}},
        {"jsonrpc": "2.0", "id": 6, "method": "resources/list"},
        {"jsonrpc": "2.0", "id": 7, "method": "ping"},
        "not json",
        {"jsonrpc": "2.0", "id": 8, "method": "tools/call", "params": {"name": "plain"}},
        {"jsonrpc": "2.0", "id": 9, "method": "tools/call", "params": {"name": "echo", "arguments": [1]}},
        "",
    ])
    by = {m.get("id"): m for m in out}
    r1 = by[1]["result"]
    assert r1["protocolVersion"] == "2025-03-26" and r1["capabilities"] == {"tools": {"listChanged": False}}
    assert r1["serverInfo"] == {"name": "t", "version": "1"} and r1["instructions"] == "hello"
    assert [t["name"] for t in by[2]["result"]["tools"]] == ["echo", "boom", "plain"]
    assert by[2]["result"]["tools"][0] == {"name": "echo", "description": "d", "inputSchema": {"type": "object"}}
    assert json.loads(by[3]["result"]["content"][0]["text"]) == {"got": {"a": 1}}
    assert by[4]["result"]["isError"] is True
    assert by[4]["result"]["content"][0]["text"] == 'HTTP 402: {"error":"insufficient_balance"}'
    assert by[5]["error"] == {"code": -32602, "message": 'unknown tool "nope"'}
    assert by[6]["error"]["code"] == -32601
    assert by[7]["result"] == {}
    assert by[None]["error"]["code"] == -32700
    assert by[8]["result"] == {"content": [{"type": "text", "text": "bad input"}], "isError": True}
    assert by[9]["error"] == {"code": -32602, "message": "arguments must be an object"}
    assert len(out) == 10  # the notification got no answer


async def test_mcp_offers_the_latest_protocol_to_an_unknown_version():
    [m] = await rpc([], [{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "1999-01-01"}}])
    assert m["result"]["protocolVersion"] == "2025-06-18"


def test_mcp_over_real_stdio_lists_the_tools(tmp_path):
    msgs = "\n".join(json.dumps(m) for m in [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2024-11-05"}},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
    ]) + "\n"
    env = {k: v for k, v in __import__("os").environ.items() if k != "PMW_API_KEY"}
    env["PMW_MCP_STATE"] = str(tmp_path / "c.json")
    r = subprocess.run([sys.executable, "-m", "pmwallets.cli", "mcp"], input=msgs, capture_output=True, text=True, env=env, timeout=30)
    out = [json.loads(x) for x in r.stdout.strip().split("\n")]
    assert out[0]["result"]["instructions"] == MCP_INSTRUCTIONS
    assert out[0]["result"]["serverInfo"] == {"name": "pmwallets", "version": pmwallets.VERSION}
    assert [t["name"] for t in out[1]["result"]["tools"]] == [
        "leaderboard", "signals", "radar", "entity", "subscriptions", "subscribe", "cancel_subscription", "new_fills", "fills", "market", "account"]


def row(block, log_index):
    return {"eventId": f"137:{block}:h:t:{log_index}", "chain": 137, "entityId": "0xe", "wallet": "0xw", "ts": "2026-10-14 00:00:00",
            "block": block, "blockHash": "h", "txHash": "t", "logIndex": log_index, "exchange": "x", "side": "SELL", "role": "maker",
            "tokenId": "9", "price": "0.5", "shares": "2000000", "usdc": "1000000", "fee": "0"}


class FakeApi:
    def __init__(self, rows):
        self.rows = rows
        self.asked = []

    async def latency(self):
        return {"head": {"block": 5000}}

    async def fills(self, since_block=0, since_log_index=0, limit=500):
        self.asked.append({"sinceBlock": since_block, "sinceLogIndex": since_log_index, "limit": limit})
        after = [r for r in self.rows if r["block"] > since_block or (r["block"] == since_block and r["logIndex"] > since_log_index)]
        page = after[:limit]
        nxt = {"sinceBlock": page[-1]["block"], "sinceLogIndex": page[-1]["logIndex"]} if len(after) > len(page) else None
        return {"rows": page, "next": nxt, "subscriptions": 1}


async def test_new_fills_starts_an_hour_back_hands_each_fill_once_and_keeps_its_place(tmp_path):
    file = CursorFile(tmp_path / "sub" / "c.json")
    c = FakeApi([row(3000, 1), row(4000, 2), row(4000, 5), row(4500, 0)])
    first = await new_fills(c, file, None, 2)
    assert c.asked[0] == {"sinceBlock": 3200, "sinceLogIndex": 0, "limit": 2}
    assert [f["block"] for f in first["fills"]] == [4000, 4000]
    assert first["fills"][0]["usdcAmount"] == "1.000000"
    assert first["more"] is True
    assert "first call" in first["note"]
    assert json.loads(file.path.read_text()) == {"sinceBlock": 4000, "sinceLogIndex": 5}
    second = await new_fills(c, file, None, 2)
    assert [f["block"] for f in second["fills"]] == [4500]
    assert second["more"] is False and "note" not in second
    third = await new_fills(c, file, None, 2)
    assert third["fills"] == []
    assert file.load() == {"sinceBlock": 4500, "sinceLogIndex": 0}


async def test_new_fills_limit_is_clamped_and_a_nonnumber_means_100(tmp_path):
    c = FakeApi([])
    file = CursorFile(tmp_path / "c.json")
    for given, sent in [(10_000, 500), (0, 1), (2.7, 2), ("abc", 100), (float("inf"), 100)]:
        await new_fills(c, file, None, given)
        assert c.asked[-1]["limit"] == sent


async def test_fills_tool_wants_whole_numbers(tmp_path):
    c = FakeApi([row(10, 1)])
    tools = {t.name: t for t in pmwallets_tools(c, markets=None, cursor=CursorFile(tmp_path / "c.json"))}
    page = await tools["fills"].handler({"sinceBlock": "5"})
    assert c.asked[-1] == {"sinceBlock": 5, "sinceLogIndex": 0, "limit": 100}
    assert page["rows"][0]["usdcAmount"] == "1.000000" and page["rows"][0]["market"] is None
    for bad in [{}, {"sinceBlock": 1.5}, {"sinceBlock": -1}, {"sinceBlock": "x"}, {"sinceBlock": 1, "limit": 0.5}]:
        with pytest.raises(ValueError, match="must be a whole number"):
            await tools["fills"].handler(bad)
    await tools["fills"].handler({"sinceBlock": 1, "limit": 9999})
    assert c.asked[-1]["limit"] == 500


def test_cursor_file_ignores_a_broken_file(tmp_path):
    f = tmp_path / "c.json"
    f.write_text("{oops")
    assert CursorFile(f).load() is None
    f.write_text(json.dumps({"sinceBlock": 1.5, "sinceLogIndex": 0}))
    assert CursorFile(f).load() is None


async def test_every_tool_tells_an_agent_how_to_fix_a_missing_api_key(tmp_path):
    tools = pmwallets_tools(None, markets=None, cursor=CursorFile(tmp_path / "c.json"))
    sub = next(t for t in tools if t.name == "subscriptions")
    with pytest.raises(RuntimeError, match="PMW_API_KEY"):
        await sub.handler({})
    assert len(tools) == 11


def test_version_matches_pyproject():
    m = re.search(r'^version = "([^"]+)"', (ROOT / "pyproject.toml").read_text(), re.M)
    assert m and pmwallets.VERSION == pmwallets.__version__ == m.group(1)


def test_skill_ships_in_the_package_and_matches_the_repo_copy():
    from pmwallets.cli import _skill
    assert _skill() == (ROOT / "SKILL.md").read_text("utf-8")
    assert _skill().startswith("---\nname: pmwallets\n")


def test_cli_errors_are_one_json_line_on_stderr_with_exit_1(tmp_path):
    env = {k: v for k, v in __import__("os").environ.items() if k != "PMW_API_KEY"}
    r = subprocess.run([sys.executable, "-m", "pmwallets.cli", "subscriptions"], capture_output=True, text=True, env=env, timeout=30)
    assert r.returncode == 1 and r.stdout == ""
    assert json.loads(r.stderr) == {"error": "PMW_API_KEY is not set: create a key on https://pmwallets.com/keys and put it in the MCP server's env"}
    r = subprocess.run([sys.executable, "-m", "pmwallets.cli", "watch", "--exec"], capture_output=True, text=True, env=env, timeout=30)
    assert r.returncode == 1 and "--exec needs a command" in json.loads(r.stderr)["error"]
    r = subprocess.run([sys.executable, "-m", "pmwallets.cli", "nope"], capture_output=True, text=True, env=env, timeout=30)
    assert r.returncode == 1 and r.stdout.startswith(f"pmwallets {pmwallets.VERSION} ")


def test_websocket_proxy_follows_https_proxy_and_no_proxy(monkeypatch):
    # what `pmwallets watch` relies on for proxies: websockets' own env handling (see the note in cli.py)
    try:
        from websockets.proxy import get_proxy
    except ImportError:  # websockets 15.0 kept it in websockets.uri
        from websockets.uri import get_proxy
    from websockets.uri import parse_uri
    for k in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy", "NO_PROXY", "no_proxy"):
        monkeypatch.delenv(k, raising=False)
    uri = parse_uri("wss://api.pmwallets.com/v1/ws")
    monkeypatch.setenv("https_proxy", "http://proxy.example:3128")
    assert get_proxy(uri) == "http://proxy.example:3128"
    for bypass, used in [(".pmwallets.com", False), ("pmwallets.com", False), ("*", False), ("localhost,127.0.0.1", True), ("evilpmwallets.com", True)]:
        monkeypatch.setenv("no_proxy", bypass)
        assert (get_proxy(uri) is not None) is used, bypass


def test_the_rest_client_and_market_lookup_trust_the_proxy_env():
    from pmwallets import AsyncClient, Client
    assert Client("k")._http.trust_env and AsyncClient("k")._http.trust_env and MarketLookup()._http.trust_env


async def test_concurrent_new_fills_hand_out_disjoint_fills_and_the_cursor_never_moves_back(tmp_path):
    import asyncio
    file = CursorFile(tmp_path / "c.json")
    file.save({"sinceBlock": 3999, "sinceLogIndex": 0})
    class Slow(FakeApi):
        async def fills(self, *a, **k):
            await asyncio.sleep(0.01)  # both calls are in flight at once
            return await super().fills(*a, **k)
    c = Slow([row(4000, 1), row(4000, 2), row(4001, 0), row(4002, 0)])
    a, b = await asyncio.gather(new_fills(c, file, None, 2), new_fills(c, file, None, 2))
    ids = [f["eventId"] for f in a["fills"] + b["fills"]]
    assert len(set(ids)) == 4
    # another process went further: an older answer must not rewind it
    c2 = FakeApi([row(9500, 0)])
    file.save({"sinceBlock": 9600, "sinceLogIndex": 0})

    class Racing(FakeApi):
        async def fills(self, *a, **k):
            page = await super().fills(*a, **k)
            file.save({"sinceBlock": 9700, "sinceLogIndex": 0})  # saved by someone else while this call ran
            return page
    file.save({"sinceBlock": 9000, "sinceLogIndex": 0})
    await new_fills(Racing([row(9500, 0)]), CursorFile(file.path), None, 2)
    assert file.load() == {"sinceBlock": 9700, "sinceLogIndex": 0}
    await new_fills(c2, CursorFile(file.path), None, 2)
    assert file.load() == {"sinceBlock": 9700, "sinceLogIndex": 0}


async def test_the_cursor_moves_only_once_the_answer_is_complete(tmp_path):
    file = CursorFile(tmp_path / "c.json")
    file.save({"sinceBlock": 3999, "sinceLogIndex": 0})
    c = FakeApi([row(4000, 1)])

    class Failing:
        async def by_token(self, token_id):
            raise RuntimeError("interrupted")
    with pytest.raises(RuntimeError, match="interrupted"):
        await new_fills(c, file, Failing(), 10)
    assert file.load() == {"sinceBlock": 3999, "sinceLogIndex": 0}


async def test_one_file_reached_by_two_spellings_of_its_path_is_still_one_queue(tmp_path):
    import asyncio
    import os
    abs_path = tmp_path / "c.json"
    rel = os.path.relpath(abs_path, os.getcwd())
    CursorFile(abs_path).save({"sinceBlock": 3999, "sinceLogIndex": 0})

    class Slow(FakeApi):
        async def fills(self, *a, **k):
            await asyncio.sleep(0.01)  # both calls are in flight at once
            return await super().fills(*a, **k)
    c = Slow([row(4000, 1), row(4000, 2), row(4001, 0), row(4002, 0)])
    a, b = await asyncio.gather(new_fills(c, CursorFile(abs_path), None, 2), new_fills(c, CursorFile(rel), None, 2))
    assert len({f["eventId"] for f in a["fills"] + b["fills"]}) == 4
