"""A Model Context Protocol server over stdio (newline-delimited JSON-RPC 2.0), tools only. Small enough to carry no
dependency: initialize, ping, tools/list and tools/call are all an agent host needs from it.
Anything written to stdout that is not a protocol message breaks the host — log to stderr only."""

from __future__ import annotations

import asyncio
import inspect
import json
import sys
from dataclasses import dataclass
from typing import IO, Any, Awaitable, Callable, Optional

MCP_PROTOCOL_VERSIONS = ["2025-06-18", "2025-03-26", "2024-11-05"]


@dataclass(frozen=True)
class McpTool:
    name: str
    description: str
    input_schema: dict  # JSON Schema of the arguments object; sent as "inputSchema"
    # the result is sent to the agent as JSON text (a str as-is); an exception becomes a tool error the agent can read
    handler: Callable[[dict], Awaitable[Any]]


async def serve_mcp(*, name: str, version: str, tools: list[McpTool], instructions: Optional[str] = None,
                    input: Optional[IO[str]] = None, output: Optional[IO[str]] = None) -> None:
    """Serves until the input ends. Requests are handled concurrently; replies go out as each one finishes."""
    inp = input if input is not None else sys.stdin
    out = output if output is not None else sys.stdout
    by_name = {t.name: t for t in tools}

    def send(msg: dict[str, Any]) -> None:
        # ensure_ascii: the line is plain ASCII whatever the console encoding is; the JSON is the same
        out.write(json.dumps(msg, separators=(",", ":")) + "\n")
        out.flush()

    def reply(id_: Any, result: Any) -> None:
        send({"jsonrpc": "2.0", "id": id_, "result": result})

    def fail(id_: Any, code: int, message: str) -> None:
        send({"jsonrpc": "2.0", "id": id_, "error": {"code": code, "message": message}})

    async def handle(req: dict[str, Any]) -> None:
        id_ = req.get("id")
        is_call = id_ is not None
        method = req.get("method")
        if not isinstance(method, str):
            if is_call:
                fail(id_, -32600, "invalid request")
            return
        params = req.get("params")
        p: dict[str, Any] = params if isinstance(params, dict) else {}
        if method == "initialize":
            asked = p.get("protocolVersion")
            result: dict[str, Any] = {
                "protocolVersion": asked if isinstance(asked, str) and asked in MCP_PROTOCOL_VERSIONS else MCP_PROTOCOL_VERSIONS[0],
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": name, "version": version},
            }
            if instructions:
                result["instructions"] = instructions
            reply(id_, result)
        elif method == "ping":
            reply(id_, {})
        elif method == "tools/list":
            reply(id_, {"tools": [{"name": t.name, "description": t.description, "inputSchema": t.input_schema} for t in tools]})
        elif method == "tools/call":
            tool = by_name.get(_str(p.get("name")))
            if tool is None:
                fail(id_, -32602, f"unknown tool {json.dumps(p.get('name'))}")
                return
            if "arguments" in p and not isinstance(p["arguments"], dict):
                fail(id_, -32602, "arguments must be an object")
                return
            try:
                r = tool.handler(p.get("arguments") or {})
                if inspect.isawaitable(r):
                    r = await r
                text = r if isinstance(r, str) else json.dumps(r, indent=2, ensure_ascii=False)
                reply(id_, {"content": [{"type": "text", "text": text}]})
            except Exception as e:
                reply(id_, {"content": [{"type": "text", "text": error_text(e)}], "isError": True})
        elif is_call:
            # notifications (initialized, cancelled, …) need no answer; an unknown request does
            fail(id_, -32601, f"method not found: {method}")

    async def guarded(req: dict[str, Any]) -> None:
        try:
            await handle(req)
        except Exception as e:
            fail(req.get("id"), -32603, error_text(e))

    inflight: set[asyncio.Task[None]] = set()
    while True:
        # a blocking readline in a thread: works for any text stream (a pipe, a file, StringIO) on every platform
        line = await asyncio.to_thread(inp.readline)
        if not line:
            break
        if not line.strip():
            continue
        try:
            req = json.loads(line)
        except ValueError:
            fail(None, -32700, "parse error")
            continue
        if not isinstance(req, dict):
            fail(None, -32600, "invalid request")
            continue
        task = asyncio.create_task(guarded(req))
        inflight.add(task)
        task.add_done_callback(inflight.discard)
    if inflight:
        await asyncio.gather(*inflight)


def error_text(e: BaseException) -> str:
    """An API error (status + body, e.g. PmwError) as 'HTTP <status>: <body>'; anything else as its message."""
    status, body = getattr(e, "status", None), getattr(e, "body", None)
    if status is not None and hasattr(e, "body"):
        return f"HTTP {status}: {body if isinstance(body, str) else json.dumps(body, separators=(',', ':'), ensure_ascii=False)}"
    return str(e) or type(e).__name__


def _str(v: Any) -> str:
    return "" if v is None else str(v)
