"""Official PMWallets SDK — https://pmwallets.com/docs"""

from .client import DEFAULT_BASE_URL, AsyncClient, Client, PmwError
from .market import MarketInfo, MarketLookup, micro_to_decimal
from .mcp import MCP_PROTOCOL_VERSIONS, McpTool, serve_mcp
from .stream import CLOSE_API_SEAT_TAKEN, CLOSE_SERVICE_RESTART, FileStateStore, FillMeta, FillStream, MemoryStateStore, StreamState, UpgradeRefused, websockets_connector
from .tools import MCP_INSTRUCTIONS, CursorFile, describe_fill, new_fills, pmwallets_tools
from .types import Fill, FillCursor, FillsPage
from .version import VERSION
from .watch import Watcher
from .webhook import verify_webhook

__all__ = [
    "DEFAULT_BASE_URL", "AsyncClient", "Client", "PmwError",
    "CLOSE_API_SEAT_TAKEN", "CLOSE_SERVICE_RESTART", "FileStateStore", "FillMeta", "FillStream", "MemoryStateStore", "StreamState", "UpgradeRefused", "websockets_connector",
    "Fill", "FillCursor", "FillsPage", "verify_webhook",
    "MarketInfo", "MarketLookup", "micro_to_decimal", "Watcher",
    "MCP_PROTOCOL_VERSIONS", "McpTool", "serve_mcp", "MCP_INSTRUCTIONS", "CursorFile", "describe_fill", "new_fills", "pmwallets_tools",
    "VERSION",
]
__version__ = VERSION
