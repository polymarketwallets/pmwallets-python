from __future__ import annotations

import hashlib
import hmac
import re
from typing import Optional, Union

_HEX = re.compile(r"^[0-9a-fA-F]+$")


def verify_webhook(raw_body: Union[bytes, str], signature_hex: Optional[str], secret: str) -> bool:
    """Verify `x-pmw-signature`: hex HMAC-SHA256 of the RAW request body, keyed with your webhook secret.

    Pass the raw bytes, before any JSON parsing — re-serialising changes them and every check fails.
    """
    if not signature_hex or not secret or not _HEX.match(signature_hex) or len(signature_hex) % 2:
        return False
    body = raw_body.encode() if isinstance(raw_body, str) else raw_body
    mine = hmac.new(secret.encode(), body, hashlib.sha256).digest()
    return hmac.compare_digest(bytes.fromhex(signature_hex), mine)
