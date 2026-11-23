"""Content identity used by the cache and by staleness checks."""

import hashlib


def content_hash(content: str) -> str:
    """SHA-256 of the raw UTF-8 content. Whitespace is significant and nothing is normalized."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()
