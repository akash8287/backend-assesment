"""UTC timestamps stored on documents and claims."""

from datetime import datetime, timezone


def utcnow() -> datetime:
    """Current time in UTC, timezone-aware, for MongoDB comparisons."""
    return datetime.now(timezone.utc)
