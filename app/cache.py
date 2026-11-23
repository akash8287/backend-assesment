"""Cache of completed summaries and tags, keyed by content hash."""

import json
import logging
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.config import Settings
from app.logging import log

logger = logging.getLogger(__name__)


class ContentCache:
    """Redis copy of a finished result.

    The key is the content hash, never the document id, so a content change
    cannot read the previous result. A Redis failure is a miss or a skipped write.
    """

    def __init__(self, client: Redis, ttl_seconds: int) -> None:
        self._client = client
        self._ttl = ttl_seconds

    @staticmethod
    def key(content_hash: str) -> str:
        """Redis key for one content hash."""
        return f"ci:result:{content_hash}"

    async def get(self, content_hash: str) -> tuple[str, list[str]] | None:
        """Return the cached summary and tags, or None on a miss, a bad value, or a Redis error."""
        try:
            raw = await self._client.get(self.key(content_hash))
        except RedisError:
            log(logger, logging.WARNING, "content cache read failed", content_hash=content_hash)
            return None
        if raw is None:
            return None
        parsed = self._parse(raw)
        if parsed is None:
            log(logger, logging.WARNING, "content cache entry discarded", content_hash=content_hash)
            try:
                await self._client.delete(self.key(content_hash))
            except RedisError:
                log(logger, logging.WARNING, "content cache delete failed", content_hash=content_hash)
            return None
        return parsed

    async def put(self, content_hash: str, summary: str, tags: list[str]) -> None:
        """Store a completed result until the configured TTL. A write failure is logged."""
        payload = json.dumps({"summary": summary, "tags": tags})
        try:
            await self._client.set(self.key(content_hash), payload, ex=self._ttl)
        except RedisError:
            log(logger, logging.WARNING, "content cache write failed", content_hash=content_hash)

    @staticmethod
    def _parse(raw: str) -> tuple[str, list[str]] | None:
        """Accept only a JSON object with a string summary and a list of string tags."""
        try:
            payload: dict[str, Any] = json.loads(raw)
            summary = payload["summary"]
            tags = payload["tags"]
        except (json.JSONDecodeError, KeyError, TypeError):
            return None
        if not isinstance(summary, str) or not isinstance(tags, list):
            return None
        if not all(isinstance(tag, str) for tag in tags):
            return None
        return summary, tags


def redis_client(settings: Settings) -> Redis:
    """Build an async Redis client. Connection happens on the first command."""
    return Redis.from_url(settings.redis_url, decode_responses=True)
