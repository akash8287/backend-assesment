import asyncio
import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.errors import UnavailableError
from app.logging import log

logger = logging.getLogger(__name__)

_ADMIT = """
local zset = KEYS[1]
local meta = KEYS[2]
local now = tonumber(ARGV[1])
local lease_at = tonumber(ARGV[2])
local doc = ARGV[3]
local version = ARGV[4]
local limit = tonumber(ARGV[5])

local expired = redis.call('ZRANGEBYSCORE', zset, '-inf', now)
if #expired > 0 then
  redis.call('ZREM', zset, unpack(expired))
  redis.call('HDEL', meta, unpack(expired))
end

local current = redis.call('HGET', meta, doc)
if current then
  redis.call('HSET', meta, doc, version)
  redis.call('ZADD', zset, lease_at, doc)
  redis.call('EXPIRE', zset, 86400)
  redis.call('EXPIRE', meta, 86400)
  return 2
end

if redis.call('ZCARD', zset) >= limit then
  return 0
end

redis.call('HSET', meta, doc, version)
redis.call('ZADD', zset, lease_at, doc)
redis.call('EXPIRE', zset, 86400)
redis.call('EXPIRE', meta, 86400)
return 1
"""

_RELEASE = """
local zset = KEYS[1]
local meta = KEYS[2]
local doc = ARGV[1]
local version = ARGV[2]
if redis.call('HGET', meta, doc) == version then
  redis.call('ZREM', zset, doc)
  redis.call('HDEL', meta, doc)
  return 1
end
return 0
"""

_TOUCH = """
local zset = KEYS[1]
local meta = KEYS[2]
local lease_at = tonumber(ARGV[1])
local doc = ARGV[2]
local version = ARGV[3]
if redis.call('HGET', meta, doc) == version then
  redis.call('ZADD', zset, lease_at, doc)
  return 1
end
return 0
"""

_UNLOCK = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


class RateLimiter:
    """Tracks active documents per user in Redis.

    Membership is `document_id -> content_version`. A release for an older
    version does not drop the slot after a newer version has been admitted.
    Callers that mutate membership for one user share `hold`.
    """

    def __init__(
        self,
        client: Redis,
        limit: int,
        lease_seconds: int,
        lock_ttl_ms: int,
        lock_wait_seconds: float,
    ) -> None:
        self._client = client
        self._limit = limit
        self._lease_seconds = lease_seconds
        self._lock_ttl_ms = lock_ttl_ms
        self._lock_wait_seconds = lock_wait_seconds

    @staticmethod
    def _zset(user_id: str) -> str:
        return f"ci:active:{user_id}"

    @staticmethod
    def _meta(user_id: str) -> str:
        return f"ci:active-meta:{user_id}"

    @staticmethod
    def _lock_key(user_id: str) -> str:
        return f"ci:lock:{user_id}"

    @asynccontextmanager
    async def hold(self, user_id: str) -> AsyncIterator[bool]:
        token = await self._lock(user_id)
        try:
            yield token is not None
        finally:
            if token is not None:
                await self._unlock(user_id, token)

    async def admit(self, user_id: str, document_id: str, version: int) -> str:
        now = time.time()
        try:
            code = await self._client.eval(
                _ADMIT,
                2,
                self._zset(user_id),
                self._meta(user_id),
                now,
                now + self._lease_seconds,
                document_id,
                str(version),
                self._limit,
            )
        except RedisError:
            log(logger, logging.WARNING, "rate limiter admit failed", user_id=user_id)
            raise
        if int(code) == 0:
            return "rejected"
        if int(code) == 2:
            return "refreshed"
        return "added"

    async def retire(self, user_id: str, document_id: str, version: int) -> None:
        try:
            await self._client.eval(
                _RELEASE,
                2,
                self._zset(user_id),
                self._meta(user_id),
                document_id,
                str(version),
            )
        except RedisError:
            log(
                logger,
                logging.WARNING,
                "rate limiter release failed",
                user_id=user_id,
                document_id=document_id,
                content_version=version,
            )

    async def release(self, user_id: str, document_id: str, version: int) -> None:
        try:
            async with self.hold(user_id) as locked:
                if locked:
                    await self.retire(user_id, document_id, version)
        except (RedisError, UnavailableError):
            log(
                logger,
                logging.WARNING,
                "rate limiter release failed",
                user_id=user_id,
                document_id=document_id,
                content_version=version,
            )

    async def touch(self, user_id: str, document_id: str, version: int) -> bool:
        try:
            async with self.hold(user_id) as locked:
                if not locked:
                    return True
                code = await self._client.eval(
                    _TOUCH,
                    2,
                    self._zset(user_id),
                    self._meta(user_id),
                    time.time() + self._lease_seconds,
                    document_id,
                    str(version),
                )
        except (RedisError, UnavailableError):
            log(logger, logging.WARNING, "rate limiter touch failed", document_id=document_id)
            return True
        return int(code) == 1

    async def _lock(self, user_id: str) -> str | None:
        token = uuid.uuid4().hex
        key = self._lock_key(user_id)
        deadline = time.monotonic() + self._lock_wait_seconds
        while True:
            try:
                acquired = await self._client.set(key, token, nx=True, px=self._lock_ttl_ms)
            except RedisError:
                log(logger, logging.WARNING, "rate limiter lock unavailable", user_id=user_id)
                return None
            if acquired:
                return token
            if time.monotonic() >= deadline:
                raise UnavailableError("admission lock is busy")
            await asyncio.sleep(0.02)

    async def _unlock(self, user_id: str, token: str) -> None:
        try:
            await self._client.eval(_UNLOCK, 1, self._lock_key(user_id), token)
        except RedisError:
            log(logger, logging.WARNING, "rate limiter unlock failed", user_id=user_id)
