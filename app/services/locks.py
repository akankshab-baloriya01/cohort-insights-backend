"""Short-lived Redis mutex used to serialize creation of the same (user_id, client_doc_ref)."""

import logging
import uuid
from enum import Enum

from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

# Delete only if we still own it, so an expired-and-reacquired lock is never released by the old holder.
_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


class LockState(Enum):
    ACQUIRED = "acquired"
    HELD_ELSEWHERE = "held_elsewhere"
    UNAVAILABLE = "unavailable"  # Redis down: caller proceeds without the lock


class RedisLock:
    def __init__(self, redis: Redis, ttl_ms: int = 5000):
        self.redis = redis
        self.ttl_ms = ttl_ms
        self._release = redis.register_script(_RELEASE_LUA)

    async def acquire(self, key: str) -> tuple[LockState, str]:
        token = uuid.uuid4().hex
        try:
            ok = await self.redis.set(key, token, nx=True, px=self.ttl_ms)
        except RedisError as exc:
            logger.warning("lock unavailable, proceeding unlocked", extra={"key": key, "error": str(exc)})
            return LockState.UNAVAILABLE, token
        return (LockState.ACQUIRED if ok else LockState.HELD_ELSEWHERE), token

    async def release(self, key: str, token: str) -> None:
        try:
            await self._release(keys=[key], args=[token])
        except RedisError as exc:
            logger.warning("lock release failed; it will expire", extra={"key": key, "error": str(exc)})
