"""Per-user cap on documents in {queued, processing, enriching}.

Redis layout: one sorted set per user, `rl:active:{user_id}`.
  member = "{document_id}:{content_version}"   (one pipeline run)
  score  = unix time after which the slot is considered leaked

Why a sorted set instead of an INCR/DECR counter:
  * Idempotent: acquiring/releasing the same run twice cannot drift the count.
  * Self-healing: a release lost while Redis was unreachable expires on its own
    (workers refresh the score whenever they claim a stage).
  * Versioned members make the PATCH-vs-completion race safe: a PATCH moves
    the slot from `id:v` to `id:v+1`, so a worker finishing run `v` late
    releases a member that no longer exists instead of the new run's slot.
"""

import logging
import time
from enum import Enum

from bson import ObjectId
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.repository import DocumentRepository

logger = logging.getLogger(__name__)

# KEYS[1] = user zset
# ARGV = now, slot_expires_at, limit, member, prev_member ('' if none), key_ttl
_ACQUIRE_LUA = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
local member = ARGV[4]
local prev = ARGV[5]
if prev ~= '' and redis.call('ZSCORE', KEYS[1], prev) then
  redis.call('ZREM', KEYS[1], prev)
  redis.call('ZADD', KEYS[1], ARGV[2], member)
  redis.call('EXPIRE', KEYS[1], ARGV[6])
  return 2
end
if redis.call('ZSCORE', KEYS[1], member) then
  redis.call('ZADD', KEYS[1], ARGV[2], member)
  redis.call('EXPIRE', KEYS[1], ARGV[6])
  return 2
end
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[3]) then
  return 0
end
redis.call('ZADD', KEYS[1], ARGV[2], member)
redis.call('EXPIRE', KEYS[1], ARGV[6])
return 1
"""


class Acquire(Enum):
    DENIED = 0
    ADDED = 1  # a new slot was taken
    HELD = 2  # the run already held a slot (or inherited the previous version's slot)
    UNTRACKED = 3  # Redis unavailable; allowed by the MongoDB fallback count


class ActiveJobLimiter:
    def __init__(self, redis: Redis, repo: DocumentRepository, limit: int, slot_ttl_seconds: int):
        self.redis = redis
        self.repo = repo
        self.limit = limit
        self.slot_ttl = slot_ttl_seconds
        self._acquire = redis.register_script(_ACQUIRE_LUA)

    @staticmethod
    def _key(user_id: str) -> str:
        return f"rl:active:{user_id}"

    @staticmethod
    def member(doc_id: ObjectId, version: int) -> str:
        return f"{doc_id}:{version}"

    async def acquire(self, user_id: str, doc_id: ObjectId, version: int, prev_version: int | None = None) -> Acquire:
        now = time.time()
        prev = self.member(doc_id, prev_version) if prev_version is not None else ""
        try:
            code = await self._acquire(
                keys=[self._key(user_id)],
                args=[now, now + self.slot_ttl, self.limit, self.member(doc_id, version), prev, self.slot_ttl],
            )
            return Acquire(int(code))
        except RedisError as exc:
            # Degrade to an authoritative-but-racy count from MongoDB rather than failing open or closed.
            logger.warning("rate limiter redis unavailable, using mongo count", extra={"error": str(exc)})
            active = await self.repo.count_active(user_id, exclude_id=doc_id)
            return Acquire.UNTRACKED if active < self.limit else Acquire.DENIED

    async def release(self, user_id: str, doc_id: ObjectId, version: int) -> None:
        try:
            await self.redis.zrem(self._key(user_id), self.member(doc_id, version))
        except RedisError as exc:
            # The slot's score makes it expire on its own; log and move on.
            logger.warning("rate limiter release failed", extra={"error": str(exc), "document_id": str(doc_id)})

    async def touch(self, user_id: str, doc_id: ObjectId, version: int) -> None:
        """Extend a held slot's expiry (only if it is still held)."""
        try:
            await self.redis.zadd(
                self._key(user_id), {self.member(doc_id, version): time.time() + self.slot_ttl}, xx=True
            )
        except RedisError as exc:
            logger.warning("rate limiter touch failed", extra={"error": str(exc), "document_id": str(doc_id)})
