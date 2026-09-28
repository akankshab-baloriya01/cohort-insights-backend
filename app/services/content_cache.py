"""Content-addressed result cache.

Key: `cache:result:{sha256(content)}` -> {"content_hash", "summary", "tags"} with a TTL.
The key is a function of content only, never of document_id, so a PATCH simply
looks up a different key; there is no entry for an old version to leak from.

Tier 2 is MongoDB itself: any document whose published `result.content_hash`
equals the hash (indexed). That covers Redis TTL expiry and Redis outages.
"""

import json
import logging
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.repository import DocumentRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CachedResult:
    content_hash: str
    summary: str
    tags: list[str]


class ContentCache:
    def __init__(self, redis: Redis, repo: DocumentRepository, ttl_seconds: int):
        self.redis = redis
        self.repo = repo
        self.ttl = ttl_seconds

    @staticmethod
    def _key(hash_: str) -> str:
        return f"cache:result:{hash_}"

    async def lookup(self, hash_: str) -> CachedResult | None:
        cached = await self._redis_get(hash_)
        if cached is not None:
            return cached
        stored = await self.repo.find_result_by_hash(hash_)
        if stored is None:
            return None
        cached = CachedResult(hash_, stored["summary"], list(stored["tags"]))
        await self.store(cached)
        return cached

    async def store(self, result: CachedResult) -> None:
        payload = json.dumps({"content_hash": result.content_hash, "summary": result.summary, "tags": result.tags})
        try:
            await self.redis.set(self._key(result.content_hash), payload, ex=self.ttl)
        except RedisError as exc:
            logger.warning("content cache write failed", extra={"error": str(exc)})

    async def _redis_get(self, hash_: str) -> CachedResult | None:
        try:
            raw = await self.redis.get(self._key(hash_))
        except RedisError as exc:
            logger.warning("content cache read failed, falling back to mongo", extra={"error": str(exc)})
            return None
        if raw is None:
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("content cache entry corrupt, ignoring", extra={"content_hash": hash_})
            return None
        # Defensive: never serve an entry whose embedded hash disagrees with the key.
        if data.get("content_hash") != hash_:
            return None
        return CachedResult(hash_, data["summary"], list(data["tags"]))
