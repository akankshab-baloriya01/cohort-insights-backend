"""Construction of long-lived clients shared by the API process and the worker process."""

from dataclasses import dataclass

from pymongo import AsyncMongoClient
from redis.asyncio import Redis

from app.config import Settings
from app.repository import DocumentRepository
from app.services.content_cache import ContentCache
from app.services.locks import RedisLock
from app.services.rate_limiter import ActiveJobLimiter


@dataclass
class Resources:
    settings: Settings
    mongo: AsyncMongoClient
    redis: Redis
    repo: DocumentRepository
    limiter: ActiveJobLimiter
    cache: ContentCache
    locks: RedisLock

    async def close(self) -> None:
        await self.redis.aclose()
        await self.mongo.close()


async def open_resources(settings: Settings) -> Resources:
    mongo: AsyncMongoClient = AsyncMongoClient(settings.mongo_uri, tz_aware=True, serverSelectionTimeoutMS=5000)
    # Redis is lazy-connecting: the service starts (degraded) even if Redis is down.
    redis = Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_timeout=settings.redis_timeout_seconds,
        socket_connect_timeout=settings.redis_timeout_seconds,
    )
    repo = DocumentRepository(mongo[settings.mongo_db]["documents"])
    await repo.ensure_indexes()
    return Resources(
        settings=settings,
        mongo=mongo,
        redis=redis,
        repo=repo,
        limiter=ActiveJobLimiter(redis, repo, settings.max_active_docs_per_user, settings.active_slot_ttl_seconds),
        cache=ContentCache(redis, repo, settings.content_cache_ttl_seconds),
        locks=RedisLock(redis),
    )
