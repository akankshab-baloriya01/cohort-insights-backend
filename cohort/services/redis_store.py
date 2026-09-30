import json
import logging
from typing import Optional

from redis.exceptions import RedisError

from config import ACTIVE_KEY_TTL, CACHE_TTL, MAX_ACTIVE_PER_USER
from database import redis

logger = logging.getLogger("cohort.redis")
ACTIVE_STATUSES = ["queued", "processing", "enriching"]


async def acquire_slot(user_id: str, collection) -> bool:
    key = f"active:{user_id}"
    try:
        async with redis.pipeline(transaction=True) as pipe:
            count, _ = await pipe.incr(key).expire(key, ACTIVE_KEY_TTL).execute()
        if count > MAX_ACTIVE_PER_USER:
            await redis.decr(key)
            return False
        return True
    except RedisError:
        logger.warning("Redis unavailable, counting active jobs in MongoDB")
        active = await collection.count_documents(
            {"user_id": user_id, "status": {"$in": ACTIVE_STATUSES}}
        )
        return active < MAX_ACTIVE_PER_USER


async def release_slot(user_id: str) -> None:
    key = f"active:{user_id}"
    try:
        if await redis.decr(key) < 0:
            await redis.set(key, 0, ex=ACTIVE_KEY_TTL)
    except RedisError:
        logger.warning("Redis unavailable, could not release slot for %s", user_id)


async def get_cached_result(content_hash: str, collection) -> Optional[dict]:
    try:
        value = await redis.get(f"cache:{content_hash}")
        return json.loads(value) if value else None
    except RedisError:
        logger.warning("Redis unavailable, looking up a processed copy in MongoDB")
    doc = await collection.find_one({"content_hash": content_hash, "status": "completed"})
    if doc is None:
        return None
    return {"summary": doc["stages"]["processing"]["summary"], "tags": doc["stages"]["enriching"]["tags"]}


async def set_cached_result(content_hash: str, summary: str, tags: list[str]) -> None:
    try:
        await redis.set(
            f"cache:{content_hash}", json.dumps({"summary": summary, "tags": tags}), ex=CACHE_TTL
        )
    except RedisError:
        logger.warning("Redis unavailable, result not cached")
