import asyncio
import logging
import random
import re
from collections import Counter
from datetime import datetime, timezone

from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

from config import FAILURE_RATE, MAX_ATTEMPTS, STAGE_TIME_SCALE, WORKER_COUNT
from services.redis_store import release_slot, set_cached_result

logger = logging.getLogger("cohort.pipeline")
STAGES = {
    "processing": {"claim_status": "queued", "next": "enriching", "seconds": (10, 20)},
    "enriching": {"claim_status": "enriching", "next": "completed", "seconds": (5, 15)},
}


def pending_stages() -> dict:
    base = {"state": "pending", "content_version": None, "attempts": 0, "error": None}
    return {"processing": {**base, "summary": None}, "enriching": {**base, "tags": None}}


def completed_stages(version: int, summary: str, tags: list[str]) -> dict:
    base = {"state": "completed", "content_version": version, "attempts": 0, "error": None}
    return {"processing": {**base, "summary": summary}, "enriching": {**base, "tags": tags}}


def summarize(content: str) -> str:
    words = content.split()
    return " ".join(words[:25]) + (" ..." if len(words) > 25 else "")


def make_tags(summary: str) -> list[str]:
    words = re.findall(r"[a-z]{4,}", summary.lower())
    return [word for word, _ in Counter(words).most_common(5)]


def stage_output(name: str, doc: dict) -> dict:
    if name == "processing":
        return {"summary": summarize(doc["content"])}
    return {"tags": make_tags(doc["stages"]["processing"]["summary"])}


async def claim(collection, name: str):
    return await collection.find_one_and_update(
        {"status": STAGES[name]["claim_status"], f"stages.{name}.state": "pending"},
        {"$set": {"status": name, f"stages.{name}.state": "running", "updated_at": now()}},
        sort=[("updated_at", 1)],
        return_document=ReturnDocument.AFTER,
    )


def now() -> datetime:
    return datetime.now(timezone.utc)


async def write_stage(collection, name: str, doc: dict, status: str, fields: dict) -> bool:
    guard = {"_id": doc["_id"], "content_version": doc["content_version"], f"stages.{name}.state": "running"}
    update = {f"stages.{name}.{key}": value for key, value in fields.items()}
    result = await collection.update_one(guard, {"$set": {"status": status, "updated_at": now(), **update}})
    if result.modified_count == 0:
        logger.info("Dropped %s result for %s: content changed", name, doc["_id"])
        return False
    if status in ("completed", "failed"):
        await release_slot(doc["user_id"])
    return True


async def run_stage(collection, name: str, doc: dict) -> None:
    low, high = STAGES[name]["seconds"]
    for attempt in range(1, MAX_ATTEMPTS + 1):
        await asyncio.sleep(random.uniform(low, high) * STAGE_TIME_SCALE)
        if random.random() >= FAILURE_RATE:
            output = stage_output(name, doc)
            fields = {"state": "completed", "content_version": doc["content_version"], "attempts": attempt, "error": None, **output}
            next_status = STAGES[name]["next"]
            if await write_stage(collection, name, doc, next_status, fields) and next_status == "completed":
                await set_cached_result(doc["content_hash"], doc["stages"]["processing"]["summary"], output["tags"])
            return
        logger.warning("Stage %s failed for %s (attempt %d)", name, doc["_id"], attempt)
        if attempt < MAX_ATTEMPTS:
            if not await write_stage(collection, name, doc, name, {"attempts": attempt, "error": "simulated failure"}):
                return
            await asyncio.sleep(2 ** (attempt - 1) * STAGE_TIME_SCALE)
    await write_stage(collection, name, doc, "failed", {"state": "failed", "attempts": MAX_ATTEMPTS, "error": "simulated failure"})


async def worker(collection) -> None:
    while True:
        try:
            doc = await claim(collection, "enriching") or await claim(collection, "processing")
            if doc is None:
                await asyncio.sleep(0.5)
                continue
            await run_stage(collection, doc["status"], doc)
        except PyMongoError:
            logger.exception("Worker database error")
            await asyncio.sleep(1)


async def start_workers(collection) -> list[asyncio.Task]:
    await collection.update_many(
        {"status": "processing"}, {"$set": {"status": "queued", "stages.processing.state": "pending"}}
    )
    await collection.update_many(
        {"status": "enriching", "stages.enriching.state": "running"},
        {"$set": {"stages.enriching.state": "pending"}},
    )
    return [asyncio.create_task(worker(collection)) for _ in range(WORKER_COUNT)]
