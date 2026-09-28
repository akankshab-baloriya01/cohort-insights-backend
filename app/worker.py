"""Pipeline worker: `python -m app.worker`.

MongoDB is the job queue. A worker leases one document at a time with an atomic
find_one_and_update, runs exactly one stage, and writes the outcome back with a
compare-and-set fenced on (content_version, lease token). Run as many worker
processes as you like; `WORKER_CONCURRENCY` bounds the stages each one runs at once.
"""

import asyncio
import logging
import random
import signal
import socket
import uuid
from dataclasses import dataclass
from datetime import timedelta

from bson import ObjectId
from pymongo.errors import PyMongoError

from app.config import Settings, get_settings
from app.domain import ResultSource, Stage, Status, build_result, utcnow
from app.logging_config import configure_logging
from app.resources import Resources, open_resources
from app.services.content_cache import CachedResult
from app.services.stages import StageExecutor, StageFailure

logger = logging.getLogger("app.worker")


@dataclass(frozen=True)
class Job:
    doc_id: ObjectId
    user_id: str
    version: int
    content_hash: str
    token: str
    stage: Stage
    attempt: int
    content: str
    draft_summary: str | None


class Worker:
    def __init__(self, resources: Resources, executor: StageExecutor, worker_id: str | None = None):
        self.repo = resources.repo
        self.limiter = resources.limiter
        self.cache = resources.cache
        self.settings: Settings = resources.settings
        self.executor = executor
        self.worker_id = worker_id or f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}"

    # ----- claiming --------------------------------------------------------

    async def claim(self) -> Job | None:
        token = uuid.uuid4().hex
        doc = await self.repo.claim_next(self.worker_id, token, utcnow(), self.settings.lease_seconds)
        if doc is None:
            return None
        stage = Stage.ENRICHING if doc["status"] == Status.ENRICHING else Stage.PROCESSING
        version = doc["content_version"]

        if doc["stages"][stage.value]["attempts"] >= self.settings.stage_max_attempts:
            # Previous attempts were lost (e.g. worker crashed mid-stage and its lease expired).
            await self._fail(doc["_id"], doc["user_id"], version, token, stage, "attempts exhausted")
            return None

        started = await self.repo.start_stage(doc["_id"], version, token, stage, utcnow())
        if started is None:
            return None  # superseded by a PATCH between claim and start
        await self.limiter.touch(doc["user_id"], doc["_id"], version)
        draft = started.get("draft") or {}
        return Job(
            doc_id=doc["_id"],
            user_id=doc["user_id"],
            version=version,
            content_hash=started["content_hash"],
            token=token,
            stage=stage,
            attempt=started["stages"][stage.value]["attempts"],
            content=started["content"],
            draft_summary=draft.get("summary") if draft.get("content_version") == version else None,
        )

    # ----- execution -------------------------------------------------------

    async def execute(self, job: Job) -> None:
        log = {"document_id": str(job.doc_id), "stage": job.stage.value, "version": job.version, "attempt": job.attempt}
        logger.info("stage started", extra=log)
        try:
            if job.stage is Stage.PROCESSING:
                summary = await self.executor.summarize(job.content)
                ok = await self.repo.complete_processing(
                    job.doc_id, job.version, job.token, job.content_hash, summary, utcnow()
                )
            else:
                if job.draft_summary is None:
                    raise StageFailure("no stage-1 summary for this content_version")
                tags = await self.executor.enrich(job.draft_summary)
                ok = await self._publish(job, job.draft_summary, tags)
        except StageFailure as exc:
            logger.warning("stage failed", extra=log | {"error": str(exc)})
            await self._handle_failure(job, str(exc))
            return
        except PyMongoError:
            # Could not record the outcome; the lease will expire and the stage will be re-run.
            logger.exception("stage result not persisted", extra=log)
            return
        except Exception as exc:
            # A bug in stage code must not kill the worker loop; record it on the document.
            logger.exception("stage crashed", extra=log)
            await self._handle_failure(job, f"internal error: {type(exc).__name__}")
            return

        if ok:
            logger.info("stage succeeded", extra=log)
        else:
            logger.info("stage result discarded: content changed or lease lost", extra=log)

    async def _publish(self, job: Job, summary: str, tags: list[str]) -> bool:
        now = utcnow()
        result = build_result(
            version=job.version,
            hash_=job.content_hash,
            summary=summary,
            tags=tags,
            source=ResultSource.PIPELINE,
            now=now,
        )
        if not await self.repo.complete_enriching(job.doc_id, job.version, job.token, result, now):
            return False
        await self.cache.store(CachedResult(job.content_hash, summary, tags))
        await self.limiter.release(job.user_id, job.doc_id, job.version)
        return True

    async def _handle_failure(self, job: Job, error: str) -> None:
        if job.attempt < self.settings.stage_max_attempts:
            delay = self._backoff(job.attempt)
            now = utcnow()
            await self.repo.schedule_retry(
                job.doc_id, job.version, job.token, job.stage, error, now + timedelta(seconds=delay), now
            )
            logger.info("stage retry scheduled", extra={"document_id": str(job.doc_id), "delay_s": round(delay, 2)})
        else:
            await self._fail(job.doc_id, job.user_id, job.version, job.token, job.stage, error)

    async def _fail(self, doc_id: ObjectId, user_id: str, version: int, token: str, stage: Stage, error: str) -> None:
        if await self.repo.fail_stage(doc_id, version, token, stage, error, utcnow()):
            await self.limiter.release(user_id, doc_id, version)
            logger.warning("document failed", extra={"document_id": str(doc_id), "stage": stage.value})

    def _backoff(self, attempt: int) -> float:
        base = self.settings.retry_backoff_base_seconds * (2 ** (attempt - 1))
        capped = min(base, self.settings.retry_backoff_max_seconds)
        return random.uniform(capped / 2, capped)  # jitter so retries don't synchronise

    # ----- loops -----------------------------------------------------------

    async def run_once(self) -> bool:
        """Claim and run a single stage. Returns False if nothing was runnable."""
        job = await self.claim()
        if job is None:
            return False
        await self.execute(job)
        return True

    async def run(self, stop: asyncio.Event) -> None:
        async def slot(index: int) -> None:
            while not stop.is_set():
                try:
                    worked = await self.run_once()
                except PyMongoError:
                    logger.exception("worker slot error", extra={"slot": index})
                    worked = False
                if not worked:
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=self.settings.worker_poll_interval_seconds)
                    except TimeoutError:
                        pass  # poll interval elapsed; look for work again

        logger.info("worker started", extra={"worker_id": self.worker_id, "slots": self.settings.worker_concurrency})
        await asyncio.gather(*(slot(i) for i in range(self.settings.worker_concurrency)))
        logger.info("worker stopped", extra={"worker_id": self.worker_id})


def build_executor(settings: Settings, rng: random.Random | None = None) -> StageExecutor:
    return StageExecutor(
        processing_range=(settings.processing_min_seconds, settings.processing_max_seconds),
        enriching_range=(settings.enriching_min_seconds, settings.enriching_max_seconds),
        failure_rate=settings.stage_failure_rate,
        rng=rng,
    )


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    resources = await open_resources(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await Worker(resources, build_executor(settings)).run(stop)
    finally:
        await resources.close()


if __name__ == "__main__":
    asyncio.run(main())
