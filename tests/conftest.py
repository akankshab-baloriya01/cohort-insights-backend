"""Integration fixtures: real MongoDB + Redis, in-process API (httpx.AsyncClient) and in-process worker.

Connection targets come from TEST_MONGO_URI / TEST_REDIS_URL, falling back to MONGO_URI / REDIS_URL
(with Redis DB 15), so `docker-compose run --rm api pytest` works without extra configuration.
Each test gets its own Mongo database and a flushed Redis DB.
"""

import os
import re
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis

from app.config import Settings
from app.main import create_app
from app.resources import Resources
from app.services.stages import StageExecutor, StageFailure
from app.worker import Worker

MONGO_URI = os.environ.get("TEST_MONGO_URI") or os.environ.get("MONGO_URI", "mongodb://localhost:27017")
REDIS_URL = os.environ.get("TEST_REDIS_URL") or re.sub(
    r"/\d+$", "/15", os.environ.get("REDIS_URL", "redis://localhost:6379/0")
)


class ScriptedExecutor(StageExecutor):
    """Zero-latency stages whose failures are scripted per stage: `fail["enriching"] = [True, False]`."""

    def __init__(self) -> None:
        super().__init__((0, 0), (0, 0), failure_rate=0.0)
        self.fail: dict[str, list[bool]] = {"processing": [], "enriching": []}
        self.calls: dict[str, int] = {"processing": 0, "enriching": 0}

    async def _simulate(self, bounds: tuple[float, float], stage: str) -> None:
        self.calls[stage] += 1
        plan = self.fail[stage]
        if plan and plan.pop(0):
            raise StageFailure(f"scripted {stage} failure")


@dataclass
class Env:
    client: AsyncClient
    resources: Resources
    executor: ScriptedExecutor

    def worker(self, worker_id: str = "test-worker") -> Worker:
        return Worker(self.resources, self.executor, worker_id=worker_id)

    async def drain(self, max_steps: int = 50) -> None:
        worker = self.worker()
        for _ in range(max_steps):
            if not await worker.run_once():
                return
        raise AssertionError("pipeline did not settle")

    @property
    def collection(self) -> Any:
        return self.resources.repo.col

    async def submit(self, user: str = "alice", content: str = "some content", **extra: Any) -> Any:
        body = {"user_id": user, "title": "t", "content": content, **extra}
        return await self.client.post("/documents", json=body)

    async def get(self, doc_id: str, user: str = "alice") -> Any:
        return await self.client.get(f"/documents/{doc_id}", headers={"X-User-Id": user})

    async def patch(self, doc_id: str, content: str, user: str = "alice", **extra: Any) -> Any:
        return await self.client.patch(
            f"/documents/{doc_id}", json={"content": content, **extra}, headers={"X-User-Id": user}
        )


def make_settings(**overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "mongo_uri": MONGO_URI,
        "mongo_db": f"cia_test_{uuid.uuid4().hex[:10]}",
        "redis_url": REDIS_URL,
        "processing_min_seconds": 0,
        "processing_max_seconds": 0,
        "enriching_min_seconds": 0,
        "enriching_max_seconds": 0,
        "stage_failure_rate": 0,
        "retry_backoff_base_seconds": 0,
        "retry_backoff_max_seconds": 0,
        "worker_poll_interval_seconds": 0.05,
    }
    return Settings(**(base | overrides))


@asynccontextmanager
async def running_env(**overrides: Any) -> AsyncIterator[Env]:
    settings = make_settings(**overrides)
    app = create_app(settings)
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(app.router.lifespan_context(app))
        resources: Resources = app.state.resources
        client = await stack.enter_async_context(AsyncClient(transport=ASGITransport(app=app), base_url="http://test"))
        try:
            yield Env(client=client, resources=resources, executor=ScriptedExecutor())
        finally:
            await resources.mongo.drop_database(settings.mongo_db)


@pytest.fixture(autouse=True)
async def _flush_redis() -> AsyncIterator[None]:
    redis = Redis.from_url(REDIS_URL)
    await redis.flushdb()
    yield
    await redis.flushdb()
    await redis.aclose()


@pytest.fixture
async def env() -> AsyncIterator[Env]:
    async with running_env() as e:
        yield e


@pytest.fixture
def env_factory() -> Callable[..., Any]:
    """For tests that need non-default settings: `async with env_factory(stage_max_attempts=1) as env:`."""
    return running_env
