import asyncio
import os

os.environ.setdefault("MONGO_DB", "cohort_test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/15")
os.environ.setdefault("STAGE_TIME_SCALE", "0.05")
os.environ.setdefault("FAILURE_RATE", "0")

import httpx
import pytest

from database import client, document_collection, redis
from main import app


@pytest.fixture(scope="session")
async def api():
    await client.drop_database(os.environ["MONGO_DB"])
    await redis.flushdb()
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
            yield http


@pytest.fixture
def collection():
    return document_collection


async def submit(api, user, content, ref=None):
    body = {"user_id": user, "title": "t", "content": content}
    if ref:
        body["client_doc_ref"] = ref
    return await api.post("/documents", json=body, headers={"X-User-Id": user})


async def wait_done(api, user, document_id, timeout=10):
    for _ in range(int(timeout / 0.1)):
        body = (await api.get(f"/documents/{document_id}", headers={"X-User-Id": user})).json()
        if body["status"] in ("completed", "failed"):
            return body
        await asyncio.sleep(0.1)
    raise AssertionError(f"document {document_id} did not finish")
