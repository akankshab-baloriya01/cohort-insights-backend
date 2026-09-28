import asyncio
from datetime import timedelta

from bson import ObjectId

from app.domain import utcnow
from app.services.stages import mock_summary, mock_tags
from tests.conftest import Env


async def test_two_stage_happy_path(env: Env) -> None:
    content = "Solar panels convert sunlight into electricity for homes"
    doc_id = (await env.submit(content=content)).json()["document_id"]
    worker = env.worker()

    assert await worker.run_once()  # stage 1
    mid = (await env.get(doc_id)).json()
    assert mid["status"] == "enriching"
    assert mid["stages"]["processing"]["state"] == "succeeded"
    assert mid["stages"]["enriching"]["state"] == "pending"
    assert mid["result"] is None  # nothing is published until both stages finish

    assert await worker.run_once()  # stage 2
    done = (await env.get(doc_id)).json()
    assert done["status"] == "completed"
    summary = mock_summary(content)
    assert done["result"]["summary"] == summary
    assert done["result"]["tags"] == mock_tags(summary)
    assert done["result"]["content_version"] == 1
    assert done["result"]["is_current"] is True
    assert done["result"]["source"] == "pipeline"


async def test_enrichment_failure_preserves_stage1_and_retry_resumes_there(env_factory) -> None:
    async with env_factory(stage_max_attempts=1) as env:
        env.executor.fail["enriching"] = [True]
        doc_id = (await env.submit(content="alpha beta gamma")).json()["document_id"]
        await env.drain()

        failed = (await env.get(doc_id)).json()
        assert failed["status"] == "failed"
        assert failed["failed_stage"] == "enriching"
        assert failed["stages"]["processing"]["state"] == "succeeded"
        assert failed["stages"]["enriching"]["state"] == "failed"
        assert "scripted enriching failure" in failed["stages"]["enriching"]["error"]

        resp = await env.client.post(f"/documents/{doc_id}/retry", headers={"X-User-Id": "alice"})
        assert resp.status_code == 202
        assert resp.json()["status"] == "enriching"
        await env.drain()

        done = (await env.get(doc_id)).json()
        assert done["status"] == "completed"
        assert env.executor.calls == {"processing": 1, "enriching": 2}  # stage 1 never redone


async def test_processing_failure_is_reported_as_processing(env_factory) -> None:
    async with env_factory(stage_max_attempts=1) as env:
        env.executor.fail["processing"] = [True]
        doc_id = (await env.submit()).json()["document_id"]
        await env.drain()
        body = (await env.get(doc_id)).json()
        assert body["status"] == "failed"
        assert body["failed_stage"] == "processing"
        assert body["stages"]["enriching"]["state"] == "pending"
        assert env.executor.calls["enriching"] == 0


async def test_retry_only_allowed_for_failed(env: Env) -> None:
    doc_id = (await env.submit()).json()["document_id"]
    resp = await env.client.post(f"/documents/{doc_id}/retry", headers={"X-User-Id": "alice"})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "not_failed"


async def test_automatic_retry_with_backoff(env_factory) -> None:
    async with env_factory(stage_max_attempts=3, retry_backoff_base_seconds=30, retry_backoff_max_seconds=60) as env:
        env.executor.fail["processing"] = [True]
        doc_id = (await env.submit()).json()["document_id"]
        worker = env.worker()
        await worker.run_once()

        body = (await env.get(doc_id)).json()
        assert body["status"] == "processing"
        assert body["stages"]["processing"]["state"] == "retry_scheduled"
        # Backoff: the job is not runnable yet.
        raw = await env.collection.find_one({"_id": ObjectId(doc_id)})
        assert raw["next_run_at"] > utcnow() + timedelta(seconds=10)
        assert not await worker.run_once()

        await env.collection.update_one({"_id": ObjectId(doc_id)}, {"$set": {"next_run_at": utcnow()}})
        await env.drain()
        body = (await env.get(doc_id)).json()
        assert body["status"] == "completed"
        assert body["stages"]["processing"]["attempts"] == 2


async def test_two_workers_cannot_claim_the_same_job(env: Env) -> None:
    await env.submit()
    workers = [env.worker(f"w{i}") for i in range(5)]
    jobs = await asyncio.gather(*(w.claim() for w in workers))
    assert sum(job is not None for job in jobs) == 1


async def test_expired_lease_is_reclaimed_and_old_worker_is_fenced(env: Env) -> None:
    doc_id = (await env.submit(content="lease test")).json()["document_id"]
    w1, w2 = env.worker("w1"), env.worker("w2")
    job1 = await w1.claim()
    assert job1 is not None and await w2.claim() is None  # lease held

    # Simulate w1 stalling past its lease.
    await env.collection.update_one(
        {"_id": ObjectId(doc_id)}, {"$set": {"lease.expires_at": utcnow() - timedelta(seconds=1)}}
    )
    job2 = await w2.claim()
    assert job2 is not None and job2.stage == job1.stage

    await w1.execute(job1)  # late write from the stale worker is dropped
    raw = await env.collection.find_one({"_id": ObjectId(doc_id)})
    assert raw["status"] == "processing" and raw["lease"]["owner"] == "w2"

    await w2.execute(job2)
    assert (await env.get(doc_id)).json()["status"] == "enriching"
