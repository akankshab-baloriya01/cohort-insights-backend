"""Per-user active-pipeline limit, content-hash cache, and Redis-outage degradation."""

from app.services.stages import mock_summary
from tests.conftest import Env


async def test_fourth_active_document_is_rejected(env: Env) -> None:
    for i in range(3):
        assert (await env.submit(content=f"c{i}")).status_code == 201
    resp = await env.submit(content="c3")
    assert resp.status_code == 429
    assert resp.json()["error"]["code"] == "rate_limited"
    # Other users are unaffected.
    assert (await env.submit(user="bob", content="c3")).status_code == 201


async def test_limit_counts_all_active_stages(env: Env) -> None:
    for i in range(3):
        await env.submit(content=f"c{i}")
    worker = env.worker()
    for _ in range(3):
        await worker.run_once()  # all three now "enriching", still active
    assert (await env.submit(content="c3")).status_code == 429


async def test_slot_freed_on_completion_and_on_failure(env_factory) -> None:
    async with env_factory(stage_max_attempts=1) as env:
        env.executor.fail["processing"] = [True]
        for i in range(3):
            await env.submit(content=f"c{i}")
        await env.drain()  # one failed, two completed
        for i in range(3, 6):
            assert (await env.submit(content=f"c{i}")).status_code == 201


async def test_patch_of_idle_document_needs_a_slot(env: Env) -> None:
    done_id = (await env.submit(content="finished")).json()["document_id"]
    await env.drain()
    for i in range(3):
        await env.submit(content=f"active {i}")
    assert (await env.patch(done_id, "brand new text")).status_code == 429


async def test_patch_of_active_document_reuses_its_slot(env: Env) -> None:
    ids = [(await env.submit(content=f"c{i}")).json()["document_id"] for i in range(3)]
    assert (await env.patch(ids[0], "changed")).status_code == 200
    assert (await env.submit(content="c4")).status_code == 429  # still exactly 3 held
    await env.drain()
    assert (await env.submit(content="c4")).status_code == 201


async def test_identical_content_served_from_cache_immediately(env: Env) -> None:
    content = "Shared content that has been processed before"
    await env.submit(user="alice", content=content)
    await env.drain()

    # bob is at his limit, but a cache hit never enters the pipeline.
    for i in range(3):
        await env.submit(user="bob", content=f"bob {i}")
    resp = await env.submit(user="bob", content=content)
    assert resp.status_code == 201
    assert resp.json()["status"] == "completed"
    assert resp.json()["served_from_cache"] is True

    body = (await env.get(resp.json()["document_id"], user="bob")).json()
    assert body["result"]["summary"] == mock_summary(content)
    assert body["result"]["source"] == "cache"
    assert body["result"]["is_current"] is True
    assert body["stages"]["processing"]["state"] == "skipped"


async def test_cache_is_keyed_by_content_across_patch(env: Env) -> None:
    x, y, z = "content X about rivers", "content Y about mountains", "content Z about deserts"
    await env.submit(content=x)
    doc_id = (await env.submit(content=y)).json()["document_id"]
    await env.drain()

    # PATCH to content processed elsewhere: X's result, attributed to this document's new version.
    body = (await env.patch(doc_id, x)).json()
    assert body["status"] == "completed"
    assert body["content_version"] == 2
    assert body["result"]["summary"] == mock_summary(x)
    assert body["result"]["content_version"] == 2 and body["result"]["is_current"] is True

    # PATCH to never-seen content: the old (X) result must not be presented as Z's.
    body = (await env.patch(doc_id, z)).json()
    assert body["status"] == "queued"
    assert body["result"]["is_current"] is False
    await env.drain()
    body = (await env.get(doc_id)).json()
    assert body["result"]["summary"] == mock_summary(z)

    # PATCH back to Y: served from cache as Y's result, not X's or Z's.
    body = (await env.patch(doc_id, y)).json()
    assert body["result"]["summary"] == mock_summary(y)


async def test_cache_survives_redis_eviction_via_mongo_tier(env: Env) -> None:
    content = "persisted content"
    await env.submit(content=content)
    await env.drain()
    await env.resources.redis.flushdb()
    resp = await env.submit(user="bob", content=content)
    assert resp.json()["served_from_cache"] is True


async def test_redis_outage_degrades_gracefully(env_factory) -> None:
    async with env_factory(redis_url="redis://127.0.0.1:1/0", redis_timeout_seconds=0.2) as env:
        health = await env.client.get("/health")
        assert health.status_code == 200
        assert health.json() == {"status": "degraded", "checks": {"mongo": "ok", "redis": "unavailable"}}

        # Rate limit falls back to a MongoDB count.
        ids = [(await env.submit(content=f"c{i}")).json()["document_id"] for i in range(3)]
        assert (await env.submit(content="c3")).status_code == 429

        # Pipeline and cache (Mongo tier) still work.
        await env.drain()
        assert (await env.get(ids[0])).json()["status"] == "completed"
        resp = await env.submit(user="bob", content="c0")
        assert resp.json()["served_from_cache"] is True
