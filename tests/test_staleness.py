"""Staleness on update: results are always attributable to exactly one content_version."""

import asyncio
from typing import Any

from bson import ObjectId

from app.services.stages import mock_summary, mock_tags
from tests.conftest import Env

V1 = "Original text about volcanoes and lava flows"
V2 = "Rewritten text about glaciers and ice sheets"


def assert_consistent(body: dict[str, Any], contents_by_version: dict[int, str]) -> None:
    """Invariant a reader relies on: summary AND tags both derive from result.content_version's content."""
    result = body["result"]
    if result is None:
        return
    source = contents_by_version[result["content_version"]]
    assert result["summary"] == mock_summary(source)
    assert result["tags"] == mock_tags(result["summary"])
    assert result["is_current"] == (result["content_version"] == body["content_version"])
    if result["is_current"]:
        assert result["content_hash"] == body["content_hash"]


async def test_patch_makes_old_result_visibly_not_current(env: Env) -> None:
    doc_id = (await env.submit(content=V1)).json()["document_id"]
    await env.drain()

    resp = await env.patch(doc_id, V2)
    assert resp.status_code == 200
    body = resp.json()
    assert body["content_version"] == 2
    assert body["status"] == "queued"
    assert body["stages"]["processing"]["state"] == "pending"
    assert body["result"]["content_version"] == 1
    assert body["result"]["is_current"] is False  # never presented as current
    assert body["result"]["content_hash"] != body["content_hash"]
    assert_consistent(body, {1: V1, 2: V2})

    await env.drain()
    body = (await env.get(doc_id)).json()
    assert body["result"]["content_version"] == 2 and body["result"]["is_current"] is True
    assert_consistent(body, {1: V1, 2: V2})


async def test_inflight_stage1_for_old_content_is_discarded(env: Env) -> None:
    doc_id = (await env.submit(content=V1)).json()["document_id"]
    worker = env.worker()
    job = await worker.claim()  # processing v1 is "in flight"

    assert (await env.patch(doc_id, V2)).status_code == 200
    await worker.execute(job)  # finishes after the PATCH

    raw = await env.collection.find_one({"_id": ObjectId(doc_id)})
    assert raw["content_version"] == 2
    assert raw["status"] == "queued"
    assert raw["draft"] is None  # v1 summary was not written

    await env.drain()
    assert_consistent((await env.get(doc_id)).json(), {1: V1, 2: V2})
    assert (await env.get(doc_id)).json()["result"]["content_version"] == 2


async def test_old_summary_never_combined_with_new_tags(env: Env) -> None:
    """PATCH lands between stage 1 (v1 summary) and stage 2: v1's enrichment must not publish."""
    doc_id = (await env.submit(content=V1)).json()["document_id"]
    worker = env.worker()
    await worker.run_once()  # stage 1 for v1 -> draft(v1)
    enrich_job = await worker.claim()
    assert enrich_job is not None and enrich_job.version == 1

    assert (await env.patch(doc_id, V2)).status_code == 200
    await worker.execute(enrich_job)  # would have published v1 summary + tags

    body = (await env.get(doc_id)).json()
    assert body["result"] is None
    assert body["status"] == "queued"

    await env.drain()
    body = (await env.get(doc_id)).json()
    assert body["result"]["summary"] == mock_summary(V2)
    assert_consistent(body, {1: V1, 2: V2})


async def test_polling_during_repeated_patches_never_sees_mixed_versions(env: Env) -> None:
    contents = {1: "one apple banana", 2: "two cherry durian", 3: "three elder fig", 4: "four grape honeydew"}
    doc_id = (await env.submit(content=contents[1])).json()["document_id"]
    worker = env.worker()
    for version in (2, 3, 4):
        await worker.run_once()
        assert_consistent((await env.get(doc_id)).json(), contents)
        await env.patch(doc_id, contents[version])
        assert_consistent((await env.get(doc_id)).json(), contents)
        await worker.run_once()
        assert_consistent((await env.get(doc_id)).json(), contents)
    await env.drain()
    final = (await env.get(doc_id)).json()
    assert final["result"]["content_version"] == 4
    assert_consistent(final, contents)


async def test_racing_patches_with_expected_version_exactly_one_wins(env: Env) -> None:
    doc_id = (await env.submit(content=V1)).json()["document_id"]
    a, b = await asyncio.gather(
        env.patch(doc_id, "writer A content", expected_version=1),
        env.patch(doc_id, "writer B content", expected_version=1),
    )
    assert sorted([a.status_code, b.status_code]) == [200, 409]
    loser = a if a.status_code == 409 else b
    assert loser.json()["error"]["code"] == "version_conflict"
    body = (await env.get(doc_id)).json()
    assert body["content_version"] == 2
    winner_content = "writer A content" if a.status_code == 200 else "writer B content"
    assert body["content"] == winner_content


async def test_racing_patches_without_expected_version_serialize(env: Env) -> None:
    doc_id = (await env.submit(content=V1)).json()["document_id"]
    a, b = await asyncio.gather(env.patch(doc_id, "A"), env.patch(doc_id, "B"))
    assert a.status_code == b.status_code == 200
    versions = sorted([a.json()["content_version"], b.json()["content_version"]])
    assert versions == [2, 3]  # each write got its own version; none was lost silently
    body = (await env.get(doc_id)).json()
    assert body["content_version"] == 3


async def test_patch_with_identical_content_is_a_noop(env: Env) -> None:
    doc_id = (await env.submit(content=V1)).json()["document_id"]
    await env.drain()
    body = (await env.patch(doc_id, V1)).json()
    assert body["content_version"] == 1
    assert body["status"] == "completed"


async def test_patch_stale_expected_version_is_409(env: Env) -> None:
    doc_id = (await env.submit(content=V1)).json()["document_id"]
    await env.patch(doc_id, V2)
    resp = await env.patch(doc_id, "third", expected_version=1)
    assert resp.status_code == 409
