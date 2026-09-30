import asyncio
from datetime import datetime, timezone

from conftest import submit, wait_done
from services import pipeline
from services.create_document import content_fields, ref_query


async def test_health(api):
    response = await api.get("/health")
    assert response.status_code == 200
    assert response.json() == {"mongodb": "ok", "redis": "ok"}


async def test_validation_rejects_blank_content(api):
    response = await submit(api, "val", "   ")
    assert response.status_code == 422


async def test_pipeline_completes_both_stages(api):
    response = await submit(api, "alice", "Mongo and Redis power the insights pipeline")
    assert response.status_code == 201
    assert response.json()["status"] == "queued"
    doc = await wait_done(api, "alice", response.json()["document_id"])
    assert doc["status"] == "completed"
    assert doc["summary"] and doc["tags"]
    assert doc["stages"]["processing"]["content_version"] == doc["content_version"] == 1
    assert doc["stages"]["enriching"]["content_version"] == 1


async def test_other_user_gets_same_404_as_missing(api):
    document_id = (await submit(api, "owner", "private notes")).json()["document_id"]
    other = await api.get(f"/documents/{document_id}", headers={"X-User-Id": "intruder"})
    missing = await api.get("/documents/000000000000000000000000", headers={"X-User-Id": "intruder"})
    assert other.status_code == missing.status_code == 404
    assert other.json() == missing.json()
    listing = await api.get("/users/owner/documents", headers={"X-User-Id": "intruder"})
    assert listing.status_code == 404


async def test_patch_hides_old_result_and_reprocesses(api):
    document_id = (await submit(api, "bob", "original content about apples")).json()["document_id"]
    await wait_done(api, "bob", document_id)
    patched = await api.patch(
        f"/documents/{document_id}", json={"content": "fresh content about oranges"}, headers={"X-User-Id": "bob"}
    )
    body = patched.json()
    assert patched.status_code == 200
    assert body["content_version"] == 2 and body["status"] == "queued"
    assert body["summary"] is None and body["tags"] is None
    assert body["stages"]["processing"]["summary"] is None and body["stages"]["enriching"]["tags"] is None
    doc = await wait_done(api, "bob", document_id)
    assert "oranges" in doc["summary"] and "apples" not in doc["summary"]
    assert doc["stages"]["processing"]["content_version"] == doc["stages"]["enriching"]["content_version"] == 2


async def test_in_flight_result_for_old_version_is_dropped(api, collection):
    now = datetime.now(timezone.utc)
    doc = {"user_id": "carol", "title": "t", "created_at": now, **content_fields("old text here", 1, None)}
    doc["status"], doc["stages"]["processing"]["state"] = "processing", "running"
    doc["_id"] = (await collection.insert_one(doc)).inserted_id
    await api.patch(f"/documents/{doc['_id']}", json={"content": "new text here"}, headers={"X-User-Id": "carol"})
    await pipeline.run_stage(collection, "processing", doc)
    stored = await collection.find_one({"_id": doc["_id"]})
    assert stored["content_version"] == 2
    assert stored["stages"]["processing"]["content_version"] in (None, 2)
    assert stored["stages"]["processing"]["summary"] != "old text here"


async def test_enriching_failure_keeps_summary(api, collection, monkeypatch):
    now = datetime.now(timezone.utc)
    doc = {"user_id": "dave", "title": "t", "created_at": now, **content_fields("enrich me please", 1, None)}
    doc["status"] = "enriching"
    doc["stages"]["processing"].update(state="completed", content_version=1, summary="enrich me please")
    doc["stages"]["enriching"]["state"] = "running"
    doc["_id"] = (await collection.insert_one(doc)).inserted_id
    monkeypatch.setattr(pipeline, "FAILURE_RATE", 1.0)
    monkeypatch.setattr(pipeline, "STAGE_TIME_SCALE", 0.001)
    await pipeline.run_stage(collection, "enriching", doc)
    body = (await api.get(f"/documents/{doc['_id']}", headers={"X-User-Id": "dave"})).json()
    assert body["status"] == "failed"
    assert body["stages"]["enriching"]["state"] == "failed"
    assert body["stages"]["enriching"]["attempts"] == pipeline.MAX_ATTEMPTS
    assert body["stages"]["processing"]["state"] == "completed"
    assert body["stages"]["processing"]["summary"] == "enrich me please"


async def test_concurrent_patches_one_wins(api):
    document_id = (await submit(api, "erin", "racing content")).json()["document_id"]
    headers = {"X-User-Id": "erin"}
    results = await asyncio.gather(
        api.patch(f"/documents/{document_id}", json={"content": "A", "expected_version": 1}, headers=headers),
        api.patch(f"/documents/{document_id}", json={"content": "B", "expected_version": 1}, headers=headers),
    )
    assert sorted(r.status_code for r in results) == [200, 409]
    loser = next(r for r in results if r.status_code == 409)
    assert loser.json()["detail"]["content_version"] == 2


async def test_rate_limit_counts_all_active_stages(api):
    codes = [(await submit(api, "frank", f"doc number {i}")).status_code for i in range(4)]
    assert codes == [201, 201, 201, 429]


async def test_identical_content_served_from_cache(api):
    first = (await submit(api, "gina", "cache this exact text")).json()
    done = await wait_done(api, "gina", first["document_id"])
    second = await submit(api, "hank", "cache this exact text")
    assert second.status_code == 201 and second.json()["status"] == "completed"
    cached = (await api.get(f"/documents/{second.json()['document_id']}", headers={"X-User-Id": "hank"})).json()
    assert cached["summary"] == done["summary"] and cached["tags"] == done["tags"]


async def test_crosswalk_repeat_submissions(api, collection):
    first = await submit(api, "ivy", "partner content", ref="cms-1")
    assert first.status_code == 201
    retry = await submit(api, "ivy", "partner content", ref="cms-1")
    assert retry.status_code == 200
    assert retry.json()["document_id"] == first.json()["document_id"]
    assert (await submit(api, "ivy", "different content", ref="cms-1")).status_code == 409
    assert (await submit(api, "jack", "partner content", ref="cms-1")).status_code == 409
    found = await api.get("/documents/by-ref/cms-1", headers={"X-User-Id": "ivy"})
    assert found.status_code == 200 and found.json()["document_id"] == first.json()["document_id"]
    assert (await api.get("/documents/by-ref/cms-1", headers={"X-User-Id": "jack"})).status_code == 404
    assert (await api.get("/documents/by-ref/missing", headers={"X-User-Id": "ivy"})).status_code == 404
    plan = await collection.find({**ref_query("cms-1"), "user_id": "ivy"}).explain()
    assert "client_doc_ref_unique" in str(plan["queryPlanner"]["winningPlan"])


async def test_list_is_scoped_and_filtered(api):
    for i in range(2):
        await submit(api, "kim", f"kim doc {i}")
    listing = await api.get("/users/kim/documents?page=1&page_size=1", headers={"X-User-Id": "kim"})
    assert listing.status_code == 200 and len(listing.json()) == 1
    assert listing.json()[0]["content"] == "kim doc 1"
    failed = await api.get("/users/kim/documents?status=failed", headers={"X-User-Id": "kim"})
    assert failed.json() == []
