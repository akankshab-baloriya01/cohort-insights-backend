from bson import ObjectId

from tests.conftest import Env


async def test_health_ok(env: Env) -> None:
    resp = await env.client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "checks": {"mongo": "ok", "redis": "ok"}}


async def test_submit_returns_201_queued(env: Env) -> None:
    resp = await env.submit(content="hello world")
    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "queued"
    assert body["content_version"] == 1
    assert body["outcome"] == "created"
    assert ObjectId.is_valid(body["document_id"])


async def test_submit_validation_error_is_422(env: Env) -> None:
    resp = await env.client.post("/documents", json={"user_id": "alice", "title": "", "content": "x"})
    assert resp.status_code == 422


async def test_read_requires_identity(env: Env) -> None:
    doc_id = (await env.submit()).json()["document_id"]
    assert (await env.client.get(f"/documents/{doc_id}")).status_code == 401


async def test_poll_shows_both_stages(env: Env) -> None:
    doc_id = (await env.submit()).json()["document_id"]
    body = (await env.get(doc_id)).json()
    assert body["status"] == "queued"
    assert body["stages"]["processing"]["state"] == "pending"
    assert body["stages"]["enriching"]["state"] == "pending"
    assert body["failed_stage"] is None
    assert body["result"] is None


async def test_foreign_document_indistinguishable_from_missing(env: Env) -> None:
    doc_id = (await env.submit(user="alice")).json()["document_id"]
    foreign = await env.get(doc_id, user="mallory")
    missing = await env.get(str(ObjectId()), user="mallory")
    malformed = await env.get("not-an-object-id", user="mallory")
    assert foreign.status_code == missing.status_code == malformed.status_code == 404
    assert foreign.json() == missing.json() == malformed.json()
    # Also not patchable or retryable by a non-owner.
    assert (await env.patch(doc_id, "new", user="mallory")).status_code == 404
    retry = await env.client.post(f"/documents/{doc_id}/retry", headers={"X-User-Id": "mallory"})
    assert retry.status_code == 404


async def test_list_pagination_newest_first_and_status_filter(env_factory) -> None:
    async with env_factory(max_active_docs_per_user=10) as env:
        ids = [(await env.submit(content=f"doc number {i}")).json()["document_id"] for i in range(2)]
        await env.drain()  # the oldest two are completed
        ids += [(await env.submit(content=f"doc number {i}")).json()["document_id"] for i in range(2, 5)]
        await env.submit(user="bob", content="bob's doc")

        headers = {"X-User-Id": "alice"}
        page1 = (await env.client.get("/users/alice/documents?page=1&page_size=2", headers=headers)).json()
        page3 = (await env.client.get("/users/alice/documents?page=3&page_size=2", headers=headers)).json()
        assert [i["document_id"] for i in page1["items"]] == [ids[4], ids[3]]
        assert page1["total"] == 5 and page1["has_next"] is True
        assert [i["document_id"] for i in page3["items"]] == [ids[0]]
        assert page3["has_next"] is False

        done = (await env.client.get("/users/alice/documents?status=completed", headers=headers)).json()
        assert {i["document_id"] for i in done["items"]} == {ids[0], ids[1]}
        assert all(i["result_is_current"] for i in done["items"])


async def test_list_is_owner_scoped(env: Env) -> None:
    await env.submit(user="alice")
    resp = await env.client.get("/users/alice/documents", headers={"X-User-Id": "mallory"})
    assert resp.status_code == 404
    own = await env.client.get("/users/mallory/documents", headers={"X-User-Id": "mallory"})
    assert own.json()["items"] == []


async def test_list_rejects_bad_query(env: Env) -> None:
    headers = {"X-User-Id": "alice"}
    assert (await env.client.get("/users/alice/documents?status=bogus", headers=headers)).status_code == 422
    assert (await env.client.get("/users/alice/documents?page=0", headers=headers)).status_code == 422


async def test_request_id_is_echoed(env: Env) -> None:
    resp = await env.client.get("/health", headers={"X-Request-ID": "abc123"})
    assert resp.headers["x-request-id"] == "abc123"
