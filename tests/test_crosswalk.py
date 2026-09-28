"""client_doc_ref crosswalk: lookup, repeat submissions, ordering, ownership, index usage."""

import asyncio
import json

from tests.conftest import Env


async def by_ref(env: Env, ref: str, user: str = "alice"):
    return await env.client.get(f"/documents/by-ref/{ref}", headers={"X-User-Id": user})


async def test_lookup_by_ref(env: Env) -> None:
    doc_id = (await env.submit(content="partner doc", client_doc_ref="cms-42")).json()["document_id"]
    resp = await by_ref(env, "cms-42")
    assert resp.status_code == 200
    assert resp.json()["document_id"] == doc_id
    assert (await by_ref(env, "cms-unknown")).status_code == 404


async def test_repeat_same_ref_same_content_is_idempotent(env: Env) -> None:
    first = await env.submit(content="same", client_doc_ref="cms-1")
    again = await env.submit(content="same", client_doc_ref="cms-1")
    assert first.status_code == 201
    assert again.status_code == 200
    assert again.json()["outcome"] == "unchanged"
    assert again.json()["document_id"] == first.json()["document_id"]
    assert again.json()["content_version"] == 1
    assert await env.collection.count_documents({}) == 1


async def test_repeat_same_ref_different_content_is_new_version(env: Env) -> None:
    first = (await env.submit(content="draft one", client_doc_ref="cms-1")).json()
    await env.drain()
    again = await env.submit(content="draft two", client_doc_ref="cms-1")
    assert again.status_code == 200
    body = again.json()
    assert body["outcome"] == "new_version"
    assert body["document_id"] == first["document_id"]
    assert body["content_version"] == 2
    assert body["status"] == "queued"

    doc = (await by_ref(env, "cms-1")).json()
    assert doc["content"] == "draft two"
    assert doc["result"]["is_current"] is False
    assert await env.collection.count_documents({}) == 1


async def test_out_of_order_ref_versions(env: Env) -> None:
    await env.submit(content="rev 2", client_doc_ref="cms-9", ref_version=2)

    older = await env.submit(content="rev 1", client_doc_ref="cms-9", ref_version=1)
    assert older.status_code == 409
    assert older.json()["error"]["code"] == "stale_ref_version"

    clash = await env.submit(content="rev 2 but different", client_doc_ref="cms-9", ref_version=2)
    assert clash.status_code == 409
    assert clash.json()["error"]["code"] == "ref_version_conflict"

    replay = await env.submit(content="rev 2", client_doc_ref="cms-9", ref_version=2)
    assert replay.status_code == 200 and replay.json()["outcome"] == "unchanged"

    newer = await env.submit(content="rev 3", client_doc_ref="cms-9", ref_version=3)
    assert newer.status_code == 200 and newer.json()["outcome"] == "new_version"

    doc = (await by_ref(env, "cms-9")).json()
    assert doc["content"] == "rev 3" and doc["ref_version"] == 3


async def test_concurrent_duplicate_submissions_create_one_document(env: Env) -> None:
    responses = await asyncio.gather(*(env.submit(content="dup", client_doc_ref="cms-dup") for _ in range(5)))
    codes = sorted(r.status_code for r in responses)
    assert codes == [200, 200, 200, 200, 201]
    assert len({r.json()["document_id"] for r in responses}) == 1
    assert await env.collection.count_documents({}) == 1


async def test_refs_are_scoped_to_their_owner(env: Env) -> None:
    alice_id = (await env.submit(user="alice", content="a", client_doc_ref="shared-ref")).json()["document_id"]
    assert (await by_ref(env, "shared-ref", user="bob")).status_code == 404

    bob = await env.submit(user="bob", content="b", client_doc_ref="shared-ref")
    assert bob.status_code == 201
    assert bob.json()["document_id"] != alice_id
    assert (await by_ref(env, "shared-ref", user="alice")).json()["content"] == "a"


async def test_documents_without_ref_do_not_collide(env: Env) -> None:
    assert (await env.submit(content="x")).status_code == 201
    assert (await env.submit(content="y")).status_code == 201


async def test_by_ref_lookup_uses_the_index(env: Env) -> None:
    await env.submit(content="indexed", client_doc_ref="cms-ix")
    plan = await env.collection.find({"user_id": "alice", "client_doc_ref": "cms-ix"}).explain()
    text = json.dumps(plan["queryPlanner"], default=str)
    assert "user_client_doc_ref_unique" in text
    assert "COLLSCAN" not in text
