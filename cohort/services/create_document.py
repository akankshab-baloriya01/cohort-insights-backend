import hashlib
from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import HTTPException, status
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from schemas import CreateDocument, CreateDocumentResponse, ListDocumentResponse, UpdateDocument
from services.pipeline import completed_stages, pending_stages
from services.redis_store import ACTIVE_STATUSES, acquire_slot, get_cached_result, release_slot


def not_found() -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found")


def ref_conflict() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail="client_doc_ref already exists with different content; use PATCH to change it",
    )


def version_conflict(current_version: int) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={"message": "Document was modified concurrently", "content_version": current_version},
    )


def too_many_active() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail="Too many documents in progress; wait for one to finish",
    )


def to_object_id(document_id: str) -> ObjectId:
    try:
        return ObjectId(document_id)
    except InvalidId:
        raise not_found()


def hash_content(content: str) -> str:
    return "sha256:" + hashlib.sha256(content.encode()).hexdigest()


def ref_query(client_doc_ref: str) -> dict:
    return {"client_doc_ref": {"$eq": client_doc_ref, "$type": "string"}}


def content_fields(content: str, version: int, cached: Optional[dict]) -> dict:
    return {
        "content": content,
        "content_hash": hash_content(content),
        "content_version": version,
        "status": "completed" if cached else "queued",
        "stages": completed_stages(version, cached["summary"], cached["tags"]) if cached else pending_stages(),
        "updated_at": datetime.now(timezone.utc),
    }


def to_response(doc: dict) -> ListDocumentResponse:
    version = doc["content_version"]
    processing, enriching = doc["stages"]["processing"], doc["stages"]["enriching"]
    current = processing["content_version"] == version and enriching["content_version"] == version
    ready = doc["status"] == "completed" and current
    return ListDocumentResponse(
        document_id=str(doc["_id"]),
        user_id=doc["user_id"],
        title=doc["title"],
        content=doc["content"],
        client_doc_ref=doc.get("client_doc_ref"),
        status=doc["status"],
        content_version=version,
        stages=doc["stages"],
        summary=processing["summary"] if ready else None,
        tags=enriching["tags"] if ready else None,
        created_at=doc["created_at"],
        updated_at=doc["updated_at"],
    )


async def find_repeat_submission(
    request: CreateDocument, collection
) -> Optional[CreateDocumentResponse]:
    existing = await collection.find_one(ref_query(request.client_doc_ref))
    if existing is None:
        return None
    if existing["user_id"] == request.user_id and existing["content_hash"] == hash_content(request.content):
        return CreateDocumentResponse(document_id=str(existing["_id"]), status=existing["status"])
    raise ref_conflict()


async def create_document_service(
    request: CreateDocument, caller: str, collection
) -> tuple[CreateDocumentResponse, bool]:
    if request.user_id != caller:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="user_id must match X-User-Id")
    if request.client_doc_ref is not None:
        existing = await find_repeat_submission(request, collection)
        if existing is not None:
            return existing, False

    cached = await get_cached_result(hash_content(request.content), collection)
    if not cached and not await acquire_slot(request.user_id, collection):
        raise too_many_active()

    document = {
        **request.model_dump(exclude_none=True),
        **content_fields(request.content, 1, cached),
    }
    document["created_at"] = document["updated_at"]
    try:
        result = await collection.insert_one(document)
    except DuplicateKeyError:
        if not cached:
            await release_slot(request.user_id)
        existing = await find_repeat_submission(request, collection)
        if existing is not None:
            return existing, False
        raise ref_conflict()

    return CreateDocumentResponse(document_id=str(result.inserted_id), status=document["status"]), True


async def get_document_service(
    user_id: str,
    collection,
    page: int = 1,
    page_size: int = 10,
    status_filter: Optional[str] = None,
) -> list[ListDocumentResponse]:
    query = {"user_id": user_id}
    if status_filter is not None:
        query["status"] = status_filter

    skip = (page - 1) * page_size
    cursor = collection.find(query).sort("created_at", -1).skip(skip).limit(page_size)
    documents = await cursor.to_list(length=page_size)
    return [to_response(doc) for doc in documents]


async def get_document_by_doc_id(document_id: str, caller: str, collection) -> ListDocumentResponse:
    doc = await collection.find_one({"_id": to_object_id(document_id), "user_id": caller})
    if doc is None:
        raise not_found()
    return to_response(doc)


async def update_document_content(
    document_id: str, request: UpdateDocument, caller: str, collection
) -> ListDocumentResponse:
    query = {"_id": to_object_id(document_id), "user_id": caller}
    doc = await collection.find_one(query)
    if doc is None:
        raise not_found()
    version = request.expected_version or doc["content_version"]
    if version != doc["content_version"]:
        raise version_conflict(doc["content_version"])

    cached = await get_cached_result(hash_content(request.content), collection)
    was_active = doc["status"] in ACTIVE_STATUSES
    needs_slot = not cached and not was_active
    if needs_slot and not await acquire_slot(caller, collection):
        raise too_many_active()

    active_filter = {"$in": ACTIVE_STATUSES} if was_active else {"$nin": ACTIVE_STATUSES}
    updated = await collection.find_one_and_update(
        {**query, "content_version": version, "status": active_filter},
        {"$set": content_fields(request.content, version + 1, cached)},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        if needs_slot:
            await release_slot(caller)
        current = await collection.find_one(query, {"content_version": 1})
        raise version_conflict(current["content_version"]) if current else not_found()
    if cached and was_active:
        await release_slot(caller)
    return to_response(updated)


async def get_document_by_client_doc_ref(
    client_doc_ref: str, caller: str, collection
) -> ListDocumentResponse:
    doc = await collection.find_one({**ref_query(client_doc_ref), "user_id": caller})
    if doc is None:
        raise not_found()
    return to_response(doc)
