from typing import Annotated

from fastapi import APIRouter, Path, Response, status

from app.dependencies import CallerId, Service
from app.models import (
    CLIENT_DOC_REF_PATTERN,
    DocumentCreate,
    DocumentUpdate,
    DocumentView,
    SubmitOutcome,
    SubmitResponse,
)

router = APIRouter(prefix="/documents", tags=["documents"])

_NOT_FOUND = {404: {"description": "No such document for this caller"}}


@router.post(
    "",
    response_model=SubmitResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        200: {"description": "Repeat of a known client_doc_ref (unchanged or new_version)"},
        409: {"description": "Stale or conflicting ref_version"},
        429: {"description": "Too many documents in the pipeline"},
    },
)
async def submit_document(body: DocumentCreate, response: Response, service: Service) -> SubmitResponse:
    result = await service.submit(body)
    if result.outcome is not SubmitOutcome.CREATED:
        response.status_code = status.HTTP_200_OK
    return SubmitResponse(
        document_id=str(result.doc["_id"]),
        status=result.doc["status"],
        content_version=result.doc["content_version"],
        outcome=result.outcome,
        served_from_cache=result.served_from_cache,
    )


@router.get("/by-ref/{client_doc_ref}", response_model=DocumentView, responses=_NOT_FOUND)
async def get_document_by_ref(
    client_doc_ref: Annotated[str, Path(pattern=CLIENT_DOC_REF_PATTERN)], caller: CallerId, service: Service
) -> DocumentView:
    return DocumentView.from_doc(await service.get_by_ref(client_doc_ref, caller))


@router.get("/{document_id}", response_model=DocumentView, responses=_NOT_FOUND)
async def get_document(document_id: str, caller: CallerId, service: Service) -> DocumentView:
    return DocumentView.from_doc(await service.get(document_id, caller))


@router.patch(
    "/{document_id}",
    response_model=DocumentView,
    responses={**_NOT_FOUND, 409: {"description": "expected_version mismatch"}, 429: {"description": "Rate limited"}},
)
async def update_document(document_id: str, body: DocumentUpdate, caller: CallerId, service: Service) -> DocumentView:
    doc = await service.update_content(document_id, caller, body.content, body.expected_version)
    return DocumentView.from_doc(doc)


@router.post(
    "/{document_id}/retry",
    response_model=DocumentView,
    status_code=status.HTTP_202_ACCEPTED,
    responses={**_NOT_FOUND, 409: {"description": "Document is not failed"}, 429: {"description": "Rate limited"}},
)
async def retry_document(document_id: str, caller: CallerId, service: Service) -> DocumentView:
    return DocumentView.from_doc(await service.retry(document_id, caller))
