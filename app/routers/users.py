from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.dependencies import CallerId, Service, get_resources
from app.domain import Status
from app.errors import NotFound
from app.models import DocumentListItem, DocumentPage
from app.resources import Resources

router = APIRouter(prefix="/users", tags=["users"])


@router.get("/{user_id}/documents", response_model=DocumentPage, responses={404: {"description": "Not the caller"}})
async def list_user_documents(
    user_id: str,
    caller: CallerId,
    service: Service,
    resources: Annotated[Resources, Depends(get_resources)],
    page: Annotated[int, Query(ge=1, le=10_000)] = 1,
    page_size: Annotated[int | None, Query(ge=1)] = None,
    status: Status | None = None,
) -> DocumentPage:
    if user_id != caller:
        # Same response as for an unknown user: nothing about other users is confirmed.
        raise NotFound("user not found")
    settings = resources.settings
    size = min(page_size or settings.default_page_size, settings.max_page_size)
    docs, total = await service.list(user_id, status, page, size)
    return DocumentPage(
        items=[DocumentListItem.from_doc(d) for d in docs],
        page=page,
        page_size=size,
        total=total,
        has_next=page * size < total,
    )
