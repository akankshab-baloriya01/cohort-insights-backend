import re
from typing import Annotated

from fastapi import Depends, Header, Request

from app.errors import Unauthorized
from app.models import USER_ID_PATTERN
from app.resources import Resources
from app.services.documents import DocumentService

_USER_ID_RE = re.compile(USER_ID_PATTERN)


def get_resources(request: Request) -> Resources:
    return request.app.state.resources


def get_document_service(resources: Annotated[Resources, Depends(get_resources)]) -> DocumentService:
    return DocumentService(resources.repo, resources.limiter, resources.cache, resources.locks, resources.settings)


def get_caller_id(x_user_id: Annotated[str | None, Header()] = None) -> str:
    """Identity of the caller.

    Stand-in for real authentication: in production this would be the subject of a verified
    token set by an auth gateway, never a value the client picks freely.
    """
    if x_user_id is None or not _USER_ID_RE.fullmatch(x_user_id):
        raise Unauthorized("missing or malformed X-User-Id header")
    return x_user_id


CallerId = Annotated[str, Depends(get_caller_id)]
Service = Annotated[DocumentService, Depends(get_document_service)]
