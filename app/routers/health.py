import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Response, status
from pymongo.errors import PyMongoError
from redis.exceptions import RedisError

from app.dependencies import get_resources
from app.models import HealthResponse
from app.resources import Resources

router = APIRouter(tags=["health"])
logger = logging.getLogger(__name__)


@router.get("/health", response_model=HealthResponse, responses={503: {"model": HealthResponse}})
async def health(response: Response, resources: Annotated[Resources, Depends(get_resources)]) -> HealthResponse:
    checks: dict[str, str] = {}
    try:
        await resources.mongo.admin.command("ping")
        checks["mongo"] = "ok"
    except PyMongoError as exc:
        logger.warning("health: mongo unavailable", extra={"error": str(exc)})
        checks["mongo"] = "unavailable"
    try:
        await resources.redis.ping()
        checks["redis"] = "ok"
    except RedisError as exc:
        logger.warning("health: redis unavailable", extra={"error": str(exc)})
        checks["redis"] = "unavailable"

    if checks["mongo"] != "ok":
        # MongoDB is the system of record: without it we cannot serve anything.
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        overall = "unavailable"
    elif checks["redis"] != "ok":
        # Rate limiting and caching degrade to MongoDB-backed fallbacks.
        overall = "degraded"
    else:
        overall = "ok"
    return HealthResponse(status=overall, checks=checks)
