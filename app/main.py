import logging
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response

from app.config import Settings, get_settings
from app.errors import register_error_handlers
from app.logging_config import configure_logging, request_id_var
from app.resources import open_resources
from app.routers import documents, health, users

logger = logging.getLogger("app.http")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.resources = await open_resources(settings)
        logger.info("api started", extra={"mongo_db": settings.mongo_db})
        try:
            yield
        finally:
            await app.state.resources.close()

    app = FastAPI(title="Cohort Insights API", version="1.0.0", lifespan=lifespan)
    register_error_handlers(app)

    @app.middleware("http")
    async def request_context(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        token = request_id_var.set(request_id)
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            logger.exception("unhandled error", extra={"method": request.method, "path": request.url.path})
            raise
        finally:
            request_id_var.reset(token)
        response.headers["x-request-id"] = request_id
        logger.info(
            "request",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "duration_ms": round((time.perf_counter() - started) * 1000, 1),
            },
        )
        return response

    app.include_router(health.router)
    app.include_router(documents.router)
    app.include_router(users.router)
    return app


def build() -> FastAPI:
    """Entrypoint for uvicorn (`uvicorn app.main:build --factory`)."""
    settings = get_settings()
    configure_logging(settings.log_level)
    return create_app(settings)
