import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from pymongo.errors import PyMongoError

from config import setup_logging
from database import create_indexes, document_collection, redis
from routers import router
from services.pipeline import start_workers

setup_logging()
logger = logging.getLogger("cohort")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_indexes()
    workers = await start_workers(document_collection)
    logger.info("Started %d pipeline workers", len(workers))
    yield
    for task in workers:
        task.cancel()
    await asyncio.gather(*workers, return_exceptions=True)
    await redis.aclose()


app = FastAPI(title="Cohort Insights API", lifespan=lifespan)
app.include_router(router)


@app.exception_handler(PyMongoError)
async def mongo_error_handler(request: Request, exc: PyMongoError):
    logger.exception("MongoDB error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "Database unavailable, please retry"},
    )
