from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis

from config import MONGO_DB, MONGO_URL, REDIS_URL

client = AsyncIOMotorClient(MONGO_URL, tz_aware=True, serverSelectionTimeoutMS=5000)
database = client[MONGO_DB]
document_collection = database["documents"]
redis = Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=1, socket_connect_timeout=1)


async def create_indexes():
    await document_collection.create_index(
        "client_doc_ref",
        name="client_doc_ref_unique",
        unique=True,
        partialFilterExpression={"client_doc_ref": {"$type": "string"}},
    )
    await document_collection.create_index(
        [("user_id", 1), ("created_at", -1)], name="user_created"
    )
    await document_collection.create_index(
        [("user_id", 1), ("status", 1), ("created_at", -1)], name="user_status_created"
    )
    await document_collection.create_index("content_hash", name="content_hash")
    await document_collection.create_index([("status", 1), ("updated_at", 1)], name="status_updated")


async def get_document_collection():
    return document_collection
