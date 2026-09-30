import json
import logging
import os

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "cohort")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MAX_ACTIVE_PER_USER = int(os.getenv("MAX_ACTIVE_PER_USER", "3"))
ACTIVE_KEY_TTL = int(os.getenv("ACTIVE_KEY_TTL", "3600"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "86400"))
WORKER_COUNT = int(os.getenv("WORKER_COUNT", "2"))
STAGE_TIME_SCALE = float(os.getenv("STAGE_TIME_SCALE", "1"))
FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.1"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "time": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["error"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def setup_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=LOG_LEVEL, handlers=[handler], force=True)
