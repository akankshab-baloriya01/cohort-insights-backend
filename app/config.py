from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All runtime configuration, sourced from environment variables (or a local .env file)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "cohort-insights-api"
    log_level: str = "INFO"

    # Storage
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "cohort_insights"
    redis_url: str = "redis://localhost:6379/0"
    redis_timeout_seconds: float = Field(default=0.5, gt=0)

    # Per-user active pipeline limit (queued + processing + enriching)
    max_active_docs_per_user: int = Field(default=3, ge=1)
    # Upper bound on how long a slot may be held without being refreshed by a worker.
    # Self-heals leaked slots (e.g. a release that failed while Redis was down).
    active_slot_ttl_seconds: int = Field(default=900, ge=60)

    # Content-hash result cache
    content_cache_ttl_seconds: int = Field(default=86_400, ge=1)

    # Simulated pipeline
    processing_min_seconds: float = Field(default=10.0, ge=0)
    processing_max_seconds: float = Field(default=20.0, ge=0)
    enriching_min_seconds: float = Field(default=5.0, ge=0)
    enriching_max_seconds: float = Field(default=15.0, ge=0)
    stage_failure_rate: float = Field(default=0.10, ge=0, le=1)

    # Retry policy (per stage)
    stage_max_attempts: int = Field(default=3, ge=1)
    retry_backoff_base_seconds: float = Field(default=2.0, ge=0)
    retry_backoff_max_seconds: float = Field(default=60.0, ge=0)

    # Worker
    worker_concurrency: int = Field(default=4, ge=1)
    worker_poll_interval_seconds: float = Field(default=0.5, gt=0)
    # Must exceed the longest simulated stage; a lease that expires lets another worker reclaim the job.
    lease_seconds: int = Field(default=60, ge=1)

    # Pagination
    default_page_size: int = Field(default=20, ge=1)
    max_page_size: int = Field(default=100, ge=1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
