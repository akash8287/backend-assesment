"""Process settings loaded from the environment."""

from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration. Environment variables override these defaults."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "cohort_insights"
    mongo_server_selection_ms: int = Field(default=5000, ge=100)

    redis_url: str = "redis://localhost:6379/0"

    max_active_jobs_per_user: int = Field(default=3, ge=1)
    active_job_lease_seconds: int = Field(default=180, ge=1)
    admission_lock_ttl_ms: int = Field(default=10000, ge=1000)
    admission_lock_wait_seconds: float = Field(default=0.5, gt=0)

    cache_ttl_seconds: int = Field(default=86400, ge=1)

    pipeline_processing_min_seconds: float = Field(default=10, ge=0)
    pipeline_processing_max_seconds: float = Field(default=20, ge=0)
    pipeline_enriching_min_seconds: float = Field(default=5, ge=0)
    pipeline_enriching_max_seconds: float = Field(default=15, ge=0)
    pipeline_processing_failure_rate: float = Field(default=0.1, ge=0, le=1)
    pipeline_enriching_failure_rate: float = Field(default=0.1, ge=0, le=1)
    pipeline_max_attempts: int = Field(default=3, ge=1)
    pipeline_retry_backoff_seconds: float = Field(default=1, ge=0)
    pipeline_poll_interval_seconds: float = Field(default=0.5, gt=0)
    pipeline_claim_lease_seconds: int = Field(default=90, ge=1)
    worker_concurrency: int = Field(default=4, ge=1)
    worker_enabled: bool = True

    log_level: str = "INFO"

    @model_validator(mode="after")
    def check_stage_ranges(self) -> "Settings":
        """Reject a stage whose minimum simulated duration is above its maximum."""
        if self.pipeline_processing_min_seconds > self.pipeline_processing_max_seconds:
            raise ValueError("pipeline processing min seconds exceeds max")
        if self.pipeline_enriching_min_seconds > self.pipeline_enriching_max_seconds:
            raise ValueError("pipeline enriching min seconds exceeds max")
        return self


@lru_cache
def get_settings() -> Settings:
    """Return the process settings. Repeated calls reuse the first result."""
    return Settings()
