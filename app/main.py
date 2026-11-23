"""Application factory, request logging, and the dependency health check."""

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError

from app.api import router
from app.cache import ContentCache, redis_client
from app.config import Settings, get_settings
from app.db import MongoDatabase
from app.logging import configure_logging, log
from app.pipeline import PipelineRunner
from app.rate_limit import RateLimiter
from app.repository import DocumentRepository
from app.service import DocumentService

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the API, connect its datastores, and start the pipeline worker.

    MongoDB must answer before startup finishes. Redis being down is logged
    and the process still serves, with cache misses and a MongoDB admission
    count. The worker starts only when worker_enabled is true.
    """
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Open MongoDB and Redis, then close them when the process stops."""
        if (
            settings.pipeline_claim_lease_seconds < settings.pipeline_processing_max_seconds
            or settings.pipeline_claim_lease_seconds < settings.pipeline_enriching_max_seconds
        ):
            log(
                logger,
                logging.WARNING,
                "claim lease is shorter than a stage duration; a live worker can be reclaimed",
            )
        mongo = MongoDatabase(settings)
        await mongo.connect()
        redis = redis_client(settings)
        try:
            await redis.ping()
        except RedisError:
            log(logger, logging.ERROR, "redis is unavailable; cache and admission will degrade")
        cache = ContentCache(redis, settings.cache_ttl_seconds)
        limiter = RateLimiter(
            redis,
            settings.max_active_jobs_per_user,
            settings.active_job_lease_seconds,
            settings.admission_lock_ttl_ms,
            settings.admission_lock_wait_seconds,
        )
        repository = DocumentRepository(mongo.documents)
        service = DocumentService(repository, cache, limiter, settings)
        runner = PipelineRunner(repository, cache, limiter, settings)
        app.state.settings = settings
        app.state.mongo = mongo
        app.state.redis = redis
        app.state.service = service
        app.state.runner = runner
        if settings.worker_enabled:
            await runner.start()
        try:
            yield
        finally:
            await runner.stop()
            await redis.aclose()
            await mongo.close()

    app = FastAPI(
        title="Cohort Insights API",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.include_router(router)

    @app.middleware("http")
    async def log_request(request: Request, call_next):
        """Write one JSON access log line for each request."""
        started = time.perf_counter()
        response = await call_next(request)
        log(
            logger,
            logging.INFO,
            "request",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            elapsed_ms=round((time.perf_counter() - started) * 1000, 1),
        )
        return response

    @app.get("/health")
    async def health(request: Request) -> JSONResponse:
        """Report whether MongoDB and Redis each answer a ping.

        Returns 200 only when both are reachable. Otherwise returns 503 and
        names the dependency that failed.
        """
        mongo_ok = await request.app.state.mongo.ping()
        redis_ok = False
        try:
            redis_ok = bool(await request.app.state.redis.ping())
        except RedisError:
            log(logger, logging.WARNING, "redis health check failed")
        body = {
            "status": "ok" if mongo_ok and redis_ok else "degraded",
            "mongo": "ok" if mongo_ok else "unavailable",
            "redis": "ok" if redis_ok else "unavailable",
        }
        return JSONResponse(status_code=200 if mongo_ok and redis_ok else 503, content=body)

    return app


app = create_app()
