import json
import uuid
from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient
from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis

from app.config import Settings
from app.main import create_app

MONGO_URI = "mongodb://localhost:27017"
REDIS_URL = "redis://localhost:6379/0"


async def dependencies_available() -> bool:
    mongo = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=800)
    redis = Redis.from_url(REDIS_URL, socket_connect_timeout=0.8)
    try:
        await mongo.admin.command("ping")
        await redis.ping()
    except Exception:
        return False
    finally:
        mongo.close()
        await redis.aclose()
    return True


def build_settings(**overrides) -> Settings:
    values = {
        "mongo_uri": MONGO_URI,
        "mongo_db": f"cohort_test_{uuid.uuid4().hex}",
        "mongo_server_selection_ms": 2000,
        "redis_url": REDIS_URL,
        "max_active_jobs_per_user": 3,
        "active_job_lease_seconds": 180,
        "admission_lock_wait_seconds": 1,
        "cache_ttl_seconds": 3600,
        "pipeline_processing_min_seconds": 0.01,
        "pipeline_processing_max_seconds": 0.02,
        "pipeline_enriching_min_seconds": 0.01,
        "pipeline_enriching_max_seconds": 0.02,
        "pipeline_processing_failure_rate": 0,
        "pipeline_enriching_failure_rate": 0,
        "pipeline_max_attempts": 3,
        "pipeline_retry_backoff_seconds": 0.01,
        "pipeline_poll_interval_seconds": 0.02,
        "pipeline_claim_lease_seconds": 30,
        "worker_concurrency": 2,
        "worker_enabled": False,
        "log_level": "WARNING",
    }
    values.update(overrides)
    return Settings(**values)


@pytest.fixture
async def app() -> AsyncIterator:
    if not await dependencies_available():
        pytest.skip("MongoDB and Redis are not running on localhost")
    settings = build_settings()
    application = create_app(settings)
    async with application.router.lifespan_context(application):
        yield application
        await application.state.mongo.client.drop_database(settings.mongo_db)


@pytest.fixture
async def pipeline_app() -> AsyncIterator:
    if not await dependencies_available():
        pytest.skip("MongoDB and Redis are not running on localhost")
    settings = build_settings(worker_enabled=True)
    application = create_app(settings)
    async with application.router.lifespan_context(application):
        yield application
        await application.state.mongo.client.drop_database(settings.mongo_db)


@pytest.fixture
async def client(app) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http:
        yield http


@pytest.fixture
async def pipeline_client(pipeline_app) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=pipeline_app)
    async with AsyncClient(transport=transport, base_url="http://test") as http:
        yield http


@pytest.fixture
def user_id() -> str:
    return f"user{uuid.uuid4().hex[:12]}"


def auth(user_id: str) -> dict[str, str]:
    return {"X-User-Id": user_id}


async def submit(http: AsyncClient, user_id: str, content: str, **extra) -> dict:
    payload = {"user_id": user_id, "title": extra.pop("title", "Notes"), "content": content, **extra}
    response = await http.post("/documents", headers=auth(user_id), json=payload)
    assert response.status_code in {200, 201}, response.text
    return response.json()


async def wait_for(http: AsyncClient, user_id: str, document_id: str, statuses: set[str]) -> dict:
    import asyncio
    import time

    deadline = time.monotonic() + 8
    last = None
    while time.monotonic() < deadline:
        response = await http.get(f"/documents/{document_id}", headers=auth(user_id))
        assert response.status_code == 200, response.text
        last = response.json()
        if last["status"] in statuses:
            return last
        await asyncio.sleep(0.05)
    raise AssertionError(last)


def seed_cache(digest: str, summary: str, tags: list[str]) -> str:
    return json.dumps({"summary": summary, "tags": tags})
