import asyncio
import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from app.domain.hashing import content_hash
from app.main import create_app
from tests.integration.conftest import auth, build_settings, dependencies_available, submit, wait_for


@pytest.mark.asyncio
async def test_pipeline_completes_and_cache_is_keyed_by_content(pipeline_client, pipeline_app, user_id):
    content = f"alpha alpha beta gamma {uuid.uuid4().hex}"
    created = await submit(pipeline_client, user_id, content, title="First")
    assert created["status"] == "queued"
    assert created["summary"] is None
    assert created["results_current"] is False

    done = await wait_for(pipeline_client, user_id, created["document_id"], {"completed"})
    assert done["results_current"] is True
    assert done["summary"]["content_version"] == done["content_version"]
    assert done["summary"]["content_hash"] == done["content_hash"]
    assert done["tags"]["content_version"] == done["content_version"]
    assert done["tags"]["summary_content_version"] == done["summary"]["content_version"]
    assert done["stages"]["processing"]["state"] == "succeeded"
    assert done["stages"]["enriching"]["state"] == "succeeded"

    digest = content_hash(content)
    await pipeline_app.state.redis.delete(f"ci:result:{digest}")
    replay = await submit(pipeline_client, user_id, content, title="Second")

    assert replay["document_id"] != done["document_id"]
    assert replay["status"] == "completed"
    assert replay["summary"]["text"] == done["summary"]["text"]
    assert replay["tags"]["values"] == done["tags"]["values"]
    assert replay["content_version"] == 1


@pytest.mark.asyncio
async def test_patch_hides_the_previous_result_until_the_new_version_finishes(pipeline_client, user_id):
    original_content = f"original words alpha beta {uuid.uuid4().hex}"
    created = await submit(pipeline_client, user_id, original_content)
    original = await wait_for(pipeline_client, user_id, created["document_id"], {"completed"})
    replacement = f"replacement words gamma delta {uuid.uuid4().hex}"

    patched = await pipeline_client.patch(
        f"/documents/{created['document_id']}",
        headers=auth(user_id),
        json={"content": replacement, "expected_content_version": original["content_version"]},
    )
    immediate = await pipeline_client.get(
        f"/documents/{created['document_id']}",
        headers=auth(user_id),
    )
    finished = await wait_for(pipeline_client, user_id, created["document_id"], {"completed"})

    assert patched.status_code == 200
    hidden = patched.json()
    assert hidden["content"] == replacement
    assert hidden["content_version"] == original["content_version"] + 1
    assert hidden["summary"] is None
    assert hidden["tags"] is None
    assert hidden["results_current"] is False
    assert hidden["status"] == "queued"
    assert immediate.json()["summary"] is None or (
        immediate.json()["summary"]["content_version"] == immediate.json()["content_version"]
        and immediate.json()["summary"]["text"] != original["summary"]["text"]
    )
    assert finished["content"] == replacement
    assert finished["content_version"] == original["content_version"] + 1
    assert finished["results_current"] is True
    assert finished["summary"]["text"] != original["summary"]["text"]
    assert finished["summary"]["content_hash"] == finished["content_hash"]
    assert finished["tags"]["content_version"] == finished["content_version"]
    assert finished["tags"]["summary_content_version"] == finished["summary"]["content_version"]


@pytest.mark.asyncio
async def test_enriching_failure_preserves_the_summary(user_id):
    if not await dependencies_available():
        pytest.skip("MongoDB and Redis are not running on localhost")
    settings = build_settings(
        worker_enabled=True,
        pipeline_processing_failure_rate=0,
        pipeline_enriching_failure_rate=1,
        pipeline_max_attempts=2,
    )
    application = create_app(settings)
    async with application.router.lifespan_context(application):
        transport = ASGITransport(app=application)
        async with AsyncClient(transport=transport, base_url="http://test") as http:
            created = await submit(http, user_id, f"enriching failure body {uuid.uuid4().hex}")
            failed = await wait_for(http, user_id, created["document_id"], {"failed"})
        await application.state.mongo.client.drop_database(settings.mongo_db)

    assert failed["failed_stage"] == "enriching"
    assert failed["summary"] is not None
    assert failed["summary"]["content_version"] == failed["content_version"]
    assert failed["summary"]["content_hash"] == failed["content_hash"]
    assert failed["tags"] is None
    assert failed["results_current"] is False
    assert failed["stages"]["processing"]["state"] == "succeeded"
    assert failed["stages"]["enriching"]["state"] == "failed"
    assert failed["stages"]["enriching"]["attempts"] == 2


@pytest.mark.asyncio
async def test_processing_retries_then_fails(user_id):
    if not await dependencies_available():
        pytest.skip("MongoDB and Redis are not running on localhost")
    settings = build_settings(
        worker_enabled=True,
        pipeline_processing_failure_rate=1,
        pipeline_enriching_failure_rate=0,
        pipeline_max_attempts=2,
    )
    application = create_app(settings)
    async with application.router.lifespan_context(application):
        transport = ASGITransport(app=application)
        async with AsyncClient(transport=transport, base_url="http://test") as http:
            created = await submit(http, user_id, f"processing failure body {uuid.uuid4().hex}")
            failed = await wait_for(http, user_id, created["document_id"], {"failed"})
        await application.state.mongo.client.drop_database(settings.mongo_db)

    assert failed["failed_stage"] == "processing"
    assert failed["summary"] is None
    assert failed["tags"] is None
    assert failed["stages"]["processing"]["attempts"] == 2
    assert failed["stages"]["processing"]["state"] == "failed"


@pytest.mark.asyncio
async def test_parallel_patches_with_the_same_version_conflict(pipeline_client, user_id):
    created = await submit(pipeline_client, user_id, f"race body {uuid.uuid4().hex}")
    document_id = created["document_id"]

    async def patch(text: str):
        return await pipeline_client.patch(
            f"/documents/{document_id}",
            headers=auth(user_id),
            json={"content": text, "expected_content_version": 1},
        )

    left, right = await asyncio.gather(
        patch(f"left body {uuid.uuid4().hex}"),
        patch(f"right body {uuid.uuid4().hex}"),
    )
    codes = sorted(response.status_code for response in (left, right))

    assert codes == [200, 409]
