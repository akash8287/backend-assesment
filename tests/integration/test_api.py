import uuid

import pytest

from tests.integration.conftest import auth, submit


@pytest.mark.asyncio
async def test_health(client):
    response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "mongo": "ok", "redis": "ok"}


@pytest.mark.asyncio
async def test_validation_and_missing_identity(client, user_id):
    missing = await client.post(
        "/documents",
        json={"user_id": user_id, "title": "Notes", "content": "hello world"},
    )
    blank = await client.post(
        "/documents",
        headers=auth(user_id),
        json={"user_id": user_id, "title": "Notes", "content": "   "},
    )
    mismatch = await client.post(
        "/documents",
        headers=auth(user_id),
        json={"user_id": f"other{user_id}", "title": "Notes", "content": "hello world"},
    )

    assert missing.status_code == 401
    assert blank.status_code == 422
    assert mismatch.status_code == 403


@pytest.mark.asyncio
async def test_owner_can_read_and_other_users_cannot(client, user_id):
    created = await submit(client, user_id, f"owned document {uuid.uuid4().hex}")
    document_id = created["document_id"]
    stranger = f"user{uuid.uuid4().hex[:12]}"

    own = await client.get(f"/documents/{document_id}", headers=auth(user_id))
    hidden = await client.get(f"/documents/{document_id}", headers=auth(stranger))
    missing = await client.get("/documents/not-an-object-id", headers=auth(user_id))
    other_list = await client.get(f"/users/{stranger}/documents", headers=auth(user_id))
    own_list = await client.get(f"/users/{user_id}/documents", headers=auth(user_id))

    assert own.status_code == 200
    assert hidden.status_code == 404
    assert missing.status_code == 404
    assert other_list.status_code == 404
    assert own_list.status_code == 200
    assert own_list.json()["total"] == 1
    assert own_list.json()["items"][0]["document_id"] == document_id


@pytest.mark.asyncio
async def test_list_is_newest_first_and_filterable(client, user_id):
    first = await submit(client, user_id, f"first document {uuid.uuid4().hex}", title="First")
    second = await submit(client, user_id, f"second document {uuid.uuid4().hex}", title="Second")
    third = await submit(client, user_id, f"third document {uuid.uuid4().hex}", title="Third")

    page = await client.get(
        f"/users/{user_id}/documents",
        headers=auth(user_id),
        params={"page": 1, "page_size": 2},
    )
    next_page = await client.get(
        f"/users/{user_id}/documents",
        headers=auth(user_id),
        params={"page": 2, "page_size": 2},
    )
    queued = await client.get(
        f"/users/{user_id}/documents",
        headers=auth(user_id),
        params={"status": "queued"},
    )

    assert page.status_code == 200
    body = page.json()
    assert [item["document_id"] for item in body["items"]] == [
        third["document_id"],
        second["document_id"],
    ]
    assert body["total"] == 3
    assert [item["document_id"] for item in next_page.json()["items"]] == [first["document_id"]]
    assert queued.json()["total"] == 3


@pytest.mark.asyncio
async def test_active_job_cap_and_cache_do_not_share_slots(client, app, user_id):
    from app.domain.hashing import content_hash

    cached_content = f"cached body {uuid.uuid4().hex}"
    digest = content_hash(cached_content)
    await app.state.redis.set(
        f"ci:result:{digest}",
        '{"summary": "Cached summary of the body", "tags": ["cached", "summary-words-5"]}',
        ex=60,
    )

    cached = await submit(client, user_id, cached_content, title="Cached")
    created = []
    for index in range(3):
        created.append(await submit(client, user_id, f"active body {index} {uuid.uuid4().hex}"))
    rejected = await client.post(
        "/documents",
        headers=auth(user_id),
        json={
            "user_id": user_id,
            "title": "Overflow",
            "content": f"overflow body {uuid.uuid4().hex}",
        },
    )
    another_cached = await submit(client, user_id, cached_content, title="Cached again")
    listed = await client.get(f"/users/{user_id}/documents", headers=auth(user_id))

    assert cached["status"] == "completed"
    assert cached["results_current"] is True
    assert cached["summary"]["text"] == "Cached summary of the body"
    assert cached["summary"]["content_version"] == cached["content_version"]
    assert rejected.status_code == 429
    assert rejected.headers["retry-after"] == "5"
    assert another_cached["status"] == "completed"
    assert another_cached["document_id"] != cached["document_id"]
    assert listed.json()["total"] == 5
    assert [item["status"] for item in created] == ["queued", "queued", "queued"]


@pytest.mark.asyncio
async def test_patch_of_an_active_document_does_not_take_another_slot(client, user_id):
    docs = []
    for index in range(3):
        docs.append(await submit(client, user_id, f"slot body {index} {uuid.uuid4().hex}"))
    updated = await client.patch(
        f"/documents/{docs[0]['document_id']}",
        headers=auth(user_id),
        json={"content": f"replacement body {uuid.uuid4().hex}", "expected_content_version": 1},
    )
    rejected = await client.post(
        "/documents",
        headers=auth(user_id),
        json={"user_id": user_id, "title": "Fourth", "content": f"fourth body {uuid.uuid4().hex}"},
    )

    assert updated.status_code == 200
    body = updated.json()
    assert body["document_id"] == docs[0]["document_id"]
    assert body["content_version"] == 2
    assert body["status"] == "queued"
    assert body["summary"] is None
    assert body["tags"] is None
    assert body["results_current"] is False
    assert rejected.status_code == 429


@pytest.mark.asyncio
async def test_repeat_client_ref_is_defined(client, user_id):
    ref = f"ref{uuid.uuid4().hex[:16]}"
    stranger = f"user{uuid.uuid4().hex[:12]}"
    original = await client.post(
        "/documents",
        headers=auth(user_id),
        json={
            "user_id": user_id,
            "title": "Partner",
            "content": f"partner body {uuid.uuid4().hex}",
            "client_doc_ref": ref,
        },
    )
    replay = await client.post(
        "/documents",
        headers=auth(user_id),
        json={
            "user_id": user_id,
            "title": "Partner renamed",
            "content": original.json()["content"],
            "client_doc_ref": ref,
        },
    )
    replaced = await client.post(
        "/documents",
        headers=auth(user_id),
        json={
            "user_id": user_id,
            "title": "Partner",
            "content": f"partner replacement {uuid.uuid4().hex}",
            "client_doc_ref": ref,
        },
    )
    conflict = await client.post(
        "/documents",
        headers=auth(stranger),
        json={
            "user_id": stranger,
            "title": "Stolen",
            "content": f"other body {uuid.uuid4().hex}",
            "client_doc_ref": ref,
        },
    )
    hidden = await client.get(f"/documents/by-ref/{ref}", headers=auth(stranger))
    visible = await client.get(f"/documents/by-ref/{ref}", headers=auth(user_id))
    missing = await client.get(f"/documents/by-ref/missing{uuid.uuid4().hex[:8]}", headers=auth(user_id))

    assert original.status_code == 201
    assert replay.status_code == 200
    assert replay.json()["document_id"] == original.json()["document_id"]
    assert replay.json()["content_version"] == 1
    assert replay.json()["title"] == "Partner renamed"
    assert replaced.status_code == 200
    assert replaced.json()["document_id"] == original.json()["document_id"]
    assert replaced.json()["content_version"] == 2
    assert replaced.json()["summary"] is None
    assert replaced.json()["results_current"] is False
    assert conflict.status_code == 409
    assert hidden.status_code == 404
    assert visible.status_code == 200
    assert visible.json()["document_id"] == original.json()["document_id"]
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_client_ref_lookup_uses_the_unique_index(app):
    indexes = await app.state.mongo.documents.index_information()
    index = indexes["client_doc_ref_unique"]

    assert index["unique"] is True
    assert index["key"] == [("client_doc_ref", 1)]
    assert index["partialFilterExpression"] == {"client_doc_ref": {"$type": "string"}}


@pytest.mark.asyncio
async def test_patch_conflict_when_the_observed_version_loses(client, user_id):
    created = await submit(client, user_id, f"versioned body {uuid.uuid4().hex}")
    document_id = created["document_id"]

    winner = await client.patch(
        f"/documents/{document_id}",
        headers=auth(user_id),
        json={"content": f"winner body {uuid.uuid4().hex}", "expected_content_version": 1},
    )
    loser = await client.patch(
        f"/documents/{document_id}",
        headers=auth(user_id),
        json={"content": f"loser body {uuid.uuid4().hex}", "expected_content_version": 1},
    )

    assert winner.status_code == 200
    assert winner.json()["content_version"] == 2
    assert loser.status_code == 409
    assert loser.json()["content_version"] == 2
    assert loser.json()["detail"] == "content version conflict"


@pytest.mark.asyncio
async def test_stored_mismatch_is_invisible_to_the_reader(client, app, user_id):
    from datetime import datetime, timezone

    from bson import ObjectId

    document_id = ObjectId()
    now = datetime.now(timezone.utc)
    await app.state.mongo.documents.insert_one(
        {
            "_id": document_id,
            "user_id": user_id,
            "title": "Mixed",
            "content": "new content",
            "content_hash": "hash-new",
            "content_version": 4,
            "status": "completed",
            "failed_stage": None,
            "summary": {
                "text": "old summary",
                "content_version": 3,
                "content_hash": "hash-old",
            },
            "tags": {
                "values": ["new-tag"],
                "content_version": 4,
                "content_hash": "hash-new",
                "summary_content_version": 4,
            },
            "stages": {
                "processing": {"state": "succeeded", "attempts": 1, "content_version": 3},
                "enriching": {"state": "succeeded", "attempts": 1, "content_version": 4},
            },
            "created_at": now,
            "updated_at": now,
        }
    )

    response = await client.get(f"/documents/{document_id}", headers=auth(user_id))
    body = response.json()

    assert response.status_code == 200
    assert body["content_version"] == 4
    assert body["summary"] is None
    assert body["tags"] is None
    assert body["results_current"] is False
    assert body["stages"]["processing"]["state"] == "pending"
    assert body["stages"]["enriching"]["state"] == "pending"
