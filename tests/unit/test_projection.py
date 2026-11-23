from datetime import datetime, timezone

from app.domain.hashing import content_hash
from app.domain.projection import project_document
from app.domain.text import build_summary, build_tags


def _document(**overrides):
    now = datetime.now(timezone.utc)
    document = {
        "_id": "656f6f6e6f6e6f6e6f6e6f6e",
        "user_id": "user-1",
        "title": "Notes",
        "content": "alpha beta",
        "content_hash": "hash-2",
        "content_version": 2,
        "status": "completed",
        "failed_stage": None,
        "summary": {
            "text": "current summary",
            "content_version": 2,
            "content_hash": "hash-2",
        },
        "tags": {
            "values": ["alpha", "beta"],
            "content_version": 2,
            "content_hash": "hash-2",
            "summary_content_version": 2,
        },
        "stages": {
            "processing": {"state": "succeeded", "attempts": 1, "content_version": 2},
            "enriching": {"state": "succeeded", "attempts": 1, "content_version": 2},
        },
        "created_at": now,
        "updated_at": now,
    }
    document.update(overrides)
    return document


def test_matching_version_is_visible():
    view = project_document(_document())

    assert view["results_current"] is True
    assert view["summary"]["text"] == "current summary"
    assert view["summary"]["content_version"] == 2
    assert view["tags"]["values"] == ["alpha", "beta"]
    assert view["stages"]["processing"]["state"] == "succeeded"


def test_previous_summary_is_hidden_after_version_bump():
    view = project_document(
        _document(
            content="replacement",
            content_hash="hash-3",
            content_version=3,
            status="queued",
            summary={
                "text": "old summary",
                "content_version": 2,
                "content_hash": "hash-2",
            },
            tags={
                "values": ["old"],
                "content_version": 2,
                "content_hash": "hash-2",
                "summary_content_version": 2,
            },
            stages={
                "processing": {"state": "failed", "attempts": 2, "content_version": 2},
                "enriching": {"state": "succeeded", "attempts": 1, "content_version": 2},
            },
        )
    )

    assert view["content_version"] == 3
    assert view["summary"] is None
    assert view["tags"] is None
    assert view["results_current"] is False
    assert view["stages"]["processing"]["state"] == "pending"
    assert view["stages"]["processing"]["attempts"] == 0
    assert view["failed_stage"] is None


def test_mixed_summary_and_tags_are_not_both_returned():
    view = project_document(
        _document(
            summary={
                "text": "version two summary",
                "content_version": 2,
                "content_hash": "hash-2",
            },
            tags={
                "values": ["from-version-one"],
                "content_version": 1,
                "content_hash": "hash-1",
                "summary_content_version": 1,
            },
        )
    )

    assert view["summary"]["text"] == "version two summary"
    assert view["tags"] is None
    assert view["results_current"] is False
    assert view["stages"]["enriching"]["state"] == "pending"


def test_tags_derived_from_another_summary_version_are_hidden():
    view = project_document(
        _document(
            tags={
                "values": ["alpha"],
                "content_version": 2,
                "content_hash": "hash-2",
                "summary_content_version": 1,
            }
        )
    )

    assert view["summary"]["content_version"] == 2
    assert view["tags"] is None
    assert view["results_current"] is False


def test_failed_enriching_keeps_only_the_matching_summary():
    view = project_document(
        _document(
            status="failed",
            failed_stage="enriching",
            tags=None,
            stages={
                "processing": {"state": "succeeded", "attempts": 1, "content_version": 2},
                "enriching": {"state": "failed", "attempts": 3, "content_version": 2},
            },
        )
    )

    assert view["failed_stage"] == "enriching"
    assert view["summary"]["content_hash"] == "hash-2"
    assert view["tags"] is None
    assert view["results_current"] is False
    assert view["stages"]["enriching"]["state"] == "failed"


def test_content_hash_is_the_raw_utf8_bytes():
    assert content_hash("same") == content_hash("same")
    assert content_hash("same") != content_hash("same ")
    assert len(content_hash("same")) == 64


def test_tags_depend_on_the_summary_they_were_built_from():
    content = "alpha alpha beta gamma"
    short = build_summary("alpha")
    long = build_summary(content)

    assert build_tags(content, short) != build_tags(content, long)
    assert build_tags(content, long)[-1].startswith("summary-words-")
