"""Reader view of a document. Derived fields from another version are omitted."""

from collections.abc import Mapping
from typing import Any


def _matches(artifact: Any, version: int, content_hash: str) -> bool:
    """True when the artifact was computed from this version and this content hash."""
    return (
        isinstance(artifact, dict)
        and artifact.get("content_version") == version
        and artifact.get("content_hash") == content_hash
    )


def _stage_view(doc: Mapping[str, Any], name: str, version: int, artifact_visible: bool) -> dict[str, Any]:
    """Stage progress for the current version.

    A record from another version, or a success whose artifact is not visible,
    is reported as pending so the caller cannot see a finished stage without its result.
    """
    stages = doc.get("stages")
    raw = stages.get(name) if isinstance(stages, dict) else None
    if not isinstance(raw, dict) or raw.get("content_version") != version:
        return {"state": "pending", "attempts": 0}
    state = raw.get("state")
    if state not in {"pending", "running", "succeeded", "failed"}:
        state = "pending"
    attempts = raw.get("attempts")
    attempts = attempts if isinstance(attempts, int) else 0
    if state == "succeeded" and not artifact_visible:
        return {"state": "pending", "attempts": 0}
    return {"state": state, "attempts": attempts}


def project_document(doc: Mapping[str, Any]) -> dict[str, Any]:
    """Return a reader view where summary and tags are visible only for this version.

    An artifact stamped with another content_version or content_hash is omitted.
    Tags are omitted unless they were derived from the summary of this same version.
    """
    version = doc["content_version"]
    digest = doc["content_hash"]
    summary = doc.get("summary")
    tags = doc.get("tags")
    summary_ok = _matches(summary, version, digest) and isinstance(summary.get("text"), str)
    tags_ok = (
        summary_ok
        and _matches(tags, version, digest)
        and tags.get("summary_content_version") == summary.get("content_version")
        and isinstance(tags.get("values"), list)
    )
    status = doc["status"]
    failed_stage = doc.get("failed_stage") if status == "failed" else None
    if failed_stage not in {"processing", "enriching"}:
        failed_stage = None
    return {
        "document_id": str(doc["_id"]),
        "user_id": doc["user_id"],
        "title": doc["title"],
        "content": doc["content"],
        "content_hash": digest,
        "content_version": version,
        "client_doc_ref": doc.get("client_doc_ref"),
        "status": status,
        "failed_stage": failed_stage,
        "stages": {
            "processing": _stage_view(doc, "processing", version, summary_ok),
            "enriching": _stage_view(doc, "enriching", version, tags_ok),
        },
        "summary": (
            {
                "text": summary["text"],
                "content_version": summary["content_version"],
                "content_hash": summary["content_hash"],
            }
            if summary_ok
            else None
        ),
        "tags": (
            {
                "values": list(tags["values"]),
                "content_version": tags["content_version"],
                "content_hash": tags["content_hash"],
                "summary_content_version": tags["summary_content_version"],
            }
            if tags_ok
            else None
        ),
        "results_current": bool(summary_ok and tags_ok and status == "completed"),
        "created_at": doc["created_at"],
        "updated_at": doc["updated_at"],
    }
