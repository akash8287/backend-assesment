import logging
from typing import Any

from bson import ObjectId
from pymongo.errors import DuplicateKeyError
from redis.exceptions import RedisError

from app.cache import ContentCache
from app.config import Settings
from app.domain.artifacts import fresh_stages, summary_artifact, tags_artifact
from app.domain.constants import ACTIVE_STATUSES
from app.domain.hashing import content_hash
from app.errors import ConflictError, NotFoundError, RateLimitError
from app.logging import log
from app.rate_limit import RateLimiter
from app.repository import DocumentRepository
from app.schemas import DocumentCreate
from app.time import utcnow

logger = logging.getLogger(__name__)


class DocumentService:
    def __init__(
        self,
        repository: DocumentRepository,
        cache: ContentCache,
        limiter: RateLimiter,
        settings: Settings,
    ) -> None:
        self._repository = repository
        self._cache = cache
        self._limiter = limiter
        self._settings = settings

    async def submit(self, payload: DocumentCreate) -> tuple[dict[str, Any], bool]:
        digest = content_hash(payload.content)
        if payload.client_doc_ref is not None:
            existing = await self._repository.find_by_ref(payload.client_doc_ref)
            if existing is not None:
                document = await self._resubmit(existing, payload, digest)
                return document, False

        reusable = await self._reusable(digest)
        if reusable is not None:
            document = self._build(payload, digest, reusable)
            try:
                await self._repository.insert(document)
            except DuplicateKeyError:
                existing = await self._require_ref(payload.client_doc_ref)
                document = await self._resubmit(existing, payload, digest)
                return document, False
            log(logger, logging.INFO, "document stored from cache", document_id=str(document["_id"]))
            return document, True

        document_id = ObjectId()
        duplicate = False
        async with self._limiter.hold(payload.user_id) as locked:
            decision = await self._admit(locked, payload.user_id, str(document_id), 1, False)
            if decision == "rejected":
                log(logger, logging.INFO, "submit rejected by active job cap", user_id=payload.user_id)
                raise RateLimitError()
            document = self._build(payload, digest, None, document_id)
            try:
                await self._repository.insert(document)
            except DuplicateKeyError:
                duplicate = True
                await self._retire(locked, payload.user_id, str(document_id), 1)
            except Exception:
                await self._retire(locked, payload.user_id, str(document_id), 1)
                raise
        if duplicate:
            existing = await self._require_ref(payload.client_doc_ref)
            document = await self._resubmit(existing, payload, digest)
            return document, False
        log(
            logger,
            logging.INFO,
            "document queued",
            document_id=str(document["_id"]),
            user_id=payload.user_id,
            content_version=document["content_version"],
        )
        return document, True

    async def update_content(
        self,
        document_id: str,
        user_id: str,
        content: str,
        expected_content_version: int | None,
    ) -> dict[str, Any]:
        current = await self._owned(self._parse_id(document_id), user_id)
        return await self._replace(current, user_id, content, expected_content_version)

    async def get(self, document_id: str, user_id: str) -> dict[str, Any]:
        return await self._owned(self._parse_id(document_id), user_id)

    async def get_by_ref(self, client_doc_ref: str, user_id: str) -> dict[str, Any]:
        document = await self._repository.find_by_ref(client_doc_ref)
        if document is None or document["user_id"] != user_id:
            raise NotFoundError()
        return document

    async def list_documents(
        self,
        user_id: str,
        status: str | None,
        page: int,
        page_size: int,
    ) -> tuple[list[dict[str, Any]], int]:
        skip = (page - 1) * page_size
        return await self._repository.list_for_user(user_id, status, skip, page_size)

    async def _resubmit(
        self,
        existing: dict[str, Any],
        payload: DocumentCreate,
        digest: str,
    ) -> dict[str, Any]:
        if existing["user_id"] != payload.user_id:
            raise ConflictError("client_doc_ref is already associated with another user")
        if existing["content_hash"] == digest:
            if existing["title"] != payload.title:
                updated = await self._repository.set_title_if_current(
                    existing["_id"],
                    payload.user_id,
                    existing["content_version"],
                    digest,
                    payload.title,
                )
                if updated is not None:
                    return updated
                refreshed = await self._repository.find_by_id(existing["_id"])
                if refreshed is not None:
                    return refreshed
            log(
                logger,
                logging.INFO,
                "client_doc_ref replay returned the current document",
                document_id=str(existing["_id"]),
                client_doc_ref=payload.client_doc_ref,
            )
            return existing
        log(
            logger,
            logging.INFO,
            "client_doc_ref replay replaced content",
            document_id=str(existing["_id"]),
            client_doc_ref=payload.client_doc_ref,
        )
        return await self._replace(existing, payload.user_id, payload.content, None)

    async def _replace(
        self,
        document: dict[str, Any],
        user_id: str,
        content: str,
        expected_content_version: int | None,
    ) -> dict[str, Any]:
        digest = content_hash(content)
        async with self._limiter.hold(user_id) as locked:
            current = await self._repository.find_by_id(document["_id"])
            if current is None or current["user_id"] != user_id:
                raise NotFoundError()
            if (
                expected_content_version is not None
                and current["content_version"] != expected_content_version
            ):
                raise ConflictError("content version conflict", current["content_version"])
            if current["content_hash"] == digest and current["content"] == content:
                return current

            version_now = current["content_version"]
            target_version = version_now + 1
            active_now = current["status"] in ACTIVE_STATUSES
            reusable = await self._reusable(digest)
            admitted = False
            if reusable is None:
                decision = await self._admit(
                    locked,
                    user_id,
                    str(current["_id"]),
                    target_version,
                    active_now,
                )
                if decision == "rejected":
                    log(
                        logger,
                        logging.INFO,
                        "update rejected by active job cap",
                        document_id=str(current["_id"]),
                        user_id=user_id,
                    )
                    raise RateLimitError()
                admitted = True

            if reusable is None:
                updates = self._pipeline_updates(content, digest, target_version)
            else:
                summary, tags = reusable
                updates = self._completed_updates(content, digest, target_version, summary, tags)

            updated = await self._repository.replace_version(
                current["_id"],
                user_id,
                version_now,
                updates,
            )
            if updated is None:
                if admitted:
                    await self._align(locked, user_id, current["_id"], target_version)
                latest = await self._repository.find_by_id(current["_id"])
                observed = latest["content_version"] if latest is not None else None
                raise ConflictError("content version conflict", observed)

            if reusable is not None and active_now and locked:
                await self._limiter.retire(user_id, str(current["_id"]), version_now)
            log(
                logger,
                logging.INFO,
                "document content replaced",
                document_id=str(updated["_id"]),
                content_version=updated["content_version"],
                status=updated["status"],
            )
            return updated

    async def _reusable(self, digest: str) -> tuple[str, list[str]] | None:
        cached = await self._cache.get(digest)
        if cached is not None:
            return cached
        source = await self._repository.find_completed_match(digest)
        if source is None:
            return None
        summary = source["summary"]["text"]
        tags = list(source["tags"]["values"])
        await self._cache.put(digest, summary, tags)
        return summary, tags

    async def _admit(
        self,
        locked: bool,
        user_id: str,
        document_id: str,
        version: int,
        already_active: bool,
    ) -> str:
        if locked:
            try:
                return await self._limiter.admit(user_id, document_id, version)
            except RedisError:
                log(logger, logging.WARNING, "rate limiter admit failed", user_id=user_id)
        if already_active:
            return "refreshed"
        active = await self._repository.count_active(user_id)
        if active >= self._settings.max_active_jobs_per_user:
            return "rejected"
        log(logger, logging.WARNING, "admitting from mongodb because redis is unavailable", user_id=user_id)
        return "added"

    async def _retire(self, locked: bool, user_id: str, document_id: str, version: int) -> None:
        if locked:
            await self._limiter.retire(user_id, document_id, version)

    async def _align(
        self,
        locked: bool,
        user_id: str,
        document_id: ObjectId,
        attempted_version: int,
    ) -> None:
        if not locked:
            return
        await self._limiter.retire(user_id, str(document_id), attempted_version)
        current = await self._repository.find_by_id(document_id)
        if current is not None and current["status"] in ACTIVE_STATUSES:
            await self._limiter.admit(user_id, str(document_id), current["content_version"])

    async def _require_ref(self, client_doc_ref: str | None) -> dict[str, Any]:
        if client_doc_ref is None:
            raise ConflictError("document identity conflict")
        existing = await self._repository.find_by_ref(client_doc_ref)
        if existing is None:
            raise ConflictError("document identity conflict")
        return existing

    async def _owned(self, document_id: ObjectId, user_id: str) -> dict[str, Any]:
        document = await self._repository.find_by_id(document_id)
        if document is None or document["user_id"] != user_id:
            raise NotFoundError()
        return document

    @staticmethod
    def _parse_id(document_id: str) -> ObjectId:
        if not ObjectId.is_valid(document_id):
            raise NotFoundError()
        return ObjectId(document_id)

    def _build(
        self,
        payload: DocumentCreate,
        digest: str,
        reusable: tuple[str, list[str]] | None,
        document_id: ObjectId | None = None,
    ) -> dict[str, Any]:
        now = utcnow()
        version = 1
        if reusable is None:
            summary = None
            tags = None
            status = "queued"
            stages = fresh_stages(version)
        else:
            summary = summary_artifact(reusable[0], version, digest)
            tags = tags_artifact(reusable[1], version, digest)
            status = "completed"
            stages = fresh_stages(version, "succeeded")
        document: dict[str, Any] = {
            "_id": document_id or ObjectId(),
            "user_id": payload.user_id,
            "title": payload.title,
            "content": payload.content,
            "content_hash": digest,
            "content_version": version,
            "status": status,
            "failed_stage": None,
            "summary": summary,
            "tags": tags,
            "stages": stages,
            "claim_token": None,
            "claim_expires_at": None,
            "not_before": now,
            "created_at": now,
            "updated_at": now,
        }
        if payload.client_doc_ref is not None:
            document["client_doc_ref"] = payload.client_doc_ref
        return document

    @staticmethod
    def _pipeline_updates(content: str, digest: str, version: int) -> dict[str, Any]:
        return {
            "content": content,
            "content_hash": digest,
            "status": "queued",
            "failed_stage": None,
            "summary": None,
            "tags": None,
            "stages": fresh_stages(version),
            "claim_token": None,
            "claim_expires_at": None,
            "not_before": utcnow(),
        }

    @staticmethod
    def _completed_updates(
        content: str,
        digest: str,
        version: int,
        summary: str,
        tags: list[str],
    ) -> dict[str, Any]:
        return {
            "content": content,
            "content_hash": digest,
            "status": "completed",
            "failed_stage": None,
            "summary": summary_artifact(summary, version, digest),
            "tags": tags_artifact(tags, version, digest),
            "stages": fresh_stages(version, "succeeded"),
            "claim_token": None,
            "claim_expires_at": None,
            "not_before": utcnow(),
        }
