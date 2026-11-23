from datetime import datetime
from typing import Any

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import ReturnDocument

from app.domain.constants import ACTIVE_STATUSES
from app.time import utcnow


class DocumentRepository:
    def __init__(self, collection: AsyncIOMotorCollection) -> None:
        self._collection = collection

    async def insert(self, document: dict[str, Any]) -> dict[str, Any]:
        await self._collection.insert_one(document)
        return document

    async def find_by_id(self, document_id: ObjectId) -> dict[str, Any] | None:
        return await self._collection.find_one({"_id": document_id})

    async def find_by_ref(self, client_doc_ref: str) -> dict[str, Any] | None:
        return await self._collection.find_one({"client_doc_ref": client_doc_ref})

    async def find_completed_match(self, content_hash: str) -> dict[str, Any] | None:
        return await self._collection.find_one(
            {
                "content_hash": content_hash,
                "status": "completed",
                "summary.content_hash": content_hash,
                "tags.content_hash": content_hash,
                "$expr": {
                    "$and": [
                        {"$eq": ["$summary.content_version", "$content_version"]},
                        {"$eq": ["$tags.content_version", "$content_version"]},
                        {"$eq": ["$tags.summary_content_version", "$summary.content_version"]},
                    ]
                },
            }
        )

    async def count_active(self, user_id: str, excluding: ObjectId | None = None) -> int:
        query: dict[str, Any] = {"user_id": user_id, "status": {"$in": list(ACTIVE_STATUSES)}}
        if excluding is not None:
            query["_id"] = {"$ne": excluding}
        return await self._collection.count_documents(query)

    async def list_for_user(
        self,
        user_id: str,
        status: str | None,
        skip: int,
        limit: int,
    ) -> tuple[list[dict[str, Any]], int]:
        query: dict[str, Any] = {"user_id": user_id}
        if status is not None:
            query["status"] = status
        total = await self._collection.count_documents(query)
        cursor = (
            self._collection.find(query)
            .sort([("created_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(limit)
        )
        return await cursor.to_list(length=limit), total

    async def set_title_if_current(
        self,
        document_id: ObjectId,
        user_id: str,
        content_version: int,
        content_hash: str,
        title: str,
    ) -> dict[str, Any] | None:
        return await self._collection.find_one_and_update(
            {
                "_id": document_id,
                "user_id": user_id,
                "content_version": content_version,
                "content_hash": content_hash,
            },
            {"$set": {"title": title, "updated_at": utcnow()}},
            return_document=ReturnDocument.AFTER,
        )

    async def replace_version(
        self,
        document_id: ObjectId,
        user_id: str,
        expected_version: int,
        updates: dict[str, Any],
    ) -> dict[str, Any] | None:
        updates = {**updates, "updated_at": utcnow()}
        return await self._collection.find_one_and_update(
            {
                "_id": document_id,
                "user_id": user_id,
                "content_version": expected_version,
            },
            {"$set": updates, "$inc": {"content_version": 1}},
            return_document=ReturnDocument.AFTER,
        )

    async def claim_queued(
        self,
        now: datetime,
        claim_expires_at: datetime,
        token: str,
        max_attempts: int,
    ) -> dict[str, Any] | None:
        return await self._collection.find_one_and_update(
            {
                "status": "queued",
                "not_before": {"$lte": now},
                "$expr": {"$lt": ["$stages.processing.attempts", max_attempts]},
            },
            [
                {
                    "$set": {
                        "status": "processing",
                        "claim_token": token,
                        "claim_expires_at": claim_expires_at,
                        "updated_at": now,
                        "stages.processing.state": "running",
                        "stages.processing.content_version": "$content_version",
                        "stages.processing.attempts": {"$add": ["$stages.processing.attempts", 1]},
                    }
                }
            ],
            sort=[("not_before", 1), ("created_at", 1)],
            return_document=ReturnDocument.AFTER,
        )

    async def claim_expired(
        self,
        status: str,
        stage: str,
        now: datetime,
        claim_expires_at: datetime,
        token: str,
        max_attempts: int,
    ) -> dict[str, Any] | None:
        attempts_field = f"$stages.{stage}.attempts"
        return await self._collection.find_one_and_update(
            {
                "status": status,
                "claim_expires_at": {"$lte": now},
                "$expr": {"$lt": [attempts_field, max_attempts]},
            },
            [
                {
                    "$set": {
                        "claim_token": token,
                        "claim_expires_at": claim_expires_at,
                        "updated_at": now,
                        f"stages.{stage}.state": "running",
                        f"stages.{stage}.content_version": "$content_version",
                        f"stages.{stage}.attempts": {"$add": [attempts_field, 1]},
                    }
                }
            ],
            sort=[("claim_expires_at", 1)],
            return_document=ReturnDocument.AFTER,
        )

    async def claim_enriching_retry(
        self,
        now: datetime,
        claim_expires_at: datetime,
        token: str,
        max_attempts: int,
    ) -> dict[str, Any] | None:
        return await self._collection.find_one_and_update(
            {
                "status": "enriching",
                "claim_token": None,
                "not_before": {"$lte": now},
                "$expr": {"$lt": ["$stages.enriching.attempts", max_attempts]},
            },
            [
                {
                    "$set": {
                        "claim_token": token,
                        "claim_expires_at": claim_expires_at,
                        "updated_at": now,
                        "stages.enriching.state": "running",
                        "stages.enriching.content_version": "$content_version",
                        "stages.enriching.attempts": {"$add": ["$stages.enriching.attempts", 1]},
                    }
                }
            ],
            sort=[("not_before", 1), ("created_at", 1)],
            return_document=ReturnDocument.AFTER,
        )

    async def write_claimed(
        self,
        document_id: ObjectId,
        claim_token: str,
        content_version: int,
        status: str,
        updates: dict[str, Any],
    ) -> dict[str, Any] | None:
        updates = {**updates, "updated_at": utcnow()}
        return await self._collection.find_one_and_update(
            {
                "_id": document_id,
                "claim_token": claim_token,
                "content_version": content_version,
                "status": status,
            },
            {"$set": updates},
            return_document=ReturnDocument.AFTER,
        )

    async def fail_exhausted(self, now: datetime, max_attempts: int) -> list[dict[str, Any]]:
        failed: list[dict[str, Any]] = []
        queries = (
            (
                "processing",
                {
                    "status": "processing",
                    "claim_expires_at": {"$lte": now},
                    "stages.processing.attempts": {"$gte": max_attempts},
                },
            ),
            (
                "enriching",
                {
                    "status": "enriching",
                    "claim_expires_at": {"$lte": now},
                    "stages.enriching.attempts": {"$gte": max_attempts},
                },
            ),
        )
        for stage, query in queries:
            while True:
                document = await self._collection.find_one_and_update(
                    query,
                    {
                        "$set": {
                            "status": "failed",
                            "failed_stage": stage,
                            "claim_token": None,
                            "claim_expires_at": None,
                            "updated_at": now,
                            f"stages.{stage}.state": "failed",
                        }
                    },
                    return_document=ReturnDocument.AFTER,
                )
                if document is None:
                    break
                failed.append(document)
        return failed
