"""MongoDB connection and the indexes the API queries through."""

import logging

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import ASCENDING, DESCENDING, IndexModel

from app.config import Settings

logger = logging.getLogger(__name__)

DOCUMENTS = "documents"

INDEXES = [
    IndexModel(
        [("user_id", ASCENDING), ("created_at", DESCENDING), ("_id", DESCENDING)],
        name="user_created",
    ),
    IndexModel(
        [
            ("user_id", ASCENDING),
            ("status", ASCENDING),
            ("created_at", DESCENDING),
            ("_id", DESCENDING),
        ],
        name="user_status_created",
    ),
    IndexModel(
        [("content_hash", ASCENDING), ("status", ASCENDING)],
        name="content_hash_status",
    ),
    IndexModel(
        [("client_doc_ref", ASCENDING)],
        name="client_doc_ref_unique",
        unique=True,
        partialFilterExpression={"client_doc_ref": {"$type": "string"}},
    ),
    IndexModel(
        [("status", ASCENDING), ("not_before", ASCENDING), ("created_at", ASCENDING)],
        name="pipeline_claim",
    ),
]


class MongoDatabase:
    """Owns the Motor client and creates document indexes on connect."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self.client: AsyncIOMotorClient | None = None
        self.db = None

    async def connect(self) -> None:
        """Ping MongoDB and ensure indexes. A failure aborts startup."""
        self.client = AsyncIOMotorClient(
            self._settings.mongo_uri,
            tz_aware=True,
            serverSelectionTimeoutMS=self._settings.mongo_server_selection_ms,
        )
        self.db = self.client[self._settings.mongo_db]
        await self.client.admin.command("ping")
        await self.documents.create_indexes(INDEXES)

    async def close(self) -> None:
        """Close the client if connect succeeded."""
        if self.client is not None:
            self.client.close()

    @property
    def documents(self) -> AsyncIOMotorCollection:
        """Return the documents collection. Raises if connect has not finished."""
        if self.db is None:
            raise RuntimeError("MongoDB is not connected")
        return self.db[DOCUMENTS]

    async def ping(self) -> bool:
        """Return whether MongoDB answers. Logs and returns False on error."""
        if self.client is None:
            return False
        try:
            await self.client.admin.command("ping")
        except Exception:
            logger.warning("mongodb ping failed", exc_info=True)
            return False
        return True
