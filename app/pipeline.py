"""Background worker for the processing stage and the enriching stage."""

import asyncio
import logging
import random
import uuid
from datetime import timedelta

from app.cache import ContentCache
from app.config import Settings
from app.domain.artifacts import summary_artifact, tags_artifact
from app.domain.text import build_summary, build_tags
from app.logging import log
from app.rate_limit import RateLimiter
from app.repository import DocumentRepository
from app.time import utcnow

logger = logging.getLogger(__name__)


class StageFailure(Exception):
    """One simulated attempt of a stage failed and may be retried."""

    def __init__(self, stage: str) -> None:
        self.stage = stage
        super().__init__(stage)


class PipelineRunner:
    """Claim documents and run each stage without blocking the request path.

    A result is written only when the claim token and the content version
    captured for that attempt are still current. Enriching reads the summary
    and does not repeat processing when it fails.
    """

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
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._rng = random.Random()

    async def start(self) -> None:
        """Start one claim loop per configured worker."""
        for index in range(self._settings.worker_concurrency):
            task = asyncio.create_task(self._loop(), name=f"pipeline-{index}")
            self._tasks.append(task)
        log(
            logger,
            logging.INFO,
            "pipeline worker started",
            concurrency=self._settings.worker_concurrency,
        )

    async def stop(self) -> None:
        """Ask the loops to stop and wait for the current sleep or claim to finish."""
        self._stop.set()
        if not self._tasks:
            return
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _loop(self) -> None:
        """Claim work until the runner is stopped. A tick error is logged and the loop continues."""
        while not self._stop.is_set():
            try:
                worked = await self._tick()
            except Exception:
                logger.exception("pipeline tick failed")
                worked = False
            if not worked and not await self._sleep(self._settings.pipeline_poll_interval_seconds):
                return

    async def _tick(self) -> bool:
        """Run one claim. Return False when nothing was ready."""
        now = utcnow()
        expires = now + timedelta(seconds=self._settings.pipeline_claim_lease_seconds)
        max_attempts = self._settings.pipeline_max_attempts
        token = uuid.uuid4().hex

        document = await self._repository.claim_queued(now, expires, token, max_attempts)
        if document is not None:
            await self._run_processing(document)
            return True

        document = await self._repository.claim_enriching_retry(now, expires, token, max_attempts)
        if document is not None:
            await self._run_enriching(document)
            return True

        document = await self._repository.claim_expired(
            "processing",
            "processing",
            now,
            expires,
            token,
            max_attempts,
        )
        if document is not None:
            await self._run_processing(document)
            return True

        document = await self._repository.claim_expired(
            "enriching",
            "enriching",
            now,
            expires,
            token,
            max_attempts,
        )
        if document is not None:
            await self._run_enriching(document)
            return True

        exhausted = await self._repository.fail_exhausted(now, max_attempts)
        for failed in exhausted:
            await self._limiter.release(
                failed["user_id"],
                str(failed["_id"]),
                failed["content_version"],
            )
            log(
                logger,
                logging.WARNING,
                "exhausted stage claim marked failed",
                document_id=str(failed["_id"]),
                failed_stage=failed.get("failed_stage"),
            )
        return False

    async def _run_processing(self, document: dict) -> None:
        """Write the summary for the claimed version, then run enriching.

        A version change or a lost claim discards the summary instead of
        attaching it to newer content.
        """
        version = document["content_version"]
        digest = document["content_hash"]
        token = document["claim_token"]
        attempt = document["stages"]["processing"]["attempts"]
        document_id = document["_id"]
        if not await self._limiter.touch(document["user_id"], str(document_id), version):
            log(logger, logging.INFO, "processing stopped because the version moved", document_id=str(document_id))
            return
        try:
            if not await self._wait_stage("processing"):
                return
            summary = build_summary(document["content"])
        except StageFailure:
            await self._fail_processing(document, attempt)
            return

        enriching_attempts = document["stages"]["enriching"]["attempts"] + 1
        updated = await self._repository.write_claimed(
            document_id,
            token,
            version,
            "processing",
            {
                "status": "enriching",
                "failed_stage": None,
                "summary": summary_artifact(summary, version, digest),
                "claim_expires_at": utcnow()
                + timedelta(seconds=self._settings.pipeline_claim_lease_seconds),
                "stages.processing.state": "succeeded",
                "stages.processing.content_version": version,
                "stages.enriching.state": "running",
                "stages.enriching.content_version": version,
                "stages.enriching.attempts": enriching_attempts,
            },
        )
        if updated is None:
            log(logger, logging.INFO, "discarded stale processing result", document_id=str(document_id), content_version=version)
            return
        await self._run_enriching(updated)

    async def _run_enriching(self, document: dict) -> None:
        """Write tags derived from this version's summary.

        The summary is used only when its version and hash match. Failure
        leaves that summary in place and retries enriching alone.
        """
        version = document["content_version"]
        digest = document["content_hash"]
        token = document["claim_token"]
        document_id = document["_id"]
        summary = document.get("summary") or {}
        if summary.get("content_version") != version or summary.get("content_hash") != digest:
            await self._repository.write_claimed(
                document_id,
                token,
                version,
                "enriching",
                {
                    "status": "queued",
                    "claim_token": None,
                    "claim_expires_at": None,
                    "not_before": utcnow(),
                    "stages.processing.state": "pending",
                    "stages.processing.content_version": version,
                    "stages.processing.attempts": 0,
                    "stages.enriching.state": "pending",
                    "stages.enriching.content_version": version,
                    "stages.enriching.attempts": 0,
                },
            )
            log(logger, logging.WARNING, "enriching found no matching summary and requeued processing", document_id=str(document_id))
            return

        attempt = document["stages"]["enriching"]["attempts"]
        if not await self._limiter.touch(document["user_id"], str(document_id), version):
            log(logger, logging.INFO, "enriching stopped because the version moved", document_id=str(document_id))
            return
        try:
            if not await self._wait_stage("enriching"):
                return
            tags = build_tags(document["content"], summary["text"])
        except StageFailure:
            await self._fail_enriching(document, attempt)
            return

        updated = await self._repository.write_claimed(
            document_id,
            token,
            version,
            "enriching",
            {
                "status": "completed",
                "failed_stage": None,
                "tags": tags_artifact(tags, version, digest),
                "claim_token": None,
                "claim_expires_at": None,
                "stages.enriching.state": "succeeded",
                "stages.enriching.content_version": version,
            },
        )
        if updated is None:
            log(logger, logging.INFO, "discarded stale enriching result", document_id=str(document_id), content_version=version)
            return
        await self._cache.put(digest, summary["text"], tags)
        await self._limiter.release(document["user_id"], str(document_id), version)
        log(logger, logging.INFO, "document completed", document_id=str(document_id), content_version=version)

    async def _fail_processing(self, document: dict, attempt: int) -> None:
        """Requeue processing with backoff, or mark the stage failed when attempts are spent."""
        version = document["content_version"]
        document_id = document["_id"]
        if attempt >= self._settings.pipeline_max_attempts:
            updated = await self._repository.write_claimed(
                document_id,
                document["claim_token"],
                version,
                "processing",
                {
                    "status": "failed",
                    "failed_stage": "processing",
                    "claim_token": None,
                    "claim_expires_at": None,
                    "stages.processing.state": "failed",
                    "stages.processing.content_version": version,
                },
            )
            if updated is not None:
                await self._limiter.release(document["user_id"], str(document_id), version)
            log(logger, logging.WARNING, "processing failed", document_id=str(document_id), attempt=attempt)
            return

        delay = self._backoff(attempt)
        updated = await self._repository.write_claimed(
            document_id,
            document["claim_token"],
            version,
            "processing",
            {
                "status": "queued",
                "claim_token": None,
                "claim_expires_at": None,
                "not_before": utcnow() + timedelta(seconds=delay),
                "stages.processing.state": "pending",
                "stages.processing.content_version": version,
            },
        )
        if updated is not None:
            log(logger, logging.INFO, "processing scheduled for retry", document_id=str(document_id), attempt=attempt, backoff_seconds=delay)

    async def _fail_enriching(self, document: dict, attempt: int) -> None:
        """Retry enriching without clearing the summary, or fail that stage."""
        version = document["content_version"]
        document_id = document["_id"]
        if attempt >= self._settings.pipeline_max_attempts:
            updated = await self._repository.write_claimed(
                document_id,
                document["claim_token"],
                version,
                "enriching",
                {
                    "status": "failed",
                    "failed_stage": "enriching",
                    "claim_token": None,
                    "claim_expires_at": None,
                    "stages.enriching.state": "failed",
                    "stages.enriching.content_version": version,
                },
            )
            if updated is not None:
                await self._limiter.release(document["user_id"], str(document_id), version)
            log(
                logger,
                logging.WARNING,
                "enriching failed and the processing summary was kept",
                document_id=str(document_id),
                attempt=attempt,
            )
            return

        delay = self._backoff(attempt)
        updated = await self._repository.write_claimed(
            document_id,
            document["claim_token"],
            version,
            "enriching",
            {
                "status": "enriching",
                "claim_token": None,
                "claim_expires_at": None,
                "not_before": utcnow() + timedelta(seconds=delay),
                "stages.enriching.state": "pending",
                "stages.enriching.content_version": version,
            },
        )
        if updated is not None:
            log(logger, logging.INFO, "enriching scheduled for retry", document_id=str(document_id), attempt=attempt, backoff_seconds=delay)

    async def _wait_stage(self, stage: str) -> bool:
        """Sleep for the simulated stage duration.

        Returns False when shutdown interrupts the sleep. Raises StageFailure
        when this attempt is chosen to fail.
        """
        if stage == "processing":
            rate = self._settings.pipeline_processing_failure_rate
            low = self._settings.pipeline_processing_min_seconds
            high = self._settings.pipeline_processing_max_seconds
        else:
            rate = self._settings.pipeline_enriching_failure_rate
            low = self._settings.pipeline_enriching_min_seconds
            high = self._settings.pipeline_enriching_max_seconds
        if self._rng.random() < rate:
            raise StageFailure(stage)
        delay = self._rng.uniform(low, high) if high > low else low
        return await self._sleep(delay)

    def _backoff(self, attempt: int) -> float:
        """Exponential delay before the next attempt of a failed stage."""
        return self._settings.pipeline_retry_backoff_seconds * (2 ** max(attempt - 1, 0))

    async def _sleep(self, seconds: float) -> bool:
        """Wait for seconds, or return False immediately when the runner is stopping."""
        if seconds <= 0:
            return not self._stop.is_set()
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except TimeoutError:
            return True
        return False
