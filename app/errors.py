"""Domain errors. Routers map these to HTTP responses."""


class AppError(Exception):
    """Base for errors the API knows how to answer."""


class NotFoundError(AppError):
    """The document is missing, or the caller does not own it."""


class RateLimitError(AppError):
    """The caller already has the maximum number of active documents."""

    def __init__(self, retry_after: int = 5) -> None:
        self.retry_after = retry_after
        super().__init__("too many documents in the active pipeline")


class ConflictError(AppError):
    """A version check lost, or a client_doc_ref belongs to another user."""

    def __init__(self, detail: str, content_version: int | None = None) -> None:
        self.detail = detail
        self.content_version = content_version
        super().__init__(detail)


class UnavailableError(AppError):
    """Admission could not be decided, usually because the user lock stayed busy."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(detail)
