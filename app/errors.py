class AppError(Exception):
    pass


class NotFoundError(AppError):
    pass


class RateLimitError(AppError):
    def __init__(self, retry_after: int = 5) -> None:
        self.retry_after = retry_after
        super().__init__("too many documents in the active pipeline")


class ConflictError(AppError):
    def __init__(self, detail: str, content_version: int | None = None) -> None:
        self.detail = detail
        self.content_version = content_version
        super().__init__(detail)


class UnavailableError(AppError):
    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(detail)
