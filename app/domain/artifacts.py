"""Stored shape of derived results. Each value records the version it came from."""


def summary_artifact(text: str, version: int, content_hash: str) -> dict:
    """Summary plus the content version and hash it was computed from."""
    return {
        "text": text,
        "content_version": version,
        "content_hash": content_hash,
    }


def tags_artifact(values: list[str], version: int, content_hash: str) -> dict:
    """Tags plus the content version and the summary version they were derived from."""
    return {
        "values": values,
        "content_version": version,
        "content_hash": content_hash,
        "summary_content_version": version,
    }


def stage_record(state: str, version: int, attempts: int = 0) -> dict:
    """One stage's state, stamped with the content version it describes."""
    return {
        "state": state,
        "attempts": attempts,
        "content_version": version,
    }


def fresh_stages(version: int, state: str = "pending", attempts: int = 0) -> dict:
    """Both stages at the same state and version, used when content is replaced."""
    return {
        "processing": stage_record(state, version, attempts),
        "enriching": stage_record(state, version, attempts),
    }
