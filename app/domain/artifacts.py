def summary_artifact(text: str, version: int, content_hash: str) -> dict:
    return {
        "text": text,
        "content_version": version,
        "content_hash": content_hash,
    }


def tags_artifact(values: list[str], version: int, content_hash: str) -> dict:
    return {
        "values": values,
        "content_version": version,
        "content_hash": content_hash,
        "summary_content_version": version,
    }


def stage_record(state: str, version: int, attempts: int = 0) -> dict:
    return {
        "state": state,
        "attempts": attempts,
        "content_version": version,
    }


def fresh_stages(version: int, state: str = "pending", attempts: int = 0) -> dict:
    return {
        "processing": stage_record(state, version, attempts),
        "enriching": stage_record(state, version, attempts),
    }
