from typing import Literal

Status = Literal["queued", "processing", "enriching", "completed", "failed"]
StageName = Literal["processing", "enriching"]
StageState = Literal["pending", "running", "succeeded", "failed"]

ACTIVE_STATUSES: frozenset[str] = frozenset({"queued", "processing", "enriching"})
TERMINAL_STATUSES: frozenset[str] = frozenset({"completed", "failed"})

USER_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$"
CLIENT_REF_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$"
