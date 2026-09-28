from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

TERMINAL = frozenset({"completed", "failed", "rejected", "canceled"})
BUSY = frozenset({"accepted", "working"})


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def timestamp(value: str) -> float:
    date = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if date.tzinfo is None:
        raise ValueError("Timezone required")
    return date.timestamp()


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(dumps(value).encode()).hexdigest()


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


class Capability(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$")
    connector: str
    input_schema: dict = Field(default_factory=lambda: {"type": "object"})
    output_schema: dict = Field(default_factory=lambda: {"type": "object"})
    timeout_seconds: float = Field(default=180, gt=0, le=86400)
    poll_seconds: float = Field(default=2, ge=0.1)
    side_effects: bool = False
    allow_continue: bool = False
    allow_cancel: bool = False


class BusinessResult(BaseModel):
    """Connector contract, deliberately independent of any business task number."""
    model_config = ConfigDict(extra="forbid")
    state: Literal["working", "awaiting-input", "succeeded", "failed", "canceled"]
    output: dict[str, Any] = Field(default_factory=dict)
    text: str = ""
    job_id: str | None = None
    error_code: str | None = None
    # True only when the business operation is known to have stopped/completed.
    settled: bool = True


class ExecutionContext(BaseModel):
    request_id: str
    task_id: str
    session_id: str
    group_id: str
    leader_aic: str
    partner_aic: str
    capability_id: str
