"""Pydantic request/response models + internal row types.

Kept in one file because the surface is small. The DB row types are
plain dataclasses to keep the storage layer free of Pydantic at the
hot path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, HttpUrl


class Mode(str, Enum):
    SYNC = "sync"
    ASYNC = "async"


class RequestStatus(str, Enum):
    RECEIVED = "received"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CALLBACK_PENDING = "callback_pending"
    CALLBACK_DELIVERED = "callback_delivered"
    CALLBACK_FAILED = "callback_failed"


class WorkPayload(BaseModel):
    text: str = Field(..., max_length=100_000)
    complexity: int = Field(1, ge=1, le=20)


class SyncRequest(WorkPayload):
    pass


class AsyncRequest(WorkPayload):
    callback_url: HttpUrl


class SyncResponse(BaseModel):
    request_id: str
    result: dict[str, Any]
    took_ms: float


class AckResponse(BaseModel):
    request_id: str
    status: str = "accepted"
    queued_at: datetime


class CallbackBody(BaseModel):
    request_id: str
    status: RequestStatus
    result: dict[str, Any] | None = None
    error: str | None = None
    completed_at: datetime
    attempt: int


class HealthResponse(BaseModel):
    status: str  # "ok" | "degraded" | "unhealthy"
    db_ok: bool
    queue_depth: int
    queue_maxsize: int
    workers_alive: int
    expected_workers: int


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class RequestRow:
    id: str
    mode: Mode
    status: RequestStatus
    input_payload: dict[str, Any]
    result: dict[str, Any] | None
    error: str | None
    callback_url: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    callback_attempts: int = 0
    last_callback_status: int | None = None
    last_callback_error: str | None = None
    callback_delivered_at: datetime | None = None


@dataclass
class CallbackAttemptRow:
    request_id: str
    attempt: int
    started_at: datetime
    finished_at: datetime | None
    http_status: int | None
    error: str | None
    will_retry: bool


@dataclass
class RequestDetail:
    request: RequestRow
    callback_attempts: list[CallbackAttemptRow] = field(default_factory=list)
