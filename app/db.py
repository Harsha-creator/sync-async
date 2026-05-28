"""Persistence layer (SQLite via aiosqlite).

Design notes:
- One long-lived aiosqlite connection. aiosqlite serialises all calls on
  that connection through a single worker thread, which means we get
  serialised writes for free without building a writer-task channel.
  Under the demo's load (sub-10k req/s) this is fine; the README calls
  out Postgres as the next step.
- WAL mode + synchronous=NORMAL trade a tiny durability window for
  meaningful throughput.
- Schema is two tables: `requests` (one row per submission) and
  `callback_attempts` (one row per delivery attempt, append-only).
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Iterable

import aiosqlite

from app.models import (
    CallbackAttemptRow,
    Mode,
    RequestDetail,
    RequestRow,
    RequestStatus,
    utcnow,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS requests (
    id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    input_payload TEXT NOT NULL,
    result TEXT,
    error TEXT,
    callback_url TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    callback_attempts INTEGER NOT NULL DEFAULT 0,
    last_callback_status INTEGER,
    last_callback_error TEXT,
    callback_delivered_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_requests_mode_created
    ON requests(mode, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_requests_status
    ON requests(status);

CREATE TABLE IF NOT EXISTS callback_attempts (
    request_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    http_status INTEGER,
    error TEXT,
    will_retry INTEGER NOT NULL,
    PRIMARY KEY (request_id, attempt)
);
"""


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


def _parse_iso(s: str | None) -> datetime | None:
    return datetime.fromisoformat(s) if s else None


def _dump(obj: Any) -> str | None:
    return json.dumps(obj) if obj is not None else None


def _load(s: str | None) -> Any:
    return json.loads(s) if s else None


class Database:
    """Thin async wrapper around a single aiosqlite connection."""

    def __init__(self, path: str):
        self.path = path
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute("PRAGMA synchronous=NORMAL;")
        await self._conn.execute("PRAGMA foreign_keys=ON;")
        await self._conn.executescript(SCHEMA)
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def ping(self) -> bool:
        """Cheap liveness probe. Returns True iff the connection responds."""
        if self._conn is None:
            return False
        try:
            async with self._conn.execute("SELECT 1") as cur:
                row = await cur.fetchone()
            return row is not None and row[0] == 1
        except Exception:  # noqa: BLE001 - any failure means "not healthy"
            return False

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database not connected")
        return self._conn

    async def insert_request(
        self,
        *,
        request_id: str,
        mode: Mode,
        status: RequestStatus,
        input_payload: dict[str, Any],
        callback_url: str | None = None,
    ) -> RequestRow:
        created = utcnow()
        await self.conn.execute(
            """
            INSERT INTO requests (
                id, mode, status, input_payload, callback_url, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                mode.value,
                status.value,
                _dump(input_payload),
                callback_url,
                _iso(created),
            ),
        )
        await self.conn.commit()
        return RequestRow(
            id=request_id,
            mode=mode,
            status=status,
            input_payload=input_payload,
            result=None,
            error=None,
            callback_url=callback_url,
            created_at=created,
            started_at=None,
            completed_at=None,
        )

    async def mark_started(self, request_id: str) -> None:
        await self.conn.execute(
            "UPDATE requests SET status=?, started_at=? WHERE id=?",
            (RequestStatus.PROCESSING.value, _iso(utcnow()), request_id),
        )
        await self.conn.commit()

    async def mark_completed(
        self,
        request_id: str,
        *,
        result: dict[str, Any] | None,
        error: str | None,
        next_status: RequestStatus,
    ) -> None:
        await self.conn.execute(
            """
            UPDATE requests
            SET status=?, result=?, error=?, completed_at=?
            WHERE id=?
            """,
            (
                next_status.value,
                _dump(result),
                error,
                _iso(utcnow()),
                request_id,
            ),
        )
        await self.conn.commit()

    async def record_callback_attempt(
        self,
        *,
        request_id: str,
        attempt: int,
        started_at: datetime,
        finished_at: datetime | None,
        http_status: int | None,
        error: str | None,
        will_retry: bool,
    ) -> None:
        await self.conn.execute(
            """
            INSERT INTO callback_attempts (
                request_id, attempt, started_at, finished_at,
                http_status, error, will_retry
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                attempt,
                _iso(started_at),
                _iso(finished_at),
                http_status,
                error,
                1 if will_retry else 0,
            ),
        )
        await self.conn.execute(
            """
            UPDATE requests
            SET callback_attempts=?,
                last_callback_status=?,
                last_callback_error=?
            WHERE id=?
            """,
            (attempt, http_status, error, request_id),
        )
        await self.conn.commit()

    async def mark_callback_delivered(self, request_id: str) -> None:
        await self.conn.execute(
            """
            UPDATE requests
            SET status=?, callback_delivered_at=?
            WHERE id=?
            """,
            (
                RequestStatus.CALLBACK_DELIVERED.value,
                _iso(utcnow()),
                request_id,
            ),
        )
        await self.conn.commit()

    async def mark_callback_failed(
        self, request_id: str, *, error: str
    ) -> None:
        await self.conn.execute(
            """
            UPDATE requests
            SET status=?, last_callback_error=?
            WHERE id=?
            """,
            (RequestStatus.CALLBACK_FAILED.value, error, request_id),
        )
        await self.conn.commit()

    async def get_request(self, request_id: str) -> RequestDetail | None:
        async with self.conn.execute(
            "SELECT * FROM requests WHERE id=?", (request_id,)
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return None
        request = _row_to_request(row)
        async with self.conn.execute(
            """
            SELECT * FROM callback_attempts
            WHERE request_id=? ORDER BY attempt ASC
            """,
            (request_id,),
        ) as cur:
            attempt_rows = await cur.fetchall()
        attempts = [_row_to_attempt(r) for r in attempt_rows]
        return RequestDetail(request=request, callback_attempts=attempts)

    async def list_requests(
        self, *, mode: Mode | None = None, limit: int = 100
    ) -> list[RequestRow]:
        limit = max(1, min(1000, limit))
        if mode is not None:
            query = (
                "SELECT * FROM requests WHERE mode=? "
                "ORDER BY created_at DESC LIMIT ?"
            )
            params: Iterable[Any] = (mode.value, limit)
        else:
            query = "SELECT * FROM requests ORDER BY created_at DESC LIMIT ?"
            params = (limit,)
        async with self.conn.execute(query, params) as cur:
            rows = await cur.fetchall()
        return [_row_to_request(r) for r in rows]


def _row_to_request(row: aiosqlite.Row) -> RequestRow:
    return RequestRow(
        id=row["id"],
        mode=Mode(row["mode"]),
        status=RequestStatus(row["status"]),
        input_payload=_load(row["input_payload"]) or {},
        result=_load(row["result"]),
        error=row["error"],
        callback_url=row["callback_url"],
        created_at=_parse_iso(row["created_at"]),  # type: ignore[arg-type]
        started_at=_parse_iso(row["started_at"]),
        completed_at=_parse_iso(row["completed_at"]),
        callback_attempts=row["callback_attempts"] or 0,
        last_callback_status=row["last_callback_status"],
        last_callback_error=row["last_callback_error"],
        callback_delivered_at=_parse_iso(row["callback_delivered_at"]),
    )


def _row_to_attempt(row: aiosqlite.Row) -> CallbackAttemptRow:
    return CallbackAttemptRow(
        request_id=row["request_id"],
        attempt=row["attempt"],
        started_at=_parse_iso(row["started_at"]),  # type: ignore[arg-type]
        finished_at=_parse_iso(row["finished_at"]),
        http_status=row["http_status"],
        error=row["error"],
        will_retry=bool(row["will_retry"]),
    )
