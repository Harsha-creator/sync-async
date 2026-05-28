"""FastAPI application wiring + HTTP routes."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response

from app.callback import CallbackClient, CallbackUrlError, validate_callback_url
from app.config import Settings, get_settings
from app.db import Database
from app.models import (
    AckResponse,
    AsyncRequest,
    HealthResponse,
    Mode,
    RequestDetail,
    RequestRow,
    RequestStatus,
    SyncRequest,
    SyncResponse,
    utcnow,
)
from app.work import run_work
from app.worker import Job, WorkerPool

log = logging.getLogger("consuma.api")


def _serialize_request_row(row: RequestRow) -> dict[str, Any]:
    d = asdict(row)
    d["mode"] = row.mode.value
    d["status"] = row.status.value
    for k in (
        "created_at",
        "started_at",
        "completed_at",
        "callback_delivered_at",
    ):
        v = d.get(k)
        if isinstance(v, datetime):
            d[k] = v.isoformat()
    return d


def _serialize_request_detail(detail: RequestDetail) -> dict[str, Any]:
    out = _serialize_request_row(detail.request)
    attempts: list[dict[str, Any]] = []
    for a in detail.callback_attempts:
        ad = asdict(a)
        for k in ("started_at", "finished_at"):
            v = ad.get(k)
            if isinstance(v, datetime):
                ad[k] = v.isoformat()
        attempts.append(ad)
    out["callback_attempt_log"] = attempts
    return out


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )
    db = Database(settings.database_url)
    await db.connect()
    callback_client = CallbackClient(settings)
    pool = WorkerPool(settings, db, callback_client)
    await pool.start()

    app.state.settings = settings
    app.state.db = db
    app.state.callback_client = callback_client
    app.state.pool = pool

    log.info(
        "consuma started: workers=%d queue_maxsize=%d db=%s allow_local=%s",
        settings.workers,
        settings.queue_maxsize,
        settings.database_url,
        settings.callback_allow_local,
    )
    try:
        yield
    finally:
        log.info("draining worker pool")
        await pool.stop()
        await callback_client.aclose()
        await db.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="consuma",
        version="0.1.0",
        lifespan=lifespan,
    )

    def get_settings_dep(request: Request) -> Settings:
        return request.app.state.settings

    def get_db(request: Request) -> Database:
        return request.app.state.db

    def get_pool(request: Request) -> WorkerPool:
        return request.app.state.pool

    @app.get("/healthz", response_model=HealthResponse)
    async def healthz(
        response: Response,
        pool: WorkerPool = Depends(get_pool),
        db: Database = Depends(get_db),
    ):
        db_ok = await db.ping()
        alive = pool.alive
        expected = pool.settings.workers
        depth = pool.depth
        maxsize = pool.settings.queue_maxsize

        queue_pressure = (depth / maxsize) if maxsize > 0 else 0.0
        workers_missing = expected > 0 and alive < expected

        # Unhealthy: DB unreachable, or we expected workers and none are
        # alive. Either way the service can't actually do its job, so we
        # return 503 to let an LB pull this instance out of rotation.
        if not db_ok or (expected > 0 and alive == 0):
            status = "unhealthy"
            response.status_code = 503
        elif workers_missing or queue_pressure >= 0.9:
            status = "degraded"
        else:
            status = "ok"

        return HealthResponse(
            status=status,
            db_ok=db_ok,
            queue_depth=depth,
            queue_maxsize=maxsize,
            workers_alive=alive,
            expected_workers=expected,
        )

    @app.post("/sync", response_model=SyncResponse)
    async def sync_endpoint(
        body: SyncRequest,
        db: Database = Depends(get_db),
    ):
        request_id = str(uuid.uuid4())
        await db.insert_request(
            request_id=request_id,
            mode=Mode.SYNC,
            status=RequestStatus.PROCESSING,
            input_payload=body.model_dump(),
        )
        t0 = time.perf_counter()
        try:
            result = await asyncio.to_thread(run_work, body.model_dump())
        except Exception as exc:  # noqa: BLE001
            await db.mark_completed(
                request_id,
                result=None,
                error=f"{type(exc).__name__}: {exc}",
                next_status=RequestStatus.FAILED,
            )
            raise HTTPException(status_code=400, detail=str(exc))
        took_ms = (time.perf_counter() - t0) * 1000.0
        await db.mark_completed(
            request_id,
            result=result,
            error=None,
            next_status=RequestStatus.COMPLETED,
        )
        return SyncResponse(
            request_id=request_id, result=result, took_ms=took_ms
        )

    @app.post("/async", status_code=202, response_model=AckResponse)
    async def async_endpoint(
        body: AsyncRequest,
        db: Database = Depends(get_db),
        pool: WorkerPool = Depends(get_pool),
        settings: Settings = Depends(get_settings_dep),
    ):
        callback_url = str(body.callback_url)
        try:
            validate_callback_url(
                callback_url, allow_local=settings.callback_allow_local
            )
        except CallbackUrlError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        request_id = str(uuid.uuid4())
        payload = {"text": body.text, "complexity": body.complexity}
        row = await db.insert_request(
            request_id=request_id,
            mode=Mode.ASYNC,
            status=RequestStatus.RECEIVED,
            input_payload=payload,
            callback_url=callback_url,
        )

        job = Job(
            request_id=request_id,
            payload=payload,
            callback_url=callback_url,
        )
        if not pool.try_enqueue(job):
            await db.mark_completed(
                request_id,
                result=None,
                error="queue full",
                next_status=RequestStatus.FAILED,
            )
            raise HTTPException(
                status_code=503,
                detail="queue full, retry later",
                headers={"Retry-After": "1"},
            )

        return AckResponse(
            request_id=request_id,
            status="accepted",
            queued_at=row.created_at,
        )

    @app.get("/requests")
    async def list_requests(
        mode: Mode | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=1000),
        db: Database = Depends(get_db),
    ):
        rows = await db.list_requests(mode=mode, limit=limit)
        return {
            "count": len(rows),
            "items": [_serialize_request_row(r) for r in rows],
        }

    @app.get("/requests/{request_id}")
    async def get_request(
        request_id: str, db: Database = Depends(get_db)
    ):
        detail = await db.get_request(request_id)
        if detail is None:
            raise HTTPException(status_code=404, detail="request not found")
        return _serialize_request_detail(detail)

    return app


app = create_app()
