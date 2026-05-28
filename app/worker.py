"""In-process worker pool for async requests.

Design:
- A single bounded `asyncio.Queue` is the source of truth for pending
  async work. Bounded so that under sustained overload `/async` returns
  503 instead of growing memory unboundedly.
- N worker coroutines `await queue.get()` in a loop, run the shared
  work function in a thread (to keep CPU work off the event loop),
  persist status changes, then deliver the callback via CallbackClient.
- Shutdown: the lifespan posts N sentinels into the queue and waits
  up to `shutdown_drain_seconds` for workers to drain inflight items.
  Anything still being processed past the deadline is cancelled and
  marked accordingly.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from app.callback import (
    CallbackClient,
    build_callback_body,
)
from app.config import Settings
from app.db import Database
from app.models import RequestStatus
from app.work import run_work

log = logging.getLogger("consuma.worker")


@dataclass
class Job:
    request_id: str
    payload: dict[str, Any]
    callback_url: str


_SENTINEL: Job | None = None  # used to wake workers for clean shutdown


class WorkerPool:
    def __init__(
        self,
        settings: Settings,
        db: Database,
        callback_client: CallbackClient,
    ):
        self.settings = settings
        self.db = db
        self.callback_client = callback_client
        self.queue: asyncio.Queue[Job | None] = asyncio.Queue(
            maxsize=settings.queue_maxsize
        )
        self._workers: list[asyncio.Task[None]] = []
        self._stopping = asyncio.Event()

    @property
    def depth(self) -> int:
        return self.queue.qsize()

    @property
    def alive(self) -> int:
        return sum(1 for w in self._workers if not w.done())

    def try_enqueue(self, job: Job) -> bool:
        """Non-blocking enqueue. Returns False when the queue is full."""
        try:
            self.queue.put_nowait(job)
            return True
        except asyncio.QueueFull:
            return False

    async def start(self) -> None:
        for i in range(self.settings.workers):
            self._workers.append(
                asyncio.create_task(self._run_worker(i), name=f"worker-{i}")
            )

    async def stop(self) -> None:
        self._stopping.set()
        for _ in self._workers:
            try:
                self.queue.put_nowait(_SENTINEL)
            except asyncio.QueueFull:
                pass

        try:
            await asyncio.wait_for(
                asyncio.gather(*self._workers, return_exceptions=True),
                timeout=self.settings.shutdown_drain_seconds,
            )
        except asyncio.TimeoutError:
            log.warning(
                "worker drain exceeded %.1fs, cancelling remaining workers",
                self.settings.shutdown_drain_seconds,
            )
            for w in self._workers:
                if not w.done():
                    w.cancel()
            await asyncio.gather(*self._workers, return_exceptions=True)

    async def _run_worker(self, idx: int) -> None:
        log.info("worker %d started", idx)
        while True:
            job = await self.queue.get()
            try:
                if job is None:
                    return
                await self._process_job(job)
            except Exception:  # noqa: BLE001
                log.exception("worker %d unexpected error", idx)
            finally:
                self.queue.task_done()

    async def _process_job(self, job: Job) -> None:
        await self.db.mark_started(job.request_id)

        result: dict[str, Any] | None = None
        error: str | None = None
        try:
            result = await asyncio.to_thread(run_work, job.payload)
            terminal_status = RequestStatus.CALLBACK_PENDING
        except Exception as exc:  # noqa: BLE001
            error = f"{type(exc).__name__}: {exc}"
            terminal_status = RequestStatus.CALLBACK_PENDING

        await self.db.mark_completed(
            job.request_id,
            result=result,
            error=error,
            next_status=terminal_status,
        )

        body = build_callback_body(
            request_id=job.request_id,
            result=result,
            error=error,
            status=(
                RequestStatus.COMPLETED if error is None else RequestStatus.FAILED
            ),
        )

        async def _persist_attempt(**kwargs):
            await self.db.record_callback_attempt(
                request_id=job.request_id, **kwargs
            )

        delivered, last_err = await self.callback_client.deliver(
            job.callback_url, body, on_attempt=_persist_attempt
        )

        if delivered:
            await self.db.mark_callback_delivered(job.request_id)
        else:
            await self.db.mark_callback_failed(
                job.request_id,
                error=last_err or "callback delivery failed",
            )
