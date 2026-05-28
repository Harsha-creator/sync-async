"""Tiny embedded callback receiver used by the load generator.

Runs an in-process Starlette+Uvicorn server. Records `(request_id, arrival_time)`
into a shared dict so the runner can compute time-to-callback by subtracting
the request submission time.

Exposes a `fail_mode` knob so the demo can show retry behaviour:
- "ok"    -> always 200
- "5xx"   -> always 500 (drives retries until cap)
- "flaky" -> first attempt 500, subsequent attempts 200 (shows recovery)
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

log = logging.getLogger("consuma.loadgen.callback")


@dataclass
class CallbackRecorder:
    arrivals: dict[str, float] = field(default_factory=dict)
    attempts: dict[str, int] = field(default_factory=dict)
    fail_mode: str = "ok"  # "ok" | "5xx" | "flaky"


def build_app(recorder: CallbackRecorder) -> Starlette:
    async def receive(request: Request):
        body = await request.json()
        rid = body.get("request_id", "?")
        recorder.attempts[rid] = recorder.attempts.get(rid, 0) + 1

        if recorder.fail_mode == "5xx":
            return JSONResponse({"error": "forced"}, status_code=500)
        if (
            recorder.fail_mode == "flaky"
            and recorder.attempts[rid] == 1
        ):
            return JSONResponse({"error": "flaky"}, status_code=500)

        recorder.arrivals.setdefault(rid, time.perf_counter())
        return JSONResponse({"ok": True})

    return Starlette(routes=[Route("/callback", receive, methods=["POST"])])


class CallbackServer:
    """Async-context-managed uvicorn server."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        fail_mode: str = "ok",
    ):
        self.recorder = CallbackRecorder(fail_mode=fail_mode)
        self._app = build_app(self.recorder)
        self._config = uvicorn.Config(
            self._app,
            host=host,
            port=port,
            log_level="warning",
            access_log=False,
        )
        self._server = uvicorn.Server(self._config)
        self._task: asyncio.Task | None = None

    @property
    def url(self) -> str:
        servers = self._server.servers
        if not servers:
            raise RuntimeError("server not started yet")
        sock = servers[0].sockets[0]
        host, port = sock.getsockname()[:2]
        return f"http://{host}:{port}/callback"

    async def __aenter__(self) -> "CallbackServer":
        self._task = asyncio.create_task(self._server.serve())
        while not self._server.started:
            await asyncio.sleep(0.01)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._server.should_exit = True
        if self._task is not None:
            await self._task
