"""End-to-end API smoke tests.

These use httpx.ASGITransport so we don't need a real port for most
flows. For the callback delivery + retry tests we spin up the embedded
callback server (loadgen.callback_server.CallbackServer) on a real
loopback port -- httpx still talks to the API in-process, but the
worker pool does a real outbound HTTP POST.

CONSUMA_CALLBACK_ALLOW_LOCAL=true is set per-test via monkeypatch
because we need to call back into 127.0.0.1.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from app import config as cfg
from app.main import create_app
from loadgen.callback_server import CallbackServer


def _fresh_db_path() -> str:
    fd, path = tempfile.mkstemp(prefix="consuma-test-", suffix=".db")
    os.close(fd)
    return path


@pytest.fixture(autouse=True)
def _reset_settings_cache():
    cfg.reset_settings_for_tests()
    yield
    cfg.reset_settings_for_tests()


@pytest.fixture
def env(monkeypatch):
    """Default env for tests: local-allow on, tiny callback backoffs."""
    monkeypatch.setenv("CONSUMA_CALLBACK_ALLOW_LOCAL", "true")
    monkeypatch.setenv("CONSUMA_CALLBACK_INITIAL_BACKOFF_SECONDS", "0.01")
    monkeypatch.setenv("CONSUMA_CALLBACK_MAX_BACKOFF_SECONDS", "0.05")
    monkeypatch.setenv("CONSUMA_CALLBACK_TIMEOUT_SECONDS", "2.0")
    monkeypatch.setenv("CONSUMA_DATABASE_URL", _fresh_db_path())
    cfg.reset_settings_for_tests()
    yield


async def _client(app) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test"
    ) as c:
        yield c


@pytest.mark.asyncio
async def test_sync_happy_path(env):
    app = create_app()
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as c:
            r = await c.post("/sync", json={"text": "hello", "complexity": 1})
            assert r.status_code == 200
            body = r.json()
            assert body["result"]["char_count"] == 5
            assert body["result"]["word_count"] == 1
            assert "sha256" in body["result"]
            assert body["took_ms"] >= 0

            # request shows up in listing
            lst = await c.get("/requests?mode=sync")
            assert lst.status_code == 200
            assert lst.json()["count"] == 1

            # individual lookup
            detail = await c.get(f"/requests/{body['request_id']}")
            assert detail.status_code == 200
            assert detail.json()["status"] == "completed"


@pytest.mark.asyncio
async def test_async_callback_delivered(env):
    app = create_app()
    async with app.router.lifespan_context(app):
        async with CallbackServer(fail_mode="ok") as cb:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test"
            ) as c:
                r = await c.post(
                    "/async",
                    json={
                        "text": "hi",
                        "complexity": 1,
                        "callback_url": cb.url,
                    },
                )
                assert r.status_code == 202
                rid = r.json()["request_id"]

                # wait for callback to land
                for _ in range(200):
                    if rid in cb.recorder.arrivals:
                        break
                    await asyncio.sleep(0.05)
                assert rid in cb.recorder.arrivals

                # DB reflects delivery
                for _ in range(40):
                    d = (await c.get(f"/requests/{rid}")).json()
                    if d["status"] == "callback_delivered":
                        break
                    await asyncio.sleep(0.05)
                assert d["status"] == "callback_delivered"
                assert d["callback_attempts"] >= 1
                assert d["callback_attempt_log"][0]["http_status"] == 200


@pytest.mark.asyncio
async def test_async_callback_retries_then_fails(env, monkeypatch):
    monkeypatch.setenv("CONSUMA_CALLBACK_MAX_ATTEMPTS", "3")
    cfg.reset_settings_for_tests()

    app = create_app()
    async with app.router.lifespan_context(app):
        async with CallbackServer(fail_mode="5xx") as cb:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test"
            ) as c:
                r = await c.post(
                    "/async",
                    json={
                        "text": "hi",
                        "complexity": 1,
                        "callback_url": cb.url,
                    },
                )
                assert r.status_code == 202
                rid = r.json()["request_id"]

                # wait for terminal callback_failed state
                for _ in range(200):
                    d = (await c.get(f"/requests/{rid}")).json()
                    if d["status"] == "callback_failed":
                        break
                    await asyncio.sleep(0.05)
                assert d["status"] == "callback_failed"
                assert d["callback_attempts"] == 3
                # all attempts recorded, all 500s, last one not retried
                log = d["callback_attempt_log"]
                assert len(log) == 3
                assert all(a["http_status"] == 500 for a in log)
                assert log[-1]["will_retry"] is False


@pytest.mark.asyncio
async def test_async_callback_recovers_on_flaky(env):
    app = create_app()
    async with app.router.lifespan_context(app):
        async with CallbackServer(fail_mode="flaky") as cb:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test"
            ) as c:
                r = await c.post(
                    "/async",
                    json={
                        "text": "hi",
                        "complexity": 1,
                        "callback_url": cb.url,
                    },
                )
                rid = r.json()["request_id"]
                for _ in range(200):
                    d = (await c.get(f"/requests/{rid}")).json()
                    if d["status"] == "callback_delivered":
                        break
                    await asyncio.sleep(0.05)
                assert d["status"] == "callback_delivered"
                assert d["callback_attempts"] == 2
                assert d["callback_attempt_log"][0]["http_status"] == 500
                assert d["callback_attempt_log"][1]["http_status"] == 200


@pytest.mark.asyncio
async def test_async_rejects_metadata_ip(env):
    app = create_app()
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as c:
            r = await c.post(
                "/async",
                json={
                    "text": "x",
                    "complexity": 1,
                    "callback_url": "http://169.254.169.254/latest/",
                },
            )
            assert r.status_code == 400
            assert "disallowed" in r.json()["detail"]


@pytest.mark.asyncio
async def test_async_503_when_queue_full(env, monkeypatch):
    # Workers=0 means nothing will drain the queue, so it stays full
    # deterministically. The /async endpoint should respond with 503.
    monkeypatch.setenv("CONSUMA_QUEUE_MAXSIZE", "1")
    monkeypatch.setenv("CONSUMA_WORKERS", "0")
    cfg.reset_settings_for_tests()

    app = create_app()
    async with app.router.lifespan_context(app):
        pool = app.state.pool
        from app.worker import Job

        pool.queue.put_nowait(
            Job(request_id="prefill", payload={"text": "x"}, callback_url="http://x")
        )
        assert pool.queue.full()

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as c:
            r = await c.post(
                "/async",
                json={
                    "text": "x",
                    "complexity": 1,
                    "callback_url": "http://127.0.0.1:1/cb",
                },
            )
            assert r.status_code == 503
            assert r.headers.get("retry-after") == "1"

        # Drain the queue so the lifespan shutdown is fast.
        try:
            pool.queue.get_nowait()
            pool.queue.task_done()
        except asyncio.QueueEmpty:
            pass


@pytest.mark.asyncio
async def test_healthz_reports_queue_state(env):
    app = create_app()
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as c:
            r = await c.get("/healthz")
            assert r.status_code == 200
            body = r.json()
            assert body["status"] == "ok"
            assert body["db_ok"] is True
            assert body["queue_depth"] == 0
            assert body["workers_alive"] >= 1
            assert body["queue_maxsize"] >= 1
            assert body["expected_workers"] == body["workers_alive"]


@pytest.mark.asyncio
async def test_healthz_degraded_under_queue_pressure(env, monkeypatch):
    # Tiny queue + no workers means even a single pre-loaded job
    # pushes queue_depth/maxsize to 100% -> "degraded".
    monkeypatch.setenv("CONSUMA_QUEUE_MAXSIZE", "2")
    monkeypatch.setenv("CONSUMA_WORKERS", "1")
    cfg.reset_settings_for_tests()

    app = create_app()
    async with app.router.lifespan_context(app):
        pool = app.state.pool
        # Cancel the worker so the queue can stay full.
        for w in pool._workers:
            w.cancel()
        await asyncio.gather(*pool._workers, return_exceptions=True)

        from app.worker import Job
        pool.queue.put_nowait(
            Job(request_id="x", payload={"text": "x"}, callback_url="http://x")
        )
        pool.queue.put_nowait(
            Job(request_id="y", payload={"text": "x"}, callback_url="http://x")
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as c:
            r = await c.get("/healthz")
            # workers_alive=0 with expected_workers=1 -> unhealthy 503
            # (workers all gone is a worse signal than queue pressure)
            assert r.status_code == 503
            body = r.json()
            assert body["status"] == "unhealthy"
            assert body["workers_alive"] == 0
            assert body["expected_workers"] == 1

        # Drain so shutdown is fast.
        for _ in range(2):
            try:
                pool.queue.get_nowait()
                pool.queue.task_done()
            except asyncio.QueueEmpty:
                break


@pytest.mark.asyncio
async def test_healthz_unhealthy_when_db_closed(env):
    app = create_app()
    async with app.router.lifespan_context(app):
        # Close the underlying connection out from under the app to
        # simulate DB death. `ping()` should fail, status -> unhealthy.
        await app.state.db.close()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as c:
            r = await c.get("/healthz")
            assert r.status_code == 503
            body = r.json()
            assert body["status"] == "unhealthy"
            assert body["db_ok"] is False


@pytest.mark.asyncio
async def test_request_not_found(env):
    app = create_app()
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as c:
            r = await c.get("/requests/00000000-0000-0000-0000-000000000000")
            assert r.status_code == 404
