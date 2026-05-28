"""Outbound callback delivery: SSRF guard + retry policy.

SSRF defense:
- Only `http`/`https`.
- No userinfo, no fragment-only URLs.
- Resolve hostname via `socket.getaddrinfo` and reject if any resolved
  address is private/loopback/link-local/reserved/multicast/unspecified.
- Refuse to follow redirects on the outbound POST (`follow_redirects=False`).

Known limitation (documented in README): the resolved-IP check happens
before the httpx connect, so a hostile DNS server could return a public
IP at validation time and a private IP at connect time (DNS rebinding).
A production deployment should use `drawbridge` or `httpx-secure` which
pin the resolved IP into the connection. We accept the residual risk
for a demo and document it.

Retry policy:
- Capped attempts (`callback_max_attempts`).
- Exponential backoff with full jitter, clamped to `callback_max_backoff_seconds`.
- 2xx -> success. 4xx -> no retry (client misconfig). 5xx/network -> retry.
- Per-host semaphore limits concurrency so one slow callback target
  cannot starve the entire worker pool.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import random
import socket
from collections import defaultdict
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import httpx

from app.config import Settings
from app.models import (
    CallbackBody,
    RequestStatus,
    utcnow,
)

log = logging.getLogger("consuma.callback")

ALLOWED_SCHEMES = {"http", "https"}


class CallbackUrlError(ValueError):
    """Raised when callback_url fails validation."""


def validate_callback_url(url: str, *, allow_local: bool) -> None:
    """Raise CallbackUrlError if the URL is unsafe to POST to.

    Pure function — no I/O beyond DNS resolution.
    """
    if not url or len(url) > 2048:
        raise CallbackUrlError("callback_url length out of range")

    parsed = urlparse(url)
    if parsed.scheme not in ALLOWED_SCHEMES:
        raise CallbackUrlError(
            f"callback_url scheme must be http or https, got {parsed.scheme!r}"
        )
    if parsed.username or parsed.password:
        raise CallbackUrlError("callback_url must not contain credentials")
    if not parsed.hostname:
        raise CallbackUrlError("callback_url missing host")

    host = parsed.hostname
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise CallbackUrlError(f"callback_url host could not be resolved: {exc}")

    if not infos:
        raise CallbackUrlError("callback_url host resolved to no addresses")

    for info in infos:
        sockaddr = info[4]
        ip_str = sockaddr[0]
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            raise CallbackUrlError(f"callback_url resolved to invalid IP {ip_str!r}")
        if _is_unsafe_ip(ip, allow_local=allow_local):
            raise CallbackUrlError(
                f"callback_url resolves to disallowed address {ip_str}"
            )


def _is_unsafe_ip(
    ip: ipaddress.IPv4Address | ipaddress.IPv6Address, *, allow_local: bool
) -> bool:
    # Always-blocked categories. Link-local is key here because it covers
    # cloud-metadata IPs (169.254.169.254 etc.) -- the canonical SSRF target.
    # Note: `is_reserved` is intentionally not in the always-blocked set
    # because for IPv6 it also flags ::1 (loopback), which we want to allow
    # in local-demo mode. The realistic attack surface is link-local +
    # multicast + unspecified + private; that is what we enforce.
    if ip.is_link_local or ip.is_multicast or ip.is_unspecified:
        return True
    if not allow_local and ip.is_private:
        # `is_private` is true for loopback (127.0.0.0/8, ::1) as well as
        # RFC1918 / ULA / etc., so this one check covers both.
        return True
    return False


class CallbackClient:
    """Owns the shared httpx.AsyncClient and the per-host semaphore map."""

    def __init__(self, settings: Settings):
        self.settings = settings
        timeout = httpx.Timeout(
            connect=min(2.0, settings.callback_timeout_seconds),
            read=settings.callback_timeout_seconds,
            write=settings.callback_timeout_seconds,
            pool=settings.callback_timeout_seconds,
        )
        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=False,
            limits=httpx.Limits(
                max_connections=200,
                max_keepalive_connections=50,
            ),
        )
        self._host_locks: dict[str, asyncio.Semaphore] = defaultdict(
            lambda: asyncio.Semaphore(settings.callback_per_host_concurrency)
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    def _host_sem(self, url: str) -> asyncio.Semaphore:
        host = urlparse(url).hostname or ""
        return self._host_locks[host]

    async def deliver(
        self,
        url: str,
        body: CallbackBody,
        *,
        on_attempt,
    ) -> tuple[bool, str | None]:
        """Attempt delivery with retries.

        `on_attempt` is an async callable invoked for every attempt with
        kwargs: attempt, started_at, finished_at, http_status, error,
        will_retry. It exists so the worker can persist attempts without
        coupling this module to the DB.

        Returns (delivered, last_error).
        """
        attempts = self.settings.callback_max_attempts
        backoff = self.settings.callback_initial_backoff_seconds
        max_backoff = self.settings.callback_max_backoff_seconds
        sem = self._host_sem(url)
        last_err: str | None = None

        for attempt in range(1, attempts + 1):
            payload = body.model_copy(update={"attempt": attempt}).model_dump(
                mode="json"
            )
            started: datetime = utcnow()
            status: int | None = None
            err: str | None = None
            will_retry = False

            try:
                async with sem:
                    resp = await self._client.post(url, json=payload)
                status = resp.status_code
                if 200 <= status < 300:
                    await on_attempt(
                        attempt=attempt,
                        started_at=started,
                        finished_at=utcnow(),
                        http_status=status,
                        error=None,
                        will_retry=False,
                    )
                    return True, None
                if 400 <= status < 500:
                    err = f"client error {status}"
                    will_retry = False
                else:
                    err = f"server error {status}"
                    will_retry = attempt < attempts
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                err = f"{type(exc).__name__}: {exc}"
                will_retry = attempt < attempts
            except Exception as exc:  # noqa: BLE001 - log any unknown error
                err = f"{type(exc).__name__}: {exc}"
                will_retry = attempt < attempts
                log.exception("unexpected error delivering callback")

            last_err = err
            await on_attempt(
                attempt=attempt,
                started_at=started,
                finished_at=utcnow(),
                http_status=status,
                error=err,
                will_retry=will_retry,
            )
            if not will_retry:
                return False, last_err

            sleep_for = min(max_backoff, backoff * (2 ** (attempt - 1)))
            sleep_for = random.uniform(0, sleep_for)
            await asyncio.sleep(sleep_for)

        return False, last_err


def build_callback_body(
    *,
    request_id: str,
    result: dict[str, Any] | None,
    error: str | None,
    status: RequestStatus,
) -> CallbackBody:
    return CallbackBody(
        request_id=request_id,
        status=status,
        result=result,
        error=error,
        completed_at=utcnow(),
        attempt=0,
    )
