"""Load generator CLI.

Fires N requests against /sync or /async with bounded concurrency and
reports latency or time-to-callback percentiles.

Usage examples:
    python -m loadgen.runner --mode sync --n 500 --concurrency 50
    python -m loadgen.runner --mode async --n 500 --concurrency 50
    python -m loadgen.runner --mode async --n 100 --fail-mode flaky

Design:
- For `sync`, latency = time from request send to response receive.
- For `async`, time-to-callback = time from submit to callback arrival
  (the callback server records arrival timestamps).
- Both modes share `Stats` so the summary tables look identical.
- The callback server runs in-process so the demo needs no extra terminal.
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
from dataclasses import dataclass, field

import httpx

from loadgen.callback_server import CallbackServer


@dataclass
class Stats:
    label: str
    sent: int = 0
    ok: int = 0
    failed: int = 0
    rejected_503: int = 0
    rejected_other: int = 0
    timings_ms: list[float] = field(default_factory=list)
    elapsed_s: float = 0.0

    def add_success(self, ms: float) -> None:
        self.ok += 1
        self.timings_ms.append(ms)

    def percentile(self, p: float) -> float:
        if not self.timings_ms:
            return float("nan")
        ordered = sorted(self.timings_ms)
        k = max(0, min(len(ordered) - 1, int(round((p / 100.0) * (len(ordered) - 1)))))
        return ordered[k]

    def render(self) -> str:
        rps = self.sent / self.elapsed_s if self.elapsed_s > 0 else float("nan")
        lines = [
            f"--- {self.label} ---",
            f"  sent:           {self.sent}",
            f"  success:        {self.ok}",
            f"  failed (net):   {self.failed}",
            f"  rejected 503:   {self.rejected_503}",
            f"  rejected other: {self.rejected_other}",
            f"  elapsed:        {self.elapsed_s:.2f}s ({rps:.1f} req/s sent)",
        ]
        if self.timings_ms:
            lines.extend(
                [
                    f"  p50:            {self.percentile(50):.2f} ms",
                    f"  p95:            {self.percentile(95):.2f} ms",
                    f"  p99:            {self.percentile(99):.2f} ms",
                    f"  mean:           {statistics.mean(self.timings_ms):.2f} ms",
                    f"  max:            {max(self.timings_ms):.2f} ms",
                ]
            )
        return "\n".join(lines)


async def run_sync(
    *,
    base_url: str,
    n: int,
    concurrency: int,
    text: str,
    complexity: int,
) -> Stats:
    stats = Stats(label=f"sync n={n} concurrency={concurrency}")
    sem = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(
        max_connections=concurrency * 2,
        max_keepalive_connections=concurrency,
    )

    async with httpx.AsyncClient(
        base_url=base_url,
        limits=limits,
        timeout=httpx.Timeout(60.0),
    ) as client:

        async def one():
            async with sem:
                t0 = time.perf_counter()
                try:
                    resp = await client.post(
                        "/sync", json={"text": text, "complexity": complexity}
                    )
                except Exception:
                    stats.failed += 1
                    return
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                stats.sent += 1
                if resp.status_code == 200:
                    stats.add_success(elapsed_ms)
                elif resp.status_code == 503:
                    stats.rejected_503 += 1
                else:
                    stats.rejected_other += 1

        t_start = time.perf_counter()
        await asyncio.gather(*(one() for _ in range(n)))
        stats.elapsed_s = time.perf_counter() - t_start
    return stats


async def run_async(
    *,
    base_url: str,
    n: int,
    concurrency: int,
    text: str,
    complexity: int,
    fail_mode: str,
    wait_timeout_s: float,
) -> Stats:
    label_extra = f" fail_mode={fail_mode}" if fail_mode != "ok" else ""
    stats = Stats(label=f"async n={n} concurrency={concurrency}{label_extra}")

    async with CallbackServer(fail_mode=fail_mode) as cb:
        callback_url = cb.url
        submit_times: dict[str, float] = {}

        sem = asyncio.Semaphore(concurrency)
        limits = httpx.Limits(
            max_connections=concurrency * 2,
            max_keepalive_connections=concurrency,
        )
        async with httpx.AsyncClient(
            base_url=base_url,
            limits=limits,
            timeout=httpx.Timeout(30.0),
        ) as client:

            async def one():
                async with sem:
                    t0 = time.perf_counter()
                    try:
                        resp = await client.post(
                            "/async",
                            json={
                                "text": text,
                                "complexity": complexity,
                                "callback_url": callback_url,
                            },
                        )
                    except Exception:
                        stats.failed += 1
                        return
                    stats.sent += 1
                    if resp.status_code == 202:
                        rid = resp.json()["request_id"]
                        submit_times[rid] = t0
                    elif resp.status_code == 503:
                        stats.rejected_503 += 1
                    else:
                        stats.rejected_other += 1

            t_start = time.perf_counter()
            await asyncio.gather(*(one() for _ in range(n)))

            deadline = time.perf_counter() + wait_timeout_s
            while time.perf_counter() < deadline:
                outstanding = [
                    rid for rid in submit_times if rid not in cb.recorder.arrivals
                ]
                if not outstanding:
                    break
                await asyncio.sleep(0.05)

            for rid, sent_at in submit_times.items():
                arrival = cb.recorder.arrivals.get(rid)
                if arrival is None:
                    stats.failed += 1
                else:
                    stats.add_success((arrival - sent_at) * 1000.0)

            stats.elapsed_s = time.perf_counter() - t_start
    return stats


async def amain(args: argparse.Namespace) -> int:
    if args.mode == "sync":
        stats = await run_sync(
            base_url=args.base_url,
            n=args.n,
            concurrency=args.concurrency,
            text=args.text,
            complexity=args.complexity,
        )
    elif args.mode == "async":
        stats = await run_async(
            base_url=args.base_url,
            n=args.n,
            concurrency=args.concurrency,
            text=args.text,
            complexity=args.complexity,
            fail_mode=args.fail_mode,
            wait_timeout_s=args.wait_timeout,
        )
    elif args.mode == "both":
        sync_stats = await run_sync(
            base_url=args.base_url,
            n=args.n,
            concurrency=args.concurrency,
            text=args.text,
            complexity=args.complexity,
        )
        async_stats = await run_async(
            base_url=args.base_url,
            n=args.n,
            concurrency=args.concurrency,
            text=args.text,
            complexity=args.complexity,
            fail_mode=args.fail_mode,
            wait_timeout_s=args.wait_timeout,
        )
        print(sync_stats.render())
        print()
        print(async_stats.render())
        return 0
    else:
        raise SystemExit(f"unknown mode {args.mode!r}")

    print(stats.render())
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Consuma load generator")
    p.add_argument(
        "--base-url", default="http://127.0.0.1:8000", help="server base URL"
    )
    p.add_argument(
        "--mode",
        choices=("sync", "async", "both"),
        default="both",
    )
    p.add_argument("--n", type=int, default=200, help="total requests")
    p.add_argument(
        "--concurrency",
        type=int,
        default=50,
        help="max in-flight requests at any time",
    )
    p.add_argument("--text", default="lorem ipsum dolor sit amet", help="work text")
    p.add_argument(
        "--complexity",
        type=int,
        default=1,
        help="work complexity 1..20 (drives CPU per request)",
    )
    p.add_argument(
        "--fail-mode",
        choices=("ok", "5xx", "flaky"),
        default="ok",
        help="behaviour of the embedded callback server (async only)",
    )
    p.add_argument(
        "--wait-timeout",
        type=float,
        default=60.0,
        help="seconds to wait for all callbacks to arrive (async only)",
    )
    return p


def main() -> None:
    args = build_parser().parse_args()
    raise SystemExit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
