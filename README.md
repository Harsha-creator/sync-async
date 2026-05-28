# consuma

A Python backend that exposes the same "work" in two interaction styles:

- `POST /sync` — runs the work inline and returns the result.
- `POST /async` — accepts the request, returns a `request_id` immediately, then delivers the result to a caller-supplied `callback_url` (with retries).

Ships with a CLI load generator that hosts its own callback receiver and reports latency / time-to-callback percentiles.

## Architecture at a glance

```
client ──POST /sync──▶ FastAPI ──── run_work() ──▶ response (inline)
       │
       └─POST /async──▶ FastAPI ──▶ bounded asyncio.Queue ──▶ worker pool
                          │                                        │
                          ├──insert──▶ SQLite (WAL)                ├──run_work()
                          │                                        ├──validate + POST callback_url
                          │                                        │   (retries w/ exp-backoff + jitter)
                          └──ack 202──▶ client                     └──update SQLite
```

Single Python process. The `asyncio.Queue` and worker pool live in-process, so they only make sense with a single Uvicorn worker (see [Tradeoffs](#tradeoffs)).

## Quickstart

Requires Python 3.11+.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Start the API (CALLBACK_ALLOW_LOCAL=true so the loadgen on 127.0.0.1 works)
CONSUMA_CALLBACK_ALLOW_LOCAL=true \
  uvicorn app.main:app --host 127.0.0.1 --port 8000
```

OpenAPI docs are at `http://127.0.0.1:8000/docs`.

### Smoke check

```bash
curl http://127.0.0.1:8000/healthz
curl -X POST http://127.0.0.1:8000/sync \
  -H 'content-type: application/json' \
  -d '{"text":"hello world","complexity":1}'
```

### Run the test suite

```bash
pytest -q
```

## API surface

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/sync` | Runs work inline. Returns `{request_id, result, took_ms}`. |
| `POST` | `/async` | Validates `callback_url`, enqueues. Returns `202 {request_id, status, queued_at}` or `503` (with `Retry-After`) if the queue is full. |
| `GET` | `/requests?mode=sync\|async&limit=N` | Lists recent requests, newest first. |
| `GET` | `/requests/{id}` | Full record including the per-attempt callback log. |
| `GET` | `/healthz` | Real health check (see below). |

### Request / callback bodies

`POST /sync` and `POST /async` accept:

```json
{ "text": "string", "complexity": 1, "callback_url": "https://..." }
```

(`callback_url` is required for `/async` only. `complexity` is clamped to `1..20` and drives the cost of the work — see [The "work"](#the-work).)

The callback body the server POSTs to your `callback_url`:

```json
{
  "request_id": "uuid",
  "status": "completed" | "failed",
  "result": { ... } | null,
  "error": null | "...",
  "completed_at": "ISO8601",
  "attempt": 1
}
```

### `/healthz` semantics

`/healthz` is a real health check, not a hardcoded `"ok"`. The response body looks like:

```json
{
  "status": "ok" | "degraded" | "unhealthy",
  "db_ok": true,
  "queue_depth": 0,
  "queue_maxsize": 10000,
  "workers_alive": 8,
  "expected_workers": 8
}
```

Decision rules:

- **`unhealthy` → HTTP 503** when the DB ping (`SELECT 1`) fails, or every expected worker is gone. A load balancer should pull the instance out of rotation.
- **`degraded` → HTTP 200** when some (but not all) workers are missing, or the queue is at ≥ 90% capacity. Still serving, but worth paging on.
- **`ok` → HTTP 200** when DB responds, all expected workers are alive, and queue pressure is < 90%.

## Load generator

The load generator lives in [loadgen/runner.py](loadgen/runner.py). It hosts its own callback receiver in-process so you don't need a separate terminal.

```bash
# Sync only
python -m loadgen.runner --base-url http://127.0.0.1:8000 \
  --mode sync --n 500 --concurrency 50 --complexity 1

# Async only (with the embedded callback server)
python -m loadgen.runner --base-url http://127.0.0.1:8000 \
  --mode async --n 500 --concurrency 50 --complexity 1

# Both (default) — easiest summary to compare
python -m loadgen.runner --base-url http://127.0.0.1:8000 --n 200

# Exercise retry behaviour: callback returns 500 on the first attempt only
python -m loadgen.runner --base-url http://127.0.0.1:8000 \
  --mode async --n 50 --fail-mode flaky

# All callback attempts return 500 (drives callbacks to callback_failed)
python -m loadgen.runner --base-url http://127.0.0.1:8000 \
  --mode async --n 20 --fail-mode 5xx
```

Output reports `p50/p95/p99/mean/max`, throughput, and how many requests succeeded vs. were rejected with `503`.

## The "work"

A single pure function in [app/work.py](app/work.py):

```python
def run_work(payload: dict) -> dict:
    # SHA-256 of `text`, iterated `complexity * 50_000` times.
    # Returns {sha256, char_count, word_count, byte_count, complexity, iterations}.
```

Same input → identical output (covered by [tests/test_work.py](tests/test_work.py)). The iterated hash is real CPU work, so it stresses the right things under load — unlike `sleep()`, it actually contends for the GIL and exposes whether the sync path is blocking the event loop.

Both `/sync` and the async worker call this same function. There is **no duplicated business logic**.

## Design decisions and tradeoffs

### Concurrency model

- `/sync` runs `run_work` via `asyncio.to_thread`. CPU work happens off the event loop so a slow `/sync` request can't stall `/async` acks. The bottleneck is then the default asyncio thread pool (~32 threads on CPython 3.12), which is the right answer here.
- `/async` does **not** run the work inline. It writes a `received` row, attempts a non-blocking `queue.put_nowait`, and returns `202`. The worker pool runs the actual work + callback delivery.
- The queue is bounded (`CONSUMA_QUEUE_MAXSIZE`, default 10,000). When full, `/async` returns `503` with `Retry-After: 1`. This is the only way to give honest backpressure without OOMing the process under sustained overload.

### Persistence

- SQLite with `journal_mode=WAL` and `synchronous=NORMAL`, accessed through `aiosqlite`. A single connection serialises writes through the aiosqlite worker thread, which means no `database is locked` errors under burst.
- The schema is two tables: `requests` (one row per submission, with all timestamps and final state) and `callback_attempts` (append-only, one row per delivery attempt with `http_status`, `error`, and `will_retry`). This is what makes `GET /requests/{id}` a real audit trail rather than a status guess.

### Callback delivery (`app/callback.py`)

- One shared `httpx.AsyncClient` with `follow_redirects=False`, bounded timeouts.
- Retry policy: exponential backoff with **full jitter** (`uniform(0, min(max_backoff, base * 2^(n-1)))`), capped at `CONSUMA_CALLBACK_MAX_ATTEMPTS` (default 5). 2xx is success, 4xx is non-retryable (caller bug), 5xx and network errors retry.
- A per-host `asyncio.Semaphore` caps concurrent in-flight callbacks to the same host (`CONSUMA_CALLBACK_PER_HOST_CONCURRENCY`, default 16). One slow callback target cannot starve the entire worker pool.

### SSRF defense — the most fiddly part

The `callback_url` is attacker-controlled. The validator in `validate_callback_url` does, in order:

1. Length and presence checks.
2. Scheme is `http` or `https`.
3. No userinfo (`user:pass@`).
4. `socket.getaddrinfo(host)` and reject if any resolved IP is **link-local, multicast, or unspecified** unconditionally, and **private/loopback** unless `CONSUMA_CALLBACK_ALLOW_LOCAL=true`.
5. Outbound POST disables redirects (`follow_redirects=False`).

Link-local is the important one — it covers `169.254.169.254` (AWS / GCP / Azure metadata). `CALLBACK_ALLOW_LOCAL=true` is what lets the loadgen on `127.0.0.1` work in demos; it loosens loopback/RFC1918 but **does not** loosen link-local. Tests cover that.

**Known residual risk: DNS rebinding.** Between the `getaddrinfo` check and the actual httpx connect there is a small window in which a hostile DNS server could return a different IP. The production fix is to resolve once, pin the IP into the connection, and override the `Host` header (and re-validate on every redirect). Libraries that do this correctly are [`drawbridge`](https://github.com/tachyon-oss/drawbridge) and [`httpx-secure`](https://github.com/Zaczero/httpx-secure). I chose not to pull either as a dependency for a demo, but if this were going to production that's the first thing I would add.

### Ordering and timing guarantees

- The async queue is FIFO. Workers `await queue.get()` in declaration order.
- With N workers, **dispatch order is FIFO** but **completion order is not** — a slow item can be overtaken by faster items behind it. This is the standard guarantee for a pool of independent workers. Per-key strict ordering (e.g. "all requests with the same `customer_id` finish in order") would require keyed sub-queues. That's a known extension, not implemented.
- Every state change is timestamped in the DB, so the actual order is fully auditable via `GET /requests/{id}`.

### Why no Celery/Redis/Postgres/Docker

- The assignment said "don't over-implement." The in-process worker pool with a bounded `asyncio.Queue` covers all the requirements (sync vs async, retries, ordering, backpressure, persistence, traceability) with no external infrastructure to spin up. Spending time wiring Celery would mean spending less time on the parts of this that actually matter (SSRF, retry policy, backpressure semantics).
- The cost is durability: if the process crashes mid-job, in-flight callbacks are lost (the row in the DB stays at `callback_pending`). For real production scale, replace the queue with Redis Streams / RabbitMQ / SQS so jobs survive a restart.

## Configuration

All knobs are environment variables, prefixed `CONSUMA_`:

| Variable | Default | Purpose |
| --- | --- | --- |
| `CONSUMA_DATABASE_URL` | `consuma.db` | SQLite file path (or `:memory:`). |
| `CONSUMA_WORKERS` | `8` | Async worker coroutines. |
| `CONSUMA_QUEUE_MAXSIZE` | `10000` | Bounded queue capacity. |
| `CONSUMA_CALLBACK_TIMEOUT_SECONDS` | `5.0` | Per-attempt total timeout. |
| `CONSUMA_CALLBACK_MAX_ATTEMPTS` | `5` | Retries cap. |
| `CONSUMA_CALLBACK_INITIAL_BACKOFF_SECONDS` | `0.5` | Initial backoff. |
| `CONSUMA_CALLBACK_MAX_BACKOFF_SECONDS` | `30.0` | Backoff ceiling. |
| `CONSUMA_CALLBACK_PER_HOST_CONCURRENCY` | `16` | Per-target concurrency cap. |
| `CONSUMA_CALLBACK_ALLOW_LOCAL` | `false` | Permit loopback/RFC1918 callbacks (demo only). |
| `CONSUMA_MAX_PAYLOAD_TEXT_CHARS` | `100000` | Inbound `text` cap. |
| `CONSUMA_SHUTDOWN_DRAIN_SECONDS` | `10.0` | Worker drain deadline at shutdown. |

## Tradeoffs

| Choice | Cost | When to revisit |
| --- | --- | --- |
| In-memory `asyncio.Queue` | Crash-loses pending jobs; multi-worker Uvicorn would have N independent queues. | When you need durability or horizontal scaling — swap for Redis Streams / RabbitMQ. |
| SQLite | Single-writer, single-machine. | When concurrent ops > a few thousand/sec or you want multi-process readers — move to Postgres. |
| Custom SSRF validator (not IP-pinned) | DNS-rebinding window. | The day this faces the public internet — switch to `drawbridge`. |
| No HMAC on callback bodies | Receiver can't authenticate the source. | If callbacks ever cross trust boundaries — add a shared-secret HMAC header. |
| No idempotency keys | A retried client submission creates a new `request_id`. | If clients want safe retries — accept `Idempotency-Key`, dedupe in the DB. |
| Single Uvicorn worker | No horizontal scaling on a single box. | Replace queue first, then multi-worker. |

## Next steps if this were going to production

1. Replace the in-process queue with Redis Streams (or RabbitMQ) so jobs survive restarts and multiple processes/machines can share the work.
2. Move from SQLite to Postgres for the durable record; let workers and the API share the same DB.
3. Sign callback bodies with HMAC, and let receivers verify.
4. Use `drawbridge` for callback delivery to close the DNS-rebinding gap.
5. Accept client-supplied `Idempotency-Key` headers; dedupe on `(client_id, key)`.
6. Add OpenTelemetry traces — `/async` → enqueue → worker → callback POST is exactly the multi-hop shape OTel is designed for.
7. Add a `/metrics` endpoint with queue depth, attempt distribution, callback latency histograms.

## How I used AI

This whole project was built in Cursor with Claude as the pair. The flow:

1. I read the assignment and asked Cursor to spend extra time on gotchas before writing any code — Plan Mode → a single design doc that called out SSRF, event-loop blocking, backpressure semantics, ordering caveats, and callback amplification before code existed.
2. I implemented in plan order: shared work fn first, then config + models + DB, then SSRF + callback delivery + worker pool + routes, then the load gen, then tests. Tests caught a real bug — the `Retry-After` header was being set on a `Response` that `HTTPException` discarded, so 503 went out without the header.
3. The SSRF logic I rewrote once: my first cut used `is_reserved` as an "always block" flag, but `is_reserved` is True for IPv6 `::1`, which broke `CALLBACK_ALLOW_LOCAL=true`. The current model — always block link-local/multicast/unspecified, conditionally block private/loopback — is both more precise and easier to defend.
4. Everything you see was reviewed line by line. Architecture, tradeoff choices, and the specific decision to skip Celery/Docker were mine.
