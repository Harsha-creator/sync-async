"""Shared, pure, deterministic 'work' function.

The same function is invoked from `/sync` (inline) and from the async worker
pool. Keeping it pure (no I/O, no globals) is what lets the two paths share
business logic without duplication.

The work is intentionally simple but real: compute SHA-256 + character/word
stats over an input string. A small `complexity` knob (1..MAX_COMPLEXITY)
iterates the hash so the demo can produce meaningfully slow requests without
sleeping (sleeping would not exercise the event-loop-blocking concern that
matters for the sync path).
"""

from __future__ import annotations

import hashlib
from typing import Any

MAX_COMPLEXITY = 20
HASH_ITERATIONS_PER_COMPLEXITY = 50_000


def run_work(payload: dict[str, Any]) -> dict[str, Any]:
    """Run deterministic work over `payload`.

    Expects `payload["text"]` (str). Honours optional `payload["complexity"]`
    in 1..MAX_COMPLEXITY (clamped). Returns a JSON-serialisable dict.

    Pure: same input -> identical output, no side effects.
    """
    text = payload.get("text", "")
    if not isinstance(text, str):
        raise ValueError("payload.text must be a string")

    raw_complexity = payload.get("complexity", 1)
    try:
        complexity = int(raw_complexity)
    except (TypeError, ValueError) as exc:
        raise ValueError("payload.complexity must be an integer") from exc
    complexity = max(1, min(MAX_COMPLEXITY, complexity))

    encoded = text.encode("utf-8")
    digest = hashlib.sha256(encoded).digest()
    iterations = complexity * HASH_ITERATIONS_PER_COMPLEXITY
    for _ in range(iterations):
        digest = hashlib.sha256(digest).digest()

    return {
        "sha256": digest.hex(),
        "char_count": len(text),
        "word_count": len(text.split()),
        "byte_count": len(encoded),
        "complexity": complexity,
        "iterations": iterations,
    }
