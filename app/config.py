"""Environment-driven settings.

All knobs that matter for tuning under load (workers, queue depth,
timeouts, retry policy, SSRF posture) live here so they can be flipped
per environment without code edits.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CONSUMA_",
        extra="ignore",
    )

    database_url: str = "consuma.db"
    workers: int = 8
    queue_maxsize: int = 10_000

    callback_timeout_seconds: float = 5.0
    callback_max_attempts: int = 5
    callback_initial_backoff_seconds: float = 0.5
    callback_max_backoff_seconds: float = 30.0
    callback_per_host_concurrency: int = 16
    callback_allow_local: bool = False

    max_payload_text_chars: int = 100_000

    shutdown_drain_seconds: float = 10.0


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings_for_tests() -> None:
    """Allow tests to pick up env changes."""
    global _settings
    _settings = None
