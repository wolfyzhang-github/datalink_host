from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable


_PROVIDER_LOCK = threading.RLock()
_wall_time_provider: Callable[[], float] | None = None
_base_log_record_factory: Callable[..., logging.LogRecord] | None = None


def set_wall_time_provider(provider: Callable[[], float] | None) -> None:
    """Set the process-wide wall clock provider.

    Passing ``None`` restores the host system clock fallback.
    """
    with _PROVIDER_LOCK:
        global _wall_time_provider
        _wall_time_provider = provider


def wall_time() -> float:
    with _PROVIDER_LOCK:
        provider = _wall_time_provider
    if provider is None:
        return time.time()
    try:
        return float(provider())
    except Exception:
        return time.time()


def wall_time_us() -> int:
    return int(round(wall_time() * 1_000_000))


def install_logging_clock() -> None:
    """Make LogRecord timestamps use the process-wide wall clock provider."""
    global _base_log_record_factory
    if _base_log_record_factory is not None:
        return
    _base_log_record_factory = logging.getLogRecordFactory()

    def record_factory(*args: object, **kwargs: object) -> logging.LogRecord:
        assert _base_log_record_factory is not None
        record = _base_log_record_factory(*args, **kwargs)
        created = wall_time()
        record.created = created
        record.msecs = (created - int(created)) * 1000.0
        record.relativeCreated = (created - logging._startTime) * 1000.0  # noqa: SLF001
        return record

    logging.setLogRecordFactory(record_factory)
