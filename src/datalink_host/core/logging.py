from __future__ import annotations

from collections import deque
import logging
from pathlib import Path
import sys
import threading


LOG_FORMAT = "%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s"
_LOG_BUFFER_SIZE = 2000
_LOG_BUFFER: deque[str] = deque(maxlen=_LOG_BUFFER_SIZE)
_LOG_LOCK = threading.Lock()


class InMemoryLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        message = self.format(record)
        with _LOG_LOCK:
            _LOG_BUFFER.append(message)


def get_recent_logs(limit: int = 200) -> list[str]:
    with _LOG_LOCK:
        if limit <= 0:
            return []
        return list(_LOG_BUFFER)[-limit:]


def _can_use_console_stream() -> bool:
    stream = sys.stderr
    return stream is not None and hasattr(stream, "write")


def configure_logging(log_path: Path | None = None) -> None:
    memory_handler = InMemoryLogHandler()
    memory_handler.setFormatter(logging.Formatter(LOG_FORMAT))

    handlers: list[logging.Handler] = [memory_handler]
    if _can_use_console_stream():
        handlers.insert(0, logging.StreamHandler())
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))

    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, handlers=handlers, force=True)
