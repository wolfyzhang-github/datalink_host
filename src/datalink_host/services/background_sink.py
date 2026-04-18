from __future__ import annotations

from dataclasses import dataclass, field
import logging
import queue
import threading
import time
from typing import Callable

from datalink_host.models.messages import ProcessedFrame


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class BackgroundSinkStats:
    queue_depth: int = 0
    frames_processed: int = 0
    frames_dropped: int = 0
    last_error: str | None = None


@dataclass(slots=True)
class _QueuedFrame:
    frame: ProcessedFrame
    enqueued_at_monotonic: float = field(default_factory=time.monotonic)


class BackgroundFrameSink:
    def __init__(
        self,
        name: str,
        handler: Callable[[ProcessedFrame], None],
        *,
        maxsize: int,
        backlog_warning_depth: int,
        queue_wait_warning_ms: float,
        handle_warning_ms: float,
    ) -> None:
        self._name = name
        self._handler = handler
        self._backlog_warning_depth = max(backlog_warning_depth, 1)
        self._queue_wait_warning_ms = max(queue_wait_warning_ms, 0.0)
        self._handle_warning_ms = max(handle_warning_ms, 0.0)
        self._queue: queue.Queue[_QueuedFrame | None] = queue.Queue(maxsize=max(maxsize, 1))
        self._lock = threading.Lock()
        self._stats = BackgroundSinkStats()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name=f"{self._name}-sink", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is None:
            return
        while True:
            try:
                self._queue.put(None, timeout=0.1)
                break
            except queue.Full:
                if not self._thread.is_alive():
                    break
        self._thread.join(timeout=5.0)
        self._thread = None
        with self._lock:
            self._stats.queue_depth = self._queue.qsize()

    def submit(self, frame: ProcessedFrame) -> None:
        if self._stop_event.is_set():
            return
        item = _QueuedFrame(frame)
        dropped_existing = False
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            dropped_existing = self._drop_oldest_frame()
            if not dropped_existing:
                with self._lock:
                    self._stats.frames_dropped += 1
                    self._stats.queue_depth = self._queue.qsize()
                    self._stats.last_error = f"{self._name} sink queue is full"
                LOGGER.warning("%s sink queue is full; dropping newest frame", self._name)
                return
            try:
                self._queue.put_nowait(item)
            except queue.Full:
                with self._lock:
                    self._stats.frames_dropped += 1
                    self._stats.queue_depth = self._queue.qsize()
                    self._stats.last_error = f"{self._name} sink queue is still full after overflow handling"
                LOGGER.warning("%s sink queue is still full after overflow handling; dropping newest frame", self._name)
                return

        queue_depth = self._queue.qsize()
        with self._lock:
            if dropped_existing:
                self._stats.frames_dropped += 1
                self._stats.last_error = f"{self._name} sink queue overflow; dropped oldest frame"
            self._stats.queue_depth = queue_depth
        if queue_depth >= self._backlog_warning_depth and queue_depth % self._backlog_warning_depth == 0:
            LOGGER.warning("%s sink queue backlog is %s frames", self._name, queue_depth)

    def stats(self) -> BackgroundSinkStats:
        with self._lock:
            return BackgroundSinkStats(
                queue_depth=self._stats.queue_depth,
                frames_processed=self._stats.frames_processed,
                frames_dropped=self._stats.frames_dropped,
                last_error=self._stats.last_error,
            )

    def _drop_oldest_frame(self) -> bool:
        while True:
            try:
                dropped = self._queue.get_nowait()
            except queue.Empty:
                return False
            if dropped is None:
                continue
            LOGGER.warning("%s sink queue overflow; dropping oldest queued frame", self._name)
            return True

    def _run(self) -> None:
        while True:
            try:
                item = self._queue.get(timeout=0.5)
            except queue.Empty:
                if self._stop_event.is_set():
                    break
                continue
            if item is None:
                if self._stop_event.is_set():
                    break
                continue
            started_at = time.monotonic()
            queue_wait_ms = (started_at - item.enqueued_at_monotonic) * 1000.0
            try:
                self._handler(item.frame)
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("%s sink handler failed", self._name)
                with self._lock:
                    self._stats.last_error = str(exc)
                    self._stats.queue_depth = self._queue.qsize()
                continue
            handle_ms = (time.monotonic() - started_at) * 1000.0
            queue_depth = self._queue.qsize()
            with self._lock:
                self._stats.frames_processed += 1
                self._stats.queue_depth = queue_depth
                self._stats.last_error = None
            if (
                queue_wait_ms >= self._queue_wait_warning_ms
                or handle_ms >= self._handle_warning_ms
                or queue_depth >= self._backlog_warning_depth
            ):
                LOGGER.warning(
                    "%s sink is lagging: queue_wait_ms=%.1f, handle_ms=%.1f, queue_depth=%s",
                    self._name,
                    queue_wait_ms,
                    handle_ms,
                    queue_depth,
                )
