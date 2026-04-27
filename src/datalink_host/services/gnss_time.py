from __future__ import annotations

import logging
import re
import threading
import time
from collections import deque
from dataclasses import dataclass, replace
from datetime import datetime, timezone

import serial
from serial.tools import list_ports

from datalink_host.core.config import GnssSettings


LOGGER = logging.getLogger(__name__)
GNSS_HOST_SKEW_WARNING_SECONDS = 2.0
_DEBUG_PATTERN = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2}) (?P<clock>\d{2}:\d{2}:\d{2}) (?P<fraction>\d{9})$"
)
_DEPLOY_PATTERN = re.compile(r"^(?P<stamp>\d{14})(?P<fraction>\d{6})$")
_DEPLOY_DOTTED_PATTERN = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2}) (?P<clock>\d{2}:\d{2}:\d{2})\.(?P<fraction>\d{6})$"
)


def gnss_timestamp_to_us(raw_value: str, mode: str) -> int:
    value = raw_value.strip()
    if mode == "debug":
        match = _DEBUG_PATTERN.fullmatch(value)
        if match is None:
            raise ValueError(f"Unsupported GNSS debug timestamp format: {value!r}")
        fraction = match.group("fraction")[:6]
        dt = datetime.strptime(
            f"{match.group('date')} {match.group('clock')} {fraction}",
            "%Y-%m-%d %H:%M:%S %f",
        ).replace(tzinfo=timezone.utc)
        return int(round(dt.timestamp() * 1_000_000))

    if mode == "deploy":
        match = _DEPLOY_PATTERN.fullmatch(value)
        if match is not None:
            dt = datetime.strptime(
                f"{match.group('stamp')}{match.group('fraction')}",
                "%Y%m%d%H%M%S%f",
            ).replace(tzinfo=timezone.utc)
            return int(round(dt.timestamp() * 1_000_000))
        dotted_match = _DEPLOY_DOTTED_PATTERN.fullmatch(value)
        if dotted_match is not None:
            dt = datetime.strptime(
                f"{dotted_match.group('date')} {dotted_match.group('clock')} {dotted_match.group('fraction')}",
                "%Y-%m-%d %H:%M:%S %f",
            ).replace(tzinfo=timezone.utc)
            return int(round(dt.timestamp() * 1_000_000))
        raise ValueError(f"Unsupported GNSS deploy timestamp format: {value!r}")

    raise ValueError(f"Unsupported GNSS mode: {mode}")


def format_timestamp_us(timestamp_us: int) -> str:
    seconds, micros = divmod(int(timestamp_us), 1_000_000)
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return dt.strftime("%Y%m%d%H%M%S") + f"{micros:06d}"


def format_timestamp_iso_utc(timestamp_us: int | None) -> str:
    if timestamp_us is None:
        return "-"
    seconds, micros = divmod(int(timestamp_us), 1_000_000)
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{micros:06d}Z"


@dataclass(slots=True)
class GnssStatus:
    enabled: bool
    connected: bool
    mode: str
    port: str
    baudrate: int
    poll_interval_seconds: float
    last_timestamp_us: int | None
    last_error: str | None


class GnssTimeService:
    def __init__(self, settings: GnssSettings) -> None:
        self._settings = settings
        self._lock = threading.Lock()
        self._timestamp_condition = threading.Condition(self._lock)
        self._status = GnssStatus(
            enabled=settings.enabled,
            connected=False,
            mode=settings.mode,
            port=settings.port,
            baudrate=settings.baudrate,
            poll_interval_seconds=settings.poll_interval_seconds,
            last_timestamp_us=None,
            last_error=None,
        )
        self._current_timestamp_anchor_us: int | None = None
        self._last_timestamp_monotonic: float | None = None
        self._timestamp_events: deque[int] = deque()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._host_skew_warning_active = False

    def start(self) -> None:
        with self._lock:
            should_run = self._settings.enabled and bool(self._settings.port.strip())
            if not should_run or self._thread is not None:
                last_error = self._status.last_error
                if self._settings.enabled and not self._settings.port.strip():
                    last_error = "GNSS port is not configured"
                self._status = replace(
                    self._status,
                    enabled=self._settings.enabled,
                    mode=self._settings.mode,
                    port=self._settings.port,
                    baudrate=self._settings.baudrate,
                    poll_interval_seconds=self._settings.poll_interval_seconds,
                    last_error=last_error,
                    connected=False if not should_run else self._status.connected,
                )
                return
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run, name="gnss-time-service", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        with self._lock:
            thread = self._thread
            self._thread = None
            self._status = replace(self._status, connected=False)
        if thread is not None:
            thread.join(timeout=2.0)

    def update_settings(self, settings: GnssSettings) -> None:
        with self._lock:
            changed = (
                settings.enabled != self._settings.enabled
                or settings.port != self._settings.port
                or settings.baudrate != self._settings.baudrate
                or settings.mode != self._settings.mode
                or settings.poll_interval_seconds != self._settings.poll_interval_seconds
                or settings.packet_timestamp_timeout_seconds != self._settings.packet_timestamp_timeout_seconds
                or settings.serial_timeout_seconds != self._settings.serial_timeout_seconds
                or settings.command != self._settings.command
            )
            self._settings = settings
            self._status = replace(
                self._status,
                enabled=settings.enabled,
                mode=settings.mode,
                port=settings.port,
                baudrate=settings.baudrate,
                poll_interval_seconds=settings.poll_interval_seconds,
            )
        if not changed:
            return
        self.stop()
        self.start()

    def status(self) -> GnssStatus:
        with self._lock:
            return replace(self._status)

    def current_time_us(self) -> int | None:
        with self._lock:
            return self._current_time_us_locked(time.monotonic())

    def wait_for_next_timestamp_us(self, timeout_seconds: float) -> int | None:
        deadline = time.monotonic() + max(timeout_seconds, 0.0)
        with self._timestamp_condition:
            while not self._timestamp_events:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self._timestamp_condition.wait(timeout=remaining)
            return self._timestamp_events.popleft()

    def clear_pending_timestamps(self) -> int:
        with self._timestamp_condition:
            count = len(self._timestamp_events)
            self._timestamp_events.clear()
            return count

    def available_ports(self) -> list[str]:
        return [port.device for port in list_ports.comports()]

    def _run(self) -> None:
        while not self._stop_event.is_set():
            settings = self._snapshot_settings()
            if not settings.enabled:
                break
            if not settings.port.strip():
                self._set_error("GNSS port is not configured")
                break
            try:
                LOGGER.info(
                    "Opening GNSS serial port: port=%s baudrate=%s mode=%s poll_interval_s=%.3f timeout_s=%.3f",
                    settings.port,
                    settings.baudrate,
                    settings.mode,
                    settings.poll_interval_seconds,
                    settings.serial_timeout_seconds,
                )
                with serial.Serial(
                    settings.port,
                    baudrate=settings.baudrate,
                    timeout=settings.serial_timeout_seconds,
                ) as conn:
                    self._set_connected(True)
                    dropped_bytes = self._discard_stale_input(conn)
                    LOGGER.info(
                        "GNSS serial port is ready: port=%s mode=%s stale_input_bytes_dropped=%s",
                        settings.port,
                        settings.mode,
                        dropped_bytes,
                    )
                    self._read_loop(conn, settings)
            except Exception as exc:  # noqa: BLE001
                if self._stop_event.is_set():
                    break
                self._set_error(str(exc))
                LOGGER.error("GNSS serial connection failed: %s", exc)
                if self._stop_event.wait(1.0):
                    break
            finally:
                self._set_connected(False)

    def _read_loop(self, conn: serial.Serial, settings: GnssSettings) -> None:
        next_poll_at = 0.0
        while not self._stop_event.is_set():
            now = time.monotonic()
            if settings.mode == "debug" and now >= next_poll_at:
                conn.write((settings.command + "\r\n").encode("ascii"))
                conn.flush()
                next_poll_at = now + max(settings.poll_interval_seconds, 0.01)

            raw_line = conn.readline()
            if not raw_line:
                continue
            raw_value = raw_line.decode("utf-8", errors="ignore").strip()
            try:
                timestamp_us = gnss_timestamp_to_us(raw_value, settings.mode)
            except Exception as exc:  # noqa: BLE001
                self._set_error(str(exc))
                LOGGER.warning("Failed to parse GNSS timestamp: raw=%r error=%s", raw_value, exc)
                continue
            anchor_timestamp_us = self._record_timestamp(timestamp_us)
            self._warn_on_host_skew(
                raw_value=raw_value,
                parsed_timestamp_us=timestamp_us,
                anchor_timestamp_us=anchor_timestamp_us,
            )

    def _snapshot_settings(self) -> GnssSettings:
        with self._lock:
            return replace(self._settings)

    def _set_connected(self, connected: bool) -> None:
        with self._lock:
            self._status = replace(self._status, connected=connected)

    def _set_error(self, message: str) -> None:
        with self._lock:
            self._status = replace(self._status, last_error=message)

    @staticmethod
    def _discard_stale_input(conn: serial.Serial) -> int:
        buffered_bytes = 0
        try:
            buffered_bytes = int(getattr(conn, "in_waiting", 0) or 0)
        except Exception:  # noqa: BLE001
            buffered_bytes = 0
        reset = getattr(conn, "reset_input_buffer", None)
        if callable(reset):
            reset()
        return buffered_bytes

    def _current_time_us_locked(self, now_monotonic: float) -> int | None:
        anchor_timestamp_us = self._current_timestamp_anchor_us
        last_timestamp_monotonic = self._last_timestamp_monotonic
        if anchor_timestamp_us is None or last_timestamp_monotonic is None:
            return None
        elapsed_us = int(round((now_monotonic - last_timestamp_monotonic) * 1_000_000))
        return anchor_timestamp_us + max(elapsed_us, 0)

    def _record_timestamp(self, timestamp_us: int, *, recorded_at_monotonic: float | None = None) -> int:
        now_monotonic = time.monotonic() if recorded_at_monotonic is None else recorded_at_monotonic
        with self._lock:
            current_time_us = self._current_time_us_locked(now_monotonic)
            anchor_timestamp_us = timestamp_us if current_time_us is None else max(timestamp_us, current_time_us)
            self._status = replace(self._status, last_timestamp_us=timestamp_us, last_error=None)
            self._current_timestamp_anchor_us = anchor_timestamp_us
            self._last_timestamp_monotonic = now_monotonic
            self._timestamp_events.append(timestamp_us)
            self._timestamp_condition.notify_all()
        return anchor_timestamp_us

    def _warn_on_host_skew(
        self,
        *,
        raw_value: str,
        parsed_timestamp_us: int,
        anchor_timestamp_us: int,
    ) -> None:
        host_timestamp_us = int(round(time.time() * 1_000_000))
        host_delta_ms = (parsed_timestamp_us - host_timestamp_us) / 1_000
        if abs(host_delta_ms) < (GNSS_HOST_SKEW_WARNING_SECONDS * 1_000):
            self._host_skew_warning_active = False
            return
        if self._host_skew_warning_active:
            return
        self._host_skew_warning_active = True
        LOGGER.warning(
            "GNSS timestamp differs from host clock: raw=%r parsed_utc=%s host_utc=%s "
            "host_delta_ms=%.1f anchor_utc=%s",
            raw_value,
            format_timestamp_iso_utc(parsed_timestamp_us),
            format_timestamp_iso_utc(host_timestamp_us),
            host_delta_ms,
            format_timestamp_iso_utc(anchor_timestamp_us),
        )
