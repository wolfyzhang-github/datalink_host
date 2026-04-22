from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone

import serial
from serial.tools import list_ports

from datalink_host.core.config import GpsSettings


LOGGER = logging.getLogger(__name__)
_DEBUG_PATTERN = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2}) (?P<clock>\d{2}:\d{2}:\d{2}) (?P<fraction>\d{9})$"
)
_DEPLOY_PATTERN = re.compile(r"^(?P<stamp>\d{14})(?P<fraction>\d{6})$")
_DEPLOY_DOTTED_PATTERN = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2}) (?P<clock>\d{2}:\d{2}:\d{2})\.(?P<fraction>\d{6})$"
)


def gps_timestamp_to_us(raw_value: str, mode: str) -> int:
    value = raw_value.strip()
    if mode == "debug":
        match = _DEBUG_PATTERN.fullmatch(value)
        if match is None:
            raise ValueError(f"Unsupported GPS debug timestamp format: {value!r}")
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
        raise ValueError(f"Unsupported GPS deploy timestamp format: {value!r}")

    raise ValueError(f"Unsupported GPS mode: {mode}")


def format_timestamp_us(timestamp_us: int) -> str:
    seconds, micros = divmod(int(timestamp_us), 1_000_000)
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return dt.strftime("%Y%m%d%H%M%S") + f"{micros:06d}"


@dataclass(slots=True)
class GpsStatus:
    enabled: bool
    connected: bool
    mode: str
    port: str
    baudrate: int
    poll_interval_seconds: float
    last_timestamp_us: int | None
    last_error: str | None


class GpsTimeService:
    def __init__(self, settings: GpsSettings) -> None:
        self._settings = settings
        self._lock = threading.Lock()
        self._status = GpsStatus(
            enabled=settings.enabled,
            connected=False,
            mode=settings.mode,
            port=settings.port,
            baudrate=settings.baudrate,
            poll_interval_seconds=settings.poll_interval_seconds,
            last_timestamp_us=None,
            last_error=None,
        )
        self._last_timestamp_monotonic: float | None = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        with self._lock:
            should_run = self._settings.enabled and bool(self._settings.port.strip())
            if not should_run or self._thread is not None:
                self._status = replace(
                    self._status,
                    enabled=self._settings.enabled,
                    mode=self._settings.mode,
                    port=self._settings.port,
                    baudrate=self._settings.baudrate,
                    poll_interval_seconds=self._settings.poll_interval_seconds,
                    connected=False if not should_run else self._status.connected,
                )
                return
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run, name="gps-time-service", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        with self._lock:
            thread = self._thread
            self._thread = None
            self._status = replace(self._status, connected=False)
        if thread is not None:
            thread.join(timeout=2.0)

    def update_settings(self, settings: GpsSettings) -> None:
        with self._lock:
            changed = (
                settings.enabled != self._settings.enabled
                or settings.port != self._settings.port
                or settings.baudrate != self._settings.baudrate
                or settings.mode != self._settings.mode
                or settings.poll_interval_seconds != self._settings.poll_interval_seconds
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

    def status(self) -> GpsStatus:
        with self._lock:
            return replace(self._status)

    def current_time_us(self) -> int | None:
        with self._lock:
            last_timestamp_us = self._status.last_timestamp_us
            last_timestamp_monotonic = self._last_timestamp_monotonic
        if last_timestamp_us is None or last_timestamp_monotonic is None:
            return None
        elapsed_us = int(round((time.monotonic() - last_timestamp_monotonic) * 1_000_000))
        return last_timestamp_us + max(elapsed_us, 0)

    def available_ports(self) -> list[str]:
        return [port.device for port in list_ports.comports()]

    def _run(self) -> None:
        while not self._stop_event.is_set():
            settings = self._snapshot_settings()
            if not settings.enabled:
                break
            if not settings.port.strip():
                self._set_error("GPS port is not configured")
                break
            try:
                with serial.Serial(
                    settings.port,
                    baudrate=settings.baudrate,
                    timeout=settings.serial_timeout_seconds,
                ) as conn:
                    self._set_connected(True)
                    self._read_loop(conn, settings)
            except Exception as exc:  # noqa: BLE001
                if self._stop_event.is_set():
                    break
                self._set_error(str(exc))
                LOGGER.error("GPS serial connection failed: %s", exc)
                if self._stop_event.wait(1.0):
                    break
            finally:
                self._set_connected(False)

    def _read_loop(self, conn: serial.Serial, settings: GpsSettings) -> None:
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
            try:
                timestamp_us = gps_timestamp_to_us(raw_line.decode("utf-8", errors="ignore"), settings.mode)
            except Exception as exc:  # noqa: BLE001
                self._set_error(str(exc))
                continue
            with self._lock:
                self._status = replace(self._status, last_timestamp_us=timestamp_us, last_error=None)
                self._last_timestamp_monotonic = time.monotonic()

    def _snapshot_settings(self) -> GpsSettings:
        with self._lock:
            return replace(self._settings)

    def _set_connected(self, connected: bool) -> None:
        with self._lock:
            self._status = replace(self._status, connected=connected)

    def _set_error(self, message: str) -> None:
        with self._lock:
            self._status = replace(self._status, last_error=message)
