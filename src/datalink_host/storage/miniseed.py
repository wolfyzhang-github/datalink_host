from __future__ import annotations

import io
import logging
import shutil
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from obspy import Stream, Trace, UTCDateTime

from datalink_host.core.config import StorageSettings
from datalink_host.core.logging import get_recent_logs
from datalink_host.core.output_encoding import encode_samples_for_miniseed
from datalink_host.models.messages import ProcessedFrame
from datalink_host.services.gnss_time import format_timestamp_iso_utc


LOGGER = logging.getLogger(__name__)
STORAGE_FILE_TYPE_MARKER = "R"
LOG_CHANNEL_CODE = "LOG"
LOG_DIRECTORY_NAME = "log"


@dataclass(slots=True)
class _StreamBuffer:
    sample_rate: float
    next_segment_start: UTCDateTime | None = None
    buffer_start: UTCDateTime | None = None
    data: np.ndarray | None = None


@dataclass(slots=True)
class DiskUsage:
    total_bytes: int
    used_bytes: int
    free_bytes: int
    usage_percent: float


class MiniSeedWriter:
    def __init__(self, settings: StorageSettings) -> None:
        self._settings = settings
        self._lock = threading.Lock()
        self._buffers: dict[tuple[str, int], _StreamBuffer] = {}

    def update_settings(self, settings: StorageSettings) -> None:
        with self._lock:
            self._settings = settings

    def disk_usage(self) -> DiskUsage | None:
        with self._lock:
            root = self._settings.root
        try:
            usage = shutil.disk_usage(self._existing_disk_path(root))
        except OSError as exc:
            LOGGER.warning("Unable to read storage disk usage: root=%s error=%s", root, exc)
            return None
        usage_percent = (usage.used / usage.total * 100.0) if usage.total > 0 else 0.0
        return DiskUsage(
            total_bytes=int(usage.total),
            used_bytes=int(usage.used),
            free_bytes=int(usage.free),
            usage_percent=usage_percent,
        )

    def close(self) -> None:
        with self._lock:
            pending = list(self._buffers.items())
            self._buffers.clear()
            settings = self._settings
        for key, state in pending:
            self._flush_buffer(key, state, settings, flush_all=True)

    def write(self, frame: ProcessedFrame) -> None:
        outputs = (
            ("data1", frame.data1, frame.data1_sample_rate),
            ("data2", frame.data2, frame.data2_sample_rate),
        )
        with self._lock:
            settings = self._settings
            for group_name, channels, sample_rate in outputs:
                if channels.size == 0:
                    continue
                if sample_rate <= 0:
                    continue
                for channel_index in range(channels.shape[0]):
                    key = (group_name, channel_index)
                    state = self._buffers.get(key)
                    if state is None or not np.isclose(state.sample_rate, sample_rate):
                        state = _StreamBuffer(sample_rate=sample_rate)
                        self._buffers[key] = state
                    self._append_segment(
                        key=key,
                        state=state,
                        values=np.asarray(channels[channel_index]),
                        timestamp_us=frame.timestamp_us,
                    )
                    self._flush_buffer(key, state, settings)

    def _append_segment(
        self,
        *,
        key: tuple[str, int],
        state: _StreamBuffer,
        values: np.ndarray,
        timestamp_us: int | None,
    ) -> None:
        if values.size == 0:
            return
        if timestamp_us is None:
            LOGGER.warning(
                "跳过 MiniSEED 存储：当前帧没有可用时间戳，无法确定文件名中的 UTC 起始时间。"
                "原因=frame.timestamp_us is None"
            )
            return
        if state.next_segment_start is None:
            state.next_segment_start = UTCDateTime(timestamp_us / 1_000_000.0)
        segment_start = state.next_segment_start
        state.next_segment_start = segment_start + (values.size / state.sample_rate)
        if state.data is None or state.data.size == 0:
            state.data = values.copy()
            state.buffer_start = segment_start
            return
        state.data = np.concatenate([state.data, values])

    def _flush_buffer(
        self,
        key: tuple[str, int],
        state: _StreamBuffer,
        settings: StorageSettings,
        *,
        flush_all: bool = False,
    ) -> None:
        if state.data is None or state.data.size == 0 or state.buffer_start is None:
            return
        samples_per_file = max(int(round(state.sample_rate * settings.file_duration_seconds)), 1)
        while state.data is not None and state.data.size >= samples_per_file:
            chunk = state.data[:samples_per_file]
            self._write_chunk(key, state.buffer_start, state.sample_rate, chunk, settings)
            state.data = state.data[samples_per_file:]
            state.buffer_start = state.buffer_start + (samples_per_file / state.sample_rate)
            if state.data.size == 0:
                state.data = None
                return
        if flush_all and state.data is not None and state.data.size > 0:
            self._write_chunk(key, state.buffer_start, state.sample_rate, state.data, settings)
            state.data = None
            state.buffer_start = None

    def _write_chunk(
        self,
        key: tuple[str, int],
        start_time: UTCDateTime,
        sample_rate: float,
        values: np.ndarray,
        settings: StorageSettings,
    ) -> None:
        group_name, channel_index = key
        output_dir = settings.root / f"{group_name.title()}-{channel_index + 1:02d}"
        log_dir = settings.root / LOG_DIRECTORY_NAME
        channel_code = settings.channel_codes[channel_index]
        timestamp = self._format_timestamp(start_time)
        filename = self._build_filename(
            settings=settings,
            timestamp=timestamp,
            channel_code=channel_code,
            extension="mseed",
        )
        encoded = encode_samples_for_miniseed(
            values,
            output_data_type=settings.output_data_type,
            int32_gain=settings.int32_gain,
        )
        trace = Trace(encoded.values)
        trace.stats.network = settings.network
        trace.stats.station = settings.station
        trace.stats.location = settings.location
        trace.stats.channel = channel_code
        trace.stats.starttime = start_time
        trace.stats.sampling_rate = sample_rate
        end_time = start_time + (encoded.values.size / max(sample_rate, 1e-9))
        output_path = output_dir / filename
        mseed_payload = self._encode_miniseed(trace, encoded.miniseed_encoding)
        log_filename = self._build_filename(
            settings=settings,
            timestamp=timestamp,
            channel_code=LOG_CHANNEL_CODE,
            extension="log",
        )
        log_content = self._build_log_content(start_time=start_time, end_time=end_time)
        self._ensure_disk_space(settings.root, len(mseed_payload) + len(log_content.encode("utf-8")))
        output_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(mseed_payload)
        (log_dir / log_filename).write_text(log_content, encoding="utf-8")

    @staticmethod
    def _encode_miniseed(trace: Trace, encoding: str) -> bytes:
        buffer = io.BytesIO()
        Stream([trace]).write(buffer, format="MSEED", encoding=encoding)
        return buffer.getvalue()

    @staticmethod
    def _build_filename(
        *,
        settings: StorageSettings,
        timestamp: str,
        channel_code: str,
        extension: str,
    ) -> str:
        return (
            f"{settings.network}.{settings.station}.{timestamp}.{STORAGE_FILE_TYPE_MARKER}."
            f"{settings.location}.{channel_code}.{extension}"
        )

    @staticmethod
    def _build_log_content(*, start_time: UTCDateTime, end_time: UTCDateTime) -> str:
        lines = get_recent_logs(2000)
        header = (
            f"# MiniSEED sidecar log\n"
            f"# start_utc={format_timestamp_iso_utc(start_time.ns // 1000)}\n"
            f"# end_utc={format_timestamp_iso_utc(end_time.ns // 1000)}\n"
        )
        return header + ("\n".join(lines) if lines else "") + "\n"

    def _ensure_disk_space(self, root: Path, required_bytes: int) -> None:
        root.mkdir(parents=True, exist_ok=True)
        while True:
            usage = shutil.disk_usage(root)
            if usage.free >= required_bytes:
                return
            oldest_mseed = self._oldest_mseed_file(root)
            if oldest_mseed is None:
                raise OSError(
                    f"Insufficient disk space for MiniSEED write: required_bytes={required_bytes}, "
                    f"free_bytes={usage.free}, root={root}"
                )
            self._delete_stored_mseed(oldest_mseed, root)

    def _oldest_mseed_file(self, root: Path) -> Path | None:
        candidates = [path for path in root.rglob("*.mseed") if path.is_file()]
        if not candidates:
            return None
        return min(candidates, key=self._mseed_sort_key)

    @staticmethod
    def _mseed_sort_key(path: Path) -> tuple[str, float, str]:
        parts = path.name.split(".")
        if len(parts) >= 7 and parts[-1] == "mseed":
            timestamp = parts[2]
        else:
            timestamp = ""
        try:
            modified = path.stat().st_mtime
        except OSError:
            modified = 0.0
        return (timestamp, modified, str(path))

    def _delete_stored_mseed(self, path: Path, root: Path) -> None:
        log_paths = self._log_sidecars_for_mseed(path, root)
        timestamp = self._mseed_timestamp(path)
        try:
            path.unlink()
            LOGGER.warning("Deleted oldest MiniSEED file to free disk space: path=%s", path)
        except FileNotFoundError:
            pass
        delete_shared_log = timestamp is None or not self._has_mseed_with_timestamp(root, timestamp)
        for log_path, shared in log_paths:
            if shared and not delete_shared_log:
                continue
            try:
                log_path.unlink()
                LOGGER.warning("Deleted MiniSEED sidecar log during rolling storage cleanup: path=%s", log_path)
            except FileNotFoundError:
                pass

    @staticmethod
    def _log_sidecars_for_mseed(path: Path, root: Path) -> list[tuple[Path, bool]]:
        parts = path.name.split(".")
        if len(parts) < 7 or parts[-1] != "mseed":
            return []
        parts[-2] = LOG_CHANNEL_CODE
        parts[-1] = "log"
        log_filename = ".".join(parts)
        return [
            (root / LOG_DIRECTORY_NAME / log_filename, True),
            (path.with_name(log_filename), False),
        ]

    @staticmethod
    def _mseed_timestamp(path: Path) -> str | None:
        parts = path.name.split(".")
        if len(parts) >= 7 and parts[-1] == "mseed":
            return parts[2]
        return None

    @staticmethod
    def _has_mseed_with_timestamp(root: Path, timestamp: str) -> bool:
        for candidate in root.rglob("*.mseed"):
            parts = candidate.name.split(".")
            if len(parts) >= 7 and parts[-1] == "mseed" and parts[2] == timestamp:
                return True
        return False

    @staticmethod
    def _existing_disk_path(path: Path) -> Path:
        current = path.expanduser()
        while not current.exists() and current != current.parent:
            current = current.parent
        return current if current.exists() else Path(".")

    @staticmethod
    def _format_timestamp(timestamp: UTCDateTime) -> str:
        seconds, micros = divmod(timestamp.ns // 1000, 1_000_000)
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
        return dt.strftime("%Y%m%d%H%M%S") + f"{micros // 1000:03d}"
