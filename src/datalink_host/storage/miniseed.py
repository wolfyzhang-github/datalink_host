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
from datalink_host.models.messages import ProcessedFrame
from datalink_host.services.gnss_time import format_timestamp_iso_utc


LOGGER = logging.getLogger(__name__)
STORAGE_FILE_TYPE_MARKER = "R"
LOG_CHANNEL_CODE = "LOG"


@dataclass(slots=True)
class _StreamBuffer:
    sample_rate: float
    next_segment_start: UTCDateTime | None = None
    buffer_start: UTCDateTime | None = None
    data: np.ndarray | None = None
    anchor_timestamp_us: int | None = None
    anchor_start: UTCDateTime | None = None
    segments_appended: int = 0
    samples_appended: int = 0
    chunks_written: int = 0


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
                        values=np.asarray(channels[channel_index], dtype=np.float32),
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
            state.anchor_timestamp_us = timestamp_us
            state.anchor_start = state.next_segment_start
            LOGGER.info(
                "存储时间锚点建立：该通道第一次收到可写样本，后续文件名时间会从这个锚点按采样率连续推进。"
                "流=%s 时间来源=frame.timestamp_us 原始微秒时间戳(frame_timestamp_us)=%s "
                "对应UTC(frame_timestamp_utc)=%s 转换公式=start_time=UTCDateTime(frame.timestamp_us / 1000000.0) "
                "转换后的Unix秒(start_epoch_s)=%.6f 转换后的纳秒(start_ns)=%s "
                "采样率(sample_rate_hz)=%.3f 首段样本数(first_segment_samples)=%s",
                self._stream_label(key),
                timestamp_us,
                format_timestamp_iso_utc(timestamp_us),
                timestamp_us / 1_000_000.0,
                state.next_segment_start.ns,
                state.sample_rate,
                values.size,
            )
        segment_start = state.next_segment_start
        state.next_segment_start = segment_start + (values.size / state.sample_rate)
        state.segments_appended += 1
        state.samples_appended += int(values.size)
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
            self._write_chunk(key, state.buffer_start, state.sample_rate, chunk, settings, state)
            state.data = state.data[samples_per_file:]
            state.buffer_start = state.buffer_start + (samples_per_file / state.sample_rate)
            if state.data.size == 0:
                state.data = None
                return
        if flush_all and state.data is not None and state.data.size > 0:
            LOGGER.info(
                "刷新残留 MiniSEED 缓冲：程序关闭或强制 flush 时，不足一个完整文件时长的样本也会写盘。"
                "流=%s 待写样本数(pending_samples)=%s 采样率(sample_rate_hz)=%.3f "
                "残留段起始UTC(start_utc)=%s",
                self._stream_label(key),
                state.data.size,
                state.sample_rate,
                format_timestamp_iso_utc(state.buffer_start.ns // 1000),
            )
            self._write_chunk(key, state.buffer_start, state.sample_rate, state.data, settings, state)
            state.data = None
            state.buffer_start = None

    def _write_chunk(
        self,
        key: tuple[str, int],
        start_time: UTCDateTime,
        sample_rate: float,
        values: np.ndarray,
        settings: StorageSettings,
        state: _StreamBuffer,
    ) -> None:
        group_name, channel_index = key
        output_dir = settings.root / f"{group_name.title()}-{channel_index + 1:02d}"
        output_dir.mkdir(parents=True, exist_ok=True)
        channel_code = settings.channel_codes[channel_index]
        start_time_us = start_time.ns // 1000
        start_seconds, start_micros = divmod(start_time_us, 1_000_000)
        timestamp_millis = start_micros // 1000
        timestamp = self._format_timestamp(start_time)
        filename = self._build_filename(
            settings=settings,
            timestamp=timestamp,
            channel_code=channel_code,
            extension="mseed",
        )
        encoded_values = np.asarray(values, dtype=np.float32)
        trace = Trace(encoded_values)
        trace.stats.network = settings.network
        trace.stats.station = settings.station
        trace.stats.location = settings.location
        trace.stats.channel = channel_code
        trace.stats.starttime = start_time
        trace.stats.sampling_rate = sample_rate
        end_time = start_time + (encoded_values.size / max(sample_rate, 1e-9))
        output_path = output_dir / filename
        mseed_payload = self._encode_miniseed(trace)
        log_filename = self._build_filename(
            settings=settings,
            timestamp=timestamp,
            channel_code=LOG_CHANNEL_CODE,
            extension="log",
        )
        log_content = self._build_log_content(start_time=start_time, end_time=end_time)
        anchor_offset_seconds = None
        if state.anchor_start is not None:
            anchor_offset_seconds = float(start_time - state.anchor_start)
        LOGGER.info(
            "MiniSEED 文件名拼接明细："
            "步骤1=取当前待写chunk的起始时间start_time，它来自流缓冲buffer_start，首个buffer_start由frame.timestamp_us锚定，后续按样本数/采样率推进；"
            "步骤2=把start_time转成UTC微秒并拆成秒和微秒；"
            "步骤3=文件名时间字段只保留毫秒，timestamp_millis=timestamp_split_micros//1000，微秒尾数会被截断；"
            "步骤4=按模板{network}.{station}.{timestamp}.R.{location}.{channel_code}.mseed拼出最终文件名。"
            "流(stream)=%s 时间来源(source)=start_time_from_stream_buffer "
            "数据组(group_name)=%s 通道下标0基(channel_index_zero_based)=%s 通道号1基(channel_index_one_based)=%s "
            "存储根目录(root)=%s 目录拼接表达式(output_dir_expr)='%s / %s' 输出目录(output_dir)=%s "
            "台网(network)=%s 台站(station)=%s 位置(location)=%s 通道码(channel_code)=%s "
            "文件类型标记(file_type_marker)=%s 扩展名(extension)=mseed "
            "chunk起始UTC(start_time_utc)=%s chunk起始纳秒(start_time_ns)=%s chunk起始微秒(start_time_us)=%s "
            "UTC拆分后的秒(timestamp_split_seconds)=%s UTC拆分后的微秒(timestamp_split_micros)=%s "
            "截断到毫秒(timestamp_millis)=%03d "
            "时间字段公式(timestamp_expr)=strftime('%%Y%%m%%d%%H%%M%%S') + 三位毫秒 timestamp=%s "
            "文件名模板(filename_expr)='{network}.{station}.{timestamp}.R.{location}.{channel_code}.mseed' "
            "文件名(filename)=%s 完整路径(output_path)=%s "
            "首帧锚点微秒(anchor_timestamp_us)=%s 首帧锚点UTC(anchor_timestamp_utc)=%s "
            "当前chunk相对锚点偏移秒(anchor_offset_s)=%s 采样率(sample_rate_hz)=%.3f "
            "chunk样本数(chunk_samples)=%s chunk时长秒(chunk_duration_s)=%.6f "
            "配置单文件时长(file_duration_seconds)=%s 已追加段数(segments_appended)=%s "
            "已追加样本数(samples_appended)=%s 写入前已落盘chunk数(chunks_written_before)=%s",
            self._stream_label(key),
            group_name,
            channel_index,
            channel_index + 1,
            settings.root,
            settings.root,
            f"{group_name.title()}-{channel_index + 1:02d}",
            output_dir,
            settings.network,
            settings.station,
            settings.location,
            channel_code,
            STORAGE_FILE_TYPE_MARKER,
            format_timestamp_iso_utc(start_time_us),
            start_time.ns,
            start_time_us,
            start_seconds,
            start_micros,
            timestamp_millis,
            timestamp,
            filename,
            output_path,
            state.anchor_timestamp_us,
            format_timestamp_iso_utc(state.anchor_timestamp_us),
            "-" if anchor_offset_seconds is None else f"{anchor_offset_seconds:.6f}",
            sample_rate,
            encoded_values.size,
            encoded_values.size / max(sample_rate, 1e-9),
            settings.file_duration_seconds,
            state.segments_appended,
            state.samples_appended,
            state.chunks_written,
        )
        self._ensure_disk_space(settings.root, len(mseed_payload) + len(log_content.encode("utf-8")))
        output_path.write_bytes(mseed_payload)
        state.chunks_written += 1
        LOGGER.info(
            "MiniSEED 文件已落盘：写入完成后可用该完整路径定位文件；文件名中的timestamp就是上一步由start_utc截断到毫秒得到的值。"
            "完整路径(path)=%s 流(stream)=%s 起始UTC(start_utc)=%s 结束UTC(end_utc)=%s "
            "时长秒(duration_s)=%.3f 样本数(samples)=%s 采样率(sample_rate_hz)=%.3f "
            "数据类型(dtype)=%s 编码(encoding)=FLOAT32 文件名(filename)=%s "
            "文件名时间字段(timestamp)=%s 已落盘chunk数(chunks_written)=%s",
            output_path,
            self._stream_label(key),
            format_timestamp_iso_utc(start_time.ns // 1000),
            format_timestamp_iso_utc(end_time.ns // 1000),
            encoded_values.size / max(sample_rate, 1e-9),
            encoded_values.size,
            sample_rate,
            encoded_values.dtype,
            filename,
            timestamp,
            state.chunks_written,
        )
        (output_dir / log_filename).write_text(log_content, encoding="utf-8")

    @staticmethod
    def _encode_miniseed(trace: Trace) -> bytes:
        buffer = io.BytesIO()
        Stream([trace]).write(buffer, format="MSEED", encoding="FLOAT32")
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
            self._delete_stored_mseed(oldest_mseed)

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

    def _delete_stored_mseed(self, path: Path) -> None:
        log_path = self._log_sidecar_for_mseed(path)
        try:
            path.unlink()
            LOGGER.warning("Deleted oldest MiniSEED file to free disk space: path=%s", path)
        except FileNotFoundError:
            pass
        if log_path is None:
            return
        try:
            log_path.unlink()
            LOGGER.warning("Deleted MiniSEED sidecar log during rolling storage cleanup: path=%s", log_path)
        except FileNotFoundError:
            pass

    @staticmethod
    def _log_sidecar_for_mseed(path: Path) -> Path | None:
        parts = path.name.split(".")
        if len(parts) < 7 or parts[-1] != "mseed":
            return None
        parts[-2] = LOG_CHANNEL_CODE
        parts[-1] = "log"
        return path.with_name(".".join(parts))

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

    @staticmethod
    def _stream_label(key: tuple[str, int]) -> str:
        group_name, channel_index = key
        return f"{group_name}:{channel_index + 1:02d}"
