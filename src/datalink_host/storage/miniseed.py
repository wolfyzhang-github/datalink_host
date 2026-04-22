from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

import numpy as np
from obspy import Stream, Trace, UTCDateTime

from datalink_host.core.config import StorageSettings
from datalink_host.models.messages import ProcessedFrame
from datalink_host.services.gps_time import format_timestamp_iso_utc, format_timestamp_us


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class _StreamBuffer:
    sample_rate: float
    next_segment_start: UTCDateTime | None = None
    buffer_start: UTCDateTime | None = None
    data: np.ndarray | None = None


class MiniSeedWriter:
    def __init__(self, settings: StorageSettings) -> None:
        self._settings = settings
        self._lock = threading.Lock()
        self._buffers: dict[tuple[str, int], _StreamBuffer] = {}

    def update_settings(self, settings: StorageSettings) -> None:
        with self._lock:
            self._settings = settings

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
            LOGGER.warning("Skipping storage write because frame timestamp is unavailable")
            return
        if state.next_segment_start is None:
            state.next_segment_start = UTCDateTime(timestamp_us / 1_000_000.0)
            LOGGER.info(
                "Storage stream anchored: key=%s start_utc=%s sample_rate_hz=%.3f first_segment_samples=%s",
                self._stream_label(key),
                format_timestamp_iso_utc(timestamp_us),
                state.sample_rate,
                values.size,
            )
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
            LOGGER.info(
                "Flushing residual MiniSEED buffer: key=%s pending_samples=%s sample_rate_hz=%.3f start_utc=%s",
                self._stream_label(key),
                state.data.size,
                state.sample_rate,
                format_timestamp_iso_utc(state.buffer_start.ns // 1000),
            )
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
        output_dir.mkdir(parents=True, exist_ok=True)
        channel_code = settings.channel_codes[channel_index]
        timestamp = self._format_timestamp(start_time)
        filename = (
            f"{settings.network}.{settings.station}.{timestamp}.{settings.location}.{channel_code}.mseed"
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
        Stream([trace]).write(str(output_dir / filename), format="MSEED", encoding="FLOAT32")
        LOGGER.info(
            "Wrote MiniSEED file: path=%s stream=%s start_utc=%s end_utc=%s duration_s=%.3f "
            "samples=%s sample_rate_hz=%.3f dtype=%s encoding=FLOAT32",
            output_dir / filename,
            self._stream_label(key),
            format_timestamp_iso_utc(start_time.ns // 1000),
            format_timestamp_iso_utc(end_time.ns // 1000),
            encoded_values.size / max(sample_rate, 1e-9),
            encoded_values.size,
            sample_rate,
            encoded_values.dtype,
        )

    @staticmethod
    def _format_timestamp(timestamp: UTCDateTime) -> str:
        return format_timestamp_us(timestamp.ns // 1000)

    @staticmethod
    def _stream_label(key: tuple[str, int]) -> str:
        group_name, channel_index = key
        return f"{group_name}:{channel_index + 1:02d}"
