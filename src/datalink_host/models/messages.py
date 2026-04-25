from __future__ import annotations

from dataclasses import dataclass, field
from time import monotonic
from typing import Any

import numpy as np

from datalink_host.core.clock import wall_time


@dataclass(slots=True)
class TcpPacket:
    sample_rate: float
    payload_bytes: int
    payload: bytes
    raw_bytes: bytes
    received_at: float = field(default_factory=wall_time)


@dataclass(slots=True)
class ChannelFrame:
    sample_rate: float
    channels: np.ndarray
    received_at: float = field(default_factory=wall_time)
    timestamp_us: int | None = None
    enqueued_at_monotonic: float = field(default_factory=monotonic)


@dataclass(slots=True)
class ProcessedFrame:
    # Source sample rate before any downsampling, in Hz.
    sample_rate: float
    # Raw input samples with shape (channels, samples).
    raw: np.ndarray
    # Phase-unwrapped version of raw data; same shape as raw.
    unwrapped: np.ndarray
    # First downsampled output group with shape (channels, samples).
    data1: np.ndarray
    # Effective sample rate of data1 after downsampling, in Hz.
    data1_sample_rate: float
    # Second downsampled output group with shape (channels, samples).
    data2: np.ndarray
    # Effective sample rate of data2 after downsampling, in Hz.
    data2_sample_rate: float
    # Time when this frame finished ingest, as a Unix timestamp in seconds.
    received_at: float = field(default_factory=wall_time)
    # Authoritative frame start time in microseconds since Unix epoch.
    timestamp_us: int | None = None


@dataclass(slots=True)
class RuntimeSnapshot:
    data_connected: bool
    control_connected: bool
    packets_received: int
    bytes_received: int
    frames_dropped: int
    last_error: str | None
    source_sample_rate: float | None
    data1_rate: float
    data2_rate: float
    baseline_length_meters: float | None
    queue_depth: int
    datalink_enabled: bool
    datalink_connected: bool
    datalink_packets_sent: int
    datalink_bytes_sent: int
    datalink_reconnects: int
    datalink_last_error: str | None
    datalink_publish_queue_depth: int
    datalink_publish_frames_dropped: int
    datalink_publish_last_error: str | None
    storage_enabled: bool
    storage_queue_depth: int
    storage_frames_dropped: int
    storage_last_error: str | None
    storage_disk_total_bytes: int | None
    storage_disk_used_bytes: int | None
    storage_disk_free_bytes: int | None
    storage_disk_usage_percent: float | None
    capture_enabled: bool
    gnss_enabled: bool
    gnss_connected: bool
    gnss_mode: str
    gnss_port: str
    gnss_baudrate: int
    gnss_last_timestamp: str | None
    gnss_last_error: str | None
    gnss_fallback_active: bool
    latest_raw: np.ndarray | None
    latest_unwrapped: np.ndarray | None
    latest_data1: np.ndarray | None
    latest_data2: np.ndarray | None
    updated_at: float


@dataclass(slots=True)
class ControlResponse:
    status: str
    payload: dict[str, Any]
