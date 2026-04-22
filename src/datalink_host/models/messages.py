from __future__ import annotations

from dataclasses import dataclass, field
from time import monotonic, time
from typing import Any

import numpy as np


@dataclass(slots=True)
class TcpPacket:
    sample_rate: float
    payload_bytes: int
    payload: bytes
    raw_bytes: bytes
    received_at: float = field(default_factory=time)


@dataclass(slots=True)
class ChannelFrame:
    sample_rate: float
    channels: np.ndarray
    received_at: float = field(default_factory=time)
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
    # Time when this frame finished ingest on the host, as a Unix timestamp in seconds.
    received_at: float = field(default_factory=time)
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
    capture_enabled: bool
    gps_enabled: bool
    gps_connected: bool
    gps_mode: str
    gps_port: str
    gps_baudrate: int
    gps_last_timestamp: str | None
    gps_last_error: str | None
    gps_fallback_active: bool
    latest_raw: np.ndarray | None
    latest_unwrapped: np.ndarray | None
    latest_data1: np.ndarray | None
    latest_data2: np.ndarray | None
    updated_at: float


@dataclass(slots=True)
class ControlResponse:
    status: str
    payload: dict[str, Any]
