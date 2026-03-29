from __future__ import annotations

from dataclasses import dataclass, field
from time import time
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


@dataclass(slots=True)
class ProcessedFrame:
    sample_rate: float
    raw: np.ndarray
    unwrapped: np.ndarray
    data1: np.ndarray
    data2: np.ndarray
    received_at: float = field(default_factory=time)


@dataclass(slots=True)
class RuntimeSnapshot:
    data_connected: bool
    control_connected: bool
    packets_received: int
    bytes_received: int
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
    storage_enabled: bool
    capture_enabled: bool
    latest_raw: np.ndarray | None
    latest_unwrapped: np.ndarray | None
    latest_data1: np.ndarray | None
    latest_data2: np.ndarray | None
    updated_at: float


@dataclass(slots=True)
class ControlResponse:
    status: str
    payload: dict[str, Any]
