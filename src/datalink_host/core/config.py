from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from datalink_host.core.paths import default_capture_path
from datalink_host.core.output_encoding import DEFAULT_INT32_GAIN


@dataclass(slots=True)
class ProtocolSettings:
    frame_header: int = 11
    frame_header_size: int = 2
    length_field_size: int = 8
    length_field_format: str = "float64"
    length_field_units: str = "values"
    byte_order: str = "big"
    channels: int = 8
    channel_layout: str = "interleaved"


@dataclass(slots=True)
class DataServerSettings:
    mode: str = "client"
    host: str = "0.0.0.0"
    port: int = 3677
    remote_host: str = "127.0.0.1"
    remote_port: int = 6340
    recv_size: int = 65536
    reconnect_interval_seconds: float = 3.0
    connect_timeout_seconds: float = 5.0
    max_packet_payload_bytes: int = 8 * 1024 * 1024
    max_pending_buffer_bytes: int = 16 * 1024 * 1024


@dataclass(slots=True)
class ControlServerSettings:
    host: str = "0.0.0.0"
    port: int = 19001


@dataclass(slots=True)
class ProcessingSettings:
    data1_rate: float = 100.0
    data2_rate: float = 10.0
    enable_phase_unwrap: bool = True
    baseline_length_meters: float | None = None


@dataclass(slots=True)
class StorageSettings:
    enabled: bool = True
    root: Path = Path(r"E:\data")
    file_duration_seconds: int = 60
    output_data_type: str = "float32"
    int32_gain: float = DEFAULT_INT32_GAIN
    network: str = "SC"
    station: str = "S0001"
    location: str = "10"
    channel_codes: tuple[str, ...] = (
        "HSH",
        "HSZ",
        "HS1",
        "HS2",
        "HS3",
        "HS4",
        "HTH",
        "HTZ",
    )


@dataclass(slots=True)
class DataLinkSettings:
    enabled: bool = True
    host: str = "10.2.12.61"
    port: int = 16000
    stream_id_template: str = "{network}_{station}_{location}_{channel}/MSEED"
    ack_required: bool = True
    send_data2: bool = False
    reconnect_interval_seconds: float = 5.0
    socket_timeout_seconds: float = 5.0
    client_id: str = "datalink-host:runtime:0:python"


@dataclass(slots=True)
class GnssSettings:
    enabled: bool = True
    port: str = ""
    baudrate: int = 115200
    mode: str = "deploy"
    poll_interval_seconds: float = 0.1
    timestamp_interval_seconds: float = 1.0
    packet_timestamp_timeout_seconds: float = 1.0
    serial_timeout_seconds: float = 0.1
    command: str = "timestamp"


@dataclass(slots=True)
class WebSettings:
    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 18080


@dataclass(slots=True)
class GuiSettings:
    refresh_interval_ms: int = 250
    plot_window_seconds: float = 20.0
    plot_history_seconds: float = 120.0
    max_points_per_trace: int = 200000
    display_max_points_per_trace: int = 5000


@dataclass(slots=True)
class CaptureSettings:
    enabled: bool = False
    path: Path = field(default_factory=default_capture_path)


@dataclass(slots=True)
class AppSettings:
    protocol: ProtocolSettings = field(default_factory=ProtocolSettings)
    data_server: DataServerSettings = field(default_factory=DataServerSettings)
    control_server: ControlServerSettings = field(default_factory=ControlServerSettings)
    processing: ProcessingSettings = field(default_factory=ProcessingSettings)
    storage: StorageSettings = field(default_factory=StorageSettings)
    datalink: DataLinkSettings = field(default_factory=DataLinkSettings)
    gnss: GnssSettings = field(default_factory=GnssSettings)
    web: WebSettings = field(default_factory=WebSettings)
    gui: GuiSettings = field(default_factory=GuiSettings)
    capture: CaptureSettings = field(default_factory=CaptureSettings)
