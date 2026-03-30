from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


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
    remote_host: str = "169.254.56.252"
    remote_port: int = 3677
    recv_size: int = 65536
    reconnect_interval_seconds: float = 3.0
    connect_timeout_seconds: float = 5.0


@dataclass(slots=True)
class ControlServerSettings:
    host: str = "0.0.0.0"
    port: int = 19001


@dataclass(slots=True)
class ProcessingSettings:
    data1_rate: float = 100.0
    data2_rate: float = 10.0
    enable_phase_unwrap: bool = True


@dataclass(slots=True)
class StorageSettings:
    enabled: bool = False
    root: Path = Path("./var/storage")
    file_duration_seconds: int = 300
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
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 16000
    stream_id_template: str = "{network}_{station}_{location}_{channel}/MSEED"
    ack_required: bool = True
    send_data2: bool = False
    reconnect_interval_seconds: float = 5.0
    socket_timeout_seconds: float = 5.0
    client_id: str = "datalink-host:runtime:0:python"


@dataclass(slots=True)
class GuiSettings:
    refresh_interval_ms: int = 250
    max_points_per_trace: int = 4000


@dataclass(slots=True)
class CaptureSettings:
    enabled: bool = False
    path: Path = Path("./var/captures/session.dlhcap")


@dataclass(slots=True)
class AppSettings:
    protocol: ProtocolSettings = field(default_factory=ProtocolSettings)
    data_server: DataServerSettings = field(default_factory=DataServerSettings)
    control_server: ControlServerSettings = field(default_factory=ControlServerSettings)
    processing: ProcessingSettings = field(default_factory=ProcessingSettings)
    storage: StorageSettings = field(default_factory=StorageSettings)
    datalink: DataLinkSettings = field(default_factory=DataLinkSettings)
    gui: GuiSettings = field(default_factory=GuiSettings)
    capture: CaptureSettings = field(default_factory=CaptureSettings)
