from __future__ import annotations

from collections import deque
import logging
import queue
import threading
import time
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from typing import Any

import numpy as np

from datalink_host.core.clock import set_wall_time_provider, wall_time
from datalink_host.core.config import AppSettings
from datalink_host.core.output_encoding import normalize_int32_gain, normalize_output_data_type
from datalink_host.core.validation import (
    parse_choice,
    parse_port,
    parse_positive_float,
    parse_positive_int,
)
from datalink_host.debug.capture import PacketCaptureWriter
from datalink_host.ingest.control_server import TcpControlServer
from datalink_host.ingest.data_server import TcpDataServer
from datalink_host.models.messages import ChannelFrame, ProcessedFrame, RuntimeSnapshot, TcpPacket
from datalink_host.processing.pipeline import ProcessingPipeline
from datalink_host.services.background_sink import BackgroundFrameSink
from datalink_host.services.gnss_time import (
    GnssTimeService,
    format_timestamp_iso_utc,
    format_timestamp_us,
)
from datalink_host.storage.miniseed import MiniSeedWriter
from datalink_host.transport.datalink import DataLinkPublisher


LOGGER = logging.getLogger(__name__)
QUEUE_BACKLOG_WARNING_DEPTH = 8
FRAME_QUEUE_WAIT_WARNING_MS = 200.0
FRAME_PROCESSING_WARNING_MS = 200.0
FAN_OUT_ENQUEUE_WARNING_MS = 50.0
MONITOR_PACKET_HISTORY_LIMIT = 40
MONITOR_PACKET_DUMP_LIMIT_BYTES = 1024
STORAGE_SINK_QUEUE_MAXSIZE = 16
STORAGE_SINK_BACKLOG_WARNING_DEPTH = 4
STORAGE_SINK_QUEUE_WAIT_WARNING_MS = 500.0
STORAGE_SINK_HANDLE_WARNING_MS = 500.0
DATALINK_SINK_QUEUE_MAXSIZE = 16
DATALINK_SINK_BACKLOG_WARNING_DEPTH = 4
DATALINK_SINK_QUEUE_WAIT_WARNING_MS = 500.0
DATALINK_SINK_HANDLE_WARNING_MS = 500.0
DATA_SERVER_MODES = {"client", "server"}
LENGTH_FIELD_FORMATS = {"uint", "float64"}
LENGTH_FIELD_UNITS = {"bytes", "values"}
BYTE_ORDERS = {"little", "big"}
CHANNEL_LAYOUTS = {"interleaved", "channel-major"}
GNSS_MODES = {"debug", "deploy"}
INITIAL_GNSS_TIMESTAMP_WAIT_MARGIN_SECONDS = 0.25


class RuntimeService:
    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings
        self._pipeline = ProcessingPipeline(settings.processing)
        self._storage = MiniSeedWriter(settings.storage)
        self._datalink = DataLinkPublisher(settings.datalink, settings.storage)
        self._gnss_time = GnssTimeService(settings.gnss)
        set_wall_time_provider(self._wall_time_seconds)
        self._storage_sink = BackgroundFrameSink(
            "storage",
            self._storage.write,
            maxsize=STORAGE_SINK_QUEUE_MAXSIZE,
            backlog_warning_depth=STORAGE_SINK_BACKLOG_WARNING_DEPTH,
            queue_wait_warning_ms=STORAGE_SINK_QUEUE_WAIT_WARNING_MS,
            handle_warning_ms=STORAGE_SINK_HANDLE_WARNING_MS,
        )
        self._datalink_sink = BackgroundFrameSink(
            "datalink-publish",
            self._datalink.publish,
            maxsize=DATALINK_SINK_QUEUE_MAXSIZE,
            backlog_warning_depth=DATALINK_SINK_BACKLOG_WARNING_DEPTH,
            queue_wait_warning_ms=DATALINK_SINK_QUEUE_WAIT_WARNING_MS,
            handle_warning_ms=DATALINK_SINK_HANDLE_WARNING_MS,
        )
        self._capture = PacketCaptureWriter(settings.capture.path) if settings.capture.enabled else None
        self._queue: queue.Queue[ChannelFrame] = queue.Queue(maxsize=32)
        self._lock = threading.Lock()
        self._snapshot = RuntimeSnapshot(
            data_connected=False,
            control_connected=False,
            packets_received=0,
            bytes_received=0,
            frames_dropped=0,
            last_error=None,
            source_sample_rate=None,
            data1_rate=settings.processing.data1_rate,
            data2_rate=settings.processing.data2_rate,
            baseline_length_meters=settings.processing.baseline_length_meters,
            queue_depth=0,
            datalink_enabled=settings.datalink.enabled,
            datalink_connected=False,
            datalink_packets_sent=0,
            datalink_bytes_sent=0,
            datalink_reconnects=0,
            datalink_last_error=None,
            storage_enabled=settings.storage.enabled,
            datalink_publish_queue_depth=0,
            datalink_publish_frames_dropped=0,
            datalink_publish_last_error=None,
            storage_queue_depth=0,
            storage_frames_dropped=0,
            storage_last_error=None,
            storage_disk_total_bytes=None,
            storage_disk_used_bytes=None,
            storage_disk_free_bytes=None,
            storage_disk_usage_percent=None,
            capture_enabled=settings.capture.enabled,
            gnss_enabled=settings.gnss.enabled,
            gnss_connected=False,
            gnss_mode=settings.gnss.mode,
            gnss_port=settings.gnss.port,
            gnss_baudrate=settings.gnss.baudrate,
            gnss_last_timestamp=None,
            gnss_last_error=None,
            gnss_fallback_active=False,
            latest_raw=None,
            latest_unwrapped=None,
            latest_data1=None,
            latest_data2=None,
            updated_at=wall_time(),
        )
        self._data_server = self._build_data_server()
        self._control_server = TcpControlServer(
            settings.control_server,
            on_connection_state=self._set_control_connected,
            on_message=self._handle_control_message,
        )
        self._stop_event = threading.Event()
        self._processor_thread = threading.Thread(target=self._run_processor, name="processor", daemon=True)
        self._runtime_started = False
        self._data_server_active = False
        self._last_frame_start_us: int | None = None
        self._last_frame_duration_us: int | None = None
        self._recent_packets: deque[dict[str, Any]] = deque(maxlen=MONITOR_PACKET_HISTORY_LIMIT)
        self._timestamp_resolution_count = 0
        self._last_timestamp_resolution_warning_key: tuple[str, str | None] | None = None

    def _build_data_server(self) -> TcpDataServer:
        return TcpDataServer(
            self._settings.data_server,
            self._settings.protocol,
            on_packet=self._on_packet,
            on_frame=self._on_frame,
            on_connection_state=self._set_data_connected,
            on_bytes_received=self._add_bytes_received,
            on_error=self._set_error,
        )

    def _wall_time_seconds(self) -> float:
        if self._settings.gnss.enabled:
            gnss_time_us = self._gnss_time.current_time_us()
            if gnss_time_us is not None:
                return gnss_time_us / 1_000_000.0
        return time.time()

    def _restart_data_server(self) -> None:
        was_active = self._data_server_active
        self._data_server.stop()
        self._data_server = self._build_data_server()
        if was_active:
            self._data_server.start()
            self._data_server_active = True

    def start(self) -> None:
        if self._runtime_started:
            return
        self._runtime_started = True
        self._stop_event.clear()
        self._gnss_time.start()
        self._storage_sink.start()
        self._datalink_sink.start()
        self._processor_thread = threading.Thread(target=self._run_processor, name="processor", daemon=True)
        self._processor_thread.start()
        self._control_server.start()
        self.resume_processing()

    def stop(self) -> None:
        if not self._runtime_started:
            return
        self._runtime_started = False
        self._stop_event.set()
        self.pause_processing()
        self._control_server.stop()
        self._processor_thread.join(timeout=2.0)
        self._storage_sink.stop()
        self._datalink_sink.stop()
        self._gnss_time.stop()
        self._storage.close()
        self._datalink.close()
        if self._capture is not None:
            self._capture.close()

    def is_processing_active(self) -> bool:
        return self._data_server_active

    def gnss_ports(self) -> list[str]:
        return self._gnss_time.available_ports()

    def resume_processing(self) -> None:
        if self._data_server_active:
            return
        self._data_server.start()
        self._data_server_active = True

    def pause_processing(self) -> None:
        if not self._data_server_active:
            return
        self._data_server.stop()
        self._data_server_active = False
        self._pipeline.reset()
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break
        with self._lock:
            self._snapshot.data_connected = False
            self._snapshot.queue_depth = 0
            self._snapshot.updated_at = wall_time()

    def snapshot(self) -> RuntimeSnapshot:
        storage_stats = self._storage_sink.stats()
        storage_disk_usage = self._storage.disk_usage()
        datalink_publish_stats = self._datalink_sink.stats()
        datalink_stats = self._datalink.stats()
        gnss_status = self._gnss_time.status()
        with self._lock:
            gnss_last_timestamp = self._snapshot.gnss_last_timestamp
            if gnss_last_timestamp is None and gnss_status.last_timestamp_us is not None:
                gnss_last_timestamp = format_timestamp_us(gnss_status.last_timestamp_us)
            gnss_last_error = self._snapshot.gnss_last_error or gnss_status.last_error
            return replace(
                self._snapshot,
                datalink_connected=datalink_stats.connected,
                datalink_packets_sent=datalink_stats.packets_sent,
                datalink_bytes_sent=datalink_stats.bytes_sent,
                datalink_reconnects=datalink_stats.reconnects,
                datalink_last_error=datalink_stats.last_error,
                datalink_publish_queue_depth=datalink_publish_stats.queue_depth,
                datalink_publish_frames_dropped=datalink_publish_stats.frames_dropped,
                datalink_publish_last_error=datalink_publish_stats.last_error,
                storage_queue_depth=storage_stats.queue_depth,
                storage_frames_dropped=storage_stats.frames_dropped,
                storage_last_error=storage_stats.last_error,
                storage_disk_total_bytes=(
                    None if storage_disk_usage is None else storage_disk_usage.total_bytes
                ),
                storage_disk_used_bytes=(
                    None if storage_disk_usage is None else storage_disk_usage.used_bytes
                ),
                storage_disk_free_bytes=(
                    None if storage_disk_usage is None else storage_disk_usage.free_bytes
                ),
                storage_disk_usage_percent=(
                    None if storage_disk_usage is None else storage_disk_usage.usage_percent
                ),
                gnss_enabled=self._settings.gnss.enabled,
                gnss_connected=gnss_status.connected,
                gnss_mode=self._settings.gnss.mode,
                gnss_port=self._settings.gnss.port,
                gnss_baudrate=self._settings.gnss.baudrate,
                gnss_last_timestamp=gnss_last_timestamp,
                gnss_last_error=gnss_last_error,
            )

    def current_config(self) -> dict[str, Any]:
        with self._lock:
            return {
                "processing": {
                    "data1_rate": self._settings.processing.data1_rate,
                    "data2_rate": self._settings.processing.data2_rate,
                    "enable_phase_unwrap": self._settings.processing.enable_phase_unwrap,
                    "baseline_length_meters": self._settings.processing.baseline_length_meters,
                },
                "protocol": {
                    "frame_header": self._settings.protocol.frame_header,
                    "frame_header_size": self._settings.protocol.frame_header_size,
                    "length_field_size": self._settings.protocol.length_field_size,
                    "length_field_format": self._settings.protocol.length_field_format,
                    "length_field_units": self._settings.protocol.length_field_units,
                    "byte_order": self._settings.protocol.byte_order,
                    "channel_layout": self._settings.protocol.channel_layout,
                    "channels": self._settings.protocol.channels,
                },
                "data_server": {
                    "mode": self._settings.data_server.mode,
                    "host": self._settings.data_server.host,
                    "port": self._settings.data_server.port,
                    "remote_host": self._settings.data_server.remote_host,
                    "remote_port": self._settings.data_server.remote_port,
                },
                "storage": {
                    "enabled": self._settings.storage.enabled,
                    "root": str(self._settings.storage.root),
                    "file_duration_seconds": self._settings.storage.file_duration_seconds,
                    "output_data_type": self._settings.storage.output_data_type,
                    "int32_gain": self._settings.storage.int32_gain,
                    "network": self._settings.storage.network,
                    "station": self._settings.storage.station,
                    "location": self._settings.storage.location,
                    "channel_codes": list(self._settings.storage.channel_codes),
                },
                "datalink": {
                    "enabled": self._settings.datalink.enabled,
                    "host": self._settings.datalink.host,
                    "port": self._settings.datalink.port,
                    "stream_id_template": self._settings.datalink.stream_id_template,
                    "ack_required": self._settings.datalink.ack_required,
                    "send_data2": self._settings.datalink.send_data2,
                },
                "gnss": {
                    "enabled": self._settings.gnss.enabled,
                    "port": self._settings.gnss.port,
                    "baudrate": self._settings.gnss.baudrate,
                    "mode": self._settings.gnss.mode,
                    "poll_interval_seconds": self._settings.gnss.poll_interval_seconds,
                    "timestamp_interval_seconds": self._settings.gnss.timestamp_interval_seconds,
                    "packet_timestamp_timeout_seconds": self._settings.gnss.packet_timestamp_timeout_seconds,
                },
                "capture": {
                    "enabled": self._settings.capture.enabled,
                    "path": str(self._settings.capture.path),
                },
            }

    def monitor_view(
        self,
        mode: str = "raw",
        *,
        max_points: int = 2048,
        max_packets: int = 20,
        window_seconds: float | None = None,
    ) -> dict[str, Any]:
        snapshot = self.snapshot()
        data, sample_rate = self._series_for_monitor(
            snapshot,
            mode=mode,
            max_points=max_points,
            window_seconds=window_seconds,
        )
        with self._lock:
            recent_packets = [dict(packet) for packet in list(self._recent_packets)[-max_packets:]]
            channel_codes = list(self._settings.storage.channel_codes)
            protocol_channels = self._settings.protocol.channels
        return {
            "processing_active": self.is_processing_active(),
            "channel_codes": channel_codes[:protocol_channels],
            "waveform": {
                "mode": mode,
                "sample_rate": sample_rate,
                "channels": int(data.shape[0]) if data is not None else protocol_channels,
                "points": int(data.shape[1]) if data is not None else 0,
                "window_seconds": window_seconds,
                "series": data.tolist() if data is not None else [],
                "updated_at": snapshot.updated_at,
            },
            "recent_packets": recent_packets,
        }

    def update_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        if "data1_rate" in payload or "data2_rate" in payload:
            payload = {
                **payload,
                "processing": {
                    **payload.get("processing", {}),
                    **{
                        key: payload[key]
                        for key in ("data1_rate", "data2_rate")
                        if key in payload
                    },
                },
            }
        if any(key in payload for key in ("storage_enabled", "storage_root", "file_duration_seconds")):
            payload = {
                **payload,
                "storage": {
                    **payload.get("storage", {}),
                    **(
                        {"enabled": payload["storage_enabled"]}
                        if "storage_enabled" in payload
                        else {}
                    ),
                    **({"root": payload["storage_root"]} if "storage_root" in payload else {}),
                    **(
                        {"file_duration_seconds": payload["file_duration_seconds"]}
                        if "file_duration_seconds" in payload
                        else {}
                    ),
                },
            }
        if "capture_enabled" in payload or "capture_path" in payload:
            payload = {
                **payload,
                "capture": {
                    **payload.get("capture", {}),
                    **({"enabled": payload["capture_enabled"]} if "capture_enabled" in payload else {}),
                    **({"path": payload["capture_path"]} if "capture_path" in payload else {}),
                },
            }
        with self._lock:
            processing = payload.get("processing", {})
            protocol = payload.get("protocol", {})
            data_server = payload.get("data_server", {})
            storage = payload.get("storage", {})
            datalink = payload.get("datalink", {})
            gnss = payload.get("gnss", {})
            capture = payload.get("capture", {})
            restart_data_server = False

            if "data1_rate" in processing or "data2_rate" in processing:
                data1_rate = parse_positive_float(
                    processing.get("data1_rate", self._settings.processing.data1_rate),
                    "processing.data1_rate",
                )
                data2_rate = parse_positive_float(
                    processing.get("data2_rate", self._settings.processing.data2_rate),
                    "processing.data2_rate",
                )
                self._pipeline.update_rates(data1_rate, data2_rate)
                self._settings.processing.data1_rate = data1_rate
                self._settings.processing.data2_rate = data2_rate
                self._snapshot.data1_rate = data1_rate
                self._snapshot.data2_rate = data2_rate

            if "enable_phase_unwrap" in processing:
                self._settings.processing.enable_phase_unwrap = bool(processing["enable_phase_unwrap"])
                self._pipeline.reset()

            if "baseline_length_meters" in processing:
                baseline_length = processing["baseline_length_meters"]
                if baseline_length in (None, ""):
                    self._settings.processing.baseline_length_meters = None
                else:
                    self._settings.processing.baseline_length_meters = parse_positive_float(
                        baseline_length,
                        "processing.baseline_length_meters",
                    )
                self._snapshot.baseline_length_meters = self._settings.processing.baseline_length_meters

            if protocol:
                protocol_changed = False
                if "frame_header" in protocol:
                    frame_header = int(str(protocol["frame_header"]), 0)
                    if frame_header < 0:
                        raise ValueError("protocol.frame_header must be greater than or equal to 0")
                    protocol_changed = protocol_changed or frame_header != self._settings.protocol.frame_header
                    self._settings.protocol.frame_header = frame_header
                if "frame_header_size" in protocol:
                    frame_header_size = int(protocol["frame_header_size"])
                    if frame_header_size not in {2, 4, 8}:
                        raise ValueError("protocol.frame_header_size must be one of: 2, 4, 8")
                    protocol_changed = protocol_changed or frame_header_size != self._settings.protocol.frame_header_size
                    self._settings.protocol.frame_header_size = frame_header_size
                if "length_field_size" in protocol:
                    length_field_size = int(protocol["length_field_size"])
                    if length_field_size not in {4, 8}:
                        raise ValueError("protocol.length_field_size must be one of: 4, 8")
                    protocol_changed = protocol_changed or length_field_size != self._settings.protocol.length_field_size
                    self._settings.protocol.length_field_size = length_field_size
                if "length_field_format" in protocol:
                    length_field_format = parse_choice(
                        protocol["length_field_format"],
                        "protocol.length_field_format",
                        LENGTH_FIELD_FORMATS,
                    )
                    protocol_changed = protocol_changed or length_field_format != self._settings.protocol.length_field_format
                    self._settings.protocol.length_field_format = length_field_format
                if "length_field_units" in protocol:
                    length_field_units = parse_choice(
                        protocol["length_field_units"],
                        "protocol.length_field_units",
                        LENGTH_FIELD_UNITS,
                    )
                    protocol_changed = protocol_changed or length_field_units != self._settings.protocol.length_field_units
                    self._settings.protocol.length_field_units = length_field_units
                if "byte_order" in protocol:
                    byte_order = parse_choice(protocol["byte_order"], "protocol.byte_order", BYTE_ORDERS)
                    protocol_changed = protocol_changed or byte_order != self._settings.protocol.byte_order
                    self._settings.protocol.byte_order = byte_order
                if "channel_layout" in protocol:
                    channel_layout = parse_choice(
                        protocol["channel_layout"],
                        "protocol.channel_layout",
                        CHANNEL_LAYOUTS,
                    )
                    protocol_changed = protocol_changed or channel_layout != self._settings.protocol.channel_layout
                    self._settings.protocol.channel_layout = channel_layout
                if "channels" in protocol:
                    channels = int(protocol["channels"])
                    if channels != self._settings.protocol.channels:
                        raise ValueError("Changing channel count at runtime is not supported yet")
                restart_data_server = restart_data_server or protocol_changed

            if data_server:
                data_server_changed = False
                if "mode" in data_server:
                    mode = parse_choice(data_server["mode"], "data_server.mode", DATA_SERVER_MODES)
                    data_server_changed = data_server_changed or mode != self._settings.data_server.mode
                    self._settings.data_server.mode = mode
                if "host" in data_server:
                    host = str(data_server["host"])
                    data_server_changed = data_server_changed or host != self._settings.data_server.host
                    self._settings.data_server.host = host
                if "port" in data_server:
                    port = parse_port(data_server["port"], "data_server.port")
                    data_server_changed = data_server_changed or port != self._settings.data_server.port
                    self._settings.data_server.port = port
                if "remote_host" in data_server:
                    remote_host = str(data_server["remote_host"])
                    data_server_changed = data_server_changed or remote_host != self._settings.data_server.remote_host
                    self._settings.data_server.remote_host = remote_host
                if "remote_port" in data_server:
                    remote_port = parse_port(data_server["remote_port"], "data_server.remote_port")
                    data_server_changed = data_server_changed or remote_port != self._settings.data_server.remote_port
                    self._settings.data_server.remote_port = remote_port
                restart_data_server = restart_data_server or data_server_changed

            if storage:
                new_storage = deepcopy(self._settings.storage)
                if "enabled" in storage:
                    new_storage.enabled = bool(storage["enabled"])
                if "root" in storage:
                    new_storage.root = Path(storage["root"])
                if "file_duration_seconds" in storage:
                    new_storage.file_duration_seconds = parse_positive_int(
                        storage["file_duration_seconds"],
                        "storage.file_duration_seconds",
                    )
                if "output_data_type" in storage:
                    new_storage.output_data_type = normalize_output_data_type(storage["output_data_type"])
                if "int32_gain" in storage:
                    new_storage.int32_gain = normalize_int32_gain(storage["int32_gain"])
                if "network" in storage:
                    new_storage.network = str(storage["network"])
                if "station" in storage:
                    new_storage.station = str(storage["station"])
                if "location" in storage:
                    new_storage.location = str(storage["location"])
                if "channel_codes" in storage:
                    codes = tuple(str(code) for code in storage["channel_codes"])
                    if len(codes) != self._settings.protocol.channels:
                        raise ValueError("channel_codes length must match protocol channel count")
                    new_storage.channel_codes = codes
                self._settings.storage = new_storage
                self._storage.update_settings(new_storage)
                self._datalink.update_settings(self._settings.datalink, new_storage)
                self._snapshot.storage_enabled = new_storage.enabled

            if datalink:
                if "enabled" in datalink:
                    self._settings.datalink.enabled = bool(datalink["enabled"])
                if "host" in datalink:
                    self._settings.datalink.host = str(datalink["host"])
                if "port" in datalink:
                    self._settings.datalink.port = parse_port(datalink["port"], "datalink.port")
                if "stream_id_template" in datalink:
                    self._settings.datalink.stream_id_template = str(datalink["stream_id_template"])
                if "ack_required" in datalink:
                    self._settings.datalink.ack_required = bool(datalink["ack_required"])
                if "send_data2" in datalink:
                    self._settings.datalink.send_data2 = bool(datalink["send_data2"])
                self._snapshot.datalink_enabled = self._settings.datalink.enabled
                self._datalink.update_settings(self._settings.datalink, self._settings.storage)

            if gnss:
                if "enabled" in gnss:
                    self._settings.gnss.enabled = bool(gnss["enabled"])
                if "port" in gnss:
                    self._settings.gnss.port = str(gnss["port"])
                if "baudrate" in gnss:
                    self._settings.gnss.baudrate = parse_positive_int(gnss["baudrate"], "gnss.baudrate")
                if "mode" in gnss:
                    self._settings.gnss.mode = parse_choice(gnss["mode"], "gnss.mode", GNSS_MODES)
                if "poll_interval_seconds" in gnss:
                    self._settings.gnss.poll_interval_seconds = parse_positive_float(
                        gnss["poll_interval_seconds"],
                        "gnss.poll_interval_seconds",
                    )
                if "timestamp_interval_seconds" in gnss:
                    self._settings.gnss.timestamp_interval_seconds = parse_positive_float(
                        gnss["timestamp_interval_seconds"],
                        "gnss.timestamp_interval_seconds",
                    )
                if "packet_timestamp_timeout_seconds" in gnss:
                    self._settings.gnss.packet_timestamp_timeout_seconds = parse_positive_float(
                        gnss["packet_timestamp_timeout_seconds"],
                        "gnss.packet_timestamp_timeout_seconds",
                    )
                self._gnss_time.update_settings(deepcopy(self._settings.gnss))
                self._snapshot.gnss_enabled = self._settings.gnss.enabled
                self._snapshot.gnss_mode = self._settings.gnss.mode
                self._snapshot.gnss_port = self._settings.gnss.port
                self._snapshot.gnss_baudrate = self._settings.gnss.baudrate

            if capture:
                if "enabled" in capture:
                    self._settings.capture.enabled = bool(capture["enabled"])
                if "path" in capture:
                    self._settings.capture.path = Path(capture["path"])
                if self._settings.capture.enabled and self._capture is None:
                    self._capture = PacketCaptureWriter(self._settings.capture.path)
                elif not self._settings.capture.enabled and self._capture is not None:
                    self._capture.close()
                    self._capture = None
                elif self._settings.capture.enabled and self._capture is not None:
                    self._capture.close()
                    self._capture = PacketCaptureWriter(self._settings.capture.path)
                self._snapshot.capture_enabled = self._settings.capture.enabled

            self._snapshot.updated_at = wall_time()
        if restart_data_server:
            self._restart_data_server()
        return self.current_config()

    def _on_packet(self, packet: TcpPacket) -> None:
        if self._capture is not None:
            self._capture.write_record(
                received_at=packet.received_at,
                sample_rate=packet.sample_rate,
                payload_bytes=packet.payload_bytes,
                packet_bytes=packet.raw_bytes,
            )
        hex_dump, truncated_bytes = format_packet_hex_dump(packet.raw_bytes)
        with self._lock:
            self._recent_packets.append(
                {
                    "received_at": packet.received_at,
                    "sample_rate": packet.sample_rate,
                    "payload_bytes": packet.payload_bytes,
                    "raw_bytes": len(packet.raw_bytes),
                    "hex_dump": hex_dump,
                    "truncated_bytes": truncated_bytes,
                }
            )

    def _on_frame(self, frame: ChannelFrame) -> None:
        timestamp_us, used_fallback, gnss_error = self._resolve_frame_timestamp(frame)
        frame.timestamp_us = timestamp_us
        try:
            self._queue.put_nowait(frame)
        except queue.Full:
            with self._lock:
                self._snapshot.frames_dropped += 1
                self._snapshot.last_error = "Processing queue is full"
                self._snapshot.updated_at = wall_time()
            LOGGER.warning("Processing queue is full; dropping decoded frame")
            return
        with self._lock:
            if timestamp_us is not None:
                self._last_frame_start_us = timestamp_us
                self._last_frame_duration_us = self._frame_duration_us(frame)
            self._snapshot.gnss_fallback_active = used_fallback
            self._snapshot.gnss_last_error = gnss_error
            self._snapshot.packets_received += 1
            self._snapshot.source_sample_rate = frame.sample_rate
            self._snapshot.queue_depth = self._queue.qsize()
            self._snapshot.updated_at = wall_time()
            queue_depth = self._snapshot.queue_depth
        if queue_depth >= QUEUE_BACKLOG_WARNING_DEPTH and queue_depth % QUEUE_BACKLOG_WARNING_DEPTH == 0:
            LOGGER.warning(
                "Frame queue backlog is %s; decoded frames are arriving faster than processing/display can consume",
                queue_depth,
            )

    def _resolve_frame_timestamp(self, frame: ChannelFrame) -> tuple[int | None, bool, str | None]:
        sample_count = frame.channels.shape[1]
        frame_duration_us = self._frame_duration_us(frame)
        with self._lock:
            last_frame_start_us = self._last_frame_start_us
            last_frame_duration_us = self._last_frame_duration_us
        expected_next_us = (
            None
            if last_frame_start_us is None or last_frame_duration_us is None
            else last_frame_start_us + last_frame_duration_us
        )
        if not self._settings.gnss.enabled or not self._settings.gnss.port.strip():
            timestamp_us = self._start_time_from_reference_timestamp(
                reference_timestamp_us=int(round(frame.received_at * 1_000_000)),
                frame_duration_us=frame_duration_us,
            )
            self._log_timestamp_resolution(
                source="host_received_at",
                assigned_timestamp_us=timestamp_us,
                gnss_raw_timestamp_us=None,
                gnss_now_timestamp_us=None,
                last_frame_start_us=last_frame_start_us,
                expected_next_us=expected_next_us,
                frame_duration_us=frame_duration_us,
                sample_rate=frame.sample_rate,
                sample_count=sample_count,
                used_fallback=False,
                error=None,
                gnss_skew_us=None,
            )
            return timestamp_us, False, None

        gnss_status = self._gnss_time.status()
        timeout_seconds = self._gnss_timestamp_wait_timeout(
            gnss_status=gnss_status,
            last_frame_start_us=last_frame_start_us,
        )
        gnss_packet_end_us = self._gnss_time.wait_for_next_timestamp_us(timeout_seconds)
        if gnss_packet_end_us is not None:
            timestamp_us = self._start_time_from_reference_timestamp(
                reference_timestamp_us=gnss_packet_end_us,
                frame_duration_us=frame_duration_us,
            )
            self._log_timestamp_resolution(
                source="gnss_packet_end_time",
                assigned_timestamp_us=timestamp_us,
                gnss_raw_timestamp_us=gnss_packet_end_us,
                gnss_now_timestamp_us=None,
                last_frame_start_us=last_frame_start_us,
                expected_next_us=expected_next_us,
                frame_duration_us=frame_duration_us,
                sample_rate=frame.sample_rate,
                sample_count=sample_count,
                used_fallback=False,
                error=None,
                gnss_skew_us=None,
            )
            return timestamp_us, False, None

        if last_frame_start_us is not None and last_frame_duration_us is not None:
            timestamp_us = last_frame_start_us + last_frame_duration_us
            error = (
                gnss_status.last_error
                or f"GNSS packet timestamp did not arrive within {timeout_seconds:.3f}s; "
                "using previous frame timestamp fallback"
            )
            self._log_timestamp_resolution(
                source="previous_frame_fallback",
                assigned_timestamp_us=timestamp_us,
                gnss_raw_timestamp_us=gnss_status.last_timestamp_us,
                gnss_now_timestamp_us=None,
                last_frame_start_us=last_frame_start_us,
                expected_next_us=expected_next_us,
                frame_duration_us=frame_duration_us,
                sample_rate=frame.sample_rate,
                sample_count=sample_count,
                used_fallback=True,
                error=error,
                gnss_skew_us=None,
            )
            return (timestamp_us, True, error)

        timestamp_us = self._start_time_from_reference_timestamp(
            reference_timestamp_us=int(round(frame.received_at * 1_000_000)),
            frame_duration_us=frame_duration_us,
        )
        error = gnss_status.last_error or (
            f"GNSS packet timestamp did not arrive within {timeout_seconds:.3f}s; "
            "using host received timestamp fallback for first frame"
        )
        self._log_timestamp_resolution(
            source="host_received_at_fallback",
            assigned_timestamp_us=timestamp_us,
            gnss_raw_timestamp_us=gnss_status.last_timestamp_us,
            gnss_now_timestamp_us=None,
            last_frame_start_us=last_frame_start_us,
            expected_next_us=expected_next_us,
            frame_duration_us=frame_duration_us,
            sample_rate=frame.sample_rate,
            sample_count=sample_count,
            used_fallback=True,
            error=error,
            gnss_skew_us=None,
        )
        return timestamp_us, True, error

    def _gnss_timestamp_wait_timeout(
        self,
        *,
        gnss_status: Any,
        last_frame_start_us: int | None,
    ) -> float:
        timeout_seconds = max(
            self._settings.gnss.packet_timestamp_timeout_seconds,
            self._settings.gnss.serial_timeout_seconds,
        )
        if last_frame_start_us is None and gnss_status.last_timestamp_us is None:
            startup_timeout = (
                self._settings.gnss.timestamp_interval_seconds
                + timeout_seconds
                + INITIAL_GNSS_TIMESTAMP_WAIT_MARGIN_SECONDS
            )
            return max(timeout_seconds, startup_timeout)
        return timeout_seconds

    @staticmethod
    def _frame_duration_us(frame: ChannelFrame) -> int | None:
        if frame.sample_rate <= 0:
            return None
        sample_count = frame.channels.shape[1]
        return int(round((sample_count / frame.sample_rate) * 1_000_000))

    @staticmethod
    def _start_time_from_reference_timestamp(
        *,
        reference_timestamp_us: int | None,
        frame_duration_us: int | None,
    ) -> int | None:
        if reference_timestamp_us is None:
            return None
        if frame_duration_us is None:
            return reference_timestamp_us
        return max(reference_timestamp_us - frame_duration_us, 0)

    def _log_timestamp_resolution(
        self,
        *,
        source: str,
        assigned_timestamp_us: int | None,
        gnss_raw_timestamp_us: int | None,
        gnss_now_timestamp_us: int | None,
        last_frame_start_us: int | None,
        expected_next_us: int | None,
        frame_duration_us: int | None,
        sample_rate: float,
        sample_count: int,
        used_fallback: bool,
        error: str | None,
        gnss_skew_us: int | None,
    ) -> None:
        self._timestamp_resolution_count += 1
        should_warn = used_fallback or error is not None or source == "gnss_resync_to_current_time"
        if not should_warn:
            self._last_timestamp_resolution_warning_key = None
            return
        warning_key = (source, error)
        if warning_key == self._last_timestamp_resolution_warning_key:
            return
        self._last_timestamp_resolution_warning_key = warning_key
        LOGGER.warning(
            "Frame timestamp resolved: source=%s assigned_utc=%s gnss_raw_utc=%s gnss_now_utc=%s "
            "last_frame_start_utc=%s expected_next_utc=%s frame_duration_ms=%s gnss_skew_ms=%s "
            "sample_rate_hz=%.3f sample_count=%s fallback=%s error=%s decision_index=%s",
            source,
            format_timestamp_iso_utc(assigned_timestamp_us),
            format_timestamp_iso_utc(gnss_raw_timestamp_us),
            format_timestamp_iso_utc(gnss_now_timestamp_us),
            format_timestamp_iso_utc(last_frame_start_us),
            format_timestamp_iso_utc(expected_next_us),
            "-"
            if frame_duration_us is None
            else f"{frame_duration_us / 1_000:.3f}",
            "-"
            if gnss_skew_us is None
            else f"{gnss_skew_us / 1_000:.3f}",
            sample_rate,
            sample_count,
            "yes" if used_fallback else "no",
            error or "-",
            self._timestamp_resolution_count,
        )

    def _run_processor(self) -> None:
        while not self._stop_event.is_set():
            try:
                frame = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                processing_started_at = time.monotonic()
                queue_wait_ms = (processing_started_at - frame.enqueued_at_monotonic) * 1000.0
                processed = self._pipeline.process(frame)
                pipeline_ms = (time.monotonic() - processing_started_at) * 1000.0
                fan_out_started_at = time.monotonic()
                storage_enqueue_ms, datalink_publish_enqueue_ms = self._fan_out(processed)
                fan_out_ms = (time.monotonic() - fan_out_started_at) * 1000.0
                total_ms = (time.monotonic() - processing_started_at) * 1000.0
                queue_depth_after = self._queue.qsize()
                if (
                    queue_wait_ms >= FRAME_QUEUE_WAIT_WARNING_MS
                    or pipeline_ms >= FRAME_PROCESSING_WARNING_MS
                    or fan_out_ms >= FAN_OUT_ENQUEUE_WARNING_MS
                ):
                    LOGGER.warning(
                        "Frame processing is lagging: queue_wait_ms=%.1f, pipeline_ms=%.1f, "
                        "storage_enqueue_ms=%.1f, datalink_publish_enqueue_ms=%.1f, "
                        "fan_out_ms=%.1f, total_ms=%.1f, queue_depth_after=%s, "
                        "sample_rate=%.3f, raw_shape=%s, data1_shape=%s, data2_shape=%s",
                        queue_wait_ms,
                        pipeline_ms,
                        storage_enqueue_ms,
                        datalink_publish_enqueue_ms,
                        fan_out_ms,
                        total_ms,
                        queue_depth_after,
                        processed.sample_rate,
                        processed.raw.shape,
                        processed.data1.shape,
                        processed.data2.shape,
                    )
                with self._lock:
                    self._snapshot.latest_raw = self._append_plot_history(
                        self._snapshot.latest_raw,
                        processed.raw,
                        processed.sample_rate,
                    )
                    self._snapshot.latest_unwrapped = self._append_plot_history(
                        self._snapshot.latest_unwrapped,
                        processed.unwrapped,
                        processed.sample_rate,
                    )
                    self._snapshot.latest_data1 = self._append_plot_history(
                        self._snapshot.latest_data1,
                        processed.data1,
                        processed.data1_sample_rate,
                    )
                    self._snapshot.latest_data2 = self._append_plot_history(
                        self._snapshot.latest_data2,
                        processed.data2,
                        processed.data2_sample_rate,
                    )
                    self._snapshot.queue_depth = queue_depth_after
                    self._snapshot.updated_at = wall_time()
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Processing pipeline failed")
                self._set_error(f"Processing pipeline failed: {exc}")

    def _fan_out(self, frame: ProcessedFrame) -> tuple[float, float]:
        storage_enqueue_ms = 0.0
        datalink_publish_enqueue_ms = 0.0
        if self._settings.storage.enabled:
            storage_started_at = time.monotonic()
            self._storage_sink.submit(frame)
            storage_enqueue_ms = (time.monotonic() - storage_started_at) * 1000.0
        if self._settings.datalink.enabled:
            publish_started_at = time.monotonic()
            self._datalink_sink.submit(frame)
            datalink_publish_enqueue_ms = (time.monotonic() - publish_started_at) * 1000.0
        return storage_enqueue_ms, datalink_publish_enqueue_ms

    def _set_data_connected(self, connected: bool) -> None:
        if not connected:
            self._pipeline.reset()
        else:
            dropped = self._gnss_time.clear_pending_timestamps()
            if dropped:
                LOGGER.info("Cleared %s pending GNSS timestamp events when data connection opened", dropped)
        with self._lock:
            self._snapshot.data_connected = connected
            if connected:
                self._snapshot.latest_raw = None
                self._snapshot.latest_unwrapped = None
                self._snapshot.latest_data1 = None
                self._snapshot.latest_data2 = None
            self._snapshot.updated_at = wall_time()

    def _set_control_connected(self, connected: bool) -> None:
        with self._lock:
            self._snapshot.control_connected = connected
            self._snapshot.updated_at = wall_time()

    def _add_bytes_received(self, count: int) -> None:
        with self._lock:
            self._snapshot.bytes_received += count
            self._snapshot.updated_at = wall_time()

    def _set_error(self, message: str) -> None:
        LOGGER.error("%s", message)
        with self._lock:
            self._snapshot.last_error = message
            self._snapshot.updated_at = wall_time()

    def _handle_control_message(self, request: dict[str, Any]) -> dict[str, Any]:
        message_type = request.get("type")
        if message_type == "get_status":
            snapshot = self.snapshot()
            return {
                "status": "ok",
                "payload": {
                    "data_connected": snapshot.data_connected,
                    "control_connected": snapshot.control_connected,
                    "packets_received": snapshot.packets_received,
                    "bytes_received": snapshot.bytes_received,
                    "frames_dropped": snapshot.frames_dropped,
                    "queue_depth": snapshot.queue_depth,
                    "source_sample_rate": snapshot.source_sample_rate,
                    "data1_rate": snapshot.data1_rate,
                    "data2_rate": snapshot.data2_rate,
                    "baseline_length_meters": snapshot.baseline_length_meters,
                    "storage_enabled": snapshot.storage_enabled,
                    "storage_queue_depth": snapshot.storage_queue_depth,
                    "storage_frames_dropped": snapshot.storage_frames_dropped,
                    "storage_last_error": snapshot.storage_last_error,
                    "storage_disk_total_bytes": snapshot.storage_disk_total_bytes,
                    "storage_disk_used_bytes": snapshot.storage_disk_used_bytes,
                    "storage_disk_free_bytes": snapshot.storage_disk_free_bytes,
                    "storage_disk_usage_percent": snapshot.storage_disk_usage_percent,
                    "datalink_enabled": snapshot.datalink_enabled,
                    "datalink_connected": snapshot.datalink_connected,
                    "datalink_packets_sent": snapshot.datalink_packets_sent,
                    "datalink_bytes_sent": snapshot.datalink_bytes_sent,
                    "datalink_reconnects": snapshot.datalink_reconnects,
                    "datalink_last_error": snapshot.datalink_last_error,
                    "datalink_publish_queue_depth": snapshot.datalink_publish_queue_depth,
                    "datalink_publish_frames_dropped": snapshot.datalink_publish_frames_dropped,
                    "datalink_publish_last_error": snapshot.datalink_publish_last_error,
                    "capture_enabled": snapshot.capture_enabled,
                    "gnss_enabled": snapshot.gnss_enabled,
                    "gnss_connected": snapshot.gnss_connected,
                    "gnss_mode": snapshot.gnss_mode,
                    "gnss_port": snapshot.gnss_port,
                    "gnss_baudrate": snapshot.gnss_baudrate,
                    "gnss_last_timestamp": snapshot.gnss_last_timestamp,
                    "gnss_last_error": snapshot.gnss_last_error,
                    "gnss_fallback_active": snapshot.gnss_fallback_active,
                    "last_error": snapshot.last_error,
                },
            }

        if message_type == "get_config":
            return {"status": "ok", "payload": self.current_config()}

        if message_type == "set_config":
            return {"status": "ok", "payload": self.update_config(request.get("payload", {}))}

        if message_type == "set_feature":
            payload = request.get("payload", {})
            config_payload: dict[str, Any] = {}
            if "storage_enabled" in payload:
                config_payload.setdefault("storage", {})["enabled"] = bool(payload["storage_enabled"])
            if "datalink_enabled" in payload:
                config_payload.setdefault("datalink", {})["enabled"] = bool(payload["datalink_enabled"])
            if "gnss_enabled" in payload:
                config_payload.setdefault("gnss", {})["enabled"] = bool(payload["gnss_enabled"])
            return {"status": "ok", "payload": self.update_config(config_payload)}

        raise ValueError(f"Unsupported control message type: {message_type}")

    def _series_for_monitor(
        self,
        snapshot: RuntimeSnapshot,
        *,
        mode: str,
        max_points: int,
        window_seconds: float | None = None,
    ) -> tuple[np.ndarray | None, float | None]:
        mode_name = mode if mode in {"raw", "unwrapped", "data1", "data2"} else "raw"
        if mode_name == "raw":
            return self._slice_monitor_series(
                snapshot.latest_raw,
                snapshot.source_sample_rate,
                max_points,
                window_seconds,
            )
        if mode_name == "data1":
            return self._slice_monitor_series(
                snapshot.latest_data1,
                snapshot.data1_rate,
                max_points,
                window_seconds,
            )
        if mode_name == "data2":
            return self._slice_monitor_series(
                snapshot.latest_data2,
                snapshot.data2_rate,
                max_points,
                window_seconds,
            )
        return self._slice_monitor_series(
            snapshot.latest_unwrapped,
            snapshot.source_sample_rate,
            max_points,
            window_seconds,
        )

    def _append_plot_history(
        self,
        current: np.ndarray | None,
        incoming: np.ndarray,
        sample_rate: float | None,
    ) -> np.ndarray | None:
        if incoming.size == 0:
            return current
        max_points = self._plot_history_points(sample_rate)
        if current is None or current.size == 0 or current.shape[0] != incoming.shape[0]:
            return slice_for_plot(incoming, max_points)
        return slice_for_plot(np.concatenate([current, incoming], axis=1), max_points)

    def _plot_history_points(self, sample_rate: float | None) -> int:
        configured_limit = max(int(self._settings.gui.max_points_per_trace), 1)
        seconds = max(float(self._settings.gui.plot_history_seconds), 1.0)
        if sample_rate is None or sample_rate <= 0:
            return configured_limit
        return max(1, min(configured_limit, int(round(sample_rate * seconds))))

    @staticmethod
    def _slice_monitor_series(
        data: np.ndarray | None,
        sample_rate: float | None,
        max_points: int,
        window_seconds: float | None,
    ) -> tuple[np.ndarray | None, float | None]:
        if window_seconds is not None and sample_rate is not None and sample_rate > 0:
            window_points = max(1, int(round(sample_rate * max(window_seconds, 0.0))))
            data = slice_for_plot(data, window_points)
        plotted, step = downsample_for_plot(data, max_points)
        effective_rate = sample_rate / step if sample_rate is not None and sample_rate > 0 else sample_rate
        return plotted, effective_rate


def slice_for_plot(data: np.ndarray | None, max_points: int) -> np.ndarray | None:
    if data is None or data.size == 0:
        return data
    if data.shape[1] <= max_points:
        return data
    return data[:, -max_points:]


def downsample_for_plot(data: np.ndarray | None, max_points: int) -> tuple[np.ndarray | None, int]:
    if data is None or data.size == 0:
        return data, 1
    point_limit = max(int(max_points), 1)
    point_count = data.shape[1]
    if point_count <= point_limit:
        return data, 1
    step = max(int(np.ceil(point_count / point_limit)), 1)
    indices = np.arange(point_count - 1, -1, -step, dtype=np.int64)[::-1]
    return data[:, indices], step


def format_packet_hex_dump(packet_bytes: bytes, max_bytes: int = MONITOR_PACKET_DUMP_LIMIT_BYTES) -> tuple[str, int]:
    visible = packet_bytes[:max_bytes]
    lines: list[str] = []
    for offset in range(0, len(visible), 16):
        chunk = visible[offset : offset + 16]
        hex_part = " ".join(f"{value:02X}" for value in chunk)
        ascii_part = "".join(chr(value) if 32 <= value <= 126 else "." for value in chunk)
        lines.append(f"{offset:06X}  {hex_part:<47}  {ascii_part}")
    return "\n".join(lines), max(len(packet_bytes) - len(visible), 0)
