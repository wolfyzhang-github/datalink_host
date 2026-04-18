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

from datalink_host.core.config import AppSettings
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
from datalink_host.services.gps_time import GpsTimeService, format_timestamp_us
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
GPS_MODES = {"debug", "deploy"}


class RuntimeService:
    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings
        self._pipeline = ProcessingPipeline(settings.processing)
        self._storage = MiniSeedWriter(settings.storage)
        self._datalink = DataLinkPublisher(settings.datalink, settings.storage)
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
        self._gps_time = GpsTimeService(settings.gps)
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
            capture_enabled=settings.capture.enabled,
            gps_enabled=settings.gps.enabled,
            gps_connected=False,
            gps_mode=settings.gps.mode,
            gps_port=settings.gps.port,
            gps_baudrate=settings.gps.baudrate,
            gps_last_timestamp=None,
            gps_last_error=None,
            gps_fallback_active=False,
            latest_raw=None,
            latest_unwrapped=None,
            latest_data1=None,
            latest_data2=None,
            updated_at=time.time(),
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
        self._frames_enqueued = 0
        self._frames_processed = 0
        self._last_frame_end_us: int | None = None
        self._recent_packets: deque[dict[str, Any]] = deque(maxlen=MONITOR_PACKET_HISTORY_LIMIT)

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
        self._gps_time.start()
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
        self._gps_time.stop()
        self._storage.close()
        self._datalink.close()
        if self._capture is not None:
            self._capture.close()

    def is_processing_active(self) -> bool:
        return self._data_server_active

    def gps_ports(self) -> list[str]:
        return self._gps_time.available_ports()

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
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break
        with self._lock:
            self._snapshot.data_connected = False
            self._snapshot.queue_depth = 0
            self._snapshot.updated_at = time.time()

    def snapshot(self) -> RuntimeSnapshot:
        storage_stats = self._storage_sink.stats()
        datalink_publish_stats = self._datalink_sink.stats()
        datalink_stats = self._datalink.stats()
        gps_status = self._gps_time.status()
        with self._lock:
            gps_last_timestamp = self._snapshot.gps_last_timestamp
            if gps_last_timestamp is None and gps_status.last_timestamp_us is not None:
                gps_last_timestamp = format_timestamp_us(gps_status.last_timestamp_us)
            gps_last_error = self._snapshot.gps_last_error or gps_status.last_error
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
                gps_enabled=self._settings.gps.enabled,
                gps_connected=gps_status.connected,
                gps_mode=self._settings.gps.mode,
                gps_port=self._settings.gps.port,
                gps_baudrate=self._settings.gps.baudrate,
                gps_last_timestamp=gps_last_timestamp,
                gps_last_error=gps_last_error,
            )

    def current_config(self) -> dict[str, Any]:
        with self._lock:
            return {
                "processing": {
                    "data1_rate": self._settings.processing.data1_rate,
                    "data2_rate": self._settings.processing.data2_rate,
                    "enable_phase_unwrap": self._settings.processing.enable_phase_unwrap,
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
                "gps": {
                    "enabled": self._settings.gps.enabled,
                    "port": self._settings.gps.port,
                    "baudrate": self._settings.gps.baudrate,
                    "mode": self._settings.gps.mode,
                    "poll_interval_seconds": self._settings.gps.poll_interval_seconds,
                },
                "capture": {
                    "enabled": self._settings.capture.enabled,
                    "path": str(self._settings.capture.path),
                },
            }

    def monitor_view(self, mode: str = "raw", *, max_points: int = 1200, max_packets: int = 20) -> dict[str, Any]:
        snapshot = self.snapshot()
        data, sample_rate = self._series_for_monitor(snapshot, mode=mode, max_points=max_points)
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
            gps = payload.get("gps", {})
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

            if gps:
                if "enabled" in gps:
                    self._settings.gps.enabled = bool(gps["enabled"])
                if "port" in gps:
                    self._settings.gps.port = str(gps["port"])
                if "baudrate" in gps:
                    self._settings.gps.baudrate = parse_positive_int(gps["baudrate"], "gps.baudrate")
                if "mode" in gps:
                    mode = parse_choice(gps["mode"], "gps.mode", GPS_MODES)
                    self._settings.gps.mode = mode
                if "poll_interval_seconds" in gps:
                    self._settings.gps.poll_interval_seconds = parse_positive_float(
                        gps["poll_interval_seconds"],
                        "gps.poll_interval_seconds",
                    )
                self._gps_time.update_settings(deepcopy(self._settings.gps))
                self._snapshot.gps_enabled = self._settings.gps.enabled
                self._snapshot.gps_mode = self._settings.gps.mode
                self._snapshot.gps_port = self._settings.gps.port
                self._snapshot.gps_baudrate = self._settings.gps.baudrate

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

            self._snapshot.updated_at = time.time()
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
        timestamp_us, used_fallback, gps_error = self._resolve_frame_timestamp(frame)
        frame.timestamp_us = timestamp_us
        try:
            self._queue.put_nowait(frame)
        except queue.Full:
            with self._lock:
                self._snapshot.frames_dropped += 1
                self._snapshot.last_error = "Processing queue is full"
                self._snapshot.updated_at = time.time()
            LOGGER.warning("Processing queue is full; dropping decoded frame")
            return
        self._frames_enqueued += 1
        if self._frames_enqueued == 1:
            LOGGER.info(
                "Enqueued first decoded frame: sample_rate=%.3f Hz, channels=%s, samples=%s",
                frame.sample_rate,
                frame.channels.shape[0],
                frame.channels.shape[1],
            )
        with self._lock:
            if timestamp_us is not None:
                self._last_frame_end_us = timestamp_us
                if self._settings.gps.enabled:
                    self._snapshot.gps_last_timestamp = format_timestamp_us(timestamp_us)
            self._snapshot.gps_fallback_active = used_fallback
            self._snapshot.gps_last_error = gps_error
            self._snapshot.packets_received += 1
            self._snapshot.source_sample_rate = frame.sample_rate
            self._snapshot.queue_depth = self._queue.qsize()
            self._snapshot.updated_at = time.time()
            queue_depth = self._snapshot.queue_depth
        if queue_depth >= QUEUE_BACKLOG_WARNING_DEPTH and queue_depth % QUEUE_BACKLOG_WARNING_DEPTH == 0:
            LOGGER.warning(
                "Frame queue backlog is %s; decoded frames are arriving faster than processing/display can consume",
                queue_depth,
            )

    def _resolve_frame_timestamp(self, frame: ChannelFrame) -> tuple[int | None, bool, str | None]:
        if not self._settings.gps.enabled:
            return int(round(frame.received_at * 1_000_000)), False, None

        gps_now_us = self._gps_time.current_time_us()
        gps_status = self._gps_time.status()
        if gps_now_us is not None:
            return gps_now_us, False, None

        duration_us = self._frame_duration_us(frame)
        with self._lock:
            last_frame_end_us = self._last_frame_end_us
        if last_frame_end_us is not None and duration_us is not None:
            return (
                last_frame_end_us + duration_us,
                True,
                gps_status.last_error or "GPS unavailable; using previous frame timestamp fallback",
            )

        return None, False, gps_status.last_error or "GPS unavailable and no fallback timestamp is available"

    @staticmethod
    def _frame_duration_us(frame: ChannelFrame) -> int | None:
        if frame.sample_rate <= 0:
            return None
        sample_count = frame.channels.shape[1]
        return int(round((sample_count / frame.sample_rate) * 1_000_000))

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
                self._frames_processed += 1
                if self._frames_processed == 1:
                    LOGGER.info(
                        "Processed first frame: raw_shape=%s, unwrapped_shape=%s, data1_shape=%s, data2_shape=%s",
                        processed.raw.shape,
                        processed.unwrapped.shape,
                        processed.data1.shape,
                        processed.data2.shape,
                    )
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
                    self._snapshot.latest_raw = processed.raw
                    self._snapshot.latest_unwrapped = processed.unwrapped
                    self._snapshot.latest_data1 = processed.data1
                    self._snapshot.latest_data2 = processed.data2
                    self._snapshot.queue_depth = queue_depth_after
                    self._snapshot.updated_at = time.time()
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
        with self._lock:
            self._snapshot.data_connected = connected
            self._snapshot.updated_at = time.time()

    def _set_control_connected(self, connected: bool) -> None:
        with self._lock:
            self._snapshot.control_connected = connected
            self._snapshot.updated_at = time.time()

    def _add_bytes_received(self, count: int) -> None:
        with self._lock:
            self._snapshot.bytes_received += count
            self._snapshot.updated_at = time.time()

    def _set_error(self, message: str) -> None:
        LOGGER.error("%s", message)
        with self._lock:
            self._snapshot.last_error = message
            self._snapshot.updated_at = time.time()

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
                    "storage_enabled": snapshot.storage_enabled,
                    "storage_queue_depth": snapshot.storage_queue_depth,
                    "storage_frames_dropped": snapshot.storage_frames_dropped,
                    "storage_last_error": snapshot.storage_last_error,
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
                    "gps_enabled": snapshot.gps_enabled,
                    "gps_connected": snapshot.gps_connected,
                    "gps_mode": snapshot.gps_mode,
                    "gps_port": snapshot.gps_port,
                    "gps_baudrate": snapshot.gps_baudrate,
                    "gps_last_timestamp": snapshot.gps_last_timestamp,
                    "gps_last_error": snapshot.gps_last_error,
                    "gps_fallback_active": snapshot.gps_fallback_active,
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
            if "gps_enabled" in payload:
                config_payload.setdefault("gps", {})["enabled"] = bool(payload["gps_enabled"])
            return {"status": "ok", "payload": self.update_config(config_payload)}

        raise ValueError(f"Unsupported control message type: {message_type}")

    def _series_for_monitor(
        self,
        snapshot: RuntimeSnapshot,
        *,
        mode: str,
        max_points: int,
    ) -> tuple[np.ndarray | None, float | None]:
        mode_name = mode if mode in {"raw", "unwrapped", "data1", "data2"} else "raw"
        if mode_name == "raw":
            return slice_for_plot(snapshot.latest_raw, max_points), snapshot.source_sample_rate
        if mode_name == "data1":
            return slice_for_plot(snapshot.latest_data1, max_points), snapshot.data1_rate
        if mode_name == "data2":
            return slice_for_plot(snapshot.latest_data2, max_points), snapshot.data2_rate
        return slice_for_plot(snapshot.latest_unwrapped, max_points), snapshot.source_sample_rate


def slice_for_plot(data: np.ndarray | None, max_points: int) -> np.ndarray | None:
    if data is None or data.size == 0:
        return data
    if data.shape[1] <= max_points:
        return data
    return data[:, -max_points:]


def format_packet_hex_dump(packet_bytes: bytes, max_bytes: int = MONITOR_PACKET_DUMP_LIMIT_BYTES) -> tuple[str, int]:
    visible = packet_bytes[:max_bytes]
    lines: list[str] = []
    for offset in range(0, len(visible), 16):
        chunk = visible[offset : offset + 16]
        hex_part = " ".join(f"{value:02X}" for value in chunk)
        ascii_part = "".join(chr(value) if 32 <= value <= 126 else "." for value in chunk)
        lines.append(f"{offset:06X}  {hex_part:<47}  {ascii_part}")
    return "\n".join(lines), max(len(packet_bytes) - len(visible), 0)
