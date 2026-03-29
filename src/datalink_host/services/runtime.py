from __future__ import annotations

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
from datalink_host.debug.capture import PacketCaptureWriter
from datalink_host.ingest.control_server import TcpControlServer
from datalink_host.ingest.data_server import TcpDataServer
from datalink_host.models.messages import ChannelFrame, ProcessedFrame, RuntimeSnapshot, TcpPacket
from datalink_host.processing.pipeline import ProcessingPipeline
from datalink_host.storage.miniseed import MiniSeedWriter
from datalink_host.transport.datalink import DataLinkPublisher, DataLinkStats


LOGGER = logging.getLogger(__name__)
QUEUE_BACKLOG_WARNING_DEPTH = 8


class RuntimeService:
    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings
        self._pipeline = ProcessingPipeline(settings.processing)
        self._storage = MiniSeedWriter(settings.storage)
        self._datalink = DataLinkPublisher(settings.datalink, settings.storage)
        self._capture = PacketCaptureWriter(settings.capture.path) if settings.capture.enabled else None
        self._queue: queue.Queue[ChannelFrame] = queue.Queue(maxsize=32)
        self._lock = threading.Lock()
        self._snapshot = RuntimeSnapshot(
            data_connected=False,
            control_connected=False,
            packets_received=0,
            bytes_received=0,
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
            capture_enabled=settings.capture.enabled,
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
        self._storage.close()
        self._datalink.close()
        if self._capture is not None:
            self._capture.close()

    def is_processing_active(self) -> bool:
        return self._data_server_active

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
        with self._lock:
            stats = self._datalink.stats()
            return replace(
                self._snapshot,
                datalink_connected=stats.connected,
                datalink_packets_sent=stats.packets_sent,
                datalink_bytes_sent=stats.bytes_sent,
                datalink_reconnects=stats.reconnects,
                datalink_last_error=stats.last_error,
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
                "capture": {
                    "enabled": self._settings.capture.enabled,
                    "path": str(self._settings.capture.path),
                },
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
            capture = payload.get("capture", {})
            restart_data_server = False

            if "data1_rate" in processing or "data2_rate" in processing:
                data1_rate = float(processing.get("data1_rate", self._settings.processing.data1_rate))
                data2_rate = float(processing.get("data2_rate", self._settings.processing.data2_rate))
                self._pipeline.update_rates(data1_rate, data2_rate)
                self._settings.processing.data1_rate = data1_rate
                self._settings.processing.data2_rate = data2_rate
                self._snapshot.data1_rate = data1_rate
                self._snapshot.data2_rate = data2_rate

            if "enable_phase_unwrap" in processing:
                self._settings.processing.enable_phase_unwrap = bool(processing["enable_phase_unwrap"])

            if protocol:
                if "frame_header" in protocol:
                    self._settings.protocol.frame_header = int(str(protocol["frame_header"]), 0)
                if "frame_header_size" in protocol:
                    self._settings.protocol.frame_header_size = int(protocol["frame_header_size"])
                if "length_field_size" in protocol:
                    self._settings.protocol.length_field_size = int(protocol["length_field_size"])
                if "length_field_units" in protocol:
                    self._settings.protocol.length_field_units = str(protocol["length_field_units"])
                if "byte_order" in protocol:
                    self._settings.protocol.byte_order = str(protocol["byte_order"])
                if "channel_layout" in protocol:
                    self._settings.protocol.channel_layout = str(protocol["channel_layout"])
                if "channels" in protocol:
                    channels = int(protocol["channels"])
                    if channels != self._settings.protocol.channels:
                        raise ValueError("Changing channel count at runtime is not supported yet")
                restart_data_server = True

            if data_server:
                if "mode" in data_server:
                    self._settings.data_server.mode = str(data_server["mode"])
                if "host" in data_server:
                    self._settings.data_server.host = str(data_server["host"])
                if "port" in data_server:
                    self._settings.data_server.port = int(data_server["port"])
                if "remote_host" in data_server:
                    self._settings.data_server.remote_host = str(data_server["remote_host"])
                if "remote_port" in data_server:
                    self._settings.data_server.remote_port = int(data_server["remote_port"])
                restart_data_server = True

            if storage:
                new_storage = deepcopy(self._settings.storage)
                if "enabled" in storage:
                    new_storage.enabled = bool(storage["enabled"])
                if "root" in storage:
                    new_storage.root = Path(storage["root"])
                if "file_duration_seconds" in storage:
                    new_storage.file_duration_seconds = int(storage["file_duration_seconds"])
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
                    self._settings.datalink.port = int(datalink["port"])
                if "stream_id_template" in datalink:
                    self._settings.datalink.stream_id_template = str(datalink["stream_id_template"])
                if "ack_required" in datalink:
                    self._settings.datalink.ack_required = bool(datalink["ack_required"])
                if "send_data2" in datalink:
                    self._settings.datalink.send_data2 = bool(datalink["send_data2"])
                self._snapshot.datalink_enabled = self._settings.datalink.enabled
                self._datalink.update_settings(self._settings.datalink, self._settings.storage)

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

    def _on_frame(self, frame: ChannelFrame) -> None:
        try:
            self._queue.put_nowait(frame)
        except queue.Full:
            self._set_error("Processing queue is full")
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

    def _run_processor(self) -> None:
        while not self._stop_event.is_set():
            try:
                frame = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                processed = self._pipeline.process(frame)
                self._fan_out(processed)
                self._frames_processed += 1
                if self._frames_processed == 1:
                    LOGGER.info(
                        "Processed first frame: raw_shape=%s, unwrapped_shape=%s, data1_shape=%s, data2_shape=%s",
                        processed.raw.shape,
                        processed.unwrapped.shape,
                        processed.data1.shape,
                        processed.data2.shape,
                    )
                with self._lock:
                    self._snapshot.latest_raw = processed.raw
                    self._snapshot.latest_unwrapped = processed.unwrapped
                    self._snapshot.latest_data1 = processed.data1
                    self._snapshot.latest_data2 = processed.data2
                    self._snapshot.queue_depth = self._queue.qsize()
                    self._snapshot.updated_at = time.time()
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Processing pipeline failed")
                self._set_error(f"Processing pipeline failed: {exc}")

    def _fan_out(self, frame: ProcessedFrame) -> None:
        if self._settings.storage.enabled:
            self._storage.write(frame)
        if self._settings.datalink.enabled:
            try:
                self._datalink.publish(frame)
            except Exception as exc:  # noqa: BLE001
                self._set_error(f"DataLink publish failed: {exc}")

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
                    "queue_depth": snapshot.queue_depth,
                    "source_sample_rate": snapshot.source_sample_rate,
                    "data1_rate": snapshot.data1_rate,
                    "data2_rate": snapshot.data2_rate,
                    "storage_enabled": snapshot.storage_enabled,
                    "datalink_enabled": snapshot.datalink_enabled,
                    "datalink_connected": snapshot.datalink_connected,
                    "datalink_packets_sent": snapshot.datalink_packets_sent,
                    "datalink_bytes_sent": snapshot.datalink_bytes_sent,
                    "datalink_reconnects": snapshot.datalink_reconnects,
                    "datalink_last_error": snapshot.datalink_last_error,
                    "capture_enabled": snapshot.capture_enabled,
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
            return {"status": "ok", "payload": self.update_config(config_payload)}

        raise ValueError(f"Unsupported control message type: {message_type}")


def slice_for_plot(data: np.ndarray | None, max_points: int) -> np.ndarray | None:
    if data is None or data.size == 0:
        return data
    if data.shape[1] <= max_points:
        return data
    return data[:, -max_points:]
