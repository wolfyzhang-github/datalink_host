from __future__ import annotations

import io
import logging
import queue
import socket
import threading
import time
from copy import deepcopy
from dataclasses import dataclass

import numpy as np
from obspy import Stream, Trace, UTCDateTime

from datalink_host.core.config import DataLinkSettings, StorageSettings

from datalink_host.models.messages import ProcessedFrame


LOGGER = logging.getLogger(__name__)
PUBLISH_QUEUE_MAXSIZE = 512
MINISEED_RECORD_LENGTH = 512


@dataclass(slots=True)
class PendingDataLinkPacket:
    stream_id: str
    payload: bytes
    start_time: float
    end_time: float
    ack_required: bool


@dataclass(slots=True)
class DataLinkStats:
    connected: bool = False
    packets_sent: int = 0
    bytes_sent: int = 0
    reconnects: int = 0
    last_error: str | None = None
    last_send_at: float | None = None


class DataLinkPublisher:
    def __init__(self, settings: DataLinkSettings, storage_settings: StorageSettings) -> None:
        self._settings = deepcopy(settings)
        self._storage_settings = deepcopy(storage_settings)
        self._socket: socket.socket | None = None
        self._lock = threading.Lock()
        self._stats = DataLinkStats()
        self._next_start_by_stream: dict[str, float] = {}
        self._warned_group_suffix = False
        self._send_queue: queue.Queue[PendingDataLinkPacket | None] = queue.Queue(maxsize=PUBLISH_QUEUE_MAXSIZE)
        self._stop_event = threading.Event()
        self._worker = threading.Thread(target=self._run_sender, name="datalink-publisher", daemon=True)
        self._worker.start()

    def update_settings(self, settings: DataLinkSettings, storage_settings: StorageSettings) -> None:
        with self._lock:
            host_changed = (settings.host, settings.port) != (self._settings.host, self._settings.port)
            self._settings = deepcopy(settings)
            self._storage_settings = deepcopy(storage_settings)
            if host_changed or not settings.enabled:
                self._close_socket_locked()
        if not settings.enabled:
            self._clear_queue()

    def close(self) -> None:
        self._stop_event.set()
        self._enqueue_control(None)
        if self._worker.is_alive():
            self._worker.join(timeout=2.0)
        with self._lock:
            self._close_socket_locked()

    def stats(self) -> DataLinkStats:
        with self._lock:
            return DataLinkStats(
                connected=self._stats.connected,
                packets_sent=self._stats.packets_sent,
                bytes_sent=self._stats.bytes_sent,
                reconnects=self._stats.reconnects,
                last_error=self._stats.last_error,
                last_send_at=self._stats.last_send_at,
            )

    def publish(self, frame: ProcessedFrame) -> None:
        with self._lock:
            settings = deepcopy(self._settings)
            storage_settings = deepcopy(self._storage_settings)
        outputs: list[tuple[str, np.ndarray, float]] = []
        if frame.data1.size > 0:
            outputs.append(("data1", frame.data1, self._group_sample_rate(frame.data1, frame)))
        if settings.send_data2 and frame.data2.size > 0:
            outputs.append(("data2", frame.data2, self._group_sample_rate(frame.data2, frame)))
        if not outputs:
            return

        for group_name, channels, sample_rate in outputs:
            if sample_rate <= 0:
                continue
            for channel_index in range(channels.shape[0]):
                packets = self._serialize_channel_packets(
                    group_name=group_name,
                    channel_index=channel_index,
                    values=channels[channel_index],
                    sample_rate=sample_rate,
                    received_at=frame.received_at,
                    settings=settings,
                    storage_settings=storage_settings,
                )
                for payload, stream_id, start_time, end_time in packets:
                    self._enqueue_packet(
                        PendingDataLinkPacket(
                            stream_id=stream_id,
                            payload=payload,
                            start_time=start_time,
                            end_time=end_time,
                            ack_required=settings.ack_required,
                        )
                    )

    def _group_sample_rate(self, data: np.ndarray, frame: ProcessedFrame) -> float:
        if data.size == 0:
            return 0.0
        duration = frame.raw.shape[1] / max(frame.sample_rate, 1e-9)
        return data.shape[1] / max(duration, 1e-9)

    def _serialize_channel_packets(
        self,
        *,
        group_name: str,
        channel_index: int,
        values: np.ndarray,
        sample_rate: float,
        received_at: float,
        settings: DataLinkSettings | None = None,
        storage_settings: StorageSettings | None = None,
    ) -> list[tuple[bytes, str, float, float]]:
        if settings is None or storage_settings is None:
            with self._lock:
                settings = deepcopy(self._settings)
                storage_settings = deepcopy(self._storage_settings)
        stream_id = self._stream_id_for(
            group_name=group_name,
            channel_index=channel_index,
            settings=settings,
            storage_settings=storage_settings,
        )
        start_time = self._assign_stream_time_window(
            stream_id=stream_id,
            sample_rate=sample_rate,
            values_count=values.size,
            received_at=received_at,
        )
        return self._serialize_channel_segment(
            stream_id=stream_id,
            channel_index=channel_index,
            values=np.asarray(values, dtype=np.float64),
            sample_rate=sample_rate,
            start_time=start_time,
            settings=settings,
            storage_settings=storage_settings,
        )

    def _serialize_channel_segment(
        self,
        *,
        stream_id: str,
        channel_index: int,
        values: np.ndarray,
        sample_rate: float,
        start_time: float,
        settings: DataLinkSettings,
        storage_settings: StorageSettings,
    ) -> list[tuple[bytes, str, float, float]]:
        if values.size == 0:
            return []

        payload = self._encode_miniseed_record(
            channel_index=channel_index,
            values=values,
            sample_rate=sample_rate,
            start_time=start_time,
            storage_settings=storage_settings,
        )
        end_time = start_time + (values.size / max(sample_rate, 1e-9))

        if len(payload) == MINISEED_RECORD_LENGTH:
            return [(payload, stream_id, start_time, end_time)]
        if len(payload) < MINISEED_RECORD_LENGTH:
            raise ValueError(f"MiniSEED payload shorter than {MINISEED_RECORD_LENGTH} bytes: {len(payload)}")
        if values.size <= 1:
            raise ValueError(
                f"Single-sample MiniSEED payload exceeds {MINISEED_RECORD_LENGTH} bytes: {len(payload)}"
            )

        midpoint = values.size // 2
        split_time = start_time + (midpoint / max(sample_rate, 1e-9))
        return self._serialize_channel_segment(
            stream_id=stream_id,
            channel_index=channel_index,
            values=values[:midpoint],
            sample_rate=sample_rate,
            start_time=start_time,
            settings=settings,
            storage_settings=storage_settings,
        ) + self._serialize_channel_segment(
            stream_id=stream_id,
            channel_index=channel_index,
            values=values[midpoint:],
            sample_rate=sample_rate,
            start_time=split_time,
            settings=settings,
            storage_settings=storage_settings,
        )

    def _encode_miniseed_record(
        self,
        *,
        channel_index: int,
        values: np.ndarray,
        sample_rate: float,
        start_time: float,
        storage_settings: StorageSettings,
    ) -> bytes:
        trace = Trace(np.asarray(values, dtype=np.float64))
        trace.stats.network = storage_settings.network
        trace.stats.station = storage_settings.station
        trace.stats.location = storage_settings.location
        trace.stats.channel = storage_settings.channel_codes[channel_index]
        trace.stats.starttime = UTCDateTime(start_time)
        trace.stats.sampling_rate = sample_rate

        buffer = io.BytesIO()
        Stream([trace]).write(
            buffer,
            format="MSEED",
            reclen=MINISEED_RECORD_LENGTH,
            encoding="FLOAT64",
            flush=True,
        )
        return buffer.getvalue()

    def _stream_id_for(
        self,
        *,
        group_name: str,
        channel_index: int,
        settings: DataLinkSettings,
        storage_settings: StorageSettings,
    ) -> str:
        stream_id = settings.stream_id_template.format(
            network=storage_settings.network,
            station=storage_settings.station,
            location=storage_settings.location,
            channel=storage_settings.channel_codes[channel_index],
            group=group_name,
        )
        if settings.send_data2 and "{group}" not in settings.stream_id_template:
            if stream_id.endswith("/MSEED"):
                stream_id = f"{stream_id[:-6]}_{group_name}/MSEED"
            else:
                stream_id = f"{stream_id}_{group_name}"
            if not self._warned_group_suffix:
                LOGGER.warning(
                    "DataLink stream_id_template does not include {group}; appending group suffix automatically"
                )
                self._warned_group_suffix = True
        return stream_id

    def _assign_stream_time_window(
        self,
        *,
        stream_id: str,
        sample_rate: float,
        values_count: int,
        received_at: float,
    ) -> float:
        duration = values_count / max(sample_rate, 1e-9)
        fallback_start = received_at - duration
        # Received-at timestamps jitter with network delivery; keep per-stream continuity when possible.
        with self._lock:
            next_start = self._next_start_by_stream.get(stream_id)
            if next_start is None or abs(fallback_start - next_start) > max(duration * 2, 1.0):
                start_time = fallback_start
            else:
                start_time = next_start
            self._next_start_by_stream[stream_id] = start_time + duration
        return start_time

    def _write_packet_locked(self, item: PendingDataLinkPacket) -> None:
        sock = self._ensure_connected_locked()
        start_time = int(round(item.start_time * 1_000_000))
        end_time = int(round(item.end_time * 1_000_000))
        flag = "A" if item.ack_required else "N"
        header = f"WRITE {item.stream_id} {start_time} {end_time} {flag} {len(item.payload)}"
        packet = self._encode_packet(header, item.payload)
        try:
            sock.sendall(packet)
            if item.ack_required:
                response_header, _ = self._read_packet(sock)
                if not response_header.startswith("OK "):
                    raise RuntimeError(f"Unexpected DataLink response: {response_header}")
            self._stats.packets_sent += 1
            self._stats.bytes_sent += len(item.payload)
            self._stats.last_send_at = time.time()
            self._stats.last_error = None
        except Exception as exc:  # noqa: BLE001
            self._stats.last_error = str(exc)
            self._close_socket_locked()
            raise

    def _ensure_connected_locked(self) -> socket.socket:
        if self._socket is not None:
            return self._socket
        sock = socket.create_connection(
            (self._settings.host, self._settings.port),
            timeout=self._settings.socket_timeout_seconds,
        )
        sock.settimeout(self._settings.socket_timeout_seconds)
        sock.sendall(self._encode_packet(f"ID {self._settings.client_id}"))
        response_header, _ = self._read_packet(sock)
        if not response_header.startswith("ID DataLink"):
            sock.close()
            raise RuntimeError(f"Unexpected DataLink server identification: {response_header}")
        self._socket = sock
        self._stats.connected = True
        self._stats.reconnects += 1
        LOGGER.info("Connected to DataLink server %s:%s", self._settings.host, self._settings.port)
        return sock

    def _close_socket_locked(self) -> None:
        if self._socket is not None:
            try:
                self._socket.close()
            except OSError:
                pass
        self._socket = None
        self._stats.connected = False

    def _enqueue_packet(self, item: PendingDataLinkPacket) -> None:
        try:
            self._send_queue.put_nowait(item)
        except queue.Full:
            with self._lock:
                self._stats.last_error = "DataLink send queue is full"
            LOGGER.warning("DataLink send queue is full; dropping outbound packet for %s", item.stream_id)

    def _enqueue_control(self, item: PendingDataLinkPacket | None) -> None:
        while True:
            try:
                self._send_queue.put_nowait(item)
                return
            except queue.Full:
                try:
                    self._send_queue.get_nowait()
                except queue.Empty:
                    return

    def _clear_queue(self) -> None:
        while True:
            try:
                self._send_queue.get_nowait()
            except queue.Empty:
                return

    def _retry_delay_seconds(self) -> float:
        with self._lock:
            if not self._settings.enabled:
                return -1.0
            return max(self._settings.reconnect_interval_seconds, 0.0)

    def _run_sender(self) -> None:
        current_item: PendingDataLinkPacket | None = None
        while True:
            if current_item is None:
                try:
                    item = self._send_queue.get(timeout=0.5)
                except queue.Empty:
                    if self._stop_event.is_set():
                        break
                    continue
                if item is None:
                    if self._stop_event.is_set():
                        break
                    continue
                current_item = item
            try:
                with self._lock:
                    self._write_packet_locked(current_item)
                current_item = None
            except Exception as exc:  # noqa: BLE001
                with self._lock:
                    self._stats.last_error = str(exc)
                LOGGER.error("DataLink publish failed: %s", exc)
                retry_delay = self._retry_delay_seconds()
                if retry_delay < 0:
                    current_item = None
                    continue
                LOGGER.warning(
                    "Retrying DataLink packet for %s in %.3fs",
                    current_item.stream_id,
                    retry_delay,
                )
                if self._stop_event.wait(retry_delay):
                    break

    @staticmethod
    def _encode_packet(header: str, data: bytes = b"") -> bytes:
        header_bytes = header.encode("ascii")
        if len(header_bytes) > 255:
            raise ValueError("DataLink header exceeds 255 bytes")
        return b"DL" + bytes((len(header_bytes),)) + header_bytes + data

    @staticmethod
    def _read_exact(sock: socket.socket, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = sock.recv(size - len(chunks))
            if not chunk:
                raise ConnectionError("DataLink socket closed")
            chunks.extend(chunk)
        return bytes(chunks)

    @classmethod
    def _read_packet(cls, sock: socket.socket) -> tuple[str, bytes]:
        preheader = cls._read_exact(sock, 3)
        if preheader[:2] != b"DL":
            raise RuntimeError(f"Invalid DataLink preheader: {preheader!r}")
        header = cls._read_exact(sock, preheader[2]).decode("ascii")
        data_size = cls._extract_data_size(header)
        payload = cls._read_exact(sock, data_size) if data_size > 0 else b""
        return header, payload

    @staticmethod
    def _extract_data_size(header: str) -> int:
        parts = header.split()
        if not parts:
            return 0
        if parts[0] == "WRITE" and len(parts) >= 6:
            return int(parts[-1])
        if parts[0] in {"OK", "ERROR"} and len(parts) >= 3:
            return int(parts[2])
        if parts[0] in {"INFO", "PACKET"} and parts:
            return int(parts[-1])
        return 0
