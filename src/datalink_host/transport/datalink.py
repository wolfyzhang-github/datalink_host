from __future__ import annotations

import io
import logging
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

    def update_settings(self, settings: DataLinkSettings, storage_settings: StorageSettings) -> None:
        with self._lock:
            host_changed = (settings.host, settings.port) != (self._settings.host, self._settings.port)
            self._settings = deepcopy(settings)
            self._storage_settings = deepcopy(storage_settings)
            if host_changed:
                self._close_socket_locked()

    def close(self) -> None:
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
        outputs: list[tuple[str, np.ndarray, float]] = []
        if frame.data1.size > 0:
            outputs.append(("data1", frame.data1, self._group_sample_rate(frame.data1, frame)))
        if self._settings.send_data2 and frame.data2.size > 0:
            outputs.append(("data2", frame.data2, self._group_sample_rate(frame.data2, frame)))
        if not outputs:
            return

        with self._lock:
            for _, channels, sample_rate in outputs:
                if sample_rate <= 0:
                    continue
                for channel_index in range(channels.shape[0]):
                    payload, stream_id = self._serialize_channel_packet(
                        channel_index=channel_index,
                        values=channels[channel_index],
                        sample_rate=sample_rate,
                        received_at=frame.received_at,
                    )
                    self._write_packet(stream_id, payload, frame.received_at, sample_rate, values_count=channels.shape[1])

    def _group_sample_rate(self, data: np.ndarray, frame: ProcessedFrame) -> float:
        if data.size == 0:
            return 0.0
        duration = frame.raw.shape[1] / max(frame.sample_rate, 1e-9)
        return data.shape[1] / max(duration, 1e-9)

    def _serialize_channel_packet(
        self,
        *,
        channel_index: int,
        values: np.ndarray,
        sample_rate: float,
        received_at: float,
    ) -> tuple[bytes, str]:
        start_time = UTCDateTime(received_at) - (values.size / max(sample_rate, 1e-9))
        trace = Trace(np.asarray(values, dtype=np.float64))
        trace.stats.network = self._storage_settings.network
        trace.stats.station = self._storage_settings.station
        trace.stats.location = self._storage_settings.location
        trace.stats.channel = self._storage_settings.channel_codes[channel_index]
        trace.stats.starttime = start_time
        trace.stats.sampling_rate = sample_rate

        buffer = io.BytesIO()
        Stream([trace]).write(buffer, format="MSEED")
        stream_id = self._settings.stream_id_template.format(
            network=self._storage_settings.network,
            station=self._storage_settings.station,
            location=self._storage_settings.location,
            channel=self._storage_settings.channel_codes[channel_index],
        )
        return buffer.getvalue(), stream_id

    def _write_packet(
        self,
        stream_id: str,
        payload: bytes,
        received_at: float,
        sample_rate: float,
        *,
        values_count: int,
    ) -> None:
        sock = self._ensure_connected_locked()
        start_time = int(round((received_at - (values_count / max(sample_rate, 1e-9))) * 1_000_000))
        end_time = int(round(received_at * 1_000_000))
        flag = "A" if self._settings.ack_required else "N"
        header = f"WRITE {stream_id} {start_time} {end_time} {flag} {len(payload)}"
        packet = self._encode_packet(header, payload)
        try:
            sock.sendall(packet)
            if self._settings.ack_required:
                response_header, _ = self._read_packet(sock)
                if not response_header.startswith("OK "):
                    raise RuntimeError(f"Unexpected DataLink response: {response_header}")
            self._stats.packets_sent += 1
            self._stats.bytes_sent += len(payload)
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
