from __future__ import annotations

import io
import logging
import queue
import re
import socket
import threading
import time
from copy import deepcopy
from dataclasses import dataclass, field

import numpy as np
from obspy import Stream, Trace, UTCDateTime

from datalink_host.core.clock import wall_time
from datalink_host.core.config import DataLinkSettings, StorageSettings
from datalink_host.core.output_encoding import encode_samples_for_miniseed
from datalink_host.models.messages import ProcessedFrame


LOGGER = logging.getLogger(__name__)
PUBLISH_QUEUE_MAXSIZE = 512
SEND_QUEUE_BACKLOG_WARNING_DEPTH = 64
PUBLISH_SERIALIZATION_WARNING_MS = 100.0
SEND_QUEUE_WAIT_WARNING_MS = 500.0
SEND_PACKET_WARNING_MS = 200.0
DEFAULT_DATALINK_PACKET_SIZE = 512
MIN_MSEED_RECORD_LENGTH = 256
MAX_MSEED_RECORD_LENGTH = 4096
MAX_MSEED_SEQUENCE_NUMBER = 999999
DATALINK_PUBLISH_CHANNELS = 6
PACKETSIZE_PATTERN = re.compile(r"PACKETSIZE(?:\s*[:=]\s*|\s+)(\d+)")


class DataLinkSendError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.retryable = retryable


@dataclass(slots=True)
class PendingDataLinkPacket:
    stream_id: str
    payload: bytes
    start_time: float
    end_time: float
    ack_required: bool
    enqueued_at_monotonic: float = field(default_factory=time.monotonic)


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
        self._mseed_sequence_lock = threading.Lock()
        self._next_mseed_sequence_number = 1
        self._stats = DataLinkStats()
        self._warned_group_suffix = False
        self._server_packet_size_bytes: int | None = None
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
                if host_changed:
                    self._server_packet_size_bytes = None
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
        publish_started_at = time.monotonic()
        with self._lock:
            settings = deepcopy(self._settings)
            storage_settings = deepcopy(self._storage_settings)
            max_payload_bytes = self._server_packet_size_bytes or DEFAULT_DATALINK_PACKET_SIZE
        outputs: list[tuple[str, np.ndarray, float]] = []
        if frame.data1.size > 0:
            outputs.append(("data1", frame.data1, frame.data1_sample_rate))
        if settings.send_data2 and frame.data2.size > 0:
            outputs.append(("data2", frame.data2, frame.data2_sample_rate))
        if not outputs:
            return

        packet_count = 0
        payload_bytes = 0
        for group_name, channels, sample_rate in outputs:
            if sample_rate <= 0:
                continue
            publish_channels = min(channels.shape[0], DATALINK_PUBLISH_CHANNELS)
            for channel_index in range(publish_channels):
                packets = self._serialize_channel_packets(
                    group_name=group_name,
                    channel_index=channel_index,
                    values=channels[channel_index],
                    sample_rate=sample_rate,
                    timestamp_us=frame.timestamp_us,
                    settings=settings,
                    storage_settings=storage_settings,
                    max_payload_bytes=max_payload_bytes,
                )
                for payload, stream_id, start_time, end_time in packets:
                    packet_count += 1
                    payload_bytes += len(payload)
                    self._enqueue_packet(
                        PendingDataLinkPacket(
                            stream_id=stream_id,
                            payload=payload,
                            start_time=start_time,
                            end_time=end_time,
                            ack_required=settings.ack_required,
                        )
                    )
        serialization_ms = (time.monotonic() - publish_started_at) * 1000.0
        send_queue_depth = self._send_queue.qsize()
        if (
            serialization_ms >= PUBLISH_SERIALIZATION_WARNING_MS
            or send_queue_depth >= SEND_QUEUE_BACKLOG_WARNING_DEPTH
        ):
            LOGGER.warning(
                "DataLink publish is lagging: serialization_ms=%.1f, send_queue_depth=%s, packets=%s, "
                "payload_bytes=%s, data1_shape=%s, data2_shape=%s, ack_required=%s",
                serialization_ms,
                send_queue_depth,
                packet_count,
                payload_bytes,
                frame.data1.shape,
                frame.data2.shape,
                settings.ack_required,
            )

    def _serialize_channel_packets(
        self,
        *,
        group_name: str,
        channel_index: int,
        values: np.ndarray,
        sample_rate: float,
        timestamp_us: int | None,
        settings: DataLinkSettings | None = None,
        storage_settings: StorageSettings | None = None,
        max_payload_bytes: int | None = None,
    ) -> list[tuple[bytes, str, float, float]]:
        if settings is None or storage_settings is None:
            with self._lock:
                settings = deepcopy(self._settings)
                storage_settings = deepcopy(self._storage_settings)
                max_payload_bytes = max_payload_bytes or self._server_packet_size_bytes or DEFAULT_DATALINK_PACKET_SIZE
        elif max_payload_bytes is None:
            with self._lock:
                max_payload_bytes = self._server_packet_size_bytes or DEFAULT_DATALINK_PACKET_SIZE
        stream_id = self._stream_id_for(
            group_name=group_name,
            channel_index=channel_index,
            settings=settings,
            storage_settings=storage_settings,
        )
        values = np.asarray(values)
        if values.size == 0:
            return []
        if timestamp_us is None:
            LOGGER.warning("Skipping DataLink publish because frame timestamp is unavailable")
            return []
        start_time = timestamp_us / 1_000_000.0
        packets = self._encode_miniseed_packets(
            channel_index=channel_index,
            values=values,
            sample_rate=sample_rate,
            start_time=start_time,
            storage_settings=storage_settings,
            max_payload_bytes=max_payload_bytes,
        )
        return [(payload, stream_id, packet_start, packet_end) for payload, packet_start, packet_end in packets]

    def _encode_miniseed_record(
        self,
        *,
        channel_index: int,
        values: np.ndarray,
        sample_rate: float,
        start_time: float,
        storage_settings: StorageSettings,
        record_length_bytes: int,
        sequence_number: int | None = None,
    ) -> bytes:
        encoded = encode_samples_for_miniseed(
            values,
            output_data_type=storage_settings.output_data_type,
            int32_gain=storage_settings.int32_gain,
        )
        trace = Trace(encoded.values)
        trace.stats.network = storage_settings.network
        trace.stats.station = storage_settings.station
        trace.stats.location = storage_settings.location
        trace.stats.channel = storage_settings.channel_codes[channel_index]
        trace.stats.starttime = UTCDateTime(start_time)
        trace.stats.sampling_rate = sample_rate

        buffer = io.BytesIO()
        write_kwargs: dict[str, object] = {
            "format": "MSEED",
            "encoding": encoded.miniseed_encoding,
            "reclen": record_length_bytes,
        }
        if sequence_number is not None:
            write_kwargs["sequence_number"] = sequence_number
        Stream([trace]).write(buffer, **write_kwargs)
        return buffer.getvalue()

    def _encode_miniseed_packets(
        self,
        *,
        channel_index: int,
        values: np.ndarray,
        sample_rate: float,
        start_time: float,
        storage_settings: StorageSettings,
        max_payload_bytes: int,
    ) -> list[tuple[bytes, float, float]]:
        record_length_bytes = self._preferred_record_length_bytes(max_payload_bytes)
        payload = self._encode_miniseed_record(
            channel_index=channel_index,
            values=values,
            sample_rate=sample_rate,
            start_time=start_time,
            storage_settings=storage_settings,
            record_length_bytes=record_length_bytes,
        )
        end_time = start_time + (values.size / max(sample_rate, 1e-9))
        if len(payload) <= max_payload_bytes:
            record_count = self._count_mseed_records(payload, record_length_bytes)
            sequence_number = self._reserve_mseed_sequence_numbers(record_count)
            if sequence_number != 1:
                payload = self._encode_miniseed_record(
                    channel_index=channel_index,
                    values=values,
                    sample_rate=sample_rate,
                    start_time=start_time,
                    storage_settings=storage_settings,
                    record_length_bytes=record_length_bytes,
                    sequence_number=sequence_number,
                )
            return [(payload, start_time, end_time)]
        if values.size <= 1:
            raise DataLinkSendError(
                f"MiniSEED payload exceeds DataLink packet limit: payload_bytes={len(payload)}, "
                f"limit_bytes={max_payload_bytes}",
                retryable=False,
            )
        split_index = max(1, values.size // 2)
        first_values = values[:split_index]
        second_values = values[split_index:]
        second_start_time = start_time + (split_index / max(sample_rate, 1e-9))
        return [
            *self._encode_miniseed_packets(
                channel_index=channel_index,
                values=first_values,
                sample_rate=sample_rate,
                start_time=start_time,
                storage_settings=storage_settings,
                max_payload_bytes=max_payload_bytes,
            ),
            *self._encode_miniseed_packets(
                channel_index=channel_index,
                values=second_values,
                sample_rate=sample_rate,
                start_time=second_start_time,
                storage_settings=storage_settings,
                max_payload_bytes=max_payload_bytes,
            ),
        ]

    @staticmethod
    def _preferred_record_length_bytes(max_payload_bytes: int) -> int:
        record_length = MIN_MSEED_RECORD_LENGTH
        capped_limit = max(MIN_MSEED_RECORD_LENGTH, min(max_payload_bytes, MAX_MSEED_RECORD_LENGTH))
        while record_length * 2 <= capped_limit:
            record_length *= 2
        return record_length

    @staticmethod
    def _count_mseed_records(payload: bytes, record_length_bytes: int) -> int:
        if len(payload) % record_length_bytes != 0:
            raise RuntimeError(
                "MiniSEED payload size does not align with record length: "
                f"payload_bytes={len(payload)}, record_length_bytes={record_length_bytes}"
            )
        return len(payload) // record_length_bytes

    def _reserve_mseed_sequence_numbers(self, record_count: int) -> int:
        with self._mseed_sequence_lock:
            sequence_number = self._next_mseed_sequence_number
            self._next_mseed_sequence_number = self._advance_mseed_sequence_number(
                sequence_number,
                record_count,
            )
        return sequence_number

    def _assign_mseed_sequence_numbers(self, payload: bytes, record_length_bytes: int) -> bytes:
        record_count = self._count_mseed_records(payload, record_length_bytes)
        with self._mseed_sequence_lock:
            sequence_number = self._next_mseed_sequence_number
            self._next_mseed_sequence_number = self._advance_mseed_sequence_number(
                sequence_number,
                record_count,
            )
        numbered_payload = bytearray(payload)
        for record_index in range(record_count):
            record_sequence_number = self._advance_mseed_sequence_number(sequence_number, record_index)
            record_offset = record_index * record_length_bytes
            numbered_payload[record_offset : record_offset + 6] = f"{record_sequence_number:06d}".encode("ascii")
        return bytes(numbered_payload)

    @staticmethod
    def _advance_mseed_sequence_number(sequence_number: int, record_count: int) -> int:
        return ((sequence_number - 1 + record_count) % MAX_MSEED_SEQUENCE_NUMBER) + 1

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
                response_header, response_payload = self._read_packet(sock)
                if not response_header.startswith("OK "):
                    raise DataLinkSendError(
                        f"Unexpected DataLink response: "
                        f"{self._format_status_response(response_header, response_payload)}",
                        retryable=False,
                    )
            self._stats.packets_sent += 1
            self._stats.bytes_sent += len(item.payload)
            self._stats.last_send_at = wall_time()
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
            raise DataLinkSendError(
                f"Unexpected DataLink server identification: {response_header}",
                retryable=False,
            )
        self._server_packet_size_bytes = self._parse_packet_size(response_header)
        self._socket = sock
        self._stats.connected = True
        self._stats.reconnects += 1
        if self._server_packet_size_bytes is None:
            LOGGER.info("Connected to DataLink server %s:%s", self._settings.host, self._settings.port)
        else:
            LOGGER.info(
                "Connected to DataLink server %s:%s (PACKETSIZE=%s bytes)",
                self._settings.host,
                self._settings.port,
                self._server_packet_size_bytes,
            )
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
            queue_depth = self._send_queue.qsize()
            if (
                queue_depth >= SEND_QUEUE_BACKLOG_WARNING_DEPTH
                and queue_depth % SEND_QUEUE_BACKLOG_WARNING_DEPTH == 0
            ):
                LOGGER.warning(
                    "DataLink send queue backlog is %s packets; outbound traffic is slower than publish",
                    queue_depth,
                )
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
            send_started_at = time.monotonic()
            queue_wait_ms = (send_started_at - current_item.enqueued_at_monotonic) * 1000.0
            try:
                with self._lock:
                    self._write_packet_locked(current_item)
                send_ms = (time.monotonic() - send_started_at) * 1000.0
                queue_depth_after = self._send_queue.qsize()
                if (
                    queue_wait_ms >= SEND_QUEUE_WAIT_WARNING_MS
                    or send_ms >= SEND_PACKET_WARNING_MS
                    or queue_depth_after >= SEND_QUEUE_BACKLOG_WARNING_DEPTH
                ):
                    LOGGER.warning(
                        "DataLink sender is lagging: stream_id=%s, queue_wait_ms=%.1f, send_ms=%.1f, "
                        "queue_depth_after=%s, payload_bytes=%s, ack_required=%s",
                        current_item.stream_id,
                        queue_wait_ms,
                        send_ms,
                        queue_depth_after,
                        len(current_item.payload),
                        current_item.ack_required,
                    )
                current_item = None
            except Exception as exc:  # noqa: BLE001
                with self._lock:
                    self._stats.last_error = str(exc)
                LOGGER.error("DataLink publish failed: %s", exc)
                retryable = getattr(exc, "retryable", isinstance(exc, OSError))
                if not retryable:
                    LOGGER.warning(
                        "Dropping DataLink packet for %s after permanent error",
                        current_item.stream_id,
                    )
                    current_item = None
                    continue
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

    @staticmethod
    def _format_status_response(header: str, payload: bytes) -> str:
        if not payload:
            return header
        message = payload.decode("utf-8", errors="replace").strip()
        return f"{header} | {message}" if message else header

    @staticmethod
    def _parse_packet_size(response_header: str) -> int | None:
        match = PACKETSIZE_PATTERN.search(response_header)
        if match is None:
            return None
        packet_size = int(match.group(1))
        return packet_size if packet_size > 0 else None
