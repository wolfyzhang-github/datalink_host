from __future__ import annotations

import struct
from dataclasses import dataclass, field

import numpy as np

from datalink_host.core.config import ProtocolSettings
from datalink_host.models.messages import ChannelFrame, TcpPacket


def _byte_order_prefix(byte_order: str) -> str:
    return "<" if byte_order == "little" else ">"


def _unsigned_format(size: int) -> str:
    formats = {
        2: "H",
        4: "I",
        8: "Q",
    }
    try:
        return formats[size]
    except KeyError as exc:
        raise ValueError(f"Unsupported integer field size: {size}") from exc


def _header_struct(settings: ProtocolSettings) -> struct.Struct:
    prefix = _byte_order_prefix(settings.byte_order)
    frame_fmt = _unsigned_format(settings.frame_header_size)
    length_fmt = _unsigned_format(settings.length_field_size)
    return struct.Struct(f"{prefix}{frame_fmt}d{length_fmt}")


def _other_byte_order(byte_order: str) -> str:
    return "big" if byte_order == "little" else "little"


@dataclass(slots=True)
class PacketDecoder:
    settings: ProtocolSettings
    _buffer: bytearray = field(default_factory=bytearray)

    def pending_bytes(self) -> int:
        return len(self._buffer)

    def feed(self, chunk: bytes) -> list[TcpPacket]:
        self._buffer.extend(chunk)
        packets: list[TcpPacket] = []
        header_struct = _header_struct(self.settings)
        while len(self._buffer) >= header_struct.size:
            frame_header, sample_rate, payload_length = header_struct.unpack_from(self._buffer)
            if frame_header != self.settings.frame_header:
                message = (
                    f"Unexpected frame header {frame_header:#x}, expected {self.settings.frame_header:#x}"
                )
                other_header_struct = _header_struct(
                    ProtocolSettings(
                        frame_header=self.settings.frame_header,
                        frame_header_size=self.settings.frame_header_size,
                        length_field_size=self.settings.length_field_size,
                        length_field_units=self.settings.length_field_units,
                        byte_order=_other_byte_order(self.settings.byte_order),
                        channels=self.settings.channels,
                        channel_layout=self.settings.channel_layout,
                    )
                )
                swapped_frame_header = other_header_struct.unpack_from(self._buffer)[0]
                if swapped_frame_header == self.settings.frame_header:
                    message = (
                        f"{message}; possible byte_order mismatch "
                        f"(configured {self.settings.byte_order}, peer may be {_other_byte_order(self.settings.byte_order)})"
                    )
                raise ValueError(
                    message
                )
            payload_bytes = _payload_size_in_bytes(payload_length, self.settings)
            packet_size = header_struct.size + payload_bytes
            if len(self._buffer) < packet_size:
                break
            raw_bytes = bytes(self._buffer[:packet_size])
            payload = bytes(self._buffer[header_struct.size:packet_size])
            del self._buffer[:packet_size]
            packets.append(
                TcpPacket(
                    sample_rate=sample_rate,
                    payload_bytes=payload_bytes,
                    payload=payload,
                    raw_bytes=raw_bytes,
                )
            )
        return packets


def _payload_size_in_bytes(length_value: int, settings: ProtocolSettings) -> int:
    if settings.length_field_units == "bytes":
        payload_bytes = length_value
    elif settings.length_field_units == "values":
        payload_bytes = length_value * 8
    else:
        raise ValueError(f"Unsupported length field units: {settings.length_field_units}")
    if payload_bytes < 0:
        raise ValueError(f"Negative payload size: {payload_bytes}")
    return payload_bytes


def packet_to_frame(packet: TcpPacket, settings: ProtocolSettings) -> ChannelFrame:
    dtype = np.dtype(f"{_byte_order_prefix(settings.byte_order)}f8")
    values = np.frombuffer(packet.payload, dtype=dtype)
    if values.size % settings.channels != 0:
        raise ValueError(
            f"Payload size {values.size} is not divisible by channel count {settings.channels}"
        )

    if settings.channel_layout == "interleaved":
        channels = values.reshape((-1, settings.channels)).T
    elif settings.channel_layout == "channel-major":
        channels = values.reshape((settings.channels, -1))
    else:
        raise ValueError(f"Unsupported channel layout: {settings.channel_layout}")

    return ChannelFrame(sample_rate=packet.sample_rate, channels=channels.copy(), received_at=packet.received_at)


def build_packet(sample_rate: float, channels: np.ndarray, settings: ProtocolSettings) -> bytes:
    if channels.ndim != 2:
        raise ValueError("channels must be 2D with shape (channels, samples)")
    if channels.shape[0] != settings.channels:
        raise ValueError(f"Expected {settings.channels} channels, got {channels.shape[0]}")

    prefix = _byte_order_prefix(settings.byte_order)
    if settings.channel_layout == "interleaved":
        payload_values = channels.T.reshape(-1)
    elif settings.channel_layout == "channel-major":
        payload_values = channels.reshape(-1)
    else:
        raise ValueError(f"Unsupported channel layout: {settings.channel_layout}")

    payload = np.asarray(payload_values, dtype=np.float64).astype(np.dtype(f"{prefix}f8"), copy=False).tobytes()
    header_struct = _header_struct(settings)
    if settings.length_field_units == "bytes":
        payload_length = len(payload)
    elif settings.length_field_units == "values":
        payload_length = payload_values.size
    else:
        raise ValueError(f"Unsupported length field units: {settings.length_field_units}")
    header = header_struct.pack(settings.frame_header, sample_rate, payload_length)
    return header + payload
