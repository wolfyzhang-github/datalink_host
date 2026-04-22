from __future__ import annotations

import argparse
import logging
import math
import socket
import threading
import time
from dataclasses import dataclass, field

import numpy as np

from datalink_host.core.config import ProtocolSettings
from datalink_host.core.logging import configure_logging
from datalink_host.ingest.protocol import build_packet


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class SenderSettings:
    host: str = "127.0.0.1"
    port: int = 3677
    sample_rate: float = 1000.0
    packet_samples: int = 1000
    channels: int = 8
    reconnect_interval_seconds: float = 1.0
    protocol: ProtocolSettings = field(default_factory=ProtocolSettings)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Synthetic TCP sender for datalink-host")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3677)
    parser.add_argument("--sample-rate", type=float, default=1000.0)
    parser.add_argument("--packet-samples", type=int, default=None)
    parser.add_argument("--packet-seconds", type=float, default=1.0)
    parser.add_argument("--channels", type=int, default=8)
    parser.add_argument("--frame-header", default="11")
    parser.add_argument("--frame-header-size", type=int, choices=(2, 4, 8), default=2)
    parser.add_argument("--length-field-size", type=int, choices=(4, 8), default=8)
    parser.add_argument("--length-field-format", choices=("uint", "float64"), default="float64")
    parser.add_argument("--length-field-units", choices=("bytes", "values"), default="values")
    parser.add_argument("--byte-order", choices=("little", "big"), default="big")
    parser.add_argument("--channel-layout", choices=("interleaved", "channel-major"), default="interleaved")
    return parser


def resolve_packet_samples(
    sample_rate: float,
    packet_samples: int | None,
    packet_seconds: float | None,
) -> int:
    if packet_samples is not None:
        if packet_samples <= 0:
            raise ValueError("packet_samples must be positive")
        return packet_samples
    if packet_seconds is not None:
        if packet_seconds <= 0:
            raise ValueError("packet_seconds must be positive")
        return max(int(round(sample_rate * packet_seconds)), 1)
    if sample_rate <= 0:
        raise ValueError("sample_rate must be positive")
    return max(int(round(sample_rate)), 1)


def _generate_channels(channel_count: int, sample_rate: float, sample_count: int, phase: float) -> np.ndarray:
    t = np.arange(sample_count, dtype=np.float64) / sample_rate
    channels = []
    for index in range(channel_count):
        base_freq = 0.5 + index * 0.25
        signal = np.sin(2.0 * math.pi * base_freq * t + phase + index * 0.15)
        signal += 0.05 * np.sin(2.0 * math.pi * 10.0 * t)
        channels.append(signal)
    return np.stack(channels, axis=0)


class SyntheticSender:
    def __init__(self, settings: SenderSettings) -> None:
        if settings.sample_rate <= 0:
            raise ValueError("sample_rate must be positive")
        if settings.packet_samples <= 0:
            raise ValueError("packet_samples must be positive")
        self._settings = settings
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="synthetic-sender", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _run(self) -> None:
        protocol = self._settings.protocol
        phase = 0.0
        packet_interval_seconds = self._settings.packet_samples / self._settings.sample_rate
        while not self._stop_event.is_set():
            try:
                LOGGER.info("Connecting to %s:%s", self._settings.host, self._settings.port)
                with socket.create_connection((self._settings.host, self._settings.port), timeout=5.0) as sock:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    next_send_at = time.monotonic()
                    while not self._stop_event.is_set():
                        wait_seconds = max(next_send_at - time.monotonic(), 0.0)
                        if wait_seconds > 0 and self._stop_event.wait(wait_seconds):
                            break
                        channels = _generate_channels(
                            self._settings.channels,
                            self._settings.sample_rate,
                            self._settings.packet_samples,
                            phase,
                        )
                        packet = build_packet(self._settings.sample_rate, channels, protocol)
                        sock.sendall(packet)
                        phase += 0.25
                        next_send_at += packet_interval_seconds
                        now = time.monotonic()
                        if next_send_at < now - packet_interval_seconds:
                            next_send_at = now
            except OSError as exc:
                LOGGER.warning("Synthetic sender connection issue: %s", exc)
                if self._stop_event.wait(self._settings.reconnect_interval_seconds):
                    break


def main() -> int:
    args = build_parser().parse_args()
    configure_logging()
    sender = SyntheticSender(
        SenderSettings(
            host=args.host,
            port=args.port,
            sample_rate=args.sample_rate,
            packet_samples=resolve_packet_samples(args.sample_rate, args.packet_samples, args.packet_seconds),
            channels=args.channels,
            protocol=ProtocolSettings(
                frame_header=int(str(args.frame_header), 0),
                frame_header_size=args.frame_header_size,
                length_field_size=args.length_field_size,
                length_field_format=args.length_field_format,
                length_field_units=args.length_field_units,
                byte_order=args.byte_order,
                channels=args.channels,
                channel_layout=args.channel_layout,
            ),
        )
    )
    sender.start()
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        sender.stop()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
