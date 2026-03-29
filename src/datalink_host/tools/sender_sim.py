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
    packet_seconds: float = 1.0
    channels: int = 8
    reconnect_interval_seconds: float = 1.0
    protocol: ProtocolSettings = field(default_factory=ProtocolSettings)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Synthetic TCP sender for datalink-host")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3677)
    parser.add_argument("--sample-rate", type=float, default=1000.0)
    parser.add_argument("--packet-seconds", type=float, default=1.0)
    parser.add_argument("--channels", type=int, default=8)
    parser.add_argument("--frame-header", default="11")
    parser.add_argument("--frame-header-size", type=int, choices=(2, 4, 8), default=2)
    parser.add_argument("--length-field-size", type=int, choices=(4, 8), default=8)
    parser.add_argument("--length-field-units", choices=("bytes", "values"), default="bytes")
    parser.add_argument("--byte-order", choices=("little", "big"), default="little")
    parser.add_argument("--channel-layout", choices=("interleaved", "channel-major"), default="interleaved")
    return parser


def _generate_channels(channel_count: int, sample_rate: float, packet_seconds: float, phase: float) -> np.ndarray:
    sample_count = max(int(sample_rate * packet_seconds), 1)
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
        while not self._stop_event.is_set():
            try:
                LOGGER.info("Connecting to %s:%s", self._settings.host, self._settings.port)
                with socket.create_connection((self._settings.host, self._settings.port), timeout=5.0) as sock:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    while not self._stop_event.is_set():
                        channels = _generate_channels(
                            self._settings.channels,
                            self._settings.sample_rate,
                            self._settings.packet_seconds,
                            phase,
                        )
                        packet = build_packet(self._settings.sample_rate, channels, protocol)
                        sock.sendall(packet)
                        phase += 0.25
                        time.sleep(self._settings.packet_seconds)
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
            packet_seconds=args.packet_seconds,
            channels=args.channels,
            protocol=ProtocolSettings(
                frame_header=int(str(args.frame_header), 0),
                frame_header_size=args.frame_header_size,
                length_field_size=args.length_field_size,
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
