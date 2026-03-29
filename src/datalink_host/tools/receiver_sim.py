from __future__ import annotations

import argparse
import socket
import threading
from pathlib import Path
from dataclasses import dataclass

from datalink_host.core.logging import configure_logging
from datalink_host.transport.datalink import DataLinkPublisher


@dataclass(slots=True)
class ReceiverSettings:
    host: str = "127.0.0.1"
    port: int = 16000
    output_dir: Path = Path("./var/datalink-dumps")
    once: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fake DataLink receiver for local debugging")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=16000)
    parser.add_argument("--output-dir", default="./var/datalink-dumps")
    parser.add_argument("--once", action="store_true")
    return parser


def _encode(header: str, payload: bytes = b"") -> bytes:
    return DataLinkPublisher._encode_packet(header, payload)  # type: ignore[attr-defined]


class FakeDataLinkReceiver:
    def __init__(self, settings: ReceiverSettings) -> None:
        self._settings = settings
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._server: socket.socket | None = None
        self._packet_index = 0

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="fake-datalink-receiver", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._server is not None:
            try:
                self._server.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        output_dir = self._settings.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server = server
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self._settings.host, self._settings.port))
        server.listen(1)
        server.settimeout(1.0)
        print(f"Fake DataLink receiver listening on {self._settings.host}:{self._settings.port}")
        try:
            while not self._stop_event.is_set():
                try:
                    conn, addr = server.accept()
                except TimeoutError:
                    continue
                except OSError:
                    if self._stop_event.is_set():
                        break
                    raise
                print(f"Accepted connection from {addr[0]}:{addr[1]}")
                with conn:
                    header, _ = DataLinkPublisher._read_packet(conn)  # type: ignore[attr-defined]
                    print(f"<- {header}")
                    conn.sendall(_encode("ID DataLink 1.0 :: fake-receiver"))
                    while not self._stop_event.is_set():
                        try:
                            header, payload = DataLinkPublisher._read_packet(conn)  # type: ignore[attr-defined]
                        except Exception:
                            break
                        print(f"<- {header} ({len(payload)} bytes)")
                        if header.startswith("WRITE "):
                            (output_dir / f"packet-{self._packet_index:06d}.mseed").write_bytes(payload)
                            self._packet_index += 1
                            conn.sendall(_encode("OK 0 0"))
                        else:
                            conn.sendall(_encode("ERROR UNSUPPORTED 0"))
                if self._settings.once:
                    break
        finally:
            try:
                server.close()
            except OSError:
                pass
            self._server = None


def main() -> int:
    args = build_parser().parse_args()
    configure_logging()
    receiver = FakeDataLinkReceiver(
        ReceiverSettings(
            host=args.host,
            port=args.port,
            output_dir=Path(args.output_dir),
            once=args.once,
        )
    )
    receiver.start()
    try:
        while True:
            if args.once and not receiver.is_running():
                break
            threading.Event().wait(1.0)
    except KeyboardInterrupt:
        receiver.stop()
        return 0
    finally:
        receiver.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
