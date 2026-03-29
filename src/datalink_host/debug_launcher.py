from __future__ import annotations

import argparse
import sys
from pathlib import Path

from PySide6 import QtWidgets

from datalink_host.core.config import AppSettings
from datalink_host.core.logging import configure_logging
from datalink_host.gui.main_window import MainWindow
from datalink_host.services.runtime import RuntimeService
from datalink_host.tools.receiver_sim import FakeDataLinkReceiver, ReceiverSettings
from datalink_host.tools.sender_sim import SenderSettings, SyntheticSender


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="One-command local debug launcher")
    parser.add_argument("--no-sender", action="store_true")
    parser.add_argument("--no-datalink-receiver", action="store_true")
    parser.add_argument("--sample-rate", type=float, default=1000.0)
    parser.add_argument("--packet-seconds", type=float, default=1.0)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    configure_logging(Path("./var/logs/datalink-host.log"))

    settings = AppSettings()
    settings.data_server.mode = "server"
    settings.data_server.host = "127.0.0.1"
    settings.data_server.remote_host = "127.0.0.1"
    settings.data_server.remote_port = settings.data_server.port
    settings.control_server.host = "127.0.0.1"
    settings.storage.enabled = True
    settings.storage.root = Path("./var/storage")
    settings.capture.enabled = True
    settings.capture.path = Path("./var/captures/session.dlhcap")
    settings.datalink.enabled = not args.no_datalink_receiver
    settings.datalink.host = "127.0.0.1"
    settings.datalink.port = 16000
    settings.datalink.ack_required = True

    runtime = RuntimeService(settings)
    receiver: FakeDataLinkReceiver | None = None
    sender: SyntheticSender | None = None

    if not args.no_datalink_receiver:
        receiver = FakeDataLinkReceiver(
            ReceiverSettings(
                host="127.0.0.1",
                port=settings.datalink.port,
                output_dir=Path("./var/datalink-dumps"),
            )
        )
        receiver.start()

    runtime.start()

    if not args.no_sender:
        sender = SyntheticSender(
            SenderSettings(
                host="127.0.0.1",
                port=settings.data_server.port,
                sample_rate=args.sample_rate,
                packet_seconds=args.packet_seconds,
                channels=settings.protocol.channels,
                protocol=settings.protocol,
            )
        )
        sender.start()

    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow(runtime, settings)
    window.setWindowTitle("DataLink Host Debug")
    window.show()
    exit_code = app.exec()

    if sender is not None:
        sender.stop()
    runtime.stop()
    if receiver is not None:
        receiver.stop()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
