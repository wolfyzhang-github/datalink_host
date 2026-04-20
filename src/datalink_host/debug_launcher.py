from __future__ import annotations

import argparse
import sys

from PySide6 import QtWidgets

from datalink_host.core.config import AppSettings
from datalink_host.core.frozen import patch_obspy_version_path_for_frozen
from datalink_host.core.logging import configure_logging
from datalink_host.core.paths import (
    default_capture_path,
    default_datalink_dump_root,
    default_storage_root,
    ensure_runtime_dirs,
    log_file_path,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="One-command local debug launcher")
    parser.add_argument("--no-sender", action="store_true")
    parser.add_argument("--no-datalink-receiver", action="store_true")
    parser.add_argument("--sample-rate", type=float, default=1000.0)
    parser.add_argument("--packet-seconds", type=float, default=1.0)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    patch_obspy_version_path_for_frozen()

    from datalink_host.gui.main_window import MainWindow
    from datalink_host.services.runtime import RuntimeService
    from datalink_host.services.web_api import WebApiService
    from datalink_host.tools.receiver_sim import FakeDataLinkReceiver, ReceiverSettings
    from datalink_host.tools.sender_sim import SenderSettings, SyntheticSender

    ensure_runtime_dirs()
    configure_logging(log_file_path())

    settings = AppSettings()
    settings.data_server.mode = "server"
    settings.data_server.host = "127.0.0.1"
    settings.data_server.remote_host = "127.0.0.1"
    settings.data_server.remote_port = settings.data_server.port
    settings.control_server.host = "127.0.0.1"
    settings.storage.enabled = True
    settings.storage.root = default_storage_root()
    settings.capture.enabled = True
    settings.capture.path = default_capture_path()
    settings.datalink.enabled = not args.no_datalink_receiver
    settings.datalink.host = "127.0.0.1"
    settings.datalink.port = 16000
    settings.datalink.ack_required = True

    runtime = RuntimeService(settings)
    web_api = WebApiService(runtime, settings.web)
    receiver: FakeDataLinkReceiver | None = None
    sender: SyntheticSender | None = None

    if not args.no_datalink_receiver:
        receiver = FakeDataLinkReceiver(
            ReceiverSettings(
                host="127.0.0.1",
                port=settings.datalink.port,
                output_dir=default_datalink_dump_root(),
            )
        )
        receiver.start()

    runtime.start()
    web_api.start()

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
    window.setWindowTitle("长基线光纤应变信号监控软件")
    window.show()
    exit_code = app.exec()

    if sender is not None:
        sender.stop()
    web_api.stop()
    runtime.stop()
    if receiver is not None:
        receiver.stop()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
