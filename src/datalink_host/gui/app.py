from __future__ import annotations

import argparse
import sys
from pathlib import Path

from datalink_host.core.config import AppSettings
from datalink_host.core.frozen import patch_obspy_version_path_for_frozen
from datalink_host.core.logging import configure_logging
from datalink_host.core.paths import ensure_runtime_dirs, log_file_path
from datalink_host.self_check import run_self_check


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="datalink-host GUI")
    parser.add_argument(
        "--self-check",
        action="store_true",
        help="Run a frozen-package smoke test and exit.",
    )
    parser.add_argument(
        "--self-check-output",
        type=Path,
        help="Optional path to write a JSON smoke test report.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    patch_obspy_version_path_for_frozen()
    if args.self_check:
        return run_self_check(args.self_check_output)

    from PySide6 import QtWidgets

    from datalink_host.gui.main_window import MainWindow
    from datalink_host.services.runtime import RuntimeService
    from datalink_host.services.web_api import WebApiService

    ensure_runtime_dirs()
    configure_logging(log_file_path())
    settings = AppSettings()
    runtime = RuntimeService(settings)
    web_api = WebApiService(runtime, settings.web)
    runtime.start()
    web_api.start()

    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow(runtime, settings)
    window.show()
    exit_code = app.exec()
    web_api.stop()
    runtime.stop()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
