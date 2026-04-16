from __future__ import annotations

import sys
from pathlib import Path

from PySide6 import QtWidgets

from datalink_host.core.config import AppSettings
from datalink_host.core.logging import configure_logging
from datalink_host.gui.main_window import MainWindow
from datalink_host.services.runtime import RuntimeService
from datalink_host.services.web_api import WebApiService


def main() -> int:
    configure_logging(Path("./var/logs/datalink-host.log"))
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
