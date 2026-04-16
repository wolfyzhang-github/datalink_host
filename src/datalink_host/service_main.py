from __future__ import annotations

import signal
import threading
import time
from pathlib import Path

from datalink_host.core.config import AppSettings
from datalink_host.core.logging import configure_logging
from datalink_host.services.runtime import RuntimeService
from datalink_host.services.web_api import WebApiService


def main() -> int:
    configure_logging(Path("./var/logs/datalink-host.log"))
    settings = AppSettings()
    runtime = RuntimeService(settings)
    web_api = WebApiService(runtime, settings.web)
    stop_event = threading.Event()

    def _handle_signal(_signum: int, _frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    runtime.start()
    web_api.start()
    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    finally:
        web_api.stop()
        runtime.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
