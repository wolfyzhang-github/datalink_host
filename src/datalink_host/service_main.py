from __future__ import annotations

import signal
import threading
import time

from datalink_host.core.config import AppSettings
from datalink_host.core.frozen import patch_obspy_version_path_for_frozen
from datalink_host.core.logging import configure_logging
from datalink_host.core.paths import ensure_runtime_dirs, log_file_path


def main() -> int:
    patch_obspy_version_path_for_frozen()

    from datalink_host.services.runtime import RuntimeService
    from datalink_host.services.web_api import WebApiService

    ensure_runtime_dirs()
    configure_logging(log_file_path())
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
