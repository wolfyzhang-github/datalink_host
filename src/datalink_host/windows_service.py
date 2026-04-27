from __future__ import annotations

import argparse
from collections.abc import Sequence
import logging
import os
from pathlib import Path
import shlex
import subprocess
import sys
import threading
import time
from typing import Any

from datalink_host.core.config import AppSettings
from datalink_host.core.frozen import patch_obspy_version_path_for_frozen
from datalink_host.core.logging import configure_logging
from datalink_host.core.paths import ensure_runtime_dirs, log_file_path


LOGGER = logging.getLogger(__name__)
SERVICE_NAME = "DatalinkHost"
SERVICE_DISPLAY_NAME = "DataLink Host Runtime"
SERVICE_DESCRIPTION = "DataLink host runtime and Web API service."
SERVICE_STOP_TIMEOUT_SECONDS = 30.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Windows service entrypoint for datalink-host runtime")
    subparsers = parser.add_subparsers(dest="command")

    install_parser = subparsers.add_parser("install", help="Install the Windows service")
    install_parser.add_argument("--startup", choices=("auto", "demand"), default="auto")
    install_parser.add_argument("--delayed-auto-start", action="store_true")
    install_parser.add_argument("--start", action="store_true", help="Start the service after installation")

    subparsers.add_parser("run", help="Run under the Windows Service Control Manager")
    subparsers.add_parser("debug", help="Run in the foreground for troubleshooting")
    subparsers.add_parser("start", help="Start the installed service")
    subparsers.add_parser("stop", help="Stop the installed service")
    subparsers.add_parser("restart", help="Restart the installed service")
    subparsers.add_parser("remove", help="Stop and remove the installed service")
    subparsers.add_parser("status", help="Show service status")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    command = args.command
    if command is None:
        command = "run" if os.name == "nt" else "debug"

    if command == "install":
        install_service(startup=args.startup, delayed_auto_start=args.delayed_auto_start)
        if args.start:
            run_sc("start", SERVICE_NAME, check=True)
        return 0
    if command == "start":
        run_sc("start", SERVICE_NAME, check=True)
        return 0
    if command == "stop":
        stop_service(check=True)
        return 0
    if command == "restart":
        stop_service(check=False)
        run_sc("start", SERVICE_NAME, check=True)
        return 0
    if command == "remove":
        stop_service(check=False)
        run_sc("delete", SERVICE_NAME, check=True)
        return 0
    if command == "status":
        run_sc("query", SERVICE_NAME, check=False)
        return 0
    if command == "debug":
        return run_foreground()
    if command == "run":
        return run_service_dispatcher()

    raise ValueError(f"Unsupported service command: {command}")


def install_service(*, startup: str, delayed_auto_start: bool) -> None:
    start_type = "auto" if startup == "auto" else "demand"
    bin_path = service_bin_path()
    run_sc(
        "create",
        SERVICE_NAME,
        "binPath=",
        bin_path,
        "start=",
        start_type,
        "DisplayName=",
        SERVICE_DISPLAY_NAME,
        check=True,
    )
    run_sc("description", SERVICE_NAME, SERVICE_DESCRIPTION, check=False)
    run_sc(
        "failure",
        SERVICE_NAME,
        "reset=",
        "86400",
        "actions=",
        "restart/5000/restart/5000/none/5000",
        check=False,
    )
    if delayed_auto_start and startup == "auto":
        run_sc("config", SERVICE_NAME, "delayed-auto-start=", "yes", check=False)


def service_bin_path() -> str:
    if getattr(sys, "frozen", False):
        args = "--mode service run" if launched_through_app_mode() else "run"
        return f"{quote_for_cmd(sys.executable)} {args}"
    module_args = "-m datalink_host.app --mode service run"
    return f"{quote_for_cmd(sys.executable)} {module_args}"


def launched_through_app_mode() -> bool:
    args = list(sys.argv[1:])
    return "--mode" in args and "service" in args


def quote_for_cmd(value: str) -> str:
    return subprocess.list2cmdline([str(Path(value))])


def run_sc(*args: str, check: bool) -> subprocess.CompletedProcess[str]:
    command = ["sc.exe", *args]
    print(shlex.join(command))
    completed = subprocess.run(command, text=True, check=False)
    if check and completed.returncode != 0:
        raise SystemExit(completed.returncode)
    return completed


def stop_service(*, check: bool) -> None:
    completed = run_sc("stop", SERVICE_NAME, check=False)
    if completed.returncode != 0:
        if check:
            raise SystemExit(completed.returncode)
        return
    deadline = time.monotonic() + SERVICE_STOP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        query = subprocess.run(["sc.exe", "query", SERVICE_NAME], capture_output=True, text=True, check=False)
        if "STOPPED" in query.stdout:
            return
        time.sleep(0.5)


def run_foreground() -> int:
    stop_event = threading.Event()
    try:
        return run_runtime_until_stopped(stop_event)
    except KeyboardInterrupt:
        stop_event.set()
        return 0


def run_runtime_until_stopped(stop_event: threading.Event) -> int:
    patch_obspy_version_path_for_frozen()

    from datalink_host.services.runtime import RuntimeService
    from datalink_host.services.web_api import WebApiService

    ensure_runtime_dirs()
    configure_logging(log_file_path())
    settings = AppSettings()
    runtime = RuntimeService(settings)
    web_api = WebApiService(runtime, settings.web)
    runtime.start()
    web_api.start()
    LOGGER.info("DataLink host Windows service runtime started")
    try:
        while not stop_event.wait(0.5):
            pass
    finally:
        LOGGER.info("DataLink host Windows service runtime stopping")
        web_api.stop()
        runtime.stop()
    return 0


def run_service_dispatcher() -> int:
    if os.name != "nt":
        raise SystemExit("Windows service mode is only available on Windows.")
    service_class, servicemanager = build_service_class()
    servicemanager.Initialize()
    servicemanager.PrepareToHostSingle(service_class)
    servicemanager.StartServiceCtrlDispatcher()
    return 0


def build_service_class() -> tuple[type[Any], Any]:
    try:
        import servicemanager
        import win32service
        import win32serviceutil
    except ImportError as exc:
        raise SystemExit(
            "Windows service mode requires pywin32. Install it before packaging: "
            "pip install pywin32"
        ) from exc

    class DatalinkHostWindowsService(win32serviceutil.ServiceFramework):  # type: ignore[misc, valid-type]
        _svc_name_ = SERVICE_NAME
        _svc_display_name_ = SERVICE_DISPLAY_NAME
        _svc_description_ = SERVICE_DESCRIPTION

        def __init__(self, args: Sequence[str]) -> None:
            super().__init__(args)
            self._stop_event = threading.Event()

        def SvcStop(self) -> None:  # noqa: N802
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            self._stop_event.set()

        def SvcDoRun(self) -> None:  # noqa: N802
            servicemanager.LogInfoMsg(f"{SERVICE_DISPLAY_NAME} is starting")
            self.ReportServiceStatus(win32service.SERVICE_RUNNING)
            try:
                run_runtime_until_stopped(self._stop_event)
            except Exception as exc:  # noqa: BLE001
                servicemanager.LogErrorMsg(f"{SERVICE_DISPLAY_NAME} failed: {exc}")
                raise
            finally:
                servicemanager.LogInfoMsg(f"{SERVICE_DISPLAY_NAME} stopped")

    return DatalinkHostWindowsService, servicemanager


if __name__ == "__main__":
    raise SystemExit(main())
