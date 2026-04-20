from __future__ import annotations

import os
import sys
from pathlib import Path


APP_HOME_ENV = "DATALINK_HOST_HOME"


def project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def application_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return project_root()


def runtime_root() -> Path:
    override = os.environ.get(APP_HOME_ENV)
    if override:
        return Path(override).expanduser().resolve()
    return application_root()


def runtime_path(*parts: str) -> Path:
    return runtime_root().joinpath(*parts)


def default_storage_root() -> Path:
    return runtime_path("var", "storage")


def default_capture_path() -> Path:
    return runtime_path("var", "captures", "session.dlhcap")


def default_datalink_dump_root() -> Path:
    return runtime_path("var", "datalink-dumps")


def log_file_path() -> Path:
    return runtime_path("var", "logs", "datalink-host.log")


def ensure_runtime_dirs() -> None:
    for path in (
        runtime_path("var"),
        runtime_path("var", "logs"),
        default_storage_root(),
        runtime_path("var", "captures"),
        default_datalink_dump_root(),
    ):
        path.mkdir(parents=True, exist_ok=True)
