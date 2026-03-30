from __future__ import annotations

import os
import sys
from pathlib import Path


def _prepend_env_path(paths: list[Path]) -> None:
    existing = os.environ.get("PATH", "")
    joined = os.pathsep.join(str(path) for path in paths)
    os.environ["PATH"] = joined if not existing else joined + os.pathsep + existing


def _configure_pyside6_runtime() -> None:
    if sys.platform != "win32":
        return

    base = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
    candidates = [
        base,
        base / "PySide6",
        base / "shiboken6",
    ]
    existing = [path for path in candidates if path.exists()]
    for path in existing:
        try:
            os.add_dll_directory(str(path))
        except (AttributeError, FileNotFoundError):
            pass
    _prepend_env_path(existing)

    plugin_dir = base / "PySide6" / "plugins"
    qml_dir = base / "PySide6" / "qml"
    if plugin_dir.exists():
        os.environ["QT_PLUGIN_PATH"] = str(plugin_dir)
    if qml_dir.exists():
        os.environ["QML2_IMPORT_PATH"] = str(qml_dir)


_configure_pyside6_runtime()
