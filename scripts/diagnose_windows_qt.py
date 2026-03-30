from __future__ import annotations

import argparse
import ctypes
import os
import sys
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Diagnose packaged PySide6 DLL loading on Windows")
    parser.add_argument(
        "bundle_root",
        nargs="?",
        default="dist/datalink-host-gui/_internal",
        help="Path to the PyInstaller _internal directory.",
    )
    return parser


def _add_search_path(path: Path) -> None:
    if not path.exists():
        return
    try:
        os.add_dll_directory(str(path))
    except (AttributeError, FileNotFoundError):
        pass
    existing = os.environ.get("PATH", "")
    os.environ["PATH"] = str(path) if not existing else str(path) + os.pathsep + existing


def _load(label: str, path: Path) -> bool:
    try:
        ctypes.WinDLL(str(path))
    except OSError as exc:
        print(f"FAIL {label}: {path}")
        print(f"  {exc}")
        return False
    print(f"OK   {label}: {path}")
    return True


def main() -> int:
    if sys.platform != "win32":
        print("This diagnostic only runs on Windows.")
        return 1

    args = build_parser().parse_args()
    bundle_root = Path(args.bundle_root).resolve()
    pyside6_dir = bundle_root / "PySide6"
    shiboken6_dir = bundle_root / "shiboken6"

    print(f"bundle_root={bundle_root}")
    print(f"pyside6_dir={pyside6_dir}")
    print(f"shiboken6_dir={shiboken6_dir}")

    for path in (bundle_root, pyside6_dir, shiboken6_dir):
        _add_search_path(path)

    print("\n== shiboken6 files ==")
    if shiboken6_dir.exists():
        for entry in sorted(shiboken6_dir.iterdir()):
            print(entry.name)

    print("\n== direct loads ==")
    candidates = [
        ("pyside6.abi3.dll", pyside6_dir / "pyside6.abi3.dll"),
        ("Qt6Core.dll", pyside6_dir / "Qt6Core.dll"),
        ("Qt6Gui.dll", pyside6_dir / "Qt6Gui.dll"),
        ("Qt6Widgets.dll", pyside6_dir / "Qt6Widgets.dll"),
        ("QtCore.pyd", pyside6_dir / "QtCore.pyd"),
        ("QtGui.pyd", pyside6_dir / "QtGui.pyd"),
        ("QtWidgets.pyd", pyside6_dir / "QtWidgets.pyd"),
    ]
    if shiboken6_dir.exists():
        for name in ("shiboken6.abi3.dll", "Shiboken.pyd"):
            path = shiboken6_dir / name
            if path.exists():
                candidates.insert(0, (name, path))

    success = True
    for label, path in candidates:
        if path.exists():
            success = _load(label, path) and success
        else:
            print(f"MISSING {label}: {path}")
            success = False

    print("\n== import test ==")
    try:
        sys.path.insert(0, str(bundle_root))
        import PySide6.QtWidgets  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL import PySide6.QtWidgets: {exc!r}")
        success = False
    else:
        print("OK   import PySide6.QtWidgets")

    return 0 if success else 2


if __name__ == "__main__":
    raise SystemExit(main())
