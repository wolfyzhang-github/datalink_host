# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path
import importlib.util

from PyInstaller.utils.hooks import collect_data_files, collect_submodules


PROJECT_ROOT = Path.cwd()


def _module_dir(module_name: str) -> Path:
    spec = importlib.util.find_spec(module_name)
    if spec is None or spec.origin is None:
        raise RuntimeError(f"Could not resolve module path for {module_name}")
    return Path(spec.origin).resolve().parent


def _collect_root_binaries(module_name: str, patterns: list[str], target_dir: str) -> list[tuple[str, str]]:
    module_dir = _module_dir(module_name)
    binaries: list[tuple[str, str]] = []
    seen: set[Path] = set()
    for pattern in patterns:
        for path in module_dir.glob(pattern):
            if path in seen or not path.is_file():
                continue
            binaries.append((str(path), target_dir))
            seen.add(path)
    return binaries

datas = collect_data_files("obspy") + collect_data_files("pyqtgraph")
hiddenimports = [
    "PySide6.QtCore",
    "PySide6.QtGui",
    "PySide6.QtWidgets",
    "shiboken6",
] + collect_submodules("obspy")
binaries = _collect_root_binaries(
    "PySide6",
    [
        "*.dll",
        "Qt*.pyd",
    ],
    "PySide6",
) + _collect_root_binaries(
    "shiboken6",
    [
        "*.dll",
        "*.pyd",
    ],
    "shiboken6",
)

a = Analysis(
    [str(PROJECT_ROOT / "src" / "datalink_host" / "gui" / "app.py")],
    pathex=[str(PROJECT_ROOT / "src")],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["PyQt5", "PyQt6", "PySide2"],
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="datalink-host-gui",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name="datalink-host-gui",
)
