# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path
import importlib.util

from PyInstaller.utils.hooks import (
    collect_all,
    collect_data_files,
    collect_delvewheel_libs_directory,
    collect_submodules,
    copy_metadata,
)


PROJECT_ROOT = Path(SPECPATH).resolve()


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


def _unique_items(items):
    unique = []
    seen = set()
    for item in items:
        key = tuple(item) if isinstance(item, (list, tuple)) else item
        if key in seen:
            continue
        unique.append(item)
        seen.add(key)
    return unique


def _keep_obspy_submodule(name: str) -> bool:
    return ".tests" not in name and not name.endswith(".conftest")


obspy_datas, obspy_binaries, obspy_hiddenimports = collect_all(
    "obspy",
    filter_submodules=_keep_obspy_submodule,
    exclude_datas=[
        "**/tests/*",
        "**/.coveragerc",
        "**/pytest.ini",
        "**/conftest.py",
    ],
)

datas = _unique_items(
    obspy_datas
    + collect_data_files("pyqtgraph")
    + collect_data_files("datalink_host")
    + copy_metadata("obspy")
)
hiddenimports = [
    "PySide6.QtCore",
    "PySide6.QtGui",
    "PySide6.QtWidgets",
    "shiboken6",
] + obspy_hiddenimports + collect_submodules("uvicorn") + collect_submodules("serial")
binaries = obspy_binaries + _collect_root_binaries(
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
datas, binaries = collect_delvewheel_libs_directory("PySide6", datas=datas, binaries=binaries)
datas, binaries = collect_delvewheel_libs_directory("shiboken6", datas=datas, binaries=binaries)
hiddenimports = sorted(set(hiddenimports))
binaries = _unique_items(binaries)

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
    a.binaries,
    a.datas,
    [],
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
