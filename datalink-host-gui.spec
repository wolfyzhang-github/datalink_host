# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

from PyInstaller.utils.hooks import collect_all, collect_data_files, collect_submodules


PROJECT_ROOT = Path.cwd()

pyside6_datas, pyside6_binaries, pyside6_hiddenimports = collect_all("PySide6")
shiboken6_datas, shiboken6_binaries, shiboken6_hiddenimports = collect_all("shiboken6")

datas = (
    collect_data_files("obspy")
    + collect_data_files("pyqtgraph")
    + pyside6_datas
    + shiboken6_datas
)
binaries = pyside6_binaries + shiboken6_binaries
hiddenimports = collect_submodules("obspy") + pyside6_hiddenimports + shiboken6_hiddenimports

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
