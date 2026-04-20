from __future__ import annotations

import inspect
import sys
from pathlib import Path


_ORIGINAL_GETFILE = inspect.getfile
_PATCHED = False


def patch_obspy_version_path_for_frozen() -> None:
    global _PATCHED
    if _PATCHED or not getattr(sys, "frozen", False):
        return

    bundle_root = Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
    bundled_version_py = bundle_root / "obspy" / "core" / "util" / "version.py"
    if not bundled_version_py.exists():
        return

    def _patched_getfile(obj):
        filename = _ORIGINAL_GETFILE(obj)
        normalized = str(filename).replace("\\", "/")
        if normalized.endswith("obspy/core/util/version.py"):
            return str(bundled_version_py)
        return filename

    inspect.getfile = _patched_getfile
    _PATCHED = True
