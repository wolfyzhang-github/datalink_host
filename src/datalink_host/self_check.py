from __future__ import annotations

import importlib.metadata
import json
import sys
import traceback
from pathlib import Path
from typing import Any

from datalink_host.core.config import AppSettings
from datalink_host.core.frozen import patch_obspy_version_path_for_frozen
from datalink_host.core.paths import ensure_runtime_dirs, runtime_root
from datalink_host.services.web_ui import load_index_html


def _write_report(report: dict[str, Any], output_path: Path | None) -> None:
    if output_path is None:
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


def run_self_check(output_path: Path | None = None) -> int:
    report: dict[str, Any] = {
        "ok": False,
        "platform": sys.platform,
        "python": sys.version,
        "frozen": bool(getattr(sys, "frozen", False)),
        "executable": sys.executable,
        "runtime_root": str(runtime_root()),
        "checks": {},
    }
    try:
        patch_obspy_version_path_for_frozen()

        import io

        import numpy as np
        from obspy import Stream, Trace, read
        from PySide6 import QtWidgets

        ensure_runtime_dirs()

        settings = AppSettings()
        report["checks"]["paths"] = {
            "storage_root": str(settings.storage.root),
            "capture_path": str(settings.capture.path),
        }

        html = load_index_html()
        if "<html" not in html.lower():
            raise ValueError("Embedded web UI was not loaded correctly")
        report["checks"]["web_ui"] = {"bytes": len(html)}

        entry_points = list(
            importlib.metadata.entry_points(
                group="obspy.plugin.waveform.MSEED",
                name="writeFormat",
            )
        )
        if not entry_points:
            raise ImportError("ObsPy MSEED write entry point is missing")
        report["checks"]["obspy_entry_points"] = {
            "count": len(entry_points),
            "distributions": sorted(
                {
                    getattr(getattr(entry_point, "dist", None), "name", "unknown")
                    for entry_point in entry_points
                }
            ),
        }

        trace = Trace(np.asarray([1.25, 2.5, 3.75], dtype=np.float32))
        trace.stats.network = "SC"
        trace.stats.station = "S0001"
        trace.stats.location = "10"
        trace.stats.channel = "HSH"
        trace.stats.sampling_rate = 100.0

        payload = io.BytesIO()
        Stream([trace]).write(payload, format="MSEED", encoding="FLOAT32", reclen=256)
        roundtrip = read(io.BytesIO(payload.getvalue()), format="MSEED")
        if roundtrip[0].stats.mseed.encoding != "FLOAT32":
            raise ValueError("ObsPy MSEED roundtrip encoding check failed")
        report["checks"]["obspy_mseed"] = {
            "payload_bytes": len(payload.getvalue()),
            "sample_count": int(roundtrip[0].stats.npts),
            "encoding": roundtrip[0].stats.mseed.encoding,
        }

        report["checks"]["qt_import"] = {"module": QtWidgets.__name__}
        report["ok"] = True
        _write_report(report, output_path)
        return 0
    except Exception as exc:  # noqa: BLE001
        report["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
        _write_report(report, output_path)
        return 1
