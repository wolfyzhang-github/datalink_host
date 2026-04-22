from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from datalink_host.core.config import AppSettings
from datalink_host.core.paths import default_capture_path, default_storage_root, runtime_root
from datalink_host.self_check import run_self_check
from datalink_host.services.web_ui import bundled_web_asset_names, load_index_html, web_assets_path


class PackagingSupportTests(unittest.TestCase):
    def test_web_ui_assets_are_bundled_locally(self) -> None:
        html = load_index_html()
        self.assertNotIn("cdn.jsdelivr.net", html)
        self.assertIn('./assets/bootstrap.min.css', html)
        self.assertIn('./assets/bootstrap.bundle.min.js', html)
        self.assertIn('./assets/chart.umd.min.js', html)

        assets_dir = web_assets_path()
        self.assertTrue(assets_dir.is_dir())

        bundled_assets = bundled_web_asset_names()
        self.assertIn("bootstrap.min.css", bundled_assets)
        self.assertIn("bootstrap.bundle.min.js", bundled_assets)
        self.assertIn("chart.umd.min.js", bundled_assets)

        for asset_name in bundled_assets:
            asset_path = assets_dir / asset_name
            self.assertGreater(asset_path.stat().st_size, 0)

    def test_default_paths_follow_runtime_override(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(os.environ, {"DATALINK_HOST_HOME": temp_dir}, clear=False):
                settings = AppSettings()
                expected_root = Path(temp_dir).resolve()
                self.assertEqual(expected_root, runtime_root())
                self.assertEqual(expected_root / "var" / "storage", default_storage_root())
                self.assertEqual(expected_root / "var" / "captures" / "session.dlhcap", default_capture_path())
                self.assertEqual(expected_root / "var" / "storage", settings.storage.root)
                self.assertEqual(expected_root / "var" / "captures" / "session.dlhcap", settings.capture.path)

    def test_self_check_writes_success_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report_path = Path(temp_dir) / "smoke-report.json"
            with patch.dict(os.environ, {"DATALINK_HOST_HOME": temp_dir}, clear=False):
                exit_code = run_self_check(report_path)
            self.assertEqual(0, exit_code)

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertTrue(report["ok"])
            self.assertGreater(report["checks"]["obspy_entry_points"]["count"], 0)
            self.assertEqual("FLOAT32", report["checks"]["obspy_mseed"]["encoding"])
            self.assertIn("bootstrap.min.css", report["checks"]["web_ui"]["assets"])
            self.assertIn("bootstrap.bundle.min.js", report["checks"]["web_ui"]["assets"])
            self.assertIn("chart.umd.min.js", report["checks"]["web_ui"]["assets"])
