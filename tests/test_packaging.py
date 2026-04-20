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


class PackagingSupportTests(unittest.TestCase):
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
