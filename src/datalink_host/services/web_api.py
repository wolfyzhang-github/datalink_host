from __future__ import annotations

import threading
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from datalink_host.core.logging import get_recent_logs
from datalink_host.core.config import WebSettings
from datalink_host.services.runtime import RuntimeService
from datalink_host.services.web_ui import INDEX_HTML, web_assets_path


def _status_payload(runtime: RuntimeService) -> dict[str, Any]:
    snapshot = runtime.snapshot()
    return {
        "processing_active": runtime.is_processing_active(),
        "data_connected": snapshot.data_connected,
        "control_connected": snapshot.control_connected,
        "packets_received": snapshot.packets_received,
        "bytes_received": snapshot.bytes_received,
        "frames_dropped": snapshot.frames_dropped,
        "queue_depth": snapshot.queue_depth,
        "source_sample_rate": snapshot.source_sample_rate,
        "data1_rate": snapshot.data1_rate,
        "data2_rate": snapshot.data2_rate,
        "storage_enabled": snapshot.storage_enabled,
        "storage_queue_depth": snapshot.storage_queue_depth,
        "storage_frames_dropped": snapshot.storage_frames_dropped,
        "storage_last_error": snapshot.storage_last_error,
        "datalink_enabled": snapshot.datalink_enabled,
        "datalink_connected": snapshot.datalink_connected,
        "datalink_packets_sent": snapshot.datalink_packets_sent,
        "datalink_bytes_sent": snapshot.datalink_bytes_sent,
        "datalink_reconnects": snapshot.datalink_reconnects,
        "datalink_last_error": snapshot.datalink_last_error,
        "datalink_publish_queue_depth": snapshot.datalink_publish_queue_depth,
        "datalink_publish_frames_dropped": snapshot.datalink_publish_frames_dropped,
        "datalink_publish_last_error": snapshot.datalink_publish_last_error,
        "capture_enabled": snapshot.capture_enabled,
        "gps_enabled": snapshot.gps_enabled,
        "gps_connected": snapshot.gps_connected,
        "gps_mode": snapshot.gps_mode,
        "gps_port": snapshot.gps_port,
        "gps_baudrate": snapshot.gps_baudrate,
        "gps_last_timestamp": snapshot.gps_last_timestamp,
        "gps_last_error": snapshot.gps_last_error,
        "gps_fallback_active": snapshot.gps_fallback_active,
        "last_error": snapshot.last_error,
        "updated_at": snapshot.updated_at,
    }


def create_app(runtime: RuntimeService) -> FastAPI:
    app = FastAPI(title="datalink-host web control")
    assets_dir = web_assets_path()
    if not assets_dir.is_dir():
        raise FileNotFoundError(f"Bundled web assets are missing: {assets_dir}")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return INDEX_HTML

    @app.get("/api/status")
    async def status() -> dict[str, Any]:
        return {"status": "ok", "payload": _status_payload(runtime)}

    @app.get("/api/config")
    async def config() -> dict[str, Any]:
        return {"status": "ok", "payload": runtime.current_config()}

    @app.post("/api/config")
    async def update_config(request: Request) -> dict[str, Any]:
        payload = await request.json()
        try:
            updated = runtime.update_config(payload)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"status": "ok", "payload": updated}

    @app.get("/api/monitor")
    async def monitor(
        mode: str = Query(default="raw"),
        max_points: int = Query(default=2048, ge=128, le=4096),
        max_packets: int = Query(default=20, ge=1, le=40),
    ) -> dict[str, Any]:
        payload = runtime.monitor_view(mode=mode, max_points=max_points, max_packets=max_packets)
        payload["status"] = _status_payload(runtime)
        return {"status": "ok", "payload": payload}

    @app.post("/api/processing/start")
    async def processing_start() -> dict[str, Any]:
        runtime.resume_processing()
        return {"status": "ok", "payload": _status_payload(runtime)}

    @app.post("/api/processing/stop")
    async def processing_stop() -> dict[str, Any]:
        runtime.pause_processing()
        return {"status": "ok", "payload": _status_payload(runtime)}

    @app.get("/api/gps/ports")
    async def gps_ports() -> dict[str, Any]:
        return {"status": "ok", "payload": runtime.gps_ports()}

    @app.get("/api/logs")
    async def logs(
        limit: int = Query(default=500, ge=1, le=2000),
        level: str = Query(default="all"),
    ) -> dict[str, Any]:
        normalized_level = level.strip().upper()
        lines = get_recent_logs(limit=max(limit * 3, 200))
        if normalized_level in {"INFO", "WARNING", "ERROR"}:
            lines = [line for line in lines if f" {normalized_level} " in line]
        elif normalized_level not in {"ALL", ""}:
            raise HTTPException(status_code=400, detail="Unsupported log level")
        return {"status": "ok", "payload": {"lines": lines[-limit:]}}

    @app.post("/api/gps/start")
    async def gps_start() -> dict[str, Any]:
        return {"status": "ok", "payload": runtime.update_config({"gps": {"enabled": True}})}

    @app.post("/api/gps/stop")
    async def gps_stop() -> dict[str, Any]:
        return {"status": "ok", "payload": runtime.update_config({"gps": {"enabled": False}})}

    return app


class WebApiService:
    def __init__(self, runtime: RuntimeService, settings: WebSettings) -> None:
        self._runtime = runtime
        self._settings = settings
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if not self._settings.enabled or self._thread is not None:
            return
        app = create_app(self._runtime)
        config = uvicorn.Config(
            app,
            host=self._settings.host,
            port=self._settings.port,
            log_config=None,
            log_level="info",
            access_log=False,
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, name="web-api-server", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        self._server = None
        self._thread = None
