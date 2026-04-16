from __future__ import annotations

import threading
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import uvicorn

from datalink_host.core.config import WebSettings
from datalink_host.services.runtime import RuntimeService


def _status_payload(runtime: RuntimeService) -> dict[str, Any]:
    snapshot = runtime.snapshot()
    return {
        "data_connected": snapshot.data_connected,
        "control_connected": snapshot.control_connected,
        "packets_received": snapshot.packets_received,
        "bytes_received": snapshot.bytes_received,
        "queue_depth": snapshot.queue_depth,
        "source_sample_rate": snapshot.source_sample_rate,
        "data1_rate": snapshot.data1_rate,
        "data2_rate": snapshot.data2_rate,
        "storage_enabled": snapshot.storage_enabled,
        "datalink_enabled": snapshot.datalink_enabled,
        "datalink_connected": snapshot.datalink_connected,
        "datalink_packets_sent": snapshot.datalink_packets_sent,
        "datalink_bytes_sent": snapshot.datalink_bytes_sent,
        "datalink_reconnects": snapshot.datalink_reconnects,
        "datalink_last_error": snapshot.datalink_last_error,
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

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return _INDEX_HTML

    @app.get("/api/status")
    async def status() -> dict[str, Any]:
        return {"status": "ok", "payload": _status_payload(runtime)}

    @app.get("/api/config")
    async def config() -> dict[str, Any]:
        return {"status": "ok", "payload": runtime.current_config()}

    @app.post("/api/config")
    async def update_config(request: Request) -> dict[str, Any]:
        payload = await request.json()
        return {"status": "ok", "payload": runtime.update_config(payload)}

    @app.get("/api/gps/ports")
    async def gps_ports() -> dict[str, Any]:
        return {"status": "ok", "payload": runtime.gps_ports()}

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


_INDEX_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>datalink-host 控制台</title>
  <style>
    :root {
      --bg: #f3f5ef;
      --panel: #fffdf7;
      --ink: #1f2a1f;
      --accent: #006d5b;
      --line: #d8ddcf;
    }
    body {
      margin: 0;
      font-family: "PingFang SC", "Noto Sans SC", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, rgba(0,109,91,0.10), transparent 28%),
        linear-gradient(180deg, #f9faf6 0%, var(--bg) 100%);
    }
    main {
      max-width: 960px;
      margin: 0 auto;
      padding: 24px;
    }
    h1 {
      margin: 0 0 18px;
      font-size: 28px;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 16px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 18px;
      box-shadow: 0 10px 30px rgba(34, 44, 34, 0.06);
    }
    label {
      display: block;
      margin: 10px 0 6px;
      font-size: 14px;
    }
    input, select, button {
      width: 100%;
      box-sizing: border-box;
      border-radius: 10px;
      border: 1px solid #c7d0bf;
      padding: 10px 12px;
      font-size: 14px;
    }
    button {
      margin-top: 12px;
      background: var(--accent);
      color: white;
      border: none;
      cursor: pointer;
    }
    button.secondary {
      background: #75826c;
    }
    pre {
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font-size: 13px;
      line-height: 1.5;
    }
    .row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .hint {
      font-size: 12px;
      color: #5f6b5f;
      margin-top: 8px;
    }
  </style>
</head>
<body>
  <main>
    <h1>datalink-host Web 控制台</h1>
    <div class="grid">
      <section class="panel">
        <h2>采样与存储</h2>
        <label for="data1-rate">采样率 1</label>
        <input id="data1-rate" type="number" step="0.1">
        <label for="data2-rate">采样率 2</label>
        <input id="data2-rate" type="number" step="0.1">
        <label for="storage-root">存储路径</label>
        <input id="storage-root" type="text">
        <button id="save-core">保存采样/存储配置</button>
      </section>
      <section class="panel">
        <h2>GPS 设置</h2>
        <label for="gps-port">串口号</label>
        <div class="row">
          <select id="gps-port"></select>
          <button id="refresh-ports" class="secondary">刷新端口</button>
        </div>
        <label for="gps-baudrate">波特率</label>
        <input id="gps-baudrate" type="number" step="1">
        <label for="gps-mode">模式</label>
        <select id="gps-mode">
          <option value="debug">调试模式</option>
          <option value="deploy">部署模式</option>
        </select>
        <label for="gps-poll">调试轮询间隔（秒）</label>
        <input id="gps-poll" type="number" step="0.01">
        <button id="save-gps">保存 GPS 配置</button>
        <button id="gps-start">启用 GPS</button>
        <button id="gps-stop" class="secondary">停用 GPS</button>
        <div class="hint">部署模式下不会主动发送 timestamp 查询。</div>
      </section>
      <section class="panel">
        <h2>运行状态</h2>
        <pre id="status-view">加载中...</pre>
      </section>
    </div>
  </main>
  <script>
    async function fetchJson(url, options) {
      const response = await fetch(url, options);
      const data = await response.json();
      if (data.status !== 'ok') throw new Error(data.message || '请求失败');
      return data.payload;
    }

    function fillCore(config) {
      document.getElementById('data1-rate').value = config.processing.data1_rate;
      document.getElementById('data2-rate').value = config.processing.data2_rate;
      document.getElementById('storage-root').value = config.storage.root;
      document.getElementById('gps-baudrate').value = config.gps.baudrate;
      document.getElementById('gps-mode').value = config.gps.mode;
      document.getElementById('gps-poll').value = config.gps.poll_interval_seconds;
    }

    async function refreshPorts(selectedValue) {
      const ports = await fetchJson('/api/gps/ports');
      const select = document.getElementById('gps-port');
      select.innerHTML = '';
      if (!ports.length) {
        const option = document.createElement('option');
        option.value = '';
        option.textContent = '未发现串口';
        select.appendChild(option);
      } else {
        for (const port of ports) {
          const option = document.createElement('option');
          option.value = port;
          option.textContent = port;
          if (port === selectedValue) option.selected = true;
          select.appendChild(option);
        }
      }
    }

    async function loadConfig() {
      const config = await fetchJson('/api/config');
      fillCore(config);
      await refreshPorts(config.gps.port);
    }

    async function refreshStatus() {
      const payload = await fetchJson('/api/status');
      document.getElementById('status-view').textContent = JSON.stringify(payload, null, 2);
    }

    document.getElementById('refresh-ports').onclick = async () => {
      await refreshPorts(document.getElementById('gps-port').value);
    };

    document.getElementById('save-core').onclick = async () => {
      await fetchJson('/api/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          processing: {
            data1_rate: Number(document.getElementById('data1-rate').value),
            data2_rate: Number(document.getElementById('data2-rate').value),
          },
          storage: {
            root: document.getElementById('storage-root').value,
          }
        })
      });
      await refreshStatus();
    };

    document.getElementById('save-gps').onclick = async () => {
      await fetchJson('/api/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          gps: {
            port: document.getElementById('gps-port').value,
            baudrate: Number(document.getElementById('gps-baudrate').value),
            mode: document.getElementById('gps-mode').value,
            poll_interval_seconds: Number(document.getElementById('gps-poll').value),
          }
        })
      });
      await refreshStatus();
    };

    document.getElementById('gps-start').onclick = async () => {
      await fetchJson('/api/gps/start', {method: 'POST'});
      await refreshStatus();
    };

    document.getElementById('gps-stop').onclick = async () => {
      await fetchJson('/api/gps/stop', {method: 'POST'});
      await refreshStatus();
    };

    loadConfig().then(refreshStatus);
    setInterval(refreshStatus, 1000);
  </script>
</body>
</html>
"""
