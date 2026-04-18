from __future__ import annotations

import json
import logging
import socketserver
import threading
from collections.abc import Callable
from typing import Any

from datalink_host.core.config import ControlServerSettings


LOGGER = logging.getLogger(__name__)


class _ControlServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler: type[socketserver.StreamRequestHandler],
        on_connection_state: Callable[[bool], None],
        on_message: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> None:
        super().__init__(server_address, handler)
        self.on_connection_state = on_connection_state
        self.on_message = on_message
        self._connection_lock = threading.Lock()
        self._connection_count = 0

    def mark_connection_opened(self) -> None:
        with self._connection_lock:
            self._connection_count += 1
            connected = self._connection_count > 0
        self.on_connection_state(connected)

    def mark_connection_closed(self) -> None:
        with self._connection_lock:
            self._connection_count = max(self._connection_count - 1, 0)
            connected = self._connection_count > 0
        self.on_connection_state(connected)


class _ControlHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        server = self.server
        assert isinstance(server, _ControlServer)
        server.mark_connection_opened()
        LOGGER.info("Control connection accepted from %s", self.client_address)
        try:
            while True:
                line = self.rfile.readline()
                if not line:
                    break
                try:
                    request = json.loads(line.decode("utf-8"))
                    response = server.on_message(request)
                except Exception as exc:  # noqa: BLE001
                    response = {"status": "error", "message": str(exc)}
                self.wfile.write(json.dumps(response).encode("utf-8") + b"\n")
        finally:
            server.mark_connection_closed()
            LOGGER.info("Control connection closed from %s", self.client_address)


class TcpControlServer:
    def __init__(
        self,
        settings: ControlServerSettings,
        on_connection_state: Callable[[bool], None],
        on_message: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> None:
        self._settings = settings
        self._on_connection_state = on_connection_state
        self._on_message = on_message
        self._server: _ControlServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._server is not None:
            return
        self._server = _ControlServer(
            (self._settings.host, self._settings.port),
            _ControlHandler,
            self._on_connection_state,
            self._on_message,
        )
        self._thread = threading.Thread(target=self._server.serve_forever, name="tcp-control-server", daemon=True)
        self._thread.start()
        LOGGER.info("Control server listening on %s:%s", self._settings.host, self._settings.port)

    def stop(self) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        self._server = None
        self._thread = None
