from __future__ import annotations

import logging
import socket
import threading
from collections.abc import Callable

from datalink_host.core.config import DataServerSettings, ProtocolSettings
from datalink_host.ingest.protocol import PacketDecoder, packet_to_frame
from datalink_host.models.messages import ChannelFrame, TcpPacket


LOGGER = logging.getLogger(__name__)


class TcpDataServer:
    def __init__(
        self,
        settings: DataServerSettings,
        protocol_settings: ProtocolSettings,
        on_packet: Callable[[TcpPacket], None],
        on_frame: Callable[[ChannelFrame], None],
        on_connection_state: Callable[[bool], None],
        on_bytes_received: Callable[[int], None],
        on_error: Callable[[str], None],
    ) -> None:
        self._settings = settings
        self._protocol_settings = protocol_settings
        self._on_packet = on_packet
        self._on_frame = on_frame
        self._on_connection_state = on_connection_state
        self._on_bytes_received = on_bytes_received
        self._on_error = on_error
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._socket_lock = threading.Lock()
        self._server_socket: socket.socket | None = None
        self._connection_socket: socket.socket | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="tcp-data-server", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._close_managed_sockets()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _run(self) -> None:
        try:
            if self._settings.mode == "server":
                self._run_server_mode()
                return
            if self._settings.mode == "client":
                self._run_client_mode()
                return
            raise ValueError(f"Unsupported data connection mode: {self._settings.mode}")
        except Exception as exc:  # noqa: BLE001
            if not self._stop_event.is_set():
                self._on_error(str(exc))

    def _run_server_mode(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            self._set_server_socket(server)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self._settings.host, self._settings.port))
            server.listen(1)
            server.settimeout(1.0)
            LOGGER.info("Data server listening on %s:%s", self._settings.host, self._settings.port)
            while not self._stop_event.is_set():
                try:
                    conn, addr = server.accept()
                except TimeoutError:
                    continue
                except OSError as exc:
                    if self._stop_event.is_set():
                        break
                    self._on_error(str(exc))
                    continue
                LOGGER.info("Data connection accepted from %s:%s", *addr)
                self._handle_connection(conn)
        self._set_server_socket(None)

    def _run_client_mode(self) -> None:
        while not self._stop_event.is_set():
            try:
                LOGGER.info(
                    "Data client connecting to %s:%s",
                    self._settings.remote_host,
                    self._settings.remote_port,
                )
                conn = socket.create_connection(
                    (self._settings.remote_host, self._settings.remote_port),
                    timeout=self._settings.connect_timeout_seconds,
                )
            except OSError as exc:
                if self._stop_event.is_set():
                    break
                self._on_error(str(exc))
                self._stop_event.wait(self._settings.reconnect_interval_seconds)
                continue
            with conn:
                self._set_connection_socket(conn)
                try:
                    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                except OSError:
                    pass
                LOGGER.info(
                    "Data client connected to %s:%s",
                    self._settings.remote_host,
                    self._settings.remote_port,
                )
                self._handle_connection(conn, manage_socket=False)
            self._set_connection_socket(None)
            if not self._stop_event.is_set():
                self._stop_event.wait(self._settings.reconnect_interval_seconds)

    def _handle_connection(self, conn: socket.socket, manage_socket: bool = True) -> None:
        if manage_socket:
            self._set_connection_socket(conn)
        self._on_connection_state(True)
        decoder = PacketDecoder(self._protocol_settings)
        try:
            conn.settimeout(1.0)
            while not self._stop_event.is_set():
                try:
                    chunk = conn.recv(self._settings.recv_size)
                except TimeoutError:
                    continue
                except OSError as exc:
                    if not self._stop_event.is_set():
                        self._on_error(str(exc))
                    break
                if not chunk:
                    break
                self._on_bytes_received(len(chunk))
                try:
                    for packet in decoder.feed(chunk):
                        self._on_packet(packet)
                        self._on_frame(packet_to_frame(packet, self._protocol_settings))
                except Exception as exc:  # noqa: BLE001
                    LOGGER.exception("Failed to decode data packet")
                    self._on_error(str(exc))
                    break
        finally:
            LOGGER.info("Data connection closed")
            self._on_connection_state(False)
            if manage_socket:
                self._set_connection_socket(None)

    def _set_server_socket(self, sock: socket.socket | None) -> None:
        with self._socket_lock:
            self._server_socket = sock

    def _set_connection_socket(self, sock: socket.socket | None) -> None:
        with self._socket_lock:
            self._connection_socket = sock

    def _close_managed_sockets(self) -> None:
        with self._socket_lock:
            sockets = [self._connection_socket, self._server_socket]
            self._connection_socket = None
            self._server_socket = None
        for sock in sockets:
            if sock is None:
                continue
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass
