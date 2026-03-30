from __future__ import annotations

import tempfile
import threading
import time
import unittest
import socket
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np

from datalink_host.core.logging import configure_logging, get_recent_logs
from datalink_host.debug.capture import PacketCaptureWriter, read_capture
from datalink_host.core.config import AppSettings, DataLinkSettings, DataServerSettings, ProtocolSettings, StorageSettings
from datalink_host.ingest.data_server import TcpDataServer
from datalink_host.ingest.protocol import PacketDecoder, build_packet, packet_to_frame
from datalink_host.models.messages import ChannelFrame, ProcessedFrame
from datalink_host.processing.pipeline import compute_psd
from datalink_host.services.runtime import RuntimeService
from datalink_host.storage.miniseed import MiniSeedWriter
from datalink_host.transport.datalink import DataLinkPublisher


class ProtocolTests(unittest.TestCase):
    def test_round_trip_interleaved_packet(self) -> None:
        settings = ProtocolSettings(
            frame_header=0x12345678,
            frame_header_size=4,
            length_field_size=4,
            length_field_format="uint",
            channels=8,
            channel_layout="interleaved",
        )
        channels = np.arange(16, dtype=np.float64).reshape(8, 2)
        payload = build_packet(1000.0, channels, settings)

        decoder = PacketDecoder(settings)
        packets = decoder.feed(payload)
        self.assertEqual(1, len(packets))

        frame = packet_to_frame(packets[0], settings)
        self.assertTrue(np.array_equal(channels, frame.channels))

    def test_round_trip_labview_style_packet(self) -> None:
        settings = ProtocolSettings(
            frame_header=11,
            frame_header_size=2,
            length_field_size=8,
            length_field_format="float64",
            length_field_units="values",
            channels=8,
            channel_layout="interleaved",
        )
        channels = np.arange(80, dtype=np.float64).reshape(8, 10)
        payload = build_packet(10000.0, channels, settings)

        decoder = PacketDecoder(settings)
        packets = decoder.feed(payload)
        self.assertEqual(1, len(packets))
        self.assertEqual(channels.size * 8, packets[0].payload_bytes)

        frame = packet_to_frame(packets[0], settings)
        self.assertTrue(np.array_equal(channels, frame.channels))

    def test_round_trip_big_endian_labview_style_packet(self) -> None:
        settings = ProtocolSettings(
            frame_header=11,
            frame_header_size=2,
            length_field_size=8,
            length_field_format="float64",
            length_field_units="values",
            byte_order="big",
            channels=8,
            channel_layout="interleaved",
        )
        channels = np.arange(80, dtype=np.float64).reshape(8, 10)
        payload = build_packet(10000.0, channels, settings)

        decoder = PacketDecoder(settings)
        packets = decoder.feed(payload)
        self.assertEqual(1, len(packets))
        self.assertEqual(channels.size * 8, packets[0].payload_bytes)

        frame = packet_to_frame(packets[0], settings)
        self.assertTrue(np.array_equal(channels, frame.channels))

    def test_decoder_reports_possible_byte_order_mismatch(self) -> None:
        sender_settings = ProtocolSettings(
            frame_header=11,
            frame_header_size=2,
            length_field_size=8,
            length_field_format="float64",
            length_field_units="values",
            byte_order="big",
            channels=8,
            channel_layout="interleaved",
        )
        receiver_settings = ProtocolSettings(
            frame_header=11,
            frame_header_size=2,
            length_field_size=8,
            length_field_format="float64",
            length_field_units="values",
            byte_order="little",
            channels=8,
            channel_layout="interleaved",
        )
        channels = np.arange(80, dtype=np.float64).reshape(8, 10)
        payload = build_packet(10000.0, channels, sender_settings)

        decoder = PacketDecoder(receiver_settings)
        with self.assertRaisesRegex(ValueError, "possible byte_order mismatch"):
            decoder.feed(payload)

    def test_round_trip_value_count_length_field(self) -> None:
        settings = ProtocolSettings(
            frame_header=11,
            frame_header_size=2,
            length_field_size=8,
            length_field_format="float64",
            length_field_units="values",
            channels=8,
            channel_layout="channel-major",
        )
        channels = np.arange(80, dtype=np.float64).reshape(8, 10)
        payload = build_packet(10000.0, channels, settings)

        decoder = PacketDecoder(settings)
        packets = decoder.feed(payload)
        self.assertEqual(1, len(packets))

        frame = packet_to_frame(packets[0], settings)
        self.assertTrue(np.array_equal(channels, frame.channels))

    def test_data_server_client_mode_receives_packet(self) -> None:
        protocol = ProtocolSettings(
            frame_header=11,
            frame_header_size=2,
            length_field_size=8,
            length_field_format="float64",
            length_field_units="values",
            channels=8,
            channel_layout="interleaved",
        )
        channels = np.arange(80, dtype=np.float64).reshape(8, 10)
        packet = build_packet(10000.0, channels, protocol)
        frames: list[np.ndarray] = []
        frame_event = threading.Event()
        errors: list[str] = []

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as upstream:
            try:
                upstream.bind(("127.0.0.1", 0))
            except PermissionError as exc:
                self.skipTest(f"socket bind is not allowed in this environment: {exc}")
            upstream.listen(1)
            upstream.settimeout(2.0)
            port = upstream.getsockname()[1]
            server = TcpDataServer(
                DataServerSettings(
                    mode="client",
                    remote_host="127.0.0.1",
                    remote_port=port,
                    connect_timeout_seconds=0.5,
                    reconnect_interval_seconds=0.1,
                ),
                protocol,
                on_packet=lambda packet: None,
                on_frame=lambda frame: (frames.append(frame.channels.copy()), frame_event.set()),
                on_connection_state=lambda connected: None,
                on_bytes_received=lambda count: None,
                on_error=errors.append,
            )
            server.start()
            try:
                conn, _ = upstream.accept()
                with conn:
                    conn.sendall(packet)
                    self.assertTrue(frame_event.wait(2.0))
            finally:
                server.stop()

        self.assertFalse(errors)
        self.assertEqual(1, len(frames))
        self.assertTrue(np.array_equal(channels, frames[0]))

    def test_runtime_restart_data_server_respects_processing_state(self) -> None:
        runtime = RuntimeService(AppSettings())
        running_server = Mock()
        restarted_server = Mock()
        runtime._data_server = running_server
        runtime._build_data_server = Mock(return_value=restarted_server)  # type: ignore[method-assign]
        runtime._data_server_active = True

        runtime._restart_data_server()

        running_server.stop.assert_called_once()
        restarted_server.start.assert_called_once()
        self.assertIs(runtime._data_server, restarted_server)
        self.assertTrue(runtime.is_processing_active())

        paused_server = Mock()
        rebuilt_server = Mock()
        runtime._data_server = paused_server
        runtime._build_data_server = Mock(return_value=rebuilt_server)  # type: ignore[method-assign]
        runtime._data_server_active = False

        runtime._restart_data_server()

        paused_server.stop.assert_called_once()
        rebuilt_server.start.assert_not_called()
        self.assertIs(runtime._data_server, rebuilt_server)
        self.assertFalse(runtime.is_processing_active())

    def test_runtime_pause_and_resume_processing_update_state(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime._data_server = Mock()
        runtime._data_server_active = False
        runtime._queue.put_nowait(ChannelFrame(sample_rate=10.0, channels=np.zeros((8, 1))))

        runtime.resume_processing()

        runtime._data_server.start.assert_called_once()
        self.assertTrue(runtime.is_processing_active())

        runtime.pause_processing()

        runtime._data_server.stop.assert_called_once()
        self.assertFalse(runtime.is_processing_active())
        self.assertEqual(0, runtime.snapshot().queue_depth)

    def test_runtime_update_config_does_not_restart_data_server_when_protocol_and_network_are_unchanged(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime._restart_data_server = Mock()  # type: ignore[method-assign]

        runtime.update_config(
            {
                "processing": {"data1_rate": runtime._settings.processing.data1_rate},
                "data_server": {
                    "mode": runtime._settings.data_server.mode,
                    "host": runtime._settings.data_server.host,
                    "port": runtime._settings.data_server.port,
                    "remote_host": runtime._settings.data_server.remote_host,
                    "remote_port": runtime._settings.data_server.remote_port,
                },
                "protocol": {
                    "frame_header": runtime._settings.protocol.frame_header,
                    "frame_header_size": runtime._settings.protocol.frame_header_size,
                    "length_field_size": runtime._settings.protocol.length_field_size,
                    "length_field_format": runtime._settings.protocol.length_field_format,
                    "length_field_units": runtime._settings.protocol.length_field_units,
                    "byte_order": runtime._settings.protocol.byte_order,
                    "channel_layout": runtime._settings.protocol.channel_layout,
                },
                "storage": {"enabled": runtime._settings.storage.enabled},
            }
        )

        runtime._restart_data_server.assert_not_called()

    def test_runtime_update_config_restarts_data_server_when_protocol_changes(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime._restart_data_server = Mock()  # type: ignore[method-assign]

        runtime.update_config(
            {
                "protocol": {
                    "length_field_units": "bytes"
                    if runtime._settings.protocol.length_field_units == "values"
                    else "values"
                }
            }
        )

        runtime._restart_data_server.assert_called_once()

    def test_data_server_logs_when_connected_without_payload(self) -> None:
        conn = Mock()
        conn.getpeername.return_value = ("169.254.56.252", 3677)
        conn.recv.side_effect = [
            TimeoutError(),
            TimeoutError(),
            TimeoutError(),
            TimeoutError(),
            TimeoutError(),
            b"",
        ]
        states: list[bool] = []
        errors: list[str] = []
        server = TcpDataServer(
            DataServerSettings(mode="client", remote_host="169.254.56.252", remote_port=3677),
            ProtocolSettings(),
            on_packet=lambda packet: None,
            on_frame=lambda frame: None,
            on_connection_state=states.append,
            on_bytes_received=lambda count: None,
            on_error=errors.append,
        )

        with (
            patch("datalink_host.ingest.data_server.time.monotonic", side_effect=[0.0, 1.0, 2.0, 3.0, 4.0, 5.1]),
            patch("datalink_host.ingest.data_server.LOGGER.warning") as warning_mock,
        ):
            server._handle_connection(conn, manage_socket=False)

        self.assertEqual([True, False], states)
        self.assertFalse(errors)
        self.assertTrue(
            any("no payload bytes have arrived" in call.args[0] for call in warning_mock.call_args_list)
        )

    def test_data_server_logs_when_bytes_arrive_without_complete_packet(self) -> None:
        conn = Mock()
        conn.getpeername.return_value = ("169.254.56.252", 3677)
        conn.recv.side_effect = [
            b"\x00\x0b",
            TimeoutError(),
            TimeoutError(),
            TimeoutError(),
            TimeoutError(),
            TimeoutError(),
            b"",
        ]
        server = TcpDataServer(
            DataServerSettings(mode="client", remote_host="169.254.56.252", remote_port=3677),
            ProtocolSettings(),
            on_packet=lambda packet: None,
            on_frame=lambda frame: None,
            on_connection_state=lambda connected: None,
            on_bytes_received=lambda count: None,
            on_error=lambda message: None,
        )

        with (
            patch(
                "datalink_host.ingest.data_server.time.monotonic",
                side_effect=[0.0, 0.2, 0.3, 1.0, 2.0, 3.0, 4.0, 5.1, 5.2, 5.3],
            ),
            patch("datalink_host.ingest.data_server.LOGGER.warning") as warning_mock,
        ):
            server._handle_connection(conn, manage_socket=False)

        self.assertTrue(
            any("decoded 0 packets" in call.args[0] for call in warning_mock.call_args_list)
        )

    def test_round_trip_unsigned_value_count_length_field(self) -> None:
        settings = ProtocolSettings(
            frame_header=11,
            frame_header_size=2,
            length_field_size=8,
            length_field_format="uint",
            length_field_units="values",
            channels=8,
            channel_layout="channel-major",
        )
        channels = np.arange(80, dtype=np.float64).reshape(8, 10)
        payload = build_packet(10000.0, channels, settings)

        decoder = PacketDecoder(settings)
        packets = decoder.feed(payload)
        self.assertEqual(1, len(packets))

        frame = packet_to_frame(packets[0], settings)
        self.assertTrue(np.array_equal(channels, frame.channels))

    def test_runtime_processor_exception_updates_last_error(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime._pipeline = Mock()
        runtime._pipeline.process.side_effect = ValueError("boom")
        runtime._queue.put_nowait(ChannelFrame(sample_rate=10.0, channels=np.zeros((8, 1))))

        worker = threading.Thread(target=runtime._run_processor, daemon=True)
        worker.start()
        time.sleep(0.05)
        runtime._stop_event.set()
        worker.join(timeout=1.0)

        self.assertIn("Processing pipeline failed: boom", runtime.snapshot().last_error or "")

    def test_miniseed_writer_rotates_output_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = MiniSeedWriter(
                StorageSettings(enabled=True, root=Path(tmpdir), file_duration_seconds=1)
            )
            frame = ProcessedFrame(
                sample_rate=100.0,
                raw=np.zeros((8, 100), dtype=np.float64),
                unwrapped=np.zeros((8, 100), dtype=np.float64),
                data1=np.ones((8, 100), dtype=np.float64),
                data2=np.ones((8, 10), dtype=np.float64),
                received_at=1_700_000_000.0,
            )
            writer.write(frame)
            writer.close()

            data1_files = sorted((Path(tmpdir) / "data1").glob("*.mseed"))
            data2_files = sorted((Path(tmpdir) / "data2").glob("*.mseed"))
            self.assertEqual(8, len(data1_files))
            self.assertEqual(8, len(data2_files))

    def test_datalink_packet_encoding_and_stream_id(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(),
            StorageSettings(),
        )
        payload, stream_id = publisher._serialize_channel_packet(  # type: ignore[attr-defined]
            channel_index=0,
            values=np.ones(100, dtype=np.float64),
            sample_rate=100.0,
            received_at=1_700_000_000.0,
        )
        packet = publisher._encode_packet(f"WRITE {stream_id} 1 2 A {len(payload)}", payload)  # type: ignore[attr-defined]
        self.assertTrue(packet.startswith(b"DL"))
        self.assertEqual("SC_S0001_10_HSH/MSEED", stream_id)
        self.assertGreater(len(payload), 0)
        self.assertEqual(len(payload), publisher._extract_data_size(f"WRITE {stream_id} 1 2 A {len(payload)}"))  # type: ignore[attr-defined]

    def test_capture_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.dlhcap"
            writer = PacketCaptureWriter(path)
            writer.write_record(
                received_at=1.0,
                sample_rate=1000.0,
                payload_bytes=32,
                packet_bytes=b"example-packet",
            )
            writer.close()

            records = list(read_capture(path))
            self.assertEqual(1, len(records))
            self.assertEqual(b"example-packet", records[0].packet_bytes)
            self.assertEqual(1000.0, records[0].sample_rate)

    def test_compute_psd_returns_frequency_axis(self) -> None:
        signal = np.sin(2 * np.pi * 5 * np.arange(256) / 100.0)
        freqs, psd = compute_psd(signal, 100.0)
        self.assertEqual(freqs.shape, psd.shape)
        self.assertGreater(freqs[np.argmax(psd)], 0.0)

    def test_in_memory_logging_collects_messages(self) -> None:
        configure_logging()
        import logging

        logging.getLogger("test.logger").warning("buffer-check")
        logs = get_recent_logs(20)
        self.assertTrue(any("buffer-check" in line for line in logs))


if __name__ == "__main__":
    unittest.main()
