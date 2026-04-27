from __future__ import annotations

from collections import namedtuple
import io
import tempfile
import threading
import time
import unittest
import socket
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
from fastapi.testclient import TestClient
from obspy import Stream, read

from datalink_host.core.config import (
    AppSettings,
    DataLinkSettings,
    DataServerSettings,
    GnssSettings,
    ProtocolSettings,
    StorageSettings,
)
from datalink_host.core.logging import configure_logging, get_recent_logs
from datalink_host.debug.capture import PacketCaptureWriter, read_capture
from datalink_host.ingest.data_server import TcpDataServer
from datalink_host.ingest.protocol import PacketDecoder, build_packet, packet_to_frame
from datalink_host.models.messages import ChannelFrame, ProcessedFrame, TcpPacket
from datalink_host.processing.pipeline import ProcessingPipeline, compute_psd
from datalink_host.services.gnss_time import GnssStatus, GnssTimeService, format_timestamp_us, gnss_timestamp_to_us
from datalink_host.services.runtime import RuntimeService
from datalink_host.services.web_api import WebApiService, create_app
from datalink_host.storage.miniseed import MiniSeedWriter
from datalink_host.tools.sender_sim import _generate_channels, resolve_packet_samples
from datalink_host.transport.datalink import DataLinkPublisher, DataLinkSendError, PendingDataLinkPacket


class ProtocolTests(unittest.TestCase):
    def test_sender_sim_uses_fixed_packet_sample_count(self) -> None:
        channels = _generate_channels(8, 1000.0, 128, 0.0)

        self.assertEqual((8, 128), channels.shape)

    def test_sender_sim_resolves_packet_samples_from_count_or_duration(self) -> None:
        self.assertEqual(128, resolve_packet_samples(1000.0, 128, 1.0))
        self.assertEqual(250, resolve_packet_samples(1000.0, None, 0.25))
        self.assertEqual(1000, resolve_packet_samples(1000.0, None, None))

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

    def test_data_server_reuses_last_valid_sample_rate_after_reconnect_when_peer_reports_zero(self) -> None:
        protocol = ProtocolSettings()
        channels = np.arange(80, dtype=np.float64).reshape(8, 10)
        valid_packet = build_packet(1000.0, channels, protocol)
        zero_rate_packet = build_packet(0.0, channels, protocol)
        frame_rates: list[float] = []
        packet_rates: list[float] = []
        states: list[bool] = []
        errors: list[str] = []
        server = TcpDataServer(
            DataServerSettings(mode="client", remote_host="169.254.56.252", remote_port=3677),
            protocol,
            on_packet=lambda packet: packet_rates.append(packet.sample_rate),
            on_frame=lambda frame: frame_rates.append(frame.sample_rate),
            on_connection_state=states.append,
            on_bytes_received=lambda count: None,
            on_error=errors.append,
        )

        first_conn = Mock()
        first_conn.getpeername.return_value = ("169.254.56.252", 3677)
        first_conn.recv.side_effect = [valid_packet, b""]
        second_conn = Mock()
        second_conn.getpeername.return_value = ("169.254.56.252", 3677)
        second_conn.recv.side_effect = [zero_rate_packet, b""]

        server._handle_connection(first_conn, manage_socket=False)
        server._handle_connection(second_conn, manage_socket=False)

        self.assertFalse(errors)
        self.assertEqual([True, False, True, False], states)
        self.assertEqual([1000.0, 1000.0], packet_rates)
        self.assertEqual([1000.0, 1000.0], frame_rates)

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

    def test_runtime_update_config_accepts_storage_and_datalink_editable_fields(self) -> None:
        runtime = RuntimeService(AppSettings())
        channel_codes = ["EHZ", "EHN", "EHE", "BH1", "BH2", "BH3", "LHZ", "LHN"]

        updated = runtime.update_config(
            {
                "storage": {
                    "enabled": False,
                    "root": "./var/custom-storage",
                    "file_duration_seconds": 120,
                    "output_data_type": "INT32",
                    "int32_gain": 1000.0,
                    "network": "ZZ",
                    "station": "EDIT1",
                    "location": "20",
                    "channel_codes": channel_codes,
                },
                "datalink": {
                    "enabled": False,
                    "host": "192.0.2.10",
                    "port": 18000,
                    "stream_id_template": "{network}.{station}.{location}.{channel}",
                    "ack_required": False,
                    "send_data2": True,
                },
            }
        )

        self.assertEqual(channel_codes, updated["storage"]["channel_codes"])
        self.assertEqual("var/custom-storage", updated["storage"]["root"])
        self.assertEqual("int32", updated["storage"]["output_data_type"])
        self.assertEqual(1000.0, updated["storage"]["int32_gain"])
        self.assertEqual("{network}.{station}.{location}.{channel}", updated["datalink"]["stream_id_template"])
        self.assertEqual("192.0.2.10", updated["datalink"]["host"])
        self.assertEqual(18000, updated["datalink"]["port"])
        self.assertFalse(updated["datalink"]["ack_required"])
        self.assertTrue(updated["datalink"]["send_data2"])
        runtime._datalink.close()

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
                side_effect=[0.0, 0.2, 1.0, 2.0, 3.0, 4.0, 5.1],
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

    def test_processing_pipeline_reports_stable_downsample_rates_with_carry(self) -> None:
        pipeline = ProcessingPipeline(AppSettings().processing)
        first = pipeline.process(
            ChannelFrame(sample_rate=1000.0, channels=np.zeros((8, 15), dtype=np.float64), received_at=1.0)
        )
        second = pipeline.process(
            ChannelFrame(sample_rate=1000.0, channels=np.zeros((8, 15), dtype=np.float64), received_at=2.0)
        )

        self.assertEqual((8, 1), first.data1.shape)
        self.assertEqual((8, 2), second.data1.shape)
        self.assertEqual(100.0, first.data1_sample_rate)
        self.assertEqual(100.0, second.data1_sample_rate)
        self.assertEqual(10.0, first.data2_sample_rate)
        self.assertEqual(10.0, second.data2_sample_rate)

    def test_processing_pipeline_unwraps_across_frame_boundaries(self) -> None:
        pipeline = ProcessingPipeline(AppSettings().processing)
        first_channels = np.repeat(np.array([[2.5, 3.0, -2.8]], dtype=np.float64), 8, axis=0)
        second_channels = np.repeat(np.array([[-2.3, -1.8]], dtype=np.float64), 8, axis=0)

        first = pipeline.process(ChannelFrame(sample_rate=100.0, channels=first_channels, received_at=1.0))
        second = pipeline.process(ChannelFrame(sample_rate=100.0, channels=second_channels, received_at=2.0))

        stitched = np.concatenate([first.unwrapped[0], second.unwrapped[0]])
        expected = np.unwrap(np.concatenate([first_channels[0], second_channels[0]]))
        self.assertTrue(np.allclose(expected, stitched))

    def test_processing_pipeline_reset_clears_unwrap_state(self) -> None:
        pipeline = ProcessingPipeline(AppSettings().processing)
        first_channels = np.repeat(np.array([[2.5, 3.0, -2.8]], dtype=np.float64), 8, axis=0)
        second_channels = np.repeat(np.array([[-2.3, -1.8]], dtype=np.float64), 8, axis=0)

        pipeline.process(ChannelFrame(sample_rate=100.0, channels=first_channels, received_at=1.0))
        pipeline.reset()
        second = pipeline.process(ChannelFrame(sample_rate=100.0, channels=second_channels, received_at=2.0))

        self.assertTrue(np.array_equal(second.unwrapped[0], second_channels[0]))

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
                data1_sample_rate=100.0,
                data2=np.ones((8, 10), dtype=np.float64),
                data2_sample_rate=10.0,
                received_at=1_700_000_000.0,
                timestamp_us=1_700_000_000_000_000,
            )
            writer.write(frame)
            writer.close()

            data1_files = sorted(Path(tmpdir).glob("Data1-*/*.mseed"))
            data2_files = sorted(Path(tmpdir).glob("Data2-*/*.mseed"))
            self.assertEqual(8, len(data1_files))
            self.assertEqual(8, len(data2_files))

    def test_miniseed_writer_uses_frame_timestamp_as_segment_start(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = MiniSeedWriter(
                StorageSettings(enabled=True, root=Path(tmpdir), file_duration_seconds=1)
            )
            frame = ProcessedFrame(
                sample_rate=100.0,
                raw=np.zeros((8, 100), dtype=np.float64),
                unwrapped=np.zeros((8, 100), dtype=np.float64),
                data1=np.ones((8, 100), dtype=np.float64),
                data1_sample_rate=100.0,
                data2=np.zeros((8, 0), dtype=np.float64),
                data2_sample_rate=10.0,
                received_at=1_700_000_000.0,
                timestamp_us=1_700_000_000_123_456,
            )

            writer.write(frame)
            writer.close()

            data1_files = sorted(Path(tmpdir).glob("Data1-*/*.mseed"))
            self.assertTrue(data1_files)
            expected_name = "SC.S0001.20231114221320123.R.10.HSH.mseed"
            self.assertEqual(expected_name, data1_files[0].name)
            log_file = Path(tmpdir) / "log" / "SC.S0001.20231114221320123.R.10.LOG.log"
            self.assertTrue(log_file.is_file())
            self.assertIn("# MiniSEED sidecar log", log_file.read_text(encoding="utf-8"))
            stream = read(str(data1_files[0]))
            self.assertAlmostEqual(1_700_000_000.123456, float(stream[0].stats.starttime.timestamp), places=6)
            self.assertEqual(np.dtype("float32"), stream[0].data.dtype)
            self.assertEqual("FLOAT32", stream[0].stats.mseed.encoding)

    def test_miniseed_writer_appends_one_second_chunks_to_configured_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = MiniSeedWriter(
                StorageSettings(enabled=True, root=Path(tmpdir), file_duration_seconds=10)
            )

            first_frame = ProcessedFrame(
                sample_rate=100.0,
                raw=np.zeros((1, 100), dtype=np.float64),
                unwrapped=np.zeros((1, 100), dtype=np.float64),
                data1=np.ones((1, 100), dtype=np.float64),
                data1_sample_rate=100.0,
                data2=np.zeros((1, 0), dtype=np.float64),
                data2_sample_rate=10.0,
                received_at=1_700_000_000.0,
                timestamp_us=1_700_000_000_000_000,
            )
            second_frame = ProcessedFrame(
                sample_rate=100.0,
                raw=np.zeros((1, 100), dtype=np.float64),
                unwrapped=np.zeros((1, 100), dtype=np.float64),
                data1=np.full((1, 100), 2.0, dtype=np.float64),
                data1_sample_rate=100.0,
                data2=np.zeros((1, 0), dtype=np.float64),
                data2_sample_rate=10.0,
                received_at=1_700_000_001.0,
                timestamp_us=1_700_000_001_000_000,
            )

            writer.write(first_frame)
            data1_files = sorted(Path(tmpdir).glob("Data1-*/*.mseed"))
            self.assertEqual(1, len(data1_files))
            first_size = data1_files[0].stat().st_size
            first_stream = read(str(data1_files[0]))
            self.assertEqual(100, sum(trace.stats.npts for trace in first_stream))

            writer.write(second_frame)
            writer.close()

            data1_files = sorted(Path(tmpdir).glob("Data1-*/*.mseed"))
            self.assertEqual(1, len(data1_files))
            self.assertGreater(data1_files[0].stat().st_size, first_size)
            stream = read(str(data1_files[0]))
            stream.sort(keys=["starttime"])
            samples = np.concatenate([trace.data for trace in stream])
            self.assertEqual(200, samples.size)
            self.assertTrue(np.allclose(np.ones(100, dtype=np.float32), samples[:100]))
            self.assertTrue(np.allclose(np.full(100, 2.0, dtype=np.float32), samples[100:]))

    def test_miniseed_writer_can_store_int32_with_gain(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = MiniSeedWriter(
                StorageSettings(
                    enabled=True,
                    root=Path(tmpdir),
                    file_duration_seconds=1,
                    output_data_type="int32",
                    int32_gain=10.0,
                )
            )
            frame = ProcessedFrame(
                sample_rate=3.0,
                raw=np.zeros((1, 3), dtype=np.float64),
                unwrapped=np.zeros((1, 3), dtype=np.float64),
                data1=np.array([[1.24, -2.56, 3.14]], dtype=np.float64),
                data1_sample_rate=3.0,
                data2=np.zeros((1, 0), dtype=np.float64),
                data2_sample_rate=1.0,
                received_at=1_700_000_000.0,
                timestamp_us=1_700_000_000_000_000,
            )

            writer.write(frame)
            writer.close()

            data1_files = sorted(Path(tmpdir).glob("Data1-*/*.mseed"))
            self.assertEqual(1, len(data1_files))
            stream = read(str(data1_files[0]))
            self.assertEqual(np.dtype("int32"), stream[0].data.dtype)
            self.assertEqual("INT32", stream[0].stats.mseed.encoding)
            self.assertEqual([12, -26, 31], stream[0].data.tolist())

    def test_miniseed_writer_reports_storage_disk_usage(self) -> None:
        usage_result = namedtuple("usage", "total used free")
        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "datalink_host.storage.miniseed.shutil.disk_usage",
            return_value=usage_result(total=1_000, used=250, free=750),
        ):
            writer = MiniSeedWriter(StorageSettings(enabled=True, root=Path(tmpdir)))

            usage = writer.disk_usage()

            self.assertIsNotNone(usage)
            assert usage is not None
            self.assertEqual(1_000, usage.total_bytes)
            self.assertEqual(250, usage.used_bytes)
            self.assertEqual(750, usage.free_bytes)
            self.assertAlmostEqual(25.0, usage.usage_percent)

    def test_miniseed_writer_deletes_oldest_file_when_disk_space_is_low(self) -> None:
        usage_result = namedtuple("usage", "total used free")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            old_dir = root / "Data1-01"
            log_dir = root / "log"
            old_dir.mkdir()
            log_dir.mkdir()
            old_mseed = old_dir / "SC.S0001.20200101000000000.R.10.HSH.mseed"
            old_log = old_dir / "SC.S0001.20200101000000000.R.10.LOG.log"
            shared_old_log = log_dir / "SC.S0001.20200101000000000.R.10.LOG.log"
            old_mseed.write_bytes(b"old")
            old_log.write_text("old log\n", encoding="utf-8")
            shared_old_log.write_text("shared old log\n", encoding="utf-8")

            def disk_usage(_: Path) -> object:
                if old_mseed.exists():
                    return usage_result(total=20_000_000, used=19_999_999, free=1)
                return usage_result(total=20_000_000, used=1_000, free=19_999_000)

            writer = MiniSeedWriter(
                StorageSettings(enabled=True, root=root, file_duration_seconds=1)
            )
            frame = ProcessedFrame(
                sample_rate=100.0,
                raw=np.zeros((1, 100), dtype=np.float64),
                unwrapped=np.zeros((1, 100), dtype=np.float64),
                data1=np.ones((1, 100), dtype=np.float64),
                data1_sample_rate=100.0,
                data2=np.zeros((1, 0), dtype=np.float64),
                data2_sample_rate=10.0,
                received_at=1_700_000_000.0,
                timestamp_us=1_700_000_000_000_000,
            )

            with patch("datalink_host.storage.miniseed.shutil.disk_usage", side_effect=disk_usage):
                writer.write(frame)
                writer.close()

            self.assertFalse(old_mseed.exists())
            self.assertFalse(old_log.exists())
            self.assertFalse(shared_old_log.exists())
            new_files = sorted(root.glob("Data1-*/*.mseed"))
            self.assertEqual(1, len(new_files))
            self.assertNotEqual(old_mseed.name, new_files[0].name)

    def test_miniseed_writer_rolls_2s_files_from_first_gnss_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = MiniSeedWriter(
                StorageSettings(enabled=True, root=Path(tmpdir), file_duration_seconds=2)
            )

            for index in range(20):
                frame = ProcessedFrame(
                    sample_rate=5.0,
                    raw=np.zeros((1, 1), dtype=np.float64),
                    unwrapped=np.zeros((1, 1), dtype=np.float64),
                    data1=np.ones((1, 1), dtype=np.float64),
                    data1_sample_rate=5.0,
                    data2=np.zeros((1, 0), dtype=np.float64),
                    data2_sample_rate=1.0,
                    received_at=1_700_000_000.0 + (index * 0.2),
                    timestamp_us=1_700_000_000_000_000 + (index * 200_000),
                )
                writer.write(frame)
            writer.close()

            data1_files = sorted(Path(tmpdir).glob("Data1-*/*.mseed"))
            self.assertEqual(2, len(data1_files))
            first_stream = read(str(data1_files[0]))
            second_stream = read(str(data1_files[1]))
            self.assertAlmostEqual(1_700_000_000.0, float(first_stream[0].stats.starttime.timestamp), places=6)
            self.assertAlmostEqual(1_700_000_002.0, float(second_stream[0].stats.starttime.timestamp), places=6)

    def test_datalink_packet_encoding_and_stream_id(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(),
            StorageSettings(),
        )
        packets = publisher._serialize_channel_packets(  # type: ignore[attr-defined]
            group_name="data1",
            channel_index=0,
            values=np.ones(100, dtype=np.float64),
            sample_rate=100.0,
            timestamp_us=1_700_000_000_000_000,
        )
        self.assertGreater(len(packets), 0)
        for payload, stream_id, start_time, end_time in packets:
            packet = publisher._encode_packet(f"WRITE {stream_id} 1 2 A {len(payload)}", payload)  # type: ignore[attr-defined]
            self.assertTrue(packet.startswith(b"DL"))
            self.assertEqual("SC_S0001_10_HSH/MSEED", stream_id)
            self.assertGreater(len(payload), 0)
            self.assertLess(start_time, end_time)
            self.assertEqual(len(payload), publisher._extract_data_size(f"WRITE {stream_id} 1 2 A {len(payload)}"))  # type: ignore[attr-defined]
        publisher.close()

    def test_datalink_parses_packet_size_from_server_identification(self) -> None:
        self.assertEqual(
            512,
            DataLinkPublisher._parse_packet_size("ID DataLink 1.0 :: DLPROTO:1.0 PACKETSIZE:512 WRITE"),
        )
        self.assertIsNone(DataLinkPublisher._parse_packet_size("ID DataLink 1.0 :: WRITE"))

    def test_datalink_payload_uses_float32_miniseed_encoding(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(),
            StorageSettings(),
        )
        packets = publisher._serialize_channel_packets(  # type: ignore[attr-defined]
            group_name="data1",
            channel_index=0,
            values=np.array([1.25, 2.5, 3.75], dtype=np.float64),
            sample_rate=100.0,
            timestamp_us=1_700_000_000_000_000,
        )

        self.assertEqual(1, len(packets))
        payload, _, _, _ = packets[0]
        stream = read(io.BytesIO(payload), format="MSEED")
        self.assertEqual(np.dtype("float32"), stream[0].data.dtype)
        self.assertEqual("FLOAT32", stream[0].stats.mseed.encoding)
        publisher.close()

    def test_datalink_payload_can_use_int32_miniseed_encoding_with_gain(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(),
            StorageSettings(output_data_type="int32", int32_gain=10.0),
        )
        packets = publisher._serialize_channel_packets(  # type: ignore[attr-defined]
            group_name="data1",
            channel_index=0,
            values=np.array([1.24, -2.56, 3.14], dtype=np.float64),
            sample_rate=100.0,
            timestamp_us=1_700_000_000_000_000,
        )

        self.assertEqual(1, len(packets))
        payload, _, _, _ = packets[0]
        stream = read(io.BytesIO(payload), format="MSEED")
        self.assertEqual(np.dtype("int32"), stream[0].data.dtype)
        self.assertEqual("INT32", stream[0].stats.mseed.encoding)
        self.assertEqual([12, -26, 31], stream[0].data.tolist())
        publisher.close()

    def test_datalink_serialization_splits_packets_to_match_smaller_limit(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(),
            StorageSettings(),
        )
        packets = publisher._serialize_channel_packets(  # type: ignore[attr-defined]
            group_name="data1",
            channel_index=0,
            values=np.ones(100, dtype=np.float64),
            sample_rate=100.0,
            timestamp_us=1_700_000_000_000_000,
            max_payload_bytes=256,
        )

        self.assertEqual(2, len(packets))
        self.assertTrue(all(len(payload) <= 256 for payload, *_ in packets))
        self.assertAlmostEqual(1_700_000_000.0, packets[0][2], places=6)
        self.assertAlmostEqual(1_700_000_000.5, packets[0][3], places=6)
        self.assertAlmostEqual(1_700_000_000.5, packets[1][2], places=6)
        self.assertAlmostEqual(1_700_000_001.0, packets[1][3], places=6)
        publisher.close()

    def test_datalink_miniseed_sequence_numbers_increment_across_packets(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(),
            StorageSettings(),
        )
        packets = publisher._serialize_channel_packets(  # type: ignore[attr-defined]
            group_name="data1",
            channel_index=0,
            values=np.ones(100, dtype=np.float64),
            sample_rate=100.0,
            timestamp_us=1_700_000_000_000_000,
            max_payload_bytes=256,
        )
        self.assertEqual(["000001", "000002"], [payload[:6].decode("ascii") for payload, *_ in packets])

        next_packets = publisher._serialize_channel_packets(  # type: ignore[attr-defined]
            group_name="data1",
            channel_index=0,
            values=np.ones(10, dtype=np.float64),
            sample_rate=100.0,
            timestamp_us=1_700_000_001_000_000,
            max_payload_bytes=256,
        )
        self.assertEqual("000003", next_packets[0][0][:6].decode("ascii"))
        publisher.close()

    def test_datalink_miniseed_sequence_numbers_advance_by_record_count(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(),
            StorageSettings(),
        )
        raw_first_payload = publisher._encode_miniseed_record(  # type: ignore[attr-defined]
            channel_index=0,
            values=np.ones(10_000, dtype=np.float32),
            sample_rate=100.0,
            start_time=1_700_000_000.0,
            storage_settings=StorageSettings(),
            record_length_bytes=4096,
        )
        first_payload = publisher._assign_mseed_sequence_numbers(raw_first_payload, 4096)  # type: ignore[attr-defined]
        self.assertEqual("000001", first_payload[:6].decode("ascii"))
        self.assertEqual("000002", first_payload[4096:4102].decode("ascii"))

        raw_second_payload = publisher._encode_miniseed_record(  # type: ignore[attr-defined]
            channel_index=0,
            values=np.ones(100, dtype=np.float32),
            sample_rate=100.0,
            start_time=1_700_000_100.0,
            storage_settings=StorageSettings(),
            record_length_bytes=4096,
        )
        second_payload = publisher._assign_mseed_sequence_numbers(raw_second_payload, 4096)  # type: ignore[attr-defined]
        record_count = len(first_payload) // 4096
        self.assertEqual(f"{record_count + 1:06d}", second_payload[:6].decode("ascii"))
        publisher.close()

    def test_datalink_miniseed_encoder_uses_provided_sequence_number(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(),
            StorageSettings(),
        )
        payload = publisher._encode_miniseed_record(  # type: ignore[attr-defined]
            channel_index=0,
            values=np.ones(10_000, dtype=np.float32),
            sample_rate=100.0,
            start_time=1_700_000_000.0,
            storage_settings=StorageSettings(),
            record_length_bytes=4096,
            sequence_number=42,
        )
        self.assertEqual("000042", payload[:6].decode("ascii"))
        self.assertEqual("000043", payload[4096:4102].decode("ascii"))
        publisher.close()

    def test_datalink_send_data2_uses_distinct_stream_suffix_without_group_placeholder(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(send_data2=True),
            StorageSettings(),
        )
        data1_packets = publisher._serialize_channel_packets(  # type: ignore[attr-defined]
            group_name="data1",
            channel_index=0,
            values=np.ones(10, dtype=np.float64),
            sample_rate=100.0,
            timestamp_us=1_700_000_000_000_000,
        )
        data2_packets = publisher._serialize_channel_packets(  # type: ignore[attr-defined]
            group_name="data2",
            channel_index=0,
            values=np.ones(10, dtype=np.float64),
            sample_rate=10.0,
            timestamp_us=1_700_000_000_000_000,
        )
        self.assertTrue(all(packet[1].endswith("_data1/MSEED") for packet in data1_packets))
        self.assertTrue(all(packet[1].endswith("_data2/MSEED") for packet in data2_packets))
        publisher.close()

    def test_datalink_packet_time_window_matches_frame_duration(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(),
            StorageSettings(),
        )
        packets = publisher._serialize_channel_packets(  # type: ignore[attr-defined]
            group_name="data1",
            channel_index=0,
            values=np.ones(100, dtype=np.float64),
            sample_rate=100.0,
            timestamp_us=1_700_000_000_000_000,
        )
        self.assertEqual(1, len(packets))
        _, _, start_time, end_time = packets[0]
        self.assertAlmostEqual(1_700_000_000.0, start_time, places=6)
        self.assertAlmostEqual(1_700_000_001.0, end_time, places=6)
        publisher.close()

    def test_datalink_uses_continuous_timeline_after_first_timestamp(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(),
            StorageSettings(),
        )

        first_packets = publisher._serialize_channel_packets(  # type: ignore[attr-defined]
            group_name="data1",
            channel_index=0,
            values=np.ones(100, dtype=np.float64),
            sample_rate=100.0,
            timestamp_us=1_700_000_000_000_000,
        )
        second_packets = publisher._serialize_channel_packets(  # type: ignore[attr-defined]
            group_name="data1",
            channel_index=0,
            values=np.ones(100, dtype=np.float64),
            sample_rate=100.0,
            timestamp_us=1_700_000_001_123_456,
        )

        self.assertAlmostEqual(1_700_000_000.0, first_packets[0][2], places=6)
        self.assertAlmostEqual(1_700_000_001.0, second_packets[0][2], places=6)
        stream = read(io.BytesIO(second_packets[0][0]), format="MSEED")
        self.assertAlmostEqual(1_700_000_001.0, float(stream[0].stats.starttime.timestamp), places=6)
        publisher.close()

    def test_storage_and_datalink_payloads_have_matching_times_and_samples(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage_settings = StorageSettings(
                enabled=True,
                root=Path(tmpdir),
                file_duration_seconds=10,
            )
            writer = MiniSeedWriter(storage_settings)
            publisher = DataLinkPublisher(
                DataLinkSettings(send_data2=True),
                storage_settings,
            )
            datalink_payloads: dict[tuple[str, int], list[bytes]] = {}

            for frame_index, timestamp_us in enumerate(
                (1_700_000_000_000_000, 1_700_000_001_123_456)
            ):
                data1 = np.vstack(
                    [
                        np.arange(100, dtype=np.float64) + channel_index * 1_000 + frame_index * 100
                        for channel_index in range(8)
                    ]
                )
                data2 = np.vstack(
                    [
                        np.arange(10, dtype=np.float64) + channel_index * 100 + frame_index * 10
                        for channel_index in range(8)
                    ]
                )
                frame = ProcessedFrame(
                    sample_rate=100.0,
                    raw=np.zeros((8, 100), dtype=np.float64),
                    unwrapped=np.zeros((8, 100), dtype=np.float64),
                    data1=data1,
                    data1_sample_rate=100.0,
                    data2=data2,
                    data2_sample_rate=10.0,
                    received_at=1_700_000_000.0 + frame_index,
                    timestamp_us=timestamp_us,
                )

                writer.write(frame)
                for group_name, channels, sample_rate in (
                    ("data1", data1, 100.0),
                    ("data2", data2, 10.0),
                ):
                    for channel_index in range(6):
                        packets = publisher._serialize_channel_packets(  # type: ignore[attr-defined]
                            group_name=group_name,
                            channel_index=channel_index,
                            values=channels[channel_index],
                            sample_rate=sample_rate,
                            timestamp_us=timestamp_us,
                        )
                        datalink_payloads.setdefault((group_name, channel_index), []).extend(
                            payload for payload, *_ in packets
                        )

            writer.close()

            for group_name, sample_rate in (("data1", 100.0), ("data2", 10.0)):
                for channel_index in range(6):
                    storage_path = (
                        Path(tmpdir) / f"{group_name.title()}-{channel_index + 1:02d}"
                    )
                    storage_files = sorted(storage_path.glob("*.mseed"))
                    self.assertEqual(1, len(storage_files))
                    storage_stream = read(str(storage_files[0]))
                    datalink_stream = Stream()
                    for payload in datalink_payloads[(group_name, channel_index)]:
                        datalink_stream += read(io.BytesIO(payload), format="MSEED")

                    storage_stream.sort(keys=["starttime"])
                    datalink_stream.sort(keys=["starttime"])
                    storage_stream.merge(method=1)
                    datalink_stream.merge(method=1)
                    self.assertEqual(1, len(storage_stream))
                    self.assertEqual(1, len(datalink_stream))
                    stored_trace = storage_stream[0]
                    sent_trace = datalink_stream[0]
                    self.assertEqual(stored_trace.id, sent_trace.id)
                    self.assertAlmostEqual(sample_rate, stored_trace.stats.sampling_rate, places=6)
                    self.assertAlmostEqual(sample_rate, sent_trace.stats.sampling_rate, places=6)
                    self.assertAlmostEqual(
                        float(stored_trace.stats.starttime.timestamp),
                        float(sent_trace.stats.starttime.timestamp),
                        places=6,
                    )
                    self.assertEqual(stored_trace.stats.npts, sent_trace.stats.npts)
                    self.assertEqual(stored_trace.stats.mseed.encoding, sent_trace.stats.mseed.encoding)
                    self.assertTrue(np.array_equal(stored_trace.data, sent_trace.data))
            publisher.close()

    def test_datalink_publish_uses_background_sender(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(),
            StorageSettings(),
        )
        sent = threading.Event()

        def _fake_write(item: object) -> None:
            sent.set()

        publisher._write_packet_locked = _fake_write  # type: ignore[method-assign]
        frame = ProcessedFrame(
            sample_rate=100.0,
            raw=np.zeros((8, 100), dtype=np.float64),
            unwrapped=np.zeros((8, 100), dtype=np.float64),
            data1=np.ones((8, 100), dtype=np.float64),
            data1_sample_rate=100.0,
            data2=np.zeros((8, 0), dtype=np.float64),
            data2_sample_rate=10.0,
            received_at=1_700_000_000.0,
            timestamp_us=1_700_000_000_000_000,
        )

        publisher.publish(frame)

        self.assertTrue(sent.wait(1.0))
        publisher.close()

    def test_datalink_publish_skips_temperature_channels(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(send_data2=True),
            StorageSettings(),
        )
        enqueued: list[str] = []
        publisher._enqueue_packet = lambda item: enqueued.append(item.stream_id)  # type: ignore[method-assign]
        frame = ProcessedFrame(
            sample_rate=100.0,
            raw=np.zeros((8, 10), dtype=np.float64),
            unwrapped=np.zeros((8, 10), dtype=np.float64),
            data1=np.ones((8, 10), dtype=np.float64),
            data1_sample_rate=100.0,
            data2=np.ones((8, 10), dtype=np.float64),
            data2_sample_rate=10.0,
            received_at=1_700_000_000.0,
            timestamp_us=1_700_000_000_000_000,
        )

        publisher.publish(frame)

        self.assertEqual(12, len(enqueued))
        self.assertFalse(any("_HTH" in stream_id or "_HTZ" in stream_id for stream_id in enqueued))
        publisher.close()

    def test_datalink_background_sender_records_error_without_blocking_publish(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(),
            StorageSettings(),
        )

        def _failing_write(item: object) -> None:
            raise TimeoutError("timed out")

        publisher._write_packet_locked = _failing_write  # type: ignore[method-assign]
        frame = ProcessedFrame(
            sample_rate=100.0,
            raw=np.zeros((8, 100), dtype=np.float64),
            unwrapped=np.zeros((8, 100), dtype=np.float64),
            data1=np.ones((8, 100), dtype=np.float64),
            data1_sample_rate=100.0,
            data2=np.zeros((8, 0), dtype=np.float64),
            data2_sample_rate=10.0,
            received_at=1_700_000_000.0,
            timestamp_us=1_700_000_000_000_000,
        )

        publisher.publish(frame)
        time.sleep(0.05)

        self.assertIn("timed out", publisher.stats().last_error or "")
        publisher.close()

    def test_datalink_write_error_includes_server_payload(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(),
            StorageSettings(),
        )

        class _FakeSocket:
            def sendall(self, _data: bytes) -> None:
                return

            def close(self) -> None:
                return

        publisher._ensure_connected_locked = lambda: _FakeSocket()  # type: ignore[method-assign]
        publisher._read_packet = lambda _sock: (  # type: ignore[method-assign]
            "ERROR 0 23",
            b"write permission denied",
        )

        with self.assertRaises(RuntimeError) as excinfo:
            publisher._write_packet_locked(  # type: ignore[attr-defined]
                PendingDataLinkPacket(
                    stream_id="SC_S0001_10_HSH/MSEED",
                    payload=b"x" * 512,
                    start_time=1_700_000_000.0,
                    end_time=1_700_000_001.0,
                    ack_required=True,
                )
            )

        self.assertIn("ERROR 0 23", str(excinfo.exception))
        self.assertIn("write permission denied", str(excinfo.exception))
        publisher.close()

    def test_datalink_background_sender_drops_permanent_errors_without_retrying(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(enabled=True, reconnect_interval_seconds=1.0),
            StorageSettings(),
        )
        sent = threading.Event()
        calls: list[str] = []

        def _write(item: PendingDataLinkPacket) -> None:
            calls.append(item.stream_id)
            if item.stream_id.endswith("HSH/MSEED"):
                raise DataLinkSendError("write permission denied", retryable=False)
            sent.set()

        publisher._write_packet_locked = _write  # type: ignore[method-assign]
        publisher._enqueue_packet(
            PendingDataLinkPacket(
                stream_id="SC_S0001_10_HSH/MSEED",
                payload=b"x" * 256,
                start_time=1_700_000_000.0,
                end_time=1_700_000_000.5,
                ack_required=True,
            )
        )
        publisher._enqueue_packet(
            PendingDataLinkPacket(
                stream_id="SC_S0001_10_HSZ/MSEED",
                payload=b"y" * 256,
                start_time=1_700_000_000.5,
                end_time=1_700_000_001.0,
                ack_required=True,
            )
        )

        self.assertTrue(sent.wait(0.2))
        self.assertEqual(
            ["SC_S0001_10_HSH/MSEED", "SC_S0001_10_HSZ/MSEED"],
            calls[:2],
        )
        publisher.close()

    def test_datalink_background_sender_retries_failed_packet_in_memory(self) -> None:
        publisher = DataLinkPublisher(
            DataLinkSettings(enabled=True, reconnect_interval_seconds=0.01),
            StorageSettings(),
        )
        sent = threading.Event()
        calls = 0

        def _flaky_write(item: object) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise TimeoutError("timed out")
            sent.set()

        publisher._write_packet_locked = _flaky_write  # type: ignore[method-assign]
        publisher._enqueue_packet(
            PendingDataLinkPacket(
                stream_id="SC_S0001_10_HSH/MSEED",
                payload=b"x" * 512,
                start_time=1_700_000_000.0,
                end_time=1_700_000_001.0,
                ack_required=True,
            )
        )

        self.assertTrue(sent.wait(1.0))
        self.assertGreaterEqual(calls, 2)
        publisher.close()

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

    def test_gnss_timestamp_parsing_supports_debug_and_deploy_modes(self) -> None:
        debug_ts = gnss_timestamp_to_us("2026-04-09 13:30:32 000000010", "debug")
        deploy_ts = gnss_timestamp_to_us("20260409133032000000", "deploy")
        deploy_dotted_ts = gnss_timestamp_to_us("2026-04-22 08:18:48.900000", "deploy")

        self.assertEqual("20260409133032000000", format_timestamp_us(debug_ts))
        self.assertEqual("20260409133032000000", format_timestamp_us(deploy_ts))
        self.assertEqual("20260422081848900000", format_timestamp_us(deploy_dotted_ts))

    def test_gnss_current_time_stays_monotonic_when_device_repeats_whole_seconds(self) -> None:
        service = GnssTimeService(GnssSettings(enabled=True, mode="deploy", port="tty.usbmodem"))
        service._record_timestamp(1_700_000_000_000_000, recorded_at_monotonic=10.0)

        with patch("datalink_host.services.gnss_time.time.monotonic", return_value=10.05):
            first_now = service.current_time_us()

        service._record_timestamp(1_700_000_000_000_000, recorded_at_monotonic=10.10)
        with patch("datalink_host.services.gnss_time.time.monotonic", return_value=10.10):
            second_now = service.current_time_us()

        assert first_now is not None
        assert second_now is not None
        self.assertGreaterEqual(second_now, first_now)

    def test_gnss_service_discards_stale_serial_input_when_connection_opens(self) -> None:
        class _FakeSerial:
            def __init__(self) -> None:
                self.in_waiting = 4096
                self.reset_called = False

            def reset_input_buffer(self) -> None:
                self.reset_called = True
                self.in_waiting = 0

        conn = _FakeSerial()

        dropped_bytes = GnssTimeService._discard_stale_input(conn)  # type: ignore[arg-type]

        self.assertEqual(4096, dropped_bytes)
        self.assertTrue(conn.reset_called)

    def test_runtime_wall_clock_uses_gnss_current_time_when_available(self) -> None:
        runtime = RuntimeService(
            AppSettings(gnss=GnssSettings(enabled=True, mode="deploy", port="tty.usbmodem"))
        )
        runtime._gnss_time.current_time_us = Mock(  # type: ignore[method-assign]
            return_value=1_700_000_123_456_789
        )

        runtime._set_control_connected(True)
        packet = TcpPacket(sample_rate=100.0, payload_bytes=0, payload=b"", raw_bytes=b"")
        snapshot = runtime.snapshot()

        self.assertAlmostEqual(1_700_000_123.456789, packet.received_at, places=6)
        self.assertAlmostEqual(1_700_000_123.456789, snapshot.updated_at, places=6)
        runtime._datalink.close()

    def test_runtime_uses_next_raw_gnss_timestamp_as_packet_end_time(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime._settings.gnss = GnssSettings(enabled=True, mode="deploy", port="tty.usbmodem")
        runtime._gnss_time.wait_for_next_timestamp_us = Mock(  # type: ignore[method-assign]
            return_value=1_700_000_000_500_000
        )
        runtime._gnss_time.status = Mock(  # type: ignore[method-assign]
            return_value=GnssStatus(
                enabled=True,
                connected=True,
                mode="deploy",
                port="tty.usbmodem",
                baudrate=115200,
                poll_interval_seconds=0.1,
                last_timestamp_us=1_700_000_000_000_000,
                last_error=None,
            )
        )

        frame = ChannelFrame(sample_rate=1000.0, channels=np.zeros((1, 10), dtype=np.float32))
        runtime._on_frame(frame)
        snapshot = runtime.snapshot()

        self.assertEqual("20231114221320000000", snapshot.gnss_last_timestamp)
        self.assertEqual(1_700_000_000_490_000, frame.timestamp_us)

    def test_runtime_without_gnss_uses_host_receive_time_as_frame_end(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime._settings.gnss = GnssSettings(enabled=False)

        frame = ChannelFrame(
            sample_rate=1000.0,
            channels=np.zeros((1, 1000), dtype=np.float32),
            received_at=1_700_000_001.250,
        )

        timestamp_us, used_fallback, error = runtime._resolve_frame_timestamp(frame)

        self.assertEqual(1_700_000_000_250_000, timestamp_us)
        self.assertFalse(used_fallback)
        self.assertIsNone(error)
        runtime._datalink.close()

    def test_runtime_with_gnss_enabled_but_no_port_uses_host_receive_time(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime._settings.gnss = GnssSettings(enabled=True, port="")
        runtime._gnss_time.wait_for_next_timestamp_us = Mock()  # type: ignore[method-assign]

        frame = ChannelFrame(
            sample_rate=1000.0,
            channels=np.zeros((1, 1000), dtype=np.float32),
            received_at=1_700_000_001.250,
        )

        timestamp_us, used_fallback, error = runtime._resolve_frame_timestamp(frame)

        self.assertEqual(1_700_000_000_250_000, timestamp_us)
        self.assertFalse(used_fallback)
        self.assertIsNone(error)
        runtime._gnss_time.wait_for_next_timestamp_us.assert_not_called()
        runtime._datalink.close()

    def test_runtime_subtracts_frame_duration_from_packet_end_gnss_timestamp(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime._settings.gnss = GnssSettings(
            enabled=True,
            mode="deploy",
            port="tty.usbmodem",
        )
        runtime._gnss_time.wait_for_next_timestamp_us = Mock(  # type: ignore[method-assign]
            return_value=1_700_000_240_000_000
        )
        runtime._gnss_time.status = Mock(  # type: ignore[method-assign]
            return_value=GnssStatus(
                enabled=True,
                connected=True,
                mode="deploy",
                port="tty.usbmodem",
                baudrate=115200,
                poll_interval_seconds=0.1,
                last_timestamp_us=1_700_000_240_000_000,
                last_error=None,
            )
        )

        frame = ChannelFrame(
            sample_rate=1000.0,
            channels=np.zeros((1, 1000), dtype=np.float32),
            received_at=1_700_000_240.243,
        )
        timestamp_us, used_fallback, error = runtime._resolve_frame_timestamp(frame)

        self.assertEqual(1_700_000_239_000_000, timestamp_us)
        self.assertFalse(used_fallback)
        self.assertIsNone(error)
        runtime._datalink.close()

    def test_runtime_uses_previous_frame_timestamp_when_gnss_is_unavailable(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime._settings.gnss = GnssSettings(enabled=True, mode="debug", port="tty.usbmodem")
        runtime._last_frame_start_us = 1_700_000_000_000_000
        runtime._last_frame_duration_us = 1_000_000
        runtime._gnss_time.wait_for_next_timestamp_us = Mock(return_value=None)  # type: ignore[method-assign]
        runtime._gnss_time.status = Mock(  # type: ignore[method-assign]
            return_value=GnssStatus(
                enabled=True,
                connected=False,
                mode="debug",
                port="tty.usbmodem",
                baudrate=115200,
                poll_interval_seconds=0.1,
                last_timestamp_us=None,
                last_error="gnss offline",
            )
        )

        frame = ChannelFrame(
            sample_rate=100.0,
            channels=np.zeros((8, 100), dtype=np.float64),
            received_at=1_700_000_001.0,
        )
        timestamp_us, used_fallback, error = runtime._resolve_frame_timestamp(frame)

        self.assertEqual(1_700_000_001_000_000, timestamp_us)
        self.assertTrue(used_fallback)
        self.assertIn("gnss offline", error or "")
        runtime._datalink.close()

    def test_runtime_extends_initial_gnss_wait_until_first_timestamp(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime._settings.gnss = GnssSettings(
            enabled=True,
            mode="deploy",
            port="tty.usbmodem",
            timestamp_interval_seconds=1.0,
            packet_timestamp_timeout_seconds=1.0,
            serial_timeout_seconds=0.1,
        )
        runtime._gnss_time.wait_for_next_timestamp_us = Mock(  # type: ignore[method-assign]
            return_value=1_700_000_001_000_000
        )
        runtime._gnss_time.status = Mock(  # type: ignore[method-assign]
            return_value=GnssStatus(
                enabled=True,
                connected=True,
                mode="deploy",
                port="tty.usbmodem",
                baudrate=115200,
                poll_interval_seconds=0.1,
                last_timestamp_us=None,
                last_error=None,
            )
        )

        frame = ChannelFrame(
            sample_rate=1000.0,
            channels=np.zeros((1, 1000), dtype=np.float32),
            received_at=1_700_000_000.243,
        )
        timestamp_us, used_fallback, error = runtime._resolve_frame_timestamp(frame)

        self.assertEqual(1_700_000_000_000_000, timestamp_us)
        self.assertFalse(used_fallback)
        self.assertIsNone(error)
        wait_timeout = runtime._gnss_time.wait_for_next_timestamp_us.call_args.args[0]
        self.assertAlmostEqual(5.0, wait_timeout)
        runtime._datalink.close()

    def test_runtime_uses_host_time_when_initial_gnss_wait_times_out(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime._settings.gnss = GnssSettings(
            enabled=True,
            mode="deploy",
            port="tty.usbmodem",
            timestamp_interval_seconds=1.0,
            packet_timestamp_timeout_seconds=1.0,
        )
        runtime._gnss_time.wait_for_next_timestamp_us = Mock(return_value=None)  # type: ignore[method-assign]
        runtime._gnss_time.status = Mock(  # type: ignore[method-assign]
            return_value=GnssStatus(
                enabled=True,
                connected=True,
                mode="deploy",
                port="tty.usbmodem",
                baudrate=115200,
                poll_interval_seconds=0.1,
                last_timestamp_us=None,
                last_error=None,
            )
        )

        frame = ChannelFrame(
            sample_rate=1000.0,
            channels=np.zeros((1, 1000), dtype=np.float32),
            received_at=1_700_000_001.250,
        )
        timestamp_us, used_fallback, error = runtime._resolve_frame_timestamp(frame)

        self.assertEqual(1_700_000_000_250_000, timestamp_us)
        self.assertTrue(used_fallback)
        self.assertIn("host received timestamp fallback", error or "")
        self.assertNotIn("no fallback timestamp is available", error or "")
        wait_timeout = runtime._gnss_time.wait_for_next_timestamp_us.call_args.args[0]
        self.assertAlmostEqual(5.0, wait_timeout)
        runtime._datalink.close()

    def test_runtime_uses_regular_gnss_wait_after_initial_grace_is_consumed(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime._settings.gnss = GnssSettings(
            enabled=True,
            mode="deploy",
            port="tty.usbmodem",
            timestamp_interval_seconds=1.0,
            packet_timestamp_timeout_seconds=1.0,
        )
        runtime._gnss_time.wait_for_next_timestamp_us = Mock(return_value=None)  # type: ignore[method-assign]
        runtime._gnss_time.status = Mock(  # type: ignore[method-assign]
            return_value=GnssStatus(
                enabled=True,
                connected=True,
                mode="deploy",
                port="tty.usbmodem",
                baudrate=115200,
                poll_interval_seconds=0.1,
                last_timestamp_us=None,
                last_error=None,
            )
        )

        first_frame = ChannelFrame(
            sample_rate=1000.0,
            channels=np.zeros((1, 1000), dtype=np.float32),
            received_at=1_700_000_001.250,
        )
        timestamp_us, _, _ = runtime._resolve_frame_timestamp(first_frame)
        runtime._last_frame_start_us = timestamp_us
        runtime._last_frame_duration_us = 1_000_000
        second_frame = ChannelFrame(
            sample_rate=1000.0,
            channels=np.zeros((1, 1000), dtype=np.float32),
            received_at=1_700_000_002.250,
        )
        runtime._resolve_frame_timestamp(second_frame)

        wait_timeouts = [
            call.args[0] for call in runtime._gnss_time.wait_for_next_timestamp_us.call_args_list
        ]
        self.assertEqual([5.0, 1.0], wait_timeouts)
        runtime._datalink.close()

    def test_runtime_packet_timestamp_does_not_add_fractional_monotonic_elapsed_time(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime._settings.gnss = GnssSettings(
            enabled=True,
            mode="deploy",
            port="tty.usbmodem",
        )
        runtime._gnss_time.wait_for_next_timestamp_us = Mock(  # type: ignore[method-assign]
            return_value=1_700_000_001_000_000
        )
        runtime._gnss_time.status = Mock(  # type: ignore[method-assign]
            return_value=GnssStatus(
                enabled=True,
                connected=True,
                mode="deploy",
                port="tty.usbmodem",
                baudrate=115200,
                poll_interval_seconds=0.1,
                last_timestamp_us=1_700_000_001_000_000,
                last_error=None,
            )
        )

        frame = ChannelFrame(
            sample_rate=1000.0,
            channels=np.zeros((1, 1000), dtype=np.float32),
            received_at=1_700_000_000.243,
        )
        timestamp_us, used_fallback, error = runtime._resolve_frame_timestamp(frame)

        self.assertEqual(1_700_000_000_000_000, timestamp_us)
        self.assertFalse(used_fallback)
        self.assertIsNone(error)
        runtime._datalink.close()

    def test_runtime_uses_actual_frame_duration_for_packet_end_timestamp(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime._settings.gnss = GnssSettings(
            enabled=True,
            mode="deploy",
            port="tty.usbmodem",
        )
        runtime._gnss_time.wait_for_next_timestamp_us = Mock(  # type: ignore[method-assign]
            return_value=1_700_000_001_000_000
        )
        runtime._gnss_time.status = Mock(  # type: ignore[method-assign]
            return_value=GnssStatus(
                enabled=True,
                connected=True,
                mode="deploy",
                port="tty.usbmodem",
                baudrate=115200,
                poll_interval_seconds=0.2,
                last_timestamp_us=1_700_000_001_000_000,
                last_error=None,
            )
        )

        frame = ChannelFrame(
            sample_rate=1000.0,
            channels=np.zeros((1, 200), dtype=np.float32),
            received_at=1_700_000_000.590,
        )
        timestamp_us, used_fallback, error = runtime._resolve_frame_timestamp(frame)

        self.assertEqual(1_700_000_000_800_000, timestamp_us)
        self.assertFalse(used_fallback)
        self.assertIsNone(error)
        runtime._datalink.close()

    def test_runtime_exposes_manual_gnss_timestamp_interval_in_config(self) -> None:
        runtime = RuntimeService(AppSettings())

        runtime.update_config({"gnss": {"timestamp_interval_seconds": 0.2}})

        self.assertEqual(0.2, runtime.current_config()["gnss"]["timestamp_interval_seconds"])
        runtime._datalink.close()

    def test_runtime_exposes_gnss_packet_timestamp_timeout_in_config(self) -> None:
        runtime = RuntimeService(AppSettings())

        runtime.update_config({"gnss": {"packet_timestamp_timeout_seconds": 0.25}})

        self.assertEqual(0.25, runtime.current_config()["gnss"]["packet_timestamp_timeout_seconds"])
        runtime._datalink.close()

    def test_runtime_exposes_baseline_length_in_status_and_config(self) -> None:
        runtime = RuntimeService(AppSettings())

        updated = runtime.update_config({"processing": {"baseline_length_meters": 12.5}})
        snapshot = runtime.snapshot()

        self.assertEqual(12.5, updated["processing"]["baseline_length_meters"])
        self.assertEqual(12.5, snapshot.baseline_length_meters)
        runtime._datalink.close()

    def test_web_api_exposes_runtime_status_and_ports(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime.gnss_ports = Mock(return_value=["tty.usbmodem1101"])  # type: ignore[method-assign]
        app = create_app(runtime)

        with TestClient(app) as client:
            status_response = client.get("/api/status")
            self.assertEqual(200, status_response.status_code)
            self.assertEqual("ok", status_response.json()["status"])

            ports_response = client.get("/api/gnss/ports")
            self.assertEqual(200, ports_response.status_code)
            self.assertEqual(["tty.usbmodem1101"], ports_response.json()["payload"])

        runtime._datalink.close()

    def test_runtime_monitor_view_includes_packet_dump_and_waveform(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime._on_packet(
            TcpPacket(
                sample_rate=1000.0,
                payload_bytes=16,
                payload=b"\x01\x02\x03\x04",
                raw_bytes=b"\x01\x02\x03\x04DATA",
            )
        )
        with runtime._lock:
            runtime._snapshot.source_sample_rate = 1000.0
            runtime._snapshot.latest_raw = np.array(
                [
                    [1.0, 2.0, 3.0],
                    [4.0, 5.0, 6.0],
                ],
                dtype=np.float64,
            )

        payload = runtime.monitor_view(mode="raw", max_points=2, max_packets=10)

        self.assertTrue(payload["recent_packets"])
        self.assertEqual(2, payload["waveform"]["points"])
        self.assertEqual([[1.0, 3.0], [4.0, 6.0]], payload["waveform"]["series"])
        self.assertIn("000000", payload["recent_packets"][0]["hex_dump"])
        runtime._datalink.close()

    def test_runtime_monitor_view_slices_by_window_seconds(self) -> None:
        settings = AppSettings()
        settings.gui.plot_history_seconds = 3.0
        settings.gui.max_points_per_trace = 1000
        runtime = RuntimeService(settings)
        with runtime._lock:
            runtime._snapshot.source_sample_rate = 2.0
            runtime._snapshot.latest_raw = runtime._append_plot_history(
                runtime._snapshot.latest_raw,
                np.array([[1.0, 2.0, 3.0, 4.0]], dtype=np.float64),
                2.0,
            )
            runtime._snapshot.latest_raw = runtime._append_plot_history(
                runtime._snapshot.latest_raw,
                np.array([[5.0, 6.0, 7.0, 8.0]], dtype=np.float64),
                2.0,
            )

        payload = runtime.monitor_view(mode="raw", max_points=100, max_packets=1, window_seconds=2.0)

        self.assertEqual(4, payload["waveform"]["points"])
        self.assertEqual([[5.0, 6.0, 7.0, 8.0]], payload["waveform"]["series"])
        runtime._datalink.close()

    def test_web_api_exposes_monitor_and_processing_controls(self) -> None:
        runtime = RuntimeService(AppSettings())
        runtime.resume_processing = Mock()  # type: ignore[method-assign]
        runtime.pause_processing = Mock()  # type: ignore[method-assign]
        runtime._on_packet(
            TcpPacket(
                sample_rate=200.0,
                payload_bytes=8,
                payload=b"\x00\x01",
                raw_bytes=b"\x00\x01packet",
            )
        )
        with runtime._lock:
            runtime._snapshot.source_sample_rate = 200.0
            runtime._snapshot.latest_raw = np.ones((8, 4), dtype=np.float64)
        app = create_app(runtime)

        with TestClient(app) as client:
            monitor_response = client.get("/api/monitor?mode=raw&max_points=512&max_packets=5")
            self.assertEqual(200, monitor_response.status_code)
            monitor_payload = monitor_response.json()["payload"]
            self.assertIn("status", monitor_payload)
            self.assertIn("recent_packets", monitor_payload)
            self.assertEqual(4, monitor_payload["waveform"]["points"])

            start_response = client.post("/api/processing/start")
            self.assertEqual(200, start_response.status_code)
            runtime.resume_processing.assert_called_once()

            stop_response = client.post("/api/processing/stop")
            self.assertEqual(200, stop_response.status_code)
            runtime.pause_processing.assert_called_once()

        runtime._datalink.close()

    def test_web_api_rejects_monitor_requests_over_200000_points(self) -> None:
        runtime = RuntimeService(AppSettings())
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/api/monitor?mode=raw&max_points=200001&max_packets=5")
            self.assertEqual(422, response.status_code)

        runtime._datalink.close()

    def test_packet_decoder_rejects_oversized_payload(self) -> None:
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
        payload = build_packet(1000.0, channels, settings)

        decoder = PacketDecoder(settings, max_payload_bytes=64)
        with self.assertRaisesRegex(ValueError, "exceeds safety limit"):
            decoder.feed(payload)

    def test_runtime_marks_frame_drop_when_processing_queue_is_full(self) -> None:
        runtime = RuntimeService(AppSettings())
        for _ in range(runtime._queue.maxsize):
            runtime._queue.put_nowait(ChannelFrame(sample_rate=10.0, channels=np.zeros((8, 1), dtype=np.float64)))

        runtime._on_frame(
            ChannelFrame(
                sample_rate=100.0,
                channels=np.zeros((8, 4), dtype=np.float64),
                received_at=1_700_000_000.0,
            )
        )

        snapshot = runtime.snapshot()
        self.assertEqual(1, snapshot.frames_dropped)
        self.assertEqual("Processing queue is full", snapshot.last_error)
        runtime._datalink.close()

    def test_web_api_exposes_logs_and_extended_queue_metrics(self) -> None:
        configure_logging()
        import logging

        logging.getLogger("test.web").error("web-log-check")
        runtime = RuntimeService(AppSettings())
        queued_frame = ProcessedFrame(
            sample_rate=100.0,
            raw=np.zeros((8, 10), dtype=np.float64),
            unwrapped=np.zeros((8, 10), dtype=np.float64),
            data1=np.ones((8, 10), dtype=np.float64),
            data1_sample_rate=100.0,
            data2=np.zeros((8, 0), dtype=np.float64),
            data2_sample_rate=10.0,
            received_at=1_700_000_000.0,
            timestamp_us=1_700_000_000_000_000,
        )
        runtime._storage_sink.submit(queued_frame)
        runtime._datalink_sink.submit(queued_frame)
        with runtime._lock:
            runtime._snapshot.frames_dropped = 3
        app = create_app(runtime)

        with TestClient(app) as client:
            status_payload = client.get("/api/status").json()["payload"]
            self.assertEqual(3, status_payload["frames_dropped"])
            self.assertGreaterEqual(status_payload["storage_queue_depth"], 1)
            self.assertGreaterEqual(status_payload["datalink_publish_queue_depth"], 1)
            self.assertIsInstance(status_payload["storage_disk_total_bytes"], int)
            self.assertIsInstance(status_payload["storage_disk_usage_percent"], float)
            self.assertIn("baseline_length_meters", status_payload)

            logs_payload = client.get("/api/logs?limit=50&level=error").json()["payload"]
            self.assertTrue(any("web-log-check" in line for line in logs_payload["lines"]))

        runtime._datalink.close()

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

    def test_configure_logging_handles_missing_stderr(self) -> None:
        with patch.object(sys, "stderr", None):
            configure_logging()
            import logging

            logging.getLogger("test.logger").warning("no-stderr-check")
            logs = get_recent_logs(20)
            self.assertTrue(any("no-stderr-check" in line for line in logs))

    def test_web_api_service_uses_existing_logging_configuration(self) -> None:
        runtime = RuntimeService(AppSettings())
        settings = AppSettings().web
        service = WebApiService(runtime, settings)
        fake_server = Mock()

        with patch("datalink_host.services.web_api.uvicorn.Config") as config_cls, patch(
            "datalink_host.services.web_api.uvicorn.Server",
            return_value=fake_server,
        ), patch("datalink_host.services.web_api.threading.Thread") as thread_cls:
            fake_thread = Mock()
            thread_cls.return_value = fake_thread

            service.start()

            self.assertTrue(config_cls.called)
            self.assertIsNone(config_cls.call_args.kwargs["log_config"])
            fake_thread.start.assert_called_once()

            service.stop()
            fake_thread.join.assert_called_once()

        runtime._datalink.close()


if __name__ == "__main__":
    unittest.main()
