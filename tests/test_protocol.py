from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np

from datalink_host.core.logging import configure_logging, get_recent_logs
from datalink_host.debug.capture import PacketCaptureWriter, read_capture
from datalink_host.core.config import DataLinkSettings, ProtocolSettings, StorageSettings
from datalink_host.ingest.protocol import PacketDecoder, build_packet, packet_to_frame
from datalink_host.models.messages import ProcessedFrame
from datalink_host.processing.pipeline import compute_psd
from datalink_host.storage.miniseed import MiniSeedWriter
from datalink_host.transport.datalink import DataLinkPublisher


class ProtocolTests(unittest.TestCase):
    def test_round_trip_interleaved_packet(self) -> None:
        settings = ProtocolSettings(
            frame_header=0x12345678,
            frame_header_size=4,
            length_field_size=4,
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
            length_field_units="bytes",
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

    def test_round_trip_value_count_length_field(self) -> None:
        settings = ProtocolSettings(
            frame_header=11,
            frame_header_size=2,
            length_field_size=8,
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
