from __future__ import annotations

import json
import struct
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterator


MAGIC = b"DLHCAP1\n"
HEADER_STRUCT = struct.Struct("<II")


@dataclass(slots=True)
class CaptureRecord:
    received_at: float
    sample_rate: float
    payload_bytes: int
    packet_bytes: bytes


class PacketCaptureWriter:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._fh: BinaryIO = self._path.open("ab")
        if self._fh.tell() == 0:
            self._fh.write(MAGIC)
            self._fh.flush()

    def write_record(self, *, received_at: float, sample_rate: float, payload_bytes: int, packet_bytes: bytes) -> None:
        metadata = json.dumps(
            {
                "received_at": received_at,
                "sample_rate": sample_rate,
                "payload_bytes": payload_bytes,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        with self._lock:
            self._fh.write(HEADER_STRUCT.pack(len(metadata), len(packet_bytes)))
            self._fh.write(metadata)
            self._fh.write(packet_bytes)
            self._fh.flush()

    def close(self) -> None:
        with self._lock:
            self._fh.close()


def read_capture(path: Path) -> Iterator[CaptureRecord]:
    with path.open("rb") as fh:
        magic = fh.read(len(MAGIC))
        if magic != MAGIC:
            raise ValueError(f"Invalid capture file magic for {path}")
        while True:
            header = fh.read(HEADER_STRUCT.size)
            if not header:
                break
            if len(header) != HEADER_STRUCT.size:
                raise ValueError("Truncated capture header")
            metadata_len, packet_len = HEADER_STRUCT.unpack(header)
            metadata_bytes = fh.read(metadata_len)
            packet_bytes = fh.read(packet_len)
            if len(metadata_bytes) != metadata_len or len(packet_bytes) != packet_len:
                raise ValueError("Truncated capture record")
            metadata = json.loads(metadata_bytes.decode("utf-8"))
            yield CaptureRecord(
                received_at=float(metadata["received_at"]),
                sample_rate=float(metadata["sample_rate"]),
                payload_bytes=int(metadata["payload_bytes"]),
                packet_bytes=packet_bytes,
            )
