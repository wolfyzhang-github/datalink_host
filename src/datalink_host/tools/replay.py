from __future__ import annotations

import argparse
import socket
import time
from pathlib import Path

from datalink_host.core.logging import configure_logging
from datalink_host.debug.capture import read_capture


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay captured TCP packets to a runtime server")
    parser.add_argument("capture", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19000)
    parser.add_argument("--speed", type=float, default=1.0)
    parser.add_argument("--no-timing", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    configure_logging()
    records = list(read_capture(args.capture))
    if not records:
        print("No records found in capture file")
        return 0

    with socket.create_connection((args.host, args.port), timeout=5.0) as sock:
        previous_time: float | None = None
        for record in records:
            if not args.no_timing and previous_time is not None:
                interval = max(record.received_at - previous_time, 0.0)
                time.sleep(interval / max(args.speed, 1e-9))
            sock.sendall(record.packet_bytes)
            previous_time = record.received_at

    print(f"Replayed {len(records)} packets to {args.host}:{args.port}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
