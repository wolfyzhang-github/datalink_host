from __future__ import annotations

import argparse
from collections.abc import Sequence


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="datalink host application")
    parser.add_argument(
        "--mode",
        choices=("gui", "runtime", "service"),
        default="gui",
        help="Application mode.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args, remaining = build_parser().parse_known_args(argv)
    if args.mode == "gui":
        from datalink_host.gui.app import main as gui_main

        return gui_main()
    if args.mode == "runtime":
        from datalink_host.service_main import main as runtime_main

        return runtime_main()
    if args.mode == "service":
        from datalink_host.windows_service import main as service_main

        return service_main(remaining)
    return 0
