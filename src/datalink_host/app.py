from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="datalink host application")
    parser.add_argument(
        "--mode",
        choices=("gui", "runtime"),
        default="gui",
        help="Application mode.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.mode == "gui":
        from datalink_host.gui.app import main as gui_main

        return gui_main()
    if args.mode == "runtime":
        from datalink_host.service_main import main as runtime_main

        return runtime_main()
    return 0
