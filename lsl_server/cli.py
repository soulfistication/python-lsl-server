"""Command-line interface for the LSL LAN event server."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from lsl_server.discover import list_streams
from lsl_server.listener import EventListener
from lsl_server.sender import DEFAULT_SOURCE_ID, DEFAULT_STREAM_NAME, EventSender
from lsl_server.streams import StreamFilters


def main(argv: list[str] | None = None) -> int:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print connection and discovery details.",
    )
    parser = argparse.ArgumentParser(
        prog="python -m lsl_server",
        description="Listen for Lab Streaming Layer events on the local network.",
        parents=[common],
    )
    sub = parser.add_subparsers(dest="command", required=True)

    listen = sub.add_parser(
        "listen",
        parents=[common],
        help="Subscribe to event streams on the LAN.",
    )
    _add_filter_args(listen)
    listen.add_argument(
        "--log",
        type=Path,
        help="Append received events to this JSONL file.",
    )

    send = sub.add_parser(
        "send",
        parents=[common],
        help="Advertise a test event stream (run on the other machine).",
    )
    send.add_argument("--name", default=DEFAULT_STREAM_NAME, help="LSL stream name.")
    send.add_argument("--source-id", default=DEFAULT_SOURCE_ID, help="Stable LSL source id.")
    send.add_argument("--event", help="Send this one marker and exit.")
    send.add_argument(
        "--interval",
        type=float,
        help="Send incrementing demo markers at this interval in seconds.",
    )
    send.add_argument(
        "--wait",
        type=float,
        default=10.0,
        help="Seconds to wait for a listener before sending --event (default: 10).",
    )

    discover = sub.add_parser(
        "discover",
        parents=[common],
        help="List LSL streams currently visible on the LAN.",
    )
    _add_filter_args(discover, default_type=None)
    discover.add_argument(
        "--wait",
        type=float,
        default=2.0,
        help="Seconds to search the network (default: 2).",
    )

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.command == "listen":
        return _listen(args)
    if args.command == "send":
        return _send(args)
    if args.command == "discover":
        return _discover(args)
    parser.error(f"unknown command: {args.command}")
    return 2


def _add_filter_args(parser: argparse.ArgumentParser, default_type: str | None = "Markers") -> None:
    parser.add_argument(
        "--type",
        dest="stream_type",
        default=default_type,
        help="LSL content type to match. Default for listen is Markers. Use '' for any type.",
    )
    parser.add_argument("--name", help="Only match this stream name.")
    parser.add_argument("--hostname", help="Only match streams advertised by this machine.")
    parser.add_argument("--source-id", help="Only match this LSL source id.")


def _filters_from_args(args: argparse.Namespace) -> StreamFilters:
    stream_type = args.stream_type or None
    return StreamFilters(
        stream_type=stream_type,
        name=args.name,
        hostname=args.hostname,
        source_id=args.source_id,
    )


def _listen(args: argparse.Namespace) -> int:
    EventListener(filters=_filters_from_args(args), log_path=args.log).run()
    return 0


def _send(args: argparse.Namespace) -> int:
    sender = EventSender(name=args.name, source_id=args.source_id)
    if args.event is not None:
        sender.wait_for_consumers(timeout=args.wait)
        sender.send(args.event)
        time.sleep(0.5)
        return 0
    if args.interval is not None:
        sender.send_demo(interval=args.interval)
        return 0
    sender.send_stdin()
    return 0


def _discover(args: argparse.Namespace) -> int:
    lines = list_streams(wait_time=args.wait, filters=_filters_from_args(args))
    if not lines:
        print("No matching LSL streams found on the LAN.", file=sys.stderr)
        return 1
    for line in lines:
        print(line)
    return 0
