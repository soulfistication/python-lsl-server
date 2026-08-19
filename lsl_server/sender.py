"""Publish string event markers onto the local LSL network."""

from __future__ import annotations

import logging
import socket
import sys
import time
import uuid

from pylsl import StreamInfo, StreamOutlet, local_clock

try:
    from pylsl import IRREGULAR_RATE
except ImportError:  # pragma: no cover
    IRREGULAR_RATE = 0.0

logger = logging.getLogger(__name__)

DEFAULT_STREAM_NAME = "LabEvents"
DEFAULT_SOURCE_ID = "pylsl-server-events"


class EventSender:
    """Advertise a Markers outlet that other machines on the LAN can resolve."""

    def __init__(
        self,
        name: str = DEFAULT_STREAM_NAME,
        source_id: str = DEFAULT_SOURCE_ID,
    ) -> None:
        info = StreamInfo(
            name=name,
            type="Markers",
            channel_count=1,
            nominal_srate=IRREGULAR_RATE,
            channel_format="string",
            source_id=source_id,
        )
        info.desc().append_child_value("manufacturer", "pylsl-server")
        info.desc().append_child_value("description", "LAN event markers")
        if hasattr(info, "set_channel_labels"):
            info.set_channel_labels(["marker"])
        self.outlet = StreamOutlet(info)
        self.name = name
        logger.info(
            "Advertising %r on host %s (source_id=%s). Waiting for listeners...",
            name,
            socket.gethostname(),
            source_id,
        )

    def send(self, marker: str, timestamp: float | None = None) -> float:
        stamp = local_clock() if timestamp is None else timestamp
        self.outlet.push_sample([marker], stamp)
        logger.info("Sent %r at %.6f", marker, stamp)
        return stamp

    def wait_for_consumers(self, timeout: float = 10.0) -> bool:
        logger.info("Waiting up to %.1fs for a listener to connect...", timeout)
        connected = self.outlet.wait_for_consumers(timeout)
        if connected:
            logger.info("Listener connected")
        else:
            logger.warning("No listener connected; the event may be missed")
        return connected

    def send_demo(self, interval: float = 1.0) -> None:
        n = 0
        try:
            while True:
                n += 1
                self.send(f"demo_{n}")
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Stopped sending")

    def send_stdin(self) -> None:
        logger.info("Type an event and press Enter. Ctrl-D to quit.")
        try:
            for line in sys.stdin:
                marker = line.strip()
                if marker:
                    self.send(marker)
        except KeyboardInterrupt:
            logger.info("Stopped sending")


def unique_source_id(prefix: str = DEFAULT_SOURCE_ID) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"
