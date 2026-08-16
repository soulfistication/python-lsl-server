"""Subscribe to LSL event streams advertised on the local network."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Callable

from pylsl import (
    ContinuousResolver,
    StreamInlet,
    local_clock,
    proc_clocksync,
    proc_dejitter,
    proc_monotonize,
)

from lsl_server.streams import StreamFilters, describe_stream

try:
    from pylsl.util import LostError
except ImportError:  # pragma: no cover
    LostError = RuntimeError

logger = logging.getLogger(__name__)

POSTPROCESS = proc_clocksync | proc_dejitter | proc_monotonize
END_MARKERS = frozenset({"end"})
EventCallback = Callable[["ReceivedEvent"], None]


@dataclass(frozen=True)
class ReceivedEvent:
    stream_name: str
    stream_type: str
    hostname: str
    source_id: str
    uid: str
    sample: list
    timestamp: float
    received_at: float

    @property
    def latency_s(self) -> float:
        return self.received_at - self.timestamp

    @property
    def marker(self) -> str:
        if not self.sample:
            return ""
        return str(self.sample[0]).strip()

    @property
    def is_end(self) -> bool:
        return self.marker.lower() in END_MARKERS

    def as_dict(self) -> dict:
        return {
            "stream_name": self.stream_name,
            "stream_type": self.stream_type,
            "hostname": self.hostname,
            "source_id": self.source_id,
            "uid": self.uid,
            "sample": self.sample,
            "timestamp": self.timestamp,
            "received_at": self.received_at,
            "latency_s": self.latency_s,
        }


@dataclass
class _Connection:
    inlet: StreamInlet
    name: str
    stream_type: str
    hostname: str
    source_id: str
    uid: str


class EventListener:
    """Discover matching LSL outlets on the LAN and pull events from them."""

    def __init__(
        self,
        filters: StreamFilters | None = None,
        *,
        log_path: Path | None = None,
        on_event: EventCallback | None = None,
        forget_after: float = 5.0,
    ) -> None:
        self.filters = filters or StreamFilters()
        self.log_path = log_path
        self.on_event = on_event or self._print_event
        self.forget_after = forget_after
        self._log_file: IO[str] | None = None
        self._closed_uids: set[str] = set()

    def run(self) -> None:
        predicate = self.filters.predicate()
        if predicate:
            resolver = ContinuousResolver(pred=predicate, forget_after=self.forget_after)
            logger.info("Listening for LSL streams matching %s", predicate)
        else:
            resolver = ContinuousResolver(forget_after=self.forget_after)
            logger.info("Listening for all LSL streams on the LAN")

        connections: dict[str, _Connection] = {}
        self._open_log()
        try:
            while True:
                self._refresh_connections(resolver, connections)
                received = False
                for uid, conn in list(connections.items()):
                    try:
                        sample, timestamp = conn.inlet.pull_sample(timeout=0.0)
                    except LostError:
                        logger.warning("Lost stream %s", describe_stream_from_conn(conn))
                        self._close_connection(connections, uid, reason="connection lost")
                        continue
                    if sample is None:
                        continue
                    received = True
                    event = ReceivedEvent(
                        stream_name=conn.name,
                        stream_type=conn.stream_type,
                        hostname=conn.hostname,
                        source_id=conn.source_id,
                        uid=conn.uid,
                        sample=list(sample),
                        timestamp=float(timestamp),
                        received_at=local_clock(),
                    )
                    self._record(event)
                    self.on_event(event)
                    if event.is_end:
                        self._close_connection(
                            connections,
                            uid,
                            reason="received end marker",
                        )
                if not received:
                    time.sleep(0.01)
        except KeyboardInterrupt:
            logger.info("Stopped listening")
        finally:
            self._close_log()

    def _refresh_connections(
        self,
        resolver: ContinuousResolver,
        connections: dict[str, _Connection],
    ) -> None:
        visible = {info.uid(): info for info in resolver.results() if self.filters.matches(info)}
        for uid in list(connections):
            if uid not in visible:
                logger.info("Stream disappeared: %s", connections[uid].name)
                self._close_connection(connections, uid, reason="no longer visible")

        for uid, info in visible.items():
            if uid in connections or uid in self._closed_uids:
                continue
            inlet = StreamInlet(info, max_buflen=360, recover=False, processing_flags=POSTPROCESS)
            try:
                inlet.open_stream(timeout=2.0)
            except TimeoutError:
                logger.warning("Timed out opening %s; will retry", describe_stream(info))
                continue
            connections[uid] = _Connection(
                inlet=inlet,
                name=info.name(),
                stream_type=info.type(),
                hostname=info.hostname(),
                source_id=info.source_id(),
                uid=uid,
            )
            logger.info("Connected to %s", describe_stream(info))

    def _close_connection(
        self,
        connections: dict[str, _Connection],
        uid: str,
        *,
        reason: str,
    ) -> None:
        conn = connections.pop(uid, None)
        if conn is None:
            return
        self._closed_uids.add(uid)
        try:
            conn.inlet.close_stream()
        except Exception:
            logger.debug("close_stream failed for %s", conn.name, exc_info=True)
        logger.info("Closed stream %s (%s)", conn.name, reason)

    def _record(self, event: ReceivedEvent) -> None:
        if self._log_file is None:
            return
        self._log_file.write(json.dumps(event.as_dict()) + "\n")
        self._log_file.flush()

    def _open_log(self) -> None:
        if self.log_path is None:
            return
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_file = self.log_path.open("a", encoding="utf-8")
        logger.info("Appending events to %s", self.log_path)

    def _close_log(self) -> None:
        if self._log_file is not None:
            self._log_file.close()
            self._log_file = None

    @staticmethod
    def _print_event(event: ReceivedEvent) -> None:
        value = event.sample[0] if len(event.sample) == 1 else event.sample
        print(
            f"{event.timestamp:.6f}  {event.hostname}/{event.stream_name}  "
            f"{value}  latency={event.latency_s * 1000:.2f} ms",
            flush=True,
        )


def describe_stream_from_conn(conn: _Connection) -> str:
    return f"{conn.name!r} host={conn.hostname!r} uid={conn.uid!r}"
