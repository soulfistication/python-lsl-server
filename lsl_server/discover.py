"""One-shot discovery of LSL streams visible on the LAN."""

from __future__ import annotations

from pylsl import resolve_streams

from lsl_server.streams import StreamFilters, describe_stream


def list_streams(wait_time: float = 2.0, filters: StreamFilters | None = None) -> list[str]:
    filters = filters or StreamFilters(stream_type=None)
    found = [info for info in resolve_streams(wait_time=wait_time) if filters.matches(info)]
    return [describe_stream(info) for info in found]
