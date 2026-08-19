"""Shared helpers for describing and matching LSL streams."""

from __future__ import annotations

from dataclasses import dataclass

from pylsl import StreamInfo


@dataclass(frozen=True)
class StreamFilters:
    stream_type: str | None = "Markers"
    name: str | None = None
    hostname: str | None = None
    source_id: str | None = None

    def predicate(self) -> str | None:
        parts: list[str] = []
        if self.stream_type:
            parts.append(f"type='{self.stream_type}'")
        if self.name:
            parts.append(f"name='{self.name}'")
        if self.hostname:
            parts.append(f"hostname='{self.hostname}'")
        if self.source_id:
            parts.append(f"source_id='{self.source_id}'")
        return " and ".join(parts) if parts else None

    def matches(self, info: StreamInfo) -> bool:
        if self.stream_type and info.type() != self.stream_type:
            return False
        if self.name and info.name() != self.name:
            return False
        if self.hostname and info.hostname() != self.hostname:
            return False
        if self.source_id and info.source_id() != self.source_id:
            return False
        return True


def describe_stream(info: StreamInfo) -> str:
    rate = info.nominal_srate()
    rate_label = "irregular" if rate == 0 else f"{rate:g} Hz"
    return (
        f"{info.name()!r} type={info.type()!r} host={info.hostname()!r} "
        f"channels={info.channel_count()} rate={rate_label} "
        f"source_id={info.source_id()!r} uid={info.uid()!r}"
    )
