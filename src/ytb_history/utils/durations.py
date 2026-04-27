"""Duration helpers."""

from __future__ import annotations

import re

_YOUTUBE_DURATION_RE = re.compile(
    r"^P"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T"
    r"(?:(?P<hours>\d+)H)?"
    r"(?:(?P<minutes>\d+)M)?"
    r"(?:(?P<seconds>\d+)S)?"
    r")?$"
)


def parse_youtube_duration_to_seconds(value: str | None) -> int:
    if not value or not isinstance(value, str):
        return 0

    match = _YOUTUBE_DURATION_RE.match(value.strip())
    if not match:
        return 0

    parts = match.groupdict(default="0")

    days = int(parts["days"])
    hours = int(parts["hours"])
    minutes = int(parts["minutes"])
    seconds = int(parts["seconds"])

    return (days * 86400) + (hours * 3600) + (minutes * 60) + seconds
