"""Batching helpers."""

from __future__ import annotations

from collections.abc import Iterable


def chunked(items: Iterable[str], size: int) -> list[list[str]]:
    if size <= 0:
        raise ValueError("size must be positive")
    batch: list[str] = []
    output: list[list[str]] = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            output.append(batch)
            batch = []
    if batch:
        output.append(batch)
    return output
