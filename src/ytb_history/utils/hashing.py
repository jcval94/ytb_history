"""Hashing helpers."""

from __future__ import annotations

import hashlib


def fingerprint_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def fingerprint_tags(tags: list[str]) -> str:
    normalized = "|".join(sorted(tags))
    return fingerprint_text(normalized)
