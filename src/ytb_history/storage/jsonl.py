"""JSONL read/write helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ytb_history.storage.atomic_write import atomic_write_text


def read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    file_path = Path(path)
    if not file_path.exists():
        return []

    rows: list[dict[str, Any]] = []
    with file_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def write_jsonl(path: str | Path, rows: list[dict[str, Any]]) -> None:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    if payload:
        payload = f"{payload}\n"
    atomic_write_text(file_path, payload)
